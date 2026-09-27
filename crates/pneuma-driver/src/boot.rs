//! Bringing the service up, on a `LocalSet` because it has to be.
//!
//! # `drive` cannot be `tokio::spawn`ed, and that is deliberate upstream
//!
//! `pneuma_runner::driver::Component::call` returns `impl Future` with **no**
//! `Send` bound, and its own doc records why: `restate_sdk`'s
//! `ContextSideEffects::run` makes its caller non-`Send`, so a `Send` bound
//! there would have excluded the one transport this port adopted. The cost
//! lands here — `drive` is not `Send`, so it cannot be spawned across threads.
//!
//! `tokio::task::LocalSet` is the answer, and this is the file that would have
//! been rewritten if the bound had been discovered at wiring time rather than
//! read beforehand. Runs are `spawn_local`ed onto a `LocalSet` running on this
//! thread; a semaphore bounds how many are in flight at once, because a replica
//! holds one `Execution` per run in memory.
//!
//! # One thread, and why that is not the bottleneck it looks like
//!
//! A run in flight is a run *waiting on a component*: `drive` awaits each call
//! before asking for the next task. The work is elsewhere, and the thread is
//! only ever assembling messages and matching results. What bounds this replica
//! is memory, which is what the semaphore is for.

use std::net::SocketAddr;
use std::sync::Arc;

use futures_lite::StreamExt;
use mongodb::bson::{doc, Document};
use mongodb::{Client as MongoClient, Collection};
use pneuma_core::resolver::resolve;
use pneuma_proto::envelope::MessageInit;
use pneuma_runner::driver::{drive_recording, RunInput};
use secrecy::ExposeSecret;
use tokio::sync::Semaphore;
use tokio::task::LocalSet;
use tokio_util::sync::CancellationToken;

use crate::config::Config;
use crate::correlate::Pending;
use crate::host::{describe, pipeline_of, status_for, wire_status, HostError, RunOutcome};
use crate::message::RunContext;
use crate::nats::{receive, NatsComponent};

/// The name every route is mounted under.
pub const SERVICE: &str = "pneuma-driver";

/// Why the service could not start, or could not keep running.
#[derive(Debug, thiserror::Error)]
pub enum BootError {
    /// Mongo could not be reached.
    #[error("could not reach mongo: {0}")]
    Mongo(#[from] mongodb::error::Error),

    /// The database the mirror writes to could not be reached.
    ///
    /// A `String` rather than the `sqlx::Error`, so a DSN cannot reach a log
    /// through a `Debug` -- the design notes
    #[error("could not reach the database: {0}")]
    Database(String),

    /// The database is there, but `node_run` is not.
    ///
    /// Separate from [`BootError::Database`] because the operator action is
    /// different: one is a wrong address or a database that is down, the other
    /// is a migration that has not been run -- or a `search_path` that does not
    /// reach the schema it ran in. Which of the two is decided by the SQLSTATE
    /// rather than by "the probe failed", so a role that may connect but not
    /// read `node_run` is not reported as a missing migration.
    #[error("the database has no node_run to mirror into: {0}")]
    Unmigrated(String),

    /// NATS could not be reached.
    ///
    /// A startup failure rather than the first backoff: a URI that is wrong is
    /// wrong for ever, and a pod that never becomes ready says so more usefully
    /// than one that reconnects in a loop.
    #[error("could not reach nats: {0}")]
    Nats(String),

    /// The health routes could not be built.
    #[error("{0}")]
    Router(#[from] pneuma_serve::RouterError),

    /// The listener could not be bound, or serving failed.
    #[error("could not serve on {address}: {reason}")]
    Serve {
        /// What was asked for.
        address: SocketAddr,
        /// What the operating system said.
        reason: String,
    },
}

/// Runs the service until `token` is cancelled.
///
/// **Not `Send`.** The returned future holds a [`LocalSet`], because `drive` is
/// not `Send` and cannot be spawned across threads — see the module docs. A
/// caller therefore awaits this in its own task rather than handing it to
/// `tokio::spawn`; `main` does exactly that, and so does every test here. The
/// bound is not an oversight to be worked around: it is the shape
/// `Component::call`'s deliberate lack of a `Send` promise gives everything
/// above it.
pub async fn run(
    config: &Config,
    token: CancellationToken,
    bound: impl FnOnce(SocketAddr),
) -> Result<(), BootError> {
    let mongo = MongoClient::with_uri_str(config.mongodb_uri.expose_secret()).await?;
    let database = mongo.database(&config.mongodb_database);
    let runs: Collection<Document> = database.collection("runs");

    // Connected before anything is served, so a database that is not there is a
    // pod that never becomes ready rather than one that reports healthy and
    // mirrors nothing. The design notes
    let pool = connected(config).await?;
    let mirror = pneuma_mirror::Mirror::new(pool.clone(), SERVICE);
    // And reachable is not the same as migrated: a DSN that opens onto a
    // database with no `node_run` -- migrations not run, or a `search_path`
    // that misses the schema they ran in -- would log one refusal per step for
    // ever. Bounded like the open above it, because a database that completes
    // the handshake and then stalls would otherwise block startup with no port
    // to ask and no line to read.
    if let Err(why) = mirror.ready_within(CONNECT_TIMEOUT).await {
        return Err(unready(why));
    }

    let client = async_nats::connect(config.nats_uri.expose_secret())
        .await
        .map_err(|error| BootError::Nats(error.to_string()))?;
    let pending = Arc::new(Pending::new());

    // The checks are bound first so the call fits on one line: an argument on
    // its own line of a multi-line call is not attributed to the call that ran,
    // and the gate then reports a line every run executes as unreached.
    let mongo = Arc::new(pneuma_store::MongoHealth::new(database.clone()));
    let postgres = Arc::new(pneuma_store::PostgresHealth::new(pool));
    let checks: Vec<Arc<dyn pneuma_telemetry::HealthCheckable>> = vec![mongo, postgres];
    let health = pneuma_serve::router(SERVICE, checks)?;
    let address = config.listen;
    let stopping = token.clone();
    let loops = token.clone();
    let serving = async move {
        let shutdown = loops.clone();
        let served = pneuma_serve::serve(health, address, bound, async move {
            shutdown.cancelled().await;
        })
        .await;
        // Whatever ended the server ends the two loops with it, including a
        // bind that never succeeded.
        stopping.cancel();
        served
    };

    let results = subscribe(
        &client,
        &config.results_subject,
        Arc::clone(&pending),
        token.clone(),
    );
    let announcements = accept(
        client.clone(),
        runs,
        Arc::clone(&pending),
        mirror,
        config.clone(),
        token,
    );

    let (served, (), ()) = tokio::join!(serving, results, announcements);
    match served {
        Ok(()) => Ok(()),
        Err(error) => Err(BootError::Serve {
            address,
            reason: error.to_string(),
        }),
    }
}

/// Hands every result that arrives to whichever call is waiting for it.
///
/// A plain subscription, not a queue group: a run's `Execution` lives in the
/// replica that started it, so a queue group would deliver each result to
/// exactly one replica and usually the wrong one.
async fn subscribe(
    client: &async_nats::Client,
    subject: &str,
    pending: Arc<Pending>,
    token: CancellationToken,
) {
    let subscribed = client.subscribe(subject.to_owned()).await;
    let Ok(mut results) = subscribed else {
        eprintln!("{SERVICE}: could not subscribe to {subject}");
        // Not a panic and not a silent return: without results nothing can
        // finish, so the process should go away and be restarted rather than
        // sit there accepting runs it can never complete.
        token.cancel();
        return;
    };
    while let Some(message) = token.run_until_cancelled(results.next()).await.flatten() {
        let outcome = receive(pending.as_ref(), &message.payload).await;
        report(&outcome);
    }
}

/// One line about a result, whichever way it went.
///
/// Pure-ish and separate so the subscription loop has no branch in it: the
/// arms that only a malformed message or another replica's run can reach are
/// then a test rather than a region of a loop nothing can drive.
fn report(outcome: &crate::nats::Received) {
    eprintln!("{SERVICE}: {}", crate::nats::describe_received(outcome));
}

/// Accepts announced runs and drives them, bounded by a semaphore.
async fn accept(
    client: async_nats::Client,
    runs: Collection<Document>,
    pending: Arc<Pending>,
    mirror: pneuma_mirror::Mirror,
    config: Config,
    token: CancellationToken,
) {
    let subscribed = client.subscribe(config.runs_subject.clone()).await;
    let Ok(mut announcements) = subscribed else {
        eprintln!("{SERVICE}: could not subscribe to {}", config.runs_subject);
        token.cancel();
        return;
    };
    let permits = Arc::new(Semaphore::new(config.max_concurrent_runs));
    let local = LocalSet::new();

    local
        .run_until(async {
            while let Some(message) = token
                .run_until_cancelled(announcements.next())
                .await
                .flatten()
            {
                let Ok(init) = serde_json::from_slice::<MessageInit>(&message.payload) else {
                    eprintln!("{SERVICE}: an announcement that is not a run message");
                    continue;
                };
                let Ok(permit) = Arc::clone(&permits).acquire_owned().await else {
                    // The semaphore is only closed when the process is going
                    // away, so this is a shutdown rather than a failure.
                    return;
                };
                let one = One {
                    client: client.clone(),
                    runs: runs.clone(),
                    pending: Arc::clone(&pending),
                    timeout: config.call_timeout,
                    mirror: mirror.clone(),
                };
                // `spawn_local`, not `spawn`: `drive` is not `Send`, because
                // `Component::call` deliberately promises no `Send` -- see the
                // module docs.
                tokio::task::spawn_local(async move {
                    let outcome = one.drive_run(init).await;
                    drop(permit);
                    outcome
                });
            }
        })
        .await;
}

/// Everything one run needs, so the spawned task owns it.
struct One {
    client: async_nats::Client,
    runs: Collection<Document>,
    pending: Arc<Pending>,
    timeout: std::time::Duration,
    /// Where this run's steps are written down.
    ///
    /// Cloned per run, like `client`: a `PgPool` is an `Arc` inside, so the
    /// clone shares the connections rather than opening more.
    ///
    /// There is no journal on this path -- a replica's `Execution` is lost on a
    /// crash, which the module docs are explicit about -- so these are plain
    /// at-least-once writes. The idempotency is the statements': the insert is
    /// `ON CONFLICT (path) DO NOTHING` over an id derived from the path, and
    /// every move carries its guard in the `WHERE`.
    mirror: pneuma_mirror::Mirror,
}

impl One {
    /// Drives one announced run to completion and records what happened.
    async fn drive_run(&self, init: MessageInit) {
        let run_id = init.run_id.as_str().to_owned();
        let outcome = match self.execute(&init).await {
            Ok(outcome) => outcome,
            Err(error) => RunOutcome::Failed(error.to_string()),
        };
        eprintln!(
            "{SERVICE}: run {run_id} ended as {:?}",
            status_for(&outcome)
        );
        self.settle(&run_id, &outcome).await;
    }

    /// The run itself: look it up, resolve it, drive it.
    async fn execute(&self, init: &MessageInit) -> Result<RunOutcome, HostError> {
        let run_id = init.run_id.as_str().to_owned();
        let found = self
            .runs
            .find_one(doc! { "run_id": run_id.as_str() })
            .await
            .map_err(|error| HostError::Database {
                run_id: run_id.clone(),
                reason: error.to_string(),
            })?;
        let Some(document) = found else {
            return Err(HostError::NoSuchRun { run_id });
        };
        let pipeline = pipeline_of(&run_id, document)?;
        let registry = resolve(&pipeline).map_err(|error| HostError::Unresolvable {
            run_id: run_id.clone(),
            reason: error.to_string(),
        })?;

        let component = NatsComponent::new(
            self.client.clone(),
            Arc::clone(&self.pending),
            Arc::new(registry),
            RunContext {
                meta: init.meta.clone(),
                run_id: init.run_id.clone(),
                pipeline_id: init.pipeline_id.clone(),
            },
        )
        .with_timeout(self.timeout);
        let input = RunInput {
            meta: init.meta.clone(),
            input: init.step_input.to_value(),
            custom_data: Some(serde_json::Value::Object(init.custom_data.clone())),
        };
        let driven = drive_recording(component.registry(), &input, &component, &self.mirror).await;
        Ok(match driven {
            Ok(completed) => RunOutcome::Finished(completed),
            Err(error) => RunOutcome::Failed(describe(&error)),
        })
    }

    /// Writes the run's ending into its document.
    async fn settle(&self, run_id: &str, outcome: &RunOutcome) {
        let update = doc! { "$set": { "status": wire_status(status_for(outcome)) } };
        if let Err(error) = self
            .runs
            .update_one(doc! { "run_id": run_id }, update)
            .await
        {
            eprintln!("{SERVICE}: could not record {run_id}'s ending: {error}");
        }
    }
}

/// Which refusal an unready mirror is.
///
/// Two variants because the operator action is different, and only
/// `undefined_table` means "run the migrations" -- a role that may connect but
/// not read `node_run` is a common least-privilege setup, and reporting it as a
/// missing migration sends somebody to re-run migrations that already applied.
fn unready(why: pneuma_mirror::NotReady) -> BootError {
    match why {
        pneuma_mirror::NotReady::NoTable(reason) => BootError::Unmigrated(reason),
        pneuma_mirror::NotReady::Unusable(reason) => BootError::Database(reason),
    }
}

/// Opens the pool the mirror writes through.
///
/// Its own function so the bound is on this await rather than on the pool.
/// `PoolOptions::acquire_timeout` would have been the shorter spelling and the
/// wrong one: sqlx applies it to every acquire for the life of the pool, so a
/// startup bound written there becomes a runtime one, and a burst of concurrent
/// runs contending for [`MAX_CONNECTIONS`] would start dropping rows the moment
/// a wait exceeded it.
async fn connected(config: &Config) -> Result<sqlx::PgPool, BootError> {
    let opening = sqlx::postgres::PgPoolOptions::new()
        .max_connections(pool_size(config.max_concurrent_runs))
        .connect(config.database_url.expose_secret());
    match tokio::time::timeout(CONNECT_TIMEOUT, opening).await {
        Ok(Ok(pool)) => Ok(pool),
        Ok(Err(error)) => Err(BootError::Database(error.to_string())),
        Err(_) => Err(BootError::Database(format!(
            "no answer in {CONNECT_TIMEOUT:?}"
        ))),
    }
}

/// How long startup waits for the database before refusing.
///
/// `sqlx` defaults to thirty seconds and retries a refused connection for the
/// whole of it, which is the wrong shape for a startup probe: a pod that takes
/// half a minute to say why it will not start looks like a pod that is hanging,
/// and the orchestrator's restart backoff is the right place for the patience.
pub const CONNECT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

/// How many Postgres connections one replica may hold.
///
/// Derived from `PNEUMA_MAX_CONCURRENT_RUNS` rather than fixed beside it. A run
/// drives its steps sequentially and awaits each mirror write, so a replica has
/// at most one write in flight per concurrent run — and a pool sized to
/// something *smaller* than the concurrency turns a burst into a queue at the
/// pool. That queue is bounded by sqlx's thirty-second acquire timeout, and an
/// acquire that times out is a dropped row, logged and invisible: exactly the
/// silent mirror the required `DATABASE_URL` exists to prevent, reintroduced by
/// a tuning knob.
///
/// [`MAX_CONNECTIONS`] is the ceiling, because the concurrency knob has no
/// upper bound of its own and a replica must not be able to take an unbounded
/// share of the database's connection limit. Above it, contention is real but
/// the statements are short.
pub fn pool_size(concurrent_runs: usize) -> u32 {
    let wanted = u32::try_from(concurrent_runs).unwrap_or(MAX_CONNECTIONS);
    wanted.clamp(1, MAX_CONNECTIONS)
}

/// The most connections one replica may hold, however high the concurrency is
/// turned. See [`pool_size`].
pub const MAX_CONNECTIONS: u32 = 16;
