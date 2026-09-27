//! Bringing the service up.
//!
//! Everything `main` would otherwise do. `scripts/coverage.sh` excludes a
//! binary's `main` and refuses to let it hold definitions, so the startup path
//! lives here where it is measured.
//!
//! # Three queues, one process, one token
//!
//! Each queue gets its own supervised loop, and they are peers: none of them
//! supervises the others, and any of them stopping means the process is going
//! away. The original runs the three under
//! `asyncio.gather(..., return_exceptions=True)` specifically so one crashing
//! task does not cancel the other two
//! — the same intent, reached from the
//! other side: here a loop that loses its broker reconnects rather than
//! crashing, and only a cancelled token ends it.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;

use mongodb::bson::Document;
use mongodb::{Client as MongoClient, Collection};
use pneuma_amqp::QueueName;
use pneuma_store::{MongoHealth, PipelineStore, RunStore};
use pneuma_transport::{decide, Action, Amqp, AmqpError, Backoff, Consuming, Event, QueueSpec};
use reqwest::Client;
use secrecy::ExposeSecret;
use tokio_util::sync::CancellationToken;

use crate::adapt::{Admission, Forwarder};
use crate::config::Config;
use crate::consume::pump;
use crate::handle::{handle_definition, handle_event, handle_run, Outcome};

/// The name every route is mounted under.
pub const SERVICE: &str = "pneuma-intake";

/// Why the service could not start, or could not keep running.
#[derive(Debug, thiserror::Error)]
pub enum BootError {
    /// Mongo could not be reached.
    #[error("could not reach mongo: {0}")]
    Mongo(#[from] mongodb::error::Error),

    /// The broker could not be reached at startup.
    ///
    /// A startup failure rather than the first backoff: a URI that is wrong is
    /// wrong for ever, and a pod that never becomes ready says so more usefully
    /// than one that reconnects in a loop.
    #[error("{0}")]
    Broker(#[from] AmqpError),

    /// The HTTP client could not be built.
    #[error("could not build the admission client: {0}")]
    Client(#[from] reqwest::Error),

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
pub async fn run(
    config: &Config,
    token: CancellationToken,
    bound: impl FnOnce(SocketAddr),
) -> Result<(), BootError> {
    let mongo = MongoClient::with_uri_str(config.mongodb_uri.expose_secret()).await?;
    let database = mongo.database(&config.mongodb_database);
    let pipelines = Arc::new(PipelineStore::new(collection(&database, "pipelines")));
    let runs = Arc::new(RunStore::new(collection(&database, "runs")));

    // Opened once at startup so a wrong URI is a pod that never becomes ready,
    // rather than one that reports healthy and reconnects for ever.
    let publishing = Amqp::connect(config.rabbitmq_uri.expose_secret()).await?;
    let forwarder = Arc::new(Forwarder::new(publishing, config.event_key.clone()));
    let admission = Arc::new(Admission::new(
        Client::builder().build()?,
        &config.admission,
    ));

    // Mongo is the dependency this process cannot work without, so it is the
    // one the liveness probe asks about. The broker is deliberately not probed:
    // losing it is what `supervise` is for, and a pod restarted for it would
    // come back to the same broker.
    let health = pneuma_serve::router(SERVICE, vec![Arc::new(MongoHealth::new(database))])?;
    let address = config.listen;
    let stopping = token.clone();
    let loops = token.clone();
    let serving = async move {
        let shutdown = token.clone();
        let served = pneuma_serve::serve(health, address, bound, async move {
            shutdown.cancelled().await;
        })
        .await;
        // Whatever ended the server ends the three loops with it, including a
        // bind that never succeeded.
        stopping.cancel();
        served
    };

    let uri = config.rabbitmq_uri.expose_secret().to_owned();
    let runs_loop = supervise(
        &uri,
        &config.runs,
        "intake-runs",
        config.backoff,
        loops.clone(),
        {
            let pipelines = Arc::clone(&pipelines);
            let runs = Arc::clone(&runs);
            let admission = Arc::clone(&admission);
            move |body: Vec<u8>| {
                let pipelines = Arc::clone(&pipelines);
                let runs = Arc::clone(&runs);
                let admission = Arc::clone(&admission);
                async move {
                    handle_run(
                        &body,
                        pipelines.as_ref(),
                        runs.as_ref(),
                        admission.as_ref(),
                        chrono::Utc::now(),
                    )
                    .await
                }
            }
        },
    );

    let definitions_loop = supervise(
        &uri,
        &config.definitions,
        "intake-definitions",
        config.backoff,
        loops.clone(),
        {
            let pipelines = Arc::clone(&pipelines);
            move |body: Vec<u8>| {
                let pipelines = Arc::clone(&pipelines);
                async move { handle_definition(&body, pipelines.as_ref()).await }
            }
        },
    );

    let events_loop = supervise(
        &uri,
        &config.events,
        "intake-events",
        config.backoff,
        loops.clone(),
        {
            let forwarder = Arc::clone(&forwarder);
            move |body: Vec<u8>| {
                let forwarder = Arc::clone(&forwarder);
                async move { handle_event(&body, forwarder.as_ref()).await }
            }
        },
    );

    let (served, (), (), ()) = tokio::join!(serving, runs_loop, definitions_loop, events_loop);
    match served {
        Ok(()) => Ok(()),
        Err(error) => Err(BootError::Serve {
            address,
            reason: error.to_string(),
        }),
    }
}

/// The collection, named once so a typo is in one place.
fn collection(database: &mongodb::Database, name: &str) -> Collection<Document> {
    database.collection(name)
}

/// Consumes `queue` for ever, reconnecting the way `decide` says.
///
/// The loop has no rule in it: `pneuma_transport::decide` is the rule and is pure,
/// so the branches only a misbehaving broker reaches are tested there against
/// an enum rather than here against a broker somebody has to break.
pub async fn supervise<H, F>(
    uri: &str,
    queue: &QueueName,
    tag: &str,
    backoff: Backoff,
    token: CancellationToken,
    mut handle: H,
) where
    H: FnMut(Vec<u8>) -> F,
    F: std::future::Future<Output = Outcome>,
{
    let mut attempt = 0;
    // The loop's condition rather than a `return` inside it: a bare `return` in
    // a match arm is not attributed to the branch that took it, so the gate
    // cannot see the shutdown path run even when it does.
    let mut running = true;
    while running {
        // Raced against the token rather than checked between attempts. A
        // consumer sits in `next()` until the broker sends something, which on
        // a quiet queue is for ever -- so a loop that only checked the token
        // between reconnections would never notice a shutdown at all, and the
        // process would hang until it was killed. Found by a test that hung.
        let attached = token.run_until_cancelled(attach(uri, queue, tag, &mut handle));
        let event = match attached.await {
            None => Event::Shutdown,
            Some(event) => event,
        };
        let step = decide(&event, attempt, backoff);
        attempt = step.attempt;
        running = match step.action {
            // `Consume` is `decide`'s answer to `Event::Up`, which this loop
            // never sends: it attaches and consumes in one step, so the only
            // events it produces are a failure, a drop, or a shutdown. Folded
            // in with `Stop` rather than left as an arm nothing can reach.
            Action::Stop | Action::Consume => false,
            Action::Reconnect { after, why } => {
                eprintln!("{tag}: {why}; reconnecting in {after:?}");
                // The result is discarded on purpose. A token cancelled during
                // the wait cancels the *next* attach immediately, which becomes
                // `Event::Shutdown` and ends the loop at the one place it ends
                // -- rather than adding a second exit here that only a
                // cancelled sleep can reach.
                let _ = token.run_until_cancelled(tokio::time::sleep(after)).await;
                true
            }
        };
    }
}

/// One attach-and-consume, reported as the event that ended it.
///
/// Public so the failure paths can be tested without a race. Driving them
/// through [`supervise`] means spawning a loop and cancelling it after a
/// sleep — and under coverage instrumentation the spawned task is not always
/// scheduled before that sleep elapses, so the loop is cancelled having done
/// nothing and the test still passes. Calling this directly is deterministic.
pub async fn attach<H, F>(uri: &str, queue: &QueueName, tag: &str, handle: &mut H) -> Event
where
    H: FnMut(Vec<u8>) -> F,
    F: std::future::Future<Output = Outcome>,
{
    let up = Instant::now();
    match connect(uri, queue, tag).await {
        // A call rather than the constructor inline: a match arm whose whole
        // body is an enum constructor is not attributed to the branch that
        // took it, and the log showed this one running thousands of times
        // while the gate reported it unreached.
        Err(why) => failed(why),
        // The connection is held for as long as the consumer is: dropping the
        // last `Amqp` closes it, taking the channel the consumer reads from
        // with it.
        Ok((_amqp, mut consuming)) => pump(&mut consuming, up, handle).await,
    }
}

/// A failure to attach, as the event the supervisor takes.
fn failed(why: String) -> Event {
    Event::ConnectFailed(why)
}

/// Connects, declares both queues, and attaches — or says why it could not.
///
/// Three fallible steps and three `?`, rather than three `match`es each ending
/// in a bare `return`: a bare `return` in a match arm is not attributed to the
/// branch that took it, so the gate could not see the middle one run even
/// though a test made it run.
async fn connect(uri: &str, queue: &QueueName, tag: &str) -> Result<(Amqp, Consuming), String> {
    let amqp = Amqp::connect(uri)
        .await
        .map_err(|error| error.to_string())?;
    let spec = QueueSpec::new(queue.clone()).map_err(|error| error.to_string())?;
    let consuming = amqp
        .consume(&spec, tag)
        .await
        .map_err(|error| error.to_string())?;
    Ok((amqp, consuming))
}
