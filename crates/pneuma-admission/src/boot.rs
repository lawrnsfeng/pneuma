//! Bringing the service up.
//!
//! Everything `main` would otherwise do. `scripts/coverage.sh` excludes a
//! binary's `main` and refuses to let it hold definitions, so the startup path
//! lives here where it is measured — and what is left in `main` is reading the
//! environment and reporting a refusal.
//!
//! # Two loops, one process, one token
//!
//! The HTTP surface and the dispatch loop are peers: neither is the other's
//! supervisor, and either stopping must stop the other. A process that served
//! `/runs` after the dispatcher died would keep accepting work nothing will
//! ever send, and one that kept dispatching after the server failed to bind is
//! a replica nobody can drain. So both watch one [`CancellationToken`], and the
//! server's return cancels it whether it returned from a shutdown or from a
//! bind that failed.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use chrono::{TimeDelta, Utc};
use secrecy::ExposeSecret;
use sqlx::postgres::PgPoolOptions;
use tokio_util::sync::CancellationToken;

use pneuma_store::{PostgresHealth, SubmissionStore};

use crate::client::Ingress;
use crate::config::Config;
use crate::dispatcher::{report_line, tick, Dispatchable, Settings};
use crate::restate::Invoker;

/// The name every route is mounted under.
///
/// The crate's own name, matching `pneuma-serve`'s health routes and the
/// gateway before them: these are reached through one ingress, and an
/// unprefixed `/runs` on several services is one route several ways.
pub const SERVICE: &str = "pneuma-admission";

/// How many Postgres connections one replica may hold.
///
/// Small on purpose. This service does one short query per request and a
/// handful per round; the pool exists so those do not serialise, not so the
/// replica can hold a share of the database's connection limit proportional to
/// nothing.
pub const MAX_CONNECTIONS: u32 = 8;

/// Why the service could not start, or could not keep running.
#[derive(Debug, thiserror::Error)]
pub enum BootError {
    /// Postgres could not be reached.
    #[error("could not reach postgres: {0}")]
    Database(#[from] sqlx::Error),

    /// The client for Restate could not be built, or its URL is not one.
    ///
    /// A startup failure rather than a per-submission one, which is the whole
    /// point: an unparseable submission URL fails every dispatch as a transport
    /// error, and a transport error is retried for ever.
    #[error("{0}")]
    Ingress(#[from] crate::client::IngressError),

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
/// `bound` is called with the address actually bound, before serving, so a
/// caller that asked for port 0 can learn what it got — which is what lets the
/// whole of this function be tested without a fixed port.
pub async fn run(
    config: &Config,
    token: CancellationToken,
    bound: impl FnOnce(SocketAddr),
) -> Result<(), BootError> {
    let pool = PgPoolOptions::new()
        .max_connections(MAX_CONNECTIONS)
        .connect(config.database_url.expose_secret())
        .await?;
    let store = Arc::new(SubmissionStore::new(pool.clone()));
    let invoker = Ingress::new(&config.ingress, &config.handler)?;
    // The pool is shared with the probe rather than given its own connection:
    // a liveness check against a pool the service does not use answers about
    // the wrong thing.
    let health = pneuma_serve::router(SERVICE, vec![Arc::new(PostgresHealth::new(pool))])?;
    let app = crate::ingress::router(SERVICE, Arc::clone(&store)).merge(health);

    let settings = Settings {
        // Saturating rather than fallible: `Config` has already refused
        // anything below 1, and on this target every positive `i64` is a
        // `usize`. Saturating keeps the arithmetic total without adding a
        // startup failure for a case that cannot arise.
        batch_size: usize::try_from(config.batch_size).unwrap_or(usize::MAX),
        per_tenant: config.per_tenant,
        weights: config.weights.clone(),
    };
    let dispatching = dispatch(
        Arc::clone(&store),
        invoker,
        settings,
        config.interval,
        config.reclaim_after,
        token.clone(),
    );

    let address = config.listen;
    let stopping = token.clone();
    let serving = async move {
        let shutdown = token.clone();
        let served = pneuma_serve::serve(app, address, bound, async move {
            shutdown.cancelled().await;
        })
        .await;
        // Whatever ended the server ends the round loop with it, including a
        // bind that never succeeded -- otherwise a failed start would hang
        // here dispatching for ever instead of exiting non-zero.
        stopping.cancel();
        served
    };

    // Joined rather than spawned. A spawned loop's panic arrives as a
    // `JoinError` nobody is obliged to read, and the two arms of reading it
    // are two arms the gate cannot reach; awaited together, a panic in either
    // is a panic in this task and reaches the process the way it should.
    let (served, ()) = tokio::join!(serving, dispatching);
    match served {
        Ok(()) => Ok(()),
        Err(error) => Err(BootError::Serve {
            address,
            reason: error.to_string(),
        }),
    }
}

/// Sweeps and dispatches on `interval` until `token` is cancelled.
///
/// The body has no branch in it: [`tick`] returns a `Result` and
/// [`report_line`] renders both arms, so a database that goes away mid-loop is
/// a log line rather than an arm of this function that only a broken database
/// could reach.
async fn dispatch<Q, I>(
    queue: Arc<Q>,
    invoker: I,
    settings: Settings,
    interval: Duration,
    reclaim_after: TimeDelta,
    token: CancellationToken,
) where
    Q: Dispatchable + 'static,
    I: Invoker + 'static,
{
    let invoker = Arc::new(invoker);
    pneuma_serve::every(interval, token, move || {
        let queue = Arc::clone(&queue);
        let invoker = Arc::clone(&invoker);
        let settings = settings.clone();
        async move {
            // The cutoff is computed per tick rather than once: a process that
            // ran for a week would otherwise be sweeping against the instant it
            // started, which reclaims everything.
            let older_than = Utc::now() - reclaim_after;
            let outcome = tick(queue.as_ref(), invoker.as_ref(), &settings, older_than).await;
            eprintln!("{SERVICE}: {}", report_line(&outcome));
        }
    })
    .await;
}
