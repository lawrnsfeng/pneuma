//! What the service reads from its environment.
//!
//! The variable names are the original's where they still mean the same thing,
//! so a deployment running the original
//! controller today runs this one with the same manifest.

use std::net::SocketAddr;
use std::time::Duration;

use pneuma_config::{ConfigError, Env};
use secrecy::SecretString;

/// Where the `node_run` mirror is written.
///
/// Required, with no default, and that is the point. A replica that silently
/// mirrors nothing looks exactly like one that is working, and the only visible
/// symptom is `pneuma-janitor` finding no stale runs — weeks later, and
/// attributed to the janitor. `DATABASE_URL` rather than a `PNEUMA_` name for
/// the reason every other crate here gives: it is a de-facto convention (sqlx,
/// Rails, Django) rather than an inheritance. The design notes
pub const DATABASE_URL: &str = "DATABASE_URL";
/// Where Mongo is.
pub const MONGODB_URL: &str = "PNEUMA_MONGODB_URL";
/// Which database holds `runs`.
pub const MONGODB_DATABASE: &str = "PNEUMA_MONGODB_DATABASE";
/// Where NATS is.
pub const NATS_URL: &str = "PNEUMA_NATS_URL";
/// The subject new runs are announced on.
pub const RUNS_SUBJECT: &str = "PNEUMA_RUNS_SUBJECT";
/// The subject component results arrive on.
pub const RESULT_SUBJECT: &str = "PNEUMA_RESULT_SUBJECT";
/// How many runs one replica drives at once.
pub const MAX_CONCURRENT_RUNS: &str = "PNEUMA_MAX_CONCURRENT_RUNS";
/// How long one component call may take, in seconds.
pub const CALL_TIMEOUT_SECS: &str = "PNEUMA_COMPONENT_TIMEOUT_SECS";
/// Where the health endpoints bind.
pub const LISTEN: &str = "PNEUMA_LISTEN";

/// The original's Mongo defaults.
pub const DEFAULT_MONGODB_URL: &str = "mongodb://admin:password@metadb:27017";
/// The original's database name.
pub const DEFAULT_MONGODB_DATABASE: &str = "pneuma";
/// The original's NATS default.
pub const DEFAULT_NATS_URL: &str = "nats://nats:4222";
/// The subject a run is announced on.
pub const DEFAULT_RUNS_SUBJECT: &str = "pneuma.run.start";
/// The subject a step's result is published to.
///
/// `ResultQueue` in the original service's configuration.
/// Named here as
/// its own variable because it is a *fixed* subject shared by every run and
/// every replica, which is the fact the whole correlation table exists for.
pub const DEFAULT_RESULT_SUBJECT: &str = "pneuma.result";
/// Where to listen when [`LISTEN`] is unset.
pub const DEFAULT_LISTEN: &str = "0.0.0.0:9083";
/// How many runs one replica drives at once, by default.
///
/// A replica holds one `Execution` per run in memory and one entry per
/// outstanding call, so this bounds the memory a replica can occupy. Modest,
/// because the work is elsewhere: a run in flight is a run waiting on a
/// component.
pub const DEFAULT_MAX_CONCURRENT_RUNS: i64 = 64;
/// The largest any of the numbers above may be.
///
/// The seconds ceiling exists for the reason `pneuma-admission`'s does: past
/// tokio's range the sleep arithmetic panics, which is a process that binds,
/// reports ready, and dies later.
pub const MAX_BOUND: i64 = 24 * 60 * 60;

/// Everything the service needs to run.
#[derive(Debug, Clone)]
pub struct Config {
    /// Where a run's steps are mirrored.
    pub database_url: SecretString,
    /// The Mongo connection string.
    pub mongodb_uri: SecretString,
    /// Which database holds `runs`.
    pub mongodb_database: String,
    /// The NATS connection string.
    pub nats_uri: SecretString,
    /// The subject new runs are announced on.
    pub runs_subject: String,
    /// The subject component results arrive on.
    pub results_subject: String,
    /// How many runs one replica drives at once.
    pub max_concurrent_runs: usize,
    /// How long one component call may take.
    pub call_timeout: Duration,
    /// Where the health endpoints bind.
    pub listen: SocketAddr,
}

/// Why the service will not start.
#[derive(Debug, thiserror::Error)]
pub enum ConfigureError {
    /// A variable is missing or unusable.
    #[error("{0}")]
    Variable(#[from] ConfigError),

    /// The database a run's steps are mirrored into was not configured.
    ///
    /// A unit variant rather than one carrying the name: only one variable can
    /// be missing here, so a field would always hold the same constant. The
    /// message still names it.
    #[error("{DATABASE_URL} is required")]
    NoDatabase,

    /// The listen address is not an address.
    #[error("{LISTEN} is not a socket address: {value:?}")]
    Listen {
        /// What was given.
        value: String,
    },

    /// A number outside the range that means anything.
    #[error("{name} must be between 1 and {MAX_BOUND}, got {value}")]
    OutOfRange {
        /// Which variable.
        name: &'static str,
        /// What was given.
        value: i64,
    },
}

impl Config {
    /// Reads the whole configuration, or refuses to start.
    pub fn from_env(env: &Env) -> Result<Self, ConfigureError> {
        let listen_raw = defaulted(env, LISTEN, DEFAULT_LISTEN)?;
        let Ok(listen) = listen_raw.parse::<SocketAddr>() else {
            return Err(ConfigureError::Listen { value: listen_raw });
        };
        let runs = bounded(
            MAX_CONCURRENT_RUNS,
            env.parse_or(MAX_CONCURRENT_RUNS, DEFAULT_MAX_CONCURRENT_RUNS)?,
        )?;
        let seconds = bounded(
            CALL_TIMEOUT_SECS,
            env.parse_or(CALL_TIMEOUT_SECS, crate::nats::DEFAULT_TIMEOUT_SECS_I64)?,
        )?;
        // Trimmed and then required, like every other identifier here: blank
        // is what `${PG_DSN}` renders to in a compose file when the outer
        // variable is not set, and taking it literally is a pod that starts and
        // mirrors nothing.
        let database_url = env
            .lookup(DATABASE_URL)?
            .map(|value| value.trim().to_owned());
        let Some(database_url) = database_url.filter(|value| !value.is_empty()) else {
            return Err(ConfigureError::NoDatabase);
        };
        Ok(Config {
            database_url: SecretString::from(database_url),
            mongodb_uri: SecretString::from(defaulted(env, MONGODB_URL, DEFAULT_MONGODB_URL)?),
            mongodb_database: defaulted(env, MONGODB_DATABASE, DEFAULT_MONGODB_DATABASE)?,
            nats_uri: SecretString::from(defaulted(env, NATS_URL, DEFAULT_NATS_URL)?),
            runs_subject: defaulted(env, RUNS_SUBJECT, DEFAULT_RUNS_SUBJECT)?,
            results_subject: defaulted(env, RESULT_SUBJECT, DEFAULT_RESULT_SUBJECT)?,
            // The cast cannot lose anything: `bounded` refused everything
            // outside `1..=MAX_BOUND`, and every one of those is a `usize`.
            max_concurrent_runs: usize::try_from(runs).unwrap_or(usize::MAX),
            call_timeout: Duration::from_secs(seconds.unsigned_abs()),
            listen,
        })
    }
}

/// A variable with a default, where blank means unset.
///
/// Blank is what `${VAR}` renders to when `VAR` is not set, so taking it
/// literally turns an unset variable into an empty subject.
fn defaulted(env: &Env, key: &str, default: &str) -> Result<String, ConfigureError> {
    let set = env
        .lookup(key)?
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty());
    Ok(set.unwrap_or_else(|| default.to_owned()))
}

/// Refuses a number outside `1..=MAX_BOUND`.
fn bounded(name: &'static str, value: i64) -> Result<i64, ConfigureError> {
    if (1..=MAX_BOUND).contains(&value) {
        return Ok(value);
    }
    Err(ConfigureError::OutOfRange { name, value })
}
