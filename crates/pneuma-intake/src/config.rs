//! What the service reads from its environment.
//!
//! The variable names are the original's, so a deployment that runs
//! the original's bootstrap today runs this one with the same manifest.
//! The exceptions are the two that no
//! longer mean the same thing, and both are named rather than reused: the
//! original's `BACKEND_URI`/`BACKEND_TOPIC` publish a `MessageInit` to NATS for
//! the controller, while here a run is offered to `pneuma-admission` over
//! HTTP.

use std::net::SocketAddr;
use std::time::Duration;

use pneuma_amqp::{AmqpNameError, QueueName, RoutingKey};
use pneuma_config::{ConfigError, Env};
use pneuma_transport::{Backoff, BackoffError};
use secrecy::SecretString;

/// Where Mongo is.
pub const MONGODB_URL: &str = "PNEUMA_MONGODB_URL";
/// Which database holds `runs` and `pipelines`.
pub const MONGODB_DATABASE: &str = "PNEUMA_MONGODB_DATABASE";
/// Where RabbitMQ is.
pub const AMQP_URL: &str = "PNEUMA_AMQP_URL";
/// The queue runs arrive on.
pub const RUNS_QUEUE: &str = "PNEUMA_RUNS_QUEUE";
/// The queue lifecycle events arrive on.
pub const EVENTS_QUEUE: &str = "PNEUMA_EVENTS_QUEUE";
/// The queue pipeline definitions arrive on.
pub const PIPELINES_QUEUE: &str = "PNEUMA_PIPELINES_QUEUE";
/// How long to wait after the first failed reconnection, in seconds.
pub const RECONNECT_DELAY: &str = "PNEUMA_RECONNECT_DELAY_SECS";
/// Where admitted runs are offered.
pub const ADMISSION_URL: &str = "PNEUMA_ADMISSION_URL";
/// Where validated events are forwarded.
pub const EVENT_ROUTING_KEY: &str = "PNEUMA_EVENT_ROUTING_KEY";
/// Where the health endpoints bind.
pub const LISTEN: &str = "PNEUMA_LISTEN";

/// The original's Mongo defaults.
pub const DEFAULT_MONGODB_URL: &str = "mongodb://admin:password@metadb:27017";
/// The original's database name.
pub const DEFAULT_MONGODB_DATABASE: &str = "pneuma";
/// The original's broker default.
pub const DEFAULT_AMQP_URL: &str = "amqp://guest:guest@rmq";
/// The original's input queue.
pub const DEFAULT_RUNS_QUEUE: &str = "pneuma.input";
/// The original's event queue.
pub const DEFAULT_EVENTS_QUEUE: &str = "pneuma.event";
/// The original's create-pipeline queue.
pub const DEFAULT_PIPELINES_QUEUE: &str = "pneuma.pipeline.create";
/// Where to listen when [`LISTEN`] is unset.
pub const DEFAULT_LISTEN: &str = "0.0.0.0:9082";
/// The original's reconnect delay, which becomes the backoff's floor.
pub const DEFAULT_RECONNECT_SECS: i64 = 5;
/// The longest the backoff grows to.
pub const MAX_RECONNECT_SECS: i64 = 60;
/// How long a connection must last to count as having worked.
pub const STABLE_SECS: i64 = 60;
/// The largest number of seconds any of the above may be set to.
///
/// A day. The ceiling exists for the same reason `pneuma-admission`'s does:
/// past chrono's and tokio's ranges these arithmetic operations panic, which is
/// a process that binds, reports ready, and dies later.
pub const MAX_SECONDS: i64 = 24 * 60 * 60;

/// Everything the service needs to run.
#[derive(Debug, Clone)]
pub struct Config {
    /// The Mongo connection string.
    pub mongodb_uri: SecretString,
    /// Which database holds the collections.
    pub mongodb_database: String,
    /// The AMQP connection string.
    pub rabbitmq_uri: SecretString,
    /// The queue runs arrive on.
    pub runs: QueueName,
    /// The queue events arrive on.
    pub events: QueueName,
    /// The queue definitions arrive on.
    pub definitions: QueueName,
    /// Where validated events are forwarded.
    pub event_key: RoutingKey,
    /// Where admitted runs are offered.
    pub admission: String,
    /// Where the health endpoints bind.
    pub listen: SocketAddr,
    /// How reconnection backs off.
    pub backoff: Backoff,
}

/// Why the service will not start.
#[derive(Debug, thiserror::Error)]
pub enum ConfigureError {
    /// A variable is missing or unusable.
    #[error("{0}")]
    Variable(#[from] ConfigError),

    /// A queue name or routing key a broker would refuse.
    ///
    /// Refused at startup rather than at the first declare, which is where the
    /// derived dead-letter name would otherwise fail — a 244-byte queue name is
    /// legal and its `.dead_letter` is not.
    #[error("{key} is not a usable name: {source}")]
    Name {
        /// Which variable.
        key: &'static str,
        /// Why the broker would refuse it.
        source: AmqpNameError,
    },

    /// The listen address is not an address.
    #[error("{LISTEN} is not a socket address: {value:?}")]
    Listen {
        /// What was given.
        value: String,
    },

    /// A number of seconds outside the range that means anything.
    #[error("{name} must be between 1 and {MAX_SECONDS}, got {value}")]
    OutOfRange {
        /// Which variable.
        name: &'static str,
        /// What was given.
        value: i64,
    },

    /// The backoff those seconds describe is not one.
    #[error("{0}")]
    Backoff(#[from] BackoffError),
}

impl Config {
    /// Reads the whole configuration, or refuses to start.
    pub fn from_env(env: &Env) -> Result<Self, ConfigureError> {
        let listen_raw = defaulted(env, LISTEN, DEFAULT_LISTEN)?;
        let Ok(listen) = listen_raw.parse::<SocketAddr>() else {
            return Err(ConfigureError::Listen { value: listen_raw });
        };
        let seconds = bounded(
            RECONNECT_DELAY,
            env.parse_or(RECONNECT_DELAY, DEFAULT_RECONNECT_SECS)?,
        )?;
        let backoff = Backoff::new(
            Duration::from_secs(seconds.unsigned_abs()),
            Duration::from_secs(MAX_RECONNECT_SECS.unsigned_abs()),
            Duration::from_secs(STABLE_SECS.unsigned_abs()),
        )?;
        Ok(Config {
            mongodb_uri: SecretString::from(defaulted(env, MONGODB_URL, DEFAULT_MONGODB_URL)?),
            mongodb_database: defaulted(env, MONGODB_DATABASE, DEFAULT_MONGODB_DATABASE)?,
            rabbitmq_uri: SecretString::from(defaulted(env, AMQP_URL, DEFAULT_AMQP_URL)?),
            runs: queue(env, RUNS_QUEUE, DEFAULT_RUNS_QUEUE)?,
            events: queue(env, EVENTS_QUEUE, DEFAULT_EVENTS_QUEUE)?,
            definitions: queue(env, PIPELINES_QUEUE, DEFAULT_PIPELINES_QUEUE)?,
            event_key: key(env, EVENT_ROUTING_KEY, DEFAULT_EVENTS_QUEUE)?,
            admission: env.require_non_empty(ADMISSION_URL)?,
            listen,
            backoff,
        })
    }
}

/// A variable with a default, where blank means unset.
///
/// Blank is what `${VAR}` renders to in a compose file when `VAR` is not set,
/// so taking it literally turns an unset variable into an empty queue name.
/// Trimmed for the same reason `pneuma_janitor::connect` trims: a value read
/// out of a rendered secret carries a trailing newline routinely.
fn defaulted(env: &Env, key: &str, default: &str) -> Result<String, ConfigureError> {
    let set = env
        .lookup(key)?
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty());
    Ok(set.unwrap_or_else(|| default.to_owned()))
}

/// A queue name, refused here rather than at the first declare.
fn queue(env: &Env, name: &'static str, default: &str) -> Result<QueueName, ConfigureError> {
    let raw = defaulted(env, name, default)?;
    QueueName::new(&raw).map_err(|source| ConfigureError::Name { key: name, source })
}

/// A routing key, refused here rather than at the first publish.
fn key(env: &Env, name: &'static str, default: &str) -> Result<RoutingKey, ConfigureError> {
    let raw = defaulted(env, name, default)?;
    RoutingKey::new(&raw).map_err(|source| ConfigureError::Name { key: name, source })
}

/// Refuses a number of seconds outside `1..=MAX_SECONDS`.
fn bounded(name: &'static str, value: i64) -> Result<i64, ConfigureError> {
    if (1..=MAX_SECONDS).contains(&value) {
        return Ok(value);
    }
    Err(ConfigureError::OutOfRange { name, value })
}
