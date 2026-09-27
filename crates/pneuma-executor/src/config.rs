//! What the service reads from its environment.
//!
//! The variable names are the original's, so a
//! deployment running it today runs this with the same manifest.

use std::net::SocketAddr;
use std::time::Duration;

use pneuma_config::{ConfigError, Env};
use pneuma_nats::{Subject, SubjectError};
use secrecy::SecretString;

/// Where NATS is.
pub const NATS_URL: &str = "PNEUMA_NATS_URL";
/// The subject work arrives on.
pub const WORK_SUBJECT: &str = "PNEUMA_WORK_SUBJECT";
/// The subject results are published to.
pub const RESULT_SUBJECT: &str = "PNEUMA_RESULT_SUBJECT";
/// The subject events are published to.
pub const EVENT_SUBJECT: &str = "PNEUMA_EVENT_SUBJECT";
/// Where the component is.
pub const COMPONENT_ENDPOINT: &str = "PNEUMA_COMPONENT_ENDPOINT";
/// How long one call to the component may take, in seconds.
pub const COMPONENT_TIMEOUT: &str = "PNEUMA_COMPONENT_TIMEOUT_SECS";
/// How many times a retryable failure is tried.
pub const COMPONENT_ATTEMPTS: &str = "PNEUMA_COMPONENT_ATTEMPTS";
/// How many messages one replica handles at once.
pub const MAX_CONCURRENT_STEPS: &str = "PNEUMA_MAX_CONCURRENT_STEPS";
/// Where the health endpoints bind.
pub const LISTEN: &str = "PNEUMA_LISTEN";

/// The original's NATS default.
pub const DEFAULT_NATS_URL: &str = "nats://nats:4222";
/// The subject the original consumes.
pub const DEFAULT_WORK_SUBJECT: &str = "pneuma.step";
/// The subject the original publishes results to.
pub const DEFAULT_RESULT_SUBJECT: &str = "pneuma.result";
/// The subject the original publishes events to.
pub const DEFAULT_EVENT_SUBJECT: &str = "pneuma.event";
/// Where to listen when [`LISTEN`] is unset.
pub const DEFAULT_LISTEN: &str = "0.0.0.0:9085";
/// How many messages one replica handles at once, by default.
pub const DEFAULT_MAX_CONCURRENT_STEPS: i64 = 8;
/// The largest any of the numbers above may be.
pub const MAX_BOUND: i64 = 24 * 60 * 60;

/// Everything the service needs to run.
#[derive(Debug, Clone)]
pub struct Config {
    /// The NATS connection string.
    pub nats_uri: SecretString,
    /// The subject work arrives on.
    pub work: Subject,
    /// The queue group replicas share.
    ///
    /// Derived from the work subject, as the original derives it
    /// (the original replaces `.` with `-`), so
    /// replicas of this service split the work rather than duplicating it.
    ///
    /// A `String` rather than a `SubjectToken`, and the derivation is why: a
    /// subject's separators are the only characters a token forbids that a
    /// subject allows, so replacing them yields a token by construction.
    /// Re-checking it would be an error arm no input can reach — the
    /// derivation is the proof.
    pub queue: String,
    /// The subject results are published to.
    pub results: Subject,
    /// The subject events are published to.
    pub events: Subject,
    /// Where the component is.
    pub component: String,
    /// How long one call may take.
    pub request_timeout: Duration,
    /// How many times a retryable failure is tried.
    pub attempts: u32,
    /// How many messages one replica handles at once.
    pub senders: usize,
    /// Where the health endpoints bind.
    pub listen: SocketAddr,
}

/// Why the service will not start.
#[derive(Debug, thiserror::Error)]
pub enum ConfigureError {
    /// A variable is missing or unusable.
    #[error("{0}")]
    Variable(#[from] ConfigError),

    /// The listen address is not an address.
    #[error("{LISTEN} is not a socket address: {value:?}")]
    Listen {
        /// What was given.
        value: String,
    },

    /// A subject NATS would refuse.
    #[error("{key} {value:?} is not a subject: {source}")]
    Subject {
        /// Which variable.
        key: &'static str,
        /// What was given.
        value: String,
        /// Why NATS would refuse it.
        source: SubjectError,
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
        let work = subject(env, WORK_SUBJECT, DEFAULT_WORK_SUBJECT)?;
        let queue = queue_group(&work);
        let default_timeout = crate::component::DEFAULT_REQUEST_TIMEOUT_SECS_I64;
        let seconds = bounded(
            COMPONENT_TIMEOUT,
            env.parse_or(COMPONENT_TIMEOUT, default_timeout)?,
        )?;
        let default_attempts = crate::component::DEFAULT_ATTEMPTS_I64;
        let attempts = bounded(
            COMPONENT_ATTEMPTS,
            env.parse_or(COMPONENT_ATTEMPTS, default_attempts)?,
        )?;
        let senders = bounded(
            MAX_CONCURRENT_STEPS,
            env.parse_or(MAX_CONCURRENT_STEPS, DEFAULT_MAX_CONCURRENT_STEPS)?,
        )?;
        Ok(Config {
            nats_uri: SecretString::from(defaulted(env, NATS_URL, DEFAULT_NATS_URL)?),
            work,
            queue,
            results: subject(env, RESULT_SUBJECT, DEFAULT_RESULT_SUBJECT)?,
            events: subject(env, EVENT_SUBJECT, DEFAULT_EVENT_SUBJECT)?,
            component: env.require_non_empty(COMPONENT_ENDPOINT)?,
            request_timeout: Duration::from_secs(seconds.unsigned_abs()),
            // Every value `bounded` allows fits, so the fallbacks below are
            // unreachable arithmetic rather than a policy.
            attempts: u32::try_from(attempts).unwrap_or(u32::MAX),
            senders: usize::try_from(senders).unwrap_or(usize::MAX),
            listen,
        })
    }
}

/// The queue group replicas of one work subject share.
///
/// Derived rather than configured, and derived the way the original services derive
/// theirs: the subject with its separators replaced, because a queue group may
/// not contain one. Two replicas reading the same subject must be in the same
/// group or every message is handled twice — so making it a *function* of the
/// subject removes the way that goes wrong.
pub fn queue_group(work: &Subject) -> String {
    work.as_str().replace('.', "-")
}

/// A subject, refused at startup rather than at the first subscribe.
fn subject(env: &Env, key: &'static str, default: &str) -> Result<Subject, ConfigureError> {
    let raw = defaulted(env, key, default)?;
    Subject::parse(&raw).map_err(|source| ConfigureError::Subject {
        key,
        value: raw,
        source,
    })
}

/// A variable with a default, where blank means unset.
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
