//! What the service reads from its environment.
//!
//! The variable names are the original's, so a
//! deployment running it today runs this with the same manifest — which is what
//! makes this one of the two services the plan calls independently deployable
//! against the live system.

use std::net::SocketAddr;

use pneuma_config::{ConfigError, Env};
use pneuma_nats::{Subject, SubjectError, SubjectToken};
use secrecy::SecretString;

/// Where NATS is.
pub const NATS_URL: &str = "PNEUMA_NATS_URL";
/// The subjects to demultiplex, comma-separated.
pub const INPUT_SUBJECTS: &str = "PNEUMA_INPUT_SUBJECTS";
/// The queue group replicas share.
pub const QUEUE_GROUP: &str = "PNEUMA_QUEUE_GROUP";
/// Where the health endpoints bind.
pub const LISTEN: &str = "PNEUMA_LISTEN";

/// The original's NATS default.
pub const DEFAULT_NATS_URL: &str = "nats://nats:4222";
/// The subject the original demultiplexes by default.
pub const DEFAULT_INPUT_SUBJECTS: &str = "pneuma.input";
/// The queue group replicas share by default.
pub const DEFAULT_QUEUE_GROUP: &str = "pneuma-broker";
/// Where to listen when [`LISTEN`] is unset.
pub const DEFAULT_LISTEN: &str = "0.0.0.0:9084";

/// Everything the service needs to run.
#[derive(Debug, Clone)]
pub struct Config {
    /// The NATS connection string.
    pub nats_uri: SecretString,
    /// The subjects to demultiplex.
    pub inputs: Vec<Subject>,
    /// The queue group replicas share.
    ///
    /// A [`SubjectToken`] rather than a string, for the same reason the tenant
    /// id is one: a queue group with a space or a `.` in it is one NATS
    /// refuses, and the refusal arrives at the *subscribe* — after the process
    /// has reported ready, with a consumer that never attaches and a pod that
    /// looks fine.
    pub queue: SubjectToken,
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

    /// One of the input subjects is not a subject.
    ///
    /// Refused at startup rather than at the first subscribe. A subject NATS
    /// will not accept produces a consumer that never attaches, on a process
    /// that has already reported ready.
    #[error("{INPUT_SUBJECTS} entry {entry:?} is not a subject: {source}")]
    Subject {
        /// The entry that was wrong.
        entry: String,
        /// Why it is not a subject.
        source: SubjectError,
    },

    /// The queue group is not a name NATS accepts.
    #[error("{QUEUE_GROUP} {value:?} is not a queue group: {source}")]
    Queue {
        /// What was given.
        value: String,
        /// Why NATS would refuse it.
        source: SubjectError,
    },

    /// There is nothing to demultiplex.
    ///
    /// Refused rather than tolerated: a broker with no inputs subscribes to
    /// nothing and reports healthy for ever, which is the quietest way for a
    /// tenant's messages to stop arriving.
    #[error("{INPUT_SUBJECTS} is empty; a broker with no inputs routes nothing")]
    NoInputs,
}

impl Config {
    /// Reads the whole configuration, or refuses to start.
    pub fn from_env(env: &Env) -> Result<Self, ConfigureError> {
        let listen_raw = defaulted(env, LISTEN, DEFAULT_LISTEN)?;
        let Ok(listen) = listen_raw.parse::<SocketAddr>() else {
            return Err(ConfigureError::Listen { value: listen_raw });
        };
        let inputs = subjects(&defaulted(env, INPUT_SUBJECTS, DEFAULT_INPUT_SUBJECTS)?)?;
        Ok(Config {
            nats_uri: SecretString::from(defaulted(env, NATS_URL, DEFAULT_NATS_URL)?),
            inputs,
            queue: queue_group(&defaulted(env, QUEUE_GROUP, DEFAULT_QUEUE_GROUP)?)?,
            listen,
        })
    }
}

/// Reads `a.b,c.d` into subjects.
///
/// Pure, so every way of getting the list wrong is a test rather than a
/// deployment. An empty entry is skipped -- a trailing comma is a typo, not a
/// subject -- but an empty *list* is refused.
pub fn subjects(raw: &str) -> Result<Vec<Subject>, ConfigureError> {
    let mut parsed = Vec::new();
    for entry in raw.split(',') {
        let entry = entry.trim();
        if entry.is_empty() {
            continue;
        }
        let subject = Subject::parse(entry).map_err(|source| ConfigureError::Subject {
            entry: entry.to_owned(),
            source,
        })?;
        parsed.push(subject);
    }
    if parsed.is_empty() {
        return Err(ConfigureError::NoInputs);
    }
    Ok(parsed)
}

/// A queue group NATS will accept.
///
/// The same grammar a subject token has, which is what NATS applies: no
/// separator, no whitespace, no wildcards, no control characters.
pub fn queue_group(raw: &str) -> Result<SubjectToken, ConfigureError> {
    SubjectToken::new(raw).map_err(|source| ConfigureError::Queue {
        value: raw.to_owned(),
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
