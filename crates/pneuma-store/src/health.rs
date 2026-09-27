//! Liveness probes for the two stores.
//!
//! The first implementations of `pneuma_telemetry::HealthCheckable` in the
//! workspace: the trait has been defined and unimplemented since it was
//! written, so `liveness` had a shape and nothing to report on.
//!
//! # Each probe carries its own deadline, and that is the point
//!
//! `run_liveness_checks` awaits every `ping` in turn and imposes no timeout —
//! its own header says so, and says the deadline belongs here because this is
//! where the right value is known. Without one, a dependency whose client is
//! established but unresponsive hangs the whole aggregation: the endpoint never
//! answers, the report that would have named the other dependencies is never
//! produced, and the failure reads as a dead process rather than as "postgres
//! is down". That is the worst possible answer from a liveness endpoint,
//! because Kubernetes reacts to it by restarting a process that is fine.
//!
//! Neither original bounds its ping, so this is a deliberate departure rather than a port.
//!
//! The default is short on purpose. A probe is asked on an interval measured in
//! seconds and its answer is only useful before the next one; a check that
//! takes thirty seconds to say "down" has already been overtaken by the
//! question it was answering.
//!
//! # What the message can and cannot tell you
//!
//! An earlier version of this header claimed that a driver error is an answer
//! and a timeout is the absence of one, so the two wordings distinguished
//! "refused" from "hung". **They do not**, and the difference was measured
//! rather than reasoned about. Both clients retry internally for about thirty
//! seconds by default — sqlx's pool until `acquire_timeout`, Mongo's until
//! `serverSelectionTimeoutMS` — and both are far longer than any sane probe
//! deadline, so the probe's own timeout fires first. Against a port that
//! refuses instantly:
//!
//! | client's own timeout | Postgres reports | Mongo reports |
//! |---|---|---|
//! | default (~30 s) | `did not answer within …` | `did not answer within …` |
//! | shorter than the probe's | `pool timed out …` — the refusal is dropped by sqlx | `… Connection refused (os error 111) …` |
//!
//! So: the deadline is what makes the endpoint keep answering, which is this
//! module's job and which it does. Naming the *cause* is a property of the
//! handle it was given, not of the probe — and for Postgres the cause is never
//! named at all, because sqlx's pool reports its own timeout and discards the
//! `ECONNREFUSED` beneath it.
//!
//! For a deployment that wants the cause in the report, the rule is to set the
//! client's own timeout below [`DEFAULT_PROBE_TIMEOUT`]; on Mongo that works,
//! on Postgres it only makes the "degraded" arrive sooner. Either way an
//! operator reading `postgres did not answer within 5s` should go and look at
//! postgres, which is the same action a refusal would have prompted.

use std::time::Duration;

use async_trait::async_trait;
use mongodb::bson::doc;
use mongodb::Database;
use pneuma_telemetry::HealthCheckable;
use sqlx::PgPool;

/// How long a probe waits before calling the dependency unreachable.
///
/// Five seconds: long enough to survive a garbage collection pause or a
/// momentarily busy server, short enough to answer within any sane probe
/// interval.
pub const DEFAULT_PROBE_TIMEOUT: Duration = Duration::from_secs(5);

/// The message a probe reports when its deadline passes.
///
/// It says the dependency did not answer *within the deadline*, which is
/// exactly what is known — and deliberately not "is unreachable" or "is
/// hanging", because a client that retries internally produces this wording
/// for a connection that was refused instantly. See the module header.
fn timed_out(name: &str, after: Duration) -> String {
    format!("{name} did not answer within {after:?}")
}

/// Probes Postgres with `SELECT 1`.
#[derive(Debug, Clone)]
pub struct PostgresHealth {
    pool: PgPool,
    timeout: Duration,
}

impl PostgresHealth {
    /// Probes `pool`, with [`DEFAULT_PROBE_TIMEOUT`].
    pub fn new(pool: PgPool) -> Self {
        PostgresHealth {
            pool,
            timeout: DEFAULT_PROBE_TIMEOUT,
        }
    }

    /// The same, with a deadline of the caller's choosing.
    ///
    /// A parameter because it is a deployment property, and because it is what
    /// lets a test observe the deadline being reached without waiting five
    /// seconds for it.
    pub fn with_timeout(pool: PgPool, timeout: Duration) -> Self {
        PostgresHealth { pool, timeout }
    }
}

#[async_trait]
impl HealthCheckable for PostgresHealth {
    async fn ping(&self) -> Result<(), String> {
        // `SELECT 1` rather than checking the pool's own state: a pool reports
        // idle connections it has not spoken to since they were opened, so
        // asking it is asking whether it once worked. A round trip is the only
        // thing that answers "is the server there now".
        let query = sqlx::query("SELECT 1").execute(&self.pool);
        match tokio::time::timeout(self.timeout, query).await {
            Ok(Ok(_)) => Ok(()),
            Ok(Err(error)) => Err(error.to_string()),
            Err(_) => Err(timed_out(self.name(), self.timeout)),
        }
    }

    fn name(&self) -> &str {
        // A dashboard
        // keyed on the gateway's name has to keep working against this.
        "postgres"
    }
}

/// Probes Mongo with `{ping: 1}`.
#[derive(Debug, Clone)]
pub struct MongoHealth {
    database: Database,
    timeout: Duration,
}

impl MongoHealth {
    /// Probes `database`, with [`DEFAULT_PROBE_TIMEOUT`].
    pub fn new(database: Database) -> Self {
        MongoHealth {
            database,
            timeout: DEFAULT_PROBE_TIMEOUT,
        }
    }

    /// The same, with a deadline of the caller's choosing.
    pub fn with_timeout(database: Database, timeout: Duration) -> Self {
        MongoHealth { database, timeout }
    }
}

#[async_trait]
impl HealthCheckable for MongoHealth {
    async fn ping(&self) -> Result<(), String> {
        let command = self.database.run_command(doc! { "ping": 1 });
        match tokio::time::timeout(self.timeout, command).await {
            Ok(Ok(_)) => Ok(()),
            Ok(Err(error)) => Err(error.to_string()),
            Err(_) => Err(timed_out(self.name(), self.timeout)),
        }
    }

    fn name(&self) -> &str {
        // `mongodb`, not
        // `mongo`: the name is a key in a JSON body something already parses.
        "mongodb"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // The two names are asserted in `tests/health.rs`, as the *keys of the
    // report* rather than as the return value of `name()` -- which is what
    // makes them a wire contract, and which needs a real handle to reach.

    #[test]
    fn a_timeout_message_says_it_was_silence_rather_than_an_answer() {
        // A driver error and a deadline mean different things to whoever reads
        // the report: one is the dependency answering, the other is it not.
        let message = timed_out("postgres", Duration::from_millis(250));
        assert!(message.contains("postgres"), "{message}");
        assert!(message.contains("250ms"), "{message}");
        assert!(message.contains("did not answer"), "{message}");
    }
}
