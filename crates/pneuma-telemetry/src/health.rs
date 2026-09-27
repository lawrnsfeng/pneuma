//! Dependency health checks, and the report the `liveness` endpoint returns.
//!
//! Ported the source and the
//! `HealthCheckable` trait the source. The gateway
//! already separates the aggregation from its axum handler "for unit testing
//! without a full `AppState`" — this crate takes that one step further and
//! keeps the aggregation in a crate that does not depend on axum at all.
//!
//! # The wire shape is the gateway's, exactly
//!
//! `{"status": "ok" | "degraded", "checks": {"<name>": "ok" | "error: <msg>"}}`
//!
//! including the `error: ` prefix on a failed check, which is a literal part of
//! what operators and dashboards already read.
//!
//! # Two behaviours worth naming
//!
//! **A check that fails does not stop the others.** Every dependency is probed
//! on every request, so the report names all of them rather than just the first
//! one to break — which is the difference between "postgres is down" and "one
//! of your four dependencies is down".
//!
//! **Names are a map key, so a duplicate silently wins.** The gateway inserts
//! into a `HashMap` keyed by `name()`, so two checks reporting the same name
//! yield one entry and the later one overwrites. That is reproduced rather than
//! fixed — see [`LivenessReport::checks`] — because the report shape is what
//! dashboards parse, and silently growing a `"postgres-2"` key would be a wire
//! change. [`run_liveness_checks`] does record the collision in
//! [`LivenessReport::duplicate_names`] so a caller can complain at startup.
//!
//! One ordering of that is worse than it first looks, and is pinned by a test
//! so it is not mistaken for a regression later: when the *failing* check comes
//! first and a passing one with the same name follows, the pass overwrites the
//! `error:` entry while the status stays `degraded`. The body is then
//! self-contradictory —
//! `{"status":"degraded","checks":{"postgres":"ok"}}` — degraded with every
//! listed check passing. The status is still correct; the evidence for it has
//! been overwritten. `duplicate_names` is the only thing that explains it,
//! which is the argument for checking it at startup rather than at 3am.
//!
//! **Probing is sequential and unbounded.** Every `ping` is awaited in turn with
//! no timeout imposed here, so one dependency whose client has no connect or
//! read deadline hangs the whole aggregation: the endpoint never answers, and
//! the report that would have named the other three dependencies is never
//! produced. The failure then looks like a dead process rather than "postgres
//! is down". This matches the gateway, and a deadline belongs in each
//! [`HealthCheckable`] implementation where the right value is known — but it
//! is stated here because a reader could otherwise reasonably assume the
//! aggregation is bounded.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

/// Prefix the gateway puts on a failed check's message. Part of the wire shape.
const ERROR_PREFIX: &str = "error: ";

/// The string a passing check reports.
const OK: &str = "ok";

/// A dependency that can be probed for reachability.
///
/// `async_trait` rather than a native `async fn` because the checks are held as
/// `Arc<dyn HealthCheckable>`, and a native async fn in a trait is not yet
/// dyn-safe. This also matches the gateway's own definition.
#[async_trait]
pub trait HealthCheckable: Send + Sync {
    /// Returns `Ok(())` if the dependency is reachable.
    async fn ping(&self) -> Result<(), String>;

    /// Human-readable name, used as the key in the report — e.g. `postgres`.
    fn name(&self) -> &str;
}

/// Whether every dependency answered.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum HealthStatus {
    /// Every check passed.
    Ok,
    /// At least one check failed.
    Degraded,
}

impl HealthStatus {
    /// Whether this status should be served as a success.
    ///
    /// The gateway maps `ok` to 200 and `degraded` to 503. Exposed as a
    /// predicate rather than a status code so this crate stays HTTP-free.
    pub fn is_healthy(self) -> bool {
        matches!(self, HealthStatus::Ok)
    }
}

/// The body of a `liveness` response.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LivenessReport {
    /// `ok` or `degraded`.
    pub status: HealthStatus,
    /// One entry per check name: `ok`, or `error: <message>`.
    ///
    /// A `BTreeMap` rather than the gateway's `HashMap` so the JSON key order
    /// is stable. That is not a wire change — JSON objects are unordered — and
    /// it makes the output diffable between requests.
    pub checks: BTreeMap<String, String>,
    /// Names reported by more than one check, which therefore collided in
    /// [`Self::checks`].
    ///
    /// Not serialized: it is a configuration mistake for the operator, not part
    /// of the response the gateway defines.
    #[serde(skip)]
    pub duplicate_names: BTreeSet<String>,
}

/// Probes every dependency and summarises the result.
///
/// Every check runs even if an earlier one failed.
pub async fn run_liveness_checks(checks: &[Arc<dyn HealthCheckable>]) -> LivenessReport {
    let mut results: BTreeMap<String, String> = BTreeMap::new();
    let mut duplicate_names = BTreeSet::new();
    let mut healthy = true;

    for checker in checks {
        let outcome = match checker.ping().await {
            Ok(()) => OK.to_owned(),
            Err(message) => {
                healthy = false;
                format!("{ERROR_PREFIX}{message}")
            }
        };
        let name = checker.name().to_owned();
        if results.insert(name.clone(), outcome).is_some() {
            duplicate_names.insert(name);
        }
    }

    LivenessReport {
        status: if healthy {
            HealthStatus::Ok
        } else {
            HealthStatus::Degraded
        },
        checks: results,
        duplicate_names,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A check with a fixed verdict, and a counter proving it was probed.
    struct Fake {
        name: &'static str,
        result: Result<(), String>,
        probed: std::sync::atomic::AtomicUsize,
    }

    impl Fake {
        fn ok(name: &'static str) -> Arc<Self> {
            Arc::new(Fake {
                name,
                result: Ok(()),
                probed: std::sync::atomic::AtomicUsize::new(0),
            })
        }

        fn failing(name: &'static str, message: &str) -> Arc<Self> {
            Arc::new(Fake {
                name,
                result: Err(message.to_owned()),
                probed: std::sync::atomic::AtomicUsize::new(0),
            })
        }

        fn probe_count(&self) -> usize {
            self.probed.load(std::sync::atomic::Ordering::SeqCst)
        }
    }

    #[async_trait]
    impl HealthCheckable for Fake {
        async fn ping(&self) -> Result<(), String> {
            self.probed
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.result.clone()
        }

        fn name(&self) -> &str {
            self.name
        }
    }

    fn as_checks(items: Vec<Arc<Fake>>) -> Vec<Arc<dyn HealthCheckable>> {
        items
            .into_iter()
            .map(|item| item as Arc<dyn HealthCheckable>)
            .collect()
    }

    #[tokio::test]
    async fn all_passing_is_ok() {
        let report =
            run_liveness_checks(&as_checks(vec![Fake::ok("postgres"), Fake::ok("mongodb")])).await;

        assert_eq!(report.status, HealthStatus::Ok);
        assert!(report.status.is_healthy());
        assert_eq!(report.checks["postgres"], "ok");
        assert_eq!(report.checks["mongodb"], "ok");
        assert!(report.duplicate_names.is_empty());
    }

    #[tokio::test]
    async fn one_failure_degrades_the_whole_report() {
        let report = run_liveness_checks(&as_checks(vec![
            Fake::ok("postgres"),
            Fake::failing("mongodb", "connection refused"),
        ]))
        .await;

        assert_eq!(report.status, HealthStatus::Degraded);
        assert!(!report.status.is_healthy());
        assert_eq!(report.checks["postgres"], "ok");
        // The `error: ` prefix is part of the wire shape operators already read.
        assert_eq!(report.checks["mongodb"], "error: connection refused");
    }

    #[tokio::test]
    async fn a_failing_check_does_not_stop_the_others() {
        // The difference between "postgres is down" and "one of your four
        // dependencies is down".
        let first = Fake::failing("first", "boom");
        let second = Fake::ok("second");
        let third = Fake::failing("third", "also boom");
        let checks = as_checks(vec![
            Arc::clone(&first),
            Arc::clone(&second),
            Arc::clone(&third),
        ]);

        let report = run_liveness_checks(&checks).await;

        assert_eq!(first.probe_count(), 1);
        assert_eq!(second.probe_count(), 1, "a later check must still run");
        assert_eq!(third.probe_count(), 1);
        assert_eq!(report.checks.len(), 3);
        assert_eq!(report.status, HealthStatus::Degraded);
    }

    #[tokio::test]
    async fn no_checks_is_healthy() {
        // A binary with no external dependencies is not degraded.
        let report = run_liveness_checks(&[]).await;
        assert_eq!(report.status, HealthStatus::Ok);
        assert!(report.checks.is_empty());
    }

    #[tokio::test]
    async fn a_duplicate_name_collapses_and_is_reported() {
        // The gateway keys its report by name, so two checks with one name
        // yield a single entry. Reproduced, because the report shape is what
        // dashboards parse -- but surfaced, because it is a config mistake.
        let report = run_liveness_checks(&as_checks(vec![
            Fake::ok("postgres"),
            Fake::failing("postgres", "replica down"),
        ]))
        .await;

        assert_eq!(report.checks.len(), 1, "the name collided");
        assert_eq!(report.checks["postgres"], "error: replica down");
        assert!(report.duplicate_names.contains("postgres"));
        // The failure still counts, even though its entry overwrote a pass.
        assert_eq!(report.status, HealthStatus::Degraded);
    }

    #[tokio::test]
    async fn a_failure_overwritten_by_a_later_pass_still_degrades() {
        // The confusing ordering: the pass overwrites the error entry, so the
        // body says degraded while every listed check reads ok. The status is
        // right and its evidence is gone -- duplicate_names is what explains it.
        let report = run_liveness_checks(&as_checks(vec![
            Fake::failing("postgres", "replica down"),
            Fake::ok("postgres"),
        ]))
        .await;

        assert_eq!(report.status, HealthStatus::Degraded);
        assert_eq!(
            report.checks["postgres"], "ok",
            "the later pass overwrites the failure's entry"
        );
        assert!(
            report.duplicate_names.contains("postgres"),
            "the only signal that explains a degraded status with no failing check"
        );
    }

    #[tokio::test]
    async fn the_wire_shape_matches_the_gateway() -> Result<(), serde_json::Error> {
        let report = run_liveness_checks(&as_checks(vec![
            Fake::ok("postgres"),
            Fake::failing("nats", "timeout"),
        ]))
        .await;

        let json = serde_json::to_value(&report)?;
        assert_eq!(json["status"], "degraded");
        assert_eq!(json["checks"]["postgres"], "ok");
        assert_eq!(json["checks"]["nats"], "error: timeout");
        // duplicate_names is ours, not the gateway's -- it must not appear.
        assert!(json.get("duplicate_names").is_none());
        Ok(())
    }

    #[tokio::test]
    async fn checks_serialize_in_a_stable_order() -> Result<(), serde_json::Error> {
        // A BTreeMap rather than the gateway's HashMap: not a wire change,
        // since JSON objects are unordered, but it makes two responses
        // diffable.
        let report = run_liveness_checks(&as_checks(vec![
            Fake::ok("zeta"),
            Fake::ok("alpha"),
            Fake::ok("mid"),
        ]))
        .await;

        let text = serde_json::to_string(&report)?;
        assert!(text.find("alpha") < text.find("mid"), "{text}");
        assert!(text.find("mid") < text.find("zeta"), "{text}");
        Ok(())
    }

    #[test]
    fn status_serializes_lowercase() -> Result<(), serde_json::Error> {
        assert_eq!(serde_json::to_string(&HealthStatus::Ok)?, "\"ok\"");
        assert_eq!(
            serde_json::to_string(&HealthStatus::Degraded)?,
            "\"degraded\""
        );
        assert_eq!(
            serde_json::from_str::<HealthStatus>("\"degraded\"")?,
            HealthStatus::Degraded
        );
        assert!(serde_json::from_str::<HealthStatus>("\"OK\"").is_err());
        Ok(())
    }

    #[test]
    fn status_predicate_and_derives() {
        assert!(HealthStatus::Ok.is_healthy());
        assert!(!HealthStatus::Degraded.is_healthy());
        assert_eq!(HealthStatus::Ok, HealthStatus::Ok);
        assert_ne!(HealthStatus::Ok, HealthStatus::Degraded);
        assert!(format!("{:?}", HealthStatus::Degraded).contains("Degraded"));

        use std::collections::HashSet;
        let set: HashSet<_> = [HealthStatus::Ok, HealthStatus::Ok].into();
        assert_eq!(set.len(), 1);
    }

    #[tokio::test]
    async fn a_report_round_trips() -> Result<(), serde_json::Error> {
        let report = run_liveness_checks(&as_checks(vec![Fake::ok("postgres")])).await;
        let text = serde_json::to_string(&report)?;
        let back: LivenessReport = serde_json::from_str(&text)?;
        assert_eq!(back.status, report.status);
        assert_eq!(back.checks, report.checks);
        assert!(format!("{report:?}").contains("postgres"));
        assert_eq!(report.clone(), report);
        Ok(())
    }
}
