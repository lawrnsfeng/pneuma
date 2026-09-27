//! The two endpoints every binary exposes.
//!
//! The wire shape is `pneuma-gateway`'s exactly, because a probe configured
//! against one service has to work against the next:
//!
//! - `GET /{service}/healthz` → `200 {"status":"ok"}`. The process is running.
//!   Nothing is probed, so this answers while a dependency is down — which is
//!   the point: Kubernetes restarts a pod that fails a liveness probe, and
//!   restarting it will not bring Postgres back.
//! - `GET /{service}/liveness` → `200` or `503` with
//!   `{"status": "ok"|"degraded", "checks": {name: "ok"|"error: …"}}`.
//!
//! The shape matches the platform's read-API service's own health endpoint.

use std::collections::BTreeSet;
use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::get;
use axum::{Json, Router};
use pneuma_telemetry::{run_liveness_checks, HealthCheckable};

/// Why a router could not be built.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RouterError {
    /// Two checks report the same name.
    ///
    /// Refused at construction rather than served. `run_liveness_checks` keys
    /// its report by name, so a collision means one check silently overwrites
    /// the other and `liveness` reports on a dependency nobody is watching —
    /// while still answering `200`. `LivenessReport::duplicate_names` exists to
    /// notice it after the fact; this is the same fault caught before the
    /// process starts, which is where an operator can still do something about
    /// it.
    #[error("more than one health check is called {}", names.iter().cloned().collect::<Vec<_>>().join(", "))]
    DuplicateCheckNames {
        /// Every name reported by more than one check.
        names: BTreeSet<String>,
    },

    /// The service name is not a plain path segment.
    ///
    /// It is interpolated straight into the route as `/{service}/healthz`, and
    /// that string is a *pattern*, not a literal. Three ways it goes wrong, all
    /// of them measured against axum 0.8:
    ///
    /// - Empty gives `//healthz`.
    /// - A `/` silently moves both endpoints somewhere a probe is not looking.
    /// - Braces are axum's path-parameter syntax. `"{svc}"` builds a router
    ///   that answers `GET /anything/healthz` with `200` -- the health
    ///   endpoints become a wildcard that swallows every first path segment and
    ///   shadows every other route it is merged with -- and `"{"` makes
    ///   `Router::route` **panic**, which is a fallible constructor aborting
    ///   the process instead of returning the error it exists to return.
    ///
    /// So the rule is a charset rather than a list of things to avoid: what a
    /// service is called is `pneuma-janitor`, and anything that is not shaped
    /// like that is a mistake rather than a requirement.
    #[error(
        "the service name {name:?} must be a non-empty path segment of          letters, digits, '-', '_' or '.'"
    )]
    UnusableServiceName {
        /// What was given.
        name: String,
    },
}

/// The checks, shared with the handler.
#[derive(Clone)]
struct Checks(Arc<Vec<Arc<dyn HealthCheckable>>>);

/// The health routes for `service`, mounted under its own prefix.
///
/// The prefix is the service's own name, matching the gateway
/// (`/pneuma-gateway/healthz`), because these are reached through one ingress
/// and an unprefixed `/healthz` on six services is one route six ways.
pub fn router(service: &str, checks: Vec<Arc<dyn HealthCheckable>>) -> Result<Router, RouterError> {
    if !is_path_segment(service) {
        return Err(RouterError::UnusableServiceName {
            name: service.to_owned(),
        });
    }
    let duplicates = repeated_names(&checks);
    if !duplicates.is_empty() {
        return Err(RouterError::DuplicateCheckNames { names: duplicates });
    }
    Ok(Router::new()
        .route(&format!("/{service}/healthz"), get(healthz))
        .route(&format!("/{service}/liveness"), get(liveness))
        .with_state(Checks(Arc::new(checks))))
}

/// Whether `name` can be interpolated into a route as a literal segment.
///
/// An allow-list, because the failure is not a fixed set of characters: the
/// route string is an axum pattern, so anything with meaning in that grammar --
/// today `{` and `}`, tomorrow whatever the next version adds -- changes what
/// the route matches or panics while building it. A name that is letters,
/// digits and the three separators a service name actually uses cannot mean
/// anything but itself, in this version or a later one.
fn is_path_segment(name: &str) -> bool {
    !name.is_empty()
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.'))
}

/// Names claimed by more than one check.
///
/// Pure, and separate from [`router`] so the rule has a test that builds no
/// router and needs no HTTP at all.
fn repeated_names(checks: &[Arc<dyn HealthCheckable>]) -> BTreeSet<String> {
    let mut seen = BTreeSet::new();
    let mut repeated = BTreeSet::new();
    for check in checks {
        if !seen.insert(check.name().to_owned()) {
            repeated.insert(check.name().to_owned());
        }
    }
    repeated
}

/// `GET /{service}/healthz` — the process is running.
async fn healthz() -> impl IntoResponse {
    Json(serde_json::json!({"status": "ok"}))
}

/// `GET /{service}/liveness` — every dependency answered.
async fn liveness(State(checks): State<Checks>) -> impl IntoResponse {
    let report = run_liveness_checks(&checks.0).await;
    // 503, not 500. A dependency being down is not this process failing, and
    // the distinction is what stops Kubernetes restarting a healthy pod because
    // its database is slow.
    let status = if report.status.is_healthy() {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (status, Json(report))
}
