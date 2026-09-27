//! Every route, status and body — without binding a port.
//!
//! An `axum::Router` is a `tower::Service`, so `ServiceExt::oneshot` drives a
//! request through the real routing, the real extractors and the real handlers
//! and hands back the real response. No listener, no address, nothing to race
//! with another test, and nothing to wait for.

use std::sync::Arc;

use async_trait::async_trait;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use pneuma_serve::health::{router, RouterError};
use pneuma_telemetry::HealthCheckable;
use serde_json::Value;
use tower::ServiceExt;

/// A dependency that answers however the test says.
struct Stub {
    name: &'static str,
    answer: Result<(), String>,
}

#[async_trait]
impl HealthCheckable for Stub {
    async fn ping(&self) -> Result<(), String> {
        self.answer.clone()
    }
    fn name(&self) -> &str {
        self.name
    }
}

fn healthy(name: &'static str) -> Arc<dyn HealthCheckable> {
    Arc::new(Stub {
        name,
        answer: Ok(()),
    })
}

fn broken(name: &'static str, why: &str) -> Arc<dyn HealthCheckable> {
    Arc::new(Stub {
        name,
        answer: Err(why.to_owned()),
    })
}

/// The status and the parsed body of one request against a fresh router.
async fn get(checks: Vec<Arc<dyn HealthCheckable>>, path: &str) -> (StatusCode, Value) {
    let Ok(app) = router("pneuma-janitor", checks) else {
        panic!("a router with distinct check names builds");
    };
    let request = match Request::builder().uri(path).body(Body::empty()) {
        Ok(request) => request,
        Err(error) => panic!("{path} is a request: {error}"),
    };
    // No `else`: a `Router`'s error type is `Infallible`, so the `Err` arm has
    // no variants to match and the binding is exhaustive on its own.
    let Ok(response) = app.oneshot(request).await;
    let status = response.status();
    let Ok(collected) = response.into_body().collect().await else {
        panic!("the body reads");
    };
    let body = serde_json::from_slice(&collected.to_bytes()).unwrap_or(Value::Null);
    (status, body)
}

#[tokio::test]
async fn healthz_answers_while_a_dependency_is_down() {
    // The distinction Kubernetes acts on. A liveness probe failing gets the pod
    // restarted, and restarting it will not bring Postgres back -- so `healthz`
    // reports the process, and only `liveness` reports what it depends on.
    let (status, body) = get(
        vec![broken("postgres", "connection refused")],
        "/pneuma-janitor/healthz",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, serde_json::json!({"status": "ok"}));
}

#[tokio::test]
async fn liveness_is_200_when_everything_answers() {
    let (status, body) = get(
        vec![healthy("postgres"), healthy("mongo")],
        "/pneuma-janitor/liveness",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["status"], "ok");
    assert_eq!(body["checks"]["postgres"], "ok");
    assert_eq!(body["checks"]["mongo"], "ok");
}

#[tokio::test]
async fn liveness_is_503_and_names_what_failed() {
    // 503, not 500: this process is fine and its dependency is not, and that is
    // what stops a healthy pod being restarted because its database is slow.
    let (status, body) = get(
        vec![healthy("mongo"), broken("postgres", "connection refused")],
        "/pneuma-janitor/liveness",
    )
    .await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(body["status"], "degraded");
    assert_eq!(
        body["checks"]["mongo"], "ok",
        "the healthy one still reports"
    );
    // The `error: ` prefix is the gateway's wire shape, not a description.
    assert_eq!(body["checks"]["postgres"], "error: connection refused");
}

#[tokio::test]
async fn no_checks_at_all_is_healthy_rather_than_an_error() {
    // A service with no dependencies -- the broker, the original executor
    // -- still has to answer a probe.
    let (status, body) = get(vec![], "/pneuma-janitor/liveness").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["status"], "ok");
    assert_eq!(body["checks"], serde_json::json!({}));
}

#[tokio::test]
async fn the_routes_live_under_the_service_name() {
    // One ingress, six services: an unprefixed `/healthz` is one route six
    // ways. The gateway does the same (`/pneuma-gateway/healthz`).
    let (unprefixed, _) = get(vec![], "/healthz").await;
    assert_eq!(unprefixed, StatusCode::NOT_FOUND);
    let (other_service, _) = get(vec![], "/pneuma-store/healthz").await;
    assert_eq!(other_service, StatusCode::NOT_FOUND);
}

#[test]
fn two_checks_with_one_name_are_refused_before_the_process_starts() {
    // `run_liveness_checks` keys its report by name, so a collision means one
    // check silently overwrites the other: `liveness` then reports on a
    // dependency nobody is watching, and answers 200 while doing it.
    let Err(error) = router(
        "pneuma-janitor",
        vec![
            healthy("postgres"),
            broken("postgres", "x"),
            healthy("mongo"),
        ],
    ) else {
        panic!("a duplicate name must be refused");
    };
    let RouterError::DuplicateCheckNames { names } = &error else {
        panic!("wrong variant: {error:?}");
    };
    assert_eq!(names.len(), 1);
    assert!(names.contains("postgres"), "{names:?}");
    // And the message names it, because the operator has to find which two.
    assert!(error.to_string().contains("postgres"), "{error}");
}

#[test]
fn a_service_name_that_would_break_the_routes_is_refused() {
    // The name is interpolated into an axum route *pattern*, not a literal.
    // Empty gives `//healthz`; a slash silently moves both endpoints; and
    // braces are path-parameter syntax -- `"{svc}"` built a router that
    // answered `GET /anything/healthz` with 200, turning the health endpoints
    // into a wildcard that shadows every route it is merged with, while `"{"`
    // made `Router::route` panic, which is a fallible constructor aborting the
    // process rather than returning the error it exists to return.
    for name in [
        "", "a/b", "/leading", "{svc}", "{", "}", "svc name", "svc?x=1", "svc:8080", "*",
    ] {
        let Err(RouterError::UnusableServiceName { .. }) = router(name, vec![]) else {
            panic!("{name:?} is not a plain path segment");
        };
    }
    // And the names a service actually has still work.
    for name in ["pneuma-janitor", "pneuma_serve", "svc.v2", "a", "A1"] {
        assert!(router(name, vec![]).is_ok(), "{name:?} is a service name");
    }
}
