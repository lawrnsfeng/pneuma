//! What a gateway answer means, and how the client is built.
//!
//! Pure except for the stub gateway at the end, which exists because the one
//! thing a pure test cannot show is that a real `reqwest` client reaches a real
//! URL built from a real base.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::routing::post;
use axum::Router;
use pneuma_janitor::{disposition, Disposition, Gateway, TerminateError};

#[test]
fn a_conflict_is_success() {
    // The decision this module exists for. A termination that already exists
    // means the run this pass found stale is already being cancelled, which is
    // the outcome asked for -- and treating it as a failure would make a
    // cancellation spanning two passes look like a broken gateway.
    assert_eq!(disposition(409), Disposition::AlreadyTerminating);
}

#[test]
fn every_class_of_answer_has_a_disposition() {
    // Boundaries, not representatives: 199/200/299/300 are where an off-by-one
    // in the success range would show, and 499/500 where the retryable split
    // is.
    assert_eq!(disposition(200), Disposition::Submitted);
    assert_eq!(disposition(201), Disposition::Submitted);
    assert_eq!(disposition(202), Disposition::Submitted);
    assert_eq!(disposition(299), Disposition::Submitted);

    assert_eq!(disposition(199), Disposition::Refused { status: 199 });
    assert_eq!(disposition(300), Disposition::Refused { status: 300 });
    // A 404 is a wrong base URL, not a missing termination -- there is nothing
    // to be missing on a create. Refused rather than retried, so a deployment
    // error does not become a quiet loop.
    assert_eq!(disposition(404), Disposition::Refused { status: 404 });
    assert_eq!(disposition(422), Disposition::Refused { status: 422 });
    assert_eq!(disposition(499), Disposition::Refused { status: 499 });

    assert_eq!(disposition(500), Disposition::Retryable { status: 500 });
    assert_eq!(disposition(502), Disposition::Retryable { status: 502 });
    assert_eq!(disposition(503), Disposition::Retryable { status: 503 });
}

#[test]
fn a_base_that_is_not_a_url_is_refused_at_construction() {
    let Err(error) = Gateway::new("not a url", Duration::from_secs(1)) else {
        panic!("that is not a gateway address");
    };
    let TerminateError::Url { url, reason } = &error else {
        panic!("wrong variant: {error:?}");
    };
    assert_eq!(url, "not a url");
    assert_eq!(reason, "not a URL");
}

#[test]
fn a_url_that_is_not_http_is_refused_too() {
    // It parses, which is why the scheme needs its own check: `reqwest` takes
    // it and fails at send time with an error naming the scheme, once per pass,
    // for ever.
    for base in ["mailto:ops@example.com", "file:///tmp/gateway"] {
        let Err(error) = Gateway::new(base, Duration::from_secs(1)) else {
            panic!("{base} is not a gateway address");
        };
        let TerminateError::Url { reason, .. } = &error else {
            panic!("wrong variant: {error:?}");
        };
        assert!(reason.starts_with("scheme is"), "{reason}");
    }
}

#[test]
fn a_trailing_slash_reaches_the_same_url() {
    // The difference between `format!` and a join, pinned: a manifest that
    // writes the base with a trailing slash must not produce `//terminations`,
    // which some routers answer with a 404 and some with a redirect.
    let Ok(bare) = Gateway::new("http://gw:8080", Duration::from_secs(1)) else {
        panic!("that is a gateway address");
    };
    let Ok(slashed) = Gateway::new("http://gw:8080/", Duration::from_secs(1)) else {
        panic!("that is a gateway address");
    };
    assert_eq!(bare.url(), slashed.url());
    assert_eq!(
        bare.url(),
        "http://gw:8080/pneuma-gateway/api/v1/terminations"
    );

    // And surrounding whitespace, which a value read out of a rendered secret
    // carries routinely.
    let Ok(padded) = Gateway::new("  http://gw:8080\n", Duration::from_secs(1)) else {
        panic!("that is a gateway address");
    };
    assert_eq!(padded.url(), bare.url());
}

/// A gateway that answers with whatever status the test set, recording the
/// bodies it was sent.
async fn stub(status: Arc<AtomicU16>, seen: Arc<Mutex<Vec<serde_json::Value>>>) -> SocketAddr {
    let app = Router::new().route(
        "/pneuma-gateway/api/v1/terminations",
        post(move |axum::Json(body): axum::Json<serde_json::Value>| {
            let status = Arc::clone(&status);
            let seen = Arc::clone(&seen);
            async move {
                match seen.lock() {
                    Ok(mut seen) => seen.push(body),
                    Err(poisoned) => poisoned.into_inner().push(body),
                }
                axum::http::StatusCode::from_u16(status.load(Ordering::SeqCst))
                    .unwrap_or(axum::http::StatusCode::OK)
            }
        }),
    );
    let Ok(listener) = tokio::net::TcpListener::bind("127.0.0.1:0").await else {
        panic!("could not bind a stub gateway");
    };
    let Ok(address) = listener.local_addr() else {
        panic!("a bound listener has an address");
    };
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    address
}

#[tokio::test]
async fn a_termination_reaches_the_gateway_with_the_job_id_in_the_body() {
    let status = Arc::new(AtomicU16::new(201));
    let seen: Arc<Mutex<Vec<serde_json::Value>>> = Arc::new(Mutex::new(Vec::new()));
    let address = stub(Arc::clone(&status), Arc::clone(&seen)).await;
    let Ok(gateway) = Gateway::new(&format!("http://{address}"), Duration::from_secs(5)) else {
        panic!("that is a gateway address");
    };

    assert_eq!(
        gateway.create_termination("run-1").await,
        Ok(Disposition::Submitted)
    );

    // The id travels in the body, not the path -- so nothing about it needs
    // encoding, which is the defect notes's other half.
    let bodies = match seen.lock() {
        Ok(seen) => seen.clone(),
        Err(poisoned) => poisoned.into_inner().clone(),
    };
    assert_eq!(bodies.len(), 1);
    assert_eq!(bodies[0]["job_id"], serde_json::json!("run-1"));

    // And the gateway's own answer is carried through rather than flattened.
    status.store(409, Ordering::SeqCst);
    assert_eq!(
        gateway.create_termination("run-1").await,
        Ok(Disposition::AlreadyTerminating)
    );
    status.store(503, Ordering::SeqCst);
    assert_eq!(
        gateway.create_termination("run-1").await,
        Ok(Disposition::Retryable { status: 503 })
    );
}

#[tokio::test]
async fn a_gateway_that_is_not_there_is_an_error_not_a_disposition() {
    // The distinction the original collapses: nothing reached the gateway, so
    // there is no answer to interpret. Port 1 is not listening and, on a
    // machine running as root, connecting to it is refused rather than denied
    // -- which is the same `is_connect` either way.
    let Ok(gateway) = Gateway::new("http://127.0.0.1:1", Duration::from_millis(500)) else {
        panic!("that is a gateway address");
    };
    let Err(reason) = gateway.create_termination("run-1").await else {
        panic!("nothing is listening there");
    };
    assert!(!reason.is_empty(), "and it says why: {reason}");
}
