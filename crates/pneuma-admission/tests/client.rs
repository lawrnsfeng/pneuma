//! The real client, against servers that answer badly on purpose.
//!
//! What Restate answers is measured in `restate.rs`; what those answers *mean*
//! is decided purely in `crate::restate`. What is left for this file is the
//! part in between — the URL, the header Restate deduplicates on, and the two
//! ways reading a response can fail — which needs a server that misbehaves on
//! command rather than a container that behaves.

use std::net::SocketAddr;

use axum::http::HeaderMap;
use axum::routing::post;
use axum::Router;
use pneuma_admission::{disposition, send_url, Disposition, Ingress, IngressError, Invoker};
use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

#[test]
fn a_base_and_a_handler_compose_however_they_are_written() {
    // Both halves are things an operator writes by hand, and both are
    // reasonable with or without slashes. Concatenating the reasonable ones
    // gives `http://restate:8080//PneumaRunner/run`, which Restate answers with
    // a 404 that reads like an unregistered service.
    for (base, handler) in [
        ("http://restate:8080", "PneumaRunner/run"),
        ("http://restate:8080/", "PneumaRunner/run"),
        ("http://restate:8080", "/PneumaRunner/run"),
        ("http://restate:8080/", "/PneumaRunner/run/"),
    ] {
        assert_eq!(
            send_url(base, handler),
            "http://restate:8080/PneumaRunner/run/send",
            "{base} + {handler}"
        );
    }
}

#[test]
fn the_url_is_resolved_once_at_startup() {
    let Ok(ingress) = Ingress::new("http://restate:8080/", "/PneumaRunner/run") else {
        panic!("a client should build");
    };
    assert_eq!(ingress.url(), "http://restate:8080/PneumaRunner/run/send");
}

#[test]
fn an_ingress_without_a_scheme_is_refused_at_startup() {
    // `PNEUMA_RESTATE_INGRESS=restate:8080` is an ordinary compose typo, and
    // `Config` cannot tell it from a hostname. Left to `Client::post`, which
    // parses lazily, it starts, binds, reports ready -- and then fails every
    // submission with a builder error, which is a *transport* failure, which is
    // `Retry`. Rows stay claimed, the sweep hands them back, and the loop turns
    // for ever with nothing reaching Restate.
    // And parsing alone would not have caught it: `restate:8080/...` *is* a
    // URL, whose scheme is `restate` and whose path is `8080/...`. It is
    // reqwest that refuses it -- per request, for ever.
    for (ingress, handler) in [
        ("restate:8080", "PneumaRunner/run"),
        ("", ""),
        ("mailto:ops@example.com", "PneumaRunner/run"),
    ] {
        let Err(IngressError::Url { url, reason }) = Ingress::new(ingress, handler) else {
            panic!("{ingress:?} is not somewhere to submit to");
        };
        assert_eq!(url, send_url(ingress, handler));
        assert!(!reason.is_empty(), "the operator is told what is wrong");
    }
}

/// Serves `router` on an ephemeral port, and says where.
async fn stub(router: Router) -> SocketAddr {
    let Ok(listener) = TcpListener::bind("127.0.0.1:0").await else {
        panic!("a loopback port");
    };
    let Ok(address) = listener.local_addr() else {
        panic!("a bound listener has an address");
    };
    tokio::spawn(async move {
        let _ = axum::serve(listener, router).await;
    });
    address
}

#[tokio::test]
async fn a_submission_carries_the_idempotency_key_and_the_payload() {
    // The key is the whole of the identity Restate compares -- it never looks
    // at the body (the design notes) -- so a client that sent the payload
    // and not the header would deduplicate nothing and pay for every
    // redelivered run twice.
    let router = Router::new().route(
        "/PneumaRunner/run/send",
        post(|headers: HeaderMap, body: String| async move {
            let key = headers
                .get("idempotency-key")
                .and_then(|value| value.to_str().ok())
                .unwrap_or("<absent>")
                .to_owned();
            axum::Json(json!({"status": "Accepted", "key": key, "body": body}))
        }),
    );
    let address = stub(router).await;
    let Ok(ingress) = Ingress::new(&format!("http://{address}"), "PneumaRunner/run") else {
        panic!("a client should build");
    };

    let Ok((status, body)) = ingress.send("run-7", &json!({"job_id": "run-7"})).await else {
        panic!("the stub answered");
    };
    assert_eq!(status, 200);
    assert_eq!(body.get("key").and_then(Value::as_str), Some("run-7"));
    assert_eq!(
        body.get("body").and_then(Value::as_str),
        Some(r#"{"job_id":"run-7"}"#),
        "the payload goes as JSON, verbatim"
    );
}

#[tokio::test]
async fn a_body_that_is_not_json_keeps_its_status() {
    // A proxy's HTML error page in front of Restate. Read through
    // `response.json()` this would be a transport failure, which classifies as
    // `Retry` for ever -- while the status that page carried may well have been
    // a permanent rejection.
    let router = Router::new().route(
        "/PneumaRunner/run/send",
        post(|| async { (axum::http::StatusCode::BAD_GATEWAY, "<html>no</html>") }),
    );
    let address = stub(router).await;
    let Ok(ingress) = Ingress::new(&format!("http://{address}"), "PneumaRunner/run") else {
        panic!("a client should build");
    };

    let Ok((status, body)) = ingress.send("run-7", &json!({})).await else {
        panic!("a 502 with an HTML body is still an answer");
    };
    assert_eq!(status, 502);
    assert_eq!(body, Value::Null, "unparseable, not fatal");
}

#[tokio::test]
async fn nothing_listening_is_an_error_rather_than_a_status() {
    // There is no status to classify, which is a different thing from a status
    // this code does not recognise -- and the difference is why the trait's
    // error type exists at all.
    let Ok(listener) = TcpListener::bind("127.0.0.1:0").await else {
        panic!("a loopback port");
    };
    let Ok(address) = listener.local_addr() else {
        panic!("a bound listener has an address");
    };
    drop(listener);

    let Ok(ingress) = Ingress::new(&format!("http://{address}"), "PneumaRunner/run") else {
        panic!("a client should build");
    };
    let Err(error) = ingress.send("run-7", &json!({})).await else {
        panic!("nothing is listening on {address}");
    };
    assert!(!error.is_empty(), "the message reaches the log");
}

#[tokio::test]
async fn a_response_that_stops_mid_body_is_an_error_too() {
    // The failure a stub router cannot produce and a container will not produce
    // on demand: headers arrive, the status looks fine, and the connection dies
    // before the body does. Classifying that as its status would settle a
    // submission on an answer nobody finished sending.
    let Ok(listener) = TcpListener::bind("127.0.0.1:0").await else {
        panic!("a loopback port");
    };
    let Ok(address) = listener.local_addr() else {
        panic!("a bound listener has an address");
    };
    tokio::spawn(async move {
        let Ok((mut socket, _)) = listener.accept().await else {
            return;
        };
        let mut scratch = [0_u8; 1024];
        let _ = socket.read(&mut scratch).await;
        // Promises a hundred bytes and sends five, then hangs up.
        let _ = socket
            .write_all(b"HTTP/1.1 202 Accepted\r\nContent-Length: 100\r\n\r\nshort")
            .await;
        let _ = socket.shutdown().await;
    });

    let Ok(ingress) = Ingress::new(&format!("http://{address}"), "PneumaRunner/run") else {
        panic!("a client should build");
    };
    let Err(error) = ingress.send("run-7", &json!({})).await else {
        panic!("a truncated body is not an answer");
    };
    assert!(!error.is_empty(), "the message reaches the log");
}

#[tokio::test]
async fn a_redirect_is_reported_rather_than_followed() {
    // reqwest follows up to ten redirects by default, and on a 301, 302 or 303
    // it rewrites the POST to a GET and **drops the body**. An `http` → `https`
    // redirect on an ingress or load balancer would therefore turn a submission
    // into a bodiless GET; Restate would answer 404 or 405; `disposition` reads
    // 4xx as permanent; and `round` would settle the row `failed` -- a run
    // marked as having failed without ever having been offered.
    //
    // Not followed, the redirect comes back as itself and classifies as
    // `Retry`, which is the honest answer: something in front of Restate is
    // misconfigured, and the run has not been decided either way.
    let router = Router::new()
        .route(
            "/PneumaRunner/run/send",
            post(|| async {
                (
                    axum::http::StatusCode::FOUND,
                    [(axum::http::header::LOCATION, "/elsewhere")],
                )
            }),
        )
        .route(
            "/elsewhere",
            axum::routing::any(|| async { axum::http::StatusCode::METHOD_NOT_ALLOWED }),
        );
    let address = stub(router).await;
    let Ok(ingress) = Ingress::new(&format!("http://{address}"), "PneumaRunner/run") else {
        panic!("a client should build");
    };

    let Ok((status, body)) = ingress.send("run-7", &json!({})).await else {
        panic!("a redirect is an answer");
    };
    assert_eq!(status, 302, "reported, not followed");
    assert!(
        matches!(disposition(status, &body), Disposition::Retry(_)),
        "a misconfigured proxy is not a run that failed"
    );
}
