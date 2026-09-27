//! The two properties that need a real process.
//!
//! Everything about *routing* is tested through `ServiceExt::oneshot` in
//! `health.rs`, without a port. What is left is what a `Router` cannot show:
//! that the server actually binds and answers, that completing the shutdown
//! future ends it, and that a real SIGTERM is caught rather than killing the
//! process.
//!
//! Port 0 throughout, so these run alongside anything else. `serve` hands back
//! the address it got, which is the only reason port 0 is usable at all.

use std::sync::Arc;
use std::time::Duration;

use pneuma_serve::health::router;
use pneuma_serve::shutdown::{serve, termination, Signal};
use pneuma_telemetry::HealthCheckable;
use tokio::sync::oneshot;

fn app() -> axum::Router {
    let checks: Vec<Arc<dyn HealthCheckable>> = Vec::new();
    match router("pneuma-serve-test", checks) {
        Ok(app) => app,
        Err(error) => panic!("a router with no checks builds: {error}"),
    }
}

#[tokio::test]
async fn it_binds_answers_and_stops_when_told() {
    let (address_tx, address_rx) = oneshot::channel();
    let (stop_tx, stop_rx) = oneshot::channel::<()>();

    let serving = tokio::spawn(async move {
        let bound = |address| {
            // The receiver is alive until the request below is answered, so a
            // failure here means the test itself is broken.
            let _ = address_tx.send(address);
        };
        serve(app(), ([127, 0, 0, 1], 0).into(), bound, async move {
            let _ = stop_rx.await;
        })
        .await
    });

    let Ok(address) = address_rx.await else {
        panic!("the server reports the address it bound");
    };
    assert_ne!(address.port(), 0, "port 0 means 'pick one', not 'use 0'");

    let Ok(response) = reqwest::get(format!("http://{address}/pneuma-serve-test/healthz")).await
    else {
        panic!("the server answers on {address}");
    };
    assert!(response.status().is_success());

    // Completing the future is what ends it. Not a timeout, not a kill.
    let _ = stop_tx.send(());
    let Ok(Ok(Ok(()))) = tokio::time::timeout(Duration::from_secs(10), serving).await else {
        panic!("a completed shutdown future ends the server");
    };

    // And the port is released, which is what "graceful" has to mean for a
    // rolling deploy: the replacement pod binds the same address.
    let Ok(rebound) = tokio::net::TcpListener::bind(address).await else {
        panic!("the address is free again once the server has stopped");
    };
    drop(rebound);
}

#[tokio::test]
async fn an_address_already_taken_is_an_error_rather_than_a_panic() {
    let Ok(held) = tokio::net::TcpListener::bind(("127.0.0.1", 0)).await else {
        panic!("a port is available");
    };
    let Ok(taken) = held.local_addr() else {
        panic!("a bound listener has an address");
    };
    let (never_tx, never_rx) = oneshot::channel::<()>();
    let outcome = serve(
        app(),
        taken,
        |_| panic!("nothing should be bound"),
        async move {
            let _ = never_rx.await;
        },
    )
    .await;
    drop(never_tx);
    let Err(error) = outcome else {
        panic!("binding a taken port must fail");
    };
    assert_eq!(error.kind(), std::io::ErrorKind::AddrInUse);
}

#[tokio::test]
async fn a_real_sigterm_is_caught_rather_than_killing_the_process() {
    // Kubernetes sends SIGTERM and waits `terminationGracePeriodSeconds` before
    // SIGKILL, so a process that does not catch it has its in-flight work
    // destroyed on a timer every deploy. The SDK's own `listen_and_serve`
    // handles SIGINT and not SIGTERM, which is right for a terminal and wrong
    // for a cluster -- so this sends the real signal to this process rather
    // than trusting that the handler is installed.
    //
    // If `termination` failed to install its handler, SIGTERM's default
    // disposition would kill this test binary outright: the failure is loud and
    // confined to this target rather than silent.
    let waiting = tokio::spawn(termination());

    // The handler is installed inside the task, so give it a moment to run
    // before raising anything. `yield_now` alone is not enough on a
    // multi-threaded runtime.
    tokio::time::sleep(Duration::from_millis(250)).await;

    let raised = std::process::Command::new("kill")
        .args(["-TERM", &std::process::id().to_string()])
        .status();
    let Ok(status) = raised else {
        panic!("could not send SIGTERM to this process");
    };
    assert!(status.success(), "kill reported {status}");

    let Ok(Ok(Ok(signal))) = tokio::time::timeout(Duration::from_secs(10), waiting).await else {
        panic!("SIGTERM should have been caught");
    };
    assert_eq!(
        signal,
        Signal::Terminate,
        "and reported as itself, so a log line can say a deploy replaced this \
         process rather than that someone pressed Ctrl-C"
    );
}
