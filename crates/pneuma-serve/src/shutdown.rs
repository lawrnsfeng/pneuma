//! Stopping when asked, rather than when killed.
//!
//! Kubernetes sends **SIGTERM** and waits `terminationGracePeriodSeconds`
//! before SIGKILL. A process that ignores SIGTERM therefore has its in-flight
//! work destroyed on a timer, every deploy — and for a janitor mid-pass that
//! means an archive written and the matching delete not.
//!
//! The SDK's own `listen_and_serve` handles SIGINT and not SIGTERM
//! (`restate-sdk-0.11.1/src/http_server.rs`), which is the right choice for a
//! terminal and the wrong one for a cluster. Both are handled here: SIGINT
//! because that is what Ctrl-C sends to someone running the binary by hand.

use std::net::SocketAddr;

use tokio::net::TcpListener;

/// Which signal asked the process to stop.
///
/// Returned rather than discarded so a caller can log it. The two mean
/// different things operationally — SIGTERM is a deploy or an eviction, SIGINT
/// is a person at a terminal — and a log line that says which turns "the
/// process exited" into "the process was replaced".
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Signal {
    /// SIGTERM: Kubernetes, `docker stop`, an orderly shutdown.
    Terminate,
    /// SIGINT: Ctrl-C.
    Interrupt,
}

/// Completes when the process is asked to stop.
///
/// Both handlers are installed *before* either is awaited, so a signal that
/// arrives between the two is still caught. Installing them one at a time
/// inside a `select!` leaves a window in which SIGTERM has its default
/// disposition and kills the process outright.
pub async fn termination() -> std::io::Result<Signal> {
    use tokio::signal::unix::{signal, SignalKind};

    let mut terminate = signal(SignalKind::terminate())?;
    let mut interrupt = signal(SignalKind::interrupt())?;
    Ok(tokio::select! {
        _ = terminate.recv() => Signal::Terminate,
        _ = interrupt.recv() => Signal::Interrupt,
    })
}

/// Serves `router` on `address` until `shutdown` completes.
///
/// Returns the address actually bound, through `bound`, before serving —
/// otherwise a caller asking for port 0 has no way to learn what it got, and
/// every test of this function would need a fixed port and could not run
/// alongside another.
///
/// `shutdown` is a future rather than a signal handler so that this composes:
/// a caller can shut down on a signal, on a token, on a deadline, or on any of
/// them at once, and the test for *this* function does not need to send a
/// signal to itself.
pub async fn serve<S>(
    router: axum::Router,
    address: SocketAddr,
    bound: impl FnOnce(SocketAddr),
    shutdown: S,
) -> std::io::Result<()>
where
    S: std::future::Future<Output = ()> + Send + 'static,
{
    let listener = TcpListener::bind(address).await?;
    bound(listener.local_addr()?);
    axum::serve(listener, router)
        .with_graceful_shutdown(shutdown)
        .await
}
