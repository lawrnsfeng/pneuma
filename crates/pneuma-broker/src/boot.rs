//! Bringing the service up.
//!
//! One task per input subject, all sharing a queue group so replicas split the
//! work rather than duplicating it — which is the difference from
//! `pneuma-driver`, where every replica must see every message because the
//! state is in memory. Here there is no state, so a message belongs to whoever
//! picks it up.

use std::net::SocketAddr;

use futures_lite::StreamExt;
use pneuma_nats::Subject;
use secrecy::ExposeSecret;
use tokio_util::sync::CancellationToken;

use crate::config::Config;
use crate::route::{route, RouteError};

/// The name every route is mounted under.
pub const SERVICE: &str = "pneuma-broker";

/// How long getting one message onto the wire may take.
///
/// The same bound `pneuma-driver` puts on its publish, and for the same
/// measured reason: `async_nats::Client::publish` writes into a local buffer
/// and returns `Ok` even when the broker has gone, so `flush` is what makes
/// "routed" mean anything — and an *unbounded* flush against a broker that is
/// not answering hangs the whole demux loop on one message.
///
/// The duplication with `pneuma-driver` is deliberate rather than
/// overlooked. The obvious home is `pneuma-transport`, which is where broker
/// clients live — but that crate holds `lapin`, and moving this there would
/// make every NATS-only service link an AMQP client. Two call sites is the
/// cheaper of the two costs; a third is the point to split that crate by
/// transport.
pub const PUBLISH_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

/// Why the service could not start, or could not keep running.
#[derive(Debug, thiserror::Error)]
pub enum BootError {
    /// NATS could not be reached.
    #[error("could not reach nats: {0}")]
    Nats(String),

    /// The health routes could not be built.
    #[error("{0}")]
    Router(#[from] pneuma_serve::RouterError),

    /// The listener could not be bound, or serving failed.
    #[error("could not serve on {address}: {reason}")]
    Serve {
        /// What was asked for.
        address: SocketAddr,
        /// What the operating system said.
        reason: String,
    },
}

/// Runs the service until `token` is cancelled.
pub async fn run(
    config: &Config,
    token: CancellationToken,
    bound: impl FnOnce(SocketAddr),
) -> Result<(), BootError> {
    let client = async_nats::connect(config.nats_uri.expose_secret())
        .await
        .map_err(|error| BootError::Nats(error.to_string()))?;

    // No dependency probe: this service has no database, and NATS is not
    // probed for the reason the bootstrap's broker is not -- losing it is
    // something the process is already reconnecting from, and a pod restarted
    // for it would come back to the same NATS.
    let health = pneuma_serve::router(SERVICE, Vec::new())?;
    let address = config.listen;
    let stopping = token.clone();
    let shutdown = token.clone();
    let serving = async move {
        let served = pneuma_serve::serve(health, address, bound, async move {
            shutdown.cancelled().await;
        })
        .await;
        // Whatever ended the server ends every demux with it, including a bind
        // that never succeeded.
        stopping.cancel();
        served
    };

    // Subscribed *before* anything reports ready, and that is the point: a
    // broker that could not attach would otherwise pass its health check and
    // route nothing, which is the quietest way for a tenant's messages to stop
    // arriving. A failure here is a pod that never becomes ready.
    //
    // It also removes an arm nothing could reach. Attempted the other way
    // round, `queue_subscribe` failing is not something a test can arrange:
    // `Subject::parse` refuses every subject NATS would, `Config::from_env`
    // refuses every queue group NATS would, and a *drained* async-nats client
    // subscribes successfully anyway -- measured.
    let mut attached = Vec::new();
    for input in &config.inputs {
        let queue = config.queue.as_str().to_owned();
        let messages = client
            .queue_subscribe(input.as_str().to_owned(), queue)
            .await
            .map_err(|error| BootError::Nats(error.to_string()))?;
        attached.push((input.clone(), messages));
    }

    // One task per input subject. Spawned rather than joined in place, because
    // nothing here is `!Send` -- the reason `pneuma-driver` cannot spawn
    // its driver does not apply to a service that holds no execution state.
    let mut demuxing = Vec::new();
    for (input, messages) in attached {
        demuxing.push(tokio::spawn(demux(
            client.clone(),
            input,
            messages,
            token.clone(),
        )));
    }

    let served = serving.await;
    // The server returning cancelled the token, so every demux is on its way
    // out; awaiting them is what makes shutdown orderly rather than abrupt.
    for task in demuxing {
        // Awaited for ordering, not for a result. A `JoinError` here means the
        // task panicked -- which has already unwound and printed -- or that it
        // was cancelled, which is what we asked for. There is nothing left to
        // report, and an arm for it would be one nothing can reach.
        let _ = task.await;
    }
    match served {
        Ok(()) => Ok(()),
        Err(error) => Err(BootError::Serve {
            address,
            reason: error.to_string(),
        }),
    }
}

/// Reads one input subject and republishes each message to its tenant's.
async fn demux(
    client: async_nats::Client,
    input: Subject,
    mut messages: async_nats::Subscriber,
    token: CancellationToken,
) {
    while let Some(message) = token.run_until_cancelled(messages.next()).await.flatten() {
        let outcome = forward(&client, &input, &message.payload).await;
        eprintln!("{SERVICE}: {outcome}");
    }
}

/// Routes and republishes one message, and says what happened.
///
/// One line out, whichever way it went, so the loop above has no branch in it:
/// the arms only a malformed message can reach are then a test rather than a
/// region of a loop nothing here can drive.
pub async fn forward(client: &async_nats::Client, input: &Subject, body: &[u8]) -> String {
    let routed = match route(input, body) {
        Ok(routed) => routed,
        // Every routing failure is permanent -- a body that will not parse will
        // not parse next time, and a tenant id that is not a subject token will
        // not become one -- so the message is dropped with a reason rather than
        // retried. The original saves it to a dead-letter subject; that belongs
        // with the JetStream durable this service deliberately does not use,
        // and the design notes record the pair together.
        Err(error) => return describe(&error),
    };
    let subject = routed.subject.as_str().to_owned();
    let sent = async {
        client
            .publish(subject.clone(), body.to_vec().into())
            .await
            .map_err(|error| error.to_string())?;
        client.flush().await.map_err(|error| error.to_string())
    };
    // The two failures are collapsed to one `Option` before they are matched
    // on: the client refusing and the broker never answering are the same fact
    // to a caller -- this message was not routed -- and only the reason
    // differs.
    let refused = match tokio::time::timeout(PUBLISH_TIMEOUT, sent).await {
        Ok(result) => result.err(),
        Err(_) => Some(format!(
            "the broker did not admit it within {PUBLISH_TIMEOUT:?}"
        )),
    };
    match refused {
        None => format!("routed to {subject}"),
        Some(reason) => format!("could not publish to {subject}: {reason}"),
    }
}

/// One line about a routing failure.
///
/// Pure and separate, so each reason is a test rather than a log line nobody
/// reads until it matters.
pub fn describe(error: &RouteError) -> String {
    format!("dropped: {error}")
}
