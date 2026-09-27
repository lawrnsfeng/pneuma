//! Bringing the service up.
//!
//! One subscription, a bounded number of messages in flight, and a health
//! endpoint. Everything a message means is decided in [`crate::verdict`] and
//! [`mod@crate::report`], both pure, so what is left here is publishing.

use std::net::SocketAddr;
use std::sync::Arc;

use futures_lite::StreamExt;
use pneuma_nats::Subject;
use pneuma_proto::dispatch::MessageRun;
use secrecy::ExposeSecret;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

use crate::component::Component;
use crate::config::Config;
use crate::report::{report, started};
use pneuma_proto::component::{ComponentMeta, ComponentRequest};
use pneuma_proto::payload::StepPayload;

use crate::verdict::Verdict;

/// The name every route is mounted under.
pub const SERVICE: &str = "pneuma-executor";

/// How long getting one message onto the wire may take.
///
/// The same bound the controller and the broker put on theirs, for the
/// same measured reason: `publish` writes into a local buffer and returns `Ok`
/// even when the broker has gone, and an unbounded `flush` against a broker
/// that is not answering hangs the handler on one message.
pub const PUBLISH_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

/// Why the service could not start, or could not keep running.
#[derive(Debug, thiserror::Error)]
pub enum BootError {
    /// NATS could not be reached, or the subscription could not be made.
    #[error("could not reach nats: {0}")]
    Nats(String),

    /// The component client could not be built.
    #[error("{0}")]
    Component(#[from] crate::component::ComponentError),

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
    let component = Arc::new(
        Component::new(&config.component, config.request_timeout)?.with_attempts(config.attempts),
    );

    // Subscribed before anything reports ready, for the reason the tenant
    // broker subscribes first: a provider that could not attach would pass its
    // health check and call no components at all.
    let messages = client
        .queue_subscribe(
            config.work.as_str().to_owned(),
            config.queue.as_str().to_owned(),
        )
        .await
        .map_err(|error| BootError::Nats(error.to_string()))?;

    let health = pneuma_serve::router(SERVICE, Vec::new())?;
    let address = config.listen;
    let stopping = token.clone();
    let shutdown = token.clone();
    let serving = async move {
        let served = pneuma_serve::serve(health, address, bound, async move {
            shutdown.cancelled().await;
        })
        .await;
        stopping.cancel();
        served
    };

    let consuming = tokio::spawn(consume(
        client,
        messages,
        component,
        config.results.clone(),
        config.events.clone(),
        config.senders,
        token,
    ));

    let served = serving.await;
    // Awaited for ordering, not for a result: a `JoinError` means the task
    // panicked -- which has already unwound and printed -- or was cancelled,
    // which is what we asked for.
    let _ = consuming.await;
    match served {
        Ok(()) => Ok(()),
        Err(error) => Err(BootError::Serve {
            address,
            reason: error.to_string(),
        }),
    }
}

/// Reads work and handles it, a bounded number at a time.
async fn consume(
    client: async_nats::Client,
    mut messages: async_nats::Subscriber,
    component: Arc<Component>,
    results: Subject,
    events: Subject,
    senders: usize,
    token: CancellationToken,
) {
    let permits = Arc::new(Semaphore::new(senders));
    let mut running = Vec::new();
    while let Some(message) = token.run_until_cancelled(messages.next()).await.flatten() {
        let Ok(permit) = Arc::clone(&permits).acquire_owned().await else {
            // Only closed when the process is going away.
            break;
        };
        let one = One {
            client: client.clone(),
            component: Arc::clone(&component),
            results: results.clone(),
            events: events.clone(),
        };
        running.push(tokio::spawn(async move {
            let said = one.handle(&message.payload).await;
            drop(permit);
            eprintln!("{SERVICE}: {said}");
        }));
    }
    for task in running {
        let _ = task.await;
    }
}

/// Everything one message needs, so the spawned task owns it.
struct One {
    client: async_nats::Client,
    component: Arc<Component>,
    results: Subject,
    events: Subject,
}

impl One {
    /// Calls the component for one message and publishes what it said.
    async fn handle(&self, body: &[u8]) -> String {
        handle(
            &self.client,
            &self.component,
            &self.results,
            &self.events,
            body,
        )
        .await
    }
}

/// Calls the component for one message and publishes what it said.
///
/// Public and free-standing so the whole path is testable against a stub
/// component and a real broker, without starting the service.
pub async fn handle(
    client: &async_nats::Client,
    component: &Component,
    results: &Subject,
    events: &Subject,
    body: &[u8],
) -> String {
    // Both halves are a line to log, so the `Result` is a control-flow device
    // rather than a distinction the caller acts on -- which is what lets every
    // failure below be a `?` on a line that runs every time, instead of an arm
    // reachable only by a broker that went away between two publishes.
    match one(client, component, results, events, body).await {
        Ok(said) => said,
        Err(said) => said,
    }
}

/// Handles one message, with every failure as a line to log.
async fn one(
    client: &async_nats::Client,
    component: &Component,
    results: &Subject,
    events: &Subject,
    body: &[u8],
) -> Result<String, String> {
    // Dropped rather than retried: a body that will not parse will not parse
    // next time. The original dead-letters it; that
    // belongs with the JetStream stream this service deliberately does not use,
    // and the design notes record the pair together.
    let run: MessageRun = serde_json::from_slice(body)
        .map_err(|error| format!("dropped: not a run message: {error}"))?;
    let node = run.node.node_id.as_str().to_owned();

    // Published before the call and unconditionally, as the original publishes
    // it -- including for a call that is about to fail, so a step that never
    // finishes still shows as having started. A broker that will not take it
    // stops the message here: calling the component anyway would spend a model
    // call nobody can be told the result of.
    deliver(client, events, &started(&run), "the start of", &node).await?;

    let request = request_for(&run);
    let verdict = component.call(&request).await;
    let output = step_output(&verdict);
    let outcome = report(&run, &verdict, output.as_ref());

    deliver(client, events, &outcome.event, "the event for", &node).await?;
    match outcome.result {
        // A result is what the *next* step reads, and there isn't one. The run
        // is told the step ended rather than left waiting for one that is never
        // coming.
        None => Ok(format!("{node} ended without a result: {verdict:?}")),
        Some(result) => {
            deliver(client, results, &result, "the result for", &node).await?;
            Ok(format!("{node} answered"))
        }
    }
}

/// Publishes one message, or the line to log if it could not be published.
async fn deliver<T: serde::Serialize>(
    client: &async_nats::Client,
    subject: &Subject,
    message: &T,
    what: &str,
    node: &str,
) -> Result<(), String> {
    publish(client, subject, message)
        .await
        .map_err(|reason| format!("could not publish {what} {node}: {reason}"))
}

/// What a component is sent, built from the message that asked for it.
///
/// Pure, so the one wire surface a rebuilt model reads is a test rather than
/// something only a running broker can show.
///
/// # This used to be the step input alone
///
/// It posted `run.step_input` as the entire body — which was wrong against the
/// original, whose `MessageSeldonInputV2` carries all five fields,
/// and is wrong against
/// `docs/as-built/component-api.md`, which is now the published contract. A
/// model rebuilt to that contract reads `body.step_input` and finds nothing.
/// The `pneuma-restate` path never had the defect, because it goes through
/// `pneuma_runner::Dispatch`, and that divergence is exactly what a shared type
/// exists to prevent — so this builds the same [`ComponentRequest`] that path
/// does.
///
/// Empty is absent for the two optional maps, matching the original's `omitempty`: a
/// component may distinguish "no custom data" from `{}`, and production never
/// sends the latter.
pub fn request_for(run: &MessageRun) -> ComponentRequest {
    ComponentRequest {
        meta: ComponentMeta::from(&run.meta),
        // `to_value` rather than `serde_json::to_value`, which would be a
        // fallible conversion with no failing input: a `StepPayload` is an
        // object or an array by construction.
        step_input: run.step_input.to_value(),
        custom_data: (!run.custom_data.is_empty())
            .then(|| serde_json::Value::Object(run.custom_data.clone())),
        node_env_vars: (!run.node_env_vars.is_empty()).then(|| {
            run.node_env_vars
                .iter()
                .map(|(key, value)| (key.to_string(), value.to_string()))
                .collect()
        }),
        headers: run.headers.clone(),
    }
}

/// The step output a verdict carries, if it carries one.
///
/// Only a successful answer does, and only if it is the shape a component
/// answers with. A 200 whose body has no `step_output` is a component
/// that answered without answering -- the original dead-letters it,
/// and here it becomes a result-less report, so
/// the run is told the step ended rather than left waiting.
///
/// The payload is typed on the way through: the wire allows an object or an
/// array, and a component returning a scalar is the protocol notes's live
/// divergence -- the original services forward it and original refuses it later.
pub fn step_output(verdict: &Verdict) -> Option<StepPayload> {
    let Verdict::Answered(body) = verdict else {
        return None;
    };
    let output = pneuma_proto::component::extract_step_output(body).ok()?;
    serde_json::from_value(output.clone()).ok()
}

/// Publishes one message, bounded.
async fn publish<T: serde::Serialize>(
    client: &async_nats::Client,
    subject: &Subject,
    message: &T,
) -> Result<(), String> {
    let body = serde_json::to_vec(message).map_err(|error| error.to_string())?;
    let sent = async {
        client
            .publish(subject.as_str().to_owned(), body.into())
            .await
            .map_err(|error| error.to_string())?;
        client.flush().await.map_err(|error| error.to_string())
    };
    // The message is bound before the match: an argument on its own line of a
    // multi-line call is not attributed to the call that ran, and the gate then
    // reports a line every timeout executes as unreached.
    let too_slow = format!("the broker did not admit it in {PUBLISH_TIMEOUT:?}");
    match tokio::time::timeout(PUBLISH_TIMEOUT, sent).await {
        Ok(result) => result,
        Err(_) => Err(too_slow),
    }
}
