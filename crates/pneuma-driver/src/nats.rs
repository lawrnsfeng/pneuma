//! The transport: publish a run message, wait for its result.
//!
//! Everything decidable is decided in [`crate::message`] and
//! [`crate::correlate`], both pure. What is left here is a publish, an await
//! and a deadline.
//!
//! # The deadline is not optional
//!
//! Nothing on the other side promises to answer. A component that dies between
//! taking the message and publishing its result leaves a `oneshot::Receiver`
//! that never resolves — and because `pneuma_runner::drive` awaits each call
//! before asking for the next task, that is not one lost step but a run that
//! stops for ever, holding its `Execution` and its entry in the correlation
//! table. The original has the same exposure and no timeout at all: the
//! controller publishes and returns, so a run whose component never answers
//! simply stays `processing` until the janitor's stale sweep finds it
//! (the defect notes). A bounded wait here turns that into a failed
//! run with a reason.

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use pneuma_core::step::StepRegistry;
use pneuma_proto::dispatch::{MessageResult, MessageRun};
use pneuma_runner::driver::{Component, Dispatch};
use serde_json::Value;

use crate::correlate::{CallKey, Delivered, Occupied, Pending};
use crate::message::{as_response, key_of, message_run, MessageError, RunContext};

/// How long one component call may take before the run gives up on it.
///
/// Five minutes, which is the original's own model timeout
/// (the survey notes) and the same figure `pneuma-restate` defaults
/// to. Matching it means a deployment that tunes one is not surprised by the
/// other.
pub const DEFAULT_TIMEOUT_SECS: u64 = 300;

/// The same figure, as configuration reads it.
///
/// Two constants rather than a cast at the call site: the environment is
/// parsed as `i64` because a *negative* value has to be refusable, and a `u64`
/// default would have to be cast into that comparison -- which is the one
/// place a silent wrap could turn "refuse this" into "accept it".
pub const DEFAULT_TIMEOUT_SECS_I64: i64 = 300;

/// How long getting one message onto the wire may take.
///
/// Separate from the call deadline, and much shorter, because the two fail
/// differently: a component thinking for five minutes is normal, and a broker
/// taking ten seconds to admit a publish is a broker that is not there. The
/// deadline actually used is the smaller of this and the call's own — a
/// publish can never usefully take longer than the whole call it belongs to.
pub const PUBLISH_TIMEOUT_SECS: u64 = 10;

/// Why a component call failed.
#[derive(Debug, thiserror::Error)]
pub enum CallError {
    /// The message could not be built.
    #[error("{0}")]
    Message(#[from] MessageError),

    /// A call for that run and node is already outstanding.
    #[error("{0}")]
    Occupied(#[from] Occupied),

    /// The broker refused the publish.
    #[error("could not publish to {subject}: {reason}")]
    Publish {
        /// Where it was going.
        subject: String,
        /// What the client said.
        reason: String,
    },

    /// Nobody answered in time.
    #[error("{node_id} did not answer within {}s", .after.as_secs())]
    Timeout {
        /// The node that was called.
        node_id: String,
        /// How long it was given.
        after: Duration,
    },

    /// The wait was abandoned.
    ///
    /// The sender was dropped without a result, which means the correlation
    /// table gave up on this call — a cancelled run, or a shutdown. Distinct
    /// from a timeout because nothing was waited *for*.
    #[error("the wait for {node_id} was abandoned")]
    Abandoned {
        /// The node that was called.
        node_id: String,
    },
}

/// A component reached by publishing to its own subject.
pub struct NatsComponent {
    client: async_nats::Client,
    pending: Arc<Pending>,
    registry: Arc<StepRegistry>,
    context: RunContext,
    timeout: Duration,
}

impl NatsComponent {
    /// A transport for one run.
    pub fn new(
        client: async_nats::Client,
        pending: Arc<Pending>,
        registry: Arc<StepRegistry>,
        context: RunContext,
    ) -> Self {
        NatsComponent {
            client,
            pending,
            registry,
            context,
            timeout: Duration::from_secs(DEFAULT_TIMEOUT_SECS),
        }
    }

    /// The same, with a different deadline.
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// How long a call is given.
    pub fn timeout(&self) -> Duration {
        self.timeout
    }

    /// The registry this transport builds its messages from.
    ///
    /// Exposed so the caller can hand the same one to `drive` — the two must
    /// agree, and a caller holding its own copy is how they stop agreeing.
    pub fn registry(&self) -> &StepRegistry {
        &self.registry
    }

    /// Puts one message on the wire and waits for the server to admit it.
    ///
    /// `publish` alone is not enough, and the reason is the same one the AMQP
    /// side reaches through publisher confirms: it writes into a local buffer
    /// and returns, so a broker that has gone away yields `Ok` and the run then
    /// waits the whole timeout for an answer nobody was ever asked for.
    /// Measured — a drained client publishes "successfully".
    ///
    /// Core NATS has no per-message acknowledgement, so `flush` is the
    /// strongest guarantee available: it sends a `PING` and waits for the
    /// `PONG`, which the server sends only after everything written before it.
    /// One round trip per call, which `drive` can afford because it awaits each
    /// call anyway.
    /// Encoding happens here rather than in the caller because it cannot fail:
    /// `MessageRun`'s hand-written `Serialize` skips a reserved key rather than
    /// refusing it, and every type beneath it is a map or a `Value`. A separate
    /// error variant for it would be one no input can produce, and the `?`
    /// below is honest about that -- it runs on every call and reports whatever
    /// a future field might one day make possible.
    async fn send(&self, subject: &str, run: &MessageRun) -> Result<(), String> {
        let body = serde_json::to_vec(run).map_err(|error| error.to_string())?;
        let deadline = self.timeout.min(Duration::from_secs(PUBLISH_TIMEOUT_SECS));
        let sent = async {
            self.client
                .publish(subject.to_owned(), body.into())
                .await
                .map_err(|error| error.to_string())?;
            self.client.flush().await.map_err(|error| error.to_string())
        };
        // Bounded, because an unbounded flush against a broker that is not
        // answering hangs the run inside the publish -- which looks exactly
        // like a slow component and is not.
        match tokio::time::timeout(deadline, sent).await {
            Ok(result) => result,
            Err(_) => Err(format!("the broker did not admit it within {deadline:?}")),
        }
    }

    /// Publishes and waits. The body of [`Component::call`].
    async fn dispatch(&self, dispatch: Dispatch) -> Result<Value, CallError> {
        let node_id = dispatch.node_id.as_str().to_owned();
        let run = message_run(&self.context, &self.registry, &dispatch)?;

        // Registered *before* the publish. The result can arrive while the
        // publish call is still returning, and a table that was not yet
        // expecting it would drop it as another replica's -- then wait the
        // whole timeout for an answer that had already come and gone.
        let key = CallKey::new(self.context.run_id.as_str(), &node_id);
        let waiting = self.pending.register(key.clone()).await?;

        let subject = dispatch.component.clone();
        if let Err(reason) = self.send(&subject, &run).await {
            // The registration is undone rather than left behind: a publish
            // that never happened has no answer coming, the entry would sit in
            // the table until the process ended, and the next attempt for that
            // node would look like a duplicate.
            self.pending.forget(&key).await;
            return Err(CallError::Publish { subject, reason });
        }

        match tokio::time::timeout(self.timeout, waiting).await {
            Ok(Ok(output)) => Ok(as_response(&output)),
            Ok(Err(_)) => Err(CallError::Abandoned { node_id }),
            Err(_) => {
                self.pending.forget(&key).await;
                Err(CallError::Timeout {
                    node_id,
                    after: self.timeout,
                })
            }
        }
    }
}

impl Component for NatsComponent {
    type Error = CallError;

    fn call(&self, dispatch: Dispatch) -> impl Future<Output = Result<Value, Self::Error>> {
        self.dispatch(dispatch)
    }
}

/// What reading one result off the wire produced.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Received {
    /// It was handed to the call waiting for it.
    Delivered(Delivered),
    /// The bytes are not a result message.
    ///
    /// Not fatal, and not this replica's problem to fix: the shared subject
    /// carries everything, and a message that will not parse is one nobody
    /// here can act on. Counted and logged rather than allowed to end the
    /// subscription, because a subscription that dies on one bad message takes
    /// every run on the replica with it.
    Unreadable(String),
}

/// One line about a received result, whichever way it went.
///
/// Pure, so the subscription loop that calls it has no branch in it — the arms
/// only a malformed message or another replica's run can reach are then a test
/// rather than a region of a loop nothing here can drive.
pub fn describe_received(outcome: &Received) -> String {
    match outcome {
        Received::Unreadable(why) => format!("a result that will not parse: {why}"),
        Received::Delivered(Delivered::Taken) => "a result reached its call".to_owned(),
        // The ordinary case on a shared subject, and deliberately not a
        // warning: most results belong to runs another replica is driving.
        Received::Delivered(Delivered::Unclaimed) => "a result for another replica".to_owned(),
        Received::Delivered(Delivered::Abandoned) => {
            "a result for a call that had given up".to_owned()
        }
    }
}

/// Hands one raw result to whoever is waiting for it.
///
/// Separate from the subscription loop so the parse-and-deliver rule is a test
/// that needs no broker; the loop around it is three lines.
pub async fn receive(pending: &Pending, body: &[u8]) -> Received {
    let result: MessageResult = match serde_json::from_slice(body) {
        Ok(result) => result,
        Err(error) => return Received::Unreadable(error.to_string()),
    };
    let key = key_of(&result);
    // `to_value` rather than `serde_json::to_value`, which would be a fallible
    // conversion with no failing input: a `StepPayload` is an object or an
    // array by construction.
    let output = result.step_output.to_value();
    Received::Delivered(pending.deliver(&key, output).await)
}
