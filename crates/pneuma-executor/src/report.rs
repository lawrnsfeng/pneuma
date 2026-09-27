//! What to publish about a verdict.
//!
//! Pure. The original interleaves this with the sending,
//! so which
//! event a failure produces is decided inside a function that is also making
//! HTTP requests. Here it is a function of the verdict alone.

use pneuma_core::status::NodeStatus;
use pneuma_proto::dispatch::{MessageResult, MessageRun};
use pneuma_proto::event::MessageEvent;
use pneuma_proto::payload::StepPayload;
use serde_json::Value;

use crate::verdict::Verdict;

/// What a component's answer produced.
#[derive(Debug, Clone, PartialEq)]
pub struct Report {
    /// The lifecycle event to publish.
    pub event: MessageEvent,
    /// The result to publish, when there is one.
    ///
    /// `None` for every failure. The original publishes an event and no result
    /// when a call fails (the original returns after `SendEvent`),
    /// and that is right: a result is what the *next* step reads, and there
    /// isn't one.
    pub result: Option<MessageResult>,
}

/// The event published before the component is called.
///
/// Sent unconditionally, as the original sends it
/// — including for a call that is about to fail, so
/// a step that never finishes still shows as having started.
pub fn started(run: &MessageRun) -> MessageEvent {
    MessageEvent {
        event: NodeStatus::Processing,
        meta: run.meta.clone(),
        node: Some(run.node.clone()),
        error_code: None,
        error_message: None,
        headers: run.headers.clone(),
    }
}

/// What to publish for a verdict.
///
/// `step_output` is the component's, already unwrapped. It is passed in rather
/// than dug out of the verdict because only one verdict carries one, and a
/// function that both classified and unwrapped would be two rules in one.
pub fn report(run: &MessageRun, verdict: &Verdict, step_output: Option<&StepPayload>) -> Report {
    let (status, code, message) = match verdict {
        Verdict::Answered(_) => (NodeStatus::Finished, None, None),
        // The model ran and refused the input. An error, not a timeout: the
        // component answered, and what it said was no.
        Verdict::Refused(body) => (
            NodeStatus::Error,
            field(body, "error_code"),
            field(body, "error_message"),
        ),
        Verdict::TimedOut(why) => (NodeStatus::TimedOut, None, Some(why.clone())),
        Verdict::Retry(why) | Verdict::Failed(why) => (NodeStatus::Error, None, Some(why.clone())),
    };
    let event = MessageEvent {
        event: status,
        meta: run.meta.clone(),
        node: Some(run.node.clone()),
        error_code: code.map(Into::into),
        error_message: message.map(Into::into),
        headers: run.headers.clone(),
    };
    Report {
        event,
        result: step_output.map(|output| result_for(run, output)),
    }
}

/// The result message carrying one step's output.
fn result_for(run: &MessageRun, step_output: &StepPayload) -> MessageResult {
    MessageResult {
        meta: run.meta.clone(),
        node: run.node.clone(),
        step_output: step_output.clone(),
        step_input: run.step_input.clone(),
        custom_data: run.custom_data.clone(),
        reply_to_result: run.reply_to_result.clone(),
        reply_to_error: run.reply_to_error.clone(),
        reply_to_event: run.reply_to_event.clone(),
        headers: run.headers.clone(),
        extra: Default::default(),
    }
}

/// A string field of a component's error body, if it has one.
fn field(body: &Value, key: &str) -> Option<String> {
    body.get(key).and_then(Value::as_str).map(ToOwned::to_owned)
}
