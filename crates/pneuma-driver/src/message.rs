//! Building the message a component is sent, and reading the one it answers.
//!
//! Pure. Everything here is a shape question — what the original publishes,
//! what it correlates on — and none of it needs a broker to decide.
//!
//! # Why the registry is consulted here
//!
//! `pneuma_runner::Dispatch` carries a node id, a component name and the
//! narrowed component body: everything a *component* is entitled to see. The
//! original's `MessageRun` carries more — a whole [`NodeRunInfo`], with the
//! path, the node kind, the pipeline and the enclosing aggregator — because
//! that is what the controller's own result listener correlates on.
//! None of it belongs in `Dispatch`,
//! which is deliberately the component's view, so it is rebuilt from the
//! registry the driver already holds.

use pneuma_core::ids::{NodeId, PipelineId, RunId};
use pneuma_core::step::StepRegistry;
use pneuma_proto::dispatch::{MessageResult, MessageRun, NodeEnvVars};
use pneuma_proto::envelope::CustomData;
use pneuma_proto::meta::Meta;
use pneuma_proto::node::NodeRunInfo;
use pneuma_runner::driver::Dispatch;
use serde_json::{json, Value};

use crate::correlate::CallKey;

/// Why a dispatch could not be turned into a message.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum MessageError {
    /// The registry has no such step.
    ///
    /// Only reachable if a dispatch and the registry it came from disagree,
    /// which would mean the driver handed out a task for a node it does not
    /// have. Reported rather than assumed away, because the alternative is
    /// publishing a message with an invented path that nothing will ever
    /// answer.
    #[error("the registry has no step {node_id}")]
    NoSuchStep {
        /// The node that was dispatched.
        node_id: String,
    },

    /// A step input that is not an object or an array.
    ///
    /// The wire type allows only those two, matching the original's
    /// `dict[str, Any] | list[Any]` — the original services take an `interface{}`
    /// and would forward a scalar, which the protocol notes record as a
    /// live divergence. Refused here rather than sent.
    #[error("the step input for {node_id} is not an object or an array")]
    NotAPayload {
        /// The node whose input it is.
        node_id: String,
    },

    /// Caller passthrough that is not an object.
    ///
    /// `custom_data` is a map everywhere it is declared. A caller that sends a
    /// scalar is sending something no consumer can read, and forwarding it
    /// would move the failure to whichever component looks at it first.
    #[error("custom_data for {node_id} is not an object")]
    NotAnObject {
        /// The node whose dispatch carried it.
        node_id: String,
    },
}

/// The coordinate the original gives a node within a run.
///
/// `f"{run_id}.{step.full_path}"`.
/// The fan-out suffix the original appends for a child index is absent here for
/// a reason: this controller's fan-out lives inside
/// `pneuma_interpreter::Execution` as frames, and a frame is not a separate
/// node run — so there is no child index to append and inventing one would put
/// a coordinate on the wire that nothing else in the system agrees with.
///
/// `full_path` is `None` for a step the resolver did not nest, in which case
/// the node id is the path — which is what the resolver would have produced.
pub fn path_of(run_id: &RunId, registry: &StepRegistry, node_id: &NodeId) -> Option<String> {
    let step = registry.get(node_id)?;
    let path = match step.common().full_path.as_ref() {
        Some(path) => path.to_string(),
        None => node_id.as_str().to_owned(),
    };
    Some(format!("{}.{path}", run_id.as_str()))
}

/// The `node` a `MessageRun` carries for one dispatched node.
pub fn node_for(
    registry: &StepRegistry,
    run_id: &RunId,
    pipeline_id: &PipelineId,
    node_id: &NodeId,
) -> Result<NodeRunInfo, MessageError> {
    let missing = || MessageError::NoSuchStep {
        node_id: node_id.as_str().to_owned(),
    };
    let step = registry.get(node_id).ok_or_else(missing)?;
    let path = path_of(run_id, registry, node_id).ok_or_else(missing)?;
    let common = step.common();
    let parent_id = common.parent_id.clone();
    // The parent's path, when there is a parent. Built from the same rule, so
    // a nested node's `parent_path` is exactly the path its parent was
    // dispatched under.
    let parent_path = match parent_id.as_ref() {
        None => None,
        Some(parent) => path_of(run_id, registry, parent).map(Into::into),
    };
    // A string, not a `NodeKind`: the wire field is a bare `str` in original
    // even though its sibling `type` is typed, and tightening it here would
    // reject messages the original accepts.
    let parent_kind = match parent_id.as_ref() {
        None => None,
        Some(parent) => registry.get(parent).map(|step| wire_kind(kind_of(step))),
    };
    Ok(NodeRunInfo {
        path: path.into(),
        node_id: node_id.clone(),
        name: common
            .name
            .clone()
            .unwrap_or_else(|| node_id.as_str().into()),
        node_kind: kind_of(step),
        pipeline_id: pipeline_id.clone(),
        run_id: run_id.clone(),
        parent_id,
        parent_path,
        parent_kind,
        // Fan-out indices are absent for the same reason the path carries no
        // child suffix: this controller's fan-out lives inside
        // `pneuma_interpreter::Execution` as frames, and a frame is not a
        // separate node run. `sibling_index` is left absent rather than
        // defaulted, because `None` means "the wire did not carry it" and is
        // deliberately not the same as position one.
        child_index: None,
        sibling_index: None,
        parent_index: None,
    })
}

/// Which call a result answers.
///
/// Read off the `node` the original executor echoes back
/// (the original, where the output
/// is built from the input), which is the only thing on the result that
/// identifies the call — the subject does not, because every result for every
/// run arrives on the same one.
pub fn key_of(result: &MessageResult) -> CallKey {
    CallKey::new(result.node.run_id.as_str(), result.node.node_id.as_str())
}

/// A step output, in the shape `pneuma_runner` unwraps.
///
/// `Component::call` is documented to return the component's *raw* response,
/// and `extract_step_output` reads `step_output` out of it — the shape an HTTP
/// component answers with. Over NATS the executor hands back the step output on
/// its own, so the transport puts it back into the shape the contract expects.
/// Re-wrapping here rather than special-casing the driver keeps one unwrapping
/// rule for every transport.
pub fn as_response(step_output: &Value) -> Value {
    json!({ "step_output": step_output })
}

/// A kind, as the untyped `parent_kind` field spells it.
///
/// Public and hand-written rather than derived from the serde impl, for two
/// reasons. It pins a *wire* spelling, which is a fact worth a test of its own
/// however few callers it has. And only an aggregator can be a parent, so
/// reaching the `Model` and `Condition` arms through [`node_for`] is
/// impossible — a spelling that is only ever checked through its callers is a
/// spelling that is only half checked.
pub fn wire_kind(kind: pneuma_core::node::NodeKind) -> compact_str::CompactString {
    // Delegated rather than repeated. `pneuma-runner` needs the same mapping
    // for the `parent_kind` column, and both transports go through it -- two
    // copies of a total mapping between two enums is how the two come to
    // disagree about one of the four.
    pneuma_runner::wire_kind(kind).into()
}

/// Which kind a resolved step is.
///
/// Public for the same reason as [`wire_kind`]: it is a total mapping between
/// two enums, and checking it through the one pipeline a fixture happens to
/// contain leaves most of it unchecked.
pub fn kind_of(step: &pneuma_core::step::Step) -> pneuma_core::node::NodeKind {
    // Delegated, for the reason [`wire_kind`] gives.
    pneuma_runner::kind_of(step)
}

/// Everything about a run that a [`Dispatch`] does not carry.
///
/// Held by the transport for the life of one run, because every field is
/// constant across its calls: the controller's envelope, which run it is, and
/// which pipeline it came from.
#[derive(Debug, Clone)]
pub struct RunContext {
    /// The controller's envelope, forwarded whole.
    ///
    /// Not the narrowed [`pneuma_proto::component::ComponentMeta`] that reaches a
    /// component: `MessageRun` carries the full `Meta`, caller extras and all,
    /// because the original executor echoes it back and the controller's own
    /// listener reads it.
    pub meta: Meta,
    /// Which run this is.
    pub run_id: RunId,
    /// Which pipeline it came from.
    pub pipeline_id: PipelineId,
}

/// The message the original publishes for one dispatched node.
///
/// Pure, so what goes on the wire is a test rather than something read back off
/// a broker. Every field comes from either the dispatch or the context —
/// nothing is invented here, which is what makes the three failures below
/// failures rather than defaults.
pub fn message_run(
    context: &RunContext,
    registry: &StepRegistry,
    dispatch: &Dispatch,
) -> Result<MessageRun, MessageError> {
    let node_id = dispatch.node_id.clone();
    let node = node_for(registry, &context.run_id, &context.pipeline_id, &node_id)?;
    let body = &dispatch.request;

    let step_input = match serde_json::from_value(body.step_input.clone()) {
        Ok(payload) => payload,
        Err(_) => {
            return Err(MessageError::NotAPayload {
                node_id: node_id.as_str().to_owned(),
            })
        }
    };
    let custom_data = match body.custom_data.as_ref() {
        None => CustomData::new(),
        Some(Value::Object(map)) => map.clone(),
        Some(_) => {
            return Err(MessageError::NotAnObject {
                node_id: node_id.as_str().to_owned(),
            })
        }
    };
    let node_env_vars: NodeEnvVars = body
        .node_env_vars
        .iter()
        .flatten()
        .map(|(name, value)| (name.as_str().into(), value.as_str().into()))
        .collect();

    Ok(MessageRun {
        meta: context.meta.clone(),
        node,
        step_input,
        custom_data,
        node_env_vars,
        // Always absent on this leg, as the type's own docs record: the reply
        // addresses belong to the run, not to a component call.
        reply_to_result: None,
        reply_to_error: None,
        reply_to_event: None,
        headers: body.headers.clone(),
        extra: Default::default(),
    })
}
