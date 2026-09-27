//! The loop: ask for a task, call the component, report the output.

use std::collections::BTreeMap;
use std::future::Future;

use pneuma_core::ids::NodeId;
use pneuma_core::step::{Step, StepRegistry};
use pneuma_interpreter::{Execution, FrameId, ScheduleError, Task};
use pneuma_proto::component::{
    extract_step_output, ComponentMeta, ComponentRequest, ComponentResponseError,
};
use pneuma_proto::meta::Meta;
use serde_json::Value;

use crate::record::{self, Failure, NoRecord, Record, Recorder};

/// One component call, fully formed.
///
/// Carries the resolved component name beside the request because the caller
/// needs it to route — over HTTP it is a path, over NATS a subject — and
/// digging it back out of the registry would mean every implementation of
/// [`Component`] repeating the same lookup and the same failure case.
#[derive(Debug, Clone, PartialEq)]
pub struct Dispatch {
    /// Which step this is, for logging and for attributing a failure.
    pub node_id: NodeId,
    /// The component to call: `StepCommon::name`, which is also the subject
    /// work is published to in the original.
    pub component: String,
    /// The body, already narrowed to the five-key meta a component sees.
    pub request: ComponentRequest,
}

/// One run's inputs: everything a dispatch needs that is not the step itself.
///
/// A struct rather than three arguments to [`drive`], decided before any
/// transport is built on the signature. `custom_data` is the reason: the
/// original forwards a caller-supplied passthrough (`custom_data=custom_data`,
/// the original) and a component that reads
/// `custom_data` gets `null` without it. Adding it as a fourth
/// positional argument, then a fifth for the next thing, is how a signature
/// becomes impossible to change once callers exist.
#[derive(Debug, Clone, PartialEq)]
pub struct RunInput {
    /// The controller's envelope. Narrowed to five keys per dispatch.
    pub meta: Meta,
    /// What the first steps are given.
    pub input: Value,
    /// Caller passthrough, forwarded to every component unchanged.
    ///
    /// `None` and `Some(Value::Null)` are different on the wire -- the field is
    /// omitted for the first and sent for the second -- and a component that
    /// distinguishes them is entitled to.
    pub custom_data: Option<Value>,
}

/// Whatever actually calls a component.
///
/// One method, taking a fully-formed [`Dispatch`], so an implementation is a
/// transport and nothing more — it makes no decisions about what to send.
///
/// The error is the implementation's own. This crate never inspects it, only
/// attributes it to a step, because "why did the call fail" is a transport
/// question and pretending to enumerate it here would be inventing a taxonomy
/// no transport actually reports.
pub trait Component {
    /// What a failed call reports.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Calls the component and returns its raw response body.
    ///
    /// The *raw* response, not the step output: reading `step_output` out of it
    /// is [`pneuma_proto::component::extract_step_output`]'s job, and doing it
    /// here would put the wire contract in every transport.
    ///
    /// # Deliberately not `Send`
    ///
    /// This returned `+ Send` first, on the reasoning that a transport is
    /// shared and real ones would be. Wiring the actual one disproved it:
    /// `restate_sdk`'s `ContextSideEffects::run` returns
    /// `impl RunFuture<..>` with no `Send` promise, so awaiting it makes the
    /// caller non-`Send` and the bound made the *primary* implementation
    /// impossible to write. A bound that excludes the one transport this port
    /// adopted is not a safety property, it is a mistake.
    ///
    /// The cost is real and small: [`drive`] cannot be spawned across threads.
    /// It does not need to be. A run is one durable handler executing its steps
    /// in sequence — under Restate that ordering *is* the journal
    /// (`VERDICT.md` §1), so a driver that could be split across threads would
    /// be solving a problem this design does not have.
    fn call(&self, dispatch: Dispatch) -> impl Future<Output = Result<Value, Self::Error>>;
}

/// Why a run could not be driven to completion.
#[derive(Debug, thiserror::Error)]
pub enum DriveError<E: std::error::Error + Send + Sync + 'static> {
    /// The interpreter refused. Its own errors, unwrapped.
    #[error("scheduling failed: {0}")]
    Schedule(#[from] ScheduleError),

    /// A step the scheduler offered is not in the registry.
    ///
    /// The scheduler works from the registry, so this cannot happen without the
    /// two disagreeing — which is worth failing on rather than skipping the
    /// step and reporting a run that silently did less than it claimed.
    #[error("the scheduler offered {node_id}, which the registry does not have")]
    UnknownStep {
        /// Which step.
        node_id: NodeId,
    },

    /// A step has no component name, so there is nowhere to send it.
    #[error("{node_id} has no component name, so it cannot be dispatched")]
    NoComponent {
        /// Which step.
        node_id: NodeId,
    },

    /// The transport failed.
    #[error("calling {node_id} failed: {source}")]
    Call {
        /// Which step.
        node_id: NodeId,
        /// What the transport reported.
        source: E,
    },

    /// The component answered with something this system cannot read.
    #[error("{node_id} answered with an unusable response: {source}")]
    Response {
        /// Which step.
        node_id: NodeId,
        /// What was wrong with it.
        source: ComponentResponseError,
    },

    /// Nothing is runnable and the run is not finished.
    ///
    /// Distinct from an error at a step: every step that ran succeeded, and the
    /// run is wedged anyway — a join whose prerequisites can never all arrive,
    /// or an aggregator over nothing. The blocked steps are named because the
    /// whole difficulty of this failure is finding them.
    // `{frame:?}` rather than reaching for the index: `FrameId` is documented
    // as opaque and keeps its field private, and a driver is exactly the kind
    // of caller that should not be the reason that stops being true.
    #[error(
        "the run is stalled with nothing runnable, blocked at: {}",
        .blocked.iter().map(|(frame, node)| format!("{node} in {frame:?}"))
            .collect::<Vec<_>>().join(", ")
    )]
    Stalled {
        /// Every step blocking the run, across every branch.
        blocked: Vec<(FrameId, NodeId)>,
    },
}

/// A run that reached the end.
#[derive(Debug, Clone, PartialEq)]
pub struct Completed {
    /// Every top-level step nothing follows, with its output, in `NodeId`
    /// order. Plural because a pipeline may end in more than one place.
    pub outputs: Vec<(NodeId, Value)>,
    /// Every step that ran, in the order it was dispatched.
    ///
    /// The audit trail, and the thing to compare between an original run and a
    /// replay: if these differ, the journal and the interpreter disagree.
    pub ran: Vec<NodeId>,
}

/// Builds the dispatch for one task.
///
/// Split out because it is the whole of the wire-facing decision-making, and
/// pure: given a registry, a meta and a task, the request is determined.
fn dispatch_for<E: std::error::Error + Send + Sync + 'static>(
    registry: &StepRegistry,
    run: &RunInput,
    task: &Task,
) -> Result<Dispatch, DriveError<E>> {
    let Some(step) = registry.get(&task.node_id) else {
        return Err(DriveError::UnknownStep {
            node_id: task.node_id.clone(),
        });
    };
    let Some(name) = step.common().name.as_ref() else {
        return Err(DriveError::NoComponent {
            node_id: task.node_id.clone(),
        });
    };
    Ok(Dispatch {
        node_id: task.node_id.clone(),
        component: name.to_string(),
        request: ComponentRequest {
            meta: ComponentMeta::from(&run.meta),
            step_input: task.input.clone(),
            custom_data: run.custom_data.clone(),
            node_env_vars: node_env_vars(step),
            headers: Default::default(),
        },
    })
}

/// A step's per-node configuration, as the component receives it.
///
/// `step.params` is what the original sends as `node_env_vars`
/// (the original, and `pneuma_proto::dispatch`'s table).
/// Dropping it is silent: the component simply runs unconfigured, which for
/// `pipeline_params.yaml`'s node `A` means losing both declared keys.
///
/// `None` rather than an empty map when a step declares none, because it is the
/// honest representation of "this step declares no params" -- not because the
/// two differ on the wire. They do not: that field's
/// `skip_serializing_if = "is_absent_or_empty"` omits `Some(empty)` exactly as
/// it omits `None`, which is the whole point of the original's `omitempty` there.
///
/// Said precisely because the earlier version of this comment claimed the
/// difference *was* observable, and a later reader could have built a
/// present-versus-absent behaviour on a distinction the encoder erases.
fn node_env_vars(step: &Step) -> Option<BTreeMap<String, String>> {
    let params = &step.common().params;
    if params.is_empty() {
        return None;
    }
    Some(
        params
            .iter()
            .map(|(key, value)| (key.to_string(), value.to_string()))
            .collect(),
    )
}

/// Decides what "nothing runnable" means.
///
/// `next_task` returning `None` is not the same as finished: a wedged run also
/// has nothing runnable. The interpreter is asked which it is rather than
/// assumed.
///
/// Today no pipeline reaches the stalled arm through [`drive`]. The design notes
/// §18 is explicit that the port does not hang -- an unreachable join comes to
/// rest, the aggregator completes with `[]`, and `Execution::stalled` is
/// advisory rather than fatal -- so the only route to unfinished-with-nothing-
/// runnable is `Execution::abandon`, which this driver never calls.
///
/// The guard stays, and is a function so it can be tested rather than excluded.
/// Abandonment is exactly what arrives with step timeouts and permanently
/// failed steps, and the failure it would otherwise cause is a wedged run
/// reported as a successful one.
///
/// `blocked` is a closure because collecting the blocked set walks every frame,
/// and the answer is thrown away on the path that is always taken.
fn conclude<E: std::error::Error + Send + Sync + 'static>(
    finished: bool,
    blocked: impl FnOnce() -> Vec<(FrameId, NodeId)>,
) -> Result<(), DriveError<E>> {
    if finished {
        return Ok(());
    }
    Err(DriveError::Stalled { blocked: blocked() })
}

/// Runs `registry` to completion, calling `component` for every step.
///
/// Sequential on purpose. The interpreter offers one task at a time, and under
/// Restate the ordering of journalled calls is what replay depends on
/// (`VERDICT.md` §1) — so concurrency here would be a correctness change, not a
/// tuning knob, and belongs behind a deliberate design rather than falling out
/// of the driver.
pub async fn drive<C: Component>(
    registry: &StepRegistry,
    run: &RunInput,
    component: &C,
) -> Result<Completed, DriveError<C::Error>> {
    drive_recording(registry, run, component, &NoRecord).await
}

/// [`drive`], writing down what it did as it goes.
///
/// The recorder is a separate function rather than a fourth argument to
/// [`drive`] because most callers have nothing to write to and should not have
/// to say so — and because `NoRecord` in a signature reads as an omission where
/// a missing argument reads as the default.
///
/// # Where the hooks are, and why four and not three
///
/// The obvious three are: a step is about to be called, a step produced an
/// output, and the interpreter resolved something on its own. That set misses
/// every failure. A component that never answers and a component that answers
/// with something unusable both leave this function by an early return, and a
/// step whose row stays `PROCESSING` for ever is exactly the shape
/// `pneuma-janitor`'s stale detection exists to find — so recording it as an
/// error is the difference between the janitor working and the janitor having
/// nothing to look at.
pub async fn drive_recording<C: Component, R: Recorder>(
    registry: &StepRegistry,
    run: &RunInput,
    component: &C,
    recorder: &R,
) -> Result<Completed, DriveError<C::Error>> {
    let mut execution = Execution::new(registry, run.input.clone());

    loop {
        // Not `next_task(registry)?`. A scheduling failure still leaves behind
        // whatever the interpreter did before it -- a conditional it resolved,
        // an aggregator it announced -- and propagating first would discard
        // those, so the mirror would be missing exactly the steps that explain
        // the failure.
        let next = execution.next_task(registry);
        // Drained *before* the task is handled, so an aggregator's row exists
        // before any step inside its branches -- `node_run.parent_path` is a
        // non-deferrable foreign key, and `Happening` documents the ordering
        // this depends on.
        write_all(&mut execution, registry, run, recorder).await;
        let Some(task) = next? else { break };

        let dispatch = dispatch_for(registry, run, &task)?;
        // `&task` rather than its three fields spread over four lines: a
        // multi-line argument list is one of the shapes `cargo-tarpaulin`
        // attributes to its first line only, and `docs/verification.md` says
        // to bind the value so the interesting line is one line.
        let starting = record::dispatching(&execution, registry, run, &task);
        write(recorder, starting).await;

        let node_id = task.node_id.clone();
        let response = match component.call(dispatch).await {
            Ok(response) => response,
            Err(source) => {
                let failure = failure(record::TRANSPORT_ERROR, &source);
                let records = record::failing(&execution, run, &task, failure);
                write(recorder, records).await;
                return Err(DriveError::Call { node_id, source });
            }
        };
        let output = match extract_step_output(&response) {
            Ok(output) => output.clone(),
            Err(source) => {
                let failure = failure(record::UNUSABLE_RESPONSE, &source);
                let records = record::failing(&execution, run, &task, failure);
                write(recorder, records).await;
                return Err(DriveError::Response { node_id, source });
            }
        };
        let finished = record::finishing(&execution, run, &task, output.clone());
        write(recorder, finished).await;
        execution.report(registry, &task, output)?;
        write_all(&mut execution, registry, run, recorder).await;
    }

    conclude(execution.is_finished(), || execution.stalled())?;

    Ok(Completed {
        outputs: execution.run_output(registry)?,
        ran: execution.ran().to_vec(),
    })
}

/// A failure, from anything that prints.
fn failure(code: &str, source: &impl std::fmt::Display) -> Failure {
    Failure {
        code: code.to_owned(),
        message: source.to_string(),
    }
}

/// Writes a batch down, in order.
async fn write<R: Recorder>(recorder: &R, records: Vec<Record>) {
    for record in records {
        recorder.record(record).await;
    }
}

/// Writes down everything the interpreter has done since it was last asked.
async fn write_all<R: Recorder>(
    execution: &mut Execution,
    registry: &StepRegistry,
    run: &RunInput,
    recorder: &R,
) {
    // Drained first and derived after, because `happened` borrows the execution
    // and `drain_happenings` takes it mutably.
    let happenings = execution.drain_happenings();
    for happening in happenings {
        let records = record::happened(execution, registry, run, &happening);
        write(recorder, records).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pneuma_core::resolver::resolve;

    /// The transport error a driver test never actually raises -- `dispatch_for`
    /// and `conclude` are generic over it, so one is needed to name the type.
    #[derive(Debug, thiserror::Error)]
    #[error("unused")]
    struct NoError;

    fn meta() -> Meta {
        let value = serde_json::json!({
            "job_id": "job-1",
            "tenant_id": "tenant-1",
            "pipeline_type": "invoice",
            "pipeline_level": "page",
            "pipeline_name": "default",
        });
        match serde_json::from_value(value) {
            Ok(meta) => meta,
            Err(error) => panic!("the fixture meta must parse: {error}"),
        }
    }

    fn registry() -> StepRegistry {
        let yaml = "pipeline_id: p\nstart: A\ncomponents:\n\
                    \x20 - node_id: A\n    name: comp-a\n    type: Model\n    children: [end]\n";
        let definition = match serde_yaml::from_str(yaml) {
            Ok(definition) => definition,
            Err(error) => panic!("fixture yaml must parse: {error}"),
        };
        match resolve(&definition) {
            Ok(registry) => registry,
            Err(error) => panic!("fixture must resolve: {error}"),
        }
    }

    fn run() -> RunInput {
        RunInput {
            meta: meta(),
            input: serde_json::json!({}),
            custom_data: None,
        }
    }

    fn task(node: &str) -> Task {
        Task {
            frame: FrameId::ROOT,
            node_id: NodeId::new(node),
            input: serde_json::json!({}),
        }
    }

    #[test]
    fn a_task_naming_a_step_the_registry_does_not_have_is_refused() {
        // Not reachable through `drive`: the scheduler works from the same
        // registry, so offering a step it does not contain would mean the two
        // disagree. Tested here rather than excluded, because the alternative
        // to failing is dispatching nowhere and reporting a run that silently
        // did less than it claimed.
        let Err(error) = dispatch_for::<NoError>(&registry(), &run(), &task("ghost")) else {
            panic!("a step that does not exist cannot be dispatched");
        };
        let DriveError::UnknownStep { node_id } = &error else {
            panic!("wrong variant: {error:?}");
        };
        assert_eq!(node_id, &NodeId::new("ghost"));
        assert!(
            error.to_string().contains("the registry does not have"),
            "{error}"
        );
    }

    #[test]
    fn a_task_naming_a_real_step_produces_its_dispatch() {
        // The other arm, so the refusal above is not passing merely because
        // everything is refused.
        let Ok(dispatch) = dispatch_for::<NoError>(&registry(), &run(), &task("A")) else {
            panic!("a real step dispatches");
        };
        assert_eq!(dispatch.component, "comp-a");
        assert_eq!(dispatch.node_id, NodeId::new("A"));
    }

    #[test]
    fn a_finished_run_concludes_and_does_not_pay_for_the_blocked_set() {
        // The closure is why: walking every frame to collect blocked steps is
        // wasted on the path always taken.
        let Ok(()) = conclude::<NoError>(true, || panic!("must not be asked")) else {
            panic!("a finished run concludes");
        };
    }

    #[test]
    fn a_run_with_nothing_runnable_and_no_finish_is_stalled_and_names_the_blockage() {
        let blocked = vec![(FrameId::ROOT, NodeId::new("Join"))];
        let Err(error) = conclude::<NoError>(false, || blocked.clone()) else {
            panic!("nothing runnable and not finished is a stall");
        };
        let DriveError::Stalled { blocked: named } = &error else {
            panic!("wrong variant: {error:?}");
        };
        assert_eq!(named, &blocked);
        // The message names the step, because finding the blockage is the
        // entire difficulty of this failure.
        assert!(error.to_string().contains("Join"), "{error}");
    }
}
