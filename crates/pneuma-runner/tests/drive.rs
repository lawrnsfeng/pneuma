//! Drives real corpus-shaped pipelines against a fake component.
//!
//! The point of the [`Component`] trait: every branch of the driver is
//! reachable here with no server, no transport and no Restate — including the
//! ones a live component would only produce by misbehaving.

use std::sync::Mutex;

use pneuma_core::ids::NodeId;
use pneuma_core::resolver::resolve;
use pneuma_core::status::NodeStatus;
use pneuma_core::step::StepRegistry;
use pneuma_proto::meta::Meta;
use pneuma_runner::{
    drive, drive_recording, Completed, Component, Dispatch, DriveError, Failure, Record, Recorder,
    RunInput,
};
use serde_json::{json, Value};

/// What a fake is told to do when a step is called.
#[derive(Clone)]
enum Reply {
    /// A well-formed component response carrying this output.
    Output(Value),
    /// A response that is not a component response.
    Raw(Value),
    /// The transport itself failed.
    Fail(&'static str),
}

#[derive(Debug, thiserror::Error)]
#[error("transport failed: {0}")]
struct FakeError(&'static str);

/// Answers per node id, and records what it was asked, in order.
struct Fake {
    replies: Vec<(&'static str, Reply)>,
    seen: Mutex<Vec<Dispatch>>,
}

impl Fake {
    fn new(replies: Vec<(&'static str, Reply)>) -> Self {
        Fake {
            replies,
            seen: Mutex::new(Vec::new()),
        }
    }

    /// Every node answers with an object echoing its own name, which is enough
    /// for the interpreter and keeps the fixtures short.
    fn echoing() -> Self {
        Fake::new(Vec::new())
    }
}

impl Component for Fake {
    type Error = FakeError;

    async fn call(&self, dispatch: Dispatch) -> Result<Value, FakeError> {
        let node = dispatch.node_id.as_str().to_owned();
        // A `Mutex` rather than a `RefCell` only because a shared-by-reference
        // fake is the honest shape. It is no longer forced: `Component::call`
        // dropped its `Send` bound once the Restate transport proved the bound
        // made itself unimplementable.
        match self.seen.lock() {
            Ok(mut seen) => seen.push(dispatch),
            Err(poisoned) => panic!("the fake's log was poisoned: {poisoned}"),
        }
        let reply = self
            .replies
            .iter()
            .find(|(name, _)| *name == node)
            .map(|(_, reply)| reply.clone())
            .unwrap_or_else(|| Reply::Output(json!({ "from": node })));
        match reply {
            Reply::Output(value) => Ok(json!({ "step_output": value })),
            Reply::Raw(value) => Ok(value),
            Reply::Fail(why) => Err(FakeError(why)),
        }
    }
}

fn run(input: Value) -> RunInput {
    RunInput {
        meta: meta(),
        input,
        custom_data: None,
    }
}

fn meta() -> Meta {
    let value = json!({
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

fn registry(yaml: &str) -> StepRegistry {
    let definition = match serde_yaml::from_str(yaml) {
        Ok(definition) => definition,
        Err(error) => panic!("fixture yaml must parse: {error}"),
    };
    match resolve(&definition) {
        Ok(registry) => registry,
        Err(error) => panic!("fixture must resolve: {error}"),
    }
}

/// A -> {C, B}, both ending. Declared C-before-B so the output ordering
/// assertion is about `NodeId` order rather than declaration order.
fn diamond() -> StepRegistry {
    registry(
        "pipeline_id: p\nstart: A\ncomponents:\n\
         \x20 - node_id: A\n    name: comp-a\n    type: Model\n    children: [B, C]\n\
         \x20 - node_id: C\n    name: comp-c\n    type: Model\n    children: [end]\n\
         \x20 - node_id: B\n    name: comp-b\n    type: Model\n    children: [end]\n",
    )
}

#[tokio::test]
async fn a_pipeline_runs_every_step_and_reports_every_ending() {
    let registry = diamond();
    let fake = Fake::echoing();
    let Ok(Completed { outputs, ran }) = drive(&registry, &run(json!({"doc": "d"})), &fake).await
    else {
        panic!("a well-formed pipeline should run");
    };

    assert_eq!(ran.len(), 3, "every step ran once: {ran:?}");
    assert_eq!(ran.first(), Some(&NodeId::new("A")), "the start went first");

    let ended: Vec<&str> = outputs.iter().map(|(id, _)| id.as_str()).collect();
    assert_eq!(
        ended,
        vec!["B", "C"],
        "both endings, in NodeId order despite C being declared first"
    );
}

#[tokio::test]
async fn the_dispatch_carries_the_component_name_and_the_narrowed_meta() {
    // The wire-facing half. A component's `name` is what the caller routes on,
    // and the meta it receives is the five-key narrowing -- not the
    // controller's envelope, which carries a derived `pipeline_id` and caller
    // extras a component never sees.
    let registry = diamond();
    let fake = Fake::echoing();
    if drive(&registry, &run(json!({"doc": "d"})), &fake)
        .await
        .is_err()
    {
        panic!("should run");
    }

    let Ok(seen) = fake.seen.lock() else {
        panic!("the fake's log was poisoned");
    };
    let Some(first) = seen.first() else {
        panic!("something was dispatched");
    };
    assert_eq!(first.component, "comp-a", "routed on the component name");
    assert_eq!(first.request.step_input, json!({"doc": "d"}));

    let Ok(encoded) = serde_json::to_value(&first.request) else {
        panic!("the request must serialise");
    };
    let Some(meta_keys) = encoded.pointer("/meta").and_then(Value::as_object) else {
        panic!("the request has a meta: {encoded}");
    };
    let mut keys: Vec<&str> = meta_keys.keys().map(String::as_str).collect();
    keys.sort_unstable();
    assert_eq!(
        keys,
        vec![
            "job_id",
            "pipeline_level",
            "pipeline_name",
            "pipeline_type",
            "tenant_id"
        ],
        "exactly the five keys a component is sent, and no pipeline_id"
    );
}

#[tokio::test]
async fn a_transport_failure_names_the_step_it_failed_on() {
    let registry = diamond();
    let fake = Fake::new(vec![("B", Reply::Fail("connection reset"))]);
    let Err(error) = drive(&registry, &run(json!({})), &fake).await else {
        panic!("a failed call must fail the run");
    };
    let DriveError::Call { node_id, source } = &error else {
        panic!("wrong variant: {error:?}");
    };
    assert_eq!(node_id, &NodeId::new("B"));
    assert_eq!(source.to_string(), "transport failed: connection reset");
    // The message names the step, because "a call failed" without saying which
    // is the report that sends someone reading logs.
    assert!(error.to_string().contains("calling B failed"), "{error}");
}

#[tokio::test]
async fn a_response_this_system_cannot_read_names_the_step_too() {
    // A zero-SDK component that answers 200 with the wrong shape. The
    // difference from a transport failure matters: the component was reached
    // and replied, so retrying the call is not the fix.
    let registry = diamond();
    let fake = Fake::new(vec![("A", Reply::Raw(json!({"result": "unwrapped"})))]);
    let Err(error) = drive(&registry, &run(json!({})), &fake).await else {
        panic!("an unusable response must fail the run");
    };
    let DriveError::Response { node_id, .. } = &error else {
        panic!("wrong variant: {error:?}");
    };
    assert_eq!(node_id, &NodeId::new("A"));
    assert!(error.to_string().contains("unusable response"), "{error}");
}

#[tokio::test]
async fn a_step_with_no_component_name_is_refused_rather_than_dispatched_nowhere() {
    let registry = registry(
        "pipeline_id: p\nstart: A\ncomponents:\n\
         \x20 - node_id: A\n    type: Model\n    children: [end]\n",
    );
    let fake = Fake::echoing();
    let Err(error) = drive(&registry, &run(json!({})), &fake).await else {
        panic!("there is nowhere to send it");
    };
    let DriveError::NoComponent { node_id } = &error else {
        panic!("wrong variant: {error:?}");
    };
    assert_eq!(node_id, &NodeId::new("A"));
    let Ok(seen) = fake.seen.lock() else {
        panic!("the fake's log was poisoned");
    };
    assert!(
        seen.is_empty(),
        "and nothing was dispatched before the refusal"
    );
}

// --- the corpus -----------------------------------------------------------
//
// The hand-written diamond above exercises the driver's own branches. It does
// not exercise the graphs the driver exists for: a fan-out opens child frames,
// so `Task::frame` is no longer `ROOT`, and `run_output` deliberately reports
// only the root. None of that has branch-specific code in the driver, which is
// exactly why 100% line coverage says nothing about it -- and why these run the
// real fixtures instead.

fn corpus(name: &str) -> StepRegistry {
    let path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../pneuma-core/tests/fixtures/pipelines/"
    );
    let Ok(text) = std::fs::read_to_string(format!("{path}{name}.yaml")) else {
        panic!("{name}.yaml should be readable");
    };
    registry(&text)
}

/// Shapes its answer to what the *next* step needs, the way the interpreter's
/// own test driver does.
///
/// A fan-out feeding a list aggregator needs an array; anything else may echo
/// its input. Answering with a bare object everywhere makes list aggregators
/// fail, which reads as a driver bug and is not one.
struct Corpus<'a> {
    registry: &'a StepRegistry,
    seen: Mutex<Vec<Dispatch>>,
}

impl Component for Corpus<'_> {
    type Error = FakeError;

    async fn call(&self, dispatch: Dispatch) -> Result<Value, FakeError> {
        let input = dispatch.request.step_input.clone();
        let node = dispatch.node_id.clone();
        match self.seen.lock() {
            Ok(mut seen) => seen.push(dispatch),
            Err(poisoned) => panic!("the fake's log was poisoned: {poisoned}"),
        }
        let feeds_list_aggregator = self
            .registry
            .get(&node)
            .map(|step| {
                step.common().next_nodes.iter().any(|id| {
                    matches!(
                        self.registry.get(id),
                        Some(pneuma_core::step::Step::ListAggregator { .. })
                    )
                })
            })
            .unwrap_or(false);
        let output = if feeds_list_aggregator {
            json!([input])
        } else if input.is_object() {
            input
        } else {
            json!({ "step_output": input })
        };
        Ok(json!({ "step_output": output }))
    }
}

fn corpus_input() -> Value {
    json!({
        "doc": "d",
        "items": [1, 2],
        "equals": "value",
        "contains": "a value here",
        "isempty": "",
        "isnotempty": "something",
        "resultB": "value",
        "resultC": "a value here"
    })
}

#[tokio::test]
async fn every_corpus_pipeline_drives_to_completion() {
    // The fixtures the port is measured against, driven end to end through the
    // real loop rather than a shape invented to suit it.
    for name in [
        "pipeline1",
        "pipeline2",
        "pipeline3",
        "pipeline4",
        "pipeline_case1",
        "pipeline_case2",
        "pipeline_case3",
        "pipeline_condition_dict",
        "pipeline_condition_list",
        "pipeline_nested_list_dict",
        "pipeline_params",
    ] {
        let registry = corpus(name);
        let fake = Corpus {
            registry: &registry,
            seen: Mutex::new(Vec::new()),
        };
        let completed = match drive(&registry, &run(corpus_input()), &fake).await {
            Ok(completed) => completed,
            Err(error) => panic!("{name} should drive to completion: {error}"),
        };
        assert!(
            !completed.ran.is_empty(),
            "{name} ran at least one step -- a pipeline that runs nothing and \
             reports success is the failure this asserts against"
        );
        assert!(
            !completed.outputs.is_empty(),
            "{name} reported at least one ending"
        );
    }
}

#[tokio::test]
async fn a_fan_out_dispatches_from_child_frames_and_reports_only_the_root() {
    // `pipeline1` has a DictAggregator fan-out, so steps run inside child
    // frames. The driver hands `Task::frame` straight back to `report`, and
    // getting that wrong misfiles a branch's result -- invisible in the
    // diamond, where every frame is ROOT.
    let registry = corpus("pipeline1");
    let fake = Corpus {
        registry: &registry,
        seen: Mutex::new(Vec::new()),
    };
    let Ok(completed) = drive(&registry, &run(corpus_input()), &fake).await else {
        panic!("pipeline1 should drive");
    };

    let Ok(seen) = fake.seen.lock() else {
        panic!("the fake's log was poisoned");
    };
    assert!(
        seen.len() > completed.outputs.len(),
        "more steps were dispatched than were reported as endings: {} vs {}",
        seen.len(),
        completed.outputs.len()
    );
    // `run_output` reports the root frame alone, so a step that only ever runs
    // inside a branch must not appear among the endings.
    let ended: Vec<&str> = completed
        .outputs
        .iter()
        .map(|(id, _)| id.as_str())
        .collect();
    assert!(
        ended.len() < completed.ran.len(),
        "endings are a subset of what ran: {ended:?} of {:?}",
        completed.ran
    );
}

#[tokio::test]
async fn a_steps_declared_params_reach_the_component_as_node_env_vars() {
    // `pipeline_params.yaml` node `A` declares two. They were dropped: the
    // dispatch was built with `node_env_vars: None`, so the component ran
    // unconfigured and nothing said so.
    let registry = corpus("pipeline_params");
    let fake = Corpus {
        registry: &registry,
        seen: Mutex::new(Vec::new()),
    };
    if drive(&registry, &run(corpus_input()), &fake).await.is_err() {
        panic!("pipeline_params should drive");
    }

    let Ok(seen) = fake.seen.lock() else {
        panic!("the fake's log was poisoned");
    };
    let Some(a) = seen.iter().find(|d| d.node_id.as_str() == "A") else {
        panic!("A was dispatched");
    };
    let Some(env) = a.request.node_env_vars.as_ref() else {
        panic!("A declares params, so they must be sent");
    };
    let keys: Vec<&str> = env.keys().map(String::as_str).collect();
    assert_eq!(
        keys,
        vec!["key_1", "key_2"],
        "both, in a deterministic order"
    );

    // And a step that declares none sends absent, not an empty map: the original's
    // `omitempty` makes present-versus-absent observable to a component.
    let Some(b) = seen.iter().find(|d| d.node_id.as_str() == "B") else {
        panic!("B was dispatched");
    };
    assert_eq!(b.request.node_env_vars, None);
}

#[tokio::test]
async fn caller_custom_data_is_forwarded_to_every_component() {
    // Unreachable before `RunInput`: the field was hardcoded `None`, so any
    // component reading `custom_data` saw null under the port.
    let registry = diamond();
    let fake = Fake::echoing();
    let input = RunInput {
        meta: meta(),
        input: json!({"doc": "d"}),
        custom_data: Some(json!({"tenant_hint": "acme"})),
    };
    if drive(&registry, &input, &fake).await.is_err() {
        panic!("should drive");
    }

    let Ok(seen) = fake.seen.lock() else {
        panic!("the fake's log was poisoned");
    };
    assert_eq!(seen.len(), 3, "every step was called");
    for dispatch in seen.iter() {
        assert_eq!(
            dispatch.request.custom_data,
            Some(json!({"tenant_hint": "acme"})),
            "{} received it unchanged",
            dispatch.node_id
        );
    }
}

// --- the record a run leaves behind ---------------------------------------

/// Collects every record, in order.
///
/// The whole point of `Recorder` being a port: what a run writes down is
/// assertable with no database, so the derivation is tested where it lives
/// rather than through two transports that would each have to be stood up.
#[derive(Default)]
struct Ledger {
    written: Mutex<Vec<Record>>,
}

impl Ledger {
    fn entries(&self) -> Vec<Record> {
        match self.written.lock() {
            Ok(written) => written.clone(),
            Err(poisoned) => poisoned.into_inner().clone(),
        }
    }

    /// Every path a row was created for, in creation order.
    fn created(&self) -> Vec<String> {
        self.entries()
            .into_iter()
            .filter_map(|record| match record {
                Record::Created(step) => Some(step.path.clone()),
                _ => None,
            })
            .collect()
    }

    /// Every `(path, status)` a row was moved to.
    fn moved(&self) -> Vec<(String, NodeStatus)> {
        self.entries()
            .into_iter()
            .filter_map(|record| match record {
                Record::Moved { path, status, .. } => Some((path, status)),
                _ => None,
            })
            .collect()
    }

    /// Every `(path, failure)` a move carried.
    fn failures(&self) -> Vec<(String, Failure)> {
        self.entries()
            .into_iter()
            .filter_map(|record| match record {
                Record::Moved {
                    path,
                    failure: Some(failure),
                    ..
                } => Some((path, failure)),
                _ => None,
            })
            .collect()
    }

    /// Every `(path, status)` a row produced an output at.
    fn produced(&self) -> Vec<(String, NodeStatus)> {
        self.entries()
            .into_iter()
            .filter_map(|record| match record {
                Record::Produced { path, status, .. } => Some((path, status)),
                _ => None,
            })
            .collect()
    }
}

impl Recorder for Ledger {
    async fn record(&self, record: Record) {
        match self.written.lock() {
            Ok(mut written) => written.push(record),
            Err(poisoned) => poisoned.into_inner().push(record),
        }
    }
}

#[tokio::test]
async fn a_step_is_created_then_processing_then_finished() {
    let registry = diamond();
    let fake = Fake::echoing();
    let ledger = Ledger::default();
    let Ok(_) = drive_recording(&registry, &run(json!({"doc": "d"})), &fake, &ledger).await else {
        panic!("should run");
    };

    // Every dispatched step gets exactly one row, and the path is the
    // coordination key the original spells `{run}.{pipeline}.{node}`.
    let created = ledger.created();
    assert!(
        created
            .iter()
            .all(|path| path.starts_with("job-1.invoice.page.default.")),
        "{created:?}"
    );
    assert_eq!(
        created.len(),
        created
            .iter()
            .collect::<std::collections::BTreeSet<_>>()
            .len(),
        "one row per step, not one per visit: {created:?}"
    );

    // `CREATED` -> `PROCESSING` -> `FINISHED`, in that order, for the first
    // step. The middle one is not redundant: `FORKED` is admitted only from
    // `CREATED`, so a row that skipped `PROCESSING` would still fork and one
    // that skipped `CREATED` could not.
    let first = &created[0];
    assert!(
        ledger
            .moved()
            .iter()
            .any(|(path, status)| path == first && *status == NodeStatus::Processing),
        "{:?}",
        ledger.moved()
    );
    assert!(
        ledger
            .produced()
            .iter()
            .any(|(path, status)| path == first && *status == NodeStatus::Finished),
        "{:?}",
        ledger.produced()
    );
}

#[tokio::test]
async fn a_fan_out_writes_the_aggregator_before_anything_inside_it() {
    // `node_run.parent_path` is a non-deferrable foreign key onto `path`, so a
    // branch's row cannot be written before its aggregator's. Asserted on the
    // *order* of the ledger rather than on the rows existing, because both
    // orders produce the same set.
    let registry = corpus("pipeline1");
    let component = Corpus {
        registry: &registry,
        seen: Mutex::new(Vec::new()),
    };
    let ledger = Ledger::default();
    let Ok(_) = drive_recording(&registry, &run(corpus_input()), &component, &ledger).await else {
        panic!("pipeline1 should run");
    };

    let created = ledger.created();
    // A branch's path carries its aggregator's path as a prefix, so anything
    // nested is longer than the aggregator that holds it -- and every one of
    // them must appear after it.
    for (index, path) in created.iter().enumerate() {
        let Some(parent) = created
            .iter()
            .take(index)
            .rev()
            .find(|earlier| path.starts_with(&format!("{earlier}:")))
        else {
            continue;
        };
        assert!(
            created.iter().position(|p| p == parent) < Some(index),
            "{parent} must be written before {path}"
        );
    }

    // Something fanned out, or this test proves nothing.
    assert!(
        created.iter().any(|path| path.contains(':')),
        "pipeline1 fans out: {created:?}"
    );
}

#[tokio::test]
async fn every_branch_of_a_fan_out_gets_its_own_rows() {
    // The failure this exists to prevent is silent: `node_run.path` is unique
    // and `create` is `ON CONFLICT DO NOTHING`, so branches sharing a path do
    // not error -- every branch after the first simply vanishes from the
    // record.
    let registry = corpus("pipeline1");
    let component = Corpus {
        registry: &registry,
        seen: Mutex::new(Vec::new()),
    };
    let ledger = Ledger::default();
    let Ok(_) = drive_recording(&registry, &run(corpus_input()), &component, &ledger).await else {
        panic!("pipeline1 should run");
    };

    let created = ledger.created();
    let unique: std::collections::BTreeSet<&String> = created.iter().collect();
    assert_eq!(created.len(), unique.len(), "no two rows share a path");

    // And an aggregator is forked by the steps inside it.
    assert!(
        ledger
            .moved()
            .iter()
            .any(|(_, status)| *status == NodeStatus::Forked),
        "an aggregator forks: {:?}",
        ledger.moved()
    );
    // And aggregates when its branches come to rest.
    assert!(
        ledger
            .produced()
            .iter()
            .any(|(_, status)| *status == NodeStatus::Aggregated),
        "an aggregator aggregates: {:?}",
        ledger.produced()
    );
}

#[tokio::test]
async fn a_failure_is_recorded_and_carried_up_to_the_root() {
    // A step whose row stays `PROCESSING` for ever is exactly what
    // `pneuma-janitor`'s stale detection looks for, so a driver that recorded
    // only successes would leave the janitor with nothing to find.
    let registry = diamond();
    let fake = Fake::new(vec![("B", Reply::Fail("connection reset"))]);
    let ledger = Ledger::default();
    let Err(_) = drive_recording(&registry, &run(json!({})), &fake, &ledger).await else {
        panic!("a failed call must fail the run");
    };

    let errored: Vec<(String, NodeStatus)> = ledger
        .moved()
        .into_iter()
        .filter(|(_, status)| *status == NodeStatus::Error)
        .collect();
    assert_eq!(errored.len(), 1, "one step failed: {errored:?}");
    assert!(errored[0].0.ends_with(".B"), "{errored:?}");

    // The failure carries its reason, not just its status.
    let Some(Record::Moved { failure, .. }) = ledger.entries().into_iter().find(|record| {
        matches!(
            record,
            Record::Moved {
                status: NodeStatus::Error,
                ..
            }
        )
    }) else {
        panic!("the failure is recorded");
    };
    let Some(failure) = failure else {
        panic!("with a reason");
    };
    assert!(failure.message.contains("connection reset"), "{failure:?}");
}

#[tokio::test]
async fn a_response_that_cannot_be_read_is_recorded_as_a_failure_too() {
    // The second early return. A hook only where the interpreter commits a
    // result would miss it, and the row would stay `PROCESSING` for a step that
    // is definitively over.
    let registry = diamond();
    let fake = Fake::new(vec![("B", Reply::Raw(json!({"not": "a response"})))]);
    let ledger = Ledger::default();
    let Err(_) = drive_recording(&registry, &run(json!({})), &fake, &ledger).await else {
        panic!("an unusable response must fail the run");
    };
    assert!(
        ledger
            .moved()
            .iter()
            .any(|(path, status)| path.ends_with(".B") && *status == NodeStatus::Error),
        "{:?}",
        ledger.moved()
    );
}

#[tokio::test]
async fn a_conditional_is_recorded_although_it_never_dispatches() {
    // Conditionals are resolved inside the interpreter and never become tasks,
    // so a recorder watching only the dispatch loop would omit them -- and a
    // run's shape is mostly the steps that are not model steps.
    let registry = corpus("pipeline_condition_list");
    let component = Corpus {
        registry: &registry,
        seen: Mutex::new(Vec::new()),
    };
    let ledger = Ledger::default();
    let Ok(_) = drive_recording(&registry, &run(corpus_input()), &component, &ledger).await else {
        panic!("that pipeline should run");
    };

    let conditionals: Vec<String> = ledger
        .entries()
        .into_iter()
        .filter_map(|record| match record {
            Record::Created(step) if step.kind == pneuma_core::node::NodeKind::Condition => {
                Some(step.path.clone())
            }
            _ => None,
        })
        .collect();
    assert!(!conditionals.is_empty(), "that pipeline has a conditional");
    for path in conditionals {
        assert!(
            ledger
                .produced()
                .iter()
                .any(|(p, status)| *p == path && *status == NodeStatus::Finished),
            "a conditional finishes with its own input as its output"
        );
    }
}

#[tokio::test]
async fn a_run_that_records_nothing_behaves_identically() {
    // `drive` is `drive_recording` with `NoRecord`, and the mirror must not be
    // able to change what a run does -- that is the whole basis for it being
    // unable to fail one.
    let registry = corpus("pipeline1");
    let with = Corpus {
        registry: &registry,
        seen: Mutex::new(Vec::new()),
    };
    let without = Corpus {
        registry: &registry,
        seen: Mutex::new(Vec::new()),
    };
    let ledger = Ledger::default();
    let Ok(recorded) = drive_recording(&registry, &run(corpus_input()), &with, &ledger).await
    else {
        panic!("should run");
    };
    let Ok(plain) = drive(&registry, &run(corpus_input()), &without).await else {
        panic!("should run");
    };
    assert_eq!(recorded.ran, plain.ran);
    assert_eq!(recorded.outputs, plain.outputs);
    assert!(
        !ledger.entries().is_empty(),
        "and one of them wrote it down"
    );
}

#[tokio::test]
async fn a_failure_inside_a_fan_out_is_carried_up_to_every_ancestor() {
    // An aggregator did not itself fail, and the useful thing to know about it
    // is which of its children did — so its `error_message` is the failing
    // child's path rather than the error text, which is what the original
    // writes.
    let registry = corpus("pipeline1");
    // `C` is one of `X`'s declared starts, so it only ever runs inside a branch.
    let component = Failing {
        registry: &registry,
        fails: "C",
    };
    let ledger = Ledger::default();
    let Err(_) = drive_recording(&registry, &run(corpus_input()), &component, &ledger).await else {
        panic!("a step inside the fan-out fails");
    };

    let blamed: Vec<(String, NodeStatus)> = ledger
        .moved()
        .into_iter()
        .filter(|(_, status)| *status == NodeStatus::HasChildError)
        .collect();
    assert!(
        !blamed.is_empty(),
        "an ancestor learns of it: {:?}",
        ledger.moved()
    );

    // And it names the child, not the transport error.
    let Some(Record::Moved { failure, .. }) = ledger.entries().into_iter().find(|record| {
        matches!(
            record,
            Record::Moved {
                status: NodeStatus::HasChildError,
                ..
            }
        )
    }) else {
        panic!("just asserted there is one");
    };
    let Some(failure) = failure else {
        panic!("with a reason");
    };
    assert!(
        failure.message.contains(':'),
        "the failing branch's path, not the error text: {failure:?}"
    );
}

/// Answers like [`Corpus`], except for one node, which always fails.
///
/// Named rather than "the second call", because which call is the second is a
/// property of the fixture: `pipeline1` runs `A` then `B` at the root and only
/// then enters `X`'s body. Failing `C` is failing something that can only run
/// inside a branch, which is what this test is about.
struct Failing<'a> {
    registry: &'a StepRegistry,
    fails: &'static str,
}

impl Component for Failing<'_> {
    type Error = FakeError;

    async fn call(&self, dispatch: Dispatch) -> Result<Value, FakeError> {
        if dispatch.node_id == NodeId::new(self.fails) {
            return Err(FakeError("connection reset"));
        }
        let corpus = Corpus {
            registry: self.registry,
            seen: Mutex::new(Vec::new()),
        };
        corpus.call(dispatch).await
    }
}

#[tokio::test]
async fn an_aggregator_with_no_way_in_still_gets_a_row() {
    // A dict aggregator whose every declared start has a prerequisite has a
    // body nothing can enter. It is abandoned rather than given a fabricated
    // `[]` — the defect notes's second half — and a run that stops
    // there is precisely one an operator will come looking for, so the row has
    // to exist.
    let mut registry = corpus("pipeline1");
    let Some(pneuma_core::step::Step::DictAggregator { refs, .. }) =
        registry.get(&NodeId::new("X"))
    else {
        panic!("pipeline1's X is a dict aggregator");
    };
    let starts: Vec<NodeId> = refs.start.node_ids().cloned().collect();
    for id in &starts {
        match registry.get_mut(id) {
            Some(step) => step.common_mut().num_prerequisites = 1,
            None => panic!("{id} is in the registry"),
        }
    }

    let component = Corpus {
        registry: &registry,
        seen: Mutex::new(Vec::new()),
    };
    let ledger = Ledger::default();
    // It stalls, which is the point: nothing can enter the body.
    let _ = drive_recording(&registry, &run(corpus_input()), &component, &ledger).await;

    assert!(
        ledger.created().iter().any(|path| path.ends_with(".X")),
        "the abandoned aggregator has a row: {:?}",
        ledger.created()
    );
    // And the row says the run stopped there. A `CREATED` row with no error is
    // indistinguishable from one about to run, so an operator asking which step
    // a stalled run is stuck on would get no answer -- and `pneuma-janitor`
    // would only notice once the staleness threshold elapsed.
    let stopped: Vec<(String, NodeStatus)> = ledger
        .moved()
        .into_iter()
        .filter(|(path, _)| path.ends_with(".X"))
        .collect();
    assert_eq!(
        stopped,
        vec![("job-1.invoice.page.default.X".to_owned(), NodeStatus::Error)],
        "the abandoned aggregator is marked, once: {:?}",
        ledger.moved()
    );
    assert!(
        ledger
            .failures()
            .iter()
            .any(|(path, failure)| path.ends_with(".X")
                && failure.code == pneuma_runner::record::ABANDONED),
        "and says why: {:?}",
        ledger.failures()
    );
}

#[test]
fn every_kind_has_one_spelling() {
    // A total mapping between two enums. Checked directly rather than through
    // the one pipeline a fixture happens to contain, because only an aggregator
    // is ever a parent — so two of the four arms are unreachable through any
    // caller, and a spelling checked only through its callers is half checked.
    use pneuma_core::node::NodeKind;
    for (kind, spelled) in [
        (NodeKind::Model, "Model"),
        (NodeKind::ListAggregator, "ListAggregator"),
        (NodeKind::DictAggregator, "DictAggregator"),
        (NodeKind::Condition, "Condition"),
    ] {
        assert_eq!(pneuma_runner::wire_kind(kind), spelled);
    }
}

#[test]
fn every_step_maps_to_its_kind() {
    // The other half of the same total mapping, and the reason both are public.
    use pneuma_core::node::NodeKind;
    let registry = corpus("pipeline_condition_list");
    let mut seen: Vec<NodeKind> = registry.iter().map(pneuma_runner::kind_of).collect();
    seen.sort_unstable_by_key(|kind| format!("{kind:?}"));
    seen.dedup();
    assert!(
        seen.contains(&NodeKind::Model) && seen.contains(&NodeKind::Condition),
        "that fixture has both: {seen:?}"
    );
}
