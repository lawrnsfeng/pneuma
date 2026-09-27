//! The shape a component is sent, and what identifies the answer.
//!
//! Built from the corpus fixture through the real resolver, so the assertions
//! are about what a real definition produces rather than about a shape invented
//! here — including the nested case, which is the only one where `parent_path`
//! and `parent_kind` are anything but `None`.

use pneuma_core::ids::{NodeId, PipelineId, RunId};
use pneuma_core::node::{NodeKind, Pipeline};
use pneuma_core::resolver::resolve;
use pneuma_core::step::StepRegistry;
use pneuma_driver::{
    as_response, key_of, kind_of, message_run, node_for, path_of, wire_kind, CallKey, MessageError,
    RunContext,
};
use pneuma_proto::dispatch::MessageResult;
use serde_json::json;

const PIPELINE: &str = include_str!("fixtures/pipeline1.yaml");

fn registry() -> StepRegistry {
    let Ok(pipeline) = serde_yaml::from_str::<Pipeline>(PIPELINE) else {
        panic!("the corpus fixture parses");
    };
    match resolve(&pipeline) {
        Ok(registry) => registry,
        Err(error) => panic!("the corpus fixture resolves: {error}"),
    }
}

fn run() -> RunId {
    RunId::new("job-1")
}

fn pipeline_id() -> PipelineId {
    PipelineId::new("invoice.page.default")
}

#[test]
fn a_slug_is_the_run_and_the_steps_own_path() {
    // `f"{run_id}.{step.full_path}"` -- the original.
    let registry = registry();
    let Some(path) = path_of(&run(), &registry, &NodeId::new("A")) else {
        panic!("A is in the fixture");
    };
    assert!(path.starts_with("job-1."), "{path}");
    assert!(path.ends_with(".A"), "{path}");

    // A node the registry does not have has no path, rather than a path built
    // out of nothing -- a coordinate nobody else agrees with is worse than an
    // absent one.
    assert_eq!(path_of(&run(), &registry, &NodeId::new("nope")), None);
}

#[test]
fn the_node_carries_what_the_result_is_correlated_on() {
    let registry = registry();
    let Ok(info) = node_for(&registry, &run(), &pipeline_id(), &NodeId::new("A")) else {
        panic!("A is in the fixture");
    };
    assert_eq!(info.run_id.as_str(), "job-1");
    assert_eq!(info.node_id.as_str(), "A");
    assert_eq!(info.pipeline_id.as_str(), "invoice.page.default");
    assert_eq!(info.node_kind, NodeKind::Model);
    assert!(
        info.name.contains('A'),
        "the component's own name: {}",
        info.name
    );

    // A top-level node has no enclosing aggregator.
    assert_eq!(info.parent_id, None);
    assert_eq!(info.parent_path, None);
    assert_eq!(info.parent_kind, None);

    // The fan-out indices stay absent. This controller's fan-out lives inside
    // the interpreter as frames, and a frame is not a separate node run -- so
    // there is no child index, and `None` is deliberately not the same as
    // position one.
    assert_eq!(info.child_index, None);
    assert_eq!(info.sibling_index, None);
    assert_eq!(info.parent_index, None);
}

#[test]
fn a_nested_node_names_its_aggregator_and_the_aggregators_own_slug() {
    // The only case where the parent fields are anything but `None`, and the
    // one where a mistake would be invisible: a `parent_path` built by a
    // different rule from the path its parent was dispatched under would
    // correlate against nothing.
    let registry = registry();
    let Some(nested) = registry
        .iter()
        .find(|step| step.common().parent_id.is_some())
        .map(|step| step.common().node_id.clone())
    else {
        panic!("the fixture has a nested node");
    };
    let Ok(info) = node_for(&registry, &run(), &pipeline_id(), &nested) else {
        panic!("{nested} is in the fixture");
    };
    let Some(parent) = info.parent_id.clone() else {
        panic!("{nested} is nested");
    };
    let Some(parent_path) = info.parent_path.clone() else {
        panic!("a nested node knows its parent's path");
    };
    let Some(expected) = path_of(&run(), &registry, &parent) else {
        panic!("the parent has a path");
    };
    assert_eq!(
        parent_path.as_str(),
        expected,
        "built by the same rule the parent was dispatched under"
    );
    assert_eq!(
        info.parent_kind.as_deref(),
        Some("DictAggregator"),
        "a string, because the wire field is untyped in the original"
    );
}

#[test]
fn a_dispatch_the_registry_does_not_know_is_reported() {
    // Only reachable if a dispatch and its registry disagree, which would mean
    // the driver handed out a task for a node it does not have. Reported
    // rather than assumed away: the alternative is publishing a message with an
    // invented path that nothing will ever answer.
    let Err(MessageError::NoSuchStep { node_id }) =
        node_for(&registry(), &run(), &pipeline_id(), &NodeId::new("ghost"))
    else {
        panic!("there is no step called ghost");
    };
    assert_eq!(node_id, "ghost");
}

#[test]
fn a_result_is_correlated_by_its_node_not_its_subject() {
    // Every result for every run arrives on one subject, so the subject
    // identifies nothing. The `node` the original executor echoes back is the
    // only thing that does.
    let body = json!({
        "meta": {
            "job_id": "job-1", "tenant_id": "acme",
            "pipeline_type": "invoice", "pipeline_level": "page", "pipeline_name": "default",
        },
        "node": {
            "path": "job-1.invoice.page.default.A",
            "node_id": "A",
            "name": "invoice.page.default.A",
            "kind": "Model",
            "pipeline_id": "invoice.page.default",
            "run_id": "job-1",
        },
        "step_output": {"pages": 3},
    });
    let Ok(result) = serde_json::from_value::<MessageResult>(body) else {
        panic!("that is a result message");
    };
    assert_eq!(key_of(&result), CallKey::new("job-1", "A"));
}

#[test]
fn a_step_output_is_rewrapped_into_the_shape_the_driver_unwraps() {
    // `Component::call` returns a component's raw response and
    // `extract_step_output` reads `step_output` out of it -- the shape an HTTP
    // component answers with. Over NATS the step output arrives on its own, so
    // the transport puts it back rather than the driver learning a second
    // shape.
    let wrapped = as_response(&json!({"pages": 3}));
    let Ok(output) = pneuma_proto::component::extract_step_output(&wrapped) else {
        panic!("the driver should be able to unwrap it");
    };
    assert_eq!(output, &json!({"pages": 3}));
}

/// One step of each kind, for the two total mappings below.
fn one_of_each() -> Vec<(NodeKind, pneuma_core::step::Step)> {
    use pneuma_core::child_ref::ChildRef;
    use pneuma_core::node::ConditionalSuccessors;
    use pneuma_core::step::{AggregatorRefs, Step, StepCommon, StepStatus};
    use std::collections::BTreeMap;

    let common = |id: &str| StepCommon::new(NodeId::new(id));
    let refs = AggregatorRefs {
        start: pneuma_core::start_set::StartSet::new(
            pneuma_core::start_set::StartEntry {
                node_id: NodeId::new("inner"),
                key: "inner".into(),
            },
            Vec::new(),
        ),
        component_ids: Vec::new(),
        terminal_children: Vec::new(),
    };
    vec![
        (
            NodeKind::Model,
            Step::Model {
                common: common("m"),
                status: StepStatus::default(),
                extra: BTreeMap::new(),
            },
        ),
        (
            NodeKind::ListAggregator,
            Step::ListAggregator {
                common: common("l"),
                status: StepStatus::default(),
                refs: refs.clone(),
                extra: BTreeMap::new(),
            },
        ),
        (
            NodeKind::DictAggregator,
            Step::DictAggregator {
                common: common("d"),
                status: StepStatus::default(),
                refs,
                expected_children: None,
                extra: BTreeMap::new(),
            },
        ),
        (
            NodeKind::Condition,
            Step::Condition {
                common: common("c"),
                conditions: Vec::new(),
                children: ConditionalSuccessors {
                    on_true: ChildRef::End,
                    on_false: ChildRef::End,
                },
                extra: BTreeMap::new(),
            },
        ),
    ]
}

#[test]
fn every_kind_maps_both_ways() {
    // Total mappings, checked over every variant rather than over the ones a
    // fixture happens to contain. `NodeKind::ALL` is the guard: a fifth kind
    // added to the enum fails this rather than silently going unmapped.
    let mut seen = Vec::new();
    for (expected, step) in one_of_each() {
        assert_eq!(kind_of(&step), expected);
        seen.push(expected);
    }
    assert_eq!(seen, NodeKind::ALL.to_vec(), "one step per kind, in order");

    for kind in NodeKind::ALL {
        let spelled = wire_kind(kind);
        // The wire spelling is PascalCase and matches serde's, which is what
        // the original writes -- pinned here because `parent_kind` is an
        // untyped string on the wire and nothing else would catch a drift.
        let Ok(serde_json::Value::String(by_serde)) = serde_json::to_value(kind) else {
            panic!("a kind serialises as a string");
        };
        assert_eq!(spelled.as_str(), by_serde, "{kind:?}");
    }
}

#[test]
fn a_step_the_resolver_did_not_nest_uses_its_own_id_as_its_path() {
    // `full_path` is filled in by resolution. A step that has none is not a
    // step the resolver produced -- but the path rule has to be total, and the
    // honest answer is the node id, which is what the resolver would have
    // written for an unnested node.
    use pneuma_core::start_set::{StartEntry, StartSet};
    use std::collections::BTreeMap;

    let node = NodeId::new("bare");
    let (_kind, step) = match one_of_each().into_iter().next() {
        Some(pair) => pair,
        None => panic!("there is at least one kind"),
    };
    let mut steps = BTreeMap::new();
    steps.insert(node.clone(), step);
    let registry = StepRegistry::new(
        steps,
        StartSet::new(
            StartEntry {
                node_id: node.clone(),
                key: "bare".into(),
            },
            Vec::new(),
        ),
    );

    assert_eq!(
        path_of(&run(), &registry, &node),
        Some("job-1.bare".to_owned())
    );
}

/// A dispatch for node `A` carrying `step_input` and `custom_data` verbatim.
fn dispatch_with(
    step_input: serde_json::Value,
    custom_data: Option<serde_json::Value>,
) -> pneuma_runner::driver::Dispatch {
    use pneuma_core::ids::{JobId, TenantId};
    use pneuma_proto::component::{ComponentMeta, ComponentRequest};

    let mut node_env_vars = std::collections::BTreeMap::new();
    node_env_vars.insert("MODEL".to_owned(), "v2".to_owned());
    pneuma_runner::driver::Dispatch {
        node_id: NodeId::new("A"),
        component: "invoice.page.default.A".to_owned(),
        request: ComponentRequest {
            meta: ComponentMeta {
                job_id: JobId::new("job-1"),
                tenant_id: TenantId::new("acme"),
                pipeline_type: "invoice".into(),
                pipeline_level: "page".into(),
                pipeline_name: "default".into(),
            },
            step_input,
            custom_data,
            node_env_vars: Some(node_env_vars),
            headers: Default::default(),
        },
    }
}

fn context() -> RunContext {
    let raw = json!({
        "job_id": "job-1", "tenant_id": "acme",
        "pipeline_type": "invoice", "pipeline_level": "page", "pipeline_name": "default",
        "a_caller_extra": "kept",
    });
    let Ok(meta) = serde_json::from_value(raw) else {
        panic!("that is a meta");
    };
    RunContext {
        meta,
        run_id: run(),
        pipeline_id: pipeline_id(),
    }
}

#[test]
fn a_run_message_carries_the_whole_envelope_and_the_dispatchs_own_body() {
    let Ok(message) = message_run(
        &context(),
        &registry(),
        &dispatch_with(json!({"doc": "d"}), Some(json!({"caller": "x"}))),
    ) else {
        panic!("that is a dispatchable node");
    };
    // The *full* controller envelope, not the narrowed five-key meta a
    // component sees: the original executor echoes it back and the controller's
    // own listener reads it.
    assert!(message.meta.extra.contains_key("a_caller_extra"));
    assert_eq!(message.node.node_id.as_str(), "A");
    assert_eq!(
        message.step_input.as_object().and_then(|o| o.get("doc")),
        Some(&json!("d"))
    );
    assert_eq!(message.custom_data.get("caller"), Some(&json!("x")));
    assert_eq!(
        message
            .node_env_vars
            .get("MODEL")
            .map(compact_str::CompactString::as_str),
        Some("v2")
    );
    // Always absent on this leg: the reply addresses belong to the run, not to
    // a component call.
    assert_eq!(message.reply_to_result, None);
    assert_eq!(message.reply_to_error, None);
    assert_eq!(message.reply_to_event, None);

    // Absent custom_data is an empty map rather than a failure -- most runs
    // carry none.
    let Ok(bare) = message_run(&context(), &registry(), &dispatch_with(json!([1, 2]), None)) else {
        panic!("an array input is a payload too");
    };
    assert!(bare.custom_data.is_empty());
    assert_eq!(bare.step_input.as_array().map(<[_]>::len), Some(2));
}

#[test]
fn a_scalar_step_input_is_refused_rather_than_forwarded() {
    // The wire type allows an object or an array, matching the original's
    // `dict[str, Any] | list[Any]`. The original services take an `interface{}` and
    // would forward a scalar, which the protocol notes record as a live
    // divergence -- so refusing here is a decision, not an oversight.
    let Err(MessageError::NotAPayload { node_id }) = message_run(
        &context(),
        &registry(),
        &dispatch_with(json!("just a string"), None),
    ) else {
        panic!("a string is not a step input");
    };
    assert_eq!(node_id, "A");
}

#[test]
fn scalar_custom_data_is_refused_too() {
    // `custom_data` is a map everywhere it is declared. Forwarding a scalar
    // would move the failure to whichever component looks at it first.
    let Err(MessageError::NotAnObject { node_id }) = message_run(
        &context(),
        &registry(),
        &dispatch_with(json!({}), Some(json!(7))),
    ) else {
        panic!("7 is not custom data");
    };
    assert_eq!(node_id, "A");
}
