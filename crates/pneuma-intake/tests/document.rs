//! What the run document has to contain, and the two ways of getting it wrong
//! that nothing downstream would report.
//!
//! Pure, so the whole of it runs without Mongo. The pipeline it is built from
//! is the corpus fixture, resolved by the real resolver — the shape being
//! asserted is the shape a real definition produces, not one invented here.

use chrono::{TimeZone, Utc};
use mongodb::bson::{doc, Bson, Document};
use pneuma_core::node::Pipeline;
use pneuma_core::resolver::resolve;
use pneuma_core::step::StepRegistry;
use pneuma_intake::{run_document, CREATED_AT, RUN_ID, STATE, STATUS, STEP_INPUT};
use pneuma_proto::envelope::Message;

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

fn message() -> Message {
    let body = serde_json::json!({
        "meta": {
            "job_id": "job-1", "tenant_id": "acme",
            "pipeline_type": "invoice", "pipeline_level": "page", "pipeline_name": "default",
        },
        "step_input": {"doc": "d"},
        "reply_to_result": "out.q",
    });
    match serde_json::from_value(body) {
        Ok(message) => message,
        Err(error) => panic!("that is a message: {error}"),
    }
}

/// The stored definition, as it comes back from Mongo: with an `_id`.
fn stored() -> Document {
    doc! {
        "_id": mongodb::bson::oid::ObjectId::new(),
        "pipeline_id": "invoice.page.default",
        "start": "A",
        "an_unmodelled_field": "kept",
    }
}

fn built() -> Document {
    let Some(at) = Utc.with_ymd_and_hms(2026, 3, 1, 12, 0, 0).single() else {
        panic!("a real instant");
    };
    match run_document(stored(), &registry(), "job-1", &message(), at) {
        Ok(document) => document,
        Err(error) => panic!("the document should build: {error}"),
    }
}

#[test]
fn the_pipeline_id_does_not_become_the_run_id() {
    // The trap: the run document starts as the *pipeline* document, and a
    // document read from Mongo carries its `_id`. Copying it in gives every run
    // of one pipeline the same primary key -- so the first insert succeeds and
    // every later one fails with `DuplicateKey`, which this crate reads as
    // "a redelivery already handled it". Every run of that pipeline after the
    // first would be acked, never written, never dispatched, with nothing
    // logged. The `_id` that matters is the one Mongo generates per run.
    assert!(
        !built().contains_key("_id"),
        "the run gets its own key, not the pipeline's"
    );
}

#[test]
fn state_is_the_flat_map_the_rest_of_the_system_reads() {
    // `StepRegistry` serialises as `{"steps": {...}, "start": ...}`, which is a
    // better shape and the wrong one. The gateway indexes `state.<node_id>` to
    // show a node's status, so written as-is every per-node display goes blank
    // with no error anywhere.
    let document = built();
    let Ok(state) = document.get_document(STATE) else {
        panic!("state should be a document");
    };
    assert!(
        !state.contains_key("steps") && !state.contains_key("start"),
        "flat, not the registry's own shape: {state:?}"
    );
    assert_eq!(
        state.len(),
        registry().len(),
        "one entry per resolved step, keyed by node id"
    );
    for step in registry().iter() {
        let node_id = step.common().node_id.as_str();
        let Ok(entry) = state.get_document(node_id) else {
            panic!("state.{node_id} should be a document");
        };
        assert_eq!(
            entry.get_str("node_id").ok(),
            Some(node_id),
            "the step keeps its own id as well as being keyed by it"
        );
        assert!(entry.contains_key("type"), "and its kind: {entry:?}");
    }
}

#[test]
fn everything_the_pipeline_carried_is_still_there() {
    // The run document is the definition plus a few keys, exactly as the
    // original builds it from `model_dump()`. A definition field this port does
    // not model still has to reach the run, or the port quietly narrows what a
    // pipeline can say.
    let document = built();
    assert_eq!(document.get_str("an_unmodelled_field").ok(), Some("kept"));
    assert_eq!(
        document.get_str("pipeline_id").ok(),
        Some("invoice.page.default")
    );
}

#[test]
fn the_run_starts_created_and_stamped() {
    let document = built();
    assert_eq!(document.get_str(RUN_ID).ok(), Some("job-1"));
    assert_eq!(document.get_str(STATUS).ok(), Some("created"));
    let Ok(created_at) = document.get_datetime(CREATED_AT) else {
        panic!("created_at should be a BSON date");
    };
    assert_eq!(created_at.timestamp_millis(), 1_772_366_400_000);
    let Ok(input) = document.get_document(STEP_INPUT) else {
        panic!("step_input should be a document");
    };
    assert_eq!(input.get_str("doc").ok(), Some("d"));
}

#[test]
fn the_three_reply_topics_are_always_present() {
    // Written even when absent, as `Null`. The original sets all three
    // unconditionally from values that may be `None`, so a run document there
    // always has the keys -- and a consumer telling "no reply address" apart
    // from "a run document that predates the field" needs them to keep being
    // there.
    let document = built();
    assert_eq!(document.get_str("reply_to_result").ok(), Some("out.q"));
    assert_eq!(document.get("reply_to_error"), Some(&Bson::Null));
    assert_eq!(document.get("reply_to_event"), Some(&Bson::Null));
}

#[test]
fn a_step_carrying_something_bson_cannot_hold_is_named_rather_than_dropped() {
    // Reachable from real wire data, not hypothetical: `serde_json` holds
    // integers up to `u64::MAX` and BSON's largest integer is an `i64`, so a
    // caller sending a number above `i64::MAX` in an unmodelled step field
    // produces a document that cannot be encoded. The alternatives to failing
    // are worse -- dropping the field silently, or truncating the number -- and
    // both would be discovered by somebody reading a run months later.
    use pneuma_core::ids::NodeId;
    use pneuma_core::start_set::{StartEntry, StartSet};
    use pneuma_core::step::{Step, StepCommon, StepRegistry};
    use std::collections::BTreeMap;

    let node = NodeId::new("A");
    let mut extra = BTreeMap::new();
    extra.insert(
        "too_big".to_owned(),
        serde_json::json!(u64::from(u32::MAX) * 4_294_967_296),
    );
    let step = Step::Model {
        common: StepCommon::new(node.clone()),
        status: pneuma_core::step::StepStatus::default(),
        extra,
    };
    let mut steps = BTreeMap::new();
    steps.insert(node.clone(), step);
    let start = StartSet::new(
        StartEntry {
            node_id: node,
            key: "A".into(),
        },
        Vec::new(),
    );
    let registry = StepRegistry::new(steps, start);

    let Some(at) = Utc.with_ymd_and_hms(2026, 3, 1, 12, 0, 0).single() else {
        panic!("a real instant");
    };
    let Err(error) = run_document(stored(), &registry, "job-1", &message(), at) else {
        panic!("that number does not fit in BSON");
    };
    assert!(
        error.to_string().contains("state"),
        "the message says which part: {error}"
    );
}
