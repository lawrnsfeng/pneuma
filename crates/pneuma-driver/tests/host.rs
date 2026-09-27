//! What a run's outcome means, and where its pipeline comes from.
//!
//! Pure, so none of it needs a broker or a database — which is the point: the
//! run document is read back into a pipeline by exactly the rule intake
//! wrote it with, and a mismatch between those two is a run that cannot start
//! for a reason nothing else would report.

use mongodb::bson::{doc, Document};
use pneuma_core::ids::NodeId;
use pneuma_core::resolver::resolve;
use pneuma_core::status::RunStatus;
use pneuma_driver::{
    describe, pipeline_of, status_for, wire_status, CallError, HostError, RunOutcome,
};
use pneuma_runner::driver::{Completed, DriveError};
use serde_json::Value;

const PIPELINE: &str = include_str!("fixtures/pipeline1.yaml");

/// A run document, exactly as `pneuma-intake` writes one: the pipeline's own
/// fields, plus the run's, plus Mongo's key.
fn run_document() -> Document {
    let Ok(value) = serde_yaml::from_str::<Value>(PIPELINE) else {
        panic!("the corpus fixture parses");
    };
    let Ok(mut document) = mongodb::bson::serialize_to_document(&value) else {
        panic!("and converts to BSON");
    };
    document.insert("_id", mongodb::bson::oid::ObjectId::new());
    document.insert("run_id", "job-1");
    document.insert("status", "created");
    document.insert("state", doc! { "A": { "status": "pending" } });
    document.insert("step_input", doc! { "doc": "d" });
    document
}

#[test]
fn a_run_carries_the_pipeline_it_was_created_from() {
    // Read out of the *run*, not the `pipelines` collection. A definition
    // edited between a run being created and being driven would otherwise
    // change what the run does halfway through.
    let Ok(pipeline) = pipeline_of("job-1", run_document()) else {
        panic!("a run document contains its pipeline");
    };
    assert_eq!(pipeline.pipeline_id.as_str(), "invoice.page.default");
    assert!(!pipeline.components.is_empty());

    // And it resolves, which is what `drive` needs.
    let Ok(registry) = resolve(&pipeline) else {
        panic!("the corpus fixture resolves");
    };
    assert!(registry.contains(&NodeId::new("A")));
}

#[test]
fn the_runs_own_key_does_not_travel_with_the_pipeline() {
    // `Pipeline::extra` is a catch-all, so an `_id` left in would reach every
    // dispatch as a `{"$oid": ...}` nobody downstream reads -- and the run's
    // key is not the definition's in any case.
    let Ok(pipeline) = pipeline_of("job-1", run_document()) else {
        panic!("a run document contains its pipeline");
    };
    assert!(!pipeline.extra.contains_key("_id"), "{:?}", pipeline.extra);

    // The run's own fields do survive in `extra`, because a catch-all keeps
    // what it does not model -- which is what lets a run document round-trip.
    assert!(pipeline.extra.contains_key("run_id"));
}

#[test]
fn a_document_that_is_not_a_pipeline_names_the_run() {
    // A run created by something that did not write a pipeline into it. The
    // message names the run, because "invalid document" without saying which
    // sends somebody to read a collection.
    let Err(HostError::NotAPipeline { run_id, reason }) = pipeline_of(
        "job-2",
        doc! { "run_id": "job-2", "components": "not a list" },
    ) else {
        panic!("that is not a pipeline");
    };
    assert_eq!(run_id, "job-2");
    assert!(!reason.is_empty());
}

#[test]
fn a_finished_run_is_finished_and_everything_else_is_an_error() {
    // One failed variant for every way of failing, because the run document's
    // answer to all of them is the same -- it is over and it did not succeed.
    // Separating them here would invent a taxonomy the document has no column
    // for.
    let finished = RunOutcome::Finished(Completed {
        outputs: vec![(NodeId::new("A"), Value::Null)],
        ran: vec![NodeId::new("A")],
    });
    assert_eq!(status_for(&finished), RunStatus::Finished);
    assert_eq!(
        status_for(&RunOutcome::Failed(
            "the component never answered".to_owned()
        )),
        RunStatus::Error
    );

    // There is deliberately no `Cancelled` arm. Cancellation is not something
    // `drive` reports -- it arrives on a different path entirely -- and a
    // driver that guessed at it from a failure message would mark a genuinely
    // failed run as cancelled the first time an error string contained the
    // word.
    assert_eq!(
        status_for(&RunOutcome::Failed("cancelled by the user".to_owned())),
        RunStatus::Error
    );
}

#[test]
fn a_drive_failure_is_described_rather_than_classified() {
    // What a person reads, kept separate from what the system reads, so only
    // one of the two can change without a migration.
    let error: DriveError<CallError> = DriveError::Call {
        node_id: NodeId::new("A"),
        source: CallError::Timeout {
            node_id: "A".to_owned(),
            after: std::time::Duration::from_secs(300),
        },
    };
    let said = describe(&error);
    assert!(said.contains('A'), "it names the step: {said}");
    assert!(said.contains("300"), "and what went wrong: {said}");
}

#[test]
fn every_status_is_spelled_the_way_the_run_document_spells_it() {
    // Hand-written rather than reached through `serde_json::to_value`, because
    // that conversion is infallible in fact and fallible in type -- using it
    // would put an arm in the *writing* path that no input can reach. This
    // holds the hand-written spelling to serde's, which is the one the rest of
    // the system reads.
    for status in [
        RunStatus::Created,
        RunStatus::Processing,
        RunStatus::Finished,
        RunStatus::Error,
        RunStatus::TimedOut,
        RunStatus::Cancelled,
    ] {
        let Ok(Value::String(by_serde)) = serde_json::to_value(status) else {
            panic!("a status serialises as a string");
        };
        assert_eq!(wire_status(status), by_serde, "{status:?}");
    }
}
