//! Drives [`RunStore`] against a real MongoDB.
//!
//! Requires `PNEUMA_TEST_MONGO_URL`; fails rather than skips without it, for
//! the reason given in `barrier.rs`.

use mongodb::bson::{doc, Document};
use mongodb::{Client, Collection};
use pneuma_core::step::StepStatus;
use pneuma_store::{RunStatus, RunStore, StepId};

async fn runs_collection() -> Collection<Document> {
    let Ok(url) = std::env::var("PNEUMA_TEST_MONGO_URL") else {
        panic!("PNEUMA_TEST_MONGO_URL is not set; see tests/barrier.rs");
    };
    let Ok(client) = Client::with_uri_str(&url).await else {
        panic!("could not connect to {url}");
    };
    // Its own collection, so this binary and tests/barrier.rs cannot drop each
    // other's data when a runner parallelises test targets.
    let runs: Collection<Document> = client.database("pneuma_test").collection("run_store");
    if let Err(error) = runs.drop().await {
        panic!("could not clear the test collection: {error}");
    }
    runs
}

/// A string field, or a panic naming the field — `bson`'s accessors return a
/// `Result` that is not comparable, and unwrapping at each site loses which
/// field was missing.
fn field<'a>(document: &'a Document, key: &str) -> &'a str {
    match document.get_str(key) {
        Ok(value) => value,
        Err(error) => panic!("{key} should be a string: {error}"),
    }
}

fn step(value: &str) -> StepId {
    match StepId::new(value) {
        Ok(id) => id,
        Err(error) => panic!("{value} should build: {error}"),
    }
}

#[tokio::test]
async fn every_run_store_method_works_against_mongo() {
    let runs = runs_collection().await;
    let store = RunStore::new(runs.clone());

    for (run_id, status) in [
        ("r-created", "created"),
        ("r-processing", "processing"),
        ("r-finished", "finished"),
        ("r-cancelled", "cancelled"),
    ] {
        if let Err(error) = runs
            .insert_one(doc! {
                "run_id": run_id,
                "status": status,
                "state": { "agg": { "status": "pending" } }
            })
            .await
        {
            panic!("could not seed {run_id}: {error}");
        }
    }

    // --- step status ------------------------------------------------------
    let agg = step("agg");
    let Ok(moved) = store
        .update_step_status("r-created", &agg, StepStatus::Forked)
        .await
    else {
        panic!("update_step_status failed");
    };
    assert!(moved);
    let Ok(Some(document)) = runs.find_one(doc! { "run_id": "r-created" }).await else {
        panic!("could not read it back");
    };
    let stored = document
        .get_document("state")
        .and_then(|state| state.get_document("agg"))
        .and_then(|agg| agg.get_str("status"));
    let Ok(stored) = stored else {
        panic!("the step status should be a string");
    };
    assert_eq!(stored, "forked", "the StepStatus spelling, not NodeStatus");

    // The existence filter: a step the run does not have is not created.
    let Ok(absent_step) = store
        .update_step_status("r-created", &step("nosuch"), StepStatus::Finished)
        .await
    else {
        panic!("update_step_status failed");
    };
    assert!(!absent_step, "a $set must not invent a step entry");
    let Ok(Some(document)) = runs.find_one(doc! { "run_id": "r-created" }).await else {
        panic!("could not read it back");
    };
    let Ok(state) = document.get_document("state") else {
        panic!("state should still be a document");
    };
    assert!(state.get("nosuch").is_none(), "nothing was created");

    let Ok(absent_run) = store
        .update_step_status("nosuch", &agg, StepStatus::Finished)
        .await
    else {
        panic!("update_step_status failed");
    };
    assert!(!absent_run);

    // --- run status -------------------------------------------------------
    //
    // Two things are asserted here that the original does not do: an omitted
    // error_slug leaves the stored one alone, and a terminal run is not moved.
    // Both are the design notes

    // First move: created -> processing, recording a path.
    let Ok(updated) = store
        .update_run_status("r-created", RunStatus::Processing, Some("run1.bad"))
        .await
    else {
        panic!("update_run_status failed");
    };
    assert!(updated);
    let Ok(Some(document)) = runs.find_one(doc! { "run_id": "r-created" }).await else {
        panic!("could not read it back");
    };
    assert_eq!(field(&document, "status"), "processing");
    assert_eq!(field(&document, "error_slug"), "run1.bad");

    // Still non-terminal, so this moves -- and the omitted path must not clear
    // the recorded one. The original writes None over it, and that field is
    // read the original.
    let Ok(moved) = store
        .update_run_status("r-created", RunStatus::Cancelled, None)
        .await
    else {
        panic!("update_run_status failed");
    };
    assert!(moved);
    let Ok(Some(document)) = runs.find_one(doc! { "run_id": "r-created" }).await else {
        panic!("could not read it back");
    };
    assert_eq!(field(&document, "status"), "cancelled");
    assert_eq!(
        field(&document, "error_slug"),
        "run1.bad",
        "an omitted path must not clear the recorded one"
    );

    // Now terminal. A late node result must not flip a cancelled job to
    // finished -- the run-level instance of the defect §1 closes at the node
    // level, and the rule cancel_runs already applies.
    let Ok(after_terminal) = store
        .update_run_status("r-created", RunStatus::Finished, None)
        .await
    else {
        panic!("update_run_status failed");
    };
    assert!(!after_terminal, "a cancelled run must not flip to finished");
    let Ok(Some(document)) = runs.find_one(doc! { "run_id": "r-created" }).await else {
        panic!("could not read it back");
    };
    assert_eq!(field(&document, "status"), "cancelled", "still cancelled");

    let Ok(also) = store
        .update_run_status("r-finished", RunStatus::Processing, None)
        .await
    else {
        panic!("update_run_status failed");
    };
    assert!(!also, "a finished run must not go back to processing");

    let Ok(missing) = store
        .update_run_status("nosuch", RunStatus::Error, None)
        .await
    else {
        panic!("update_run_status failed");
    };
    assert!(!missing);

    // --- the two listings -------------------------------------------------
    let Ok(finalized) = store.finalized_runs(100).await else {
        panic!("finalized_runs failed");
    };
    assert!(finalized.contains(&"r-finished".to_owned()));
    assert!(finalized.contains(&"r-cancelled".to_owned()));
    assert!(
        !finalized.contains(&"r-processing".to_owned()),
        "processing is not finalized"
    );

    let Ok(active) = store.active_runs(100).await else {
        panic!("active_runs failed");
    };
    assert_eq!(
        active,
        vec!["r-processing".to_owned()],
        "r-created is now cancelled"
    );

    let Ok(limited) = store.finalized_runs(1).await else {
        panic!("finalized_runs failed");
    };
    assert_eq!(limited.len(), 1, "the limit is applied");

    // A non-positive limit means nothing, not everything: MongoDB treats
    // `limit: 0` as no limit, so a janitor computing `batch - done` and
    // reaching zero would stream the whole collection.
    let Ok(nothing) = store.finalized_runs(0).await else {
        panic!("finalized_runs failed");
    };
    assert!(nothing.is_empty(), "a zero limit returns nothing");
    let Ok(nothing) = store.active_runs(-5).await else {
        panic!("active_runs failed");
    };
    assert!(nothing.is_empty(), "a negative limit returns nothing");

    // A document without a readable run_id must not block the batch. Erroring
    // on it made it a poison pill: the sort is `_id: 1`, so one malformed row
    // sits in every page forever and the janitor never progresses. The filter
    // now requires a string `run_id`, so the row neither blocks the batch nor
    // consumes the limit.
    if let Err(error) = runs.insert_one(doc! { "status": "finished" }).await {
        panic!("could not seed the malformed run: {error}");
    }
    let Ok(still_works) = store.finalized_runs(100).await else {
        panic!("a row without run_id must not fail the batch");
    };
    assert!(
        still_works.contains(&"r-finished".to_owned()),
        "the good runs are still returned"
    );
    assert!(
        !still_works.iter().any(String::is_empty),
        "and the malformed row is not among them"
    );
    if let Err(error) = runs
        .delete_many(doc! { "run_id": { "$exists": false } })
        .await
    {
        panic!("could not clean up: {error}");
    }

    // --- bulk cancel ------------------------------------------------------
    let ids = vec![
        "r-processing".to_owned(),
        "r-finished".to_owned(),
        // A duplicate, and one that does not exist.
        "r-processing".to_owned(),
        "nosuch".to_owned(),
    ];
    let Ok(cancelled) = store.cancel_runs(&ids).await else {
        panic!("cancel_runs failed");
    };
    assert_eq!(
        cancelled, 1,
        "only the non-terminal run moves; a finished run is left alone"
    );
    let Ok(Some(finished)) = runs.find_one(doc! { "run_id": "r-finished" }).await else {
        panic!("could not read it back");
    };
    assert_eq!(
        field(&finished, "status"),
        "finished",
        "cancelling must not overwrite a terminal run"
    );

    // Idempotent: the run is now terminal, so a repeat moves nothing.
    let Ok(again) = store.cancel_runs(&ids).await else {
        panic!("cancel_runs failed");
    };
    assert_eq!(again, 0);

    let Ok(none) = store.cancel_runs(&[]).await else {
        panic!("cancel_runs failed");
    };
    assert_eq!(none, 0, "an empty list is answered without a query");
}
