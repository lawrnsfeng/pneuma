//! "Already there" against a real MongoDB, with a real unique index.
//!
//! The distinction being checked is the one the defect notes turns
//! on: a redelivered message must not fail, and a database that cannot be
//! reached must not look like a redelivered message. Both halves need a real
//! driver — the first because the error code and shape are the driver's, the
//! second because the failure has to be a genuine one rather than a
//! hand-built value.
//!
//! ```sh
//! PNEUMA_TEST_MONGO_URL=mongodb://127.0.0.1:27018 \
//!     cargo test -p pneuma-store --test conflict
//! ```

use mongodb::bson::{doc, Document};
use mongodb::options::IndexOptions;
use mongodb::{Client, Collection, IndexModel};
use pneuma_store::{is_duplicate_key, Created, PipelineStore, RunStore, DUPLICATE_KEY};

async fn collection(name: &str) -> Collection<Document> {
    let Ok(url) = std::env::var("PNEUMA_TEST_MONGO_URL") else {
        panic!("PNEUMA_TEST_MONGO_URL is not set; see the header of this file");
    };
    let Ok(client) = Client::with_uri_str(&url).await else {
        panic!("could not connect to {url}");
    };
    let collection: Collection<Document> = client.database("pneuma_test").collection(name);
    if let Err(error) = collection.drop().await {
        panic!("could not clear {name}: {error}");
    }
    collection
}

/// The index §25 argues for: the one that makes the conflict happen at all.
async fn unique_on(collection: &Collection<Document>, field: &str) {
    let model = IndexModel::builder()
        .keys(doc! { field: 1 })
        .options(Some(IndexOptions::builder().unique(true).build()))
        .build();
    if let Err(error) = collection.create_index(model).await {
        panic!("could not build the unique index on {field}: {error}");
    }
}

/// What an insert did, or a panic naming what went wrong instead.
///
/// `BarrierError` is not `PartialEq` -- a `mongodb::error::Error` is not -- so
/// the assertions unwrap here rather than comparing `Result`s.
fn created(outcome: Result<Created, pneuma_store::BarrierError>, what: &str) -> Created {
    match outcome {
        Ok(created) => created,
        Err(error) => panic!("{what} should have worked: {error}"),
    }
}

#[tokio::test]
async fn a_second_insert_of_the_same_run_is_success_not_a_fork() {
    // Today's index is non-unique, so this second insert *succeeds* and splits
    // the run across two documents that later reads pick between arbitrarily.
    // The index here is what §25 asks for; `Created::AlreadyExists` is what
    // makes asking for it safe.
    let runs = collection("conflict_runs").await;
    unique_on(&runs, "run_id").await;
    let store = RunStore::new(runs);

    let document = doc! { "run_id": "job-1", "status": "created", "state": {} };
    assert_eq!(
        created(store.create(document.clone()).await, "the first delivery"),
        Created::Inserted
    );
    assert_eq!(
        created(store.create(document).await, "the redelivery"),
        Created::AlreadyExists,
        "a redelivery is not a failure"
    );
}

#[tokio::test]
async fn a_redelivered_pipeline_definition_is_success_too() {
    let pipelines = collection("conflict_pipelines").await;
    unique_on(&pipelines, "pipeline_id").await;
    let store = PipelineStore::new(pipelines);

    let document = doc! { "pipeline_id": "p", "start": "A" };
    assert_eq!(
        created(store.create(document.clone()).await, "the first delivery"),
        Created::Inserted
    );
    assert_eq!(
        created(store.create(document).await, "the redelivery"),
        Created::AlreadyExists
    );

    let Ok(Some(found)) = store.by_pipeline_id("p").await else {
        panic!("the definition should be readable back");
    };
    assert_eq!(found.get_str("start").ok(), Some("A"));

    // And an unknown id is `None`, not an error: it is a statement about the
    // submission rather than about the database, and the caller rejects the
    // message rather than retrying.
    let Ok(None) = store.by_pipeline_id("nope").await else {
        panic!("an unknown pipeline is absent, not an error");
    };

    // Named, for a caller composing several stores and needing to check they
    // do not point at the same collection.
    assert_eq!(store.namespace().coll, "conflict_pipelines");
}

#[tokio::test]
async fn a_database_that_cannot_be_reached_is_not_a_duplicate() {
    // The half that matters more. Reporting "already there" for a database
    // that never answered would turn an outage into runs silently skipped --
    // acked, never inserted, never dispatched, with nothing recording it.
    let Ok(client) =
        Client::with_uri_str("mongodb://127.0.0.1:1/?serverSelectionTimeoutMS=100").await
    else {
        panic!("that is a well-formed URI");
    };
    let store = RunStore::new(client.database("pneuma_test").collection("unreachable"));
    let Err(error) = store.create(doc! { "run_id": "job-1" }).await else {
        panic!("nothing is listening on port 1");
    };
    assert!(
        error
            .to_string()
            .to_lowercase()
            .contains("server selection")
            || error.to_string().to_lowercase().contains("connect"),
        "the message should name the connection: {error}"
    );
}

#[tokio::test]
async fn a_pipeline_write_to_an_unreachable_database_is_an_error_too() {
    // The same half, for the other store. A `create_pipeline` message that was
    // acked because the database was down is a definition that never arrived
    // and nothing will retry.
    let Ok(client) =
        Client::with_uri_str("mongodb://127.0.0.1:1/?serverSelectionTimeoutMS=100").await
    else {
        panic!("that is a well-formed URI");
    };
    let store = PipelineStore::new(client.database("pneuma_test").collection("unreachable"));
    if store.create(doc! { "pipeline_id": "p" }).await.is_ok() {
        panic!("nothing is listening on port 1");
    }
}

#[test]
fn the_code_is_the_one_mongo_actually_sends() {
    assert_eq!(DUPLICATE_KEY, 11000);
}

#[tokio::test]
async fn the_classifier_says_no_to_anything_that_is_not_a_write_conflict() {
    let Ok(client) =
        Client::with_uri_str("mongodb://127.0.0.1:1/?serverSelectionTimeoutMS=100").await
    else {
        panic!("that is a well-formed URI");
    };
    let collection: Collection<Document> = client.database("pneuma_test").collection("unreachable");
    let Err(error) = collection.insert_one(doc! { "a": 1 }).await else {
        panic!("nothing is listening on port 1");
    };
    assert!(
        !is_duplicate_key(&error),
        "a server-selection failure is not a conflict"
    );
}
