//! Drives the Mongo half against a real server.
//!
//! Index creation has no pure half worth testing: every rule that matters
//! belongs to Mongo, and the two that decide this module's shape -- that the
//! name makes creation idempotent, and that the same key under a *different*
//! name is an error -- are only observable against a server.
//!
//! ```sh
//! docker run -d --name pn-mongo -p 27018:27017 mongo:5.0.28
//! PNEUMA_TEST_MONGO_URL=mongodb://127.0.0.1:27018 \
//!     cargo test -p pneuma-migrate --test mongo
//! ```

use mongodb::bson::{doc, Bson, Document};
use mongodb::options::IndexOptions;
use mongodb::{Client, Collection, IndexModel};
use pneuma_migrate::{duplicate_run_ids, ensure_run_id_index, Duplicate, RunKey, RUN_ID_INDEX};

/// A collection of its own per test, so tests cannot see each other's indexes.
async fn runs(name: &str) -> Collection<Document> {
    let Ok(url) = std::env::var("PNEUMA_TEST_MONGO_URL") else {
        panic!("PNEUMA_TEST_MONGO_URL is not set; see the header of this file");
    };
    let Ok(client) = Client::with_uri_str(&url).await else {
        panic!("could not connect to {url}");
    };
    let collection = client.database("pneuma_migrate_test").collection(name);
    if let Err(error) = collection.drop().await {
        panic!("could not drop {name}: {error}");
    }
    collection
}

async fn index_names(collection: &Collection<Document>) -> Vec<String> {
    match collection.list_index_names().await {
        Ok(mut names) => {
            names.sort();
            names
        }
        Err(error) => panic!("could not list indexes: {error}"),
    }
}

#[tokio::test]
async fn the_index_is_created_and_creating_it_again_is_a_no_op() {
    // Idempotence is the property that lets this run on every deploy the way a
    // migration does.
    let collection = runs("idempotent").await;
    if let Err(error) = collection.insert_one(doc! { "run_id": "a" }).await {
        panic!("could not seed: {error}");
    }

    for attempt in 1..=3 {
        if let Err(error) = ensure_run_id_index(&collection).await {
            panic!("attempt {attempt} failed: {error}");
        }
    }

    assert_eq!(
        index_names(&collection).await,
        vec!["_id_".to_owned(), RUN_ID_INDEX.to_owned()],
        "one run_id index, however many times it was asked for"
    );
}

#[tokio::test]
async fn an_original_era_index_is_renamed_rather_than_collided_with() {
    // The case every existing database is in, and the one the fresh-collection
    // tests above cannot reach. Mongo refuses a second index over the same key
    // under a different name -- `IndexOptionsConflict`, "Index already exists
    // with a different name" -- so creating alongside would make
    // `pneuma-migrate mongo unique` an error on exactly the databases it is
    // meant to be run against.
    let collection = runs("rename").await;
    let legacy = IndexModel::builder()
        .keys(doc! { "run_id": 1 })
        .options(
            IndexOptions::builder()
                .name("idx_run_id".to_owned())
                .unique(false)
                .build(),
        )
        .build();
    if let Err(error) = collection.create_index(legacy).await {
        panic!("could not seed original-era index: {error}");
    }

    for attempt in 1..=2 {
        if let Err(error) = ensure_run_id_index(&collection).await {
            panic!("attempt {attempt} against an original-era database failed: {error}");
        }
    }

    assert_eq!(
        index_names(&collection).await,
        vec!["_id_".to_owned(), RUN_ID_INDEX.to_owned()],
        "renamed, not duplicated -- and no `idx_run_id` left behind"
    );
}

#[tokio::test]
async fn an_auto_named_index_is_renamed_too() {
    // What `createIndex` leaves when it is called without a name, which is what
    // the original the original migration tool revision would have produced had it not passed one.
    // Matched on the key rather than on the one old name we happen to know.
    let collection = runs("autonamed").await;
    let unnamed = IndexModel::builder().keys(doc! { "run_id": 1 }).build();
    if let Err(error) = collection.create_index(unnamed).await {
        panic!("could not seed an auto-named index: {error}");
    }

    if let Err(error) = ensure_run_id_index(&collection).await {
        panic!("an auto-named index should be renamed, not collided with: {error}");
    }
    assert_eq!(
        index_names(&collection).await,
        vec!["_id_".to_owned(), RUN_ID_INDEX.to_owned()]
    );
}

#[tokio::test]
async fn the_index_it_creates_has_the_shape_original_created() {
    // A rehoming, so the shape has to match what production already has: the
    // same key, ascending, and *not* unique. The *name* is this port's --
    // `RUN_ID_INDEX` says why -- but unique would be a behaviour change that
    // the defect notes show needs a service fix first, and the name
    // must not claim otherwise.
    let collection = runs("shape").await;
    if let Err(error) = ensure_run_id_index(&collection).await {
        panic!("could not create: {error}");
    }

    let mut cursor = match collection.list_indexes().await {
        Ok(cursor) => cursor,
        Err(error) => panic!("could not list: {error}"),
    };
    let mut found = None;
    use futures_util::TryStreamExt;
    // Not `while let Ok(Some(..))`: that reads a cursor *error* as end of
    // stream, and the test would then fail with "the index was not created",
    // blaming `ensure_run_id_index` for a dropped connection.
    loop {
        match cursor.try_next().await {
            Ok(Some(model)) => {
                if model.options.as_ref().and_then(|o| o.name.as_deref()) == Some(RUN_ID_INDEX) {
                    found = Some(model);
                }
            }
            Ok(None) => break,
            Err(error) => panic!("the index cursor failed: {error}"),
        }
    }
    let Some(model) = found else {
        panic!("the index was not created");
    };
    assert_eq!(model.keys, doc! { "run_id": 1 }, "ascending on run_id");
    assert_ne!(
        model.options.and_then(|o| o.unique),
        Some(true),
        "not unique -- production's shape, deliberately; see §25"
    );
}

#[tokio::test]
async fn a_collection_that_already_has_duplicates_still_takes_the_index() {
    // The reason the index is non-unique is that duplicates exist. Creating it
    // must therefore succeed over them, or rehoming would fail on exactly the
    // databases it has to run against.
    let collection = runs("dupes_ok").await;
    for _ in 0..2 {
        if let Err(error) = collection.insert_one(doc! { "run_id": "forked" }).await {
            panic!("could not seed: {error}");
        }
    }
    if let Err(error) = ensure_run_id_index(&collection).await {
        panic!("a non-unique index must build over duplicates: {error}");
    }
    assert!(index_names(&collection)
        .await
        .contains(&RUN_ID_INDEX.to_owned()));
}

#[tokio::test]
async fn duplicates_are_reported_worst_first_and_a_clean_collection_reports_none() {
    let collection = runs("report").await;
    for (run_id, copies) in [("once", 1), ("twice", 2), ("thrice", 3)] {
        for _ in 0..copies {
            if let Err(error) = collection.insert_one(doc! { "run_id": run_id }).await {
                panic!("could not seed: {error}");
            }
        }
    }

    let Ok(found) = duplicate_run_ids(&collection).await else {
        panic!("should report");
    };
    assert_eq!(
        found,
        vec![
            Duplicate {
                run_id: RunKey::Id("thrice".to_owned()),
                count: 3,
            },
            Duplicate {
                run_id: RunKey::Id("twice".to_owned()),
                count: 2,
            },
        ],
        "worst first, and a run_id appearing once is not a duplicate"
    );

    let clean = runs("report_clean").await;
    if let Err(error) = clean.insert_one(doc! { "run_id": "alone" }).await {
        panic!("could not seed: {error}");
    }
    let Ok(none) = duplicate_run_ids(&clean).await else {
        panic!("should report");
    };
    assert!(
        none.is_empty(),
        "an empty report is the answer that permits a unique index: {none:?}"
    );
}

#[tokio::test]
async fn the_index_name_is_what_makes_creation_idempotent() {
    // Measured, and the reason `RUN_ID_INDEX` is passed explicitly rather than
    // letting Mongo name the index: the same key under an auto-generated name
    // is rejected outright once ours exists. Had `ensure_run_id_index` omitted
    // the name it would have created `run_id_1` on a fresh database and then
    // failed forever against production's `idx_run_id`.
    let collection = runs("naming").await;
    if let Err(error) = ensure_run_id_index(&collection).await {
        panic!("could not create: {error}");
    }

    let unnamed = IndexModel::builder().keys(doc! { "run_id": 1 }).build();
    let Err(error) = collection.create_index(unnamed).await else {
        panic!("the same key under a different name must be refused");
    };
    let rendered = error.to_string();
    assert!(
        rendered.contains("different name") || rendered.contains(RUN_ID_INDEX),
        "and it says why: {rendered}"
    );
}

#[tokio::test]
async fn it_is_idempotent_against_the_index_original_actually_built() {
    // The assumption the whole rehoming rests on, now measured rather than
    // assumed. The live index was created by pymongo as
    // `create_index([("run_id", 1)], name="idx_run_id")` -- with **no** unique
    // option at all -- while `ensure_run_id_index` sends an explicit
    // `unique: false`. If the server treated those as different options it
    // would raise `IndexOptionsConflict` (code 85), and the rehoming would fail
    // on every database it was written for while passing every test that
    // created its own index first.
    let collection = runs("original_shaped").await;
    if let Err(error) = collection.insert_one(doc! { "run_id": "a" }).await {
        panic!("could not seed: {error}");
    }
    let as_original_built_it = IndexModel::builder()
        .keys(doc! { "run_id": 1 })
        .options(
            mongodb::options::IndexOptions::builder()
                .name(RUN_ID_INDEX.to_owned())
                .build(),
        )
        .build();
    if let Err(error) = collection.create_index(as_original_built_it).await {
        panic!("could not build original-shaped index: {error}");
    }

    if let Err(error) = ensure_run_id_index(&collection).await {
        panic!("an explicit unique:false must not conflict with an absent one: {error}");
    }
    assert_eq!(
        index_names(&collection).await,
        vec!["_id_".to_owned(), RUN_ID_INDEX.to_owned()],
        "the existing index was adopted, not duplicated"
    );
}

#[tokio::test]
async fn documents_with_no_run_id_are_reported_rather_than_aborting_the_report() {
    // They collide under a unique index -- measured: building one over a pair
    // of them fails with `DuplicateKey` and `keyValue: {"run_id": null}` -- so
    // they are duplicates the measurement must show. Decoding them as an error
    // used to abort the whole pass, taking the ordinary duplicates found
    // alongside them with it.
    let collection = runs("null_keys").await;
    for document in [
        doc! { "other": 1 },
        doc! { "run_id": Bson::Null },
        doc! { "run_id": "real" },
        doc! { "run_id": "real" },
    ] {
        if let Err(error) = collection.insert_one(document).await {
            panic!("could not seed: {error}");
        }
    }

    let Ok(found) = duplicate_run_ids(&collection).await else {
        panic!("a null group must not abort the report");
    };
    assert!(
        found.contains(&Duplicate {
            run_id: RunKey::Null,
            count: 2,
        }),
        "the null group is reported: {found:?}"
    );
    assert!(
        found.contains(&Duplicate {
            run_id: RunKey::Id("real".to_owned()),
            count: 2,
        }),
        "and so is the ordinary duplicate alongside it: {found:?}"
    );
}
