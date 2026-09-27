//! Drives the run archive against a real MongoDB.
//!
//! `$merge` is the whole design, and its behaviour is the part worth checking
//! against a server rather than a mock: whether it creates the target, whether
//! a second pass duplicates, whether the merge pipeline refreshes the archived
//! payload while holding `archived_at` where the first pass put it, and whether
//! it adopts a copy the original janitor left unstamped. A fake that agreed with
//! my reading of the manual would prove nothing — and four separate properties
//! here turned out other than expected when actually run.
//!
//! Requires `PNEUMA_TEST_MONGO_URL`, and fails rather than skips without it,
//! for the same reason `tests/barrier.rs` does.
//!
//! ```sh
//! docker run -d --name pn-mongo -p 27018:27017 mongo:5.0.28
//! PNEUMA_TEST_MONGO_URL=mongodb://127.0.0.1:27018 \
//!     cargo test -p pneuma-store --test history
//! ```

use chrono::{Duration, Utc};
use mongodb::bson::oid::ObjectId;
use mongodb::bson::{doc, Document};
use mongodb::{Client, Collection};
use pneuma_store::{retention_cutoff, BarrierError, RunHistoryStore, ARCHIVED_AT};

/// A fresh database per test, so a failure leaves evidence and two runs of the
/// suite cannot collide.
async fn collections(name: &str) -> (Collection<Document>, Collection<Document>) {
    let Ok(url) = std::env::var("PNEUMA_TEST_MONGO_URL") else {
        panic!("PNEUMA_TEST_MONGO_URL is not set; see the header of this file");
    };
    let Ok(client) = Client::with_uri_str(&url).await else {
        panic!("should connect to {url}");
    };
    let db = client.database(&format!("pneuma_history_{name}"));
    if db.drop().await.is_err() {
        panic!("should drop the test database");
    }
    (db.collection("runs"), db.collection("run_history"))
}

fn run(run_id: &str, status: &str) -> Document {
    doc! { "run_id": run_id, "status": status, "step_output": { "big": "payload" } }
}

#[tokio::test]
async fn archiving_copies_runs_and_is_safe_to_repeat() {
    let (runs, history) = collections("repeat").await;
    let Ok(_) = runs
        .insert_many(vec![run("run-1", "finished"), run("run-2", "cancelled")])
        .await
    else {
        panic!("should seed");
    };
    let Ok(store) = RunHistoryStore::new(runs.clone(), history.clone()) else {
        panic!("distinct collections should be accepted");
    };
    let ids = vec!["run-1".to_owned(), "run-2".to_owned()];

    let Ok(()) = store.archive(&ids).await else {
        panic!("should archive");
    };
    let Ok(after_first) = history.count_documents(doc! {}).await else {
        panic!("should count");
    };
    assert_eq!(after_first, 2, "both runs were copied");

    let Ok(Some(first)) = history.find_one(doc! { "run_id": "run-1" }).await else {
        panic!("run-1 should be in history");
    };
    let Some(stamped) = first.get(ARCHIVED_AT).cloned() else {
        panic!("archive stamps {ARCHIVED_AT}");
    };

    // The retry that a plain insert cannot survive -- the defect notes
    // §17 is exactly this failure on the Postgres side.
    let Ok(()) = store.archive(&ids).await else {
        panic!("archiving twice should succeed");
    };
    let Ok(after_second) = history.count_documents(doc! {}).await else {
        panic!("should count");
    };
    assert_eq!(after_second, 2, "and did not duplicate");

    let Ok(Some(again)) = history.find_one(doc! { "run_id": "run-1" }).await else {
        panic!("still there");
    };
    assert_eq!(
        again.get(ARCHIVED_AT),
        Some(&stamped),
        "re-archiving does not push {ARCHIVED_AT} forward, which would make a \
         run archived every pass never expire"
    );

    // A late write reaches history on the next pass, and does not restart the
    // retention clock. This is route C in the defect notes: a cancelled
    // run whose in-flight nodes keep landing results, where
    // `update_step_status` and the barrier writes carry no terminal guard. If a
    // pass archives and then fails before deleting, the original's
    // `$set: run` upsert captured those writes on the retry -- so this must
    // too, or the crate silently keeps a poorer record than the code it
    // replaces.
    let Ok(_) = runs
        .update_one(
            doc! { "run_id": "run-1" },
            doc! { "$set": { "status": "cancelled" } },
        )
        .await
    else {
        panic!("should update the live run");
    };
    let Ok(()) = store.archive(&ids).await else {
        panic!("should archive again");
    };
    let Ok(Some(refreshed)) = history.find_one(doc! { "run_id": "run-1" }).await else {
        panic!("still there");
    };
    assert_eq!(
        refreshed.get_str("status").ok(),
        Some("cancelled"),
        "the late write reached history"
    );
    assert_eq!(
        refreshed.get(ARCHIVED_AT),
        Some(&stamped),
        "and the retention clock did not restart"
    );

    // The live documents are untouched until the caller says so.
    let Ok(live) = runs.count_documents(doc! {}).await else {
        panic!("should count");
    };
    assert_eq!(live, 2, "archiving does not delete");
}

#[tokio::test]
async fn archiving_copies_only_what_was_named() {
    let (runs, history) = collections("subset").await;
    let Ok(_) = runs
        .insert_many(vec![
            run("keep-me", "processing"),
            run("archive-me", "finished"),
        ])
        .await
    else {
        panic!("should seed");
    };
    let Ok(store) = RunHistoryStore::new(runs.clone(), history.clone()) else {
        panic!("distinct collections should be accepted");
    };

    let Ok(()) = store.archive(&["archive-me".to_owned()]).await else {
        panic!("should archive");
    };
    let Ok(copied) = history.count_documents(doc! {}).await else {
        panic!("should count");
    };
    assert_eq!(copied, 1, "only the named run");
    let Ok(wrong) = history.count_documents(doc! { "run_id": "keep-me" }).await else {
        panic!("should count");
    };
    assert_eq!(wrong, 0, "the unnamed run stayed out of history");

    // An empty batch is a no-op, not "everything".
    let Ok(()) = store.archive(&[]).await else {
        panic!("an empty archive should succeed");
    };
    let Ok(unchanged) = history.count_documents(doc! {}).await else {
        panic!("should count");
    };
    assert_eq!(unchanged, 1, "an empty slice copied nothing");
}

#[tokio::test]
async fn deleting_archived_removes_only_the_named_runs() {
    let (runs, history) = collections("delete").await;
    let Ok(_) = runs
        .insert_many(vec![run("gone", "finished"), run("stays", "processing")])
        .await
    else {
        panic!("should seed");
    };
    let Ok(store) = RunHistoryStore::new(runs.clone(), history.clone()) else {
        panic!("distinct collections should be accepted");
    };

    let Ok(none) = store.delete_archived(&[]).await else {
        panic!("an empty delete should succeed");
    };
    assert_eq!(none, 0, "an empty slice deleted nothing");

    let Ok(deleted) = store.delete_archived(&["gone".to_owned()]).await else {
        panic!("should delete");
    };
    assert_eq!(deleted, 1);
    let Ok(left) = runs.count_documents(doc! {}).await else {
        panic!("should count");
    };
    assert_eq!(left, 1, "the other run is untouched");
    let Ok(still) = runs.count_documents(doc! { "run_id": "stays" }).await else {
        panic!("should count");
    };
    assert_eq!(still, 1, "and it is the right one");
}

#[tokio::test]
async fn retention_expires_by_when_it_was_archived() {
    let (runs, history) = collections("retention").await;
    let Ok(_) = runs.insert_many(vec![run("old", "finished")]).await else {
        panic!("should seed");
    };
    let Ok(store) = RunHistoryStore::new(runs.clone(), history.clone()) else {
        panic!("distinct collections should be accepted");
    };
    let Ok(()) = store.archive(&["old".to_owned()]).await else {
        panic!("should archive");
    };

    // A cutoff before the archive keeps it: the document is younger than the
    // window, whatever the run's own age.
    let Ok(kept) = store.delete_outdated(Utc::now() - Duration::hours(1)).await else {
        panic!("should run");
    };
    assert_eq!(kept, 0, "not yet outdated");
    let Ok(present) = history.count_documents(doc! {}).await else {
        panic!("should count");
    };
    assert_eq!(present, 1);

    // A cutoff after it expires it.
    let Ok(removed) = store.delete_outdated(Utc::now() + Duration::hours(1)).await else {
        panic!("should run");
    };
    assert_eq!(removed, 1, "outdated by the later cutoff");
    let Ok(empty) = history.count_documents(doc! {}).await else {
        panic!("should count");
    };
    assert_eq!(empty, 0);
}

#[tokio::test]
async fn retention_leaves_a_document_whose_age_cannot_be_known() {
    // No `archived_at`, and an `_id` that is not an ObjectId, so there is
    // nothing to infer an age from. It is left alone rather than guessed at.
    //
    // Two things hold it there, and only one is the `$type` guard. Query
    // comparison in MongoDB is type-bracketed, so `$lt` against an ObjectId
    // matches ObjectIds and nothing else on its own. An earlier version of this
    // comment claimed the opposite -- reasoning from BSON's *sort* order, which
    // really does place strings below ObjectIds and is a different rule. The
    // assertion below is right either way; the explanation was not.
    let (runs, history) = collections("unknown_age").await;
    let Ok(_) = history
        .insert_many(vec![doc! {
            "_id": "a-string-id",
            "run_id": "hand-written",
            "status": "finished",
        }])
        .await
    else {
        panic!("should seed");
    };
    let Ok(store) = RunHistoryStore::new(runs, history.clone()) else {
        panic!("distinct collections should be accepted");
    };

    let Ok(removed) = store
        .delete_outdated(Utc::now() + Duration::days(365))
        .await
    else {
        panic!("should run");
    };
    assert_eq!(removed, 0, "an age that cannot be known is not expired");
    let Ok(present) = history.count_documents(doc! {}).await else {
        panic!("should count");
    };
    assert_eq!(present, 1);
}

#[tokio::test]
async fn archiving_adopts_a_copy_the_reference_janitor_left_unstamped() {
    // The cutover case, and the one an earlier version of this module lost.
    // The original janitor archives a run verbatim -- no `archived_at` -- and
    // dies before deleting it from `runs`. This janitor then picks the run up
    // and re-archives it. With `whenMatched: "keepExisting"` the existing copy
    // was kept *including its missing stamp*, so `delete_outdated`'s legacy
    // branch expired it by its `_id` on the very pass that adopted it. For a
    // run older than the window that is total loss of exactly the record
    // the defect notes exist to protect.
    let (runs, history) = collections("adopt").await;

    // An id minted two hours ago, as a long-lived run's would be.
    let old = Utc::now() - Duration::hours(2);
    let Ok(seconds) = u32::try_from(old.timestamp()) else {
        panic!("the epoch fits");
    };
    let mut bytes = [0_u8; 12];
    bytes[..4].copy_from_slice(&seconds.to_be_bytes());
    bytes[11] = 7;
    let id = ObjectId::from_bytes(bytes);

    let Ok(_) = runs
        .insert_many(vec![doc! {
            "_id": id, "run_id": "left-over", "status": "finished", "state": "current",
        }])
        .await
    else {
        panic!("should seed the live run");
    };
    // What the original janitor wrote: verbatim, unstamped, and slightly older.
    let Ok(_) = history
        .insert_many(vec![doc! {
            "_id": id, "run_id": "left-over", "status": "finished", "state": "as-archived",
        }])
        .await
    else {
        panic!("should seed the legacy copy");
    };

    let Ok(store) = RunHistoryStore::new(runs, history.clone()) else {
        panic!("distinct collections should be accepted");
    };
    let Ok(()) = store.archive(&["left-over".to_owned()]).await else {
        panic!("should archive");
    };

    let Ok(Some(adopted)) = history.find_one(doc! { "run_id": "left-over" }).await else {
        panic!("still there");
    };
    assert!(
        adopted.get(ARCHIVED_AT).is_some(),
        "the legacy copy is stamped as it is adopted"
    );
    assert_eq!(
        adopted.get_str("state").ok(),
        Some("current"),
        "and the payload is refreshed from the live run, as the original's \
         `$set: run` upsert did -- a late write landing after an earlier pass \
         archived the run reaches history on the next one"
    );
    assert!(
        adopted.keys().all(|key| !key.starts_with("__pneuma")),
        "and the pipeline's scratch field never reaches a stored document: {:?}",
        adopted.keys().collect::<Vec<_>>()
    );

    // The point of the stamp: it is now judged by when it was archived, not by
    // the run's two-hour-old id, so a window that would have expired it on
    // `_id` keeps it.
    let Ok(kept) = store.delete_outdated(Utc::now() - Duration::hours(1)).await else {
        panic!("should run");
    };
    assert_eq!(kept, 0, "not expired on the pass that adopted it");
    let Ok(present) = history.count_documents(doc! {}).await else {
        panic!("should count");
    };
    assert_eq!(present, 1);
}

#[tokio::test]
async fn archiving_targets_the_history_collection_own_database() {
    // `$merge` resolves a bare `into` string against the *aggregation's*
    // database. Naming only the collection therefore archives into the source
    // database while retention reads the intended one -- history that grows
    // where nothing looks and never expires, with no error anywhere. Verified
    // against the server: with the unqualified form the copy lands beside
    // `runs` and the intended target stays empty.
    let Ok(url) = std::env::var("PNEUMA_TEST_MONGO_URL") else {
        panic!("PNEUMA_TEST_MONGO_URL is not set; see the header of this file");
    };
    let Ok(client) = Client::with_uri_str(&url).await else {
        panic!("should connect to {url}");
    };
    let source = client.database("pneuma_history_split_src");
    let archive_db = client.database("pneuma_history_split_dst");
    for db in [&source, &archive_db] {
        if db.drop().await.is_err() {
            panic!("should drop the test database");
        }
    }
    let runs: Collection<Document> = source.collection("runs");
    let history: Collection<Document> = archive_db.collection("run_history");
    let Ok(_) = runs.insert_many(vec![run("split", "finished")]).await else {
        panic!("should seed");
    };

    let Ok(store) = RunHistoryStore::new(runs, history.clone()) else {
        panic!("distinct collections should be accepted");
    };
    let Ok(()) = store.archive(&["split".to_owned()]).await else {
        panic!("should archive");
    };

    let Ok(landed) = history.count_documents(doc! {}).await else {
        panic!("should count");
    };
    assert_eq!(
        landed, 1,
        "the copy went to the history collection's database"
    );
    let stray: Collection<Document> = source.collection("run_history");
    let Ok(beside_source) = stray.count_documents(doc! {}).await else {
        panic!("should count");
    };
    assert_eq!(
        beside_source, 0,
        "and not to a same-named collection beside the source"
    );
}

#[tokio::test]
async fn a_store_cannot_be_built_from_one_collection_twice() {
    // `$merge` into the collection being aggregated is accepted by the server,
    // so `archive` would report success while nothing reached any archive --
    // and a caller honouring the documented ordering would then delete every
    // finished run on the strength of it.
    //
    // Measured under the current pipeline, not carried over: the self-merge
    // *writes*, stamping `archived_at` onto the live `runs` documents. An
    // earlier version of this comment said it wrote nothing, which was true of
    // `keepExisting` and understates the damage -- a reader trusting it would
    // think removing this guard yields a harmless no-op.
    let (runs, _) = collections("same").await;
    assert!(matches!(
        RunHistoryStore::new(runs.clone(), runs.clone()),
        Err(BarrierError::SameCollection { .. })
    ));
}

#[tokio::test]
async fn retention_expires_documents_the_original_janitor_wrote() {
    // Every document the original janitor put in `run_history` is a verbatim copy
    // of the run, with no `archived_at`. Filtering on the stamp alone would
    // make the whole pre-existing archive permanently unexpirable on cutover --
    // in the collection retention exists to bound, holding documents that carry
    // both payloads. A legacy document is expired the way the original expired
    // it, by its `ObjectId`'s own timestamp.
    let (runs, history) = collections("legacy").await;

    // An id minted "two hours ago", as the original's `ObjectId.from_datetime`
    // comparison would see it.
    let old = Utc::now() - Duration::hours(2);
    let Ok(seconds) = u32::try_from(old.timestamp()) else {
        panic!("the epoch fits");
    };
    let mut bytes = [0_u8; 12];
    bytes[..4].copy_from_slice(&seconds.to_be_bytes());
    bytes[11] = 1;
    let Ok(_) = history
        .insert_many(vec![doc! {
            "_id": ObjectId::from_bytes(bytes),
            "run_id": "written-by-reference",
            "status": "finished",
        }])
        .await
    else {
        panic!("should seed");
    };

    let Ok(store) = RunHistoryStore::new(runs, history.clone()) else {
        panic!("distinct collections should be accepted");
    };

    let Ok(kept) = store.delete_outdated(Utc::now() - Duration::hours(3)).await else {
        panic!("should run");
    };
    assert_eq!(kept, 0, "younger than the window by its own id");

    let Ok(removed) = store.delete_outdated(Utc::now() - Duration::hours(1)).await else {
        panic!("should run");
    };
    assert_eq!(removed, 1, "and expired once the window passes it");
}

#[test]
fn a_non_positive_retention_window_means_do_not_delete() {
    // The original returns early with a warning for `retention_days <= 0`.
    // The obvious call-site arithmetic turns `0`
    // into `now`, which would delete the entire archive -- so the setting that
    // disabled expiry would have become the one that empties it.
    assert!(retention_cutoff(0).is_none());
    assert!(retention_cutoff(-1).is_none());
    // And the far end, which has two limits and needs a case for each.
    // Measured against the pinned chrono: `try_days` fails above
    // 106,751,991,167 days, but subtracting from `Utc::now()` fails from
    // 96,485,986 days -- so a value between them exercises
    // `checked_sub_signed` and nothing else does. `i64::MAX` is rejected by
    // `try_days` alone, which is how a "simplification" back to
    // `map(|w| Utc::now() - w)` could pass a suite that tests only that.
    assert!(retention_cutoff(i64::MAX).is_none(), "rejected by try_days");
    assert!(
        retention_cutoff(1_000_000_000).is_none(),
        "in the band where try_days succeeds and the subtraction does not"
    );
    assert!(
        retention_cutoff(10_000_000).is_some(),
        "absurd but representable"
    );
    let Some(cutoff) = retention_cutoff(14) else {
        panic!("a positive window has a cutoff");
    };
    let age = Utc::now() - cutoff;
    assert!(
        age >= Duration::days(14) && age < Duration::days(15),
        "fourteen days back, not fifteen or none: {age}"
    );
}

#[tokio::test]
async fn a_store_reports_the_collections_it_was_wired_to() {
    // For a caller composing several stores and needing to check they agree --
    // `pneuma_janitor::Janitor::new` refuses a `RunStore` and a
    // `RunHistoryStore` reading different run collections, because selection
    // goes through one and both the archive and the delete through the other,
    // so a typo leaves every pass reporting runs it never moved.
    let (runs, history) = collections("namespaces").await;
    let Ok(store) = RunHistoryStore::new(runs.clone(), history.clone()) else {
        panic!("distinct collections should be accepted");
    };
    assert_eq!(
        store.source_namespace(),
        runs.namespace(),
        "the archive reads the collection it was given"
    );

    let run_store = pneuma_store::RunStore::new(runs.clone());
    assert_eq!(run_store.namespace(), runs.namespace());
    assert_ne!(
        run_store.namespace(),
        history.namespace(),
        "and the two are distinguishable, which is the whole point"
    );
}

#[tokio::test]
async fn counting_outdated_agrees_with_deleting_it() {
    // The Mongo half shares one filter between the count and the delete, so
    // they cannot drift -- this asserts the sharing actually holds, including
    // over the legacy `_id` branch, which is where a preview built from a
    // separate copy of the rule would most plausibly disagree.
    let (runs, history) = collections("count").await;
    let Ok(_) = runs.insert_many(vec![run("r1", "finished")]).await else {
        panic!("should seed");
    };
    let Ok(store) = RunHistoryStore::new(runs, history.clone()) else {
        panic!("distinct collections should be accepted");
    };
    let Ok(()) = store.archive(&["r1".to_owned()]).await else {
        panic!("should archive");
    };
    // Plus one the original janitor would have left: unstamped, judged by `_id`.
    let old = Utc::now() - Duration::hours(2);
    let Ok(seconds) = u32::try_from(old.timestamp()) else {
        panic!("the epoch fits");
    };
    let mut bytes = [0_u8; 12];
    bytes[..4].copy_from_slice(&seconds.to_be_bytes());
    bytes[11] = 3;
    let Ok(_) = history
        .insert_many(vec![
            doc! { "_id": ObjectId::from_bytes(bytes), "run_id": "legacy" },
        ])
        .await
    else {
        panic!("should seed the legacy copy");
    };

    for cutoff in [
        Utc::now() - Duration::hours(1),
        Utc::now() + Duration::hours(1),
    ] {
        let Ok(counted) = store.count_outdated(cutoff).await else {
            panic!("should count");
        };
        let Ok(deleted) = store.delete_outdated(cutoff).await else {
            panic!("should delete");
        };
        assert_eq!(counted, deleted, "count and delete disagree at {cutoff}");
    }
    let Ok(left) = history.count_documents(doc! {}).await else {
        panic!("should count");
    };
    assert_eq!(left, 0, "and between them they removed both");
}
