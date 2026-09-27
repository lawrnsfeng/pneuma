//! Drives every [`NodeRunStore`] method against a real Postgres.
//!
//! Requires `PNEUMA_TEST_DATABASE_URL`, and fails rather than skips without it,
//! for the reason given in `schema.rs`.
//!
//! One test, one runtime, one schema — see `schema.rs` for why three separate
//! `#[tokio::test]`s do not work here.

use chrono::{Duration, Utc};
use pneuma_core::node::NodeKind;
use pneuma_core::status::NodeStatus;
use pneuma_store::{NewNodeRun, NodeRunStore};
use sqlx::{Executor, PgPool};
use uuid::Uuid;

async fn fresh() -> PgPool {
    let Ok(url) = std::env::var("PNEUMA_TEST_DATABASE_URL") else {
        panic!("PNEUMA_TEST_DATABASE_URL is not set; see tests/schema.rs");
    };
    let Ok(pool) = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .after_connect(|conn, _| {
            Box::pin(async move {
                conn.execute("SET search_path TO pneuma_test_store").await?;
                Ok(())
            })
        })
        .connect(&url)
        .await
    else {
        panic!("could not connect to {url}");
    };
    // Each test binary gets its own Postgres schema. Both binaries create the
    // same tables in the same database, and they pass today only because
    // `cargo test` runs test targets one at a time -- under a runner that
    // parallelises them (`cargo nextest`), one would drop the tables out from
    // under the other. That is the same failure this file's header records for
    // parallel tests, reintroduced at target granularity.
    for statement in [
        "DROP SCHEMA IF EXISTS pneuma_test_store CASCADE",
        "CREATE SCHEMA pneuma_test_store",
        "SET search_path TO pneuma_test_store",
    ] {
        if let Err(error) = pool.execute(statement).await {
            panic!("could not isolate the test schema: {error}");
        }
    }

    let Ok(mut connection) = pool.acquire().await else {
        panic!("could not acquire a connection to migrate on");
    };
    if let Err(error) = pneuma_store::migrator().run(&mut *connection).await {
        panic!("the migrations do not apply to a real Postgres: {error}");
    }
    drop(connection);
    pool
}

/// Explicit ids, so the `v4` feature (and its `rand` dependency) is not needed
/// just for tests.
///
/// The first attempt derived one from the path with a rolling hash and then
/// `| 1` to avoid the nil UUID. That maps consecutive values onto each other,
/// so `run1.c1` and `run1.c2` -- whose hashes differ by one -- collided on the
/// primary key. Counting is not worth being clever about.
fn node_with_id(n: u128, path: &str, run_id: &str) -> NewNodeRun {
    NewNodeRun {
        id: Uuid::from_u128(n),
        path: path.to_owned(),
        node_id: path.to_owned(),
        name: "a node".to_owned(),
        kind: NodeKind::Model,
        pipeline_id: "p.q.r".to_owned(),
        run_id: run_id.to_owned(),
        parent_id: None,
        parent_path: None,
        parent_kind: None,
        child_index: None,
        sibling_index: None,
        status: NodeStatus::Created,
        step_input: Some(serde_json::json!({"page": 1})),
        step_output: None,
    }
}

fn node(path: &str, run_id: &str) -> NewNodeRun {
    node_with_id(1, path, run_id)
}

#[tokio::test]
async fn every_store_method_works_against_postgres() {
    let pool = fresh().await;
    let store = NodeRunStore::new(pool.clone());

    // --- create, and the redelivery fallback ------------------------------
    let first = node("run1.a", "run1");
    let created = match store.create(&first).await {
        Ok(row) => row,
        Err(error) => panic!("create failed: {error}"),
    };
    assert_eq!(created.path, "run1.a");
    assert_eq!(created.status, NodeStatus::Created);
    assert_eq!(created.step_input, Some(serde_json::json!({"page": 1})));

    // A redelivery: ON CONFLICT returns nothing, so create falls back to
    // looking the existing row up rather than raising.
    let mut again = node("run1.a", "run1");
    again.id = Uuid::from_u128(999);
    let same = match store.create(&again).await {
        Ok(row) => row,
        Err(error) => panic!("a repeated create should return the existing row: {error}"),
    };
    assert_eq!(same.id, created.id, "the original row, not the new id");

    // --- get_by_path ------------------------------------------------------
    let Ok(Some(found)) = store.get_by_path("run1.a").await else {
        panic!("should find it");
    };
    assert_eq!(found.id, created.id);
    let Ok(missing) = store.get_by_path("nope").await else {
        panic!("a miss is Ok(None), not an error");
    };
    assert!(missing.is_none());

    // --- children, and that the filters filter ----------------------------
    for (id, path, node_id, idx) in [
        (2u128, "run1.c1", "wanted", 1u32),
        (3, "run1.c2", "other", 2),
    ] {
        let mut child = node_with_id(id, path, "run1");
        child.node_id = node_id.to_owned();
        child.parent_path = Some("run1.a".to_owned());
        let Ok(index) = pneuma_core::child_index::ChildIndex::new(idx) else {
            panic!("{idx} should be a valid child index");
        };
        child.child_index = Some(index);
        if let Err(error) = store.create(&child).await {
            panic!("could not create {path}: {error}");
        }
    }
    let Ok(all_children) = store.get_by_parent_path("run1.a", None, None).await else {
        panic!("children query failed");
    };
    assert_eq!(all_children.len(), 2);
    assert_eq!(
        all_children[0]
            .child_index
            .map(pneuma_core::child_index::ChildIndex::get),
        Some(1),
        "ordered by child_index"
    );

    let wanted = vec!["wanted".to_owned()];
    let Ok(filtered) = store
        .get_by_parent_path("run1.a", None, Some(&wanted))
        .await
    else {
        panic!("filtered children query failed");
    };
    assert_eq!(filtered.len(), 1, "node_ids restricts the set");

    let Ok(by_one) = store
        .get_by_parent_path("run1.a", Some("other"), None)
        .await
    else {
        panic!("node_id filter failed");
    };
    assert_eq!(by_one.len(), 1, "node_id restricts the set");

    // --- get_by_paths / get_by_run_id -------------------------------------
    let paths = vec!["run1.a".to_owned(), "run1.c1".to_owned()];
    let Ok(some) = store.get_by_paths(&paths).await else {
        panic!("get_by_paths failed");
    };
    assert_eq!(some.len(), 2);
    let Ok(whole_run) = store.get_by_run_id("run1").await else {
        panic!("get_by_run_id failed");
    };
    assert_eq!(whole_run.len(), 3);

    // --- update_status, including the guard --------------------------------
    let Ok(Some(moved)) = store
        .update_status("run1.a", NodeStatus::Processing, None, None)
        .await
    else {
        panic!("an allowed transition should return the row");
    };
    assert_eq!(moved.status, NodeStatus::Processing);
    assert!(moved.started_at.is_some(), "started_at is stamped");

    let Ok(refused) = store
        .update_status("run1.a", NodeStatus::Forked, None, None)
        .await
    else {
        panic!("a refused transition is Ok(None), not an error");
    };
    assert!(refused.is_none(), "FORKED only from CREATED");

    let Ok(Some(failed)) = store
        .update_status("run1.c1", NodeStatus::Error, Some("E1"), Some("boom"))
        .await
    else {
        panic!("error transition failed");
    };
    assert_eq!(failed.error_code.as_deref(), Some("E1"));
    assert_eq!(failed.error_message.as_deref(), Some("boom"));

    // Now terminal: the guard refuses anything further.
    let Ok(after_terminal) = store
        .update_status("run1.c1", NodeStatus::Processing, None, None)
        .await
    else {
        panic!("should not error");
    };
    assert!(after_terminal.is_none(), "a finalised row never moves");

    let Ok(no_such) = store
        .update_status("absent", NodeStatus::Processing, None, None)
        .await
    else {
        panic!("should not error");
    };
    assert!(no_such.is_none());

    // --- record_output ------------------------------------------------------
    let output = serde_json::json!({"pages": [1, 2]});
    let Ok(Some(recorded)) = store
        .record_output("run1.c2", NodeStatus::Finished, Some(&output))
        .await
    else {
        panic!("record_output should return the row");
    };
    assert_eq!(recorded.step_output, Some(output));
    assert_eq!(recorded.status, NodeStatus::Finished);

    // --- staleness ----------------------------------------------------------
    let Ok(fresh_runs) = store
        .stale_inprogress_run_ids(Utc::now() - Duration::days(1), 10)
        .await
    else {
        panic!("stale query failed");
    };
    assert!(fresh_runs.is_empty(), "nothing is a day old yet");

    let Ok(stale_now) = store.stale_inprogress_run_ids(Utc::now(), 10).await else {
        panic!("stale query failed");
    };
    assert_eq!(
        stale_now,
        vec!["run1".to_owned()],
        "run1 still has open nodes"
    );

    // --- max_updated_at ------------------------------------------------------
    let Ok(empty) = store.max_updated_at_by_run_ids(&[]).await else {
        panic!("empty input is answered without a query");
    };
    assert!(empty.is_empty());
    let runs = vec!["run1".to_owned()];
    let Ok(maxima) = store.max_updated_at_by_run_ids(&runs).await else {
        panic!("max_updated_at failed");
    };
    assert_eq!(maxima.len(), 1);
    assert_eq!(maxima[0].0, "run1");

    // --- history: backup is idempotent, delete honours the cutoff ------------
    let Ok(backed_up) = store.backup_to_history(&runs).await else {
        panic!("backup failed");
    };
    assert_eq!(backed_up, 3);
    let Ok(again_backed) = store.backup_to_history(&runs).await else {
        panic!("a repeated backup must be a no-op, not a key violation");
    };
    assert_eq!(again_backed, 0);
    let Ok(none_backed) = store.backup_to_history(&[]).await else {
        panic!("empty input is answered without a query");
    };
    assert_eq!(none_backed, 0);

    let Ok(kept) = store
        .delete_outdated_history(Utc::now() - Duration::days(1))
        .await
    else {
        panic!("delete failed");
    };
    assert_eq!(kept, 0, "nothing is a day old");
    let Ok(swept) = store.delete_outdated_history(Utc::now()).await else {
        panic!("delete failed");
    };
    assert_eq!(swept, 3, "everything is older than now");

    // --- delete ---------------------------------------------------------------
    let Ok(nothing) = store.delete_by_run_ids(&[]).await else {
        panic!("empty input is answered without a query");
    };
    assert_eq!(nothing, 0);
    let Ok(gone) = store.delete_by_run_ids(&runs).await else {
        panic!("delete failed");
    };
    assert_eq!(gone, 3, "parent and children go in one statement");
    let Ok(left) = store.get_by_run_id("run1").await else {
        panic!("query failed");
    };
    assert!(left.is_empty());

    // --- the embedded migrations ------------------------------------------
    // `baseline` compares a live database against what these produce and then
    // writes rows claiming they ran, so the two must come from one source.
    // Asserting the versions here means a migration added without a matching
    // expectation is caught in this crate rather than in `pneuma-migrate`.
    let embedded: Vec<i64> = pneuma_store::migrator()
        .iter()
        .map(|migration| migration.version)
        .collect();
    assert_eq!(
        embedded,
        vec![1, 2, 3, 4],
        "0001_noderun, 0002_noderun_history, 0003_submission and 0004_rename"
    );

    // And the split between them, which is what `baseline` acts on. `0001` and
    // `0002` transcribe the original migration tool revisions, so a production database has both;
    // `0003` creates `submission`, which the original migration tool never made and no existing
    // database has, and `0004` renames what `0001` and `0002` created.
    // Baselining against all four would refuse a database that is in fact
    // correct -- or, if the comparison were loosened, record `0003` as applied
    // against a database with no `submission` table, so `sqlx migrate run`
    // would skip creating it and every submission query would fail at run time
    // on a deployment whose baseline reported success. `0004` is the same
    // argument from the other end: an original-era database has the *old* names,
    // so it can only match an expectation built from `0001` and `0002` alone.
    let adoptable: Vec<i64> = pneuma_store::original_migrator()
        .iter()
        .map(|migration| migration.version)
        .collect();
    assert_eq!(
        adoptable,
        vec![1, 2],
        "only what the original migration tool already created"
    );
    assert!(
        adoptable
            .iter()
            .all(|version| *version <= pneuma_store::ORIGINAL_THROUGH),
        "and the constant is what says where that ends"
    );
    assert!(
        embedded.len() > adoptable.len(),
        "a migration after the original migration tool line is the case this split exists for; \
         without one the two lists agree and this proves nothing"
    );
    assert!(
        pneuma_store::migrator()
            .iter()
            .all(|migration| !migration.checksum.is_empty()),
        "each carries the checksum `baseline` will record"
    );

    // --- the preview surface, folded in rather than given its own test.
    // This binary runs one test on purpose: the file's header records that
    // two would race on the shared schema, and adding a second proved it.
    // Two separate statements, so nothing but this stops their `WHERE` clauses
    // drifting -- and a `--dry-run` reporting from a drifted predicate is worse
    // than none, because it is confidently wrong about what a real pass would
    // do. The row's own timestamp is the cutoff that matters: cutoffs away from
    // the data agree under either `<` or `<=`.
    let Ok(_) = store.create(&node("agree-path", "agree-run")).await else {
        panic!("should create");
    };
    let Ok(copied) = store.backup_to_history(&["agree-run".to_owned()]).await else {
        panic!("should back up");
    };
    assert_eq!(copied, 1);

    let Ok(created_at): Result<chrono::DateTime<chrono::Utc>, _> =
        sqlx::query_scalar("SELECT created_at FROM node_run_history LIMIT 1")
            .fetch_one(&pool)
            .await
    else {
        panic!("the backup should have written a row");
    };

    for cutoff in [
        created_at,
        created_at + chrono::Duration::microseconds(1),
        chrono::Utc::now() + chrono::Duration::days(1),
    ] {
        let Ok(counted) = store.count_outdated_history(cutoff).await else {
            panic!("should count");
        };
        let Ok(deleted) = store.delete_outdated_history(cutoff).await else {
            panic!("should delete");
        };
        assert_eq!(counted, deleted, "the two predicates disagree at {cutoff}");
    }
}
