//! Drives the janitor's passes against both real databases.
//!
//! This crate is sequencing, and sequencing is the part a fake cannot check.
//! What matters is that the archive exists *before* the delete, and that a pass
//! which fails part-way can be repeated — properties of two servers agreeing,
//! not of a mock agreeing with itself.
//!
//! The defect notes are the reason the second one is tested here rather
//! than trusted: the original's node_run backup is a plain `INSERT`, so a run
//! already copied makes the retry a primary-key violation, which aborts before
//! the delete and re-selects the same runs for ever. One transient failure
//! stops cleanup permanently. `archive_survives_a_history_row_that_already_exists`
//! is that scenario end to end.
//!
//! Requires both databases, and fails rather than skips without them:
//!
//! ```sh
//! docker run -d --name pn-pg    -p 5433:5432 -e POSTGRES_PASSWORD=pneuma postgres:16
//! docker run -d --name pn-mongo -p 27018:27017 mongo:5.0.28
//! PNEUMA_TEST_DATABASE_URL=postgres://postgres:pneuma@127.0.0.1:5433/postgres \
//! PNEUMA_TEST_MONGO_URL=mongodb://127.0.0.1:27018 \
//!     cargo test -p pneuma-janitor
//! ```

use chrono::{Duration, Utc};
use mongodb::bson::{doc, Document};
use mongodb::{Client, Collection};
use pneuma_janitor::{Janitor, Settings};
use pneuma_store::{NodeRunStore, RunHistoryStore, RunStore};
use sqlx::{Executor, PgPool};

/// Both stores, emptied, in a schema and database of this binary's own.
///
/// Its own Postgres schema because `pneuma-store`'s tests create the same
/// tables in the same database, and its own Mongo database per test for the
/// same reason.
async fn fresh(name: &str) -> (Janitor, PgPool, Collection<Document>, Collection<Document>) {
    let Ok(pg_url) = std::env::var("PNEUMA_TEST_DATABASE_URL") else {
        panic!("PNEUMA_TEST_DATABASE_URL is not set; see the header of this file");
    };
    let Ok(mongo_url) = std::env::var("PNEUMA_TEST_MONGO_URL") else {
        panic!("PNEUMA_TEST_MONGO_URL is not set; see the header of this file");
    };

    // A Postgres schema per *test*, not per binary. `cargo test` runs the tests
    // in one binary concurrently, and the migration creates types as well as
    // tables -- so a shared schema means two tests racing on `CREATE TYPE` and
    // one losing to a duplicate-key error on `pg_type`. `pneuma-store`'s suite
    // avoids this by keeping one test per binary; per-test schemas scale
    // better and match what this file already does with Mongo databases.
    let schema = format!("pneuma_janitor_{name}");
    let search_path = format!("SET search_path TO {schema}");
    let Ok(pool) = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .after_connect({
            let search_path = search_path.clone();
            move |conn, _| {
                let search_path = search_path.clone();
                Box::pin(async move {
                    conn.execute(search_path.as_str()).await?;
                    Ok(())
                })
            }
        })
        .connect(&pg_url)
        .await
    else {
        panic!("could not connect to {pg_url}");
    };
    for statement in [
        format!("DROP SCHEMA IF EXISTS {schema} CASCADE"),
        format!("CREATE SCHEMA {schema}"),
        search_path,
    ] {
        if let Err(error) = pool.execute(statement.as_str()).await {
            panic!("setup failed on {statement:?}: {error}");
        }
    }
    let Ok(mut connection) = pool.acquire().await else {
        panic!("could not acquire a connection to migrate on");
    };
    if let Err(error) = pneuma_store::migrator().run(&mut *connection).await {
        panic!("the migrations do not apply to a real Postgres: {error}");
    }
    drop(connection);

    let Ok(client) = Client::with_uri_str(&mongo_url).await else {
        panic!("could not connect to {mongo_url}");
    };
    let db = client.database(&format!("pneuma_janitor_{name}"));
    if db.drop().await.is_err() {
        panic!("should drop the test database");
    }
    let runs: Collection<Document> = db.collection("runs");
    let history: Collection<Document> = db.collection("run_history");

    let Ok(run_history) = RunHistoryStore::new(runs.clone(), history.clone()) else {
        panic!("distinct collections should be accepted");
    };
    let Ok(janitor) = Janitor::new(
        RunStore::new(runs.clone()),
        run_history,
        NodeRunStore::new(pool.clone()),
    ) else {
        panic!("stores agreeing on the run collection should be accepted");
    };
    (janitor, pool, runs, history)
}

/// A run document in a terminal state, so `finalized_runs` selects it.
fn finished(run_id: &str) -> Document {
    doc! { "run_id": run_id, "status": "finished", "step_output": { "big": "payload" } }
}

/// One node_run row for a run. Only the columns the copy touches matter.
async fn seed_noderun(pool: &PgPool, run_id: &str, path: &str) {
    let sql = "INSERT INTO node_run (id, path, node_id, name, kind, pipeline_id, run_id, \
               sibling_index, status, created_at, updated_at) \
               VALUES ($1, $2, 'A', 'a', 'Model', 'p', $3, 1, 'FINISHED', now(), now())";
    if let Err(error) = sqlx::query(sql)
        .bind(uuid::Uuid::new_v4())
        .bind(path)
        .bind(run_id)
        .execute(pool)
        .await
    {
        panic!("could not seed a node_run: {error}");
    }
}

/// A node_run row that has stopped moving: unfinished, and long untouched.
///
/// `PROCESSING` and an `updated_at` well behind any stale window, which is what
/// `stale_inprogress_run_ids` looks for -- per row, not per run.
async fn seed_stale_noderun(pool: &PgPool, run_id: &str, path: &str) {
    let sql = "INSERT INTO node_run (id, path, node_id, name, kind, pipeline_id, run_id, \
               sibling_index, status, created_at, updated_at) \
               VALUES ($1, $2, 'A', 'a', 'Model', 'p', $3, 1, 'PROCESSING', \
               now() - interval '400 days', now() - interval '400 days')";
    if let Err(error) = sqlx::query(sql)
        .bind(uuid::Uuid::new_v4())
        .bind(path)
        .bind(run_id)
        .execute(pool)
        .await
    {
        panic!("could not seed a stale node_run: {error}");
    }
}

async fn count(pool: &PgPool, table: &str) -> i64 {
    let sql = format!("SELECT count(*) FROM {table}");
    match sqlx::query_scalar(&sql).fetch_one(pool).await {
        Ok(n) => n,
        Err(error) => panic!("could not count {table}: {error}"),
    }
}

#[tokio::test]
async fn a_pass_archives_to_both_stores_then_deletes_from_both() {
    let (janitor, pool, runs, history) = fresh("full").await;
    let Ok(_) = runs.insert_many(vec![finished("r1"), finished("r2")]).await else {
        panic!("should seed runs");
    };
    seed_noderun(&pool, "r1", "s1").await;
    seed_noderun(&pool, "r2", "s2").await;

    let Ok(cleaned) = janitor.cleanup_runs(10).await else {
        panic!("should clean up");
    };
    assert_eq!(cleaned.runs, 2);
    assert_eq!(cleaned.noderuns_archived, 2);
    assert_eq!(cleaned.noderuns_deleted, 2);
    assert_eq!(cleaned.runs_deleted, 2);

    // Archived in both stores...
    let Ok(archived_runs) = history.count_documents(doc! {}).await else {
        panic!("should count");
    };
    assert_eq!(archived_runs, 2, "run documents reached history");
    assert_eq!(count(&pool, "node_run_history").await, 2, "so did the rows");

    // ...and gone from both live ones.
    let Ok(live_runs) = runs.count_documents(doc! {}).await else {
        panic!("should count");
    };
    assert_eq!(live_runs, 0, "the live runs were removed");
    assert_eq!(count(&pool, "node_run").await, 0, "and the live rows");
}

#[tokio::test]
async fn archive_survives_a_history_row_that_already_exists() {
    // The defect notes, end to end. A previous pass copied the
    // node_runs and then failed before deleting anything -- a lost connection at
    // the wrong moment. The run is still finalised, so this pass selects it
    // again and copies it again.
    //
    // In the original that second copy is a plain `INSERT` against a primary key
    // the row already holds, so it raises, the janitor logs "Aborting..." and
    // returns, and every later pass does the same. Cleanup stops for good and
    // both stores grow without bound.
    let (janitor, pool, runs, _history) = fresh("retry").await;
    let Ok(_) = runs.insert_many(vec![finished("r1")]).await else {
        panic!("should seed runs");
    };
    seed_noderun(&pool, "r1", "s1").await;

    // What the interrupted pass left behind: history written, nothing deleted.
    let Ok(copied) = sqlx::query("INSERT INTO node_run_history SELECT * FROM node_run")
        .execute(&pool)
        .await
    else {
        panic!("should pre-copy");
    };
    assert_eq!(copied.rows_affected(), 1, "the earlier pass got this far");

    let Ok(cleaned) = janitor.cleanup_runs(10).await else {
        panic!("the retry must make progress, not raise");
    };
    assert_eq!(
        cleaned.noderuns_archived, 0,
        "nothing new to copy, which is not an error"
    );
    assert_eq!(
        cleaned.noderuns_deleted, 1,
        "and the pass got past the copy"
    );
    assert_eq!(cleaned.runs_deleted, 1);
    assert_eq!(
        count(&pool, "node_run_history").await,
        1,
        "the row is archived exactly once"
    );
    assert_eq!(count(&pool, "node_run").await, 0);
}

#[tokio::test]
async fn a_pass_leaves_runs_that_are_not_finished() {
    let (janitor, pool, runs, history) = fresh("active").await;
    let Ok(_) = runs
        .insert_many(vec![doc! { "run_id": "busy", "status": "processing" }])
        .await
    else {
        panic!("should seed runs");
    };
    seed_noderun(&pool, "busy", "s1").await;

    let Ok(cleaned) = janitor.cleanup_runs(10).await else {
        panic!("should run");
    };
    assert_eq!(cleaned, Default::default(), "nothing was selected");
    let Ok(live) = runs.count_documents(doc! {}).await else {
        panic!("should count");
    };
    assert_eq!(live, 1, "the running run is untouched");
    assert_eq!(count(&pool, "node_run").await, 1, "and its rows");
    let Ok(archived) = history.count_documents(doc! {}).await else {
        panic!("should count");
    };
    assert_eq!(archived, 0, "nothing was archived");
}

#[tokio::test]
async fn expiring_history_removes_from_both_stores() {
    let (janitor, pool, runs, history) = fresh("expire").await;
    let Ok(_) = runs.insert_many(vec![finished("r1")]).await else {
        panic!("should seed runs");
    };
    seed_noderun(&pool, "r1", "s1").await;
    let Ok(_) = janitor.cleanup_runs(10).await else {
        panic!("should clean up");
    };

    // A cutoff before the archive keeps everything.
    let Ok(kept) = janitor
        .expire_history(Utc::now() - Duration::hours(1))
        .await
    else {
        panic!("should run");
    };
    assert_eq!(kept.runs, 0);
    assert_eq!(kept.node_runs, 0);

    let Ok(gone) = janitor
        .expire_history(Utc::now() + Duration::hours(1))
        .await
    else {
        panic!("should run");
    };
    assert_eq!(gone.runs, 1, "the run history expired");
    assert_eq!(gone.node_runs, 1, "and the node_run history");
    let Ok(left) = history.count_documents(doc! {}).await else {
        panic!("should count");
    };
    assert_eq!(left, 0);
    assert_eq!(count(&pool, "node_run_history").await, 0);
}

#[tokio::test]
async fn stale_runs_are_selected_but_not_acted_on() {
    // Selection only: submitting a termination is HTTP, so it belongs to the
    // binary. The janitor's job here is to say which runs have stopped moving.
    let (janitor, pool, runs, _history) = fresh("stale").await;
    let Ok(_) = runs
        .insert_many(vec![doc! { "run_id": "stuck", "status": "processing" }])
        .await
    else {
        panic!("should seed runs");
    };
    let sql = "INSERT INTO node_run (id, path, node_id, name, kind, pipeline_id, run_id, \
               sibling_index, status, created_at, updated_at) \
               VALUES ($1, 's1', 'A', 'a', 'Model', 'p', 'stuck', 1, 'PROCESSING', \
               now() - interval '2 hours', now() - interval '2 hours')";
    if let Err(error) = sqlx::query(sql)
        .bind(uuid::Uuid::new_v4())
        .execute(&pool)
        .await
    {
        panic!("could not seed a stale node_run: {error}");
    }

    let Ok(fresh_enough) = janitor
        .stale_run_ids(Utc::now() - Duration::hours(3), 10)
        .await
    else {
        panic!("should run");
    };
    assert!(
        fresh_enough.is_empty(),
        "not stale against a older threshold"
    );

    let Ok(stale) = janitor
        .stale_run_ids(Utc::now() - Duration::hours(1), 10)
        .await
    else {
        panic!("should run");
    };
    assert_eq!(
        stale,
        vec!["stuck".to_owned()],
        "stale against a recent one"
    );

    // And nothing was cancelled or deleted by asking.
    let Ok(live) = runs.count_documents(doc! {}).await else {
        panic!("should count");
    };
    assert_eq!(live, 1);
    assert_eq!(count(&pool, "node_run").await, 1);
}

#[tokio::test]
async fn a_preview_reports_exactly_what_the_pass_would_do() {
    // the original plan: ship with `--dry-run`, run a week, diff against
    // original. That diff is only worth anything if the preview and the action
    // ask the same question, so this asserts they agree rather than that the
    // preview merely returns something.
    let (janitor, pool, runs, history) = fresh("preview").await;
    let Ok(_) = runs
        .insert_many(vec![
            finished("r1"),
            finished("r2"),
            doc! { "run_id": "busy", "status": "processing" },
        ])
        .await
    else {
        panic!("should seed runs");
    };
    seed_noderun(&pool, "r1", "s1").await;
    seed_noderun(&pool, "r2", "s2").await;
    seed_noderun(&pool, "busy", "s3").await;

    let Ok(mut previewed) = janitor.preview_cleanup(10).await else {
        panic!("should preview");
    };
    previewed.sort();
    assert_eq!(
        previewed,
        vec!["r1".to_owned(), "r2".to_owned()],
        "the finished runs, and not the running one"
    );
    // Previewing changed nothing.
    let Ok(still_live) = runs.count_documents(doc! {}).await else {
        panic!("should count");
    };
    assert_eq!(still_live, 3, "a preview does not clean");
    assert_eq!(count(&pool, "node_run").await, 3);

    let Ok(cleaned) = janitor.cleanup_runs(10).await else {
        panic!("should clean up");
    };
    assert_eq!(
        cleaned.runs,
        previewed.len(),
        "and the pass takes exactly what the preview named"
    );

    // The same agreement for expiry, which is the half where the two
    // predicates live in different statements and can drift.
    let after = Utc::now() + Duration::hours(1);
    let Ok(would) = janitor.preview_expiry(after).await else {
        panic!("should preview");
    };
    let Ok(left_alone) = history.count_documents(doc! {}).await else {
        panic!("should count");
    };
    assert_eq!(left_alone, 2, "previewing expiry does not expire");

    let Ok(did) = janitor.expire_history(after).await else {
        panic!("should expire");
    };
    assert_eq!(
        would, did,
        "counting outdated history agrees with deleting it -- in both stores"
    );
    assert!(
        did.runs > 0 && did.node_runs > 0,
        "and it was not trivially zero"
    );
}

#[tokio::test]
async fn counting_outdated_history_agrees_with_deleting_it() {
    // The Postgres half specifically. Its count and its delete are two separate
    // SQL statements, so nothing but this stops their `WHERE` clauses drifting
    // -- and a `--dry-run` reporting from a drifted predicate is worse than no
    // dry run, because it is confidently wrong about what a real pass would do.
    let (janitor, pool, runs, _history) = fresh("agree").await;
    let Ok(_) = runs.insert_many(vec![finished("r1")]).await else {
        panic!("should seed runs");
    };
    seed_noderun(&pool, "r1", "s1").await;
    let Ok(_) = janitor.cleanup_runs(10).await else {
        panic!("should clean up");
    };

    // The row's own timestamp, exactly. Cutoffs far from the data agree under
    // either `<` or `<=`, so they cannot see a boundary that has drifted --
    // measured: changing the count's predicate to `<=` passed a version of this
    // test that used now±1 day. The equal case is the only one where the two
    // spellings differ, so it is the one worth asserting.
    let Ok(archived_at): Result<chrono::DateTime<Utc>, _> =
        sqlx::query_scalar("SELECT created_at FROM node_run_history LIMIT 1")
            .fetch_one(&pool)
            .await
    else {
        panic!("the cleanup should have archived a row");
    };

    for cutoff in [
        archived_at,
        archived_at + Duration::microseconds(1),
        Utc::now() + Duration::days(1),
    ] {
        let Ok(counted) = janitor.preview_expiry(cutoff).await else {
            panic!("should preview");
        };
        let Ok(deleted) = janitor.expire_history(cutoff).await else {
            panic!("should expire");
        };
        assert_eq!(
            counted.node_runs, deleted.node_runs,
            "the two Postgres predicates disagree at {cutoff}"
        );
        assert_eq!(
            counted.runs, deleted.runs,
            "and the two Mongo ones at {cutoff}"
        );
    }
}

#[tokio::test]
async fn stores_wired_to_different_run_collections_are_refused() {
    // Selection reads `RunStore`, while archive and delete both act on
    // `RunHistoryStore`'s own handle. Point them at different collections --
    // `run` for `runs`, a plausible typo -- and every pass selects runs that
    // are then archived from, and deleted from, somewhere else. Nothing moves,
    // nothing errors, and `Cleaned` reports the runs it selected, for ever.
    let (_janitor, pool, runs, history) = fresh("split").await;
    let db = runs.namespace().db;
    let client = runs.client().clone();
    let elsewhere: Collection<Document> = client.database(&db).collection("run");

    let Ok(mismatched) = RunHistoryStore::new(elsewhere, history) else {
        panic!("those two are distinct, so the history store is happy");
    };
    assert!(
        matches!(
            Janitor::new(RunStore::new(runs), mismatched, NodeRunStore::new(pool),),
            Err(pneuma_janitor::JanitorError::SplitRunCollection { .. })
        ),
        "the mismatch has to be caught where the two stores meet"
    );
}

#[tokio::test]
async fn a_non_positive_stale_limit_selects_nothing_rather_than_failing() {
    // Postgres rejects a negative `LIMIT` outright, so a caller paging with
    // `limit - done` and reaching below zero would turn what should be an
    // empty result into a failed pass.
    let (janitor, _pool, _runs, _history) = fresh("stale_limit").await;
    for limit in [0, -1] {
        let Ok(none) = janitor.stale_run_ids(Utc::now(), limit).await else {
            panic!("a non-positive limit should select nothing, not fail");
        };
        assert!(none.is_empty(), "limit {limit} selected something");
    }
}

/// Settings without touching the process environment.
///
/// `Settings::from_env` is tested in its own module; these tests are about what
/// a pass does with the values, so they build them directly.
fn settings(retention_days: i64, terminate_stale: bool) -> Settings {
    Settings {
        batch: 10,
        retention_days,
        stale_after: Duration::seconds(3600),
        terminate_stale,
    }
}

#[tokio::test]
async fn a_cycle_expires_before_it_archives() {
    // The regression test for the ordering, and the previous version of this
    // test was vacuous: with expiry running first the history collection is
    // empty when it runs, so `expired.runs == 0` however `archive` behaves. It
    // also seeded through `seed_noderun`, which writes `created_at = now()`, so
    // the Postgres half -- the *only* half the fix is about -- was never
    // exercised with an aged row. Reverting the ordering left all 21 tests
    // passing.
    //
    // What the fix is about: `history_backup.sql` copies `node_run.created_at`
    // verbatim and `history_delete_outdated.sql` still filters on it
    // (the design notes's last bullet defers that half), so cleaning up
    // first would archive a twenty-day-old row and expire it moments later --
    // the defect notes as a guarantee rather than a race.
    let (janitor, pool, runs, history) = fresh("order").await;
    let Ok(_) = runs.insert_many(vec![finished("long-lived")]).await else {
        panic!("should seed");
    };
    let sql = "INSERT INTO node_run (id, path, node_id, name, kind, pipeline_id, run_id, \
               sibling_index, status, created_at, updated_at) \
               VALUES ($1, 'aged', 'A', 'a', 'Model', 'p', 'long-lived', 1, 'FINISHED', \
               now() - interval '20 days', now())";
    if let Err(error) = sqlx::query(sql)
        .bind(uuid::Uuid::new_v4())
        .execute(&pool)
        .await
    {
        panic!("could not seed an aged node_run: {error}");
    }

    let Ok(report) = janitor.pass(&settings(14, false)).await else {
        panic!("a cycle should run");
    };
    assert_eq!(report.cleaned.runs, 1, "the run was cleaned up");
    assert_eq!(
        count(&pool, "node_run_history").await,
        1,
        "and its aged row survives the cycle that archived it -- cleaning up \
         first would have expired it in the same pass"
    );
    let Ok(kept) = history.count_documents(doc! {}).await else {
        panic!("should count");
    };
    assert_eq!(kept, 1, "as does the run document");

    // And the reprieve is exactly one cycle, not a fix. The Postgres half still
    // measures from creation, so the next pass expires the row -- which under
    // the original's `INTERVAL_MINUTES` of `*/5` is about five minutes later,
    // not days. Asserting it keeps the mitigation from reading as a cure and
    // the missing `archived_at` migration from being deprioritised.
    let Ok(next) = janitor.pass(&settings(14, false)).await else {
        panic!("a second cycle should run");
    };
    let Some(then) = next.expired else {
        panic!("retention is enabled");
    };
    assert_eq!(
        then.node_runs, 1,
        "§24 is mitigated on the Postgres half, not fixed"
    );
    assert_eq!(count(&pool, "node_run_history").await, 0);
    // The Mongo half is genuinely safe: its stamp is fresh, so no cutoff this
    // cycle computes can reach it, however many cycles run.
    assert_eq!(then.runs, 0, "the run document is not expired by its stamp");
}

#[tokio::test]
async fn a_cycle_does_not_report_a_run_it_is_about_to_delete_as_stale() {
    // A run finalised in Mongo can still hold a non-finalised, stale row in
    // Postgres: `delete_by_run_ids` removes rows whatever their status. The
    // stale scan runs before that delete -- it has to, or `pass` and
    // `preview_pass` would disagree -- so without filtering, this pass reports
    // a run as needing termination and then deletes every trace of it. The
    // binary would POST a termination for a run that exists nowhere.
    let (janitor, pool, runs, _history) = fresh("both").await;
    let Ok(_) = runs.insert_many(vec![finished("finished-but-stuck")]).await else {
        panic!("should seed");
    };
    let sql = "INSERT INTO node_run (id, path, node_id, name, kind, pipeline_id, run_id, \
               sibling_index, status, created_at, updated_at) \
               VALUES ($1, 'stuck', 'A', 'a', 'Model', 'p', 'finished-but-stuck', 1, \
               'PROCESSING', now() - interval '2 hours', now() - interval '2 hours')";
    if let Err(error) = sqlx::query(sql)
        .bind(uuid::Uuid::new_v4())
        .execute(&pool)
        .await
    {
        panic!("could not seed: {error}");
    }

    // It is genuinely stale by the scan's own rule...
    let Ok(scanned) = janitor
        .stale_run_ids(Utc::now() - Duration::hours(1), 10)
        .await
    else {
        panic!("should scan");
    };
    assert_eq!(scanned, vec!["finished-but-stuck".to_owned()]);

    // ...and a full cycle still must not name it, because it is taking it.
    let Ok(report) = janitor.pass(&settings(14, true)).await else {
        panic!("a cycle should run");
    };
    assert_eq!(report.cleaned.runs, 1, "the cycle took it");
    assert!(
        report.stale.is_empty(),
        "and did not also ask for it to be terminated: {:?}",
        report.stale
    );
    assert_eq!(count(&pool, "node_run").await, 0, "its rows are gone");
}

#[tokio::test]
async fn a_cycle_skips_expiry_when_retention_is_disabled() {
    // `None`, not zero: "kept everything on purpose" and "expired nothing this
    // time" are different answers, and only the second is worth investigating.
    let (janitor, pool, runs, history) = fresh("keep").await;
    let Ok(_) = runs.insert_many(vec![finished("r1")]).await else {
        panic!("should seed");
    };
    seed_noderun(&pool, "r1", "s1").await;

    let Ok(report) = janitor.pass(&settings(0, false)).await else {
        panic!("a cycle should run");
    };
    assert!(
        report.expired.is_none(),
        "retention disabled reports absence, not a zero"
    );
    let Ok(kept) = history.count_documents(doc! {}).await else {
        panic!("should count");
    };
    assert_eq!(kept, 1, "and nothing was expired");
}

#[tokio::test]
async fn a_preview_cycle_changes_nothing() {
    // The `--dry-run` phase 7 runs for a week before cutover. If it moved
    // anything it would not be a dry run, and the diff against the original
    // janitor would be against a system this one had already altered.
    let (janitor, pool, runs, history) = fresh("dry").await;
    let Ok(_) = runs.insert_many(vec![finished("r1"), finished("r2")]).await else {
        panic!("should seed");
    };
    seed_noderun(&pool, "r1", "s1").await;
    seed_noderun(&pool, "r2", "s2").await;

    let Ok(preview) = janitor.preview_pass(&settings(14, true)).await else {
        panic!("a preview should run");
    };
    assert_eq!(preview.cleaned.runs, 2, "it names what a pass would take");

    let Ok(live) = runs.count_documents(doc! {}).await else {
        panic!("should count");
    };
    assert_eq!(live, 2, "the runs are still there");
    assert_eq!(count(&pool, "node_run").await, 2, "and their rows");
    let Ok(archived) = history.count_documents(doc! {}).await else {
        panic!("should count");
    };
    assert_eq!(archived, 0, "nothing was archived");
    assert_eq!(count(&pool, "node_run_history").await, 0);

    // With retention off, a preview reports absence rather than a zero, for the
    // same reason a real cycle does.
    let Ok(no_expiry) = janitor.preview_pass(&settings(0, false)).await else {
        panic!("a preview should run");
    };
    assert!(no_expiry.expired.is_none());

    // And the real cycle then takes exactly what the preview named.
    let Ok(report) = janitor.pass(&settings(14, true)).await else {
        panic!("a cycle should run");
    };
    assert_eq!(report.cleaned.runs, preview.cleaned.runs);
}

#[tokio::test]
async fn a_preview_reports_the_same_stale_runs_the_real_pass_does() {
    // The comparison `a_preview_cycle_changes_nothing` never made, and the
    // defect it would have caught. `pass` drops from its stale list the runs
    // this very cycle is about to archive and delete -- otherwise it names runs
    // whose records then exist nowhere, and the binary submits terminations for
    // them. `preview_pass` did not, so the `--dry-run` that phase 7 runs
    // against production for a week disagreed with the pass it is supposed to
    // be previewing. That disagreement is the one thing the diff cannot afford,
    // and the comment in `pass` says so three lines above where it was.
    let (janitor, pool, runs, _history) = fresh("stalediff").await;

    // Finalised in Mongo *and* holding a stale unfinished row: this cycle
    // archives and deletes it, so neither report should name it.
    let Ok(_) = runs.insert_many(vec![finished("taken")]).await else {
        panic!("should seed");
    };
    seed_stale_noderun(&pool, "taken", "s-taken").await;
    // Not finalised, so nothing takes it. Genuinely stale, and both reports
    // must say so -- otherwise this test would pass on an empty list either
    // way, which is the shape of the bug it exists to catch.
    seed_stale_noderun(&pool, "left", "s-left").await;

    let settings = settings(14, true);
    let Ok(preview) = janitor.preview_pass(&settings).await else {
        panic!("a preview should run");
    };
    let Ok(report) = janitor.pass(&settings).await else {
        panic!("a cycle should run");
    };

    assert_eq!(
        preview.stale, report.stale,
        "the preview reports the stale runs the pass reports"
    );
    assert_eq!(
        preview.stale,
        vec!["left".to_owned()],
        "the one nothing is taking -- and not the empty list, which would let \
         this pass without the filter running at all"
    );
    assert!(
        !preview.stale.contains(&"taken".to_owned()),
        "a run this cycle archives and deletes is not also reported stale"
    );
}

#[tokio::test]
async fn a_stale_window_too_large_to_subtract_is_an_error() {
    // `Settings::from_env` refuses a window this size, but `Settings` has
    // public fields and can be built directly -- which is why
    // `stale_threshold` returns an `Option` rather than subtracting and
    // panicking.
    //
    // The `None` is then an *error*, not an empty list. Empty means "nothing is
    // stale", so returning it would have the binary terminate nothing while
    // reporting success -- the silent no-op this crate propagates errors to
    // avoid, and which `settings.rs` rejects the neighbouring out-of-range
    // values loudly for.
    let (janitor, _pool, _runs, _history) = fresh("overflow").await;
    let Some(enormous) = Duration::try_days(100_000_000) else {
        panic!("representable as a duration, just not subtractable from now");
    };
    let settings = Settings {
        batch: 10,
        retention_days: 0,
        stale_after: enormous,
        terminate_stale: true,
    };
    assert!(
        settings.stale_threshold().is_none(),
        "this test only means something while the subtraction really overflows"
    );

    assert!(
        matches!(
            janitor.pass(&settings).await,
            Err(pneuma_janitor::JanitorError::UnusableStaleWindow { .. })
        ),
        "an unusable window is an error, not an empty list -- empty would mean \
         'nothing is stale' and have the binary terminate nothing while \
         reporting success"
    );
}
