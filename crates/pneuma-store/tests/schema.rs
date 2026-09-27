//! Checks the SQL against a real Postgres.
//!
//! Every statement in [`pneuma_store::queries::ALL`] is sent to the server to
//! `PREPARE`, which makes the server parse it, resolve every column against the
//! real schema, and infer every parameter type. A typo, a renamed column, or a
//! type the driver cannot bind fails here — without the repository layer having
//! to exist yet.
//!
//! Requires `PNEUMA_TEST_DATABASE_URL`. It **fails** rather than skips when that
//! is unset: a schema test that silently passes on a machine with no database
//! is worse than no test, because it reports success for work it did not do.
//!
//! ```sh
//! docker run -d --name pn-pg -p 5433:5432 -e POSTGRES_PASSWORD=pneuma postgres:16
//! PNEUMA_TEST_DATABASE_URL=postgres://postgres:pneuma@127.0.0.1:5433/postgres \
//!     cargo test -p pneuma-store --test schema
//! ```

use sqlx::{Executor, PgPool};

async fn fresh_schema() -> PgPool {
    let Ok(url) = std::env::var("PNEUMA_TEST_DATABASE_URL") else {
        panic!(
            "PNEUMA_TEST_DATABASE_URL is not set. This test verifies the SQL \
             against a real server; skipping it would report success for work \
             it did not do. See this file's docs for the one-line container."
        );
    };
    let Ok(pool) = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .after_connect(|conn, _| {
            Box::pin(async move {
                conn.execute("SET search_path TO pneuma_test_schema")
                    .await?;
                Ok(())
            })
        })
        .connect(&url)
        .await
    else {
        panic!("could not connect to {url}");
    };
    // Start from nothing, so a stale table cannot mask a bad migration.
    // Each test binary gets its own Postgres schema. Both binaries create the
    // same tables in the same database, and they pass today only because
    // `cargo test` runs test targets one at a time -- under a runner that
    // parallelises them (`cargo nextest`), one would drop the tables out from
    // under the other. That is the same failure this file's header records for
    // parallel tests, reintroduced at target granularity.
    // Dropped whole and rebuilt by the *migrator*, rather than by a list of
    // tables to drop and files to apply. That list was hand-maintained and it
    // drifted the first time a migration was added: `0003_submission` created
    // a table this fixture did not, so `SUBMISSION_ENQUEUE` failed to prepare
    // against a schema that was missing it -- caught by `queries::ALL`, which
    // exists for exactly that, but only after the fact. Running the migrator
    // means the next migration cannot break it the same way.
    for statement in [
        "DROP SCHEMA IF EXISTS pneuma_test_schema CASCADE",
        "CREATE SCHEMA pneuma_test_schema",
        "SET search_path TO pneuma_test_schema",
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

/// All three schema checks in one test, deliberately.
///
/// They were three `#[tokio::test]`s and that was wrong twice over. Cargo runs
/// tests in parallel threads against the one database, so they raced on
/// `DROP TYPE`/`CREATE TYPE` and failed with
/// `duplicate key value violates unique constraint "pg_type_typname_nsp_index"`.
/// Sharing one pool through a `OnceCell` fixed the race and introduced a second
/// problem: each `#[tokio::test]` builds its own runtime, so the pool outlived
/// the runtime that created it and later queries failed with "A Tokio 1.x
/// context was found, but it is being shutdown".
///
/// One test, one runtime, one schema. The three checks stay separate as
/// sections with their own assertions.
#[tokio::test]
async fn the_schema_matches_a_real_postgres() {
    let pool = fresh_schema().await;

    // --- every query parses against the real schema -----------------------
    //
    // One connection for all of them. Preparing on the pool directly acquires a
    // connection per statement and holds it for that statement's lifetime,
    // which exhausted the pool and failed with "pool timed out while waiting
    // for an open connection" -- a misleading error for a resource mistake in
    // the test.
    let Ok(mut conn) = pool.acquire().await else {
        panic!("could not acquire a connection");
    };
    for (name, sql) in pneuma_store::queries::ALL {
        // PREPARE makes the server parse the statement, resolve every column
        // against the schema just created, and infer every parameter type.
        if let Err(error) = conn.prepare(sql).await {
            panic!("query {name} does not prepare against the real schema: {error}\n{sql}");
        }
    }
    drop(conn);

    // --- the enums carry exactly the original migration tool's labels -------------------------
    //
    // pneuma-core pins these without a database. This pins that the migration
    // in *this* crate creates the same ones, so the two cannot drift.
    for (type_name, expected) in [
        (
            "node_kind",
            vec!["Model", "ListAggregator", "DictAggregator", "Condition"],
        ),
        (
            "node_status",
            vec![
                "CREATED",
                "PROCESSING",
                "FINISHED",
                "ERROR",
                "TIMED_OUT",
                "CANCELLED",
                "FORKED",
                "AGGREGATED",
                "HAS_CHILD_ERROR",
                "HAS_CHILD_TIMED_OUT",
            ],
        ),
    ] {
        // Scoped to the current schema: each test binary creates its own
        // `node_kind`/`node_status`, so an unscoped pg_enum read returns every
        // copy and the labels come back doubled.
        let query = "SELECT e.enumlabel::text FROM pg_enum e \
                     JOIN pg_type t ON t.oid = e.enumtypid \
                     JOIN pg_namespace n ON n.oid = t.typnamespace \
                     WHERE t.typname = $1 AND n.nspname = current_schema() \
                     ORDER BY e.enumsortorder";
        let Ok(labels) = sqlx::query_scalar::<_, String>(query)
            .bind(type_name)
            .fetch_all(&pool)
            .await
        else {
            panic!("could not read the {type_name} labels");
        };
        assert_eq!(labels, expected, "{type_name} labels drifted");
    }

    // --- parent_path really is a self-referential foreign key -------------
    //
    // Load-bearing for insert ordering: a child cannot be written before its
    // parent. Asserted against the server rather than read off the DDL.
    let orphan = "INSERT INTO node_run \
        (id, path, node_id, name, kind, pipeline_id, run_id, parent_path, status, created_at, updated_at) \
        VALUES (gen_random_uuid(), 'child', 'n', 'n', 'Model', 'p', 'r', 'absent-parent', 'CREATED', NOW(), NOW())";
    let Err(error) = pool.execute(orphan).await else {
        panic!("inserting a child before its parent should violate the foreign key");
    };
    let message = error.to_string();
    assert!(
        message.contains("noderun_parent_slug_fkey") || message.contains("foreign key"),
        "expected a foreign-key violation, got: {message}"
    );

    // --- the update guard behaves like should_update_status_from ----------
    //
    // The guard lives in the WHERE clause, so "refused" shows up as zero rows
    // rather than as an error. Checking it against the server is the only way
    // to know the SQL expresses the original predicate and not something that
    // merely parses.
    let insert = "INSERT INTO node_run \
        (id, path, node_id, name, kind, pipeline_id, run_id, status, created_at, updated_at) \
        VALUES (gen_random_uuid(), $1, 'n', 'n', 'Model', 'p', 'r', $2::node_status, NOW(), NOW())";

    for (path, from, to, expected, why) in [
        (
            "a",
            "CREATED",
            "PROCESSING",
            1,
            "an ordinary transition is allowed",
        ),
        (
            "b",
            "CREATED",
            "FORKED",
            1,
            "FORKED is allowed from CREATED",
        ),
        (
            "c",
            "PROCESSING",
            "FORKED",
            0,
            "FORKED is refused from anything else",
        ),
        (
            "d",
            "FINISHED",
            "PROCESSING",
            0,
            "a finalised row is never moved",
        ),
        ("e", "AGGREGATED", "ERROR", 0, "AGGREGATED is finalised too"),
        // CANCELLED is terminal here, unlike in the original. That is a
        // signed-off deviation the design notes record, and it is the whole point:
        // a late result must not resurrect a cancelled node. An earlier
        // version of this test asserted the opposite and pinned the defect.
        (
            "f",
            "CANCELLED",
            "PROCESSING",
            0,
            "a cancelled node is never resurrected",
        ),
    ] {
        let Ok(_) = sqlx::query(insert)
            .bind(path)
            .bind(from)
            .execute(&pool)
            .await
        else {
            panic!("could not insert {path} as {from}");
        };
        let Ok(rows) = sqlx::query(pneuma_store::queries::UPDATE_STATUS)
            .bind(path)
            .bind(to)
            .bind(Option::<String>::None)
            .bind(Option::<String>::None)
            .fetch_all(&pool)
            .await
        else {
            panic!("update failed for {path}");
        };
        assert_eq!(rows.len(), expected, "{from} -> {to}: {why}");
    }

    // --- the parent_path filters actually filter --------------------------
    //
    // Dropping the node_ids filter is silent: the query still returns rows in
    // child_index order, just the wrong set of them. `a` already exists as a row
    // from the guard section above, so it can serve as the parent.
    for (path, node_id, idx) in [("p1", "wanted", 1), ("p2", "unwanted", 2)] {
        let sql = "INSERT INTO node_run \
            (id, path, node_id, name, kind, pipeline_id, run_id, parent_path, child_index, status, created_at, updated_at) \
            VALUES (gen_random_uuid(), $1, $2, 'n', 'Model', 'p', 'r', 'a', $3, 'CREATED', NOW(), NOW())";
        if let Err(error) = sqlx::query(sql)
            .bind(path)
            .bind(node_id)
            .bind(idx)
            .execute(&pool)
            .await
        {
            panic!("could not insert {path}: {error}");
        }
    }

    let by_parent = pneuma_store::queries::GET_BY_PARENT_PATH;
    // `query` rather than `query_scalar`: column 0 is `id`, a UUID, so a
    // scalar decode into String fails. Only the row count matters here.
    let unfiltered = match sqlx::query(by_parent)
        .bind("a")
        .bind(Option::<String>::None)
        .bind(Option::<Vec<String>>::None)
        .fetch_all(&pool)
        .await
    {
        Ok(rows) => rows,
        Err(error) => panic!("unfiltered query failed: {error}"),
    };
    assert_eq!(unfiltered.len(), 2, "no filter returns every child");

    let filtered = match sqlx::query(by_parent)
        .bind("a")
        .bind(Option::<String>::None)
        .bind(Some(vec!["wanted".to_owned()]))
        .fetch_all(&pool)
        .await
    {
        Ok(rows) => rows,
        Err(error) => panic!("filtered query failed: {error}"),
    };
    assert_eq!(filtered.len(), 1, "node_ids must restrict the set");

    // --- create is idempotent on path -------------------------------------
    //
    // Retry on this path is a known condition (fa3ce4e18f15's own comment says
    // so), and uq_noderun_slug would otherwise turn a redelivery into an error.
    let create = "INSERT INTO node_run \
        (id, path, node_id, name, kind, pipeline_id, run_id, status, created_at, updated_at) \
        VALUES (gen_random_uuid(), 'dup', 'n', 'n', 'Model', 'p', 'r', 'CREATED', NOW(), NOW()) \
        ON CONFLICT (path) DO NOTHING RETURNING path";
    let Ok(first) = sqlx::query(create).fetch_all(&pool).await else {
        panic!("first create failed");
    };
    assert_eq!(first.len(), 1, "the first create returns its row");
    let Ok(second) = sqlx::query(create).fetch_all(&pool).await else {
        panic!("a repeated create must return no row, not raise");
    };
    assert!(second.is_empty(), "the second create returns nothing");

    // --- history backup is idempotent, and delete is timezone-proof --------
    //
    // Both properties are fixes, not transcriptions (the defect notes
    // and §16), so both are asserted against the server rather than trusted.
    let seed = "INSERT INTO node_run \
        (id, path, node_id, name, kind, pipeline_id, run_id, status, created_at, updated_at) \
        VALUES (gen_random_uuid(), 'h1', 'n', 'n', 'Model', 'p', 'histrun', 'FINISHED', $1, NOW())";
    let old_row = chrono::Utc::now() - chrono::Duration::days(30);
    if let Err(error) = sqlx::query(seed).bind(old_row).execute(&pool).await {
        panic!("could not seed a history candidate: {error}");
    }

    let backup = pneuma_store::queries::HISTORY_BACKUP;
    let runs = vec!["histrun".to_owned()];
    let Ok(first) = sqlx::query(backup).bind(&runs).execute(&pool).await else {
        panic!("first backup failed");
    };
    assert_eq!(first.rows_affected(), 1, "the row is copied");

    // The whole point of §17: the ids are copied, so without ON CONFLICT this
    // is a primary-key violation, and in the original it wedges the janitor on
    // every subsequent pass.
    let Ok(second) = sqlx::query(backup).bind(&runs).execute(&pool).await else {
        panic!("a repeated backup must be a no-op, not a key violation");
    };
    assert_eq!(
        second.rows_affected(),
        0,
        "the second backup copies nothing"
    );

    // The shipped delete, run under a non-UTC session.
    //
    // This section has been wrong twice, so it is worth saying what it does and
    // does not establish. It ran a hand-written predicate rather than the
    // shipped statement, only under UTC, and asserted a property that holds
    // regardless -- it could not fail. Rewritten to assert that a naive cutoff
    // decides differently, it failed: the premise of the defect notes
    // turned out to be false for a bound parameter, and that entry is now
    // retracted.
    //
    // What remains is a genuine but modest check: the real statement, on a
    // session that is not UTC, deletes what it should. It is not a regression
    // guard for anything, because there is no longer a defect here to guard.
    //
    // One connection throughout: `SET TimeZone` applies to a session, and
    // `pool.execute` may hand back a different connection next time.
    let Ok(mut session) = pool.acquire().await else {
        panic!("could not acquire a connection");
    };
    if let Err(error) = session.execute("SET TimeZone='Asia/Taipei'").await {
        panic!("could not set the session timezone: {error}");
    }

    // Start from an empty history: the §17 section above left a 30-day-old row
    // that is also past this cutoff, which made the count ambiguous.
    if let Err(error) = session.execute("DELETE FROM node_run_history").await {
        panic!("could not clear history: {error}");
    }

    let cutoff = chrono::Utc::now() - chrono::Duration::days(7);
    let seed_history = "INSERT INTO node_run_history \
        (id, path, node_id, name, kind, pipeline_id, run_id, status, created_at, updated_at) \
        VALUES (gen_random_uuid(), 'tz', 'n', 'n', 'Model', 'p', 'r', 'FINISHED', $1, NOW())";
    if let Err(error) = sqlx::query(seed_history)
        .bind(cutoff - chrono::Duration::hours(2))
        .execute(&mut *session)
        .await
    {
        panic!("could not seed the timezone row: {error}");
    }

    let Ok(deleted) = sqlx::query(pneuma_store::queries::HISTORY_DELETE_OUTDATED)
        .bind(cutoff)
        .execute(&mut *session)
        .await
    else {
        panic!("delete failed");
    };
    assert_eq!(
        deleted.rows_affected(),
        1,
        "the shipped statement deletes the outdated row under Asia/Taipei too"
    );
    drop(session);

    pool.close().await;
}
