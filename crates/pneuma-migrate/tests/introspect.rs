//! Drives introspection against a real Postgres.
//!
//! The point of this file is one property: **the same migrations applied to two
//! differently-named schemas compare identical.** That is what `baseline`
//! depends on, and it is exactly what the schema qualifier in
//! `pg_get_indexdef` and `pg_get_constraintdef` breaks — both render the schema
//! name into the definition, so without normalisation every index and every
//! foreign key differs between the live database and the scratch schema the
//! expectation is built in, and `baseline` refuses a database that matches
//! perfectly.
//!
//! A mock could not have shown that. Two schemas, the same SQL, and a real
//! server rendering the definitions is the whole test.
//!
//! ```sh
//! docker run -d --name pn-pg -p 5433:5432 -e POSTGRES_PASSWORD=pneuma postgres:16
//! PNEUMA_TEST_DATABASE_URL=postgres://postgres:pneuma@127.0.0.1:5433/postgres \
//!     cargo test -p pneuma-migrate --test introspect
//! ```

use pneuma_migrate::{introspect, Difference};
use sqlx::{Executor, PgPool};

async fn pool() -> PgPool {
    let Ok(url) = std::env::var("PNEUMA_TEST_DATABASE_URL") else {
        panic!("PNEUMA_TEST_DATABASE_URL is not set; see the header of this file");
    };
    match sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(&url)
        .await
    {
        Ok(pool) => pool,
        Err(error) => panic!("could not connect to {url}: {error}"),
    }
}

/// Applies every migration into a schema of that name, from scratch.
///
/// The migrator rather than a hand-picked list of files: this fixture asserts
/// on table and index *names*, and `0004_rename.sql` changes them. A list that
/// stops at 0002 would have this file asserting against a schema no deployment
/// ever has.
async fn migrate_into(pool: &PgPool, schema: &str) {
    for statement in [
        format!("DROP SCHEMA IF EXISTS {schema} CASCADE"),
        format!("CREATE SCHEMA {schema}"),
        format!("SET search_path TO {schema}"),
    ] {
        if let Err(error) = pool.execute(statement.as_str()).await {
            panic!("setup failed on {statement:?}: {error}");
        }
    }
    let Ok(mut connection) = pool.acquire().await else {
        panic!("could not acquire a connection to migrate on");
    };
    if let Err(error) = pneuma_store::migrator().run(&mut *connection).await {
        panic!("the migrations do not apply: {error}");
    }
    drop(connection);
    // Back to a search_path that does *not* contain the schema, so
    // `pg_get_indexdef` qualifies its output -- which is the production shape
    // and the case the normalisation exists for. Leaving it on search_path
    // would make this test pass without the normalisation.
    if let Err(error) = pool.execute("SET search_path TO public").await {
        panic!("could not reset the search path: {error}");
    }
}

#[tokio::test]
async fn the_same_migrations_in_two_schemas_compare_identical() {
    let pool = pool().await;
    migrate_into(&pool, "pneuma_fp_live").await;
    migrate_into(&pool, "pneuma_fp_scratch").await;

    let Ok(live) = introspect(&pool, "pneuma_fp_live").await else {
        panic!("should introspect the live schema");
    };
    let Ok(scratch) = introspect(&pool, "pneuma_fp_scratch").await else {
        panic!("should introspect the scratch schema");
    };

    // The test only means something if it actually read a schema.
    assert!(
        live.tables.contains_key("node_run") && live.tables.contains_key("node_run_history"),
        "both tables were read: {:?}",
        live.tables.keys().collect::<Vec<_>>()
    );
    assert_eq!(
        live.enums.len(),
        3,
        "node_status, node_kind and submission_state"
    );
    let Some(node_run) = live.tables.get("node_run") else {
        panic!("just asserted it is there");
    };
    assert!(
        node_run.indexes.len() >= 5,
        "the migration's indexes plus the ones Postgres synthesises: {:?}",
        node_run.indexes.keys().collect::<Vec<_>>()
    );
    assert!(
        node_run
            .constraints
            .contains_key("node_run_parent_path_fkey"),
        "the self-referential foreign key, which produces no index: {:?}",
        node_run.constraints.keys().collect::<Vec<_>>()
    );

    let differences = live.differences(&scratch);
    assert!(
        differences.is_empty(),
        "the same SQL in two schemas must fingerprint identical, or `baseline` \
         refuses every database that matches: {differences:?}"
    );
}

#[tokio::test]
async fn a_schema_missing_an_index_is_reported_against_one_that_has_it() {
    // The other direction: introspection has to *see* drift, not just agree
    // with itself. Dropping one index is the smallest real difference.
    let pool = pool().await;
    migrate_into(&pool, "pneuma_fp_dropped").await;
    migrate_into(&pool, "pneuma_fp_intact").await;
    if let Err(error) = pool
        .execute("DROP INDEX pneuma_fp_dropped.ix_node_run_run_id")
        .await
    {
        panic!("could not drop the index: {error}");
    }

    let Ok(dropped) = introspect(&pool, "pneuma_fp_dropped").await else {
        panic!("should introspect");
    };
    let Ok(intact) = introspect(&pool, "pneuma_fp_intact").await else {
        panic!("should introspect");
    };

    assert_eq!(
        dropped.differences(&intact),
        vec![Difference::MissingIndex {
            table: "node_run".to_owned(),
            index: "ix_node_run_run_id".to_owned(),
        }],
        "exactly the one index, named"
    );
}

#[tokio::test]
async fn a_schema_that_does_not_exist_reads_as_empty() {
    // `baseline` compares an unmigrated database against what the migrations
    // produce, and "every table is missing" is the report that should give.
    let pool = pool().await;
    let Ok(nothing) = introspect(&pool, "pneuma_fp_absent").await else {
        panic!("an absent schema is not an error");
    };
    assert!(nothing.tables.is_empty());
    assert!(nothing.enums.is_empty());

    migrate_into(&pool, "pneuma_fp_present").await;
    let Ok(present) = introspect(&pool, "pneuma_fp_present").await else {
        panic!("should introspect");
    };
    let differences = nothing.differences(&present);
    assert!(
        differences
            .iter()
            .any(|d| matches!(d, Difference::MissingTable { .. })),
        "the report names the tables that are missing: {differences:?}"
    );
    assert!(
        differences
            .iter()
            .any(|d| matches!(d, Difference::MissingEnum { .. })),
        "and the types"
    );
}

#[tokio::test]
async fn a_materialized_view_is_not_read_as_a_table() {
    // Measured on PG 16: a matview appears in `pg_indexes` and *not* in
    // `information_schema.tables` at all. So filtering only the column pass to
    // `BASE TABLE` invents a table with indexes and no columns, and `baseline`
    // reports `UnexpectedTable` and `UnexpectedIndex` against a database whose
    // real tables match perfectly.
    //
    // Plain views are the opposite case -- present in `information_schema` and
    // absent from `pg_indexes` -- so both filters are needed and neither
    // subsumes the other.
    let pool = pool().await;
    migrate_into(&pool, "pneuma_fp_mv").await;
    migrate_into(&pool, "pneuma_fp_plain").await;
    for statement in [
        "CREATE MATERIALIZED VIEW pneuma_fp_mv.mv AS SELECT id FROM pneuma_fp_mv.node_run",
        "CREATE INDEX ix_mv ON pneuma_fp_mv.mv (id)",
        "CREATE VIEW pneuma_fp_mv.plain_view AS SELECT id FROM pneuma_fp_mv.node_run",
    ] {
        if let Err(error) = pool.execute(statement).await {
            panic!("could not create the view: {error}");
        }
    }

    let Ok(with_views) = introspect(&pool, "pneuma_fp_mv").await else {
        panic!("should introspect");
    };
    assert!(
        !with_views.tables.contains_key("mv"),
        "a materialized view is not a table: {:?}",
        with_views.tables.keys().collect::<Vec<_>>()
    );
    assert!(
        !with_views.tables.contains_key("plain_view"),
        "nor is a plain one"
    );

    let Ok(without) = introspect(&pool, "pneuma_fp_plain").await else {
        panic!("should introspect");
    };
    assert!(
        with_views.differences(&without).is_empty(),
        "adding views to a schema does not change its fingerprint: {:?}",
        with_views.differences(&without)
    );
}

#[tokio::test]
async fn a_foreign_table_is_not_read_as_a_table() {
    // The constraint pass needs its own filter, and *not* for the reason the
    // other two do: Postgres refuses `ADD CONSTRAINT` on a view and on a
    // materialized view, so no view can ever reach `pg_constraint`. A foreign
    // table can -- it carries a `CHECK`, and `information_schema` calls it
    // `FOREIGN`, so the column and index passes already drop it. Without
    // `relkind IN ('r', 'p')` the constraint pass alone would resurrect it as a
    // table with one constraint and nothing else.
    let pool = pool().await;
    migrate_into(&pool, "pneuma_fp_fdw").await;
    for statement in [
        "CREATE EXTENSION IF NOT EXISTS postgres_fdw",
        "DROP SERVER IF EXISTS pneuma_fp_srv CASCADE",
        "CREATE SERVER pneuma_fp_srv FOREIGN DATA WRAPPER postgres_fdw \
         OPTIONS (host 'localhost', dbname 'postgres')",
        // Never queried -- introspection reads catalogs, so the server does not
        // have to be reachable for this to be a faithful test.
        "CREATE FOREIGN TABLE pneuma_fp_fdw.ft (id int, n int CHECK (n > 0)) \
         SERVER pneuma_fp_srv",
    ] {
        if let Err(error) = pool.execute(statement).await {
            panic!("could not set up the foreign table ({statement:?}): {error}");
        }
    }

    let Ok(schema) = introspect(&pool, "pneuma_fp_fdw").await else {
        panic!("should introspect");
    };
    assert!(
        !schema.tables.contains_key("ft"),
        "a foreign table is not a base table: {:?}",
        schema.tables.keys().collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn an_under_privileged_role_reads_the_schema_as_empty_rather_than_as_phantoms() {
    // The reason all three passes filter through `information_schema.tables`
    // rather than each spelling "a real table" in its own dialect.
    //
    // `information_schema` is privilege-filtered; `pg_indexes` and
    // `pg_constraint` are not. Measured here: a role with only USAGE sees every
    // index and every constraint, and no tables and no columns. Had the
    // constraint pass kept `relkind IN ('r', 'p')` -- which selects exactly the
    // same relations for a superuser -- that role would read tables carrying a
    // constraint and nothing else, and `baseline` would report drift against a
    // database that matches. Reading empty is also wrong, but `baseline`
    // reports it as every table missing and refuses to write.
    //
    // Its own pool, so `SET ROLE` cannot leak into a concurrently running test.
    let pool = pool().await;
    migrate_into(&pool, "pneuma_fp_priv").await;
    for statement in [
        "DROP ROLE IF EXISTS pneuma_fp_peeker",
        "CREATE ROLE pneuma_fp_peeker",
        "GRANT USAGE ON SCHEMA pneuma_fp_priv TO pneuma_fp_peeker",
    ] {
        if let Err(error) = pool.execute(statement).await {
            panic!("could not create the role ({statement:?}): {error}");
        }
    }

    // The premise: this role really can still see the catalogs that are not
    // privilege-filtered. Without this the test would pass on a role that
    // simply sees nothing at all, and prove nothing about the filter.
    if let Err(error) = pool.execute("SET ROLE pneuma_fp_peeker").await {
        panic!("could not assume the role: {error}");
    }
    let visible_indexes: i64 = match sqlx::query_scalar(
        "SELECT count(*) FROM pg_indexes WHERE schemaname = 'pneuma_fp_priv'",
    )
    .fetch_one(&pool)
    .await
    {
        Ok(count) => count,
        Err(error) => panic!("could not count indexes: {error}"),
    };
    assert!(
        visible_indexes > 0,
        "pg_indexes is not privilege-filtered, so the unprivileged role still \
         sees indexes -- that is the whole hazard being guarded against"
    );

    let Ok(schema) = introspect(&pool, "pneuma_fp_priv").await else {
        panic!("an unreadable schema is not an error");
    };
    if let Err(error) = pool.execute("RESET ROLE").await {
        panic!("could not reset the role: {error}");
    }

    assert!(
        schema.tables.is_empty(),
        "no tables, and not phantom ones holding only constraints: {:?}",
        schema.tables
    );
    // Pinned because it is the surprising half, and because the module doc used
    // to claim the schema reads as *empty*. It does not: `pg_type`/`pg_enum`
    // are not privilege-filtered and `information_schema` offers no enum view
    // to route the pass through, so the enums come back in full.
    assert_eq!(
        schema.enums.len(),
        3,
        "the enum pass is not privilege-filtered, and cannot be: {:?}",
        schema.enums.keys().collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn a_column_default_that_differs_is_drift() {
    // Measured before this was carried: a live `created_at TIMESTAMPTZ NOT NULL
    // DEFAULT now()` and a migration's `created_at TIMESTAMPTZ NOT NULL` agreed
    // on data_type, udt_name, nullability, every length and both precisions --
    // byte-identical `Column`s. `baseline` would have affirmed that the
    // migrations produced a schema they did not, which is the one thing it
    // exists to refuse.
    let pool = pool().await;
    for statement in [
        "DROP SCHEMA IF EXISTS pneuma_fp_def CASCADE",
        "CREATE SCHEMA pneuma_fp_def",
        "DROP SCHEMA IF EXISTS pneuma_fp_nodef CASCADE",
        "CREATE SCHEMA pneuma_fp_nodef",
        "CREATE TABLE pneuma_fp_def.t (id int, created_at timestamptz NOT NULL DEFAULT now())",
        "CREATE TABLE pneuma_fp_nodef.t (id int, created_at timestamptz NOT NULL)",
    ] {
        if let Err(error) = pool.execute(statement).await {
            panic!("setup failed on {statement:?}: {error}");
        }
    }

    let (Ok(with_default), Ok(without)) = (
        introspect(&pool, "pneuma_fp_def").await,
        introspect(&pool, "pneuma_fp_nodef").await,
    ) else {
        panic!("should introspect both");
    };

    let Some(defaulted) = with_default
        .tables
        .get("t")
        .and_then(|table| table.columns.get("created_at"))
    else {
        panic!("the column was read");
    };
    assert_eq!(
        defaulted.default.as_deref(),
        Some("now()"),
        "the default is captured verbatim as Postgres renders it"
    );

    let differences = with_default.differences(&without);
    let [Difference::ColumnMismatch {
        table,
        column,
        found,
        expected,
    }] = differences.as_slice()
    else {
        panic!("exactly one column mismatch, got: {differences:?}");
    };
    assert_eq!((table.as_str(), column.as_str()), ("t", "created_at"));
    // The report carries both sides, so an operator can see *which* default
    // differs rather than only that something does.
    assert_eq!(found.default.as_deref(), Some("now()"));
    assert_eq!(expected.default, None);
}
