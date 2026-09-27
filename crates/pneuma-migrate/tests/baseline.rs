//! Drives `baseline` against a real Postgres.
//!
//! The property that matters is not "it inserts three rows". It is that **sqlx
//! afterwards agrees the migrations have run**: `Migrator::run` on a baselined
//! database must do nothing and succeed. That is the claim a baseline makes,
//! and it is only true if the version, the description and the checksum all
//! match what sqlx would have written itself. A test asserting the row count
//! would pass with the checksum wrong, and the failure would appear later, on
//! whichever machine next ran a migration.
//!
//! ```sh
//! docker run -d --name pn-pg -p 5433:5432 -e POSTGRES_PASSWORD=pneuma postgres:16
//! PNEUMA_TEST_DATABASE_URL=postgres://postgres:pneuma@127.0.0.1:5433/postgres \
//!     cargo test -p pneuma-migrate --test baseline
//! ```

use pneuma_migrate::{baseline, expected_schema, introspect, BaselineError, Plan};
use sqlx::{Executor, PgPool};

/// The migrations that transcribe what the original migration tool already created.
///
/// What `original_shaped` below builds, and therefore what these tests compare
/// against. Not `pneuma_store::migrator()`, which also carries
/// `0003_submission` -- a table the original migration tool never created, so no fixture here has
/// it and no production database does either. See `pneuma_store::ORIGINAL_THROUGH`.
fn original_era() -> sqlx::migrate::Migrator {
    pneuma_store::original_migrator()
}

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

/// A schema in the state the original migration tool leaves behind: correct tables, no sqlx rows.
async fn original_shaped(pool: &PgPool, schema: &str) {
    for statement in [
        format!("DROP SCHEMA IF EXISTS {schema} CASCADE"),
        format!("CREATE SCHEMA {schema}"),
        format!("SET search_path TO {schema}"),
        // The migrations `original_migrator` carries, applied by hand -- so the
        // schema is right and `_sqlx_migrations` is absent, which is the state
        // this fixture is named for. Taken from the migrator rather than from
        // two `include_str!`s, because "the shape the original migration tool leaves" now has a
        // definition (`pneuma_store::ORIGINAL_THROUGH`) and a hand-written list
        // beside it is a second definition that can disagree with the first.
        pneuma_store::original_migrator()
            .iter()
            .map(|migration| migration.sql.to_string())
            .collect::<Vec<_>>()
            .join(";\n"),
        "SET search_path TO public".to_owned(),
    ] {
        if let Err(error) = pool.execute(statement.as_str()).await {
            panic!("setup failed on {statement:?}: {error}");
        }
    }
}

async fn empty_schema(pool: &PgPool, schema: &str) {
    for statement in [
        format!("DROP SCHEMA IF EXISTS {schema} CASCADE"),
        format!("CREATE SCHEMA {schema}"),
    ] {
        if let Err(error) = pool.execute(statement.as_str()).await {
            panic!("setup failed on {statement:?}: {error}");
        }
    }
}

#[tokio::test]
async fn a_baselined_database_is_one_sqlx_will_not_migrate_again() {
    let pool = pool().await;
    original_shaped(&pool, "pn_base_live").await;

    let decided = match baseline(&pool, "pn_base_live", "pn_base_scratch", &original_era()).await {
        Ok(decided) => decided,
        Err(error) => panic!("a schema the migrations produce must be baselineable: {error}"),
    };
    assert_eq!(decided, Plan::Record(vec![1, 2]));

    // The real assertion. If any checksum is wrong sqlx raises VersionMismatch;
    // if a version is missing it tries to apply it and fails on the existing
    // tables. Doing nothing and succeeding is the only outcome that means the
    // rows are right.
    let Ok(mut connection) = pool.acquire().await else {
        panic!("should acquire");
    };
    if let Err(error) = connection.execute("SET search_path TO pn_base_live").await {
        panic!("could not set the search path: {error}");
    }
    if let Err(error) = &original_era().run(&mut *connection).await {
        panic!("sqlx does not accept the baseline this wrote: {error}");
    }
    if let Err(error) = connection.execute("SET search_path TO DEFAULT").await {
        panic!("could not reset the search path: {error}");
    }
    drop(connection);

    // And running it did not alter the schema -- proof the migrations were
    // skipped rather than somehow re-applied.
    let Ok(after) = introspect(&pool, "pn_base_live").await else {
        panic!("should introspect");
    };
    assert!(after.tables.contains_key("noderun"));
    assert!(after.tables.contains_key("noderun_history"));
}

#[tokio::test]
async fn baselining_twice_is_a_no_op_rather_than_an_error() {
    let pool = pool().await;
    original_shaped(&pool, "pn_base_twice").await;
    let migrator = &original_era();

    let first = match baseline(&pool, "pn_base_twice", "pn_base_twice_s", migrator).await {
        Ok(first) => first,
        Err(error) => panic!("first baseline should succeed: {error}"),
    };
    assert_eq!(first, Plan::Record(vec![1, 2]));

    let second = match baseline(&pool, "pn_base_twice", "pn_base_twice_s", migrator).await {
        Ok(second) => second,
        Err(error) => panic!("a repeated baseline must be safe, not an error: {error}"),
    };
    assert_eq!(second, Plan::AlreadyRecorded);
}

#[tokio::test]
async fn an_unmigrated_database_is_refused_and_not_marked_as_current() {
    // The failure this whole module exists to prevent: marking an empty schema
    // as migrated means the next real migration runs against something it does
    // not expect.
    let pool = pool().await;
    empty_schema(&pool, "pn_base_empty").await;

    let Err(error) = baseline(&pool, "pn_base_empty", "pn_base_empty_s", &original_era()).await
    else {
        panic!("an unmigrated schema must not be baselined");
    };
    let BaselineError::SchemaMismatch(differences) = &error else {
        panic!("wrong variant: {error:?}");
    };
    assert!(
        !differences.is_empty(),
        "the report names what is missing, not just that something is"
    );
    assert!(error.to_string().contains("noderun"), "{error}");

    // Nothing was written. A refusal that still created the tracking table
    // would leave the next attempt in a state this tool refuses to touch.
    let recorded: Option<i64> = match sqlx::query_scalar(
        "SELECT count(*) FROM information_schema.tables \
         WHERE table_schema = 'pn_base_empty' AND table_name = '_sqlx_migrations'",
    )
    .fetch_one(&pool)
    .await
    {
        Ok(count) => count,
        Err(error) => panic!("could not check: {error}"),
    };
    assert_eq!(recorded, Some(0), "no tracking table was left behind");
}

#[tokio::test]
async fn a_database_sqlx_has_fully_migrated_is_already_recorded_not_an_error() {
    // Migrated properly by sqlx, then handed to `baseline`. Every version is
    // already recorded and the schema matches, so this is the `AlreadyRecorded`
    // path -- the interesting half is that it reaches it via a database sqlx
    // built, not one this test faked.
    let pool = pool().await;
    empty_schema(&pool, "pn_base_managed").await;
    let Ok(mut connection) = pool.acquire().await else {
        panic!("should acquire");
    };
    if let Err(error) = connection
        .execute("SET search_path TO pn_base_managed")
        .await
    {
        panic!("could not set the search path: {error}");
    }
    // The **full** migrator, because "fully migrated" is what this test is
    // named for and what production reaches: adopted at `[1, 2]`, then
    // `sqlx migrate run` applies `0003`. Migrating with the adoption subset
    // instead would model a state nothing ever gets to, and would leave the
    // case that actually broke -- a database ahead of the adoption -- untested.
    if let Err(error) = pneuma_store::migrator().run(&mut *connection).await {
        panic!("sqlx should migrate a fresh schema: {error}");
    }
    if let Err(error) = connection.execute("SET search_path TO DEFAULT").await {
        panic!("could not reset: {error}");
    }
    drop(connection);

    // Baselining it is a no-op, and this is the regression test for a real
    // break: with the schema checked before the record, `expected_schema` had
    // no `submission` while the live schema did, so this refused a correct
    // database with `SchemaMismatch` and exit code 3. On the second run of any
    // deploy script that baselines before migrating, which is every deploy
    // after the first.
    let decided = match baseline(
        &pool,
        "pn_base_managed",
        "pn_base_managed_s",
        &original_era(),
    )
    .await
    {
        Ok(decided) => decided,
        Err(error) => panic!("a fully migrated database is already recorded: {error}"),
    };
    assert_eq!(decided, Plan::AlreadyRecorded);

    // And repeatedly, which is the property the module claims.
    let Ok(again) = baseline(
        &pool,
        "pn_base_managed",
        "pn_base_managed_s",
        &original_era(),
    )
    .await
    else {
        panic!("baselining twice is a no-op");
    };
    assert_eq!(again, Plan::AlreadyRecorded);
}

#[tokio::test]
async fn a_partly_recorded_database_is_refused_and_names_both_sides() {
    let pool = pool().await;
    original_shaped(&pool, "pn_base_partial").await;
    // Exactly the state `sqlx migrate run` leaves when it applied 1 and has not
    // yet applied 2. Written directly because reaching it otherwise would mean
    // shipping a deliberately failing migration.
    for statement in [
        "CREATE TABLE pn_base_partial._sqlx_migrations ( \
            version BIGINT PRIMARY KEY, description TEXT NOT NULL, \
            installed_on TIMESTAMPTZ NOT NULL DEFAULT now(), \
            success BOOLEAN NOT NULL, checksum BYTEA NOT NULL, \
            execution_time BIGINT NOT NULL)",
        "INSERT INTO pn_base_partial._sqlx_migrations \
            (version, description, success, checksum, execution_time) \
            VALUES (1, 'noderun', TRUE, '\\x00'::bytea, -1)",
    ] {
        if let Err(error) = pool.execute(statement).await {
            panic!("setup failed on {statement:?}: {error}");
        }
    }

    let Err(error) = baseline(
        &pool,
        "pn_base_partial",
        "pn_base_partial_s",
        &original_era(),
    )
    .await
    else {
        panic!("a partly recorded database must be refused");
    };
    let BaselineError::PartiallyRecorded { recorded, embedded } = &error else {
        panic!("wrong variant: {error:?}");
    };
    assert_eq!(
        (recorded.as_slice(), embedded.as_slice()),
        (&[1][..], &[1, 2][..])
    );
}

#[tokio::test]
async fn deriving_the_expectation_leaves_no_scratch_schema_behind() {
    // It runs real DDL in a real schema. If it leaked, a second run would build
    // on the leftover and derive an expectation that is quietly wrong -- and
    // the scratch name is fixed, so it would leak into every later run.
    let pool = pool().await;
    let derived = match expected_schema(&pool, "pn_base_derived", &original_era()).await {
        Ok(derived) => derived,
        Err(error) => panic!("should derive the expected schema: {error}"),
    };
    assert!(
        derived.tables.contains_key("noderun") && derived.tables.contains_key("noderun_history"),
        "the derivation actually ran the migrations: {:?}",
        derived.tables.keys().collect::<Vec<_>>()
    );
    assert_eq!(derived.enums.len(), 2, "nodestatus and nodetype");

    let left: Option<i64> = match sqlx::query_scalar(
        "SELECT count(*) FROM information_schema.schemata WHERE schema_name = 'pn_base_derived'",
    )
    .fetch_one(&pool)
    .await
    {
        Ok(count) => count,
        Err(error) => panic!("could not check: {error}"),
    };
    assert_eq!(left, Some(0), "the scratch schema was dropped");
}

#[tokio::test]
async fn a_migration_that_fails_leaves_no_scratch_schema_behind_either() {
    // The cleanup is outside the success path on purpose. A derivation that
    // failed halfway is exactly when a leftover schema is most damaging: the
    // scratch name is fixed, so the next run would migrate on top of a partial
    // schema and derive an expectation that is wrong without being empty.
    //
    // Built from a directory rather than `migrate!` so the broken SQL is not
    // compiled into the crate. The failure is the database rejecting it, which
    // is the same shape as a genuinely bad migration reaching production.
    let directory = std::env::temp_dir().join("pn_base_broken_migrations");
    if let Err(error) = std::fs::create_dir_all(&directory) {
        panic!("could not create {directory:?}: {error}");
    }
    if let Err(error) = std::fs::write(
        directory.join("0001_broken.sql"),
        "CREATE TABLE fine (id int);\nTHIS IS NOT SQL;\n",
    ) {
        panic!("could not write the migration: {error}");
    }
    let broken = match sqlx::migrate::Migrator::new(directory.as_path()).await {
        Ok(broken) => broken,
        Err(error) => panic!("could not load the migrations: {error}"),
    };

    let pool = pool().await;
    let Err(error) = expected_schema(&pool, "pn_base_broken", &broken).await else {
        panic!("a migration that does not run cannot yield an expectation");
    };
    assert!(
        matches!(error, BaselineError::Migrate(_)),
        "wrong variant: {error:?}"
    );

    let left: Option<i64> = match sqlx::query_scalar(
        "SELECT count(*) FROM information_schema.schemata WHERE schema_name = 'pn_base_broken'",
    )
    .fetch_one(&pool)
    .await
    {
        Ok(count) => count,
        Err(error) => panic!("could not check: {error}"),
    };
    assert_eq!(
        left,
        Some(0),
        "the scratch schema was dropped despite the failure"
    );

    // The pool's connection is still usable -- `search_path` was reset before
    // it went back, so the next borrower is not pointed at a dropped schema.
    let Ok(after) = introspect(&pool, "public").await else {
        panic!("the connection was left in a usable state");
    };
    assert!(
        !after.tables.contains_key("fine"),
        "nothing leaked to public"
    );
}

#[tokio::test]
async fn a_scratch_name_equal_to_the_target_leaves_the_target_intact() {
    // The guard proved against a real schema rather than only in a unit test:
    // the first statement of a derivation is `DROP SCHEMA ... CASCADE`, so a
    // regression here is silent until it has destroyed something.
    let pool = pool().await;
    original_shaped(&pool, "pn_base_selfdrop").await;

    let Err(error) = baseline(
        &pool,
        "pn_base_selfdrop",
        "pn_base_selfdrop",
        &original_era(),
    )
    .await
    else {
        panic!("baselining into itself must be refused");
    };
    assert!(
        matches!(error, BaselineError::UnsafeScratch { .. }),
        "wrong variant: {error:?}"
    );

    // Still there, with its tables.
    let Ok(after) = introspect(&pool, "pn_base_selfdrop").await else {
        panic!("should introspect");
    };
    assert!(
        after.tables.contains_key("noderun") && after.tables.contains_key("noderun_history"),
        "the target schema survived: {:?}",
        after.tables.keys().collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn a_scratch_schema_that_already_holds_something_is_refused_intact() {
    // The case the name checks cannot see, and the one that actually loses
    // data. `public`, `pg_*` and the target are refused by name; a real
    // application schema under any other name was not, and the first statement
    // of a derivation is `DROP SCHEMA ... CASCADE`. One mistyped `--scratch`
    // and it is gone, with no error and no warning -- the run would even
    // succeed.
    let pool = pool().await;
    original_shaped(&pool, "pn_base_occupied").await;
    empty_schema(&pool, "pn_base_target").await;

    let Err(error) = baseline(&pool, "pn_base_target", "pn_base_occupied", &original_era()).await
    else {
        panic!("a populated scratch schema must be refused");
    };
    let BaselineError::UnsafeScratch { scratch, reason } = &error else {
        panic!("wrong variant: {error:?}");
    };
    assert_eq!(scratch, "pn_base_occupied");
    assert!(reason.contains("already holds objects"), "{reason}");
    // The operator reads this literally, so it must read like a sentence. A
    // multi-line string literal that rustfmt reflows onto one line keeps the
    // indentation of its continuations, which put a 22-space gap in the middle
    // of this message.
    assert!(!reason.contains("  "), "no run of spaces: {reason:?}");

    // Untouched, which is the whole assertion. `introspect` reading its tables
    // back is what says `DROP SCHEMA` never ran.
    let Ok(after) = introspect(&pool, "pn_base_occupied").await else {
        panic!("should introspect");
    };
    assert!(
        after.tables.contains_key("noderun") && after.tables.contains_key("noderun_history"),
        "the occupied schema survived: {:?}",
        after.tables.keys().collect::<Vec<_>>()
    );

    // And an empty one is still ordinary scratch space, so the guard has not
    // simply refused everything.
    empty_schema(&pool, "pn_base_free").await;
    original_shaped(&pool, "pn_base_ok").await;
    let Ok(Plan::Record(_)) = baseline(&pool, "pn_base_ok", "pn_base_free", &original_era()).await
    else {
        panic!("an empty scratch schema is usable");
    };
}

#[tokio::test]
async fn baselining_adopts_the_database_and_migrate_run_finishes_the_job() {
    // The sequence a real adoption goes through, end to end, and the one that
    // catches the mistake this test was written after making: adding
    // `0003_submission` -- a table the original migration tool never created -- made `baseline`
    // compare a production-shaped database against a migration set describing
    // a table it cannot have.
    //
    // Recording all three would have been the dangerous outcome, not the
    // failing one: `sqlx migrate run` would then see `0003` already applied,
    // skip creating `submission`, and every submission query would fail at run
    // time on a deployment whose baseline reported success.
    let pool = pool().await;
    original_shaped(&pool, "pn_base_adopt").await;

    // 1. Adopt. Only the migrations describing what is already there.
    let Ok(Plan::Record(recorded)) = baseline(
        &pool,
        "pn_base_adopt",
        "pn_base_adopt_scratch",
        &original_era(),
    )
    .await
    else {
        panic!("a production-shaped database is adoptable");
    };
    assert_eq!(
        recorded,
        vec![1, 2],
        "exactly original-shaped migrations, and not 0003"
    );

    let sql = "SELECT count(*) FROM information_schema.tables                WHERE table_schema = 'pn_base_adopt' AND table_name = 'submission'";
    let Ok(before) = sqlx::query_scalar::<_, i64>(sql).fetch_one(&pool).await else {
        panic!("should count");
    };
    assert_eq!(before, 0, "and the table it does not describe is not there");

    // 2. Migrate forward. The full migrator, as a deployment would run it.
    let Ok(mut connection) = pool.acquire().await else {
        panic!("should acquire");
    };
    for statement in ["SET search_path TO pn_base_adopt"] {
        if let Err(error) = connection.execute(statement).await {
            panic!("could not set the search path: {error}");
        }
    }
    if let Err(error) = pneuma_store::migrator().run(&mut *connection).await {
        panic!("the remaining migrations should apply: {error}");
    }
    drop(connection);

    // 3. `submission` exists, created by the migration rather than claimed by
    //    the baseline.
    let Ok(after) = sqlx::query_scalar::<_, i64>(sql).fetch_one(&pool).await else {
        panic!("should count");
    };
    assert_eq!(after, 1, "migrate run created what baseline did not claim");

    let versions = "SELECT version FROM pn_base_adopt._sqlx_migrations ORDER BY version";
    let Ok(applied) = sqlx::query_scalar::<_, i64>(versions)
        .fetch_all(&pool)
        .await
    else {
        panic!("should read the tracking table");
    };
    assert_eq!(applied, vec![1, 2, 3, 4], "and all four are now recorded");

    // 4. And the rename landed. This is the assertion the original plan:304` asks
    //    for: an original-era database, adopted without running anything against
    //    it, then carried all the way to the schema this port actually queries.
    //    `0004_rename.sql` is the first migration since that gate was written
    //    that could break it, because it is the first one to touch a table
    //    `baseline` claimed rather than create a new one.
    let Ok(after) = introspect(&pool, "pn_base_adopt").await else {
        panic!("should introspect");
    };
    assert!(
        after.tables.contains_key("node_run") && after.tables.contains_key("node_run_history"),
        "the rename reached an adopted database: {:?}",
        after.tables.keys().collect::<Vec<_>>()
    );
    assert!(
        !after.tables.contains_key("noderun"),
        "and left nothing under the old name"
    );
}

#[tokio::test]
async fn a_schema_holding_only_types_is_refused_too() {
    // `0001_noderun.sql` creates two enums before its first table, so "a
    // half-applied migration" and "a schema of only types" are the same shape.
    // Counting `pg_class` alone read that as empty and dropped it.
    let pool = pool().await;
    empty_schema(&pool, "pn_base_typesonly").await;
    if let Err(error) = pool
        .execute("CREATE TYPE pn_base_typesonly.colour AS ENUM ('red')")
        .await
    {
        panic!("could not create a type: {error}");
    }
    original_shaped(&pool, "pn_base_typesonly_target").await;

    let Err(error) = baseline(
        &pool,
        "pn_base_typesonly_target",
        "pn_base_typesonly",
        &original_era(),
    )
    .await
    else {
        panic!("a schema holding a type must be refused");
    };
    assert!(
        matches!(error, BaselineError::UnsafeScratch { .. }),
        "wrong variant: {error:?}"
    );
}

#[tokio::test]
async fn a_derivation_killed_halfway_does_not_wedge_every_later_run() {
    // The recovery the guard would otherwise have taken away, and the reason
    // the scratch schema is stamped. `expected_schema` drops on the way in as
    // well as on the way out precisely so a run killed between the two heals
    // itself -- and a guard that refuses any populated scratch schema turns
    // that one crash into a permanent refusal, needing an operator with psql.
    //
    // The wreckage is built by hand rather than by killing a process: a schema
    // carrying the marker and some leftover contents is exactly what a `SIGKILL`
    // mid-derivation leaves.
    let pool = pool().await;
    original_shaped(&pool, "pn_base_heal_target").await;
    empty_schema(&pool, "pn_base_heal_scratch").await;
    for statement in [
        "CREATE TABLE pn_base_heal_scratch.leftover (x int)".to_owned(),
        format!(
            "COMMENT ON SCHEMA pn_base_heal_scratch IS '{}'",
            pneuma_migrate::SCRATCH_MARKER
        ),
    ] {
        if let Err(error) = pool.execute(statement.as_str()).await {
            panic!("could not build the wreckage: {error}");
        }
    }

    let Ok(Plan::Record(_)) = baseline(
        &pool,
        "pn_base_heal_target",
        "pn_base_heal_scratch",
        &original_era(),
    )
    .await
    else {
        panic!("a scratch schema this tool left behind is reusable");
    };

    // An unstamped schema with the same contents is still refused, so the
    // recovery is keyed on the marker and not on the contents being harmless.
    empty_schema(&pool, "pn_base_heal_foreign").await;
    if let Err(error) = pool
        .execute("CREATE TABLE pn_base_heal_foreign.leftover (x int)")
        .await
    {
        panic!("could not seed: {error}");
    }
    original_shaped(&pool, "pn_base_heal_target2").await;
    let Err(BaselineError::UnsafeScratch { .. }) = baseline(
        &pool,
        "pn_base_heal_target2",
        "pn_base_heal_foreign",
        &original_era(),
    )
    .await
    else {
        panic!("an unstamped schema with contents is still someone else's");
    };
}

#[tokio::test]
async fn no_migrations_is_refused_before_anything_is_dropped() {
    // The order matters, not just the answer. `plan` has always refused an
    // empty migrator -- but it runs *after* `expected_schema` has dropped the
    // scratch schema, recreated it and run every migration into it. So the tool
    // did all of its destructive work and then reported that there was nothing
    // to do. Here the scratch schema is created first and must still be there
    // afterwards, untouched, with the marker it was given.
    let pool = pool().await;
    original_shaped(&pool, "pn_base_nomig_target").await;
    empty_schema(&pool, "pn_base_nomig_scratch").await;
    if let Err(error) = pool
        .execute("CREATE TABLE pn_base_nomig_scratch.marker (x int)")
        .await
    {
        panic!("could not mark the scratch schema: {error}");
    }

    // Created here rather than assumed: a missing directory makes
    // `Migrator::new` fail, and the test would then be refused for a reason
    // that has nothing to do with what it is checking.
    let directory = std::env::temp_dir().join("pn_base_no_migrations");
    if let Err(error) = std::fs::create_dir_all(&directory) {
        panic!("could not create {}: {error}", directory.display());
    }
    let empty = match sqlx::migrate::Migrator::new(directory.as_path()).await {
        Ok(migrator) => migrator,
        Err(error) => panic!("an empty directory is a valid migrator: {error}"),
    };
    let Err(error) = baseline(
        &pool,
        "pn_base_nomig_target",
        "pn_base_nomig_scratch",
        &empty,
    )
    .await
    else {
        panic!("nothing to baseline must be refused");
    };
    // `UnsafeScratch` would also be a refusal, and would mean the *other*
    // guard fired -- so the variant is what says which check ran first.
    assert!(
        matches!(error, BaselineError::NoMigrations),
        "wrong variant: {error:?}"
    );

    let Ok(after) = introspect(&pool, "pn_base_nomig_scratch").await else {
        panic!("should introspect");
    };
    assert!(
        after.tables.contains_key("marker"),
        "the scratch schema was never dropped: {:?}",
        after.tables.keys().collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn a_failed_derivation_does_not_leave_the_migration_lock_held() {
    // `Migrator::run` takes a session-level `pg_advisory_lock` and releases it
    // only on success, so a failed run on a *pooled* connection would hold it
    // for that connection's whole life and block every later migration against
    // the database -- including from other processes.
    let directory = std::env::temp_dir().join("pn_base_lock_migrations");
    if let Err(error) = std::fs::create_dir_all(&directory) {
        panic!("could not create {directory:?}: {error}");
    }
    if let Err(error) = std::fs::write(directory.join("0001_broken.sql"), "THIS IS NOT SQL;\n") {
        panic!("could not write the migration: {error}");
    }
    let broken = match sqlx::migrate::Migrator::new(directory.as_path()).await {
        Ok(broken) => broken,
        Err(error) => panic!("could not load: {error}"),
    };

    let pool = pool().await;
    let Err(_) = expected_schema(&pool, "pn_base_lock", &broken).await else {
        panic!("the migration should have failed");
    };

    // The observable consequence, not the mechanism: a real migration still
    // runs. With the lock leaked this blocks until the test harness times out.
    empty_schema(&pool, "pn_base_lock_after").await;
    let Ok(mut connection) = pool.acquire().await else {
        panic!("should acquire");
    };
    if let Err(error) = connection
        .execute("SET search_path TO pn_base_lock_after")
        .await
    {
        panic!("could not set the search path: {error}");
    }
    if let Err(error) = &original_era().run(&mut *connection).await {
        panic!("the migration lock was not released: {error}");
    }
    if let Err(error) = connection.execute("SET search_path TO DEFAULT").await {
        panic!("could not reset: {error}");
    }
    drop(connection);

    // Deliberately no `SELECT count(*) FROM pg_locks` here. An advisory lock is
    // database-wide, and the sibling tests in this file hold one legitimately
    // while their own `Migrator::run` is in flight -- so that assertion passes
    // alone and fails under `cargo test`'s default parallelism. Measured, not
    // assumed: it was written, it failed in parallel, and it passed under
    // `--test-threads=1`.
    //
    // The migration completing above is the property that matters anyway. With
    // the lock leaked it does not complete; it blocks until the harness gives
    // up, which is the production symptom too.
}

#[tokio::test]
async fn a_reversible_migration_is_recorded_once_not_once_per_direction() {
    // `Migrator::iter` yields the down migration too, and a reversible pair
    // shares one `version` -- measured, it iterates as `[(1, down), (1, up)]`.
    // Unfiltered, the insert loop would write `version` twice and fail on the
    // primary key, and `recorded == embedded` could never hold again.
    let directory = std::env::temp_dir().join("pn_base_reversible_migrations");
    if let Err(error) = std::fs::create_dir_all(&directory) {
        panic!("could not create {directory:?}: {error}");
    }
    for (name, body) in [
        (
            "0001_t.up.sql",
            "CREATE TABLE reversible_t (id int primary key);",
        ),
        ("0001_t.down.sql", "DROP TABLE reversible_t;"),
    ] {
        if let Err(error) = std::fs::write(directory.join(name), body) {
            panic!("could not write {name}: {error}");
        }
    }
    let reversible = match sqlx::migrate::Migrator::new(directory.as_path()).await {
        Ok(reversible) => reversible,
        Err(error) => panic!("could not load: {error}"),
    };
    // The premise: both directions really are present under one version.
    let versions: Vec<i64> = reversible.iter().map(|m| m.version).collect();
    assert_eq!(versions, vec![1, 1], "a reversible pair shares its version");

    let pool = pool().await;
    empty_schema(&pool, "pn_base_rev").await;
    for statement in [
        "SET search_path TO pn_base_rev",
        "CREATE TABLE reversible_t (id int primary key)",
        "SET search_path TO public",
    ] {
        if let Err(error) = pool.execute(statement).await {
            panic!("setup failed on {statement:?}: {error}");
        }
    }

    let decided = match baseline(&pool, "pn_base_rev", "pn_base_rev_s", &reversible).await {
        Ok(decided) => decided,
        Err(error) => panic!("a reversible migration must baseline once: {error}"),
    };
    assert_eq!(decided, Plan::Record(vec![1]), "one row, not two");

    // And it is idempotent, which the duplicate version would have broken.
    let repeated = match baseline(&pool, "pn_base_rev", "pn_base_rev_s", &reversible).await {
        Ok(repeated) => repeated,
        Err(error) => panic!("should be a no-op: {error}"),
    };
    assert_eq!(repeated, Plan::AlreadyRecorded);
}
