//! Marking a database as already migrated, without running the migrations.
//!
//! For the environments the original migration tool migrated before this port existed. Their
//! schema is already correct; what they lack is the `_sqlx_migrations` rows
//! that say so. Writing those rows is a claim — "these migrations have run" —
//! and the next real migration is applied on the strength of it. So the claim
//! has to be checked before it is written, and the check is the whole module.
//!
//! # The check is a comparison, not a checksum
//!
//! What the live database has, against what the migrations produce. The second
//! is derived by *running* them into a scratch schema and reading it back with
//! [`introspect()`], never by writing an expectation down: a hand-written
//! expectation is a second copy of someone's reading of the SQL, and it drifts
//! silently from the SQL it claims to describe.
//!
//! # The decision is separated from the doing
//!
//! [`plan`] is pure and takes the three facts that decide the outcome. Every
//! rule about when a baseline is refused therefore has a test that needs no
//! database, and [`baseline`] is left as the part that gathers and writes.

use sqlx::migrate::{Migrate, Migrator};
use sqlx::{Executor, PgPool, Postgres, Transaction};

use crate::introspect::{introspect, IntrospectError};
use crate::schema::{Difference, Schema};

/// What a baseline will do, decided before anything is written.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Plan {
    /// Record these migration versions, in this order. Nothing is executed.
    Record(Vec<i64>),
    /// Every embedded migration is already recorded. Doing it again is a no-op,
    /// which makes a repeated `baseline` safe rather than an error.
    AlreadyRecorded,
}

/// Why a database could not be baselined.
#[derive(Debug, thiserror::Error)]
pub enum BaselineError {
    /// There are no migrations to record. Baselining nothing would create the
    /// tracking table and claim nothing, which is a silent no-op dressed as
    /// success.
    #[error("no migrations are embedded, so there is nothing to baseline")]
    NoMigrations,

    /// The live schema is not what the migrations produce. Reported in full
    /// rather than as a count: the operator has to decide whether the database
    /// or the migrations are wrong, and cannot do that from a number.
    #[error(
        "the live schema does not match the migrations, so it cannot be \
         baselined ({} difference(s)): {}",
        .0.len(),
        .0.iter().map(ToString::to_string).collect::<Vec<_>>().join("; ")
    )]
    SchemaMismatch(Vec<Difference>),

    /// Some migrations are recorded and some are not.
    ///
    /// Refused rather than topped up. A partial record means the database has
    /// been managed by `sqlx migrate run` already, so the missing versions are
    /// missing for a reason -- they have genuinely not been applied, and
    /// recording them would skip them forever.
    #[error(
        "{} of {} migrations are already recorded, so this database is \
         already managed by sqlx and must be migrated rather than baselined \
         (recorded: {recorded:?}, embedded: {embedded:?})",
        recorded.len(),
        embedded.len()
    )]
    PartiallyRecorded {
        /// Versions already in `_sqlx_migrations`.
        recorded: Vec<i64>,
        /// Versions the binary carries.
        embedded: Vec<i64>,
    },

    /// The scratch schema is one that must not be dropped.
    ///
    /// Deriving the expectation begins by dropping the scratch schema, so the
    /// name is a destructive argument. It arrives from a CLI flag or the
    /// environment, where it can be mistyped, defaulted, or left equal to the
    /// target -- and this is the tool whose entire purpose is refusing to touch
    /// a database it cannot verify.
    #[error("refusing to use {scratch:?} as the scratch schema: {reason}")]
    UnsafeScratch {
        /// The name that was given.
        scratch: String,
        /// Why it cannot be used.
        reason: &'static str,
    },

    /// The live schema could not be read.
    #[error("could not read the live schema: {0}")]
    Introspect(#[from] IntrospectError),

    /// The migrations could not be run into the scratch schema.
    #[error("could not derive the expected schema: {0}")]
    Migrate(#[from] sqlx::migrate::MigrateError),

    /// The database refused, or was unreachable.
    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),
}

/// Decides what to do, from the three facts that determine it.
///
/// Takes the differences rather than the two schemas so that the rule and the
/// comparison are tested separately -- `differences` is
/// [`Schema::differences`]'s job and has its own tests.
///
/// "Already adopted" is decided **before** the schema, and that ordering is the
/// opposite of what it was. The schema check used to come first, on the
/// argument that a database whose rows are right and whose schema is wrong must
/// not be affirmed -- which held while `embedded` was every migration this
/// repository has.
///
/// It is not every migration any more. `baseline` adopts a database only
/// through `pneuma_store::ORIGINAL_THROUGH`, so a database that has since run
/// `sqlx migrate run` is legitimately *ahead* of the expectation: it has
/// `submission`, and the adoption subset does not describe it. Comparing them
/// then answers the wrong question, and answering it made `baseline` refuse a
/// correct database with `SchemaMismatch` -- on the second run of a deploy
/// script that baselines before migrating, which is to say on every deploy
/// after the first.
///
/// So once every embedded version is recorded, this returns
/// [`Plan::AlreadyRecorded`] and says nothing about the schema, because it can
/// no longer say anything true about it. Drift in an *adopted* database is
/// `fingerprint`'s to report, and drift in an *unadopted* one is still caught
/// below, where the comparison still means what it says.
pub fn plan(
    differences: &[Difference],
    recorded: &[i64],
    embedded: &[i64],
) -> Result<Plan, BaselineError> {
    if embedded.is_empty() {
        return Err(BaselineError::NoMigrations);
    }
    // A superset, not equality. After adoption the database goes on migrating,
    // so `recorded` grows past `embedded` -- and `recorded == embedded` would
    // have called that `PartiallyRecorded` and refused it.
    if embedded.iter().all(|version| recorded.contains(version)) {
        return Ok(Plan::AlreadyRecorded);
    }
    if !differences.is_empty() {
        return Err(BaselineError::SchemaMismatch(differences.to_vec()));
    }
    if recorded.is_empty() {
        return Ok(Plan::Record(embedded.to_vec()));
    }
    Err(BaselineError::PartiallyRecorded {
        recorded: recorded.to_vec(),
        embedded: embedded.to_vec(),
    })
}

/// Refuses a scratch name that would destroy something.
///
/// The first statement of a derivation is `DROP SCHEMA ... CASCADE` on this
/// name. `baseline(pool, "public", "public", ..)` -- one mis-set variable --
/// would therefore destroy the production schema before a single check ran.
/// The guard costs a comparison and removes that entirely.
fn ensure_scratch_is_safe(scratch: &str, target: Option<&str>) -> Result<(), BaselineError> {
    let unsafe_because = |reason| {
        Err(BaselineError::UnsafeScratch {
            scratch: scratch.to_owned(),
            reason,
        })
    };
    if scratch.is_empty() {
        return unsafe_because("it is empty");
    }
    if target == Some(scratch) {
        return unsafe_because("it is the schema being baselined, which would be dropped");
    }
    // Case-insensitively, because an unquoted name in a shell is folded to
    // lower case by Postgres while this module quotes it -- so `PUBLIC` from a
    // CLI and `public` in the database are the same schema often enough that
    // the difference must not be what protects it.
    let folded = scratch.to_ascii_lowercase();
    if folded == "public" {
        return unsafe_because("the public schema is not scratch space");
    }
    if folded == "information_schema" || folded.starts_with("pg_") {
        return unsafe_because("it is a system schema");
    }
    Ok(())
}

/// The migrations that go *forward*, which are the only ones a baseline records.
///
/// `Migrator::iter` yields down migrations too, and a reversible pair shares
/// one `version` -- measured: a `0003_x.up.sql`/`0003_x.down.sql` pair iterates
/// as `[(3, down), (3, up)]`. Left unfiltered, `embedded` becomes `[3, 3]`, so
/// `recorded == embedded` can never hold and [`Plan::AlreadyRecorded`] becomes
/// unreachable; worse, the insert loop would write `version` twice and fail the
/// whole baseline on the primary key. `Migrator::run_direct` skips them the
/// same way, which is the behaviour being matched rather than invented.
fn up_migrations(migrator: &Migrator) -> impl Iterator<Item = &sqlx::migrate::Migration> {
    migrator
        .iter()
        .filter(|migration| !migration.migration_type.is_down_migration())
}

/// sqlx's own bookkeeping table.
///
/// Excluded from the fingerprint on **both** sides. Deriving the expectation
/// runs the migrations, and `Migrator::run` creates this table as it goes, so
/// the expectation always contains it; a database the original migration tool migrated never does,
/// and one sqlx already manages always does. Comparing it would therefore
/// report a difference in every direction except the one nobody runs.
///
/// It is not drift being swept up. The table's shape is sqlx's to own, this
/// crate reuses `ensure_migrations_table` rather than declaring it, and its
/// *contents* are checked -- that is what [`plan`]'s `recorded` argument is.
pub const MIGRATIONS_TABLE: &str = "_sqlx_migrations";

/// Drops the tracking table from a schema, so two schemas can be compared on
/// what the migrations actually declare.
fn without_tracking(mut schema: Schema) -> Schema {
    schema.tables.remove(MIGRATIONS_TABLE);
    schema
}

/// Quotes an identifier for interpolation into DDL.
///
/// Schema names reach this module from a CLI flag or the environment, and
/// `SET search_path` and `CREATE SCHEMA` cannot take a bind parameter -- the
/// name is part of the statement, not a value in it. Postgres quotes an
/// identifier with double quotes and escapes an embedded double quote by
/// doubling it, which is enough to make any string a single identifier token.
fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// Refuses a scratch schema that already holds anything.
///
/// The name checks in [`ensure_scratch_is_safe`] catch the schemas whose names
/// say they must not be dropped -- `public`, `pg_*`, the one being baselined.
/// They do not catch the one that actually loses data: a real application
/// schema under some other name, handed to `--scratch` by a mistyped flag or a
/// copied command. `DROP SCHEMA ... CASCADE` then takes it, and nothing before
/// this asked whether it contained anything.
///
/// A scratch schema is *scratch*: created by this function's caller, dropped by
/// it, and empty in between. Anything already in it is either someone else's or
/// the wreckage of a derivation that was killed halfway -- and the second is
/// worth stopping for too, because migrating on top of a partial leftover
/// derives an expectation that is wrong without being empty.
///
/// A schema that does not exist holds nothing, which is the ordinary case and
/// is fine.
async fn ensure_scratch_is_empty(pool: &PgPool, scratch: &str) -> Result<(), BaselineError> {
    // Ours already, and therefore droppable. A derivation stamps the schema
    // it creates (see `run_into`), and the stamp survives the process being
    // killed -- which is the whole point. Without this the guard would take
    // away the recovery that `DROP SCHEMA IF EXISTS` on the way in exists to
    // provide: one `baseline` killed mid-derivation would leave a populated
    // scratch schema, and every later run would refuse it for ever until an
    // operator dropped it by hand.
    if is_ours(pool, scratch).await? {
        return Ok(());
    }
    // `pg_catalog` rather than `information_schema`: the latter shows only
    // objects the current user has a privilege on, so a schema full of another
    // owner's tables would read as empty and be dropped anyway -- `CASCADE`
    // needing the privilege is not the check, it is the damage.
    //
    // Relations, and also types and functions. Counting `pg_class` alone was
    // not enough: `0001_noderun.sql` creates two enums before its first table,
    // so a schema holding only types -- which is exactly what a half-applied
    // migration leaves -- read as empty.
    let held: i64 = sqlx::query_scalar(HELD_OBJECTS)
        .bind(scratch)
        .fetch_one(pool)
        .await?;
    if held > 0 {
        return Err(BaselineError::UnsafeScratch {
            scratch: scratch.to_owned(),
            reason: "it already holds objects and is not a scratch schema this \
                     tool created, so it will not be dropped -- name a schema \
                     that does not exist, or drop this one deliberately",
        });
    }
    Ok(())
}

/// What the scratch schema is stamped with, so a later run knows it is ours.
///
/// A schema comment rather than a marker table: it is set in the same
/// transaction-less breath as `CREATE SCHEMA`, it survives a `SIGKILL`, and
/// nothing a real application does puts this string on a schema.
pub const SCRATCH_MARKER: &str = "pneuma-migrate scratch schema; safe to drop";

/// Counts the objects that make a schema not scratch space.
///
/// Types are counted only where they are a user's own -- an enum, a domain, a
/// range or a multirange. Every table also creates a composite type and an
/// array type, so counting `pg_type` flatly would double-count the relations
/// already counted above.
const HELD_OBJECTS: &str = "SELECT (     SELECT count(*) FROM pg_class c      JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = $1   ) + (     SELECT count(*) FROM pg_type t      JOIN pg_namespace n ON n.oid = t.typnamespace      WHERE n.nspname = $1 AND t.typtype IN ('e', 'd', 'r', 'm')   ) + (     SELECT count(*) FROM pg_proc p      JOIN pg_namespace n ON n.oid = p.pronamespace WHERE n.nspname = $1   )";

/// Whether this schema carries [`SCRATCH_MARKER`], and so was left by a
/// derivation of this tool's own rather than belonging to anyone.
async fn is_ours(pool: &PgPool, scratch: &str) -> Result<bool, BaselineError> {
    let comment: Option<String> = sqlx::query_scalar(
        "SELECT obj_description(n.oid, 'pg_namespace') FROM pg_namespace n \
         WHERE n.nspname = $1",
    )
    .bind(scratch)
    .fetch_optional(pool)
    .await?
    .flatten();
    Ok(comment.as_deref() == Some(SCRATCH_MARKER))
}

/// Derives what the migrations produce, by running them into a scratch schema.
///
/// The scratch schema is dropped on the way in as well as on the way out: a
/// previous run killed between the two would otherwise leave it behind, and
/// migrations applied on top of a partial leftover would derive an expectation
/// that is quietly wrong.
pub async fn expected_schema(
    pool: &PgPool,
    scratch: &str,
    migrator: &Migrator,
) -> Result<Schema, BaselineError> {
    ensure_scratch_is_safe(scratch, None)?;
    // Before the first `DROP SCHEMA ... CASCADE`, and before anything else
    // touches the database. `expected_schema` is `pub` and takes no target to
    // compare the scratch name against, so the name checks alone cannot tell it
    // that `--scratch` was handed the live schema -- this can, because the live
    // schema has tables in it.
    ensure_scratch_is_empty(pool, scratch).await?;
    let quoted = quote_ident(scratch);

    let ran = derive(pool, &quoted, migrator).await;
    let read = match ran {
        Ok(()) => introspect(pool, scratch).await.map_err(BaselineError::from),
        Err(error) => Err(error),
    };

    // Runs on every exit, including the ones `?` used to take before reaching
    // it: a derivation that failed halfway is exactly when a leftover schema is
    // most damaging, since the scratch name is fixed and the next run would
    // migrate on top of it and derive an expectation that is wrong without
    // being empty.
    let dropped = pool
        .execute(format!("DROP SCHEMA IF EXISTS {quoted} CASCADE").as_str())
        .await;

    prefer_cause(read, dropped.map(|_| ()))
}

/// Combines an outcome with the outcome of its cleanup, keeping the cause.
///
/// Cleanup here runs unconditionally, so it can fail *after* the work already
/// failed. Reporting the cleanup's error then would name the symptom and hide
/// the reason -- a `DROP SCHEMA` that could not get its lock, reported instead
/// of the migration error that is why anyone is looking. A cleanup failure is
/// still reported when there is nothing else to report.
///
/// Split out rather than written inline at both call sites because the branch
/// that matters -- work fine, cleanup broken -- needs a database that fails on
/// command to reach otherwise. As a function it is three lines of policy with a
/// test each.
fn prefer_cause<T>(
    work: Result<T, BaselineError>,
    cleanup: Result<(), sqlx::Error>,
) -> Result<T, BaselineError> {
    match (work, cleanup) {
        (Err(error), _) => Err(error),
        (Ok(_), Err(error)) => Err(BaselineError::from(error)),
        (Ok(value), Ok(())) => Ok(value),
    }
}

/// Runs the migrations into the scratch schema on a connection that is then
/// **closed rather than returned to the pool**.
///
/// `Migrator::run` takes a session-level `pg_advisory_lock` keyed on the
/// database and releases it only on the success path -- every `?` between
/// sqlx-core's `conn.lock()` and its `unlock()` returns while still holding it.
/// A pooled connection carrying that lock would keep it for its whole life, and
/// the next `sqlx migrate run` from any process against that database would
/// block forever. Closing the session releases it unconditionally.
///
/// Closing also removes the need to restore `search_path`: the connection this
/// pointed at a doomed schema does not survive to confuse its next borrower.
async fn derive(pool: &PgPool, quoted: &str, migrator: &Migrator) -> Result<(), BaselineError> {
    let mut connection = pool.acquire().await?;
    let ran = run_into(&mut connection, quoted, migrator).await;
    // Not `?` -- the migration's own error is the one worth reporting, and a
    // close that fails must not mask it.
    let closed = connection.close().await;
    prefer_cause(ran, closed)
}

/// The statements themselves, split out so `derive` can close the connection
/// on every path without an early return skipping it.
async fn run_into(
    connection: &mut sqlx::pool::PoolConnection<Postgres>,
    quoted: &str,
    migrator: &Migrator,
) -> Result<(), BaselineError> {
    connection
        .execute(format!("DROP SCHEMA IF EXISTS {quoted} CASCADE").as_str())
        .await?;
    connection
        .execute(format!("CREATE SCHEMA {quoted}").as_str())
        .await?;
    // Stamped immediately, so a process killed anywhere after this leaves a
    // schema the next run can recognise as its own and drop. Without the stamp
    // `ensure_scratch_is_empty` cannot tell this tool's wreckage from someone
    // else's schema, and refusing both means one crash wedges every later run.
    connection
        .execute(format!("COMMENT ON SCHEMA {quoted} IS '{SCRATCH_MARKER}'").as_str())
        .await?;
    // Everything the migrator does lands here, including its own
    // `_sqlx_migrations` table, which is dropped with the schema.
    connection
        .execute(format!("SET search_path TO {quoted}").as_str())
        .await?;
    migrator.run(&mut **connection).await?;
    Ok(())
}

/// Reads which migration versions are already recorded.
///
/// Uses sqlx's own `ensure_migrations_table` and `list_applied_migrations`
/// rather than issuing the DDL and the query here. Those two would be a second
/// copy of a shape sqlx owns, and a baseline written against a table one column
/// different from the one sqlx reads is worse than no baseline at all.
async fn recorded_versions(
    transaction: &mut Transaction<'_, Postgres>,
) -> Result<Vec<i64>, BaselineError> {
    // `Migrate` is implemented for the connection, so the transaction is
    // dereferenced to it. The statements still run inside the transaction --
    // this is the same connection, not a new one -- which is what keeps the
    // read and the writes that follow atomic.
    let connection = &mut **transaction;
    connection.ensure_migrations_table().await?;
    let mut versions: Vec<i64> = connection
        .list_applied_migrations()
        .await?
        .into_iter()
        .map(|applied| applied.version)
        .collect();
    // `list_applied_migrations` does not promise an order, and `plan` compares
    // this against the migrator's own order with `==`.
    versions.sort_unstable();
    Ok(versions)
}

/// Everything [`baseline`] decides, before it writes anything.
///
/// Shared rather than duplicated, and that is not tidiness. A preview written
/// as its own copy of this comparison *was* duplicated once and immediately
/// diverged -- it forgot [`without_tracking`], so it reported
/// `_sqlx_migrations is missing` against a database that matched perfectly. A
/// dry run is only worth running if it decides what the real command decides,
/// which means it has to be the same code, not the same intent.
///
/// Takes the transaction so `recorded` is read inside it, exactly as the write
/// path does; a preview reads it and rolls back.
/// Returns the open transaction alongside the decision, so the caller chooses
/// whether it commits.
///
/// The **ordering is load-bearing**: the schema comparison happens before the
/// transaction is opened, because `expected_schema` acquires a connection of
/// its own and a pool of one -- which is what the tests use, and what a small
/// `max_connections` in production would be -- deadlocks if a transaction is
/// already holding it. A first version of this factoring opened the
/// transaction first and every integration test failed with "pool timed out
/// while waiting for an open connection".
async fn decide<'p>(
    pool: &'p PgPool,
    schema: &str,
    scratch: &str,
    migrator: &Migrator,
) -> Result<(Plan, Transaction<'p, Postgres>), BaselineError> {
    ensure_scratch_is_safe(scratch, Some(schema))?;
    let embedded: Vec<i64> = up_migrations(migrator)
        .map(|migration| migration.version)
        .collect();
    // Asked here as well as inside `plan`, and the difference is where it
    // happens. `plan` runs after `expected_schema` has already dropped and
    // recreated the scratch schema and run every migration into it -- so with
    // no migrations embedded, the tool did all of that destructive work and
    // then reported that there was nothing to do. Refusing first costs
    // nothing and touches no schema. `plan` keeps its own check because it is
    // pure, is `pub`, and is tested on its own.
    if embedded.is_empty() {
        return Err(BaselineError::NoMigrations);
    }
    let expected = without_tracking(expected_schema(pool, scratch, migrator).await?);
    let live = without_tracking(introspect(pool, schema).await?);
    let differences = live.differences(&expected);

    let mut transaction = pool.begin().await?;
    transaction
        .execute(format!("SET LOCAL search_path TO {}", quote_ident(schema)).as_str())
        .await?;
    let recorded = recorded_versions(&mut transaction).await?;
    let decided = plan(&differences, &recorded, &embedded)?;
    Ok((decided, transaction))
}

/// What [`baseline`] would do, without writing to `schema`.
///
/// The transaction is rolled back rather than committed, so
/// `ensure_migrations_table` -- which `recorded_versions` calls, and which
/// creates the table if it is absent -- leaves nothing behind. A preview that
/// created the tracking table would change the very thing it is reporting on.
///
/// **It is not read-only, and the distinction matters to whoever runs it
/// against production.** Deriving the expectation means `expected_schema`
/// `DROP SCHEMA ... CASCADE`s `scratch`, recreates it, and runs every
/// migration into it -- real DDL, on an autocommit connection, holding the
/// session-level advisory lock sqlx keys on the *database*, so it serialises
/// against a concurrent `sqlx migrate run`. That is the same work [`baseline`]
/// does, which is the point: a dry run that derived the expectation some
/// cheaper way would be answering a different question. `scratch` is guarded
/// by `ensure_scratch_is_safe`; `schema` is what is left untouched.
pub async fn preview(
    pool: &PgPool,
    schema: &str,
    scratch: &str,
    migrator: &Migrator,
) -> Result<Plan, BaselineError> {
    let (decided, transaction) = decide(pool, schema, scratch, migrator).await?;
    transaction.rollback().await?;
    Ok(decided)
}

/// Baselines `schema`: checks it matches the migrations, then records them.
///
/// Everything after the check happens in one transaction, so a database is
/// never left holding a partial set of rows -- which is precisely the
/// [`BaselineError::PartiallyRecorded`] state this refuses to create.
pub async fn baseline(
    pool: &PgPool,
    schema: &str,
    scratch: &str,
    migrator: &Migrator,
) -> Result<Plan, BaselineError> {
    let (decided, mut transaction) = decide(pool, schema, scratch, migrator).await?;
    if let Plan::Record(ref versions) = decided {
        for migration in up_migrations(migrator) {
            if !versions.contains(&migration.version) {
                continue;
            }
            // `execution_time` is sqlx's own sentinel for "not measured": it
            // writes -1 before running and overwrites it after. Nothing ran
            // here, so it stays -1 rather than claiming a duration of zero.
            sqlx::query(
                "INSERT INTO _sqlx_migrations \
                 (version, description, success, checksum, execution_time) \
                 VALUES ($1, $2, TRUE, $3, -1)",
            )
            .bind(migration.version)
            .bind(&*migration.description)
            .bind(&*migration.checksum)
            .execute(&mut *transaction)
            .await?;
        }
    }
    transaction.commit().await?;
    Ok(decided)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn missing(table: &str) -> Difference {
        Difference::MissingTable {
            table: table.to_owned(),
        }
    }

    #[test]
    fn an_empty_database_with_a_matching_schema_records_every_migration() {
        let Ok(decided) = plan(&[], &[], &[1, 2]) else {
            panic!("a matching, unrecorded database is the whole point");
        };
        assert_eq!(decided, Plan::Record(vec![1, 2]));
    }

    #[test]
    fn a_database_already_holding_every_version_is_a_no_op() {
        // Repeating a baseline must be safe. An operator who cannot tell
        // whether the first run committed will run it again, and the second
        // run answering "already done" is the difference between a recoverable
        // step and one that has to be reasoned about.
        let Ok(decided) = plan(&[], &[1, 2], &[1, 2]) else {
            panic!("should be a no-op");
        };
        assert_eq!(decided, Plan::AlreadyRecorded);
    }

    #[test]
    fn a_schema_that_does_not_match_is_refused_and_the_differences_are_named() {
        let differences = [missing("node_run"), missing("node_run_history")];
        let Err(error) = plan(&differences, &[], &[1, 2]) else {
            panic!("a mismatched schema must not be baselined");
        };
        let BaselineError::SchemaMismatch(reported) = &error else {
            panic!("wrong variant: {error:?}");
        };
        assert_eq!(reported.len(), 2);
        // The message carries every difference, not a count. An operator has to
        // decide whether the database or the migrations are wrong.
        let rendered = error.to_string();
        assert!(rendered.contains("node_run is missing"), "{rendered}");
        assert!(
            rendered.contains("node_run_history is missing"),
            "{rendered}"
        );
        assert!(rendered.contains("2 difference(s)"), "{rendered}");
    }

    #[test]
    fn drift_is_reported_while_the_database_is_still_unadopted() {
        // The comparison means what it says only before adoption: nothing is
        // recorded, so the live schema is claimed to be exactly what the
        // adoption subset produces, and a difference is a real disagreement.
        let Err(error) = plan(&[missing("node_run")], &[], &[1, 2]) else {
            panic!("drift must be reported before anything is written");
        };
        assert!(
            matches!(error, BaselineError::SchemaMismatch(_)),
            "wrong variant: {error:?}"
        );
        // And on a partially recorded one, where the schema is equally not
        // supposed to have moved.
        assert!(matches!(
            plan(&[missing("node_run")], &[1], &[1, 2]),
            Err(BaselineError::SchemaMismatch(_))
        ));
    }

    #[test]
    fn an_adopted_database_is_not_judged_against_the_adoption_subset() {
        // The reverse of the case above, and a deliberate change of behaviour.
        // The schema check used to come first, so this returned
        // `SchemaMismatch`. That was right while `embedded` was every migration
        // in the repository; it is wrong now that `baseline` adopts only
        // through `ORIGINAL_THROUGH`, because an adopted database goes on
        // migrating and is *supposed* to be ahead. Refusing it made `baseline`
        // fail on the second run of any deploy script that baselines before
        // migrating.
        //
        // A difference here is therefore not evidence of drift -- it is what
        // `0003_submission` looks like from inside the adoption subset -- so
        // this says nothing about the schema rather than something false.
        let Ok(decided) = plan(&[missing("node_run")], &[1, 2], &[1, 2]) else {
            panic!("an adopted database has nothing to baseline");
        };
        assert_eq!(decided, Plan::AlreadyRecorded);
    }

    #[test]
    fn a_partly_recorded_database_is_refused_rather_than_topped_up() {
        // It is already managed by `sqlx migrate run`, so version 2 is absent
        // because it has genuinely not been applied. Recording it would skip it
        // permanently.
        let Err(error) = plan(&[], &[1], &[1, 2]) else {
            panic!("a partial record must be refused");
        };
        let BaselineError::PartiallyRecorded { recorded, embedded } = &error else {
            panic!("wrong variant: {error:?}");
        };
        assert_eq!(
            (recorded.as_slice(), embedded.as_slice()),
            (&[1][..], &[1, 2][..])
        );
        let rendered = error.to_string();
        assert!(rendered.contains("1 of 2 migrations"), "{rendered}");
        assert!(
            rendered.contains("migrated rather than baselined"),
            "{rendered}"
        );
    }

    #[test]
    fn a_database_that_migrated_past_the_adoption_has_nothing_to_baseline() {
        // The ordinary state of an adopted database: it was baselined at
        // `[1, 2]` and `sqlx migrate run` has since applied `0003`. A superset
        // is what "already recorded" means here -- equality would call this
        // `PartiallyRecorded` and refuse a database that is entirely correct.
        let Ok(decided) = plan(&[], &[1, 2, 3], &[1, 2]) else {
            panic!("a migrated database has nothing to baseline");
        };
        assert_eq!(decided, Plan::AlreadyRecorded);
    }

    #[test]
    fn a_record_with_a_gap_is_still_refused() {
        // Not a superset: version 2 is genuinely absent, so this database is
        // managed by `sqlx migrate run` and is missing a migration. Recording
        // it would skip that migration for ever.
        let Err(error) = plan(&[], &[1, 3], &[1, 2]) else {
            panic!("a gap in the record must be refused");
        };
        assert!(
            matches!(error, BaselineError::PartiallyRecorded { .. }),
            "wrong variant: {error:?}"
        );
    }

    #[test]
    fn baselining_nothing_is_refused_rather_than_reported_as_success() {
        // Would otherwise create the tracking table, claim nothing, and exit 0
        // -- a no-op an operator would read as "this database is baselined".
        let Err(error) = plan(&[], &[], &[]) else {
            panic!("there is nothing to baseline");
        };
        assert!(
            matches!(error, BaselineError::NoMigrations),
            "wrong variant: {error:?}"
        );
        assert!(error.to_string().contains("nothing to baseline"));
    }

    #[test]
    fn having_no_migrations_is_reported_before_any_other_complaint() {
        // Ordering pinned from the other side: with no migrations embedded,
        // every difference is spurious -- an empty expectation makes every live
        // table "unexpected" -- so reporting the mismatch would be misleading.
        let Err(error) = plan(&[missing("node_run")], &[1], &[]) else {
            panic!("should refuse");
        };
        assert!(
            matches!(error, BaselineError::NoMigrations),
            "wrong variant: {error:?}"
        );
    }

    #[test]
    fn a_cleanup_failure_never_hides_the_reason_the_work_failed() {
        // The arm that needs a database failing on command to reach in situ,
        // which is why the policy is a function.
        let work: Result<(), BaselineError> = Err(BaselineError::NoMigrations);
        let Err(error) = prefer_cause(work, Err(sqlx::Error::PoolClosed)) else {
            panic!("an error must survive a failed cleanup");
        };
        assert!(
            matches!(error, BaselineError::NoMigrations),
            "the cause, not the cleanup: {error:?}"
        );
    }

    #[test]
    fn a_cleanup_failure_is_reported_when_there_is_nothing_else_to_report() {
        // Not swallowed. A scratch schema that could not be dropped is a real
        // problem for the next run, which builds on whatever is left.
        let Err(error) = prefer_cause(Ok(()), Err(sqlx::Error::PoolClosed)) else {
            panic!("a failed cleanup is still a failure");
        };
        assert!(
            matches!(error, BaselineError::Database(_)),
            "wrong variant: {error:?}"
        );
    }

    #[test]
    fn both_succeeding_yields_the_work() {
        let Ok(value) = prefer_cause(Ok(7), Ok(())) else {
            panic!("nothing failed");
        };
        assert_eq!(value, 7);
    }

    #[test]
    fn a_scratch_name_equal_to_the_target_is_refused_before_anything_is_dropped() {
        // The whole point of the guard. Deriving begins with `DROP SCHEMA
        // ... CASCADE` on the scratch name, so this one comparison is what
        // stands between a mis-set variable and the production schema.
        let Err(error) = ensure_scratch_is_safe("pneuma", Some("pneuma")) else {
            panic!("dropping the schema being baselined must be refused");
        };
        let rendered = error.to_string();
        assert!(rendered.contains("which would be dropped"), "{rendered}");
        assert!(rendered.contains("pneuma"), "the name is named: {rendered}");
    }

    #[test]
    fn the_public_and_system_schemas_are_refused_however_they_are_spelled() {
        for name in ["public", "PUBLIC", "Public"] {
            assert!(
                ensure_scratch_is_safe(name, Some("pneuma")).is_err(),
                "{name} is not scratch space"
            );
        }
        for name in ["information_schema", "pg_catalog", "pg_toast", "PG_CATALOG"] {
            assert!(
                ensure_scratch_is_safe(name, Some("pneuma")).is_err(),
                "{name} is a system schema"
            );
        }
        // Case folding is not what protects them, but it is why the check is
        // case-insensitive: an unquoted name in a shell reaches Postgres folded,
        // while this module quotes it.
        let Err(error) = ensure_scratch_is_safe("PUBLIC", None) else {
            panic!("should refuse");
        };
        assert!(error.to_string().contains("not scratch space"));
    }

    #[test]
    fn an_empty_scratch_name_is_refused() {
        let Err(error) = ensure_scratch_is_safe("", None) else {
            panic!("an empty name would quote to \"\" and drop nothing useful");
        };
        assert!(error.to_string().contains("empty"));
    }

    #[test]
    fn an_ordinary_scratch_name_is_allowed() {
        assert!(ensure_scratch_is_safe("pneuma_scratch", Some("pneuma")).is_ok());
        // A name that merely *contains* a reserved word is fine -- the check is
        // equality, not a substring search, which would refuse legitimate names.
        assert!(ensure_scratch_is_safe("public_scratch", None).is_ok());
        assert!(ensure_scratch_is_safe("mypg_scratch", None).is_ok());
    }

    #[test]
    fn an_identifier_is_quoted_and_an_embedded_quote_is_doubled() {
        assert_eq!(quote_ident("pneuma"), "\"pneuma\"");
        // `SET search_path` takes no bind parameter, so the name is part of the
        // statement. Doubling is what keeps a hostile one a single token
        // rather than the end of the identifier and the start of a statement.
        assert_eq!(quote_ident("we\"ird"), "\"we\"\"ird\"");
        assert_eq!(
            quote_ident("a\"; DROP SCHEMA public CASCADE; --"),
            "\"a\"\"; DROP SCHEMA public CASCADE; --\""
        );
    }
}
