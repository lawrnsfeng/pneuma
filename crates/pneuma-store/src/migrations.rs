//! The migrations, embedded, so one source describes the schema.
//!
//! The original plan puts `sqlx::migrate!` here and has `pneuma-migrate`
//! run it. That split matters for `baseline`, which inserts rows asserting the
//! migrations *have run* without executing them: the expectation it compares a
//! live database against, and the rows it then writes, must come from the same
//! place. Two copies would eventually claim a database matches migrations it
//! does not.
//!
//! Behind a function rather than a bare `static` in `lib.rs`, because
//! `scripts/coverage.sh` refuses definitions there — the gate excludes that
//! file, so anything defined in it is silently unmeasured.
//!
//! That is the whole of the reason. An earlier version of this comment claimed
//! the function form also gets the definition *covered*, since "a function has
//! a body and a test calls it". It does not: measured, `migrator()` contributes
//! **zero coverable lines**, and this file does not appear in tarpaulin's
//! report at all while it holds only this function. The gate reads 100% either
//! way, so it cannot tell you that.
//!
//! The file is still watched, which is the part that matters — adding a
//! never-called function here drops the crate to 99.02% and fails the gate. So
//! real logic added to this module *would* be measured. Only this one
//! `&'static` return is invisible, and it is invisible because there is nothing
//! in it to execute.

use std::borrow::Cow;

use sqlx::migrate::Migrator;

/// Every migration this crate defines, in order.
pub fn migrator() -> &'static Migrator {
    static MIGRATOR: Migrator = sqlx::migrate!("./migrations");
    &MIGRATOR
}

/// The last migration that transcribes a table **the original migration tool already created**.
///
/// The dividing line, and it is not cosmetic. `pneuma-migrate baseline` adopts
/// a database the original migration tool has been managing: it compares the live schema against
/// what the migrations produce and, if they agree, records those migrations as
/// applied *without running them*. That only makes sense for migrations
/// describing objects the database already has.
///
/// `0001_noderun` and `0002_noderun_history` are transcriptions of the original migration tool
/// revisions, so a production database has both. `0003_submission` is new to
/// this port -- the original migration tool never created `submission`, and no existing database
/// has it. Baselining against all three would either refuse a production
/// database that is in fact correct, or, worse, record `0003` as applied
/// against a database with no `submission` table: `sqlx migrate run` would then
/// skip creating it and every submission query would fail at run time, on a
/// deployment whose baseline reported success.
///
/// So the adoption uses [`original_migrator`] and everything after this version
/// is an ordinary forward migration that `sqlx migrate run` applies.
pub const ORIGINAL_THROUGH: i64 = 2;

/// Only the migrations describing what the original migration tool already created.
///
/// What [`ORIGINAL_THROUGH`] means, as a value `baseline` can take.
///
/// Built by narrowing a clone of [`migrator`]. `Migrator`'s fields are
/// `#[doc(hidden)]` and documented as semver-exempt, which is worth naming: a
/// future sqlx could change them. It would do so as a *compile* error against a
/// pinned dependency, not as a silent behaviour change, and the alternative --
/// threading a version bound through `expected_schema`, `decide`, `preview` and
/// `baseline` so each can filter for itself -- puts the same rule in four
/// places instead of one.
pub fn original_migrator() -> Migrator {
    let full = migrator();
    // Every field named, rather than `..full.clone()`: `Migrator` is not
    // `Clone`, and spelling them out means a field added by a future sqlx is a
    // compile error here -- which is the loud version of this going wrong.
    Migrator {
        migrations: Cow::Owned(
            full.migrations
                .iter()
                .filter(|migration| migration.version <= ORIGINAL_THROUGH)
                .cloned()
                .collect(),
        ),
        ignore_missing: full.ignore_missing,
        locking: full.locking,
        no_tx: full.no_tx,
    }
}
