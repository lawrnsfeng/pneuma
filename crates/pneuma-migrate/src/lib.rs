//! Deciding whether a live database already matches the schema.
//!
//! The original plan: `pneuma-migrate baseline` "fingerprints the live
//! schema and inserts the baseline row without executing", for the
//! environments the original migration tool has already migrated. That row asserts the migrations
//! *have run*, so it must not be written on a database where they have not —
//! marking an unmigrated schema as current means the next real migration runs
//! against something it does not expect.
//!
//! The fingerprint is therefore the load-bearing part, and it is a comparison
//! of two descriptions rather than of two checksums: what the live database
//! has, against what the migrations produce. Deriving the second by *running*
//! them into a scratch schema is what keeps this honest — an expected
//! description written out by hand is a second copy of my reading of the SQL,
//! and it drifts.

#![forbid(unsafe_code)]
#![deny(clippy::unwrap_used, clippy::expect_used)]
#![deny(missing_docs)]

pub mod baseline;
pub mod cli;
pub mod introspect;
pub mod mongo;
pub mod run;
pub mod schema;

pub use baseline::{baseline, expected_schema, plan, preview, BaselineError, Plan, SCRATCH_MARKER};
pub use cli::{command, describe, exit_code, parse, Invocation};
pub use introspect::{introspect, IntrospectError};
pub use mongo::{
    duplicate_run_ids, ensure_run_id_index, Duplicate, MongoError, RunKey, RUN_ID, RUN_ID_INDEX,
};
pub use run::{run, CliError, Report};
pub use schema::{strip_schema_qualifier, Column, Difference, Schema, Table};
