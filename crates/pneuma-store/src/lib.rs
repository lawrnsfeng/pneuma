//! Repositories for the records a run leaves behind, in both stores.
//!
//! Postgres holds the `node_run` and `node_run_history` tables — [`store`],
//! [`node_run`], [`queries`]. MongoDB holds the run document — [`run`] for its
//! reads and status writes, [`barrier`] for the aggregation counters, and
//! [`history`] for archiving and expiry. The split is the original's, not a
//! choice made here.
//!
//! The first crate here that performs I/O. `pneuma-core` depends on `sqlx` for
//! its enum derives alone; this crate adds the driver, the runtime and TLS.
//!
//! **What that does and does not guarantee, measured rather than assumed.**
//! `cargo tree -p pneuma-core` resolves features for that package alone and
//! shows no runtime, which is what `scripts/forbid-deps.sh` checks and what
//! keeps `cargo test -p pneuma-core` a container-free, runtime-free build. But
//! cargo unifies features across a whole-workspace build, so
//! `cargo tree -i tokio` from the workspace root shows `sqlx -> pneuma-core`:
//! in that build, the `sqlx` pneuma-core links has `runtime-tokio` enabled
//! because this crate asked for it.
//!
//! So "pneuma-core pulls in no runtime" is true of its own dependency contract
//! and not of every artifact it appears in. The invariant that actually matters
//! — that no source in `pneuma-core` names an executor, a pool or a connection
//! — is unaffected. If the stronger property is ever wanted, the way to get it
//! is to put pneuma-core's `sqlx` derives behind an off-by-default feature that
//! this crate enables; that is a deliberate change, not a cleanup, so it is
//! recorded here rather than done in passing.
//!
//! # Scope
//!
//! The design notes gates the shape of the resolved graph stored in the run
//! document's `state` blob, and it does not gate anything here. Two halves to
//! that, and an earlier version of this note got the second one wrong by
//! claiming the crate was Postgres-only:
//!
//! * The Postgres tables hold per-node-run records, not the resolved graph, so
//!   the `component_ids` change alters no column.
//! * This crate *does* write into `state` — [`run::RunStore::update_step_status`]
//!   moves one step's status — but only that field. It never writes the graph,
//!   which is `pneuma-intake`'s single gated write. Reading a status out of
//!   a `state` entry does not depend on how that entry stores its children.
//!
//! It also excludes terminations. That table belongs to `pneuma-gateway`, which
//! owns all eight of its queries, and the janitor reaches it over HTTP rather
//! than the database — see `pneuma-gateway-client`.
//!
//! # The schema is transcribed, not designed
//!
//! `migrations/0001_noderun.sql` is the table the original migration tool already created, written
//! out in SQL. Nothing here invents a column. That matters for two reasons: a
//! test can stand up a Postgres that matches production, and `pneuma-migrate`
//! can fingerprint a live database against a baseline rather than trying to
//! migrate one that is already correct.

#![forbid(unsafe_code)]
#![deny(clippy::unwrap_used, clippy::expect_used)]
#![deny(missing_docs)]

pub mod barrier;
pub mod conflict;
pub mod health;
pub mod history;
pub mod migrations;
pub mod node_run;
pub mod pipeline;
pub mod queries;
pub mod refpath;
pub mod run;
pub mod store;
pub mod submission;

pub use barrier::{Arrival, BarrierError, BarrierStore};
pub use conflict::{is_duplicate_key, Created, DUPLICATE_KEY};
pub use health::{MongoHealth, PostgresHealth, DEFAULT_PROBE_TIMEOUT};
pub use history::{retention_cutoff, RunHistoryStore, ARCHIVED_AT};
pub use migrations::{migrator, original_migrator, ORIGINAL_THROUGH};
pub use node_run::{NodeRun, NodeRunRow, RowError};
pub use pipeline::{PipelineStore, PIPELINE_ID};
pub use refpath::{RefPath, RefPathError, StepId};
// `RunStatus` is `pneuma_core`'s, re-exported here only so a caller does not
// need both crates in scope for one type. Deliberately not redefined: an
// earlier version had a second six-variant copy with its own wire table.
pub use pneuma_core::status::RunStatus;
pub use run::RunStore;
pub use store::{NewNodeRun, NodeRunStore, StoreError};
pub use submission::{Accepted, Outcome, Queued, SubmissionStore};
