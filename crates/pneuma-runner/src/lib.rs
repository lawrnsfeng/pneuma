//! Driving a resolved pipeline to completion.
//!
//! The layer between [`pneuma_interpreter`], which decides *what* runs next,
//! and whatever actually calls a component. It owns the loop and nothing else:
//! ask for a task, call the component, report the output, repeat.
//!
//! # Why the caller supplies the component
//!
//! [`Component`] is a trait, so this crate has no HTTP client, no transport,
//! and no Restate. Three things follow. The loop is testable against a fake
//! that returns canned outputs and canned failures, so every branch here is
//! reachable without a server. The same driver runs under Restate's
//! `ctx.run(..)` and under a plain client, which is what keeps the adoption
//! decision reversible. And `scripts/forbid-deps.sh` can hold this crate to it.
//!
//! # Determinism
//!
//! `spikes/restate/VERDICT.md` §1: Restate replays a handler in a fresh
//! process, so the *order* of component calls must not vary. This loop makes no
//! ordering decisions of its own — it calls [`pneuma_interpreter::Execution::next_task`]
//! and does
//! what it is told, and that is deterministic because the interpreter's
//! containers are ordered. `scripts/forbid-nondeterminism.sh` keeps it that
//! way. Nothing here may iterate a map to decide what to call.

#![forbid(unsafe_code)]
#![deny(clippy::unwrap_used, clippy::expect_used)]
#![deny(missing_docs)]

pub mod driver;
pub mod record;

pub use driver::{drive, drive_recording, Completed, Component, Dispatch, DriveError, RunInput};
pub use record::{kind_of, wire_kind, Failure, NewStep, NoRecord, Record, Recorder};
// Re-exported because `DriveError::Schedule` carries it: a caller that matches
// on that variant needs the inner type, and taking a direct dependency on
// `pneuma-interpreter` just to name it would make every such caller declare a
// crate it does not otherwise use.
pub use pneuma_interpreter::ScheduleError;
