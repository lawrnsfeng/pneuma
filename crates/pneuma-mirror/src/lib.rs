//! Writes a run's steps into the `node_run` table.
//!
//! One [`pneuma_runner::Recorder`], usable by either transport. It exists as a
//! crate rather than as a module in one of them for a reason that is structural
//! rather than tidy: `pneuma-runner` may not link a database — the gate holds
//! it to that so the drive loop stays testable against a fake — and
//! `pneuma-store` has no business knowing what a run is. The adapter can live
//! in neither, and the alternative is the same code in both binaries, drifting
//! until a fan-out records differently under Restate than under the broker.
//!
//! # What it does not decide
//!
//! Nothing. Every judgement — a step's path, which row is whose parent, which
//! status a moment implies — was made in `pneuma_runner::record`, purely, and
//! this is a three-arm match onto three statements that already existed.
//! `pneuma-janitor` has been reading `node_run` since it was written; until now
//! nothing wrote it.
//!
//! # A failure here is a log line
//!
//! [`pneuma_runner::Recorder::record`] returns `()` by signature, and this
//! honours that literally: a database that cannot be reached costs the mirror
//! a row and costs the run nothing. Losing a completed model call — paid for —
//! because an audit table was down is the outcome that shape exists to
//! prevent.
//!
//! The visible consequence of a broken mirror is not silence: it is
//! `pneuma-janitor` finding no stale runs, and the `node_run` count on a
//! dashboard sitting still. Both are conditions an operator can see.

#![forbid(unsafe_code)]
#![deny(clippy::unwrap_used, clippy::expect_used)]
#![deny(missing_docs)]

pub mod mirror;

pub use mirror::{Mirror, NotReady};
