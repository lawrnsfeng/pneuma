//! Which step runs next, and with what input.
//!
//! The decision half of executing a pipeline: no I/O, no engine. A driver asks
//! what is ready, runs it however it likes, and reports the result back.
//!
//! That split earns its keep twice. The scheduling rules can be tested
//! exhaustively without a component, a queue or a workflow runtime; and they
//! survive a change of engine, which this port has already reconsidered once.
//!
//! # What this is not
//!
//! It does not call components, persist anything, or retry. Under the adopted
//! branch the driver is a Restate workflow handler and durability comes from
//! the engine's journal. Nothing here knows that.

#![forbid(unsafe_code)]
#![deny(clippy::unwrap_used, clippy::expect_used)]
#![deny(missing_docs)]

pub mod execution;
pub mod schedule;

pub use execution::{Execution, FrameId, Happening, Origin, Task};
pub use schedule::{
    aggregate, branch_outputs, fanout_of, refs_of, Fanout, Ready, ScheduleError, Scheduler, SubRun,
};
