//! Running a pipeline as a durable Restate handler.
//!
//! The engine half of the adopted branch. `pneuma_runner::drive` owns the loop
//! and knows no transport; this crate supplies the transport and the durability
//! by implementing [`pneuma_runner::Component`] on top of `ctx.run`.
//!
//! # What Restate contributes, and what it does not
//!
//! `spikes/restate/VERDICT.md` §2: the aggregation barrier collapses to a local
//! counter, because the whole run is one replayed handler rather than a set of
//! distributed workers coordinating through a table. So there is no barrier
//! store, no outbox and no lease here — that is the 45% of the port this
//! adoption removed.
//!
//! What it does not contribute is fairness (§4: a grep of the SDK surface for
//! `priority|fairness|weight|quota` returns nothing), which stays pneuma\'s own
//! work in front of the engine rather than inside it.

#![forbid(unsafe_code)]
#![deny(clippy::unwrap_used, clippy::expect_used)]
#![deny(missing_docs)]

pub mod component;
pub mod endpoint;
pub mod journal;
pub mod serve;
pub mod service;

pub use component::{CallError, HttpComponent};
pub use endpoint::{Endpoint, EndpointError};
pub use journal::Journalled;
pub use serve::{serve, ServeError};
pub use service::{RunReport, RunRequest, Runner};
