//! The AMQP ingress: a pipeline id becomes a run document and a submission.
//!
//! Reimplements the original's boot listener in Rust, with one substitution.
//! The original ends by publishing a `MessageInit` to the task queue for the
//! controller to pick up; here the run is handed to
//! `pneuma-admission`, which is what puts it in front of the fair dispatcher
//! and then Restate. Everything before that is the same work: look the
//! pipeline up, resolve it, write the run document, and only then tell anyone
//! about it.
//!
//! # The order is the durability
//!
//! Write first, announce second. A submission announced before the run
//! document exists is a run the rest of the system can ask about and get
//! nothing for; a run document written and never announced is recoverable,
//! because the message has not been acknowledged and will come back. So the
//! insert happens first, and a redelivery finds it already there — which is
//! why `pneuma_store::Created::AlreadyExists` is a result rather than an
//! error (the defect notes).

#![deny(missing_docs)]
#![deny(clippy::unwrap_used, clippy::expect_used)]
#![forbid(unsafe_code)]

pub mod adapt;
pub mod boot;
pub mod config;
pub mod consume;
pub mod document;
pub mod handle;

pub use adapt::{Admission, Forwarder};
pub use boot::{attach, run, supervise, BootError, SERVICE};
pub use config::{Config, ConfigureError};
pub use consume::{pump, settle};
pub use document::{
    run_document, DocumentError, CREATED_AT, ID, RUN_ID, STATE, STATUS, STEP_INPUT,
};
pub use handle::{
    handle_definition, handle_event, handle_run, Admits, Events, Outcome, Pipelines, Runs,
};
