//! NATS to a component: take a step, call it, publish what it said.
//!
//! the original executor in Rust. It is the one service that talks to a
//! model, and everything it does is decided before the call or from the
//! answer — so [`verdict`] and [`mod@report`] are pure and [`component`] is a client
//! with no opinions.
//!
//! # Why this exists in the port at all
//!
//! `BROKER-PORTABILITY.md`: Restate replaces the *engine*, not every
//! transport. Under the Restate path `pneuma-restate` calls components over
//! HTTP directly and this service is not in the way. It is here for the
//! deployment `pneuma-driver` serves — and, like the broker, it is
//! independently deployable against the live system today, because it consumes
//! and publishes exactly what the original service does.
//!
//! # What it does not do yet
//!
//! Termination sync. The original service holds a `TerminationStore` populated from
//! the gateway's terminations API and short-circuits a cancelled job before
//! calling the model.
//! That path depends on the gateway's shape, which is still being decided —
//! the design notes record the omission rather than guessing at an API. The
//! cost is bounded and known: a cancelled job's remaining steps are still
//! executed, which is what happens today whenever the store has not caught up.

#![deny(missing_docs)]
#![deny(clippy::unwrap_used, clippy::expect_used)]
#![forbid(unsafe_code)]

pub mod boot;
pub mod component;
pub mod config;
pub mod report;
pub mod verdict;

pub use boot::{handle, run, step_output, BootError, PUBLISH_TIMEOUT, SERVICE};
pub use component::{
    classify, Component, ComponentError, DEFAULT_ATTEMPTS, DEFAULT_REQUEST_TIMEOUT_SECS,
};
pub use config::{queue_group, Config, ConfigureError};
pub use report::{report, started, Report};
pub use verdict::{
    from_status, from_transport, is_retryable, Transport, Verdict, PREDICTION_REFUSED,
};
