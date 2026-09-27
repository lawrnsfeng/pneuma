//! Observability primitives shared by pneuma's binaries.
//!
//! Deliberately HTTP-free. The endpoints that expose these values are wired up
//! per binary; what lives here is the logic underneath them, so it can be
//! tested without binding a port or standing up a router.

#![deny(clippy::unwrap_used, clippy::expect_used)]
#![forbid(unsafe_code)]

pub mod health;

pub use health::{run_liveness_checks, HealthCheckable, HealthStatus, LivenessReport};
