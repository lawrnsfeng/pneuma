//! The HTTP surface, shutdown and scheduling every pneuma binary shares.
//!
//! Six processes need the same three things: health endpoints Kubernetes can
//! probe, a shutdown that finishes in-flight work instead of dropping it, and —
//! for the janitor — something that runs a body on an interval and can say when
//! a tick was missed. Written once here rather than six times.
//!
//! # Not folded into `pneuma-telemetry`
//!
//! That crate's doc says it is deliberately HTTP-free, and that is what lets
//! its aggregation be tested without binding a port. This crate is the HTTP
//! half and depends on it, so the split holds in the direction it was drawn.
//!
//! # Everything here is testable without a port
//!
//! An [`axum::Router`] is a `tower::Service`, so every route, status and body
//! is a unit test through `ServiceExt::oneshot` — no listener, no address, no
//! waiting. The one integration test that does bind takes port 0 and exists to
//! prove the graceful path completes, which is the one property a `Router`
//! alone cannot show.
//!
//! `tokio::time::pause()` does the same for the ticker: a sixty-minute interval
//! is a microsecond test, because the runtime advances the clock rather than
//! the wall.

#![deny(missing_docs)]
#![deny(clippy::unwrap_used, clippy::expect_used)]
#![forbid(unsafe_code)]

pub mod health;
pub mod shutdown;
pub mod ticker;

pub use health::{router, RouterError};
pub use shutdown::{serve, termination, Signal};
pub use ticker::{every, missed};
