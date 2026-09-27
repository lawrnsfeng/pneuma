//! `pneuma-core` — the pure, I/O-free domain kernel of the pneuma workflow framework.
//!
//! This crate contains no async runtime, no network I/O, no database driver
//! runtime, and no filesystem access. Every type and function here is a
//! deterministic, synchronous transformation over plain data. That constraint
//! is deliberate: it is what makes 100% unit test coverage on this crate
//! achievable rather than aspirational, and it is the substrate every
//! higher-level crate (`pneuma-engine`, transport adapters, storage adapters)
//! is built on top of.
//!
//! # Dependency direction
//!
//! Nothing in this crate may depend on `tokio`, `async-nats`, `mongodb`, or any
//! `sqlx` runtime/TLS feature. The one exception is `sqlx`'s `derive` +
//! `postgres` features on `NodeStatus`, which provide compile-time `sqlx::Type`
//! mapping without pulling in a runtime — enforced in CI via `cargo tree`.
//!
//! # Module map
//!
//! Modules are added incrementally, each with its own logic-plus-tests commit
//! reaching 100% coverage before the next begins. See the design notes at the
//! workspace root for every behavioral divergence from the original
//! original this crate ports from.

#![deny(clippy::unwrap_used, clippy::expect_used)]
#![forbid(unsafe_code)]

pub mod child_index;
pub mod child_ref;
pub mod condition;
pub mod evaluator;
pub mod ids;
pub mod node;
pub mod resolver;
pub mod sibling_index;
pub mod slug;
pub mod start_set;
pub mod status;
pub mod step;
