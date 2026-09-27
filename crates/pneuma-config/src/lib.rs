//! Typed environment-variable helpers.
//!
//! This is a *pattern*, not a configuration framework. Each binary still writes
//! its own `Config::from_env()` that reads top to bottom, the way the
//! platform's read-API service already does — these helpers only remove the
//! `.context("X is required")` boilerplate from every call site.
//!
//! There is deliberately no derive macro and no deserialization. The platform's
//! one production Rust service hand-rolls `std::env::var` with `anyhow`
//! context, and matching that convention matters more than saving keystrokes.
//! What it does *not* do well is scale: the controller alone has several dozen
//! keys against the gateway's ten, and hand-writing the same fifteen lines six
//! times is how the six copies drift apart.
//!
//! # The reader is a value, not a global
//!
//! [`Env::from_process`] is what a binary uses and [`Env::from_pairs`] is what
//! a test uses. This crate's helpers were free functions over `std::env` until
//! that proved to be the wrong shape: a free function over a process global can
//! only be tested by mutating that global, which is a data race under any
//! parallel test runner and is why `std::env::set_var` is `unsafe` from edition
//! 2024. `pneuma-janitor` had grown a crate-wide `Mutex` for it. See [`source`].
//!
//! # Two deliberate departures from the gateway
//!
//! **A non-UTF-8 value is an error, not a missing value.** `std::env::var`
//! fails with `NotPresent` *or* `NotUnicode`, and the gateway's
//! `unwrap_or_else(|_| default)` cannot tell them apart — so a variable that is
//! set but mis-encoded silently becomes the default, and the operator who set
//! it sees no complaint. Here only `NotPresent` yields a default; a mis-encoded
//! value is [`ConfigError::NotUnicode`].
//!
//! **Credential-bearing values are [`SecretString`].** `pneuma-gateway`'s
//! `Config` (`src/config.rs:6-7`) is `#[derive(Debug, Clone)]` over
//! `database_url: String` and `mongodb_uri: String`, so any `{:?}` of it —
//! including one added later by someone debugging something else — renders the
//! credentials. [`Env::require_secret`] returns a value whose `Debug` is
//! redacted,
//! so that mistake is not available.
//!
//! Both are recorded in the design notes rather than being silent improvements.
//!
//! # Empty is a value
//!
//! `FOO=` sets `FOO` to the empty string. [`Env::require`] returns `Ok("")` for
//! it, matching `std::env::var` and the original settings library alike. That is faithful
//! and it is also a footgun — an empty `DATABASE_URL` is a misconfiguration
//! that reads as configured — so [`Env::require_non_empty`] exists for the
//! cases where a blank value cannot be meaningful.

#![deny(clippy::unwrap_used, clippy::expect_used)]
// `forbid` again, like every other crate here. It was `deny` because this crate
// mutated the process environment in its own tests, and `env::set_var` becomes
// an `unsafe fn` in edition 2024 -- so `forbid`, which cannot be locally
// overridden, would have made the test module uncompilable under
// `cargo fix --edition`. Nothing mutates the environment any more: see
// [`source`].
#![forbid(unsafe_code)]

pub mod env;
pub mod source;

pub use secrecy::SecretString;

pub use env::ConfigError;
pub use source::{Env, Reader};
