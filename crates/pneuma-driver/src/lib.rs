//! The broker-portability driver: `pneuma_runner::drive` over NATS.
//!
//! **Not a second engine.** `pneuma_runner::driver::Component` exists for
//! exactly this: a transport that makes a call and returns the response, with
//! every decision about *what* to call staying in `pneuma-interpreter`. The
//! fan-in is the local counter inside `Execution`, so
//! `CONCURRENCY-AND-DIRECTION.md` §1.5's race stays unrepresentable and no
//! barrier table, refcount, outbox or lease comes back.
//!
//! `scripts/forbid-symbols.sh` enforces that, because the temptation this
//! crate will face is specific and will arrive in a hurry: a run's `Execution`
//! lives in memory and is lost on a crash, and the obvious cure is to write the
//! barrier state down. That is the design the port removed.
//!
//! # Why a correlation table and not request-reply
//!
//! The original's controller does not wait for anything. It publishes a
//! `MessageRun` to the component's own subject
//! and returns; the result arrives
//! later, on a different subject, as a `MessageResult` its listener handles.
//! The original executor publishes that
//! result to a **fixed configured subject**
//! (the original,
//! `SendResult(config.Config.ResultQueue, ...)`), not to a reply inbox — so
//! NATS's own request-reply is not available without changing a service this
//! port must stay wire-compatible with.
//!
//! So the round trip is reassembled here: publish, register a waiter, and let
//! a background subscription hand the result back. [`correlate`] is that
//! table, and it is pure.
//!
//! # Every replica sees every result
//!
//! The result subject is shared, and a run's `Execution` lives in the replica
//! that started it — so a queue group would deliver each result to exactly one
//! replica, which is usually the wrong one. The subscription is therefore a
//! plain one: every replica sees every result and keeps the ones it is waiting
//! for. A result nobody here is waiting for is another replica's run, and
//! [`correlate::Delivered::Unclaimed`] is the ordinary case rather than an
//! error. The cost is that every replica reads all result traffic, which is
//! the price of holding execution state in memory — and is why
//! the design notes record Restate as the multi-tenant production path and
//! this as the deployment for where Restate cannot go.

#![deny(missing_docs)]
#![deny(clippy::unwrap_used, clippy::expect_used)]
#![forbid(unsafe_code)]

pub mod boot;
pub mod config;
pub mod correlate;
pub mod host;
pub mod message;
pub mod nats;

pub use boot::{run, BootError, SERVICE};
pub use config::{Config, ConfigureError, DATABASE_URL};
pub use correlate::{CallKey, Delivered, Occupied, Pending};
pub use host::{describe, pipeline_of, status_for, wire_status, HostError, RunOutcome};
pub use message::{
    as_response, key_of, kind_of, message_run, node_for, path_of, wire_kind, MessageError,
    RunContext,
};
pub use nats::{
    describe_received, receive, CallError, NatsComponent, Received, DEFAULT_TIMEOUT_SECS,
    DEFAULT_TIMEOUT_SECS_I64, PUBLISH_TIMEOUT_SECS,
};
