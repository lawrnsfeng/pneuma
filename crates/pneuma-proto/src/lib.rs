//! `pneuma-proto` — the wire messages exchanged between pneuma components.
//!
//! This crate defines *shapes*, not transport. It knows nothing about NATS,
//! AMQP, HTTP, or Restate; it holds the structs that cross a process boundary
//! and the serde attributes that keep them byte-compatible with the original and
//! original services already running in production.
//!
//! # Why this is separate from `pneuma-core`
//!
//! `pneuma-core` models the *domain* — what a pipeline is, how it resolves,
//! what a node status means. This crate models the *protocol* — what one
//! process says to another. They are deliberately not the same types. A domain
//! type is free to be renamed for clarity (`nth` became `sibling_index`); a
//! wire type is not free, because a peer that was not rebuilt is still sending
//! the old field name. Keeping them apart is what lets the domain be readable
//! without breaking compatibility.
//!
//! # The compatibility rule
//!
//! Every type here is pinned to a real message observed in the existing
//! services, cited in the module that defines it. Where two components
//! disagree about a field name for the same logical message, that drift is
//! recorded rather than silently normalised — a reader must be able to parse
//! what the current producers actually emit, not what they ought to emit.
//!
//! Renaming a field here is a breaking protocol change and must be treated as
//! one, even though the Rust type is internal.

#![deny(clippy::unwrap_used, clippy::expect_used)]
#![forbid(unsafe_code)]

pub mod backend;
pub mod component;
pub mod dispatch;
pub mod envelope;
pub mod event;
pub mod headers;
pub mod meta;
pub mod node;
pub mod payload;
pub mod timestamp;
