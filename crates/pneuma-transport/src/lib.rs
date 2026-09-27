//! The broker clients `pneuma-nats` and `pneuma-amqp` are forbidden to hold.
//!
//! Those two are naming and topology crates: their only dependencies are
//! `compact_str` and `thiserror`, and `scripts/forbid-deps.sh` keeps it that
//! way so a subject or a queue name can be validated without a runtime. This
//! crate is the other half — the part that connects, consumes and settles.
//!
//! # Two things here, both of them defect classes rather than features
//!
//! [`delivery`] makes "a message that was neither acknowledged nor returned"
//! unrepresentable. That is the shape of a live bug in the original message
//! provider, whose `defer DeleteMsg` deletes a message on *every* path out of
//! the handler including the failing ones — so work is dropped rather than
//! retried, and nothing anywhere records that it happened.
//!
//! [`supervise`] is the reconnect loop, split into a pure decision and a
//! three-line application. `RabbitMQReceiver.run`
//! is the original: an
//! unbounded `while True` around connect-and-consume with a **fixed** five
//! second delay, whose comment explains that `connect_robust` reconnects at
//! the TCP level and not at the application level, so without the loop a
//! cancelled `Channel.Open` leaves a permanently dead consumer.

#![deny(missing_docs)]
#![deny(clippy::unwrap_used, clippy::expect_used)]
#![forbid(unsafe_code)]

pub mod amqp;
pub mod delivery;
pub mod supervise;

pub use amqp::{
    Acknowledger, Amqp, AmqpError, Consuming, QueueSpec, DELIVERY_LIMIT, PREFETCH, QUEUE_TYPE,
    UNLIMITED,
};
pub use delivery::{Delivery, Disposition, Settle};
pub use supervise::{decide, delay, Action, Backoff, BackoffError, Event, Step};
