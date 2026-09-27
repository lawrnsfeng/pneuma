//! One input subject in, one subject per tenant out.
//!
//! the original broker in Rust. It does one thing: read a message, work out
//! whose it is, and republish it to that tenant's subject.
//! No database, no state, no
//! decisions about the message beyond whether it can be routed at all.
//!
//! # The whole point is that a bad tenant id cannot reach a subject
//!
//! The defect notes: the id is interpolated into subjects at three
//! sites with no validation, so a tenant whose id contains a `.`, a space or a
//! NATS wildcard produces a subject other than the one intended — and nothing
//! rejects it at any boundary, so the misrouting is silent. Here the id has to
//! become a `SubjectToken` before it can be appended, and
//! `Subject::tenant_scoped` takes nothing else. A tenant that would break the
//! grammar is refused at the door with a reason.
//!
//! # Deliberately not a JetStream consumer
//!
//! The original uses a durable JetStream queue subscription, and this uses a
//! core-NATS queue subscription. The design notes record why: the durable's
//! value is redelivery after a crash, and this service does nothing between
//! receiving and republishing that a redelivery could repair — it holds no
//! state, so a message lost to a crash here is one the *upstream* publisher
//! still has to have handled. Adding a durable would add an ack window and a
//! `MaxDeliver` cliff to a service whose entire body is one publish.

#![deny(missing_docs)]
#![deny(clippy::unwrap_used, clippy::expect_used)]
#![forbid(unsafe_code)]

pub mod boot;
pub mod config;
pub mod route;

pub use boot::{describe, forward, run, BootError, PUBLISH_TIMEOUT, SERVICE};
pub use config::{queue_group, subjects, Config, ConfigureError};
pub use route::{route, RouteError, Routed};
