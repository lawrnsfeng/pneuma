//! NATS subject construction, and the validation the running system lacks.
//!
//! Deliberately client-free. What lives here is the naming: which subject a
//! message goes to, and what makes a subject valid. The connection, the
//! JetStream consumer, and the topology reconciler are separate concerns and
//! will not compile into this crate.

#![deny(clippy::unwrap_used, clippy::expect_used)]
#![forbid(unsafe_code)]

pub mod subject;
pub mod topology;

pub use subject::{Subject, SubjectError, SubjectPattern, SubjectToken};
pub use topology::{
    Divergence, Replicas, Retention, StreamName, StreamNameError, StreamTopology, TopologyError,
};
