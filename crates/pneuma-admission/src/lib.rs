//! The ingress that admits runs to the fair dispatch queue.
//!
//! Everything a submission must survive before it costs anything: the pipeline
//! has to resolve, and the tenant id has to identify a flow. Both are decided
//! here, before a row is written, because the alternative is discovering them
//! after a dispatcher has picked the work up.
//!
//! # Why the pipeline is resolved at the door
//!
//! `pneuma-restate`'s handler resolves it too, and answers `400` when it does
//! not (`cannot resolve the pipeline`). By then the submission has been
//! queued, selected against a tenant's quota, claimed, and sent — so a
//! definition naming a step that does not exist consumes a slot in somebody's
//! fair share to produce an error that was knowable at the moment it arrived.
//! Resolution is pure and deterministic, so doing it twice costs nothing and
//! the second one is the one that matters for correctness.

#![deny(missing_docs)]
#![deny(clippy::unwrap_used, clippy::expect_used)]
#![forbid(unsafe_code)]

pub mod accept;
pub mod boot;
pub mod client;
pub mod config;
pub mod dispatch;
pub mod dispatcher;
pub mod ingress;
pub mod restate;
pub mod store;
pub mod supervise;

pub use accept::{accept, Admitted, Rejected, Submission, TENANT_DIMENSION};
pub use boot::{run, BootError, MAX_CONNECTIONS, SERVICE};
pub use client::{send_url, Ingress, IngressError, CONNECT_TIMEOUT, SUBMIT_TIMEOUT};
pub use config::{
    weights, Config, ConfigureError, BATCH_SIZE, DATABASE_URL, DEFAULT_HANDLER, DEFAULT_LISTEN,
    DISPATCH_INTERVAL_SECS, LISTEN, MAX_COUNT, MAX_SECONDS, PER_TENANT, RECLAIM_AFTER_SECS,
    RESTATE_HANDLER, RESTATE_INGRESS, TENANT_WEIGHTS,
};
pub use dispatch::{alarm, backlogs, group, round_base, Group};
pub use dispatcher::{report_line, round, tick, Dispatchable, RoundReport, Settings, Tick};
pub use ingress::{router, status_for, Receipt, Submissions};
pub use restate::{disposition, submit, Disposition, Invoker};
pub use supervise::{wedged, Invocation, Wedged, BACKING_OFF};
