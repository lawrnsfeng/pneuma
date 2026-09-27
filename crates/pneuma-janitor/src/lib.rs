//! The janitor's passes over the run stores.
//!
//! Sequencing and nothing else. The queries belong to `pneuma-store` and the
//! HTTP to the binary; what is decided here is which operation follows which,
//! and what a failure part-way through means.

#![forbid(unsafe_code)]
#![deny(clippy::unwrap_used, clippy::expect_used)]
#![deny(missing_docs)]

pub mod boot;
pub mod cleanup;
pub mod connect;
pub mod settings;
pub mod terminate;

pub use boot::{run, BootError, Config, Mode, PassFreshness, SERVICE};
pub use cleanup::{Cleaned, Expired, Janitor, JanitorError, Pass};
pub use connect::Endpoints;
pub use settings::{Settings, SettingsError};
pub use terminate::{disposition, Disposition, Gateway, TerminateError};
