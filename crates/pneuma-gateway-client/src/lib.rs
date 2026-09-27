//! The `pneuma-gateway` HTTP contract: paths and wire types, no HTTP client.
//!
//! Client-free in the same way as `pneuma-nats` and `pneuma-amqp`. What a
//! request *is* — which path, carrying which id, expecting which body — is
//! decided here and can be tested without a socket. Choosing a transport is
//! left to whoever makes the call.
//!
//! # Why this crate exists
//!
//! The original client builds its paths with an f-string:
//!
//! ```text
//! url = f"/pneuma-gateway/api/v1/terminations/by-job/{job_id}"
//! ```
//!
//! `job_id` is caller-supplied and unvalidated, and nothing encodes it. That is
//! the defect notes, and it is not cosmetic: because the gateway
//! deletes by **LIKE prefix** rather than by equality, a `job_id` that gets
//! *truncated* on the way into a URL deletes more than it was asked to. A
//! `job_id` of `abc?x` becomes the path `.../by-job/abc`, and the gateway
//! executes `LIKE 'abc%'`.
//!
//! Here a path segment cannot be assembled except through
//! [`percent_encode_segment`].
//!
//! Encoding alone is not quite enough, which is worth stating because the fix
//! §13 prescribes for the original client has the same hole: `.` and `..` are RFC
//! 3986 *unreserved*, so `quote(safe="")` and this encoder both leave them
//! untouched, and the client then strips them as dot-segments before sending.
//! [`JobId`] refuses those two values outright for that reason — it is the one
//! case encoding cannot cover.
//!
//! # What is deliberately not validated
//!
//! Almost nothing about the *content* of a [`JobId`]. Percent-encoding makes
//! any byte sequence safe to carry in a path, so rejecting slashes or spaces
//! would refuse ids the system can handle perfectly well once they are encoded.
//! The only rejection is the one the gateway itself makes — an id that is empty
//! or all whitespace —
//! plus control characters, for the reasons in the design notes.

#![forbid(unsafe_code)]
#![deny(clippy::unwrap_used, clippy::expect_used)]
#![deny(missing_docs)]

pub mod endpoint;
pub mod job;
pub mod model;

pub use endpoint::{percent_encode_segment, Endpoint, API_PREFIX};
pub use job::{JobId, JobIdError};
pub use model::{
    CreateTerminationRequest, DataResponse, DeleteResponse, ItemsResponse, ListResponse,
    Termination, TerminationStatus,
};
