//! Working out where one message goes. Pure.
//!
//! Every failure here is permanent: a body that will not parse will not parse
//! next time, and a tenant id that is not a subject token will not become one.
//! So the caller's answer to all of them is the same — do not retry — and the
//! difference between them is what a person reads.

use pneuma_nats::{Subject, SubjectError, SubjectToken};
use pneuma_proto::envelope::Message;

/// Where a message is going, and what to send.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Routed {
    /// The tenant-scoped subject.
    pub subject: Subject,
}

/// Why a message could not be routed.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RouteError {
    /// The body is not a message.
    #[error("not a message: {0}")]
    NotAMessage(String),

    /// The message carries no tenant.
    ///
    /// The original's `IsValid` checks exactly this and the job id,
    /// and answers with a
    /// bare `false` — so the log says "Invalid message" and a person has to
    /// guess which half. Named here, because the two have different causes.
    #[error("the message has no tenant")]
    NoTenant,

    /// The message carries no job.
    #[error("the message has no job")]
    NoJob,

    /// The tenant id is not something a subject can contain.
    ///
    /// The defect notes: interpolated instead, a `.` or a wildcard in
    /// the id produces a subject other than the one intended, and nothing
    /// rejects it — so the message is delivered somewhere nobody is looking,
    /// or to *everybody*, and no error is raised at any point.
    #[error("the tenant {tenant:?} cannot be part of a subject: {source}")]
    BadTenant {
        /// What the message claimed.
        tenant: String,
        /// Why a subject cannot contain it.
        source: SubjectError,
    },
}

/// Where `body` should be republished, given the subject it arrived on.
///
/// The message itself is not rewritten. The original republishes what it
/// received (the original marshals the
/// message it was handed), and a broker that re-encoded would be a broker that
/// could silently drop a field it does not model — which is exactly what this
/// service must never do, since everything downstream reads fields it has no
/// opinion about.
pub fn route(arrived_on: &Subject, body: &[u8]) -> Result<Routed, RouteError> {
    let message: Message = match serde_json::from_slice(body) {
        Ok(message) => message,
        Err(error) => return Err(RouteError::NotAMessage(error.to_string())),
    };
    // Both halves of the original's `IsValid`, told apart. A message with no
    // job is as unroutable as one with no tenant -- it would be delivered to a
    // tenant's subject and then fail wherever a job id is first needed.
    if message.meta.job_id.as_str().trim().is_empty() {
        return Err(RouteError::NoJob);
    }
    let tenant = message.meta.tenant_id.as_str().trim();
    if tenant.is_empty() {
        return Err(RouteError::NoTenant);
    }
    let token = SubjectToken::new(tenant).map_err(|source| RouteError::BadTenant {
        tenant: tenant.to_owned(),
        source,
    })?;
    Ok(Routed {
        subject: arrived_on.tenant_scoped(&token),
    })
}
