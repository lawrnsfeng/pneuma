//! Submitting an admitted run to Restate, and reading what it answers.
//!
//! # The classification is written against measurements, not guesses
//!
//! The design notes record what `restatedev/restate:1.7.8` actually
//! answers, taken from a real container with a real component counting its own
//! calls. Every arm below cites a row of that table rather than an expectation,
//! which matters because two of them are counter-intuitive: a redelivered
//! submission is a **202**, not a 409, and there is no 409 branch at all --
//! `/send` distinguishes the two cases in a machine-readable `status` field
//! instead. A classifier written from first principles would have had one.
//!
//! # Fire-and-forget, deliberately
//!
//! Submission uses `/send`, so a run lasting minutes does not depend on a live
//! HTTP connection from this process. The outcome is recovered later by
//! attaching, which §22 measured as working from a *fresh client* -- that
//! measurement is the reason `pneuma-restate` could stay an unkeyed service
//! rather than becoming a `#[workflow]`.
//!
//! # What is measured, and what is inferred
//!
//! Every status below comes from §22's table except one inference, named here
//! rather than left to be discovered: **a `/send` against a key whose run
//! already failed was not measured.** §22 measured the cached failure on a
//! *blocking* call, and measured `/send` against a key whose run *succeeded*
//! (a 202 `PreviouslyAccepted`). Whether `/send` against a failed key answers
//! 202 or the cached 500 is therefore unknown, and both are handled -- the
//! first as [`Disposition::AlreadyKnown`], the second as
//! [`Disposition::Failed`]. If it turns out to be 202, the failure is invisible
//! to this path and belongs to the watchdog instead.
//!
//! # The hazard §22 found, restated because it binds here
//!
//! Restate keys on the idempotency key alone and never compares request
//! bodies. A different pipeline submitted under a used key returns the *first*
//! submission's report, with 200 and nothing to say the body was ignored. That
//! is correct for an idempotency key and a silent wrong answer for a caller
//! that reuses a run id -- which is why [`accept()`](crate::accept::accept) refuses a blank job
//! id, and why the queue's primary key and the payload's `job_id` are made to
//! agree in [`crate::ingress`].

use async_trait::async_trait;
use serde_json::Value;

/// What Restate's answer to a *submission* means.
///
/// Submission only. Attaching for an outcome asks a different question -- a 404
/// there means "never submitted", which is a useful answer, while a 404 here
/// means the path is wrong -- and mapping both through one function would give
/// one of them the other's meaning. The dispatcher that attaches gets its own
/// classifier when it exists.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Disposition {
    /// Restate has it. Nothing more to do.
    Accepted,
    /// This key was already submitted, and the run is Restate's problem
    /// already. Not an error: it is what a redelivery looks like.
    AlreadyKnown,
    /// It already ran, and the answer came back with the submission.
    Completed,
    /// The submission is wrong and will be wrong next time.
    Rejected(String),
    /// The run itself ran and did not succeed, and Restate has cached that.
    ///
    /// Distinct from [`Disposition::Retry`], and the distinction is the whole
    /// difference between recording a failure and looping for a day. Under the
    /// deployment's `idempotency_retention` -- `1d` by default -- every
    /// resubmission under this key returns the same cached failure in
    /// milliseconds with **zero** component calls (the design notes). A
    /// caller told `Retry` would keep asking until the retention window
    /// expired, make no progress, and never record the run as failed.
    Failed(String),

    /// Something transient. Worth another attempt.
    Retry(String),
}

/// The `source` Restate puts on an error its *handler* produced.
///
/// Measured, and it is what separates "this run failed" from "Restate is
/// unwell": a terminal handler failure comes back as
/// `{"code":500,"message":"calling A failed: …","source":"invocation"}`, while
/// the ingress's own answers carry `"source":"ingress"` -- which is how
/// the design notes tells a 404 for an unknown key apart from anything
/// else. Both are 5xx or 4xx, so the status alone cannot do it.
const FROM_HANDLER: &str = "invocation";

/// Whether an answer came from the handler rather than from the ingress.
fn from_handler(body: &Value) -> bool {
    body.get("source").and_then(Value::as_str) == Some(FROM_HANDLER)
}

/// The `status` field `/send` returns for a key that had already been used.
///
/// Spelled out because it is Restate's wire value, not a rendering of anything
/// on this side: `{"invocationId":"…","status":"PreviouslyAccepted"}`
/// (the design notes).
///
/// Its counterpart `"Accepted"` deliberately has no constant. It is not matched
/// on -- the catch-all below covers it, along with any label a future Restate
/// introduces -- so a constant for it would be a name nothing uses, and the
/// compiler said so.
const PREVIOUSLY_ACCEPTED: &str = "PreviouslyAccepted";

/// Classifies Restate's answer to a submission.
///
/// Pure, so every arm is a unit test and none of them needs a container. The
/// fork between an unkeyed service and a `#[workflow]` is then a change of
/// what is *called*, not a redesign of what its answers mean.
pub fn disposition(status: u16, body: &Value) -> Disposition {
    match status {
        // `/send` answers 202 for both a new submission and a repeated one, and
        // distinguishes them in the body rather than the status
        // (the design notes). So the status alone is not enough here, and a
        // classifier that read only the status would call a redelivery a fresh
        // submission and count it twice.
        202 => match body.get("status").and_then(Value::as_str) {
            Some(PREVIOUSLY_ACCEPTED) => Disposition::AlreadyKnown,
            // `"Accepted"`, and anything else. Not matched explicitly: a 202
            // means Restate took it whatever it called the state, so the two
            // cases have the same answer -- and treating an unrecognised label
            // as a failure would make a future Restate version look like an
            // outage, with the submission retried against a key that already
            // has it.
            _ => Disposition::Accepted,
        },
        // A blocking submit that returned the run's report -- which is what a
        // resubmission of a finished key does, in 5.8 ms and with zero
        // component calls.
        200..=299 => Disposition::Completed,
        // The three 4xx that mean "later" rather than "no". A gateway shedding
        // load with 429, a proxy timing out the ingress POST with 408, a
        // client asked to wait with 425 -- all transient, and all swallowed by
        // a blanket `400..=499 => Rejected` that drops the submission
        // permanently. Retrying is free and safe here: the idempotency key is
        // what makes a second attempt cost nothing.
        408 | 425 | 429 => Disposition::Retry(describe(status, body)),
        // The submission is wrong: a malformed body, an unknown handler, a
        // service that is not registered. None of those change on a retry, and
        // retrying them is how a deployment mistake becomes a silent hang.
        400..=499 => Disposition::Rejected(describe(status, body)),
        // A 5xx the *handler* produced is the run having failed terminally,
        // and Restate caches it: every resubmission under this key returns it
        // again, in milliseconds, with no work done. Calling that `Retry`
        // means looping until the retention window expires without ever
        // recording the failure.
        _ if from_handler(body) => Disposition::Failed(describe(status, body)),
        // Anything else 5xx: Restate itself, or whatever is in front of it.
        _ => Disposition::Retry(describe(status, body)),
    }
}

/// What to put in the log, from whatever shape the answer had.
///
/// Restate's errors are `{"code":…,"message":…,"source":…}`, but a proxy or a
/// gateway in front of it is not obliged to be, so this falls back to the
/// status rather than to nothing.
fn describe(status: u16, body: &Value) -> String {
    match body.get("message").and_then(Value::as_str) {
        Some(message) => format!("restate answered {status}: {message}"),
        None => format!("restate answered {status}"),
    }
}

/// Somewhere a run can be submitted.
///
/// A trait so the classification above can be exercised against a stub for
/// every arm -- including a mid-response transport failure, which a real
/// container will not produce on demand -- while the real container answers
/// the one question a stub cannot: whether the statuses in the design notes
/// are still what 1.7.8 sends.
#[async_trait]
pub trait Invoker: Send + Sync {
    /// Submits `payload` under `idempotency_key`, fire-and-forget.
    ///
    /// Returns the status and the parsed body, which is what
    /// [`disposition`] classifies. A transport failure is `Err`, because there
    /// is no status to classify -- and that is a different thing from a status
    /// this code does not recognise.
    async fn send(&self, idempotency_key: &str, payload: &Value) -> Result<(u16, Value), String>;
}

/// Submits a run and says what became of it.
///
/// The `idempotency_key` is the run id, which is what makes a redelivery free:
/// §22 measured a repeated submission at 5.8 ms and **zero** component calls.
/// It is also why [`accept()`](crate::accept::accept) refuses a blank job id -- the key is the
/// whole of the identity Restate compares, and it never looks at the body.
pub async fn submit<I: Invoker + ?Sized>(
    invoker: &I,
    run_id: &str,
    payload: &Value,
) -> Disposition {
    match invoker.send(run_id, payload).await {
        Ok((status, body)) => disposition(status, &body),
        // No status came back, so there is nothing to classify. Retryable by
        // construction: a connection that failed mid-flight may or may not have
        // delivered, and the idempotency key is what makes trying again safe.
        Err(error) => Disposition::Retry(format!("could not reach restate: {error}")),
    }
}
