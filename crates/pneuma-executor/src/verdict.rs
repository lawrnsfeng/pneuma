//! What one call to a component means, decided from typed facts.
//!
//! # The defect this replaces
//!
//! The original decides whether a
//! transport failure is worth retrying by **matching its error text**:
//!
//! ```go
//! if (strings.Contains(err.Error(), "dial tcp") && strings.Contains(err.Error(), "connection refused")) ||
//!     strings.Contains(err.Error(), "EOF") ||
//!     (strings.Contains(err.Error(), "read tcp") && strings.Contains(err.Error(), "connection reset by peer")) {
//! ```
//!
//! Three sentences of prose from a standard library, matched as substrings. The
//! comment above them is the error strings themselves, copied from a terminal
//! — which is the honest admission that nothing else pins them. An original upgrade
//! that rewords `connection refused`, a proxy that reports a refusal
//! differently, or an IPv6 address formatted another way all turn a retryable
//! failure into a permanent one, silently: the run is failed instead of
//! retried, and nothing says the classifier stopped matching.
//!
//! Here the same decision comes off `reqwest::Error`'s own predicates, which
//! are what the client knows rather than what it prints.
//!
//! # The status codes are the original's, including the odd one
//!
//! The status codes: 200 is success, 449 is a prediction error, 500 is
//! retried, 400 is not. **449 is not an HTTP status** — it is Seldon's, and it
//! means the model ran and refused the input, which is a fact about the request
//! rather than about the service. The `jsonData` envelope Seldon also gave this
//! system is gone (the design notes); this code is not, because a deployed
//! model that sends it today would otherwise fall into the unknown-status arm.

use serde_json::Value;

/// Seldon's status for "the model ran and refused the input".
///
/// Not an IANA-registered code. Named rather than inlined because a bare `449`
/// in a match arm reads like a typo for 400 or 499.
pub const PREDICTION_REFUSED: u16 = 449;

/// What the transport reported, in the terms the classifier needs.
///
/// A small enum rather than the client's error type, so the rule is decided
/// without a client at all — and so the one thing that matters about a
/// transport failure, whether trying again could help, is stated by whoever
/// knows rather than inferred from a message.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Transport {
    /// The request did not reach the component: a refused connection, a reset,
    /// a connection closed mid-response. Trying again can help.
    Unreachable,
    /// The component did not answer in time.
    ///
    /// Not retried, matching the original: a model that took longer than the
    /// request timeout is a model that will probably take that long again, and
    /// the run is told it timed out rather than made to wait for it twice.
    TimedOut,
    /// Something else about the request itself was wrong.
    Broken,
}

/// What one attempt produced.
#[derive(Debug, Clone, PartialEq)]
pub enum Verdict {
    /// The component answered, and this is the answer.
    Answered(Value),
    /// The model ran and refused the input.
    Refused(Value),
    /// Worth another attempt.
    Retry(String),
    /// The component did not answer in time.
    TimedOut(String),
    /// Wrong in a way another attempt cannot fix.
    Failed(String),
}

/// What a transport failure means.
pub fn from_transport(transport: Transport) -> Verdict {
    match transport {
        Transport::Unreachable => Verdict::Retry("could not reach the component".to_owned()),
        Transport::TimedOut => Verdict::TimedOut("the component did not answer in time".to_owned()),
        Transport::Broken => Verdict::Failed("the request could not be made".to_owned()),
    }
}

/// What an answered request means.
///
/// The unknown-status arm is a *failure*, which the original gets wrong. Its
/// `default` branch logs and returns `nil` **without
/// setting `result`** — so the zero `SendResult` survives, `Success` is false,
/// `Response` is nil, and the handler's `GetStringFromMap(nil, "error_message")`
/// fails; the message is dead-lettered with an error about a missing map key
/// rather than about the status nobody expected.
pub fn from_status(status: u16, body: &Value) -> Verdict {
    match status {
        200 => Verdict::Answered(body.clone()),
        PREDICTION_REFUSED => Verdict::Refused(body.clone()),
        // Retried, as the original retries it: a 500 from a model server is
        // usually a worker that has just been replaced.
        500 => Verdict::Retry(format!("the component answered {status}")),
        // Not retried. A request the component calls malformed is malformed the
        // second time too.
        400 => Verdict::Failed(format!("the component answered {status}")),
        other => Verdict::Failed(format!(
            "the component answered {other}, which is not a status this understands"
        )),
    }
}

/// Whether another attempt could help.
///
/// Separate from the verdict so the retry loop has no knowledge of what the
/// verdicts *mean*, only of which one it may try again.
pub fn is_retryable(verdict: &Verdict) -> bool {
    matches!(verdict, Verdict::Retry(_))
}
