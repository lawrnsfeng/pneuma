//! The HTTP door: one route that queues a run.
//!
//! Every decision worth making has already been made by [`accept()`](crate::accept::accept),
//! which is pure. What is left here is turning three outcomes into three
//! statuses, and that is worth getting right: the status is the only part of
//! this a caller's retry logic reads.
//!
//! # The statuses, and why each is that one
//!
//! - **202 Accepted** — queued, or already queued, or a dispatcher already has
//!   it. All three mean the same thing to the caller: it is in, and it has not
//!   run yet. A redelivery is the ordinary case for an at-least-once transport,
//!   so answering it differently would make callers write a branch that does
//!   the same thing on both sides.
//! - **409 Conflict** — this run id has already *run*. Not 202, which promises
//!   it will run: nothing will run it again under that id, so telling the
//!   caller to poll would have them poll for ever.
//! - **400 Bad Request** — the pipeline does not resolve, or an id is blank.
//!   The submission is wrong and will be wrong next time.
//! - **503 Service Unavailable** — the queue could not be reached. Not the
//!   caller's fault and worth retrying, which is exactly what 503 tells them
//!   and 500 does not.
//!
//! Two more come from axum's `Json` extractor before this module sees the
//! request, and they are **not** in the `{"error": ...}` shape the rest of this
//! answers in: a missing or wrong `content-type` is **415** and a body that is
//! not JSON at all is **400**, both as `text/plain`. Stated here because a
//! client written against the documented shape would otherwise fail to parse
//! them, and because "an error body, in one shape" was not true of the whole
//! surface.

use std::sync::Arc;

use async_trait::async_trait;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::post;
use axum::{Json, Router};
use pneuma_store::Accepted;
use serde_json::Value;

use crate::accept::{accept, Admitted, Rejected, Submission};

/// The queue, as this router needs it.
///
/// A trait so every status is a unit test against a hand-rolled fake — a
/// router test should not need Postgres to find out what a rejected pipeline
/// answers. It is a seam for testing, not a dependency boundary: this crate
/// depends on `pneuma-store` regardless, because a service that queues
/// submissions queues them somewhere.
#[async_trait]
pub trait Submissions: Send + Sync + 'static {
    /// Offers an admitted submission to the queue.
    ///
    /// The error is a string because this router does nothing with its
    /// structure: every failure to reach the queue is the same 503, and the
    /// message is for the log. A typed error here would be a type the fake has
    /// to construct in order to say "the database was down".
    async fn enqueue(&self, admitted: &Admitted, payload: &Value) -> Result<Accepted, String>;
}

/// What a caller is told.
///
/// `status` is a `String`, not the `&'static str` the handler has: a
/// `#[derive(Deserialize)]` over a borrowed field adds a `'de: 'static` bound,
/// so this type could be *written* and never read back. It is the response
/// body a client parses, so the derive has to be usable -- and one that only
/// compiles against a `'static` input is a derive nobody can call.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Receipt {
    /// The run this submission is, or already was.
    pub run_id: String,
    /// One of `queued`, `already_queued`, `already_claimed`, `already_ran`.
    pub status: String,
    /// Whether the earlier run succeeded — only for `already_ran`.
    ///
    /// Carried because the store distinguishes the two and this is the last
    /// place that can pass it on. A caller retrying a job needs to tell "it
    /// already succeeded, stop" from "it failed, and nothing will retry it
    /// under this id"; those are different actions, and a bare 409 is the same
    /// answer to both.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub succeeded: Option<bool>,
}

/// The ingress routes for `service`.
///
/// Mounted under the service's own name, matching `pneuma-serve`'s health
/// routes and the gateway before them: these are reached through one ingress,
/// and an unprefixed `/runs` on several services is one route several ways.
pub fn router<S: Submissions>(service: &str, submissions: Arc<S>) -> Router {
    Router::new()
        .route(&format!("/{service}/runs"), post(submit::<S>))
        .with_state(submissions)
}

/// `POST /{service}/runs`.
///
/// Extracts a `Value` and types it here rather than extracting a `Submission`
/// directly, for two reasons that turn out to be the same one.
///
/// **What is stored is what arrived.** The body goes to the queue verbatim, so
/// what a dispatcher eventually sends is the document this door resolved. A
/// round trip through `Submission` would drop any top-level key this type does
/// not know -- it has no `#[serde(flatten)]` catch-all -- so a field the runner
/// understands and this crate has not been taught would be silently stripped
/// between the two, and the two crates' shapes are already coupled by nothing
/// but convention.
///
/// **And the failure becomes reachable.** Re-serialising a `Submission` that
/// was just deserialised cannot fail, so the arm handling it was code no test
/// could reach. Typing the body here fails on a document that is JSON but not
/// a submission, which is an ordinary thing for a caller to send, and answers
/// 400 with this crate's own message instead of the extractor's.
async fn submit<S: Submissions>(
    State(submissions): State<Arc<S>>,
    Json(body): Json<Value>,
) -> impl IntoResponse {
    let submission: Submission = match serde_json::from_value(body.clone()) {
        Ok(submission) => submission,
        Err(error) => {
            // Built as a statement rather than inline in the call: a line that
            // is only an argument to a multi-line call is not attributed to the
            // arm that ran, so this read as dead while its message was being
            // asserted by a passing test.
            let message = format!("that is not a submission: {error}");
            return problem(StatusCode::BAD_REQUEST, &message);
        }
    };
    let admitted = match accept(&submission) {
        Ok(admitted) => admitted,
        Err(rejected) => return refused(&rejected),
    };
    let payload = identified_as(body, &admitted);
    match submissions.enqueue(&admitted, &payload).await {
        Ok(accepted) => receipt(&admitted, accepted),
        Err(message) => problem(StatusCode::SERVICE_UNAVAILABLE, &message),
    }
}

/// The body as it will be stored, with the two ids `accept` normalised.
///
/// Everything else is left exactly as it arrived -- see [`submit`] for why the
/// body is kept verbatim at all. These two fields are the exception because
/// they are not just data: `accept` trims them and derives the queue's primary
/// key from the result, while the runner re-derives run identity from the
/// *payload* (`Meta::run_id` is `RunId::from_job(&self.job_id)`). Leaving the
/// payload untrimmed makes those two disagree.
///
/// The failure is quiet and bad. A submission with `"job_id": "job-1\n"` -- the
/// exact template artefact the trim exists for -- is filed under `job-1` and
/// executed under `job-1\n`, so the run happens under an id the queue never
/// knew and settling refers to a different thing. A later genuine `"job-1"`
/// is then answered `AlreadyQueued` against a row whose payload says otherwise.
///
/// Normalising here rather than refusing an untrimmed id keeps the two
/// decisions in one place: `accept` already decided that whitespace around an
/// id is an artefact rather than an identity, and this makes the stored
/// document agree with that decision.
fn identified_as(mut body: Value, admitted: &Admitted) -> Value {
    // `meta` is an object: `body` deserialised into a `Submission`, whose
    // `meta` field is a struct, so it cannot be anything else. `if let` rather
    // than an error arm for exactly that reason -- there is no failure here to
    // report, only one that cannot happen.
    if let Some(meta) = body.get_mut("meta").and_then(Value::as_object_mut) {
        meta.insert(
            "job_id".to_owned(),
            Value::String(admitted.run_id.to_string()),
        );
        meta.insert(
            "tenant_id".to_owned(),
            Value::String(admitted.tenant_id.to_string()),
        );
    }
    body
}

/// The answer for a submission the queue took.
fn receipt(admitted: &Admitted, accepted: Accepted) -> (StatusCode, Json<Value>) {
    let (status, label) = status_for(accepted);
    let body = Receipt {
        run_id: admitted.run_id.to_string(),
        status: label.to_owned(),
        succeeded: match accepted {
            Accepted::AlreadyRan { succeeded } => Some(succeeded),
            _ => None,
        },
    };
    (status, Json(serde_json::json!(body)))
}

/// What each queue answer means over HTTP.
///
/// Named `status_for` rather than `disposition` so it cannot be confused with
/// [`crate::restate::disposition`], which classifies answers coming the other
/// way -- Restate's, to a submission this service made.
///
/// Pure, and separate from the handler so all four are unit tests: reaching
/// `AlreadyRan` through the router means settling a submission first, which
/// needs a database the router deliberately does not have.
pub fn status_for(accepted: Accepted) -> (StatusCode, &'static str) {
    match accepted {
        Accepted::Queued => (StatusCode::ACCEPTED, "queued"),
        // Both mean "in, and not run yet", which is what the caller acts on.
        Accepted::AlreadyQueued => (StatusCode::ACCEPTED, "already_queued"),
        Accepted::AlreadyClaimed => (StatusCode::ACCEPTED, "already_claimed"),
        // 409, not 202. Nothing will run this id again, so telling the caller
        // to poll would have them poll for ever.
        Accepted::AlreadyRan { .. } => (StatusCode::CONFLICT, "already_ran"),
    }
}

/// The answer for a submission that was refused before the queue saw it.
fn refused(rejected: &Rejected) -> (StatusCode, Json<Value>) {
    // Every rejection is the submission being wrong, and it will be wrong next
    // time -- so 400 throughout, and the message says which.
    problem(StatusCode::BAD_REQUEST, &rejected.to_string())
}

/// An error body, in one shape.
fn problem(status: StatusCode, message: &str) -> (StatusCode, Json<Value>) {
    (status, Json(serde_json::json!({ "error": message })))
}
