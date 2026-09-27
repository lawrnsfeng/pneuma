//! Every status the ingress can answer, without a database.
//!
//! A `Router` is a `tower::Service`, so `ServiceExt::oneshot` drives a request
//! through the real routing, the real extractor and the real handler. The queue
//! is a hand-rolled fake, which is the point: finding out what a rejected
//! pipeline answers should not require Postgres, and the four outcomes the
//! queue can report include one — `already_ran` — that reaching through a real
//! database would mean claiming and settling a submission first.

use std::sync::Arc;
use std::sync::Mutex;

use async_trait::async_trait;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use pneuma_admission::{router, Admitted, Receipt, Submissions};
use pneuma_store::Accepted;
use serde_json::{json, Value};
use tower::ServiceExt;

/// A queue that answers however the test says, and records what it was given.
struct Fake {
    answer: Result<Accepted, String>,
    seen: Mutex<Vec<(String, String, Value)>>,
}

impl Fake {
    fn answering(answer: Result<Accepted, String>) -> Arc<Self> {
        Arc::new(Fake {
            answer,
            seen: Mutex::new(Vec::new()),
        })
    }
}

#[async_trait]
impl Submissions for Fake {
    async fn enqueue(&self, admitted: &Admitted, payload: &Value) -> Result<Accepted, String> {
        // A poisoned lock means another test panicked while holding it; there
        // is one test per fake, so recovering is right rather than cascading.
        let mut seen = match self.seen.lock() {
            Ok(seen) => seen,
            Err(poisoned) => poisoned.into_inner(),
        };
        seen.push((
            admitted.run_id.to_string(),
            admitted.tenant_id.to_string(),
            payload.clone(),
        ));
        self.answer.clone()
    }
}

fn body(tenant: &str, job: &str) -> Value {
    json!({
        "pipeline": {
            "pipeline_id": "p",
            "start": "A",
            "components": [
                {"node_id": "A", "name": "comp-a", "type": "Model", "children": ["end"]}
            ],
        },
        "meta": {
            "job_id": job,
            "tenant_id": tenant,
            "pipeline_type": "invoice",
            "pipeline_level": "page",
            "pipeline_name": "default",
        },
        "input": {"doc": "d"},
    })
}

/// One POST against a fresh router, and what came back.
async fn post(queue: Arc<Fake>, document: &Value) -> (StatusCode, Value) {
    let app = router("pneuma-admission", queue);
    let request = Request::builder()
        .method("POST")
        .uri("/pneuma-admission/runs")
        .header("content-type", "application/json")
        .body(Body::from(document.to_string()));
    let Ok(request) = request else {
        panic!("that is a request");
    };
    // No `else`: a `Router`'s error type is `Infallible`, so the binding is
    // exhaustive on its own.
    let Ok(response) = app.oneshot(request).await;
    let status = response.status();
    let Ok(collected) = response.into_body().collect().await else {
        panic!("the body reads");
    };
    let parsed = serde_json::from_slice(&collected.to_bytes()).unwrap_or(Value::Null);
    (status, parsed)
}

#[tokio::test]
async fn a_queued_submission_is_202_and_names_the_run() {
    let queue = Fake::answering(Ok(Accepted::Queued));
    let (status, answer) = post(Arc::clone(&queue), &body("acme", "job-1")).await;
    assert_eq!(status, StatusCode::ACCEPTED, "{answer}");
    assert_eq!(answer["run_id"], "job-1");
    assert_eq!(answer["status"], "queued");

    // The *whole submission* reached the queue, not the pieces `accept` read
    // out of it: what the dispatcher later sends must be what arrived, or the
    // resolution done at this door says nothing about the run that happens.
    let Ok(seen) = queue.seen.lock() else {
        panic!("the fake recorded the call");
    };
    assert_eq!(seen.len(), 1);
    let (run_id, tenant_id, payload) = &seen[0];
    assert_eq!(run_id, "job-1");
    assert_eq!(tenant_id, "acme");
    assert_eq!(payload["meta"]["tenant_id"], "acme");
    assert_eq!(payload["input"]["doc"], "d", "the input survives verbatim");
    assert!(payload["pipeline"]["components"].is_array());
}

#[tokio::test]
async fn a_redelivery_is_still_202_because_it_is_still_in() {
    // Both of these mean "in, and not run yet", which is what a caller acts on.
    // Answering them differently would make every caller write a branch that
    // does the same thing on both sides.
    for (accepted, label) in [
        (Accepted::AlreadyQueued, "already_queued"),
        (Accepted::AlreadyClaimed, "already_claimed"),
    ] {
        let (status, answer) = post(Fake::answering(Ok(accepted)), &body("acme", "job-1")).await;
        assert_eq!(status, StatusCode::ACCEPTED, "{answer}");
        assert_eq!(answer["status"], label);
    }
}

#[tokio::test]
async fn a_run_that_already_happened_is_409_and_says_how_it_went() {
    // 202 promises it will run. Nothing will run this id again, so a caller
    // told 202 would poll for ever.
    //
    // And the two outcomes are distinguishable, which an earlier version of
    // this test asserted they were *not* -- it looped over both booleans and
    // checked only what they had in common, locking in the loss. The store
    // keeps them apart for a reason: a caller retrying a job needs to tell "it
    // already succeeded, stop" from "it failed, and nothing will retry it
    // under this id", and those are different actions.
    for succeeded in [true, false] {
        let queue = Fake::answering(Ok(Accepted::AlreadyRan { succeeded }));
        let (status, answer) = post(queue, &body("acme", "job-1")).await;
        assert_eq!(status, StatusCode::CONFLICT, "{answer}");
        assert_eq!(answer["status"], "already_ran");
        assert_eq!(answer["succeeded"], succeeded, "{answer}");
    }

    // And it is absent, not `null`, for every other answer -- a caller reading
    // `succeeded` on a queued run is asking about a run that has not happened.
    let (_, queued) = post(Fake::answering(Ok(Accepted::Queued)), &body("a", "j")).await;
    assert!(queued.get("succeeded").is_none(), "{queued}");

    // The receipt is readable, which it was not: `status` was a
    // `&'static str`, so the derived `Deserialize` demanded a `'static` input
    // and no client could call it.
    let parsed: Receipt = match serde_json::from_str(&queued.to_string()) {
        Ok(parsed) => parsed,
        Err(error) => panic!("a client can parse its own receipt: {error}"),
    };
    assert_eq!(parsed.status, "queued");
    assert_eq!(parsed.succeeded, None);
}

#[tokio::test]
async fn a_submission_that_cannot_be_admitted_is_400_and_says_why() {
    // Never reaches the queue: the fake would answer `Queued` if it were
    // called, so a 400 here also proves nothing was enqueued.
    let ghost = json!({
        "pipeline": {
            "pipeline_id": "p",
            "start": "A",
            "components": [
                {"node_id": "A", "name": "comp-a", "type": "Model", "children": ["ghost"]}
            ],
        },
        "meta": {
            "job_id": "job-1", "tenant_id": "acme",
            "pipeline_type": "invoice", "pipeline_level": "page", "pipeline_name": "default",
        },
        "input": {},
    });
    let cases = [
        (ghost, "cannot be resolved"),
        (body("", "job-1"), "tenant id is blank"),
        (body("acme", ""), "job id is blank"),
    ];
    for (document, fragment) in cases {
        let queue = Fake::answering(Ok(Accepted::Queued));
        let (status, answer) = post(Arc::clone(&queue), &document).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{answer}");
        let error = answer["error"].as_str().unwrap_or_default();
        assert!(
            error.contains(fragment),
            "expected {fragment:?} in {error:?}"
        );
        let Ok(seen) = queue.seen.lock() else {
            panic!("the fake is readable");
        };
        assert!(seen.is_empty(), "nothing refused reaches the queue");
    }
}

#[tokio::test]
async fn a_queue_that_cannot_be_reached_is_503_rather_than_500() {
    // Not the caller's fault, and worth retrying -- which is what 503 tells
    // them and 500 does not.
    let queue = Fake::answering(Err("connection refused".to_owned()));
    let (status, answer) = post(queue, &body("acme", "job-1")).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{answer}");
    assert!(
        answer["error"]
            .as_str()
            .unwrap_or_default()
            .contains("connection refused"),
        "and the reason reaches the log: {answer}"
    );
}

#[tokio::test]
async fn a_body_that_is_json_but_not_a_submission_is_400_and_says_so() {
    // Answered by this crate rather than by the extractor, which is why the
    // body is taken as a `Value` and typed in the handler: the message names
    // what was wrong with the document instead of being axum's default.
    let queue = Fake::answering(Ok(Accepted::Queued));
    let (status, answer) = post(Arc::clone(&queue), &json!({"nonsense": true})).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{answer}");
    assert!(
        answer["error"]
            .as_str()
            .unwrap_or_default()
            .contains("not a submission"),
        "{answer}"
    );
    let Ok(seen) = queue.seen.lock() else {
        panic!("the fake is readable");
    };
    assert!(seen.is_empty());
}

#[tokio::test]
async fn what_reaches_the_queue_is_what_arrived_including_keys_we_do_not_know() {
    // `Submission` has no catch-all, so round-tripping the body through it
    // would drop any top-level key this crate has not been taught -- silently
    // stripping, between this door and the runner, a field the runner might
    // understand. The two crates' shapes are coupled by nothing but
    // convention, so the body is stored verbatim rather than re-serialised.
    let mut document = body("acme", "job-1");
    document["priority"] = json!("high");
    document["trace_id"] = json!("abc123");

    let queue = Fake::answering(Ok(Accepted::Queued));
    let (status, answer) = post(Arc::clone(&queue), &document).await;
    assert_eq!(status, StatusCode::ACCEPTED, "{answer}");

    let Ok(seen) = queue.seen.lock() else {
        panic!("the fake recorded the call");
    };
    let (_, _, payload) = &seen[0];
    assert_eq!(payload["priority"], "high", "an unknown key survives");
    assert_eq!(payload["trace_id"], "abc123");
    assert_eq!(*payload, document, "and the body is stored exactly as sent");
}

#[tokio::test]
async fn the_stored_payload_carries_the_ids_the_queue_filed_it_under() {
    // The one place the body is *not* verbatim, and it has to be. `accept`
    // trims the ids and derives the queue's primary key from the result, while
    // the runner re-derives run identity from the payload -- `Meta::run_id` is
    // `RunId::from_job(&self.job_id)`. Leave the payload untrimmed and the two
    // disagree: a submission with `"job-1\n"` is filed under `job-1` and
    // executed under `job-1\n`, so the run happens under an id the queue never
    // knew, and a later genuine `"job-1"` is answered `AlreadyQueued` against a
    // row whose payload says otherwise.
    let mut document = body("  acme\n", " job-1\n");
    document["priority"] = json!("high");

    let queue = Fake::answering(Ok(Accepted::Queued));
    let (status, answer) = post(Arc::clone(&queue), &document).await;
    assert_eq!(status, StatusCode::ACCEPTED, "{answer}");
    assert_eq!(answer["run_id"], "job-1");

    let Ok(seen) = queue.seen.lock() else {
        panic!("the fake recorded the call");
    };
    let (run_id, tenant_id, payload) = &seen[0];
    assert_eq!(run_id, "job-1");
    assert_eq!(tenant_id, "acme");
    assert_eq!(
        payload["meta"]["job_id"], "job-1",
        "the payload declares the id the queue filed it under: {payload}"
    );
    assert_eq!(payload["meta"]["tenant_id"], "acme", "{payload}");
    // And nothing else was touched.
    assert_eq!(payload["priority"], "high", "an unknown key still survives");
    assert_eq!(payload["input"]["doc"], "d");
    assert_eq!(payload["meta"]["pipeline_type"], "invoice");
}

#[tokio::test]
async fn the_route_lives_under_the_service_name() {
    let queue = Fake::answering(Ok(Accepted::Queued));
    let app = router("pneuma-admission", queue);
    let request = Request::builder()
        .method("POST")
        .uri("/runs")
        .header("content-type", "application/json")
        .body(Body::from(body("acme", "job-1").to_string()));
    let Ok(request) = request else {
        panic!("that is a request");
    };
    let Ok(response) = app.oneshot(request).await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}
