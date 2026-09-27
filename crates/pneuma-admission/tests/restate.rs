//! Every answer Restate can give a submission, and what it means.
//!
//! Two halves, and neither replaces the other.
//!
//! The **stub** half covers every arm, including ones a real container will not
//! produce on demand: a transport failure with no status to classify, a 5xx, a
//! label this code has never seen. Those are the arms a classifier gets wrong.
//!
//! The **container** half answers the one question a stub cannot: whether the
//! statuses the design notes recorded are still what `restatedev/restate`
//! sends. A stub agrees with whatever this file asserts, so on its own it would
//! pin this crate's beliefs rather than Restate's behaviour.
//!
//! ```sh
//! docker run -d --name pn-restate --add-host=host.docker.internal:host-gateway \
//!     -p 18080:8080 -p 19070:9070 restatedev/restate:1.7.8
//! cargo test -p pneuma-admission --test restate
//! ```

use async_trait::async_trait;
use pneuma_admission::{disposition, submit, Disposition, Invoker};
use serde_json::{json, Value};

/// An invoker that answers however the test says.
struct Stub(Result<(u16, Value), String>);

#[async_trait]
impl Invoker for Stub {
    async fn send(&self, _key: &str, _payload: &Value) -> Result<(u16, Value), String> {
        self.0.clone()
    }
}

#[test]
fn a_fresh_submission_and_a_redelivery_are_both_202_and_are_told_apart() {
    // The row of the design notes that a classifier written from first
    // principles gets wrong. `/send` answers 202 for both, and distinguishes
    // them in the body -- so reading only the status would count a redelivery
    // as a fresh submission, and reaching for 409 would produce a branch
    // Restate never takes.
    assert_eq!(
        disposition(202, &json!({"invocationId": "inv_1", "status": "Accepted"})),
        Disposition::Accepted
    );
    assert_eq!(
        disposition(
            202,
            &json!({"invocationId": "inv_1", "status": "PreviouslyAccepted"})
        ),
        Disposition::AlreadyKnown
    );
}

#[test]
fn a_202_with_a_label_this_code_has_never_seen_is_still_accepted() {
    // A 202 means Restate took it, whatever it called the state. Treating an
    // unrecognised label as a failure would make a future Restate version look
    // like an outage -- and the submission would be retried against a key that
    // already has it.
    for body in [
        json!({"status": "SomethingNewIn19"}),
        json!({"invocationId": "inv_1"}),
        json!({}),
        Value::Null,
    ] {
        assert_eq!(disposition(202, &body), Disposition::Accepted, "{body}");
    }
}

#[test]
fn a_blocking_submit_that_returned_the_report_is_completed() {
    // What a resubmission of a finished key does: 200 with the run's report, in
    // 5.8 ms and with zero component calls.
    let report = json!({"outputs": [["B", {"ok": true}]], "ran": ["A", "B"]});
    assert_eq!(disposition(200, &report), Disposition::Completed);
}

#[test]
fn a_4xx_is_permanent_and_a_5xx_is_worth_retrying() {
    // The distinction that decides whether a submission is dropped or tried
    // again, so getting it backwards either loses runs or retries a deployment
    // mistake for ever.
    let refused = json!({"code": 404, "message": "service not found", "source": "ingress"});
    let Disposition::Rejected(why) = disposition(404, &refused) else {
        panic!("an unknown service does not become known on a retry");
    };
    assert!(why.contains("service not found"), "{why}");
    assert!(why.contains("404"), "{why}");

    for status in [400, 403, 409, 422] {
        assert!(
            matches!(disposition(status, &Value::Null), Disposition::Rejected(_)),
            "{status} is the submission being wrong"
        );
    }

    let unwell = json!({"code": 500, "message": "internal", "source": "ingress"});
    let Disposition::Retry(why) = disposition(500, &unwell) else {
        panic!("restate being unwell is not the submission being wrong");
    };
    assert!(why.contains("internal"), "{why}");
    for status in [500, 502, 503, 504] {
        assert!(matches!(
            disposition(status, &Value::Null),
            Disposition::Retry(_)
        ));
    }
}

#[test]
fn the_three_4xx_that_mean_later_are_retried_rather_than_dropped() {
    // A blanket `400..=499 => Rejected` swallows these, and `Rejected` drops
    // the submission permanently. A gateway shedding load with 429 or a proxy
    // timing out the ingress POST with 408 is transient, and retrying is free:
    // the idempotency key is what makes a second attempt cost nothing.
    for status in [408, 425, 429] {
        assert!(
            matches!(disposition(status, &Value::Null), Disposition::Retry(_)),
            "{status} means later, not no"
        );
    }
}

#[test]
fn a_run_that_failed_is_told_apart_from_a_restate_that_is_unwell() {
    // Both are 5xx, so the status cannot do it -- but the `source` field can,
    // and that is measured: a terminal handler failure comes back as
    // `source: "invocation"`, while the ingress's own answers say `"ingress"`.
    //
    // It matters because Restate *caches* the failure. Under the deployment's
    // `idempotency_retention` -- a day by default -- every resubmission under
    // that key returns the same 500 in milliseconds with no work done, so a
    // caller told `Retry` loops until the window expires, makes no progress
    // and never records the run as failed.
    let terminal = json!({
        "code": 500,
        "message": "calling A failed: the component call did not succeed",
        "source": "invocation",
    });
    let Disposition::Failed(why) = disposition(500, &terminal) else {
        panic!("a run that ran and failed is not Restate being unwell");
    };
    assert!(why.contains("calling A failed"), "{why}");

    // The ingress's own 5xx, which is worth another attempt.
    let unwell = json!({"code": 503, "message": "unavailable", "source": "ingress"});
    assert!(matches!(disposition(503, &unwell), Disposition::Retry(_)));
    // And a 5xx with no `source` at all -- a proxy in front of Restate -- is
    // retryable, because nothing says the run itself was reached.
    assert!(matches!(
        disposition(502, &json!({"message": "bad gateway"})),
        Disposition::Retry(_)
    ));
}

#[test]
fn an_answer_with_no_message_still_says_what_the_status_was() {
    // A proxy in front of Restate is not obliged to use its error shape, so
    // falling back to nothing would put "restate refused" in a log with no way
    // to tell which refusal it was.
    let Disposition::Rejected(why) = disposition(400, &json!({"nonsense": true})) else {
        panic!("a 400 is a rejection whatever its body");
    };
    assert_eq!(why, "restate answered 400");
}

#[tokio::test]
async fn a_transport_failure_has_no_status_and_is_retryable() {
    // Distinct from a status this code does not recognise: nothing came back at
    // all. A connection that failed mid-flight may or may not have delivered,
    // and the idempotency key is exactly what makes trying again safe.
    let stub = Stub(Err("connection reset by peer".to_owned()));
    let Disposition::Retry(why) = submit(&stub, "job-1", &json!({})).await else {
        panic!("an unreachable restate is worth another attempt");
    };
    assert!(why.contains("connection reset"), "{why}");
    assert!(why.contains("could not reach restate"), "{why}");
}

#[tokio::test]
async fn submit_passes_the_answer_through_the_classifier() {
    let accepted = Stub(Ok((202, json!({"status": "Accepted"}))));
    assert_eq!(
        submit(&accepted, "job-1", &json!({})).await,
        Disposition::Accepted
    );
    let known = Stub(Ok((202, json!({"status": "PreviouslyAccepted"}))));
    assert_eq!(
        submit(&known, "job-1", &json!({})).await,
        Disposition::AlreadyKnown
    );
}

// ---------------------------------------------------------------------------
// The half a stub cannot do.
// ---------------------------------------------------------------------------

/// Restate's ingress, as the deployment's compose file exposes it.
const INGRESS: &str = "http://localhost:18080";

/// An invoker that really talks to Restate.
struct Ingress {
    client: reqwest::Client,
    handler: String,
}

#[async_trait]
impl Invoker for Ingress {
    async fn send(&self, key: &str, payload: &Value) -> Result<(u16, Value), String> {
        let url = format!("{INGRESS}/{}/send", self.handler);
        let response = self
            .client
            .post(&url)
            .header("idempotency-key", key)
            .json(payload)
            .send()
            .await
            .map_err(|error| error.to_string())?;
        let status = response.status().as_u16();
        // A body that is not JSON is not an error here: the classifier falls
        // back to the status, and pinning that is part of the point.
        let body = response.json::<Value>().await.unwrap_or(Value::Null);
        Ok((status, body))
    }
}

fn ingress(handler: &str) -> Ingress {
    let Ok(client) = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .build()
    else {
        panic!("the client must build");
    };
    Ingress {
        client,
        handler: handler.to_owned(),
    }
}

#[tokio::test]
async fn a_real_restate_still_answers_the_statuses_deviations_22_recorded() {
    // The one question the stub half cannot answer. Everything above pins what
    // this crate *believes* Restate says; this checks Restate still says it.
    //
    // No deployment is registered against this container by this test, so the
    // handler below does not exist -- which is exactly the case worth checking
    // without one: a submission to a service Restate does not know must be
    // `Rejected` and not retried, because retrying a deployment mistake for
    // ever is the silent hang this classification exists to prevent.
    let unknown = ingress("NoSuchService/run");
    let outcome = submit(&unknown, "admission-test-1", &serde_json::json!({})).await;
    match &outcome {
        Disposition::Rejected(why) => {
            assert!(
                why.contains("404") || why.contains("400"),
                "an unregistered service is a 4xx: {why}"
            );
        }
        // A container that is not running answers nothing, and this test
        // cannot tell that from Restate having changed its mind -- so it says
        // which, rather than passing on a `Retry` that means "no container".
        other => panic!(
            "expected a rejection from a real container at {INGRESS}; got {other:?}. \
             Is `pn-restate` running? See the header of this file."
        ),
    }
}
