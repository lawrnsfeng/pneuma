//! What one call to a component means.
//!
//! The rule the original gets from substrings of a printed error message,
//! stated as a function of typed facts instead.

use pneuma_executor::{
    from_status, from_transport, is_retryable, Transport, Verdict, PREDICTION_REFUSED,
};
use serde_json::json;

#[test]
fn only_an_unreachable_component_is_worth_another_attempt() {
    // The three transport outcomes, and the reason they differ. A refused
    // connection may be a worker that has just been replaced. A timeout is a
    // model that took longer than its deadline, and will probably take that
    // long again -- the original does not retry it either, and making the run
    // wait for it twice is worse than telling it what happened.
    assert!(is_retryable(&from_transport(Transport::Unreachable)));
    assert!(!is_retryable(&from_transport(Transport::TimedOut)));
    assert!(!is_retryable(&from_transport(Transport::Broken)));

    let Verdict::TimedOut(why) = from_transport(Transport::TimedOut) else {
        panic!("a timeout is its own verdict, not a generic failure");
    };
    assert!(!why.is_empty());
}

#[test]
fn the_status_codes_are_the_originals_including_the_one_that_is_not_a_status() {
    // 449 is Component's own, not an IANA code: the model ran and refused the
    // input. Dropping it would put every refusal into the unknown-status arm.
    assert_eq!(PREDICTION_REFUSED, 449);

    let body = json!({"error_code": "E1", "error_message": "bad page"});
    assert!(matches!(
        from_status(PREDICTION_REFUSED, &body),
        Verdict::Refused(_)
    ));
    assert!(matches!(from_status(200, &json!({})), Verdict::Answered(_)));

    // 500 is retried: from a model server it is usually a worker that has just
    // been replaced. 400 is not: a request the component calls malformed is
    // malformed the second time too.
    assert!(is_retryable(&from_status(500, &json!({}))));
    assert!(!is_retryable(&from_status(400, &json!({}))));
}

#[test]
fn an_unexpected_status_is_a_named_failure_not_a_confusing_one() {
    // The original's `default` branch logs and returns
    // `nil` *without setting `result`* -- so the zero `SendResult` survives,
    // `Success` is false and `Response` is nil, and the handler's
    // `GetStringFromMap(nil, "error_message")` fails. The message is then
    // dead-lettered complaining about a missing map key rather than about the
    // status nobody expected.
    for status in [201, 302, 418, 503] {
        let Verdict::Failed(why) = from_status(status, &json!({})) else {
            panic!("{status} is not a status this understands");
        };
        assert!(why.contains(&status.to_string()), "it says which: {why}");
    }
}

#[test]
fn a_verdict_carries_the_body_it_was_decided_from() {
    // Both the answer and the refusal do, because what comes next reads them:
    // the step output for one, the error code and message for the other.
    let body = json!({"step_output": {"pages": 3}});
    let Verdict::Answered(carried) = from_status(200, &body) else {
        panic!("200 is an answer");
    };
    assert_eq!(carried, body);

    let refusal = json!({"error_code": "E1", "error_message": "bad page"});
    let Verdict::Refused(carried) = from_status(PREDICTION_REFUSED, &refusal) else {
        panic!("449 is a refusal");
    };
    assert_eq!(carried, refusal);
}
