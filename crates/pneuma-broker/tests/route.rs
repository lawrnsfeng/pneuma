//! Where a message goes, decided without a broker.
//!
//! The rule this service exists for is one line long, and the reason it needs a
//! test is the defect notes: interpolated instead of validated, a
//! tenant id containing a `.` or a NATS wildcard produces a subject other than
//! the one intended, and nothing rejects it at any boundary. The message is
//! then delivered somewhere nobody is looking, or to *everybody*, with no error
//! raised at any point.

use pneuma_broker::{route, RouteError};
use pneuma_nats::Subject;
use serde_json::json;

fn subject(value: &str) -> Subject {
    match Subject::parse(value) {
        Ok(subject) => subject,
        Err(error) => panic!("{value} is a subject: {error}"),
    }
}

fn message(tenant: &str, job: &str) -> Vec<u8> {
    let body = json!({
        "meta": {
            "job_id": job, "tenant_id": tenant,
            "pipeline_type": "invoice", "pipeline_level": "page", "pipeline_name": "default",
            "a_caller_extra": "kept",
        },
        "step_input": {"doc": "d"},
    });
    match serde_json::to_vec(&body) {
        Ok(bytes) => bytes,
        Err(error) => panic!("that serialises: {error}"),
    }
}

#[test]
fn a_message_goes_to_its_tenants_subject() {
    let Ok(routed) = route(&subject("pneuma.input"), &message("acme", "job-1")) else {
        panic!("that is a routable message");
    };
    assert_eq!(routed.subject.as_str(), "pneuma.input.tenant_acme");
}

#[test]
fn a_tenant_id_that_would_break_the_subject_cannot_reach_one() {
    // The whole point. Each of these, interpolated, produces a subject other
    // than the one intended: a `.` invents a token, `*` and `>` are NATS
    // wildcards that make the message go to *everybody* subscribed below that
    // point, and a space is not a subject character at all.
    for bad in ["ac.me", "ac me", "*", ">", "acme.>", ""] {
        let outcome = route(&subject("pneuma.input"), &message(bad, "job-1"));
        match outcome {
            Err(RouteError::BadTenant { tenant, .. }) => assert_eq!(tenant, bad),
            // An empty id is refused as a missing tenant rather than as a bad
            // one, which is the more useful of the two things to be told.
            Err(RouteError::NoTenant) => assert_eq!(bad, ""),
            other => panic!("{bad:?} must not become a subject: {other:?}"),
        }
    }
}

#[test]
fn both_halves_of_the_originals_validity_check_are_told_apart() {
    // The original checks the job id and the tenant id together and answers
    // with a bare `false`, so its log says
    // "Invalid message" and a person has to guess which half. They have
    // different causes and different fixes.
    let Err(RouteError::NoJob) = route(&subject("pneuma.input"), &message("acme", "")) else {
        panic!("a message with no job is unroutable");
    };
    let Err(RouteError::NoTenant) = route(&subject("pneuma.input"), &message("", "job-1")) else {
        panic!("a message with no tenant is unroutable");
    };

    // Whitespace is not a job id either: a value read out of a rendered
    // template is blank far more often than it is deliberate.
    let Err(RouteError::NoJob) = route(&subject("pneuma.input"), &message("acme", "   ")) else {
        panic!("whitespace is not a job id");
    };
}

#[test]
fn a_body_that_is_not_a_message_is_named_as_such() {
    for bad in [&b"not json"[..], b"{}", b"{\"meta\": 3}"] {
        let Err(RouteError::NotAMessage(why)) = route(&subject("pneuma.input"), bad) else {
            panic!("{bad:?} is not a message");
        };
        assert!(!why.is_empty());
    }
}

#[test]
fn the_input_subject_is_whatever_it_arrived_on() {
    // The original demultiplexes several inputs, each into its own family of
    // tenant subjects -- so the destination is derived from the subject the
    // message came in on rather than from a constant.
    for input in ["pneuma.input", "pneuma.event", "other.thing"] {
        let Ok(routed) = route(&subject(input), &message("acme", "job-1")) else {
            panic!("that is a routable message");
        };
        assert_eq!(routed.subject.as_str(), format!("{input}.tenant_acme"));
    }
}
