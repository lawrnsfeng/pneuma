//! What gets published about a verdict.
//!
//! Pure. The original interleaves this with the sending, so which event a
//! failure produces is decided inside a function that is also making HTTP
//! requests.

use pneuma_core::status::NodeStatus;
use pneuma_executor::{report, started, step_output, Verdict};
use pneuma_proto::dispatch::MessageRun;
use serde_json::json;

fn run() -> MessageRun {
    let body = json!({
        "meta": {
            "job_id": "job-1", "tenant_id": "acme",
            "pipeline_type": "invoice", "pipeline_level": "page", "pipeline_name": "default",
        },
        "node": {
            "path": "job-1.invoice.page.default.A",
            "node_id": "A",
            "name": "invoice.page.default.A",
            "kind": "Model",
            "pipeline_id": "invoice.page.default",
            "run_id": "job-1",
        },
        "step_input": {"doc": "d"},
    });
    match serde_json::from_value(body) {
        Ok(run) => run,
        Err(error) => panic!("that is a run message: {error}"),
    }
}

#[test]
fn a_step_is_announced_before_it_is_attempted() {
    // Unconditionally, as the original announces it -- including for a call
    // that is about to fail, so a step that never finishes still shows as
    // having started rather than never appearing at all.
    let event = started(&run());
    assert_eq!(event.event, NodeStatus::Processing);
    assert_eq!(
        event.node.map(|info| info.node_id.as_str().to_owned()),
        Some("A".to_owned())
    );
    assert_eq!(event.error_code, None);
}

#[test]
fn an_answer_produces_a_finished_event_and_a_result() {
    let body = json!({"step_output": {"pages": 3}});
    let verdict = Verdict::Answered(body);
    let Some(output) = step_output(&verdict) else {
        panic!("an answer carries a step output");
    };
    let produced = report(&run(), &verdict, Some(&output));
    assert_eq!(produced.event.event, NodeStatus::Finished);
    let Some(result) = produced.result else {
        panic!("an answer produces a result");
    };
    assert_eq!(result.node.node_id.as_str(), "A");
    assert_eq!(
        result
            .step_output
            .as_object()
            .and_then(|map| map.get("pages")),
        Some(&json!(3))
    );
    // The result carries the input and the reply addresses forward, because the
    // next step reads them.
    assert_eq!(
        result.step_input.as_object().and_then(|map| map.get("doc")),
        Some(&json!("d"))
    );
}

#[test]
fn a_refusal_carries_the_components_own_error_code_and_message() {
    // The model ran and said no. An error, not a timeout: the component
    // answered, and what it said was the answer.
    let verdict = Verdict::Refused(json!({
        "error_code": "E_BAD_PAGE",
        "error_message": "page 3 is not an invoice",
    }));
    let produced = report(&run(), &verdict, None);
    assert_eq!(produced.event.event, NodeStatus::Error);
    assert_eq!(produced.event.error_code.as_deref(), Some("E_BAD_PAGE"));
    assert_eq!(
        produced.event.error_message.as_deref(),
        Some("page 3 is not an invoice")
    );
    // No result: a result is what the next step reads, and there is not one.
    assert!(produced.result.is_none());
}

#[test]
fn a_timeout_is_its_own_event_and_everything_else_is_an_error() {
    let timed_out = report(&run(), &Verdict::TimedOut("too slow".to_owned()), None);
    assert_eq!(timed_out.event.event, NodeStatus::TimedOut);
    assert_eq!(timed_out.event.error_message.as_deref(), Some("too slow"));

    // A retryable failure that ran out of attempts is an error, not a pending
    // retry: by the time this is reported, there are no attempts left.
    for verdict in [
        Verdict::Retry("could not reach the component".to_owned()),
        Verdict::Failed("the component answered 418".to_owned()),
    ] {
        let produced = report(&run(), &verdict, None);
        assert_eq!(produced.event.event, NodeStatus::Error, "{verdict:?}");
        assert!(produced.result.is_none());
    }
}

#[test]
fn an_answer_that_is_not_the_shape_a_component_answers_with_has_no_output() {
    // A 200 whose body has no `step_output` is a component that answered
    // without answering. The original dead-letters it; here it becomes a
    // result-less report, so the run is told the step ended rather than left
    // waiting for one that is never coming.
    for body in [json!({}), json!({"step_output": null}), json!(3)] {
        assert_eq!(
            step_output(&Verdict::Answered(body.clone())),
            None,
            "{body}"
        );
    }

    // And a scalar output is refused rather than forwarded: the wire allows an
    // object or an array, and the original services forwarding a scalar is a
    // live divergence the protocol notes record -- the original refuses it
    // later, further from whatever produced it.
    let scalar = json!({"step_output": 3});
    assert_eq!(step_output(&Verdict::Answered(scalar)), None);

    // Nothing but an answer carries one.
    assert_eq!(step_output(&Verdict::TimedOut("x".to_owned())), None);
}

#[test]
fn a_component_is_sent_the_whole_body_not_just_the_step_input() {
    // The contract `docs/as-built/component-api.md` publishes. This posted the
    // step input alone until a review caught it -- which was wrong against the
    // original too, whose `MessageSeldonInputV2` carries all five fields.
    let request = pneuma_executor::boot::request_for(&run());
    let Ok(encoded) = serde_json::to_value(&request) else {
        panic!("a request serialises");
    };
    assert_eq!(encoded.pointer("/step_input/doc"), Some(&json!("d")));
    assert_eq!(encoded.pointer("/meta/tenant_id"), Some(&json!("acme")));
    // Narrowed: a component never sees the derived `pipeline_id`, nor any
    // caller extra the controller's envelope carries.
    assert_eq!(encoded.pointer("/meta/pipeline_id"), None);
}

#[test]
fn the_three_optional_keys_are_absent_rather_than_empty() {
    // the original's `omitempty` omits a nil map *and* an empty one, and a component may
    // distinguish "no custom data" from `{}`. Present-versus-absent is the one
    // thing observable to a zero-SDK component, so it is asserted both ways.
    let bare = pneuma_executor::boot::request_for(&run());
    let Ok(encoded) = serde_json::to_value(&bare) else {
        panic!("a request serialises");
    };
    for key in ["custom_data", "node_env_vars", "headers"] {
        assert!(encoded.get(key).is_none(), "{key} should be omitted");
    }

    let mut carrying = run();
    carrying
        .custom_data
        .insert("requested_by".to_owned(), json!("batch-loader"));
    carrying
        .node_env_vars
        .insert("MODEL_VARIANT".into(), "large".into());
    carrying
        .headers
        .insert("traceparent".to_owned(), "00-a-b-01".to_owned());
    let Ok(encoded) = serde_json::to_value(pneuma_executor::boot::request_for(&carrying)) else {
        panic!("a request serialises");
    };
    assert_eq!(
        encoded.pointer("/custom_data/requested_by"),
        Some(&json!("batch-loader"))
    );
    assert_eq!(
        encoded.pointer("/node_env_vars/MODEL_VARIANT"),
        Some(&json!("large"))
    );
    assert_eq!(
        encoded.pointer("/headers/traceparent"),
        Some(&json!("00-a-b-01"))
    );
}
