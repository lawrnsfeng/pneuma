//! Matching late results to the calls waiting for them.
//!
//! Three outcomes, and the two that are not "taken" are the interesting ones:
//! a result nobody here wants is the ordinary case on a shared subject, and a
//! second call under a key that is already outstanding must be refused rather
//! than allowed to orphan the first.

use pneuma_driver::{CallKey, Delivered, Pending};
use serde_json::json;

fn key(run: &str, node: &str) -> CallKey {
    CallKey::new(run, node)
}

#[tokio::test]
async fn a_result_reaches_the_call_that_was_waiting() {
    let pending = Pending::new();
    let Ok(waiting) = pending.register(key("job-1", "A")).await else {
        panic!("nothing is outstanding yet");
    };
    assert_eq!(pending.outstanding().await, 1);

    assert_eq!(
        pending
            .deliver(&key("job-1", "A"), json!({"step_output": 7}))
            .await,
        Delivered::Taken
    );
    assert_eq!(pending.outstanding().await, 0, "and the entry is gone");

    let Ok(result) = waiting.await else {
        panic!("the waiter should be given the result");
    };
    assert_eq!(result, json!({"step_output": 7}));
}

#[tokio::test]
async fn a_result_for_another_replicas_run_is_not_an_error() {
    // The result subject is shared and a run's execution lives in the replica
    // that started it, so a queue group would hand each result to exactly one
    // replica -- usually the wrong one. Every replica therefore sees every
    // result, and most of them belong to somebody else.
    let pending = Pending::new();
    assert_eq!(
        pending.deliver(&key("someone-elses", "A"), json!({})).await,
        Delivered::Unclaimed
    );
    assert_eq!(pending.outstanding().await, 0);
}

#[tokio::test]
async fn a_second_call_under_the_same_key_is_refused_not_substituted() {
    // Replacing the waiter drops the first one's sender, so the call that
    // registered it never hears anything and waits for ever -- a run wedged
    // with no error anywhere. It is reachable: the ingress is not acknowledged
    // until a run completes, so a redelivery while the first attempt is still
    // running starts a second execution for the same run id.
    let pending = Pending::new();
    let Ok(first) = pending.register(key("job-1", "A")).await else {
        panic!("nothing is outstanding yet");
    };
    let Err(occupied) = pending.register(key("job-1", "A")).await else {
        panic!("the first call is still outstanding");
    };
    assert_eq!(occupied.key, key("job-1", "A"));
    assert!(occupied.to_string().contains("job-1"), "{occupied}");

    // And the first waiter is untouched, which is the point.
    assert_eq!(
        pending
            .deliver(&key("job-1", "A"), json!({"step_output": 1}))
            .await,
        Delivered::Taken
    );
    let Ok(result) = first.await else {
        panic!("the original waiter is still the one that gets it");
    };
    assert_eq!(result, json!({"step_output": 1}));
}

#[tokio::test]
async fn the_same_step_of_a_different_run_is_a_different_call() {
    let pending = Pending::new();
    let Ok(one) = pending.register(key("job-1", "A")).await else {
        panic!("nothing is outstanding yet");
    };
    let Ok(_two) = pending.register(key("job-2", "A")).await else {
        panic!("a different run is a different call");
    };
    assert_eq!(pending.outstanding().await, 2);

    assert_eq!(
        pending
            .deliver(&key("job-1", "A"), json!({"step_output": "one"}))
            .await,
        Delivered::Taken
    );
    assert_eq!(pending.outstanding().await, 1, "the other is still waiting");
    let Ok(result) = one.await else {
        panic!("the right waiter got it");
    };
    assert_eq!(result, json!({"step_output": "one"}));
}

#[tokio::test]
async fn a_late_result_is_not_given_to_whatever_is_outstanding_now() {
    // The node is in the key for this, not for uniqueness: `drive` awaits each
    // call before asking for the next task, so one run has one call in flight
    // and the run id alone would identify it. But a result that arrives after
    // its call was abandoned would then be handed to a *different step's* call
    // -- one step's output delivered as another's, which is a wrong answer
    // rather than an error.
    let pending = Pending::new();
    let Ok(_stale) = pending.register(key("job-1", "A")).await else {
        panic!("nothing is outstanding yet");
    };
    pending.forget(&key("job-1", "A")).await;
    let Ok(current) = pending.register(key("job-1", "B")).await else {
        panic!("the next step registers cleanly");
    };

    assert_eq!(
        pending
            .deliver(&key("job-1", "A"), json!({"step_output": "stale"}))
            .await,
        Delivered::Unclaimed,
        "the abandoned step's result goes nowhere"
    );
    assert_eq!(
        pending
            .deliver(&key("job-1", "B"), json!({"step_output": "current"}))
            .await,
        Delivered::Taken
    );
    let Ok(result) = current.await else {
        panic!("B gets B's output");
    };
    assert_eq!(result, json!({"step_output": "current"}));
}

#[tokio::test]
async fn a_result_for_a_caller_that_stopped_waiting_says_so() {
    // Distinct from unclaimed, because it means something *here* gave up -- a
    // cancelled run, or a `drive` that returned early on another step's
    // failure. Worth a log line, where an unclaimed result is worth nothing.
    let pending = Pending::new();
    let Ok(waiting) = pending.register(key("job-1", "A")).await else {
        panic!("nothing is outstanding yet");
    };
    drop(waiting);
    assert_eq!(
        pending.deliver(&key("job-1", "A"), json!({})).await,
        Delivered::Abandoned
    );
}

#[tokio::test]
async fn forgetting_a_call_that_was_never_registered_says_so() {
    // `forget` runs when a run ends for any reason, including one that never
    // got as far as a call -- so it has to be safe to call twice.
    let pending = Pending::new();
    assert!(!pending.forget(&key("job-1", "A")).await);
    let Ok(_waiting) = pending.register(key("job-1", "A")).await else {
        panic!("nothing is outstanding yet");
    };
    assert!(pending.forget(&key("job-1", "A")).await);
    assert!(!pending.forget(&key("job-1", "A")).await, "twice is safe");
    assert_eq!(pending.outstanding().await, 0);
}

#[tokio::test]
async fn two_replicas_worth_of_traffic_does_not_confuse_one_table() {
    // The shape the shared subject produces: a handful of calls outstanding,
    // and a stream of results most of which belong to somebody else.
    let pending = Pending::new();
    let mut waiting = Vec::new();
    for run in ["job-1", "job-2", "job-3"] {
        let Ok(receiver) = pending.register(key(run, "A")).await else {
            panic!("{run} is a fresh call");
        };
        waiting.push((run, receiver));
    }
    for stranger in ["other-1", "other-2", "job-1"] {
        let node = if stranger == "job-1" { "B" } else { "A" };
        assert_eq!(
            pending.deliver(&key(stranger, node), json!({})).await,
            Delivered::Unclaimed,
            "{stranger}.{node} is not one of ours"
        );
    }
    assert_eq!(pending.outstanding().await, 3, "none of them were consumed");

    for (run, receiver) in waiting {
        assert_eq!(
            pending
                .deliver(&key(run, "A"), json!({"step_output": run}))
                .await,
            Delivered::Taken
        );
        let Ok(result) = receiver.await else {
            panic!("{run} should get its own output");
        };
        assert_eq!(result, json!({"step_output": run}));
    }
    assert_eq!(pending.outstanding().await, 0);
}
