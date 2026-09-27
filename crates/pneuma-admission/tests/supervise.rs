//! When a run counts as wedged, and every reason it does not.
//!
//! Pure, so the whole table is here. The arms that answer "no" matter more than
//! the one that answers "yes": a watchdog that cries wolf is one that gets
//! switched off, and then the poison run it existed for waits for ever anyway.

use pneuma_admission::{wedged, Invocation, BACKING_OFF};

/// The shape the VERDICT's own query returns:
/// `SELECT id, target, status, retry_count FROM sys_invocation`.
fn seen(status: &str, retry_count: u32) -> Invocation {
    Invocation {
        id: "inv_1bnLw8".to_owned(),
        target: "PneumaRunner/run".to_owned(),
        status: status.to_owned(),
        retry_count,
    }
}

#[test]
fn a_run_still_backing_off_with_a_climbing_count_is_wedged() {
    // Exactly what the spike measured: the invocation stayed `backing-off`
    // while its `retry_count` went 5, 6, 7 and nothing gave up.
    let Some(alarm) = wedged(&seen(BACKING_OFF, 5), &seen(BACKING_OFF, 7), 7) else {
        panic!("a run that keeps failing and keeps retrying is stuck");
    };
    assert_eq!(alarm.id, "inv_1bnLw8");
    assert_eq!(alarm.retry_count, 7);
    // The target is carried so the alarm names what is stuck, and the id so it
    // can be killed -- `DELETE /invocations/{id}?mode=kill`, which the spike
    // measured actually stopping the retries rather than hiding them.
    assert_eq!(alarm.target, "PneumaRunner/run");
}

#[test]
fn a_run_that_moved_on_is_not_wedged_whatever_it_was_doing_before() {
    // The current sample is the truth. A run that was backing off and is now
    // anything else -- finished, killed, running again -- is not stuck, and
    // alarming on the earlier sample would report a run that recovered.
    for now in ["completed", "invoked", "suspended", "killed"] {
        assert_eq!(
            wedged(&seen(BACKING_OFF, 9), &seen(now, 9), 1),
            None,
            "{now} is not backing off"
        );
    }
}

#[test]
fn one_sample_of_a_retry_is_not_evidence_of_anything() {
    // Retrying is what the policy is *for*. A component restarting produces a
    // few attempts and then stops, so a single observation cannot tell a stuck
    // run from a recovering one.
    assert_eq!(wedged(&seen("invoked", 0), &seen(BACKING_OFF, 9), 1), None);
}

#[test]
fn an_unchanged_count_still_raises_the_alarm() {
    // The case that would have silenced this for exactly the runs it exists
    // for. Restate's backoff is exponential up to a ceiling, so the longer a
    // run is stuck the further apart its attempts get -- the spike watched the
    // gap widen inside forty seconds. Requiring the count to *grow* between
    // samples makes the alarm depend on polling more slowly than the current
    // backoff interval, so a watchdog sampling every minute against a
    // ten-minute ceiling sees the same `retry_count` in every consecutive pair
    // and stays quiet for ever -- the poison run waiting silently that this
    // rule exists to prevent.
    //
    // Two `backing-off` samples of one id is the evidence. The count growing
    // is a bonus.
    let Some(alarm) = wedged(&seen(BACKING_OFF, 9), &seen(BACKING_OFF, 9), 7) else {
        panic!("a run sampled twice mid-backoff is still stuck");
    };
    assert_eq!(alarm.retry_count, 9);
}

#[test]
fn a_count_that_fell_is_not_evidence_of_anything() {
    // The id was reused, or Restate reset the counter. Evidence that
    // contradicts itself is not evidence.
    assert_eq!(
        wedged(&seen(BACKING_OFF, 9), &seen(BACKING_OFF, 2), 1),
        None
    );
}

#[test]
fn two_different_invocations_are_not_two_looks_at_one() {
    // Nothing to compare, and alarming would name a run that is fine.
    let mut other = seen(BACKING_OFF, 99);
    other.id = "inv_somethingelse".to_owned();
    assert_eq!(wedged(&seen(BACKING_OFF, 1), &other, 1), None);
}

#[test]
fn the_threshold_is_the_callers_because_how_many_is_too_many_is_deployment() {
    // A component that restarts on every deploy legitimately retries a few
    // times; one that has been failing for an hour has not. Where the line
    // falls depends on the deployment, so it is a parameter rather than a
    // number invented here.
    let before = seen(BACKING_OFF, 6);
    let after = seen(BACKING_OFF, 7);
    assert_eq!(wedged(&before, &after, 8), None, "below the line");
    assert!(wedged(&before, &after, 7).is_some(), "at the line");
    assert!(wedged(&before, &after, 1).is_some(), "past it");
}

#[test]
fn a_status_this_code_has_never_seen_cannot_raise_an_alarm() {
    // The failure mode of a watchdog is crying wolf, and a Restate release
    // that renames or adds a status must not be able to do it. Anything that
    // is not `backing-off` is not backing off.
    // A *growing* count, so the unrecognised status is the only thing that can
    // produce `None`. Passing the same row twice short-circuits on the count
    // before the status is looked at -- that version passed even with
    // `is_backing_off` inverted, which is the property it exists to pin.
    assert_eq!(
        wedged(
            &seen("quiescing-in-19", 50),
            &seen("quiescing-in-19", 51),
            1
        ),
        None
    );
    let odd = seen("quiescing-in-19", 50);
    assert!(!odd.is_backing_off());
    assert!(seen(BACKING_OFF, 0).is_backing_off());
}
