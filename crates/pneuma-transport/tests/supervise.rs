//! The reconnect rule, without a broker to disconnect from.
//!
//! Every branch here is reached in production by something misbehaving, which
//! is why the decision is a pure function: a test cannot make a broker cancel
//! a `Channel.Open`, but it can ask what should happen when one does.

use std::time::Duration;

use pneuma_transport::{decide, delay, Action, Backoff, BackoffError, Event};

/// One second growing to eight, and a connection counts as having worked once
/// it has lasted ten.
fn fast() -> Backoff {
    let Ok(backoff) = Backoff::new(
        Duration::from_secs(1),
        Duration::from_secs(8),
        Duration::from_secs(10),
    ) else {
        panic!("one, eight and ten are a backoff");
    };
    backoff
}

/// A connection that lasted long enough to have worked.
fn worked(why: &str) -> Event {
    Event::Dropped {
        after: Duration::from_secs(3600),
        why: why.to_owned(),
    }
}

/// A connection that came up and went straight back down.
fn flapped(why: &str) -> Event {
    Event::Dropped {
        after: Duration::from_millis(20),
        why: why.to_owned(),
    }
}

#[test]
fn the_wait_doubles_to_a_ceiling_and_stays_there() {
    assert_eq!(delay(fast(), 1), Duration::from_secs(1), "the first retry");
    assert_eq!(delay(fast(), 2), Duration::from_secs(2));
    assert_eq!(delay(fast(), 3), Duration::from_secs(4));
    assert_eq!(delay(fast(), 4), Duration::from_secs(8), "the ceiling");
    assert_eq!(
        delay(fast(), 5),
        Duration::from_secs(8),
        "and it stays there"
    );

    // A ceiling rather than a limit on attempts. A consumer that gave up would
    // need something above it to notice, and there is nothing above it.
    assert_eq!(delay(fast(), u32::MAX), Duration::from_secs(8));
}

#[test]
fn nothing_has_failed_yet_at_attempt_zero() {
    // Total rather than a precondition the caller has to honour: `decide`
    // computes the delay from the *incremented* counter, so zero only reaches
    // here if somebody asks directly.
    assert_eq!(delay(fast(), 0), Duration::ZERO);
}

#[test]
fn the_defaults_are_the_originals_delay_as_a_floor() {
    // `RABBITMQ_RECEIVER_RECONNECT_DELAY` is 5 s,
    // so a broker that blips is
    // recovered from exactly as fast as it is today -- the change is only what
    // happens when it does not come back.
    assert_eq!(delay(Backoff::DEFAULT, 1), Duration::from_secs(5));
    assert_eq!(delay(Backoff::DEFAULT, 99), Duration::from_secs(60));
}

#[test]
fn a_connection_that_lasted_resets_the_backoff() {
    // The bug this is here for: a consumer up for a week, whose broker
    // restarts, waiting the *maximum* delay before its first retry -- because
    // the counter still holds the failures from its first startup. Reviewing
    // for it is unreliable; asserting it is not.
    let step = decide(&worked("broker restarted"), 17, fast());
    assert_eq!(
        step.action,
        Action::Reconnect {
            after: Duration::from_secs(1),
            why: "broker restarted".to_owned(),
        },
        "one second, not eight: a week of uptime is not seventeen failures"
    );
    assert_eq!(step.attempt, 1, "a fresh episode, on its first failure");
}

#[test]
fn a_flapping_broker_still_backs_off() {
    // The cure for the bug above, applied carelessly, is worse than the bug:
    // zeroing the counter the moment the consumer attaches reinstates the fixed
    // delay. A broker that accepts the connection and then closes the channel
    // -- the exact failure the original's loop was written for -- gives
    // `Up, Dropped, Up, Dropped, ...`, and a counter zeroed at every `Up` never
    // exceeds one.
    let mut attempt = 0;
    let mut waits = Vec::new();
    for _ in 0..4 {
        let up = decide(&Event::Up, attempt, fast());
        assert_eq!(up.action, Action::Consume);
        attempt = up.attempt;

        let down = decide(
            &flapped("channel closed straight after open"),
            attempt,
            fast(),
        );
        let Action::Reconnect { after, .. } = down.action else {
            panic!("a drop reconnects");
        };
        waits.push(after);
        attempt = down.attempt;
    }
    assert_eq!(
        waits,
        vec![
            Duration::from_secs(1),
            Duration::from_secs(2),
            Duration::from_secs(4),
            Duration::from_secs(8),
        ],
        "a flap is the same episode continuing, not four first failures"
    );
}

#[test]
fn both_kinds_of_failure_back_off_and_carry_their_reason() {
    // They are separate variants because they are separate failures -- a
    // socket that will not open, and a consumer that vanished while the socket
    // stayed up, which is the one the original's loop was written for. They
    // get the same treatment, and that is a decision rather than an accident.
    for event in [
        Event::ConnectFailed("connection refused".to_owned()),
        flapped("channel closed: RPC call cancelled"),
    ] {
        let step = decide(&event, 2, fast());
        assert_eq!(step.attempt, 3);
        let Action::Reconnect { after, why } = step.action else {
            panic!("a failure reconnects: {event:?}");
        };
        assert_eq!(after, Duration::from_secs(4));
        assert!(!why.is_empty(), "a loop with no reason in it says nothing");
    }
}

#[test]
fn a_counter_that_cannot_wrap() {
    // Wrapping to zero would silently reset the backoff of a broker that has
    // never once answered, turning the longest wait into the shortest.
    let step = decide(
        &Event::ConnectFailed("still down".to_owned()),
        u32::MAX,
        fast(),
    );
    assert_eq!(step.attempt, u32::MAX);
}

#[test]
fn attaching_carries_the_counter_rather_than_clearing_it() {
    let step = decide(&Event::Up, 3, fast());
    assert_eq!(step.action, Action::Consume);
    assert_eq!(step.attempt, 3, "attaching is not surviving");
}

#[test]
fn the_two_backoffs_that_fail_quietly_are_refused() {
    // A zero floor makes every delay zero however many failures there have
    // been, so configuring a backoff produces the unbounded reconnect loop a
    // backoff exists to prevent.
    let Err(BackoffError::NoFirstWait) = Backoff::new(
        Duration::ZERO,
        Duration::from_secs(60),
        Duration::from_secs(60),
    ) else {
        panic!("zero is not a first wait");
    };

    // And a ceiling below the floor is not clamped silently: it collapses every
    // delay to the ceiling, so a deployment that meant to *raise* the ceiling
    // and typed it into the wrong variable would get a shorter wait than the
    // default with nothing to say so.
    let Err(BackoffError::CeilingBelowFloor { first, max }) = Backoff::new(
        Duration::from_secs(30),
        Duration::from_secs(5),
        Duration::from_secs(60),
    ) else {
        panic!("a ceiling below the floor is not a ceiling");
    };
    assert_eq!(first, Duration::from_secs(30));
    assert_eq!(max, Duration::from_secs(5));

    // Equal is fine -- that is a fixed delay, which is what the original has.
    assert!(Backoff::new(
        Duration::from_secs(5),
        Duration::from_secs(5),
        Duration::from_secs(60),
    )
    .is_ok());
}

#[test]
fn the_defaults_are_readable() {
    assert_eq!(Backoff::DEFAULT.first(), Duration::from_secs(5));
    assert_eq!(Backoff::DEFAULT.max(), Duration::from_secs(60));
    assert_eq!(Backoff::DEFAULT.stable(), Duration::from_secs(60));
}

#[test]
fn a_shutdown_stops_without_touching_the_counter() {
    let step = decide(&Event::Shutdown, 4, fast());
    assert_eq!(step.action, Action::Stop);
    assert_eq!(step.attempt, 4, "nothing failed; there is nothing to count");
}
