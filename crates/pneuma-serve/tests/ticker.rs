//! The interval loop, on a clock the test controls.
//!
//! `tokio::time::pause()` makes the runtime advance time on demand rather than
//! by waiting, so an hourly schedule is a microsecond test. Without it these
//! would either take an hour or be written against a period so short that they
//! prove nothing about the one production uses.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use chrono::{Duration as Chrono, TimeZone, Utc};
use pneuma_serve::ticker::{every, missed};
use tokio_util::sync::CancellationToken;

/// An hour, which is `INTERVAL_MINUTES`' own default in the original.
const HOUR: Duration = Duration::from_secs(3600);

#[tokio::test(start_paused = true)]
async fn the_first_pass_is_immediate_and_the_rest_are_on_the_period() {
    // Not "wait an hour, then start". A freshly deployed janitor that idles for
    // a period before its first pass leaves exactly the backlog that built up
    // while it was being deployed.
    let runs = Arc::new(AtomicUsize::new(0));
    let token = CancellationToken::new();
    let loop_runs = Arc::clone(&runs);
    let loop_token = token.clone();
    let ticking = tokio::spawn(async move {
        every(HOUR, loop_token, move || {
            let runs = Arc::clone(&loop_runs);
            async move {
                runs.fetch_add(1, Ordering::SeqCst);
            }
        })
        .await;
    });

    // Let the immediate first tick happen.
    tokio::task::yield_now().await;
    tokio::time::sleep(Duration::from_millis(1)).await;
    assert_eq!(
        runs.load(Ordering::SeqCst),
        1,
        "the first pass is immediate"
    );

    tokio::time::advance(HOUR).await;
    tokio::task::yield_now().await;
    assert_eq!(runs.load(Ordering::SeqCst), 2, "and then hourly");

    tokio::time::advance(HOUR).await;
    tokio::task::yield_now().await;
    assert_eq!(runs.load(Ordering::SeqCst), 3);

    token.cancel();
    let Ok(()) = ticking.await else {
        panic!("the loop stops when cancelled");
    };
    let after = runs.load(Ordering::SeqCst);
    tokio::time::advance(HOUR * 5).await;
    tokio::task::yield_now().await;
    assert_eq!(
        runs.load(Ordering::SeqCst),
        after,
        "and stays stopped: five more periods run nothing"
    );
}

#[tokio::test(start_paused = true)]
async fn a_token_cancelled_before_the_first_tick_runs_nothing_at_all() {
    // The check is before the run, not only in the wait. A janitor asked to
    // stop during startup must not begin a pass on its way out -- a pass it
    // starts is a pass that may archive without deleting.
    let runs = Arc::new(AtomicUsize::new(0));
    let token = CancellationToken::new();
    token.cancel();
    let loop_runs = Arc::clone(&runs);
    every(HOUR, token, move || {
        let runs = Arc::clone(&loop_runs);
        async move {
            runs.fetch_add(1, Ordering::SeqCst);
        }
    })
    .await;
    assert_eq!(runs.load(Ordering::SeqCst), 0);
}

#[tokio::test(start_paused = true)]
async fn a_tick_due_at_the_moment_of_cancellation_does_not_start_a_pass() {
    // `run_until_cancelled` is biased towards the *future*, not the token: its
    // `poll` tries the inner future first and only then looks at cancellation.
    // So a tick whose deadline has already elapsed beats a cancellation that
    // arrives in the same poll, and without a second check the loop starts one
    // more pass after the process was told to stop.
    //
    // Reproduced deterministically: advance past the deadline so the timer is
    // ready, cancel before anything is polled, and only then let the runtime
    // run. Both are ready in that first poll, which is exactly the race.
    let runs = Arc::new(AtomicUsize::new(0));
    let token = CancellationToken::new();
    let loop_runs = Arc::clone(&runs);
    let loop_token = token.clone();
    let ticking = tokio::spawn(async move {
        every(HOUR, loop_token, move || {
            let runs = Arc::clone(&loop_runs);
            async move {
                runs.fetch_add(1, Ordering::SeqCst);
            }
        })
        .await;
    });

    // The immediate first pass, so the loop is parked on the second tick.
    tokio::time::sleep(Duration::from_millis(1)).await;
    assert_eq!(runs.load(Ordering::SeqCst), 1);

    // The deadline elapses and the token is cancelled with no poll in between.
    tokio::time::advance(HOUR).await;
    token.cancel();
    let Ok(()) = ticking.await else {
        panic!("the loop stops");
    };

    assert_eq!(
        runs.load(Ordering::SeqCst),
        1,
        "the due tick must not start a pass after cancellation -- a pass that \
         starts is a pass that may archive without deleting"
    );
}

#[test]
fn a_slow_pass_is_not_a_missed_one() {
    // The grace is the whole point of the function: a pass that takes eleven
    // minutes on an hourly schedule has not missed anything, and paging someone
    // for it is how an alert gets muted.
    let Some(last) = Utc.with_ymd_and_hms(2026, 1, 1, 12, 0, 0).single() else {
        panic!("a real instant");
    };
    let grace = Duration::from_secs(15 * 60);

    for (label, elapsed, expected) in [
        ("on time", Chrono::minutes(59), false),
        ("exactly due", Chrono::minutes(60), false),
        ("slow, inside the grace", Chrono::minutes(70), false),
        ("exactly at the grace", Chrono::minutes(75), false),
        ("past the grace", Chrono::minutes(76), true),
        ("hours late", Chrono::hours(9), true),
    ] {
        let Some(now) = last.checked_add_signed(elapsed) else {
            panic!("representable");
        };
        assert_eq!(missed(last, now, HOUR, grace), expected, "{label}");
    }
}

#[test]
fn a_clock_that_went_backwards_is_not_a_missed_tick() {
    // NTP steps a clock backwards, and reporting a missed pass because of it
    // sends someone to look at a janitor that is running perfectly.
    let Some(last) = Utc.with_ymd_and_hms(2026, 1, 1, 12, 0, 0).single() else {
        panic!("a real instant");
    };
    let Some(earlier) = last.checked_sub_signed(Chrono::hours(3)) else {
        panic!("representable");
    };
    assert!(!missed(last, earlier, HOUR, Duration::from_secs(60)));
}

#[test]
fn an_unrepresentable_period_or_grace_is_not_a_missed_tick() {
    // Settings can be built directly, so these are reachable without going
    // through a range check. Answering "missed" would have a janitor report
    // itself broken because of a number nobody could have meant.
    let Some(last) = Utc.with_ymd_and_hms(2026, 1, 1, 12, 0, 0).single() else {
        panic!("a real instant");
    };
    let now = Utc::now();
    assert!(!missed(last, now, Duration::MAX, Duration::from_secs(60)));
    assert!(!missed(last, now, HOUR, Duration::MAX));
    // And a due time past the end of representable time.
    let Some(far) = Utc.with_ymd_and_hms(262_142, 1, 1, 0, 0, 0).single() else {
        panic!("near the end of the representable range");
    };
    assert!(!missed(
        far,
        now,
        Duration::from_secs(86_400 * 365 * 200),
        Duration::ZERO
    ));
}
