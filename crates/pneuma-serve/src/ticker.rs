//! Running something on an interval, and noticing when it did not run.

use std::time::Duration;

use chrono::{DateTime, Utc};
use tokio_util::sync::CancellationToken;

/// Runs `body` every `period` until `token` is cancelled.
///
/// The first run is immediate. An interval schedule that waited a full period
/// before its first pass would leave a freshly deployed janitor idle for an
/// hour, and the pass it skipped is the backlog that accumulated while it was
/// being deployed.
///
/// Cancellation is checked *before* each run as well as while waiting, so a
/// token cancelled during a long body stops the loop rather than starting one
/// more pass on the way out.
///
/// Missed ticks are dropped rather than queued. `tokio::time::interval`'s
/// default `MissedTickBehavior::Burst` would, after a pass that overran by an
/// hour, immediately run every tick it owed — a thundering herd against the
/// database that is already slow, which is why the pass overran.
pub async fn every<F, Fut>(period: Duration, token: CancellationToken, mut body: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    let mut ticks = tokio::time::interval(period);
    // `Skip`, not `Delay`. Both drop the ticks that were missed rather than
    // running them all at once -- which is what rules out the default,
    // `Burst`, whose catch-up would throw an hour of owed passes at a database
    // that is already slow, that being why the pass overran. The difference
    // between the two is the grid: `Delay` reschedules from *now*, so an
    // hourly janitor whose pass takes twenty minutes runs at 00:00, 01:20,
    // 02:40 and drifts for ever, while `Skip` keeps the passes on the hour.
    // `missed` below judges lateness against a fixed period, so the schedule
    // it is judging had better be fixed too.
    ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    // Two checks, not one, and the second is not redundant.
    //
    // `run_until_cancelled` is biased towards the **future**, not towards
    // cancellation: its `poll` tries the inner future first and only looks at
    // the token if that is pending, and tokio-util's own doc says so in as many
    // words ("It is biased towards the Future completion",
    // `tokio-util-0.7.14/src/sync/cancellation_token.rs`). So when a due tick
    // and a cancellation land in the same poll -- a token cancelled while the
    // loop is parked on a deadline that has already elapsed, which is the
    // ordinary shutdown-during-idle case -- the tick wins and one more pass
    // starts *after* the process was told to stop. An earlier version of this
    // comment claimed the opposite bias and relied on it.
    //
    // Asking the token again, as the second half of the loop's own condition,
    // makes the guarantee hold whichever way the poll went: keep going while a
    // tick arrived *and* nothing has asked us to stop. It is worth the clause:
    // a pass that starts is a pass that may archive without deleting.
    //
    // Both checks are before the body and neither is inside it: a pass already
    // running is allowed to finish, for the same reason.
    while token.run_until_cancelled(ticks.tick()).await.is_some() && !token.is_cancelled() {
        body().await;
    }
}

/// Whether a tick should have happened by now and did not.
///
/// Pure, so the alerting rule is decided without a clock, a runtime or a
/// scheduler. `grace` is what stops a pass that is merely slow from raising an
/// alarm: a run that takes eleven minutes on an hourly schedule has not missed
/// anything.
///
/// `last` is when the last pass *finished*. Measuring from the start would call
/// a schedule healthy while every pass overran, because the starts would still
/// be one period apart.
pub fn missed(last: DateTime<Utc>, now: DateTime<Utc>, period: Duration, grace: Duration) -> bool {
    let Ok(period) = chrono::Duration::from_std(period) else {
        // A period too large to represent cannot have elapsed.
        return false;
    };
    let Ok(grace) = chrono::Duration::from_std(grace) else {
        return false;
    };
    let Some(due) = last
        .checked_add_signed(period)
        .and_then(|due| due.checked_add_signed(grace))
    else {
        // Past the end of representable time, which nothing can be later than.
        return false;
    };
    now > due
}
