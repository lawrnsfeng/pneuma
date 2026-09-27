//! One dispatch round, end to end.
//!
//! `next_round` → `queued_backlogs` → [`group`] → [`backlogs`] →
//! `select_batch` → [`alarm`] → `claim` → submit → `settle`. The two ends
//! touch the database and Restate; everything between them is pure and already
//! tested without either.
//!
//! # What a claim that is not settled means
//!
//! A submission Restate accepted is settled `done` — the *submission's* job is
//! finished, whatever the run goes on to do. One Restate refused permanently is
//! settled `failed`. But a submission whose outcome is **unknown** — a
//! transport failure, a 5xx, anything [`Disposition::Retry`] — is deliberately
//! left `claimed` and **not** settled, because settling it either way would be
//! a guess: `done` loses a run that was never submitted, `failed` gives up on
//! one that may already be running.
//!
//! Leaving it claimed is what `SubmissionStore::reclaim` exists for. The row
//! comes back to the queue once it is older than the caller's cutoff, and the
//! idempotency key makes resubmitting it free if it did land after all.

use std::collections::BTreeMap;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use pneuma_fairness::{select_batch, Weight};
use pneuma_store::{Outcome, Queued};

use crate::dispatch::{alarm, backlogs, group, round_base};
use crate::restate::{submit, Disposition, Invoker};

/// The queue, as a dispatcher needs it.
///
/// Separate from [`crate::ingress::Submissions`], which is the ingress's half.
/// Each side depends on what it uses, so a fake for one is not obliged to
/// implement the other's methods — and the ingress cannot reach `claim`.
#[async_trait]
pub trait Dispatchable: Send + Sync {
    /// The next round number, shared across replicas.
    async fn next_round(&self) -> Result<i64, String>;

    /// Every queued submission, at most `per_tenant` of each.
    async fn queued_backlogs(&self, per_tenant: i64) -> Result<Vec<Queued>, String>;

    /// Takes the chosen submissions, returning the ones actually taken.
    async fn claim(&self, run_ids: &[String]) -> Result<Vec<String>, String>;

    /// Records how a claimed submission ended.
    async fn settle(
        &self,
        run_id: &str,
        outcome: Outcome,
        detail: Option<&str>,
    ) -> Result<bool, String>;

    /// Returns claims older than `older_than` to the queue.
    ///
    /// On the trait because it is the other half of what [`round`] deliberately
    /// leaves undone: a submission whose outcome is unknown stays `claimed`,
    /// and without a sweep it stays that way for ever. A dispatcher that could
    /// claim but not reclaim would be a queue with a slow leak, so the two are
    /// one interface rather than two.
    async fn reclaim(&self, older_than: DateTime<Utc>) -> Result<Vec<String>, String>;
}

/// What one round is allowed to do.
#[derive(Debug, Clone)]
pub struct Settings {
    /// How many submissions one round may dispatch.
    pub batch_size: usize,
    /// How many of each tenant's rows to consider.
    ///
    /// Bounded per tenant rather than in total, because a global limit lets a
    /// noisy tenant crowd a quiet one out of the *input* — and the selection
    /// would then be provably fair over a sample that was not.
    pub per_tenant: i64,
    /// Each tenant's share. An absent tenant gets [`Weight::ONE`].
    pub weights: BTreeMap<String, Weight>,
}

/// What one round did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoundReport {
    /// The round number this ran as.
    pub round: i64,
    /// How many queued submissions were considered.
    pub considered: usize,
    /// How many the fair selection chose.
    pub selected: usize,
    /// How many were actually taken — fewer means another dispatcher won.
    pub claimed: usize,
    /// How many reached Restate and were settled.
    pub submitted: usize,
    /// How many were left claimed because their outcome is unknown.
    ///
    /// Not a failure count. These are the ones a later `reclaim` returns to the
    /// queue, and a number that keeps growing is the signal that Restate is
    /// unreachable rather than that runs are failing.
    pub unknown: usize,
    /// Flows that appeared more than once, which must never happen.
    pub duplicate_flows: Option<Vec<String>>,
}

/// Runs one round.
///
/// Idle is not a special case: an empty queue produces a report with zeroes
/// rather than an early return with nothing in it, so a caller logging rounds
/// sees the same shape whether or not there was work.
pub async fn round<Q, I>(queue: &Q, invoker: &I, settings: &Settings) -> Result<RoundReport, String>
where
    Q: Dispatchable + ?Sized,
    I: Invoker + ?Sized,
{
    // Taken first, and taken even when the queue turns out to be empty. The
    // round is what `select_batch` rotates its tie-break by, so a dispatcher
    // that skipped the increment on idle rounds would advance it only when
    // there was work -- and two dispatchers with different idle patterns would
    // then disagree about whose turn it is.
    let round = queue.next_round().await?;
    let queued = queue.queued_backlogs(settings.per_tenant).await?;

    let groups = group(&queued, &settings.weights).map_err(|error| error.to_string())?;
    let borrowed = backlogs(&queued, &groups);
    // `round` is a Postgres `bigint` and `select_batch` takes a `u64`. Negative
    // is not reachable from a sequence that starts at 1, and the cast is
    // saturating rather than wrapping so that if it ever were, the tie-break
    // would be stuck rather than jumping somewhere arbitrary.
    let rotation = u64::try_from(round).unwrap_or(0);
    // The base is bound rather than passed inline: an argument on its own line
    // of a multi-line call is not attributed to the call that ran.
    let base = round_base(settings.batch_size);
    let batch = select_batch(&borrowed, base, settings.batch_size, rotation);
    let duplicate_flows = alarm(&batch);

    let chosen: Vec<String> = batch.items.iter().map(|row| row.run_id.clone()).collect();
    let claimed = queue.claim(&chosen).await?;

    // Indexed by run id, so a claim that came back in a different order still
    // finds its payload. The queue is free to return them however it likes.
    let mut payloads: BTreeMap<&str, &Queued> = BTreeMap::new();
    for row in &queued {
        payloads.insert(row.run_id.as_str(), row);
    }

    let mut submitted = 0;
    let mut unknown = 0;
    for run_id in &claimed {
        // A `match` on the lookup rather than a `let ... else { continue }`:
        // the missing arm's whole body is then the arm, and a bare `continue`
        // is not attributed to the branch that took it.
        match payloads.get(run_id.as_str()) {
            // Claimed something that was not in the backlog this round read.
            // A queue that does that is misbehaving, and guessing at a payload
            // for it would be worse than leaving it alone -- so it is counted
            // as unknown and `reclaim` returns it to the queue later, exactly
            // like a submission whose outcome never came back.
            None => unknown += 1,
            Some(row) => match submit(invoker, run_id, &row.payload).await {
                Disposition::Accepted | Disposition::AlreadyKnown | Disposition::Completed => {
                    queue.settle(run_id, Outcome::Done, None).await?;
                    submitted += 1;
                }
                Disposition::Rejected(why) | Disposition::Failed(why) => {
                    queue.settle(run_id, Outcome::Failed, Some(&why)).await?;
                    submitted += 1;
                }
                // Left claimed on purpose. Settling either way would be a
                // guess: `done` loses a run that was never submitted,
                // `failed` gives up on one that may already be running.
                Disposition::Retry(_) => unknown += 1,
            },
        }
    }

    Ok(RoundReport {
        round,
        considered: queued.len(),
        selected: batch.items.len(),
        claimed: claimed.len(),
        submitted,
        unknown,
        duplicate_flows,
    })
}

/// What one tick of the dispatch loop did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Tick {
    /// Claims the sweep returned to the queue before the round ran.
    pub reclaimed: Vec<String>,
    /// What the round itself did.
    pub round: RoundReport,
}

/// Sweeps abandoned claims, then dispatches a round.
///
/// The sweep runs **first**, so a submission reclaimed on this tick is
/// eligible for the round that immediately follows it rather than waiting a
/// whole interval more. That ordering is what keeps the recovery from a
/// Restate outage to one interval instead of two.
///
/// `older_than` is an instant rather than an age because that is what makes
/// this testable: an age would have to be subtracted from `Utc::now()` in here,
/// and a test would then be asserting against the clock.
pub async fn tick<Q, I>(
    queue: &Q,
    invoker: &I,
    settings: &Settings,
    older_than: DateTime<Utc>,
) -> Result<Tick, String>
where
    Q: Dispatchable + ?Sized,
    I: Invoker + ?Sized,
{
    let reclaimed = queue.reclaim(older_than).await?;
    let round = round(queue, invoker, settings).await?;
    Ok(Tick { reclaimed, round })
}

/// One line of log for a tick, whichever way it went.
///
/// A pure function rather than a `match` inside the loop, for one reason that
/// is about this repository rather than about style: the loop's failing arm
/// needs a database that is down in order to run, and the gate holds this crate
/// to every line. Rendering both arms here makes the failure a unit test, and
/// leaves the loop body with no branch at all to leave uncovered.
///
/// The duplicate-flow alarm is spelled out rather than folded into the counts.
/// It must never fire -- `backlogs` groups by tenant, so two backlogs for one
/// flow would mean the grouping itself is broken -- which is exactly why it
/// needs to be legible in a log rather than a number that happens to be zero.
pub fn report_line(outcome: &Result<Tick, String>) -> String {
    let tick = match outcome {
        Err(error) => return format!("dispatch round failed: {error}"),
        Ok(tick) => tick,
    };
    let report = &tick.round;
    let alarm = match &report.duplicate_flows {
        None => String::new(),
        Some(flows) => format!(" ALARM duplicate flows: {}", flows.join(", ")),
    };
    format!(
        "round {} reclaimed={} considered={} selected={} claimed={} submitted={} unknown={}{}",
        report.round,
        tick.reclaimed.len(),
        report.considered,
        report.selected,
        report.claimed,
        report.submitted,
        report.unknown,
        alarm,
    )
}
