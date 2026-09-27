//! The durable submission queue a dispatcher selects from.
//!
//! Nothing in the original has this table. The original accepts a run and
//! dispatches it in the same breath, so a submission arriving while the
//! controller is down is a submission that never happened. This is the durable
//! half of what `pneuma-fairness` needs to be usable at all: a flow's backlog
//! has to survive a restart, or "fair over time" means "fair until the next
//! deploy".
//!
//! # Three decisions worth stating
//!
//! **A redelivery is not an error.** `run_id` is the primary key and [`SubmissionStore::enqueue`]
//! is `ON CONFLICT DO NOTHING`, so submitting the same run twice — the ordinary
//! case for any at-least-once transport — is [`Accepted::AlreadyQueued`] rather
//! than a failure. A queue that rejects redeliveries makes its caller write the
//! deduplication instead, and the caller has less to deduplicate with.
//!
//! **`claim` is not `SKIP LOCKED`.** The usual work-queue shape hands out
//! whatever rows a locking query reaches first, which is the opposite of a fair
//! selection: `select_batch` has to see a flow's whole bounded backlog to
//! compute its quota. So the choice is made outside the database and [`SubmissionStore::claim`]
//! is the atomic arbiter for it — two dispatchers that chose the same run race
//! on `state = 'queued'` and exactly one wins. A short returned list is that
//! working, not a fault.
//!
//! **The round is a sequence.** [`SubmissionStore::next_round`] reads `nextval`, because
//! `select_batch` rotates its tie-break by the round number and a batch that
//! does not divide evenly hands the remainder to somebody. With a fixed round
//! that is the same flow every time — measured in `pneuma-fairness` at 800
//! items against 600 over 200 rounds while every individual batch was provably
//! fair. A per-process counter resets on restart *and* is per replica, so two
//! dispatchers would both sit near zero and favour the same tenant.

use chrono::{DateTime, Utc};
use serde_json::Value;
use sqlx::{PgPool, Row};

use crate::queries;
use crate::store::StoreError;

/// What happened to a submission offered to the queue.
///
/// Four answers rather than two, because `done` and `failed` rows are kept: a
/// caller told only "something was already here" cannot tell a run waiting to
/// be dispatched from one that finished last month, and a retry of a failed
/// submission would be dropped as a duplicate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Accepted {
    /// It was not here, and now it is.
    Queued,
    /// It is already waiting. Not an error — see the module header.
    AlreadyQueued,
    /// A dispatcher already has it, and has not said how it went.
    AlreadyClaimed,
    /// It has already run. Nothing will run it again under this `run_id`.
    AlreadyRan {
        /// Whether that run succeeded.
        succeeded: bool,
    },
}

/// How a claimed submission ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    /// The run was submitted onwards successfully.
    Done,
    /// It was not, and will not be retried by this claim.
    Failed,
}

impl Outcome {
    /// The `submission_state` this settles to.
    ///
    /// Spelled here rather than derived from the variant name: these are values
    /// of a Postgres enum declared in `0003_submission.sql`, and a rename on
    /// this side that silently stopped matching would fail at run time on a
    /// query that had been working.
    fn as_state(self) -> &'static str {
        match self {
            Outcome::Done => "done",
            Outcome::Failed => "failed",
        }
    }
}

/// One queued submission, as the dispatcher reads it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Queued {
    /// The run this is for, and the queue's primary key.
    pub run_id: String,
    /// Which tenant it belongs to — the flow, for fair selection.
    pub tenant_id: String,
    /// What was submitted, forwarded verbatim when it is dispatched.
    pub payload: Value,
    /// When it arrived. The order within a tenant.
    pub enqueued_at: DateTime<Utc>,
}

/// The submission queue.
#[derive(Debug, Clone)]
pub struct SubmissionStore {
    pool: PgPool,
}

impl SubmissionStore {
    /// Wraps a pool.
    pub fn new(pool: PgPool) -> Self {
        SubmissionStore { pool }
    }

    /// Offers a submission to the queue.
    ///
    /// Idempotent on `run_id`: see [`Accepted::AlreadyQueued`].
    pub async fn enqueue(
        &self,
        run_id: &str,
        tenant_id: &str,
        payload: &Value,
    ) -> Result<Accepted, StoreError> {
        let row = sqlx::query(queries::SUBMISSION_ENQUEUE)
            .bind(run_id)
            .bind(tenant_id)
            .bind(payload)
            .fetch_one(&self.pool)
            .await?;
        let inserted: bool = row.try_get("inserted")?;
        if inserted {
            return Ok(Accepted::Queued);
        }
        let state: String = row.try_get("state")?;
        Ok(accepted_from(&state))
    }

    /// Every queued submission, at most `per_tenant` of each.
    ///
    /// Bounded per tenant rather than in total, and that is the point: a global
    /// limit would let a noisy tenant's rows crowd a quiet one's out of the
    /// input entirely, and the selection would then be provably fair over a
    /// sample that was not.
    pub async fn queued_backlogs(&self, per_tenant: i64) -> Result<Vec<Queued>, StoreError> {
        let rows = sqlx::query(queries::SUBMISSION_QUEUED_BACKLOGS)
            .bind(per_tenant)
            .fetch_all(&self.pool)
            .await?;
        rows.into_iter()
            .map(|row| {
                Ok(Queued {
                    run_id: row.try_get("run_id")?,
                    tenant_id: row.try_get("tenant_id")?,
                    payload: row.try_get("payload")?,
                    enqueued_at: row.try_get("enqueued_at")?,
                })
            })
            .collect()
    }

    /// Takes the chosen submissions, returning the ones actually taken.
    ///
    /// A shorter list than was asked for means another dispatcher got there
    /// first, which is this working rather than an error.
    pub async fn claim(&self, run_ids: &[String]) -> Result<Vec<String>, StoreError> {
        // An empty claim is answered without a query. `= ANY('{}')` is valid
        // SQL that matches nothing, so this is an optimisation rather than a
        // correctness fix -- but a dispatcher whose fair selection came back
        // empty is the common idle case, and it should not cost a round trip.
        if run_ids.is_empty() {
            return Ok(Vec::new());
        }
        let rows = sqlx::query(queries::SUBMISSION_CLAIM)
            .bind(run_ids)
            .fetch_all(&self.pool)
            .await?;
        rows.into_iter()
            .map(|row| row.try_get("run_id").map_err(StoreError::from))
            .collect()
    }

    /// Records how a claimed submission ended.
    ///
    /// `false` when there was no claimed row to settle — already settled, never
    /// claimed, or claimed by someone else. A caller that treats that as an
    /// error would fail on its own duplicate delivery.
    pub async fn settle(
        &self,
        run_id: &str,
        outcome: Outcome,
        detail: Option<&str>,
    ) -> Result<bool, StoreError> {
        let settled = sqlx::query(queries::SUBMISSION_SETTLE)
            .bind(run_id)
            .bind(outcome.as_state())
            .bind(detail)
            .fetch_optional(&self.pool)
            .await?;
        Ok(settled.is_some())
    }

    /// Returns claims nobody settled to the queue.
    ///
    /// Every submission still `claimed` from before `older_than`, moved back to
    /// `queued` and handed to the caller so it can say what it recovered.
    ///
    /// Without this a dispatcher that dies between [`SubmissionStore::claim`]
    /// and [`SubmissionStore::settle`] strands its rows for ever: nothing else
    /// looks at `claimed`, no query finds them, and re-submitting the same
    /// `run_id` collides with the primary key. Silently losing work is the one
    /// thing a durable queue must not do.
    ///
    /// The cutoff is the caller's because "too long" depends on how long a
    /// dispatch legitimately takes, and reclaiming a submission that is merely
    /// slow would run it twice.
    pub async fn reclaim(&self, older_than: DateTime<Utc>) -> Result<Vec<String>, StoreError> {
        let rows = sqlx::query(queries::SUBMISSION_RECLAIM)
            .bind(older_than)
            .fetch_all(&self.pool)
            .await?;
        rows.into_iter()
            .map(|row| row.try_get("run_id").map_err(StoreError::from))
            .collect()
    }

    /// The next dispatch round, shared across every replica.
    pub async fn next_round(&self) -> Result<i64, StoreError> {
        let row = sqlx::query(queries::SUBMISSION_NEXT_ROUND)
            .fetch_one(&self.pool)
            .await?;
        Ok(row.try_get(0)?)
    }
}

/// What an existing row's state means to a caller offering it again.
///
/// Pure, and separate from the query so every arm has a test: reaching
/// `AlreadyRan` through the database means settling a submission first, and the
/// unknown arm cannot be reached through it at all.
fn accepted_from(state: &str) -> Accepted {
    match state {
        "claimed" => Accepted::AlreadyClaimed,
        "done" => Accepted::AlreadyRan { succeeded: true },
        "failed" => Accepted::AlreadyRan { succeeded: false },
        // `queued`, and anything a future migration adds. Treating an unknown
        // state as "waiting" is the answer that cannot lose work: the caller
        // does nothing, and the row stays for whatever does understand it.
        _ => Accepted::AlreadyQueued,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_outcome_spells_the_enum_value_postgres_declared() {
        // These are values of `submission_state` in `0003_submission.sql`, not
        // a rendering of the Rust variant name. A rename on this side that
        // stopped matching would fail at run time, on a query that had been
        // working, at whatever hour the rename shipped.
        assert_eq!(Outcome::Done.as_state(), "done");
        assert_eq!(Outcome::Failed.as_state(), "failed");
    }

    #[test]
    fn every_state_a_row_can_be_in_means_something_to_a_caller() {
        // The values are `submission_state`'s, from `0003_submission.sql`.
        assert_eq!(accepted_from("queued"), Accepted::AlreadyQueued);
        assert_eq!(accepted_from("claimed"), Accepted::AlreadyClaimed);
        assert_eq!(
            accepted_from("done"),
            Accepted::AlreadyRan { succeeded: true }
        );
        assert_eq!(
            accepted_from("failed"),
            Accepted::AlreadyRan { succeeded: false }
        );
        // A state this binary does not know -- a newer migration against an
        // older binary. "Waiting" is the answer that cannot lose work: the
        // caller does nothing and the row stays.
        assert_eq!(accepted_from("quarantined"), Accepted::AlreadyQueued);
    }
}
