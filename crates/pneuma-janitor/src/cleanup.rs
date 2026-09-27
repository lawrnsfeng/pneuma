//! The janitor's passes, and the order they must happen in.
//!
//! This crate owns *sequencing* and nothing else. The queries are
//! `pneuma-store`'s and the HTTP is the binary's; what is decided here is which
//! operation follows which, and what a failure part-way through means.
//!
//! # The order is the safety property
//!
//! [`Janitor::cleanup_runs`] copies both records before deleting either:
//!
//! 1. select finalised runs — Mongo
//! 2. copy the run documents into history — Mongo
//! 3. copy their node_runs into history — Postgres
//! 4. delete the node_runs — Postgres
//! 5. delete the run documents — Mongo
//!
//! the same shape as the original.
//! Nothing is deleted that was not copied first, and any step failing abandons
//! the pass, leaving the runs finalised so the next pass repeats from the top.
//!
//! That retry is only safe because **both** copies are idempotent — `$merge` on
//! `_id`, and `ON CONFLICT (id) DO NOTHING`. The defect notes are
//! exactly this being untrue in the original: its node_run backup is a plain
//! `INSERT`, so a run already copied makes the retry a primary-key violation,
//! which aborts before the delete and re-selects the same runs for ever. One
//! transient failure stops cleanup permanently.
//!
//! # Failures propagate
//!
//! The original logs and returns at each step. Returning the error gives the
//! same abandon-the-pass behaviour while leaving the caller to decide what to
//! log — and, more usefully, makes the failure observable to a test rather than
//! only to a log line.

use chrono::{DateTime, Utc};
use pneuma_store::{NodeRunStore, RunHistoryStore, RunStore};

use crate::Settings;

/// What one cleanup pass moved.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Cleaned {
    /// Runs *selected*, which is not the same as runs archived.
    ///
    /// `archive` is a `$merge`, and `$merge` yields no documents, so there is
    /// no count of what it copied to report — the Postgres half has one only
    /// because `INSERT` returns a row count. Claiming these were archived
    /// would assert an outcome nothing observed. It matters because this is
    /// the number the original plan diffs against the original janitor.
    pub runs: usize,
    /// Noderun rows copied into history. Zero for a run whose nodes were
    /// already copied by an earlier pass that failed later on — the whole
    /// point of §17's fix, and not an error.
    pub noderuns_archived: u64,
    /// Noderun rows deleted.
    pub noderuns_deleted: u64,
    /// Run documents deleted.
    pub runs_deleted: u64,
}

/// History that a retention pass removed — or, from a preview, would remove.
///
/// The same type answers both, so the fields say "matched" rather than
/// "removed": from [`Janitor::preview_expiry`] nothing was removed, and naming
/// them for the action would assert an outcome that half of its callers never
/// perform. The same imprecision was removed from `Cleaned::runs`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Expired {
    /// Run history documents matched.
    pub runs: u64,
    /// Noderun history rows matched.
    pub node_runs: u64,
}

/// What one full cycle did, or would do.
///
/// `expired` is `None` when retention is disabled rather than zero, because
/// "kept everything on purpose" and "expired nothing this time" are different
/// answers and the second is the one worth investigating.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Pass {
    /// The cleanup half.
    pub cleaned: Cleaned,
    /// The expiry half, or `None` when `RETENTION_DAYS` disables it.
    pub expired: Option<Expired>,
    /// Runs that have stopped moving. Empty when stale termination is off.
    ///
    /// Named, not acted on: submitting a termination is HTTP and belongs to the
    /// binary.
    pub stale: Vec<String>,
}

/// Which stale runs a cycle should report, given what that cycle is taking.
///
/// A run finalised in Mongo can still hold a non-finalised, stale row in
/// Postgres, and `delete_by_run_ids` removes those rows whatever their status
/// -- so a scan taken before the delete reports runs this very cycle then
/// archives and deletes. Submitting a termination for a run whose records exist
/// nowhere is the outcome, and the binary cannot tell those from live ones.
///
/// Pure, and shared by [`Janitor::pass`] and [`Janitor::preview_pass`], because
/// the filter was written once and applied once: `pass` had it and
/// `preview_pass` did not, so the `--dry-run` that the original plan runs
/// against production for a week reported stale runs the real pass would not.
/// The comment in `pass` calls that disagreement the thing the diff cannot
/// afford, and it was there in the code beneath it. `pneuma-migrate`'s
/// `preview` and `baseline` had the same bug for the same reason: two callers,
/// one decision, written twice.
fn reportable_stale(mut stale: Vec<String>, taking: &[String]) -> Vec<String> {
    stale.retain(|run_id| !taking.contains(run_id));
    stale
}

/// Why a pass stopped.
///
/// Two stores, so two error types, and which one failed matters when reading a
/// log: a Mongo failure at step 2 means nothing was copied, a Postgres failure
/// at step 4 means both copies exist and only the deletes are outstanding.
#[derive(Debug, thiserror::Error)]
pub enum JanitorError {
    /// The run documents, or their history.
    #[error("run store: {0}")]
    Runs(#[from] pneuma_store::BarrierError),
    /// The node_run rows, or their history.
    #[error("node_run store: {0}")]
    NodeRuns(#[from] pneuma_store::StoreError),
    /// The stale window cannot be subtracted from the current instant.
    ///
    /// `Settings::from_env` refuses a window this large, so reaching this means
    /// a `Settings` was built directly.
    #[error("a stale window of {seconds}s cannot be subtracted from now")]
    UnusableStaleWindow {
        /// The window, in seconds.
        seconds: i64,
    },
    /// The two run-document stores were wired to different collections.
    #[error("selection reads {selects} but archiving acts on {archives}")]
    SplitRunCollection {
        /// Where `finalized_runs` looks.
        selects: String,
        /// Where `archive` and `delete_archived` act.
        archives: String,
    },
}

/// The janitor's passes over both stores.
#[derive(Debug, Clone)]
pub struct Janitor {
    runs: RunStore,
    history: RunHistoryStore,
    node_runs: NodeRunStore,
}

impl Janitor {
    /// Wires the three stores a pass needs.
    ///
    /// Refuses a [`RunStore`] and a [`RunHistoryStore`] reading different run
    /// collections. Selection goes through the first and both the archive and
    /// the delete through the second, so a typo in one collection name — `run`
    /// for `runs` — leaves every pass selecting runs that are then archived
    /// from, and deleted from, somewhere else. Nothing moves, nothing errors,
    /// and `Cleaned` reports the runs it selected for ever.
    ///
    /// `RunHistoryStore::new` is fallible to catch exactly this class of
    /// misconfiguration between *its* two collections; composing stores
    /// reintroduces it one level up, so it is caught again here.
    pub fn new(
        runs: RunStore,
        history: RunHistoryStore,
        node_runs: NodeRunStore,
    ) -> Result<Self, JanitorError> {
        let (selects, archives) = (runs.namespace(), history.source_namespace());
        if selects != archives {
            return Err(JanitorError::SplitRunCollection {
                selects: selects.to_string(),
                archives: archives.to_string(),
            });
        }
        Ok(Janitor {
            runs,
            history,
            node_runs,
        })
    }

    /// Archives one batch of finalised runs and removes them from the live
    /// stores.
    ///
    /// `batch` bounds the work per pass, as `RUN_BATCH_SIZE` does in the
    /// original. A non-positive
    /// batch selects nothing — `RunStore::finalized_runs` documents why that
    /// matters, MongoDB reading `limit: 0` as *no limit*.
    pub async fn cleanup_runs(&self, batch: i64) -> Result<Cleaned, JanitorError> {
        let run_ids = self.runs.finalized_runs(batch).await?;
        if run_ids.is_empty() {
            return Ok(Cleaned::default());
        }

        // Both copies first. Neither delete may run until both have succeeded,
        // and both are idempotent so a failure here costs a repeat rather than
        // a record.
        self.history.archive(&run_ids).await?;
        let noderuns_archived = self.node_runs.backup_to_history(&run_ids).await?;

        let noderuns_deleted = self.node_runs.delete_by_run_ids(&run_ids).await?;
        let runs_deleted = self.history.delete_archived(&run_ids).await?;

        Ok(Cleaned {
            runs: run_ids.len(),
            noderuns_archived,
            noderuns_deleted,
            runs_deleted,
        })
    }

    /// The runs a [`Janitor::cleanup_runs`] pass would take, without taking
    /// them.
    ///
    /// the original plan ships the janitor with `--dry-run`, runs it
    /// against production for a week and diffs it against the original one. This
    /// is the cleanup half of that diff: which runs each implementation would
    /// consider finished. It selects with exactly the query the real pass
    /// selects with, so what it reports is what would happen.
    pub async fn preview_cleanup(&self, batch: i64) -> Result<Vec<String>, JanitorError> {
        Ok(self.runs.finalized_runs(batch).await?)
    }

    /// What an [`Janitor::expire_history`] pass would remove, without removing
    /// it.
    ///
    /// The other half of the phase 7 diff. Both counts come from the same
    /// predicates the deletes use — shared outright on the Mongo side, and held
    /// to it by a test on the Postgres side, where SQL cannot share a `WHERE`
    /// between two statements. A preview built from a drifted copy of the rule
    /// is worse than none: it reports confidently about a decision the real
    /// code does not make, and nothing compares the two.
    pub async fn preview_expiry(&self, cutoff: DateTime<Utc>) -> Result<Expired, JanitorError> {
        let runs = self.history.count_outdated(cutoff).await?;
        let node_runs = self.node_runs.count_outdated_history(cutoff).await?;
        Ok(Expired { runs, node_runs })
    }

    /// Removes history older than `cutoff` from both stores.
    ///
    /// Takes the cutoff rather than a day count so the two sweeps cannot drift
    /// apart, and so a caller that decided not to expire anything —
    /// `pneuma_store::retention_cutoff` returning `None` — simply does not call
    /// this.
    ///
    /// The two halves currently measure age differently: Mongo history by when
    /// it was archived, Postgres by when the node_run was created
    /// (the design notes). Deliberate, and recorded there rather than
    /// papered over here.
    pub async fn expire_history(&self, cutoff: DateTime<Utc>) -> Result<Expired, JanitorError> {
        let runs = self.history.delete_outdated(cutoff).await?;
        let node_runs = self.node_runs.delete_outdated_history(cutoff).await?;
        Ok(Expired { runs, node_runs })
    }

    /// One full cycle: name what has stalled, expire old history, then clean
    /// up.
    ///
    /// # Three passes, and the order is forced twice
    ///
    /// **Stale selection runs first**, because `cleanup_runs` deletes the
    /// node_run rows of every finalised run regardless of their status. A run
    /// finalised in Mongo can still hold a non-finalised, stale row in
    /// Postgres; scanning after the delete would never see it, while
    /// [`Janitor::preview_pass`] — which deletes nothing — always would. The two
    /// would then disagree in exactly the week-long production diff the preview
    /// exists for.
    ///
    /// **Expiry runs before cleanup**, and this is the constraint worth
    /// spelling out because an earlier version of this method got it backwards
    /// and justified it with a claim that is false.
    ///
    /// The claim was that a document archived by this cycle carries a fresh
    /// `archived_at` and so cannot be expired by it. That holds for the *run*
    /// documents (the design notes) and not for the node_run rows — §19's own
    /// last bullet records that the change was **not carried to the Postgres
    /// half**, where `history_backup.sql` copies `node_run.created_at` verbatim
    /// and `history_delete_outdated.sql` still filters on it. So for a run that
    /// started twenty days ago and finished a minute ago, with a fourteen-day
    /// window, cleaning up first would copy its rows in with a twenty-day-old
    /// `created_at` and expire them moments later — its per-step history
    /// destroyed by the pass that archived it, every time.
    ///
    /// That is the defect notes, which says plainly not to order these
    /// two, because ordering turns a race into a guarantee. Expiring first
    /// keeps the window §24 asks for — and it is a small one: rows archived by
    /// this cycle survive until the *next cycle*, which at the original's
    /// `INTERVAL_MINUTES` of `*/5` is about five minutes, under a fourteen-day
    /// policy. This stops the method making §24 worse; it does not come close
    /// to fixing it. That needs an `archived_at` column on `node_run_history`, a
    /// migration and the product decision the design notes defers.
    pub async fn pass(&self, settings: &Settings) -> Result<Pass, JanitorError> {
        let stale = self.stale_for(settings).await?;
        let expired = match settings.retention_cutoff() {
            Some(cutoff) => Some(self.expire_history(cutoff).await?),
            None => None,
        };

        // Which runs this cycle is about to take, so they can be dropped from
        // the stale list. A run finalised in Mongo can still hold a
        // non-finalised, stale row in Postgres -- `delete_by_run_ids` removes
        // those rows whatever their status -- so scanning before the delete
        // reports runs this very pass then archives and deletes. The binary
        // would submit a termination for a run whose records exist nowhere, and
        // has no way to tell those from genuinely live ones.
        //
        // Selected read-only first rather than reordering the scan: putting the
        // scan after the delete would hide those rows from `pass` while
        // `preview_pass`, which deletes nothing, still saw them -- and the two
        // disagreeing is the thing the week-long production diff cannot afford.
        let taking = self.preview_cleanup(settings.batch).await?;
        let stale = reportable_stale(stale, &taking);

        let cleaned = self.cleanup_runs(settings.batch).await?;
        Ok(Pass {
            cleaned,
            expired,
            stale,
        })
    }

    /// What [`Janitor::pass`] would do, without doing any of it.
    ///
    /// The `--dry-run` of a whole cycle, which is what the original plan
    /// runs against production for a week before cutover. `Cleaned` carries
    /// only the count of runs a real pass would select — the other fields stay
    /// zero, because a preview cannot know what a copy or a delete would report
    /// without performing it, and guessing would put invented numbers in front
    /// of the person doing the diff.
    pub async fn preview_pass(&self, settings: &Settings) -> Result<Pass, JanitorError> {
        let selected = self.preview_cleanup(settings.batch).await?;
        let expired = match settings.retention_cutoff() {
            Some(cutoff) => Some(self.preview_expiry(cutoff).await?),
            None => None,
        };
        // Filtered by what the cycle would take, exactly as `pass` filters it.
        // It was not, and the disagreement was the one the comment in `pass`
        // says the week-long production diff cannot afford: a run that a real
        // pass archives and deletes was reported as stale by the preview and
        // not by the pass, so the diff shows a difference in behaviour where
        // there is only a difference in the code that reports it.
        //
        // Shared through `reportable_stale` rather than by repeating the line.
        // Repeating it is how the two came apart, and `preview` and `baseline`
        // in `pneuma-migrate` had the identical bug for the identical reason.
        let stale = reportable_stale(self.stale_for(settings).await?, &selected);
        Ok(Pass {
            cleaned: Cleaned {
                runs: selected.len(),
                ..Cleaned::default()
            },
            expired,
            stale,
        })
    }

    /// Stale runs, or nothing when the setting is off.
    ///
    /// Shared by the pass and its preview: selection has no side effect, so the
    /// two ask the same question, and there is nothing to keep in step.
    async fn stale_for(&self, settings: &Settings) -> Result<Vec<String>, JanitorError> {
        if !settings.terminate_stale {
            return Ok(Vec::new());
        }
        let Some(threshold) = settings.stale_threshold() else {
            // Not an empty list. Empty means "nothing is stale", and returning
            // it here would have the binary terminate nothing while reporting
            // success -- the silent no-op this crate propagates errors
            // precisely to avoid, and which `settings.rs` rejects the
            // neighbouring out-of-range values loudly for.
            return Err(JanitorError::UnusableStaleWindow {
                seconds: settings.stale_after.num_seconds(),
            });
        };
        self.stale_run_ids(threshold, settings.batch).await
    }

    /// Runs whose work has stopped moving, for the caller to terminate.
    ///
    /// Selection only. The original submits each id to the gateway over HTTP,
    /// and the gateway is the
    /// authority on cancellation — so that call belongs to the binary, and
    /// `pneuma-gateway-client` deliberately supplies the endpoint and the wire
    /// types without a client. Keeping the split here is what lets everything
    /// in this crate be driven against real databases instead of a fake.
    pub async fn stale_run_ids(
        &self,
        threshold: DateTime<Utc>,
        limit: i64,
    ) -> Result<Vec<String>, JanitorError> {
        // Postgres rejects a negative `LIMIT` outright, so a caller paging with
        // `limit - done` and reaching below zero would turn what should be an
        // empty result into a failed pass. `finalized_runs` guards its own
        // limit for the neighbouring reason — MongoDB reads `limit: 0` as *no
        // limit* — and the two now behave the same way from outside.
        if limit <= 0 {
            return Ok(Vec::new());
        }
        Ok(self
            .node_runs
            .stale_inprogress_run_ids(threshold, limit)
            .await?)
    }
}
