//! Run-document reads and status writes.
//!
//! The barrier operations live in [`crate::barrier`]; this is the rest of the
//! run document. Nothing here carries a known concurrency defect — the two that
//! do are both in that module.

use mongodb::bson::{doc, Document};
use mongodb::options::WriteConcern;
use mongodb::Collection;
use pneuma_core::status::RunStatus;
use pneuma_core::step::StepStatus;

use crate::barrier::BarrierError;
use crate::conflict::{is_duplicate_key, Created};
use crate::refpath::StepId;

/// Whether a run in this state is done.
///
/// `FINALIZED_STATUSES`, which
/// **includes** `CANCELLED`. Worth noting against the design notes: the
/// original already treats cancellation as terminal at the *run* level and
/// only fails to at the *node* level, so that deviation aligns the two rather
/// than inventing a rule.
fn is_terminal(status: RunStatus) -> bool {
    // An exhaustive `match`, not `matches!`. A variant added to
    // `pneuma_core::status::RunStatus` must be classified deliberately; with
    // `matches!` it would silently become non-terminal.
    match status {
        RunStatus::Finished | RunStatus::Error | RunStatus::TimedOut | RunStatus::Cancelled => true,
        RunStatus::Created | RunStatus::Processing => false,
    }
}

/// Every run status, for building the `$in` / `$nin` lists.
///
/// Derived from `pneuma_core::status::RunStatus` rather than redeclared. An
/// earlier version of this module defined its own six-variant enum with a
/// hand-written `as_str`, which contradicted the rule stated for
/// `step_status_wire` in this same file — and would have left the `$in` lists
/// silently short if core ever gained a variant, so a run in it would be
/// neither archived nor cancellable.
/// Forces [`ALL_RUN_STATUSES`] to be updated when a variant is added.
///
/// An array literal is not exhaustiveness-checked: adding `Paused` to core
/// leaves the array at six, so the status appears in no `$in` or `$nin` list —
/// never stale-scanned, never archived, silently non-terminal, and not covered
/// by the terminal guard. This `match` makes that a compile error. An earlier
/// version of this module claimed deriving from core prevented it; deriving the
/// *type* does not, only this does.
// Clippy calls this match unnecessary, which is precisely the point: an
// identity `match` compiles to nothing and exists only so that adding a variant
// to `pneuma_core::status::RunStatus` fails to compile here. Replacing it with
// the "obvious" `status` would remove the guard and restore the silent-short-
// list failure it was added for.
#[allow(clippy::needless_match)]
const fn every_variant_is_listed(status: RunStatus) -> RunStatus {
    match status {
        RunStatus::Created => RunStatus::Created,
        RunStatus::Processing => RunStatus::Processing,
        RunStatus::Finished => RunStatus::Finished,
        RunStatus::Error => RunStatus::Error,
        RunStatus::TimedOut => RunStatus::TimedOut,
        RunStatus::Cancelled => RunStatus::Cancelled,
    }
}

const ALL_RUN_STATUSES: [RunStatus; 6] = [
    every_variant_is_listed(RunStatus::Created),
    every_variant_is_listed(RunStatus::Processing),
    every_variant_is_listed(RunStatus::Finished),
    every_variant_is_listed(RunStatus::Error),
    every_variant_is_listed(RunStatus::TimedOut),
    every_variant_is_listed(RunStatus::Cancelled),
];

/// The wire tags of every status matching `wanted`.
///
/// `filter_map` would silently drop a status with no tag, shrinking the list —
/// and a short `$nin` is exactly how `update_run_status` would move a terminal
/// run, which is the failure its guard exists to prevent.
fn wire_list(wanted: impl Fn(RunStatus) -> bool) -> Result<Vec<String>, BarrierError> {
    ALL_RUN_STATUSES
        .into_iter()
        .filter(|status| wanted(*status))
        .map(|status| require_wire(run_status_wire(status), "runs", "status"))
        .collect()
}

/// The wire tags of the terminal statuses.
fn terminal_wire_list() -> Result<Vec<String>, BarrierError> {
    wire_list(is_terminal)
}

/// A wire tag, or the error a caller should return.
///
/// The three call sites all need "the tag, or fail" and all three arms are
/// unreachable while every status variant is a unit variant — a property of the
/// enums, not of the call sites. Collecting the decision here makes it testable
/// once instead of being dead code three times.
fn require_wire(
    tag: Option<String>,
    map: &'static str,
    key: &'static str,
) -> Result<String, BarrierError> {
    tag.ok_or_else(|| BarrierError::Malformed {
        map: map.to_owned(),
        key: key.to_owned(),
    })
}

// NOTE: this reuses `Malformed`, whose message says "run document field
// {map}.{key} is not the shape a barrier needs". For the `run_id` case that is
// accurate. For a status with no wire tag it points an operator at the database
// when the fault is in this crate's enums — worth a distinct variant if that
// arm ever becomes reachable, which it is not while every variant is a unit
// variant.

/// The wire spelling of a [`RunStatus`], as the run document stores it.
///
/// From the serde tag, for the same reason as [`step_status_wire`]: a second
/// table is a second thing to drift.
fn run_status_wire(status: RunStatus) -> Option<String> {
    tag_of(serde_json::to_value(status))
}

/// The wire spelling of a [`StepStatus`], as the run document stores it.
///
/// Derived from the serde tag rather than a second hand-written table, so the
/// two cannot drift; `pneuma-core` pins those tags against the original's three
/// enums.
fn step_status_wire(status: StepStatus) -> Option<String> {
    tag_of(serde_json::to_value(status))
}

/// The string out of a serialised unit variant.
///
/// Split from [`step_status_wire`] so the fallback arm is reachable: a
/// unit-variant enum with `rename_all` always serialises to a JSON string, so
/// through that function the arm cannot be taken and could not be tested. It
/// still has to exist — the alternative is an `unwrap` on a crate that denies
/// them.
fn tag_of(serialised: Result<serde_json::Value, serde_json::Error>) -> Option<String> {
    match serialised {
        Ok(serde_json::Value::String(tag)) => Some(tag),
        // An earlier version returned the Debug form here, which would `$set`
        // something like `Ok(Object {...})` as a step's status -- a corrupt run
        // document rather than an error. Unreachable while every variant is a
        // unit variant, which is a property of the enum and not of this
        // function, so it fails rather than guesses.
        _ => None,
    }
}

/// Reads and status writes over the `runs` collection.
#[derive(Debug, Clone)]
pub struct RunStore {
    runs: Collection<Document>,
}

impl RunStore {
    /// Wraps the `runs` collection.
    pub fn new(runs: Collection<Document>) -> Self {
        RunStore { runs }
    }

    /// Which collection this reads. For a caller composing several stores and
    /// needing to check they agree — see `pneuma_janitor::Janitor::new`.
    pub fn namespace(&self) -> mongodb::Namespace {
        self.runs.namespace()
    }

    /// Inserts a run, treating an existing one as success.
    ///
    /// The whole document, verbatim: the original builds it as the pipeline's
    /// own fields plus `state`, `run_id`, `status`, `created_at`, `step_input`
    /// and the three `topic_*` keys,
    /// so anything a pipeline carries that nothing here models still reaches
    /// the collection. Typing it would drop those fields silently, which is a
    /// wire regression rather than a tidy-up.
    ///
    /// [`Created::AlreadyExists`] rather than an error, because that is what a
    /// broker redelivery of an already-handled message looks like — see
    /// [`crate::conflict`] and the defect notes It is only reachable
    /// once `run_id` is actually unique; against today's non-unique index the
    /// second insert succeeds and forks the run, which is the defect §25 is
    /// about.
    pub async fn create(&self, document: Document) -> Result<Created, BarrierError> {
        match self.runs.insert_one(document).await {
            Ok(_) => Ok(Created::Inserted),
            Err(error) if is_duplicate_key(&error) => Ok(Created::AlreadyExists),
            Err(error) => Err(BarrierError::Mongo(error)),
        }
    }

    /// Moves a step's status within the run's `state` map.
    ///
    /// One operation, where the original reads the run, checks
    /// `step_id not in run.state`, then writes.
    /// The existence check is the
    /// filter here, so there is no window between deciding and writing, and a
    /// `$set` cannot create a step entry that was never resolved.
    ///
    /// Takes a [`StepStatus`], **not** a `NodeStatus`. The original's signature
    /// is `ModelNodeStepStatus | ListAggregatorStepStatus |
    /// DictAggregatorStepStatus`, and
    /// the two vocabularies genuinely differ — `StepStatus` has `pending` and
    /// `started`, `NodeStatus` has `created` and `cancelled`. Passing the wrong
    /// one writes a string the run document has no meaning for; this method was
    /// first written taking `NodeStatus`, which is exactly the mistake
    /// the design notes exist to prevent.
    ///
    /// `Ok(false)` means no run matched, or the run has no such step.
    pub async fn update_step_status(
        &self,
        run_id: &str,
        step_id: &StepId,
        status: StepStatus,
    ) -> Result<bool, BarrierError> {
        let exists = format!("state.{step_id}");
        let field = format!("state.{step_id}.status");
        let wire = require_wire(step_status_wire(status), "state", "status")?;
        // matched_count rather than the returned document: only whether it
        // matched is used, and the run document is large.
        let outcome = self
            .runs
            .update_one(
                doc! { "run_id": run_id, &exists: { "$exists": true } },
                doc! { "$set": { &field: wire } },
            )
            .write_concern(WriteConcern::majority())
            .await?;
        Ok(outcome.matched_count > 0)
    }

    /// Moves the run's own status.
    ///
    /// `error_slug` is written only when supplied. The original sets it
    /// unconditionally, so a status
    /// update without one clears any path already recorded — and that field is
    /// read. Leaving it alone is the
    /// smaller surprise; recorded as the design notes
    ///
    /// **A run that is already terminal is not moved.** The original writes
    /// unconditionally, which is the run-level instance of the defect
    /// the design notes close at the node level: the gateway cancels a run,
    /// an in-flight node result lands, the controller calls
    /// `update_run_status(.., Finished, ..)`, and a cancelled job is reported
    /// as successful. `cancel_runs` directly below already refuses to overwrite
    /// a terminal run; this makes the two agree. Recorded as
    /// the design notes
    ///
    /// `Ok(false)` therefore means no such run **or** the run is already
    /// terminal.
    pub async fn update_run_status(
        &self,
        run_id: &str,
        status: RunStatus,
        error_slug: Option<&str>,
    ) -> Result<bool, BarrierError> {
        let wire = require_wire(run_status_wire(status), "runs", "status")?;
        let mut set = doc! { "status": wire };
        if let Some(path) = error_slug {
            set.insert("error_slug", path);
        }
        let terminal = terminal_wire_list()?;
        // matched_count, not the document: the run document carries every
        // step's state and both payloads, and only whether it matched is used.
        let outcome = self
            .runs
            .update_one(
                doc! { "run_id": run_id, "status": { "$nin": terminal } },
                doc! { "$set": set },
            )
            .write_concern(WriteConcern::majority())
            .await?;
        Ok(outcome.matched_count > 0)
    }

    /// Cancels every run in `run_ids` that is not already terminal.
    ///
    /// The non-terminal filter is what makes this safe to retry and what stops
    /// it overwriting a finished run — the same filter the gateway's
    /// `bulk_cancel_by_job_id` uses. Duplicate ids are harmless; the filter is
    /// a set membership.
    ///
    /// Returns how many runs were actually moved.
    pub async fn cancel_runs(&self, run_ids: &[String]) -> Result<u64, BarrierError> {
        if run_ids.is_empty() {
            return Ok(0);
        }
        let terminal = terminal_wire_list()?;
        let cancelled = require_wire(run_status_wire(RunStatus::Cancelled), "runs", "status")?;
        let outcome = self
            .runs
            .update_many(
                doc! {
                    "run_id": { "$in": run_ids },
                    "status": { "$nin": terminal },
                },
                doc! { "$set": { "status": cancelled } },
            )
            .write_concern(WriteConcern::majority())
            .await?;
        Ok(outcome.modified_count)
    }

    /// Runs that have finished, oldest first, for the janitor to archive.
    pub async fn finalized_runs(&self, limit: i64) -> Result<Vec<String>, BarrierError> {
        self.runs_with_status(true, limit).await
    }

    /// Runs still `CREATED` or `PROCESSING`, oldest first, for the stale scan
    /// (`STALE_SCAN_STATUSES`, the original).
    pub async fn active_runs(&self, limit: i64) -> Result<Vec<String>, BarrierError> {
        self.runs_with_status(false, limit).await
    }

    /// MongoDB treats `limit: 0` as **no limit** and a negative limit as a
    /// single-batch hint, so a caller computing `batch - done` and reaching
    /// zero would stream the whole collection — every run document, each
    /// carrying both payloads — into a `Vec`. Non-positive means nothing here.
    async fn runs_with_status(
        &self,
        terminal: bool,
        limit: i64,
    ) -> Result<Vec<String>, BarrierError> {
        if limit <= 0 {
            return Ok(Vec::new());
        }
        let wanted = wire_list(|status| is_terminal(status) == terminal)?;
        let mut cursor = self
            .runs
            // Only the id is used, so only the id is fetched.
            // `run_id` must be a string in the filter, not checked after the
            // fact. Erroring on a bad row makes it a poison pill: the sort is
            // `_id: 1`, so one malformed document sits in every page forever
            // and the janitor never makes progress. Excluding it in the filter
            // means it neither blocks the batch nor consumes the limit.
            .find(doc! { "status": { "$in": wanted }, "run_id": { "$type": "string" } })
            .projection(doc! { "run_id": 1, "_id": 1 })
            // `_id` ascending, matching the original's `sort={"_id": 1}` --
            // insertion order, so the janitor works through the backlog rather
            // than revisiting the same page.
            .sort(doc! { "_id": 1 })
            .limit(limit)
            .await?;

        let mut found = Vec::new();
        while cursor.advance().await? {
            let document = cursor.deserialize_current()?;
            // The filter already required a string `run_id`, so this cannot
            // fail for a row the server returned. It is still an error rather
            // than a skip: a silent skip would consume the limit and hide the
            // inconsistency.
            let run_id = require_wire(
                document.get_str("run_id").ok().map(str::to_owned),
                "runs",
                "run_id",
            )?;
            found.push(run_id);
        }
        Ok(found)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn step_status_wire_spellings_come_from_the_serde_tags() {
        // All ten, and none falling through to the Debug fallback.
        for (status, wire) in [
            (StepStatus::Pending, "pending"),
            (StepStatus::Started, "started"),
            (StepStatus::Processing, "processing"),
            (StepStatus::Finished, "finished"),
            (StepStatus::Error, "error"),
            (StepStatus::TimedOut, "timed_out"),
            (StepStatus::Forked, "forked"),
            (StepStatus::Aggregated, "aggregated"),
            (StepStatus::HasChildError, "has_child_error"),
            (StepStatus::HasChildTimedOut, "has_child_timed_out"),
        ] {
            assert_eq!(step_status_wire(status).as_deref(), Some(wire));
        }
        for status in StepStatus::ALL {
            assert!(
                step_status_wire(status).is_some(),
                "{status:?} has no wire tag"
            );
        }
    }

    #[test]
    fn a_missing_wire_tag_becomes_an_error_rather_than_a_write() {
        // Unreachable through the three call sites while every status variant
        // is a unit variant -- which is a property of the enums, not of those
        // sites, so the decision is made once here and tested here.
        let Ok(tag) = require_wire(Some("finished".to_owned()), "runs", "status") else {
            panic!("a present tag is returned");
        };
        assert_eq!(tag, "finished");

        let Err(BarrierError::Malformed { map, key }) = require_wire(None, "runs", "status") else {
            panic!("an absent tag must be an error, not a Debug string written as a status");
        };
        assert_eq!(map, "runs");
        assert_eq!(key, "status");
    }

    #[test]
    fn a_non_string_serialisation_falls_back_rather_than_panicking() {
        // Unreachable through step_status_wire -- a unit variant always yields
        // a string -- but the arm exists so the crate needs no unwrap, and an
        // arm that cannot be reached cannot be tested.
        assert_eq!(
            tag_of(Ok(serde_json::Value::String("ok".to_owned()))).as_deref(),
            Some("ok")
        );
        // Not a string: `None`, so the caller errors rather than writing a
        // Debug rendering into the run document as a status.
        assert_eq!(tag_of(Ok(serde_json::json!(7))), None);
        assert_eq!(tag_of(Ok(serde_json::Value::Null)), None);
    }

    #[test]
    fn the_wire_spellings_match_originals_strenum() {
        // StrEnum with auto() lowercases the member name.
        for (status, wire) in [
            (RunStatus::Created, "created"),
            (RunStatus::Processing, "processing"),
            (RunStatus::Finished, "finished"),
            (RunStatus::Error, "error"),
            (RunStatus::TimedOut, "timed_out"),
            (RunStatus::Cancelled, "cancelled"),
        ] {
            assert_eq!(run_status_wire(status).as_deref(), Some(wire));
        }
        assert_eq!(ALL_RUN_STATUSES.len(), 6);
    }

    #[test]
    fn the_exhaustiveness_guard_is_the_identity() {
        // `every_variant_is_listed` exists so that adding a variant to core is
        // a compile error rather than a silently short `$in` list. It is a
        // `const fn` used in a const context, so nothing executes it at
        // runtime; calling it here both covers it and pins that it maps each
        // variant to itself rather than quietly rewriting one.
        for status in ALL_RUN_STATUSES {
            assert_eq!(every_variant_is_listed(status), status);
        }
        assert_eq!(ALL_RUN_STATUSES.len(), 6);
    }

    #[test]
    fn terminal_is_exactly_finalized_statuses() {
        // FINALIZED_STATUSES, the original -- four, and CANCELLED is
        // one of them, unlike the node-level list.
        let Ok(terminal) = terminal_wire_list() else {
            panic!("every status has a wire tag");
        };
        assert_eq!(terminal, ["finished", "error", "timed_out", "cancelled"]);

        let Ok(active) = wire_list(|status| !is_terminal(status)) else {
            panic!("every status has a wire tag");
        };
        assert_eq!(active, ["created", "processing"], "STALE_SCAN_STATUSES");
    }
}
