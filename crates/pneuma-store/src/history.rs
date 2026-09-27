//! Archiving finished runs, and expiring the archive.
//!
//! The Mongo half of the janitor's cleanup. The Postgres half is
//! [`crate::store::NodeRunStore::backup_to_history`] and its neighbours; this is
//! the `runs` → `run_history` copy and the two deletes that follow it.
//!
//! # Copied by the server, not through the process
//!
//! The original reads whole run documents into the janitor and writes them back
//! as a bulk upsert. Every
//! document carries both payloads, so a batch of a hundred moves a lot of JSON
//! twice for no reason. `$merge` does the copy inside the server, which is what
//! `sql/history_backup.sql` already does for Postgres and for the same reason.
//!
//! It also deletes a whole category of bookkeeping. The original has to work out
//! which documents of a partially-failed `bulk_write` actually landed, and hand
//! that subset back so the janitor deletes only those — about sixty lines of
//! index arithmetic against `writeErrors`. `$merge` is not atomic either, but it
//! *is* idempotent, so the recovery is "run it again" rather than "reconstruct
//! what happened": archive succeeds and the caller may delete, or it fails and
//! the next pass repeats it. Nothing has to be reconstructed, and the two halves
//! cannot disagree about which runs were archived.
//!
//! Measured against `mongo:5.0.28` rather than assumed: `$merge` on `_id`
//! creates the target collection if absent, leaves the count unchanged when
//! re-run, and matching nothing is a no-op rather than an error.
//!
//! # Retention runs from archiving, not from the id
//!
//! [`RunHistoryStore::archive`] stamps `archived_at`, and
//! [`RunHistoryStore::delete_outdated`] filters on it. The original derives the
//! cutoff from `ObjectId` generation time — its docstring says so — because the
//! run document has no creation timestamp. The defect notes record
//! what that costs: a run alive longer than the retention window is archived and
//! deleted on the same janitor pass, so exactly the runs worth investigating
//! keep no history at all. See the design notes

use chrono::{DateTime, Duration, Utc};
use mongodb::bson::oid::ObjectId;
use mongodb::bson::{doc, Bson, DateTime as BsonDateTime, Document};
use mongodb::options::WriteConcern;
use mongodb::Collection;

use crate::barrier::BarrierError;

/// The field [`RunHistoryStore::archive`] stamps and
/// [`RunHistoryStore::delete_outdated`] reads.
///
/// Named once so the two cannot drift apart — they are only useful as a pair,
/// and a retention sweep reading a field nothing writes deletes nothing for
/// ever, silently.
pub const ARCHIVED_AT: &str = "archived_at";

/// Scratch field the merge pipeline carries the surviving stamp in.
///
/// Removed by the pipeline's last stage, so it never reaches a stored
/// document — asserted, not assumed, in `tests/history.rs`.
const KEPT: &str = "__pneuma_kept_archived_at";

/// The smallest `ObjectId` that could have been generated at `instant`.
///
/// An `ObjectId`'s first four bytes are its creation time in whole seconds, big
/// endian, and the remaining eight are machine and counter. Zeroing those makes
/// the lowest id for that second, so `_id < oldest_id_at(cutoff)` selects
/// exactly the documents created before it — which is what
/// `ObjectId.from_datetime` builds on the original side.
///
/// One-second granularity, inherited from the encoding rather than chosen. It
/// does not matter for a window measured in days, and it is the same
/// granularity the original compares at.
fn oldest_id_at(instant: DateTime<Utc>) -> ObjectId {
    let seconds = u32::try_from(instant.timestamp().max(0)).unwrap_or(u32::MAX);
    let mut bytes = [0_u8; 12];
    bytes[..4].copy_from_slice(&seconds.to_be_bytes());
    ObjectId::from_bytes(bytes)
}

/// The cutoff a retention window implies, or `None` when there is not one.
///
/// `None` for a non-positive window, which the original spells as an early
/// return with a warning and
/// means "do not delete". Kept here rather than left to the caller because the
/// obvious call-site arithmetic — `Utc::now() - Duration::days(days)` — turns
/// `0` into "now", which deletes the entire archive including runs written
/// seconds earlier. The setting that used to disable expiry would have become
/// the one that empties it.
///
/// `None` also at the far end, where `Duration::days` *panics* — so the
/// function whose whole purpose is to make a hostile setting safe would have
/// aborted the janitor on a fat-fingered one. Both ends now answer the same
/// way: no cutoff, no deletion.
///
/// Two limits, measured against the pinned chrono rather than estimated, and
/// both are needed:
///
/// * subtracting from `Utc::now()` leaves `DateTime<Utc>`'s range above
///   **96,485,985 days** (~264,000 years) — the effective limit, and the one
///   `checked_sub_signed` catches;
/// * `Duration::try_days` itself fails above **106,751,991,167 days**
///   (~292 million years).
///
/// An earlier version of this comment said "about 106 million years", which is
/// the day count of the second limit read as a year count, and named only the
/// bound that almost never bites. Anyone sizing a test against it would have
/// picked a value inside the first band and believed they were exercising the
/// overflow path.
pub fn retention_cutoff(retention_days: i64) -> Option<DateTime<Utc>> {
    if retention_days <= 0 {
        return None;
    }
    Duration::try_days(retention_days).and_then(|window| Utc::now().checked_sub_signed(window))
}

/// Copies finished runs into the history collection, and expires it.
#[derive(Debug, Clone)]
pub struct RunHistoryStore {
    runs: Collection<Document>,
    history: Collection<Document>,
}

impl RunHistoryStore {
    /// Wraps the `runs` and `run_history` collections.
    ///
    /// Refuses the two being the same collection, which is worth a fallible
    /// constructor because of what it would otherwise do rather than because it
    /// is likely. `$merge` into the collection being aggregated is *accepted*
    /// by the server, so [`Self::archive`] would return `Ok(())` while nothing
    /// reached any archive — and a caller honouring the documented ordering
    /// would then delete every finished run on the strength of it. Silent,
    /// total loss with the success contract satisfied.
    ///
    /// Measured under the current pipeline rather than carried over: the
    /// self-merge writes, stamping `archived_at` onto the live `runs`
    /// documents, so the damage is not even a no-op. An earlier version of this
    /// note described `keepExisting`'s behaviour, which was to write nothing —
    /// true when it was written, and stale the moment the merge strategy
    /// changed.
    ///
    /// Checked here rather than in `archive` so the invalid pairing cannot be
    /// held at all, which is what the rest of this crate does with
    /// [`crate::RefPath`] and a path that cannot round-trip.
    pub fn new(
        runs: Collection<Document>,
        history: Collection<Document>,
    ) -> Result<Self, BarrierError> {
        let (source, target) = (runs.namespace(), history.namespace());
        if source == target {
            return Err(BarrierError::SameCollection {
                namespace: format!("{}.{}", target.db, target.coll),
            });
        }
        Ok(RunHistoryStore { runs, history })
    }

    /// Which collection this archives *from*. A caller holding a separate
    /// [`crate::RunStore`] can check the two agree — see
    /// `pneuma_janitor::Janitor::new`.
    pub fn source_namespace(&self) -> mongodb::Namespace {
        self.runs.namespace()
    }

    /// Copies the named runs into the history collection.
    ///
    /// Safe to repeat, which is not quite the same as leaving the copy alone.
    /// A second pass refreshes the archived payload from the live run and
    /// leaves `archived_at` where the first pass put it — so the record
    /// improves while the retention clock does not restart. Re-archiving an
    /// unstamped copy the original janitor left behind therefore *does* change
    /// it, deliberately: that is how such a copy is adopted.
    ///
    /// A terminal run's document can still change, which is why refreshing is
    /// worth doing at all: `RunStore::update_run_status` carries a terminal
    /// guard, but `update_step_status` and the barrier writes do not, and
    /// route C in the defect notes are precisely a cancelled run whose
    /// in-flight nodes keep landing results. If a pass archives a run and fails
    /// before deleting it, those late writes reach the live document and the
    /// next pass carries them into history.
    ///
    /// An earlier version kept the staler copy and called it a deliberate
    /// trade against an advancing stamp. That was a false choice — the two are
    /// only exclusive among `$merge`'s *named* modes, and a pipeline has both.
    ///
    /// An empty slice does nothing rather than matching everything. `$in: []`
    /// matches no documents, so this is belt and braces, but the cost of being
    /// wrong is copying the collection.
    pub async fn archive(&self, run_ids: &[String]) -> Result<(), BarrierError> {
        if run_ids.is_empty() {
            return Ok(());
        }
        // Qualified with the database, not just the collection name. `$merge`
        // resolves a bare string against the *aggregation's* database, which is
        // `runs`' -- so a store built from two databases would archive into the
        // source's `run_history` while `delete_outdated` read the other one,
        // which would then never shrink and never be found. Measured, not
        // inferred: with `into: "run_history"` the copy lands in the source
        // database and the intended target stays empty.
        let target = self.history.namespace();
        let stamp = BsonDateTime::from_millis(Utc::now().timestamp_millis());
        self.runs
            .aggregate(vec![
                doc! { "$match": { "run_id": { "$in": run_ids } } },
                // The *client* clock, bound in, not `$$NOW`. `$$NOW` is the
                // server's, and `delete_outdated` compares against a cutoff the
                // caller computed here -- so the pair that is only useful
                // together would have been reading two different clocks. A host
                // resuming with a bad clock then either expires history it just
                // wrote or never expires it. Sharing one clock makes the window
                // hold whatever that clock says.
                doc! { "$set": { ARCHIVED_AT: stamp } },
                doc! { "$merge": {
                    "into": { "db": target.db, "coll": target.coll },
                    "on": "_id",
                    // Refresh the payload, hold the stamp. Neither of
                    // `$merge`'s named modes does both, and both are wanted:
                    //
                    // * `"replace"` refreshes but carries the new stamp, so a
                    //   run re-archived on every pass never expires;
                    // * `"keepExisting"` holds the stamp but keeps the existing
                    //   document *including a missing one*, leaving a copy the
                    //   original janitor wrote unstamped — and
                    //   `delete_outdated`'s legacy branch then expires it by
                    //   `_id` on the very pass that adopted it, which is
                    //   the defect notes reopened for exactly the runs
                    //   this protects.
                    //
                    // So: remember whichever stamp should survive, merge the
                    // live document over the archived one — what the original's
                    // `UpdateOne(..., $set: run, upsert=True)` does — then put
                    // the remembered stamp back. Refreshing matters because a
                    // terminal run's document *can* still change: §22 route C
                    // is a cancelled run whose in-flight nodes keep landing
                    // results, and `update_step_status` and the barrier writes
                    // carry no terminal guard.
                    "whenMatched": [
                        { "$set": { KEPT: { "$ifNull": [
                            format!("${ARCHIVED_AT}"),
                            format!("$$new.{ARCHIVED_AT}"),
                        ] } } },
                        { "$replaceRoot": { "newRoot": {
                            "$mergeObjects": ["$$ROOT", "$$new"],
                        } } },
                        { "$set": { ARCHIVED_AT: format!("${KEPT}") } },
                        { "$unset": KEPT },
                    ],
                    "whenNotMatched": "insert",
                } },
            ])
            // `$merge` writes, so the concern belongs on the aggregation.
            .write_concern(WriteConcern::majority())
            .await?;
        Ok(())
    }

    /// Deletes the named runs from the live collection.
    ///
    /// Call only after [`RunHistoryStore::archive`] has returned `Ok`. The
    /// ordering is the whole safety property: archive is idempotent, so a
    /// failure costs a repeat, whereas deleting first costs the run.
    pub async fn delete_archived(&self, run_ids: &[String]) -> Result<u64, BarrierError> {
        if run_ids.is_empty() {
            return Ok(0);
        }
        let outcome = self
            .runs
            .delete_many(doc! { "run_id": { "$in": run_ids } })
            .write_concern(WriteConcern::majority())
            .await?;
        Ok(outcome.deleted_count)
    }

    /// Deletes history archived before `cutoff`.
    ///
    /// The cutoff is passed in rather than computed from `retention_days` here:
    /// "how long to keep history" is the operator's policy and belongs with the
    /// settings that express it, and a store that reads the clock cannot be
    /// tested without one.
    ///
    /// Expires by `archived_at` where it exists, and by the `_id`'s own
    /// timestamp where it does not.
    ///
    /// The second clause is not a nicety: every document the *original* janitor
    /// wrote is a verbatim copy of the run with no `archived_at`.
    /// Filtering on the stamp
    /// alone would make the entire pre-existing archive permanently
    /// unexpirable, on cutover, in the one collection retention exists to
    /// bound — and those documents carry both payloads. So a legacy document is
    /// expired the way the original expired it, by `ObjectId` generation time,
    /// and a document this crate wrote is expired by
    /// when it was archived. No backfill, and the semantics each document was
    /// written under are the ones it is judged by.
    pub async fn delete_outdated(&self, cutoff: DateTime<Utc>) -> Result<u64, BarrierError> {
        let outcome = self
            .history
            .delete_many(outdated(cutoff))
            .write_concern(WriteConcern::majority())
            .await?;
        Ok(outcome.deleted_count)
    }

    /// How many documents [`RunHistoryStore::delete_outdated`] would remove.
    ///
    /// Shares the filter rather than restating it. A preview built from a
    /// separate copy of the predicate is worse than no preview: it reports
    /// confidently about a decision the deleting code does not make, and the
    /// two drift silently because nothing compares them. The original plan
    /// runs this against production for a week and diffs it against the original
    /// janitor, which only means anything if it previews the real rule.
    pub async fn count_outdated(&self, cutoff: DateTime<Utc>) -> Result<u64, BarrierError> {
        Ok(self.history.count_documents(outdated(cutoff)).await?)
    }
}

/// History that has outlived `cutoff`.
///
/// One definition, used by both the count and the delete — see
/// [`RunHistoryStore::count_outdated`] for why that matters.
fn outdated(cutoff: DateTime<Utc>) -> Document {
    // Milliseconds rather than `DateTime::from_chrono`, which is behind a bson
    // feature this crate does not enable. BSON dates are millisecond-precision
    // anyway, so nothing is lost that the encoding would have kept.
    let before = Bson::DateTime(BsonDateTime::from_millis(cutoff.timestamp_millis()));
    doc! {
        "$or": [
            { ARCHIVED_AT: { "$lt": &before } },
            {
                ARCHIVED_AT: { "$exists": false },
                // Only an ObjectId carries a timestamp to infer an age from, so
                // only an ObjectId is judged this way.
                //
                // The `$type` is belt and braces, and the note is here because
                // the obvious reasoning for it is wrong: BSON's *sort* order
                // does place strings and numbers below ObjectIds, but query
                // comparison is type-bracketed, so `$lt` against an ObjectId
                // already matches ObjectIds alone. Measured, after an earlier
                // version of this comment asserted the opposite. It is kept
                // because it states the intent, and because `$expr`
                // comparisons are *not* type-bracketed — if this filter ever
                // moves there, the guard is what stops the rule changing
                // underneath it.
                "_id": { "$type": "objectId", "$lt": oldest_id_at(cutoff) },
            },
        ]
    }
}
