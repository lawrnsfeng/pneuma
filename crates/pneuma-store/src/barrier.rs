//! The aggregation barrier, decided from the value the atomic update returns.
//!
//! # The defect this replaces
//!
//! `increase_refcount` in the original is correct:
//! an atomic `find_one_and_update`
//! with `$addToSet` and `return_document=True`, which hands back the post-state
//! set. The caller discards that and
//! re-reads the document, and `_try_aggregate` reads a *third* time
//! and decides completion from that separate read.
//!
//! Two children arriving concurrently both `$addToSet`, both then read the
//! complete set, and **both aggregate**. Reproduced against `mongo:5.0.28` in
//! 200 of 200 trials; the prerequisite barrier twenty lines away, which decides
//! from the returned set, was correct in 200 of 200. See
//! `CONCURRENCY-AND-DIRECTION.md`.
//!
//! So this module exposes exactly one way to arrive at a barrier, and it
//! returns the post-state. There is no "read the refcount" operation to reach
//! for, because reaching for one is the defect.

use mongodb::bson::{doc, Document};
use mongodb::options::{ReadConcern, ReturnDocument, WriteConcern};
use mongodb::Collection;

use pneuma_core::child_index::ChildIndex;

use crate::refpath::{RefPath, StepId};

/// What a child's arrival established.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Arrival {
    /// The children known to have arrived, including this one.
    pub arrived: Vec<String>,
    /// Whether **this call** added the member, as opposed to finding it
    /// already present.
    ///
    /// `$addToSet` makes the *set* idempotent but not the *decision*. Without
    /// this, a redelivered arrival for a child that already arrived returns the
    /// same full set, `completes` is true a second time, and the aggregator
    /// fires twice — which is the defect this module exists to prevent,
    /// reached by a different route than the concurrent one.
    pub newly_arrived: bool,
}

impl Arrival {
    /// How many children have arrived.
    pub fn count(&self) -> usize {
        self.arrived.len()
    }

    /// Whether **this** arrival completed the barrier.
    ///
    /// Two conditions, and both are needed. The set must have reached the
    /// expected width — decided from one atomic result and one caller-supplied
    /// number, never a follow-up read — *and* this call must be the one that
    /// added the member. Exactly one caller can satisfy both, ever: concurrent
    /// first arrivals are serialised by `$addToSet`, and a redelivery is not
    /// `newly_arrived`.
    ///
    /// An earlier version tested only the width. That closed the concurrent
    /// window and left the redelivery one open.
    pub fn completes(&self, expected: usize) -> bool {
        self.newly_arrived && self.arrived.len() == expected
    }
}

/// Why an arrival could not be recorded.
#[derive(Debug, thiserror::Error)]
pub enum BarrierError {
    /// The database refused, or was unreachable.
    #[error("mongo error: {0}")]
    Mongo(#[from] mongodb::error::Error),
    /// No run document with that id.
    #[error("no run {run_id:?}")]
    RunNotFound {
        /// The run that was looked for.
        run_id: String,
    },
    /// A store was built from two collections that are the same one.
    ///
    /// Its own variant rather than a `Malformed`: nothing about the data is
    /// wrong, the wiring is. See [`crate::history::RunHistoryStore::new`].
    #[error("the source and target collections are both {namespace:?}")]
    SameCollection {
        /// The namespace given for both.
        namespace: String,
    },
    /// A barrier entry exists but is not an array, or a count is not a number.
    /// The original raises `OperationalError` in the same place.
    #[error("run document field {map}.{key} is not the shape a barrier needs")]
    Malformed {
        /// Which map.
        map: String,
        /// Which key within it.
        key: String,
    },
}

/// Records arrivals at aggregation barriers.
#[derive(Debug, Clone)]
pub struct BarrierStore {
    runs: Collection<Document>,
}

impl BarrierStore {
    /// Wraps the `runs` collection.
    pub fn new(runs: Collection<Document>) -> Self {
        BarrierStore { runs }
    }

    /// Records that `child` reached its parent's aggregation barrier, and
    /// returns the set as it stood immediately after.
    ///
    /// `$addToSet` makes this idempotent: a redelivered result for a child that
    /// already arrived does not double-count it. That matters because a
    /// redelivery is an ordinary event here.
    ///
    /// Write concern is `majority`, matching the original. The original also
    /// sets read concern `majority` on the same call; that applies to reads, and
    /// this is a write whose returned document comes from the write itself, so
    /// it is not carried over.
    ///
    /// Both are **no-ops on a standalone deployment** — the dev compose runs
    /// `mongo:5.0.28` with no `--replSet` — which the defect notes record
    /// as an open question, because it decides whether several findings are
    /// latent or active.
    pub async fn arrive(
        &self,
        run_id: &str,
        child: &RefPath,
        parent: &RefPath,
    ) -> Result<Arrival, BarrierError> {
        self.add_to_set(REFCOUNTS, run_id, parent, child.as_str())
            .await
    }

    /// Records that `prerequisite` completed for `node`, and returns the set as
    /// it stood immediately after.
    ///
    /// The *other* barrier in the run document, and in the original it is the
    /// one written correctly: its caller compares `len(...)` against
    /// `step.num_prerequisites` using the returned list,
    /// where the aggregation barrier
    /// twenty lines away re-reads instead.
    ///
    /// Here both are the same code path, which is the point. The original's
    /// defect is not that either operation is wrong — `increase_refcount` and
    /// `add_completed_prerequisites` are near-identical and both correct — but
    /// that one caller used the returned value and the other did not. Two
    /// barriers that cannot be reached except through one function cannot
    /// diverge that way.
    pub async fn complete_prerequisite(
        &self,
        run_id: &str,
        node: &RefPath,
        prerequisite: &str,
    ) -> Result<Arrival, BarrierError> {
        self.add_to_set(PREREQUISITES, run_id, node, prerequisite)
            .await
    }

    /// Records that one child of an aggregator will never arrive, and returns
    /// how many are still expected.
    ///
    /// # Why this is a set, not a counter
    ///
    /// The original writes an absolute value the caller computed from a
    /// document read earlier: `(parent_step.num_children or 0) - len(skip_node)`,
    /// where `parent_step` came from a
    /// read at the top of `process_result`. The `$set` is atomic; the
    /// arithmetic is not. Two conditionals resolving concurrently both read 5,
    /// both compute 4, both write 4. The answer is 3, and the aggregator waits
    /// forever. Measured on `mongo:5.0.28`: 200 of 200 trials wrong.
    ///
    /// A bare `$inc` delta composes and fixes that — 0 of 200 wrong — but
    /// trades it for a worse bug. The call site is **not** gated by the
    /// `redelivered` flag computed the original, so
    /// a redelivered conditional reaches it again. `$set` rewrites the same
    /// value; `$inc` decrements twice, `num_children` falls below the true
    /// child count, and the aggregator fires *early*, with a child's output
    /// missing. An earlier version of this method did exactly that.
    ///
    /// So the deduction is a **set of skipped node ids**, and the count is
    /// derived from it. Adding the same id twice changes nothing, and two
    /// different ids both land — idempotent and composable, which a counter
    /// cannot be at once. It is the same insight as the barrier above: what
    /// makes `$addToSet` right there makes it right here.
    ///
    /// Returns the number still expected: the base count less the skips
    /// recorded.
    ///
    /// `child_index` of `None` means slot 1, matching the original's
    /// `child_idx = child_idx or 1`.
    pub async fn skip_child(
        &self,
        run_id: &str,
        step_id: &StepId,
        child_index: Option<ChildIndex>,
        skipped_node: &str,
    ) -> Result<i64, BarrierError> {
        let slot = child_index.map_or(1, ChildIndex::get);
        let skipped = format!("state.{step_id}.skipped_children.{slot}");
        let updated = self
            .runs
            .find_one_and_update(
                doc! { "run_id": run_id },
                doc! { "$addToSet": { &skipped: skipped_node } },
            )
            .return_document(ReturnDocument::After)
            .write_concern(WriteConcern::majority())
            .await?;

        let Some(document) = updated else {
            return Err(BarrierError::RunNotFound {
                run_id: run_id.to_owned(),
            });
        };
        remaining_children(&document, step_id, slot)
    }

    /// How many children an aggregator still expects, without recording a skip.
    ///
    /// **The deduction lives in `skipped_children`, not in `num_children`.**
    /// `skip_child` records which node was skipped and derives the count; it
    /// deliberately does not write the derived value back, because doing so
    /// would make the write non-idempotent again — the thing recording a set
    /// was meant to avoid.
    ///
    /// The consequence, stated plainly because it is a real limitation: a
    /// reader that looks at `num_children` alone sees the *undeducted* count.
    /// Within this crate that cannot happen — this is the only way to ask. But
    /// the original service reads `state.{step}.num_children.{idx}` directly,
    /// so during any period where
    /// both run against one database, it would wait for a child this crate
    /// recorded as skipped. That is a cutover constraint, not a bug to fix
    /// here: writing the derived value back would reintroduce the
    /// double-decrement on redelivery.
    pub async fn expected_children(
        &self,
        run_id: &str,
        step_id: &StepId,
        child_index: Option<ChildIndex>,
    ) -> Result<i64, BarrierError> {
        let slot = child_index.map_or(1, ChildIndex::get);
        // `majority`, matching the original's
        // `with_options(read_concern=ReadConcern("majority"))`.
        // `arrive` declines to
        // carry it over because that is a write whose result comes from the
        // write itself; this is a read, and the completion decision is made
        // against it — without it, a replica set can serve a `skipped_children`
        // state that has not been majority-committed and can roll back.
        //
        // Projected to this step's subdocument -- not to two fields, as an
        // earlier comment claimed: `state.{step_id}: 1` brings the whole step
        // entry. The saving over the full run document, which carries every
        // other step plus both payloads, is real; the description was not.
        let step_field = format!("state.{step_id}");
        let found = self
            .runs
            .find_one(doc! { "run_id": run_id })
            .projection(doc! { &step_field: 1, "_id": 0 })
            .read_concern(ReadConcern::majority())
            .await?;
        let Some(document) = found else {
            return Err(BarrierError::RunNotFound {
                run_id: run_id.to_owned(),
            });
        };
        remaining_children(&document, step_id, slot)
    }

    /// The one atomic operation both barriers are.
    async fn add_to_set(
        &self,
        map: &str,
        run_id: &str,
        key: &RefPath,
        member: &str,
    ) -> Result<Arrival, BarrierError> {
        let field = format!("{map}.{key}");
        let updated = self
            .runs
            .find_one_and_update(
                doc! { "run_id": run_id },
                doc! { "$addToSet": { &field: member } },
            )
            // BEFORE, not After: the prior set is what says whether this call
            // added the member. With After the two are indistinguishable.
            .return_document(ReturnDocument::Before)
            .write_concern(WriteConcern::majority())
            .await?;

        let Some(document) = updated else {
            return Err(BarrierError::RunNotFound {
                run_id: run_id.to_owned(),
            });
        };

        read_arrival(&document, map, key, member)
    }
}

/// The run document's aggregation-barrier map.
const REFCOUNTS: &str = "refcounts";
/// The run document's prerequisite-barrier map.
const PREREQUISITES: &str = "completed_prerequisites";

/// Works out what an arrival established, from the document as it stood
/// *before* the update.
///
/// Separate from [`BarrierStore::add_to_set`] so its failure arm is reachable:
/// a live `$addToSet` cannot produce a non-array there.
///
/// An absent key is normal here — it is the first arrival — so, unlike an
/// earlier version, absence is not an error. What is an error is a key present
/// and not an array, because reporting that as an empty set would let a barrier
/// of zero complete immediately.
fn read_arrival(
    document: &Document,
    map: &str,
    key: &RefPath,
    member: &str,
) -> Result<Arrival, BarrierError> {
    let entry = match document.get(map) {
        None => None,
        Some(value) => match value.as_document() {
            Some(entries) => entries.get(key.as_str()),
            // Present and not a document. Reading it as an empty set would let
            // a barrier of one complete on the first arrival.
            None => {
                return Err(BarrierError::Malformed {
                    map: map.to_owned(),
                    key: key.as_str().to_owned(),
                })
            }
        },
    };

    let mut before: Vec<String> = match entry {
        None => Vec::new(),
        Some(value) => match value.as_array() {
            Some(members) => members
                .iter()
                .filter_map(|member| member.as_str().map(str::to_owned))
                .collect(),
            None => {
                return Err(BarrierError::Malformed {
                    map: map.to_owned(),
                    key: key.as_str().to_owned(),
                })
            }
        },
    };

    let newly_arrived = !before.iter().any(|existing| existing == member);
    if newly_arrived {
        before.push(member.to_owned());
    }
    Ok(Arrival {
        arrived: before,
        newly_arrived,
    })
}

/// How many children an aggregator still expects: the declared count less the
/// skips recorded against it.
///
/// Separate from [`BarrierStore::skip_child`] so its failure arms are
/// reachable — through that method the `$addToSet` has just created the skip
/// set, so only a malformed base count can fail, and only if something else
/// wrote it.
///
/// Accepts an integer of either width, and a double: `$inc` and a restore from
/// an export can each leave a different BSON numeric type, and refusing one
/// after the write has landed would leave a caller unable to retry safely.
fn remaining_children(
    document: &Document,
    step_id: &StepId,
    slot: u32,
) -> Result<i64, BarrierError> {
    let malformed = |key: String| BarrierError::Malformed {
        map: "state".to_owned(),
        key,
    };

    let step = document
        .get_document("state")
        .ok()
        .and_then(|state| state.get_document(step_id.as_str()).ok());
    let Some(step) = step else {
        return Err(malformed(step_id.as_str().to_owned()));
    };

    // `num_children` is read as a **scalar**, and the map form is not accepted.
    //
    // That is the design notes, a recorded decision: `Step::expected_children`
    // is `Option<u32>` and does not accept the map, because the cutover is a
    // drain — bootstrap ingress stops, original finishes its open runs, and only
    // then does this start — so no original-written run document is ever read.
    //
    // An earlier version accepted both shapes and justified it by "coexistence".
    // That contradicted §9 without revising it, and it was wrong on the facts:
    // under a drain the only writer of this field is this port, which
    // serialises a scalar. `ListAggregatorStep.num_children` being a
    // `dict[int, int]` while
    // `DictAggregatorStep`'s is `int | None` is the shape confusion the
    // survey notes identify as a latent hang; reproducing it here
    // is what §9 declines to do.
    //
    // The `slot` argument is kept because `skipped_children` is genuinely keyed
    // by fan-out slot.
    //
    // **This puts the drain assumption on a runtime path**, which §9 does not
    // discuss — §9 is about deserialising `Step`. Every run document written by
    // the original service carries the map form, because `set_children_number`
    // writes `state.{step}.num_children.{child_index}`; and nothing in this
    // workspace yet writes the scalar, because the run-document writer lives in
    // `pneuma-intake`, which the design notes still gates. So against a
    // pre-existing document this returns `Malformed` *after* the `$addToSet`
    // has landed. That is correct under the drain and untestable against real
    // data until that writer exists — recorded here so it is a known
    // precondition rather than a surprise.
    let declared = step.get("num_children").and_then(number_of);
    let Some(declared) = declared else {
        return Err(malformed(format!("{step_id}.num_children")));
    };

    // A skip set that exists but is not an array must not read as "no skips":
    // that returns the full count and the aggregator waits for children that
    // will never arrive. `read_arrival` refuses the same shape; so does this.
    let skipped = match step.get("skipped_children") {
        None => 0,
        Some(value) => {
            let Some(slots) = value.as_document() else {
                return Err(malformed(format!("{step_id}.skipped_children")));
            };
            match slots.get(slot.to_string()) {
                None => 0,
                Some(entry) => {
                    let Some(members) = entry.as_array() else {
                        return Err(malformed(format!("{step_id}.skipped_children.{slot}")));
                    };
                    members.len()
                }
            }
        }
    };

    Ok(declared - i64::try_from(skipped).unwrap_or(i64::MAX))
}

/// A BSON number of any width, as `i64`.
fn number_of(value: &mongodb::bson::Bson) -> Option<i64> {
    value
        .as_i32()
        .map(i64::from)
        .or_else(|| value.as_i64())
        .or_else(|| value.as_f64().map(|double| double as i64))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key() -> RefPath {
        match RefPath::new("run1.parent") {
            Ok(path) => path,
            Err(error) => panic!("should encode: {error}"),
        }
    }

    fn step() -> StepId {
        match StepId::new("agg") {
            Ok(id) => id,
            Err(error) => panic!("should build: {error}"),
        }
    }

    #[test]
    fn completion_needs_both_the_width_and_this_call_having_added_it() {
        let completing = Arrival {
            arrived: vec!["a".to_owned(), "b".to_owned()],
            newly_arrived: true,
        };
        assert!(completing.completes(2));
        assert_eq!(completing.count(), 2);

        // The redelivery case. Same full set, but this call added nothing, so
        // it must not fire the aggregation a second time.
        let redelivered = Arrival {
            arrived: vec!["a".to_owned(), "b".to_owned()],
            newly_arrived: false,
        };
        assert!(
            !redelivered.completes(2),
            "a redelivered arrival must not complete an already-full barrier"
        );
        assert!(!completing.completes(3));
    }

    #[test]
    fn the_first_arrival_sees_an_absent_key_as_an_empty_set() {
        // Absence is normal on the before-document: it is the first arrival.
        let document = doc! {};
        let Ok(arrival) = read_arrival(&document, "refcounts", &key(), "child") else {
            panic!("an absent key is the first arrival, not an error");
        };
        assert_eq!(arrival.arrived, vec!["child".to_owned()]);
        assert!(arrival.newly_arrived);
        assert!(arrival.completes(1));
    }

    #[test]
    fn an_arrival_appends_itself_and_a_repeat_does_not() {
        let document = doc! { "refcounts": { "run1/parent": ["a"] } };
        let Ok(fresh) = read_arrival(&document, "refcounts", &key(), "b") else {
            panic!("should read");
        };
        assert_eq!(fresh.arrived, vec!["a".to_owned(), "b".to_owned()]);
        assert!(fresh.newly_arrived);

        let Ok(repeat) = read_arrival(&document, "refcounts", &key(), "a") else {
            panic!("should read");
        };
        assert_eq!(repeat.arrived, vec!["a".to_owned()]);
        assert!(!repeat.newly_arrived);
    }

    #[test]
    fn an_entry_that_is_not_an_array_is_an_error_not_an_empty_set() {
        // Reporting it as empty would let a barrier of zero complete at once.
        for broken in [
            doc! { "refcounts": { "run1/parent": "not an array" } },
            doc! { "refcounts": { "run1/parent": 7 } },
        ] {
            let Err(BarrierError::Malformed { map, key: which }) =
                read_arrival(&broken, "refcounts", &key(), "c")
            else {
                panic!("{broken:?} should be rejected");
            };
            assert_eq!(map, "refcounts");
            assert_eq!(which, "run1/parent");
        }
    }

    #[test]
    fn num_children_is_read_as_a_scalar_and_the_map_form_is_refused() {
        // The design notes: the map form is deliberately not accepted, because
        // the cutover is a drain and the only writer of this field is this
        // port, which serialises a scalar. An earlier version accepted both and
        // justified it by "coexistence", contradicting §9.
        let scalar = doc! { "state": { "agg": { "num_children": 5_i32 } } };
        assert!(matches!(remaining_children(&scalar, &step(), 1), Ok(5)));

        let scalar_with_skips = doc! { "state": { "agg": {
            "num_children": 5_i32,
            "skipped_children": { "1": ["x"] }
        } } };
        assert!(matches!(
            remaining_children(&scalar_with_skips, &step(), 1),
            Ok(4)
        ));

        // The map form is an error, not a silent misread.
        let map = doc! { "state": { "agg": { "num_children": { "1": 5_i32 } } } };
        assert!(matches!(
            remaining_children(&map, &step(), 1),
            Err(BarrierError::Malformed { .. })
        ));

        // A skip map with nothing for this slot is no skips, not an error, and
        // another slot's skips are not deducted from this one.
        let other_slot = doc! { "state": { "agg": {
            "num_children": 9_i32,
            "skipped_children": { "2": ["x", "y"] }
        } } };
        assert!(matches!(remaining_children(&other_slot, &step(), 1), Ok(9)));
        assert!(matches!(remaining_children(&other_slot, &step(), 2), Ok(7)));
    }

    #[test]
    fn a_malformed_skip_set_is_an_error_not_zero_skips() {
        // Reading it as "no skips" returns the full count and the aggregator
        // waits for children that will never arrive -- erring toward a hang.
        for broken in [
            doc! { "state": { "agg": {
            "num_children": 5_i32, "skipped_children": "not a document" } } },
            doc! { "state": { "agg": {
            "num_children": 5_i32, "skipped_children": { "1": "not an array" } } } },
        ] {
            assert!(
                matches!(
                    remaining_children(&broken, &step(), 1),
                    Err(BarrierError::Malformed { .. })
                ),
                "{broken:?} should be rejected"
            );
        }
    }

    #[test]
    fn a_barrier_map_that_is_not_a_document_is_an_error() {
        // Swallowing this would give arrived=[member], newly_arrived=true, and
        // completes(1) -- a barrier of one firing on a malformed document.
        let broken = doc! { "refcounts": "not a document" };
        assert!(matches!(
            read_arrival(&broken, "refcounts", &key(), "c"),
            Err(BarrierError::Malformed { .. })
        ));
    }

    #[test]
    fn remaining_children_is_the_declared_count_less_the_skips() {
        let none_skipped = doc! { "state": { "agg": { "num_children": 5_i32 } } };
        assert!(matches!(
            remaining_children(&none_skipped, &step(), 1),
            Ok(5)
        ));

        let two_skipped = doc! { "state": { "agg": {
            "num_children": 5_i32,
            "skipped_children": { "1": ["x", "y"] }
        } } };
        assert!(matches!(
            remaining_children(&two_skipped, &step(), 1),
            Ok(3)
        ));

        // A repeated skip is already deduplicated by $addToSet, so the derived
        // count cannot drift the way a repeated $inc would.
        let repeated = doc! { "state": { "agg": {
            "num_children": 5_i32,
            "skipped_children": { "1": ["x"] }
        } } };
        assert!(matches!(remaining_children(&repeated, &step(), 1), Ok(4)));
    }

    #[test]
    fn a_count_of_any_bson_numeric_width_is_accepted() {
        // $inc and a restore from an export leave different types; refusing one
        // after the write landed would leave a caller unable to retry safely.
        for value in [
            mongodb::bson::Bson::Int32(4),
            mongodb::bson::Bson::Int64(4),
            mongodb::bson::Bson::Double(4.0),
        ] {
            let document = doc! { "state": { "agg": { "num_children": value.clone() } } };
            assert!(
                matches!(remaining_children(&document, &step(), 1), Ok(4)),
                "{value:?} should read as 4"
            );
        }
    }

    #[test]
    fn a_malformed_state_is_an_error() {
        for broken in [
            doc! {},
            doc! { "state": {} },
            doc! { "state": { "agg": {} } },
            doc! { "state": { "agg": { "num_children": {} } } },
            doc! { "state": { "agg": { "num_children": "five" } } },
        ] {
            assert!(
                matches!(
                    remaining_children(&broken, &step(), 1),
                    Err(BarrierError::Malformed { .. })
                ),
                "{broken:?} should be rejected"
            );
        }
    }

    #[test]
    fn the_errors_name_what_is_wrong() {
        assert_eq!(
            BarrierError::RunNotFound {
                run_id: "r".to_owned()
            }
            .to_string(),
            r#"no run "r""#
        );
        assert_eq!(
            BarrierError::Malformed {
                map: "refcounts".to_owned(),
                key: "p".to_owned(),
            }
            .to_string(),
            "run document field refcounts.p is not the shape a barrier needs"
        );
    }
}
