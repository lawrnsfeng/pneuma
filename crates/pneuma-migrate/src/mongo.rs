//! The Mongo half of the schema, which today lives inside a Postgres migration.
//!
//! The original creates the `runs.run_id` index from inside a migration-tool
//! revision, by opening a driver client mid-migration. The original plan calls
//! that out and gives it an honest home here, which removes three problems at
//! once:
//!
//! - Mongo does not participate in the SQL transaction. That revision runs
//!   `op.create_index` on `noderun` first and the Mongo call second, so a
//!   failure after the Mongo write rolls back Postgres and leaves the index --
//!   the two stores then disagree about which migration has run.
//! - It opens a `MongoClient` per call and never closes it.
//! - It reads `os.environ["PNEUMA_MONGODB_URL"]` directly, so a deployment that runs
//!   the Postgres migrations without Mongo configured fails the *Postgres*
//!   migration with a `KeyError`.
//!
//! # The index is created non-unique, which is a faithful port and not an
//! endorsement
//!
//! The revision's own comment reads `this index should be unique, however we
//! have retry`. The defect notes works through that: duplicates are
//! real, but they arrive by broker redelivery rather than the retry the comment
//! names, and the storage layer already maps the conflict -- so the index
//! *should* be unique and cannot be made so until a `ConflictError` is caught
//! in the running service.
//!
//! Rehoming is not the moment to change production semantics. This creates
//! exactly the index that exists today. [`duplicate_run_ids`] is provided so the
//! blast radius can be measured first, because a unique index cannot be built
//! over a collection that already holds duplicates and each duplicate is two
//! divergent run documents where neither is authoritative.

use futures_util::TryStreamExt;
use mongodb::bson::{doc, Bson, Document};
use mongodb::options::IndexOptions;
use mongodb::{Collection, IndexModel};

/// The field the index covers.
pub const RUN_ID: &str = "run_id";

/// The index's name, which is load-bearing rather than cosmetic.
///
/// Measured on `mongo:5.0.28`: creating the same key *without* a name fails
/// with `Index already exists with a different name: …`, because Mongo would
/// auto-generate `run_id_1`. Creating it *with* the name is a no-op. Passing
/// the name is therefore what makes this function idempotent.
///
/// # The rename, and the two ways of getting it wrong
///
/// the original migration tool called it `idx_run_id`. `ix_runs_run_id` says the same thing in the
/// convention the Postgres half uses (`ix_node_run_run_id`), so the two stores
/// read alike.
///
/// It is **not** called `run_id_unique`, which was the first choice and was a
/// lie twice over. This index is deliberately *not* unique — see the module
/// header, and the defect notes for why it cannot become so until the
/// running service catches the conflict — so a name asserting uniqueness would
/// have an operator drop the old index believing they had kept the guarantee,
/// which is §25's forked-run defect reintroduced under a name that hides it.
///
/// And the same measurement that makes the name load-bearing makes a rename
/// impossible to do by creating alongside: Mongo refuses a second index over
/// the same key under a different name with `IndexOptionsConflict`, so on any
/// original-era database that would be an error rather than the no-op this
/// function promises. [`ensure_run_id_index`] drops the old one first.
pub const RUN_ID_INDEX: &str = "ix_runs_run_id";

/// Why the Mongo half could not be applied.
#[derive(Debug, thiserror::Error)]
pub enum MongoError {
    /// The database refused, or was unreachable.
    #[error("mongo error: {0}")]
    Database(#[from] mongodb::error::Error),

    /// A grouped document came back without the fields the pipeline projects.
    ///
    /// `$group` always emits `_id`, and `$sum` always emits a number, so this
    /// is not reachable from the pipeline below -- only from editing it into
    /// disagreement with the decoding it feeds. That is drift worth failing on
    /// rather than reporting as "no duplicates".
    ///
    /// A *null* or non-string `run_id` is emphatically **not** this case. Those
    /// are real duplicates and are reported as [`RunKey::Null`] and
    /// [`RunKey::Other`]; treating them as malformed used to abort the whole
    /// report, including the ordinary duplicates found alongside them.
    #[error("a grouped run document was missing {field}")]
    Malformed {
        /// Which field.
        field: &'static str,
    },
}

/// Creates the `run_id` index if it is not already there.
///
/// Idempotent: re-running is a no-op rather than an error, so this is safe to
/// run on every deploy the way a migration is.
///
/// # It also performs the rename, because nothing else can
///
/// Mongo has no migration runner and no `ALTER INDEX`. An index is renamed by
/// dropping it and building it again, and this is the only place that knows
/// both names — so leaving it to the runbook would mean an operator doing by
/// hand the one step that has an ordering hazard.
///
/// The old index is dropped **before** the new one is built, which is the
/// opposite of what a unique index would want. It is safe here precisely
/// because this index is not unique: the window between the two enforces
/// nothing, so it costs a collection scan on a concurrent query and no
/// correctness. If §25 is ever acted on and this becomes unique, this order
/// has to change with it.
pub async fn ensure_run_id_index(runs: &Collection<Document>) -> Result<(), MongoError> {
    for stale in stale_names(runs).await? {
        runs.drop_index(stale).await?;
    }
    let model = IndexModel::builder()
        .keys(doc! { RUN_ID: 1 })
        .options(
            IndexOptions::builder()
                .name(RUN_ID_INDEX.to_owned())
                // Stated rather than left to the default, so the choice is
                // visible at the point it is made. See the module header: this
                // is what production has, and §25 is the argument for changing
                // it once the service can handle the conflict.
                .unique(false)
                .build(),
        )
        .build();
    runs.create_index(model).await?;
    Ok(())
}

/// Indexes over `run_id` under any name but [`RUN_ID_INDEX`].
///
/// In practice this is `idx_run_id` on an original-era database and nothing at
/// all everywhere else. Matched on the *key*, not on the old name: an index
/// Mongo auto-named `run_id_1` — which is what happens when `createIndex` is
/// called without a name — conflicts exactly the same way, and hard-coding the
/// one name we know would leave that case failing.
async fn stale_names(runs: &Collection<Document>) -> Result<Vec<String>, MongoError> {
    let key = doc! { RUN_ID: 1 };
    // A collection nothing has written to does not exist yet, and Mongo answers
    // `list_indexes` on it with `NamespaceNotFound` rather than an empty list.
    // It has no stale index by definition -- and `create_index` below creates
    // the collection as a side effect, which is why this was not reachable
    // before this function existed.
    //
    // `other?` rather than a second `Err` arm: every other failure is propagated
    // on the line the success case also takes, so there is no arm here that only
    // a broken connection can reach.
    let mut cursor = match runs.list_indexes().await {
        Err(error) if is_missing_namespace(&error) => return Ok(Vec::new()),
        other => other?,
    };
    let mut stale = Vec::new();
    // Not `while let Ok(Some(..))`: that reads a cursor *error* as the end of
    // the stream, and this function would then report "nothing stale" for a
    // dropped connection -- after which `create_index` fails with the conflict
    // this exists to prevent, blaming the wrong thing.
    while let Some(model) = cursor.try_next().await? {
        if model.keys != key {
            continue;
        }
        let name = model.options.and_then(|options| options.name);
        match name {
            Some(name) if name != RUN_ID_INDEX => stale.push(name),
            // The index is already the one we want, or Mongo did not report a
            // name for it. There is nothing to drop either way, and dropping an
            // index we cannot name is not something to guess at.
            _ => {}
        }
    }
    Ok(stale)
}

/// Whether an error is Mongo's "that collection does not exist".
///
/// Matched on the code rather than the message: 26 is `NamespaceNotFound`, and
/// the text around it is a server version's to change.
fn is_missing_namespace(error: &mongodb::error::Error) -> bool {
    const NAMESPACE_NOT_FOUND: i32 = 26;
    matches!(
        *error.kind,
        mongodb::error::ErrorKind::Command(ref command) if command.code == NAMESPACE_NOT_FOUND
    )
}

/// The value several documents share.
///
/// Not a `String`, because the values that collide are not all strings and the
/// two that are not are the ones most worth seeing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RunKey {
    /// An ordinary string `run_id`.
    Id(String),
    /// Absent, or explicitly `null`.
    ///
    /// A unique index does not ignore these: it treats a missing key as the
    /// value `null`, so two documents without a `run_id` collide with each
    /// other. Measured -- building one over such a pair fails with
    /// `DuplicateKey` and `keyValue: {"run_id": null}`. They are therefore
    /// duplicates the §25 measurement must report, and reporting them as an
    /// error used to abort the pass that exists to find them.
    Null,
    /// Present, but not a string -- an `ObjectId` or an integer in a legacy
    /// document. Rendered, because the point is to be shown to an operator.
    Other(String),
}

/// One `run_id` value held by more than one document.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Duplicate {
    /// The shared value.
    pub run_id: RunKey,
    /// How many documents carry it.
    pub count: i64,
}

/// Reports every `run_id` held by more than one document.
///
/// The measurement the defect notes asks for before anyone makes the
/// index unique. An empty result is the answer that permits it; a non-empty one
/// is a list of runs whose state has already forked, each needing a decision
/// rather than a blind delete.
///
/// Sorted by descending count then by id, so two runs of this against an
/// unchanging collection produce the same report -- `$group` does not promise
/// an order, and a report that reshuffles cannot be diffed between runs.
pub async fn duplicate_run_ids(runs: &Collection<Document>) -> Result<Vec<Duplicate>, MongoError> {
    let pipeline = vec![
        doc! { "$group": { "_id": format!("${RUN_ID}"), "n": { "$sum": 1 } } },
        doc! { "$match": { "n": { "$gt": 1 } } },
        doc! { "$sort": { "n": -1, "_id": 1 } },
    ];
    // `allow_disk_use` because this is the one function meant to be pointed at
    // production. Before MongoDB 6.0 introduced `allowDiskUseByDefault`,
    // `$group` and `$sort` are capped at 100 MB of in-memory state, so exactly
    // the large `runs` collection whose duplicate count matters most would fail
    // with `QueryExceededMemoryLimitNoDiskUseAllowed` instead of answering.
    let mut cursor = runs.aggregate(pipeline).allow_disk_use(true).await?;
    let mut found = Vec::new();
    while let Some(document) = cursor.try_next().await? {
        found.push(decode(&document)?);
    }
    Ok(found)
}

/// Reads one grouped document into a [`Duplicate`].
///
/// Separate from the cursor loop so the malformed cases have tests. Reaching
/// them through `aggregate` would mean a server that returns documents the
/// pipeline above cannot produce; as a function they are four lines and a
/// hand-built `Document`.
fn decode(document: &Document) -> Result<Duplicate, MongoError> {
    let Some(raw) = document.get("_id") else {
        return Err(MongoError::Malformed { field: "_id" });
    };
    let run_id = match raw {
        Bson::String(id) => RunKey::Id(id.clone()),
        // `$group` folds every document with no `run_id`, and every explicit
        // null, into this one group.
        Bson::Null => RunKey::Null,
        other => RunKey::Other(other.to_string()),
    };
    // `$sum: 1` yields an int32 while the count is small and an int64 once it
    // is not, so both are accepted rather than assuming one and reporting the
    // other as malformed.
    let count = match (document.get_i32("n"), document.get_i64("n")) {
        (Ok(count), _) => i64::from(count),
        (Err(_), Ok(count)) => count,
        (Err(_), Err(_)) => return Err(MongoError::Malformed { field: "n" }),
    };
    Ok(Duplicate { run_id, count })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_grouped_document_decodes_from_either_integer_width() {
        // `$sum` changes width with the count, so both are the normal case.
        let Ok(small) = decode(&doc! { "_id": "a", "n": 2_i32 }) else {
            panic!("int32 is what a small collection produces");
        };
        assert_eq!(small.count, 2);
        assert_eq!(small.run_id, RunKey::Id("a".to_owned()));

        let Ok(large) = decode(&doc! { "_id": "b", "n": 5_000_000_000_i64 }) else {
            panic!("int64 is what a large one produces");
        };
        assert_eq!(large.count, 5_000_000_000);
    }

    #[test]
    fn a_null_group_is_a_duplicate_and_not_an_error() {
        // The case that used to abort the entire report. `$group` folds every
        // document lacking `run_id` into `_id: null`, and a unique index
        // collides on exactly those -- so they are the duplicates the
        // measurement most needs to show, not a reason to stop.
        let Ok(decoded) = decode(&doc! { "_id": Bson::Null, "n": 2_i32 }) else {
            panic!("a null group is a duplicate");
        };
        assert_eq!(decoded.run_id, RunKey::Null);
        assert_eq!(decoded.count, 2);
    }

    #[test]
    fn a_non_string_group_is_reported_rendered_rather_than_refused() {
        // Legacy documents with an integer or ObjectId `run_id`. Still a
        // collision under a unique index, so still worth showing.
        let Ok(decoded) = decode(&doc! { "_id": 7_i32, "n": 2_i32 }) else {
            panic!("a non-string key is still a key");
        };
        assert_eq!(decoded.run_id, RunKey::Other("7".to_owned()));
    }

    #[test]
    fn a_group_with_no_id_at_all_is_malformed() {
        // Not reachable from the pipeline -- `$group` always emits `_id` -- but
        // reachable by editing the pipeline out of agreement with `decode`,
        // which is the drift worth failing on.
        let Err(MongoError::Malformed { field }) = decode(&doc! { "n": 2_i32 }) else {
            panic!("should be malformed");
        };
        assert_eq!(field, "_id");
    }

    #[test]
    fn a_document_whose_count_is_not_an_integer_is_malformed() {
        let Err(MongoError::Malformed { field }) = decode(&doc! { "_id": "a", "n": "two" }) else {
            panic!("a string count is not a count");
        };
        assert_eq!(field, "n");
        assert!(MongoError::Malformed { field: "n" }
            .to_string()
            .contains("missing n"));
    }
}
