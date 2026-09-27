//! The async repository over [`crate::queries`].
//!
//! Thin by design: each method binds parameters, runs one statement, and maps
//! rows through [`NodeRun::try_from`]. The decisions live in the SQL, which is
//! reviewable by someone who reads SQL and is checked against a real server by
//! `tests/schema.rs`.

use chrono::{DateTime, Utc};
use pneuma_core::child_index::ChildIndex;
use pneuma_core::sibling_index::SiblingIndex;
use pneuma_core::status::NodeStatus;
use serde_json::Value;
use sqlx::PgPool;
use uuid::Uuid;

use crate::node_run::{NodeRun, NodeRunRow, RowError};
use crate::queries;

/// Anything that can go wrong talking to Postgres.
#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    /// The database refused, or was unreachable.
    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),
    /// A row came back that the domain will not accept.
    #[error(transparent)]
    Row(#[from] RowError),
    /// An index does not fit the `INTEGER` column.
    #[error("{column} is {index}, which does not fit the INTEGER column")]
    IndexTooLarge {
        /// Which column.
        column: &'static str,
        /// The value.
        index: u32,
    },
    /// An insert conflicted, and the conflicting row could not then be read.
    #[error("create for path {path:?} conflicted, but no row with that path could be read back")]
    CreateVanished {
        /// The path that was being created.
        path: String,
    },
}

/// The columns a new node run supplies. The rest are set by the statement:
/// `created_at` and `updated_at` are `NOW()`, and the timing and error columns
/// start empty.
#[derive(Debug, Clone)]
pub struct NewNodeRun {
    /// Primary key. Chosen by the caller so a retry can reuse it.
    pub id: Uuid,
    /// The coordination key. Unique.
    pub path: String,
    /// The node's id within its pipeline definition.
    pub node_id: String,
    /// Human-readable name.
    pub name: String,
    /// Which kind of node.
    pub kind: pneuma_core::node::NodeKind,
    /// The pipeline this run belongs to.
    pub pipeline_id: String,
    /// The run this node belongs to.
    pub run_id: String,
    /// Parent node's id.
    pub parent_id: Option<String>,
    /// Parent node's path. Must already exist: it is a foreign key.
    pub parent_path: Option<String>,
    /// Parent node's kind, stored as text.
    pub parent_kind: Option<String>,
    /// Runtime fan-out index.
    ///
    /// The domain type, not a raw `i32`, because the read path rejects `0` and
    /// negatives and `convert` fails a whole batch on the first bad row — so a
    /// single bad write would make every subsequent read of that run return an
    /// error and no rows. Accepting only what can be read back closes that.
    pub child_index: Option<ChildIndex>,
    /// Static position among the parent's components. Same reasoning.
    pub sibling_index: Option<SiblingIndex>,
    /// Starting status.
    pub status: NodeStatus,
    /// The node's input.
    pub step_input: Option<Value>,
    /// The node's output, usually absent at creation.
    pub step_output: Option<Value>,
}

/// Reads and writes for the `node_run` and `node_run_history` tables.
#[derive(Debug, Clone)]
pub struct NodeRunStore {
    pool: PgPool,
}

/// Narrows a `u32` index to the `INTEGER` column, refusing rather than clamping.
///
/// Unreachable in practice — indices come from enumerating a fan-out list — but
/// `unwrap_or(i32::MAX)` would persist two distinct children as the same value
/// and the row would read back cleanly, so nothing downstream could tell. That
/// is the silent-coercion pattern `ChildIndex` exists to remove; reintroducing it
/// on the write path would be the same defect one layer down.
fn narrow_index(value: Option<u32>, column: &'static str) -> Result<Option<i32>, StoreError> {
    value
        .map(|index| i32::try_from(index).map_err(|_| StoreError::IndexTooLarge { column, index }))
        .transpose()
}

/// Decides what a conflicting insert means once the row has been looked up.
///
/// The original raises rather than returning nothing,
/// and this keeps that: "created
/// nothing, no error" is the wrong answer on the retry path `create` exists
/// for.
///
/// **How narrow the failing case is, measured rather than assumed.** It is
/// tempting to say a concurrent *uncommitted* insert of the same path produces
/// it — `ON CONFLICT DO NOTHING` returns no row, and the lookup, on a different
/// pooled connection, cannot see an uncommitted row either. That is wrong:
/// `ON CONFLICT DO NOTHING` **waits** for the conflicting transaction. Timed on
/// `postgres:16` against a holder sleeping eight seconds, the insert returned
/// after 6128 ms — it blocked. So by the time it returns, the other transaction
/// has either committed (and the lookup finds the row) or rolled back (and the
/// insert succeeded).
///
/// What remains is a committed `DELETE` landing between the two statements —
/// real, but narrow, and not reproducible without a hook in the middle. Hence
/// this function: the arm is right, and separating it is how it gets tested.
fn existing_or_vanished(found: Option<NodeRun>, path: &str) -> Result<NodeRun, StoreError> {
    found.ok_or_else(|| StoreError::CreateVanished {
        path: path.to_owned(),
    })
}

/// Maps a batch of rows, failing on the first the domain refuses.
fn convert(rows: Vec<NodeRunRow>) -> Result<Vec<NodeRun>, StoreError> {
    rows.into_iter()
        .map(|row| NodeRun::try_from(row).map_err(StoreError::from))
        .collect()
}

impl NodeRunStore {
    /// Wraps an existing pool. Connecting is the caller's business.
    pub fn new(pool: PgPool) -> Self {
        NodeRunStore { pool }
    }

    /// Inserts a node run, or returns the existing one with that path.
    ///
    /// Never returns "nothing happened": if the insert conflicts *and* the row
    /// cannot then be found, that is [`StoreError::CreateVanished`] rather than
    /// a silent absence. See the comment on that arm for when it can happen.
    ///
    /// The statement is `ON CONFLICT (path) DO NOTHING`, so a redelivery
    /// returns no row; this then looks the existing row up, matching the
    /// original's explicit "create returned None, need to query for path"
    /// fallback. Retry on this path
    /// is a documented condition.
    pub async fn create(&self, new: &NewNodeRun) -> Result<NodeRun, StoreError> {
        let inserted: Option<NodeRunRow> = sqlx::query_as(queries::CREATE)
            .bind(new.id)
            .bind(&new.path)
            .bind(&new.node_id)
            .bind(&new.name)
            .bind(new.kind)
            .bind(&new.pipeline_id)
            .bind(&new.run_id)
            .bind(&new.parent_id)
            .bind(&new.parent_path)
            .bind(&new.parent_kind)
            .bind(narrow_index(
                new.child_index.map(ChildIndex::get),
                "child_index",
            )?)
            .bind(narrow_index(
                new.sibling_index.map(SiblingIndex::get),
                "sibling_index",
            )?)
            .bind(new.status)
            .bind(&new.step_input)
            .bind(&new.step_output)
            .fetch_optional(&self.pool)
            .await?;

        match inserted {
            Some(row) => Ok(NodeRun::try_from(row)?),
            None => existing_or_vanished(self.get_by_path(&new.path).await?, &new.path),
        }
    }

    /// One node run by its unique path.
    pub async fn get_by_path(&self, path: &str) -> Result<Option<NodeRun>, StoreError> {
        let row: Option<NodeRunRow> = sqlx::query_as(queries::GET_BY_PATH)
            .bind(path)
            .fetch_optional(&self.pool)
            .await?;
        row.map(NodeRun::try_from).transpose().map_err(Into::into)
    }

    /// A parent's children, ordered by fan-out index.
    ///
    /// Both filters are optional and both are real: the aggregation path passes
    /// `node_ids = target_children`, and without it an aggregator collects
    /// children outside its target set.
    pub async fn get_by_parent_path(
        &self,
        parent_path: &str,
        node_id: Option<&str>,
        node_ids: Option<&[String]>,
    ) -> Result<Vec<NodeRun>, StoreError> {
        let rows: Vec<NodeRunRow> = sqlx::query_as(queries::GET_BY_PARENT_PATH)
            .bind(parent_path)
            .bind(node_id)
            .bind(node_ids)
            .fetch_all(&self.pool)
            .await?;
        convert(rows)
    }

    /// Many node runs by path.
    pub async fn get_by_paths(&self, paths: &[String]) -> Result<Vec<NodeRun>, StoreError> {
        let rows: Vec<NodeRunRow> = sqlx::query_as(queries::GET_BY_PATHS)
            .bind(paths)
            .fetch_all(&self.pool)
            .await?;
        convert(rows)
    }

    /// Every node run of one run.
    pub async fn get_by_run_id(&self, run_id: &str) -> Result<Vec<NodeRun>, StoreError> {
        let rows: Vec<NodeRunRow> = sqlx::query_as(queries::GET_BY_RUN_ID)
            .bind(run_id)
            .fetch_all(&self.pool)
            .await?;
        convert(rows)
    }

    /// The newest `updated_at` per run.
    ///
    /// Returns pairs rather than a map so the caller chooses the collection.
    /// An empty input is answered without a query, matching the original's
    /// early return.
    pub async fn max_updated_at_by_run_ids(
        &self,
        run_ids: &[String],
    ) -> Result<Vec<(String, DateTime<Utc>)>, StoreError> {
        if run_ids.is_empty() {
            return Ok(Vec::new());
        }
        let rows = sqlx::query_as(queries::MAX_UPDATED_AT_BY_RUN_IDS)
            .bind(run_ids)
            .fetch_all(&self.pool)
            .await?;
        Ok(rows)
    }

    /// Deletes every node run of the given runs. Returns how many went.
    pub async fn delete_by_run_ids(&self, run_ids: &[String]) -> Result<u64, StoreError> {
        if run_ids.is_empty() {
            return Ok(0);
        }
        let done = sqlx::query(queries::DELETE_BY_RUN_IDS)
            .bind(run_ids)
            .execute(&self.pool)
            .await?;
        Ok(done.rows_affected())
    }

    /// Runs with a non-finalised node run older than `threshold`.
    pub async fn stale_inprogress_run_ids(
        &self,
        threshold: DateTime<Utc>,
        limit: i64,
    ) -> Result<Vec<String>, StoreError> {
        let rows = sqlx::query_scalar(queries::STALE_INPROGRESS_RUN_IDS)
            .bind(threshold)
            .bind(limit)
            .fetch_all(&self.pool)
            .await?;
        Ok(rows)
    }

    /// Moves a node to a new status.
    ///
    /// `Ok(None)` collapses two outcomes the original distinguishes: the guard
    /// refused (already terminal, or `FORKED` from something other than
    /// `CREATED`), or there is no such path. original raises `ValueError` for the
    /// missing path and returns the unchanged row for a refusal.
    /// Nothing here does that
    /// lookup — a caller that needs to tell them apart must call
    /// [`Self::get_by_path`] itself.
    ///
    /// **`error_message` is only written when `error_code` is `Some`.** The
    /// statement gates both on `$3`, matching the original, so
    /// `update_status(path, Error, None, Some("boom"))` silently discards the
    /// message. Pass them together or not at all.
    pub async fn update_status(
        &self,
        path: &str,
        status: NodeStatus,
        error_code: Option<&str>,
        error_message: Option<&str>,
    ) -> Result<Option<NodeRun>, StoreError> {
        let row: Option<NodeRunRow> = sqlx::query_as(queries::UPDATE_STATUS)
            .bind(path)
            .bind(status)
            .bind(error_code)
            .bind(error_message)
            .fetch_optional(&self.pool)
            .await?;
        row.map(NodeRun::try_from).transpose().map_err(Into::into)
    }

    /// Records a node's output alongside its new status.
    ///
    /// `Ok(None)` collapses the same two outcomes as [`Self::update_status`] —
    /// the guard refused, or there is no such path — and nothing here
    /// distinguishes them. The original separates them more sharply still: it
    /// raises `NodeRunNotFoundError` for a missing path and returns
    /// `(row, false)` for a skip.
    ///
    /// **The guard here is wider than the original's**, which refuses only
    /// `FINISHED` at one call site and nothing at all at the other. That is a
    /// deliberate deviation — see the design notes — and it means a result
    /// arriving for a node already marked `TIMED_OUT` is dropped rather than
    /// recorded.
    pub async fn record_output(
        &self,
        path: &str,
        status: NodeStatus,
        step_output: Option<&Value>,
    ) -> Result<Option<NodeRun>, StoreError> {
        let row: Option<NodeRunRow> = sqlx::query_as(queries::RECORD_OUTPUT)
            .bind(path)
            .bind(status)
            .bind(step_output)
            .fetch_optional(&self.pool)
            .await?;
        row.map(NodeRun::try_from).transpose().map_err(Into::into)
    }

    /// Copies the given runs' node runs into history. Idempotent.
    pub async fn backup_to_history(&self, run_ids: &[String]) -> Result<u64, StoreError> {
        if run_ids.is_empty() {
            return Ok(0);
        }
        let done = sqlx::query(queries::HISTORY_BACKUP)
            .bind(run_ids)
            .execute(&self.pool)
            .await?;
        Ok(done.rows_affected())
    }

    /// How many rows [`NodeRunStore::delete_outdated_history`] would remove.
    ///
    /// A separate statement, because SQL cannot share a predicate between two
    /// without a view. `counting_outdated_history_agrees_with_deleting_it`
    /// is what keeps the two `WHERE` clauses honest — a preview that has
    /// drifted from the action is worse than none, since it reports
    /// confidently about a decision the real code does not make.
    pub async fn count_outdated_history(&self, cutoff: DateTime<Utc>) -> Result<u64, StoreError> {
        let count: i64 = sqlx::query_scalar(queries::HISTORY_COUNT_OUTDATED)
            .bind(cutoff)
            .fetch_one(&self.pool)
            .await?;
        Ok(count.max(0).unsigned_abs())
    }

    /// Deletes history older than `cutoff`.
    ///
    /// The cutoff is an aware instant. That is the honest type for it, not a
    /// safeguard: the defect notes claimed a naive one would be read
    /// in the session's timezone and is retracted — a bound parameter against a
    /// `TIMESTAMPTZ` column is typed from the column and compared absolutely.
    pub async fn delete_outdated_history(&self, cutoff: DateTime<Utc>) -> Result<u64, StoreError> {
        let done = sqlx::query(queries::HISTORY_DELETE_OUTDATED)
            .bind(cutoff)
            .execute(&self.pool)
            .await?;
        Ok(done.rows_affected())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_index_too_large_for_the_column_is_refused_not_clamped() {
        // StoreError cannot derive PartialEq -- sqlx::Error does not -- so
        // these match on the shape rather than comparing values.
        assert!(matches!(narrow_index(None, "child_index"), Ok(None)));
        assert!(matches!(narrow_index(Some(7), "child_index"), Ok(Some(7))));
        let ceiling = u32::try_from(i32::MAX).unwrap_or(u32::MAX);
        assert!(matches!(
            narrow_index(Some(ceiling), "sibling_index"),
            Ok(Some(_))
        ));
        // Clamping would persist this as i32::MAX, indistinguishable from a
        // genuine i32::MAX index.
        let too_big = ceiling + 1;
        let Err(StoreError::IndexTooLarge { column, index }) =
            narrow_index(Some(too_big), "child_index")
        else {
            panic!("a value past the column's range must be refused");
        };
        assert_eq!(column, "child_index");
        assert_eq!(index, too_big);
        assert_eq!(
            StoreError::IndexTooLarge {
                column: "sibling_index",
                index: 5,
            }
            .to_string(),
            "sibling_index is 5, which does not fit the INTEGER column"
        );
    }

    #[test]
    fn a_conflicting_insert_whose_row_cannot_be_read_back_is_an_error() {
        // Not `Ok(None)`: that would report "created nothing, no error" on the
        // retry path `create` exists for. The original raises here too.
        let Err(StoreError::CreateVanished { path }) = existing_or_vanished(None, "run1.a") else {
            panic!("a missing row after a conflict must be an error");
        };
        assert_eq!(path, "run1.a");
        assert_eq!(
            StoreError::CreateVanished {
                path: "s".to_owned()
            }
            .to_string(),
            r#"create for path "s" conflicted, but no row with that path could be read back"#
        );
    }

    #[test]
    fn a_conflicting_insert_whose_row_is_found_returns_it() {
        let row = NodeRunRow {
            id: Uuid::nil(),
            path: "run1.a".to_owned(),
            node_id: "a".to_owned(),
            name: "a".to_owned(),
            kind: pneuma_core::node::NodeKind::Model,
            pipeline_id: "p".to_owned(),
            run_id: "r".to_owned(),
            parent_id: None,
            parent_path: None,
            parent_kind: None,
            child_index: None,
            sibling_index: None,
            status: NodeStatus::Created,
            step_input: None,
            step_output: None,
            error_code: None,
            error_message: None,
            extra: None,
            created_at: DateTime::<Utc>::UNIX_EPOCH,
            updated_at: DateTime::<Utc>::UNIX_EPOCH,
            started_at: None,
            finished_at: None,
            is_deleted: None,
        };
        let Ok(existing) = NodeRun::try_from(row) else {
            panic!("should convert");
        };
        let Ok(returned) = existing_or_vanished(Some(existing.clone()), "run1.a") else {
            panic!("a found row is returned");
        };
        assert_eq!(returned, existing);
    }
}
