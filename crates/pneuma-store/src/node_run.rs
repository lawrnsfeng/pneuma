//! One row of the `node_run` table, and its domain form.

use chrono::{DateTime, Utc};
use compact_str::CompactString;
use pneuma_core::child_index::ChildIndex;
use pneuma_core::ids::{NodeId, PipelineId, RunId};
use pneuma_core::node::NodeKind;
use pneuma_core::sibling_index::SiblingIndex;
use pneuma_core::status::NodeStatus;
use serde_json::Value;
use uuid::Uuid;

/// A `node_run` row exactly as Postgres returns it.
///
/// Deliberately made of primitives. The domain newtypes reject values the
/// database can nonetheless hold — a `child_index` of `-1`, an empty `node_id` —
/// and a `FromRow` that could fail on those would turn a readable row into an
/// unreadable one at the driver boundary, where there is no useful context.
/// Reading always succeeds; [`NodeRun::try_from`] is where the domain's rules
/// are applied and where a violation is reported with the row's identity.
///
/// This mirrors `pneuma-gateway`'s own `TerminationRow` → `Termination` split.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct NodeRunRow {
    /// Primary key.
    pub id: Uuid,
    /// The coordination key: `{run_id}.{pipeline_id}.{node path}`. Unique.
    pub path: String,
    /// The node's id within its pipeline definition.
    pub node_id: String,
    /// Human-readable name.
    pub name: String,
    /// Which kind of node. Postgres enum `node_kind`.
    pub kind: NodeKind,
    /// The pipeline this run belongs to.
    pub pipeline_id: String,
    /// The run this node belongs to.
    pub run_id: String,
    /// Parent node's id, when this node is a child.
    pub parent_id: Option<String>,
    /// Parent node's path. A foreign key onto this same table's `path`.
    pub parent_path: Option<String>,
    /// Parent node's kind, as a string rather than the enum — the column is
    /// `VARCHAR`, not `node_kind`, which is itself a drift worth preserving
    /// rather than silently correcting.
    pub parent_kind: Option<String>,
    /// Runtime fan-out index.
    pub child_index: Option<i32>,
    /// Static position among the parent's declared components.
    pub sibling_index: Option<i32>,
    /// Postgres enum `node_status`.
    pub status: NodeStatus,
    /// The node's input. Column type is `JSON`, not `JSONB`.
    pub step_input: Option<Value>,
    /// The node's output. Column type is `JSON`, not `JSONB`.
    pub step_output: Option<Value>,
    /// Error code when the node failed.
    pub error_code: Option<String>,
    /// Error detail when the node failed.
    pub error_message: Option<String>,
    /// Unmodelled extras. Column type is `JSONB`.
    pub extra: Option<Value>,
    /// When the row was inserted.
    pub created_at: DateTime<Utc>,
    /// When the row was last written.
    pub updated_at: DateTime<Utc>,
    /// When the node began executing.
    pub started_at: Option<DateTime<Utc>>,
    /// When the node stopped executing.
    pub finished_at: Option<DateTime<Utc>>,
    /// Present in the schema, from the original's shared base. Both delete paths in
    /// the original are hard `DELETE`s, so nothing reads
    /// or writes it today.
    pub is_deleted: Option<bool>,
}

/// A stored node run, with the domain's rules applied.
#[derive(Debug, Clone, PartialEq)]
pub struct NodeRun {
    /// Primary key.
    pub id: Uuid,
    /// The coordination key.
    pub path: String,
    /// The node's id within its pipeline definition.
    pub node_id: NodeId,
    /// Human-readable name.
    pub name: CompactString,
    /// Which kind of node.
    pub kind: NodeKind,
    /// The pipeline this run belongs to.
    pub pipeline_id: PipelineId,
    /// The run this node belongs to.
    pub run_id: RunId,
    /// Parent node's id.
    pub parent_id: Option<NodeId>,
    /// Parent node's path.
    pub parent_path: Option<String>,
    /// Parent node's kind as stored — a string column, not the enum.
    pub parent_kind: Option<CompactString>,
    /// Runtime fan-out index.
    pub child_index: Option<ChildIndex>,
    /// Static position among the parent's declared components.
    pub sibling_index: Option<SiblingIndex>,
    /// Where the node is in its lifecycle.
    pub status: NodeStatus,
    /// The node's input.
    pub step_input: Option<Value>,
    /// The node's output.
    pub step_output: Option<Value>,
    /// Error code when the node failed.
    pub error_code: Option<CompactString>,
    /// Error detail when the node failed.
    pub error_message: Option<String>,
    /// Unmodelled extras.
    pub extra: Option<Value>,
    /// When the row was inserted.
    pub created_at: DateTime<Utc>,
    /// When the row was last written.
    pub updated_at: DateTime<Utc>,
    /// When the node began executing.
    pub started_at: Option<DateTime<Utc>>,
    /// When the node stopped executing.
    pub finished_at: Option<DateTime<Utc>>,
}

/// A stored row the domain will not accept.
///
/// There is exactly one such case, and it is worth being precise about why.
/// The id columns cannot fail: `NodeId`, `PipelineId` and `RunId` are
/// unvalidated newtypes over a string, so any `VARCHAR` the table holds is a
/// legal id. Only the two index columns can hold something the domain refuses.
///
/// The error names the row by `path`, because "invalid index" with no identity
/// is not actionable against a table of millions of rows.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RowError {
    /// `child_index` or `sibling_index` held a value outside `1..=u32::MAX`.
    ///
    /// Both are `INTEGER NOT NULL`-less columns with no `CHECK`, so Postgres
    /// accepts `0` and negatives. Both are **1-based** in the original —
    /// `child_idx=index + 1` and
    /// `nth=nth_ + 1`, with
    /// the original even coercing a falsy value to `1`
    /// — which is why `ChildIndex` and `SiblingIndex` reject zero. A row holding
    /// `0` was written by something that did not follow that convention, and
    /// silently promoting it to `1` would merge two different children's
    /// coordination state.
    #[error("node_run {path:?}: column {column} holds {value}, but the index is 1-based")]
    IndexNotPositive {
        /// The row's path.
        path: String,
        /// Which column.
        column: &'static str,
        /// What it held.
        value: i32,
    },
}

/// Turns a stored index column into its domain type.
///
/// Each check owns exactly one condition, which is what keeps both reachable:
/// `u32::try_from` rejects a negative, and the constructor rejects zero. An
/// earlier version filtered zero here too, which made the constructor's own
/// error arm dead code -- unreachable, therefore untestable, and the coverage
/// gate said so.
fn positive_index<T, E>(
    path: &str,
    column: &'static str,
    value: i32,
    build: impl FnOnce(u32) -> Result<T, E>,
) -> Result<T, RowError> {
    let fail = || RowError::IndexNotPositive {
        path: path.to_owned(),
        column,
        value,
    };
    let unsigned = u32::try_from(value).map_err(|_| fail())?;
    build(unsigned).map_err(|_| fail())
}

impl TryFrom<NodeRunRow> for NodeRun {
    type Error = RowError;

    fn try_from(row: NodeRunRow) -> Result<Self, Self::Error> {
        Ok(NodeRun {
            id: row.id,
            node_id: NodeId::new(row.node_id.as_str()),
            name: CompactString::from(row.name.as_str()),
            kind: row.kind,
            pipeline_id: PipelineId::new(row.pipeline_id.as_str()),
            run_id: RunId::new(row.run_id.as_str()),
            parent_id: row.parent_id.as_deref().map(NodeId::new),
            parent_path: row.parent_path,
            parent_kind: row.parent_kind.as_deref().map(CompactString::from),
            child_index: row
                .child_index
                .map(|value| positive_index(&row.path, "child_index", value, ChildIndex::new))
                .transpose()?,
            sibling_index: row
                .sibling_index
                .map(|value| positive_index(&row.path, "sibling_index", value, SiblingIndex::new))
                .transpose()?,
            status: row.status,
            step_input: row.step_input,
            step_output: row.step_output,
            error_code: row.error_code.as_deref().map(CompactString::from),
            error_message: row.error_message,
            extra: row.extra,
            created_at: row.created_at,
            updated_at: row.updated_at,
            started_at: row.started_at,
            finished_at: row.finished_at,
            path: row.path,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row() -> NodeRunRow {
        NodeRunRow {
            id: Uuid::nil(),
            path: "run1.p.q.r.node".to_owned(),
            node_id: "node".to_owned(),
            name: "A node".to_owned(),
            kind: NodeKind::Model,
            pipeline_id: "p.q.r".to_owned(),
            run_id: "run1".to_owned(),
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
        }
    }

    #[test]
    fn a_minimal_row_converts() {
        let Ok(run) = NodeRun::try_from(row()) else {
            panic!("should convert");
        };
        assert_eq!(run.node_id.as_str(), "node");
        assert_eq!(run.run_id.as_str(), "run1");
        assert_eq!(run.pipeline_id.as_str(), "p.q.r");
        assert_eq!(run.path, "run1.p.q.r.node");
        assert_eq!(run.kind, NodeKind::Model);
        assert_eq!(run.status, NodeStatus::Created);
        assert!(run.child_index.is_none());
        assert!(run.sibling_index.is_none());
    }

    #[test]
    fn a_fully_populated_row_converts() {
        let mut raw = row();
        raw.parent_id = Some("parent".to_owned());
        raw.parent_path = Some("run1.p.q.r.parent".to_owned());
        raw.parent_kind = Some("ListAggregator".to_owned());
        raw.child_index = Some(2);
        raw.sibling_index = Some(3);
        raw.step_input = Some(serde_json::json!({"a": 1}));
        raw.step_output = Some(serde_json::json!([1, 2]));
        raw.error_code = Some("E1".to_owned());
        raw.error_message = Some("boom".to_owned());
        raw.extra = Some(serde_json::json!({"k": "v"}));
        raw.started_at = Some(DateTime::<Utc>::UNIX_EPOCH);
        raw.finished_at = Some(DateTime::<Utc>::UNIX_EPOCH);
        raw.is_deleted = Some(false);

        let Ok(run) = NodeRun::try_from(raw) else {
            panic!("should convert");
        };
        assert_eq!(
            run.parent_id.map(|id| id.as_str().to_owned()),
            Some("parent".to_owned())
        );
        assert_eq!(run.parent_kind.as_deref(), Some("ListAggregator"));
        assert_eq!(run.child_index.map(ChildIndex::get), Some(2));
        assert_eq!(run.sibling_index.map(SiblingIndex::get), Some(3));
        assert_eq!(run.error_code.as_deref(), Some("E1"));
        assert_eq!(run.step_output, Some(serde_json::json!([1, 2])));
    }

    #[test]
    fn a_zero_index_is_rejected_because_both_are_one_based() {
        // The column has no CHECK, so Postgres accepts 0. The original
        // writes `index + 1` and `nth_ + 1`, so a 0 was written by something
        // that did not follow the convention. Promoting it to 1 would merge two
        // different children's coordination state.
        for (column, set) in [
            (
                "child_index",
                (|r: &mut NodeRunRow, v| r.child_index = Some(v)) as fn(&mut NodeRunRow, i32),
            ),
            ("sibling_index", |r: &mut NodeRunRow, v| {
                r.sibling_index = Some(v)
            }),
        ] {
            let mut raw = row();
            set(&mut raw, 0);
            assert_eq!(
                NodeRun::try_from(raw),
                Err(RowError::IndexNotPositive {
                    path: "run1.p.q.r.node".to_owned(),
                    column,
                    value: 0,
                })
            );
        }
    }

    #[test]
    fn a_negative_index_is_rejected_and_names_the_column_and_row() {
        let mut raw = row();
        raw.sibling_index = Some(-7);
        let Err(error) = NodeRun::try_from(raw) else {
            panic!("-7 should be rejected");
        };
        assert_eq!(
            error.to_string(),
            r#"node_run "run1.p.q.r.node": column sibling_index holds -7, but the index is 1-based"#
        );

        let mut raw = row();
        raw.child_index = Some(i32::MIN);
        assert!(matches!(
            NodeRun::try_from(raw),
            Err(RowError::IndexNotPositive {
                column: "child_index",
                ..
            })
        ));
    }

    #[test]
    fn ids_cannot_fail_so_any_varchar_the_table_holds_converts() {
        // NodeId/PipelineId/RunId are unvalidated newtypes over a string. This
        // pins that, because an earlier draft of this module invented an
        // `InvalidId` error for a failure that cannot occur.
        let mut raw = row();
        raw.node_id = String::new();
        raw.pipeline_id = "  ".to_owned();
        raw.run_id = "\u{1}".to_owned();
        let Ok(run) = NodeRun::try_from(raw) else {
            panic!("ids are infallible; an empty string is a legal id");
        };
        assert_eq!(run.node_id.as_str(), "");
    }
}
