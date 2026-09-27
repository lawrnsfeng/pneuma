//! The SQL, one constant per query.
//!
//! Kept as text rather than built with a query builder, and loaded with
//! `include_str!` so each statement is readable as SQL and reviewable by
//! someone who reads SQL but not Rust. This follows the convention already set
//! by the platform's read-API service, the one other production Rust service.
//!
//! Every statement here is checked against a real Postgres by
//! `tests/schema.rs`, which applies `migrations/0001_noderun.sql` and asks the
//! server to `PREPARE` each one. That catches a typo, a renamed column, or a
//! type the driver cannot bind — without needing the repository layer to exist
//! yet.

/// Insert one node run.
pub const CREATE: &str = include_str!("sql/node_run_create.sql");
/// Fetch one row by its unique path.
pub const GET_BY_PATH: &str = include_str!("sql/node_run_get_by_path.sql");
/// Fetch a parent's children, ordered by fan-out index.
pub const GET_BY_PARENT_PATH: &str = include_str!("sql/node_run_get_by_parent_path.sql");
/// Fetch many rows by path.
pub const GET_BY_PATHS: &str = include_str!("sql/node_run_get_by_paths.sql");
/// Fetch every row of one run.
pub const GET_BY_RUN_ID: &str = include_str!("sql/node_run_get_by_run_id.sql");
/// The most recent `updated_at` per run, for staleness checks.
pub const MAX_UPDATED_AT_BY_RUN_IDS: &str =
    include_str!("sql/node_run_max_updated_at_by_run_ids.sql");
/// Delete every row of the given runs.
pub const DELETE_BY_RUN_IDS: &str = include_str!("sql/node_run_delete_by_run_ids.sql");
/// Runs whose newest row is older than a cutoff and still in a running state.
pub const STALE_INPROGRESS_RUN_IDS: &str =
    include_str!("sql/node_run_stale_inprogress_run_ids.sql");
/// Move a node to a new status.
pub const UPDATE_STATUS: &str = include_str!("sql/node_run_update_status.sql");
/// Record a node's output alongside its new status.
pub const RECORD_OUTPUT: &str = include_str!("sql/node_run_record_output.sql");
/// Copy a run's node runs into `node_run_history`, idempotently.
pub const HISTORY_BACKUP: &str = include_str!("sql/history_backup.sql");
/// Delete history older than a cutoff.
pub const HISTORY_DELETE_OUTDATED: &str = include_str!("sql/history_delete_outdated.sql");

/// Counts what [`HISTORY_DELETE_OUTDATED`] would remove; the two predicates
/// must match, and a test requires it.
pub const HISTORY_COUNT_OUTDATED: &str = include_str!("sql/history_count_outdated.sql");

/// Accept a submission, or report that it was already queued.
pub const SUBMISSION_ENQUEUE: &str = include_str!("sql/submission_enqueue.sql");
/// Every queued submission, grouped by tenant and oldest first within one.
pub const SUBMISSION_QUEUED_BACKLOGS: &str = include_str!("sql/submission_queued_backlogs.sql");
/// Take exactly the submissions a fair selection chose.
pub const SUBMISSION_CLAIM: &str = include_str!("sql/submission_claim.sql");
/// Record how a claimed submission ended.
pub const SUBMISSION_SETTLE: &str = include_str!("sql/submission_settle.sql");
/// Return claims nobody settled to the queue.
pub const SUBMISSION_RECLAIM: &str = include_str!("sql/submission_reclaim.sql");
/// The next dispatch round, shared across every replica.
pub const SUBMISSION_NEXT_ROUND: &str = include_str!("sql/submission_next_round.sql");

/// Every statement, for the test that prepares them all. A new query added to
/// this module and not to this list would go unchecked.
pub const ALL: &[(&str, &str)] = &[
    ("CREATE", CREATE),
    ("GET_BY_PATH", GET_BY_PATH),
    ("GET_BY_PARENT_PATH", GET_BY_PARENT_PATH),
    ("GET_BY_PATHS", GET_BY_PATHS),
    ("GET_BY_RUN_ID", GET_BY_RUN_ID),
    ("MAX_UPDATED_AT_BY_RUN_IDS", MAX_UPDATED_AT_BY_RUN_IDS),
    ("DELETE_BY_RUN_IDS", DELETE_BY_RUN_IDS),
    ("STALE_INPROGRESS_RUN_IDS", STALE_INPROGRESS_RUN_IDS),
    ("UPDATE_STATUS", UPDATE_STATUS),
    ("RECORD_OUTPUT", RECORD_OUTPUT),
    ("HISTORY_BACKUP", HISTORY_BACKUP),
    ("HISTORY_DELETE_OUTDATED", HISTORY_DELETE_OUTDATED),
    ("HISTORY_COUNT_OUTDATED", HISTORY_COUNT_OUTDATED),
    ("SUBMISSION_ENQUEUE", SUBMISSION_ENQUEUE),
    ("SUBMISSION_QUEUED_BACKLOGS", SUBMISSION_QUEUED_BACKLOGS),
    ("SUBMISSION_CLAIM", SUBMISSION_CLAIM),
    ("SUBMISSION_SETTLE", SUBMISSION_SETTLE),
    ("SUBMISSION_RECLAIM", SUBMISSION_RECLAIM),
    ("SUBMISSION_NEXT_ROUND", SUBMISSION_NEXT_ROUND),
];

#[cfg(test)]
mod tests {
    use super::*;

    /// SQL with `--` comment lines removed.
    ///
    /// Every check below matches against this rather than the raw text. All
    /// three guarding files narrate their status list in prose above the
    /// statement, so matching the whole file would let a header comment satisfy
    /// an assertion about the `WHERE` clause — the guard would still pass while
    /// the clause it guards had lost the status.
    fn statement_only(sql: &str) -> String {
        sql.lines()
            .filter(|line| !line.trim_start().starts_with("--"))
            .collect::<Vec<_>>()
            .join("\n")
    }

    /// Statements that guard on terminal status. Named explicitly rather than
    /// counted, so a fourth one written with a different formulation — an
    /// `= ANY(ARRAY[...])`, or a list missing a label — is a failure here
    /// rather than something the scan quietly skips.
    const GUARDING: &[&str] = &["UPDATE_STATUS", "RECORD_OUTPUT", "STALE_INPROGRESS_RUN_IDS"];

    /// `CANCELLED` belongs in every terminal-status guard. That is
    /// the design notes, a signed-off deviation: `pneuma-core`'s
    /// `NodeStatus::is_terminal` includes `Cancelled` and `admit()` rejects
    /// every transition out of it, so a statement that omits it lets the
    /// database resurrect work the domain type refuses to.
    ///
    /// This is mechanical because memory already failed: `UPDATE_STATUS`
    /// shipped without it while `STALE_INPROGRESS_RUN_IDS` had it, so the two
    /// disagreed about one status and the disagreement was written up as
    /// deliberate.
    #[test]
    fn every_terminal_status_guard_includes_cancelled() {
        // The last label of the original's NODERUN_FINISHED_STATUSES, used to
        // recognise the list.
        const MARKER: &str = "'HAS_CHILD_TIMED_OUT'";

        let mut found = Vec::new();
        for (name, sql) in ALL {
            let statement = statement_only(sql);
            if !statement.contains(MARKER) {
                continue;
            }
            found.push(*name);
            assert!(
                statement.contains("'CANCELLED'"),
                "{name} guards on terminal status but omits CANCELLED -- see \
                 The design notes"
            );
        }

        found.sort_unstable();
        let mut expected = GUARDING.to_vec();
        expected.sort_unstable();
        assert_eq!(
            found, expected,
            "the set of statements guarding on terminal status changed; if a \
             statement was added or now uses a different formulation, update \
             GUARDING and make sure the new one carries CANCELLED"
        );
    }

    /// Every `.sql` file on disk must be reachable through [`ALL`].
    ///
    /// This reads the directory rather than counting, because counting does not
    /// work. An earlier version asserted `ALL.len() == 10`, which passes when a
    /// constant is added and *not* listed — the failure it claimed to catch —
    /// and fails when both are done correctly. It also could not notice
    /// `("RECORD_OUTPUT", UPDATE_STATUS)`, a mispairing that leaves ten unique
    /// names while one statement is never checked against Postgres.
    ///
    /// Comparing file contents catches all three: an unlisted file has no entry
    /// carrying its text, and a mispaired constant leaves the real file
    /// unmatched.
    #[test]
    fn every_sql_file_is_reachable_through_all() {
        let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/src/sql");
        let Ok(entries) = std::fs::read_dir(dir) else {
            panic!("cannot read {dir}");
        };

        let mut files = 0usize;
        for entry in entries {
            let Ok(entry) = entry else {
                panic!("cannot read a directory entry in {dir}");
            };
            let path = entry.path();
            if path.extension().is_none_or(|ext| ext != "sql") {
                continue;
            }
            files += 1;
            let Ok(contents) = std::fs::read_to_string(&path) else {
                panic!("cannot read {}", path.display());
            };
            assert!(
                ALL.iter().any(|(_, sql)| *sql == contents),
                "{} is not reachable through ALL, so tests/schema.rs never \
                 checks it against Postgres",
                path.display()
            );
        }

        assert_eq!(
            files,
            ALL.len(),
            "ALL has {} entries for {files} files on disk",
            ALL.len()
        );
        assert!(files > 0, "no .sql files found -- has the directory moved?");

        let mut names: Vec<&str> = ALL.iter().map(|(name, _)| *name).collect();
        names.sort_unstable();
        names.dedup();
        assert_eq!(names.len(), ALL.len(), "duplicate name in ALL");
    }
}
