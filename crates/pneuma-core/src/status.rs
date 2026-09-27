//! Node and run statuses, and the guard that decides whether a proposed
//! status transition is admissible.
//!
//! See the design notes: `Cancelled` is treated as terminal here,
//! unlike the original's `should_update_status_from`,
//! which — verified directly — omits it from
//! `NODERUN_FINISHED_STATUSES`. That gap let a late result resurrect a
//! cancelled noderun; this port closes it.

use serde::{Deserialize, Serialize};

/// The status of a single resolved step's execution.
///
/// The `sqlx` attributes must match the **existing** Postgres enum created by
/// the original:
/// the type was named `nodestatus` (no underscore — the original ORM derives it from
/// the original class name) and is now `node_status`
/// (`pneuma-store/migrations/0004_rename.sql`). Its labels are
/// `SCREAMING_SNAKE_CASE`
/// (`TIMED_OUT`, not `TIMEDOUT`). Note that sqlx's `rename_all = "UPPERCASE"`
/// is a plain `str::to_uppercase` on the variant ident, which would produce
/// `TIMEDOUT` and fail at runtime against the real database — hence
/// `SCREAMING_SNAKE_CASE` here. See the `postgres_enum_contract` tests below,
/// which pin both without needing a live connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, sqlx::Type, Serialize, Deserialize)]
#[sqlx(type_name = "node_status", rename_all = "SCREAMING_SNAKE_CASE")]
#[serde(rename_all = "snake_case")]
pub enum NodeStatus {
    Created,
    Processing,
    Finished,
    Error,
    TimedOut,
    Cancelled,
    Forked,
    Aggregated,
    HasChildError,
    HasChildTimedOut,
}

impl NodeStatus {
    /// Every variant, for exhaustive iteration in tests.
    pub const ALL: [NodeStatus; 10] = [
        NodeStatus::Created,
        NodeStatus::Processing,
        NodeStatus::Finished,
        NodeStatus::Error,
        NodeStatus::TimedOut,
        NodeStatus::Cancelled,
        NodeStatus::Forked,
        NodeStatus::Aggregated,
        NodeStatus::HasChildError,
        NodeStatus::HasChildTimedOut,
    ];

    /// A terminal status never accepts a further write. `Cancelled` is
    /// included here on purpose — see the design notes.
    pub const fn is_terminal(self) -> bool {
        matches!(
            self,
            NodeStatus::Finished
                | NodeStatus::Error
                | NodeStatus::TimedOut
                | NodeStatus::Cancelled
                | NodeStatus::Aggregated
                | NodeStatus::HasChildError
                | NodeStatus::HasChildTimedOut
        )
    }

    /// Whether this status represents a step that has begun executing.
    pub const fn is_started(self) -> bool {
        matches!(self, NodeStatus::Processing | NodeStatus::Forked)
    }

    /// Decides whether transitioning from `self` (the current status) to
    /// `proposed` is admissible. Exhaustive and total: every one of the
    /// `NodeStatus::ALL.len() ^ 2` pairs has a defined, intentional answer.
    pub fn admit(self, proposed: NodeStatus) -> Admission {
        if self.is_terminal() {
            return Admission::Reject(RejectReason::AlreadyTerminal);
        }
        if proposed == NodeStatus::Forked && self != NodeStatus::Created {
            return Admission::Reject(RejectReason::ForkFromNonCreated);
        }
        Admission::Accept
    }
}

/// The outcome of [`NodeStatus::admit`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Admission {
    Accept,
    Reject(RejectReason),
}

/// Why a proposed transition was rejected. A closed set — every rejection
/// has exactly one of these causes, never an unexplained residual case.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RejectReason {
    /// `self` is already a terminal status; nothing may be written to it.
    AlreadyTerminal,
    /// `proposed` is `Forked`, but `self` is not `Created` — a step may
    /// only fork from its initial state.
    ForkFromNonCreated,
}

/// The status of a whole run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunStatus {
    Created,
    Processing,
    Finished,
    Error,
    TimedOut,
    Cancelled,
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A golden table checked in as data, not asserted only via the loop
    /// below — a reviewer can read every cell directly. `(Cancelled,
    /// Finished)` is the fixed pair: the original would accept this
    /// transition (see the design notes); this port rejects it.
    const EXPECTED: &[(NodeStatus, NodeStatus, Admission)] = &[
        (NodeStatus::Created, NodeStatus::Forked, Admission::Accept),
        (
            NodeStatus::Processing,
            NodeStatus::Forked,
            Admission::Reject(RejectReason::ForkFromNonCreated),
        ),
        (
            NodeStatus::Cancelled,
            NodeStatus::Finished,
            Admission::Reject(RejectReason::AlreadyTerminal),
        ),
        (
            NodeStatus::Finished,
            NodeStatus::Created,
            Admission::Reject(RejectReason::AlreadyTerminal),
        ),
        (
            NodeStatus::Created,
            NodeStatus::Processing,
            Admission::Accept,
        ),
        (
            NodeStatus::Processing,
            NodeStatus::Finished,
            Admission::Accept,
        ),
    ];

    #[test]
    fn golden_table_cells_match() {
        for &(current, proposed, expected) in EXPECTED {
            assert_eq!(
                current.admit(proposed),
                expected,
                "admit({current:?}, {proposed:?})"
            );
        }
    }

    /// Exhaustive: every one of the 100 `(current, proposed)` pairs is
    /// exercised, not just the golden table above.
    #[test]
    fn admit_is_exhaustively_defined() {
        for current in NodeStatus::ALL {
            for proposed in NodeStatus::ALL {
                // Every call must return a value (it always does — the
                // point of this test is that it's actually invoked for
                // all 100 pairs, which the structural invariants below
                // then constrain).
                let _ = current.admit(proposed);
            }
        }
    }

    /// Structural invariant 1: terminal states admit nothing, ever.
    #[test]
    fn terminal_states_admit_nothing() {
        for current in NodeStatus::ALL {
            if current.is_terminal() {
                for proposed in NodeStatus::ALL {
                    assert_eq!(
                        current.admit(proposed),
                        Admission::Reject(RejectReason::AlreadyTerminal),
                        "terminal state {current:?} must reject admit({proposed:?})"
                    );
                }
            }
        }
    }

    /// Structural invariant 2: the *only* reason a non-terminal state ever
    /// rejects is the fork-from-non-created rule — every reject has exactly
    /// one of two named causes, no residual unexplained case.
    #[test]
    fn non_terminal_rejections_are_always_fork_from_non_created() {
        for current in NodeStatus::ALL {
            if current.is_terminal() {
                continue;
            }
            for proposed in NodeStatus::ALL {
                match current.admit(proposed) {
                    Admission::Accept => {}
                    Admission::Reject(reason) => {
                        assert_eq!(reason, RejectReason::ForkFromNonCreated);
                        assert_eq!(proposed, NodeStatus::Forked);
                        assert_ne!(current, NodeStatus::Created);
                    }
                }
            }
        }
    }

    #[test]
    fn is_terminal_covers_all_seven_states() {
        let terminal: Vec<NodeStatus> = NodeStatus::ALL
            .into_iter()
            .filter(|s| s.is_terminal())
            .collect();
        assert_eq!(terminal.len(), 7);
        assert!(terminal.contains(&NodeStatus::Cancelled));
    }

    #[test]
    fn is_started_covers_processing_and_forked() {
        let started: Vec<NodeStatus> = NodeStatus::ALL
            .into_iter()
            .filter(|s| s.is_started())
            .collect();
        assert_eq!(started, vec![NodeStatus::Processing, NodeStatus::Forked]);
    }

    #[test]
    fn run_status_serde_round_trips() -> Result<(), serde_json::Error> {
        for (status, wire) in [
            (RunStatus::Created, "\"created\""),
            (RunStatus::Processing, "\"processing\""),
            (RunStatus::Finished, "\"finished\""),
            (RunStatus::Error, "\"error\""),
            (RunStatus::TimedOut, "\"timed_out\""),
            (RunStatus::Cancelled, "\"cancelled\""),
        ] {
            let json = serde_json::to_string(&status)?;
            assert_eq!(json, wire);
            let back: RunStatus = serde_json::from_str(&json)?;
            assert_eq!(back, status);
        }
        Ok(())
    }

    /// Pins the Postgres enum contract without a live database.
    ///
    /// A mismatch here is a runtime failure the rest of this crate's tests
    /// structurally cannot catch (there is no DB in the loop), so it is
    /// asserted directly against the labels the original migration tool actually created.
    mod postgres_enum_contract {
        use sqlx::{Encode, Type, TypeInfo};

        use super::*;

        #[test]
        fn type_name_matches_the_migrated_enum() {
            // the original migration tool created `name="nodestatus"`; `0004_rename.sql` renames it
            // to `node_status`. The labels did **not** move with it -- only the
            // type's own name did -- which is why the label test below still
            // reads against the original migration tool revision.
            assert_eq!(
                <NodeStatus as Type<sqlx::Postgres>>::type_info().name(),
                "node_status"
            );
        }

        #[test]
        fn every_label_matches_the_original_enum() {
            // Exactly the labels from
            // the original, in order.
            let expected = [
                (NodeStatus::Created, "CREATED"),
                (NodeStatus::Processing, "PROCESSING"),
                (NodeStatus::Finished, "FINISHED"),
                (NodeStatus::Error, "ERROR"),
                (NodeStatus::TimedOut, "TIMED_OUT"),
                (NodeStatus::Cancelled, "CANCELLED"),
                (NodeStatus::Forked, "FORKED"),
                (NodeStatus::Aggregated, "AGGREGATED"),
                (NodeStatus::HasChildError, "HAS_CHILD_ERROR"),
                (NodeStatus::HasChildTimedOut, "HAS_CHILD_TIMED_OUT"),
            ];
            assert_eq!(expected.len(), NodeStatus::ALL.len());

            for (status, label) in expected {
                let mut buf = sqlx::postgres::PgArgumentBuffer::default();
                let encoded =
                    <NodeStatus as Encode<sqlx::Postgres>>::encode_by_ref(&status, &mut buf);
                assert!(encoded.is_ok(), "encoding {status:?} failed");
                assert_eq!(
                    &buf[..],
                    label.as_bytes(),
                    "{status:?} must encode as the Postgres label {label}"
                );
            }
        }
    }

    #[test]
    fn node_status_serde_round_trips_snake_case() -> Result<(), serde_json::Error> {
        let json = serde_json::to_string(&NodeStatus::TimedOut)?;
        assert_eq!(json, "\"timed_out\"");
        let back: NodeStatus = serde_json::from_str(&json)?;
        assert_eq!(back, NodeStatus::TimedOut);
        Ok(())
    }
}
