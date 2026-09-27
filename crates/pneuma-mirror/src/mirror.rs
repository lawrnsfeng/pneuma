//! The recorder itself.
//!
//! Split from `lib.rs` because `scripts/coverage.sh` excludes that file and
//! refuses to let it hold definitions -- anything defined there is silently
//! unmeasured, which for the one type in this crate would mean the gate reading
//! 100% while measuring nothing.

use pneuma_runner::{NewStep, Record, Recorder};
use pneuma_store::{NewNodeRun, NodeRunStore};
use sqlx::PgPool;
use uuid::Uuid;

/// The namespace run ids are hashed into for a row's primary key.
///
/// A v5 UUID of the step's path, rather than a v4. The id is the row's primary
/// key and `NewNodeRun` takes it from the caller specifically so a retry can
/// reuse it (`pneuma-store/src/store.rs:49-50`) — and under Restate a replay
/// *is* a retry. A random id would make the same step a different row on every
/// replay, and `ON CONFLICT (path)` would then hide the duplicate while the
/// journal quietly accumulated one row's worth of history per attempt.
///
/// Deriving it from the path instead makes the id a function of the step, so
/// the second attempt writes the row the first attempt did, and the conflict
/// clause means what it says.
const NAMESPACE: Uuid = Uuid::from_bytes([
    0x70, 0x6e, 0x65, 0x75, 0x6d, 0x61, 0x2d, 0x6e, 0x6f, 0x64, 0x65, 0x2d, 0x72, 0x75, 0x6e, 0x00,
]);

/// Why a mirror cannot be used.
///
/// Two variants because the operator action is different, and a probe that
/// collapsed them would send people to the wrong one: a role with `CONNECT` but
/// no `SELECT` on `node_run` is a common least-privilege setup, and reporting
/// it as a missing migration sends an operator to re-run migrations that
/// already applied.
///
/// A `String` inside rather than the `sqlx::Error`, so a DSN cannot reach a log
/// through a `Debug`
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum NotReady {
    /// `node_run` is not there — the migrations have not run, or the
    /// `search_path` does not reach the schema they ran in.
    #[error("{0}")]
    NoTable(String),

    /// Anything else: unreachable, refused, or not permitted.
    #[error("{0}")]
    Unusable(String),
}

/// `undefined_table`, the one SQLSTATE that means "run the migrations".
const UNDEFINED_TABLE: &str = "42P01";

/// Which kind of unreadiness an error is.
///
/// Everything but [`UNDEFINED_TABLE`] is [`NotReady::Unusable`] — `42501
/// insufficient_privilege`, `42703 undefined_column` from a `node_run` that is
/// not this one, a dropped connection, a timeout. One `matches!` rather than a
/// match with a fall-through arm, because the arm no reachable input takes is
/// the arm the coverage gate refuses.
fn classify(error: &pneuma_store::StoreError) -> NotReady {
    let missing = matches!(
        error,
        pneuma_store::StoreError::Database(inner)
            if inner.as_database_error().and_then(|failed| failed.code()).as_deref()
                == Some(UNDEFINED_TABLE)
    );
    if missing {
        return NotReady::NoTable(error.to_string());
    }
    NotReady::Unusable(error.to_string())
}

/// A recorder that writes to Postgres.
#[derive(Debug, Clone)]
pub struct Mirror {
    store: NodeRunStore,
    /// What to call this in a log line, so two transports are distinguishable.
    service: &'static str,
}

impl Mirror {
    /// Builds a mirror over `pool`.
    pub fn new(pool: PgPool, service: &'static str) -> Self {
        Mirror {
            store: NodeRunStore::new(pool),
            service,
        }
    }

    /// The row a step's first appearance becomes.
    ///
    /// Public because it is the whole of the translation and worth asserting
    /// without a database.
    pub fn row(step: &NewStep) -> NewNodeRun {
        NewNodeRun {
            id: Uuid::new_v5(&NAMESPACE, step.path.as_bytes()),
            path: step.path.clone(),
            node_id: step.node_id.as_str().to_owned(),
            name: step.name.clone(),
            kind: step.kind,
            pipeline_id: step.pipeline_id.as_str().to_owned(),
            run_id: step.run_id.as_str().to_owned(),
            parent_id: step.parent_id.as_ref().map(|id| id.as_str().to_owned()),
            parent_path: step.parent_path.clone(),
            parent_kind: step.parent_kind.clone(),
            child_index: step.child_index,
            sibling_index: step.sibling_index,
            // Every row is born `CREATED`, as the original's is: `NodeRunCreate`
            // defaults it and nothing
            // at a construction site overrides it.
            status: pneuma_core::status::NodeStatus::Created,
            step_input: step.step_input.clone(),
            step_output: None,
        }
    }

    /// Checks that the table this writes to is actually there.
    ///
    /// A DSN that opens is not the same as a database that can be mirrored: a
    /// replica pointed at a reachable Postgres whose migrations have not run —
    /// or whose `search_path` does not include the schema they ran in — starts
    /// healthy and then logs one refusal per step, for ever. That is the same
    /// silent-mirror failure the required `DATABASE_URL` exists to prevent,
    /// one step removed, so a caller can make it a startup refusal instead.
    ///
    /// A real read through the real statement rather than a `SELECT 1`: what
    /// must be true is that `node_run` is reachable *as this crate queries it*,
    /// which a probe written separately could agree with while the statements
    /// did not.
    pub async fn ready(&self) -> Result<(), NotReady> {
        match self.store.get_by_path("").await {
            // A path no run can produce, so `None` is the only answer a healthy
            // database gives -- and the row's absence is what proves the table
            // is there.
            Ok(_) => Ok(()),
            Err(error) => Err(classify(&error)),
        }
    }

    /// [`Mirror::ready`], with a deadline.
    ///
    /// The bound belongs here rather than at each caller, and not only to save
    /// a `tokio::time::timeout` twice: a database that completes the handshake
    /// and then stalls — a catalog lock held by a concurrent `ALTER`, a hung
    /// standby — would otherwise block a caller's startup with nothing bound
    /// and no line logged, which is exactly the failure the caller's *connect*
    /// bound exists to prevent, reintroduced on the next await.
    pub async fn ready_within(&self, deadline: std::time::Duration) -> Result<(), NotReady> {
        match tokio::time::timeout(deadline, self.ready()).await {
            Ok(answered) => answered,
            Err(_) => Err(NotReady::Unusable(format!("no answer in {deadline:?}"))),
        }
    }

    /// Says a write did not happen, without saying the run failed.
    fn complain(&self, what: &str, path: &str, error: &impl std::fmt::Display) {
        eprintln!(
            "{}: could not record {what} for {path}: {error}",
            self.service
        );
    }
}

impl Recorder for Mirror {
    async fn record(&self, record: Record) {
        match record {
            Record::Created(step) => {
                let row = Mirror::row(&step);
                if let Err(error) = self.store.create(&row).await {
                    self.complain("the start of", &step.path, &error);
                }
            }
            Record::Moved {
                path,
                status,
                failure,
            } => {
                let (code, message) = match &failure {
                    Some(failure) => (Some(failure.code.as_str()), Some(failure.message.as_str())),
                    None => (None, None),
                };
                // `Ok(None)` is not a failure. It means the statement's guard
                // refused -- a terminal row never moves, and `FORKED` is
                // admitted only from `CREATED` -- which is the ordinary outcome
                // of telling an aggregator twice that a child started, and the
                // reason those records are emitted unconditionally.
                if let Err(error) = self.store.update_status(&path, status, code, message).await {
                    self.complain("a move to", &path, &error);
                }
            }
            Record::Produced {
                path,
                status,
                output,
            } => {
                let written = self.store.record_output(&path, status, Some(&output)).await;
                if let Err(error) = written {
                    self.complain("the output of", &path, &error);
                }
            }
        }
    }
}
