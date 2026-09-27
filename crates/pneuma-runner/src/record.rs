//! The record a run leaves behind, and the port that writes it.
//!
//! A run's authority is the journal (under Restate) or the run document (under
//! the broker driver). This is the *audit mirror* beside it: one row per step,
//! in the shape the original's controller writes to the `noderun` table,
//! which is what
//! `pneuma-janitor`'s stale-run detection reads and what an operator asking
//! "which step is this run stuck on" looks at.
//!
//! # Why a port, and not a store
//!
//! For the reason [`crate::driver::Component`] is one: `scripts/forbid-deps.sh`
//! holds this crate to no async runtime and no client, so the loop stays
//! testable against a fake and the same driver runs under Restate and under a
//! plain broker. A `sqlx::PgPool` here would end all three of those at once.
//!
//! # Why it cannot fail
//!
//! [`Recorder::record`] returns `()`. An audit mirror must not be able to fail
//! the run it audits: losing real work — a completed model call, paid for —
//! because an audit table was unreachable is a far worse outcome than a gap in
//! the mirror. Implementations log and carry on, and the run's own authority is
//! untouched.
//!
//! That is also why there is no `DriveError` variant for it. A variant nothing
//! in production can reach is a branch no test can honestly take, and this
//! workspace's coverage gate is entitled to refuse one.
//!
//! # The derivation is here, deliberately
//!
//! Everything hard — what a step's path is, which row is whose parent, which
//! status a moment implies — is decided in this module, purely, and an
//! implementation is a three-arm match onto three statements that already
//! exist. Two implementations that each derived a path would eventually derive
//! two different ones, and the difference would show up as a foreign-key
//! violation on a fan-out under one transport and not the other.

use pneuma_core::child_index::ChildIndex;
use pneuma_core::ids::{NodeId, PipelineId, RunId};
use pneuma_core::node::NodeKind;
use pneuma_core::sibling_index::SiblingIndex;
use pneuma_core::status::NodeStatus;
use pneuma_core::step::{Step, StepRegistry};
use pneuma_interpreter::{Execution, FrameId, Happening, Origin, Task};
use serde_json::Value;

use crate::driver::RunInput;

/// Everything a step's row needs when it first appears.
///
/// Mirrors `construct_noderun` field
/// for field, minus the two the database fills in: `created_at` and
/// `updated_at` are `NOW()` in the statement.
#[derive(Debug, Clone, PartialEq)]
pub struct NewStep {
    /// The coordination key — `{run}.{pipeline}.{node path}`, with a `:{n}`
    /// suffix for a step inside a fan-out.
    pub path: String,
    /// The node's id within its pipeline definition.
    pub node_id: NodeId,
    /// The component name, or the literal `condition` for a conditional.
    ///
    /// The original writes that literal rather than the step's name,
    /// because a conditional has no
    /// component and the column would otherwise name a subject nothing serves.
    pub name: String,
    /// Which kind of node.
    pub kind: NodeKind,
    /// The pipeline this run belongs to.
    pub pipeline_id: PipelineId,
    /// The run.
    pub run_id: RunId,
    /// The enclosing aggregator's node id, when this step is inside one.
    pub parent_id: Option<NodeId>,
    /// The enclosing aggregator's own path. A foreign key: the row it names
    /// must already exist.
    pub parent_path: Option<String>,
    /// The enclosing aggregator's kind, as a string — the column is a
    /// `VARCHAR`, not the enum, which is a drift worth preserving rather than
    /// silently correcting.
    pub parent_kind: Option<String>,
    /// Which branch of the enclosing fan-out this is. 1-based.
    pub child_index: Option<ChildIndex>,
    /// This node's static position among its parent's declared components.
    /// 1-based, and unrelated to `child_index`.
    pub sibling_index: Option<SiblingIndex>,
    /// What the step was given.
    pub step_input: Option<Value>,
}

/// Why a step ended badly.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Failure {
    /// A short, stable code. `PNEUMA_INTERNAL_ERROR` where the original uses
    /// it.
    pub code: String,
    /// What went wrong, in words.
    pub message: String,
}

/// One thing worth writing down about a run.
///
/// Three variants, because there are exactly three statements to write them
/// with — `NodeRunStore::create`, `update_status` and `record_output` — and a
/// fourth kind of record would need a fourth statement rather than a new match
/// arm.
#[derive(Debug, Clone, PartialEq)]
pub enum Record {
    /// A step exists. Written `CREATED`, as the original writes it.
    Created(Box<NewStep>),
    /// A step, or one of its ancestors, moved.
    ///
    /// The guard lives in the statement: a terminal row never moves, and
    /// `FORKED` is admitted only from `CREATED`. So an ancestor told twice that
    /// a child started is a no-op the second time, which is what lets this be
    /// emitted unconditionally rather than tracked.
    Moved {
        /// Whose row.
        path: String,
        /// Where to.
        status: NodeStatus,
        /// Why, when it ended badly.
        failure: Option<Failure>,
    },
    /// A step produced an output, and is finished or aggregated.
    Produced {
        /// Whose row.
        path: String,
        /// `FINISHED` for a step, `AGGREGATED` for an aggregator.
        status: NodeStatus,
        /// What it produced.
        output: Value,
    },
}

/// Whatever writes a run's record down.
///
/// Shaped like [`crate::driver::Component`], including the deliberate absence
/// of a `Send` bound on the returned future: the primary implementation runs
/// inside `restate_sdk`'s `ctx.run`, which promises no `Send`, and requiring
/// one here would make the primary transport unimplementable exactly as it
/// would there.
pub trait Recorder {
    /// Writes one record down, or does not.
    ///
    /// Infallible by signature — see the module docs. An implementation that
    /// cannot reach its store logs and returns.
    fn record(&self, record: Record) -> impl std::future::Future<Output = ()>;
}

/// A recorder that writes nothing.
///
/// For a caller that has no store to write to, and for the tests of everything
/// that is not the recording itself. Named rather than a bare `()` impl so a
/// reader of a call site can see that the omission is deliberate.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoRecord;

impl Recorder for NoRecord {
    async fn record(&self, _record: Record) {}
}

/// The literal `name` a conditional's row carries.
///
/// the original. Public because
/// `pneuma_proto::dispatch::MessageRun::subject` refuses to treat it as a
/// subject, and the two have to agree about the spelling.
pub const CONDITION_NAME: &str = "condition";

/// Joins a prefix and a segment into a path.
///
/// The shape `construct_slug_from` builds
/// and `pneuma_core::slug::Slug`
/// enforces, minus the fan-out suffix — that is [`prefix_of`]'s, because it
/// belongs to a whole branch rather than to one step in it.
///
/// A `String` rather than a `Slug`, and that is not laziness: the column is a
/// `VARCHAR` holding paths this port did not necessarily write, and
/// `Slug::from_raw` exists for exactly that. Constructing through `Slug` here
/// would make a node id containing a `.` a *failure to record* rather than a
/// row with an awkward path, and losing the audit row is the worse of the two.
///
/// There is no parentless form. Every path has a prefix — `{run}.{pipeline}`
/// at the shallowest — so an `Option` here would be a branch nothing takes.
pub fn join(prefix: &str, segment: &str) -> String {
    format!("{prefix}.{segment}")
}

/// The prefix every step in a frame shares.
///
/// `{run}.{pipeline}` at the root, and `{aggregator path}:{child index}` inside
/// a fan-out — so every step of a branch carries that branch's index, and two
/// branches running the same node ids cannot collide.
///
/// # This is not quite the original's string
///
/// `construct_slug_from` puts the `:{child_idx}` on the branch's *start* step
/// and lets everything downstream of it hang off `parent_slug`.
/// That works there because the
/// original threads a `parent_slug` through every recursive
/// `init_next_step`. Here the suffix moves one level up, onto the prefix the
/// whole branch shares, which reaches the same property by a shorter route:
/// every path in a fan-out is distinct, and the branch it belongs to is
/// readable from the path.
///
/// The property is what matters. `node_run.path` is unique, so a scheme that
/// collides does not error — `ON CONFLICT (path) DO NOTHING` swallows every
/// branch after the first, and the mirror silently under-reports exactly the
/// runs that are hardest to reason about. The design notes record the
/// divergence.
pub fn prefix_of(execution: &Execution, run: &RunInput, frame: FrameId) -> String {
    let Some(origin) = execution.origin_of(frame) else {
        return format!("{}.{}", run.meta.run_id(), run.meta.pipeline_id());
    };
    let aggregator = aggregator_path(execution, run, origin);
    format!("{aggregator}:{}", origin.child_index.get())
}

/// The path of the aggregator that opened a frame.
fn aggregator_path(execution: &Execution, run: &RunInput, origin: &Origin) -> String {
    let parent = prefix_of(execution, run, origin.parent);
    join(&parent, origin.aggregator.as_str())
}

/// One step's path.
pub fn step_path(execution: &Execution, run: &RunInput, frame: FrameId, node: &NodeId) -> String {
    join(&prefix_of(execution, run, frame), node.as_str())
}

/// The row a step gets when it first appears.
///
/// `None` when the registry does not know the node, which is the same condition
/// `drive` turns into `DriveError::UnknownStep` — reporting a row for a step
/// that does not exist would put a foreign key on nothing.
pub fn new_step(
    execution: &Execution,
    registry: &StepRegistry,
    run: &RunInput,
    frame: FrameId,
    node: &NodeId,
    input: Option<&Value>,
) -> Option<NewStep> {
    let step = registry.get(node)?;
    let common = step.common();
    let origin = execution.origin_of(frame);
    let parent_path = origin.map(|origin| aggregator_path(execution, run, origin));
    let parent_kind = origin
        .and_then(|origin| registry.get(&origin.aggregator))
        .map(|parent| wire_kind(kind_of(parent)).to_owned());
    Some(NewStep {
        path: step_path(execution, run, frame, node),
        node_id: node.clone(),
        // The literal, for a conditional. See `CONDITION_NAME`.
        name: match step {
            Step::Condition { .. } => CONDITION_NAME.to_owned(),
            _ => common.name.clone().unwrap_or_default().to_string(),
        },
        kind: kind_of(step),
        pipeline_id: run.meta.pipeline_id(),
        run_id: run.meta.run_id(),
        parent_id: origin.map(|origin| origin.aggregator.clone()),
        parent_path,
        parent_kind,
        child_index: origin.map(|origin| origin.child_index),
        sibling_index: Some(common.sibling_index),
        step_input: input.cloned(),
    })
}

/// Every ancestor of a frame, innermost first, with its path.
///
/// What the original walks when it recurses up `parent_slug` to mark a parent
/// `FORKED` or `HAS_CHILD_ERROR`.
pub fn ancestors(execution: &Execution, run: &RunInput, frame: FrameId) -> Vec<String> {
    let mut found = Vec::new();
    let mut at = frame;
    while let Some(origin) = execution.origin_of(at) {
        found.push(aggregator_path(execution, run, origin));
        at = origin.parent;
    }
    found
}

/// A kind as the untyped `parent_kind` column and the `node` envelope spell it.
///
/// Hand-written rather than derived from the serde impl, and public, for the
/// two reasons `pneuma-driver` gave when it held the only copy: it pins a
/// *wire* spelling, which is worth a test of its own however few callers it
/// has; and only an aggregator is ever a parent, so reaching the `Model` and
/// `Condition` arms through a caller is impossible — a spelling checked only
/// through its callers is a spelling half checked.
///
/// It lives here rather than in `pneuma-driver` because both transports need
/// it now, and two copies of a total mapping between two enums is how the two
/// come to disagree.
pub fn wire_kind(kind: NodeKind) -> &'static str {
    match kind {
        NodeKind::Model => "Model",
        NodeKind::ListAggregator => "ListAggregator",
        NodeKind::DictAggregator => "DictAggregator",
        NodeKind::Condition => "Condition",
    }
}

/// Which kind a resolved step is.
///
/// Public for the same reason as [`wire_kind`]: a total mapping between two
/// enums, checked through the one pipeline a fixture happens to contain leaves
/// most of it unchecked.
pub fn kind_of(step: &Step) -> NodeKind {
    match step {
        Step::Model { .. } => NodeKind::Model,
        Step::ListAggregator { .. } => NodeKind::ListAggregator,
        Step::DictAggregator { .. } => NodeKind::DictAggregator,
        Step::Condition { .. } => NodeKind::Condition,
    }
}

/// One line, so the coverage tool can see it.
///
/// A multi-line struct literal is one of the shapes `cargo-tarpaulin`'s ptrace
/// engine attributes to its first line only — `docs/verification.md` carries
/// the table — so a record built inline at a call site reads as unreached while
/// a test asserts on what it produced. Every constructor here has a one-line
/// body for that reason, and every call site is one line.
fn moved(path: String, status: NodeStatus, failure: Option<Failure>) -> Record {
    Record::Moved {
        path,
        status,
        failure,
    }
}

/// One line, for the reason [`moved`] gives.
fn produced(path: String, status: NodeStatus, output: Value) -> Record {
    Record::Produced {
        path,
        status,
        output,
    }
}

/// What a step being dispatched writes down.
///
/// `CREATED` then `PROCESSING`, then every enclosing aggregator `FORKED`.
///
/// Two writes for the one step, and the second is not redundant: the original
/// inserts the row before the send and
/// moves it when the worker's event arrives, and here both
/// instants are the same — but `FORKED` is admitted only *from* `CREATED`
/// (`pneuma_core::status::NodeStatus::admit`), so collapsing them would leave
/// every aggregator unable to fork.
///
/// # `FORKED` is not written here
///
/// The original recurses up `parent_slug` on every child start and marks each
/// ancestor `FORKED`,
/// relying on the statement's guard — `NOT (status <> 'CREATED' AND $2 =
/// 'FORKED')` — to make every repeat a no-op. Faithful, and here it would be
/// expensive in a way it is not there: under Restate each of those writes is
/// its own journal entry, durable, round-tripped, and re-read on every replay,
/// so a hundred-branch fan-out nested two deep would journal two hundred
/// entries whose effect after the first is nil.
///
/// It is also unnecessary. [`Happening::FannedOut`] announces an aggregator
/// exactly once, before anything inside it exists, and a step can only be
/// inside a branch because that announcement already happened — so marking the
/// aggregator there writes `FORKED` once per aggregator and reaches the same
/// state. The design notes record the divergence.
pub fn dispatching(
    execution: &Execution,
    registry: &StepRegistry,
    run: &RunInput,
    task: &Task,
) -> Vec<Record> {
    let (frame, node) = (task.frame, &task.node_id);
    let path = step_path(execution, run, frame, node);
    let mut records = Vec::new();
    if let Some(step) = new_step(execution, registry, run, frame, node, Some(&task.input)) {
        records.push(Record::Created(Box::new(step)));
    }
    records.push(moved(path, NodeStatus::Processing, None));
    records
}

/// What a step's success writes down.
pub fn finishing(execution: &Execution, run: &RunInput, task: &Task, output: Value) -> Vec<Record> {
    let path = step_path(execution, run, task.frame, &task.node_id);
    vec![produced(path, NodeStatus::Finished, output)]
}

/// What a step's failure writes down, and what it tells its ancestors.
///
/// An ancestor's `error_message` is the failing *child's* path rather than the
/// error text, which is what the original writes:
/// an aggregator did not itself
/// fail, and the useful thing to know about it is which of its children did.
///
/// The status is `ERROR` and never `TIMED_OUT`, and that is forced rather than
/// chosen. [`crate::driver::Component`]'s error is opaque to this crate by
/// design — it "never inspects it, only attributes it to a step" — so the layer
/// that could tell a timeout from a refusal is the transport, and it does not
/// say. The design notes record it.
pub fn failing(
    execution: &Execution,
    run: &RunInput,
    task: &Task,
    failure: Failure,
) -> Vec<Record> {
    let path = step_path(execution, run, task.frame, &task.node_id);
    let mut records = vec![moved(path.clone(), NodeStatus::Error, Some(failure))];
    records.extend(blamed_ancestors(execution, run, task.frame, &path));
    records
}

/// Every enclosing aggregator, told which descendant failed.
fn blamed_ancestors(
    execution: &Execution,
    run: &RunInput,
    frame: FrameId,
    failed: &str,
) -> Vec<Record> {
    ancestors(execution, run, frame)
        .into_iter()
        .map(|ancestor| {
            let blamed = Failure {
                code: CHILD_FAILED.to_owned(),
                message: failed.to_owned(),
            };
            moved(ancestor, NodeStatus::HasChildError, Some(blamed))
        })
        .collect()
}

/// What one thing the interpreter did writes down.
///
/// Aggregators and conditionals never become tasks, so without this the record
/// would hold only model steps — and a run's shape is mostly the steps that are
/// not model steps.
pub fn happened(
    execution: &Execution,
    registry: &StepRegistry,
    run: &RunInput,
    happening: &Happening,
) -> Vec<Record> {
    match happening {
        // Announced before anything inside it exists, which is what makes the
        // foreign key on `parent_path` satisfiable. The zero-width case
        // announces itself here too, so an aggregator's row is created by
        // exactly one kind of entry.
        Happening::FannedOut {
            frame, aggregator, ..
        } => {
            let mut records = created_only(execution, registry, run, *frame, aggregator);
            // `FORKED` here and nowhere else. See [`dispatching`] for why the
            // original's per-child recursion is not reproduced.
            let path = step_path(execution, run, *frame, aggregator);
            records.push(moved(path, NodeStatus::Forked, None));
            records
        }
        // Not `FORKED`: an abandoned aggregator never started a branch, and a
        // row left at `CREATED` with no error says nothing about why the run
        // stopped. `Execution::abandon` is the only route to
        // `DriveError::Stalled`, so without this the mirror's answer to "which
        // step is this run stuck on" is a `CREATED` row indistinguishable from
        // one that is about to run.
        Happening::Abandoned { frame, aggregator } => {
            let mut records = created_only(execution, registry, run, *frame, aggregator);
            let path = step_path(execution, run, *frame, aggregator);
            let failure = Failure {
                code: ABANDONED.to_owned(),
                message: format!("every way into {aggregator} was blocked"),
            };
            records.push(moved(path.clone(), NodeStatus::Error, Some(failure)));
            records.extend(blamed_ancestors(execution, run, *frame, &path));
            records
        }
        Happening::Aggregated {
            frame,
            aggregator,
            output,
        } => {
            let path = step_path(execution, run, *frame, aggregator);
            vec![produced(path, NodeStatus::Aggregated, output.clone())]
        }
        Happening::Resolved {
            frame,
            node,
            output,
        } => {
            let mut records = created_only(execution, registry, run, *frame, node);
            let path = step_path(execution, run, *frame, node);
            records.push(produced(path, NodeStatus::Finished, output.clone()));
            records
        }
    }
}

/// A step's row and nothing else.
fn created_only(
    execution: &Execution,
    registry: &StepRegistry,
    run: &RunInput,
    frame: FrameId,
    node: &NodeId,
) -> Vec<Record> {
    // `into_iter` on the `Option` rather than a `match`, so the empty case is
    // the absence of an element rather than an arm no input can take. The
    // registry not knowing the node is the same condition `drive` turns into
    // `UnknownStep`, and a row for a step that does not exist would put a
    // foreign key on nothing.
    new_step(execution, registry, run, frame, node, None)
        .into_iter()
        .map(|step| Record::Created(Box::new(step)))
        .collect()
}

/// The `error_code` an ancestor carries when a descendant failed.
pub const CHILD_FAILED: &str = "PNEUMA_CHILD_ERROR";
/// The `error_code` for a component that could not be reached.
pub const TRANSPORT_ERROR: &str = "PNEUMA_TRANSPORT_ERROR";
/// The `error_code` for a component that answered unusably.
pub const UNUSABLE_RESPONSE: &str = "PNEUMA_UNUSABLE_RESPONSE";
/// The `error_code` for an aggregator every way into which was blocked.
pub const ABANDONED: &str = "PNEUMA_ABANDONED";
