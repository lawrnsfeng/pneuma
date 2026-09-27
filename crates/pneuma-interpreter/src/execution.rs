//! Walking a whole pipeline, nested aggregators included.
//!
//! [`schedule`](crate::schedule) decides what may run inside *one* graph.
//! An aggregator's children are a different graph — bounded to its
//! `component_ids`, run once per branch — so executing a real pipeline means
//! a *tree* of schedulers, not one. This module owns that tree.
//!
//! # What the driver sees
//!
//! [`Execution::next_task`] hands out one [`Task`] at a time and
//! [`Execution::report`] takes the result back. Only **model** steps become
//! tasks. Aggregators and conditionals are resolved here, because the original
//! resolves them itself rather than dispatching them to a component:
//!
//! * an aggregator fans out and later aggregates its branches' outputs
//!   ;
//! * a conditional is completed inline with its own input as its output —
//!   `process_result(..., step_output=step_input)`.
//!
//! A driver that dispatched either to an AI component would be asking a model
//! to evaluate `equals`.
//!
//! # Why a tree and not recursion
//!
//! The obvious implementation runs each branch with a recursive call. It was
//! the first one here, and it has two faults. It puts the corpus's four levels
//! of nesting on the Rust stack, and — the real problem — it forces branches to
//! run one after another, because a branch's whole execution is a single stack
//! frame. The original runs branches concurrently; each is an independent
//! message. So frames live in an arena and [`Execution::next_task`] will hand
//! out work from any live branch, which is what lets a driver run siblings at
//! the same time.
//!
//! # Zero-width fan-out
//!
//! A list aggregator over an empty list has no branches, and aggregates
//! immediately to `[]`. The original wedges there — its aggregation is only
//! ever attempted when a *child* finishes, and there are no children, so the
//! run stops for good with no error. The defect notes record it.

use std::collections::BTreeSet;

use pneuma_core::child_index::ChildIndex;
use pneuma_core::ids::NodeId;
use pneuma_core::step::{AggregatorRefs, Step, StepRegistry};
use serde_json::Value;

use crate::schedule::{
    aggregate, branch_outputs, fanout_of, successors_of, Fanout, Ready, ScheduleError, Scheduler,
};

/// Identifies one scheduler in the execution tree.
///
/// Opaque, and only meaningful to the [`Execution`] that issued it. Two
/// branches of the same aggregator run the same node ids, so a node id alone
/// does not say which result belongs where — the frame is what disambiguates.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct FrameId(usize);

impl FrameId {
    /// The run itself — the frame every execution starts with.
    pub const ROOT: FrameId = FrameId(0);
}

/// A step for the driver to run, and the input it takes.
#[derive(Debug, Clone, PartialEq)]
pub struct Task {
    /// Which branch this step belongs to. Give it back to [`Execution::report`].
    pub frame: FrameId,
    /// Which step.
    pub node_id: NodeId,
    /// Its input.
    pub input: Value,
}

/// What one pass over the frames managed to do.
///
/// The distinction that matters is between *advanced* and *idle*: resolving an
/// aggregator or a conditional produces no task but is progress, and the caller
/// must look again. Collapsing the two into `Option<Task>` was the first
/// version, and it stopped every run the moment it hit a fan-out.
enum Progress {
    /// A step for the driver.
    Task(Task),
    /// Something was resolved here; ask again.
    Advanced,
    /// Nothing runnable in any frame.
    Idle,
}

/// Where a frame came from.
///
/// Absent on the root frame and present on every other, because every other
/// frame is one branch of some aggregator's fan-out.
///
/// This exists for one reason: a fan-out runs the *same node ids* in every
/// branch, so a node id and a run id do not identify a step. The original
/// disambiguates with a `:{child_idx}` suffix on the slug,
/// and anything recording a run's
/// steps has to do the same or two branches collide on one identity. The
/// interpreter has always known this and always discarded it: `SubRun` carries
/// a `child_index` (`crate::schedule`) that `start_fanout` used to drop on the
/// floor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Origin {
    /// The frame the aggregator that opened this one runs in.
    pub parent: FrameId,
    /// The aggregator itself.
    pub aggregator: NodeId,
    /// This branch's 1-based position among that aggregator's branches.
    pub child_index: ChildIndex,
}

/// Something the interpreter did that a caller cannot see from the outside.
///
/// [`Execution::next_task`] and [`Execution::report`] describe the *dispatch*
/// half of a run, and a caller watching only those sees model steps and nothing
/// else. Aggregators and conditionals never become tasks — they are resolved in
/// here — so a caller trying to record what a run actually did would be missing
/// exactly the steps that give it its shape.
///
/// These are facts, not instructions. The interpreter does not care whether
/// anybody drains them, and dropping the whole log changes nothing about how a
/// run executes.
///
/// # Order is part of the contract
///
/// [`Happening::FannedOut`] names the aggregator *and* its branches in one
/// entry, before anything in those branches has begun. A consumer writing a row
/// per step needs the parent to exist before its children — the `node_run`
/// table's `parent_path` is a non-deferrable foreign key onto `path` — and this
/// is the same ordering the original gets by construction, inserting the
/// aggregator's row before the recursive child inserts.
#[derive(Debug, Clone, PartialEq)]
pub enum Happening {
    /// An aggregator opened a fan-out.
    FannedOut {
        /// The frame the aggregator runs in.
        frame: FrameId,
        /// The aggregator.
        aggregator: NodeId,
        /// One entry per branch, in branch order: the frame it runs in, and its
        /// 1-based position.
        branches: Vec<(FrameId, ChildIndex)>,
    },
    /// An aggregator collected its branches and completed.
    ///
    /// Always preceded by a [`Happening::FannedOut`] for the same aggregator,
    /// including the zero-width case, whose `branches` is empty and whose
    /// output is `[]` — the defect notes, where the original wedges
    /// instead. A consumer therefore creates a row on `FannedOut` alone.
    Aggregated {
        /// The frame the aggregator runs in.
        frame: FrameId,
        /// The aggregator.
        aggregator: NodeId,
        /// What it aggregated to.
        output: Value,
    },
    /// A conditional was resolved inline; its output is its input.
    Resolved {
        /// The frame it ran in.
        frame: FrameId,
        /// The conditional.
        node: NodeId,
        /// Its output, which is the input it selected on.
        output: Value,
    },
    /// An aggregator could not start: every declared way in was blocked.
    ///
    /// Distinct from an empty fan-out, and the difference is the whole of
    /// the defect notes's second half — one aggregates to `[]`, the
    /// other must not be given a fabricated output.
    Abandoned {
        /// The frame the aggregator runs in.
        frame: FrameId,
        /// The aggregator.
        aggregator: NodeId,
    },
}

/// An aggregator whose branches have not all finished.
///
/// The parent *pulls* when every branch has come to rest, rather than each
/// branch pushing its result up as it finishes. Pushing needs a slot table, a
/// counter, and a back-reference from each child, and every one of those
/// lookups is infallible by construction yet has to be written as though it
/// could fail. Pulling needs none of them, and the branch order is simply the
/// order the children were created in.
#[derive(Debug, Clone)]
struct Pending {
    /// Which aggregator, in the frame that holds this.
    aggregator: NodeId,
    /// Read the branches' outputs from their terminal children.
    refs: AggregatorRefs,
    /// One frame per branch, in branch order.
    children: Vec<FrameId>,
}

/// One scheduler in the tree.
#[derive(Debug, Clone)]
struct Frame {
    scheduler: Scheduler,
    /// Fan-outs started from this frame that are still running.
    pending: Vec<Pending>,
    /// Which branch of which aggregator this is. `None` on the root.
    origin: Option<Origin>,
}

impl Frame {
    /// Nothing to run, nothing out, and no branch still going.
    fn is_done(&self) -> bool {
        self.scheduler.is_quiet() && self.pending.is_empty()
    }
}

/// A pipeline part-way through running.
#[derive(Debug, Clone)]
pub struct Execution {
    frames: Vec<Frame>,
    ran: Vec<NodeId>,
    /// Frames that may have something ready.
    ///
    /// Without it every `next_task` rescans the whole arena from zero, so a
    /// fan-out over N items costs O(N) per task and O(N²) over the run. The
    /// corpus never notices — 33 steps at most — but this is the path that
    /// ships, and a real fan-out is one branch per page of a document.
    ///
    /// Only ever an over-approximation: a frame is added whenever something
    /// might have released work in it, and removed when it is found to have
    /// none. Being wrong in that direction costs one wasted look; being wrong
    /// the other way would lose work.
    active: BTreeSet<usize>,
    /// Frames holding an unfinished fan-out, so settling scans those alone.
    with_pending: BTreeSet<usize>,
    /// What the interpreter did that a caller cannot see — see [`Happening`].
    ///
    /// Appended to unconditionally and never read by anything in this module.
    /// A run executes identically whether or not the log is ever drained, which
    /// is what makes it safe to grow: nothing here can be broken by a consumer
    /// that ignores it.
    happenings: Vec<Happening>,
}

impl Execution {
    /// Starts a run.
    pub fn new(registry: &StepRegistry, input: Value) -> Self {
        Execution {
            frames: vec![Frame {
                scheduler: Scheduler::new(registry, input),
                pending: Vec::new(),
                origin: None,
            }],
            ran: Vec::new(),
            active: std::iter::once(0).collect(),
            with_pending: BTreeSet::new(),
            happenings: Vec::new(),
        }
    }

    /// Takes everything the interpreter has done since this was last called.
    ///
    /// Draining rather than reading, so a caller that records each batch cannot
    /// record one twice — and so the log cannot grow without bound across a
    /// long run. A caller that never calls this pays a `Vec` push per resolved
    /// aggregator or conditional and nothing else.
    pub fn drain_happenings(&mut self) -> Vec<Happening> {
        std::mem::take(&mut self.happenings)
    }

    /// Which branch of which aggregator a frame is, if it is one.
    ///
    /// [`FrameId::ROOT`] answers `None`, and so would a frame from another
    /// execution — but the ids are opaque and only meaningful to the execution
    /// that issued them, so that case is a caller mixing two runs rather than
    /// something this can be asked about.
    ///
    /// Walking this upward is how a caller builds the path the original spells
    /// `{parent_slug}.{node_id}:{child_idx}`, which is the only identity that
    /// distinguishes two branches running the same node.
    pub fn origin_of(&self, frame: FrameId) -> Option<&Origin> {
        self.frames.get(frame.0).and_then(|f| f.origin.as_ref())
    }

    /// The next model step to run, or `None` when there is nothing to hand out.
    ///
    /// `None` does not mean finished. It also means every runnable step is
    /// already out with the driver, or that the run is wedged — see
    /// [`Execution::is_finished`] and [`Execution::stalled`], which separate the
    /// three.
    ///
    /// Aggregators and conditionals are resolved here rather than returned, so
    /// a driver only ever receives steps that correspond to an actual
    /// component call.
    pub fn next_task(&mut self, registry: &StepRegistry) -> Result<Option<Task>, ScheduleError> {
        loop {
            match self.take_ready(registry)? {
                Progress::Task(task) => return Ok(Some(task)),
                Progress::Advanced => continue,
                // Nothing runnable anywhere. A branch that has come to rest
                // hands its outputs up, which may release its parent's next
                // step, so this repeats until neither makes progress.
                Progress::Idle => {
                    if !self.settle(registry)? {
                        return Ok(None);
                    }
                }
            }
        }
    }

    /// Takes one ready step from any live frame, resolving what it can.
    ///
    /// [`Progress::Idle`] only when no frame has anything ready at all;
    /// resolving an aggregator or conditional is [`Progress::Advanced`].
    fn take_ready(&mut self, registry: &StepRegistry) -> Result<Progress, ScheduleError> {
        // Lowest frame first, so a run is reproducible.
        while let Some(index) = self.active.iter().next().copied() {
            // Indexed directly, as everywhere else in this file: `active`
            // holds only indices this type put there, either an existing
            // frame's or `frames.len()` immediately before the push. A
            // defensive `get_mut` here would be a branch nothing can take.
            let Some(ready) = self.frames[index].scheduler.next_ready() else {
                // Nothing here now. It goes back the moment something releases
                // work into it.
                self.active.remove(&index);
                continue;
            };
            return self.begin(registry, FrameId(index), ready);
        }
        Ok(Progress::Idle)
    }

    /// Starts one step that has just been taken off a frame's queue.
    ///
    /// Every failure here puts the step back. `next_ready` has already popped
    /// it and marked it in flight, so returning an error without requeueing
    /// drops it: not runnable, not running, not done, and not recoverable by
    /// retrying. `Scheduler::complete` is written to the same rule — it commits
    /// nothing until everything that can fail has succeeded.
    fn begin(
        &mut self,
        registry: &StepRegistry,
        frame: FrameId,
        ready: Ready,
    ) -> Result<Progress, ScheduleError> {
        let Some(step) = registry.get(&ready.node_id) else {
            let node_id = ready.node_id.clone();
            self.frames[frame.0].scheduler.requeue(ready);
            return Err(ScheduleError::UnknownStep { node_id });
        };
        let fanout = match fanout_of(registry, step, &ready.input) {
            Ok(fanout) => fanout,
            Err(error) => {
                self.frames[frame.0].scheduler.requeue(ready);
                return Err(error);
            }
        };
        if let Some(fanout) = fanout {
            return self.start_fanout(registry, frame, ready, fanout);
        }
        if matches!(step, Step::Condition { .. }) {
            // `process_result(..., step_output=step_input)` -- a
            // conditional's output *is* its input; it selects a branch, it does
            // not produce anything. Completing it here evaluates the conditions
            // and releases the branch taken.
            let input = ready.input.clone();
            let input_for_log = input.clone();
            if let Err(error) =
                self.frames[frame.0]
                    .scheduler
                    .complete(registry, &ready.node_id, input)
            {
                // `complete` clears `dispatched` before its fallible work, so
                // without this the conditional is in neither `ready`,
                // `dispatched` nor `done` -- unrunnable, and the error not
                // repeatable, so the failure quietly becomes a stall.
                self.frames[frame.0].scheduler.requeue(ready);
                return Err(error);
            }
            // After `complete`, not before: a conditional that failed to
            // complete was requeued and has not run, and a log entry for it
            // would say otherwise. Every emit in this file follows the same
            // rule the `ran` push does.
            self.happenings.push(Happening::Resolved {
                frame,
                node: ready.node_id.clone(),
                output: input_for_log,
            });
            self.ran.push(ready.node_id);
            self.active.insert(frame.0);
            return Ok(Progress::Advanced);
        }
        // Recorded only once the step is committed. Pushing before the fallible
        // work counted a step that was requeued and never ran.
        self.ran.push(ready.node_id.clone());
        Ok(Progress::Task(Task {
            frame,
            node_id: ready.node_id,
            input: ready.input,
        }))
    }

    /// Opens a fan-out: one child frame per branch.
    fn start_fanout(
        &mut self,
        registry: &StepRegistry,
        frame: FrameId,
        ready: Ready,
        fanout: Fanout<'_>,
    ) -> Result<Progress, ScheduleError> {
        let node_id = &ready.node_id;
        if fanout.branches.is_empty() {
            if fanout.blocked_starts > 0 {
                // Not a zero-width fan-out: a body with no way in, because
                // every declared start is waiting on a prerequisite that only
                // something inside the body could supply. Completing it with
                // `[]` would fabricate an output and run the rest of the
                // pipeline off it.
                //
                // `abandon` rather than simply returning: `next_ready` has
                // marked the aggregator in flight, and nothing will ever report
                // it, so leaving it there makes the run look permanently busy
                // instead of permanently blocked. Abandoning clears that while
                // keeping it in `started`, which is what `stalled()` reads.
                self.frames[frame.0].scheduler.abandon(node_id);
                self.happenings.push(Happening::Abandoned {
                    frame,
                    aggregator: ready.node_id.clone(),
                });
                self.ran.push(ready.node_id);
                return Ok(Progress::Advanced);
            }
            // No branches, so nothing will ever finish under this aggregator.
            // Aggregating now is the whole of the defect notes: the
            // original waits for a child that cannot arrive, and the run stops
            // for good with no error and no log line.
            if let Err(error) =
                self.frames[frame.0]
                    .scheduler
                    .complete(registry, node_id, aggregate(Vec::new()))
            {
                self.frames[frame.0].scheduler.requeue(ready);
                return Err(error);
            }
            // A fan-out into nothing is still a fan-out, and saying so keeps
            // the log uniform: every aggregator is announced exactly once, by
            // a `FannedOut`, whether or not it has branches. A consumer that
            // had to create the row on `Aggregated` as well -- because this
            // case would otherwise never announce itself -- would write every
            // ordinary aggregator's row twice.
            self.happenings.push(Happening::FannedOut {
                frame,
                aggregator: ready.node_id.clone(),
                branches: Vec::new(),
            });
            self.happenings.push(Happening::Aggregated {
                frame,
                aggregator: ready.node_id.clone(),
                output: aggregate(Vec::new()),
            });
            // The aggregator started and completed here, so it ran. Moving the
            // record out of the common prologue and into each committed path
            // left this one without it, and `ran()` under-reported the
            // aggregator on the very path §21 exists for.
            self.ran.push(ready.node_id);
            self.active.insert(frame.0);
            return Ok(Progress::Advanced);
        }

        // Paired with its branch's position, which is the thing this used to
        // discard. Two branches of one fan-out run the same node ids, so
        // without it nothing downstream can tell them apart -- see [`Origin`].
        let children: Vec<(Scheduler, ChildIndex)> = {
            let parent = &self.frames[frame.0].scheduler;
            fanout
                .branches
                .iter()
                .map(|branch| {
                    let scheduler = Scheduler::scoped(
                        std::slice::from_ref(&branch.start),
                        fanout.scope(),
                        branch.input.clone(),
                        parent,
                        fanout.enclosing_dict,
                    );
                    (scheduler, branch.child_index)
                })
                .collect()
        };

        let first = FrameId(self.frames.len());
        let branches: Vec<(FrameId, ChildIndex)> = children
            .iter()
            .enumerate()
            .map(|(offset, (_, child_index))| (FrameId(first.0 + offset), *child_index))
            .collect();
        self.frames[frame.0].pending.push(Pending {
            aggregator: node_id.clone(),
            refs: fanout.refs.clone(),
            children: branches.iter().map(|(frame, _)| *frame).collect(),
        });
        // Before the branches exist, which is the ordering a consumer writing a
        // row per step depends on -- see [`Happening`].
        self.happenings.push(Happening::FannedOut {
            frame,
            aggregator: node_id.clone(),
            branches,
        });
        // Cloned out of the borrow before `ready` is consumed: `node_id`
        // borrows it, and each frame below needs its own copy anyway.
        let aggregator = node_id.clone();
        self.ran.push(ready.node_id);
        self.with_pending.insert(frame.0);
        for (scheduler, child_index) in children {
            self.active.insert(self.frames.len());
            self.frames.push(Frame {
                scheduler,
                pending: Vec::new(),
                origin: Some(Origin {
                    parent: frame,
                    aggregator: aggregator.clone(),
                    child_index,
                }),
            });
        }
        Ok(Progress::Advanced)
    }

    /// Completes one aggregator whose every branch has come to rest.
    ///
    /// Returns whether anything moved, so the caller knows to look for work
    /// again.
    fn settle(&mut self, registry: &StepRegistry) -> Result<bool, ScheduleError> {
        // Two immutable borrows -- the pending list and, inside the predicate,
        // the frames those branches live in -- then one mutable removal after
        // both have ended. `position` came from this very list and nothing
        // touched it in between, so there is no "not found" case to invent.
        let mut found: Option<(usize, usize)> = None;
        for index in self.with_pending.iter().copied() {
            if let Some(position) = self.frames[index].pending.iter().position(|pending| {
                // Done, and with nothing begun that never finished. A branch
                // whose step was abandoned is quiet and therefore "done", and
                // `branch_outputs` filters the missing output away -- so
                // aggregating it yields a silently short array and a run that
                // reports success. That has to stop here and stay visible.
                //
                // `has_unfinished_started`, *not* `stalled`. `stalled` is the
                // looser, advisory question: it also names a join whose
                // prerequisites can never all arrive because a conditional took
                // the other branch, which is an ordinary outcome. Gating on it
                // blocked every aggregator containing a conditional for ever,
                // which is worse than the bug it was fixing.
                pending.children.iter().all(|child| {
                    self.frames[child.0].is_done()
                        && !self.frames[child.0].scheduler.has_unfinished_started()
                })
            }) {
                found = Some((index, position));
                break;
            }
        }
        let Some((index, position)) = found else {
            return Ok(false);
        };
        // Read the fan-out without consuming it. Removing it first and then
        // failing in `complete` would destroy the record: the aggregator is not
        // in `done`, `complete` has already cleared it from `dispatched`, and
        // with no `Pending` left nothing can ever recompute the aggregate or
        // raise the error again. Same rule `complete` itself follows -- commit
        // nothing until everything that can fail has succeeded.
        //
        // Flattened in branch order. Every branch shares one `parent_slug`, so
        // the original's single `get_by_parent_slug` spans them all and yields
        // one flat array -- see `branch_outputs`.
        let (aggregator, collected) = {
            let pending = &self.frames[index].pending[position];
            let collected: Vec<Value> = pending
                .children
                .iter()
                .flat_map(|child| branch_outputs(&pending.refs, &self.frames[child.0].scheduler))
                .collect();
            (pending.aggregator.clone(), collected)
        };
        let output = aggregate(collected);
        self.frames[index]
            .scheduler
            .complete(registry, &aggregator, output.clone())?;
        self.happenings.push(Happening::Aggregated {
            frame: FrameId(index),
            aggregator,
            output,
        });

        self.frames[index].pending.remove(position);
        if self.frames[index].pending.is_empty() {
            self.with_pending.remove(&index);
        }
        self.active.insert(index);
        Ok(true)
    }

    /// Reports a step's output.
    pub fn report(
        &mut self,
        registry: &StepRegistry,
        task: &Task,
        output: Value,
    ) -> Result<(), ScheduleError> {
        let Some(frame) = self.frames.get_mut(task.frame.0) else {
            return Err(ScheduleError::UnknownStep {
                node_id: task.node_id.clone(),
            });
        };
        frame.scheduler.complete(registry, &task.node_id, output)?;
        self.active.insert(task.frame.0);
        Ok(())
    }

    /// Reports that a step will never complete. See [`Scheduler::abandon`].
    pub fn abandon(&mut self, task: &Task) {
        if let Some(frame) = self.frames.get_mut(task.frame.0) {
            frame.scheduler.abandon(&task.node_id);
        }
    }

    /// Whether the run came to rest with nothing left blocked.
    ///
    /// Not merely "everything is quiet". Two different things make a quiet run
    /// unfinished, and both have to be asked:
    ///
    /// * a step *begun and never finished* — abandoned, or a completion that
    ///   failed and was not retried;
    /// * a run that began nothing at all, because every declared start has
    ///   prerequisites and so none was seeded. Nothing is ever handed out
    ///   there, so the first question answers "no" and zero steps running
    ///   reads as success.
    ///
    /// Deliberately *not* [`Scheduler::stalled`], which is broader than either:
    /// it also names a join whose prerequisites can never all arrive because a
    /// conditional took the other branch, and that run has finished perfectly
    /// well. Three versions got this wrong in three different ways: gating on
    /// `stalled` refused to finish those runs; dropping `stalled` entirely lost
    /// the never-started guard; and requiring every declared start to be `done`
    /// wedged a run whenever a start sat on a branch the conditional did not
    /// take. The two questions below are narrow on purpose.
    pub fn is_finished(&self) -> bool {
        self.frames.iter().all(|frame| {
            frame.is_done()
                && !frame.scheduler.has_unfinished_started()
                && !frame.scheduler.started_nothing()
        })
    }

    /// What the run produced: the outputs of top-level steps nothing follows.
    ///
    /// The original concludes a run with `step_output` from whichever such step
    /// finishes — `if not parent_id: await self.conclude_run(run, meta,
    /// step_output)`, reached once per
    /// top-level step with no `next_nodes`.
    ///
    /// **Once per such step**, which is why this returns every one of them
    /// rather than a single value. Nothing forbids a pipeline having two: `A ->
    /// {B, C}` with both ending is an ordinary fan-out without an aggregator,
    /// and none of the resolver's six rejection rules covers it. The original
    /// then concludes the run twice — the run's recorded output is whichever
    /// finished last, and the customer's completion callback fires twice
    /// (route B in the defect notes). Handing back a list makes that
    /// visible to a caller instead of picking one and inheriting the
    /// nondeterminism silently.
    ///
    /// Only the run's own frame. A branch's terminal children are the
    /// aggregator's business and are already folded into its output.
    ///
    /// Terminality is decided by re-asking [`successors_of`] with the step's
    /// recorded output, so it is the same rule that decided what ran — a
    /// conditional whose taken branch is `end` is terminal, and one that took a
    /// branch is not.
    pub fn run_output(
        &self,
        registry: &StepRegistry,
    ) -> Result<Vec<(NodeId, Value)>, ScheduleError> {
        // Indexed, as everywhere else in this file: `new` pushes the root and
        // nothing ever pops a frame, so the arena is never empty. A defensive
        // `first()` here would be a branch nothing can take.
        let root = &self.frames[FrameId::ROOT.0];
        let mut finished = Vec::new();
        for step in registry.iter() {
            let node_id = &step.common().node_id;
            let Some(output) = root.scheduler.output_of(node_id) else {
                continue;
            };
            if successors_of(step, output)?.is_empty() {
                finished.push((node_id.clone(), output.clone()));
            }
        }
        Ok(finished)
    }

    /// Steps blocking the run, across every frame. See [`Scheduler::stalled`].
    ///
    /// Only meaningful once [`Execution::next_task`] has returned `None`:
    /// before that a frame may be quiet merely because its branches have not
    /// been settled yet.
    pub fn stalled(&self) -> Vec<(FrameId, NodeId)> {
        self.frames
            .iter()
            .enumerate()
            .flat_map(|(index, frame)| {
                frame
                    .scheduler
                    .stalled()
                    .into_iter()
                    .map(move |node_id| (FrameId(index), node_id))
            })
            .collect()
    }

    /// Every step that has been started, in the order it started.
    ///
    /// A step inside a fan-out appears once per branch, which is the point of a
    /// fan-out and not a repeat.
    pub fn ran(&self) -> &[NodeId] {
        &self.ran
    }

    /// One frame's scheduler, for reading outputs.
    pub fn scheduler(&self, frame: FrameId) -> Option<&Scheduler> {
        self.frames.get(frame.0).map(|frame| &frame.scheduler)
    }

    /// How many frames the run has opened — one plus one per branch.
    pub fn frame_count(&self) -> usize {
        self.frames.len()
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use pneuma_core::node::Pipeline;
    use pneuma_core::resolver::resolve;

    use super::*;

    fn corpus(name: &str) -> StepRegistry {
        let path = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../pneuma-core/tests/fixtures/pipelines/"
        );
        let Ok(text) = std::fs::read_to_string(format!("{path}{name}.yaml")) else {
            panic!("{name}.yaml should be readable");
        };
        let Ok(pipeline) = serde_yaml::from_str::<Pipeline>(&text) else {
            panic!("{name}.yaml should parse");
        };
        match resolve(&pipeline) {
            Ok(registry) => registry,
            Err(error) => panic!("{name}.yaml should resolve: {error}"),
        }
    }

    /// Resolves an inline pipeline.
    ///
    /// The corpus is the right input for anything the corpus covers, and
    /// [`corpus`] says why. This is for a shape the corpus happens not to
    /// contain but the system plainly does produce — it resolves under the
    /// same rules, and the gap is in the fixture set rather than in reality.
    /// Used sparingly, and only where that is the point of the test.
    fn resolved(yaml: &str) -> StepRegistry {
        let Ok(pipeline) = serde_yaml::from_str::<Pipeline>(yaml) else {
            panic!("the inline pipeline should parse");
        };
        match resolve(&pipeline) {
            Ok(registry) => registry,
            Err(error) => panic!("the inline pipeline should resolve: {error}"),
        }
    }

    /// Runs a pipeline with a stub component, one task at a time.
    ///
    /// The stub mirrors the component contract rather than the harness's
    /// convenience: a step feeding a list aggregator emits a list, and every
    /// step emits an object, because `{"step_output": ...}`'s payload is
    /// always an object. Passing the input through keeps a downstream
    /// conditional able to read its keys.
    pub(crate) fn drive(registry: &StepRegistry, input: Value) -> Result<Execution, ScheduleError> {
        let mut execution = Execution::new(registry, input);
        while let Some(task) = execution.next_task(registry)? {
            let output = stub_output(registry, &task)?;
            execution.report(registry, &task, output)?;
        }
        Ok(execution)
    }

    fn stub_output(registry: &StepRegistry, task: &Task) -> Result<Value, ScheduleError> {
        let Some(step) = registry.get(&task.node_id) else {
            return Err(ScheduleError::UnknownStep {
                node_id: task.node_id.clone(),
            });
        };
        for id in crate::schedule::successors_of(step, &Value::Null)? {
            let Some(next) = registry.get(&id) else {
                return Err(ScheduleError::UnknownSuccessor {
                    from: task.node_id.clone(),
                    missing: id,
                });
            };
            if matches!(next, Step::ListAggregator { .. }) {
                return Ok(Value::Array(vec![task.input.clone()]));
            }
        }
        if task.input.is_object() {
            return Ok(task.input.clone());
        }
        Ok(serde_json::json!({ "step_output": task.input }))
    }

    fn corpus_input() -> Value {
        serde_json::json!({
            "doc": "d",
            "items": [1, 2],
            "equals": "value",
            "contains": "a value here",
            "isempty": "",
            "isnotempty": "something",
            "resultB": "value",
            "resultC": "a value here"
        })
    }

    #[test]
    fn a_list_aggregator_over_an_empty_list_aggregates_immediately() {
        // The defect notes The original records `num_children = 0`,
        // starts nothing, and only ever attempts aggregation when a *child*
        // finishes -- so the run stops for good, with no error and no log line.
        // Here a zero-width fan-out is complete the moment it opens.
        // The empty list has to reach the aggregator by a path that does not
        // coerce it. A single-prerequisite edge turns `[]` into `{}` first
        // (`step_input or {}`, the original) and the aggregator then raises --
        // which is the accidental protection §21 notes, and covers only that
        // one path. `case 0` does not gather, so a successor recorded with no
        // prerequisites takes the `[]` as given. That is the shape here.
        let mut registry = corpus("pipeline4");
        let Some(Step::ListAggregator { .. }) = registry.get(&NodeId::new("X")) else {
            panic!("pipeline4's X is a list aggregator");
        };
        match registry.get_mut(&NodeId::new("X")) {
            Some(step) => step.common_mut().num_prerequisites = 0,
            None => panic!("pipeline4 has X"),
        }
        let mut execution = Execution::new(&registry, serde_json::json!({"doc": "d"}));
        let mut ran = 0usize;
        while let Some(task) = match execution.next_task(&registry) {
            Ok(task) => task,
            Err(error) => panic!("should schedule: {error}"),
        } {
            let output = if task.node_id == NodeId::new("A") {
                serde_json::json!([])
            } else {
                serde_json::json!({"ran": task.node_id.to_string()})
            };
            let Ok(()) = execution.report(&registry, &task, output) else {
                panic!("should accept");
            };
            ran += 1;
        }
        assert!(
            execution.is_finished(),
            "the run completes rather than wedging"
        );
        assert!(
            execution.stalled().is_empty(),
            "and nothing is blocked: {:?}",
            execution.stalled()
        );
        assert_eq!(execution.frame_count(), 1, "no branch was opened");

        let Some(root) = execution.scheduler(FrameId::ROOT) else {
            panic!("there is always a root");
        };
        assert_eq!(
            root.output_of(&NodeId::new("X")),
            Some(&serde_json::json!([])),
            "an empty fan-out aggregates to an empty array"
        );
        // `A` and `D` ran with a component; `X` did not, and `D` is downstream
        // of the aggregator, so the run really did continue past it.
        assert_eq!(ran, 2, "A and D were dispatched");
        // And `ran()` -- the public record -- includes the aggregator, which
        // started and completed here. Counting dispatched tasks locally is
        // what let a regression through: `X` is never dispatched, so a local
        // count cannot notice it going missing.
        assert_eq!(
            execution.ran(),
            [NodeId::new("A"), NodeId::new("X"), NodeId::new("D")],
            "the aggregator ran, between the two steps that were dispatched"
        );
        assert!(
            root.output_of(&NodeId::new("D")).is_some(),
            "the step after the aggregator ran"
        );
    }

    #[test]
    fn a_task_from_another_execution_is_refused_rather_than_panicking() {
        // `FrameId` is opaque but the API cannot stop a caller handing back a
        // task it did not issue. Refusing beats indexing out of bounds.
        let registry = corpus("pipeline_case1");
        let mut execution = Execution::new(&registry, serde_json::json!({"doc": "d"}));
        let Ok(Some(real)) = execution.next_task(&registry) else {
            panic!("something should be ready");
        };
        let forged = Task {
            frame: FrameId(99),
            node_id: real.node_id.clone(),
            input: real.input.clone(),
        };
        assert!(matches!(
            execution.report(&registry, &forged, serde_json::json!({})),
            Err(ScheduleError::UnknownStep { .. })
        ));
        // `abandon` takes no result, so the check is that it does nothing and
        // does not panic -- the real task is still in flight afterwards.
        execution.abandon(&forged);
        assert!(
            execution.stalled().is_empty(),
            "the real task is still in flight, so nothing is stalled"
        );
        assert!(execution.scheduler(FrameId(99)).is_none(), "no such frame");

        // And abandoning the real one does surface the stall.
        execution.abandon(&real);
        assert!(!execution.is_finished(), "the run did not finish");
    }

    #[test]
    fn a_registry_that_does_not_match_the_execution_is_an_error() {
        // The registry is passed per call rather than held, so nothing stops a
        // caller passing a different one. It is detected, not indexed into.
        let registry = corpus("pipeline_condition_list");
        let other = corpus("pipeline1");
        let mut execution = Execution::new(&registry, corpus_input());
        assert!(
            registry
                .start()
                .node_ids()
                .any(|id| other.get(id).is_none()),
            "this test needs a start the other registry lacks"
        );
        assert!(matches!(
            execution.next_task(&other),
            Err(ScheduleError::UnknownStep { .. })
        ));
    }

    #[test]
    fn no_step_in_the_corpus_produces_a_null_output() {
        // The counts pin only that steps *ran*. Every aggregator output in the
        // corpus was once `null` and every count still passed, because the
        // driver read the last id it happened to execute instead of the
        // branch's terminal children. This looks at what was produced, in
        // every frame -- branches included, which the root alone would miss.
        for name in [
            "pipeline1",
            "pipeline2",
            "pipeline3",
            "pipeline4",
            "pipeline_params",
            "pipeline_case1",
            "pipeline_case2",
            "pipeline_case3",
            "pipeline_condition_dict",
            "pipeline_condition_list",
            "pipeline_nested_list_dict",
        ] {
            let registry = corpus(name);
            let execution = match drive(&registry, corpus_input()) {
                Ok(execution) => execution,
                Err(error) => panic!("{name} should run: {error}"),
            };
            for index in 0..execution.frame_count() {
                let Some(scheduler) = execution.scheduler(FrameId(index)) else {
                    panic!("{name}: frame {index} should exist");
                };
                for node_id in registry.iter().map(|step| &step.common().node_id) {
                    let Some(output) = scheduler.output_of(node_id) else {
                        continue;
                    };
                    assert!(
                        !contains_null(output),
                        "{name}: {node_id} produced a value containing null: {output}"
                    );
                }
            }
        }
    }

    fn contains_null(value: &Value) -> bool {
        match value {
            Value::Null => true,
            Value::Array(items) => items.iter().any(contains_null),
            Value::Object(fields) => fields.values().any(contains_null),
            Value::Bool(_) | Value::Number(_) | Value::String(_) => false,
        }
    }

    #[test]
    fn an_abandoned_branch_is_not_aggregated_as_though_it_had_succeeded() {
        // A branch whose step is abandoned is *quiet*, and quiet used to be
        // enough to settle it. `branch_outputs` filters a missing output away,
        // so the aggregate came out one entry short, the run continued off it,
        // and `is_finished` said yes. A failed component inside a fan-out
        // silently truncating the result is exactly the class of defect this
        // port exists to remove.
        let registry = corpus("pipeline1");
        let mut execution = Execution::new(&registry, corpus_input());

        let mut abandoned = false;
        loop {
            let task = match execution.next_task(&registry) {
                Ok(Some(task)) => task,
                Ok(None) => break,
                Err(error) => panic!("should schedule: {error}"),
            };
            // The first step inside a branch is dropped, as a failed component
            // would be.
            if !abandoned && task.frame != FrameId::ROOT {
                execution.abandon(&task);
                abandoned = true;
                continue;
            }
            let Ok(output) = stub_output(&registry, &task) else {
                panic!("stub should produce");
            };
            let Ok(()) = execution.report(&registry, &task, output) else {
                panic!("should accept");
            };
        }
        assert!(abandoned, "a branch step should have been dispatched");

        assert!(
            !execution.is_finished(),
            "a run with an abandoned branch has not finished"
        );
        let stalled = execution.stalled();
        assert!(
            !stalled.is_empty(),
            "and the abandoned step is reported rather than averaged away"
        );
        assert!(
            stalled.iter().any(|(frame, _)| *frame != FrameId::ROOT),
            "the stall is inside a branch: {stalled:?}"
        );
        let Some(root) = execution.scheduler(FrameId::ROOT) else {
            panic!("there is always a root");
        };
        assert!(
            root.output_of(&NodeId::new("X")).is_none(),
            "the aggregator did not complete on a short aggregate"
        );
    }

    #[test]
    fn a_step_taken_before_an_error_can_still_be_run_afterwards() {
        // `next_ready` pops the step and marks it in flight, so an error after
        // that point drops it: not runnable, not running, not done, and no
        // retry can reach it. The run then makes no progress for ever while
        // reporting nothing wrong.
        let registry = corpus("pipeline_condition_list");
        let other = corpus("pipeline1");
        let mut execution = Execution::new(&registry, corpus_input());

        assert!(matches!(
            execution.next_task(&other),
            Err(ScheduleError::UnknownStep { .. })
        ));

        // The step went back on the queue, so the correct registry recovers.
        let mut ran = 0usize;
        loop {
            match execution.next_task(&registry) {
                Ok(Some(task)) => {
                    let Ok(output) = stub_output(&registry, &task) else {
                        panic!("stub should produce");
                    };
                    let Ok(()) = execution.report(&registry, &task, output) else {
                        panic!("should accept");
                    };
                    ran += 1;
                }
                Ok(None) => break,
                Err(error) => panic!("should schedule: {error}"),
            }
        }
        assert!(ran > 0, "the run recovered rather than wedging");
        assert!(execution.is_finished(), "and finished");
        assert!(execution.stalled().is_empty());
    }

    #[test]
    fn a_dict_aggregator_whose_every_start_is_blocked_does_not_fabricate_an_output() {
        // Two different reasons for a fan-out to have no branches. A *list*
        // aggregator over `[]` genuinely has nothing to do and aggregates to
        // `[]` -- the defect notes A *dict* aggregator whose every
        // declared start has prerequisites has a body with no way in, and
        // giving it `[]` invents an output and runs the rest of the pipeline
        // off it.
        let mut registry = corpus("pipeline1");
        let Some(Step::DictAggregator { refs, .. }) = registry.get(&NodeId::new("X")) else {
            panic!("pipeline1's X is a dict aggregator");
        };
        let starts: Vec<NodeId> = refs.start.node_ids().cloned().collect();
        for id in &starts {
            match registry.get_mut(id) {
                Some(step) => step.common_mut().num_prerequisites = 1,
                None => panic!("{id} is in the registry"),
            }
        }

        let mut execution = Execution::new(&registry, corpus_input());
        loop {
            match execution.next_task(&registry) {
                Ok(Some(task)) => {
                    let Ok(output) = stub_output(&registry, &task) else {
                        panic!("stub should produce");
                    };
                    let Ok(()) = execution.report(&registry, &task, output) else {
                        panic!("should accept");
                    };
                }
                Ok(None) => break,
                Err(error) => panic!("should schedule: {error}"),
            }
        }
        assert_eq!(execution.frame_count(), 1, "no branch could be opened");
        let Some(root) = execution.scheduler(FrameId::ROOT) else {
            panic!("there is always a root");
        };
        assert!(
            root.output_of(&NodeId::new("X")).is_none(),
            "the aggregator has no output rather than an invented one"
        );
        assert!(!execution.is_finished(), "the run has not finished");
        assert!(
            execution
                .stalled()
                .iter()
                .any(|(_, id)| *id == NodeId::new("X")),
            "and the aggregator is named as blocked: {:?}",
            execution.stalled()
        );
    }

    #[test]
    fn an_aggregator_given_the_wrong_shape_errors_without_losing_the_step() {
        // The other way `begin` can fail after the step has been popped. A
        // component that returns an object where the list aggregator below it
        // needs an array is `IncompatibleAggregatorInput` -- the original's
        // `IncompatibleListAggregatorInputError` -- and the aggregator must go
        // back on the queue, not vanish.
        let registry = corpus("pipeline4");
        let mut execution = Execution::new(&registry, serde_json::json!({"doc": "d"}));
        let Ok(Some(first)) = execution.next_task(&registry) else {
            panic!("A is ready");
        };
        assert_eq!(first.node_id, NodeId::new("A"));
        // An object, where `A` feeds a list aggregator and so must return a
        // list.
        let Ok(()) = execution.report(&registry, &first, serde_json::json!({"k": 1})) else {
            panic!("the scheduler accepts it; the aggregator is what refuses");
        };

        assert!(matches!(
            execution.next_task(&registry),
            Err(ScheduleError::IncompatibleAggregatorInput {
                aggregator: "list",
                ..
            })
        ));

        // Retrying finds the aggregator still there rather than dropped, so a
        // driver that fixes the component and replays gets the same error
        // rather than an invisible wedge.
        assert!(matches!(
            execution.next_task(&registry),
            Err(ScheduleError::IncompatibleAggregatorInput { .. })
        ));
        assert!(!execution.is_finished(), "the run did not finish");
    }

    #[test]
    fn a_branch_whose_conditional_skips_a_join_still_aggregates() {
        // Inside `List`: `A -> {R, B}` and `R -> Con1 -> .. -> B`, so `B` joins
        // `A` and `Con4`. When `Con1` takes `on_false` the chain goes to `E`
        // instead and `B`'s second prerequisite never arrives -- an ordinary
        // outcome of a conditional, not a failure.
        //
        // An earlier version gated settling on `stalled()`, which reports that
        // join, so every aggregator containing a conditional blocked for ever
        // and every step below it was dropped. The gate is the narrower
        // question: was anything *begun* and never finished.
        let registry = corpus("pipeline_condition_list");
        let mut input = corpus_input();
        input["equals"] = serde_json::json!("not the expected value");

        let execution = match drive(&registry, input) {
            Ok(execution) => execution,
            Err(error) => panic!("should run: {error}"),
        };
        assert!(execution.is_finished(), "the run finished");
        let Some(root) = execution.scheduler(FrameId::ROOT) else {
            panic!("there is always a root");
        };
        assert!(
            root.output_of(&NodeId::new("List")).is_some(),
            "the aggregator completed rather than blocking on a skipped join"
        );
        // The branch produces nothing, and that is a property of the fixture
        // rather than of this code: `E` is the `on_false` target of all four
        // conditionals, so it carries four prerequisites of which exactly one
        // can ever arrive. It is unreachable by construction. See
        // the defect notes -- the original wedges here, because its
        // aggregation waits on a terminal child that cannot run.
        assert_eq!(
            root.output_of(&NodeId::new("List")),
            Some(&serde_json::json!([])),
            "the branch contributed nothing, and the run says so rather than hanging"
        );
        assert!(
            !execution.ran().contains(&NodeId::new("E")),
            "E cannot run: {:?}",
            execution.ran()
        );

        // `stalled()` *does* name the skipped join. That is the difference
        // between the two questions, and why decisions are not made on it: it
        // is advisory, for a human deciding whether to look.
        let stalled = execution.stalled();
        assert!(
            stalled.iter().any(|(_, id)| *id == NodeId::new("B")),
            "the skipped join is reported as advisory: {stalled:?}"
        );
        assert!(
            stalled.iter().any(|(_, id)| *id == NodeId::new("E")),
            "as is the unreachable one: {stalled:?}"
        );
    }

    #[test]
    fn a_conditional_that_fails_to_complete_is_put_back() {
        // `Scheduler::complete` clears `dispatched` before its fallible work,
        // so a conditional whose completion errors was left in neither `ready`,
        // `dispatched` nor `done` -- unrunnable, with the error unrepeatable,
        // so the failure quietly became a stall.
        let registry = corpus("pipeline_condition_list");
        let mut execution = Execution::new(&registry, corpus_input());
        loop {
            let Ok(Some(task)) = execution.next_task(&registry) else {
                break;
            };
            // `R` feeds `Con1`; a scalar from it is a component returning
            // `step_output: "..."`, which the contract permits and a
            // conditional cannot evaluate.
            let output = if task.node_id == NodeId::new("R") {
                serde_json::json!("a scalar")
            } else {
                match stub_output(&registry, &task) {
                    Ok(output) => output,
                    Err(error) => panic!("stub should produce: {error}"),
                }
            };
            let Ok(()) = execution.report(&registry, &task, output) else {
                panic!("should accept");
            };
        }
        // The loop above ends on the first error. It must be the same error
        // again, not silence.
        assert!(matches!(
            execution.next_task(&registry),
            Err(ScheduleError::IncompatibleConditionInput { .. })
        ));
        assert!(matches!(
            execution.next_task(&registry),
            Err(ScheduleError::IncompatibleConditionInput { .. })
        ));
    }

    #[test]
    fn a_failed_aggregation_keeps_the_fanout_so_it_can_be_raised_again() {
        // `settle` used to remove the `Pending` before completing the
        // aggregator. If that completion failed, the record was gone: the
        // aggregator is not in `done`, `complete` had cleared it from
        // `dispatched`, and nothing could recompute the aggregate or raise the
        // error a second time.
        let mut registry = corpus("pipeline1");
        match registry.get_mut(&NodeId::new("X")) {
            Some(step) => step.common_mut().next_nodes = vec![NodeId::new("ghost")],
            None => panic!("pipeline1 has X"),
        }
        let mut execution = Execution::new(&registry, corpus_input());
        loop {
            let Ok(Some(task)) = execution.next_task(&registry) else {
                break;
            };
            let Ok(output) = stub_output(&registry, &task) else {
                panic!("stub should produce");
            };
            let Ok(()) = execution.report(&registry, &task, output) else {
                panic!("should accept");
            };
        }
        for _ in 0..2 {
            assert!(
                matches!(
                    execution.next_task(&registry),
                    Err(ScheduleError::UnknownSuccessor { .. })
                ),
                "the aggregation error is raised every time, not once"
            );
        }
        assert!(!execution.is_finished());
    }

    #[test]
    fn a_zero_width_aggregation_that_fails_is_put_back() {
        // The same rule for the shortcut path: completing an empty fan-out can
        // fail, and the aggregator must not be lost when it does.
        let mut registry = corpus("pipeline4");
        match registry.get_mut(&NodeId::new("X")) {
            Some(step) => {
                step.common_mut().num_prerequisites = 0;
                step.common_mut().next_nodes = vec![NodeId::new("ghost")];
            }
            None => panic!("pipeline4 has X"),
        }
        let mut execution = Execution::new(&registry, serde_json::json!({"doc": "d"}));
        let Ok(Some(first)) = execution.next_task(&registry) else {
            panic!("A is ready");
        };
        // An extraction that found nothing.
        let Ok(()) = execution.report(&registry, &first, serde_json::json!([])) else {
            panic!("should accept");
        };
        for _ in 0..2 {
            assert!(
                matches!(
                    execution.next_task(&registry),
                    Err(ScheduleError::UnknownSuccessor { .. })
                ),
                "raised every time"
            );
        }
    }

    #[test]
    fn a_run_that_could_start_nothing_has_not_finished() {
        // The half of the failure signal that `has_unfinished_started` cannot
        // see. If every declared start has prerequisites, nothing is ever
        // queued, nothing is ever handed out, and `started` stays empty -- so
        // zero steps running reads as a clean finish and a driver reports
        // success on a run in which no component was called.
        let mut registry = corpus("pipeline1");
        let starts: Vec<NodeId> = registry.start().node_ids().cloned().collect();
        for id in &starts {
            match registry.get_mut(id) {
                Some(step) => step.common_mut().num_prerequisites = 1,
                None => panic!("{id} is in the registry"),
            }
        }

        let mut execution = Execution::new(&registry, corpus_input());
        let Ok(None) = execution.next_task(&registry) else {
            panic!("nothing can be started");
        };
        assert!(execution.ran().is_empty(), "and nothing ran");
        assert!(
            !execution.is_finished(),
            "a run that started nothing has not finished"
        );
        assert!(
            !execution.stalled().is_empty(),
            "and it says which starts are blocked: {:?}",
            execution.stalled()
        );

        // The contrast that matters: a run which legitimately finishes still
        // reports finished, even though a conditional left a join short.
        let conditional = corpus("pipeline_condition_list");
        let mut input = corpus_input();
        input["equals"] = serde_json::json!("not the expected value");
        let Ok(done) = drive(&conditional, input) else {
            panic!("should run");
        };
        assert!(
            done.is_finished(),
            "a skipped join does not block finishing"
        );
        assert!(!done.stalled().is_empty(), "though it is still reported");
    }

    #[test]
    fn a_declared_start_the_conditional_never_reaches_does_not_block_finishing() {
        // `D` is declared a start *and* has a prerequisite, so it is not seeded
        // and arrives only by the `on_true` edge. Taking `on_false` means it
        // never runs -- and the run has still finished perfectly well: every
        // step that could run ran, nothing was abandoned, nothing is in flight.
        //
        // An earlier version required every declared start to be `done`, so
        // this reported unfinished for ever: a driver looping on
        // `!is_finished()` hangs, and one mapping it to run status marks a
        // successful run failed. Not in the corpus, which is why the suite
        // stayed green through it.
        let registry = resolved(
            "pipeline_id: p\nstart:\n  A: key_A\n  D: key_D\ncomponents:\n\
             \x20 - node_id: A\n    type: Model\n    children: [Con1]\n\
             \x20 - node_id: Con1\n    type: Condition\n    conditions:\n\
             \x20     - key: equals\n        type: equals\n        value: value\n\
             \x20   children:\n      on_true: B\n      on_false: C\n\
             \x20 - node_id: B\n    type: Model\n    children: [D]\n\
             \x20 - node_id: C\n    type: Model\n    children: [end]\n\
             \x20 - node_id: D\n    type: Model\n    children: [end]\n",
        );
        match registry.get(&NodeId::new("D")) {
            Some(step) => assert_eq!(step.common().num_prerequisites, 1, "D joins B"),
            None => panic!("D resolves"),
        }
        assert!(
            registry
                .start()
                .node_ids()
                .any(|id| *id == NodeId::new("D")),
            "and D really is declared a start"
        );

        let taken = match drive(&registry, serde_json::json!({"equals": "value"})) {
            Ok(execution) => execution,
            Err(error) => panic!("on_true should run: {error}"),
        };
        assert!(taken.ran().contains(&NodeId::new("D")), "on_true reaches D");
        assert!(taken.is_finished());

        let skipped = match drive(&registry, serde_json::json!({"equals": "other"})) {
            Ok(execution) => execution,
            Err(error) => panic!("on_false should run: {error}"),
        };
        assert!(
            !skipped.ran().contains(&NodeId::new("D")),
            "on_false never reaches D: {:?}",
            skipped.ran()
        );
        assert!(
            skipped.ran().contains(&NodeId::new("C")),
            "it reaches C instead"
        );
        assert!(
            skipped.is_finished(),
            "and the run has finished: every step that could run did"
        );
        // Nor is the unreached start reported as blocking anything. A caller
        // that maps a non-empty `stalled()` to a failed run would otherwise
        // mark this successful run failed.
        assert!(
            skipped.stalled().is_empty(),
            "a legitimately unreached start is not a stall: {:?}",
            skipped.stalled()
        );
    }

    #[test]
    fn a_run_reports_the_output_of_every_step_nothing_follows() {
        // What a driver concludes the run with. The original takes
        // `step_output` from whichever top-level step with no `next_nodes`
        // finishes -- once per such step.
        for (name, expected) in [
            ("pipeline1", 1),
            ("pipeline_params", 1),
            ("pipeline_nested_list_dict", 1),
            ("pipeline_condition_list", 1),
        ] {
            let registry = corpus(name);
            let Ok(execution) = drive(&registry, corpus_input()) else {
                panic!("{name} should run");
            };
            let Ok(finished) = execution.run_output(&registry) else {
                panic!("{name} should report an output");
            };
            assert_eq!(finished.len(), expected, "{name}: {finished:?}");
            assert!(
                finished.iter().all(|(_, output)| !output.is_null()),
                "{name}: a run that finished produced something"
            );
        }
    }

    #[test]
    fn two_terminal_steps_are_both_reported_rather_than_one_being_picked() {
        // `A -> {B, C}` with both ending is an ordinary fan-out without an
        // aggregator, and none of the resolver's six rejection rules forbids
        // it. The original concludes the run *twice* there: the recorded output
        // is whichever finished last and the customer's completion callback
        // fires twice (route B in the defect notes).
        //
        // No corpus pipeline has the shape, which is why it has never been
        // seen. Returning both makes the ambiguity the caller's to see rather
        // than picking one and inheriting the nondeterminism quietly.
        let registry = resolved(
            // `C` is declared *before* `B` deliberately. The assertion below
            // is on `iter()` order, so declaring them in the order asserted
            // would pass whether the registry were ordered or not -- which is
            // exactly how the previous version of this test proved nothing.
            "pipeline_id: p\nstart: A\ncomponents:\n\
             \x20 - node_id: A\n    type: Model\n    children: [B, C]\n\
             \x20 - node_id: C\n    type: Model\n    children: [end]\n\
             \x20 - node_id: B\n    type: Model\n    children: [end]\n",
        );
        let Ok(execution) = drive(&registry, serde_json::json!({"doc": "d"})) else {
            panic!("should run");
        };
        let Ok(finished) = execution.run_output(&registry) else {
            panic!("should report");
        };
        let names: Vec<&NodeId> = finished.iter().map(|(id, _)| id).collect();
        // Deliberately *not* sorted here. This walks `StepRegistry::iter`,
        // which is documented to be in `NodeId` order, and asserting the order
        // is what pins that: a run's reported output must not depend on which
        // process produced it. It used to -- the registry was a `HashMap`, this
        // assertion sorted first to cope, and Restate replaying a handler in a
        // fresh process would have seen a different order than the journal.
        assert_eq!(
            names,
            vec![&NodeId::new("B"), &NodeId::new("C")],
            "both are reported in node order, and `A` is not -- something follows it"
        );
    }

    #[test]
    fn a_conditional_ending_a_branch_is_terminal_and_one_taking_a_branch_is_not() {
        // Terminality is re-asked with the step's recorded output, so it is the
        // same rule that decided what ran: a conditional whose taken branch is
        // `end` has no successors, and one that took a branch does.
        let registry = resolved(
            "pipeline_id: p\nstart: A\ncomponents:\n\
             \x20 - node_id: A\n    type: Model\n    children: [Con]\n\
             \x20 - node_id: Con\n    type: Condition\n    conditions:\n\
             \x20     - key: equals\n        type: equals\n        value: value\n\
             \x20   children:\n      on_true: B\n      on_false: end\n\
             \x20 - node_id: B\n    type: Model\n    children: [end]\n",
        );

        let Ok(taken) = drive(&registry, serde_json::json!({"equals": "value"})) else {
            panic!("on_true should run");
        };
        let Ok(from_branch) = taken.run_output(&registry) else {
            panic!("should report");
        };
        assert_eq!(from_branch.len(), 1);
        assert_eq!(from_branch[0].0, NodeId::new("B"), "the branch's end");

        let Ok(ended) = drive(&registry, serde_json::json!({"equals": "no"})) else {
            panic!("on_false should run");
        };
        let Ok(from_conditional) = ended.run_output(&registry) else {
            panic!("should report");
        };
        assert_eq!(from_conditional.len(), 1);
        assert_eq!(
            from_conditional[0].0,
            NodeId::new("Con"),
            "the conditional itself, having gone nowhere"
        );
    }

    #[test]
    fn every_corpus_pipeline_runs_to_the_same_counts_as_the_scheduler() {
        // The same numbers `schedule.rs` pins for its recursive test driver.
        // They must not move when the orchestration is rebuilt as a tree --
        // that is the whole point of porting them across.
        for (name, executions, distinct) in [
            ("pipeline1", 5, 5),
            ("pipeline2", 5, 5),
            ("pipeline3", 5, 5),
            ("pipeline4", 5, 5),
            ("pipeline_params", 4, 4),
            ("pipeline_case1", 3, 3),
            ("pipeline_case2", 6, 6),
            ("pipeline_case3", 4, 4),
            ("pipeline_condition_dict", 8, 8),
            ("pipeline_condition_list", 10, 10),
            ("pipeline_nested_list_dict", 33, 24),
        ] {
            let registry = corpus(name);
            let execution = match drive(&registry, corpus_input()) {
                Ok(execution) => execution,
                Err(error) => panic!("{name} should run to completion: {error}"),
            };
            assert_eq!(execution.ran().len(), executions, "{name}: executions");
            let mut unique = execution.ran().to_vec();
            unique.sort();
            unique.dedup();
            assert_eq!(unique.len(), distinct, "{name}: distinct nodes reached");
            assert!(execution.is_finished(), "{name} should be finished");
            assert!(
                execution.stalled().is_empty(),
                "{name} should not be stalled: {:?}",
                execution.stalled()
            );
        }
    }

    #[test]
    fn an_aggregator_is_never_handed_to_the_driver() {
        // The original resolves aggregators and conditionals itself. A driver
        // receiving one would be asking an AI component to evaluate `equals`.
        for name in ["pipeline1", "pipeline4", "pipeline_condition_list"] {
            let registry = corpus(name);
            let mut execution = Execution::new(&registry, corpus_input());
            let mut dispatched = 0usize;
            // Matched rather than `while let Ok(Some(..))`: that form ends the
            // loop on `Err` as quietly as on `None`, so the assertions below
            // would still pass on a run that failed part-way through.
            while let Some(task) = match execution.next_task(&registry) {
                Ok(task) => task,
                Err(error) => panic!("{name} should schedule: {error}"),
            } {
                match registry.get(&task.node_id) {
                    Some(Step::Model { .. }) => dispatched += 1,
                    other => panic!("{name}: {other:?} should not be dispatched"),
                }
                let Ok(output) = stub_output(&registry, &task) else {
                    panic!("stub should produce");
                };
                let Ok(()) = execution.report(&registry, &task, output) else {
                    panic!("should accept");
                };
            }
            assert!(dispatched > 0, "{name}: something should have been run");
            assert!(
                execution.ran().len() > dispatched,
                "{name}: and more steps ran than were dispatched, since \
                 aggregators and conditionals ran without a component"
            );
        }
    }

    #[test]
    fn branches_run_concurrently_rather_than_one_after_another() {
        // The recursive driver this replaces could only run a branch to
        // completion before starting the next, because a branch was a stack
        // frame. Here a driver can hold tasks from two branches at once.
        let registry = corpus("pipeline1");
        let mut execution = Execution::new(&registry, corpus_input());

        let mut held: Vec<Task> = Vec::new();
        // Run until the dict aggregator has fanned out, then collect every task
        // available at that moment without reporting any of them.
        loop {
            let Ok(next) = execution.next_task(&registry) else {
                panic!("should schedule");
            };
            let Some(task) = next else { break };
            if execution.frame_count() > 1 {
                held.push(task);
                continue;
            }
            let Ok(output) = stub_output(&registry, &task) else {
                panic!("stub should produce");
            };
            let Ok(()) = execution.report(&registry, &task, output) else {
                panic!("should accept");
            };
        }
        assert!(
            held.len() > 1,
            "two branches should be runnable at once, got {held:?}"
        );
        let frames: std::collections::BTreeSet<FrameId> =
            held.iter().map(|task| task.frame).collect();
        assert!(
            frames.len() > 1,
            "and they should be in different frames: {frames:?}"
        );
        assert!(!execution.is_finished(), "with work still outstanding");

        // Reporting them in reverse order still completes the run, so nothing
        // depends on branches finishing in the order they started.
        held.reverse();
        for task in held {
            let Ok(output) = stub_output(&registry, &task) else {
                panic!("stub should produce");
            };
            let Ok(()) = execution.report(&registry, &task, output) else {
                panic!("should accept");
            };
        }
        let Ok(execution) = finish(&registry, execution) else {
            panic!("should finish");
        };
        assert!(execution.is_finished());
    }

    fn finish(
        registry: &StepRegistry,
        mut execution: Execution,
    ) -> Result<Execution, ScheduleError> {
        while let Some(task) = execution.next_task(registry)? {
            let output = stub_output(registry, &task)?;
            execution.report(registry, &task, output)?;
        }
        Ok(execution)
    }

    // --- what the interpreter says it did ---------------------------------

    /// Runs a pipeline, draining the log as it goes, with caller-chosen output.
    ///
    /// Drained inside the loop rather than once at the end, which is how a
    /// recorder would use it — and the difference is not cosmetic: draining
    /// only at the end would pass even if the log were appended in one burst
    /// after the run, and the ordering guarantee [`Happening`] documents would
    /// be untested.
    fn happenings_with(
        registry: &StepRegistry,
        input: Value,
        output_of: impl Fn(&Task) -> Value,
    ) -> Vec<Happening> {
        let mut execution = Execution::new(registry, input);
        let mut log = Vec::new();
        loop {
            let next = match execution.next_task(registry) {
                Ok(next) => next,
                Err(error) => panic!("the pipeline should run: {error}"),
            };
            log.extend(execution.drain_happenings());
            let Some(task) = next else { break };
            let output = output_of(&task);
            let Ok(()) = execution.report(registry, &task, output) else {
                panic!("should accept");
            };
            log.extend(execution.drain_happenings());
        }
        log
    }

    /// `pipeline4`'s list aggregator, fanned out over `width` elements.
    ///
    /// The same shape `a_list_aggregator_over_an_empty_list_aggregates_immediately`
    /// sets up, and for the same reason: the empty list has to reach the
    /// aggregator by a path that does not coerce it.
    fn fanned_out_over(width: usize) -> (StepRegistry, Vec<Happening>) {
        let mut registry = corpus("pipeline4");
        match registry.get_mut(&NodeId::new("X")) {
            Some(step) => step.common_mut().num_prerequisites = 0,
            None => panic!("pipeline4 has X"),
        }
        let elements: Vec<Value> = (0..width)
            .map(|n| serde_json::json!({ "element": n }))
            .collect();
        let log = happenings_with(&registry, serde_json::json!({"doc": "d"}), |task| {
            if task.node_id == NodeId::new("A") {
                Value::Array(elements.clone())
            } else {
                serde_json::json!({"ran": task.node_id.to_string()})
            }
        });
        (registry, log)
    }

    #[test]
    fn a_fan_out_is_announced_before_its_branches_exist() {
        // The ordering [`Happening`] documents, and the one a consumer writing
        // a row per step depends on: `node_run.parent_path` is a
        // non-deferrable foreign key onto `path`, so a branch's row cannot be
        // written before its aggregator's. The original gets this by
        // construction, inserting the aggregator before the recursive child
        // inserts.
        let (_, log) = fanned_out_over(3);

        let Some(fanned) = log
            .iter()
            .position(|h| matches!(h, Happening::FannedOut { .. }))
        else {
            panic!("that pipeline fans out: {log:?}");
        };
        let Some(aggregated) = log
            .iter()
            .position(|h| matches!(h, Happening::Aggregated { .. }))
        else {
            panic!("and aggregates: {log:?}");
        };
        assert!(
            fanned < aggregated,
            "fanned out before aggregating: {log:?}"
        );

        let Happening::FannedOut { branches, .. } = &log[fanned] else {
            panic!("just matched it");
        };
        // 1-based and contiguous, as the original's `child_idx = index + 1`.
        let indices: Vec<u32> = branches.iter().map(|(_, index)| index.get()).collect();
        assert_eq!(indices, vec![1, 2, 3]);
    }

    #[test]
    fn every_branch_of_a_fan_out_has_its_own_identity() {
        // The whole reason [`Origin`] exists. All three branches run the same
        // node ids, so without a per-branch index a consumer sees one step
        // where there are three — and `node_run.path` being unique makes that a
        // *silent* loss, swallowed by `ON CONFLICT DO NOTHING`.
        let (_, log) = fanned_out_over(3);
        let Some(Happening::FannedOut {
            branches, frame, ..
        }) = log
            .iter()
            .find(|h| matches!(h, Happening::FannedOut { .. }))
        else {
            panic!("it fans out: {log:?}");
        };
        assert_eq!(
            *frame,
            FrameId::ROOT,
            "the aggregator itself is at the root"
        );

        let frames: Vec<FrameId> = branches.iter().map(|(frame, _)| *frame).collect();
        let mut unique = frames.clone();
        unique.sort_unstable();
        unique.dedup();
        assert_eq!(frames.len(), 3);
        assert_eq!(unique.len(), 3, "one distinct frame per branch");
        for frame in &frames {
            assert_ne!(*frame, FrameId::ROOT, "a branch is never the root frame");
        }
    }

    #[test]
    fn a_branch_frame_knows_which_branch_it_is() {
        // `origin_of` walked upward is how a consumer builds the original's
        // `{parent_slug}.{node_id}:{child_idx}`.
        let mut registry = corpus("pipeline4");
        match registry.get_mut(&NodeId::new("X")) {
            Some(step) => step.common_mut().num_prerequisites = 0,
            None => panic!("pipeline4 has X"),
        }
        let mut execution = Execution::new(&registry, serde_json::json!({"doc": "d"}));
        let mut opened: Vec<(FrameId, pneuma_core::child_index::ChildIndex)> = Vec::new();
        while let Ok(Some(task)) = execution.next_task(&registry) {
            for happening in execution.drain_happenings() {
                if let Happening::FannedOut { branches, .. } = happening {
                    opened = branches;
                }
            }
            let output = if task.node_id == NodeId::new("A") {
                serde_json::json!([{"one": 1}, {"two": 2}])
            } else {
                serde_json::json!({"ran": task.node_id.to_string()})
            };
            let Ok(()) = execution.report(&registry, &task, output) else {
                panic!("should accept");
            };
            if !opened.is_empty() {
                break;
            }
        }
        assert_eq!(opened.len(), 2, "two elements, two branches");
        for (frame, expected) in &opened {
            let Some(origin) = execution.origin_of(*frame) else {
                panic!("a branch frame has an origin");
            };
            assert_eq!(origin.parent, FrameId::ROOT);
            assert_eq!(origin.aggregator, NodeId::new("X"));
            assert_eq!(origin.child_index, *expected);
        }
    }

    #[test]
    fn a_conditional_reports_its_input_as_its_output() {
        // `process_result(..., step_output=step_input)`
        // — a conditional selects a
        // branch, it does not produce anything. A consumer recording it needs
        // that to be a fact rather than an inference, because a conditional
        // never becomes a `Task` and so is invisible from the dispatch loop.
        let registry = corpus("pipeline_condition_list");
        // `corpus_input`, because a conditional evaluates against its input and
        // this fixture's conditions read keys a bare `{"doc": "d"}` lacks.
        let log = happenings_with(&registry, corpus_input(), |task| {
            match stub_output(&registry, task) {
                Ok(output) => output,
                Err(error) => panic!("stub should produce: {error}"),
            }
        });
        let Some(Happening::Resolved { output, .. }) =
            log.iter().find(|h| matches!(h, Happening::Resolved { .. }))
        else {
            panic!("that pipeline has a conditional: {log:?}");
        };
        assert!(output.is_object(), "an output, not nothing: {output:?}");
    }

    #[test]
    fn an_empty_fan_out_announces_itself_and_aggregates_at_once() {
        // The defect notes: a list aggregator over `[]` has no
        // branches and aggregates to `[]`, where the original wedges. In the
        // log that is a `FannedOut` with no branches followed immediately by
        // its `Aggregated`, so a consumer is not left waiting for children that
        // are never coming.
        let (_, log) = fanned_out_over(0);
        // Announced, with no branches: a fan-out into nothing is still a
        // fan-out, and every aggregator is announced exactly once so a consumer
        // never has to create its row from two different entries.
        let Some(Happening::FannedOut { branches, .. }) = log
            .iter()
            .find(|h| matches!(h, Happening::FannedOut { .. }))
        else {
            panic!("it announces itself: {log:?}");
        };
        assert!(branches.is_empty(), "into nothing: {branches:?}");
        let Some(Happening::Aggregated { output, .. }) = log
            .iter()
            .find(|h| matches!(h, Happening::Aggregated { .. }))
        else {
            panic!("it still aggregates: {log:?}");
        };
        assert_eq!(*output, serde_json::json!([]));
    }

    #[test]
    fn draining_takes_each_happening_once() {
        // A recorder writes what it drains, so a second read returning the same
        // entries would write every row twice — and `ON CONFLICT DO NOTHING`
        // would hide that on the insert while the status updates went through.
        let mut registry = corpus("pipeline4");
        match registry.get_mut(&NodeId::new("X")) {
            Some(step) => step.common_mut().num_prerequisites = 0,
            None => panic!("pipeline4 has X"),
        }
        let mut execution = Execution::new(&registry, serde_json::json!({"doc": "d"}));
        // Drive to the fan-out, which is the first thing that logs anything.
        let mut logged = Vec::new();
        while logged.is_empty() {
            let Ok(Some(task)) = execution.next_task(&registry) else {
                panic!("that pipeline fans out before it finishes");
            };
            logged = execution.drain_happenings();
            let output = if task.node_id == NodeId::new("A") {
                serde_json::json!([{"one": 1}])
            } else {
                serde_json::json!({"ran": task.node_id.to_string()})
            };
            let Ok(()) = execution.report(&registry, &task, output) else {
                panic!("should accept");
            };
            logged.extend(execution.drain_happenings());
        }
        assert!(!logged.is_empty(), "something happened");
        assert!(
            execution.drain_happenings().is_empty(),
            "and it is not still there"
        );
    }

    #[test]
    fn the_root_frame_came_from_nowhere() {
        let registry = corpus("pipeline1");
        let execution = Execution::new(&registry, serde_json::json!({"doc": "d"}));
        assert!(execution.origin_of(FrameId::ROOT).is_none());
    }
}
