//! The ready-set: what may run now, given what has finished.
//!
//! # Aggregator fan-out
//!
//! An aggregator is not a scheduling edge but a *nested execution*. A driver
//! asks [`fanout_of`] whether a ready step fans out; if it does, each branch
//! runs in its own [`Scheduler::scoped`] bounded to the aggregator's
//! `component_ids`, and the branch outputs become the aggregator's output via
//! [`aggregate`].
//!
//! Every corpus pipeline now runs to completion, which
//! `the_corpus_pipelines_reach_exactly_these_step_counts` pins by exact counts.
//! Two of the numbers look wrong and are not:
//!
//! * A conditional pipeline reaches fewer nodes than it has, because the
//!   untaken branch never runs. Reaching all of them would mean the conditional
//!   was not conditional.
//! * `pipeline_nested_list_dict` executes 33 steps across 24 nodes, because a
//!   fan-out runs its children once per branch.
//!
//! An earlier version of that test asserted only `order.len() <=
//! registry.len()`, which is true of the empty run, and so reported success
//! while the scheduler ran a fifth of the graph.

use std::collections::{BTreeMap, BTreeSet};

use pneuma_core::child_index::ChildIndex;
use pneuma_core::evaluator::evaluate;
use pneuma_core::ids::NodeId;
use pneuma_core::start_set::StartSet;
use pneuma_core::step::{AggregatorRefs, Step, StepRegistry};
use serde_json::Value;

/// A step that may run now, and the input it takes.
#[derive(Debug, Clone, PartialEq)]
pub struct Ready {
    /// Which step.
    pub node_id: NodeId,
    /// Its input — the output of whatever unblocked it.
    pub input: Value,
}

/// Why scheduling could not continue.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ScheduleError {
    /// A prerequisite's output is not an object, so it cannot be merged into a
    /// join's input.
    ///
    /// The original raises `InvalidDictError` in the same place.
    /// Worth knowing that a component
    /// returning a scalar `step_output` is valid by the component contract and
    /// forwarded by the original executor — see the protocol notes — so
    /// this fires at the join rather than at the boundary that accepted it.
    #[error(
        "prerequisite {from} of {into} produced {kind}, which cannot be merged into a join input"
    )]
    UnmergeablePrerequisite {
        /// The prerequisite.
        from: NodeId,
        /// The join it feeds.
        into: NodeId,
        /// What it produced instead of an object.
        kind: &'static str,
    },
    /// A step names a successor the registry does not contain.
    ///
    /// The original logs a warning and carries on, which leaves a downstream
    /// join waiting forever for a prerequisite that can never arrive — one of
    /// the defects listed in this port's changelog. Here it stops.
    #[error("step {from} names successor {missing}, which is not in the registry")]
    UnknownSuccessor {
        /// The step naming it.
        from: NodeId,
        /// The name that does not resolve.
        missing: NodeId,
    },
    /// A step was reported complete that the registry does not contain.
    #[error("step {node_id} is not in the registry")]
    UnknownStep {
        /// The step reported.
        node_id: NodeId,
    },
    /// A prerequisite is recorded as arrived but has no output.
    ///
    /// An internal inconsistency rather than a pipeline error: `complete`
    /// commits arrivals and outputs together, so this cannot happen while it is
    /// the only writer.
    #[error("prerequisite {from} of {into} arrived without an output")]
    MissingPrerequisiteOutput {
        /// The prerequisite.
        from: NodeId,
        /// The join it feeds.
        into: NodeId,
    },
    /// An input laid over a declared start's gathered input is not an object.
    ///
    /// Two different merges can fail this way — the run's input
    /// and the enclosing dict aggregator's
    /// — and the original keeps them as separate sites even
    /// though both raise `InvalidDictError`. `overlay` says which, so the
    /// message does not send someone to `run.step_input` when the aggregator's
    /// input is at fault. (Named `overlay` rather than `source` because
    /// `thiserror` reserves that name for a nested error.)
    #[error("{overlay} cannot be merged into declared start {into}: {kind} is not an object")]
    UnmergeableOverlay {
        /// The declared start.
        into: NodeId,
        /// Which merge failed, in words.
        overlay: &'static str,
        /// Whichever side was not an object.
        kind: &'static str,
    },
    /// A conditional was handed an input that is not an object.
    ///
    /// The original rejects this too, but reports it as
    /// `IncompatibleListAggregatorInputError` — the wrong
    /// class, one line from the right one. The defect notes record
    /// that; here the error names the conditional.
    #[error("condition {node_id} requires an object, but its input is {kind}")]
    IncompatibleConditionInput {
        /// The conditional step.
        node_id: NodeId,
        /// What it got instead.
        kind: &'static str,
    },
    /// A fan-out is too wide for a `ChildIndex`.
    #[error("{node_id} fans out to index {index}, which exceeds a child index")]
    FanoutTooWide {
        /// The aggregator.
        node_id: NodeId,
        /// The 0-based branch position that overflowed.
        index: usize,
    },
    /// An aggregator was handed an input of the wrong shape.
    ///
    /// The original raises `IncompatibleListAggregatorInputError` when a list
    /// aggregator's `step_input` is not a list and
    /// `IncompatibleDictAggregatorInputError` when a dict aggregator's is not a
    /// dict. An earlier version of this port claimed the
    /// original fell back to wrapping a scalar in a one-element list. It does
    /// not; there is no such line, and the invented fallback would have run one
    /// silent branch where the original fails loudly.
    #[error(
        "{node_id} is a {aggregator} aggregator and requires {expected}, but its input is {kind}"
    )]
    IncompatibleAggregatorInput {
        /// The aggregator.
        node_id: NodeId,
        /// Which kind it is -- "list" or "dict".
        aggregator: &'static str,
        /// The shape it requires.
        expected: &'static str,
        /// The shape it got.
        kind: &'static str,
    },
    /// A condition could not be evaluated against its input.
    ///
    /// Terminal rather than retryable: the same input will not evaluate
    /// differently next time.
    #[error("condition on {node_id} could not be evaluated: {reason}")]
    Condition {
        /// The conditional step.
        node_id: NodeId,
        /// What the evaluator said.
        reason: String,
    },
}

/// Tracks what has finished and decides what may run next.
///
/// # Prerequisites are a set, not a tally
///
/// A step with two prerequisites needs *both distinct* ones, not two arrivals.
/// A tally cannot tell those apart, so a prerequisite reported twice — a retry,
/// a redelivery, a driver replaying — would release the join early with one
/// branch's output missing.
///
/// The original gets this right, comparing `len(...)` against
/// `num_prerequisites` on an `$addToSet` result.
/// The spike this grew out of used a
/// counter, which is safe only because a durable journal never replays a
/// completed step — that makes the rule a property of the engine rather than of
/// the scheduler, and this port has already changed engines once.
#[derive(Debug, Clone, Default)]
pub struct Scheduler {
    ready: Vec<Ready>,
    done: BTreeMap<NodeId, Value>,
    arrived: BTreeMap<NodeId, BTreeSet<NodeId>>,
    /// When set, successors outside this set are not followed.
    ///
    /// An aggregator's children run in a sub-graph bounded to its
    /// `component_ids`; leaving that bound is how the original's fan-out would
    /// escape into the parent graph. Outside the bound is *skipped*, not an
    /// error — that is how a branch terminates, since a child's successors
    /// point at nodes the branch does not own.
    scope: Option<BTreeSet<NodeId>>,
    /// Handed out by [`Scheduler::next_ready`] and cleared by
    /// [`Scheduler::complete`].
    ///
    /// Without it a concurrent driver — which this type is explicitly meant to
    /// support — looks identical to a wedged one the moment it dispatches
    /// everything it has, and a successor already popped but not yet reported
    /// can be queued a second time.
    dispatched: BTreeSet<NodeId>,
    /// Every step handed out by [`Scheduler::next_ready`], cleared only by a
    /// *successful* [`Scheduler::complete`].
    ///
    /// `dispatched` answers "is this in flight"; this answers "was this ever
    /// started and never finished", which is the question a stall turns on. A
    /// step that was abandoned, or whose completion failed and was not retried,
    /// is in neither `dispatched` nor `done` — so without this it is in no set
    /// at all and the run looks finished.
    started: BTreeSet<NodeId>,
    /// The run's own input, merged into any declared start reached through a
    /// gather. See [`Scheduler::complete`].
    run_input: Value,
    /// The run's declared start ids — `run.start_ids` in the original.
    run_starts: BTreeSet<NodeId>,
    /// The enclosing **dict** aggregator's declared start ids, for a branch
    /// scheduler. Empty for a run and for a list aggregator's branches, which
    /// is what makes the merge below apply to neither.
    parent_starts: BTreeSet<NodeId>,
    /// The input that enclosing dict aggregator received.
    parent_input: Value,
}

impl Scheduler {
    /// Starts a run: the registry's start steps are ready, each with the run's
    /// input.
    ///
    /// Uses `start_steps`, which skips a start id with no step — the resolver
    /// has already rejected the definition errors that would make that a
    /// silent hole.
    pub fn new(registry: &StepRegistry, input: Value) -> Self {
        Scheduler {
            // A declared start with prerequisites is skipped, exactly as
            // `process_init` skips it. It is not a
            // contradiction for a join to be listed as a start:
            // `pipeline_params` declares both `A` and `D`, and `D` is the
            // diamond's fan-in. Seeding it anyway ran the join immediately on
            // the run input, before either prerequisite existed.
            ready: registry
                .start_steps()
                .filter(|step| step.common().num_prerequisites == 0)
                .map(|step| Ready {
                    node_id: step.common().node_id.clone(),
                    input: input.clone(),
                })
                .collect::<Vec<_>>(),
            done: BTreeMap::new(),
            arrived: BTreeMap::new(),
            scope: None,
            dispatched: BTreeSet::new(),
            started: BTreeSet::new(),
            // From `start_steps`, not `start().node_ids()`. `new` seeds from
            // the former, which skips a start id with no matching step
            // (`pneuma-core/src/step.rs:333-335`, deliberate defence in depth
            // for a hand-built registry). Taking the raw ids here would report
            // such an id as stalled after a fully successful run -- turning the
            // case `start_steps` exists to absorb into a permanent false wedge.
            run_starts: registry
                .start_steps()
                .map(|step| step.common().node_id.clone())
                .collect(),
            run_input: input,
            parent_starts: BTreeSet::new(),
            parent_input: Value::Null,
        }
    }

    /// A scheduler for one branch of an aggregator's fan-out.
    ///
    /// `starts` is where this branch begins and `scope` is the aggregator's
    /// `component_ids`. Successors outside the scope are not followed, which is
    /// what keeps a branch from running the parent graph.
    pub fn scoped(
        starts: &[NodeId],
        scope: &[NodeId],
        input: Value,
        parent: &Scheduler,
        enclosing_dict: Option<&StartSet>,
    ) -> Self {
        Scheduler {
            ready: starts
                .iter()
                .map(|node_id| Ready {
                    node_id: node_id.clone(),
                    input: input.clone(),
                })
                .collect::<Vec<_>>(),
            done: BTreeMap::new(),
            arrived: BTreeMap::new(),
            scope: Some(scope.iter().cloned().collect()),
            dispatched: BTreeSet::new(),
            started: BTreeSet::new(),
            // Carried from the parent, not re-derived: `run.start_ids` and
            // `run.step_input` are properties of the *run*, and the original
            // consults them at every gather regardless of how deep inside an
            // aggregator the step sits.
            run_starts: parent.run_starts.clone(),
            run_input: parent.run_input.clone(),
            // Only a *dict* aggregator merges its own input into its starts
            // (the original tests `isinstance(parent_step,
            // DictAggregatorStep)` explicitly), and every branch of a dict
            // aggregator receives that same input -- the fan-out passes
            // `step_input=step_input` unchanged -- so the branch input *is* the
            // aggregator's input.
            parent_starts: enclosing_dict
                .map(|start| start.node_ids().cloned().collect())
                .unwrap_or_default(),
            parent_input: match enclosing_dict {
                Some(_) => input,
                None => Value::Null,
            },
        }
    }

    /// Takes one step that may run now, or `None` if nothing is ready.
    ///
    /// `None` with nothing outstanding means the run is finished. A driver
    /// running steps concurrently may see `None` while work is still in flight
    /// and should ask again after reporting a completion.
    pub fn next_ready(&mut self) -> Option<Ready> {
        while let Some(candidate) = self.ready.pop() {
            // Two prerequisites completing can both propose the same
            // successor; the second proposal is dropped rather than run twice.
            if !self.done.contains_key(&candidate.node_id) {
                self.dispatched.insert(candidate.node_id.clone());
                self.started.insert(candidate.node_id.clone());
                return Some(candidate);
            }
        }
        None
    }

    /// Reports a step's output and releases whatever it unblocks.
    ///
    /// Idempotent: reporting the same step twice keeps the first output and
    /// advances nothing further, because its successors have already recorded
    /// it as an arrived prerequisite — a set, so recording it again is a no-op.
    pub fn complete(
        &mut self,
        registry: &StepRegistry,
        node_id: &NodeId,
        output: Value,
    ) -> Result<(), ScheduleError> {
        if self.done.contains_key(node_id) {
            return Ok(());
        }
        let Some(step) = registry.get(node_id) else {
            return Err(ScheduleError::UnknownStep {
                node_id: node_id.clone(),
            });
        };
        // Everything fallible happens first, and nothing is mutated until it
        // has all succeeded.
        //
        // The previous order inserted into `done`, then walked the successors
        // -- so an error from `successors_of`, an unknown successor, or an
        // unmergeable join input left the scheduler half-advanced. And because
        // this function opens by returning `Ok(())` for an already-done step, a
        // retry then advanced nothing and the error could never be raised
        // again: the join could never be released even with every output
        // sitting in `done`.
        // A failed `complete` is still a *report*: the driver has come back,
        // so the step is no longer in flight. Leaving it in `dispatched` would
        // silence `stalled()` for ever if the driver gives up -- the exact
        // failure that method exists to surface. Removed before the fallible
        // work, so every exit below agrees. A retry is unaffected: `complete`
        // reads `dispatched` for nothing, and the step is not back in `ready`,
        // so it cannot be handed out twice.
        self.dispatched.remove(node_id);
        let successors = successors_of(step, &output)?;

        let mut releases: Vec<Ready> = Vec::new();
        let mut arrivals: Vec<NodeId> = Vec::new();
        for successor in successors {
            // Outside this branch's bound: not followed, and not an error. A
            // child's successors point at nodes the branch does not own, which
            // is how the branch terminates.
            if self
                .scope
                .as_ref()
                .is_some_and(|scope| !scope.contains(&successor))
            {
                continue;
            }
            let Some(next) = registry.get(&successor) else {
                return Err(ScheduleError::UnknownSuccessor {
                    from: node_id.clone(),
                    missing: successor,
                });
            };
            // What the arrival set *would* be, without committing it.
            let mut seen = self.arrived.get(&successor).cloned().unwrap_or_default();
            seen.insert(node_id.clone());
            arrivals.push(successor.clone());

            // The original branches on `num_prerequisites` three ways,
            // and the third is an *equality*:
            //
            // * 0 -- `step_input = step_output`, run immediately;
            // * 1 -- gather with `prerequisites=None`, run immediately;
            // * n -- `if len(completed_prerequisites) != n: return`.
            //
            // An earlier version of this used `>= max(1)`, which releases a
            // join every time a further prerequisite arrives, not once when the
            // last one does. Under-counted prerequisites then queue a successor
            // twice and it runs twice. `u64` because `usize: From<u32>` does not
            // exist (16-bit targets), and the widening direction is the one that
            // cannot fail on any target.
            let needed = u64::from(next.common().num_prerequisites);
            let seen_count = seen.len() as u64;
            let released = if needed <= 1 {
                seen_count >= 1
            } else {
                seen_count == needed
            };
            // For `needed >= 2` this stands in for the atomic `$addToSet` that
            // hands the full set to exactly one caller. For `needed <= 1` the
            // original has no such protection at all -- `case 0` and `case 1`
            // run on every arrival -- so a step reached by two edges but
            // recorded with one prerequisite runs twice there and once here.
            // `dispatched` is in the check because a successor already popped by
            // the driver is neither `done` nor `ready`, and without it a
            // concurrent driver would queue it a second time.
            let already_queued = self.done.contains_key(&successor)
                || self.dispatched.contains(&successor)
                || releases.iter().any(|r| r.node_id == successor)
                || self.ready.iter().any(|r| r.node_id == successor);
            if released && !already_queued {
                // `done` does not yet contain this step: the merge is given the
                // output explicitly rather than by cloning the whole map, which
                // was O(payload) per successor.
                let mut input =
                    join_input(&seen, &self.done, node_id, &output, &successor, needed)?;
                // `if step.node_id in run.start_ids: step_input.update(
                // run.step_input)` -- the run's own input
                // is merged into a declared start, and *wins* collisions, because
                // `dict.update` overwrites.
                //
                // Only through a gather, so only when the step has at least one
                // prerequisite: `case 0` assigns `step_input = step_output`
                // without calling `_gather_step_output_from` at all.
                // A zero-prerequisite start is seeded with
                // the run input by `new` in any case.
                //
                // `pipeline_params` has exactly this shape: `D` is a declared
                // start *and* the diamond's fan-in, so the original hands it its
                // prerequisites' outputs with the run input laid over the top.
                if needed >= 1 && self.run_starts.contains(&successor) {
                    input = merge_over(input, &self.run_input, &successor, RUN_INPUT)?;
                }
                // And then the enclosing dict aggregator's own input over that
                // -- after the run input, so where both
                // apply the aggregator wins, which is the order the original writes
                // the two `update` calls in. Reached only by an edge inside the
                // aggregator: a start with prerequisites is skipped by the
                // fan-out, so this is the same shape as that skip and, like it,
                // appears in no corpus pipeline.
                if needed >= 1 && self.parent_starts.contains(&successor) {
                    input = merge_over(input, &self.parent_input, &successor, AGGREGATOR_INPUT)?;
                }
                releases.push(Ready {
                    node_id: successor,
                    input,
                });
            }
        }

        // Past here nothing can fail.
        self.started.remove(node_id);
        self.done.insert(node_id.clone(), output);
        for successor in arrivals {
            self.arrived
                .entry(successor)
                .or_default()
                .insert(node_id.clone());
        }
        self.ready.extend(releases);
        Ok(())
    }

    /// The output of a step that has finished.
    pub fn output_of(&self, node_id: &NodeId) -> Option<&Value> {
        self.done.get(node_id)
    }

    /// How many steps have finished.
    pub fn completed(&self) -> usize {
        self.done.len()
    }

    /// Puts a step that was handed out back on the queue.
    ///
    /// For a caller that took a task and could not even attempt it — a
    /// mismatched registry, a driver shutting down before dispatch. Without it
    /// the step is popped, marked in flight, and then dropped on the floor:
    /// unrecoverable, and invisible until something asks what is stalled.
    ///
    /// Distinct from [`Scheduler::abandon`], which says the step will *never*
    /// complete. This says it has not been attempted yet.
    pub fn requeue(&mut self, ready: Ready) {
        self.dispatched.remove(&ready.node_id);
        self.started.remove(&ready.node_id);
        // `insert(0)`, not `push`. `next_ready` pops from the *end*, so pushing
        // would hand this step straight back on the next call -- and a step
        // that fails deterministically would then be retried ahead of
        // everything else in the frame, for ever, starving the siblings that
        // could have made progress. At the front it is retried after them.
        self.ready.insert(0, ready);
    }

    /// Reports that a dispatched step will never complete.
    ///
    /// The driver owns failure: this type does not run anything, so it cannot
    /// know that a component errored, timed out, or was cancelled. Until it is
    /// told, a step handed out by [`Scheduler::next_ready`] is in flight, and
    /// in flight is indistinguishable from slow.
    ///
    /// Abandoning is not completing: nothing is recorded in `done`, so whatever
    /// the step feeds stays short of its prerequisites. That is what makes the
    /// blockage visible to [`Scheduler::stalled`] rather than leaving the run
    /// quietly in flight for ever.
    ///
    /// The original has no equivalent. A failed node marks its own noderun
    /// `ERROR` and the run is left in
    /// `running` for the janitor's timeout sweep to collect.
    pub fn abandon(&mut self, node_id: &NodeId) {
        self.dispatched.remove(node_id);
    }

    /// Whether any step was begun and never finished.
    ///
    /// The precise failure signal, and deliberately narrower than
    /// [`Scheduler::stalled`]: a step enters `started` only by being handed out
    /// by [`Scheduler::next_ready`], and leaves it only by completing. So this
    /// is true exactly when something was abandoned, or its completion failed
    /// and was not retried.
    ///
    /// `stalled` answers a different and looser question — it also names a join
    /// whose prerequisites can never all arrive because a conditional took the
    /// other branch, which is an ordinary outcome and not a failure at all. It
    /// is advisory, for a human deciding whether to look. This one is what
    /// decisions are made on, because acting on `stalled` would refuse to
    /// aggregate a branch that merely contains a conditional.
    ///
    /// Cheap: no allocation, and it stops at the first offender.
    pub fn has_unfinished_started(&self) -> bool {
        self.started
            .iter()
            .any(|node_id| !self.done.contains_key(node_id))
    }

    /// Whether this run never began anything at all.
    ///
    /// The other half of the failure signal, and not covered by
    /// [`Scheduler::has_unfinished_started`]: a run whose every declared start
    /// has prerequisites queues nothing, so no step is ever handed out,
    /// `started` stays empty, and zero steps running reads as a clean finish.
    ///
    /// Asks whether *anything* began, not whether every declared start
    /// finished. Two earlier attempts got this wrong in opposite directions.
    /// Requiring every declared start to be `done` wedges a run for ever
    /// whenever a start is legitimately never reached — a start with
    /// prerequisites is not seeded (see [`Scheduler::new`]) and so arrives only
    /// by an edge, which a conditional may not take. Asking nothing at all lets
    /// a run that scheduled no step at all report success. "Did anything
    /// happen" is the question that separates those without inheriting
    /// [`Scheduler::stalled`]'s false positives.
    ///
    /// Note the measure is *scheduled*, not *dispatched to a component*. A
    /// conditional or an aggregator is resolved by the interpreter itself and
    /// lands in `done` without any component being called, so a run consisting
    /// only of those answers "something happened". That is deliberate — such a
    /// run did execute its graph — but it means this is not a check that any
    /// work left the process.
    ///
    /// Only asked of a run. A branch that begins nothing is a branch whose
    /// start was blocked, which is its parent's business.
    pub fn started_nothing(&self) -> bool {
        self.scope.is_none() && self.done.is_empty() && self.started.is_empty()
    }

    /// Steps that have some prerequisites but not all of them, with nothing
    /// left to run.
    ///
    /// A join whose prerequisites can never all arrive — because a conditional
    /// took the other branch, or because a prerequisite failed — leaves the
    /// scheduler quiet with work outstanding. Quiet is exactly what completion
    /// looks like, so without this a wedged run and a finished one are the same
    /// observation.
    ///
    /// This is the port's own; the original has no equivalent, which is why a
    /// wedged run there sits in `running` until the janitor times it out.
    /// Returns empty while anything is still ready, since that is progress
    /// rather than a stall.
    pub fn stalled(&self) -> Vec<NodeId> {
        // A step handed out by `next_ready` but not yet reported is in flight,
        // not stalled. Without this a concurrent driver that dispatches
        // everything it has looks exactly like a wedge, which would make the
        // one detector for a wedged run fire on the healthy case.
        if !self.is_quiet() {
            return Vec::new();
        }
        let mut blocked: BTreeSet<NodeId> = self
            .arrived
            .keys()
            .filter(|node_id| !self.done.contains_key(*node_id))
            .cloned()
            .collect();
        // A step that was started and never finished -- abandoned, or a
        // completion that failed and was not retried. It is in neither
        // `dispatched` nor `done`, and a *branch* scheduler has no `run_starts`
        // fallback to catch it, so without this a fan-out branch whose step
        // failed reports nothing wrong and is aggregated as though it had
        // succeeded.
        blocked.extend(
            self.started
                .iter()
                .filter(|node_id| !self.done.contains_key(*node_id))
                .cloned(),
        );
        // A run that began nothing has no `arrived` entries to report, so
        // without this it looks like a clean finish of zero steps. Gated on
        // exactly that condition and not merely on being a run: a declared
        // start with prerequisites is reached by an edge, and a conditional may
        // legitimately not take it, in which case the run finished perfectly
        // well and naming that start would tell a caller the opposite.
        //
        // Known miss, accepted deliberately: a declared start with
        // prerequisites that *no* edge points at is orphaned rather than
        // skipped, and a run containing one is now reported finished with
        // nothing stalled. Telling it apart from the conditional case needs
        // static reachability over the graph, which a scheduler watching one
        // run does not have and should not acquire. It belongs in the resolver,
        // next to the same check the defect notes asks for -- both are
        // "this node can never satisfy its prerequisites", decided once at load
        // rather than guessed at every run.
        if self.started_nothing() {
            blocked.extend(
                self.run_starts
                    .iter()
                    .filter(|node_id| !self.done.contains_key(*node_id))
                    .cloned(),
            );
        }
        blocked.into_iter().collect()
    }

    /// Whether this scheduler has neither anything to run nor anything out.
    ///
    /// Quiet is not the same as finished: a branch is quiet the moment its last
    /// step is reported, and a run is quiet when a join can never be satisfied.
    /// [`Scheduler::stalled`] separates the two.
    pub fn is_quiet(&self) -> bool {
        !self.has_ready() && self.dispatched.is_empty()
    }

    /// Whether anything is waiting to run.
    pub fn has_ready(&self) -> bool {
        self.ready
            .iter()
            .any(|ready| !self.done.contains_key(&ready.node_id))
    }
}

/// The input a released step receives.
///
/// With one prerequisite it is that prerequisite's output. With several it is
/// their outputs merged — the original does the same, with a `ChainMap`
/// — because a join needs all of its
/// inputs, not whichever one happened to arrive last. Passing the last output
/// is what an earlier version of this scheduler did, and it silently dropped
/// every other branch's contribution.
///
/// **The merge order is deterministic here, and is not in the original.**
/// `ChainMap` resolves a duplicate key from the first mapping, and the list it
/// merges comes from `get_by_slugs`, which has no `ORDER BY` — so which value
/// wins is whatever the scan returns. That is the defect notes Here
/// the prerequisites are a `BTreeSet`, so they merge in `NodeId` order and the
/// **first prerequisite in that order wins** a collision, matching `ChainMap`'s
/// precedence. Deterministic and arbitrary is not the same as principled — the
/// graph does not say which branch should win — but it is reproducible, which
/// the original is not.
fn join_input(
    arrived: &BTreeSet<NodeId>,
    done: &BTreeMap<NodeId, Value>,
    just_finished: &NodeId,
    latest: &Value,
    into: &NodeId,
    needed: u64,
) -> Result<Value, ScheduleError> {
    // `case 0` never calls `_gather_step_output_from` at all -- it assigns
    // `step_input = step_output` and coerces nothing. So a
    // successor recorded with no prerequisites takes its predecessor's output
    // exactly as given, whatever shape it is. `resolve` gives every edge target
    // at least one prerequisite, so this is parity for a hand-built or
    // deserialized registry rather than a path the resolver produces.
    if needed == 0 {
        return Ok(latest.clone());
    }
    // One prerequisite is passed through unexamined, and that is the original's
    // behaviour, not a shortcut: `num_prerequisites` of 0 or 1 never reaches
    // `_gather_step_output_from`'s `if prerequisites:` block, so the
    // `InvalidDictError` check does not run (the original, `700-715`). A
    // scalar `step_output` therefore flows into a single-prerequisite successor
    // in both implementations.
    if arrived.len() <= 1 {
        // `step_input = step_input or {}`. original truthiness,
        // so *every* falsy output becomes an empty object: `null`, `false`, `0`,
        // `""`, `[]` and `{}` alike. This is not the same as passing the value
        // through -- a component that returns an empty list of extracted pages
        // hands `{}` to its successor, not `[]`, and a list aggregator
        // downstream then raises `IncompatibleListAggregatorInputError` instead
        // of fanning out zero branches.
        //
        // Only on the gather path, so only with a prerequisite: `case 0`
        // assigns `step_input = step_output` directly and coerces nothing.
        if is_falsy(latest) {
            return Ok(Value::Object(serde_json::Map::new()));
        }
        return Ok(latest.clone());
    }

    let mut merged = serde_json::Map::new();
    // Reverse `NodeId` order, so the earliest prerequisite is written last and
    // therefore wins -- ChainMap's precedence, made explicit.
    for prerequisite in arrived.iter().rev() {
        // The step that just finished is not in `done` yet -- `complete` commits
        // nothing until every successor has been resolved without error.
        let output = if prerequisite == just_finished {
            latest
        } else {
            let Some(recorded) = done.get(prerequisite) else {
                // Unreachable while `complete` is the only writer: it commits an
                // arrival and the corresponding `done` entry in the same block,
                // so every member of `arrived` other than `just_finished` has
                // already been recorded. Silently dropping it would produce a
                // join input quietly missing a branch -- the exact failure the
                // original guards with `len(step_outputs) != len(node_runs)`.
                return Err(ScheduleError::MissingPrerequisiteOutput {
                    from: prerequisite.clone(),
                    into: into.clone(),
                });
            };
            recorded
        };
        let Some(object) = output.as_object() else {
            return Err(ScheduleError::UnmergeablePrerequisite {
                from: prerequisite.clone(),
                into: into.clone(),
                kind: kind_of(output),
            });
        };
        for (key, value) in object {
            merged.insert(key.clone(), value.clone());
        }
    }
    Ok(Value::Object(merged))
}

/// Names the run's input in an [`ScheduleError::UnmergeableOverlay`].
const RUN_INPUT: &str = "the run input";

/// Names the enclosing dict aggregator's input in the same error.
const AGGREGATOR_INPUT: &str = "the enclosing dict aggregator's input";

/// Lays one input over a declared start's gathered input.
///
/// The run input wins collisions: the original writes
/// `step_input.update(run.step_input)`, and `dict.update` overwrites.
/// Either side failing to be an object is `InvalidDictError`
/// there and an error here.
fn merge_over(
    input: Value,
    overlay: &Value,
    into: &NodeId,
    which: &'static str,
) -> Result<Value, ScheduleError> {
    let Value::Object(mut merged) = input else {
        return Err(ScheduleError::UnmergeableOverlay {
            into: into.clone(),
            overlay: which,
            kind: kind_of(&input),
        });
    };
    let Some(run) = overlay.as_object() else {
        return Err(ScheduleError::UnmergeableOverlay {
            into: into.clone(),
            overlay: which,
            kind: kind_of(overlay),
        });
    };
    for (key, value) in run {
        merged.insert(key.clone(), value.clone());
    }
    Ok(Value::Object(merged))
}

/// Whether a JSON value is falsy the way original judges it.
///
/// `step_input or {}` is a truthiness test, and JSON's values
/// map onto the original's: `null`/`None`, `false`, a zero number, an empty string,
/// an empty array and an empty object are all falsy. A non-empty container is
/// truthy whatever it holds — `[0]` and `{"a": null}` included.
fn is_falsy(value: &Value) -> bool {
    match value {
        Value::Null => true,
        Value::Bool(b) => !b,
        // `0`, `0.0` and `-0.0` are all falsy; `f64` covers every JSON number
        // that can be zero, and a non-zero integer too large for `f64` is
        // truthy either way.
        Value::Number(n) => n.as_f64().is_some_and(|f| f == 0.0),
        Value::String(s) => s.is_empty(),
        Value::Array(a) => a.is_empty(),
        Value::Object(o) => o.is_empty(),
    }
}

/// What a JSON value is, for an error message.
fn kind_of(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "a boolean",
        Value::Number(_) => "a number",
        Value::String(_) => "a string",
        Value::Array(_) => "an array",
        Value::Object(_) => "an object",
    }
}

/// Everything starting a fan-out needs, resolved once.
///
/// Returned together rather than left to the caller to re-derive. An earlier
/// version handed back only the branches, so the caller looked the step up
/// again for its `refs`, its scope, and whether it was a dict — three lookups
/// that could not fail but had to be written as though they could, which is
/// unreachable error handling and unreachable code.
#[derive(Debug, Clone, PartialEq)]
pub struct Fanout<'a> {
    /// One per branch, in order.
    pub branches: Vec<SubRun>,
    /// The aggregator's own references — its scope, and the terminal children
    /// each branch's output is read from.
    pub refs: &'a AggregatorRefs,
    /// The declared starts of the enclosing aggregator, when it is a **dict**
    /// one and so merges its input into them. `None` for a
    /// list aggregator, which does not.
    pub enclosing_dict: Option<&'a StartSet>,
    /// How many declared starts were skipped for having prerequisites.
    ///
    /// The difference between "this fans out into nothing" and "every way in
    /// is blocked". Both give no branches, and they mean opposite things: the
    /// first is an empty input and aggregates to `[]`, the second is a body
    /// that cannot start and must not be given a fabricated output.
    pub blocked_starts: usize,
}

impl Fanout<'_> {
    /// The node ids a branch of this fan-out is bounded to.
    pub fn scope(&self) -> &[NodeId] {
        &self.refs.component_ids
    }
}

/// One branch of an aggregator's fan-out.
#[derive(Debug, Clone, PartialEq)]
pub struct SubRun {
    /// Where this branch begins.
    pub start: NodeId,
    /// What it receives.
    pub input: Value,
    /// Its 1-based position among the aggregator's branches.
    pub child_index: ChildIndex,
}

/// The branches an aggregator fans out into, if it is one.
///
/// `Ok(None)` for an ordinary step. The two aggregator kinds fan out
/// differently, and the difference is not cosmetic:
///
/// * A **list** aggregator fans out over its *input*: one branch per element,
///   all starting at the same node.
/// * A **dict** aggregator fans out over its *declared start ids*: one branch
///   per start, each receiving the same input — except that a start with
///   prerequisites is skipped, because something upstream will unblock it.
///
/// So a list aggregator's width is a runtime property and a dict aggregator's
/// is a property of the graph. That is the same distinction `pneuma-core` draws
/// between `ChildIndex` and `SiblingIndex`.
///
/// **Neither kind coerces its input.** A list aggregator handed a non-array, or
/// a dict aggregator handed a non-object, is an error here as it is in the
/// original (the original and `570-571`).
///
/// `child_index` counts over *all* declared starts, so skipping one leaves a gap
/// rather than renumbering — the original's `enumerate` runs over the unfiltered
/// sequence and `continue` does not consume an index.
pub fn fanout_of<'a>(
    registry: &StepRegistry,
    step: &'a Step,
    input: &Value,
) -> Result<Option<Fanout<'a>>, ScheduleError> {
    let node_id = &step.common().node_id;
    let (Step::ListAggregator { refs, .. } | Step::DictAggregator { refs, .. }) = step else {
        return Ok(None);
    };
    let (branches, enclosing_dict) = match step {
        Step::ListAggregator { .. } => (list_branches(node_id, refs, input)?, None),
        // The `refs` binding above already excluded everything but the two
        // aggregator kinds, so this arm is the dict one and there is no third
        // case to write an unreachable error for.
        _ => (
            dict_branches(registry, node_id, refs, input)?,
            Some(&refs.start),
        ),
    };
    // Deliberately one line. rustfmt splits a struct literal wider than
    // `struct_lit_width` (18 by default) across lines, and a field line that is
    // only a move compiles to no instruction of its own -- so those lines can
    // never be shown to have run, and a 100% gate reports them for ever. The
    // same reason `dict_branches` writes its skip as a positive condition
    // rather than a `continue`.
    let blocked_starts = match step {
        Step::DictAggregator { .. } => refs.start.len() - branches.len(),
        _ => 0,
    };
    #[rustfmt::skip]
    let fanout = Fanout { branches, refs, enclosing_dict, blocked_starts };
    Ok(Some(fanout))
}

/// One branch per element of the input, all starting at the same node.
fn list_branches(
    node_id: &NodeId,
    refs: &AggregatorRefs,
    input: &Value,
) -> Result<Vec<SubRun>, ScheduleError> {
    let Some(items) = input.as_array() else {
        return Err(wrong_shape(node_id, "list", "an array", input));
    };
    let start = refs.start.first_node_id().clone();
    items
        .iter()
        .enumerate()
        .map(|(index, item)| {
            Ok(SubRun {
                start: start.clone(),
                input: item.clone(),
                child_index: child_idx_at(index, node_id)?,
            })
        })
        .collect()
}

/// One branch per declared start, each receiving the same input.
fn dict_branches(
    registry: &StepRegistry,
    node_id: &NodeId,
    refs: &AggregatorRefs,
    input: &Value,
) -> Result<Vec<SubRun>, ScheduleError> {
    if !input.is_object() {
        return Err(wrong_shape(node_id, "dict", "an object", input));
    }
    let mut branches = Vec::new();
    for (index, start) in refs.start.node_ids().enumerate() {
        let Some(start_step) = registry.get(start) else {
            return Err(ScheduleError::UnknownSuccessor {
                from: node_id.clone(),
                missing: start.clone(),
            });
        };
        // A start with prerequisites is left alone: something upstream inside
        // the aggregator will unblock it, and queuing it here as well would run
        // it early, on the aggregator's input rather than its prerequisites'.
        // Written as the positive case rather than a
        // `continue`, because a bare `continue` compiles to no instruction of
        // its own and so can never be observed to have run.
        if start_step.common().num_prerequisites == 0 {
            branches.push(SubRun {
                start: start.clone(),
                input: input.clone(),
                child_index: child_idx_at(index, node_id)?,
            });
        }
    }
    Ok(branches)
}

/// An aggregator handed an input of the wrong shape.
///
/// A function rather than a literal at each site: the fields are the same two
/// constants in both, and a struct literal's field lines carry no instructions,
/// so spelling it out twice leaves lines that cannot be shown to have run.
fn wrong_shape(
    node_id: &NodeId,
    aggregator: &'static str,
    expected: &'static str,
    input: &Value,
) -> ScheduleError {
    let node_id = node_id.clone();
    let kind = kind_of(input);
    ScheduleError::IncompatibleAggregatorInput {
        node_id,
        aggregator,
        expected,
        kind,
    }
}

/// The 1-based child index of the branch at `index`.
///
/// The original writes `child_idx=index + 1` with no bound, so
/// a fan-out wider than `u32::MAX - 1` would silently wrap in Rust. It cannot
/// reach that width in practice, but "cannot in practice" is what this port
/// keeps finding to be untrue, so it is an error rather than a `as` cast.
fn child_idx_at(index: usize, node_id: &NodeId) -> Result<ChildIndex, ScheduleError> {
    u32::try_from(index)
        .ok()
        .and_then(|n| n.checked_add(1))
        .and_then(|n| ChildIndex::new(n).ok())
        .ok_or_else(|| ScheduleError::FanoutTooWide {
            node_id: node_id.clone(),
            index,
        })
}

/// The outputs a fan-out branch contributes, read from its terminal children.
///
/// **Not "the last step that ran".** The original collects the outputs of
/// `target_children` — the components whose own successors are `end`, which is
/// exactly what the aggregation barrier waits on — fetched by
/// `get_by_parent_slug(..., node_ids=parent_step.target_children)` in
/// `child_idx` order.
///
/// An earlier version of the driver used the last id it happened to execute.
/// That is a LIFO artefact: it silently drops one of a branch's two terminal
/// children, and it returned `null` for every aggregator in the corpus.
///
/// Returns a *list*, and the caller concatenates. Every branch of an
/// aggregator shares one `parent_slug` — the aggregator's own noderun
/// (the original, `584-591`) — so that single query spans all of them and
/// `[nr.step_output for nr in noderuns]` is one flat array, never an array per
/// branch. An earlier version of this returned one value per branch, collapsing
/// a lone output to a bare value. That happens to agree with the original
/// wherever each branch has exactly one terminal child, which is every case in
/// the corpus, and to disagree the moment one does not.
///
/// Only the terminal children this branch actually ran contribute; the rest
/// belong to sibling branches.
pub fn branch_outputs(refs: &AggregatorRefs, branch: &Scheduler) -> Vec<Value> {
    // `filter_map`, not `map`. `terminal_children` spans every branch of the
    // aggregator -- `resolver.rs:218` builds it from all of `agg.components`,
    // and for a dict aggregator `expected_children` is its total length, one
    // barrier arrival per terminal child across the whole body. So a single
    // branch legitimately records outputs for only its own subset, and the ones
    // it did not run are another branch's, not a hole in this one. Padding them
    // with `null` was tried and is wrong: it turned each branch of `pipeline1`
    // into `[value, null]`, which then fanned out two children instead of one.
    refs.terminal_children
        .iter()
        .filter_map(|child| branch.output_of(child).cloned())
        .collect()
}

/// The `AggregatorRefs` of a step, if it is an aggregator.
pub fn refs_of(step: &Step) -> Option<&AggregatorRefs> {
    match step {
        Step::ListAggregator { refs, .. } | Step::DictAggregator { refs, .. } => Some(refs),
        Step::Model { .. } | Step::Condition { .. } => None,
    }
}

/// An aggregator's output: its branches' outputs, in branch order.
///
/// An array for **both** kinds, which is worth stating because a "dict"
/// aggregator producing an array reads like a mistake. It is not: the original
/// builds `[nr.step_output for nr in noderuns]` for both,
/// over children fetched in `child_idx`
/// order — one flat array across every branch, not one entry per branch.
///
/// The order here is branch order, then the aggregator's declared
/// `terminal_children` order. The original's is whatever
/// `get_by_parent_slug` returns, which is database order; that is the same
/// nondeterminism the defect notes record for join input, and making
/// it deterministic is deliberate.
pub fn aggregate(outputs: Vec<Value>) -> Value {
    Value::Array(outputs)
}

/// Which steps a completed step releases.
///
/// A conditional picks exactly one branch; everything else continues to all its
/// successors. Separate so the branch rule can be tested without a scheduler.
pub fn successors_of(step: &Step, output: &Value) -> Result<Vec<NodeId>, ScheduleError> {
    match step {
        Step::Condition {
            common,
            conditions,
            children,
            ..
        } => {
            // `if not isinstance(step_input, dict): raise`.
            // Mostly redundant -- `evaluate` reads keys, and `Value::get` on a
            // non-object yields `None`, so a condition would fail with
            // `MissingKey` anyway. Not redundant for an *empty* condition list,
            // which `evaluate` answers `Ok(true)` without touching the input at
            // all (`pneuma-core/src/evaluator.rs:142-148`, matching `all(...)`
            // over an empty generator). Without this an empty conditional would
            // take its true branch on a scalar where the original refuses.
            if !output.is_object() {
                return Err(ScheduleError::IncompatibleConditionInput {
                    node_id: common.node_id.clone(),
                    kind: kind_of(output),
                });
            }
            let met = evaluate(conditions, output).map_err(|error| ScheduleError::Condition {
                node_id: common.node_id.clone(),
                reason: error.to_string(),
            })?;
            let branch = if met {
                &children.on_true
            } else {
                &children.on_false
            };
            Ok(branch.node_id().cloned().into_iter().collect())
        }
        _ => Ok(step.common().next_nodes.clone()),
    }
}

#[cfg(test)]
mod tests {
    use pneuma_core::node::Pipeline;
    use pneuma_core::resolver::resolve;
    use pneuma_core::start_set::{StartEntry, StartSet};

    use super::*;

    /// A real corpus pipeline, resolved. Invented graphs would test the
    /// scheduler against a shape the system never produces.
    fn corpus(name: &str) -> StepRegistry {
        let path = format!(
            "{}/../pneuma-core/tests/fixtures/pipelines/{name}.yaml",
            env!("CARGO_MANIFEST_DIR")
        );
        let Ok(text) = std::fs::read_to_string(&path) else {
            panic!("could not read {path}");
        };
        let Ok(pipeline) = serde_yaml::from_str::<Pipeline>(&text) else {
            panic!("{name} should parse");
        };
        match resolve(&pipeline) {
            Ok(registry) => registry,
            Err(error) => panic!("{name} should resolve: {error}"),
        }
    }

    /// Runs a whole pipeline the way a driver would, recursing into an
    /// aggregator's fan-out. Returns every step id that executed, including
    /// those inside fan-out branches.
    ///
    /// This is deliberately the same shape a real driver takes: ask what is
    /// ready, notice whether it fans out, run the branches, report the result.
    /// Runs a pipeline to completion with a stub component.
    ///
    /// Delegates to [`crate::execution::Execution`], which owns the real walk.
    /// An earlier version of this file carried its own recursive driver, so the
    /// orchestration existed twice — once in `src` and once, differently, in
    /// these tests. The tests then agreed with the copy they were testing
    /// rather than with the code that ships.
    fn drive(registry: &StepRegistry, input: Value) -> Result<Vec<NodeId>, ScheduleError> {
        drive_with_outputs(registry, input).map(|(ran, _)| ran)
    }

    /// The ids that ran, and the run's own scheduler, so a test can look at
    /// what steps produced rather than only that they ran.
    fn drive_with_outputs(
        registry: &StepRegistry,
        input: Value,
    ) -> Result<(Vec<NodeId>, Scheduler), ScheduleError> {
        let execution = crate::execution::tests::drive(registry, input)?;
        let Some(root) = execution.scheduler(crate::execution::FrameId::ROOT) else {
            panic!("an execution always has a root frame");
        };
        Ok((execution.ran().to_vec(), root.clone()))
    }

    #[test]
    fn an_aggregator_rejects_an_input_of_the_wrong_shape() {
        // The original raises rather than coercing: the original for a
        // list aggregator, `570-571` for a dict one. An earlier version of this
        // port claimed the original wrapped a scalar in a one-element list and
        // did that instead -- a citation to a line that does not exist.
        let list = corpus("pipeline4");
        let Some(agg) = list.get(&NodeId::new("X")) else {
            panic!("pipeline4 has a list aggregator X");
        };
        assert!(matches!(
            fanout_of(&list, agg, &serde_json::json!({"doc": "d"})),
            Err(ScheduleError::IncompatibleAggregatorInput {
                aggregator: "list",
                kind: "an object",
                ..
            })
        ));
        assert!(fanout_of(&list, agg, &serde_json::json!([1, 2])).is_ok());

        let dict = corpus("pipeline1");
        let Some(agg) = dict.get(&NodeId::new("X")) else {
            panic!("pipeline1 has a dict aggregator X");
        };
        assert!(matches!(
            fanout_of(&dict, agg, &serde_json::json!([1, 2])),
            Err(ScheduleError::IncompatibleAggregatorInput {
                aggregator: "dict",
                kind: "an array",
                ..
            })
        ));
    }

    #[test]
    fn a_dict_aggregator_skips_a_start_that_has_prerequisites() {
        // the original. A start with prerequisites is unblocked by
        // whatever feeds it inside the aggregator; queuing it from the fan-out as well
        // would run it early, on the aggregator's input instead.
        //
        // No corpus pipeline declares such a start, so this builds one -- which
        // is the point: without it the `continue` is unreachable and the port
        // would silently be the original minus a rule.
        let mut registry = corpus("pipeline1");
        let Some(agg) = registry.get(&NodeId::new("X")) else {
            panic!("pipeline1 has a dict aggregator X");
        };
        let Some(refs) = refs_of(agg) else {
            panic!("an aggregator has refs");
        };
        let starts: Vec<NodeId> = refs.start.node_ids().cloned().collect();
        assert!(starts.len() > 1, "this test needs a start to skip past");
        let input = serde_json::json!({});

        let Ok(Some(before)) = fanout_of(&registry, agg, &input) else {
            panic!("should fan out");
        };
        assert_eq!(before.branches.len(), starts.len(), "every start fans out");

        let Some(first) = registry.get_mut(&starts[0]) else {
            panic!("the start is in the registry");
        };
        first.common_mut().num_prerequisites = 1;

        let Some(agg) = registry.get(&NodeId::new("X")) else {
            panic!("still there");
        };
        let Ok(Some(after)) = fanout_of(&registry, agg, &input) else {
            panic!("should still fan out");
        };
        assert_eq!(
            after.branches.len(),
            starts.len() - 1,
            "the blocked start is skipped"
        );
        assert!(
            after
                .branches
                .iter()
                .all(|branch| branch.start != starts[0]),
            "and it is that one"
        );
        // The index is not renumbered: the original's `enumerate` runs over the
        // unfiltered starts and `continue` does not consume an index.
        assert_eq!(
            after.branches[0].child_index.get(),
            2,
            "the survivor keeps its declared position"
        );
    }

    #[test]
    fn a_dict_aggregator_start_must_be_in_the_registry() {
        let mut registry = corpus("pipeline1");
        let Some(agg) = registry.get_mut(&NodeId::new("X")) else {
            panic!("pipeline1 has a dict aggregator X");
        };
        match agg.aggregator_refs_mut() {
            Some(refs) => {
                refs.start = StartSet::new(
                    StartEntry {
                        node_id: NodeId::new("nowhere"),
                        key: "key_nowhere".into(),
                    },
                    vec![],
                );
            }
            None => panic!("an aggregator has refs"),
        }
        let Some(agg) = registry.get(&NodeId::new("X")) else {
            panic!("still there");
        };
        assert!(matches!(
            fanout_of(&registry, agg, &serde_json::json!({})),
            Err(ScheduleError::UnknownSuccessor { .. })
        ));
    }

    #[test]
    fn a_fanout_wider_than_a_child_index_is_an_error() {
        // Unreachable from real data -- which is why it is tested against the
        // function rather than excluded from coverage.
        let node = NodeId::new("X");
        let Ok(first) = child_idx_at(0, &node) else {
            panic!("index 0 is child 1");
        };
        assert_eq!(first.get(), 1);
        assert!(matches!(
            child_idx_at(usize::MAX, &node),
            Err(ScheduleError::FanoutTooWide { .. })
        ));
        let Ok(last) = usize::try_from(u32::MAX - 1) else {
            panic!("fits");
        };
        assert!(child_idx_at(last, &node).is_ok(), "the largest that fits");
        assert!(
            child_idx_at(last + 1, &node).is_err(),
            "and one past it does not"
        );
    }

    #[test]
    fn a_join_that_can_never_complete_is_reported_as_stalled() {
        // A quiet scheduler with an unfinished join is not a finished run.
        // Nothing distinguishes the two from `has_ready` alone, which is how a
        // wedged run passes for a successful one.
        // `pipeline_params` is A -> {B, C}, B -> {C, D}, C -> D. So D is a
        // real two-prerequisite join, fed by B and C. Named rather than
        // searched for, which was once load-bearing: `registry.iter()` walked a
        // `HashMap`, and an earlier test that picked "the first join" picked a
        // different node on different runs. It is ordered now, so naming the
        // node is merely clearer than deriving it.
        let registry = corpus("pipeline_params");
        let join = NodeId::new("D");
        let dropped = NodeId::new("C");
        match registry.get(&join) {
            Some(step) => assert_eq!(step.common().num_prerequisites, 2, "D joins B and C"),
            None => panic!("pipeline_params has D"),
        }
        let feeder = dropped;

        let mut scheduler = Scheduler::new(&registry, serde_json::json!({"doc": "d"}));
        // Run everything except one of the join's prerequisites, which is
        // abandoned rather than completed -- what a failed step leaves behind.
        // Merely dropping it is not the same thing: an unreported step is in
        // flight, and the scheduler is right to call that healthy.
        while let Some(ready) = scheduler.next_ready() {
            if ready.node_id == feeder {
                scheduler.abandon(&ready.node_id);
                continue;
            }
            let Ok(()) = scheduler.complete(&registry, &ready.node_id, serde_json::json!({"k": 1}))
            else {
                panic!("should accept");
            };
        }
        assert!(!scheduler.has_ready(), "nothing left to run");
        assert!(scheduler.output_of(&join).is_none(), "the join never ran");
        assert!(
            scheduler.stalled().contains(&join),
            "and the blocked join is named: {:?}",
            scheduler.stalled()
        );
    }

    #[test]
    fn a_run_with_work_left_is_not_stalled() {
        let registry = corpus("pipeline_params");
        let scheduler = Scheduler::new(&registry, serde_json::json!({"doc": "d"}));
        assert!(scheduler.has_ready(), "the start is ready");
        assert!(scheduler.stalled().is_empty(), "progress is not a stall");

        // And a run that finished cleanly reports no stall either.
        let Ok((_, finished)) = drive_with_outputs(&registry, serde_json::json!({"doc": "d"}))
        else {
            panic!("should schedule");
        };
        assert!(
            finished.stalled().is_empty(),
            "a finished run is not stalled"
        );
    }

    #[test]
    fn a_successor_is_queued_once_even_when_reached_twice() {
        // The original relies on an atomic `$addToSet` returning the full
        // prerequisite set to exactly one caller. Here the
        // scheduler is the only writer, so the guard in `complete` is what
        // stands in for it. Without it, `>=` re-released a join on every
        // further arrival and the successor ran more than once.
        let registry = corpus("pipeline_params");
        let mut scheduler = Scheduler::new(&registry, serde_json::json!({"doc": "d"}));
        let mut seen: Vec<NodeId> = Vec::new();
        while let Some(ready) = scheduler.next_ready() {
            let Ok(()) = scheduler.complete(&registry, &ready.node_id, serde_json::json!({"k": 1}))
            else {
                panic!("should accept");
            };
            seen.push(ready.node_id);
        }
        let mut unique = seen.clone();
        unique.sort();
        unique.dedup();
        assert_eq!(
            unique.len(),
            seen.len(),
            "no step was queued twice: {seen:?}"
        );
    }

    #[test]
    fn a_declared_start_with_prerequisites_waits_for_them() {
        // `pipeline_params` declares `A` and `D` as starts, and `D` is also the
        // diamond's two-prerequisite fan-in. `process_init` skips such a start;
        // seeding it anyway ran the join immediately on
        // the run input, before `B` or `C` existed.
        //
        // The step *counts* cannot catch this -- `D` runs exactly once either
        // way. What changes is its input, so that is what this pins.
        let registry = corpus("pipeline_params");
        let join = NodeId::new("D");
        match registry.get(&join) {
            Some(step) => assert_eq!(step.common().num_prerequisites, 2),
            None => panic!("pipeline_params has D"),
        }
        assert!(
            registry.start().node_ids().any(|id| *id == join),
            "and D really is a declared start -- otherwise this proves nothing"
        );

        let run_input = serde_json::json!({"doc": "d", "from": "the run"});
        let mut scheduler = Scheduler::new(&registry, run_input.clone());
        assert_eq!(
            scheduler.next_ready().map(|r| r.node_id),
            Some(NodeId::new("A")),
            "only A is seeded"
        );

        let mut scheduler = Scheduler::new(&registry, run_input.clone());
        let mut join_input = None;
        while let Some(ready) = scheduler.next_ready() {
            if ready.node_id == join {
                join_input = Some(ready.input.clone());
            }
            let output = serde_json::json!({ ready.node_id.to_string(): "ran" });
            let Ok(()) = scheduler.complete(&registry, &ready.node_id, output) else {
                panic!("should accept");
            };
        }
        // Its prerequisites' outputs, with the run input laid over the top --
        // `step_input.update(run.step_input)`, where the
        // run input wins collisions because `dict.update` overwrites. An earlier
        // version of this test asserted `{"B", "C"}` alone, which pinned a
        // missing merge as though it were a decision.
        assert_eq!(
            join_input,
            Some(serde_json::json!({
                "B": "ran",
                "C": "ran",
                "doc": "d",
                "from": "the run"
            })),
            "D joins its prerequisites, then the run input is merged over them"
        );
        assert!(scheduler.stalled().is_empty(), "and the run completes");
    }

    #[test]
    fn an_overlay_must_be_an_object_and_says_which_one_failed() {
        // `InvalidDictError` if either side is not a dict,
        // and the message must name the merge that failed -- the run's input
        // and the enclosing aggregator's are different things to go and look at.
        let into = NodeId::new("D");
        let Ok(merged) = merge_over(
            serde_json::json!({"B": 1, "shared": "from the join"}),
            &serde_json::json!({"doc": "d", "shared": "from the run"}),
            &into,
            RUN_INPUT,
        ) else {
            panic!("two objects merge");
        };
        assert_eq!(
            merged,
            serde_json::json!({"B": 1, "doc": "d", "shared": "from the run"}),
            "the run input wins collisions, as `dict.update` does"
        );

        assert!(matches!(
            merge_over(
                serde_json::json!([1, 2]),
                &serde_json::json!({"doc": "d"}),
                &into,
                RUN_INPUT,
            ),
            Err(ScheduleError::UnmergeableOverlay {
                kind: "an array",
                overlay: RUN_INPUT,
                ..
            })
        ));
        assert!(matches!(
            merge_over(
                serde_json::json!({}),
                &serde_json::json!("scalar"),
                &into,
                RUN_INPUT,
            ),
            Err(ScheduleError::UnmergeableOverlay {
                kind: "a string",
                overlay: RUN_INPUT,
                ..
            })
        ));

        // The same failure from the other merge names the aggregator instead,
        // so nobody is sent to `run.step_input` over an aggregator's input.
        let Err(error) = merge_over(
            serde_json::json!({}),
            &serde_json::json!([]),
            &into,
            AGGREGATOR_INPUT,
        ) else {
            panic!("an array overlay cannot merge");
        };
        assert!(
            error.to_string().contains("dict aggregator"),
            "the message should name the aggregator: {error}"
        );
    }

    #[test]
    fn a_prerequisite_that_arrived_without_an_output_is_an_error() {
        // Unreachable while `complete` is the only writer, which is why it is
        // driven against `join_input` directly rather than excluded. Dropping
        // it silently would hand the join an input missing a whole branch.
        let arrived: BTreeSet<NodeId> = [NodeId::new("B"), NodeId::new("C")].into_iter().collect();
        let mut done = BTreeMap::new();
        done.insert(NodeId::new("B"), serde_json::json!({"B": 1}));
        // `C` arrived but was never recorded.
        assert!(matches!(
            join_input(
                &arrived,
                &done,
                &NodeId::new("B"),
                &serde_json::json!({"B": 1}),
                &NodeId::new("D"),
                2,
            ),
            Err(ScheduleError::MissingPrerequisiteOutput { .. })
        ));
    }

    #[test]
    fn a_step_in_flight_is_not_a_stall_and_is_not_queued_twice() {
        // `next_ready` hands a step out; until it is reported the run is
        // working, not wedged. A driver running steps concurrently dispatches
        // everything it has and would otherwise look exactly like a wedge.
        let registry = corpus("pipeline_params");
        let mut scheduler = Scheduler::new(&registry, serde_json::json!({"doc": "d"}));
        let Some(first) = scheduler.next_ready() else {
            panic!("A is ready");
        };
        assert!(!scheduler.has_ready(), "nothing else is queued yet");
        assert!(
            scheduler.stalled().is_empty(),
            "a dispatched step is in flight, not stalled"
        );

        // And it cannot be handed out a second time while it is out.
        let Ok(()) = scheduler.complete(&registry, &first.node_id, serde_json::json!({"k": 1}))
        else {
            panic!("should accept");
        };
        let Some(second) = scheduler.next_ready() else {
            panic!("something was released");
        };
        assert!(
            !scheduler.ready.iter().any(|r| r.node_id == second.node_id),
            "a dispatched step is not still queued"
        );
        assert!(
            scheduler.dispatched.contains(&second.node_id),
            "and it is recorded as in flight"
        );
    }

    #[test]
    fn a_run_whose_every_start_is_blocked_is_reported_rather_than_looking_finished() {
        // If no declared start can be seeded, nothing runs and `arrived` stays
        // empty -- which without this reads as a clean finish of zero steps.
        let mut registry = corpus("pipeline_params");
        let starts: Vec<NodeId> = registry.start().node_ids().cloned().collect();
        for id in &starts {
            match registry.get_mut(id) {
                Some(step) => step.common_mut().num_prerequisites = 1,
                None => panic!("{id} is in the registry"),
            }
        }
        let mut scheduler = Scheduler::new(&registry, serde_json::json!({"doc": "d"}));
        assert!(scheduler.next_ready().is_none(), "nothing could be seeded");
        assert!(scheduler.completed() == 0, "and nothing ran");
        let stalled = scheduler.stalled();
        for id in &starts {
            assert!(stalled.contains(id), "{id} should be reported: {stalled:?}");
        }
    }

    #[test]
    fn a_dict_aggregator_merges_its_own_input_into_a_start_it_reaches_by_edge() {
        // The third merge in `_gather_step_output_from`, after the
        // prerequisites and the run input: if the step's parent is a *dict*
        // aggregator and the step is one of its declared starts, the
        // aggregator's own `step_input` is laid over the gathered input
        // -- and wins, since it is the later `update`.
        //
        // Reached only by an edge inside the aggregator body: a start with
        // prerequisites is skipped by the fan-out. No corpus pipeline has that
        // shape, so the shape is built here. `pipeline1`'s `X` declares starts
        // `C` and `D`; wiring `C -> D` and giving `D` a prerequisite makes `D`
        // a start that the fan-out skips and an edge reaches.
        let mut registry = corpus("pipeline1");
        match registry.get_mut(&NodeId::new("C")) {
            Some(step) => step.common_mut().next_nodes = vec![NodeId::new("D")],
            None => panic!("pipeline1 has C"),
        }
        match registry.get_mut(&NodeId::new("D")) {
            Some(step) => step.common_mut().num_prerequisites = 1,
            None => panic!("pipeline1 has D"),
        }
        let Some(Step::DictAggregator { refs, .. }) = registry.get(&NodeId::new("X")) else {
            panic!("X is a dict aggregator");
        };
        let starts = refs.start.clone();
        let scope = refs.component_ids.clone();

        let run = Scheduler::new(&registry, serde_json::json!({"run": "input"}));
        let aggregator_input = serde_json::json!({"agg": "input", "shared": "from the aggregator"});
        let mut branch = Scheduler::scoped(
            &[NodeId::new("C")],
            &scope,
            aggregator_input,
            &run,
            Some(&starts),
        );

        let Some(first) = branch.next_ready() else {
            panic!("C is ready");
        };
        assert_eq!(first.node_id, NodeId::new("C"));
        let Ok(()) = branch.complete(
            &registry,
            &first.node_id,
            serde_json::json!({"C": "ran", "shared": "from C"}),
        ) else {
            panic!("should accept");
        };

        let Some(second) = branch.next_ready() else {
            panic!("C releases D");
        };
        assert_eq!(second.node_id, NodeId::new("D"));
        assert_eq!(
            second.input,
            serde_json::json!({
                "C": "ran",
                "agg": "input",
                "shared": "from the aggregator"
            }),
            "the aggregator's input is merged over C's output and wins"
        );
        // The run input is *not* merged: `D` is a start of the aggregator, not
        // of the run.
        assert!(
            second.input.get("run").is_none(),
            "a branch start is not a run start"
        );
    }

    #[test]
    fn a_list_aggregator_does_not_merge_its_input_into_its_branch() {
        // the original tests `isinstance(parent_step, DictAggregatorStep)`
        // explicitly, so a list aggregator's branches get no such merge --
        // which is why `scoped` takes the start set only for a dict.
        let registry = corpus("pipeline4");
        let Some(Step::ListAggregator { refs, .. }) = registry.get(&NodeId::new("X")) else {
            panic!("pipeline4's X is a list aggregator");
        };
        let scope = refs.component_ids.clone();
        let run = Scheduler::new(&registry, serde_json::json!({"run": "input"}));
        let branch = Scheduler::scoped(
            &[NodeId::new("B")],
            &scope,
            serde_json::json!({"item": 1}),
            &run,
            None,
        );
        assert!(
            branch.parent_starts.is_empty(),
            "no enclosing dict, so nothing to merge"
        );
        assert_eq!(branch.parent_input, Value::Null);
    }

    #[test]
    fn a_falsy_single_prerequisite_output_becomes_an_empty_object() {
        // `step_input = step_input or {}` is original truthiness,
        // so every falsy output is replaced -- not just null. A component
        // returning an empty list of pages hands `{}` to its successor, and a
        // list aggregator downstream then raises rather than fanning out zero
        // branches.
        let arrived: BTreeSet<NodeId> = [NodeId::new("B")].into_iter().collect();
        let done = BTreeMap::new();
        let empty = serde_json::json!({});
        for falsy in [
            Value::Null,
            serde_json::json!(false),
            serde_json::json!(0),
            serde_json::json!(0.0),
            serde_json::json!(""),
            serde_json::json!([]),
            serde_json::json!({}),
        ] {
            assert!(is_falsy(&falsy), "{falsy} is falsy in original");
            let Ok(input) = join_input(
                &arrived,
                &done,
                &NodeId::new("B"),
                &falsy,
                &NodeId::new("C"),
                1,
            ) else {
                panic!("a single prerequisite always yields an input");
            };
            assert_eq!(input, empty, "{falsy} should become an empty object");
        }

        // And a truthy value passes through untouched, container or not.
        for truthy in [
            serde_json::json!(true),
            serde_json::json!(1),
            serde_json::json!(-1.5),
            serde_json::json!("x"),
            serde_json::json!([0]),
            serde_json::json!({"a": null}),
        ] {
            assert!(!is_falsy(&truthy), "{truthy} is truthy in original");
            let Ok(input) = join_input(
                &arrived,
                &done,
                &NodeId::new("B"),
                &truthy,
                &NodeId::new("C"),
                1,
            ) else {
                panic!("a single prerequisite always yields an input");
            };
            assert_eq!(input, truthy, "a non-empty container is truthy");
        }
    }

    #[test]
    fn a_conditional_requires_an_object_even_with_no_conditions() {
        // the original rejects a non-dict input to a conditional. Reading
        // keys would fail anyway -- except for an empty condition list, which
        // `evaluate` answers `Ok(true)` without touching the input, so an empty
        // conditional would take its true branch on a scalar.
        let registry = corpus("pipeline_condition_list");
        let Some(conditional) = registry
            .iter()
            .find(|step| matches!(step, Step::Condition { .. }))
            .cloned()
        else {
            panic!("pipeline_condition_list has a conditional");
        };
        assert!(matches!(
            successors_of(&conditional, &serde_json::json!("scalar")),
            Err(ScheduleError::IncompatibleConditionInput {
                kind: "a string",
                ..
            })
        ));

        // With the conditions emptied it must still refuse.
        let mut empty = conditional;
        match &mut empty {
            Step::Condition { conditions, .. } => conditions.clear(),
            _ => panic!("it is a conditional"),
        }
        assert!(matches!(
            successors_of(&empty, &serde_json::json!([1])),
            Err(ScheduleError::IncompatibleConditionInput {
                kind: "an array",
                ..
            })
        ));
        // And an object with no conditions takes the true branch, as
        // `all(...)` over an empty generator does.
        let Ok(taken) = successors_of(&empty, &serde_json::json!({})) else {
            panic!("an object is acceptable");
        };
        assert_eq!(taken.len(), 1, "exactly one branch is taken");
    }

    #[test]
    fn a_failed_completion_is_a_report_not_still_in_flight() {
        // `complete` returning `Err` means the driver came back -- the step is
        // no longer running. Leaving it in `dispatched` would silence
        // `stalled()` for ever if the driver gives up, which is precisely the
        // failure `stalled()` exists to surface.
        // `pipeline_params` is A -> {B, C}, B -> {C, D}, C -> D. `C` joins `A`
        // and `B`, so completing `B` with a scalar is what fails: a single
        // prerequisite passes through unexamined, but a merge cannot take one.
        let registry = corpus("pipeline_params");
        let mut scheduler = Scheduler::new(&registry, serde_json::json!({"doc": "d"}));
        let Some(first) = scheduler.next_ready() else {
            panic!("A is ready");
        };
        assert_eq!(first.node_id, NodeId::new("A"));
        let Ok(()) = scheduler.complete(&registry, &first.node_id, serde_json::json!({"A": 1}))
        else {
            panic!("should accept");
        };

        let Some(b) = scheduler.next_ready() else {
            panic!("B is released");
        };
        assert_eq!(b.node_id, NodeId::new("B"));
        assert!(scheduler.dispatched.contains(&b.node_id), "in flight");

        let Err(_) = scheduler.complete(&registry, &b.node_id, serde_json::json!("scalar")) else {
            panic!("a scalar cannot be merged into C's join input");
        };
        assert!(
            !scheduler.dispatched.contains(&b.node_id),
            "a failed report is not still in flight"
        );
        assert!(
            scheduler.output_of(&b.node_id).is_none(),
            "and nothing was recorded, so a retry is still possible"
        );
        assert!(
            scheduler.stalled().contains(&NodeId::new("C")),
            "so the wedge is visible rather than silent: {:?}",
            scheduler.stalled()
        );

        // And the retry the half-advance guard protects still works.
        let Ok(()) = scheduler.complete(&registry, &b.node_id, serde_json::json!({"B": 1})) else {
            panic!("a retry with a usable output should be accepted");
        };
        assert!(scheduler.has_ready(), "the run continues");
    }

    #[test]
    fn a_successor_with_no_prerequisites_takes_its_input_uncoerced() {
        // `case 0` assigns `step_input = step_output` without gathering, so it
        // neither coerces a falsy value nor merges anything.
        // `resolve` gives every edge target at least one
        // prerequisite, so this is parity for a hand-built registry rather than a live path --
        // which is exactly why it is asserted against `join_input` directly.
        let arrived: BTreeSet<NodeId> = [NodeId::new("B")].into_iter().collect();
        let done = BTreeMap::new();
        for value in [serde_json::json!([]), Value::Null, serde_json::json!(0)] {
            let Ok(input) = join_input(
                &arrived,
                &done,
                &NodeId::new("B"),
                &value,
                &NodeId::new("C"),
                0,
            ) else {
                panic!("no gather, no failure");
            };
            assert_eq!(
                input, value,
                "{value} passes through when nothing is gathered"
            );
        }
    }

    #[test]
    fn a_declared_start_with_no_step_is_not_reported_as_stalled() {
        // `new` seeds from `start_steps`, which skips a start id with no
        // matching step -- deliberate defence in depth for a hand-built or
        // deserialized registry. `run_starts` must agree, or that id is never
        // seeded, never completed, and reported stalled after a run that in
        // fact succeeded.
        let registry = corpus("pipeline_case1");
        let start = registry.start().clone();
        let ghost = StartSet::new(
            start.head().clone(),
            vec![StartEntry {
                node_id: NodeId::new("ghost"),
                key: "key_ghost".into(),
            }],
        );
        let steps: std::collections::BTreeMap<NodeId, Step> = registry
            .iter()
            .map(|step| (step.common().node_id.clone(), step.clone()))
            .collect();
        let haunted = StepRegistry::new(steps, ghost);
        assert!(haunted.get(&NodeId::new("ghost")).is_none(), "no such step");

        let Ok((_, scheduler)) = drive_with_outputs(&haunted, serde_json::json!({"doc": "d"}))
        else {
            panic!("the real steps should still run");
        };
        assert!(
            scheduler.stalled().is_empty(),
            "a start id with no step is absorbed, not reported: {:?}",
            scheduler.stalled()
        );
    }

    #[test]
    fn a_requeued_step_does_not_jump_ahead_of_its_siblings() {
        // `next_ready` pops from the end, so `push` would hand a requeued step
        // straight back. A step that fails deterministically would then be
        // retried first on every call, for ever, and the siblings that could
        // have made progress would never be dispatched.
        let mut scheduler = Scheduler::default();
        scheduler.ready.push(Ready {
            node_id: NodeId::new("first"),
            input: serde_json::json!(1),
        });
        scheduler.ready.push(Ready {
            node_id: NodeId::new("second"),
            input: serde_json::json!(2),
        });

        let Some(taken) = scheduler.next_ready() else {
            panic!("something is ready");
        };
        assert_eq!(taken.node_id, NodeId::new("second"), "LIFO");
        scheduler.requeue(taken);

        let Some(next) = scheduler.next_ready() else {
            panic!("the sibling is still queued");
        };
        assert_eq!(
            next.node_id,
            NodeId::new("first"),
            "the requeued step waits its turn"
        );
        let Some(again) = scheduler.next_ready() else {
            panic!("and the requeued one comes back after it");
        };
        assert_eq!(again.node_id, NodeId::new("second"));
        // Requeueing also undoes the bookkeeping, so a step handed back without
        // being attempted does not count as begun. `first` still does: it was
        // taken and never reported.
        scheduler.requeue(again);
        assert!(!scheduler.started.contains(&NodeId::new("second")));
        assert!(scheduler.started.contains(&NodeId::new("first")));
        assert!(scheduler.has_unfinished_started());
    }

    #[test]
    fn a_linear_pipeline_runs_every_step_once() {
        let registry = corpus("pipeline_params");
        let Ok(order) = drive(&registry, serde_json::json!({"doc": "d"})) else {
            panic!("should schedule");
        };
        assert_eq!(order.len(), registry.len(), "every step ran");

        let mut unique = order.clone();
        unique.sort();
        unique.dedup();
        assert_eq!(unique.len(), order.len(), "and none ran twice");
    }

    #[test]
    fn a_join_waits_for_every_distinct_prerequisite() {
        // The module's headline claim is that prerequisites are a set, not a
        // tally. An earlier version of this test completed one arbitrary start
        // step three times and asserted `completed()` rose by one -- which a
        // tally satisfies just as well, so it proved nothing about the claim it
        // was named for.
        //
        // This feeds a two-prerequisite join the *same* prerequisite repeatedly
        // and requires it not to be released. Under a tally the third report
        // would release it with one branch missing.
        let (registry, a, b, join) = diamond();
        let mut scheduler = Scheduler::new(&registry, serde_json::json!({}));

        for _ in 0..3 {
            let Ok(()) = scheduler.complete(&registry, &a, serde_json::json!({"from_a": 1})) else {
                panic!("should accept");
            };
        }
        let released: Vec<Ready> = std::iter::from_fn(|| scheduler.next_ready())
            .filter(|ready| ready.node_id == join)
            .collect();
        assert!(
            released.is_empty(),
            "one prerequisite reported three times must not release a two-prerequisite join"
        );

        // And the distinct second one does release it.
        let Ok(()) = scheduler.complete(&registry, &b, serde_json::json!({"from_b": 2})) else {
            panic!("should accept");
        };
        let Some(ready) =
            std::iter::from_fn(|| scheduler.next_ready()).find(|ready| ready.node_id == join)
        else {
            panic!("the distinct prerequisite should release the join");
        };
        assert_eq!(ready.input, serde_json::json!({"from_a": 1, "from_b": 2}));
    }

    #[test]
    fn a_failed_completion_leaves_the_scheduler_able_to_retry() {
        // `complete` used to mutate before doing the fallible work, so an error
        // left the run half-advanced -- and because it returns Ok(()) for an
        // already-done step, the retry advanced nothing and the error could
        // never be raised again.
        let (registry, a, b, join) = diamond();
        let mut scheduler = Scheduler::new(&registry, serde_json::json!({}));
        let Ok(()) = scheduler.complete(&registry, &a, serde_json::json!({"ok": 1})) else {
            panic!("should accept");
        };
        let before = scheduler.completed();

        // A scalar cannot merge into the join's input.
        assert!(scheduler
            .complete(&registry, &b, serde_json::json!("scalar"))
            .is_err());
        assert_eq!(
            scheduler.completed(),
            before,
            "a failed completion must not be recorded"
        );

        // So the same step can be reported again, correctly, and the join runs.
        //
        // Distinct keys. Using `ok` on both branches makes this exercise the
        // collision rule instead -- the first NodeId wins, so the assertion
        // would be about precedence rather than about the retry. An earlier
        // attempt to fix that never applied to the file, and the test only
        // started failing once `diamond()` became deterministic.
        let Ok(()) = scheduler.complete(&registry, &b, serde_json::json!({"from_b": 2})) else {
            panic!("the retry should be accepted");
        };
        let Some(ready) =
            std::iter::from_fn(|| scheduler.next_ready()).find(|ready| ready.node_id == join)
        else {
            panic!("the join should be released after the retry");
        };
        assert_eq!(ready.input, serde_json::json!({"ok": 1, "from_b": 2}));
    }

    #[test]
    fn a_conditional_releases_exactly_one_branch() {
        let registry = corpus("pipeline_condition_list");
        let conditionals: Vec<&Step> = registry
            .iter()
            .filter(|step| matches!(step, Step::Condition { .. }))
            .collect();
        assert!(
            !conditionals.is_empty(),
            "this corpus pipeline should contain a conditional"
        );

        // Inputs shaped for the corpus's own conditions -- `equals` on key
        // "equals", `contains` on "contains", `isempty` on "isempty" -- so the
        // evaluator actually succeeds and a branch is genuinely selected. A
        // test that only ever hit the error path left the branch selection
        // uncovered, which is how this was noticed.
        let mut selected_true = 0usize;
        let mut selected_false = 0usize;
        for step in conditionals {
            for input in [
                serde_json::json!({"equals": "value", "contains": "a value here", "isempty": ""}),
                serde_json::json!({"equals": "other", "contains": "nothing", "isempty": "full"}),
            ] {
                let Ok(taken) = successors_of(step, &input) else {
                    continue;
                };
                // Either branch, or none if that branch is `end` -- never both.
                assert!(
                    taken.len() <= 1,
                    "a conditional released {} successors",
                    taken.len()
                );
                if taken.is_empty() {
                    continue;
                }
                let Step::Condition { children, .. } = step else {
                    panic!("filtered to conditionals");
                };
                if children.on_true.node_id() == taken.first() {
                    selected_true += 1;
                } else {
                    selected_false += 1;
                }
            }
        }
        // Guard the guard: if no input ever satisfied a condition, the branch
        // selection above would never run and this test would prove nothing.
        assert!(
            selected_true > 0 && selected_false > 0,
            "both branches must be exercised; got {selected_true} true and {selected_false} false"
        );
    }

    #[test]
    fn a_non_conditional_releases_all_its_successors() {
        let registry = corpus("pipeline1");
        for step in registry.iter() {
            if matches!(step, Step::Condition { .. }) {
                continue;
            }
            let Ok(taken) = successors_of(step, &serde_json::json!({})) else {
                panic!("a non-conditional cannot fail");
            };
            assert_eq!(taken, step.common().next_nodes);
        }
    }

    #[test]
    fn a_branch_contributes_its_terminal_children_outputs() {
        let mut branch = Scheduler::default();
        // Recorded by hand: `complete` needs a registry, and this is about
        // reading the outputs, not about scheduling.
        let registry = corpus("pipeline_params");
        let ids: Vec<NodeId> = {
            let mut v: Vec<NodeId> = registry
                .iter()
                .map(|step| step.common().node_id.clone())
                .collect();
            v.sort();
            v
        };
        let Some(first) = ids.first().cloned() else {
            panic!("the corpus pipeline should have steps");
        };
        branch.ready.push(Ready {
            node_id: first.clone(),
            input: serde_json::json!({}),
        });
        let Some(ready) = branch.next_ready() else {
            panic!("should be ready");
        };
        let Ok(()) = branch.complete(&registry, &ready.node_id, serde_json::json!({"out": 1}))
        else {
            panic!("should accept");
        };

        // One terminal child contributes one entry -- not a bare value, because
        // the caller concatenates branches into one flat array.
        let one = AggregatorRefs {
            start: registry.start().clone(),
            component_ids: vec![],
            terminal_children: vec![first.clone()],
        };
        assert_eq!(
            branch_outputs(&one, &branch),
            vec![serde_json::json!({"out": 1})]
        );

        // A terminal child this branch never ran belongs to a sibling branch,
        // so it contributes nothing.
        let sibling = AggregatorRefs {
            start: registry.start().clone(),
            component_ids: vec![],
            terminal_children: vec![first.clone(), NodeId::new("another-branch")],
        };
        assert_eq!(
            branch_outputs(&sibling, &branch),
            vec![serde_json::json!({"out": 1})]
        );

        // Two that both ran contribute two entries, in declared order.
        let mut both = branch.clone();
        both.done
            .insert(NodeId::new("second"), serde_json::json!({"out": 2}));
        let two = AggregatorRefs {
            start: registry.start().clone(),
            component_ids: vec![],
            terminal_children: vec![first, NodeId::new("second")],
        };
        assert_eq!(
            branch_outputs(&two, &both),
            vec![serde_json::json!({"out": 1}), serde_json::json!({"out": 2})]
        );

        // No terminal children at all: nothing to contribute, so the branch
        // adds nothing to the aggregate rather than adding a `null`.
        let none = AggregatorRefs {
            start: registry.start().clone(),
            component_ids: vec![],
            terminal_children: vec![],
        };
        assert!(branch_outputs(&none, &branch).is_empty());
    }

    #[test]
    fn only_an_aggregator_has_refs() {
        let registry = corpus("pipeline_nested_list_dict");
        let mut aggregators = 0usize;
        let mut plain = 0usize;
        for step in registry.iter() {
            match (refs_of(step), step) {
                (Some(_), Step::ListAggregator { .. } | Step::DictAggregator { .. }) => {
                    aggregators += 1;
                }
                (None, Step::Model { .. } | Step::Condition { .. }) => plain += 1,
                (found, other) => {
                    panic!("refs_of disagreed with the step kind: {found:?} {other:?}")
                }
            }
        }
        assert!(aggregators > 0, "this pipeline should contain aggregators");
        assert!(plain > 0, "and ordinary steps");
    }

    #[test]
    fn an_unknown_step_is_an_error_not_a_silent_skip() {
        // Lost when a neighbouring block was rewritten; the coverage gate is
        // what noticed, since the arm went uncovered.
        let registry = corpus("pipeline_params");
        let mut scheduler = Scheduler::new(&registry, serde_json::json!({}));
        let ghost = NodeId::new("nosuchnode");
        assert_eq!(
            scheduler.complete(&registry, &ghost, serde_json::json!({})),
            Err(ScheduleError::UnknownStep {
                node_id: ghost.clone()
            })
        );
        assert_eq!(
            ScheduleError::UnknownStep { node_id: ghost }.to_string(),
            "step nosuchnode is not in the registry"
        );
    }

    #[test]
    fn a_successor_that_is_not_in_the_registry_stops_the_run() {
        // The resolver rejects this at load time -- a recorded deviation -- so
        // it cannot arise from a corpus pipeline. It is reachable by mutation,
        // and the arm has to exist because `StepRegistry` is constructible
        // directly. The original logs a warning and carries on, which leaves a
        // downstream join waiting forever.
        let mut registry = corpus("pipeline_params");
        // Not a conditional: a `Condition`'s successors come from its branches,
        // so a ghost in `next_nodes` would be ignored there and this test would
        // pass while proving nothing. Found by writing it the other way first.
        let Some(victim) = registry
            .iter()
            .find(|step| !matches!(step, Step::Condition { .. }))
            .map(|step| step.common().node_id.clone())
        else {
            panic!("this pipeline should contain a non-conditional step");
        };
        let ghost = NodeId::new("nosuchsuccessor");
        let Some(step) = registry.get_mut(&victim) else {
            panic!("the step should be gettable");
        };
        step.common_mut().next_nodes.push(ghost.clone());

        let mut scheduler = Scheduler::new(&registry, serde_json::json!({}));
        let outcome = scheduler.complete(&registry, &victim, serde_json::json!({}));
        assert_eq!(
            outcome,
            Err(ScheduleError::UnknownSuccessor {
                from: victim,
                missing: ghost,
            }),
            "an unresolvable successor must stop rather than warn"
        );
    }

    #[test]
    fn a_condition_that_cannot_be_evaluated_is_terminal() {
        // `evaluate` rejects a non-numeric operand against a numeric input -- a
        // typed error rather than a coercion, which is a recorded deviation.
        // Retrying will not change the answer, so it is not a retryable state.
        let registry = corpus("pipeline_condition_list");
        let Some(conditional) = registry
            .iter()
            .find(|step| matches!(step, Step::Condition { .. }))
        else {
            panic!("this pipeline should contain a conditional");
        };

        // Feed each conditional an input shaped to make evaluation fail, and
        // require at least one to actually fail -- otherwise this test would
        // pass while exercising nothing.
        let hostile = [
            serde_json::json!({"value": {"nested": "object"}}),
            serde_json::json!({"value": []}),
            serde_json::json!({"value": null}),
            serde_json::json!("not an object"),
        ];
        let failed = hostile.iter().any(|input| {
            matches!(
                successors_of(conditional, input),
                Err(ScheduleError::Condition { .. })
            )
        });
        assert!(
            failed,
            "no hostile input made the evaluator fail; this test would prove nothing"
        );
    }

    /// Builds a registry where `join` has two prerequisites, `a` and `b`.
    fn diamond() -> (StepRegistry, NodeId, NodeId, NodeId) {
        let mut registry = corpus("pipeline_params");
        // The join must not itself be a start node: it would then be in the
        // initial ready set as well as being released by the merge, and the
        // "released once" assertion would fail for a reason that has nothing to
        // do with joining. Found by writing it without this.
        let starts: BTreeSet<NodeId> = registry
            .start_steps()
            .map(|step| step.common().node_id.clone())
            .collect();
        // `StepRegistry::iter` is in `NodeId` order, so this is already
        // deterministic and the sort below is belt-and-braces. It was not
        // always: the registry walked a `HashMap`, and picking `a`, `b` and the
        // join from that iteration made this helper -- and every test built on
        // it -- intermittently choose different nodes. It passed most of the
        // time, which is worse than failing.
        let mut ids: Vec<NodeId> = registry
            .iter()
            .filter(|step| !matches!(step, Step::Condition { .. }))
            .map(|step| step.common().node_id.clone())
            .collect();
        ids.sort();
        let Some(join) = ids.iter().find(|id| !starts.contains(id)).cloned() else {
            panic!("need a non-conditional step that is not a start node");
        };
        let feeders: Vec<NodeId> = ids.into_iter().filter(|id| *id != join).take(2).collect();
        assert_eq!(feeders.len(), 2, "need two steps to feed the join");
        let (a, b) = (feeders[0].clone(), feeders[1].clone());

        for from in [&a, &b] {
            let Some(step) = registry.get_mut(from) else {
                panic!("step should exist");
            };
            step.common_mut().next_nodes = vec![join.clone()];
        }
        let Some(step) = registry.get_mut(&join) else {
            panic!("join should exist");
        };
        step.common_mut().num_prerequisites = 2;
        step.common_mut().next_nodes.clear();
        (registry, a, b, join)
    }

    #[test]
    fn a_join_receives_every_prerequisites_output_merged() {
        // Passing only the last output silently drops every other branch's
        // contribution, which is what an earlier version of this did.
        let (registry, a, b, join) = diamond();
        let mut scheduler = Scheduler::new(&registry, serde_json::json!({}));

        let Ok(()) = scheduler.complete(&registry, &a, serde_json::json!({"from_a": 1})) else {
            panic!("should accept");
        };
        let Ok(()) = scheduler.complete(&registry, &b, serde_json::json!({"from_b": 2})) else {
            panic!("should accept");
        };

        let released: Vec<Ready> = std::iter::from_fn(|| scheduler.next_ready())
            .filter(|ready| ready.node_id == join)
            .collect();
        assert_eq!(released.len(), 1, "the join is released once");
        assert_eq!(
            released[0].input,
            serde_json::json!({"from_a": 1, "from_b": 2}),
            "both branches contribute"
        );
    }

    #[test]
    fn a_key_collision_resolves_the_same_way_every_time() {
        // The original's answer depends on the row order `get_by_slugs`
        // happens to return, because it has no ORDER BY -- the defect notes
        // Here the prerequisites are a BTreeSet, so the order is fixed, and the
        // earliest NodeId wins, matching ChainMap's first-mapping precedence.
        let (registry, a, b, join) = diamond();
        let mut winners = std::collections::BTreeSet::new();

        // Completing in either order must give the same answer.
        for reversed in [false, true] {
            let mut scheduler = Scheduler::new(&registry, serde_json::json!({}));
            let order = if reversed { [&b, &a] } else { [&a, &b] };
            for node in order {
                // Keyed on the NODE, not on completion position. Keying it on
                // position changed the data as well as the order, so the test
                // failed for the wrong reason -- it was measuring its own
                // input, not the merge.
                let value = serde_json::json!({ "shared": format!("from_{node}") });
                let Ok(()) = scheduler.complete(&registry, node, value) else {
                    panic!("should accept");
                };
            }
            let Some(ready) =
                std::iter::from_fn(|| scheduler.next_ready()).find(|ready| ready.node_id == join)
            else {
                panic!("the join should be released");
            };
            let Some(shared) = ready.input.get("shared").and_then(Value::as_str) else {
                panic!("the merged input should carry the shared key");
            };
            winners.insert(shared.to_owned());
        }
        assert_eq!(
            winners.len(),
            1,
            "completion order must not change the answer; got {winners:?}"
        );
    }

    #[test]
    fn a_prerequisite_that_is_not_an_object_cannot_be_merged() {
        // A component may return a scalar step_output -- valid by the component
        // contract, forwarded by the original executor -- and the original raises
        // InvalidDictError here too.
        let (registry, a, b, join) = diamond();
        let mut scheduler = Scheduler::new(&registry, serde_json::json!({}));
        let Ok(()) = scheduler.complete(&registry, &a, serde_json::json!({"ok": 1})) else {
            panic!("should accept");
        };
        let outcome = scheduler.complete(&registry, &b, serde_json::json!("a scalar"));
        let Err(ScheduleError::UnmergeablePrerequisite { into, kind, .. }) = outcome else {
            panic!("a scalar prerequisite must not merge silently");
        };
        assert_eq!(into, join);
        assert_eq!(kind, "a string");
    }

    #[test]
    fn a_single_prerequisite_passes_its_output_through_unmerged() {
        // No merge, so a non-object output is fine on a one-prerequisite edge.
        let registry = corpus("pipeline_params");
        let mut scheduler = Scheduler::new(&registry, serde_json::json!({}));
        let Some(first) = scheduler.next_ready() else {
            panic!("something should be ready");
        };
        let Ok(()) = scheduler.complete(&registry, &first.node_id, serde_json::json!([1, 2]))
        else {
            panic!("a single prerequisite need not be an object");
        };
    }

    #[test]
    fn the_value_kinds_are_named_in_the_error() {
        for (value, expected) in [
            (serde_json::json!(null), "null"),
            (serde_json::json!(true), "a boolean"),
            (serde_json::json!(1), "a number"),
            (serde_json::json!("s"), "a string"),
            (serde_json::json!([]), "an array"),
            (serde_json::json!({}), "an object"),
        ] {
            assert_eq!(kind_of(&value), expected);
        }
        assert_eq!(
            ScheduleError::UnmergeablePrerequisite {
                from: NodeId::new("a"),
                into: NodeId::new("j"),
                kind: "a string",
            }
            .to_string(),
            "prerequisite a of j produced a string, which cannot be merged into a join input"
        );
    }

    #[test]
    fn the_errors_say_what_is_wrong() {
        assert_eq!(
            ScheduleError::UnknownSuccessor {
                from: NodeId::new("a"),
                missing: NodeId::new("b"),
            }
            .to_string(),
            "step a names successor b, which is not in the registry"
        );
        assert_eq!(
            ScheduleError::Condition {
                node_id: NodeId::new("c"),
                reason: "bad operand".to_owned(),
            }
            .to_string(),
            "condition on c could not be evaluated: bad operand"
        );
    }

    #[test]
    fn output_and_progress_are_reported() {
        let registry = corpus("pipeline_params");
        let mut scheduler = Scheduler::new(&registry, serde_json::json!({}));
        assert!(scheduler.has_ready());
        assert_eq!(scheduler.completed(), 0);

        let Some(ready) = scheduler.next_ready() else {
            panic!("something should be ready");
        };
        let Ok(()) = scheduler.complete(&registry, &ready.node_id, serde_json::json!({"out": 1}))
        else {
            panic!("should accept");
        };
        assert_eq!(
            scheduler.output_of(&ready.node_id),
            Some(&serde_json::json!({"out": 1}))
        );
        assert_eq!(scheduler.output_of(&NodeId::new("absent")), None);
        assert_eq!(scheduler.completed(), 1);
    }

    #[test]
    fn a_default_scheduler_has_nothing_ready() {
        let mut scheduler = Scheduler::default();
        assert!(!scheduler.has_ready());
        assert_eq!(scheduler.next_ready(), None);
        assert_eq!(scheduler.completed(), 0);
    }
}
