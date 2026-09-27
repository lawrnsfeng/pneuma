//! The **resolved** form of a pipeline — what the engine executes, as
//! opposed to [`crate::node::Node`], which is what an author writes.
//!
//! Resolution (see [`crate::resolver`]) flattens the authored tree into a
//! [`StepRegistry`]: every node, at every nesting depth, becomes one entry in
//! a flat map keyed by [`NodeId`], with its position in the graph
//! precomputed — `full_path`, `parent_id`, `sibling_index`, `next_nodes`, and
//! the `num_prerequisites` fan-in counter.
//!
//! # Names that differ from the original
//!
//! The wire names are preserved via `#[serde(rename)]`; only the Rust-side
//! names change, and only where the original's is actively unclear:
//!
//! | Original | Here | Why |
//! |---|---|---|
//! | `nth` | `sibling_index` | "nth" alone says nothing; this is a node's static position among the components its parent declares — see [`crate::sibling_index`], which is a different index from `child_index` |
//! | `target_children` | `terminal_children` | these are specifically the children whose own successors are `end` — "terminal" states that, "target" does not |
//! | `num_children` | `expected_children` | matches the Postgres column the barrier design already settled on, so the pure type and the eventual column share one name |
//!
//! # Nested components become flat references
//!
//! `AggregatorStep.components` in the original holds the full nested subtree,
//! *duplicating* every descendant that the flat
//! registry already contains. Here an aggregator stores
//! [`AggregatorRefs::component_ids`] instead — ids into the same registry —
//! so there is exactly one authoritative copy of each step. See
//! the design notes.

use std::collections::BTreeMap;

use compact_str::CompactString;
use serde::{Deserialize, Serialize};

use crate::{
    condition::Condition, ids::NodeId, node::ConditionalSuccessors, sibling_index::SiblingIndex,
    start_set::StartSet,
};

/// A resolved step's lifecycle state.
///
/// Deliberately **not** [`crate::status::NodeStatus`], which is a different
/// vocabulary: this one has `Pending` and `Started`, which `NodeStatus`
/// lacks, and lacks `Created` and `Cancelled`, which it has. The original
/// keeps them separate too — a step's status
/// within the resolved graph is not the same thing as a noderun's status in
/// the ledger, and conflating them would put the wrong strings on the wire.
///
/// The original splits this into three near-identical enums, one per step
/// kind, differing only in that `ModelNodeStepStatus` omits the four
/// aggregator-specific variants. That distinction is not worth three types
/// here; a `Model` step simply never takes those values in practice.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StepStatus {
    #[default]
    Pending,
    Started,
    Processing,
    Finished,
    Error,
    TimedOut,
    /// Aggregators only.
    Forked,
    /// Aggregators only.
    Aggregated,
    /// Aggregators only.
    HasChildError,
    /// Aggregators only.
    HasChildTimedOut,
}

impl StepStatus {
    /// Every variant, for exhaustive iteration in tests.
    pub const ALL: [StepStatus; 10] = [
        StepStatus::Pending,
        StepStatus::Started,
        StepStatus::Processing,
        StepStatus::Finished,
        StepStatus::Error,
        StepStatus::TimedOut,
        StepStatus::Forked,
        StepStatus::Aggregated,
        StepStatus::HasChildError,
        StepStatus::HasChildTimedOut,
    ];
}

/// The fields every resolved step carries, whatever its kind.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StepCommon {
    pub node_id: NodeId,
    /// The component name — **also the subject work is published to**
    /// (`send_dict(..., topic=step.name)`). The original inherits this from
    /// `BaseNode` via `BaseStep`; dropping it would
    /// leave the resolved registry unable to say where to dispatch a step.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<CompactString>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<CompactString>,
    /// The dotted path from the pipeline root to this step, assigned during
    /// resolution. `None` only on a step that has not been resolved yet.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub full_path: Option<CompactString>,
    /// The enclosing aggregator, if this step is nested inside one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_id: Option<NodeId>,
    /// Successors, resolved and validated against the registry. Distinct
    /// from [`crate::node::ModelNode::successors`], which is merely
    /// *declared* — the different name marks that difference.
    #[serde(default)]
    pub next_nodes: Vec<NodeId>,
    /// How many predecessors must finish before this step may run — the
    /// fan-in counter.
    #[serde(default)]
    pub num_prerequisites: u32,
    /// This node's static position among the components its parent declares.
    /// `nth` on the wire.
    ///
    /// Deliberately **not** a [`crate::child_index::ChildIndex`]: that is the runtime index of the
    /// input item being fanned out, while this is a property of the graph. See
    /// [`crate::sibling_index`] for the full distinction — an earlier revision
    /// of this file conflated the two.
    #[serde(rename = "nth", default = "default_sibling_index")]
    pub sibling_index: SiblingIndex,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub params: BTreeMap<CompactString, CompactString>,
}

fn default_sibling_index() -> SiblingIndex {
    SiblingIndex::FIRST
}

impl StepCommon {
    /// A minimally-populated step, before resolution fills in position.
    pub fn new(node_id: NodeId) -> Self {
        Self {
            node_id,
            name: None,
            version: None,
            full_path: None,
            parent_id: None,
            next_nodes: Vec::new(),
            num_prerequisites: 0,
            sibling_index: SiblingIndex::FIRST,
            params: BTreeMap::new(),
        }
    }
}

/// The aggregator-specific half of a resolved aggregator step.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AggregatorRefs {
    /// Which nested component(s) the fan-out begins at.
    pub start: StartSet,
    /// Flat references into the same [`StepRegistry`] — see the module docs.
    #[serde(default)]
    pub component_ids: Vec<NodeId>,
    /// The nested components whose own successors are `end`, i.e. the ones
    /// whose completion the aggregation barrier waits on.
    /// `target_children` on the wire.
    #[serde(rename = "target_children", default)]
    pub terminal_children: Vec<NodeId>,
}

/// One resolved step.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum Step {
    Model {
        #[serde(flatten)]
        common: StepCommon,
        #[serde(default)]
        status: StepStatus,
        #[serde(flatten, default, skip_serializing_if = "BTreeMap::is_empty")]
        extra: BTreeMap<String, serde_json::Value>,
    },
    ListAggregator {
        #[serde(flatten)]
        common: StepCommon,
        #[serde(default)]
        status: StepStatus,
        #[serde(flatten)]
        refs: AggregatorRefs,
        #[serde(flatten, default, skip_serializing_if = "BTreeMap::is_empty")]
        extra: BTreeMap<String, serde_json::Value>,
    },
    DictAggregator {
        #[serde(flatten)]
        common: StepCommon,
        #[serde(default)]
        status: StepStatus,
        #[serde(flatten)]
        refs: AggregatorRefs,
        /// How many terminal children the aggregation barrier expects.
        /// `num_children` on the wire; named for the Postgres column the
        /// barrier design settled on.
        #[serde(
            rename = "num_children",
            default,
            skip_serializing_if = "Option::is_none"
        )]
        expected_children: Option<u32>,
        #[serde(flatten, default, skip_serializing_if = "BTreeMap::is_empty")]
        extra: BTreeMap<String, serde_json::Value>,
    },
    Condition {
        #[serde(flatten)]
        common: StepCommon,
        #[serde(default)]
        conditions: Vec<Condition>,
        children: ConditionalSuccessors,
        #[serde(flatten, default, skip_serializing_if = "BTreeMap::is_empty")]
        extra: BTreeMap<String, serde_json::Value>,
    },
}

impl Step {
    /// The common half, whatever the kind.
    pub fn common(&self) -> &StepCommon {
        match self {
            Step::Model { common, .. }
            | Step::ListAggregator { common, .. }
            | Step::DictAggregator { common, .. }
            | Step::Condition { common, .. } => common,
        }
    }

    /// Mutable access to the common half — resolution fills these in.
    pub fn common_mut(&mut self) -> &mut StepCommon {
        match self {
            Step::Model { common, .. }
            | Step::ListAggregator { common, .. }
            | Step::DictAggregator { common, .. }
            | Step::Condition { common, .. } => common,
        }
    }

    /// Unmodelled wire fields carried through a round trip.
    ///
    /// Declared per-variant rather than on [`StepCommon`] because a
    /// catch-all nested inside one flattened struct greedily absorbs the
    /// keys belonging to a *sibling* flattened struct — here it would eat
    /// [`AggregatorRefs`]'s fields. Placing it last in each variant lets the
    /// named and structured fields claim theirs first.
    pub fn extra(&self) -> &BTreeMap<String, serde_json::Value> {
        match self {
            Step::Model { extra, .. }
            | Step::ListAggregator { extra, .. }
            | Step::DictAggregator { extra, .. }
            | Step::Condition { extra, .. } => extra,
        }
    }

    /// This step's id.
    pub fn node_id(&self) -> &NodeId {
        &self.common().node_id
    }

    /// The aggregator half, if this step is an aggregator.
    pub fn aggregator_refs(&self) -> Option<&AggregatorRefs> {
        match self {
            Step::ListAggregator { refs, .. } | Step::DictAggregator { refs, .. } => Some(refs),
            Step::Model { .. } | Step::Condition { .. } => None,
        }
    }

    /// Mutable aggregator half — resolution appends terminal children.
    pub fn aggregator_refs_mut(&mut self) -> Option<&mut AggregatorRefs> {
        match self {
            Step::ListAggregator { refs, .. } | Step::DictAggregator { refs, .. } => Some(refs),
            Step::Model { .. } | Step::Condition { .. } => None,
        }
    }

    /// Whether this step fans out and aggregates.
    pub fn is_aggregator(&self) -> bool {
        self.aggregator_refs().is_some()
    }
}

/// Every step in a resolved pipeline, flat and keyed by id.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StepRegistry {
    steps: BTreeMap<NodeId, Step>,
    start: StartSet,
}

impl StepRegistry {
    /// Builds a registry directly. [`crate::resolver::resolve`] is the
    /// normal way to obtain one.
    pub fn new(steps: BTreeMap<NodeId, Step>, start: StartSet) -> Self {
        Self { steps, start }
    }

    /// Looks up a step by id.
    pub fn get(&self, id: &NodeId) -> Option<&Step> {
        self.steps.get(id)
    }

    /// Mutable lookup — resolution's second pass links successors.
    pub fn get_mut(&mut self, id: &NodeId) -> Option<&mut Step> {
        self.steps.get_mut(id)
    }

    /// Whether the registry contains this id.
    pub fn contains(&self, id: &NodeId) -> bool {
        self.steps.contains_key(id)
    }

    /// How many steps the registry holds.
    pub fn len(&self) -> usize {
        self.steps.len()
    }

    /// Whether the registry is empty.
    pub fn is_empty(&self) -> bool {
        self.steps.is_empty()
    }

    /// Every step, **in `NodeId` order**.
    ///
    /// The order is part of the contract, not an accident of the container.
    /// `Execution::run_output` builds a run's result by walking this, and
    /// Restate replays a handler in a *different process* -- where a
    /// `HashMap`'s per-process seed gives a different order. Measured before
    /// this was a `BTreeMap`: the same seven keys iterated in four different
    /// orders across four processes, so a replayed run could report its outputs
    /// in an order the journal did not have.
    ///
    /// It had already bitten twice inside this repository, both times patched
    /// at the call site rather than here -- a resolver error that named a
    /// different aggregator each run, and a test helper that "passed most of
    /// the time, which is worse than failing".
    pub fn iter(&self) -> impl Iterator<Item = &Step> {
        self.steps.values()
    }

    /// The pipeline's start directive.
    pub fn start(&self) -> &StartSet {
        &self.start
    }

    /// The steps execution begins from, skipping any start id with no
    /// corresponding step.
    pub fn start_steps(&self) -> impl Iterator<Item = &Step> {
        self.start.node_ids().filter_map(|id| self.get(id))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{child_ref::ChildRef, start_set::StartEntry};

    fn entry(node: &str) -> StartEntry {
        StartEntry {
            node_id: NodeId::new(node),
            key: CompactString::from(node),
        }
    }

    fn model(id: &str) -> Step {
        Step::Model {
            common: StepCommon::new(NodeId::new(id)),
            status: StepStatus::Pending,
            extra: BTreeMap::new(),
        }
    }

    fn aggregator_refs(start: &str, components: &[&str], terminal: &[&str]) -> AggregatorRefs {
        AggregatorRefs {
            start: StartSet::new(entry(start), Vec::new()),
            component_ids: components.iter().map(|c| NodeId::new(*c)).collect(),
            terminal_children: terminal.iter().map(|c| NodeId::new(*c)).collect(),
        }
    }

    /// Built by hand rather than via `resolve()` on purpose: this module's
    /// coverage must stand on its own, not depend on the resolver being
    /// correct. The resolver has its own tests.
    fn registry() -> StepRegistry {
        let mut steps = BTreeMap::new();
        steps.insert(NodeId::new("A"), model("A"));
        steps.insert(
            NodeId::new("Agg"),
            Step::ListAggregator {
                common: StepCommon::new(NodeId::new("Agg")),
                status: StepStatus::Pending,
                refs: aggregator_refs("Inner", &["Inner"], &["Inner"]),
                extra: BTreeMap::new(),
            },
        );
        steps.insert(NodeId::new("Inner"), model("Inner"));
        steps.insert(
            NodeId::new("Cond"),
            Step::Condition {
                common: StepCommon::new(NodeId::new("Cond")),
                conditions: vec![Condition::IsEmpty { key: "k".into() }],
                children: ConditionalSuccessors {
                    on_true: ChildRef::Node(NodeId::new("A")),
                    on_false: ChildRef::End,
                },
                extra: BTreeMap::new(),
            },
        );
        StepRegistry::new(steps, StartSet::new(entry("A"), Vec::new()))
    }

    #[test]
    fn get_finds_a_present_step_and_misses_an_absent_one() {
        let reg = registry();
        assert_eq!(
            reg.get(&NodeId::new("A")).map(Step::node_id),
            Some(&NodeId::new("A"))
        );
        assert!(reg.get(&NodeId::new("nope")).is_none());
    }

    #[test]
    fn get_mut_allows_resolution_to_fill_in_position() {
        let mut reg = registry();
        match reg.get_mut(&NodeId::new("A")) {
            Some(step) => {
                step.common_mut().num_prerequisites = 2;
                step.common_mut().full_path = Some(CompactString::from("p.A"));
            }
            None => panic!("A must be present"),
        }
        match reg.get(&NodeId::new("A")) {
            Some(step) => {
                assert_eq!(step.common().num_prerequisites, 2);
                assert_eq!(step.common().full_path.as_deref(), Some("p.A"));
            }
            None => panic!("A must still be present"),
        }
        assert!(reg.get_mut(&NodeId::new("nope")).is_none());
    }

    #[test]
    fn contains_len_and_is_empty_agree() {
        let reg = registry();
        assert!(reg.contains(&NodeId::new("Agg")));
        assert!(!reg.contains(&NodeId::new("nope")));
        assert_eq!(reg.len(), 4);
        assert!(!reg.is_empty());

        let empty = StepRegistry::new(BTreeMap::new(), StartSet::new(entry("A"), Vec::new()));
        assert!(empty.is_empty());
        assert_eq!(empty.len(), 0);
    }

    #[test]
    fn iter_yields_every_step() {
        let reg = registry();
        assert_eq!(reg.iter().count(), 4);
    }

    #[test]
    fn start_exposes_the_directive() {
        let reg = registry();
        assert_eq!(reg.start().first_node_id(), &NodeId::new("A"));
    }

    #[test]
    fn start_steps_resolves_ids_to_steps() {
        let reg = registry();
        let ids: Vec<&NodeId> = reg.start_steps().map(Step::node_id).collect();
        assert_eq!(ids, vec![&NodeId::new("A")]);
    }

    /// A start id with no matching step is skipped rather than panicking —
    /// the resolver rejects that case up front, so this is defence in depth
    /// for a hand-built or deserialized registry.
    #[test]
    fn start_steps_skips_an_unknown_start_id() {
        let reg = StepRegistry::new(BTreeMap::new(), StartSet::new(entry("ghost"), Vec::new()));
        assert_eq!(reg.start_steps().count(), 0);
    }

    #[test]
    fn common_accessors_work_for_every_kind() {
        let reg = registry();
        for id in ["A", "Agg", "Inner", "Cond"] {
            match reg.get(&NodeId::new(id)) {
                Some(step) => assert_eq!(step.common().node_id, NodeId::new(id)),
                None => panic!("{id} must be present"),
            }
        }
    }

    #[test]
    fn common_mut_works_for_every_kind() {
        let mut steps = BTreeMap::new();
        steps.insert(NodeId::new("A"), model("A"));
        steps.insert(
            NodeId::new("L"),
            Step::ListAggregator {
                common: StepCommon::new(NodeId::new("L")),
                status: StepStatus::Pending,
                refs: aggregator_refs("A", &["A"], &["A"]),
                extra: BTreeMap::new(),
            },
        );
        steps.insert(
            NodeId::new("D"),
            Step::DictAggregator {
                common: StepCommon::new(NodeId::new("D")),
                status: StepStatus::Pending,
                refs: aggregator_refs("A", &["A"], &["A"]),
                expected_children: Some(1),
                extra: BTreeMap::new(),
            },
        );
        steps.insert(
            NodeId::new("C"),
            Step::Condition {
                common: StepCommon::new(NodeId::new("C")),
                conditions: Vec::new(),
                children: ConditionalSuccessors {
                    on_true: ChildRef::End,
                    on_false: ChildRef::End,
                },
                extra: BTreeMap::new(),
            },
        );
        let mut reg = StepRegistry::new(steps, StartSet::new(entry("A"), Vec::new()));

        for id in ["A", "L", "D", "C"] {
            match reg.get_mut(&NodeId::new(id)) {
                Some(step) => step.common_mut().num_prerequisites += 1,
                None => panic!("{id} must be present"),
            }
        }
        for id in ["A", "L", "D", "C"] {
            match reg.get(&NodeId::new(id)) {
                Some(step) => assert_eq!(step.common().num_prerequisites, 1),
                None => panic!("{id} must be present"),
            }
        }
    }

    #[test]
    fn aggregator_refs_discriminate_by_kind() {
        let reg = registry();
        match reg.get(&NodeId::new("Agg")) {
            Some(step) => {
                assert!(step.is_aggregator());
                match step.aggregator_refs() {
                    Some(refs) => {
                        assert_eq!(refs.component_ids, vec![NodeId::new("Inner")]);
                        assert_eq!(refs.terminal_children, vec![NodeId::new("Inner")]);
                    }
                    None => panic!("Agg must expose aggregator refs"),
                }
            }
            None => panic!("Agg must be present"),
        }

        for id in ["A", "Cond"] {
            match reg.get(&NodeId::new(id)) {
                Some(step) => {
                    assert!(!step.is_aggregator());
                    assert!(step.aggregator_refs().is_none());
                }
                None => panic!("{id} must be present"),
            }
        }
    }

    #[test]
    fn aggregator_refs_mut_allows_appending_terminal_children() {
        let mut reg = registry();
        match reg
            .get_mut(&NodeId::new("Agg"))
            .and_then(Step::aggregator_refs_mut)
        {
            Some(refs) => refs.terminal_children.push(NodeId::new("Extra")),
            None => panic!("Agg must expose mutable refs"),
        }
        match reg.get(&NodeId::new("Agg")).and_then(Step::aggregator_refs) {
            Some(refs) => assert_eq!(refs.terminal_children.len(), 2),
            None => panic!("Agg must expose refs"),
        }

        // Non-aggregators yield None from the mutable accessor too.
        assert!(reg
            .get_mut(&NodeId::new("A"))
            .and_then(Step::aggregator_refs_mut)
            .is_none());
    }

    #[test]
    fn step_common_new_defaults_are_unresolved() {
        let common = StepCommon::new(NodeId::new("A"));
        assert_eq!(common.full_path, None);
        assert_eq!(common.parent_id, None);
        assert!(common.next_nodes.is_empty());
        assert_eq!(common.num_prerequisites, 0);
        assert_eq!(common.sibling_index, SiblingIndex::FIRST);
        assert!(common.params.is_empty());
        assert_eq!(common.name, None);
        assert_eq!(common.version, None);
    }

    #[test]
    fn step_status_defaults_to_pending() {
        assert_eq!(StepStatus::default(), StepStatus::Pending);
    }

    /// `StepStatus` is a different vocabulary from `NodeStatus` — pinned so
    /// the two are not quietly merged later. `pending`/`started` exist only
    /// here; `created`/`cancelled` only there.
    #[test]
    fn step_status_wire_values_match_the_original() -> Result<(), serde_json::Error> {
        let expected = [
            (StepStatus::Pending, "\"pending\""),
            (StepStatus::Started, "\"started\""),
            (StepStatus::Processing, "\"processing\""),
            (StepStatus::Finished, "\"finished\""),
            (StepStatus::Error, "\"error\""),
            (StepStatus::TimedOut, "\"timed_out\""),
            (StepStatus::Forked, "\"forked\""),
            (StepStatus::Aggregated, "\"aggregated\""),
            (StepStatus::HasChildError, "\"has_child_error\""),
            (StepStatus::HasChildTimedOut, "\"has_child_timed_out\""),
        ];
        assert_eq!(expected.len(), StepStatus::ALL.len());
        for (status, wire) in expected {
            assert_eq!(serde_json::to_string(&status)?, wire);
            let back: StepStatus = serde_json::from_str(wire)?;
            assert_eq!(back, status);
        }
        Ok(())
    }

    #[test]
    fn step_status_rejects_node_status_only_values() {
        // `created` and `cancelled` belong to NodeStatus, not StepStatus.
        assert!(serde_json::from_str::<StepStatus>("\"created\"").is_err());
        assert!(serde_json::from_str::<StepStatus>("\"cancelled\"").is_err());
    }

    #[test]
    fn a_model_step_round_trips_with_the_nth_wire_name() -> Result<(), serde_json::Error> {
        let step = model("A");
        let json = serde_json::to_string(&step)?;
        assert!(
            json.contains("\"nth\":1"),
            "sibling_index must serialize as nth: {json}"
        );
        assert!(json.contains("\"type\":\"Model\""));
        let back: Step = serde_json::from_str(&json)?;
        assert_eq!(back, step);
        Ok(())
    }

    #[test]
    fn an_aggregator_step_round_trips_with_its_wire_names() -> Result<(), serde_json::Error> {
        let step = Step::DictAggregator {
            common: StepCommon::new(NodeId::new("D")),
            status: StepStatus::Pending,
            refs: aggregator_refs("A", &["A", "B"], &["B"]),
            expected_children: Some(2),
            extra: BTreeMap::new(),
        };
        let json = serde_json::to_string(&step)?;
        assert!(
            json.contains("\"target_children\":[\"B\"]"),
            "terminal_children must serialize as target_children: {json}"
        );
        assert!(
            json.contains("\"num_children\":2"),
            "expected_children must serialize as num_children: {json}"
        );
        let back: Step = serde_json::from_str(&json)?;
        assert_eq!(back, step);
        Ok(())
    }

    #[test]
    fn a_condition_step_round_trips() -> Result<(), serde_json::Error> {
        let reg = registry();
        let step = match reg.get(&NodeId::new("Cond")) {
            Some(step) => step.clone(),
            None => panic!("Cond must be present"),
        };
        let json = serde_json::to_string(&step)?;
        let back: Step = serde_json::from_str(&json)?;
        assert_eq!(back, step);
        Ok(())
    }

    #[test]
    fn a_registry_round_trips() -> Result<(), serde_json::Error> {
        let reg = registry();
        let json = serde_json::to_string(&reg)?;
        let back: StepRegistry = serde_json::from_str(&json)?;
        assert_eq!(back, reg);
        Ok(())
    }

    /// `name` is the dispatch subject, so losing it across a round trip
    /// would leave the controller unable to publish the step's work.
    #[test]
    fn name_and_version_survive_a_round_trip() -> Result<(), serde_json::Error> {
        let mut common = StepCommon::new(NodeId::new("A"));
        common.name = Some(CompactString::from("invoice.page.default.A"));
        common.version = Some(CompactString::from("latest"));
        let step = Step::Model {
            common,
            status: StepStatus::Pending,
            extra: BTreeMap::new(),
        };

        let json = serde_json::to_string(&step)?;
        assert!(
            json.contains("\"name\":\"invoice.page.default.A\""),
            "{json}"
        );
        assert!(json.contains("\"version\":\"latest\""), "{json}");
        let back: Step = serde_json::from_str(&json)?;
        assert_eq!(back, step);
        Ok(())
    }

    /// The same catch-all discipline `node.rs` applies: `Step` is persisted
    /// in the run document's `state`, which the gateway re-exposes publicly,
    /// so an unmodelled field must not be silently dropped.
    #[test]
    fn unmodelled_step_fields_survive_a_round_trip() -> Result<(), serde_json::Error> {
        let json = r#"{"type":"Model","node_id":"A","refcounts":{"x":1}}"#;
        let step: Step = serde_json::from_str(json)?;
        assert!(step.extra().contains_key("refcounts"));
        let back = serde_json::to_string(&step)?;
        assert!(back.contains("\"refcounts\""), "refcounts dropped: {back}");

        // Every variant exposes the catch-all, including the two that also
        // carry a flattened `AggregatorRefs` — the case that made a
        // `StepCommon`-level catch-all unworkable.
        let reg = registry();
        for id in ["A", "Agg", "Cond"] {
            match reg.get(&NodeId::new(id)) {
                Some(step) => assert!(step.extra().is_empty()),
                None => panic!("{id} must be present"),
            }
        }
        let dict = Step::DictAggregator {
            common: StepCommon::new(NodeId::new("D")),
            status: StepStatus::Pending,
            refs: aggregator_refs("A", &["A"], &["A"]),
            expected_children: Some(1),
            extra: BTreeMap::new(),
        };
        assert!(dict.extra().is_empty());
        Ok(())
    }

    #[test]
    fn a_step_missing_nth_defaults_to_the_first_sibling() -> Result<(), serde_json::Error> {
        let step: Step = serde_json::from_str(r#"{"type":"Model","node_id":"A"}"#)?;
        assert_eq!(step.common().sibling_index, SiblingIndex::FIRST);
        Ok(())
    }
}
