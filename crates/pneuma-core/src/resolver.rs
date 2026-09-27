//! Flattens an authored [`Pipeline`] into an executable [`StepRegistry`].
//!
//! Two passes, mirroring `RunResolver`:
//!
//! 1. **Register** — walk the nested `components` tree, giving every node a
//!    registry entry and its graph position: `full_path`, `parent_id`,
//!    `sibling_index`, plus each aggregator's `terminal_children` (and, for a
//!    `DictAggregator`, `expected_children`).
//! 2. **Link** — walk it again, resolving each node's declared successors
//!    into `next_nodes` and incrementing the fan-in counter
//!    (`num_prerequisites`) on each target.
//!
//! # Two deliberate differences from the original
//!
//! **An unknown successor is an error, not a warning.** the original
//! logs `"child = %s not found in registry"` and *continues*. The edge is
//! silently dropped, so the intended target's `num_prerequisites` is one
//! lower than the author meant — and a downstream join then waits forever for
//! a prerequisite that structurally cannot arrive. A typo in a pipeline
//! definition becomes a permanently stalled run. Here it is
//! [`ResolveError::UnknownSuccessor`], raised at load.
//!
//! **Cycles are detected.** The original has no cycle check at all, and —
//! verified — `_update_nextnode` only recurses over the static `components`
//! nesting, never over the `next_nodes` edges it writes, so a cyclic
//! definition does not hang the resolver. It produces a corrupted graph whose
//! cycle surfaces later, as a runtime hang in whatever walks `next_nodes`.
//! This is therefore new behaviour with no original precedent to differentially
//! validate against, which is why its property test (an injected back-edge is
//! always rejected) carries the weight here.

use std::collections::BTreeMap;

use compact_str::CompactString;

use crate::{
    ids::{NodeId, PipelineId},
    node::{Node, Pipeline},
    sibling_index::SiblingIndex,
    step::{AggregatorRefs, Step, StepCommon, StepRegistry, StepStatus},
};

/// Why a pipeline could not be resolved.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ResolveError {
    /// Two nodes share an id. Ids must be unique across the *whole* nested
    /// pipeline, not merely among siblings, because the registry is flat.
    #[error("duplicate node id {0}")]
    Duplicate(NodeId),
    /// A node declares a successor that no node in the pipeline provides.
    #[error("node {parent} declares unknown successor {successor}")]
    UnknownSuccessor { parent: NodeId, successor: NodeId },
    /// A `start` directive names a node the pipeline does not contain.
    #[error("start directive names unknown node {node_id}")]
    UnknownStartNode { node_id: NodeId },
    /// An aggregator's `start` names a node outside its own components.
    #[error("aggregator {aggregator} starts on {node_id}, which is not one of its components")]
    StartNodeOutsideAggregator { aggregator: NodeId, node_id: NodeId },
    /// A node id collides with the `end` sentinel, so nothing can reference it.
    #[error("node id {0} is reserved: it collides with the `end` sentinel")]
    ReservedNodeId(NodeId),
    /// The successor graph contains a cycle reaching this node.
    #[error("successor graph contains a cycle through {0}")]
    Cycle(NodeId),
}

/// Resolves a pipeline definition into a flat, executable registry.
pub fn resolve(pipeline: &Pipeline) -> Result<StepRegistry, ResolveError> {
    let mut steps = BTreeMap::new();

    for node in &pipeline.components {
        register(
            node,
            None,
            None,
            SiblingIndex::FIRST,
            &pipeline.pipeline_id,
            &mut steps,
        )?;
    }

    validate_start_ids(pipeline, &steps)?;

    let mut edges = Vec::new();
    for node in &pipeline.components {
        collect_edges(node, &steps, &mut edges)?;
    }
    apply_edges(edges, &mut steps);

    detect_cycles(&steps)?;

    Ok(StepRegistry::new(steps, pipeline.start.clone()))
}

/// Pass one: register `node` and everything nested inside it.
fn register(
    node: &Node,
    parent_id: Option<NodeId>,
    parent_path: Option<&str>,
    sibling_index: SiblingIndex,
    pipeline_id: &PipelineId,
    steps: &mut BTreeMap<NodeId, Step>,
) -> Result<(), ResolveError> {
    let node_id = node.node_id().clone();
    if steps.contains_key(&node_id) {
        return Err(ResolveError::Duplicate(node_id));
    }
    // A node called `end` can never be referenced: every `children: [end]`
    // parses as `ChildRef::End`, so nothing ever links to it, yet it would
    // still be registered and counted toward an aggregator's
    // `expected_children` — a barrier waiting on a child that cannot run.
    if node_id.as_str() == crate::child_ref::END_SENTINEL {
        return Err(ResolveError::ReservedNodeId(node_id));
    }

    // `{parent_path or pipeline_id}.{node_id}` — the original.
    let full_path = match parent_path {
        Some(path) => format!("{path}.{node_id}"),
        None => format!("{pipeline_id}.{node_id}"),
    };

    let mut common = StepCommon::new(node_id.clone());
    common.parent_id = parent_id;
    common.sibling_index = sibling_index;
    common.full_path = Some(CompactString::from(full_path.as_str()));
    common.params = node_params(node);
    let (name, version) = node_name_and_version(node);
    common.name = name;
    common.version = version;

    let step = match node {
        Node::Model(_) => Step::Model {
            common,
            status: StepStatus::default(),
            extra: Default::default(),
        },
        Node::Condition(c) => Step::Condition {
            common,
            conditions: c.conditions.clone(),
            children: c.successors.clone(),
            extra: Default::default(),
        },
        Node::ListAggregator(agg) => Step::ListAggregator {
            common,
            status: StepStatus::default(),
            refs: aggregator_refs(agg),
            extra: Default::default(),
        },
        Node::DictAggregator(agg) => Step::DictAggregator {
            common,
            status: StepStatus::default(),
            refs: aggregator_refs(agg),
            // The original counts one per terminal child,
            // leaving it `None` when there are none.
            expected_children: match terminal_children(&agg.components).len() {
                0 => None,
                n => Some(n as u32),
            },
            extra: Default::default(),
        },
    };
    steps.insert(node_id.clone(), step);

    // A ListAggregator numbers its children 1, 2, 3...; every other parent
    // gives each child index 1.
    let is_list = matches!(node, Node::ListAggregator(_));
    for (position, child) in node.components().iter().enumerate() {
        let child_index = if is_list {
            index_at(position)
        } else {
            SiblingIndex::FIRST
        };
        register(
            child,
            Some(node_id.clone()),
            Some(full_path.as_str()),
            child_index,
            pipeline_id,
            steps,
        )?;
    }

    Ok(())
}

/// 1-based sibling index for a 0-based position, saturating rather than
/// wrapping. A pipeline with `u32::MAX` siblings is not reachable in
/// practice; saturating keeps this total without an unreachable branch.
fn index_at(position: usize) -> SiblingIndex {
    let one_based = u32::try_from(position)
        .unwrap_or(u32::MAX - 1)
        .saturating_add(1);
    SiblingIndex::new(one_based).unwrap_or(SiblingIndex::FIRST)
}

fn node_params(node: &Node) -> std::collections::BTreeMap<CompactString, CompactString> {
    match node {
        Node::Model(n) => n.params.clone(),
        Node::ListAggregator(n) | Node::DictAggregator(n) => n.params.clone(),
        // A condition node carries no params — see `node.rs`.
        Node::Condition(_) => std::collections::BTreeMap::new(),
    }
}

fn node_name_and_version(node: &Node) -> (Option<CompactString>, Option<CompactString>) {
    match node {
        Node::Model(n) => (n.name.clone(), n.version.clone()),
        Node::ListAggregator(n) | Node::DictAggregator(n) => (n.name.clone(), n.version.clone()),
        // A condition node carries neither — see `node.rs`.
        Node::Condition(_) => (None, None),
    }
}

fn aggregator_refs(agg: &crate::node::AggregatorNode) -> AggregatorRefs {
    AggregatorRefs {
        start: agg.start.clone(),
        component_ids: agg.components.iter().map(|c| c.node_id().clone()).collect(),
        terminal_children: terminal_children(&agg.components),
    }
}

/// The components whose *only* successor is `end`.
///
/// The original compares the raw list for equality — `childnode.children ==
/// [Constants.END]` — so a node with `[end, X]` does not
/// qualify, and a condition node never does (its `children` is a two-branch
/// map, which is never equal to a list).
fn terminal_children(components: &[Node]) -> Vec<NodeId> {
    components
        .iter()
        .filter(|child| {
            let successors = child.successors();
            successors.len() == 1 && successors.iter().all(|s| s.is_end())
        })
        .map(|child| child.node_id().clone())
        .collect()
}

fn validate_start_ids(
    pipeline: &Pipeline,
    steps: &BTreeMap<NodeId, Step>,
) -> Result<(), ResolveError> {
    for node_id in pipeline.start.node_ids() {
        if !steps.contains_key(node_id) {
            return Err(ResolveError::UnknownStartNode {
                node_id: node_id.clone(),
            });
        }
    }

    // Sorted so a definition with two bad aggregators always names the same
    // one. `steps` is a `BTreeMap` now, so this is belt-and-braces rather than
    // the load-bearing fix it was when the map was a `HashMap` and two
    // operators debugging the same pipeline saw different node names.
    // `filter_map` rather than `filter(is_aggregator)` and then asking again.
    // `Step::is_aggregator` *is* `aggregator_refs().is_some()` (`step.rs:277`),
    // so the second question always answered `Some` and the `else` branch was
    // dead — reachable only if those two methods ever disagreed, which they
    // cannot while one is defined in terms of the other. Taking the refs from
    // the filter itself removes the branch instead of leaving one nothing can
    // take.
    let mut aggregators: Vec<(&Step, &AggregatorRefs)> = steps
        .values()
        .filter_map(|step| step.aggregator_refs().map(|refs| (step, refs)))
        .collect();
    aggregators.sort_by(|(a, _), (b, _)| a.node_id().cmp(b.node_id()));

    for (step, refs) in aggregators {
        for node_id in refs.start.node_ids() {
            if !steps.contains_key(node_id) {
                return Err(ResolveError::UnknownStartNode {
                    node_id: node_id.clone(),
                });
            }
            // An aggregator fans out into its *own* components. Starting on
            // a node elsewhere in the pipeline resolves cleanly but leaves
            // the barrier waiting on children the fan-out never reaches.
            if !refs.component_ids.contains(node_id) {
                return Err(ResolveError::StartNodeOutsideAggregator {
                    aggregator: step.node_id().clone(),
                    node_id: node_id.clone(),
                });
            }
        }
    }
    Ok(())
}

/// Pass two, part one: gather every `(from, to)` edge, validating targets.
///
/// Collected first rather than applied in place because each edge mutates
/// *two* registry entries — the source's `next_nodes` and the target's
/// `num_prerequisites`.
fn collect_edges(
    node: &Node,
    steps: &BTreeMap<NodeId, Step>,
    edges: &mut Vec<(NodeId, NodeId)>,
) -> Result<(), ResolveError> {
    let parent = node.node_id();
    for successor in node.successors() {
        let Some(target) = successor.node_id() else {
            continue; // `end` terminates the branch.
        };
        if !steps.contains_key(target) {
            return Err(ResolveError::UnknownSuccessor {
                parent: parent.clone(),
                successor: target.clone(),
            });
        }
        edges.push((parent.clone(), target.clone()));
    }

    for child in node.components() {
        collect_edges(child, steps, edges)?;
    }
    Ok(())
}

fn apply_edges(edges: Vec<(NodeId, NodeId)>, steps: &mut BTreeMap<NodeId, Step>) {
    for (from, to) in edges {
        if let Some(step) = steps.get_mut(&from) {
            step.common_mut().next_nodes.push(to.clone());
        }
        if let Some(step) = steps.get_mut(&to) {
            step.common_mut().num_prerequisites += 1;
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Visit {
    InProgress,
    Done,
}

/// Iterative depth-first search over the successor graph.
///
/// Iterative rather than recursive because the successor graph's depth is
/// unbounded by anything structural — it is whatever edges the author wrote.
///
/// Note the scope of that guarantee: `register` and `collect_edges` still
/// recurse, over the *nesting* depth rather than the edge graph. That is
/// bounded in practice by `serde`'s own recursion limit, which rejects a
/// deeply nested document before this module sees it, so the walk cannot be
/// driven deeper than parsing already allows. Worth making iterative too if
/// that ever stops being true.
fn detect_cycles(steps: &BTreeMap<NodeId, Step>) -> Result<(), ResolveError> {
    // Successor edges, *plus* each aggregator's fan-out edge into its own
    // start components. Without the fan-out edge, a component whose
    // successor is its own enclosing aggregator (`Agg` contains `B`,
    // `B.children: [Agg]`) shows no cycle in `next_nodes` alone — yet at
    // runtime `Agg` fans out to `B`, `B` completes into `Agg`, and `Agg`
    // fans out again, forever.
    let adjacency: BTreeMap<&NodeId, Vec<&NodeId>> = steps
        .iter()
        .map(|(id, step)| {
            let mut out: Vec<&NodeId> = step.common().next_nodes.iter().collect();
            if let Some(refs) = step.aggregator_refs() {
                out.extend(refs.start.node_ids());
            }
            (id, out)
        })
        .collect();
    let successors_of = |id: &NodeId| -> Vec<NodeId> {
        adjacency
            .get(id)
            .map(|v| v.iter().map(|n| (*n).clone()).collect())
            .unwrap_or_default()
    };

    let mut state: BTreeMap<NodeId, Visit> = BTreeMap::new();

    // Sorted for deterministic error reporting — see `validate_start_ids`.
    let mut roots: Vec<&NodeId> = steps.keys().collect();
    roots.sort();

    for root in roots {
        if state.contains_key(root) {
            continue;
        }
        state.insert(root.clone(), Visit::InProgress);
        let mut stack: Vec<(NodeId, usize)> = vec![(root.clone(), 0)];

        while let Some((node, edge_index)) = stack.pop() {
            let successors = successors_of(&node);
            if edge_index >= successors.len() {
                state.insert(node, Visit::Done);
                continue;
            }
            let next = successors[edge_index].clone();
            stack.push((node, edge_index + 1));

            match state.get(&next) {
                Some(Visit::InProgress) => return Err(ResolveError::Cycle(next)),
                Some(Visit::Done) => {}
                None => {
                    state.insert(next.clone(), Visit::InProgress);
                    stack.push((next, 0));
                }
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    macro_rules! corpus {
        ($file:literal) => {
            include_str!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/tests/fixtures/pipelines/",
                $file
            ))
        };
    }

    fn parse(yaml: &str) -> Result<Pipeline, serde_yaml::Error> {
        serde_yaml::from_str(yaml)
    }

    fn resolved(yaml: &str) -> Result<StepRegistry, Box<dyn std::error::Error>> {
        Ok(resolve(&parse(yaml)?)?)
    }

    fn step<'a>(registry: &'a StepRegistry, id: &str) -> &'a Step {
        match registry.get(&NodeId::new(id)) {
            Some(step) => step,
            None => panic!("registry has no step {id:?}"),
        }
    }

    const ALL_CORPUS: &[(&str, &str)] = &[
        ("pipeline1", corpus!("pipeline1.yaml")),
        ("pipeline2", corpus!("pipeline2.yaml")),
        ("pipeline3", corpus!("pipeline3.yaml")),
        ("pipeline4", corpus!("pipeline4.yaml")),
        ("pipeline_case1", corpus!("pipeline_case1.yaml")),
        ("pipeline_case2", corpus!("pipeline_case2.yaml")),
        ("pipeline_case3", corpus!("pipeline_case3.yaml")),
        (
            "pipeline_condition_dict",
            corpus!("pipeline_condition_dict.yaml"),
        ),
        (
            "pipeline_condition_list",
            corpus!("pipeline_condition_list.yaml"),
        ),
        (
            "pipeline_nested_list_dict",
            corpus!("pipeline_nested_list_dict.yaml"),
        ),
        ("pipeline_params", corpus!("pipeline_params.yaml")),
    ];

    #[test]
    fn every_corpus_pipeline_resolves() {
        for (name, yaml) in ALL_CORPUS {
            match resolved(yaml) {
                Ok(registry) => assert!(!registry.is_empty(), "{name} resolved to nothing"),
                Err(err) => panic!("{name} failed to resolve: {err}"),
            }
        }
    }

    #[test]
    fn assigns_full_path_from_the_pipeline_id() -> Result<(), Box<dyn std::error::Error>> {
        let registry = resolved(corpus!("pipeline1.yaml"))?;
        assert_eq!(
            step(&registry, "A").common().full_path.as_deref(),
            Some("invoice.page.default.A")
        );
        Ok(())
    }

    #[test]
    fn nests_full_path_through_aggregators() -> Result<(), Box<dyn std::error::Error>> {
        let registry = resolved(corpus!("pipeline1.yaml"))?;
        // C is nested inside the DictAggregator X.
        assert_eq!(
            step(&registry, "C").common().full_path.as_deref(),
            Some("invoice.page.default.X.C")
        );
        assert_eq!(
            step(&registry, "C").common().parent_id,
            Some(NodeId::new("X"))
        );
        Ok(())
    }

    #[test]
    fn top_level_nodes_have_no_parent() -> Result<(), Box<dyn std::error::Error>> {
        let registry = resolved(corpus!("pipeline1.yaml"))?;
        assert_eq!(step(&registry, "A").common().parent_id, None);
        Ok(())
    }

    /// A `ListAggregator` numbers its children 1, 2, 3…; every other parent
    /// gives each child index 1.
    #[test]
    fn list_aggregator_children_are_numbered_in_order() -> Result<(), Box<dyn std::error::Error>> {
        let registry = resolved(corpus!("pipeline_condition_list.yaml"))?;
        // `List` holds A, R, Con1..Con4, B, E, F, G in declaration order.
        assert_eq!(
            step(&registry, "A").common().sibling_index,
            SiblingIndex::FIRST
        );
        assert_eq!(step(&registry, "R").common().sibling_index.get(), 2);
        assert_eq!(step(&registry, "Con1").common().sibling_index.get(), 3);
        Ok(())
    }

    #[test]
    fn dict_aggregator_children_all_take_index_one() -> Result<(), Box<dyn std::error::Error>> {
        let registry = resolved(corpus!("pipeline1.yaml"))?;
        assert_eq!(
            step(&registry, "C").common().sibling_index,
            SiblingIndex::FIRST
        );
        assert_eq!(
            step(&registry, "D").common().sibling_index,
            SiblingIndex::FIRST
        );
        Ok(())
    }

    #[test]
    fn records_terminal_children_and_expected_count() -> Result<(), Box<dyn std::error::Error>> {
        let registry = resolved(corpus!("pipeline1.yaml"))?;
        let Step::DictAggregator {
            refs,
            expected_children,
            ..
        } = step(&registry, "X")
        else {
            panic!("X must resolve to a DictAggregator");
        };
        // C and D both end with `children: [end]`.
        assert_eq!(
            refs.terminal_children,
            vec![NodeId::new("C"), NodeId::new("D")]
        );
        assert_eq!(*expected_children, Some(2));
        assert_eq!(refs.component_ids, vec![NodeId::new("C"), NodeId::new("D")]);
        Ok(())
    }

    #[test]
    fn links_successors_and_counts_prerequisites() -> Result<(), Box<dyn std::error::Error>> {
        let registry = resolved(corpus!("pipeline_params.yaml"))?;
        // A -> B, C ; B -> C, D ; C -> D ; D -> end
        assert_eq!(
            step(&registry, "A").common().next_nodes,
            vec![NodeId::new("B"), NodeId::new("C")]
        );
        assert_eq!(step(&registry, "A").common().num_prerequisites, 0);
        assert_eq!(step(&registry, "B").common().num_prerequisites, 1);
        // C is targeted by both A and B.
        assert_eq!(step(&registry, "C").common().num_prerequisites, 2);
        // D is targeted by both B and C.
        assert_eq!(step(&registry, "D").common().num_prerequisites, 2);
        assert!(step(&registry, "D").common().next_nodes.is_empty());
        Ok(())
    }

    #[test]
    fn a_condition_links_both_branches() -> Result<(), Box<dyn std::error::Error>> {
        let registry = resolved(corpus!("pipeline_condition_list.yaml"))?;
        assert_eq!(
            step(&registry, "Con1").common().next_nodes,
            vec![NodeId::new("Con2"), NodeId::new("E")]
        );
        Ok(())
    }

    #[test]
    fn resolves_four_levels_of_nesting() -> Result<(), Box<dyn std::error::Error>> {
        let registry = resolved(corpus!("pipeline_nested_list_dict.yaml"))?;
        assert_eq!(
            step(&registry, "R").common().full_path.as_deref(),
            Some("freeform.doc.commercial-invoice-list-dict.X.M.Q.R")
        );
        assert_eq!(
            step(&registry, "R").common().parent_id,
            Some(NodeId::new("Q"))
        );
        Ok(())
    }

    #[test]
    fn carries_params_onto_the_resolved_step() -> Result<(), Box<dyn std::error::Error>> {
        let registry = resolved(corpus!("pipeline_params.yaml"))?;
        assert_eq!(step(&registry, "A").common().params.len(), 2);
        assert_eq!(step(&registry, "B").common().params.len(), 0);
        Ok(())
    }

    #[test]
    fn rejects_a_duplicate_node_id() -> Result<(), serde_yaml::Error> {
        let yaml = "pipeline_id: p\nstart: A\ncomponents:\n  - node_id: A\n    type: Model\n    children: [end]\n  - node_id: A\n    type: Model\n    children: [end]\n";
        assert_eq!(
            resolve(&parse(yaml)?),
            Err(ResolveError::Duplicate(NodeId::new("A")))
        );
        Ok(())
    }

    /// A duplicate nested *inside* an aggregator is caught too — ids are
    /// unique across the whole pipeline, not merely among siblings.
    #[test]
    fn rejects_a_duplicate_across_nesting_levels() -> Result<(), serde_yaml::Error> {
        let yaml = "pipeline_id: p\nstart: A\ncomponents:\n  - node_id: A\n    type: Model\n    children: [end]\n  - node_id: Agg\n    type: ListAggregator\n    start: A\n    children: [end]\n    components:\n      - node_id: A\n        type: Model\n        children: [end]\n";
        assert_eq!(
            resolve(&parse(yaml)?),
            Err(ResolveError::Duplicate(NodeId::new("A")))
        );
        Ok(())
    }

    /// The fixed defect: the original logs a warning and continues,
    /// leaving the intended target's `num_prerequisites` short so a join
    /// downstream waits forever.
    #[test]
    fn rejects_an_unknown_successor() -> Result<(), serde_yaml::Error> {
        let yaml = "pipeline_id: p\nstart: A\ncomponents:\n  - node_id: A\n    type: Model\n    children: [Ghost]\n";
        assert_eq!(
            resolve(&parse(yaml)?),
            Err(ResolveError::UnknownSuccessor {
                parent: NodeId::new("A"),
                successor: NodeId::new("Ghost"),
            })
        );
        Ok(())
    }

    #[test]
    fn rejects_an_unknown_pipeline_start_node() -> Result<(), serde_yaml::Error> {
        let yaml = "pipeline_id: p\nstart: Ghost\ncomponents:\n  - node_id: A\n    type: Model\n    children: [end]\n";
        assert_eq!(
            resolve(&parse(yaml)?),
            Err(ResolveError::UnknownStartNode {
                node_id: NodeId::new("Ghost"),
            })
        );
        Ok(())
    }

    #[test]
    fn rejects_an_unknown_aggregator_start_node() -> Result<(), serde_yaml::Error> {
        let yaml = "pipeline_id: p\nstart: Agg\ncomponents:\n  - node_id: Agg\n    type: ListAggregator\n    start: Ghost\n    children: [end]\n    components:\n      - node_id: Inner\n        type: Model\n        children: [end]\n";
        assert_eq!(
            resolve(&parse(yaml)?),
            Err(ResolveError::UnknownStartNode {
                node_id: NodeId::new("Ghost"),
            })
        );
        Ok(())
    }

    /// New behaviour with no original precedent — the original accepts a
    /// cyclic definition and defers the hang to execution time.
    #[test]
    fn rejects_a_two_node_cycle() -> Result<(), serde_yaml::Error> {
        let yaml = "pipeline_id: p\nstart: A\ncomponents:\n  - node_id: A\n    type: Model\n    children: [B]\n  - node_id: B\n    type: Model\n    children: [A]\n";
        assert!(matches!(
            resolve(&parse(yaml)?),
            Err(ResolveError::Cycle(_))
        ));
        Ok(())
    }

    #[test]
    fn rejects_a_three_node_cycle() -> Result<(), serde_yaml::Error> {
        let yaml = "pipeline_id: p\nstart: A\ncomponents:\n  - node_id: A\n    type: Model\n    children: [B]\n  - node_id: B\n    type: Model\n    children: [C]\n  - node_id: C\n    type: Model\n    children: [A]\n";
        assert!(matches!(
            resolve(&parse(yaml)?),
            Err(ResolveError::Cycle(_))
        ));
        Ok(())
    }

    #[test]
    fn rejects_a_self_loop() -> Result<(), serde_yaml::Error> {
        let yaml = "pipeline_id: p\nstart: A\ncomponents:\n  - node_id: A\n    type: Model\n    children: [A]\n";
        assert_eq!(
            resolve(&parse(yaml)?),
            Err(ResolveError::Cycle(NodeId::new("A")))
        );
        Ok(())
    }

    /// A diamond revisits a node without cycling — the classic false
    /// positive for a naive "already seen" check.
    #[test]
    fn accepts_a_diamond() -> Result<(), Box<dyn std::error::Error>> {
        let yaml = "pipeline_id: p\nstart: A\ncomponents:\n  - node_id: A\n    type: Model\n    children: [B, C]\n  - node_id: B\n    type: Model\n    children: [D]\n  - node_id: C\n    type: Model\n    children: [D]\n  - node_id: D\n    type: Model\n    children: [end]\n";
        let registry = resolve(&parse(yaml)?)?;
        assert_eq!(step(&registry, "D").common().num_prerequisites, 2);
        Ok(())
    }

    /// A cycle among nodes unreachable from `start` is still rejected —
    /// the search covers every registry entry, not only reachable ones.
    #[test]
    fn rejects_a_cycle_in_a_disconnected_component() -> Result<(), serde_yaml::Error> {
        let yaml = "pipeline_id: p\nstart: A\ncomponents:\n  - node_id: A\n    type: Model\n    children: [end]\n  - node_id: X\n    type: Model\n    children: [Y]\n  - node_id: Y\n    type: Model\n    children: [X]\n";
        assert!(matches!(
            resolve(&parse(yaml)?),
            Err(ResolveError::Cycle(_))
        ));
        Ok(())
    }

    /// Exercises the `0 => None` arm: an aggregator whose components all
    /// continue onward, so none is terminal.
    #[test]
    fn a_dict_aggregator_with_no_terminal_children_expects_none(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let yaml = "pipeline_id: p\nstart: Agg\ncomponents:\n  - node_id: Agg\n    type: DictAggregator\n    start: Inner\n    children: [end]\n    components:\n      - node_id: Inner\n        type: Model\n        children: [Other]\n      - node_id: Other\n        type: Model\n        children: [Outside]\n  - node_id: Outside\n    type: Model\n    children: [end]\n";
        let registry = resolve(&parse(yaml)?)?;
        let Step::DictAggregator {
            refs,
            expected_children,
            ..
        } = step(&registry, "Agg")
        else {
            panic!("Agg must resolve to a DictAggregator");
        };
        assert!(refs.terminal_children.is_empty());
        assert_eq!(*expected_children, None);
        Ok(())
    }

    /// An aggregator fans out into its *own* components; starting elsewhere
    /// leaves the barrier waiting on children the fan-out never reaches.
    #[test]
    fn rejects_an_aggregator_starting_outside_its_components() -> Result<(), serde_yaml::Error> {
        let yaml = "pipeline_id: p\nstart: Outside\ncomponents:\n  - node_id: Outside\n    type: Model\n    children: [end]\n  - node_id: Agg\n    type: DictAggregator\n    start: Outside\n    children: [end]\n    components:\n      - node_id: Inner\n        type: Model\n        children: [end]\n";
        assert_eq!(
            resolve(&parse(yaml)?),
            Err(ResolveError::StartNodeOutsideAggregator {
                aggregator: NodeId::new("Agg"),
                node_id: NodeId::new("Outside"),
            })
        );
        Ok(())
    }

    /// A node called `end` can never be referenced — every `children: [end]`
    /// parses as the sentinel — yet it would still count toward an
    /// aggregator's `expected_children`, hanging the barrier.
    #[test]
    fn rejects_a_node_id_colliding_with_the_end_sentinel() -> Result<(), serde_yaml::Error> {
        let yaml = "pipeline_id: p\nstart: A\ncomponents:\n  - node_id: A\n    type: Model\n    children: [end]\n  - node_id: end\n    type: Model\n    children: [end]\n";
        assert_eq!(
            resolve(&parse(yaml)?),
            Err(ResolveError::ReservedNodeId(NodeId::new("end")))
        );
        Ok(())
    }

    /// A component whose successor is its own enclosing aggregator loops
    /// forever at runtime: the aggregator fans out to it, it completes back
    /// into the aggregator, which fans out again. Invisible in `next_nodes`
    /// alone, which is why the fan-out edge is part of the cycle graph.
    #[test]
    fn rejects_a_component_pointing_back_at_its_aggregator() -> Result<(), serde_yaml::Error> {
        let yaml = "pipeline_id: p\nstart: Agg\ncomponents:\n  - node_id: Agg\n    type: ListAggregator\n    start: B\n    children: [end]\n    components:\n      - node_id: B\n        type: Model\n        children: [Agg]\n";
        assert!(matches!(
            resolve(&parse(yaml)?),
            Err(ResolveError::Cycle(_))
        ));
        Ok(())
    }

    /// The dispatch subject must survive resolution — without it the
    /// controller cannot publish the step's work anywhere.
    #[test]
    fn carries_name_and_version_onto_the_resolved_step() -> Result<(), Box<dyn std::error::Error>> {
        let registry = resolved(corpus!("pipeline1.yaml"))?;
        assert_eq!(
            step(&registry, "A").common().name.as_deref(),
            Some("invoice.page.default.A")
        );
        assert_eq!(
            step(&registry, "A").common().version.as_deref(),
            Some("latest")
        );
        // A condition node has neither.
        let conditions = resolved(corpus!("pipeline_condition_list.yaml"))?;
        assert_eq!(step(&conditions, "Con1").common().name, None);
        assert_eq!(step(&conditions, "Con1").common().version, None);
        Ok(())
    }

    /// A node with `[end, X]` is *not* terminal — the original compares the
    /// successor list for equality with `[end]`, not membership.
    #[test]
    fn a_node_with_end_plus_another_successor_is_not_terminal(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let yaml = "pipeline_id: p\nstart: Agg\ncomponents:\n  - node_id: Agg\n    type: DictAggregator\n    start: Inner\n    children: [end]\n    components:\n      - node_id: Inner\n        type: Model\n        children: [end, Other]\n      - node_id: Other\n        type: Model\n        children: [end]\n";
        let registry = resolve(&parse(yaml)?)?;
        let Step::DictAggregator {
            refs,
            expected_children,
            ..
        } = step(&registry, "Agg")
        else {
            panic!("Agg must resolve to a DictAggregator");
        };
        assert_eq!(refs.terminal_children, vec![NodeId::new("Other")]);
        assert_eq!(*expected_children, Some(1));
        Ok(())
    }

    #[test]
    fn errors_display_usefully() {
        assert_eq!(
            ResolveError::Duplicate(NodeId::new("A")).to_string(),
            "duplicate node id A"
        );
        assert_eq!(
            ResolveError::UnknownSuccessor {
                parent: NodeId::new("A"),
                successor: NodeId::new("B"),
            }
            .to_string(),
            "node A declares unknown successor B"
        );
        assert_eq!(
            ResolveError::UnknownStartNode {
                node_id: NodeId::new("S"),
            }
            .to_string(),
            "start directive names unknown node S"
        );
        assert_eq!(
            ResolveError::Cycle(NodeId::new("A")).to_string(),
            "successor graph contains a cycle through A"
        );
    }

    #[test]
    fn index_at_is_one_based_and_saturates() -> Result<(), crate::sibling_index::SiblingIndexError>
    {
        assert_eq!(index_at(0), SiblingIndex::FIRST);
        assert_eq!(index_at(1), SiblingIndex::new(2)?);
        assert_eq!(index_at(usize::MAX), SiblingIndex::new(u32::MAX)?);
        Ok(())
    }

    proptest::proptest! {
        /// `resolve` must return, never panic, whatever graph it is handed —
        /// including cyclic and dangling-reference ones.
        #[test]
        fn resolve_never_panics(
            edges in proptest::collection::vec((0usize..6, 0usize..7), 0..14),
        ) {
            // Six nodes A..F; target 6 means `end`.
            let names = ["A", "B", "C", "D", "E", "F"];
            let mut successors: Vec<Vec<String>> = vec![Vec::new(); names.len()];
            for (from, to) in edges {
                let target = if to >= names.len() { "end".to_string() } else { names[to].to_string() };
                successors[from].push(target);
            }
            let mut yaml = String::from("pipeline_id: p\nstart: A\ncomponents:\n");
            for (i, name) in names.iter().enumerate() {
                yaml.push_str(&format!("  - node_id: {name}\n    type: Model\n    children: ["));
                yaml.push_str(&successors[i].join(", "));
                yaml.push_str("]\n");
            }
            if let Ok(pipeline) = serde_yaml::from_str::<Pipeline>(&yaml) {
                // Must return either way; the point is that it returns.
                let _ = resolve(&pipeline);
            }
        }

        /// An injected back-edge is always rejected. This is the safety net
        /// for cycle detection specifically, which has no original precedent
        /// to differentially validate against.
        #[test]
        fn an_injected_back_edge_is_always_rejected(chain_len in 2usize..6) {
            let names = ["A", "B", "C", "D", "E"];
            let chain = &names[..chain_len];
            let mut yaml = String::from("pipeline_id: p\nstart: A\ncomponents:\n");
            for (i, name) in chain.iter().enumerate() {
                // Last node closes the loop back to the first.
                let next = if i + 1 < chain.len() { chain[i + 1] } else { chain[0] };
                yaml.push_str(&format!("  - node_id: {name}\n    type: Model\n    children: [{next}]\n"));
            }
            let pipeline: Pipeline = match serde_yaml::from_str(&yaml) {
                Ok(p) => p,
                Err(e) => return Err(proptest::test_runner::TestCaseError::fail(e.to_string())),
            };
            proptest::prop_assert!(matches!(resolve(&pipeline), Err(ResolveError::Cycle(_))));
        }

        /// Every resolved registry is internally consistent: a step naming a
        /// parent is listed among that parent's `component_ids`. The flat
        /// redesign put this specifically at risk — a nested tree cannot
        /// have a dangling parent pointer by construction, a flat map can.
        #[test]
        fn parent_and_component_links_agree(depth in 1usize..4) {
            let mut yaml = String::from("pipeline_id: p\nstart: N0\ncomponents:\n");
            let mut indent = String::from("  ");
            for level in 0..depth {
                yaml.push_str(&format!("{indent}- node_id: N{level}\n{indent}  type: ListAggregator\n{indent}  start: N{}\n{indent}  children: [end]\n{indent}  components:\n", level + 1));
                indent.push_str("    ");
            }
            yaml.push_str(&format!("{indent}- node_id: N{depth}\n{indent}  type: Model\n{indent}  children: [end]\n"));

            let pipeline: Pipeline = match serde_yaml::from_str(&yaml) {
                Ok(p) => p,
                Err(e) => return Err(proptest::test_runner::TestCaseError::fail(e.to_string())),
            };
            let registry = match resolve(&pipeline) {
                Ok(r) => r,
                Err(e) => return Err(proptest::test_runner::TestCaseError::fail(e.to_string())),
            };

            for step in registry.iter() {
                if let Some(parent_id) = &step.common().parent_id {
                    let parent = match registry.get(parent_id) {
                        Some(p) => p,
                        None => return Err(proptest::test_runner::TestCaseError::fail(
                            format!("step {} names absent parent {parent_id}", step.node_id())
                        )),
                    };
                    let refs = match parent.aggregator_refs() {
                        Some(r) => r,
                        None => return Err(proptest::test_runner::TestCaseError::fail(
                            format!("parent {parent_id} is not an aggregator")
                        )),
                    };
                    proptest::prop_assert!(
                        refs.component_ids.contains(step.node_id()),
                        "parent {} does not list child {}", parent_id, step.node_id()
                    );
                }
            }
        }
    }
}
