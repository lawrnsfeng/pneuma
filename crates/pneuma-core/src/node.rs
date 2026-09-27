//! The pipeline definition as authored — the **wire format**, before
//! resolution.
//!
//! This is the shape stored in Mongo and written by hand in YAML, so its
//! serde representation must stay compatible with the original.
//! Where a Rust name reads better than
//! the wire name, the wire name is preserved with `#[serde(rename)]` rather
//! than changed:
//!
//! - `children` on the wire is [`ModelNode::successors`] here. "Children"
//!   reads as *contained*, which collides with the genuinely contained
//!   `components`; "successors" is the precise term for "next in traversal
//!   order", and keeping the two distinguishable matters in a type that has
//!   both.
//!
//! # Fidelity, precisely
//!
//! Round-tripping is **semantic**, not byte-for-byte, in two known ways:
//!
//! 1. `params` is a [`BTreeMap`], so its keys come back sorted rather than in
//!    authored order. Unlike [`crate::start_set::StartSet`] — whose order is
//!    load-bearing, because the engine takes `start_ids[0]` for a
//!    `ListAggregator` — `params` becomes a set of environment variables for
//!    a model container, where order carries no meaning. Sorted keys are
//!    accepted deliberately; `map_key_order_is_not_preserved` pins it so the
//!    behaviour is explicit rather than a surprise.
//! 2. The bare-string `start` form normalises to a map, matching the original
//!    original's own `EnsuredStrDict` coercion.
//!
//! Everything else survives a round trip, including fields these types do not
//! model: each struct carries a `#[serde(flatten)] extra` catch-all, so a
//! definition written by another service — or Mongo's own `_id` — is
//! preserved rather than silently dropped on write-back. This follows the
//! convention the platform's Rust gateway already established for
//! cross-service documents.
//!
//! A resolved [`crate::step::Step`] is the *other* half of this: `Node` is
//! what an author writes, `Step` is what the engine executes.

use std::collections::BTreeMap;

use compact_str::CompactString;
use serde::{Deserialize, Serialize};

use crate::{
    child_ref::ChildRef,
    condition::Condition,
    ids::{NodeId, PipelineId},
    start_set::StartSet,
};

/// A whole pipeline definition.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Pipeline {
    pub pipeline_id: PipelineId,
    /// The node(s) execution begins from.
    pub start: StartSet,
    /// The top-level nodes. Aggregators nest further nodes inside their own
    /// `components`.
    pub components: Vec<Node>,
    /// Fields this type does not model — notably Mongo's `_id`. Captured so
    /// a definition read from storage and written back is not silently
    /// stripped of them.
    #[serde(flatten, default, skip_serializing_if = "BTreeMap::is_empty")]
    pub extra: BTreeMap<String, serde_json::Value>,
}

/// One node in a pipeline definition.
///
/// Internally tagged on `type` with `PascalCase` variant names, exactly as
/// the corpus writes them (`type: Model`, `type: ListAggregator`, ...).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum Node {
    /// A step executed by an external model/component.
    Model(ModelNode),
    /// Fans out over a list input, then aggregates the results.
    ListAggregator(AggregatorNode),
    /// Fans out over the keys of a dict input, then aggregates.
    DictAggregator(AggregatorNode),
    /// Branches on a predicate over the step input.
    Condition(ConditionNode),
}

impl Node {
    /// This node's id, whatever its kind.
    pub fn node_id(&self) -> &NodeId {
        match self {
            Node::Model(n) => &n.node_id,
            Node::ListAggregator(n) | Node::DictAggregator(n) => &n.node_id,
            Node::Condition(n) => &n.node_id,
        }
    }

    /// The nodes nested inside this one. Empty for everything but
    /// aggregators.
    pub fn components(&self) -> &[Node] {
        match self {
            Node::ListAggregator(n) | Node::DictAggregator(n) => &n.components,
            Node::Model(_) | Node::Condition(_) => &[],
        }
    }

    /// Every node this one may hand control to, in declaration order.
    ///
    /// A [`ConditionNode`] yields its two branches as `[on_true, on_false]`,
    /// matching how the original flattens them.
    pub fn successors(&self) -> Vec<&ChildRef> {
        match self {
            Node::Model(n) => n.successors.iter().collect(),
            Node::ListAggregator(n) | Node::DictAggregator(n) => n.successors.iter().collect(),
            Node::Condition(n) => vec![&n.successors.on_true, &n.successors.on_false],
        }
    }

    /// The aggregator `start` directive, if this node is an aggregator.
    pub fn start(&self) -> Option<&StartSet> {
        match self {
            Node::ListAggregator(n) | Node::DictAggregator(n) => Some(&n.start),
            Node::Model(_) | Node::Condition(_) => None,
        }
    }

    /// This node's kind, discarding its payload.
    pub fn kind(&self) -> NodeKind {
        match self {
            Node::Model(_) => NodeKind::Model,
            Node::ListAggregator(_) => NodeKind::ListAggregator,
            Node::DictAggregator(_) => NodeKind::DictAggregator,
            Node::Condition(_) => NodeKind::Condition,
        }
    }
}

/// Which kind of node this is, without the node itself.
///
/// [`Node`] carries its kind as a serde tag, which is the right shape for a
/// pipeline definition but useless where only the discriminant travels — most
/// obviously the `type` field of a run-info message, which names a node's kind
/// without restating the node. The original has exactly this split
/// (`NodeType` the original, used by both the definition models and
/// `NodeRunInfo`), so this mirrors it rather than inventing a parallel notion.
///
/// The wire names are the `PascalCase` tags the corpus writes, identical to
/// [`Node`]'s, so the two can never drift apart silently — a test pins that.
/// # Two encodings, one type
///
/// The serde tags above are the corpus YAML's spelling. The `sqlx` attributes
/// are the Postgres enum the original migration tool created as `nodetype` and
/// `pneuma-store/migrations/0004_rename.sql` renames to `node_kind`.
/// They happen to be the same `PascalCase` labels, which is convenient and also
/// a trap: `rename_all` is *not* set, because sqlx would otherwise transform
/// them. The two paths are independent — a serde alias would not widen the
/// database contract, and vice versa — so both are pinned by tests.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize, sqlx::Type,
)]
#[sqlx(type_name = "node_kind")]
pub enum NodeKind {
    /// See [`Node::Model`].
    Model,
    /// See [`Node::ListAggregator`].
    ListAggregator,
    /// See [`Node::DictAggregator`].
    DictAggregator,
    /// See [`Node::Condition`].
    Condition,
}

impl NodeKind {
    /// Every kind, for exhaustive tests.
    pub const ALL: [NodeKind; 4] = [
        NodeKind::Model,
        NodeKind::ListAggregator,
        NodeKind::DictAggregator,
        NodeKind::Condition,
    ];

    /// Whether nodes of this kind fan out and then aggregate.
    pub fn is_aggregator(self) -> bool {
        matches!(self, NodeKind::ListAggregator | NodeKind::DictAggregator)
    }
}

/// A model-backed step.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ModelNode {
    pub node_id: NodeId,
    /// The component name — also the subject work is published to.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<CompactString>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<CompactString>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub params: BTreeMap<CompactString, CompactString>,
    /// `children` on the wire — see the module docs.
    #[serde(rename = "children", default)]
    pub successors: Vec<ChildRef>,
    /// Unmodelled wire fields, preserved across a round trip.
    #[serde(flatten, default, skip_serializing_if = "BTreeMap::is_empty")]
    pub extra: BTreeMap<String, serde_json::Value>,
}

/// A `ListAggregator` or `DictAggregator`. Both carry identical fields; the
/// difference is entirely in how the engine fans out over the input.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AggregatorNode {
    pub node_id: NodeId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<CompactString>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<CompactString>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub params: BTreeMap<CompactString, CompactString>,
    /// Which nested component(s) the fan-out begins at.
    pub start: StartSet,
    /// `children` on the wire — see the module docs.
    #[serde(rename = "children", default)]
    pub successors: Vec<ChildRef>,
    /// The nested sub-graph. Recursive: aggregators nest aggregators.
    pub components: Vec<Node>,
    /// Unmodelled wire fields, preserved across a round trip.
    #[serde(flatten, default, skip_serializing_if = "BTreeMap::is_empty")]
    pub extra: BTreeMap<String, serde_json::Value>,
}

/// A branch node. Note it carries no `name`, `version`, or `params` — the
/// corpus never gives it any, and the original's `ConditionalStep` is a
/// separate class from `BaseStep` for exactly that reason.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ConditionNode {
    pub node_id: NodeId,
    #[serde(default)]
    pub conditions: Vec<Condition>,
    /// `children` on the wire — a two-branch map, not a list.
    #[serde(rename = "children")]
    pub successors: ConditionalSuccessors,
    /// Unmodelled wire fields, preserved across a round trip. A condition
    /// node is not *given* `name`/`version`/`params` by any corpus fixture,
    /// but Mongo holds author-written definitions beyond that corpus, so
    /// anything present is carried rather than dropped.
    #[serde(flatten, default, skip_serializing_if = "BTreeMap::is_empty")]
    pub extra: BTreeMap<String, serde_json::Value>,
}

/// Where a [`ConditionNode`] goes on each outcome.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ConditionalSuccessors {
    pub on_true: ChildRef,
    pub on_false: ChildRef,
}

#[cfg(test)]
mod tests {
    /// Pins the Postgres enum contract without a live database, exactly as
    /// `status::tests::postgres_enum_contract` does for `NodeStatus`.
    mod postgres_enum_contract {
        use sqlx::{Encode, Type, TypeInfo};

        use super::*;

        #[test]
        fn type_name_matches_the_migrated_enum() {
            // the original migration tool created `name="nodetype"`; `0004_rename.sql` renames it to
            // `node_kind`, which is also what this Rust type has always been
            // called. The labels did not move with it.
            assert_eq!(
                <NodeKind as Type<sqlx::Postgres>>::type_info().name(),
                "node_kind"
            );
        }

        #[test]
        fn every_label_matches_the_original_enum() {
            // Exactly the labels from
            // the original, in order. Note they
            // are PascalCase, unlike nodestatus's SCREAMING_SNAKE_CASE -- the
            // two enums in the same table use different conventions, so neither
            // can be derived from the other.
            let expected = [
                (NodeKind::Model, "Model"),
                (NodeKind::ListAggregator, "ListAggregator"),
                (NodeKind::DictAggregator, "DictAggregator"),
                (NodeKind::Condition, "Condition"),
            ];

            for (kind, label) in expected {
                let mut buf = sqlx::postgres::PgArgumentBuffer::default();
                let encoded = <NodeKind as Encode<sqlx::Postgres>>::encode_by_ref(&kind, &mut buf);
                assert!(encoded.is_ok(), "encoding {kind:?} failed");
                assert_eq!(
                    &buf[..],
                    label.as_bytes(),
                    "{kind:?} must encode as {label}"
                );
            }
        }

        #[test]
        fn the_database_labels_match_the_serde_tags() {
            // They are the same strings today. If someone adds a serde rename,
            // this catches that the DB contract did not move with it -- the two
            // paths are independent and a divergence would be silent.
            for kind in [
                NodeKind::Model,
                NodeKind::ListAggregator,
                NodeKind::DictAggregator,
                NodeKind::Condition,
            ] {
                let Ok(serde_tag) = serde_json::to_string(&kind) else {
                    panic!("should encode");
                };
                let mut buf = sqlx::postgres::PgArgumentBuffer::default();
                let encoded = <NodeKind as Encode<sqlx::Postgres>>::encode_by_ref(&kind, &mut buf);
                assert!(encoded.is_ok());
                assert_eq!(
                    serde_tag.trim_matches('"').as_bytes(),
                    &buf[..],
                    "{kind:?}: serde and sqlx spellings have diverged"
                );
            }
        }
    }

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

    /// Every corpus file, so a parse regression on any of them is caught.
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

    fn parse(yaml: &str) -> Result<Pipeline, serde_yaml::Error> {
        serde_yaml::from_str(yaml)
    }

    /// Finds a node by id, panicking with a useful message if the fixture
    /// does not contain it. Written as a `match` rather than `.expect()`
    /// because the crate denies `expect_used` in test code too — the point
    /// of that rule is that a bare unwrap gives a useless failure message,
    /// and a named panic here gives a better one for free.
    fn find<'a>(nodes: &'a [Node], id: &str) -> &'a Node {
        match nodes.iter().find(|n| n.node_id() == &NodeId::new(id)) {
            Some(node) => node,
            None => panic!("fixture has no node {id:?}"),
        }
    }

    #[test]
    fn every_corpus_file_deserializes() {
        for (name, yaml) in ALL_CORPUS {
            match parse(yaml) {
                Ok(pipeline) => assert!(
                    !pipeline.components.is_empty(),
                    "{name} parsed to an empty pipeline"
                ),
                Err(err) => panic!("{name} failed to parse: {err}"),
            }
        }
    }

    #[test]
    fn parses_the_bare_string_start_form() -> Result<(), serde_yaml::Error> {
        let pipeline = parse(corpus!("pipeline1.yaml"))?;
        assert_eq!(
            pipeline.pipeline_id,
            PipelineId::new("invoice.page.default")
        );
        assert_eq!(pipeline.start.len(), 1);
        assert_eq!(pipeline.start.first_node_id(), &NodeId::new("A"));
        Ok(())
    }

    #[test]
    fn parses_the_multi_key_start_map_form() -> Result<(), serde_yaml::Error> {
        // `pipeline_params.yaml` starts from two nodes.
        let pipeline = parse(corpus!("pipeline_params.yaml"))?;
        assert_eq!(pipeline.start.len(), 2);
        assert_eq!(
            pipeline.start.node_ids().collect::<Vec<_>>(),
            vec![&NodeId::new("A"), &NodeId::new("D")]
        );
        Ok(())
    }

    #[test]
    fn parses_node_params() -> Result<(), serde_yaml::Error> {
        let pipeline = parse(corpus!("pipeline_params.yaml"))?;
        let Node::Model(a) = &pipeline.components[0] else {
            panic!("expected the first component to be a Model");
        };
        assert_eq!(a.node_id, NodeId::new("A"));
        assert_eq!(
            a.params.get("key_1").map(CompactString::as_str),
            Some("<value_key_1>")
        );
        assert_eq!(a.params.len(), 2);
        Ok(())
    }

    #[test]
    fn parses_successors_from_the_children_key() -> Result<(), serde_yaml::Error> {
        let pipeline = parse(corpus!("pipeline_params.yaml"))?;
        let Node::Model(a) = &pipeline.components[0] else {
            panic!("expected a Model");
        };
        assert_eq!(
            a.successors,
            vec![
                ChildRef::Node(NodeId::new("B")),
                ChildRef::Node(NodeId::new("C"))
            ]
        );
        Ok(())
    }

    #[test]
    fn parses_the_end_sentinel_as_a_variant() -> Result<(), serde_yaml::Error> {
        let pipeline = parse(corpus!("pipeline_params.yaml"))?;
        let Node::Model(d) = &pipeline.components[3] else {
            panic!("expected a Model");
        };
        assert_eq!(d.node_id, NodeId::new("D"));
        assert_eq!(d.successors, vec![ChildRef::End]);
        Ok(())
    }

    #[test]
    fn parses_a_dict_aggregator_with_its_start_map() -> Result<(), serde_yaml::Error> {
        let pipeline = parse(corpus!("pipeline1.yaml"))?;
        let x = find(&pipeline.components, "X");
        let Node::DictAggregator(agg) = x else {
            panic!("X must be a DictAggregator, got {x:?}");
        };
        assert_eq!(
            agg.start.node_ids().collect::<Vec<_>>(),
            vec![&NodeId::new("C"), &NodeId::new("D")]
        );
        assert_eq!(agg.components.len(), 2);
        assert_eq!(agg.successors, vec![ChildRef::End]);
        Ok(())
    }

    #[test]
    fn parses_all_four_condition_operators() -> Result<(), serde_yaml::Error> {
        let pipeline = parse(corpus!("pipeline_condition_list.yaml"))?;
        let Node::ListAggregator(list) = &pipeline.components[1] else {
            panic!("expected the second component to be a ListAggregator");
        };

        let conditions: Vec<&Condition> = list
            .components
            .iter()
            .filter_map(|n| match n {
                Node::Condition(c) => c.conditions.first(),
                _ => None,
            })
            .collect();
        assert_eq!(conditions.len(), 4);
        assert!(matches!(conditions[0], Condition::Equals { .. }));
        assert!(matches!(conditions[1], Condition::Contains { .. }));
        assert!(matches!(conditions[2], Condition::IsEmpty { .. }));
        assert!(matches!(conditions[3], Condition::IsNotEmpty { .. }));
        Ok(())
    }

    #[test]
    fn parses_conditional_branches() -> Result<(), serde_yaml::Error> {
        let pipeline = parse(corpus!("pipeline_condition_list.yaml"))?;
        let Node::ListAggregator(list) = &pipeline.components[1] else {
            panic!("expected a ListAggregator");
        };
        let Node::Condition(con1) = find(&list.components, "Con1") else {
            panic!("Con1 must be a Condition");
        };
        assert_eq!(con1.successors.on_true, ChildRef::Node(NodeId::new("Con2")));
        assert_eq!(con1.successors.on_false, ChildRef::Node(NodeId::new("E")));
        Ok(())
    }

    /// Four levels deep: `X` (dict) → `M` (list) → `Q` (list) → `R`.
    /// Recursion is the whole reason `components` is `Vec<Node>` rather than
    /// a flat list, so it is asserted structurally rather than by parse
    /// success alone.
    #[test]
    fn parses_four_levels_of_aggregator_nesting() -> Result<(), serde_yaml::Error> {
        let pipeline = parse(corpus!("pipeline_nested_list_dict.yaml"))?;

        let x = find(&pipeline.components, "X");
        assert!(matches!(x, Node::DictAggregator(_)));

        let m = find(x.components(), "M");
        assert!(matches!(m, Node::ListAggregator(_)));

        let q = find(m.components(), "Q");
        assert!(matches!(q, Node::ListAggregator(_)));

        let r = find(q.components(), "R");
        assert!(matches!(r, Node::Model(_)));
        assert!(r.components().is_empty());
        Ok(())
    }

    #[test]
    fn node_accessors_discriminate_by_kind() -> Result<(), serde_yaml::Error> {
        let pipeline = parse(corpus!("pipeline_nested_list_dict.yaml"))?;

        let model = &pipeline.components[0];
        assert_eq!(model.node_id(), &NodeId::new("A"));
        assert!(model.components().is_empty());
        assert!(model.start().is_none());
        assert_eq!(model.successors().len(), 1);

        let agg = find(&pipeline.components, "X");
        assert!(agg.start().is_some());
        assert_eq!(agg.components().len(), 4);
        // An aggregator's own successors, distinct from its nested
        // components — X continues to Y once it has aggregated.
        assert_eq!(agg.successors(), vec![&ChildRef::Node(NodeId::new("Y"))]);
        Ok(())
    }

    /// A `Condition` node's two branches are flattened into successor order,
    /// matching the original.
    #[test]
    fn condition_successors_are_on_true_then_on_false() -> Result<(), serde_yaml::Error> {
        let pipeline = parse(corpus!("pipeline_condition_list.yaml"))?;
        let Node::ListAggregator(list) = &pipeline.components[1] else {
            panic!("expected a ListAggregator");
        };
        let con1 = find(&list.components, "Con1");
        assert_eq!(
            con1.successors(),
            vec![
                &ChildRef::Node(NodeId::new("Con2")),
                &ChildRef::Node(NodeId::new("E"))
            ]
        );
        assert!(con1.start().is_none());
        Ok(())
    }

    #[test]
    fn round_trips_a_dict_aggregator_pipeline() -> Result<(), Box<dyn std::error::Error>> {
        let original = parse(corpus!("pipeline1.yaml"))?;
        let json = serde_json::to_string(&original)?;
        let back: Pipeline = serde_json::from_str(&json)?;
        assert_eq!(back, original);
        Ok(())
    }

    #[test]
    fn round_trips_a_list_aggregator_pipeline() -> Result<(), Box<dyn std::error::Error>> {
        let original = parse(corpus!("pipeline_condition_list.yaml"))?;
        let json = serde_json::to_string(&original)?;
        let back: Pipeline = serde_json::from_str(&json)?;
        assert_eq!(back, original);
        Ok(())
    }

    #[test]
    fn round_trips_the_deeply_nested_pipeline() -> Result<(), Box<dyn std::error::Error>> {
        let original = parse(corpus!("pipeline_nested_list_dict.yaml"))?;
        let json = serde_json::to_string(&original)?;
        let back: Pipeline = serde_json::from_str(&json)?;
        assert_eq!(back, original);
        Ok(())
    }

    #[test]
    fn serialises_with_the_type_tag_and_children_key() -> Result<(), serde_json::Error> {
        let node = Node::Model(ModelNode {
            node_id: NodeId::new("A"),
            name: Some(CompactString::from("comp.a")),
            version: None,
            params: BTreeMap::new(),
            successors: vec![ChildRef::Node(NodeId::new("B")), ChildRef::End],
            extra: BTreeMap::new(),
        });
        let json = serde_json::to_string(&node)?;
        assert_eq!(
            json,
            r#"{"type":"Model","node_id":"A","name":"comp.a","children":["B","end"]}"#
        );
        Ok(())
    }

    /// Mongo documents carry `_id`, and other services may add fields these
    /// types do not model. Dropping them on write-back would corrupt a
    /// stored definition, so they are captured and re-emitted.
    #[test]
    fn unmodelled_pipeline_fields_survive_a_round_trip() -> Result<(), serde_json::Error> {
        let json = r#"{"_id":"66f0","pipeline_id":"p","start":"A","components":[{"type":"Model","node_id":"A","children":["end"]}],"owner":"team-x"}"#;
        let pipeline: Pipeline = serde_json::from_str(json)?;
        assert_eq!(
            pipeline.extra.get("_id").and_then(|v| v.as_str()),
            Some("66f0")
        );
        assert_eq!(
            pipeline.extra.get("owner").and_then(|v| v.as_str()),
            Some("team-x")
        );

        let back = serde_json::to_string(&pipeline)?;
        assert!(back.contains("\"_id\":\"66f0\""), "_id was dropped: {back}");
        assert!(
            back.contains("\"owner\":\"team-x\""),
            "owner dropped: {back}"
        );
        Ok(())
    }

    /// A condition node is given no `name`/`version`/`params` by any corpus
    /// fixture, but Mongo holds definitions beyond that corpus.
    #[test]
    fn unmodelled_condition_node_fields_survive_a_round_trip(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let yaml = "node_id: C\ntype: Condition\nname: legacy.name\nversion: latest\nchildren:\n  on_true: X\n  on_false: Y\n";
        let node: Node = serde_yaml::from_str(yaml)?;
        let Node::Condition(c) = &node else {
            panic!("expected a Condition");
        };
        assert_eq!(
            c.extra.get("name").and_then(|v| v.as_str()),
            Some("legacy.name")
        );

        let back = serde_json::to_string(&node)?;
        assert!(
            back.contains("\"name\":\"legacy.name\""),
            "name dropped: {back}"
        );
        assert!(
            back.contains("\"version\":\"latest\""),
            "version dropped: {back}"
        );
        Ok(())
    }

    /// Pins the one deliberate fidelity gap: `params` keys come back sorted,
    /// not in authored order. Accepted because they become environment
    /// variables, where order carries no meaning — unlike `StartSet`, whose
    /// order drives `start_ids[0]`. Documented in the module header.
    #[test]
    fn map_key_order_is_not_preserved() -> Result<(), Box<dyn std::error::Error>> {
        let yaml = "node_id: A\ntype: Model\nparams:\n  z: '1'\n  a: '2'\n";
        let node: Node = serde_yaml::from_str(yaml)?;
        let json = serde_json::to_string(&node)?;
        assert!(
            json.contains(r#""params":{"a":"2","z":"1"}"#),
            "expected sorted params, got {json}"
        );
        Ok(())
    }

    /// `params` is `dict[str, str]` in the original,
    /// not an untyped dict, so a non-string value is rejected there too.
    /// This is parity, not a tightening.
    #[test]
    fn non_string_param_values_are_rejected() {
        let yaml = "node_id: A\ntype: Model\nparams:\n  retries: 3\n";
        assert!(
            serde_yaml::from_str::<Node>(yaml).is_err(),
            "a non-string param value must be rejected, as in the original"
        );
    }

    #[test]
    fn an_unknown_node_type_is_rejected() {
        let yaml = "node_id: A\ntype: Telepathy\nchildren: []\n";
        assert!(
            serde_yaml::from_str::<Node>(yaml).is_err(),
            "an unknown node type must not deserialize"
        );
    }

    #[test]
    fn a_model_node_without_successors_defaults_to_empty() -> Result<(), serde_yaml::Error> {
        let node: Node = serde_yaml::from_str("node_id: A\ntype: Model\n")?;
        assert_eq!(node.successors().len(), 0);
        Ok(())
    }

    #[test]
    fn node_kind_wire_names_match_the_node_tags_exactly() -> Result<(), serde_json::Error> {
        // NodeKind duplicates Node's discriminant, so the two could drift and
        // start naming the same kind differently on the wire. Deriving one
        // side's names from real Node values and comparing to the other's makes
        // that impossible to do silently.
        let nodes: [(Node, NodeKind); 4] = [
            (
                serde_json::from_str(r#"{"node_id":"A","type":"Model"}"#)?,
                NodeKind::Model,
            ),
            (
                serde_json::from_str(
                    r#"{"node_id":"A","type":"ListAggregator","components":[],"start":{"A":"A"}}"#,
                )?,
                NodeKind::ListAggregator,
            ),
            (
                serde_json::from_str(
                    r#"{"node_id":"A","type":"DictAggregator","components":[],"start":{"A":"A"}}"#,
                )?,
                NodeKind::DictAggregator,
            ),
            (
                serde_json::from_str(
                    r#"{"node_id":"A","type":"Condition","conditions":[],"children":{"on_true":"end","on_false":"end"}}"#,
                )?,
                NodeKind::Condition,
            ),
        ];
        assert_eq!(nodes.len(), NodeKind::ALL.len());

        for (node, expected_kind) in nodes {
            assert_eq!(node.kind(), expected_kind);
            // The tag Node serializes under must equal the name NodeKind uses.
            let node_json = serde_json::to_value(&node)?;
            let kind_json = serde_json::to_value(expected_kind)?;
            assert_eq!(
                node_json["type"], kind_json,
                "tag drift for {expected_kind:?}"
            );
        }
        Ok(())
    }

    #[test]
    fn node_kind_round_trips_and_classifies_aggregators() -> Result<(), serde_json::Error> {
        let expected = [
            (NodeKind::Model, "\"Model\"", false),
            (NodeKind::ListAggregator, "\"ListAggregator\"", true),
            (NodeKind::DictAggregator, "\"DictAggregator\"", true),
            (NodeKind::Condition, "\"Condition\"", false),
        ];
        assert_eq!(expected.len(), NodeKind::ALL.len());

        for (kind, wire, is_aggregator) in expected {
            assert_eq!(serde_json::to_string(&kind)?, wire);
            let back: NodeKind = serde_json::from_str(wire)?;
            assert_eq!(back, kind);
            assert_eq!(kind.is_aggregator(), is_aggregator);
        }
        Ok(())
    }

    #[test]
    fn an_unknown_node_kind_is_rejected() {
        assert!(serde_json::from_str::<NodeKind>("\"Telepathy\"").is_err());
    }
}
