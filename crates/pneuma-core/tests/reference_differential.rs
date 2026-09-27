//! Differential test: the Rust resolver against the **original's own
//! output**.
//!
//! This is the one test in the crate whose reference values were not written
//! by hand — they were produced by running `RunResolver` from
//! the original (commit `0397327144dcd006cbd783de5f39b053a93f6341`) over the
//! same 11 corpus fixtures and serialising its `step_registry`. Everything
//! else here asserts what the port's author believed correct; this asserts
//! what the system being replaced actually does.
//!
//! # What is compared, and what is deliberately not
//!
//! Every scalar field both implementations agree on: `full_path`,
//! `parent_id`, `nth`/`sibling_index`, `num_prerequisites`, `next_nodes`,
//! `target_children`/`terminal_children`, and `num_children`/
//! `expected_children`.
//!
//! **Excluded: the nested `components` list.** The original stores the full
//! subtree on each aggregator, duplicating every descendant already present
//! in the flat registry; this port stores flat `component_ids` instead. That
//! is the design notes, and it is the single intended structural
//! divergence — so it is normalised away here rather than silently passing.
//!
//! # Regenerating
//!
//! ```text
//! cd the original && (the reference generator)
//! ```
//!
//! If this test starts failing after an update to the original, the question to
//! ask first is "did the original resolver change?", not "did our parsing
//! change" — hence the pinned commit above.

use std::collections::BTreeMap;

use pneuma_core::{
    condition::Condition,
    evaluator::evaluate,
    ids::NodeId,
    node::Pipeline,
    resolver::resolve,
    status::{Admission, NodeStatus},
    step::Step,
};
use serde::Deserialize;
use serde_json::json;

/// One step as the original resolver reported it.
#[derive(Debug, Deserialize, PartialEq)]
struct ReferenceStep {
    full_path: Option<String>,
    parent_id: Option<String>,
    nth: u32,
    num_prerequisites: u32,
    next_nodes: Vec<String>,
    #[serde(default)]
    target_children: Option<Vec<String>>,
    #[serde(default)]
    num_children: Option<serde_json::Value>,
}

macro_rules! case {
    ($name:literal) => {
        (
            $name,
            include_str!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/tests/fixtures/pipelines/",
                $name,
                ".yaml"
            )),
            include_str!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/tests/fixtures/reference_resolver_snapshots/",
                $name,
                ".json"
            )),
        )
    };
}

const CASES: &[(&str, &str, &str)] = &[
    case!("pipeline1"),
    case!("pipeline2"),
    case!("pipeline3"),
    case!("pipeline4"),
    case!("pipeline_case1"),
    case!("pipeline_case2"),
    case!("pipeline_case3"),
    case!("pipeline_condition_dict"),
    case!("pipeline_condition_list"),
    case!("pipeline_nested_list_dict"),
    case!("pipeline_params"),
];

#[test]
fn rust_resolver_matches_the_original() {
    let mut compared = 0usize;

    for (name, yaml, snapshot) in CASES {
        let pipeline: Pipeline = match serde_yaml::from_str(yaml) {
            Ok(p) => p,
            Err(err) => panic!("{name}: fixture did not parse: {err}"),
        };
        let registry = match resolve(&pipeline) {
            Ok(r) => r,
            Err(err) => panic!("{name}: Rust resolve failed: {err}"),
        };
        let expected: BTreeMap<String, ReferenceStep> = match serde_json::from_str(snapshot) {
            Ok(e) => e,
            Err(err) => panic!("{name}: snapshot did not parse: {err}"),
        };

        assert_eq!(
            registry.len(),
            expected.len(),
            "{name}: registry size differs from the original resolver's"
        );

        for (node_id, reference) in &expected {
            let step = match registry.get(&NodeId::new(node_id.as_str())) {
                Some(step) => step,
                None => panic!("{name}: Rust registry is missing {node_id}"),
            };
            let common = step.common();

            assert_eq!(
                common.full_path.as_deref(),
                reference.full_path.as_deref(),
                "{name}/{node_id}: full_path"
            );
            assert_eq!(
                common.parent_id.as_ref().map(NodeId::as_str),
                reference.parent_id.as_deref(),
                "{name}/{node_id}: parent_id"
            );
            assert_eq!(
                common.sibling_index.get(),
                reference.nth,
                "{name}/{node_id}: nth / sibling_index"
            );
            assert_eq!(
                common.num_prerequisites, reference.num_prerequisites,
                "{name}/{node_id}: num_prerequisites"
            );

            let next: Vec<&str> = common.next_nodes.iter().map(NodeId::as_str).collect();
            assert_eq!(next, reference.next_nodes, "{name}/{node_id}: next_nodes");

            if let Some(reference_terminal) = &reference.target_children {
                let refs = match step.aggregator_refs() {
                    Some(refs) => refs,
                    None => {
                        panic!("{name}/{node_id}: original reports an aggregator, Rust does not")
                    }
                };
                let terminal: Vec<&str> =
                    refs.terminal_children.iter().map(NodeId::as_str).collect();
                assert_eq!(
                    terminal, *reference_terminal,
                    "{name}/{node_id}: target_children / terminal_children"
                );
            }

            // Only a DictAggregator's count is statically resolvable; a
            // ListAggregator's depends on the runtime input length, and the
            // original leaves it an empty map until then.
            if let (
                Step::DictAggregator {
                    expected_children, ..
                },
                Some(reference_count),
            ) = (step, &reference.num_children)
            {
                if let Some(count) = reference_count.as_u64() {
                    assert_eq!(
                        expected_children.map(u64::from),
                        Some(count),
                        "{name}/{node_id}: num_children / expected_children"
                    );
                }
            }

            compared += 1;
        }
    }

    // Guards against the whole thing passing vacuously if the fixtures or
    // snapshots ever go missing.
    assert!(
        compared >= 60,
        "expected to compare a substantial number of steps, only did {compared}"
    );
}

// ---------------------------------------------------------------------------
// Evaluator and status guard
// ---------------------------------------------------------------------------
//
// These two modules deliberately diverge from the original
// entries 1 and 2. A naive differential test would fail on exactly the cells
// the port intends to change, which would be useless: it would either be
// disabled or its failures normalised into noise.
//
// So each comparison below carries an explicit **known-divergence** predicate.
// A cell must either match original, or be one the divergence table claims. A
// *new*, undocumented divergence fails the test; a documented one is pinned as
// a positive expectation, so silently "fixing" it back to the original's behaviour
// would also fail. That is the same discipline the resolver comparison uses
// when it normalises away `component_ids`.

const EVALUATOR_SNAPSHOT: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/fixtures/reference_evaluator_snapshot.json"
));
const STATUS_SNAPSHOT: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/fixtures/reference_status_snapshot.json"
));

/// One evaluator cell as original computed it: either a boolean, or the
/// exception type it raised.
#[derive(Debug, Deserialize)]
struct ReferenceCell {
    #[serde(default)]
    result: Option<bool>,
    #[serde(default)]
    raises: Option<String>,
}

fn key(k: &str) -> compact_str::CompactString {
    compact_str::CompactString::from(k)
}

/// The same grid the snapshot generator drove original over, in the same order.
/// Target values are wrapped as `{"k": target}` because `evaluate` looks the
/// operand up by key.
fn evaluator_grid() -> Vec<(&'static str, Condition, serde_json::Value)> {
    let contains = |v: &str| Condition::Contains {
        key: key("k"),
        value: key(v),
    };
    let equals = |v: &str| Condition::Equals {
        key: key("k"),
        value: key(v),
    };
    let is_empty = || Condition::IsEmpty { key: key("k") };
    let is_not_empty = || Condition::IsNotEmpty { key: key("k") };

    vec![
        ("contains/null", contains("x"), json!(null)),
        ("contains/bool", contains("x"), json!(true)),
        ("contains/number", contains("x"), json!(1)),
        ("contains/string_hit", contains("ell"), json!("hello")),
        ("contains/string_miss", contains("zzz"), json!("hello")),
        ("contains/array_hit", contains("a"), json!(["a", "b"])),
        ("contains/array_miss", contains("z"), json!(["a", "b"])),
        ("contains/array_number", contains("1"), json!([1, 2])),
        ("contains/object_hit", contains("a"), json!({"a": 1})),
        ("contains/object_miss", contains("z"), json!({"a": 1})),
        ("equals/null", equals("x"), json!(null)),
        ("equals/bool", equals("1"), json!(true)),
        ("equals/number_hit", equals("1"), json!(1)),
        ("equals/number_miss", equals("2"), json!(1)),
        ("equals/number_nonnumeric", equals("notanumber"), json!(1)),
        ("equals/string_hit", equals("hello"), json!("hello")),
        ("equals/string_miss", equals("other"), json!("hello")),
        ("equals/array", equals("x"), json!([1])),
        ("equals/object", equals("x"), json!({"a": 1})),
        ("is_empty/null", is_empty(), json!(null)),
        ("is_empty/bool", is_empty(), json!(true)),
        ("is_empty/number", is_empty(), json!(1)),
        ("is_empty/string_empty", is_empty(), json!("")),
        ("is_empty/string_full", is_empty(), json!("x")),
        ("is_empty/array_empty", is_empty(), json!([])),
        ("is_empty/array_full", is_empty(), json!([1])),
        ("is_empty/object_empty", is_empty(), json!({})),
        ("is_empty/object_full", is_empty(), json!({"a": 1})),
        ("is_not_empty/null", is_not_empty(), json!(null)),
        ("is_not_empty/bool", is_not_empty(), json!(true)),
        ("is_not_empty/number", is_not_empty(), json!(1)),
        ("is_not_empty/string_empty", is_not_empty(), json!("")),
        ("is_not_empty/string_full", is_not_empty(), json!("x")),
        ("is_not_empty/array_empty", is_not_empty(), json!([])),
        ("is_not_empty/array_full", is_not_empty(), json!([1])),
        ("is_not_empty/object_empty", is_not_empty(), json!({})),
        ("is_not_empty/object_full", is_not_empty(), json!({"a": 1})),
    ]
}

/// The design notes. the original's `case int() | float():` also matches
/// `bool`, because `bool` subclasses `int` — so `equals(True, "1")` is `True`
/// there. Verified by execution, not inference: the snapshot records
/// `equals/bool -> {"result": true}`. This port rejects it explicitly rather
/// than silently coercing.
fn is_known_evaluator_divergence(cell: &str) -> bool {
    cell == "equals/bool"
}

#[test]
fn rust_evaluator_matches_the_original() {
    let expected: BTreeMap<String, ReferenceCell> = match serde_json::from_str(EVALUATOR_SNAPSHOT) {
        Ok(e) => e,
        Err(err) => panic!("evaluator snapshot did not parse: {err}"),
    };

    let grid = evaluator_grid();
    assert_eq!(
        grid.len(),
        expected.len(),
        "the Rust grid and the original snapshot cover different numbers of cells"
    );

    let mut divergences_seen = 0usize;

    for (cell, condition, target) in grid {
        let reference = match expected.get(cell) {
            Some(p) => p,
            None => panic!("snapshot has no cell {cell}"),
        };
        let doc = json!({ "k": target });
        let rust = evaluate(std::slice::from_ref(&condition), &doc);

        if is_known_evaluator_divergence(cell) {
            // Pinned as a positive expectation: original accepts, we reject.
            assert_eq!(
                reference.result,
                Some(true),
                "{cell}: divergence table says original returns true; snapshot disagrees"
            );
            assert!(
                rust.is_err(),
                "{cell}: documented divergence says we reject, but we returned {rust:?}"
            );
            divergences_seen += 1;
            continue;
        }

        match (&reference.result, &reference.raises, rust) {
            (Some(expected_bool), None, Ok(actual)) => assert_eq!(
                actual, *expected_bool,
                "{cell}: Rust and original disagree on the result"
            ),
            (None, Some(_), Err(_)) => {}
            (py_result, py_raises, actual) => panic!(
                "{cell}: undocumented divergence — original {py_result:?}/{py_raises:?}, Rust {actual:?}"
            ),
        }
    }

    assert_eq!(
        divergences_seen, 1,
        "expected exactly the one documented evaluator divergence"
    );
}

fn status_name(status: NodeStatus) -> &'static str {
    match status {
        NodeStatus::Created => "CREATED",
        NodeStatus::Processing => "PROCESSING",
        NodeStatus::Finished => "FINISHED",
        NodeStatus::Error => "ERROR",
        NodeStatus::TimedOut => "TIMED_OUT",
        NodeStatus::Cancelled => "CANCELLED",
        NodeStatus::Forked => "FORKED",
        NodeStatus::Aggregated => "AGGREGATED",
        NodeStatus::HasChildError => "HAS_CHILD_ERROR",
        NodeStatus::HasChildTimedOut => "HAS_CHILD_TIMED_OUT",
    }
}

/// The design notes. `CANCELLED` is absent from the original's
/// `NODERUN_FINISHED_STATUSES`, so its own guard permits writing over a
/// cancelled noderun — verified by execution: the snapshot records
/// `CANCELLED/FINISHED -> true`. This port treats `Cancelled` as terminal, so
/// it rejects every transition out of it.
///
/// Note the two implementations still *agree* on `(CANCELLED, FORKED)`: the
/// original rejects that via its separate fork-from-non-created clause. Only
/// the pairs original actually admits are divergent.
fn is_known_status_divergence(current: NodeStatus, reference_admits: bool) -> bool {
    current == NodeStatus::Cancelled && reference_admits
}

#[test]
fn rust_status_guard_matches_the_original() {
    let expected: BTreeMap<String, bool> = match serde_json::from_str(STATUS_SNAPSHOT) {
        Ok(e) => e,
        Err(err) => panic!("status snapshot did not parse: {err}"),
    };
    assert_eq!(expected.len(), 100, "expected a full 10x10 admission grid");

    let mut compared = 0usize;
    let mut divergences_seen = 0usize;

    for current in NodeStatus::ALL {
        for proposed in NodeStatus::ALL {
            let cell = format!("{}/{}", status_name(current), status_name(proposed));
            let reference_admits = match expected.get(&cell) {
                Some(v) => *v,
                None => panic!("snapshot has no pair {cell}"),
            };
            let rust_admits = current.admit(proposed) == Admission::Accept;

            if is_known_status_divergence(current, reference_admits) {
                assert!(
                    !rust_admits,
                    "{cell}: documented divergence says we reject, but we admitted it"
                );
                divergences_seen += 1;
            } else {
                assert_eq!(
                    rust_admits, reference_admits,
                    "{cell}: undocumented divergence in the admission guard"
                );
            }
            compared += 1;
        }
    }

    assert_eq!(compared, 100);
    // Nine: every proposed status except FORKED, which the original rejects
    // anyway via its fork-from-non-created clause.
    assert_eq!(
        divergences_seen, 9,
        "expected exactly the documented CANCELLED divergences"
    );
}
