//! Pure evaluation of [`Condition`]s against a JSON input document.
//!
//! Ported from the original, with three deliberate changes:
//!
//! 1. The two `ValueError("value is required ...")` paths are gone — the
//!    operand is part of the [`Condition`] variant, so a missing operand is
//!    unrepresentable rather than caught.
//! 2. A missing input key is [`EvalError::MissingKey`] rather than a bare
//!    `KeyError` panic: the original indexes `input_data[cond.key]` directly,
//!    which raises; `serde_json::Value::get` returns an
//!    `Option`, so handling it explicitly is free.
//! 3. `Equals` rejects booleans — see the design notes. the original's
//!    `case int() | float():` also matches `bool` (a subtype of `int`), so
//!    `equals(True, "1")` returns `True` there, almost certainly by accident.
//!
//! Unsupported type/operator combinations are [`EvalError::UnsupportedType`]
//! rather than the original's `TypeError`.

use serde_json::Value;

use crate::condition::Condition;

/// Why evaluating a condition failed.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum EvalError {
    /// The input document has no such key.
    #[error("input has no key {key:?}")]
    MissingKey { key: String },
    /// The operator does not support the input value's JSON kind.
    #[error("operator {operator} does not support input of type {kind}")]
    UnsupportedType {
        operator: &'static str,
        kind: &'static str,
    },
    /// The condition's operand cannot be interpreted for this operator — a
    /// non-numeric operand compared against a numeric input, say.
    ///
    /// The original raises an uncaught `ValueError` here, from `float()`.
    /// Reporting it as a typed error keeps that
    /// semantics — the condition is malformed — while making it catchable
    /// instead of a crash. Returning `false` would be the larger change: it
    /// would silently route a malformed condition down its `on_false`
    /// branch, hiding an authoring error.
    #[error("operator {operator} cannot interpret operand {operand:?}")]
    MalformedOperand {
        operator: &'static str,
        operand: String,
    },
}

/// The JSON kind of a value, for error reporting.
fn kind_of(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "bool",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

/// Builds an [`EvalError::UnsupportedType`] for `operator` applied to
/// `value`'s JSON kind. Deliberately non-generic and returning the error
/// itself rather than a `Result<T, _>`: a generic helper is monomorphised
/// per call-site type, which fragments line-coverage attribution for no
/// benefit.
fn unsupported(operator: &'static str, value: &Value) -> EvalError {
    EvalError::UnsupportedType {
        operator,
        kind: kind_of(value),
    }
}

/// `target` contains `needle`: substring for strings, membership for arrays,
/// key-membership for objects.
fn contains(target: &Value, needle: &str) -> Result<bool, EvalError> {
    match target {
        Value::String(s) => Ok(s.contains(needle)),
        // original does `value in target_value` with a `str` operand,
        // so a non-string element never matches — `"1"`
        // does not find `1`. `Value`'s `PartialEq<&str>` has exactly that
        // semantics; rendering elements to strings first would silently
        // diverge.
        Value::Array(items) => Ok(items.iter().any(|item| item == needle)),
        Value::Object(map) => Ok(map.contains_key(needle)),
        other => Err(unsupported("contains", other)),
    }
}

/// `target` equals `operand`: string equality for strings, numeric equality
/// for numbers. Booleans are rejected — see the design notes.
fn equals(target: &Value, operand: &str) -> Result<bool, EvalError> {
    match target {
        Value::String(s) => Ok(s == operand),
        Value::Number(n) => match operand.parse::<f64>() {
            // `as_f64` is infallible for any number serde_json parses without
            // the `arbitrary_precision` feature; `NAN` is a total fallback
            // that compares unequal to everything, so an unrepresentable
            // number simply never matches — no unreachable branch to leave
            // permanently uncovered.
            Ok(rhs) => Ok(n.as_f64().unwrap_or(f64::NAN) == rhs),
            Err(_) => Err(EvalError::MalformedOperand {
                operator: "equals",
                operand: operand.to_owned(),
            }),
        },
        other => Err(unsupported("equals", other)),
    }
}

/// `target` is empty. Null counts as empty, matching the original.
fn is_empty(target: &Value) -> Result<bool, EvalError> {
    match target {
        Value::Null => Ok(true),
        Value::String(s) => Ok(s.is_empty()),
        Value::Array(items) => Ok(items.is_empty()),
        Value::Object(map) => Ok(map.is_empty()),
        other => Err(unsupported("is_empty", other)),
    }
}

/// Evaluates a single condition against the input document.
fn check(cond: &Condition, input: &Value) -> Result<bool, EvalError> {
    let target = input.get(cond.key()).ok_or_else(|| EvalError::MissingKey {
        key: cond.key().to_owned(),
    })?;

    match cond {
        Condition::Contains { value, .. } => contains(target, value),
        Condition::Equals { value, .. } => equals(target, value),
        Condition::IsEmpty { .. } => is_empty(target),
        Condition::IsNotEmpty { .. } => is_empty(target).map(|empty| !empty),
    }
}

/// Evaluates every condition against `input`, conjunctively.
///
/// Returns `Ok(true)` for an empty condition list, matching the original
/// original's `all(...)` over an empty generator.
pub fn evaluate(conds: &[Condition], input: &Value) -> Result<bool, EvalError> {
    for cond in conds {
        if !check(cond, input)? {
            return Ok(false);
        }
    }
    Ok(true)
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    /// What a given `(operator, JSON kind)` pair should produce.
    #[derive(Debug)]
    enum Expect {
        True,
        False,
        Unsupported,
        Malformed,
    }

    fn assert_cell(cond: Condition, input: Value, expect: Expect) {
        let doc = json!({ "k": input });
        let got = check(&cond, &doc);
        match (expect, got) {
            (Expect::True, Ok(true)) | (Expect::False, Ok(false)) => {}
            (Expect::Unsupported, Err(EvalError::UnsupportedType { .. }))
            | (Expect::Malformed, Err(EvalError::MalformedOperand { .. })) => {}
            (expected, actual) => {
                panic!("cond={cond:?} expected={expected:?} actual={actual:?}");
            }
        }
    }

    fn contains_k(value: &str) -> Condition {
        Condition::Contains {
            key: "k".into(),
            value: value.into(),
        }
    }
    fn equals_k(value: &str) -> Condition {
        Condition::Equals {
            key: "k".into(),
            value: value.into(),
        }
    }
    fn is_empty_k() -> Condition {
        Condition::IsEmpty { key: "k".into() }
    }
    fn is_not_empty_k() -> Condition {
        Condition::IsNotEmpty { key: "k".into() }
    }

    /// The exhaustive operator x JSON-kind table. Line coverage saturates
    /// long before these 24 cells are all checked (Rust match arms are
    /// single-expression), so this table — not the coverage number — is
    /// what actually proves the semantics.
    #[test]
    fn exhaustive_operator_by_json_kind_table() {
        // Contains
        assert_cell(contains_k("x"), json!(null), Expect::Unsupported);
        assert_cell(contains_k("x"), json!(true), Expect::Unsupported);
        assert_cell(contains_k("x"), json!(1), Expect::Unsupported);
        assert_cell(contains_k("ell"), json!("hello"), Expect::True);
        assert_cell(contains_k("zzz"), json!("hello"), Expect::False);
        assert_cell(contains_k("a"), json!(["a", "b"]), Expect::True);
        assert_cell(contains_k("z"), json!(["a", "b"]), Expect::False);
        assert_cell(contains_k("a"), json!({"a": 1}), Expect::True);
        assert_cell(contains_k("z"), json!({"a": 1}), Expect::False);

        // Equals
        assert_cell(equals_k("x"), json!(null), Expect::Unsupported);
        assert_cell(equals_k("true"), json!(true), Expect::Unsupported);
        assert_cell(equals_k("1"), json!(1), Expect::True);
        assert_cell(equals_k("2"), json!(1), Expect::False);
        // A non-numeric operand against a numeric input is a malformed
        // condition, not a false one — see `EvalError::MalformedOperand`.
        assert_cell(equals_k("notanumber"), json!(1), Expect::Malformed);
        assert_cell(equals_k("hello"), json!("hello"), Expect::True);
        assert_cell(equals_k("other"), json!("hello"), Expect::False);
        assert_cell(equals_k("x"), json!([1]), Expect::Unsupported);
        assert_cell(equals_k("x"), json!({"a": 1}), Expect::Unsupported);

        // IsEmpty
        assert_cell(is_empty_k(), json!(null), Expect::True);
        assert_cell(is_empty_k(), json!(true), Expect::Unsupported);
        assert_cell(is_empty_k(), json!(1), Expect::Unsupported);
        assert_cell(is_empty_k(), json!(""), Expect::True);
        assert_cell(is_empty_k(), json!("x"), Expect::False);
        assert_cell(is_empty_k(), json!([]), Expect::True);
        assert_cell(is_empty_k(), json!([1]), Expect::False);
        assert_cell(is_empty_k(), json!({}), Expect::True);
        assert_cell(is_empty_k(), json!({"a": 1}), Expect::False);

        // IsNotEmpty — the negation of IsEmpty, including its error cases
        assert_cell(is_not_empty_k(), json!(null), Expect::False);
        assert_cell(is_not_empty_k(), json!(true), Expect::Unsupported);
        assert_cell(is_not_empty_k(), json!(1), Expect::Unsupported);
        assert_cell(is_not_empty_k(), json!(""), Expect::False);
        assert_cell(is_not_empty_k(), json!("x"), Expect::True);
        assert_cell(is_not_empty_k(), json!([]), Expect::False);
        assert_cell(is_not_empty_k(), json!([1]), Expect::True);
        assert_cell(is_not_empty_k(), json!({}), Expect::False);
        assert_cell(is_not_empty_k(), json!({"a": 1}), Expect::True);
    }

    /// The design notes in executable form: the original's
    /// `case int() | float():` also matches `bool`, so `equals(True, "1")`
    /// is `True` there. Here it is an explicit error, never a coercion.
    #[test]
    fn equals_rejects_bool_rather_than_coercing_it() {
        let doc = json!({ "k": true });
        assert_eq!(
            check(&equals_k("1"), &doc),
            Err(EvalError::UnsupportedType {
                operator: "equals",
                kind: "bool"
            })
        );
    }

    /// Pins the original's semantics: the operand is a string, so a numeric array
    /// element never matches it (`"1" in [1, 2]` is `False` in the original).
    /// Rendering elements to strings first would make
    /// this `true` and silently diverge.
    #[test]
    fn contains_does_not_match_non_string_array_items() {
        let doc = json!({ "k": [1, 2] });
        assert_eq!(check(&contains_k("1"), &doc), Ok(false));
        assert_eq!(check(&contains_k("3"), &doc), Ok(false));

        let mixed = json!({ "k": ["1", 2] });
        assert_eq!(check(&contains_k("1"), &mixed), Ok(true));
    }

    #[test]
    fn missing_key_is_reported_by_name() {
        let doc = json!({ "other": 1 });
        assert_eq!(
            check(&is_empty_k(), &doc),
            Err(EvalError::MissingKey {
                key: "k".to_owned()
            })
        );
    }

    #[test]
    fn empty_condition_list_is_vacuously_true() -> Result<(), EvalError> {
        assert!(evaluate(&[], &json!({}))?);
        Ok(())
    }

    #[test]
    fn evaluate_is_conjunctive() -> Result<(), EvalError> {
        let doc = json!({ "k": "hello", "j": "" });
        let both_true = [contains_k("ell"), equals_k("hello")];
        assert!(evaluate(&both_true, &doc)?);

        let one_false = [contains_k("ell"), Condition::IsNotEmpty { key: "j".into() }];
        assert!(!evaluate(&one_false, &doc)?);
        Ok(())
    }

    #[test]
    fn evaluate_short_circuits_before_a_later_error() -> Result<(), EvalError> {
        // The first condition is false, so the second — which would raise
        // MissingKey — is never evaluated.
        let doc = json!({ "k": "hello" });
        let conds = [
            equals_k("not-hello"),
            Condition::IsEmpty {
                key: "absent".into(),
            },
        ];
        assert!(!evaluate(&conds, &doc)?);
        Ok(())
    }

    #[test]
    fn evaluate_propagates_errors() {
        let doc = json!({});
        let conds = [is_empty_k()];
        assert!(matches!(
            evaluate(&conds, &doc),
            Err(EvalError::MissingKey { .. })
        ));
    }

    #[test]
    fn errors_display_usefully() {
        assert_eq!(
            EvalError::MissingKey {
                key: "k".to_owned()
            }
            .to_string(),
            r#"input has no key "k""#
        );
        assert_eq!(
            EvalError::UnsupportedType {
                operator: "equals",
                kind: "bool"
            }
            .to_string(),
            "operator equals does not support input of type bool"
        );
        assert_eq!(
            EvalError::MalformedOperand {
                operator: "equals",
                operand: "abc".to_owned()
            }
            .to_string(),
            r#"operator equals cannot interpret operand "abc""#
        );
    }

    #[test]
    fn kind_of_names_every_json_variant() {
        assert_eq!(kind_of(&json!(null)), "null");
        assert_eq!(kind_of(&json!(true)), "bool");
        assert_eq!(kind_of(&json!(1)), "number");
        assert_eq!(kind_of(&json!("s")), "string");
        assert_eq!(kind_of(&json!([])), "array");
        assert_eq!(kind_of(&json!({})), "object");
    }

    proptest::proptest! {
        /// `evaluate` must never panic: any input, any conditions, always a
        /// well-formed Ok or Err. This is what would catch a stray `.unwrap()`
        /// reintroduced later that the fixed 24-cell table would not.
        #[test]
        fn never_panics(
            key in "[a-z]{0,3}",
            operand in ".*",
            which in 0u8..4,
            doc_key in "[a-z]{0,3}",
            doc_kind in 0u8..6,
        ) {
            let cond = match which {
                0 => Condition::Contains { key: key.as_str().into(), value: operand.as_str().into() },
                1 => Condition::Equals { key: key.as_str().into(), value: operand.as_str().into() },
                2 => Condition::IsEmpty { key: key.as_str().into() },
                _ => Condition::IsNotEmpty { key: key.as_str().into() },
            };
            let inner = match doc_kind {
                0 => json!(null),
                1 => json!(true),
                2 => json!(3),
                3 => json!("text"),
                4 => json!(["a", 1]),
                _ => json!({"a": 1}),
            };
            let doc = json!({ doc_key: inner });
            // Must return, not panic. Either outcome is acceptable.
            let _ = evaluate(std::slice::from_ref(&cond), &doc);
        }
    }
}
