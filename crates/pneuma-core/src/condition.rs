//! Branch conditions evaluated by `Condition` nodes.
//!
//! The operand lives *inside* the variant that needs it. This is the whole
//! point of the type: the original carries an `Optional[str] value`
//! on every condition and raises `ValueError("value is required for EQUALS
//! and CONTAINS conditionals")` at runtime when it's missing.
//! Here that error is unrepresentable — `IsEmpty`
//! has no `value` field to omit, and `Equals` has no way to lack one.

use compact_str::CompactString;
use serde::{Deserialize, Serialize};

/// A single branch condition. The wire format is internally tagged on
/// `type` with snake_case variant names, matching the original
/// exactly (see `tests/assets/pipeline_condition_list.yaml`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Condition {
    /// The value at `key` contains `value` (substring, list membership, or
    /// object key membership, depending on the input's JSON kind).
    Contains {
        key: CompactString,
        value: CompactString,
    },
    /// The value at `key` equals `value`.
    Equals {
        key: CompactString,
        value: CompactString,
    },
    /// The value at `key` is empty (or null).
    IsEmpty { key: CompactString },
    /// The value at `key` is non-empty.
    IsNotEmpty { key: CompactString },
}

impl Condition {
    /// The input key this condition inspects.
    pub fn key(&self) -> &str {
        match self {
            Condition::Contains { key, .. }
            | Condition::Equals { key, .. }
            | Condition::IsEmpty { key }
            | Condition::IsNotEmpty { key } => key.as_str(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Fixtures taken verbatim from the `Con1..Con4` nodes of the real
    /// corpus file the original —
    /// not hand-invented approximations, so a wire-shape mismatch would
    /// actually be caught here.
    #[test]
    fn deserializes_equals_from_corpus_shape() -> Result<(), serde_yaml::Error> {
        let cond: Condition = serde_yaml::from_str("key: equals\ntype: equals\nvalue: value\n")?;
        assert_eq!(
            cond,
            Condition::Equals {
                key: "equals".into(),
                value: "value".into()
            }
        );
        assert_eq!(cond.key(), "equals");
        Ok(())
    }

    #[test]
    fn deserializes_contains_from_corpus_shape() -> Result<(), serde_yaml::Error> {
        let cond: Condition =
            serde_yaml::from_str("key: contains\ntype: contains\nvalue: value\n")?;
        assert_eq!(
            cond,
            Condition::Contains {
                key: "contains".into(),
                value: "value".into()
            }
        );
        assert_eq!(cond.key(), "contains");
        Ok(())
    }

    #[test]
    fn deserializes_is_empty_from_corpus_shape() -> Result<(), serde_yaml::Error> {
        let cond: Condition = serde_yaml::from_str("key: isempty\ntype: is_empty\n")?;
        assert_eq!(
            cond,
            Condition::IsEmpty {
                key: "isempty".into()
            }
        );
        assert_eq!(cond.key(), "isempty");
        Ok(())
    }

    #[test]
    fn deserializes_is_not_empty_from_corpus_shape() -> Result<(), serde_yaml::Error> {
        let cond: Condition = serde_yaml::from_str("key: isnotempty\ntype: is_not_empty\n")?;
        assert_eq!(
            cond,
            Condition::IsNotEmpty {
                key: "isnotempty".into()
            }
        );
        assert_eq!(cond.key(), "isnotempty");
        Ok(())
    }

    #[test]
    fn serializes_with_type_tag() -> Result<(), serde_json::Error> {
        let json = serde_json::to_string(&Condition::Equals {
            key: "k".into(),
            value: "v".into(),
        })?;
        assert_eq!(json, r#"{"type":"equals","key":"k","value":"v"}"#);

        let json = serde_json::to_string(&Condition::IsNotEmpty { key: "k".into() })?;
        assert_eq!(json, r#"{"type":"is_not_empty","key":"k"}"#);
        Ok(())
    }

    #[test]
    fn round_trips_every_variant() -> Result<(), serde_json::Error> {
        for cond in [
            Condition::Contains {
                key: "a".into(),
                value: "b".into(),
            },
            Condition::Equals {
                key: "a".into(),
                value: "b".into(),
            },
            Condition::IsEmpty { key: "a".into() },
            Condition::IsNotEmpty { key: "a".into() },
        ] {
            let json = serde_json::to_string(&cond)?;
            let back: Condition = serde_json::from_str(&json)?;
            assert_eq!(back, cond);
        }
        Ok(())
    }

    #[test]
    fn unknown_condition_type_is_rejected() {
        let err = serde_yaml::from_str::<Condition>("key: k\ntype: not_a_real_operator\n");
        assert!(err.is_err(), "unknown operator must not deserialize");
    }
}
