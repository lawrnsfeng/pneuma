//! [`StepPayload`] — the value carried in `step_input` and `step_output`.
//!
//! # It is not "any JSON"
//!
//! original types both fields `dict[str, Any] | list[Any]`.
//! That union is enforced —
//! checked against the original model library the repo runs:
//!
//! ```text
//! {'a': 1}     -> OK
//! [1, 2]       -> OK
//! 'scalar'     -> REJECTED (ValidationError)
//! 7            -> REJECTED (ValidationError)
//! None         -> REJECTED (ValidationError)
//! True         -> REJECTED (ValidationError)
//! ```
//!
//! Both original services type the same fields `interface{}`,
//! which accepts
//! anything. So the two ends disagree about what is legal, and this type takes
//! the original's side — a message a Rust controller accepts is then one the original
//! controller would also accept, which is what a drop-in port needs.
//!
//! # The live failure this exposes
//!
//! The component contract requires only that a response carry `step_output`,
//! with no constraint on its type — the original's parse is untyped map access,
//! and the field is `interface{}`.
//! (It read that key out of a `jsonData` wrapper; `component.rs` says why the
//! wrapper is gone. The wrapper was never what made this happen.)
//!
//! So a component that returns `{"step_output": "hello"}` is valid by the
//! component contract, is forwarded intact by the original executor, and is then
//! **rejected** by the original controller when it validates `MessageResult`.
//! That is a real defect in the existing system, not an artefact of this port;
//! it is recorded in the protocol notes rather than papered over by accepting
//! scalars here, because accepting them would mean a Rust controller silently
//! processing work that the service it replaces would have failed.
//!
//! # Nesting is exempt — until the payload is fanned out
//!
//! The `dict | list` constraint applies to the payload itself, so a scalar
//! nested *inside* one is legal: `[1, "two", null]` decodes fine.
//!
//! That exemption stops holding the moment a `ListAggregator` consumes the
//! array. the original iterates the list and passes each element as
//! the child's **own top-level** `step_input`:
//!
//! So a component returning `step_output: [1, 2, 3]` — accepted here, and
//! accepted by the original's `MessageResult` — makes the controller raise a
//! `ValidationError` when it builds the child `MessageRun`, because `1` is not
//! a `dict | list`. Same last-hop failure as the scalar case above, one step
//! further along.
//!
//! This type cannot prevent that: the payload it is asked to validate is
//! genuinely legal, and only the graph decides whether the elements will later
//! be hoisted. Recorded in the protocol notesd so the limit of the
//! exemption is written down rather than inferred from the happy case.
//!
//! # Format requirement
//!
//! Decoding buffers into a [`Value`] to classify the shape, so this type needs
//! a self-describing format. That is JSON on every transport in this protocol,
//! and the same constraint applies to [`crate::backend::BackendCallback`],
//! which classifies on a key for the same reason.

use serde::{Deserialize, Deserializer, Serialize};
use serde_json::{Map, Value};

/// The payload of a step: a JSON object or a JSON array, never a scalar.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(untagged)]
pub enum StepPayload {
    /// A JSON object — the common case.
    Object(Map<String, Value>),
    /// A JSON array.
    ///
    /// This is what a `ListAggregator` **consumes**, not what it produces:
    /// The original rejects a non-list input outright, then
    /// hands each *element* to a child as that child's own
    /// top-level `step_input`. An earlier revision of this doc had the
    /// direction backwards.
    Array(Vec<Value>),
}

impl Default for StepPayload {
    /// An empty object, matching the original's `field(default_factory=dict)`.
    fn default() -> Self {
        StepPayload::Object(Map::new())
    }
}

impl StepPayload {
    /// Clones the payload into a plain JSON value.
    ///
    /// `to_` rather than `as_` deliberately: this deep-copies the whole
    /// payload, which on a per-message hot path is not what `as_` would lead a
    /// caller to expect. [`Self::as_object`] and [`Self::as_array`] borrow.
    pub fn to_value(&self) -> Value {
        match self {
            StepPayload::Object(map) => Value::Object(map.clone()),
            StepPayload::Array(items) => Value::Array(items.clone()),
        }
    }

    /// The object's entries, if this is an object.
    pub fn as_object(&self) -> Option<&Map<String, Value>> {
        match self {
            StepPayload::Object(map) => Some(map),
            StepPayload::Array(_) => None,
        }
    }

    /// The array's items, if this is an array.
    pub fn as_array(&self) -> Option<&[Value]> {
        match self {
            StepPayload::Array(items) => Some(items),
            StepPayload::Object(_) => None,
        }
    }

    /// Whether the payload carries nothing.
    pub fn is_empty(&self) -> bool {
        match self {
            StepPayload::Object(map) => map.is_empty(),
            StepPayload::Array(items) => items.is_empty(),
        }
    }

    /// How many entries or items the payload holds.
    ///
    /// For an array this is the fan-out width a `ListAggregator` would produce.
    pub fn len(&self) -> usize {
        match self {
            StepPayload::Object(map) => map.len(),
            StepPayload::Array(items) => items.len(),
        }
    }
}

impl TryFrom<Value> for StepPayload {
    type Error = PayloadError;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::Object(map) => Ok(StepPayload::Object(map)),
            Value::Array(items) => Ok(StepPayload::Array(items)),
            Value::Null => Err(PayloadError::NotObjectOrArray { found: "null" }),
            Value::Bool(_) => Err(PayloadError::NotObjectOrArray { found: "boolean" }),
            Value::Number(_) => Err(PayloadError::NotObjectOrArray { found: "number" }),
            Value::String(_) => Err(PayloadError::NotObjectOrArray { found: "string" }),
        }
    }
}

impl From<StepPayload> for Value {
    fn from(payload: StepPayload) -> Self {
        match payload {
            StepPayload::Object(map) => Value::Object(map),
            StepPayload::Array(items) => Value::Array(items),
        }
    }
}

/// The only way a payload can be invalid.
///
/// A distinct error rather than a bare serde message, so a caller can tell this
/// apart from a malformed message — the two want different handling, since this
/// one means a component returned a shape the protocol does not permit.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum PayloadError {
    #[error("step payload must be an object or an array, found {found}")]
    NotObjectOrArray {
        /// The JSON type that was found instead.
        found: &'static str,
    },
}

impl<'de> Deserialize<'de> for StepPayload {
    /// Buffers into a [`Value`] so the rejection carries which type was found,
    /// rather than serde's "did not match any variant" for an untagged enum.
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = Value::deserialize(deserializer)?;
        StepPayload::try_from(value).map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn object(json: &str) -> Result<StepPayload, serde_json::Error> {
        serde_json::from_str(json)
    }

    #[test]
    fn decodes_an_object() -> Result<(), serde_json::Error> {
        let payload = object(r#"{"doc":"invoice.pdf","page":3}"#)?;
        assert_eq!(payload.len(), 2);
        assert!(!payload.is_empty());
        assert!(payload.as_object().is_some_and(|m| m.contains_key("doc")));
        assert!(payload.as_array().is_none());
        Ok(())
    }

    #[test]
    fn decodes_an_array() -> Result<(), serde_json::Error> {
        // The shape a ListAggregator fans out over.
        let payload = object(r#"[{"page":1},{"page":2},{"page":3}]"#)?;
        assert_eq!(payload.len(), 3);
        assert!(payload.as_array().is_some_and(|items| items.len() == 3));
        assert!(payload.as_object().is_none());
        Ok(())
    }

    #[test]
    fn rejects_every_scalar_reference_rejects() {
        // Mirrors the checked the original model library behaviour recorded in the module docs.
        let expected = [
            ("\"scalar\"", "string"),
            ("7", "number"),
            ("null", "null"),
            ("true", "boolean"),
        ];
        for (json, found) in expected {
            let err = serde_json::from_str::<StepPayload>(json)
                .err()
                .map(|e| e.to_string())
                .unwrap_or_default();
            assert!(
                err.contains(found),
                "decoding {json} should name the type {found}, got {err:?}"
            );
        }
    }

    #[test]
    fn the_rejection_names_the_type_it_found() {
        // The reason Deserialize buffers into a Value rather than using an
        // untagged enum, whose error is "did not match any variant".
        assert_eq!(
            StepPayload::try_from(Value::String("x".into())),
            Err(PayloadError::NotObjectOrArray { found: "string" })
        );
        assert_eq!(
            PayloadError::NotObjectOrArray { found: "string" }.to_string(),
            "step payload must be an object or an array, found string"
        );
    }

    #[test]
    fn a_component_returning_a_scalar_step_output_is_rejected() {
        // The live failure recorded in the module docs: valid by the component
        // contract, forwarded by the original executor, rejected by the original
        // controller. Pinned so the port's behaviour here is a decision, not an
        // accident.
        let from_component = serde_json::json!({"step_output": "hello"});
        let step_output = from_component
            .pointer("/step_output")
            .cloned()
            .unwrap_or(Value::Null);
        assert!(StepPayload::try_from(step_output).is_err());
    }

    #[test]
    fn default_is_an_empty_object() -> Result<(), serde_json::Error> {
        // Matches the original's field(default_factory=dict).
        let payload = StepPayload::default();
        assert!(payload.is_empty());
        assert_eq!(payload.len(), 0);
        assert_eq!(serde_json::to_string(&payload)?, "{}");
        Ok(())
    }

    #[test]
    fn encodes_transparently() -> Result<(), serde_json::Error> {
        // The enum must not appear on the wire as a tagged variant.
        assert_eq!(serde_json::to_string(&object(r#"{"a":1}"#)?)?, r#"{"a":1}"#);
        assert_eq!(serde_json::to_string(&object("[1,2]")?)?, "[1,2]");
        Ok(())
    }

    #[test]
    fn round_trips_both_shapes() -> Result<(), serde_json::Error> {
        for json in [r#"{"a":1,"b":[2,3]}"#, r#"[{"x":1},"s",7,null]"#] {
            let payload = object(json)?;
            let text = serde_json::to_string(&payload)?;
            assert_eq!(serde_json::from_str::<StepPayload>(&text)?, payload);
        }
        Ok(())
    }

    #[test]
    fn scalars_are_legal_inside_a_payload_just_not_as_one() -> Result<(), serde_json::Error> {
        // The constraint is on the top level only; Any is still Any within.
        let payload = object(r#"[1,"two",null,true]"#)?;
        assert_eq!(payload.len(), 4);
        Ok(())
    }

    #[test]
    fn a_legal_array_can_still_fail_once_a_list_aggregator_hoists_it(
    ) -> Result<(), serde_json::Error> {
        // The limit of the nesting exemption. the original passes each
        // element as the CHILD's own top-level step_input, so an array this
        // type accepts can still produce a child payload it must reject.
        // Nothing here can prevent that -- only the graph knows whether the
        // elements get hoisted -- so it is pinned rather than guarded.
        let accepted_here = object("[1,2,3]")?;
        let elements = accepted_here.as_array().unwrap_or(&[]);
        assert_eq!(elements.len(), 3);

        for element in elements {
            assert!(
                StepPayload::try_from(element.clone()).is_err(),
                "each hoisted element becomes a child step_input and is rejected"
            );
        }

        // An array of objects is the shape that survives the hoist.
        let survives = object(r#"[{"page":1},{"page":2}]"#)?;
        for element in survives.as_array().unwrap_or(&[]) {
            assert!(StepPayload::try_from(element.clone()).is_ok());
        }
        Ok(())
    }

    #[test]
    fn converts_to_and_from_a_value() -> Result<(), serde_json::Error> {
        let payload = object(r#"{"a":1}"#)?;
        assert_eq!(payload.to_value(), serde_json::json!({"a":1}));
        assert_eq!(Value::from(payload.clone()), serde_json::json!({"a":1}));

        let array = object("[1,2]")?;
        assert_eq!(array.to_value(), serde_json::json!([1, 2]));
        assert_eq!(Value::from(array), serde_json::json!([1, 2]));

        assert_eq!(
            StepPayload::try_from(serde_json::json!({"a":1})),
            Ok(payload)
        );
        assert_eq!(
            StepPayload::try_from(serde_json::json!([1, 2])),
            Ok(object("[1,2]")?)
        );
        Ok(())
    }

    #[test]
    fn an_empty_array_is_empty_but_not_an_object() -> Result<(), serde_json::Error> {
        let payload = object("[]")?;
        assert!(payload.is_empty());
        assert!(payload.as_array().is_some_and(<[Value]>::is_empty));
        assert!(payload.as_object().is_none());
        Ok(())
    }

    #[test]
    fn debug_and_clone_are_available() -> Result<(), serde_json::Error> {
        let payload = object(r#"{"a":1}"#)?;
        assert_eq!(payload.clone(), payload);
        assert!(format!("{payload:?}").contains("Object"));
        Ok(())
    }
}
