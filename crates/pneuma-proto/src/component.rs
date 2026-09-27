//! The AI component contract: what pneuma sends a model, and what it reads back.
//!
//! This is differentiator #2 — a component participates over plain HTTP without
//! adopting any SDK — so the contract is the entire integration surface, and
//! getting it wrong is not recoverable by anything downstream.
//!
//! # The wrapper is gone
//!
//! A component used to be sent `{"jsonData": {...}}` and to answer
//! `{"jsonData": {"step_output": ...}}`. That shape was Seldon v1's, not this
//! system's: Seldon wrapped custom payloads in a `jsonData` field and the
//! original inherited it, along with a module named after the vendor.
//!
//! The wrapper carried no information. A request has exactly one body, so a key
//! whose only meaning is "here is the body" was a level of nesting every
//! producer and every consumer paid for and nothing read. It is dropped rather
//! than renamed: once compatibility with the originals was given up, keeping a
//! one-key envelope in order to spell it differently would have been the churn
//! without the simplification.
//!
//! **This breaks every deployed model**, which is the one change this port
//! cannot make on its own — the models are not in this repository. The
//! specification for whoever rebuilds them is `docs/as-built/component-api.md`.
//!
//! # What the originals actually parsed
//!
//! Worth keeping on the record, because it is why the response type stayed
//! permissive. the original executor declares response types
//! (`MessageSeldonOutputV2` / `JsonDataOutputV2`,
//! the original) and never uses
//! them; grep finds them referenced only by each other. The real parse was
//! untyped map access,
//! so the true contract was a `step_output` **key of any JSON type** and
//! nothing else. Modelling the declared type instead would have rejected
//! responses that work in production, which is the same mistake as encoding a
//! spec a server does not enforce. That permissiveness is kept.

use serde::{Deserialize, Serialize};
use serde_json::Value;

use compact_str::CompactString;

use pneuma_core::ids::{JobId, TenantId};

use crate::headers::TraceHeaders;
use crate::meta::Meta;

/// the original's `omitempty` for a map: nil *or* empty.
fn is_absent_or_empty(value: &Option<std::collections::BTreeMap<String, String>>) -> bool {
    value
        .as_ref()
        .is_none_or(std::collections::BTreeMap::is_empty)
}

/// What pneuma sends a component.
///
/// The fields directly, with no wrapper. It used to be
/// `{"jsonData": {...}}`, mirroring `MessageSeldonInputV2` / `JsonDataInputV2`
/// — a shape that
/// existed because Seldon v1 wrapped custom payloads in a `jsonData` field.
/// The wrapper named nothing about this system and carried no information: a
/// request has exactly one body, so a key saying "here is the body" is a level
/// of nesting every producer and every consumer paid for and nothing read.
///
/// Dropped rather than renamed. Once compatibility with the originals was given
/// up, keeping a one-key envelope only to spell it differently would have been
/// the churn without the simplification.
pub type ComponentRequest = ComponentRequestBody;

/// The five-key meta a component actually receives.
///
/// **Not** [`crate::meta::Meta`], and the difference is the point. `Meta` models
/// the *controller's* envelope: it has an `extra="allow"` catch-all and emits a
/// derived `pipeline_id`. the original's `MetaV2`
/// has exactly these five
/// keys and no catch-all, so unmarshalling drops anything else and
/// `GetPipelineID` is a method rather than a field. A component therefore never
/// sees `pipeline_id`, nor any caller extra.
///
/// Using `Meta` here would emit both, widening the one wire surface this module
/// exists to pin — a zero-SDK component that validates its input strictly, or
/// that echoes it, would see keys production never sends.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ComponentMeta {
    /// Wire key `job_id`. The same domain newtype [`Meta`] uses — narrowing the
    /// *key set* is the point of this type, not weakening the field types.
    pub job_id: JobId,
    /// Wire key `tenant_id`.
    pub tenant_id: TenantId,
    /// Wire key `pipeline_type`, where the original wrote `type`.
    pub pipeline_type: CompactString,
    /// Wire key `pipeline_level`, where the original wrote `level`.
    pub pipeline_level: CompactString,
    /// Wire key `pipeline_name`, where the original wrote `name`.
    pub pipeline_name: CompactString,
}

impl From<&Meta> for ComponentMeta {
    /// Narrows the controller's envelope to what a component is sent.
    ///
    /// Deliberately lossy: the caller extras and the derived `pipeline_id` are
    /// dropped, which is exactly what the original's unmarshal into `MetaV2` does.
    fn from(meta: &Meta) -> Self {
        ComponentMeta {
            job_id: meta.job_id.clone(),
            tenant_id: meta.tenant_id.clone(),
            pipeline_type: meta.pipeline_type.clone(),
            pipeline_level: meta.pipeline_level.clone(),
            pipeline_name: meta.pipeline_name.clone(),
        }
    }
}

/// What a component receives.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ComponentRequestBody {
    /// Routing and identity, narrowed to the five keys the original sends.
    pub meta: ComponentMeta,
    /// The component's input. `interface{}` on the original side, so any JSON value.
    pub step_input: Value,
    /// Caller-supplied passthrough. Omitted when absent, matching the original's
    /// `omitempty`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub custom_data: Option<Value>,
    /// Per-node environment overrides. `omitempty` on the original side.
    ///
    /// Skipped when empty as well as when absent. the original's `omitempty` on a
    /// `map[string]string` omits a nil map *and* an empty one, so an inbound
    /// `"node_env_vars": {}` must not be re-emitted — `Option::is_none` alone
    /// would send `{}` where the original sends nothing, and the whole point of this
    /// struct is that present-versus-absent is observable to a component.
    #[serde(default, skip_serializing_if = "is_absent_or_empty")]
    pub node_env_vars: Option<std::collections::BTreeMap<String, String>>,
    /// Trace context. `omitempty` on the original side.
    #[serde(default, skip_serializing_if = "TraceHeaders::is_empty")]
    pub headers: TraceHeaders,
}

/// Why a component response could not be used.
///
/// One variant, where there were two. The pair existed because the original's
/// NATS handler treated them differently — a missing `jsonData` dead-lettered
/// while a
/// missing `step_output` returned a bare error that, combined with the
/// unconditional delete in `handleMsg`, lost the message outright
/// (the defect notes). With the wrapper gone there is one way to fail
/// and one disposition for it.
///
/// **There is a third disposition, on the other consumer.** The Redis path
/// wraps both checks in bare `if ok` with no else branch:
/// neither failure
/// dead-letters or errors, and both fall through to publish a result carrying
/// `step_output: null`.
///
/// That `null` does **not** propagate. The controller validates the result with
/// `MessageResult`, whose `step_output` is `dict[str, Any] | list[Any]` with no
/// `None`, so it is rejected. The
/// consequence is a stalled node and a malformed-message error one service away
/// from the component that failed — not a wrong answer travelling onward.
/// `QUEUE_BACKEND` defaults to `"redis"`.
/// Recorded as
/// the defect notes, together with the larger problem on that path:
/// it never reads `result.Success` at all, so a component that is down or times
/// out takes the same route and no error event is emitted for it.
///
/// This type reports the failure either way; what a *caller* does with it is
/// where the three services differ.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ComponentResponseError {
    /// The response has no `step_output` key.
    ///
    /// A component that answered without answering. Kept as a named error
    /// rather than an `Option`, because both callers already have a path for
    /// it: the executor reports a result-less step, and the restate runner
    /// attributes the failure to the node.
    #[error("the component response has no `step_output`")]
    NoStepOutput,
}

/// Extracts a component's output.
///
/// Deliberately permissive about the *value*: `step_output` may be an object,
/// an array, a string, a number, a boolean, or `null`. the original binds it to
/// `interface{}` and a present-but-null key satisfies its `ok` check, so a
/// component returning `{"step_output": null}` is accepted today and must
/// remain accepted.
///
/// That leniency does not survive the whole pipeline. The value the original executor
/// forwards becomes `MessageResult.step_output`, which the controller types as
/// `dict[str, Any] | list[Any]` —
/// so a scalar or a `null` is accepted here, forwarded, and then rejected at
/// the controller. That mismatch is real and is recorded in
/// the protocol notes; it is not this function's job to pre-empt it, because
/// doing so would reject responses the original executor accepts.
pub fn extract_step_output(response: &Value) -> Result<&Value, ComponentResponseError> {
    response
        .get("step_output")
        .ok_or(ComponentResponseError::NoStepOutput)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn controller_meta(extra: serde_json::Value) -> Meta {
        let mut raw = serde_json::json!({
            "job_id": "job-1", "tenant_id": "acme",
            "pipeline_type": "llm", "pipeline_level": "doc", "pipeline_name": "default"
        });
        if let (Some(object), Some(more)) = (raw.as_object_mut(), extra.as_object()) {
            for (key, value) in more {
                object.insert(key.clone(), value.clone());
            }
        }
        match serde_json::from_value::<Meta>(raw) {
            Ok(meta) => meta,
            Err(err) => panic!("meta should decode: {err}"),
        }
    }

    fn meta() -> ComponentMeta {
        ComponentMeta::from(&controller_meta(serde_json::json!({})))
    }

    #[test]
    fn step_output_may_be_any_json_type() {
        // the original binds it to interface{} and only checks the key is present, so
        // every one of these is a valid component response today. Rejecting the
        // scalars -- which the declared-but-unused JsonDataOutputV2 would
        // suggest -- would break components that work.
        for value in [
            serde_json::json!({"pages": 3}),
            serde_json::json!([1, 2, 3]),
            serde_json::json!("a string"),
            serde_json::json!(42),
            serde_json::json!(1.5),
            serde_json::json!(true),
            serde_json::json!(null),
        ] {
            let response = serde_json::json!({"step_output": value});
            assert_eq!(extract_step_output(&response), Ok(&value));
        }
    }

    #[test]
    fn a_present_but_null_step_output_is_accepted_not_treated_as_missing() {
        // The distinction the original's `output, ok := jsonData["step_output"]` makes:
        // `ok` is about presence, not about the value. Reading this as "missing"
        // would turn a working component into a dead-lettered one.
        let present = serde_json::json!({"step_output": null});
        assert_eq!(extract_step_output(&present), Ok(&Value::Null));

        let absent = serde_json::json!({"meta": {}});
        assert_eq!(
            extract_step_output(&absent),
            Err(ComponentResponseError::NoStepOutput)
        );
    }

    #[test]
    fn there_is_one_way_to_fail_and_it_is_the_missing_output() {
        // There were two, because the original dead-lettered a missing
        // `jsonData` and merely errored on a missing `step_output` -- the
        // second being one of the paths that loses the message outright
        // (the defect notes). With the wrapper gone there is one
        // requirement, so one failure and one disposition.
        for empty in [
            serde_json::json!({}),
            serde_json::json!({"something_else": 1}),
        ] {
            assert_eq!(
                extract_step_output(&empty),
                Err(ComponentResponseError::NoStepOutput),
                "{empty}"
            );
        }
    }

    #[test]
    fn a_wrapped_response_is_no_longer_a_response() {
        // The old shape. A model still sending `{"jsonData": {...}}` is a model
        // that has not been rebuilt, and it fails here rather than delivering a
        // null output that stalls a node one service away
        // (the defect notes).
        let wrapped = serde_json::json!({"jsonData": {"step_output": {"pages": 3}}});
        assert_eq!(
            extract_step_output(&wrapped),
            Err(ComponentResponseError::NoStepOutput)
        );
    }

    #[test]
    fn dummy_mode_output_now_satisfies_the_contract() {
        // `sendDummy` returned {"step_output": "dummy"} with no wrapper,
        // which the old
        // extraction rejected. Dropping the wrapper makes that shape correct --
        // a small accidental improvement worth recording, since the reason
        // dummy mode was broken was the wrapper and not the payload.
        let dummy = serde_json::json!({"step_output": "dummy"});
        assert_eq!(extract_step_output(&dummy), Ok(&serde_json::json!("dummy")));
    }

    #[test]
    fn everything_else_in_the_response_is_ignored() {
        // Notably jsonData.meta, which the declared type has a field for and
        // nothing reads.
        let noisy = serde_json::json!({
            "meta": {"job_id": "other"},
            "step_output": {"ok": true},
            "unexpected": [1, 2],
            "modelName": "a-model",
            "unrelated": null
        });
        assert_eq!(
            extract_step_output(&noisy),
            Ok(&serde_json::json!({"ok": true}))
        );
    }

    #[test]
    fn the_request_meta_is_narrowed_to_the_five_keys_go_sends() {
        // The controller's Meta carries a catch-all and derives pipeline_id.
        // MetaV2 has neither, so a component sees exactly five keys. Emitting
        // more would widen the one surface this module exists to pin.
        let rich = controller_meta(serde_json::json!({
            "pipeline_id": "caller-supplied",
            "component_params": {"x": 1}
        }));
        let request = ComponentRequest {
            meta: ComponentMeta::from(&rich),
            step_input: serde_json::json!({}),
            custom_data: None,
            node_env_vars: None,
            headers: TraceHeaders::default(),
        };
        let Ok(json) = serde_json::to_value(&request) else {
            panic!("should encode");
        };
        let Some(sent) = json["meta"].as_object() else {
            panic!("meta should be an object");
        };
        let mut keys: Vec<&str> = sent.keys().map(String::as_str).collect();
        keys.sort_unstable();
        assert_eq!(
            keys,
            [
                "job_id",
                "pipeline_level",
                "pipeline_name",
                "pipeline_type",
                "tenant_id"
            ]
        );

        // Named explicitly, because these are the two that leaked before.
        assert!(
            sent.get("pipeline_id").is_none(),
            "derived id must not be sent"
        );
        assert!(
            sent.get("component_params").is_none(),
            "caller extras must not be sent"
        );
    }

    #[test]
    fn an_empty_node_env_vars_is_omitted_the_way_go_omits_it() {
        // the original's omitempty on map[string]string drops a nil map AND an empty one.
        // Skipping only `None` would re-emit `{}` where the original executor sends nothing.
        let request = ComponentRequest {
            meta: meta(),
            step_input: serde_json::json!({}),
            custom_data: None,
            node_env_vars: Some(std::collections::BTreeMap::new()),
            headers: TraceHeaders::default(),
        };
        let Ok(json) = serde_json::to_value(&request) else {
            panic!("should encode");
        };
        assert!(
            json.get("node_env_vars").is_none(),
            "an empty map must be omitted, not sent as {{}}"
        );

        // And decoding `{}` then re-encoding must not resurrect it.
        let raw = serde_json::json!({
            "meta": {"job_id": "j", "tenant_id": "t",
                     "pipeline_type": "a", "pipeline_level": "b",
                     "pipeline_name": "c"},
            "step_input": {},
            "node_env_vars": {}
        });
        let Ok(decoded) = serde_json::from_value::<ComponentRequest>(raw) else {
            panic!("should decode");
        };
        assert_eq!(
            decoded.node_env_vars,
            Some(std::collections::BTreeMap::new())
        );
        let Ok(back) = serde_json::to_value(&decoded) else {
            panic!("should encode");
        };
        assert!(back.get("node_env_vars").is_none());
    }

    #[test]
    fn a_request_serialises_with_the_gos_omitempty_behaviour() {
        let request = ComponentRequest {
            meta: meta(),
            step_input: serde_json::json!({"page": 1}),
            custom_data: None,
            node_env_vars: None,
            headers: TraceHeaders::default(),
        };
        let Ok(json) = serde_json::to_value(&request) else {
            panic!("should encode");
        };
        assert_eq!(json["step_input"], serde_json::json!({"page": 1}));
        // The three `omitempty` fields must be absent, not null: the original omits them,
        // and a component that distinguishes the two would see a difference.
        for omitted in ["custom_data", "node_env_vars", "headers"] {
            assert!(
                json.get(omitted).is_none(),
                "{omitted} should be omitted entirely"
            );
        }
        // And no wrapper of any spelling: the fields are the request.
        for gone in ["jsonData", "json_data"] {
            assert!(json.get(gone).is_none(), "{gone} should not exist");
        }
    }

    #[test]
    fn a_request_carries_the_optional_fields_when_they_are_set() {
        let mut env = std::collections::BTreeMap::new();
        env.insert("MODEL".to_owned(), "big".to_owned());
        let raw = serde_json::json!({
            "meta": {"job_id": "job-1", "tenant_id": "acme",
                     "pipeline_type": "llm", "pipeline_level": "doc",
                     "pipeline_name": "default"},
            "step_input": [1, 2],
            "custom_data": {"k": "v"},
            "node_env_vars": {"MODEL": "big"}
        });
        let Ok(request) = serde_json::from_value::<ComponentRequest>(raw) else {
            panic!("should decode");
        };
        assert_eq!(request.step_input, serde_json::json!([1, 2]));
        assert_eq!(request.custom_data, Some(serde_json::json!({"k": "v"})));
        assert_eq!(request.node_env_vars, Some(env));

        let Ok(back) = serde_json::to_value(&request) else {
            panic!("should encode");
        };
        assert_eq!(back["node_env_vars"]["MODEL"], "big");
    }

    #[test]
    fn the_error_names_the_missing_key() {
        assert_eq!(
            ComponentResponseError::NoStepOutput.to_string(),
            "the component response has no `step_output`"
        );
    }
}
