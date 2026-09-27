//! The dispatch pair: [`MessageRun`] and [`MessageResult`].
//!
//! `MessageRun` is what the controller publishes to a component's subject
//! (the original, constructed and sent at
//! the original). `MessageResult` is what the original executor
//! publishes back to `pneuma.result` (the original, consumed at
//! the original).
//!
//! Both subclass `Message` in original, so both carry its fields — including its
//! `extra="allow"` catch-all — plus a required `node`.
//!
//! # The dispatch subject is a field of the message
//!
//! the original sends with `topic=step.name`, and `node.name` is
//! normally that value — which is why
//! [`NodeRunInfo::name`](crate::node::NodeRunInfo::name) is required rather
//! than optional: a run-info that lost it could not say where to dispatch.
//!
//! The equivalence is narrower than it looks, though. `construct_noderun`
//! substitutes the literal `"condition"` for a `Condition` node's name, so
//! `node.name` is a subject for every kind *except* that one. See
//! [`MessageRun::subject`], which returns `None` there rather than handing back
//! a placeholder nothing is listening on.
//!
//! # The reply topics are never set on this leg
//!
//! `MessageRun` inherits `reply_to_result` / `reply_to_error` / `reply_to_event` from
//! `Message`, and the original passes none of them:
//!
//! They default to `None` and travel as `null`. The reply addresses live on the
//! original inbound `Message` and thereafter in the persisted run record. So
//! the fact that no original struct models them (the protocol notes) costs
//! nothing on this path — there is nothing to drop.
//!
//! # What the original structs do and do not carry
//!
//! | Field | broker `Message` | the original executor `MessageInputV2` | the original executor `MessageOutputV2` |
//! |---|---|---|---|
//! | `node_env_vars` | yes | yes | **no** |
//! | `step_output` | no | no | yes |
//! | the three topics | no | no | no |
//!
//! `node_env_vars` reaches the component and is not echoed back, which is
//! correct — it is configuration for the call, not part of its result.

use std::collections::BTreeMap;

use compact_str::CompactString;
use serde::ser::SerializeMap;
use serde::{Deserialize, Serialize, Serializer};
use serde_json::Value;

use crate::envelope::CustomData;
use crate::headers::TraceHeaders;
use crate::meta::Meta;
use crate::payload::StepPayload;
use pneuma_core::node::NodeKind;

use crate::node::NodeRunInfo;

/// Environment variables handed to the component for this call.
///
/// Populated from `step.params`, and a `dict[str, str]` in
/// original — values are strings, not arbitrary JSON.
pub type NodeEnvVars = BTreeMap<CompactString, CompactString>;

/// Wire keys [`MessageRun`] emits from its own named fields.
///
/// Same guard as [`crate::meta::Meta`] and [`crate::envelope::Message`]: the
/// catch-all is public and must never emit a key a named field already owns,
/// because the duplicate is last-wins on the original side.
const RUN_RESERVED_WIRE_KEYS: [&str; 9] = [
    "meta",
    "step_input",
    "custom_data",
    "reply_to_result",
    "reply_to_error",
    "reply_to_event",
    "headers",
    "node",
    "node_env_vars",
];

/// Wire keys [`MessageResult`] emits from its own named fields.
const RESULT_RESERVED_WIRE_KEYS: [&str; 9] = [
    "meta",
    "step_input",
    "custom_data",
    "reply_to_result",
    "reply_to_error",
    "reply_to_event",
    "headers",
    "node",
    "step_output",
];

/// Work dispatched to a component.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct MessageRun {
    pub meta: Meta,
    /// Which node of which run this is for.
    pub node: NodeRunInfo,
    #[serde(default)]
    pub step_input: StepPayload,
    #[serde(default)]
    pub custom_data: CustomData,
    /// Component configuration, from `step.params`.
    #[serde(default)]
    pub node_env_vars: NodeEnvVars,
    /// Always `None` on this leg — see the module docs.
    #[serde(default)]
    pub reply_to_result: Option<CompactString>,
    /// Always `None` on this leg.
    #[serde(default)]
    pub reply_to_error: Option<CompactString>,
    /// Always `None` on this leg.
    #[serde(default)]
    pub reply_to_event: Option<CompactString>,
    #[serde(default)]
    pub headers: TraceHeaders,
    /// Unmodelled keys, preserved (`extra="allow"`).
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}

impl MessageRun {
    /// The subject this message should be published to, if it has one.
    ///
    /// the original sends with `topic=step.name`, and `node.name` is
    /// normally that same value — but **not always**, which is why this returns
    /// an [`Option`] rather than the field.
    ///
    /// `construct_noderun` writes:
    ///
    /// So a `Condition` node's run-info carries the literal string
    /// `"condition"`, which is a placeholder, not a subject. Nothing is
    /// listening on it.
    ///
    /// Today that cannot reach a `MessageRun`: the construction site is on the
    /// `ModelStep()` arm alone, reached from `init_next_step`'s
    /// match, and conditions produce a
    /// [`MessageResult`] instead. But that is a property of one call
    /// site, not of this type, and a dispatcher written against "the name is
    /// the subject" would publish to `"condition"` and drop the work silently.
    /// Returning `None` makes that unrepresentable rather than merely unlikely.
    pub fn subject(&self) -> Option<&str> {
        match self.node.node_kind {
            NodeKind::Condition => None,
            NodeKind::Model | NodeKind::ListAggregator | NodeKind::DictAggregator => {
                Some(&self.node.name)
            }
        }
    }
}

impl Serialize for MessageRun {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(None)?;
        // Base fields first, then subclass fields: the original model library orders a subclass
        // dump that way, and this must match byte-for-byte. Verified against
        // the original model library 2.13.4, which yields meta, step_input, custom_data,
        // reply_to_result, reply_to_error, reply_to_event, node, node_env_vars.
        map.serialize_entry("meta", &self.meta)?;
        map.serialize_entry("step_input", &self.step_input)?;
        map.serialize_entry("custom_data", &self.custom_data)?;
        map.serialize_entry("reply_to_result", &self.reply_to_result)?;
        map.serialize_entry("reply_to_error", &self.reply_to_error)?;
        map.serialize_entry("reply_to_event", &self.reply_to_event)?;
        map.serialize_entry("node", &self.node)?;
        map.serialize_entry("node_env_vars", &self.node_env_vars)?;
        for (key, value) in &self.extra {
            if !RUN_RESERVED_WIRE_KEYS.contains(&key.as_str()) {
                map.serialize_entry(key, value)?;
            }
        }
        // Last, because `inject_tracing_context` appends it to the already
        // dumped dict rather than it being a declared field.
        if !self.headers.is_empty() {
            map.serialize_entry("headers", &self.headers)?;
        }
        map.end()
    }
}

/// A component's result, returning to the controller.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct MessageResult {
    pub meta: Meta,
    /// Which node of which run this answers.
    pub node: NodeRunInfo,
    /// What the component produced. Required — unlike every other payload here,
    /// original gives it no default.
    pub step_output: StepPayload,
    #[serde(default)]
    pub step_input: StepPayload,
    #[serde(default)]
    pub custom_data: CustomData,
    #[serde(default)]
    pub reply_to_result: Option<CompactString>,
    #[serde(default)]
    pub reply_to_error: Option<CompactString>,
    #[serde(default)]
    pub reply_to_event: Option<CompactString>,
    #[serde(default)]
    pub headers: TraceHeaders,
    /// Unmodelled keys, preserved (`extra="allow"`).
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}

impl Serialize for MessageResult {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(None)?;
        // Base fields first -- see MessageRun's Serialize.
        map.serialize_entry("meta", &self.meta)?;
        map.serialize_entry("step_input", &self.step_input)?;
        map.serialize_entry("custom_data", &self.custom_data)?;
        map.serialize_entry("reply_to_result", &self.reply_to_result)?;
        map.serialize_entry("reply_to_error", &self.reply_to_error)?;
        map.serialize_entry("reply_to_event", &self.reply_to_event)?;
        map.serialize_entry("node", &self.node)?;
        map.serialize_entry("step_output", &self.step_output)?;
        for (key, value) in &self.extra {
            if !RESULT_RESERVED_WIRE_KEYS.contains(&key.as_str()) {
                map.serialize_entry(key, value)?;
            }
        }
        if !self.headers.is_empty() {
            map.serialize_entry("headers", &self.headers)?;
        }
        map.end()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn meta_json() -> Value {
        serde_json::json!({
            "job_id": "job-1", "tenant_id": "acme",
            "pipeline_type": "invoice", "pipeline_level": "page", "pipeline_name": "default",
        })
    }

    fn node_json() -> Value {
        serde_json::json!({
            "path": "job-1.invoice.page.default.X.C:2",
            "node_id": "C",
            "name": "freeform.page.commercial-invoice.kv",
            "kind": "Model",
            "pipeline_id": "invoice.page.default",
            "run_id": "job-1",
            "parent_id": "X",
            "parent_path": "job-1.invoice.page.default.X",
            "parent_kind": "DictAggregator",
            "child_index": 2,
        })
    }

    /// The shape the original actually constructs.
    fn run_json() -> Value {
        serde_json::json!({
            "meta": meta_json(),
            "node": node_json(),
            "step_input": {"doc": "invoice.pdf"},
            "custom_data": {},
            "node_env_vars": {"MODEL_VERSION": "3", "MODE": "fast"},
            "headers": {"traceparent": "00-abc-def-01"},
        })
    }

    /// The shape the original executor's `MessageOutputV2` writes back.
    fn result_json() -> Value {
        serde_json::json!({
            "meta": meta_json(),
            "node": node_json(),
            "step_input": {"doc": "invoice.pdf"},
            "step_output": {"total": 42},
            "headers": {"traceparent": "00-abc-def-01"},
        })
    }

    #[test]
    fn decodes_the_dispatch_message() -> Result<(), serde_json::Error> {
        let run: MessageRun = serde_json::from_value(run_json())?;
        assert_eq!(run.meta.run_id().as_str(), "job-1");
        assert_eq!(run.node.node_id.as_str(), "C");
        assert_eq!(run.node_env_vars["MODEL_VERSION"], "3");
        assert!(run.step_input.as_object().is_some());
        Ok(())
    }

    #[test]
    fn the_subject_is_the_step_name() -> Result<(), serde_json::Error> {
        // the original sends with topic=step.name, and node.name is that
        // value -- which is why NodeRunInfo::name is required.
        let run: MessageRun = serde_json::from_value(run_json())?;
        assert_eq!(run.subject(), Some("freeform.page.commercial-invoice.kv"));
        Ok(())
    }

    #[test]
    fn a_condition_runinfo_yields_no_subject() -> Result<(), serde_json::Error> {
        // construct_noderun substitutes the literal
        // "condition" for a Condition node's name. It is a placeholder, not a
        // subject -- nothing listens on it. A dispatcher that trusted
        // node.name would publish there and drop the work silently.
        let mut raw = run_json();
        raw["node"]["kind"] = serde_json::json!("Condition");
        raw["node"]["name"] = serde_json::json!("condition");
        let run: MessageRun = serde_json::from_value(raw)?;

        assert_eq!(run.subject(), None);
        // The field still round-trips; only the interpretation is guarded.
        assert_eq!(run.node.name, "condition");
        Ok(())
    }

    #[test]
    fn every_dispatchable_kind_yields_a_subject() -> Result<(), serde_json::Error> {
        for kind in ["Model", "ListAggregator", "DictAggregator"] {
            let mut raw = run_json();
            raw["node"]["type"] = serde_json::json!(kind);
            let run: MessageRun = serde_json::from_value(raw)?;
            assert_eq!(
                run.subject(),
                Some("freeform.page.commercial-invoice.kv"),
                "{kind} should dispatch"
            );
        }
        Ok(())
    }

    #[test]
    fn declared_fields_are_emitted_in_the_originals_order() -> Result<(), serde_json::Error> {
        // the original model library dumps a subclass base-fields-first. Verified against 2.13.4:
        // meta, step_input, custom_data, reply_to_result, reply_to_error,
        // reply_to_event, node, node_env_vars.
        let run: MessageRun = serde_json::from_value(run_json())?;
        let text = serde_json::to_string(&run)?;
        let order = [
            "\"meta\":",
            "\"step_input\":",
            "\"custom_data\":",
            "\"reply_to_result\":",
            "\"reply_to_error\":",
            "\"reply_to_event\":",
            "\"node\":",
            "\"node_env_vars\":",
        ];
        let mut last = 0;
        for key in order {
            let at = text.find(key).unwrap_or(usize::MAX);
            assert!(at > last, "{key} out of order in {text}");
            last = at;
        }
        // headers is injected after the dump, so it trails everything.
        assert!(text.find("\"headers\":").unwrap_or(0) > last, "{text}");

        let result: MessageResult = serde_json::from_value(result_json())?;
        let text = serde_json::to_string(&result)?;
        assert!(text.find("\"reply_to_event\":") < text.find("\"node\":"));
        assert!(text.find("\"node\":") < text.find("\"step_output\":"));
        Ok(())
    }

    #[test]
    fn the_dispatch_leg_carries_no_reply_topics() -> Result<(), serde_json::Error> {
        // the original passes none of them, so they travel as null.
        let run: MessageRun = serde_json::from_value(run_json())?;
        assert_eq!(run.reply_to_result, None);
        assert_eq!(run.reply_to_error, None);
        assert_eq!(run.reply_to_event, None);

        let json = serde_json::to_value(&run)?;
        for key in ["reply_to_result", "reply_to_error", "reply_to_event"] {
            assert!(json[key].is_null(), "{key} should be emitted as null");
        }
        Ok(())
    }

    #[test]
    fn node_env_vars_are_strings_not_arbitrary_json() {
        // dict[str, str] in original, map[string]string in the original.
        let mut raw = run_json();
        raw["node_env_vars"] = serde_json::json!({"MODEL_VERSION": 3});
        assert!(serde_json::from_value::<MessageRun>(raw).is_err());
    }

    #[test]
    fn decodes_the_result_message() -> Result<(), serde_json::Error> {
        let result: MessageResult = serde_json::from_value(result_json())?;
        assert_eq!(result.node.child_index.map(|c| c.get()), Some(2));
        assert!(result.step_output.as_object().is_some());
        assert_eq!(result.step_output.len(), 1);
        Ok(())
    }

    #[test]
    fn step_output_is_required_but_step_input_is_not() -> Result<(), serde_json::Error> {
        // original gives step_output no default, unlike every other payload.
        let mut raw = result_json();
        let removed = raw.as_object_mut().and_then(|o| o.remove("step_output"));
        assert!(removed.is_some());
        assert!(serde_json::from_value::<MessageResult>(raw).is_err());

        let mut raw = result_json();
        raw.as_object_mut().map(|o| o.remove("step_input"));
        let result: MessageResult = serde_json::from_value(raw)?;
        assert!(result.step_input.is_empty());
        Ok(())
    }

    #[test]
    fn a_scalar_step_output_is_rejected() {
        // The protocol notes record this: valid by the component contract,
        // forwarded by the original executor, rejected here exactly as the
        // original controller rejects it.
        let mut raw = result_json();
        raw["step_output"] = serde_json::json!("hello");
        assert!(serde_json::from_value::<MessageResult>(raw).is_err());
    }

    #[test]
    fn a_result_without_node_env_vars_decodes() -> Result<(), serde_json::Error> {
        // MessageOutputV2 has no node_env_vars -- config for the call is not
        // echoed back with its result.
        let result: MessageResult = serde_json::from_value(result_json())?;
        assert_eq!(result.meta.run_id().as_str(), "job-1");
        let json = serde_json::to_value(&result)?;
        assert!(json.get("node_env_vars").is_none());
        Ok(())
    }

    #[test]
    fn neither_catch_all_can_duplicate_a_named_wire_key() -> Result<(), serde_json::Error> {
        // Both subclass Message and inherit extra="allow", so both need the
        // guard. A duplicate key is last-wins on the original side.
        let mut run: MessageRun = serde_json::from_value(run_json())?;
        for key in RUN_RESERVED_WIRE_KEYS {
            run.extra
                .insert(key.to_owned(), Value::String("evil".into()));
        }
        let text = serde_json::to_string(&run)?;
        assert!(!text.contains("evil"), "{text}");
        for key in RUN_RESERVED_WIRE_KEYS {
            assert_eq!(text.matches(&format!("\"{key}\":")).count(), 1, "{key}");
        }
        let back: MessageRun = serde_json::from_str(&text)?;
        assert_eq!(back.node.node_id.as_str(), "C");

        let mut result: MessageResult = serde_json::from_value(result_json())?;
        for key in RESULT_RESERVED_WIRE_KEYS {
            result
                .extra
                .insert(key.to_owned(), Value::String("evil".into()));
        }
        let text = serde_json::to_string(&result)?;
        assert!(!text.contains("evil"), "{text}");
        let back: MessageResult = serde_json::from_str(&text)?;
        assert_eq!(back.step_output.len(), 1);
        Ok(())
    }

    #[test]
    fn genuine_extras_survive_on_both() -> Result<(), serde_json::Error> {
        let mut raw = run_json();
        raw["request_id"] = serde_json::json!("req-9");
        let run: MessageRun = serde_json::from_value(raw)?;
        assert_eq!(run.extra["request_id"], "req-9");
        assert_eq!(serde_json::to_value(&run)?["request_id"], "req-9");

        let mut raw = result_json();
        raw["request_id"] = serde_json::json!("req-9");
        let result: MessageResult = serde_json::from_value(raw)?;
        assert_eq!(serde_json::to_value(&result)?["request_id"], "req-9");
        Ok(())
    }

    #[test]
    fn both_round_trip() -> Result<(), serde_json::Error> {
        let run: MessageRun = serde_json::from_value(run_json())?;
        let text = serde_json::to_string(&run)?;
        assert_eq!(serde_json::from_str::<MessageRun>(&text)?, run);

        let result: MessageResult = serde_json::from_value(result_json())?;
        let text = serde_json::to_string(&result)?;
        assert_eq!(serde_json::from_str::<MessageResult>(&text)?, result);
        Ok(())
    }

    #[test]
    fn headers_are_omitted_when_empty_but_declared_fields_are_not() -> Result<(), serde_json::Error>
    {
        let mut raw = run_json();
        raw.as_object_mut().map(|o| o.remove("headers"));
        let run: MessageRun = serde_json::from_value(raw)?;
        let json = serde_json::to_value(&run)?;
        assert!(json.get("headers").is_none());
        assert_eq!(json["custom_data"], serde_json::json!({}));
        Ok(())
    }

    #[test]
    fn both_require_meta_and_runinfo() {
        for omit in ["meta", "node"] {
            let mut raw = run_json();
            raw.as_object_mut().map(|o| o.remove(omit));
            assert!(
                serde_json::from_value::<MessageRun>(raw).is_err(),
                "MessageRun without {omit} should fail"
            );

            let mut raw = result_json();
            raw.as_object_mut().map(|o| o.remove(omit));
            assert!(
                serde_json::from_value::<MessageResult>(raw).is_err(),
                "MessageResult without {omit} should fail"
            );
        }
    }

    #[test]
    fn debug_and_clone_are_available() -> Result<(), serde_json::Error> {
        let run: MessageRun = serde_json::from_value(run_json())?;
        assert_eq!(run.clone(), run);
        assert!(format!("{run:?}").contains("MessageRun"));

        let result: MessageResult = serde_json::from_value(result_json())?;
        assert_eq!(result.clone(), result);
        assert!(format!("{result:?}").contains("MessageResult"));
        Ok(())
    }
}
