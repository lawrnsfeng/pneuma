//! The ingress envelopes: [`Message`] and [`MessageInit`].
//!
//! `Message` is what the backend publishes to RabbitMQ `pneuma.input`
//! (the original, consumed at
//! the original). `MessageInit` is what `boot` then publishes to NATS
//! `pneuma.bootstrap` (the original, published at
//! the original, consumed the original).
//!
//! # `step_input` and `custom_data` are not the same shape
//!
//! Easy to conflate, and the two differ:
//!
//! | Field | original type | Modelled as |
//! |---|---|---|
//! | `step_input` | `dict[str, Any] \| list[Any]` | [`StepPayload`] |
//! | `custom_data` | `dict[str, Any]` | a plain map |
//!
//! `custom_data` may **not** be a list. Widening it to [`StepPayload`] would
//! accept messages the original controller rejects, which is the direction this
//! crate does not go — see the protocol notesd.
//!
//! # Why these are separate structs rather than a shared base
//!
//! original gets `MessageRun` and `MessageResult` by subclassing `Message`. The
//! obvious Rust translation is a `MessageCommon` flattened into each, and it is
//! deliberately not used here.
//!
//! `Message` is `extra="allow"`, so it needs a `#[serde(flatten)]` catch-all.
//! Nesting a catch-all inside another flattened struct is exactly the bug this
//! port already hit once in `pneuma_core::step`, where an inner `extra`
//! greedily absorbed a sibling struct's fields and two round-trip tests failed.
//! Each envelope therefore states its own shape. The duplication is a few
//! fields; the alternative is a silent corruption whose blast radius is every
//! message.
//!
//! # What gets emitted when empty
//!
//! One rule, applied consistently, because an encoded message that differs
//! byte-for-byte from the original's for the same logical content would undermine the
//! idempotency hashing this crate's maps are ordered for.
//!
//! **How far that actually reaches, precisely.** Declared fields are emitted in
//! the original model library's own order — base-class fields first, then subclass fields, which
//! is why [`crate::dispatch::MessageRun`] emits `node` *after* the topics
//! rather than next to `meta`. `headers` goes last, matching
//! `inject_tracing_context` appending it to the already-dumped dict.
//!
//! The catch-all is where parity stops. original emits extras in insertion order;
//! this crate emits them sorted, because a `BTreeMap` is what makes the
//! encoding deterministic in the first place. For a message with no extras —
//! which is every message on the dispatch and result legs — the bytes match.
//! For one carrying caller extras, the declared prefix matches and the tail may
//! not. Claiming more than that would be claiming something untested.
//!
//! **Fields original declares are always emitted, even when empty or `None`** —
//! `model_dump()` on the real model yields
//! `{'step_input': {}, 'custom_data': {}, 'reply_to_result': None}`. So
//! `custom_data` encodes as `{}` and an unset topic encodes as `null`, matching
//! the bytes rather than the semantics. `node` already does this for its
//! absent parents.
//!
//! **`headers` is the exception, and is skipped when empty**, because original
//! does not declare it: it is injected only when `TRACING_ENABLED`, so its
//! absence is a real state rather than an empty value. Every original struct tags it
//! `omitempty` for the same reason.
//!
//! # `MessageInit` and `headers`
//!
//! `MessageInit` is a plain `BaseModel` with no `extra="allow"`, so original
//! **drops** the `headers` key that `send_dict` injected on its way out.
//! The field is on the wire and the reader
//! discards it, breaking the trace at exactly the boot→ctrl hop.
//!
//! This type keeps it. Being less lossy than the original is safe here — no
//! consumer can break because a field it never received is now present — and it
//! is the difference between a trace that survives the hop and one that does
//! not. Recorded rather than silently improved.

use std::collections::BTreeMap;

use compact_str::CompactString;
use pneuma_core::ids::{PipelineId, RunId};
use serde::ser::SerializeMap;
use serde::{Deserialize, Serialize, Serializer};
use serde_json::{Map, Value};

use crate::headers::TraceHeaders;
use crate::meta::Meta;
use crate::payload::StepPayload;

/// Free-form data the caller attaches, passed through untouched.
///
/// A `dict[str, Any]` in original — an object, never a list.
pub type CustomData = Map<String, Value>;

/// What the backend submits to start a run.
///
/// `extra="allow"` in original, so unknown keys are preserved rather than
/// dropped; [`Message::extra`] holds them.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct Message {
    pub meta: Meta,
    /// The initial input. An object or an array — see the module docs.
    #[serde(default)]
    pub step_input: StepPayload,
    /// Caller data passed through untouched. An object only.
    #[serde(default)]
    pub custom_data: CustomData,
    /// Where results are published. Dropped by every original hop — see
    /// the protocol notes
    #[serde(default)]
    pub reply_to_result: Option<CompactString>,
    /// Where errors are published.
    #[serde(default)]
    pub reply_to_error: Option<CompactString>,
    /// Where lifecycle events are published.
    #[serde(default)]
    pub reply_to_event: Option<CompactString>,
    /// Trace context, injected just before serialization rather than declared.
    #[serde(default, skip_serializing_if = "TraceHeaders::is_empty")]
    pub headers: TraceHeaders,
    /// Unmodelled keys, preserved (`extra="allow"`).
    ///
    /// Decoding is safe in any declaration order: serde matches named fields
    /// first and the catch-all takes only what is left. Verified by moving this
    /// field to the top of the struct, which changed nothing.
    ///
    /// **Encoding is not** — hence the hand-written [`Serialize`]. See
    /// `RESERVED_WIRE_KEYS` below.
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}

/// Wire keys [`Message`] emits from its own named fields.
///
/// [`Message::extra`] is a public map and `extra="allow"` means real messages
/// put real keys in it, so a caller that copies an inbound dict into `extra`
/// while also setting the typed fields lands a collision here. Flattened
/// alongside the named fields, that emits the key **twice**.
///
/// This is not theoretical and it is not symmetric. Encoding a `Message` whose
/// `extra` holds `reply_to_error` produces
/// `{... "reply_to_error":"real.err", ... "reply_to_error":"evil"}`. Rust refuses to
/// read that back (`duplicate field`), but `orjson.loads` on the original side
/// takes **last-wins** — so the reply address is silently redirected, and a
/// duplicated `meta` becomes a string that then fails validation.
///
/// Encoding skips these keys so that cannot happen. [`crate::meta::Meta`]
/// carries the same guard for the same reason; this type was written without it
/// and a review caught the omission.
const RESERVED_WIRE_KEYS: [&str; 7] = [
    "meta",
    "step_input",
    "custom_data",
    "reply_to_result",
    "reply_to_error",
    "reply_to_event",
    "headers",
];

impl Serialize for Message {
    /// Written by hand so the catch-all cannot duplicate a named wire key.
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(None)?;
        // Declared fields are always emitted, matching `model_dump()`.
        map.serialize_entry("meta", &self.meta)?;
        map.serialize_entry("step_input", &self.step_input)?;
        map.serialize_entry("custom_data", &self.custom_data)?;
        map.serialize_entry("reply_to_result", &self.reply_to_result)?;
        map.serialize_entry("reply_to_error", &self.reply_to_error)?;
        map.serialize_entry("reply_to_event", &self.reply_to_event)?;
        for (key, value) in &self.extra {
            if !RESERVED_WIRE_KEYS.contains(&key.as_str()) {
                map.serialize_entry(key, value)?;
            }
        }
        // `headers` is undeclared in original and injected only when tracing is
        // on, so an empty one is omitted rather than written as `{}` -- and it
        // goes last, because `inject_tracing_context` appends it to the dict
        // that `model_dump()` already produced.
        if !self.headers.is_empty() {
            map.serialize_entry("headers", &self.headers)?;
        }
        map.end()
    }
}

impl Message {
    /// The run this message starts, derived from [`Meta`].
    pub fn run_id(&self) -> RunId {
        self.meta.run_id()
    }

    /// The pipeline to execute, derived from [`Meta`].
    pub fn pipeline_id(&self) -> PipelineId {
        self.meta.pipeline_id()
    }

    /// Whether any **usable** reply address was supplied.
    ///
    /// All three are optional and `_send_to_backend` silently no-ops when the
    /// relevant one is absent, so a message with none
    /// executes fully and reports nowhere. This predicate exists so a caller can
    /// notice that before doing the work rather than after.
    ///
    /// **An empty string does not count**, because in original it is strictly
    /// worse than absent. Two guards on the same path disagree about it:
    ///
    /// - `_send_to_backend` tests `if topic is None: return`,
    ///   so `""` passes and it goes on to send.
    /// - Every sender then tests `topic = topic or self.topic` followed by
    ///   `if not topic: raise NoTopicSpecifiedError` — identically in all
    ///   three backends (the original,
    ///   the original). `""` is falsy, so it falls through to the
    ///   sender's own default topic, which for the controller's `event_sender`
    ///   is `None` (the original passes only a URI). The send
    ///   raises.
    ///
    /// `NoTopicSpecifiedError` is caught nowhere on this path — the only
    /// handler is the original — so it propagates out of
    /// `on_message`, `message.process()` rejects the delivery, and the message
    /// is dead-lettered via the configured `x-dead-letter-routing-key`.
    ///
    /// So the run executes fully and its result is dead-lettered rather than
    /// reported: the outcome an absent topic would give, reached by a failure
    /// path, after the work is paid for. That is what makes an empty string a
    /// non-address here rather than merely an odd one.
    ///
    /// Recorded as the defect notes
    ///
    /// An earlier version used `is_some()` and returned `true` for `Some("")`.
    pub fn has_reply_topic(&self) -> bool {
        let usable =
            |topic: &Option<CompactString>| topic.as_deref().is_some_and(|t| !t.is_empty());
        usable(&self.reply_to_result)
            || usable(&self.reply_to_error)
            || usable(&self.reply_to_event)
    }
}

/// What `boot` hands to the controller once a run has been created.
///
/// A plain `BaseModel` in original — unknown keys are ignored, not preserved, so
/// there is no catch-all here.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MessageInit {
    pub run_id: RunId,
    pub pipeline_id: PipelineId,
    pub meta: Meta,
    #[serde(default)]
    pub step_input: StepPayload,
    #[serde(default, skip_serializing_if = "Map::is_empty")]
    pub custom_data: CustomData,
    /// Present on the wire, discarded by original — see the module docs.
    #[serde(default, skip_serializing_if = "TraceHeaders::is_empty")]
    pub headers: TraceHeaders,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn meta_json() -> Value {
        serde_json::json!({
            "job_id": "job-1",
            "tenant_id": "acme",
            "pipeline_type": "invoice",
            "pipeline_level": "page",
            "pipeline_name": "default",
        })
    }

    fn message_json() -> Value {
        serde_json::json!({
            "meta": meta_json(),
            "step_input": {"doc": "invoice.pdf"},
            "custom_data": {"trace": "abc"},
            "reply_to_result": "run.out",
            "reply_to_error": "run.err",
            "reply_to_event": "run.evt",
            "headers": {"traceparent": "00-abc-def-01"},
        })
    }

    #[test]
    fn decodes_the_full_backend_message() -> Result<(), serde_json::Error> {
        let message: Message = serde_json::from_value(message_json())?;
        assert_eq!(message.run_id().as_str(), "job-1");
        assert_eq!(message.pipeline_id().as_str(), "invoice.page.default");
        assert_eq!(message.reply_to_result.as_deref(), Some("run.out"));
        assert_eq!(message.headers.get("traceparent"), Some("00-abc-def-01"));
        assert!(message.step_input.as_object().is_some());
        assert!(message.extra.is_empty());
        Ok(())
    }

    #[test]
    fn the_catch_all_does_not_swallow_named_fields() -> Result<(), serde_json::Error> {
        // The bug this module's layout exists to avoid: a flattened catch-all
        // claiming keys that a named field owns. pneuma-core::step hit exactly
        // this once. If `extra` were greedy, these would land there instead.
        let message: Message = serde_json::from_value(message_json())?;
        for key in [
            "meta",
            "step_input",
            "custom_data",
            "reply_to_result",
            "reply_to_error",
            "reply_to_event",
            "headers",
        ] {
            assert!(
                !message.extra.contains_key(key),
                "{key} was swallowed by the catch-all"
            );
        }
        assert!(message.headers.get("traceparent").is_some());
        Ok(())
    }

    #[test]
    fn unknown_keys_are_preserved() -> Result<(), serde_json::Error> {
        // extra="allow" -- a caller key must survive a round trip.
        let mut raw = message_json();
        raw["request_id"] = serde_json::json!("req-9");
        raw["page_idx"] = serde_json::json!(3);

        let message: Message = serde_json::from_value(raw)?;
        assert_eq!(message.extra["request_id"], "req-9");
        assert_eq!(message.extra["page_idx"], 3);

        let back = serde_json::to_value(&message)?;
        assert_eq!(back["request_id"], "req-9");
        assert_eq!(back["page_idx"], 3);
        Ok(())
    }

    #[test]
    fn round_trips_unchanged() -> Result<(), serde_json::Error> {
        let message: Message = serde_json::from_value(message_json())?;
        let text = serde_json::to_string(&message)?;
        assert_eq!(serde_json::from_str::<Message>(&text)?, message);
        Ok(())
    }

    #[test]
    fn payload_and_custom_data_have_different_shapes() -> Result<(), serde_json::Error> {
        // step_input is dict|list; custom_data is dict only. Widening
        // custom_data would accept what the original controller rejects.
        let mut raw = message_json();
        raw["step_input"] = serde_json::json!([{"page": 1}, {"page": 2}]);
        let message: Message = serde_json::from_value(raw)?;
        assert_eq!(message.step_input.len(), 2);

        let mut raw = message_json();
        raw["custom_data"] = serde_json::json!([1, 2]);
        assert!(
            serde_json::from_value::<Message>(raw).is_err(),
            "custom_data must reject a list"
        );
        Ok(())
    }

    #[test]
    fn declared_fields_are_emitted_even_when_empty() -> Result<(), serde_json::Error> {
        // Only `meta` is required; everything else has a original default.
        let message: Message = serde_json::from_value(serde_json::json!({"meta": meta_json()}))?;
        assert!(message.step_input.is_empty());
        assert!(message.custom_data.is_empty());
        assert!(message.headers.is_empty());
        assert!(!message.has_reply_topic());

        // the original's model_dump() emits every declared field even when empty or
        // None -- {'step_input': {}, 'custom_data': {}, 'reply_to_result': None}.
        let json = serde_json::to_value(&message)?;
        assert_eq!(json["step_input"], serde_json::json!({}));
        assert_eq!(json["custom_data"], serde_json::json!({}));
        for key in ["reply_to_result", "reply_to_error", "reply_to_event"] {
            assert!(json[key].is_null(), "{key} should be emitted as null");
        }
        // `headers` is the exception: undeclared in original and injected only
        // when tracing is on, so absent means absent.
        assert!(json.get("headers").is_none());
        Ok(())
    }

    #[test]
    fn a_message_without_any_reply_topic_is_detectable() -> Result<(), serde_json::Error> {
        // the original silently no-ops when the topic is absent, so
        // such a run executes fully and reports nowhere.
        let bare: Message = serde_json::from_value(serde_json::json!({"meta": meta_json()}))?;
        assert!(!bare.has_reply_topic());

        let full: Message = serde_json::from_value(message_json())?;
        assert!(full.has_reply_topic());

        let mut partial = message_json();
        partial["reply_to_result"] = Value::Null;
        partial["reply_to_error"] = Value::Null;
        let partial: Message = serde_json::from_value(partial)?;
        assert!(
            partial.has_reply_topic(),
            "reply_to_event alone still counts"
        );
        Ok(())
    }

    #[test]
    fn an_empty_reply_topic_does_not_count_as_supplied() {
        // the original's guard is `if topic is None`, so "" passes it and reaches
        // basic_publish as the routing key. An empty routing key is not an
        // address anyone receives on, so a run configured that way executes
        // fully and reports nowhere -- exactly what this predicate exists to
        // catch. An earlier version used is_some() and said true.
        let mut raw = message_json();
        raw["reply_to_result"] = serde_json::json!("");
        raw["reply_to_error"] = serde_json::json!("");
        raw["reply_to_event"] = serde_json::json!("");
        let Ok(message) = serde_json::from_value::<Message>(raw) else {
            panic!("should decode");
        };
        assert!(!message.has_reply_topic());

        // The fields still round-trip; only the interpretation changes.
        let Ok(json) = serde_json::to_value(&message) else {
            panic!("should encode");
        };
        assert_eq!(json["reply_to_result"], "");

        // One usable address among empties is still usable.
        let mut raw = message_json();
        raw["reply_to_result"] = serde_json::json!("");
        raw["reply_to_error"] = serde_json::json!("");
        let Ok(partial) = serde_json::from_value::<Message>(raw) else {
            panic!("should decode");
        };
        assert!(partial.has_reply_topic(), "reply_to_event is still set");
    }

    #[test]
    fn a_missing_meta_is_rejected() {
        assert!(serde_json::from_value::<Message>(serde_json::json!({})).is_err());
    }

    #[test]
    fn decodes_the_bootstrap_message() -> Result<(), serde_json::Error> {
        let init: MessageInit = serde_json::from_value(serde_json::json!({
            "run_id": "job-1",
            "pipeline_id": "invoice.page.default",
            "meta": meta_json(),
            "step_input": {"doc": "invoice.pdf"},
        }))?;
        assert_eq!(init.run_id.as_str(), "job-1");
        assert_eq!(init.pipeline_id.as_str(), "invoice.page.default");
        assert!(init.custom_data.is_empty());
        assert!(init.headers.is_empty());
        Ok(())
    }

    #[test]
    fn message_init_keeps_the_headers_reference_discards() -> Result<(), serde_json::Error> {
        // MessageInit is a plain BaseModel, so original ignores the headers key
        // send_dict injected -- breaking the trace at the boot->ctrl hop. This
        // type keeps it; see the module docs.
        let init: MessageInit = serde_json::from_value(serde_json::json!({
            "run_id": "job-1",
            "pipeline_id": "invoice.page.default",
            "meta": meta_json(),
            "headers": {"traceparent": "00-abc-def-01"},
        }))?;
        assert_eq!(init.headers.get("traceparent"), Some("00-abc-def-01"));

        let back = serde_json::to_value(&init)?;
        assert_eq!(back["headers"]["traceparent"], "00-abc-def-01");
        Ok(())
    }

    #[test]
    fn message_init_ignores_unknown_keys_rather_than_keeping_them() -> Result<(), serde_json::Error>
    {
        // No extra="allow" on this one, so parity is to drop rather than keep.
        let init: MessageInit = serde_json::from_value(serde_json::json!({
            "run_id": "job-1",
            "pipeline_id": "invoice.page.default",
            "meta": meta_json(),
            "surprise": "ignored",
        }))?;
        let back = serde_json::to_value(&init)?;
        assert!(back.get("surprise").is_none());
        Ok(())
    }

    #[test]
    fn message_init_requires_its_identifiers() {
        for omit in ["run_id", "pipeline_id", "meta"] {
            let mut raw = serde_json::json!({
                "run_id": "job-1",
                "pipeline_id": "invoice.page.default",
                "meta": meta_json(),
            });
            let removed = raw.as_object_mut().and_then(|o| o.remove(omit));
            assert!(removed.is_some(), "{omit} should have been present");
            assert!(
                serde_json::from_value::<MessageInit>(raw).is_err(),
                "omitting {omit} should fail"
            );
        }
    }

    #[test]
    fn message_init_round_trips() -> Result<(), serde_json::Error> {
        let init: MessageInit = serde_json::from_value(serde_json::json!({
            "run_id": "job-1",
            "pipeline_id": "invoice.page.default",
            "meta": meta_json(),
            "step_input": [1, 2],
            "custom_data": {"k": "v"},
            "headers": {"traceparent": "x"},
        }))?;
        let text = serde_json::to_string(&init)?;
        assert_eq!(serde_json::from_str::<MessageInit>(&text)?, init);
        Ok(())
    }

    #[test]
    fn the_catch_all_cannot_duplicate_a_named_wire_key() -> Result<(), serde_json::Error> {
        // `extra` is public and extra="allow" means real messages put real keys
        // there, so a caller copying an inbound dict into it can collide with a
        // typed field. Emitting both produces a duplicate JSON key -- which
        // Rust refuses to re-read, but orjson takes LAST-WINS, silently
        // redirecting the reply address.
        let mut message: Message = serde_json::from_value(serde_json::json!({
            "meta": meta_json(),
            "reply_to_error": "real.err",
        }))?;
        for key in RESERVED_WIRE_KEYS {
            message
                .extra
                .insert(key.to_owned(), Value::String("evil".into()));
        }

        let text = serde_json::to_string(&message)?;
        for key in RESERVED_WIRE_KEYS {
            let expected = usize::from(key != "headers");
            assert_eq!(
                text.matches(&format!("\"{key}\":")).count(),
                expected,
                "{key} emitted the wrong number of times: {text}"
            );
        }
        assert!(!text.contains("evil"), "{text}");

        // The point: what we emit, we can read back, and the typed value wins.
        let back: Message = serde_json::from_str(&text)?;
        assert_eq!(back.reply_to_error.as_deref(), Some("real.err"));
        Ok(())
    }

    #[test]
    fn a_genuine_extra_still_survives_alongside_the_guard() -> Result<(), serde_json::Error> {
        // The guard must not swallow ordinary caller keys.
        let mut message: Message = serde_json::from_value(message_json())?;
        message
            .extra
            .insert("request_id".to_owned(), Value::String("req-9".into()));
        message
            .extra
            .insert("meta".to_owned(), Value::String("dropped".into()));

        let json = serde_json::to_value(&message)?;
        assert_eq!(json["request_id"], "req-9");
        assert!(json["meta"].is_object(), "the typed meta must win");
        Ok(())
    }

    #[test]
    fn debug_and_clone_are_available() -> Result<(), serde_json::Error> {
        let message: Message = serde_json::from_value(message_json())?;
        assert_eq!(message.clone(), message);
        assert!(format!("{message:?}").contains("invoice"));
        Ok(())
    }
}
