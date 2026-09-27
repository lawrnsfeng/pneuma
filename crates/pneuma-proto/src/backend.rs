//! The backend callbacks: the only messages that leave pneuma.
//!
//! Ported from the original. Everything else in
//! this crate travels between services we control; these three go to the
//! customer's own queues, on addresses they supplied. Their shapes are a public
//! contract, which is why [`crate::timestamp::IsoTimestamp`] exists.
//!
//! # Routing is by type
//!
//! `_send_to_backend` matches on the class and reads
//! the address from the **run record**, not from the message:
//!
//! | Variant | Topic |
//! |---|---|
//! | [`BackendMessageResult`] | `run.topic_output` |
//! | [`BackendMessageError`] | `run.topic_error` |
//! | [`BackendMessageEvent`] | `run.topic_event` |
//!
//! `BackendMessageResult` and `BackendMessageError` subclass `BackendMessage`,
//! and the original's `match` tries the subclasses first. [`BackendCallback`]
//! reproduces that as an untagged enum whose variant order is load-bearing —
//! see its docs.
//!
//! **If the address is `None`, the send is skipped and nothing is logged as an
//! error.** A run submitted without a `topic_output` executes fully, produces a
//! result, and reports nowhere. [`crate::envelope::Message::has_reply_topic`]
//! exists so that is detectable before the work is done.
//!
//! # What each variant actually carries
//!
//! Read off the four construction sites rather than inferred:
//!
//! | Variant | Statuses seen | Site |
//! |---|---|---|
//! | [`BackendMessageEvent`] | `processing` | the original |
//! | [`BackendMessageResult`] | `finished` | the original |
//! | [`BackendMessageError`] | `error`, `timed_out` | the original |
//! | [`BackendMessageError`] | `error`, code `JOB_CANCELLED` | the original |
//! | [`BackendMessageError`] | `error`, from the boot dead-letter path | the original |
//!
//! Two of those rows are easy to get wrong. `boot` reports failures straight to
//! the customer without the controller involved, guarded by
//! `if not msg.topic_error: return`. And a cancellation is **not** reported with
//! a `cancelled` status: the original sends
//! `event=NodeStatus.ERROR` with `error_code="JOB_CANCELLED"`. The
//! `NodeStatus.CANCELLED` two lines above it is the *noderun* database update,
//! not the callback. A consumer branching on a `cancelled` callback status would
//! wait for something that never arrives; the only signal is the error code.
//!
//! # `error_code` and `error_message` are never null
//!
//! `BackendMessageError` types both as required `str`, while the
//! [`crate::event::MessageEvent`] they are copied from types both as optional.
//! the original bridges that with `or ""`:
//!
//! So the customer's backend has only ever received an **empty string**, never
//! `null`. Modelling these as `Option` here would emit `null` at a boundary
//! that has never seen one, which is why they are plain strings.

use compact_str::CompactString;
use pneuma_core::status::NodeStatus;
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;

use crate::meta::Meta;
use crate::payload::StepPayload;
use crate::timestamp::IsoTimestamp;

/// Which of the caller's three addresses a callback is routed to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ReplyTopic {
    /// `run.topic_output`.
    Output,
    /// `run.topic_error`.
    Error,
    /// `run.topic_event`.
    Event,
}

/// A lifecycle notification with no payload.
///
/// The base `BackendMessage` in original. Named `...Event` here because it routes
/// to `topic_event` and because "the base one" is not a description.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BackendMessageEvent {
    pub event: NodeStatus,
    pub meta: Meta,
    /// Defaults to now, matching
    /// `Field(default_factory=lambda: datetime.now(UTC))`.
    #[serde(default)]
    pub timestamp: IsoTimestamp,
}

/// A completed run's output.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BackendMessageResult {
    pub event: NodeStatus,
    pub meta: Meta,
    #[serde(default)]
    pub timestamp: IsoTimestamp,
    /// What the run produced. Required, and an object or array — never a
    /// scalar; see [`StepPayload`].
    pub step_output: StepPayload,
}

/// A failure reported to the caller.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BackendMessageError {
    pub event: NodeStatus,
    pub meta: Meta,
    #[serde(default)]
    pub timestamp: IsoTimestamp,
    /// Required, and never `null` — see the module docs.
    pub error_message: CompactString,
    /// Required, and never `null`. Known values are `JOB_CANCELLED`
    /// and `PNEUMA_INTERNAL_ERROR`,
    /// plus whatever a component supplies.
    pub error_code: CompactString,
}

/// Any of the three callbacks, distinguished by shape rather than by a field.
///
/// # Why decoding is hand-written
///
/// The obvious spelling is `#[serde(untagged)]`, and it is wrong here in a way
/// that fails silently.
///
/// [`BackendMessageEvent`] is a structural **subset** of the other two — every
/// result and every error also has `event`, `meta`, and `timestamp` — and it
/// does not reject unknown fields, because the original model it ports ignores
/// them. Untagged decoding tries each variant and takes the first that fits, so
/// a *malformed* result or error still fits `Event`. Measured before this was
/// changed:
///
/// ```text
/// {"event":"finished","meta":{…},"step_output":"hello"}  -> Event
/// {"event":"error","meta":{…},"error_code":null,…}       -> Event
/// ```
///
/// A finished run whose component returned a scalar `step_output` — the exact
/// case [`crate::payload`] documents as reachable and rejected — would decode
/// as a bare event, drop its output, and route to `topic_event` instead of
/// `topic_output`. The customer never receives the result and nothing reports
/// an error.
///
/// So the variant is chosen by which discriminating key is present, and the
/// inner error is propagated. A malformed result now fails with
/// "step payload must be an object or an array, found string" instead of
/// quietly becoming something else.
///
/// Serialization stays `untagged`: on the way out there is exactly one shape
/// per variant and nothing to disambiguate.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(untagged)]
pub enum BackendCallback {
    /// Carries `step_output`.
    Result(BackendMessageResult),
    /// Carries `error_message` and `error_code`.
    Error(BackendMessageError),
    /// Carries neither.
    Event(BackendMessageEvent),
}

/// Keys that identify which callback a payload is.
const STEP_OUTPUT_KEY: &str = "step_output";
const ERROR_KEYS: [&str; 2] = ["error_message", "error_code"];

impl<'de> Deserialize<'de> for BackendCallback {
    /// Classifies on the discriminating key, then decodes that variant and
    /// propagates its error — see the type docs for why untagged is unsafe here.
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = Value::deserialize(deserializer)?;
        // `ok_or_else` rather than a multi-line `let ... else`, whose closing
        // punctuation the coverage tool attributes unreliably.
        let object = value
            .as_object()
            .ok_or_else(|| serde::de::Error::custom("a backend callback must be a JSON object"))?;

        if object.contains_key(STEP_OUTPUT_KEY) {
            serde_json::from_value(value)
                .map(BackendCallback::Result)
                .map_err(serde::de::Error::custom)
        } else if ERROR_KEYS.iter().any(|key| object.contains_key(*key)) {
            serde_json::from_value(value)
                .map(BackendCallback::Error)
                .map_err(serde::de::Error::custom)
        } else {
            serde_json::from_value(value)
                .map(BackendCallback::Event)
                .map_err(serde::de::Error::custom)
        }
    }
}

impl BackendCallback {
    /// Which of the caller's addresses this is sent to.
    ///
    /// The routing table from `_send_to_backend`, in the type rather than in a
    /// `match` a caller has to write correctly.
    pub fn reply_topic(&self) -> ReplyTopic {
        match self {
            BackendCallback::Result(_) => ReplyTopic::Output,
            BackendCallback::Error(_) => ReplyTopic::Error,
            BackendCallback::Event(_) => ReplyTopic::Event,
        }
    }

    /// The status this callback reports.
    pub fn event(&self) -> NodeStatus {
        match self {
            BackendCallback::Result(message) => message.event,
            BackendCallback::Error(message) => message.event,
            BackendCallback::Event(message) => message.event,
        }
    }

    /// The routing metadata.
    pub fn meta(&self) -> &Meta {
        match self {
            BackendCallback::Result(message) => &message.meta,
            BackendCallback::Error(message) => &message.meta,
            BackendCallback::Event(message) => &message.meta,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pneuma_core::status::NodeStatus;
    use serde_json::Value;

    fn meta_json() -> Value {
        serde_json::json!({
            "job_id": "job-1", "tenant_id": "acme",
            "pipeline_type": "invoice", "pipeline_level": "page", "pipeline_name": "default",
        })
    }

    /// the original — the only plain BackendMessage the controller sends.
    fn event_json() -> Value {
        serde_json::json!({
            "event": "processing",
            "meta": meta_json(),
            "timestamp": "2026-08-28T04:26:45.671363+00:00",
        })
    }

    /// Matches the original.
    fn result_json() -> Value {
        serde_json::json!({
            "event": "finished",
            "meta": meta_json(),
            "timestamp": "2026-08-28T04:26:45.671363+00:00",
            "step_output": {"total": 42},
        })
    }

    /// the original — note the `or ""` coercion upstream.
    fn error_json() -> Value {
        serde_json::json!({
            "event": "error",
            "meta": meta_json(),
            "timestamp": "2026-08-28T04:26:45.671363+00:00",
            "error_message": "boom",
            "error_code": "PNEUMA_INTERNAL_ERROR",
        })
    }

    #[test]
    fn each_shape_decodes_as_its_own_variant() -> Result<(), serde_json::Error> {
        let result: BackendCallback = serde_json::from_value(result_json())?;
        assert!(matches!(result, BackendCallback::Result(_)));

        let error: BackendCallback = serde_json::from_value(error_json())?;
        assert!(matches!(error, BackendCallback::Error(_)));

        let event: BackendCallback = serde_json::from_value(event_json())?;
        assert!(matches!(event, BackendCallback::Event(_)));
        Ok(())
    }

    #[test]
    fn the_base_variant_must_stay_last() -> Result<(), serde_json::Error> {
        // BackendMessageEvent is a structural SUBSET of the other two, so if it
        // were listed first every callback would decode as it and every one
        // would route to topic_event. This pins the ordering dependency that
        // mirrors the original's match, where the subclasses are tried first.
        for (raw, expected) in [
            (result_json(), ReplyTopic::Output),
            (error_json(), ReplyTopic::Error),
            (event_json(), ReplyTopic::Event),
        ] {
            let callback: BackendCallback = serde_json::from_value(raw)?;
            assert_eq!(
                callback.reply_topic(),
                expected,
                "a subset variant matched too early"
            );
        }
        Ok(())
    }

    #[test]
    fn routing_matches_send_to_backend() -> Result<(), serde_json::Error> {
        let result: BackendCallback = serde_json::from_value(result_json())?;
        assert_eq!(result.reply_topic(), ReplyTopic::Output);
        assert_eq!(result.event(), NodeStatus::Finished);
        assert_eq!(result.meta().run_id().as_str(), "job-1");

        let error: BackendCallback = serde_json::from_value(error_json())?;
        assert_eq!(error.reply_topic(), ReplyTopic::Error);
        assert_eq!(error.event(), NodeStatus::Error);
        assert_eq!(error.meta().run_id().as_str(), "job-1");

        let event: BackendCallback = serde_json::from_value(event_json())?;
        assert_eq!(event.reply_topic(), ReplyTopic::Event);
        assert_eq!(event.event(), NodeStatus::Processing);
        assert_eq!(event.meta().pipeline_id().as_str(), "invoice.page.default");
        Ok(())
    }

    #[test]
    fn the_timestamp_reaches_the_wire_in_originals_format() -> Result<(), serde_json::Error> {
        // These are the only messages that leave the system, so the format is
        // a contract with the customer's backend.
        let result: BackendMessageResult = serde_json::from_value(result_json())?;
        let json = serde_json::to_value(&result)?;
        assert_eq!(json["timestamp"], "2026-08-28T04:26:45.671363+00:00");
        Ok(())
    }

    #[test]
    fn an_absent_timestamp_defaults_to_now() -> Result<(), serde_json::Error> {
        let mut raw = event_json();
        raw.as_object_mut().map(|o| o.remove("timestamp"));
        let event: BackendMessageEvent = serde_json::from_value(raw)?;
        // Round-trips, which is what the truncation in IsoTimestamp guarantees.
        let text = serde_json::to_string(&event)?;
        assert_eq!(serde_json::from_str::<BackendMessageEvent>(&text)?, event);
        Ok(())
    }

    #[test]
    fn error_detail_is_required_and_never_null() {
        // the original coerces with `or ""`, so the customer's backend
        // has only ever seen an empty string. Modelling these as Option would
        // emit null at a boundary that has never received one.
        for omit in ["error_message", "error_code"] {
            let mut raw = error_json();
            let removed = raw.as_object_mut().and_then(|o| o.remove(omit));
            assert!(removed.is_some());
            assert!(
                serde_json::from_value::<BackendMessageError>(raw).is_err(),
                "omitting {omit} should fail"
            );
        }

        let mut raw = error_json();
        raw["error_code"] = Value::Null;
        assert!(serde_json::from_value::<BackendMessageError>(raw).is_err());
    }

    #[test]
    fn the_empty_string_the_coercion_produces_is_accepted() -> Result<(), serde_json::Error> {
        // The shape a MessageEvent with no error detail actually turns into.
        let mut raw = error_json();
        raw["error_message"] = serde_json::json!("");
        raw["error_code"] = serde_json::json!("");
        let error: BackendMessageError = serde_json::from_value(raw)?;
        assert_eq!(error.error_code, "");
        let json = serde_json::to_value(&error)?;
        assert_eq!(json["error_code"], "");
        assert!(!json["error_code"].is_null());
        Ok(())
    }

    #[test]
    fn a_cancellation_is_reported_as_an_error_not_as_cancelled() -> Result<(), serde_json::Error> {
        // the original sends event=NodeStatus.ERROR with
        // error_code="JOB_CANCELLED". The NodeStatus.CANCELLED two lines above
        // is the noderun DB update, not the callback -- so the only signal a
        // consumer gets is the error code.
        let mut raw = error_json();
        raw["event"] = serde_json::json!("error");
        raw["error_code"] = serde_json::json!("JOB_CANCELLED");
        raw["error_message"] = serde_json::json!("Job is already cancelled");
        let error: BackendMessageError = serde_json::from_value(raw)?;

        assert_eq!(error.event, NodeStatus::Error);
        assert_ne!(
            error.event,
            NodeStatus::Cancelled,
            "no cancelled callback is ever emitted"
        );
        assert_eq!(error.error_code, "JOB_CANCELLED");
        Ok(())
    }

    #[test]
    fn the_legacy_timeout_spelling_is_refused() {
        // `"timeout"` was accepted inbound for as long as the original message
        // provider emitted it. With no original producer left it is dead
        // compatibility -- the design notes -- and refusing it is what makes
        // that true rather than merely stated. `"timed_out"` is the spelling,
        // in both directions.
        let mut raw = error_json();
        raw["event"] = serde_json::json!("timeout");
        assert!(serde_json::from_value::<BackendMessageError>(raw).is_err());
    }

    #[test]
    fn a_scalar_step_output_is_rejected() {
        let mut raw = result_json();
        raw["step_output"] = serde_json::json!("hello");
        assert!(serde_json::from_value::<BackendMessageResult>(raw).is_err());
    }

    #[test]
    fn all_three_round_trip() -> Result<(), serde_json::Error> {
        for raw in [result_json(), error_json(), event_json()] {
            let callback: BackendCallback = serde_json::from_value(raw)?;
            let text = serde_json::to_string(&callback)?;
            assert_eq!(serde_json::from_str::<BackendCallback>(&text)?, callback);
        }
        Ok(())
    }

    #[test]
    fn declared_fields_are_emitted_in_the_originals_order() -> Result<(), serde_json::Error> {
        // Base fields first, then subclass fields.
        let result: BackendMessageResult = serde_json::from_value(result_json())?;
        let text = serde_json::to_string(&result)?;
        assert!(text.find("\"event\"") < text.find("\"meta\""));
        assert!(text.find("\"meta\"") < text.find("\"timestamp\""));
        assert!(text.find("\"timestamp\"") < text.find("\"step_output\""));

        let error: BackendMessageError = serde_json::from_value(error_json())?;
        let text = serde_json::to_string(&error)?;
        assert!(text.find("\"timestamp\"") < text.find("\"error_message\""));
        assert!(text.find("\"error_message\"") < text.find("\"error_code\""));
        Ok(())
    }

    #[test]
    fn a_malformed_result_fails_loudly_rather_than_becoming_an_event() {
        // Untagged decoding downgraded this to Event, dropping step_output and
        // routing a finished run to topic_event -- the customer never receives
        // the result and nothing errors. This is the scalar step_output case
        // that payload.rs documents as reachable.
        let bad = serde_json::json!({
            "event": "finished", "meta": meta_json(), "step_output": "hello",
        });
        let err = serde_json::from_value::<BackendCallback>(bad)
            .err()
            .map(|e| e.to_string())
            .unwrap_or_default();
        assert!(
            err.contains("object or an array"),
            "expected the payload error to propagate, got {err:?}"
        );
    }

    #[test]
    fn a_malformed_error_fails_loudly_rather_than_becoming_an_event() {
        let bad = serde_json::json!({
            "event": "error", "meta": meta_json(),
            "error_message": "boom", "error_code": null,
        });
        assert!(serde_json::from_value::<BackendCallback>(bad).is_err());

        // A partial error -- only one of the two keys -- must also not slip
        // through as an Event.
        let partial = serde_json::json!({
            "event": "error", "meta": meta_json(), "error_code": "X",
        });
        assert!(serde_json::from_value::<BackendCallback>(partial).is_err());
    }

    #[test]
    fn a_non_object_callback_is_rejected() {
        for bad in ["7", "\"x\"", "null", "[]"] {
            assert!(
                serde_json::from_str::<BackendCallback>(bad).is_err(),
                "{bad} should be rejected"
            );
        }
    }

    #[test]
    fn debug_and_clone_are_available() -> Result<(), serde_json::Error> {
        let callback: BackendCallback = serde_json::from_value(result_json())?;
        assert_eq!(callback.clone(), callback);
        assert!(format!("{callback:?}").contains("Result"));
        assert_eq!(ReplyTopic::Output, ReplyTopic::Output);
        assert!(format!("{:?}", ReplyTopic::Event).contains("Event"));
        Ok(())
    }
}
