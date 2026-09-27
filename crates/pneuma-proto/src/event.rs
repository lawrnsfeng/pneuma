//! [`MessageEvent`] and [`MessageRetry`] — the controller's inbound event and
//! retry messages.
//!
//! `MessageEvent` is what the original executor publishes to
//! `pneuma.event` (the original, the original
//! counterpart the original, consumed at
//! the original). `MessageRetry` is what the controller publishes to
//! itself on `pneuma.retry` (the original, produced at
//! the original, consumed the original).
//!
//! # Emission follows the original here, not original
//!
//! Every other envelope in this crate emits declared fields unconditionally,
//! because the original's `model_dump()` does and original writes them. `MessageEvent`
//! is the exception: its producer is the original executor, and the original struct
//! tags `error_code` and `error_message` `omitempty`. A Rust producer replacing
//! that service should emit what it emits.
//!
//! `node` is emitted unconditionally, including as `null`. the original types it as a
//! value rather than a pointer, so it is always present there; original declares
//! it `NodeRunInfo | None`. Emitting the key always is the reading that both
//! ends accept.
//!
//! `headers` is skipped when empty, matching the `omitempty` the original puts on it too.
//!
//! # This type keeps the `headers` original discards
//!
//! The same deviation [`crate::envelope::MessageInit`] documents, and it is
//! worth repeating rather than cross-referencing, because the consequence here
//! is different. `MessageEvent` is a plain `BaseModel` with no `extra="allow"`,
//! so the original controller **drops** the trace context the original executor attached —
//! every event arrives having lost its span linkage.
//!
//! This type decodes `headers` and writes it back. Being less lossy is safe:
//! no consumer breaks because a field it never received is now present, and it
//! is the difference between an event that can be correlated to the dispatch
//! that caused it and one that cannot.
//!
//! # `retry_count` is written and never read
//!
//! `MessageRetry` carries a counter that bounds nothing. It appears exactly
//! twice in the whole original service — the declaration the original and
//! the increment the original — and is never compared. `process_retry`
//! re-publishes to `self.listener_retry.topic`, the retry listener's own
//! subject, so a message whose receiver is down circulates indefinitely.
//!
//! `RETRY_TIME` does not bound it either, though not for the reason an earlier
//! revision of this file gave. It claimed `RETRY_TIME` limits a retry around
//! the *initial* dispatch. It does not: the initial dispatch
//! is a single un-retried `send_dict`. The tenacity
//! loop wraps **publishing the `MessageRetry` envelope onto the
//! retry subject**, and constructs the `MessageRetry` separately.
//!
//! So `RETRY_TIME` bounds how many times the controller will try to *enqueue* a
//! retry, not how many times that retry then circulates. Once the envelope is
//! on `pneuma.retry`, nothing counts anything.
//!
//! The field is modelled faithfully rather than fixed — this crate describes
//! the protocol, and the fix belongs to whoever consumes it. See
//! the defect notes, and [`MessageRetry::has_reached`], which exists so a
//! consumer can bound the loop without re-deriving why it needs to.

use std::collections::BTreeMap;

use compact_str::CompactString;
use pneuma_core::ids::{RunId, SubjectName};
use pneuma_core::status::NodeStatus;
use serde::ser::SerializeMap;
use serde::{Deserialize, Serialize, Serializer};
use serde_json::Value;

use crate::headers::TraceHeaders;
use crate::meta::Meta;
use crate::node::NodeRunInfo;

/// A lifecycle event reported by a component runner.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct MessageEvent {
    /// The new status. Accepts the legacy `"timeout"` spelling — this is one of
    /// exactly two fields original applies that coercion to.
    pub event: NodeStatus,
    pub meta: Meta,
    /// Which node this concerns. Optional in original, always present from the original.
    #[serde(default)]
    pub node: Option<NodeRunInfo>,
    #[serde(default)]
    pub error_code: Option<CompactString>,
    #[serde(default)]
    pub error_message: Option<CompactString>,
    #[serde(default)]
    pub headers: TraceHeaders,
}

impl MessageEvent {
    /// The run this event belongs to.
    ///
    /// Prefers `node.run_id` and falls back to `meta.run_id`, matching
    /// the original. The two agree in practice; the fallback exists
    /// because `node` is optional.
    pub fn run_id(&self) -> RunId {
        match &self.node {
            Some(node) => node.run_id.clone(),
            None => self.meta.run_id(),
        }
    }

    /// Whether this event should be written back to the node's stored record.
    ///
    /// Mirrors `need_update_runinfo`, which lists
    /// exactly the four statuses below. Note this is **not** the same set as
    /// [`NodeStatus::is_terminal`]: that also counts `Aggregated`,
    /// `HasChildError` and `HasChildTimedOut`, which are derived by the
    /// controller rather than reported by a runner. Reusing the wrong predicate
    /// here would write back states no component ever sends.
    pub fn needs_runinfo_update(&self) -> bool {
        matches!(
            self.event,
            NodeStatus::Finished | NodeStatus::Error | NodeStatus::TimedOut | NodeStatus::Cancelled
        )
    }

    /// Whether this event carries error detail.
    pub fn is_error(&self) -> bool {
        self.error_code.is_some() || self.error_message.is_some()
    }
}

impl Serialize for MessageEvent {
    /// `error_code` and `error_message` are omitted when absent, matching the
    /// `omitempty` on the original producer — see the module docs.
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(None)?;
        map.serialize_entry("meta", &self.meta)?;
        map.serialize_entry("node", &self.node)?;
        map.serialize_entry("event", &self.event)?;
        if let Some(code) = &self.error_code {
            map.serialize_entry("error_code", code)?;
        }
        if let Some(message) = &self.error_message {
            map.serialize_entry("error_message", message)?;
        }
        if !self.headers.is_empty() {
            map.serialize_entry("headers", &self.headers)?;
        }
        map.end()
    }
}

/// A dispatch the controller failed to deliver, queued to try again.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MessageRetry {
    /// The subject the wrapped message should be republished to.
    ///
    /// Wire key `subject`, where the original wrote `topic`. "Topic" is broker
    /// vocabulary in an envelope that travels over AMQP, NATS *and* HTTP --
    /// the design notes
    pub subject: SubjectName,
    /// The serialized `MessageRun` to resend. Wire key `message`.
    ///
    /// Deliberately an opaque map rather than a typed `MessageRun`: the
    /// controller round-trips this blob without inspecting it
    /// (the original sends `message.content` straight back out), and
    /// parsing it here would reject a payload the current system happily
    /// forwards.
    pub message: BTreeMap<String, Value>,
    /// How many times delivery has been attempted.
    ///
    /// Incremented by the controller and **compared against nothing** — see the
    /// module docs and the defect notes
    #[serde(default)]
    pub retry_count: u32,
}

impl MessageRetry {
    /// Whether delivery has already been attempted `limit` times or more.
    ///
    /// Named `has_reached` rather than `exceeds` deliberately: this is `>=`,
    /// and `exceeds` reads as `>`. A caller writing
    /// `if retry.has_reached(max) { drop }` would allow one attempt more than they
    /// meant, and the entire reason this method exists is to be used by someone
    /// who has not read these docs.
    ///
    /// The comparison the original service never makes — see the module docs and
    /// the defect notes
    pub fn has_reached(&self, limit: u32) -> bool {
        self.retry_count >= limit
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
            "path": "job-1.invoice.page.default.A",
            "node_id": "A",
            "name": "extract",
            "kind": "Model",
            "pipeline_id": "invoice.page.default",
            "run_id": "job-1",
        })
    }

    /// The shape the original publishes before dispatch.
    fn processing_event() -> Value {
        serde_json::json!({
            "meta": meta_json(),
            "node": node_json(),
            "event": "processing",
        })
    }

    #[test]
    fn decodes_the_go_processing_event() -> Result<(), serde_json::Error> {
        let event: MessageEvent = serde_json::from_value(processing_event())?;
        assert_eq!(event.event, NodeStatus::Processing);
        assert!(!event.is_error());
        assert!(!event.needs_runinfo_update());
        Ok(())
    }

    #[test]
    fn the_legacy_timeout_spelling_is_refused() {
        // original coerced `"timeout"` on exactly two fields, for the original message
        // provider that emitted it. Neither producer exists any more, so the
        // coercion is dead compatibility rather than tolerance --
        // the design notes -- and this is the assertion that keeps it gone.
        let mut raw = processing_event();
        raw["event"] = serde_json::json!("timeout");
        assert!(serde_json::from_value::<MessageEvent>(raw).is_err());
    }

    #[test]
    fn needs_runinfo_update_is_not_the_same_set_as_is_terminal() -> Result<(), serde_json::Error> {
        // the original lists exactly four statuses. NodeStatus::is_terminal
        // counts three more that the controller derives rather than a runner
        // reporting them, so reusing it here would write back states no
        // component ever sends. Exhaustive so a new status cannot slip in.
        let expected_update = [
            NodeStatus::Finished,
            NodeStatus::Error,
            NodeStatus::TimedOut,
            NodeStatus::Cancelled,
        ];
        let mut differed = 0;
        for status in NodeStatus::ALL {
            let mut raw = processing_event();
            raw["event"] = serde_json::to_value(status)?;
            let event: MessageEvent = serde_json::from_value(raw)?;

            assert_eq!(
                event.needs_runinfo_update(),
                expected_update.contains(&status),
                "wrong verdict for {status:?}"
            );
            if event.needs_runinfo_update() != status.is_terminal() {
                differed += 1;
            }
        }
        assert_eq!(
            differed, 3,
            "expected exactly Aggregated/HasChildError/HasChildTimedOut to differ"
        );
        Ok(())
    }

    #[test]
    fn run_id_prefers_runinfo_and_falls_back_to_meta() -> Result<(), serde_json::Error> {
        let event: MessageEvent = serde_json::from_value(processing_event())?;
        assert_eq!(event.run_id().as_str(), "job-1");

        // original declares node optional, so the fallback is reachable.
        let mut raw = processing_event();
        raw.as_object_mut().map(|o| o.remove("node"));
        let event: MessageEvent = serde_json::from_value(raw)?;
        assert!(event.node.is_none());
        assert_eq!(event.run_id().as_str(), "job-1");

        // And when they disagree, node wins.
        let mut raw = processing_event();
        raw["node"]["run_id"] = serde_json::json!("other-run");
        let event: MessageEvent = serde_json::from_value(raw)?;
        assert_eq!(event.run_id().as_str(), "other-run");
        Ok(())
    }

    #[test]
    fn error_detail_is_omitted_when_absent_matching_go() -> Result<(), serde_json::Error> {
        // The original producer tags both omitempty, unlike the original's model_dump.
        let event: MessageEvent = serde_json::from_value(processing_event())?;
        let json = serde_json::to_value(&event)?;
        assert!(json.get("error_code").is_none());
        assert!(json.get("error_message").is_none());
        // node is emitted regardless, because the original always sends it.
        assert!(json["node"].is_object());
        Ok(())
    }

    #[test]
    fn an_error_event_carries_its_detail() -> Result<(), serde_json::Error> {
        let mut raw = processing_event();
        raw["event"] = serde_json::json!("error");
        raw["error_code"] = serde_json::json!("PNEUMA_INTERNAL_ERROR");
        raw["error_message"] = serde_json::json!("boom");
        let event: MessageEvent = serde_json::from_value(raw)?;

        assert!(event.is_error());
        assert!(event.needs_runinfo_update());
        let json = serde_json::to_value(&event)?;
        assert_eq!(json["error_code"], "PNEUMA_INTERNAL_ERROR");
        assert_eq!(json["error_message"], "boom");
        Ok(())
    }

    #[test]
    fn an_absent_runinfo_is_emitted_as_null() -> Result<(), serde_json::Error> {
        let mut raw = processing_event();
        raw.as_object_mut().map(|o| o.remove("node"));
        let event: MessageEvent = serde_json::from_value(raw)?;
        let json = serde_json::to_value(&event)?;
        assert!(json["node"].is_null());
        Ok(())
    }

    #[test]
    fn the_event_round_trips() -> Result<(), serde_json::Error> {
        let mut raw = processing_event();
        raw["error_code"] = serde_json::json!("JOB_CANCELLED");
        raw["headers"] = serde_json::json!({"traceparent": "00-a-b-01"});
        let event: MessageEvent = serde_json::from_value(raw)?;
        let text = serde_json::to_string(&event)?;
        assert_eq!(serde_json::from_str::<MessageEvent>(&text)?, event);
        Ok(())
    }

    #[test]
    fn an_event_requires_its_status_and_meta() {
        for omit in ["event", "meta"] {
            let mut raw = processing_event();
            let removed = raw.as_object_mut().and_then(|o| o.remove(omit));
            assert!(removed.is_some());
            assert!(
                serde_json::from_value::<MessageEvent>(raw).is_err(),
                "omitting {omit} should fail"
            );
        }
    }

    #[test]
    fn decodes_a_retry() -> Result<(), serde_json::Error> {
        let retry: MessageRetry = serde_json::from_value(serde_json::json!({
            "subject": "freeform.page.commercial-invoice.kv",
            "message": {"meta": meta_json(), "step_input": {}},
        }))?;
        assert_eq!(
            retry.subject.as_str(),
            "freeform.page.commercial-invoice.kv"
        );
        assert_eq!(retry.retry_count, 0);
        assert!(retry.message.contains_key("meta"));
        Ok(())
    }

    #[test]
    fn the_retry_counter_bounds_nothing_by_itself() -> Result<(), serde_json::Error> {
        // original increments this and never compares it, so the loop is
        // unbounded -- the defect notes 8. `exceeds` is the comparison the
        // service never makes.
        let mut retry: MessageRetry = serde_json::from_value(serde_json::json!({
            "subject": "t",
            "message": {},
            "retry_count": 4,
        }))?;
        assert!(!retry.has_reached(5));
        retry.retry_count += 1;
        assert!(retry.has_reached(5));
        assert!(retry.has_reached(0), "a zero limit stops immediately");
        Ok(())
    }

    #[test]
    fn a_retrys_message_is_opaque() -> Result<(), serde_json::Error> {
        // The controller round-trips the blob without inspecting it, so this
        // must not reject a payload the current system forwards happily.
        let retry: MessageRetry = serde_json::from_value(serde_json::json!({
            "subject": "t",
            "message": {"not": "a message at all", "n": [1, 2, 3]},
        }))?;
        assert_eq!(retry.message["n"], serde_json::json!([1, 2, 3]));
        let text = serde_json::to_string(&retry)?;
        assert_eq!(serde_json::from_str::<MessageRetry>(&text)?, retry);
        Ok(())
    }

    #[test]
    fn a_retry_requires_a_subject_and_a_message() {
        for omit in ["subject", "message"] {
            let mut raw = serde_json::json!({"subject": "t", "message": {}});
            raw.as_object_mut().map(|o| o.remove(omit));
            assert!(
                serde_json::from_value::<MessageRetry>(raw).is_err(),
                "omitting {omit} should fail"
            );
        }
    }

    #[test]
    fn keeps_the_headers_the_original_controller_discards() -> Result<(), serde_json::Error> {
        // MessageEvent is a plain BaseModel with no extra="allow", so original
        // drops the trace context the original executor attached and every event arrives
        // having lost its span linkage. This type keeps it.
        let mut raw = processing_event();
        raw["headers"] = serde_json::json!({"traceparent": "00-abc-def-01"});
        let event: MessageEvent = serde_json::from_value(raw)?;
        assert_eq!(event.headers.get("traceparent"), Some("00-abc-def-01"));

        let json = serde_json::to_value(&event)?;
        assert_eq!(json["headers"]["traceparent"], "00-abc-def-01");
        Ok(())
    }

    #[test]
    fn has_reached_is_inclusive_at_the_limit() -> Result<(), serde_json::Error> {
        // `>=`, not `>`. A caller who assumed the latter would allow one more
        // attempt than intended, which is why the method is not named `exceeds`.
        let retry: MessageRetry = serde_json::from_value(serde_json::json!({
            "subject": "t", "message": {}, "retry_count": 5,
        }))?;
        assert!(retry.has_reached(5), "equal must count as reached");
        assert!(retry.has_reached(4));
        assert!(!retry.has_reached(6));
        Ok(())
    }

    #[test]
    fn debug_and_clone_are_available() -> Result<(), serde_json::Error> {
        let event: MessageEvent = serde_json::from_value(processing_event())?;
        assert_eq!(event.clone(), event);
        assert!(format!("{event:?}").contains("MessageEvent"));
        Ok(())
    }
}
