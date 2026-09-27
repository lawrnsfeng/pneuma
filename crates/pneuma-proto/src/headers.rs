//! [`TraceHeaders`] — the trace-context map that rides on nearly every
//! message while being declared on no original model.
//!
//! # Where it comes from
//!
//! the original never declares this field. It is injected into the already-dumped
//! dict immediately before serialization, by `inject_tracing_context`:
//!
//! `CONTEXT_CONTAINER_KEY` defaults to `"headers"` and `TRACING_ENABLED`
//! defaults to `True`, and it is called
//! from each of the `send_dict` implementations —
//! the original. So in
//! a default deployment essentially every NATS and AMQP body carries a
//! top-level `headers` object, which is precisely why `Message` needs
//! `extra="allow"` to survive a round trip at all.
//!
//! Not *quite* every sender, though: the original
//! overrides `send_dict` and calls `self.send(orjson.dumps(message), topic)`
//! directly, bypassing the injection. Termination-signal bodies carry no
//! `headers` key. Anyone porting that path should not expect the field.
//!
//! This crate declares the field rather than letting a catch-all absorb it. It
//! is real protocol carried on a documented schedule, and leaving it implicit
//! is how it came to be undocumented in the first place.
//!
//! The RabbitMQ path *also* injects trace context into the AMQP protocol
//! headers, which is a different carrier from this
//! JSON body key. Only the body key is modelled here.
//!
//! # Why a newtype rather than a plain map
//!
//! Because a plain map with `#[serde(default)]` rejects an explicit `null`.
//! Serde's `default` covers a *missing* key only: a `BTreeMap` field fails with
//! `invalid type: null, expected a map`. [`TraceHeaders`] accepts missing,
//! `null`, and a map.
//!
//! **This is defensive, not observed.** An earlier revision of this module
//! justified it by claiming `MessageInputV2`
//! emits
//! `"headers": null` because it alone omits `omitempty`. It does lack
//! `omitempty` — but it is a *consumer input* struct, only ever unmarshalled
//! (the original,
//! the original) and never marshalled. All four `json.Marshal`
//! sites in that service write `MessageOutputV2`, `MessageEvent`, or
//! `MessageSeldonInputV2`, and all three tag `headers` with `omitempty`;
//! the original broker does too, and no original model declares the field at
//! all. **No producer in the system emits `"headers": null`.**
//!
//! The tolerance is kept anyway, because the cost is one `Option` and the
//! failure it prevents is a whole message rejected over a field that carries
//! only tracing metadata. But it is defence against a caller that has not been
//! observed, and this crate's docs are supposed to distinguish those.
//!
//! Note that such a caller would break the original side too:
//! `inject_tracing_context` only tests `key not in message`, so an incoming
//! explicit `null` stays `None` and is then passed to `.inject()`.

use std::collections::BTreeMap;

use compact_str::CompactString;
use serde::{Deserialize, Deserializer, Serialize};

/// W3C trace-context headers carried in the message body.
///
/// A `BTreeMap` so encoding is deterministic and byte-comparable.
///
/// The port plans to derive idempotency keys by hashing encoded messages
/// (`ARCHITECTURE-V2.md`), which would make ordering load-bearing. That does
/// not exist yet — the survey notes record that the only idempotency in
/// the system today is `construct_noderun`'s `ON CONFLICT DO NOTHING`. The
/// choice is right regardless; the future use is why it is worth pinning now.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
#[serde(transparent)]
pub struct TraceHeaders(BTreeMap<CompactString, CompactString>);

impl TraceHeaders {
    /// Whether any header is present.
    ///
    /// Named for use as `skip_serializing_if`, which is how a containing
    /// message reproduces the original's `omitempty`.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// The number of headers.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Looks up one header.
    pub fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).map(CompactString::as_str)
    }

    /// Inserts one header, returning any previous value.
    pub fn insert(
        &mut self,
        key: impl Into<CompactString>,
        value: impl Into<CompactString>,
    ) -> Option<CompactString> {
        self.0.insert(key.into(), value.into())
    }

    /// Iterates the headers in key order.
    pub fn iter(&self) -> impl Iterator<Item = (&str, &str)> {
        self.0
            .iter()
            .map(|(key, value)| (key.as_str(), value.as_str()))
    }
}

impl FromIterator<(CompactString, CompactString)> for TraceHeaders {
    fn from_iter<I: IntoIterator<Item = (CompactString, CompactString)>>(iter: I) -> Self {
        TraceHeaders(iter.into_iter().collect())
    }
}

impl<'de> Deserialize<'de> for TraceHeaders {
    /// Accepts a map, an explicit `null`, and (via `#[serde(default)]` on the
    /// containing field) a missing key.
    ///
    /// The `null` case is defensive rather than observed — see the module docs.
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let raw = Option::<BTreeMap<CompactString, CompactString>>::deserialize(deserializer)?;
        Ok(TraceHeaders(raw.unwrap_or_default()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The shape `TraceContextTextMapPropagator` injects.
    fn sample() -> TraceHeaders {
        let mut headers = TraceHeaders::default();
        headers.insert(
            "traceparent",
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
        );
        headers
    }

    #[test]
    fn decodes_a_map() -> Result<(), serde_json::Error> {
        let headers: TraceHeaders = serde_json::from_str(r#"{"traceparent":"00-abc-def-01"}"#)?;
        assert_eq!(headers.get("traceparent"), Some("00-abc-def-01"));
        assert_eq!(headers.len(), 1);
        assert!(!headers.is_empty());
        Ok(())
    }

    #[test]
    fn decodes_an_explicit_null() -> Result<(), serde_json::Error> {
        // Defensive: no producer is known to emit this, but a plain map would
        // reject the whole message over a tracing field. See the module docs.
        let headers: TraceHeaders = serde_json::from_str("null")?;
        assert!(headers.is_empty());
        Ok(())
    }

    #[test]
    fn the_naive_modelling_would_reject_that_null() {
        // Justifies the hand-written Deserialize rather than asserting it does.
        // A plain map field with #[serde(default)] covers a MISSING key, not an
        // explicit null.
        #[derive(Deserialize)]
        struct Naive {
            #[serde(default)]
            #[allow(dead_code)]
            headers: BTreeMap<CompactString, CompactString>,
        }

        // Missing key: fine.
        assert!(serde_json::from_str::<Naive>("{}").is_ok());
        // Explicit null: rejected. Asserted as `is_err` rather than on
        // serde_json's message text, which is not a stability guarantee -- a
        // dependency bump could redden this test with no behaviour change.
        assert!(serde_json::from_str::<Naive>(r#"{"headers":null}"#).is_err());

        // The real type accepts the same input.
        #[derive(Deserialize)]
        struct Real {
            #[serde(default)]
            headers: TraceHeaders,
        }
        let parsed: Result<Real, _> = serde_json::from_str(r#"{"headers":null}"#);
        assert!(parsed.is_ok_and(|r| r.headers.is_empty()));
    }

    #[test]
    fn decodes_an_empty_map() -> Result<(), serde_json::Error> {
        let headers: TraceHeaders = serde_json::from_str("{}")?;
        assert!(headers.is_empty());
        assert_eq!(headers.len(), 0);
        Ok(())
    }

    #[test]
    fn encodes_as_a_bare_map_not_a_wrapper() -> Result<(), serde_json::Error> {
        // `transparent`, so the newtype must not appear on the wire.
        let json = serde_json::to_string(&sample())?;
        assert!(json.starts_with('{'), "{json}");
        assert!(json.contains("traceparent"), "{json}");
        Ok(())
    }

    #[test]
    fn round_trips() -> Result<(), serde_json::Error> {
        let headers = sample();
        let text = serde_json::to_string(&headers)?;
        let back: TraceHeaders = serde_json::from_str(&text)?;
        assert_eq!(headers, back);
        Ok(())
    }

    #[test]
    fn encodes_deterministically() -> Result<(), serde_json::Error> {
        // Idempotency keys are derived by hashing the encoded message.
        let headers: TraceHeaders = serde_json::from_str(r#"{"zeta":"1","alpha":"2","mid":"3"}"#)?;
        let first = serde_json::to_string(&headers)?;
        for _ in 0..8 {
            assert_eq!(serde_json::to_string(&headers.clone())?, first);
        }
        assert!(first.find("alpha") < first.find("mid"));
        assert!(first.find("mid") < first.find("zeta"));
        Ok(())
    }

    #[test]
    fn insert_returns_the_previous_value() {
        let mut headers = TraceHeaders::default();
        assert_eq!(headers.insert("k", "first"), None);
        assert_eq!(headers.insert("k", "second").as_deref(), Some("first"));
        assert_eq!(headers.get("k"), Some("second"));
        assert_eq!(headers.get("absent"), None);
    }

    #[test]
    fn iterates_in_key_order() -> Result<(), serde_json::Error> {
        let headers: TraceHeaders = serde_json::from_str(r#"{"b":"2","a":"1"}"#)?;
        let seen: Vec<_> = headers.iter().collect();
        assert_eq!(seen, vec![("a", "1"), ("b", "2")]);
        Ok(())
    }

    #[test]
    fn collects_from_pairs() {
        let headers: TraceHeaders = [
            (CompactString::from("a"), CompactString::from("1")),
            (CompactString::from("b"), CompactString::from("2")),
        ]
        .into_iter()
        .collect();
        assert_eq!(headers.len(), 2);
        assert_eq!(headers.get("a"), Some("1"));
    }

    #[test]
    fn a_non_map_is_rejected() {
        assert!(serde_json::from_str::<TraceHeaders>("7").is_err());
        assert!(serde_json::from_str::<TraceHeaders>(r#""x""#).is_err());
        // Values are strings; the original models this as map[string]string.
        assert!(serde_json::from_str::<TraceHeaders>(r#"{"k":7}"#).is_err());
    }

    #[test]
    fn default_is_empty_and_debuggable() {
        let headers = TraceHeaders::default();
        assert!(headers.is_empty());
        assert_eq!(headers.clone(), headers);
        assert!(format!("{headers:?}").contains("TraceHeaders"));
    }
}
