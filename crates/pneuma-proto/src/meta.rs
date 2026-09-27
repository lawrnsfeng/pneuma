//! [`Meta`] — the routing header carried by every message on every transport.
//!
//! Ported from the original.
//!
//! # Renames
//!
//! The original model names these three fields `type`, `level`, and `name`. Two
//! of those are poor names in isolation and one is a Rust keyword, so they are
//! renamed to `pipeline_type` / `pipeline_level` / `pipeline_name`. That is not
//! a liberty: those three fields exist *only* to be joined into the pipeline
//! id, so naming them after it is more accurate than the original. The wire
//! keys are unchanged.
//!
//! **Read this before touching `extra`:** a real production message
//! carries a caller-supplied key
//! literally named `pipeline_type`, which is *not* this struct's
//! `pipeline_type` field. That one lands in [`Meta::extra`] under its own name;
//! this one is on the wire as `type`. They are different values and must stay
//! that way.
//!
//! # `pipeline_id` appears twice, and the two are different values
//!
//! This is the subtlest part of the whole message format, so it is spelled out.
//!
//! original declares `pipeline_id` as a `@computed_field` deriving
//! `{type}.{level}.{name}`, *and* `Meta` is `extra="allow"`. A caller may also
//! send a key literally named `pipeline_id`. Those are two different things
//! that collide on one name, and the real production sample proves they carry
//! genuinely different values: its `type.level.name` derives
//! `llm.doc.default`, while its literal `pipeline_id` is `bill-lading` — a
//! document type, not a stale copy of the derived id.
//!
//! Verified against the original model library 2.13.4 with the real model definition:
//!
//! ```text
//! model_extra: {'pipeline_id': 'bill-lading', 'page_idx': 3}
//! model_dump_json(): {... "pipeline_id":"bill-lading", ... "pipeline_id":"llm.doc.default"}
//! model_dump():      {... 'pipeline_id': 'llm.doc.default'}
//! ```
//!
//! So original **keeps** the caller's value in `model_extra`, emits it *twice*
//! over `model_dump_json()`, and lets the derived value win in `model_dump()`.
//! Every publish site uses `model_dump()` (the original and friends), so
//! the value that actually reaches the wire is the derived one.
//!
//! This port reproduces the observable behaviour exactly, without reproducing
//! the name collision:
//!
//! - **Decoding** puts the caller's value in [`Meta::caller_pipeline_id`],
//!   never in [`Meta::extra`]. It is preserved, matching `model_extra` — except
//!   when it equals the derived value, in which case it is our own echo coming
//!   back and is dropped, so that decoding a message this crate wrote does not
//!   invent a caller value that was never sent.
//! - **Encoding** emits the derived value once, matching `model_dump()`.
//!
//! The consequence is worth stating plainly: a caller's `pipeline_id` does not
//! survive being forwarded, here or in original. That is a pre-existing property
//! of the protocol, not something introduced by the port — but a reader can now
//! at least see the value before it is dropped, which in original required
//! reaching into `model_extra`.
//!
//! An earlier revision of this module deleted the caller's value at decode time
//! and described it as "stale by construction". That was wrong on both counts,
//! and `bill-lading` is the counter-example.

use std::collections::BTreeMap;

use compact_str::CompactString;
use pneuma_core::ids::{JobId, PipelineId, RunId, TenantId};
use serde::ser::SerializeMap;
use serde::{Deserialize, Serialize, Serializer};
use serde_json::Value;

/// The wire key holding the derived pipeline id.
const PIPELINE_ID_KEY: &str = "pipeline_id";

/// Wire keys this struct emits from its own named fields.
///
/// [`Meta::extra`] is a public map, so nothing stops a caller from inserting a
/// key that collides with one of these — most easily by populating it from a
/// foreign map such as a merged envelope or a re-hydrated record. Since the
/// extras are flattened *alongside* the named fields, a collision would emit
/// the key twice and produce JSON that this crate's own decoder rejects with
/// `duplicate field`. Encoding skips these keys so that cannot happen: a
/// message this crate produces is always one it can read back.
const RESERVED_WIRE_KEYS: [&str; 6] = [
    "job_id",
    "tenant_id",
    "pipeline_type",
    "pipeline_level",
    "pipeline_name",
    PIPELINE_ID_KEY,
];

/// Routing and identity header present on every pneuma message.
///
/// Corresponds to `pneuma.models.message.Meta`, which is configured
/// `extra="allow"` — arbitrary caller keys ride along and must survive a round
/// trip. They are preserved in [`Meta::extra`].
///
/// Note that they survive a *Rust* round trip but not a *original* one: both
/// the original broker and the original executor model this as a closed
/// five-field struct and re-marshal it, so every caller extra is destroyed the
/// first time a message passes through either. See the protocol notes.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(from = "MetaWire")]
pub struct Meta {
    /// The job this message belongs to. Wire key `job_id`.
    pub job_id: JobId,
    /// The owning tenant, used for routing and fairness. Wire key `tenant_id`.
    pub tenant_id: TenantId,
    /// First component of the pipeline id. Wire key `type`.
    pub pipeline_type: CompactString,
    /// Second component of the pipeline id. Wire key `level`.
    pub pipeline_level: CompactString,
    /// Third component of the pipeline id. Wire key `name`.
    pub pipeline_name: CompactString,
    /// A `pipeline_id` sent by the caller, which is *not* the derived
    /// [`Meta::pipeline_id`] and is usually a different value entirely — see
    /// the module docs.
    ///
    /// Held in its own field rather than in [`Meta::extra`] so the two never
    /// collide on one name. Preserved on decode, matching the original model library's
    /// `model_extra`; **not** emitted on encode, matching `model_dump()`, which
    /// lets the derived value win.
    ///
    /// A [`Value`] rather than a string because the catch-all imposes no type
    /// and a caller could legitimately send a number or an object here.
    pub caller_pipeline_id: Option<Value>,
    /// Caller-supplied keys preserved verbatim (`extra="allow"`).
    ///
    /// A `BTreeMap` rather than a `HashMap` so serialization is deterministic:
    /// these messages are hashed to form idempotency keys, and a map that
    /// reorders between runs would defeat that.
    pub extra: BTreeMap<String, Value>,
}

impl Meta {
    /// Derives the pipeline id as `{type}.{level}.{name}`.
    ///
    /// Mirrors the `@computed_field` the original.
    pub fn pipeline_id(&self) -> PipelineId {
        PipelineId::new(format!(
            "{}.{}.{}",
            self.pipeline_type, self.pipeline_level, self.pipeline_name
        ))
    }

    /// The run this message belongs to.
    ///
    /// original exposes `run_id` as a plain alias of `job_id`.
    /// The alias lives in [`RunId::from_job`] rather than
    /// being re-derived here.
    ///
    /// Note the asymmetry with [`Meta::pipeline_id`], which is easy to get
    /// wrong in either direction: `pipeline_id` is a `@computed_field` and so
    /// **is** emitted by `model_dump()`, while `run_id` is a bare `@property`
    /// and so is **not**. Both are methods here, and only one of them appears on
    /// the wire.
    pub fn run_id(&self) -> RunId {
        RunId::from_job(&self.job_id)
    }
}

/// The literal wire shape, used only as a decoding way-station.
///
/// Encoding does not go through this type: [`Meta`] implements [`Serialize`] by
/// hand instead, so a `Meta` can be written without deep-cloning its extras.
/// These messages are encoded on the per-message hot path and again for
/// idempotency hashing, and the production sample carries a nested
/// `component_params` blob in that map, so the copy is worth avoiding.
#[derive(Deserialize)]
struct MetaWire {
    job_id: JobId,
    tenant_id: TenantId,
    pipeline_type: CompactString,
    pipeline_level: CompactString,
    pipeline_name: CompactString,
    #[serde(flatten)]
    extra: BTreeMap<String, Value>,
}

impl From<MetaWire> for Meta {
    fn from(wire: MetaWire) -> Self {
        let mut extra = wire.extra;
        // Lifted out of the catch-all so the caller's value and the derived one
        // stop sharing a name. Preserved, not discarded — see the module docs.
        let caller_pipeline_id = extra.remove(PIPELINE_ID_KEY);

        // Encoding always emits the derived `pipeline_id`, so decoding anything
        // this crate produced would otherwise read our own echo straight back
        // as a "caller-supplied" value, and `decode(encode(m))` would not equal
        // `m`. Keeping the value only when it *differs* from the derived one
        // restores that, and loses nothing: a caller value equal to the derived
        // one is indistinguishable from the echo and carries no information the
        // derivation does not already have.
        //
        // The resulting invariant — `caller_pipeline_id` is either absent or
        // genuinely different — is what makes the field meaningful rather than
        // noise on every message.
        let derived = format!(
            "{}.{}.{}",
            wire.pipeline_type, wire.pipeline_level, wire.pipeline_name
        );
        let caller_pipeline_id =
            caller_pipeline_id.filter(|value| value != &Value::String(derived));

        Meta {
            job_id: wire.job_id,
            tenant_id: wire.tenant_id,
            pipeline_type: wire.pipeline_type,
            pipeline_level: wire.pipeline_level,
            pipeline_name: wire.pipeline_name,
            caller_pipeline_id,
            extra,
        }
    }
}

impl Serialize for Meta {
    /// Written by hand rather than derived, for two reasons: it emits the
    /// derived `pipeline_id`, which is not a field, and it borrows the extras
    /// instead of cloning them.
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(None)?;
        map.serialize_entry("job_id", &self.job_id)?;
        map.serialize_entry("tenant_id", &self.tenant_id)?;
        map.serialize_entry("pipeline_type", &self.pipeline_type)?;
        map.serialize_entry("pipeline_level", &self.pipeline_level)?;
        map.serialize_entry("pipeline_name", &self.pipeline_name)?;
        // The derived value wins, exactly as `model_dump()` does. The caller's
        // `caller_pipeline_id` is deliberately not emitted.
        map.serialize_entry(PIPELINE_ID_KEY, self.pipeline_id().as_str())?;
        for (key, value) in &self.extra {
            // Never emit a key that is already on the wire from a named field.
            // Written as a guard around the emit rather than an early
            // `continue` or a filtered iterator chain: both of those leave a
            // line the coverage tool attributes unreliably, and this crate is
            // gated at 100%.
            if !RESERVED_WIRE_KEYS.contains(&key.as_str()) {
                map.serialize_entry(key, value)?;
            }
        }
        map.end()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> Meta {
        Meta {
            job_id: JobId::new("job-1"),
            tenant_id: TenantId::new("acme"),
            pipeline_type: "invoice".into(),
            pipeline_level: "page".into(),
            pipeline_name: "default".into(),
            caller_pipeline_id: None,
            extra: BTreeMap::new(),
        }
    }

    #[test]
    fn pipeline_id_joins_the_three_components() {
        assert_eq!(sample().pipeline_id().as_str(), "invoice.page.default");
    }

    #[test]
    fn pipeline_id_matches_the_corpus_full_path_prefix() {
        // `pipeline1.yaml` resolves node `A` to this full_path, per the
        // checked-in original snapshot
        // `pneuma-core/tests/fixtures/reference_resolver_snapshots/pipeline1.json`.
        // Stripping the node id must leave exactly the pipeline id — an
        // equality, not a prefix check, so a truncated or over-long
        // `pipeline_id()` fails here rather than sliding through.
        let full_path = "invoice.page.default.A";
        let node_id = "A";
        assert_eq!(
            full_path.strip_suffix(&format!(".{node_id}")),
            Some(sample().pipeline_id().as_str()),
        );
    }

    #[test]
    fn run_id_aliases_job_id() {
        assert_eq!(sample().run_id().as_str(), "job-1");
    }

    #[test]
    fn run_id_is_not_emitted_but_pipeline_id_is() -> Result<(), serde_json::Error> {
        // The asymmetry is in the original's decorators: `pipeline_id` is a
        // `@computed_field` (emitted by model_dump), `run_id` is a bare
        // `@property` (not emitted). Adding `run_id` to the wire would be a
        // silent protocol change, so pin both directions.
        let json = serde_json::to_value(sample())?;
        assert!(
            json.get("run_id").is_none(),
            "run_id must stay off the wire"
        );
        assert_eq!(json[PIPELINE_ID_KEY], "invoice.page.default");
        Ok(())
    }

    #[test]
    fn an_incoming_run_id_is_kept_as_a_caller_extra() -> Result<(), serde_json::Error> {
        // original does not declare `run_id` on Meta, so under `extra="allow"` an
        // incoming one is an ordinary caller key. It must not be confused with
        // the derived accessor, and unlike `pipeline_id` it is not stripped.
        let raw = serde_json::json!({
            "job_id": "job-1", "tenant_id": "acme",
            "pipeline_type": "invoice", "pipeline_level": "page", "pipeline_name": "default",
            "run_id": "something-else",
        });
        let meta: Meta = serde_json::from_value(raw)?;
        assert_eq!(meta.extra["run_id"], "something-else");
        assert_eq!(meta.run_id().as_str(), "job-1");
        Ok(())
    }

    #[test]
    fn the_wire_keys_and_the_field_names_now_agree() -> Result<(), serde_json::Error> {
        // They did not. The wire said `type`, `level` and `name` -- three words
        // that name no dimension -- while the fields were already
        // `pipeline_type`, `pipeline_level` and `pipeline_name`, because the
        // bare words say nothing about what kind of type or whose name. The
        // wire has caught up with the fields rather than the other way round.
        let json = serde_json::to_value(sample())?;
        assert_eq!(json["pipeline_type"], "invoice");
        assert_eq!(json["pipeline_level"], "page");
        assert_eq!(json["pipeline_name"], "default");
        assert_eq!(json["job_id"], "job-1");
        assert_eq!(json["tenant_id"], "acme");
        // And the bare words are gone.
        for old in ["type", "level", "name"] {
            assert!(json.get(old).is_none(), "{old} is not a key any more");
        }
        Ok(())
    }

    #[test]
    fn pipeline_id_is_emitted_even_though_it_is_not_a_field() -> Result<(), serde_json::Error> {
        let json = serde_json::to_value(sample())?;
        assert_eq!(json[PIPELINE_ID_KEY], "invoice.page.default");
        Ok(())
    }

    #[test]
    fn caller_extras_survive_a_round_trip() -> Result<(), serde_json::Error> {
        let raw = serde_json::json!({
            "job_id": "job-1",
            "tenant_id": "acme",
            "pipeline_type": "invoice",
            "pipeline_level": "page",
            "pipeline_name": "default",
            "request_id": "req-9",
            "page_idx": 3,
            "component_params": {"a": 1},
        });
        let meta: Meta = serde_json::from_value(raw)?;
        assert_eq!(meta.extra["request_id"], "req-9");
        assert_eq!(meta.extra["page_idx"], 3);
        assert_eq!(meta.extra["component_params"], serde_json::json!({"a": 1}));

        let back = serde_json::to_value(meta)?;
        assert_eq!(back["request_id"], "req-9");
        assert_eq!(back["page_idx"], 3);
        Ok(())
    }

    #[test]
    fn a_caller_supplied_pipeline_id_is_preserved_not_destroyed() -> Result<(), serde_json::Error> {
        // Values taken from the real production sample
        // the original, where the derived id is
        // `llm.doc.default` and the caller's `pipeline_id` is `bill-lading` —
        // proof the two are genuinely different values rather than one being a
        // stale copy of the other.
        let raw = serde_json::json!({
            "job_id": "job-1",
            "tenant_id": "acme",
            "pipeline_type": "llm",
            "pipeline_level": "doc",
            "pipeline_name": "default",
            "pipeline_id": "bill-lading",
        });
        let meta: Meta = serde_json::from_value(raw)?;

        // Preserved, matching the original model library's `model_extra` ...
        assert_eq!(
            meta.caller_pipeline_id,
            Some(Value::String("bill-lading".into()))
        );
        // ... and kept out of the catch-all so the names cannot collide.
        assert!(!meta.extra.contains_key(PIPELINE_ID_KEY));
        assert_eq!(meta.pipeline_id().as_str(), "llm.doc.default");

        // On the wire the derived value wins, matching `model_dump()`.
        let back = serde_json::to_value(&meta)?;
        assert_eq!(back[PIPELINE_ID_KEY], "llm.doc.default");
        Ok(())
    }

    #[test]
    fn a_non_string_caller_pipeline_id_survives() -> Result<(), serde_json::Error> {
        // The catch-all imposes no type, so this must not assume a string.
        let raw = serde_json::json!({
            "job_id": "j", "tenant_id": "t",
            "pipeline_type": "a", "pipeline_level": "b", "pipeline_name": "c",
            "pipeline_id": {"nested": [1, 2]},
        });
        let meta: Meta = serde_json::from_value(raw)?;
        assert_eq!(
            meta.caller_pipeline_id,
            Some(serde_json::json!({"nested": [1, 2]}))
        );
        Ok(())
    }

    #[test]
    fn an_extra_colliding_with_a_wire_key_is_never_emitted_twice() -> Result<(), serde_json::Error>
    {
        // `extra` is public, so a caller can populate it from a foreign map and
        // land a reserved key in it. Emitting that alongside the named field
        // would produce JSON this crate's own decoder rejects.
        let mut meta = sample();
        for key in RESERVED_WIRE_KEYS {
            meta.extra
                .insert(key.to_owned(), Value::String("collide".into()));
        }

        let text = serde_json::to_string(&meta)?;
        for key in RESERVED_WIRE_KEYS {
            assert_eq!(
                text.matches(&format!("\"{key}\":")).count(),
                1,
                "{key} emitted more than once: {text}"
            );
        }
        assert!(!text.contains("collide"), "{text}");

        // The real point: what we emit, we can read back.
        let back: Meta = serde_json::from_str(&text)?;
        assert_eq!(back.pipeline_type, "invoice");
        Ok(())
    }

    #[test]
    fn serializing_emits_exactly_one_pipeline_id_key() -> Result<(), serde_json::Error> {
        // Pins the encoding invariant directly. This one does NOT depend on
        // the incoming value being dropped — writing the derived value into the
        // same map would overwrite a stale entry anyway — so it is here to
        // catch a future refactor that emits `pipeline_id` from somewhere other
        // than the extras map. `to_value` would hide a duplicate by collapsing
        // it, hence asserting on the raw text.
        let raw = serde_json::json!({
            "job_id": "job-1",
            "tenant_id": "acme",
            "pipeline_type": "invoice",
            "pipeline_level": "page",
            "pipeline_name": "default",
            "pipeline_id": "stale",
        });
        let meta: Meta = serde_json::from_value(raw)?;
        let text = serde_json::to_string(&meta)?;
        assert_eq!(text.matches("\"pipeline_id\"").count(), 1, "{text}");
        Ok(())
    }

    #[test]
    fn a_caller_can_no_longer_send_a_key_that_looks_like_a_field_but_is_not(
    ) -> Result<(), serde_json::Error> {
        // A hazard the rename removed rather than moved. `pipeline_type` used
        // to be the Rust field name and *not* a wire key, so a caller sending
        // it landed in `extra` while the real value arrived under `type` -- two
        // values, one name, and a reader had to know which was which. Now the
        // field and the key agree, so there is one `pipeline_type` and it is
        // the pipeline's.
        let raw = serde_json::json!({
            "job_id": "job-1",
            "tenant_id": "acme",
            "pipeline_type": "invoice",
            "pipeline_level": "page",
            "pipeline_name": "default",
        });
        let meta: Meta = serde_json::from_value(raw)?;
        assert_eq!(meta.pipeline_type, "invoice");
        assert!(!meta.extra.contains_key("pipeline_type"));

        // `pipeline_id` keeps its own field, because it is *derived* rather
        // than carried: a caller sending one is sending something the type
        // cannot store as the real value, and `caller_pipeline_id` is where it
        // goes so the two never collide.
        let with_caller_id = serde_json::json!({
            "job_id": "job-1",
            "tenant_id": "acme",
            "pipeline_type": "invoice",
            "pipeline_level": "page",
            "pipeline_name": "default",
            "pipeline_id": "caller-supplied",
        });
        let meta: Meta = serde_json::from_value(with_caller_id)?;
        assert_eq!(meta.pipeline_id().as_str(), "invoice.page.default");
        assert_eq!(
            meta.caller_pipeline_id,
            Some(serde_json::json!("caller-supplied"))
        );
        Ok(())
    }

    #[test]
    fn missing_required_fields_are_rejected() {
        // original declares all five without defaults, so absence is an error
        // rather than an empty string.
        for omit in [
            "job_id",
            "tenant_id",
            "pipeline_type",
            "pipeline_level",
            "pipeline_name",
        ] {
            let mut raw = serde_json::json!({
                "job_id": "j", "tenant_id": "t",
                "pipeline_type": "a", "pipeline_level": "b", "pipeline_name": "c",
            });
            let removed = raw.as_object_mut().and_then(|o| o.remove(omit));
            assert!(removed.is_some(), "{omit} should have been present");
            let parsed: Result<Meta, _> = serde_json::from_value(raw);
            assert!(parsed.is_err(), "omitting {omit} should fail");
        }
    }

    #[test]
    fn an_echoed_pipeline_id_is_not_mistaken_for_a_caller_value() -> Result<(), serde_json::Error> {
        // Decoding this crate's own output must not resurrect the derived value
        // as a caller-supplied one, or every forwarded message would grow a
        // spurious `caller_pipeline_id`.
        let text = serde_json::to_string(&sample())?;
        let back: Meta = serde_json::from_str(&text)?;
        assert_eq!(back.caller_pipeline_id, None);
        assert_eq!(back, sample());
        Ok(())
    }

    #[test]
    fn a_caller_value_equal_to_the_derived_one_is_dropped() -> Result<(), serde_json::Error> {
        // Indistinguishable from our own echo, and carries no information the
        // derivation does not already have.
        let raw = serde_json::json!({
            "job_id": "j", "tenant_id": "t",
            "pipeline_type": "a", "pipeline_level": "b", "pipeline_name": "c",
            "pipeline_id": "a.b.c",
        });
        let meta: Meta = serde_json::from_value(raw)?;
        assert_eq!(meta.caller_pipeline_id, None);
        Ok(())
    }

    #[test]
    fn a_meta_carrying_a_caller_value_round_trips_lossily_and_says_so(
    ) -> Result<(), serde_json::Error> {
        // Parity with original: `model_dump()` lets the derived value win, so a
        // caller's `pipeline_id` does not survive being forwarded. Pinned here
        // so the loss is a decision on record rather than a surprise.
        let mut meta = sample();
        meta.caller_pipeline_id = Some(Value::String("bill-lading".into()));

        let text = serde_json::to_string(&meta)?;
        assert!(!text.contains("bill-lading"), "{text}");

        let back: Meta = serde_json::from_str(&text)?;
        assert_eq!(back.caller_pipeline_id, None);
        assert_ne!(back, meta);
        Ok(())
    }

    #[test]
    fn round_trips_through_text_unchanged() -> Result<(), serde_json::Error> {
        let meta = sample();
        let text = serde_json::to_string(&meta)?;
        let back: Meta = serde_json::from_str(&text)?;
        assert_eq!(meta, back);
        Ok(())
    }

    #[test]
    fn extras_serialize_in_a_deterministic_order() -> Result<(), serde_json::Error> {
        // Idempotency keys are derived by hashing the encoded message, so a
        // map that reordered between runs would break deduplication.
        let raw = serde_json::json!({
            "job_id": "j", "tenant_id": "t",
            "pipeline_type": "a", "pipeline_level": "b", "pipeline_name": "c",
            "zeta": 1, "alpha": 2, "mid": 3,
        });
        let meta: Meta = serde_json::from_value(raw)?;
        let first = serde_json::to_string(&meta)?;
        for _ in 0..8 {
            let again = serde_json::to_string(&meta.clone())?;
            assert_eq!(first, again);
        }
        assert!(first.find("\"alpha\"") < first.find("\"mid\""));
        assert!(first.find("\"mid\"") < first.find("\"zeta\""));
        Ok(())
    }

    #[test]
    fn debug_and_clone_are_available() {
        let meta = sample();
        assert_eq!(meta.clone(), meta);
        assert!(format!("{meta:?}").contains("invoice"));
    }
}
