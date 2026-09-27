//! [`NodeRunInfo`] — which node of which run a message is about.
//!
//! Ported from the original, and from the two executor and broker structs
//! that claim to be the same message but are not.
//!
//! This is the most divergent type in the protocol. Three services model it,
//! no two agree, and the disagreements are not cosmetic — see the
//! protocol notes. What follows is why this type looks the way it does.
//!
//! # The fan-out index had two names, and neither was actually read
//!
//! original declared **`child_id`**. Both original services declared **`child_idx`**.
//! It is the same quantity: which child of a fan-out this message concerns.
//! This crate now writes and reads exactly one key, `child_index`, and the rest
//! of this section is why that costs nothing.
//!
//! It would be natural to assume each side read its own name and that a message
//! must therefore carry both. That is **not** what the sources say, and the
//! assumption was written into an earlier revision of this file before being
//! checked:
//!
//! - `child_id` appears exactly once in the original — the field declaration.
//!   `NodeRunInfo.from_noderun` never sets it, so the original emits
//!   `child_id: null` on every message.
//! - No original code touches `ChildIdx` outside the two struct definitions, so
//!   `child_idx` is a pure echo of whatever was unmarshalled — which, when
//!   original was the origin, is `0`.
//!
//! **The real carrier is the path.** the original appends the index as a
//! `:N` suffix (`child_tag = f":{child_idx}" if child_idx is not None else ""`)
//! and the original parses it back with `current_slug.partition(":")`.
//! That is the path on which the fan-out index actually survives a round trip
//! today, and it is modelled in [`pneuma_core::slug`], not here.
//!
//! So an earlier version of this type read both names and wrote both — cheap
//! fidelity rather than a load-bearing shim, since no live consumer read either
//! field. With both originals replaced there is one producer and one consumer,
//! and a second spelling would be inventing a disagreement rather than bridging
//! one. The protocol notes record the drift; this is where it ends.
//!
//! # Zero means absent
//!
//! original never emitted `nth`, `child_idx`, or `parent_index` on the wire, so
//! the original unmarshalled all three to `0` and re-emitted `0` regardless of the true
//! value. Anything with non-nullable integers does the same.
//! A `0` therefore does not mean "index zero" — these indices are 1-based —
//! it means "nobody told me".
//!
//! [`ChildIndex`] already exists for exactly this: it is a `NonZeroU32`, and its
//! own docs cite "the original sibling service's bare `int` field, which coerces a
//! missing `child_idx: null` to `0`" as a reason. So `0` and `null` and absent
//! all decode to `None` here, which is the one reading that is true of every
//! producer.
//!
//! Encoding omits them when absent rather than writing `0`. For an original consumer
//! the two are identical — a missing key unmarshals to the zero value — so
//! this is a smaller message with the same meaning.
//!
//! One obligation this leaves the caller: `nth`'s original default is **1**, not
//! absent. Decoding a missing
//! or zeroed `nth` to `None` is right for the wire, but anything rebuilding a
//! persisted `NodeRun` from this type must restore
//! [`SiblingIndex::FIRST`](pneuma_core::sibling_index::SiblingIndex::FIRST)
//! rather than writing `0` or `NULL`, which would be a silent corruption of
//! the original. [`pneuma_core::step`] already does this via its
//! `default_sibling_index`.
//!
//! # Signedness
//!
//! the original executor types the three indices as `int`;
//! the original broker types them as `uint`. A negative index deserializes in
//! one and fails in the other. This type is unsigned: a negative fan-out index
//! is meaningless, and the stricter of the two producers already rejects it, so
//! accepting one would be inventing a case that cannot validly occur.

use compact_str::CompactString;
use pneuma_core::child_index::ChildIndex;
use pneuma_core::ids::{NodeId, PipelineId, RunId};
use pneuma_core::node::NodeKind;
use pneuma_core::sibling_index::SiblingIndex;
use serde::ser::SerializeMap;
use serde::{Deserialize, Serialize, Serializer};

/// Identifies the node, and the run, a message concerns.
///
/// Unlike [`crate::meta::Meta`], the original is a plain `BaseModel`
/// with no `extra="allow"`, so unknown keys are dropped rather than preserved.
/// Serde's default behaviour is the same, which is why there is no catch-all
/// field here — the omission is parity, not an oversight.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(from = "NodeRunInfoWire")]
pub struct NodeRunInfo {
    /// The fully-qualified coordinate of this node within the run, including
    /// any fan-out suffix. Wire key `path`.
    pub path: CompactString,
    /// The node's id within its pipeline definition. Wire key `node_id`.
    pub node_id: NodeId,
    /// The component name — also the subject work is dispatched to, which is
    /// why losing it would leave the controller unable to route. Wire key
    /// `name`.
    pub name: CompactString,
    /// Wire key `kind`, where the originals wrote `type`.
    pub node_kind: NodeKind,
    /// Wire key `pipeline_id`.
    pub pipeline_id: PipelineId,
    /// Wire key `run_id`.
    pub run_id: RunId,
    /// The enclosing aggregator, if this node is nested. Wire key `parent_id`.
    pub parent_id: Option<NodeId>,
    /// Wire key `parent_path`.
    pub parent_path: Option<CompactString>,
    /// The enclosing aggregator's kind.
    ///
    /// A string rather than a [`NodeKind`], matching original, which types this
    /// as a bare `str` even though the sibling `type` field is a `NodeType`.
    /// Tightening it here would reject messages original accepts. Wire key
    /// `parent_kind`.
    pub parent_kind: Option<CompactString>,
    /// Which child of the enclosing fan-out this is.
    ///
    /// Wire key `child_index`. One key, read and written — it was `child_idx`
    /// from the original and `child_id` from original; see the module docs for why neither
    /// survived.
    pub child_index: Option<ChildIndex>,
    /// This node's static position among the components its parent declares.
    ///
    /// Wire key `sibling_index`, where the originals wrote `nth`. A
    /// [`SiblingIndex`], **not** a [`ChildIndex`] — see the module docs; the
    /// two are different indices and an earlier revision of this file gave them
    /// the same type.
    ///
    /// `None` means the wire did not carry it. That is not the same as
    /// "position zero", and it is also not the same as the original's default:
    /// anything rebuilding a persisted `NodeRun` from this must restore
    /// [`SiblingIndex::FIRST`], since original declares `nth: int = 1`.
    pub sibling_index: Option<SiblingIndex>,
    /// The enclosing aggregator's own child index. Wire key `parent_index`.
    pub parent_index: Option<ChildIndex>,
}

/// The wire key carrying the fan-out index.
///
/// One key. It was two — `child_id` from original, `child_idx` from the original — and this
/// crate wrote both; see the module docs.
const CHILD_INDEX_KEY: &str = "child_index";

/// Decodes a wire index, where `0`, `null`, and absent all mean "not set".
///
/// Both outcomes are reachable from real messages: the original emits `0` whenever the
/// value never reached it, and any producer may omit the key.
fn decode_index(raw: Option<u32>) -> Option<ChildIndex> {
    raw.and_then(|n| ChildIndex::new(n).ok())
}

/// Decodes `nth`, where `0`, `null`, and absent all mean "not carried".
///
/// Separate from [`decode_index`] only because the two indices are separate
/// types on purpose — see [`pneuma_core::sibling_index`].
fn decode_sibling_index(raw: Option<u32>) -> Option<SiblingIndex> {
    raw.and_then(|n| SiblingIndex::new(n).ok())
}

/// Treats the original's empty-string zero value as the absence it stands for.
///
/// original writes `null` for these; the original cannot, because it types them as bare
/// `string`. Mapping `""` to `None` is not normalising away information — an
/// empty parent path is not a parent — and it keeps a message that has passed
/// through the original comparing equal to the one that entered it.
fn decode_optional_str(raw: Option<CompactString>) -> Option<CompactString> {
    raw.filter(|s| !s.is_empty())
}

/// Mirrors [`decode_optional_str`] on the encode side, so an empty string a
/// caller built by hand does not survive as one.
fn encode_optional_str(value: Option<&str>) -> Option<&str> {
    value.filter(|s| !s.is_empty())
}

impl NodeRunInfo {
    /// Whether this node is nested inside an aggregator.
    pub fn is_nested(&self) -> bool {
        self.parent_id.is_some()
    }
}

/// The literal wire shape, used only for decoding.
///
/// Every optional field is `#[serde(default)]` because the three producers
/// disagree about which keys they write at all, and a missing key must decode
/// rather than fail.
#[derive(Deserialize)]
struct NodeRunInfoWire {
    path: CompactString,
    node_id: NodeId,
    name: CompactString,
    #[serde(rename = "kind")]
    node_kind: NodeKind,
    pipeline_id: PipelineId,
    run_id: RunId,
    /// Read as a plain string, not a [`NodeId`], so the original's empty-string zero
    /// value can be recognised as absence before it becomes an id. A
    /// `NodeId("")` would satisfy `is_nested()` and send the controller looking
    /// for a parent that does not exist.
    #[serde(default)]
    parent_id: Option<CompactString>,
    #[serde(default)]
    parent_path: Option<CompactString>,
    #[serde(default)]
    parent_kind: Option<CompactString>,
    /// The fan-out index. One key now: see [`NodeRunInfo`]'s module notes.
    #[serde(default)]
    child_index: Option<u32>,
    #[serde(default)]
    sibling_index: Option<u32>,
    #[serde(default)]
    parent_index: Option<u32>,
}

impl From<NodeRunInfoWire> for NodeRunInfo {
    fn from(wire: NodeRunInfoWire) -> Self {
        // One key, one read. It was two -- `child_idx` from the original, `child_id`
        // from original -- with a defined precedence and a fallback, because a
        // message that had been through a original hop carried one spelling and a
        // original-origin message the other (the protocol notes). With both
        // originals replaced there is one producer, so the precedence rule and
        // the zero-value fallback it needed are both gone.
        let child_index = decode_index(wire.child_index);

        NodeRunInfo {
            path: wire.path,
            node_id: wire.node_id,
            name: wire.name,
            node_kind: wire.node_kind,
            pipeline_id: wire.pipeline_id,
            run_id: wire.run_id,
            parent_id: decode_optional_str(wire.parent_id).map(NodeId::new),
            parent_path: decode_optional_str(wire.parent_path),
            parent_kind: decode_optional_str(wire.parent_kind),
            child_index,
            sibling_index: decode_sibling_index(wire.sibling_index),
            parent_index: decode_index(wire.parent_index),
        }
    }
}

impl Serialize for NodeRunInfo {
    /// Written by hand because the fan-out index goes out under two different
    /// keys, which no derive can express.
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(None)?;
        map.serialize_entry("path", &self.path)?;
        map.serialize_entry("node_id", &self.node_id)?;
        map.serialize_entry("name", &self.name)?;
        map.serialize_entry("kind", &self.node_kind)?;
        map.serialize_entry("pipeline_id", &self.pipeline_id)?;
        map.serialize_entry("run_id", &self.run_id)?;

        // Written even when absent, as `null`, matching original. the original unmarshals
        // a JSON null into its bare `string` as "", which is the same absence.
        // Normalised on the way out as well as in. Every field here is `pub`
        // and unvalidated, so `Some("")` is constructible by hand; without this
        // it would encode as `""` and decode back as `None`, breaking
        // `decode(encode(x)) == x` for a value the type permits.
        map.serialize_entry(
            "parent_id",
            &encode_optional_str(self.parent_id.as_ref().map(NodeId::as_str)),
        )?;
        map.serialize_entry(
            "parent_path",
            &encode_optional_str(self.parent_path.as_deref()),
        )?;
        map.serialize_entry(
            "parent_kind",
            &encode_optional_str(self.parent_kind.as_deref()),
        )?;

        // One name. It was two -- `child_id` for original, `child_idx` for the original --
        // because the two originals disagreed and this crate wrote both so
        // neither consumer lost the value (the protocol notes). With both
        // originals replaced there is one consumer and one spelling.
        if let Some(index) = self.child_index {
            map.serialize_entry(CHILD_INDEX_KEY, &index.get())?;
        }
        // Omitted rather than written as 0 when absent: for an original consumer a
        // missing key and a 0 are the same value.
        if let Some(index) = self.sibling_index {
            map.serialize_entry("sibling_index", &index.get())?;
        }
        if let Some(index) = self.parent_index {
            map.serialize_entry("parent_index", &index.get())?;
        }
        map.end()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> NodeRunInfo {
        NodeRunInfo {
            path: "invoice.page.default.A".into(),
            node_id: NodeId::new("A"),
            name: "extract".into(),
            node_kind: NodeKind::Model,
            pipeline_id: PipelineId::new("invoice.page.default"),
            run_id: RunId::new("run-1"),
            parent_id: None,
            parent_path: None,
            parent_kind: None,
            child_index: None,
            sibling_index: None,
            parent_index: None,
        }
    }

    /// A producer that omits an index it has no value for.
    fn wire_omitting_zeros() -> serde_json::Value {
        serde_json::json!({
            "path": "invoice.page.default.X.C:2",
            "node_id": "C",
            "name": "extract",
            "kind": "Model",
            "pipeline_id": "invoice.page.default",
            "run_id": "run-1",
            "parent_id": "X",
            "parent_path": "invoice.page.default.X",
            "parent_kind": "DictAggregator",
            "child_index": 2,
        })
    }

    /// A producer that writes `0` for an index it has no value for, which is
    /// what any language with non-nullable integers does.
    fn wire_zeroing_absent() -> serde_json::Value {
        serde_json::json!({
            "path": "invoice.page.default.X.C:2",
            "node_id": "C",
            "name": "extract",
            "kind": "Model",
            "pipeline_id": "invoice.page.default",
            "run_id": "run-1",
            "parent_id": "X",
            "parent_path": "invoice.page.default.X",
            "parent_kind": "DictAggregator",
            "child_index": 2,
            "sibling_index": 0,
            "parent_index": 0,
        })
    }

    #[test]
    fn reads_the_fan_out_index() -> Result<(), serde_json::Error> {
        let info: NodeRunInfo = serde_json::from_value(wire_omitting_zeros())?;
        assert_eq!(info.child_index, ChildIndex::new(2).ok());
        Ok(())
    }

    #[test]
    fn omitting_an_index_and_zeroing_it_decode_the_same() -> Result<(), serde_json::Error> {
        // The two ways a producer can say "I have no value here" must be
        // indistinguishable once decoded, because which one arrives depends on
        // nothing more than the language the producer was written in.
        let omitted: NodeRunInfo = serde_json::from_value(wire_omitting_zeros())?;
        let zeroed: NodeRunInfo = serde_json::from_value(wire_zeroing_absent())?;
        assert_eq!(omitted, zeroed);
        Ok(())
    }

    #[test]
    fn the_child_index_has_one_name() -> Result<(), serde_json::Error> {
        // It had two -- `child_id` for original, `child_idx` for the original -- and this
        // crate wrote both so neither consumer lost the value
        // (the protocol notes). With both originals replaced there is one
        // consumer and one spelling, and writing a second would be inventing a
        // disagreement that no longer exists.
        let info: NodeRunInfo = serde_json::from_value(wire_zeroing_absent())?;
        let json = serde_json::to_value(&info)?;
        assert_eq!(json[CHILD_INDEX_KEY], 2);
        for gone in ["child_id", "child_idx"] {
            assert!(json.get(gone).is_none(), "{gone} is not written any more");
        }
        Ok(())
    }

    #[test]
    fn an_unknown_spelling_of_the_index_is_not_read() -> Result<(), serde_json::Error> {
        // The precedence rule that used to live here is gone with the second
        // spelling. What replaces it is the guarantee that made dropping it
        // safe: an old producer's key is ignored, not silently honoured, so a
        // service that was never rebuilt fails visibly rather than half-works.
        let mut raw = wire_omitting_zeros();
        raw["child_index"] = serde_json::Value::Null;
        raw["child_idx"] = serde_json::json!(9);
        raw["child_id"] = serde_json::json!(9);
        let info: NodeRunInfo = serde_json::from_value(raw)?;
        assert_eq!(info.child_index, None);
        Ok(())
    }

    #[test]
    fn a_zero_index_means_absent_not_index_zero() -> Result<(), serde_json::Error> {
        // the original writes 0 for every index it was never told, and these indices are
        // 1-based, so 0 can only mean "not set".
        let info: NodeRunInfo = serde_json::from_value(wire_zeroing_absent())?;
        assert_eq!(info.sibling_index, None);
        assert_eq!(info.parent_index, None);
        Ok(())
    }

    #[test]
    fn a_null_index_means_absent() -> Result<(), serde_json::Error> {
        let mut raw = wire_omitting_zeros();
        raw["child_index"] = serde_json::Value::Null;
        let info: NodeRunInfo = serde_json::from_value(raw)?;
        assert_eq!(info.child_index, None);
        Ok(())
    }

    #[test]
    fn absent_index_keys_decode_rather_than_fail() -> Result<(), serde_json::Error> {
        // A producer omits the three index keys entirely when they are absent.
        let info: NodeRunInfo = serde_json::from_value(wire_omitting_zeros())?;
        assert_eq!(info.sibling_index, None);
        assert_eq!(info.parent_index, None);
        Ok(())
    }

    #[test]
    fn absent_indices_are_omitted_rather_than_written_as_zero() -> Result<(), serde_json::Error> {
        let json = serde_json::to_value(sample())?;
        for key in [CHILD_INDEX_KEY, "sibling_index", "parent_index"] {
            assert!(json.get(key).is_none(), "{key} should be omitted");
        }
        Ok(())
    }

    #[test]
    fn present_indices_are_written() -> Result<(), serde_json::Error> {
        let mut info = sample();
        info.child_index = ChildIndex::new(2).ok();
        info.sibling_index = SiblingIndex::new(3).ok();
        info.parent_index = ChildIndex::new(4).ok();
        let json = serde_json::to_value(&info)?;
        assert_eq!(json[CHILD_INDEX_KEY], 2);
        assert_eq!(json["sibling_index"], 3);
        assert_eq!(json["parent_index"], 4);
        Ok(())
    }

    #[test]
    fn gos_empty_strings_decode_as_the_absence_they_stand_for() -> Result<(), serde_json::Error> {
        // the original types parent_path/parent_kind as bare `string`, so it writes ""
        // where original writes null.
        let mut raw = wire_zeroing_absent();
        raw["parent_path"] = serde_json::json!("");
        raw["parent_kind"] = serde_json::json!("");
        let info: NodeRunInfo = serde_json::from_value(raw)?;
        assert_eq!(info.parent_path, None);
        assert_eq!(info.parent_kind, None);
        Ok(())
    }

    #[test]
    fn gos_empty_parent_id_does_not_become_a_phantom_parent() -> Result<(), serde_json::Error> {
        // the original types parent_id as a bare `string` too, so a top-level node comes
        // back with "" rather than null. Turning that into NodeId("") would
        // satisfy is_nested() and send the controller hunting for a parent that
        // does not exist.
        let mut raw = wire_zeroing_absent();
        raw["parent_id"] = serde_json::json!("");
        raw["parent_path"] = serde_json::json!("");
        raw["parent_kind"] = serde_json::json!("");
        let info: NodeRunInfo = serde_json::from_value(raw)?;
        assert_eq!(info.parent_id, None);
        assert!(!info.is_nested(), "an empty parent_id is not a parent");
        Ok(())
    }

    #[test]
    fn absent_parents_are_written_as_null_matching_reference() -> Result<(), serde_json::Error> {
        let json = serde_json::to_value(sample())?;
        assert!(json["parent_id"].is_null());
        assert!(json["parent_path"].is_null());
        assert!(json["parent_kind"].is_null());
        Ok(())
    }

    #[test]
    fn parent_type_accepts_a_value_node_type_would_reject() -> Result<(), serde_json::Error> {
        // original types parent_kind as a bare `str`, not NodeType, so tightening
        // it here would reject messages the original accepts.
        let mut raw = wire_zeroing_absent();
        raw["parent_kind"] = serde_json::json!("SomethingElse");
        let info: NodeRunInfo = serde_json::from_value(raw)?;
        assert_eq!(info.parent_kind.as_deref(), Some("SomethingElse"));
        Ok(())
    }

    #[test]
    fn an_unknown_node_type_is_rejected() {
        let mut raw = wire_zeroing_absent();
        raw["kind"] = serde_json::json!("Telepathy");
        assert!(serde_json::from_value::<NodeRunInfo>(raw).is_err());
    }

    #[test]
    fn a_negative_index_is_rejected() {
        // broker types these as uint, so it already rejects this; being
        // the stricter of the two producers is deliberate.
        let mut raw = wire_zeroing_absent();
        raw["child_index"] = serde_json::json!(-1);
        assert!(serde_json::from_value::<NodeRunInfo>(raw).is_err());
    }

    #[test]
    fn missing_required_fields_are_rejected() {
        for omit in ["path", "node_id", "name", "kind", "pipeline_id", "run_id"] {
            let mut raw = wire_zeroing_absent();
            let removed = raw.as_object_mut().and_then(|o| o.remove(omit));
            assert!(removed.is_some(), "{omit} should have been present");
            assert!(
                serde_json::from_value::<NodeRunInfo>(raw).is_err(),
                "omitting {omit} should fail"
            );
        }
    }

    #[test]
    fn unknown_keys_are_ignored_matching_the_closed_reference_model(
    ) -> Result<(), serde_json::Error> {
        let mut raw = wire_zeroing_absent();
        raw["headers"] = serde_json::json!({"traceparent": "x"});
        let info: NodeRunInfo = serde_json::from_value(raw)?;
        assert_eq!(info.node_id.as_str(), "C");
        Ok(())
    }

    #[test]
    fn a_hand_built_empty_string_still_round_trips() -> Result<(), serde_json::Error> {
        // Every field is pub and unvalidated, so Some("") is constructible.
        // Without normalising on encode it would go out as "" and come back as
        // None, so a value the type permits would not survive its own round
        // trip. The protocol notes treats that invariant as load-bearing.
        let mut info = sample();
        info.parent_id = Some(NodeId::new(""));
        info.parent_path = Some("".into());
        info.parent_kind = Some("".into());

        let text = serde_json::to_string(&info)?;
        let back: NodeRunInfo = serde_json::from_str(&text)?;

        assert_eq!(back.parent_id, None);
        assert_eq!(back.parent_path, None);
        assert_eq!(back.parent_kind, None);
        // And it is now a fixed point: encoding the decoded value agrees.
        assert_eq!(serde_json::to_string(&back)?, text);
        Ok(())
    }

    #[test]
    fn round_trips_through_text() -> Result<(), serde_json::Error> {
        let mut info: NodeRunInfo = serde_json::from_value(wire_zeroing_absent())?;
        info.sibling_index = SiblingIndex::new(3).ok();
        info.parent_index = ChildIndex::new(4).ok();
        let text = serde_json::to_string(&info)?;
        let back: NodeRunInfo = serde_json::from_str(&text)?;
        assert_eq!(info, back);
        Ok(())
    }

    #[test]
    fn a_message_survives_being_relayed() -> Result<(), serde_json::Error> {
        // This was a *simulated original hop*: original wrote `child_id`, a original service
        // read it into a struct that only knew `child_index`, and wrote
        // `child_index` back -- so the value was lost at the first step, and this
        // type existed partly to absorb that (the protocol notes). With one
        // spelling there is no drift left to absorb, and what remains worth
        // asserting is the plainer property: a node relayed by any hop that
        // decodes and re-encodes comes back the same.
        let original: NodeRunInfo = serde_json::from_value(wire_zeroing_absent())?;
        let relayed: NodeRunInfo = serde_json::from_value(serde_json::to_value(&original)?)?;
        assert_eq!(relayed, original);

        // And the same for a node carrying no indices at all, which is the
        // ordinary case: a top-level step of an unnested pipeline.
        let bare: NodeRunInfo = serde_json::from_value(wire_omitting_zeros())?;
        let relayed: NodeRunInfo = serde_json::from_value(serde_json::to_value(&bare)?)?;
        assert_eq!(relayed, bare);
        Ok(())
    }

    #[test]
    fn is_nested_reports_whether_a_parent_is_set() -> Result<(), serde_json::Error> {
        assert!(!sample().is_nested());
        let nested: NodeRunInfo = serde_json::from_value(wire_zeroing_absent())?;
        assert!(nested.is_nested());
        Ok(())
    }

    #[test]
    fn debug_and_clone_are_available() {
        let info = sample();
        assert_eq!(info.clone(), info);
        assert!(format!("{info:?}").contains("extract"));
    }
}
