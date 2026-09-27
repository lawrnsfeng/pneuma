//! The non-empty set of nodes a pipeline or aggregator starts from.
//!
//! # Why non-empty by construction
//!
//! The original stores `start` as a plain dict and raises
//! `ValueError("aggregator node has no start directive")` when a caller asks
//! for `start_ids` on an empty one. [`StartSet`]
//! keeps a mandatory `head`, so that error has no way to occur.
//!
//! # Why the values are preserved but not exposed
//!
//! The wire form is either a bare string (`start: A`, coerced by
//! `EnsuredStrDict` the original to `{A: A}`) or a map
//! (`start: {C: key_C, D: key_D}`). Only the **keys** are ever read: the sole
//! accessor is `start_ids = list(self.start.keys())`, and all eight of its
//! call sites use it. The values are nonetheless retained here, because a
//! `Pipeline` deserialised from storage may be written back, and silently
//! dropping them would corrupt the stored definition.
//!
//! # Why order matters
//!
//! `init_next_step` takes `step.start_ids[0]` for a `ListAggregator`
//! — the *first* entry specifically. original dicts are
//! insertion-ordered, so the wire order is significant and is preserved here.

use compact_str::CompactString;
use serde::{
    de::{MapAccess, Visitor},
    ser::SerializeMap,
    Deserialize, Deserializer, Serialize, Serializer,
};

use crate::ids::NodeId;

/// One `start` entry: the node to start from, and its join-key name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StartEntry {
    /// The node to start from — the map key on the wire.
    pub node_id: NodeId,
    /// The associated join-key name — the map value on the wire. Retained
    /// for round-trip fidelity; nothing reads it today.
    pub key: CompactString,
}

/// A non-empty, ordered set of start entries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StartSet {
    head: StartEntry,
    tail: Vec<StartEntry>,
}

impl StartSet {
    /// Builds a start set from a guaranteed-present first entry.
    pub fn new(head: StartEntry, tail: Vec<StartEntry>) -> Self {
        Self { head, tail }
    }

    /// The first entry. Total — there is always at least one.
    pub fn head(&self) -> &StartEntry {
        &self.head
    }

    /// The first node id. This is what `start_ids[0]` means in the original.
    pub fn first_node_id(&self) -> &NodeId {
        &self.head.node_id
    }

    /// Every entry, in wire order.
    pub fn iter(&self) -> impl Iterator<Item = &StartEntry> {
        std::iter::once(&self.head).chain(self.tail.iter())
    }

    /// Every node id, in wire order — the equivalent of `start_ids`.
    pub fn node_ids(&self) -> impl Iterator<Item = &NodeId> {
        self.iter().map(|entry| &entry.node_id)
    }

    /// The number of entries. Never zero.
    pub fn len(&self) -> usize {
        1 + self.tail.len()
    }

    /// Always `false` — present so clippy does not ask for it, and so the
    /// non-empty guarantee is discoverable from the API.
    pub fn is_empty(&self) -> bool {
        false
    }
}

impl TryFrom<Vec<StartEntry>> for StartSet {
    type Error = StartSetError;

    fn try_from(entries: Vec<StartEntry>) -> Result<Self, Self::Error> {
        let mut iter = entries.into_iter();
        let head = iter.next().ok_or(StartSetError::Empty)?;
        Ok(Self {
            head,
            tail: iter.collect(),
        })
    }
}

/// The only way building a [`StartSet`] can fail.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum StartSetError {
    #[error("start directive must name at least one node")]
    Empty,
}

impl Serialize for StartSet {
    /// Always emits the map form. The original coerces the bare-string
    /// form to a map before validation, so this matches its behaviour rather
    /// than preserving the input's surface syntax.
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(Some(self.len()))?;
        for entry in self.iter() {
            map.serialize_entry(entry.node_id.as_str(), entry.key.as_str())?;
        }
        map.end()
    }
}

impl<'de> Deserialize<'de> for StartSet {
    /// Accepts either a bare string (`start: A`) or a map
    /// (`start: {C: key_C}`), mirroring `EnsuredStrDict`.
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_any(StartSetVisitor)
    }
}

struct StartSetVisitor;

impl<'de> Visitor<'de> for StartSetVisitor {
    type Value = StartSet;

    fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("a node id string or a non-empty map of node id to join key")
    }

    fn visit_str<E: serde::de::Error>(self, v: &str) -> Result<Self::Value, E> {
        // The bare form: `start: A` means `{A: A}`.
        Ok(StartSet {
            head: StartEntry {
                node_id: NodeId::new(v),
                key: CompactString::from(v),
            },
            tail: Vec::new(),
        })
    }

    fn visit_map<A: MapAccess<'de>>(self, mut access: A) -> Result<Self::Value, A::Error> {
        let mut entries = Vec::with_capacity(access.size_hint().unwrap_or(1));
        while let Some((node_id, key)) = access.next_entry::<String, CompactString>()? {
            entries.push(StartEntry {
                node_id: NodeId::new(node_id),
                key,
            });
        }
        StartSet::try_from(entries).map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(node: &str, key: &str) -> StartEntry {
        StartEntry {
            node_id: NodeId::new(node),
            key: CompactString::from(key),
        }
    }

    #[test]
    fn try_from_rejects_an_empty_vec() {
        assert_eq!(StartSet::try_from(Vec::new()), Err(StartSetError::Empty));
    }

    #[test]
    fn try_from_accepts_one_entry() -> Result<(), StartSetError> {
        let set = StartSet::try_from(vec![entry("A", "A")])?;
        assert_eq!(set.len(), 1);
        assert!(!set.is_empty());
        assert_eq!(set.head(), &entry("A", "A"));
        assert_eq!(set.first_node_id(), &NodeId::new("A"));
        Ok(())
    }

    /// The two-entry shape from the real corpus
    /// (`pipeline1.yaml`'s `X.start = {C: key_C, D: key_D}`).
    #[test]
    fn try_from_preserves_order_across_entries() -> Result<(), StartSetError> {
        let set = StartSet::try_from(vec![entry("C", "key_C"), entry("D", "key_D")])?;
        assert_eq!(set.len(), 2);
        assert_eq!(
            set.node_ids().collect::<Vec<_>>(),
            vec![&NodeId::new("C"), &NodeId::new("D")]
        );
        // `start_ids[0]` semantics: the *first* entry specifically.
        assert_eq!(set.first_node_id(), &NodeId::new("C"));
        Ok(())
    }

    #[test]
    fn new_builds_directly() {
        let set = StartSet::new(entry("A", "A"), vec![entry("B", "B")]);
        assert_eq!(set.len(), 2);
        assert_eq!(set.iter().count(), 2);
    }

    #[test]
    fn deserializes_the_bare_string_form() -> Result<(), serde_yaml::Error> {
        // `pipeline1.yaml`: `start: A`
        let set: StartSet = serde_yaml::from_str("A")?;
        assert_eq!(set.len(), 1);
        assert_eq!(set.head(), &entry("A", "A"));
        Ok(())
    }

    #[test]
    fn deserializes_the_map_form() -> Result<(), serde_yaml::Error> {
        // `pipeline1.yaml`: `start: {C: key_C, D: key_D}`
        let set: StartSet = serde_yaml::from_str("C: key_C\nD: key_D\n")?;
        assert_eq!(
            set.iter().cloned().collect::<Vec<_>>(),
            vec![entry("C", "key_C"), entry("D", "key_D")]
        );
        Ok(())
    }

    #[test]
    fn deserializing_an_empty_map_is_an_error() {
        let result = serde_yaml::from_str::<StartSet>("{}");
        assert!(result.is_err(), "an empty start map must be rejected");
    }

    #[test]
    fn deserializing_a_wrong_type_is_an_error() {
        assert!(serde_yaml::from_str::<StartSet>("[1, 2]").is_err());
        assert!(serde_yaml::from_str::<StartSet>("true").is_err());
    }

    #[test]
    fn serializes_as_a_map_preserving_values() -> Result<(), serde_json::Error> {
        let set = StartSet::new(entry("C", "key_C"), vec![entry("D", "key_D")]);
        let json = serde_json::to_string(&set)?;
        assert_eq!(json, r#"{"C":"key_C","D":"key_D"}"#);
        Ok(())
    }

    /// The bare form is normalised to a map on the way out, matching the
    /// original's `EnsuredStrDict` coercion rather than preserving
    /// the input's surface syntax.
    #[test]
    fn the_bare_form_normalises_to_a_map() -> Result<(), Box<dyn std::error::Error>> {
        let set: StartSet = serde_yaml::from_str("A")?;
        assert_eq!(serde_json::to_string(&set)?, r#"{"A":"A"}"#);
        Ok(())
    }

    #[test]
    fn map_form_round_trips_losslessly() -> Result<(), Box<dyn std::error::Error>> {
        let original: StartSet = serde_yaml::from_str("C: key_C\nD: key_D\n")?;
        let json = serde_json::to_string(&original)?;
        let back: StartSet = serde_json::from_str(&json)?;
        assert_eq!(back, original);
        Ok(())
    }

    #[test]
    fn error_displays_usefully() {
        assert_eq!(
            StartSetError::Empty.to_string(),
            "start directive must name at least one node"
        );
    }

    #[test]
    fn expecting_message_is_descriptive() {
        // Surfaced in deserialization errors; asserted so the message stays
        // useful rather than drifting into uselessness unnoticed.
        match serde_yaml::from_str::<StartSet>("[1]") {
            Ok(unexpected) => panic!("a sequence must not deserialize: {unexpected:?}"),
            Err(err) => assert!(
                err.to_string().contains("node id string"),
                "unexpected message: {err}"
            ),
        }
    }

    proptest::proptest! {
        /// Non-emptiness is exactly the construction precondition, and
        /// order is always preserved.
        #[test]
        fn try_from_succeeds_iff_non_empty(names in proptest::collection::vec("[A-Z]{1,3}", 0..6)) {
            let entries: Vec<StartEntry> =
                names.iter().map(|n| entry(n, n)).collect();
            let result = StartSet::try_from(entries.clone());
            proptest::prop_assert_eq!(result.is_ok(), !names.is_empty());
            if let Ok(set) = result {
                proptest::prop_assert_eq!(set.iter().cloned().collect::<Vec<_>>(), entries);
            }
        }
    }
}
