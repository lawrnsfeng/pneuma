//! A reference from one node to its successor, or to the end of the graph.
//!
//! The wire format is a bare string: either the literal `"end"` sentinel or a
//! node id. Making the sentinel a variant means the two call sites that
//! special-case it in the original (both bare `== Constants.END`
//! comparisons) become exhaustive matches the
//! compiler checks.

use serde::{Deserialize, Serialize};

use crate::ids::NodeId;

/// The exact sentinel string, the original
/// (`END: Final[str] = "end"`). Lowercase, and compared case-sensitively.
///
/// Public so the resolver can reject a node id that collides with it — such
/// a node is unreferenceable, since every `children: [end]` parses as
/// [`ChildRef::End`].
pub const END_SENTINEL: &str = "end";

/// A successor reference: either another node, or the end of the graph.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(from = "String", into = "String")]
pub enum ChildRef {
    /// The `"end"` sentinel — this branch terminates here.
    End,
    /// A reference to another node by id.
    Node(NodeId),
}

impl ChildRef {
    /// The referenced node id, or `None` for [`ChildRef::End`].
    pub fn node_id(&self) -> Option<&NodeId> {
        match self {
            ChildRef::End => None,
            ChildRef::Node(id) => Some(id),
        }
    }

    /// Whether this reference terminates the branch.
    pub fn is_end(&self) -> bool {
        matches!(self, ChildRef::End)
    }
}

impl From<String> for ChildRef {
    fn from(s: String) -> Self {
        if s == END_SENTINEL {
            ChildRef::End
        } else {
            ChildRef::Node(NodeId::new(s))
        }
    }
}

impl From<ChildRef> for String {
    fn from(reference: ChildRef) -> Self {
        match reference {
            ChildRef::End => END_SENTINEL.to_owned(),
            ChildRef::Node(id) => id.as_str().to_owned(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_end_sentinel_becomes_the_end_variant() {
        assert_eq!(ChildRef::from("end".to_owned()), ChildRef::End);
    }

    #[test]
    fn any_other_string_becomes_a_node_reference() {
        assert_eq!(
            ChildRef::from("B".to_owned()),
            ChildRef::Node(NodeId::new("B"))
        );
    }

    /// The sentinel comparison is case-sensitive in the original
    /// (the original, a bare `==`), so `"END"` is an ordinary node id,
    /// not the sentinel. Named explicitly so this is not "corrected" into
    /// case-insensitivity later without someone noticing it is a
    /// compatibility decision rather than an oversight.
    #[test]
    fn the_sentinel_is_case_sensitive() {
        assert_eq!(
            ChildRef::from("END".to_owned()),
            ChildRef::Node(NodeId::new("END"))
        );
        assert_eq!(
            ChildRef::from("End".to_owned()),
            ChildRef::Node(NodeId::new("End"))
        );
    }

    #[test]
    fn converts_back_to_a_string() {
        assert_eq!(String::from(ChildRef::End), "end");
        assert_eq!(String::from(ChildRef::Node(NodeId::new("B"))), "B");
    }

    #[test]
    fn node_id_accessor_discriminates() {
        assert_eq!(ChildRef::End.node_id(), None);
        assert_eq!(
            ChildRef::Node(NodeId::new("B")).node_id(),
            Some(&NodeId::new("B"))
        );
    }

    #[test]
    fn is_end_discriminates() {
        assert!(ChildRef::End.is_end());
        assert!(!ChildRef::Node(NodeId::new("B")).is_end());
    }

    #[test]
    fn serde_uses_the_bare_string_wire_form() -> Result<(), serde_json::Error> {
        assert_eq!(serde_json::to_string(&ChildRef::End)?, "\"end\"");
        assert_eq!(
            serde_json::to_string(&ChildRef::Node(NodeId::new("B")))?,
            "\"B\""
        );

        let end: ChildRef = serde_json::from_str("\"end\"")?;
        assert_eq!(end, ChildRef::End);
        let node: ChildRef = serde_json::from_str("\"B\"")?;
        assert_eq!(node, ChildRef::Node(NodeId::new("B")));
        Ok(())
    }

    proptest::proptest! {
        /// Round-tripping through the wire form is lossless for every input.
        #[test]
        fn string_round_trip_is_lossless(s in ".*") {
            let reference = ChildRef::from(s.clone());
            proptest::prop_assert_eq!(String::from(reference), s);
        }
    }
}
