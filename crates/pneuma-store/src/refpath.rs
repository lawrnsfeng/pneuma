//! The encoding that turns a node path into a Mongo field name.
//!
//! Mongo field names may not contain `.`, so the original substitutes: the
//! `refcounts` and `completed_prerequisites` maps are keyed by
//! `node_path.replace(".", "/")` in the original.
//!
//! # That substitution is not injective, and this type says so
//!
//! `a.b` and `a/b` both encode to `a/b`. Two distinct node paths would then
//! share one coordination key — the same failure mode as the `lstrip` aliasing
//! in the survey notes, and with the same consequence: two independent
//! barriers silently merged.
//!
//! Whether it can fire depends entirely on whether a `/` can reach a node path,
//! which depends on the `node_id` alphabet in real pipelines — not determinable
//! from here, and the same caveat the survey notes record for `lstrip`.
//! Rather than assume it cannot, [`RefPath::new`] refuses a path already
//! containing `/`, so the encoding it produces is injective over what it
//! accepts. A collision becomes a loud rejection at the boundary instead of two
//! runs quietly sharing a barrier.

use compact_str::CompactString;

/// A node path encoded for use as a Mongo field name.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RefPath(CompactString);

/// A node path that cannot be encoded unambiguously.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RefPathError {
    /// The path already contains the character the encoding substitutes *to*,
    /// so encoding it could collide with a different path.
    #[error("node path {path:?} contains '/', which the refcount key encoding substitutes to; it could collide with a different path")]
    ContainsSeparator {
        /// The offending path.
        path: String,
    },
    /// Empty. An empty field name is not addressable in a Mongo update path.
    #[error("a node path may not be empty")]
    Empty,
}

impl RefPath {
    /// Encodes a node path, refusing one that cannot round-trip.
    pub fn new(path: &str) -> Result<Self, RefPathError> {
        if path.is_empty() {
            return Err(RefPathError::Empty);
        }
        if path.contains('/') {
            return Err(RefPathError::ContainsSeparator {
                path: path.to_owned(),
            });
        }
        Ok(RefPath(CompactString::from(path.replace('.', "/"))))
    }

    /// The encoded form, as it appears in the document.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// The original node path.
    ///
    /// Exact, because [`Self::new`] rejects anything that would not round trip.
    pub fn decode(&self) -> String {
        self.0.replace('/', ".")
    }
}

impl std::fmt::Display for RefPath {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dots_become_slashes_as_the_original_does() {
        let Ok(path) = RefPath::new("run1.p.q.r.node") else {
            panic!("should encode");
        };
        assert_eq!(path.as_str(), "run1/p/q/r/node");
        assert_eq!(path.to_string(), "run1/p/q/r/node");
    }

    #[test]
    fn encoding_round_trips_exactly() {
        for path in ["a", "a.b", "a.b.c", "run1.pipeline.node:2", "x.y-z_1"] {
            let Ok(encoded) = RefPath::new(path) else {
                panic!("{path} should encode");
            };
            assert_eq!(encoded.decode(), path, "{path} must round trip");
        }
    }

    #[test]
    fn a_path_containing_a_slash_is_refused_because_it_would_alias() {
        // `a.b` and `a/b` both encode to `a/b` under the original's
        // substitution. Refusing the second is what keeps the encoding
        // injective over what this type accepts.
        let Ok(dotted) = RefPath::new("a.b") else {
            panic!("should encode");
        };
        assert_eq!(dotted.as_str(), "a/b");

        assert_eq!(
            RefPath::new("a/b"),
            Err(RefPathError::ContainsSeparator {
                path: "a/b".to_owned()
            }),
            "encoding this would collide with a.b"
        );
    }

    #[test]
    fn empty_is_refused() {
        assert_eq!(RefPath::new(""), Err(RefPathError::Empty));
    }

    #[test]
    fn the_errors_explain_themselves() {
        assert_eq!(
            RefPathError::Empty.to_string(),
            "a node path may not be empty"
        );
        assert!(RefPathError::ContainsSeparator {
            path: "a/b".to_owned()
        }
        .to_string()
        .contains("could collide"));
    }
}

/// An aggregator's `node_id`, used as a field name under `state`.
///
/// Deliberately **not** a [`RefPath`]. `set_children_number` and
/// `get_children_number` interpolate `step_id` into
/// `state.{step_id}.num_children.{child_index}` with no `.replace(".", "/")`,
/// unlike `increase_refcount` which
/// does encode. Applying the substitution here would make this port write
/// `state.a/b...` where the original service reads `state.a.b...` — the two would
/// silently address different fields during any period of coexistence.
///
/// A `node_id` containing a dot is therefore refused rather than encoded: Mongo
/// cannot hold it as a single field name, and inventing an encoding is what
/// would cause the divergence.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct StepId(CompactString);

impl StepId {
    /// Builds a step id.
    pub fn new(value: &str) -> Result<Self, RefPathError> {
        if value.is_empty() {
            return Err(RefPathError::Empty);
        }
        if value.contains('.') {
            return Err(RefPathError::ContainsSeparator {
                path: value.to_owned(),
            });
        }
        Ok(StepId(CompactString::from(value)))
    }

    /// The id, verbatim.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for StepId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

#[cfg(test)]
mod step_id_tests {
    use super::*;

    #[test]
    fn a_step_id_is_not_encoded() {
        let Ok(id) = StepId::new("aggregator") else {
            panic!("should build");
        };
        assert_eq!(id.as_str(), "aggregator");
        assert_eq!(id.to_string(), "aggregator");
    }

    #[test]
    fn a_dotted_step_id_is_refused_rather_than_encoded() {
        // Encoding it would write state.a/b where original reads state.a.b.
        assert_eq!(
            StepId::new("a.b"),
            Err(RefPathError::ContainsSeparator {
                path: "a.b".to_owned()
            })
        );
        assert_eq!(StepId::new(""), Err(RefPathError::Empty));
        // A slash is fine here: nothing substitutes to it on this path.
        assert!(StepId::new("a/b").is_ok());
    }
}
