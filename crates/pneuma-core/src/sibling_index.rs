//! A node's static position among the components its parent declares.
//!
//! This is the `nth` field, and it is **not** the same thing as
//! [`crate::child_index::ChildIndex`], though the two are easy to confuse and were
//! confused here until a review caught it. Both are 1-based positive integers,
//! so nothing about the numbers distinguishes them; only their provenance does.
//!
//! | | [`SiblingIndex`] (`nth`) | [`ChildIndex`](crate::child_index::ChildIndex) (`child_index`) |
//! |---|---|---|
//! | Comes from | the pipeline **definition** | the **runtime** input |
//! | Computed at | resolve time, the original | dispatch time, the original |
//! | Enumerates | `step.components` | `enumerate(step_input)` |
//! | Meaning | which declared child this node is | which fanned-out item this run is for |
//! | Varies per run | no | yes |
//!
//! Concretely, the original computes `nth = nth_ + 1 if isinstance(step,
//! ListAggregatorStep) else 1` while enumerating an aggregator's declared
//! components, so it is a property of the *graph*. the original computes
//! `child_index=index + 1` while enumerating the actual list being fanned out, so
//! it is a property of one *execution*.
//!
//! A node declared second inside a `ListAggregator` has `nth == 2` on every run
//! of every pipeline that contains it; its `child_index` is 1 on the first input
//! item, 2 on the second, and unbounded.
//!
//! Keeping them as separate types means the two cannot be swapped by writing
//! the wrong field name, which is otherwise a one-word mistake the compiler
//! would accept.
//!
//! # The default is 1, not absent
//!
//! The original declares `nth: int = 1` in three places. A node that is not inside a
//! `ListAggregator` — including every top-level node — has `nth == 1`, not
//! "no `nth`". Anything reconstructing a persisted record from a wire message
//! where `nth` was absent must restore [`SiblingIndex::FIRST`] rather than
//! writing zero or null.

use std::num::NonZeroU32;

use serde::{Deserialize, Serialize};

/// A 1-based position among a parent's declared components. Zero is
/// unrepresentable by construction.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(try_from = "u32", into = "u32")]
pub struct SiblingIndex(NonZeroU32);

impl SiblingIndex {
    /// The first position (1), and the default for any node that is not a
    /// `ListAggregator`'s child — see the module docs.
    pub const FIRST: SiblingIndex = SiblingIndex(NonZeroU32::MIN);

    /// Constructs a sibling index, rejecting zero.
    pub fn new(n: u32) -> Result<Self, SiblingIndexError> {
        NonZeroU32::new(n).map(Self).ok_or(SiblingIndexError::Zero)
    }

    /// The underlying 1-based position.
    pub fn get(self) -> u32 {
        self.0.get()
    }
}

impl TryFrom<u32> for SiblingIndex {
    type Error = SiblingIndexError;

    fn try_from(n: u32) -> Result<Self, Self::Error> {
        Self::new(n)
    }
}

impl From<SiblingIndex> for u32 {
    fn from(index: SiblingIndex) -> Self {
        index.get()
    }
}

/// The only way constructing a [`SiblingIndex`] can fail.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum SiblingIndexError {
    #[error("sibling index must be 1-based, got 0")]
    Zero,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn first_is_one() {
        assert_eq!(SiblingIndex::FIRST.get(), 1);
    }

    #[test]
    fn rejects_zero() {
        assert_eq!(SiblingIndex::new(0), Err(SiblingIndexError::Zero));
        assert!(SiblingIndex::try_from(0).is_err());
    }

    #[test]
    fn accepts_positive_values() -> Result<(), SiblingIndexError> {
        assert_eq!(SiblingIndex::new(1)?, SiblingIndex::FIRST);
        assert_eq!(SiblingIndex::new(7)?.get(), 7);
        assert_eq!(SiblingIndex::try_from(7)?.get(), 7);
        assert_eq!(u32::from(SiblingIndex::new(7)?), 7);
        Ok(())
    }

    #[test]
    fn serializes_as_a_bare_number() -> Result<(), Box<dyn std::error::Error>> {
        let index = SiblingIndex::new(3)?;
        assert_eq!(serde_json::to_string(&index)?, "3");
        let back: SiblingIndex = serde_json::from_str("3")?;
        assert_eq!(back, index);
        Ok(())
    }

    #[test]
    fn deserializing_zero_is_an_error() {
        assert!(serde_json::from_str::<SiblingIndex>("0").is_err());
    }

    #[test]
    fn error_displays_usefully() {
        assert_eq!(
            SiblingIndexError::Zero.to_string(),
            "sibling index must be 1-based, got 0"
        );
    }

    #[test]
    fn is_ordered_and_hashable() -> Result<(), SiblingIndexError> {
        use std::collections::HashSet;
        assert!(SiblingIndex::new(1)? < SiblingIndex::new(2)?);
        let set: HashSet<_> = [SiblingIndex::new(1)?, SiblingIndex::new(1)?].into();
        assert_eq!(set.len(), 1);
        assert!(format!("{:?}", SiblingIndex::FIRST).contains('1'));
        Ok(())
    }
}
