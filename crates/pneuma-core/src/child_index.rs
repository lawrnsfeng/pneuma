//! The 1-based index of a child produced by a `ListAggregator` fan-out.
//!
//! Modelled as a [`NonZeroU32`] so that "zero" is unrepresentable rather than
//! merely invalid. This closes two related defects from the originals:
//!
//! - the original `child_idx or 1` idiom, which silently coerces a legitimate
//!   `0` to `1` (and which only *happens* to be harmless because indices are
//!   1-based to begin with);
//! - the original sibling service's bare `int` field, which coerces a missing
//!   `child_index: null` to `0`.

use std::num::NonZeroU32;

use serde::{Deserialize, Serialize};

/// A 1-based child index. Zero is unrepresentable by construction.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(try_from = "u32", into = "u32")]
pub struct ChildIndex(NonZeroU32);

impl ChildIndex {
    /// The first child index (1). Uses [`NonZeroU32::MIN`], which is exactly
    /// 1, so no fallible construction or lint exemption is needed here.
    pub const FIRST: ChildIndex = ChildIndex(NonZeroU32::MIN);

    /// Constructs a child index, rejecting zero.
    pub fn new(n: u32) -> Result<Self, ChildIndexError> {
        NonZeroU32::new(n).map(Self).ok_or(ChildIndexError::Zero)
    }

    /// The underlying 1-based index.
    pub fn get(self) -> u32 {
        self.0.get()
    }
}

impl TryFrom<u32> for ChildIndex {
    type Error = ChildIndexError;

    fn try_from(n: u32) -> Result<Self, Self::Error> {
        Self::new(n)
    }
}

impl From<ChildIndex> for u32 {
    fn from(idx: ChildIndex) -> Self {
        idx.get()
    }
}

/// The only way constructing a [`ChildIndex`] can fail.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ChildIndexError {
    #[error("child index must be 1-based, got 0")]
    Zero,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_accepts_positive() -> Result<(), ChildIndexError> {
        assert_eq!(ChildIndex::new(1)?.get(), 1);
        assert_eq!(ChildIndex::new(7)?.get(), 7);
        Ok(())
    }

    #[test]
    fn new_rejects_zero() {
        assert_eq!(ChildIndex::new(0), Err(ChildIndexError::Zero));
    }

    #[test]
    fn first_is_one() {
        assert_eq!(ChildIndex::FIRST.get(), 1);
    }

    #[test]
    fn ord_compares_by_index() -> Result<(), ChildIndexError> {
        assert!(ChildIndex::new(1)? < ChildIndex::new(2)?);
        Ok(())
    }

    #[test]
    fn try_from_u32_matches_new() {
        assert_eq!(ChildIndex::try_from(3), ChildIndex::new(3));
        assert_eq!(ChildIndex::try_from(0), Err(ChildIndexError::Zero));
    }

    #[test]
    fn into_u32_round_trips() -> Result<(), ChildIndexError> {
        let idx = ChildIndex::new(5)?;
        let raw: u32 = idx.into();
        assert_eq!(raw, 5);
        Ok(())
    }

    #[test]
    fn serde_serializes_as_bare_number() -> Result<(), Box<dyn std::error::Error>> {
        let idx = ChildIndex::new(4)?;
        let json = serde_json::to_string(&idx)?;
        assert_eq!(json, "4");
        let back: ChildIndex = serde_json::from_str(&json)?;
        assert_eq!(back, idx);
        Ok(())
    }

    #[test]
    fn serde_rejects_zero_on_the_wire() {
        let err = serde_json::from_str::<ChildIndex>("0");
        assert!(err.is_err(), "0 must not deserialize into a ChildIndex");
    }

    #[test]
    fn error_displays_usefully() {
        assert_eq!(
            ChildIndexError::Zero.to_string(),
            "child index must be 1-based, got 0"
        );
    }
}
