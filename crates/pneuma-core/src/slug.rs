//! The [`Slug`] — a run-scoped, hierarchical identifier for one executing
//! step.
//!
//! Format: `{run_id}.{pipeline_id}.{node_path}` for a root step,
//! `{parent_slug}.{node_id}` for a nested one, each optionally suffixed
//! `:{child_index}` when produced by a `ListAggregator` fan-out. This mirrors
//! `construct_slug_from`.
//!
//! # The `lstrip` defect this module fixes
//!
//! The original derives a step's coordination key with
//! `self.slug.lstrip(f"{self.run_id}.{self.pipeline_id}.")`
//! (the original).
//! `str.lstrip` takes a **character set**, not a prefix: it strips leading
//! characters for as long as each one appears anywhere in the argument. With
//! `run_id="run1"` and `pipeline_id="p.q.r"` the set is `{r,u,n,1,.,p,q}`, so
//! `run1.p.q.r.pre_process` becomes `e_process` — it eats into the node id.
//!
//! Reads and writes use the same wrong function, so it never raises; the
//! damage is silent **aliasing**, where two distinct node paths reduce to the
//! same key. [`Slug::key_within`] uses `strip_prefix`, which is
//! length-bounded and cannot do this.
//!
//! # Separator discipline
//!
//! Because `.` separates path segments and `:` introduces a fan-out index,
//! a node id containing either character would let two structurally different
//! steps render to the same slug — reintroducing the same aliasing at the
//! *writing* end. The constructors reject such ids rather than silently
//! producing a colliding slug.

use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::{
    child_index::ChildIndex,
    ids::{NodeId, PipelineId, RunId},
};

/// Separates path segments within a slug.
const SEGMENT_SEPARATOR: char = '.';
/// Introduces the fan-out child index suffix.
const INDEX_SEPARATOR: char = ':';

/// A run-scoped, hierarchical step identifier.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Slug(Arc<str>);

impl Slug {
    /// Builds a root slug: `{run}.{pipeline}.{node_path}`, optionally
    /// suffixed `:{child_index}`.
    ///
    /// `node_path` is the step's path within the pipeline and may contain
    /// `.` separators (a nested step's path is `{aggregator}.{inner}`), but
    /// may not contain `:` — see the module docs on separator discipline.
    /// Taking the [`PipelineId`] explicitly means a slug built here always
    /// satisfies [`Slug::key_within`]'s prefix expectation by construction.
    pub fn root(
        run: &RunId,
        pipeline: &PipelineId,
        node_path: &str,
        child_index: Option<ChildIndex>,
    ) -> Result<Self, SlugError> {
        reject_index_separator(node_path)?;
        let base = format!("{run}.{pipeline}.{node_path}");
        Ok(Self(render(base, child_index)))
    }

    /// Builds a nested slug: `{self}.{node}`, optionally `:{child_index}`.
    ///
    /// Rejects node ids containing `.` or `:`, either of which would let this
    /// slug collide with a structurally different one.
    pub fn child(&self, node: &NodeId, child_index: Option<ChildIndex>) -> Result<Self, SlugError> {
        reject_separators(node.as_str())?;
        let base = format!("{}.{node}", self.0);
        Ok(Self(render(base, child_index)))
    }

    /// Wraps an already-rendered slug, e.g. one read back from storage.
    ///
    /// Performs no validation: the string is taken as authoritative, because
    /// it may have been written by the original service, whose node ids were
    /// never constrained.
    pub fn from_raw(raw: impl Into<Arc<str>>) -> Self {
        Self(raw.into())
    }

    /// Borrows the whole slug.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// The slug with its `{run}.{pipeline}.` prefix removed — the
    /// per-instance coordination key.
    ///
    /// **Retains any trailing `:{child_index}`, deliberately.** This is the
    /// `$addToSet` member in the aggregation barrier,
    /// so a `ListAggregator` fanning out to
    /// `A:1`, `A:2`, `A:3` needs three *distinct* members. Stripping the
    /// suffix would collapse them to one `"A"`, `$addToSet` would dedupe,
    /// the refcount would read 1 instead of 3, and the aggregator would
    /// never reach its expected count — a permanent hang. Use
    /// [`Slug::child_index`] to read the index separately.
    ///
    /// Uses `strip_prefix`, **not** a character-set strip — see the module
    /// docs for why that distinction is load-bearing.
    pub fn key_within(&self, run: &RunId, pipeline: &PipelineId) -> Result<&str, SlugError> {
        let prefix = format!("{run}.{pipeline}.");
        self.0
            .strip_prefix(prefix.as_str())
            .ok_or_else(|| SlugError::PrefixMismatch {
                slug: self.0.to_string(),
                expected_prefix: prefix,
            })
    }

    /// The trailing `:{child_index}` suffix.
    ///
    /// Returns `Ok(None)` when there is no suffix, and `Err` when there is
    /// one that cannot be read — a corrupt suffix is not the same fact as an
    /// absent one. Conflating them would let a truncated or hand-edited slug
    /// be treated as a plain root step, so the aggregator's barrier never
    /// sees that child and the run hangs or completes short instead of
    /// failing loudly.
    ///
    /// Only the **last** dot-separated segment is inspected, and only its
    /// text after the **last** colon. Nested aggregators produce slugs with
    /// more than one colon (`run1.agg:2.inner:5`); parsing from the *first*
    /// colon — as `process_aggregation_and_next_steps` does in the original,
    /// via `str.partition` — yields `"2.inner:5"` and
    /// raises `ValueError`. `_try_aggregate` gets
    /// this right; the two parsers disagree in the original.
    pub fn child_index(&self) -> Result<Option<ChildIndex>, SlugError> {
        // `rsplit(..).next()` is always `Some`, so matching on it would leave
        // a permanently unreachable arm; `rsplit_once` has two genuinely
        // reachable outcomes instead — a dotted slug, and one with no dot.
        let last_segment = match self.0.rsplit_once(SEGMENT_SEPARATOR) {
            Some((_, tail)) => tail,
            None => &self.0,
        };
        let Some((_, raw)) = last_segment.rsplit_once(INDEX_SEPARATOR) else {
            return Ok(None);
        };
        raw.parse::<u32>()
            .ok()
            .and_then(|n| ChildIndex::new(n).ok())
            .map(Some)
            .ok_or_else(|| SlugError::MalformedChildIndex {
                slug: self.0.to_string(),
                suffix: raw.to_string(),
            })
    }
}

fn render(base: String, child_index: Option<ChildIndex>) -> Arc<str> {
    match child_index {
        // `Arc::<str>::from(String)` reuses the allocation rather than
        // copying out of a temporary `&str`.
        Some(idx) => Arc::from(format!("{base}{INDEX_SEPARATOR}{}", idx.get())),
        None => Arc::from(base),
    }
}

fn reject_index_separator(segment: &str) -> Result<(), SlugError> {
    if segment.contains(INDEX_SEPARATOR) {
        return Err(SlugError::InvalidSegment {
            segment: segment.to_owned(),
            offending: INDEX_SEPARATOR,
        });
    }
    Ok(())
}

fn reject_separators(segment: &str) -> Result<(), SlugError> {
    if segment.contains(SEGMENT_SEPARATOR) {
        return Err(SlugError::InvalidSegment {
            segment: segment.to_owned(),
            offending: SEGMENT_SEPARATOR,
        });
    }
    reject_index_separator(segment)
}

impl std::fmt::Display for Slug {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// Why building or interpreting a [`Slug`] failed.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SlugError {
    #[error("slug {slug:?} does not start with expected prefix {expected_prefix:?}")]
    PrefixMismatch {
        slug: String,
        expected_prefix: String,
    },
    #[error("slug segment {segment:?} must not contain {offending:?}")]
    InvalidSegment { segment: String, offending: char },
    #[error("slug {slug:?} has an unreadable child index {suffix:?}")]
    MalformedChildIndex { slug: String, suffix: String },
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run() -> RunId {
        RunId::new("run1")
    }
    fn pipeline() -> PipelineId {
        PipelineId::new("p.q.r")
    }
    fn idx(n: u32) -> Option<ChildIndex> {
        ChildIndex::new(n).ok()
    }

    /// Reproduces the original's `str.lstrip(chars)` — strips leading characters
    /// for as long as each is a member of `chars`. Present **only** to
    /// demonstrate, executably, that [`Slug::key_within`] does not share its
    /// behaviour. Never used outside these tests.
    fn reference_lstrip<'a>(s: &'a str, chars: &str) -> &'a str {
        s.trim_start_matches(|c| chars.contains(c))
    }

    #[test]
    fn root_renders_run_pipeline_and_path() -> Result<(), SlugError> {
        let slug = Slug::root(&run(), &pipeline(), "A", None)?;
        assert_eq!(slug.as_str(), "run1.p.q.r.A");
        assert_eq!(slug.to_string(), "run1.p.q.r.A");
        Ok(())
    }

    #[test]
    fn root_accepts_a_nested_dotted_path() -> Result<(), SlugError> {
        let slug = Slug::root(&run(), &pipeline(), "Agg.Inner", None)?;
        assert_eq!(slug.as_str(), "run1.p.q.r.Agg.Inner");
        Ok(())
    }

    #[test]
    fn root_appends_child_index() -> Result<(), SlugError> {
        let slug = Slug::root(&run(), &pipeline(), "A", idx(3))?;
        assert_eq!(slug.as_str(), "run1.p.q.r.A:3");
        Ok(())
    }

    #[test]
    fn root_rejects_an_index_separator_in_the_path() {
        let err = Slug::root(&run(), &pipeline(), "A:7", None);
        assert_eq!(
            err,
            Err(SlugError::InvalidSegment {
                segment: "A:7".to_owned(),
                offending: ':',
            })
        );
    }

    #[test]
    fn child_appends_node_id() -> Result<(), SlugError> {
        let parent = Slug::root(&run(), &pipeline(), "Agg", None)?;
        let child = parent.child(&NodeId::new("Inner"), None)?;
        assert_eq!(child.as_str(), "run1.p.q.r.Agg.Inner");
        Ok(())
    }

    #[test]
    fn child_appends_node_id_and_index() -> Result<(), SlugError> {
        let parent = Slug::root(&run(), &pipeline(), "Agg", idx(2))?;
        let child = parent.child(&NodeId::new("Inner"), idx(5))?;
        assert_eq!(child.as_str(), "run1.p.q.r.Agg:2.Inner:5");
        Ok(())
    }

    /// The writer-side aliasing this module must not permit: a node id
    /// carrying its own `:7` would render identically to a genuine `:7`
    /// fan-out suffix, making two structurally different steps share one
    /// coordination key.
    #[test]
    fn child_rejects_a_node_id_containing_an_index_separator() -> Result<(), SlugError> {
        let parent = Slug::root(&run(), &pipeline(), "A", None)?;

        let forged = parent.child(&NodeId::new("B:7"), None);
        assert_eq!(
            forged,
            Err(SlugError::InvalidSegment {
                segment: "B:7".to_owned(),
                offending: ':',
            })
        );

        // The slug it would otherwise have collided with is still buildable.
        let genuine = parent.child(&NodeId::new("B"), idx(7))?;
        assert_eq!(genuine.as_str(), "run1.p.q.r.A.B:7");
        Ok(())
    }

    #[test]
    fn child_rejects_a_node_id_containing_a_segment_separator() -> Result<(), SlugError> {
        let parent = Slug::root(&run(), &pipeline(), "A", None)?;
        assert_eq!(
            parent.child(&NodeId::new("B.C"), None),
            Err(SlugError::InvalidSegment {
                segment: "B.C".to_owned(),
                offending: '.',
            })
        );
        Ok(())
    }

    #[test]
    fn from_raw_takes_a_stored_slug_verbatim() {
        // original-written slugs were never separator-constrained, so reading
        // one back must not fail.
        let slug = Slug::from_raw("run1.p.q.r.B:7");
        assert_eq!(slug.as_str(), "run1.p.q.r.B:7");
    }

    #[test]
    fn key_within_strips_the_run_and_pipeline_prefix() -> Result<(), SlugError> {
        let slug = Slug::root(&run(), &pipeline(), "pre_process", None)?;
        assert_eq!(slug.key_within(&run(), &pipeline())?, "pre_process");
        Ok(())
    }

    /// Pins the deliberate retention of the fan-out suffix. Stripping it
    /// would make sibling children share one `$addToSet` member and hang the
    /// aggregator — see the doc comment on `key_within`.
    #[test]
    fn key_within_retains_the_child_index_so_siblings_stay_distinct() -> Result<(), SlugError> {
        let one = Slug::root(&run(), &pipeline(), "A", idx(1))?;
        let two = Slug::root(&run(), &pipeline(), "A", idx(2))?;

        assert_eq!(one.key_within(&run(), &pipeline())?, "A:1");
        assert_eq!(two.key_within(&run(), &pipeline())?, "A:2");
        assert_ne!(
            one.key_within(&run(), &pipeline())?,
            two.key_within(&run(), &pipeline())?,
            "fan-out siblings must not share a coordination key"
        );
        Ok(())
    }

    #[test]
    fn key_within_reports_a_prefix_mismatch() {
        let slug = Slug::from_raw("other.p.q.r.A");
        assert!(matches!(
            slug.key_within(&run(), &pipeline()),
            Err(SlugError::PrefixMismatch { .. })
        ));
    }

    #[test]
    fn key_within_requires_the_trailing_separator() {
        // "run1.p.q.rX" shares the prefix "run1.p.q.r" but not
        // "run1.p.q.r.", so the strip must fail rather than succeed on a
        // partial match.
        let slug = Slug::from_raw("run1.p.q.rX");
        assert!(slug.key_within(&run(), &pipeline()).is_err());
    }

    /// The pinned, production-verified regression case from the module docs.
    /// Asserts both halves: the original semantics really do corrupt this
    /// slug, and `key_within` really does not.
    #[test]
    fn key_within_does_not_reproduce_the_lstrip_corruption() -> Result<(), SlugError> {
        let slug = Slug::root(&run(), &pipeline(), "pre_process", None)?;
        let charset = format!("{}.{}.", run(), pipeline());

        assert_eq!(
            reference_lstrip(slug.as_str(), &charset),
            "e_process",
            "the original's lstrip must corrupt this slug"
        );
        assert_eq!(
            slug.key_within(&run(), &pipeline())?,
            "pre_process",
            "key_within must not"
        );
        Ok(())
    }

    /// Two distinct paths that `lstrip` collapses to the *same* string —
    /// the silent aliasing that makes the original defect dangerous rather
    /// than merely cosmetic.
    #[test]
    fn lstrip_aliases_distinct_paths_but_key_within_does_not() -> Result<(), SlugError> {
        let a = Slug::root(&run(), &pipeline(), "pre_process", None)?;
        let b = Slug::root(&run(), &pipeline(), "p.r.u.n.pre_process", None)?;
        let charset = format!("{}.{}.", run(), pipeline());

        assert_eq!(
            reference_lstrip(a.as_str(), &charset),
            reference_lstrip(b.as_str(), &charset),
            "lstrip must alias these two distinct slugs"
        );
        assert_ne!(
            a.key_within(&run(), &pipeline())?,
            b.key_within(&run(), &pipeline())?,
            "key_within must keep them distinct"
        );
        Ok(())
    }

    #[test]
    fn child_idx_reads_a_simple_suffix() -> Result<(), SlugError> {
        let slug = Slug::root(&run(), &pipeline(), "A", idx(4))?;
        assert_eq!(slug.child_index()?, idx(4));
        Ok(())
    }

    #[test]
    fn child_idx_is_none_without_a_suffix() -> Result<(), SlugError> {
        let slug = Slug::root(&run(), &pipeline(), "A", None)?;
        assert_eq!(slug.child_index()?, None);
        Ok(())
    }

    /// The nested-aggregator case that raises `ValueError` in the original's
    /// `partition`-based parser the original.
    #[test]
    fn child_idx_handles_nested_multi_colon_slugs() -> Result<(), SlugError> {
        let parent = Slug::root(&run(), &pipeline(), "Agg", idx(2))?;
        let child = parent.child(&NodeId::new("Inner"), idx(5))?;
        assert_eq!(child.as_str(), "run1.p.q.r.Agg:2.Inner:5");
        // The *last* index, not the first, and not a parse failure.
        assert_eq!(child.child_index()?, idx(5));
        Ok(())
    }

    #[test]
    fn child_idx_ignores_an_index_on_an_earlier_segment() -> Result<(), SlugError> {
        let parent = Slug::root(&run(), &pipeline(), "Agg", idx(2))?;
        let child = parent.child(&NodeId::new("Inner"), None)?;
        assert_eq!(child.as_str(), "run1.p.q.r.Agg:2.Inner");
        assert_eq!(child.child_index()?, None);
        Ok(())
    }

    /// A corrupt suffix is a distinct fact from an absent one. Reporting it
    /// as "no fan-out" would silently drop the child from its aggregator's
    /// barrier.
    #[test]
    fn child_idx_reports_a_malformed_suffix_rather_than_none() {
        for raw in [
            "run1.p.A:abc",
            "run1.p.A:0",
            "run1.p.A:",
            "run1.p.A:4294967296",
        ] {
            let slug = Slug::from_raw(raw);
            assert!(
                matches!(
                    slug.child_index(),
                    Err(SlugError::MalformedChildIndex { .. })
                ),
                "{raw} must report a malformed index, got {:?}",
                slug.child_index()
            );
        }
    }

    /// A slug with no `.` at all still parses — exercises the no-separator
    /// arm of the last-segment split.
    #[test]
    fn child_idx_handles_a_slug_without_any_dot() -> Result<(), SlugError> {
        assert_eq!(Slug::from_raw("bare").child_index()?, None);
        assert_eq!(Slug::from_raw("bare:2").child_index()?, idx(2));
        Ok(())
    }

    #[test]
    fn serde_round_trips_as_a_bare_string() -> Result<(), Box<dyn std::error::Error>> {
        let slug = Slug::root(&run(), &pipeline(), "A", None)?;
        let json = serde_json::to_string(&slug)?;
        assert_eq!(json, "\"run1.p.q.r.A\"");
        let back: Slug = serde_json::from_str(&json)?;
        assert_eq!(back, slug);
        Ok(())
    }

    #[test]
    fn errors_display_usefully() {
        assert_eq!(
            SlugError::PrefixMismatch {
                slug: "a.b".to_owned(),
                expected_prefix: "c.".to_owned(),
            }
            .to_string(),
            r#"slug "a.b" does not start with expected prefix "c.""#
        );
        assert_eq!(
            SlugError::InvalidSegment {
                segment: "B:7".to_owned(),
                offending: ':',
            }
            .to_string(),
            r#"slug segment "B:7" must not contain ':'"#
        );
        assert_eq!(
            SlugError::MalformedChildIndex {
                slug: "a:x".to_owned(),
                suffix: "x".to_owned(),
            }
            .to_string(),
            r#"slug "a:x" has an unreadable child index "x""#
        );
    }

    proptest::proptest! {
        /// **The load-bearing property.** Distinct in-pipeline paths must
        /// never collapse to the same key. The generator is biased toward
        /// the run/pipeline id's own character set, because a uniform random
        /// alphabet would essentially never stumble into the specific
        /// membership collision `lstrip` produces — an unbiased generator
        /// would pass vacuously and prove nothing.
        #[test]
        fn distinct_paths_never_alias(
            a in "[runp1.q]{1,12}",
            b in "[runp1.q]{1,12}",
        ) {
            proptest::prop_assume!(a != b);
            let (run, pipeline) = (run(), pipeline());

            let slug_a = Slug::root(&run, &pipeline, &a, None);
            let slug_b = Slug::root(&run, &pipeline, &b, None);
            proptest::prop_assert!(slug_a.is_ok() && slug_b.is_ok());

            if let (Ok(slug_a), Ok(slug_b)) = (slug_a, slug_b) {
                let key_a = slug_a.key_within(&run, &pipeline);
                let key_b = slug_b.key_within(&run, &pipeline);
                proptest::prop_assert!(key_a.is_ok() && key_b.is_ok());
                proptest::prop_assert_ne!(key_a, key_b);
            }
        }

        /// Reading an arbitrary stored slug must never panic.
        #[test]
        fn reading_a_raw_slug_never_panics(raw in ".*") {
            let slug = Slug::from_raw(raw.as_str());
            let _ = slug.key_within(&run(), &pipeline());
            let _ = slug.child_index();
        }
    }
}
