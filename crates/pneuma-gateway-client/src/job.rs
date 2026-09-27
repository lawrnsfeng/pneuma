//! The job identifier the termination endpoints are keyed by.

use compact_str::CompactString;

/// A `job_id` that cannot be empty and cannot carry a control character.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct JobId(CompactString);

/// Why a `job_id` was refused.
///
/// The rule is narrow and uniform: **reject only what cannot work.** Both
/// variants are cases where the request is guaranteed to fail or to address the
/// wrong thing, so refusing them here turns a silent misfire into an error at
/// the point the id is built. Anything else is accepted, because
/// percent-encoding carries it safely and `job_id` is an unconstrained `TEXT`
/// column.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum JobIdError {
    /// Empty, or nothing but whitespace. The gateway makes the same check on
    /// create and answers `400`, so such a request cannot succeed.
    #[error("a job id must not be empty or whitespace")]
    Blank,
    /// `.` or `..`, which no amount of encoding survives.
    ///
    /// Both are RFC 3986 *unreserved*, so percent-encoding leaves them exactly
    /// as they are — `quote("..", safe="") == ".."`. HTTP clients then remove
    /// dot-segments before sending, so the segment disappears and the request
    /// leaves the intended route. The defect notes's own measured
    /// table shows httpx doing this. Encoding cannot fix it; only refusing can.
    #[error("a job id of {value:?} is a relative path segment and is removed before the request is sent")]
    DotSegment {
        /// Which of the two it was.
        value: &'static str,
    },
}

impl JobId {
    /// Builds a job id.
    ///
    /// The blank check mirrors the gateway's own,
    /// which rejects
    /// `payload.job_id.trim().is_empty()` with `400`. Matching it here means a
    /// request that would be refused is never sent.
    ///
    /// `.` and `..` are refused for a different reason: they survive encoding
    /// untouched and are then stripped by the client as dot-segments, so the
    /// request silently addresses the wrong path.
    ///
    /// Note what is *not* checked. A `job_id` containing `/`, `?`, `#`, a
    /// space, or a control character is accepted, because
    /// [`crate::percent_encode_segment`] makes all of them safe to put in a
    /// path — `\n` becomes `%0A`. Rejecting those would refuse ids the platform
    /// stores happily (`job_id` is a `TEXT` column with no constraint) in order
    /// to work around a defect in URL construction that this crate fixes at its
    /// source. An earlier version did refuse control characters, which
    /// contradicted this rule for no measured benefit: nothing downstream
    /// rejects them, so the only effect was that a job created directly against
    /// the gateway could never be terminated through this crate.
    pub fn new(value: impl Into<CompactString>) -> Result<Self, JobIdError> {
        let value = value.into();
        if value.trim().is_empty() {
            return Err(JobIdError::Blank);
        }
        if value == "." {
            return Err(JobIdError::DotSegment { value: "." });
        }
        if value == ".." {
            return Err(JobIdError::DotSegment { value: ".." });
        }
        Ok(JobId(value))
    }

    /// The id as a string, unencoded.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for JobId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn blank_is_refused_the_way_the_gateway_refuses_it() {
        // The gateway answers 400 for `job_id.trim().is_empty()`, so these
        // requests cannot succeed and are better refused before being sent.
        for blank in ["", " ", "\t", "   ", "\u{a0}"] {
            assert_eq!(JobId::new(blank), Err(JobIdError::Blank), "{blank:?}");
        }
    }

    #[test]
    fn dot_segments_are_refused_because_encoding_cannot_save_them() {
        // `.` and `..` are RFC 3986 unreserved, so quote() and this crate both
        // leave them verbatim -- `quote("..", safe="") == ".."`. The client then
        // removes them as dot-segments and the request leaves the route. This
        // is the one case where encoding is not enough.
        assert_eq!(JobId::new("."), Err(JobIdError::DotSegment { value: "." }));
        assert_eq!(
            JobId::new(".."),
            Err(JobIdError::DotSegment { value: ".." })
        );

        // Only a segment that IS a dot-segment is affected. `..` inside a
        // longer id is fine, because the `/` around it would be encoded and the
        // whole thing stays one segment.
        for fine in ["..a", "a..", "a/../b", "...", ".hidden"] {
            assert!(JobId::new(fine).is_ok(), "{fine:?}");
        }
    }

    #[test]
    fn control_characters_are_accepted_because_encoding_handles_them() {
        // Deliberate reversal. Nothing downstream rejects them -- the gateway's
        // only content check is trim().is_empty(), and job_id is unconstrained
        // TEXT -- and `\n` encodes to %0A. Refusing them would have meant a job
        // created directly against the gateway could never be terminated here.
        for accepted in ["a\nb", "a\u{0}b", "a\tb"] {
            assert!(JobId::new(accepted).is_ok(), "{accepted:?}");
        }
    }

    #[test]
    fn characters_that_only_break_urls_are_accepted() {
        // These are exactly the ones that corrupt the path when interpolated
        // raw. They are NOT rejected here, because encoding handles them and
        // `job_id` is an unconstrained TEXT column -- refusing them would refuse
        // ids the platform stores happily, to work around a defect in URL
        // construction that this crate fixes at the source.
        for accepted in [
            "with/slash",
            "with?query",
            "with#frag",
            "with space",
            "../runs",
            "with%pct",
            "wíth-ünicode",
            " padded ",
        ] {
            assert!(JobId::new(accepted).is_ok(), "{accepted:?}");
        }
    }

    #[test]
    fn the_id_is_carried_unchanged() {
        let Ok(id) = JobId::new("with/slash") else {
            panic!("should build");
        };
        // Unencoded here: encoding is a property of a path, not of the id, and
        // the create endpoint sends it as a JSON value where it must be raw.
        assert_eq!(id.as_str(), "with/slash");
        assert_eq!(id.to_string(), "with/slash");
    }

    #[test]
    fn errors_read_clearly() {
        assert_eq!(
            JobIdError::Blank.to_string(),
            "a job id must not be empty or whitespace"
        );
        assert_eq!(
            JobIdError::DotSegment { value: ".." }.to_string(),
            r#"a job id of ".." is a relative path segment and is removed before the request is sent"#
        );
    }
}
