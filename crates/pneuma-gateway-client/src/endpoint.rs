//! Request paths, with the id encoded rather than interpolated.

use crate::job::JobId;

/// Every endpoint here sits under this prefix.
///
/// The gateway nests its v1 router at this path,
/// and the original client hard-codes the
/// same string.
pub const API_PREFIX: &str = "/pneuma-gateway/api/v1";

/// Percent-encodes one path segment, escaping everything outside RFC 3986's
/// unreserved set (`ALPHA / DIGIT / '-' / '.' / '_' / '~'`).
///
/// Equivalent to the original's `urllib.parse.quote(value, safe="")`. The empty
/// `safe` is the whole point: the default leaves `/` alone, which is the
/// character that breaks the route.
///
/// Operates on UTF-8 bytes, so a non-ASCII id becomes its percent-encoded
/// bytes — `ü` is `%C3%BC`, not one escape.
pub fn percent_encode_segment(value: &str) -> String {
    // Most ids need no escaping at all, so start at the input's length.
    let mut out = String::with_capacity(value.len());
    for byte in value.as_bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                out.push(*byte as char);
            }
            _ => {
                out.push('%');
                out.push(char::from(HEX[usize::from(byte >> 4)]));
                out.push(char::from(HEX[usize::from(byte & 0x0f)]));
            }
        }
    }
    out
}

/// Uppercase hex, matching `quote`. RFC 3986 says encoders should produce
/// uppercase, and a differential test pins that we do.
const HEX: &[u8; 16] = b"0123456789ABCDEF";

/// A request path, built rather than formatted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Endpoint(String);

impl Endpoint {
    /// `POST /pneuma-gateway/api/v1/terminations` — create a termination.
    ///
    /// The job id travels in the JSON body
    /// ([`crate::CreateTerminationRequest`]), not the path, so there is nothing
    /// to encode here.
    pub fn create_termination() -> Endpoint {
        Endpoint(format!("{API_PREFIX}/terminations"))
    }

    /// `DELETE /pneuma-gateway/api/v1/terminations/by-job/{job_id}`.
    ///
    /// This is the one the defect notes are about. The id is encoded,
    /// so it stays one path segment whatever it contains.
    pub fn terminations_by_job(job_id: &JobId) -> Endpoint {
        Endpoint(format!(
            "{API_PREFIX}/terminations/by-job/{}",
            percent_encode_segment(job_id.as_str())
        ))
    }

    /// The path as a string.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for Endpoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn job(value: &str) -> JobId {
        match JobId::new(value) {
            Ok(id) => id,
            Err(err) => panic!("{value:?} should be a valid job id: {err}"),
        }
    }

    #[test]
    fn encoding_matches_reference_quote_with_safe_empty() {
        // Generated from `urllib.parse.quote(value, safe="")`, which is the fix
        // the defect notes prescribe for the original client. Both sides
        // of that fix must agree or the two clients address different rows.
        for (input, expected) in [
            ("plain-123", "plain-123"),
            ("with/slash", "with%2Fslash"),
            ("with space", "with%20space"),
            ("with?query", "with%3Fquery"),
            ("with#frag", "with%23frag"),
            ("with%pct", "with%25pct"),
            ("../runs", "..%2Fruns"),
            ("with:colon", "with%3Acolon"),
            ("wíth-ünicode", "w%C3%ADth-%C3%BCnicode"),
            ("a.b_c-d~e", "a.b_c-d~e"),
            ("with+plus", "with%2Bplus"),
            ("with&amp", "with%26amp"),
            ("with=eq", "with%3Deq"),
            ("with@at", "with%40at"),
        ] {
            assert_eq!(percent_encode_segment(input), expected, "input {input:?}");
        }
    }

    #[test]
    fn exactly_the_rfc3986_unreserved_set_survives_unescaped() {
        // the original's quote(safe="") leaves 66 ASCII characters alone: the letters,
        // the digits, and `-._~`. Checking all 128 rather than spot-checking, so
        // a single character drifting into or out of the set cannot pass.
        let unreserved: String = (0u8..128)
            .map(char::from)
            .filter(|c| percent_encode_segment(&c.to_string()).len() == 1)
            .collect();
        assert_eq!(
            unreserved,
            "-.0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ_abcdefghijklmnopqrstuvwxyz~"
        );
        assert_eq!(unreserved.len(), 66);
    }

    #[test]
    fn escapes_are_uppercase_and_two_digits() {
        // RFC 3986 says produce uppercase; `quote` does too. A lowercase escape
        // would still decode correctly, so only a test catches the drift.
        assert_eq!(percent_encode_segment("\x0b"), "%0B");
        assert_eq!(percent_encode_segment("\x7f"), "%7F");
        assert_eq!(percent_encode_segment("\u{0}"), "%00");
        // Every escaped byte becomes exactly three characters: `%` and two hex
        // digits. Checked over real scalar values, and asserted as an exact
        // count rather than a divisibility test.
        //
        // An earlier version looped `for byte in 0u8..=255` and encoded
        // `String::from_utf8_lossy(&[byte])`, which maps every byte above 0x7F
        // to U+FFFD -- so 128 of those iterations tested the same replacement
        // character, and the assertion was `len() == 1 || len() % 3 == 0`,
        // which a two-characters-per-escape bug would also satisfy.
        for scalar in ['\u{0}', '\u{7f}', 'é', 'ü', '€', '\u{10348}', 'A', '~'] {
            let text = scalar.to_string();
            let encoded = percent_encode_segment(&text);
            let unreserved = scalar.is_ascii_alphanumeric() || "-._~".contains(scalar);
            let expected = if unreserved { 1 } else { text.len() * 3 };
            assert_eq!(
                encoded.len(),
                expected,
                "{scalar:?} is {} UTF-8 bytes and encoded to {encoded:?}",
                text.len()
            );
        }
        // Spot-check the multi-byte forms against the original's quote().
        assert_eq!(percent_encode_segment("€"), "%E2%82%AC");
        assert_eq!(percent_encode_segment("\u{10348}"), "%F0%90%8D%88");
    }

    #[test]
    fn the_by_job_path_keeps_the_id_in_one_segment() {
        // The whole point. Each of these breaks the route when interpolated
        // raw -- see the defect notes for what each does.
        for hostile in ["with/slash", "with?query", "with#frag", "../runs", "a b"] {
            let path = Endpoint::terminations_by_job(&job(hostile));
            let after = path
                .as_str()
                .strip_prefix("/pneuma-gateway/api/v1/terminations/by-job/")
                .unwrap_or_else(|| panic!("prefix should be present: {path}"));
            assert!(!after.contains('/'), "{hostile:?} produced {path}");
            assert!(!after.contains('?'), "{hostile:?} produced {path}");
            assert!(!after.contains('#'), "{hostile:?} produced {path}");
            assert_eq!(path.as_str().matches('/').count(), 6, "{path}");
        }
    }

    #[test]
    fn the_paths_match_the_gateways_routes() {
        assert_eq!(
            Endpoint::create_termination().to_string(),
            "/pneuma-gateway/api/v1/terminations"
        );
        assert_eq!(
            Endpoint::terminations_by_job(&job("abc-123")).as_str(),
            "/pneuma-gateway/api/v1/terminations/by-job/abc-123"
        );
        assert_eq!(API_PREFIX, "/pneuma-gateway/api/v1");
    }

    proptest::proptest! {
        /// Whatever the id, the encoded segment introduces no delimiter. Stated
        /// over arbitrary input rather than the hostile cases I thought of.
        #[test]
        fn no_id_can_introduce_a_path_or_query_delimiter(
            raw in ".{1,60}",
        ) {
            let Ok(id) = JobId::new(raw.as_str()) else {
                return Ok(());  // blank or control-bearing ids are refused earlier
            };
            let path = Endpoint::terminations_by_job(&id);
            let Some(segment) = path
                .as_str()
                .strip_prefix("/pneuma-gateway/api/v1/terminations/by-job/")
            else {
                panic!("prefix should be present");
            };
            proptest::prop_assert!(!segment.contains('/'));
            proptest::prop_assert!(!segment.contains('?'));
            proptest::prop_assert!(!segment.contains('#'));
            proptest::prop_assert!(segment.is_ascii());
        }
    }
}
