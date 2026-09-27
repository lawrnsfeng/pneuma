//! [`Subject`] and [`SubjectToken`] — a subject that cannot be malformed.
//!
//! # The defect this exists to remove
//!
//! `tenant_id` reaches three subject-building sites verbatim, and nothing
//! checks it against the subject grammar:
//!
//! ```go
//! fmt.Sprintf("%s.tenant_%s", topic, message.Meta.TenantId)       // the original
//! fmt.Sprintf("%s.tenant_%s", topic, message.Meta.TenantId)       // the original
//! fmt.Sprintf("%s.%s.%s.tenant_%s", Type, Level, Name, TenantId)  // the original
//! ```
//!
//! The only validation anywhere is `Meta.IsValid()`, which tests
//! `JobId != "" && TenantId != ""` — non-emptiness, not the grammar. original is
//! looser still: `tenant_id: str` with no constraint.
//!
//! NATS subjects are dot-delimited token sequences, so a tenant named
//! `acme.eu` produces `<topic>.tenant_acme.eu` — four tokens where the system
//! meant three, with `tenant_acme` and `eu` as separate tokens. It is recorded
//! as the defect notes; this crate is the fix, applied at the type
//! rather than at each of the three call sites.
//!
//! Today's data would not have tripped it: the real sample carries
//! `tenant_id: "default"`. That is an argument about current data, and the
//! trap fires on onboarding a tenant rather than under load — the worse time
//! to find it.
//!
//! # What a token may not contain
//!
//! - **`.`** — the delimiter. A token containing one silently becomes two.
//! - **whitespace** — NATS rejects it, so the publish fails rather than
//!   misroutes.
//! - **`*` and `>`** — the wildcards. Literal on publish, but a subject that
//!   *looks* like a subscription pattern is a trap for whoever reads it next,
//!   and `>` in particular would match far more than intended if it were ever
//!   copied into a subscription.
//! - **empty** — `a..b` is not a valid subject.
//!
//! # Not the same shape as `slug` or `flow_key`
//!
//! Those two escape their inputs, because they must accept arbitrary values and
//! produce a collision-free identifier. This one **rejects** instead, because
//! the subject is a shared namespace with an existing grammar: escaping a `.`
//! would produce a subject no consumer subscribes to, which is the very failure
//! being prevented. Rejecting at the boundary is the only option that keeps the
//! value usable.

use compact_str::CompactString;

/// The delimiter between subject tokens.
const SEPARATOR: char = '.';

/// The prefix NATS-side code puts before a tenant id.
const TENANT_PREFIX: &str = "tenant_";

/// A single NATS subject token.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SubjectToken(CompactString);

impl SubjectToken {
    /// Builds a token, rejecting anything the subject grammar forbids.
    pub fn new(value: impl Into<CompactString>) -> Result<Self, SubjectError> {
        let value = value.into();
        if value.is_empty() {
            return Err(SubjectError::EmptyToken);
        }
        let invalid = value.chars().find(|c| {
            *c == SEPARATOR || c.is_whitespace() || *c == '*' || *c == '>' || c.is_control()
        });
        if let Some(character) = invalid {
            let token = value.to_string();
            return Err(SubjectError::InvalidCharacter { token, character });
        }
        Ok(SubjectToken(value))
    }

    /// The token as a string.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// A validated NATS subject: one or more tokens joined by `.`.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Subject(CompactString);

impl Subject {
    /// Parses a whole subject to **publish to**, validating every token.
    ///
    /// This is how a configured destination — a step's `name`, or the
    /// bootstrap subject from the environment — enters the system. It rejects
    /// wildcards, which is right for a destination and wrong for a
    /// subscription: use [`SubjectPattern`] for those. An earlier revision
    /// claimed this was also the entry point for stream subjects, which it
    /// cannot be, since a stream's subject filter is a pattern.
    pub fn parse(value: &str) -> Result<Self, SubjectError> {
        if value.is_empty() {
            return Err(SubjectError::EmptySubject);
        }
        for token in value.split(SEPARATOR) {
            SubjectToken::new(token)?;
        }
        Ok(Subject(CompactString::from(value)))
    }

    /// Builds a subject from tokens.
    pub fn from_tokens(tokens: &[SubjectToken]) -> Result<Self, SubjectError> {
        if tokens.is_empty() {
            return Err(SubjectError::EmptySubject);
        }
        let mut joined = String::new();
        for (index, token) in tokens.iter().enumerate() {
            if index > 0 {
                joined.push(SEPARATOR);
            }
            joined.push_str(token.as_str());
        }
        Ok(Subject(CompactString::from(joined)))
    }

    /// Appends `.tenant_<id>`, the form the original broker publishes to.
    ///
    /// Takes a [`SubjectToken`] rather than a string, so a tenant id that would
    /// break the subject cannot reach this function at all — which is the whole
    /// point, and the difference from the three `Sprintf` sites it replaces.
    pub fn tenant_scoped(&self, tenant: &SubjectToken) -> Subject {
        Subject(CompactString::from(format!(
            "{}{SEPARATOR}{TENANT_PREFIX}{}",
            self.0,
            tenant.as_str()
        )))
    }

    /// The subject as a string.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// How many tokens the subject has.
    ///
    /// Token count is what a *pattern* matches against: `work.kv.*` matches
    /// `work.kv.tenant_acme` and not `work.kv.tenant_acme.eu`.
    ///
    /// **Where that actually bites in this system is narrower than an earlier
    /// version of this doc claimed.** It said a subject gaining a token "stops
    /// being delivered to the subscription meant to catch it". No such
    /// subscription was found on any verified path: the original executor's consumer uses a
    /// literal `FilterSubject` discovered from stream state,
    /// and broker filters on the
    /// unscoped topic. Both sides also build the
    /// subject with the same `%s.tenant_%s` format, so a dotted tenant id
    /// routes *consistently* through them.
    ///
    /// The token-count-sensitive point is the work stream's capture filter,
    /// `Subjects: []string{config.Config.MatchPattern}`,
    /// whose `MATCH_PATTERN` defaults to `""` and is
    /// set per deployment. If it is token-counted — `a.b.*` rather than `a.b.>`
    /// — a subject with an extra token falls outside the stream and the publish
    /// fails loudly rather than misrouting silently.
    ///
    /// So the defect notes stands, and its consequence depends on that
    /// one setting. This method exists to make the count inspectable; it does
    /// not by itself establish a delivery failure.
    pub fn token_count(&self) -> usize {
        self.0.split(SEPARATOR).count()
    }
}

impl std::fmt::Display for Subject {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// A subject **pattern**, for subscribing and for stream subject filters.
///
/// Distinct from [`Subject`] because the two grammars differ: a destination may
/// not contain wildcards, and a pattern may. Collapsing them would mean either
/// rejecting `pneuma.>` — forcing a topology reconciler back to unvalidated
/// `format!`, which is what this module exists to remove — or admitting a
/// wildcard into a published subject, where NATS treats it as a literal and the
/// message goes somewhere nobody is listening.
///
/// # The wildcard rules, checked against a real server
///
/// `*` matches exactly one token and **must be a whole token**. Measured
/// against NATS 2.x: `work.kv.*` receives a message published to
/// `work.kv.tenant_acme`, and `work.kv.tenant_*` receives **nothing**.
///
/// NATS accepts `tenant_*` as a subscription — it just treats it as a literal
/// token that happens to contain an asterisk, and so matches nothing unless a
/// publisher literally sends to `tenant_*`. **This type rejects it**, which is
/// deliberately stricter than the server.
///
/// The reasoning: a literal `*` inside a token is almost always a wildcard
/// someone spelled wrong, and NATS's response to that mistake is silence — the
/// subscription is created, receives nothing, and reports no error. That is the
/// same class of silent misrouting this module exists to prevent, so the
/// mistake is worth a startup failure that names the fix.
/// [`SubjectError::PartialWildcard`] says what to write instead.
///
/// `>` matches one or more tokens and is only legal as the final token.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SubjectPattern(CompactString);

impl SubjectPattern {
    /// Parses a subscription pattern.
    pub fn parse(value: &str) -> Result<Self, SubjectError> {
        if value.is_empty() {
            return Err(SubjectError::EmptySubject);
        }
        let tokens: Vec<&str> = value.split(SEPARATOR).collect();
        let last = tokens.len() - 1;
        for (index, token) in tokens.iter().enumerate() {
            if *token == ">" {
                if index != last {
                    return Err(SubjectError::TrailingWildcardNotLast);
                }
            } else if *token != "*" {
                // A token that contains a wildcard without being one is almost
                // certainly a mistyped wildcard, and the generic "not allowed
                // in a subject" message would steer the reader away from the
                // pattern they actually want.
                if let Some(wildcard) = token.chars().find(|c| *c == '*' || *c == '>') {
                    let suggestion = if wildcard == '>' {
                        "`>` as the final token, matching one or more tokens"
                    } else {
                        "`*` as a whole token, matching exactly one"
                    };
                    return Err(SubjectError::PartialWildcard {
                        token: (*token).to_owned(),
                        wildcard,
                        suggestion,
                    });
                }
                SubjectToken::new(*token)?;
            }
        }
        Ok(SubjectPattern(CompactString::from(value)))
    }

    /// The pattern as a string.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// JetStream's own API namespace, which a stream subject may not reach
    /// into unless the stream sets `no_ack`.
    ///
    /// Not a `const` because [`SubjectPattern`] wraps a `CompactString`.
    pub fn jetstream_api() -> SubjectPattern {
        SubjectPattern(CompactString::from("$JS.>"))
    }

    /// Whether some subject exists that both patterns would match.
    ///
    /// This is not "one pattern matches the other" — `a.*` and `*.b` match
    /// neither each other nor anything the other does textually, yet both
    /// match `a.b`, so they overlap. Containment is the special case.
    ///
    /// JetStream cares because it refuses to create a stream whose own subject
    /// list overlaps itself. Verified against `nats:2`:
    ///
    /// ```text
    /// $ nats stream add dup --subjects "a.b,a.b"
    /// error: subject "a.b" overlaps with "a.b" (10052)
    /// $ nats stream add ovl --subjects "x.*,x.b"
    /// error: subject "x.*" overlaps with "x.b" (10052)
    /// ```
    ///
    /// The distinct code `10065` is the cross-stream case — a subject already
    /// claimed by a *different* stream — which no single topology can see.
    pub fn overlaps(&self, other: &SubjectPattern) -> bool {
        fn go(left: &[&str], right: &[&str]) -> bool {
            // `>` first: it matches one or more tokens, so it is satisfiable
            // against any non-empty remainder and against nothing else. Testing
            // it before the empty cases is what makes `a.>` and `a` correctly
            // *not* overlap -- `>` still demands a token that `a` has spent.
            if left.first() == Some(&">") {
                return !right.is_empty();
            }
            if right.first() == Some(&">") {
                return !left.is_empty();
            }
            match (left.split_first(), right.split_first()) {
                (None, None) => true,
                (None, Some(_)) | (Some(_), None) => false,
                (Some((head, tail)), Some((other_head, other_tail))) => {
                    // `*` matches exactly one token, so it agrees with any
                    // single token including another `*`.
                    (*head == "*" || *other_head == "*" || head == other_head)
                        && go(tail, other_tail)
                }
            }
        }

        let left: Vec<&str> = self.0.split(SEPARATOR).collect();
        let right: Vec<&str> = other.0.split(SEPARATOR).collect();
        go(&left, &right)
    }
}

impl std::fmt::Display for SubjectPattern {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// Why a subject or token was rejected.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SubjectError {
    /// A token was blank — `a..b`, or a leading or trailing `.`.
    #[error("a subject token must not be empty")]
    EmptyToken,
    /// The whole subject was blank.
    #[error("a subject must have at least one token")]
    EmptySubject,
    /// A token contained a wildcard character without being a wildcard.
    ///
    /// The message names the wildcard that was actually typed, because the two
    /// mean different things: someone who wrote `a.b>c` meaning `a.b.>` wants
    /// "one or more tokens", and telling them to write `*` would silently
    /// narrow that to exactly one.
    #[error(
        "subject token {token:?} contains {wildcard:?} but is not a wildcard; \
         wildcards are only wildcards as whole tokens, so write {suggestion} \
         instead"
    )]
    PartialWildcard {
        /// The offending token.
        token: String,
        /// The wildcard character found inside it.
        wildcard: char,
        /// What to write instead, phrased for that wildcard.
        suggestion: &'static str,
    },
    /// `>` appeared somewhere other than the final token.
    #[error("the `>` wildcard is only allowed as the last token of a pattern")]
    TrailingWildcardNotLast,
    /// A token contained something the grammar forbids.
    #[error("subject token {token:?} contains {character:?}, which is not allowed in a subject")]
    InvalidCharacter {
        /// The offending token.
        token: String,
        /// The first disallowed character.
        character: char,
    },
}

#[cfg(test)]
mod tests {
    /// Whether a pattern matches one concrete subject. Deliberately naive and
    /// written only for the oracle below -- it is the definition of matching,
    /// so the fast `overlaps` can be checked against it.
    fn matches_concrete(pattern: &str, subject: &str) -> bool {
        fn go(pattern: &[&str], subject: &[&str]) -> bool {
            if pattern.first() == Some(&">") {
                return !subject.is_empty();
            }
            match (pattern.split_first(), subject.split_first()) {
                (None, None) => true,
                (None, Some(_)) | (Some(_), None) => false,
                (Some((head, tail)), Some((token, rest))) => {
                    (*head == "*" || head == token) && go(tail, rest)
                }
            }
        }
        go(
            &pattern.split('.').collect::<Vec<_>>(),
            &subject.split('.').collect::<Vec<_>>(),
        )
    }

    /// Every concrete subject over {a, b} of one to three tokens.
    fn concrete_universe() -> Vec<String> {
        let alphabet = ["a", "b"];
        let mut all = Vec::new();
        for first in alphabet {
            all.push(first.to_owned());
            for second in alphabet {
                all.push(format!("{first}.{second}"));
                for third in alphabet {
                    all.push(format!("{first}.{second}.{third}"));
                }
            }
        }
        all
    }

    /// Every pattern over {a, b, *} of one to three tokens, plus the `>` forms.
    fn pattern_universe() -> Vec<String> {
        let alphabet = ["a", "b", "*"];
        let mut all = vec![">".to_owned()];
        for first in alphabet {
            all.push(first.to_owned());
            all.push(format!("{first}.>"));
            for second in alphabet {
                all.push(format!("{first}.{second}"));
                all.push(format!("{first}.{second}.>"));
                for third in alphabet {
                    all.push(format!("{first}.{second}.{third}"));
                }
            }
        }
        all
    }

    #[test]
    fn the_jetstream_api_pattern_matches_what_the_parser_would_build() {
        // jetstream_api() constructs the pattern directly rather than going
        // through parse(), to avoid a Result on a constant. That bypass is only
        // safe while the two agree, so pin it.
        let Ok(parsed) = SubjectPattern::parse("$JS.>") else {
            panic!("$JS.> should parse -- `$` is not a forbidden character");
        };
        assert_eq!(SubjectPattern::jetstream_api(), parsed);
        assert_eq!(SubjectPattern::jetstream_api().as_str(), "$JS.>");
    }

    #[test]
    fn overlaps_agrees_with_brute_force_over_every_pattern_pair() {
        // `overlaps` answers "does a subject exist that both match". The
        // definition of that is: enumerate subjects, test both. Doing it the
        // slow way over a closed universe is the only check that does not just
        // restate the implementation.
        let subjects = concrete_universe();
        let patterns = pattern_universe();
        let mut checked = 0usize;
        let mut overlapping = 0usize;

        for left_str in &patterns {
            let Ok(left) = SubjectPattern::parse(left_str) else {
                panic!("{left_str} should parse");
            };
            for right_str in &patterns {
                let Ok(right) = SubjectPattern::parse(right_str) else {
                    panic!("{right_str} should parse");
                };
                // The universe is capped at three tokens, so a `>` pair can
                // agree on a longer subject that is not enumerated. Those are
                // the only cases the oracle cannot speak to.
                let truncation_risk = left_str.ends_with('>') && right_str.ends_with('>');
                let expected = subjects
                    .iter()
                    .any(|s| matches_concrete(left_str, s) && matches_concrete(right_str, s));
                let actual = left.overlaps(&right);
                if !truncation_risk {
                    assert_eq!(
                        actual, expected,
                        "{left_str} vs {right_str}: brute force says {expected}"
                    );
                    checked += 1;
                }
                if actual {
                    overlapping += 1;
                }
            }
        }

        // Guard the guard: a bug that made the universes empty would let the
        // loop above pass vacuously.
        assert!(checked > 2000, "only {checked} pairs compared");
        assert!(overlapping > 0 && overlapping < patterns.len() * patterns.len());
    }

    #[test]
    fn overlaps_is_symmetric_and_reflexive() {
        let patterns = pattern_universe();
        for left_str in &patterns {
            let Ok(left) = SubjectPattern::parse(left_str) else {
                panic!("should parse");
            };
            // Every pattern matches something, so it overlaps itself. This is
            // the duplicate-subject case JetStream rejects with 10052.
            assert!(left.overlaps(&left), "{left_str} should overlap itself");
            for right_str in &patterns {
                let Ok(right) = SubjectPattern::parse(right_str) else {
                    panic!("should parse");
                };
                assert_eq!(
                    left.overlaps(&right),
                    right.overlaps(&left),
                    "{left_str} vs {right_str} is asymmetric"
                );
            }
        }
    }

    #[test]
    fn overlap_cases_that_pin_the_wildcard_rules() {
        let overlap = |a: &str, b: &str| {
            let (Ok(left), Ok(right)) = (SubjectPattern::parse(a), SubjectPattern::parse(b)) else {
                panic!("{a} / {b} should parse");
            };
            left.overlaps(&right)
        };

        // The two transcripts from the live server.
        assert!(overlap("a.b", "a.b"), "10052: exact duplicate");
        assert!(overlap("x.*", "x.b"), "10052: wildcard over literal");

        // Neither matches the other, both match `a.b`. This is the case a
        // containment check would miss.
        assert!(overlap("a.*", "*.b"));

        // `>` needs at least one token, so it does not reach the empty tail.
        assert!(!overlap("a.>", "a"));
        assert!(overlap("a.>", "a.b"));
        assert!(overlap("a.>", "a.b.c.d"));

        // `>` against a longer prefix, on both sides.
        assert!(overlap(">", "a.b.c"));
        assert!(overlap("a.b.>", "a.>"));
        assert!(!overlap("a.>", "b.>"));

        // Same length, no common subject.
        assert!(!overlap("a.b", "a.c"));
        // Different length without a `>` can never agree.
        assert!(!overlap("a.*", "a.b.c"));
        assert!(!overlap("a.b.c", "a.*"));
        // `*` agrees with `*`.
        assert!(overlap("a.*", "a.*"));
    }

    use proptest::prelude::*;

    use super::*;

    fn token(value: &str) -> SubjectToken {
        match SubjectToken::new(value) {
            Ok(token) => token,
            Err(err) => panic!("{value:?} should be a valid token: {err}"),
        }
    }

    fn subject(value: &str) -> Subject {
        match Subject::parse(value) {
            Ok(subject) => subject,
            Err(err) => panic!("{value:?} should be a valid subject: {err}"),
        }
    }

    #[test]
    fn a_real_dispatch_subject_parses() {
        // From the original -- a step name is itself
        // multi-token, which is why the base subject is a sequence and only the
        // tenant id must be a single token.
        let dispatch = subject("freeform.page.commercial-invoice.kv");
        assert_eq!(dispatch.token_count(), 4);
        assert_eq!(dispatch.as_str(), "freeform.page.commercial-invoice.kv");
    }

    #[test]
    fn tenant_scoping_matches_the_go_format() {
        // fmt.Sprintf("%s.tenant_%s", topic, message.Meta.TenantId)
        let scoped = subject("freeform.page.kv").tenant_scoped(&token("acme"));
        assert_eq!(scoped.as_str(), "freeform.page.kv.tenant_acme");
        assert_eq!(scoped.token_count(), 4);
    }

    #[test]
    fn the_queue_in_form_matches_too() {
        // Meta.GetQueueIn: "%s.%s.%s.tenant_%s"
        let base = match Subject::from_tokens(&[token("llm"), token("doc"), token("default")]) {
            Ok(base) => base,
            Err(err) => panic!("should build: {err}"),
        };
        let scoped = base.tenant_scoped(&token("default"));
        assert_eq!(scoped.as_str(), "llm.doc.default.tenant_default");
    }

    #[test]
    fn a_dotted_tenant_id_cannot_reach_a_subject() {
        // The defect notes record this: in the original code it produces
        // "<topic>.tenant_acme.eu" -- four tokens where three were meant --
        // and nothing anywhere says so. Here it cannot be built at all.
        assert_eq!(
            SubjectToken::new("acme.eu"),
            Err(SubjectError::InvalidCharacter {
                token: "acme.eu".to_owned(),
                character: '.',
            })
        );
    }

    #[test]
    fn the_token_count_is_what_a_wildcard_matches() {
        // Token counting itself, verified against NATS 2.x: `work.kv.*`
        // receives a message published to `work.kv.tenant_acme` and would not
        // receive one with an extra token. Whether that costs anything in this
        // system depends on MATCH_PATTERN -- see `token_count`'s docs, which
        // record that the "subscription meant to catch it" framing was not
        // established on any real path.
        //
        // An earlier version of this test wrote that subscription as
        // `work.kv.tenant_*`, which receives nothing at all -- `*` must be a
        // whole token, so that is a literal token spelled `tenant_*`. The
        // conclusion was right and the example could never have matched.
        let good = subject("work.kv").tenant_scoped(&token("acme"));
        assert_eq!(good.token_count(), 3);

        // The shape the original code would produce, if it could be built here.
        let leaked = subject("work.kv.tenant_acme.eu");
        assert_eq!(leaked.token_count(), 4, "one extra token is the failure");
        assert_ne!(good.token_count(), leaked.token_count());

        // And the pattern that would have matched the first but not the second.
        let matching = match SubjectPattern::parse("work.kv.*") {
            Ok(pattern) => pattern,
            Err(err) => panic!("should parse: {err}"),
        };
        assert_eq!(matching.as_str(), "work.kv.*");
    }

    #[test]
    fn whitespace_is_rejected_because_nats_rejects_it() {
        for bad in ["a b", "a\tb", "a\nb", " a", "a "] {
            assert!(
                SubjectToken::new(bad).is_err(),
                "{bad:?} should be rejected"
            );
        }
    }

    #[test]
    fn wildcards_are_rejected_in_a_published_subject() {
        // Literal on publish, but a subject that looks like a subscription
        // pattern is a trap for the next reader -- and `>` copied into a
        // subscription would match far more than intended.
        assert!(SubjectToken::new("*").is_err());
        assert!(SubjectToken::new(">").is_err());
        assert!(SubjectToken::new("tenant_*").is_err());
        assert!(SubjectToken::new("a>b").is_err());
    }

    #[test]
    fn control_characters_are_rejected() {
        assert!(SubjectToken::new("a\0b").is_err());
        assert!(SubjectToken::new("a\rb").is_err());
    }

    #[test]
    fn empty_tokens_are_rejected_wherever_they_appear() {
        assert_eq!(SubjectToken::new(""), Err(SubjectError::EmptyToken));
        assert_eq!(Subject::parse(""), Err(SubjectError::EmptySubject));
        // A leading, trailing, or doubled separator all yield an empty token.
        for bad in [".a", "a.", "a..b"] {
            assert_eq!(
                Subject::parse(bad),
                Err(SubjectError::EmptyToken),
                "{bad:?} should be rejected"
            );
        }
        assert_eq!(Subject::from_tokens(&[]), Err(SubjectError::EmptySubject));
    }

    #[test]
    fn a_single_token_is_a_valid_subject() {
        let one = subject("bootstrap");
        assert_eq!(one.token_count(), 1);
        assert_eq!(one.to_string(), "bootstrap");
    }

    #[test]
    fn the_real_pneuma_subjects_all_parse() {
        // Every subject the system actually uses, the original and
        // the original.
        for value in [
            "pneuma.input",
            "pneuma.event",
            "pneuma.pipeline.create",
            "pneuma.run.start",
            "pneuma.result",
            "pneuma.retry",
            "pneuma.dlq",
            "pneuma.termination",
            "freeform.page.commercial-invoice.kv",
        ] {
            assert!(Subject::parse(value).is_ok(), "{value} should parse");
        }
    }

    #[test]
    fn a_pattern_accepts_the_wildcards_a_destination_rejects() {
        // The split that makes a topology reconciler possible without dropping
        // back to an unvalidated format!.
        for value in ["pneuma.>", "work.kv.*", "a.*.c", ">", "*"] {
            assert!(
                SubjectPattern::parse(value).is_ok(),
                "{value} should be a valid pattern"
            );
            assert!(
                Subject::parse(value).is_err(),
                "{value} must not be publishable"
            );
        }
    }

    #[test]
    fn the_trailing_wildcard_must_be_last() {
        // `>` matches one or more tokens, so anything after it is unreachable.
        assert_eq!(
            SubjectPattern::parse("a.>.c"),
            Err(SubjectError::TrailingWildcardNotLast)
        );
        assert!(SubjectPattern::parse("a.b.>").is_ok());
        assert_eq!(
            SubjectError::TrailingWildcardNotLast.to_string(),
            "the `>` wildcard is only allowed as the last token of a pattern"
        );
    }

    #[test]
    fn a_pattern_still_rejects_what_is_not_a_wildcard() {
        // Leniency is exactly the two wildcard tokens, not a general escape.
        for bad in ["a.ten ant", "a..b", "", "a.b\u{0}c"] {
            assert!(
                SubjectPattern::parse(bad).is_err(),
                "{bad:?} should be rejected"
            );
        }
        // A partial wildcard is rejected -- stricter than NATS, which would
        // accept it as a literal and then silently match nothing. The previous
        // version of this assertion used "a.tenant_star", which contains no
        // wildcard at all, so it pinned neither behaviour.
        assert_eq!(
            SubjectPattern::parse("a.tenant_*"),
            Err(SubjectError::PartialWildcard {
                token: "tenant_*".to_owned(),
                wildcard: '*',
                suggestion: "`*` as a whole token, matching exactly one",
            })
        );
        // The exact variant, not merely "some error" -- the weakness this
        // test's own comment criticises in the assertion it replaced.
        //
        // This assertion is also the ONLY thing pinning that `parse` chooses
        // the suggestion matching the wildcard it found. The dedicated
        // diagnostic test below builds the error by hand, so it exercises the
        // `Display` impl and would pass unchanged if `parse` always suggested
        // `*`. Verified by making it do exactly that: this test fails, that one
        // does not.
        assert_eq!(
            SubjectPattern::parse("a.b>c"),
            Err(SubjectError::PartialWildcard {
                token: "b>c".to_owned(),
                wildcard: '>',
                suggestion: "`>` as the final token, matching one or more tokens",
            })
        );
        // A token with no wildcard character is still fine.
        assert!(SubjectPattern::parse("a.tenant_star").is_ok());
    }

    #[test]
    fn pattern_accessors_and_derives() {
        let Ok(pattern) = SubjectPattern::parse("a.>") else {
            panic!("should parse");
        };
        assert_eq!(pattern.as_str(), "a.>");
        assert_eq!(pattern.to_string(), "a.>");
        assert_eq!(pattern.clone(), pattern);
        assert!(format!("{pattern:?}").contains("a.>"));

        use std::collections::HashSet;
        let set: HashSet<_> = [pattern.clone(), pattern].into();
        assert_eq!(set.len(), 1);
    }

    #[test]
    fn the_partial_wildcard_error_names_the_pattern_to_write_instead() {
        // The diagnostic is the only guidance this API gives, and the generic
        // "not allowed in a subject" message read as "wildcards are
        // unsupported" -- steering a reader away from `work.kv.*`, which is
        // exactly what they want.
        let star = SubjectError::PartialWildcard {
            token: "tenant_*".to_owned(),
            wildcard: '*',
            suggestion: "`*` as a whole token, matching exactly one",
        }
        .to_string();
        assert!(star.contains("tenant_*"), "{star}");
        assert!(star.contains("whole tokens"), "{star}");
        assert!(star.contains("exactly one"), "{star}");

        // A `>` typo must not be told to write `*`, which would narrow
        // "one or more tokens" to exactly one.
        let arrow = SubjectError::PartialWildcard {
            token: "b>c".to_owned(),
            wildcard: '>',
            suggestion: "`>` as the final token, matching one or more tokens",
        }
        .to_string();
        assert!(arrow.contains("one or more"), "{arrow}");
        assert!(!arrow.contains("exactly one"), "{arrow}");
    }

    #[test]
    fn a_destination_still_rejects_a_partial_wildcard_too() {
        // Subject::parse routes through SubjectToken, so it rejects for the
        // grammar reason rather than the guidance reason. Both refuse it.
        assert!(Subject::parse("a.tenant_*").is_err());
        assert!(SubjectToken::new("tenant_*").is_err());
    }

    #[test]
    fn errors_display_usefully() {
        assert_eq!(
            SubjectError::EmptyToken.to_string(),
            "a subject token must not be empty"
        );
        assert_eq!(
            SubjectError::EmptySubject.to_string(),
            "a subject must have at least one token"
        );
        assert_eq!(
            SubjectError::InvalidCharacter {
                token: "acme.eu".to_owned(),
                character: '.',
            }
            .to_string(),
            "subject token \"acme.eu\" contains '.', which is not allowed in a subject"
        );
    }

    #[test]
    fn accessors_and_derives() {
        let t = token("acme");
        assert_eq!(t.as_str(), "acme");
        assert_eq!(t.clone(), t);
        assert!(format!("{t:?}").contains("acme"));

        let s = subject("a.b");
        assert_eq!(s.clone(), s);
        assert_eq!(s.to_string(), s.as_str());
        assert!(format!("{s:?}").contains("a.b"));

        use std::collections::HashSet;
        let set: HashSet<_> = [s.clone(), s].into();
        assert_eq!(set.len(), 1);
    }

    proptest! {
        /// A scoped subject always has exactly one more token than its base.
        ///
        /// This is the property the original code violates: splicing an unvalidated
        /// tenant id in can add any number of tokens. Since the token type
        /// rejects separators, the count here is arithmetic — the generator is
        /// deliberately heavy in the characters that would break it, so a
        /// weakened validator shows up rather than being argued away.
        #[test]
        fn scoping_adds_exactly_one_token(
            base in "[a-b]{1,3}(\\.[a-b]{1,3}){0,3}",
            tenant in "[a-b._* >]{1,5}",
        ) {
            let Ok(base) = Subject::parse(&base) else {
                return Ok(());
            };
            let before = base.token_count();
            match SubjectToken::new(tenant.as_str()) {
                Ok(tenant) => {
                    let scoped = base.tenant_scoped(&tenant);
                    prop_assert_eq!(scoped.token_count(), before + 1);
                }
                // Rejected inputs are the point: they are exactly the ones that
                // would have added more than one token.
                Err(_) => prop_assert!(
                    tenant.contains('.')
                        || tenant.contains(char::is_whitespace)
                        || tenant.contains('*')
                        || tenant.contains('>')
                ),
            }
        }
    }
}
