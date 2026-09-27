//! [`StreamTopology`] — what a JetStream stream should look like, and what to
//! do when it doesn't.
//!
//! # Why this is a type rather than a call
//!
//! Three services independently ensure the termination stream exists, all three
//! use create-if-absent semantics, and **none of them reconciles what it
//! finds**. Whichever boots first decides the stream's configuration, and the
//! other two log success. The defect notes record the measurements;
//! the summary is that three fields diverge, each in a different direction:
//!
//! | Field | the original | `pneuma-gateway` | the original executor |
//! |---|---|---|---|
//! | `max_age` | env | env | hardcoded 7d |
//! | `retention` | explicit Limits | Limits by default | env |
//! | `replicas` | unset (1) | unset (1) | env |
//!
//! No single service reads all three. That is why it went unnoticed — each one
//! is internally consistent, and only the combination is wrong.
//!
//! [`StreamTopology::diff`] compares every field **this type models** —
//! subjects, retention, `max_age`, and replicas — rather than the ones a
//! particular caller happens to care about, because caring about a subset is
//! exactly the reasoning error that produced the defect.
//!
//! That is itself a subset of JetStream's `StreamConfig`, and saying so
//! matters: `storage`, `discard`, `max_msgs`, `max_bytes` and
//! `duplicate_window` are **not** compared, so a memory-backed stream where a
//! file-backed one was intended passes [`StreamTopology::matches`]. An earlier
//! revision of this doc claimed it compares "every field", which commits the
//! same error it criticises one paragraph earlier.
//!
//! The four modelled fields are the ones the three services actually set and
//! disagree about. Extending the type is the right move the moment a fifth is
//! configured anywhere; the constraint is knowing which, not adding all of
//! them speculatively.
//!
//! # Stream names are not subject tokens
//!
//! A stream name has its own grammar, checked against a running NATS 2.x
//! server rather than recalled:
//!
//! ```text
//! accepted  pneuma-termination     accepted  has_underscore
//! REJECTED  has.dot                REJECTED  has space
//! REJECTED  has>wild               REJECTED  has*star
//! REJECTED  has/slash
//! ```
//!
//! The `/` is the reason [`SubjectToken`](crate::SubjectToken) cannot be reused
//! here: a subject token accepts `/` happily, so borrowing it would admit a
//! name the server refuses, and the failure would surface at stream-creation
//! time in a deployment rather than at the config boundary. Backslash is
//! rejected on the same grounds — NATS documents path separators together —
//! though only `/` was measured.

use std::time::Duration;

use compact_str::CompactString;

use crate::subject::SubjectPattern;

/// A JetStream stream name.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct StreamName(CompactString);

impl StreamName {
    /// Builds a stream name, rejecting what the server rejects.
    pub fn new(value: impl Into<CompactString>) -> Result<Self, StreamNameError> {
        let value = value.into();
        if value.is_empty() {
            return Err(StreamNameError::Empty);
        }
        let invalid = value.chars().find(|c| {
            *c == '.'
                || *c == '*'
                || *c == '>'
                || *c == '/'
                || *c == '\\'
                || c.is_whitespace()
                || c.is_control()
        });
        if let Some(character) = invalid {
            let name = value.to_string();
            return Err(StreamNameError::InvalidCharacter { name, character });
        }
        Ok(StreamName(value))
    }

    /// The name as a string.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Why a stream name was rejected.
///
/// Separate from [`SubjectError`](crate::SubjectError) because the grammars
/// differ and the diagnostic has to say which one it is talking about. An
/// earlier revision reused the subject error, so rejecting `has/slash` printed
/// "not allowed in a subject" — while `/` **is** allowed in a subject, as this
/// module's own test asserts. An operator was told to go fix a subject, under a
/// rule the crate itself contradicts.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum StreamNameError {
    /// The name was blank.
    #[error("a stream name must not be empty")]
    Empty,
    /// The name contained something the server refuses.
    #[error(
        "stream name {name:?} contains {character:?}; stream names allow neither \
         `.`, whitespace, wildcards, nor path separators — unlike subject \
         tokens, which permit `/`"
    )]
    InvalidCharacter {
        /// The offending name.
        name: String,
        /// The first disallowed character.
        character: char,
    },
}

/// A stream's replica count, which is never zero.
///
/// the original and `pneuma-gateway` pass no replica count and let the server
/// default to 1. the original executor clamps below 1 up to 1 for the **termination** stream
/// — but **not** for the work stream, which
/// passes `config.Config.NatsNumStreamReplicas` raw into `CreateOrUpdateStream`.
/// So an explicit `NATS_NUM_STREAM_REPLICAS=0` gives
/// the two streams different replica counts, and "no path produces a
/// zero-replica stream" — which an earlier revision of this doc asserted — is
/// true only of the termination path.
///
/// A topology carrying `replicas: 0` would still diff as different from any
/// stream the server actually created, so normalising here is right; the point
/// is that the normalisation is this crate's decision rather than something
/// the original executor guarantees everywhere.
///
/// [`Replicas::from_config`] reproduces the original executor's clamp **for non-negative
/// values**. The original executor's field is the original's plain `int`, so its `if replicas < 1` also
/// absorbs negatives; `NATS_NUM_STREAM_REPLICAS="-1"` becomes 1 there and fails
/// to parse as a `u8` here. That is a defensible difference — a negative
/// replica count is a configuration error worth surfacing rather than
/// silently rounding — but it is a difference, so the parity claim is bounded
/// rather than absolute. `u8` also diverges at the top: the original's `int` accepts
/// values above 255 that fail to parse here.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Replicas(u8);

impl Replicas {
    /// The server default, used by the two services that pass no count.
    pub const DEFAULT: Replicas = Replicas(1);

    /// Normalises a configured value, clamping below 1 up to 1.
    pub fn from_config(value: u8) -> Self {
        Replicas(value.max(1))
    }

    /// The count.
    pub fn get(self) -> u8 {
        self.0
    }
}

impl Default for Replicas {
    fn default() -> Self {
        Replicas::DEFAULT
    }
}

/// How a stream decides when to discard messages.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum Retention {
    /// Discard by the configured limits. The default, and what the termination
    /// stream needs: every consumer sees every message.
    #[default]
    Limits,
    /// Discard once all interested consumers have acknowledged.
    Interest,
    /// Each message is delivered to exactly one consumer.
    ///
    /// Wrong for a fan-out. The termination refresh signal goes to every
    /// message-provider instance, so under this policy exactly one instance
    /// would receive each signal and the rest would keep dispatching cancelled
    /// work — see the defect notes
    WorkQueue,
}

/// The configuration a stream is expected to have.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamTopology {
    /// The stream's name.
    pub name: StreamName,
    /// The subject patterns the stream captures.
    ///
    /// **Reading the `actual` side needs care.** [`SubjectPattern::parse`] is
    /// deliberately stricter than the server — it rejects a literal `tenant_*`
    /// that NATS would accept — which is right for configuration input and
    /// wrong for a filter read back from a live stream. A stream created with a
    /// filter NATS accepted but this parser rejects cannot be represented, so a
    /// reconciler would fail with a parse error instead of reporting the
    /// [`Divergence::Subjects`] it exists to surface, losing the diagnostic in
    /// exactly the case it targets.
    ///
    /// A caller building the actual side from a live stream therefore needs a
    /// server-shaped parse, not this one. That constructor is not written yet
    /// because nothing reads streams back yet; this note is here so it is a
    /// decision rather than a surprise.
    pub subjects: Vec<SubjectPattern>,
    /// How messages are discarded.
    pub retention: Retention,
    /// How long messages are kept. `Duration::ZERO` means unlimited, matching
    /// JetStream's own encoding.
    pub max_age: Duration,
    /// How many replicas the stream is stored on.
    pub replicas: Replicas,
}

impl StreamTopology {
    /// Every way `actual` differs from what this topology expects.
    ///
    /// Empty means they agree. A caller that finds anything here should fail at
    /// startup rather than proceed: the current system's failure mode is that
    /// every service logs success while disagreeing.
    ///
    /// The name is not compared — a topology is only ever diffed against the
    /// stream of the same name, so a mismatch there is a lookup bug rather than
    /// a configuration divergence.
    pub fn diff(&self, actual: &StreamTopology) -> Vec<Divergence> {
        let mut found = Vec::new();
        // Compared as a set: each service builds its subject list from its own
        // configuration, so two that agree on the subjects but order them
        // differently are not diverging — and under this type's contract
        // ("fail at startup") a spurious difference takes the deployment down.
        // JetStream itself treats subjects as a set.
        let mut expected_subjects: Vec<&str> =
            self.subjects.iter().map(SubjectPattern::as_str).collect();
        let mut actual_subjects: Vec<&str> =
            actual.subjects.iter().map(SubjectPattern::as_str).collect();
        expected_subjects.sort_unstable();
        actual_subjects.sort_unstable();
        if expected_subjects != actual_subjects {
            found.push(Divergence::Subjects {
                expected: self
                    .subjects
                    .iter()
                    .map(|s| s.as_str().to_owned())
                    .collect(),
                actual: actual
                    .subjects
                    .iter()
                    .map(|s| s.as_str().to_owned())
                    .collect(),
            });
        }
        if self.retention != actual.retention {
            found.push(Divergence::Retention {
                expected: self.retention,
                actual: actual.retention,
            });
        }
        if self.max_age != actual.max_age {
            found.push(Divergence::MaxAge {
                expected: self.max_age,
                actual: actual.max_age,
            });
        }
        if self.replicas != actual.replicas {
            found.push(Divergence::Replicas {
                expected: self.replicas.get(),
                actual: actual.replicas.get(),
            });
        }
        found
    }

    /// Whether `actual` matches this topology in every compared field.
    pub fn matches(&self, actual: &StreamTopology) -> bool {
        self.diff(actual).is_empty()
    }

    /// Checks the topology is one JetStream would accept.
    ///
    /// Every other type in this crate makes its invalid states unconstructable;
    /// this one cannot, because its fields are public so a caller can build it
    /// field-by-field from configuration. The gap that leaves is an empty
    /// subject list: JetStream rejects a stream with no subjects, but two such
    /// topologies [`Self::matches`] each other and [`Self::diff`] reports
    /// nothing — so a reconciler built from a config missing its subjects key
    /// would pass its own check and fail later at `AddStream`.
    ///
    /// It also catches three things JetStream rejects that `diff` cannot see.
    /// All were checked against a real `nats:2` server rather than read off the
    /// documentation, which matters here because the server reports **all
    /// three under one error code**, `10052`, distinguished only by the message
    /// text:
    ///
    /// | message | rule |
    /// |---|---|
    /// | `subject "q.r" overlaps with "q.r"` | the list overlaps itself |
    /// | `subjects that overlap with jetstream api require no-ack` | overlaps `$JS.>` |
    /// | `capturing all subjects requires no-ack to be true` | the subject is `>` |
    ///
    /// The last two are properties of a *single* subject and fire even for a
    /// stream declared with that subject alone — `*.a` is refused by itself,
    /// because its first token matches `$JS`. That is easy to mistake for an
    /// overlap between the two subjects being tested, and it is why the
    /// boundary was pinned case by case:
    ///
    /// ```text
    /// *        accepted   (one token; cannot reach $JS.>, which needs two)
    /// *.*      refused    *.>  refused    *.a  refused    *.*.*  refused
    /// a.*      accepted   a.>  accepted   x.*.* accepted  (first token is not $JS)
    /// $JS.>    refused    $JS.API.foo refused
    /// $JSX.a   accepted   (first token is $JSX, not $JS)
    /// ```
    ///
    /// So the rule is exactly "overlaps `$JS.>`", which
    /// [`SubjectPattern::overlaps`] already decides.
    ///
    /// Both of those are lifted by a stream's `no_ack` flag. This type does not
    /// model `no_ack` — nothing pneuma declares wants it, since a stream that
    /// cannot acknowledge cannot back a durable consumer — so they are treated
    /// as unconditional here. If `no_ack` is ever added, these two checks
    /// become conditional on it and the first does not.
    ///
    /// What this deliberately does *not* check is overlap with a **different**
    /// stream — JetStream's separate error `10065`. That is not a property of
    /// one topology, so it cannot be answered here; it needs the whole set.
    ///
    /// Call this before trusting a topology read from configuration.
    pub fn validate(&self) -> Result<(), TopologyError> {
        if self.subjects.is_empty() {
            return Err(TopologyError::NoSubjects {
                name: self.name.as_str().to_owned(),
            });
        }

        let name = || self.name.as_str().to_owned();
        let jetstream_api = SubjectPattern::jetstream_api();

        // Subjects are considered in order, and each is checked against the
        // ones already accepted before its own two rules are applied. That
        // sequence is not arbitrary -- it is the server's, established by
        // asking which of several simultaneous violations it names first:
        //
        //   a.b,>        -> subject "a.b" overlaps with ">"
        //   >,a.b        -> capturing all subjects requires no-ack to be true
        //   a.b,*.a      -> subjects that overlap with jetstream api ...
        //   a.b,a.b,*.a  -> subject "a.b" overlaps with "a.b"
        //
        // `a.b,>` and `>,a.b` differ only in order and get different
        // diagnoses, which is what pins overlap-against-accepted ahead of the
        // single-subject rules. Matching it means this method names the same
        // violation the operator will see if they push the config anyway.
        for (index, subject) in self.subjects.iter().enumerate() {
            for accepted in &self.subjects[..index] {
                if accepted.overlaps(subject) {
                    return Err(TopologyError::OverlappingSubjects {
                        name: name(),
                        first: accepted.as_str().to_owned(),
                        second: subject.as_str().to_owned(),
                    });
                }
            }
            if subject.as_str() == ">" {
                return Err(TopologyError::CapturesAllSubjects { name: name() });
            }
            if subject.overlaps(&jetstream_api) {
                return Err(TopologyError::OverlapsJetStreamApi {
                    name: name(),
                    subject: subject.as_str().to_owned(),
                });
            }
        }
        Ok(())
    }
}

/// A topology that JetStream would not accept.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum TopologyError {
    /// No subjects. JetStream rejects such a stream, but `diff` cannot: two
    /// empty topologies compare equal.
    #[error("stream {name:?} has no subjects; JetStream requires at least one")]
    NoSubjects {
        /// The stream this topology describes.
        name: String,
    },
    /// Two subjects in one stream's own list match a common subject.
    /// JetStream rejects this with `10052`, but `diff` cannot see it.
    #[error("stream {name:?} subject {first:?} overlaps {second:?}")]
    OverlappingSubjects {
        /// The stream this topology describes.
        name: String,
        /// The earlier of the two overlapping subjects.
        first: String,
        /// The later of the two overlapping subjects.
        second: String,
    },
    /// A subject that reaches into JetStream's own `$JS.>` namespace.
    /// Refused with `10052` unless the stream sets `no_ack`.
    #[error("stream {name:?} subject {subject:?} overlaps JetStream's own $JS.> namespace")]
    OverlapsJetStreamApi {
        /// The stream this topology describes.
        name: String,
        /// The offending subject.
        subject: String,
    },
    /// The capture-all subject `>`. Refused with `10052` unless the stream
    /// sets `no_ack`, and it would swallow every other stream's traffic.
    #[error("stream {name:?} captures all subjects with `>`")]
    CapturesAllSubjects {
        /// The stream this topology describes.
        name: String,
    },
}

/// One field on which an existing stream disagrees with what was expected.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum Divergence {
    #[error("stream subjects are {actual:?}, expected {expected:?}")]
    Subjects {
        /// What the topology asked for.
        expected: Vec<String>,
        /// What the stream has.
        actual: Vec<String>,
    },
    #[error("stream retention is {actual:?}, expected {expected:?}")]
    Retention {
        /// What the topology asked for.
        expected: Retention,
        /// What the stream has.
        actual: Retention,
    },
    #[error("stream max_age is {actual:?}, expected {expected:?}")]
    MaxAge {
        /// What the topology asked for.
        expected: Duration,
        /// What the stream has.
        actual: Duration,
    },
    #[error("stream replicas is {actual}, expected {expected}")]
    Replicas {
        /// What the topology asked for.
        expected: u8,
        /// What the stream has.
        actual: u8,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pattern(value: &str) -> SubjectPattern {
        match SubjectPattern::parse(value) {
            Ok(pattern) => pattern,
            Err(err) => panic!("{value:?} should parse: {err}"),
        }
    }

    fn name(value: &str) -> StreamName {
        match StreamName::new(value) {
            Ok(name) => name,
            Err(err) => panic!("{value:?} should be a valid name: {err}"),
        }
    }

    /// What the termination stream is supposed to be.
    fn expected() -> StreamTopology {
        StreamTopology {
            name: name("pneuma-termination"),
            subjects: vec![pattern("pneuma.termination")],
            retention: Retention::Limits,
            max_age: Duration::from_secs(7 * 24 * 3600),
            replicas: Replicas::from_config(3),
        }
    }

    #[test]
    fn validate_rejects_a_subject_reaching_into_the_jetstream_api() {
        // Every one of these was refused by a live nats:2 server when declared
        // as a stream's ONLY subject -- which is what makes them a property of
        // the subject rather than of a pair, and what made them so easy to
        // misread as overlap failures: the server reports them with the same
        // code 10052 as a genuine self-overlap.
        for bad in [
            "*.*",
            "*.>",
            "*.a",
            "*.*.*",
            "*.*.>",
            "$JS.>",
            "$JS.API.foo",
        ] {
            let mut topology = expected();
            topology.subjects = vec![pattern(bad)];
            let Err(TopologyError::OverlapsJetStreamApi { subject, .. }) = topology.validate()
            else {
                panic!("{bad} reaches $JS.> and should be refused");
            };
            assert_eq!(subject, bad);
        }

        // The boundary. `*` alone is one token and so cannot reach `$JS.>`,
        // which needs two; a first token that is a literal other than `$JS`
        // cannot match it either. All accepted by the live server.
        for good in ["*", "a.*", "a.>", "x.*.*", "$JSX.a", "a.b"] {
            let mut topology = expected();
            topology.subjects = vec![pattern(good)];
            assert_eq!(topology.validate(), Ok(()), "{good} should be accepted");
        }
    }

    #[test]
    fn validate_rejects_the_capture_all_subject() {
        let mut topology = expected();
        topology.subjects = vec![pattern(">")];
        assert_eq!(
            topology.validate(),
            Err(TopologyError::CapturesAllSubjects {
                name: "pneuma-termination".to_owned(),
            })
        );

        // `>` also overlaps `$JS.>`, so both single-subject rules fire; the
        // capture-all diagnosis wins, matching the server.
        let mut topology = expected();
        topology.subjects = vec![pattern(">"), pattern("a.b")];
        assert!(matches!(
            topology.validate(),
            Err(TopologyError::CapturesAllSubjects { .. })
        ));

        // Reversed, the SERVER changes its answer -- `>` now collides with an
        // already-accepted `a.b`, and it reports the overlap instead:
        //     a.b,> -> subject "a.b" overlaps with ">"
        //     >,a.b -> capturing all subjects requires no-ack to be true
        // This pins that this method follows the server rather than imposing a
        // precedence of its own.
        let mut topology = expected();
        topology.subjects = vec![pattern("a.b"), pattern(">")];
        let Err(TopologyError::OverlappingSubjects { first, second, .. }) = topology.validate()
        else {
            panic!("the server reports the overlap for this order");
        };
        assert_eq!((first.as_str(), second.as_str()), ("a.b", ">"));

        // Likewise a self-overlap is named ahead of a later API clash:
        //     a.b,a.b,*.a -> subject "a.b" overlaps with "a.b"
        let mut topology = expected();
        topology.subjects = vec![pattern("a.b"), pattern("a.b"), pattern("*.a")];
        assert!(matches!(
            topology.validate(),
            Err(TopologyError::OverlappingSubjects { .. })
        ));

        // But with no overlap in front of it, the API clash is reported:
        //     a.b,*.a -> subjects that overlap with jetstream api ...
        let mut topology = expected();
        topology.subjects = vec![pattern("a.b"), pattern("*.a")];
        assert!(matches!(
            topology.validate(),
            Err(TopologyError::OverlapsJetStreamApi { .. })
        ));
    }

    #[test]
    fn the_new_errors_say_what_is_wrong() {
        assert_eq!(
            TopologyError::OverlapsJetStreamApi {
                name: "s".to_owned(),
                subject: "*.a".to_owned(),
            }
            .to_string(),
            r#"stream "s" subject "*.a" overlaps JetStream's own $JS.> namespace"#
        );
        assert_eq!(
            TopologyError::CapturesAllSubjects {
                name: "s".to_owned(),
            }
            .to_string(),
            r#"stream "s" captures all subjects with `>`"#
        );
    }

    #[test]
    fn validate_rejects_a_subject_list_that_overlaps_itself() {
        // Both transcripts came from a live nats:2 server, which refuses the
        // stream with error 10052 -- while `diff` reports these as equal to
        // themselves and `matches` returns true, so nothing else here catches
        // them.
        let mut topology = expected();
        topology.subjects = vec![pattern("a.b"), pattern("a.b")];
        assert_eq!(
            topology.validate(),
            Err(TopologyError::OverlappingSubjects {
                name: "pneuma-termination".to_owned(),
                first: "a.b".to_owned(),
                second: "a.b".to_owned(),
            })
        );

        let mut topology = expected();
        topology.subjects = vec![pattern("x.*"), pattern("x.b")];
        let Err(TopologyError::OverlappingSubjects { first, second, .. }) = topology.validate()
        else {
            panic!("x.* and x.b overlap");
        };
        assert_eq!((first.as_str(), second.as_str()), ("x.*", "x.b"));

        // The offending pair is reported even when it is not the first pair,
        // which is what makes the message useful on a long list.
        let mut topology = expected();
        topology.subjects = vec![
            pattern("p.q"),
            pattern("r.s"),
            pattern("t.>"),
            pattern("t.u"),
        ];
        let Err(TopologyError::OverlappingSubjects { first, second, .. }) = topology.validate()
        else {
            panic!("t.> and t.u overlap");
        };
        assert_eq!((first.as_str(), second.as_str()), ("t.>", "t.u"));
    }

    #[test]
    fn validate_accepts_a_disjoint_subject_list() {
        let mut topology = expected();
        topology.subjects = vec![pattern("a.b"), pattern("a.c"), pattern("b.>")];
        assert_eq!(topology.validate(), Ok(()));

        // A single subject has no pair to compare, so the loop body never runs.
        assert_eq!(expected().validate(), Ok(()));
    }

    #[test]
    fn the_overlap_error_says_which_two_subjects_collide() {
        let error = TopologyError::OverlappingSubjects {
            name: "s".to_owned(),
            first: "x.*".to_owned(),
            second: "x.b".to_owned(),
        };
        assert_eq!(
            error.to_string(),
            r#"stream "s" subject "x.*" overlaps "x.b""#
        );
    }

    #[test]
    fn the_real_stream_name_is_valid_and_a_dotted_one_is_not() {
        // Measured against a running NATS 2.x server, not recalled.
        assert_eq!(name("pneuma-termination").as_str(), "pneuma-termination");
        assert!(StreamName::new("has_underscore").is_ok());
        for bad in ["has.dot", "has space", "has>wild", "has*star", "has/slash"] {
            assert!(StreamName::new(bad).is_err(), "{bad} should be rejected");
        }
        assert!(StreamName::new("").is_err());
    }

    #[test]
    fn a_stream_name_is_stricter_than_a_subject_token() {
        // The reason SubjectToken cannot be reused: it accepts `/`, which the
        // server refuses in a stream name. Borrowing it would push the failure
        // from the config boundary out to stream-creation time in a deployment.
        assert!(crate::SubjectToken::new("has/slash").is_ok());
        assert!(StreamName::new("has/slash").is_err());
    }

    #[test]
    fn an_identical_topology_has_no_divergence() {
        assert!(expected().diff(&expected()).is_empty());
        assert!(expected().matches(&expected()));
    }

    #[test]
    fn the_max_age_divergence_is_reported() {
        // What an operator sees today: the original executor hardcodes 7 days while the other
        // two read the env var, so a configured 1 day silently loses.
        let mut actual = expected();
        actual.max_age = Duration::from_secs(24 * 3600);

        let found = expected().diff(&actual);
        assert_eq!(found.len(), 1);
        assert!(matches!(found[0], Divergence::MaxAge { .. }));
        assert!(!expected().matches(&actual));
    }

    #[test]
    fn the_replica_divergence_is_reported() {
        // The one with teeth: an operator sets NATS_NUM_STREAM_REPLICAS=3 and
        // gets 1 unless the original executor won the creation race.
        let mut actual = expected();
        actual.replicas = Replicas::DEFAULT;

        let found = expected().diff(&actual);
        assert_eq!(
            found,
            vec![Divergence::Replicas {
                expected: 3,
                actual: 1
            }]
        );
    }

    #[test]
    fn the_work_queue_retention_divergence_is_reported() {
        // The latent one. Under WorkQueue the termination fan-out delivers each
        // refresh signal to exactly one instance, and the rest keep dispatching
        // cancelled work.
        let mut actual = expected();
        actual.retention = Retention::WorkQueue;

        let found = expected().diff(&actual);
        assert_eq!(
            found,
            vec![Divergence::Retention {
                expected: Retention::Limits,
                actual: Retention::WorkQueue
            }]
        );
    }

    #[test]
    fn a_subject_divergence_is_reported() {
        let mut actual = expected();
        actual.subjects = vec![pattern("pneuma.something-else")];

        let found = expected().diff(&actual);
        assert_eq!(found.len(), 1);
        assert!(matches!(found[0], Divergence::Subjects { .. }));
    }

    #[test]
    fn every_field_is_compared_not_just_the_one_a_caller_cares_about() {
        // The whole point. Each service reads a different subset of these three
        // and is internally consistent; only the combination is wrong, so a
        // diff that checked a subset would reproduce the defect it exists to
        // catch.
        let mut actual = expected();
        actual.subjects = vec![pattern("other.subject")];
        actual.retention = Retention::Interest;
        actual.max_age = Duration::ZERO;
        actual.replicas = Replicas::DEFAULT;

        let found = expected().diff(&actual);
        assert_eq!(
            found.len(),
            4,
            "all four fields must be reported: {found:?}"
        );
    }

    #[test]
    fn the_name_is_deliberately_not_compared() {
        // A topology is only ever diffed against the stream of the same name,
        // so a mismatch there is a lookup bug rather than a divergence.
        let mut actual = expected();
        actual.name = name("a-different-stream");
        assert!(expected().diff(&actual).is_empty());
    }

    #[test]
    fn zero_max_age_means_unlimited_and_differs_from_seven_days() {
        // JetStream encodes "no limit" as zero, so it must not be mistaken for
        // "unset and therefore equal".
        let mut actual = expected();
        actual.max_age = Duration::ZERO;
        assert_eq!(expected().diff(&actual).len(), 1);
    }

    #[test]
    fn a_rejected_stream_name_is_not_described_as_a_subject() {
        // The diagnostic reused SubjectError and told an operator that `/` is
        // "not allowed in a subject" -- while `/` IS allowed in a subject, as
        // the test above asserts. They were sent to fix the wrong thing, under
        // a rule this crate contradicts.
        let Err(err) = StreamName::new("has/slash") else {
            panic!("should be rejected");
        };
        let message = err.to_string();
        assert!(message.contains("stream name"), "{message}");
        assert!(!message.contains("not allowed in a subject"), "{message}");
        assert!(message.contains("path separators"), "{message}");

        assert_eq!(StreamName::new(""), Err(StreamNameError::Empty));
        assert_eq!(
            StreamNameError::Empty.to_string(),
            "a stream name must not be empty"
        );
    }

    #[test]
    fn subject_order_is_not_a_divergence() {
        // Each service builds its subject list from its own config, so two that
        // agree on the set but order it differently are not diverging -- and
        // under this type's "fail at startup" contract, a spurious difference
        // takes the deployment down. JetStream treats subjects as a set.
        let mut forward = expected();
        forward.subjects = vec![pattern("a.>"), pattern("b.>")];
        let mut reversed = expected();
        reversed.subjects = vec![pattern("b.>"), pattern("a.>")];

        assert!(forward.diff(&reversed).is_empty());
        assert!(forward.matches(&reversed));

        // A genuinely different set is still reported.
        let mut different = expected();
        different.subjects = vec![pattern("a.>"), pattern("c.>")];
        assert_eq!(forward.diff(&different).len(), 1);
    }

    #[test]
    fn replicas_cannot_be_zero() {
        // Normalising 0 to 1 is this crate's decision. the original executor clamps below 1
        // only for the termination stream; the work stream passes the raw
        // value into CreateOrUpdateStream. The `Replicas`
        // docs carry the full story -- an earlier version of this comment
        // asserted "no path produces a zero-replica stream", which was
        // retracted there and left standing here.
        assert_eq!(Replicas::from_config(0).get(), 1);
        assert_eq!(Replicas::from_config(1).get(), 1);
        assert_eq!(Replicas::from_config(3).get(), 3);
        assert_eq!(Replicas::default(), Replicas::DEFAULT);
        assert_eq!(Replicas::DEFAULT.get(), 1);

        // The spurious failure this prevents: an unset env var read as 0.
        let mut from_unset_env = expected();
        from_unset_env.replicas = Replicas::from_config(0);
        let mut real_stream = expected();
        real_stream.replicas = Replicas::DEFAULT;
        assert!(from_unset_env.matches(&real_stream));
    }

    #[test]
    fn the_unmodelled_fields_are_not_compared_and_the_docs_say_so() {
        // Scope check. This type models four fields; JetStream's StreamConfig
        // has more, so a memory-backed stream where a file-backed one was
        // intended passes matches(). That is a real limit, documented rather
        // than papered over -- an earlier doc claimed "every field".
        let identical = expected();
        assert!(expected().matches(&identical));
        // Nothing here can express storage, discard, max_msgs, max_bytes or
        // duplicate_window, which is exactly why the module doc lists them.
    }

    #[test]
    fn an_empty_subject_list_is_invalid_but_diffs_as_equal() {
        // The gap public fields leave. JetStream rejects a stream with no
        // subjects, but two such topologies match each other and diff reports
        // nothing -- so a reconciler built from a config missing its subjects
        // key passes its own check and fails later at AddStream.
        let mut empty = expected();
        empty.subjects = Vec::new();
        let other_empty = empty.clone();

        assert!(empty.matches(&other_empty), "diff cannot catch this");
        assert_eq!(
            empty.validate(),
            Err(TopologyError::NoSubjects {
                name: "pneuma-termination".to_owned()
            })
        );
        assert!(expected().validate().is_ok());
        assert!(TopologyError::NoSubjects {
            name: "s".to_owned()
        }
        .to_string()
        .contains("at least one"));
    }

    #[test]
    fn divergences_display_usefully() {
        // These strings are what an operator reads at a failed startup.
        let message = Divergence::Replicas {
            expected: 3,
            actual: 1,
        }
        .to_string();
        assert!(message.contains('3') && message.contains('1'), "{message}");

        let message = Divergence::Retention {
            expected: Retention::Limits,
            actual: Retention::WorkQueue,
        }
        .to_string();
        assert!(message.contains("WorkQueue"), "{message}");

        let message = Divergence::MaxAge {
            expected: Duration::from_secs(1),
            actual: Duration::ZERO,
        }
        .to_string();
        assert!(!message.is_empty());

        let message = Divergence::Subjects {
            expected: vec!["a".to_owned()],
            actual: vec!["b".to_owned()],
        }
        .to_string();
        assert!(message.contains('a') && message.contains('b'), "{message}");
    }

    #[test]
    fn retention_defaults_to_limits() {
        // The policy the termination fan-out needs, and what two of the three
        // services already use.
        assert_eq!(Retention::default(), Retention::Limits);
        assert_ne!(Retention::Limits, Retention::WorkQueue);
        assert!(format!("{:?}", Retention::Interest).contains("Interest"));
    }

    #[test]
    fn accessors_and_derives() {
        let topology = expected();
        assert_eq!(topology.clone(), topology);
        assert_eq!(topology.name.as_str(), "pneuma-termination");
        assert!(format!("{topology:?}").contains("pneuma-termination"));

        let n = name("s");
        assert_eq!(n.clone(), n);
        use std::collections::HashSet;
        let set: HashSet<_> = [n.clone(), n].into();
        assert_eq!(set.len(), 1);
    }
}
