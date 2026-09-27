//! Queue names and routing keys, validated against what a broker accepts.

use compact_str::CompactString;

/// The AMQP `shortstr` ceiling, in **bytes**.
///
/// A `shortstr` carries its length in a single octet, so 255 is the largest
/// value representable and 256 cannot be put on the wire at all. This was
/// checked in the encoders themselves, with no connection open: `pika` raises
/// `ShortStringTooLong`, and `pamqp` — the encoder beneath `aiormq` and
/// `aio_pika`, and so beneath the original — raises `TypeError: string exceeds
/// maximum length of 255 bytes`.
///
/// Counted in bytes, not characters: 128 two-byte characters is 256 bytes and
/// is refused; 127 of them is 254 bytes and is accepted. A live
/// `rabbitmq:3-management` agreed on both sides of the boundary.
pub const MAX_NAME_BYTES: usize = 255;

/// The queue-name prefix RabbitMQ reserves for its own internal queues.
///
/// Declaring one is refused with `403 ACCESS_REFUSED - queue name 'amq.x'
/// contains reserved prefix 'amq.*'`. Measured, along with its exact boundary:
/// `amq` alone, `amqz.ok`, `xamq.foo` and `AMQ.upper` are all accepted, so it
/// is this literal lowercase prefix and nothing broader.
///
/// This is the one queue-name *content* rule RabbitMQ genuinely enforces, which
/// is why it is here while the AMQP charset is not.
pub const RESERVED_QUEUE_PREFIX: &str = "amq.";

/// What `RabbitMQReceiver` appends to build its dead-letter queue name.
///
/// Twelve bytes, which is the whole reason [`QueueName::dead_letter`] can fail.
pub const DEAD_LETTER_SUFFIX: &str = ".dead_letter";

/// A name or key that a broker would refuse, or that would hang it.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum AmqpNameError {
    /// Empty. Legal to AMQP in both positions and wrong in both for pneuma:
    /// an empty queue name asks the broker to invent one, and an empty routing
    /// key publishes successfully to nowhere.
    #[error("{kind} may not be empty")]
    Empty {
        /// Which of the two was being built.
        kind: NameKind,
    },
    /// Longer than [`MAX_NAME_BYTES`] bytes.
    #[error("{kind} is {actual} bytes, over the AMQP shortstr limit of {MAX_NAME_BYTES}")]
    TooLong {
        /// Which of the two was being built.
        kind: NameKind,
        /// Its length in bytes.
        actual: usize,
    },
    /// A queue name beginning with `amq.`, which RabbitMQ reserves for itself.
    #[error("a queue name may not begin with the reserved prefix {RESERVED_QUEUE_PREFIX:?}")]
    ReservedPrefix,
    /// Contains a control character. `\r` and `\n` are refused because they
    /// hang the broker rather than producing an error; the rest are refused
    /// alongside them by choice.
    #[error("{kind} contains the control character {character:?}")]
    ControlCharacter {
        /// Which of the two was being built.
        kind: NameKind,
        /// The offending character.
        character: char,
    },
}

/// Which kind of name a [`AmqpNameError`] is about, so one error type can serve
/// both without the message going vague.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NameKind {
    /// A queue name.
    Queue,
    /// A routing key.
    RoutingKey,
}

impl std::fmt::Display for NameKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            NameKind::Queue => "a queue name",
            NameKind::RoutingKey => "a routing key",
        })
    }
}

/// Shared validation. Both types have identical rules; only the error wording
/// and the reason empty is wrong differ.
fn validate(value: &str, kind: NameKind) -> Result<(), AmqpNameError> {
    if value.is_empty() {
        return Err(AmqpNameError::Empty { kind });
    }
    if value.len() > MAX_NAME_BYTES {
        return Err(AmqpNameError::TooLong {
            kind,
            actual: value.len(),
        });
    }
    if let Some(character) = value.chars().find(|c| c.is_control()) {
        return Err(AmqpNameError::ControlCharacter { kind, character });
    }
    Ok(())
}

/// A queue name a broker will accept.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct QueueName(CompactString);

impl QueueName {
    /// Builds a queue name, rejecting what a broker would refuse.
    ///
    /// Note what is *not* checked: the character set. RabbitMQ accepts spaces,
    /// slashes, `#`, `*` and non-ASCII in queue names despite the AMQP spec
    /// restricting them, so rejecting those here would reject names that work.
    pub fn new(value: impl Into<CompactString>) -> Result<Self, AmqpNameError> {
        let value = value.into();
        validate(&value, NameKind::Queue)?;
        // Declaration-only. The broker enforces this when a queue is declared;
        // nothing measured says a routing key carrying the prefix is refused,
        // and on a topic exchange it would be an ordinary key. So RoutingKey
        // does not carry this rule.
        if value.starts_with(RESERVED_QUEUE_PREFIX) {
            return Err(AmqpNameError::ReservedPrefix);
        }
        Ok(QueueName(value))
    }

    /// The name as a string.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// The dead-letter queue for this one: `{self}.dead_letter`.
    ///
    /// **This is the fallible operation the crate exists for.** The suffix is
    /// twelve bytes, so a valid queue name of 244 bytes or more derives one
    /// that cannot be encoded. Both halves were checked: a live broker accepts
    /// a 244-byte queue name, and the 256-byte derived name is rejected by the
    /// encoder before it reaches any broker.
    ///
    /// original performs this derivation with an f-string and no check,
    /// then declares the dead-letter queue
    /// *before* the main one, so the failure takes down
    /// consumer startup rather than the dead-letter path — see
    /// the defect notes
    pub fn dead_letter(&self) -> Result<QueueName, AmqpNameError> {
        let mut derived = CompactString::from(self.0.as_str());
        derived.push_str(DEAD_LETTER_SUFFIX);
        QueueName::new(derived)
    }

    /// The routing key that reaches this queue through the default exchange.
    ///
    /// pneuma publishes exclusively to the default exchange
    /// (`channel.default_exchange.publish`, the original), where the routing
    /// key *is* the queue name. The conversion cannot fail: the two types
    /// enforce identical rules, and this is the one place that is relied upon.
    pub fn as_routing_key(&self) -> RoutingKey {
        RoutingKey(self.0.clone())
    }
}

impl std::fmt::Display for QueueName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// A routing key pneuma may publish to.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RoutingKey(CompactString);

impl RoutingKey {
    /// Builds a routing key.
    ///
    /// Empty is rejected, and that is the point of the type. The broker accepts
    /// an empty routing key and publishes successfully — the message is simply
    /// unroutable and disappears. Every original sender guards with
    /// `if not topic: raise NoTopicSpecifiedError`, so an empty reply address
    /// never reaches the wire; it raises after the pipeline has already run.
    /// See the defect notes
    pub fn new(value: impl Into<CompactString>) -> Result<Self, AmqpNameError> {
        let value = value.into();
        validate(&value, NameKind::RoutingKey)?;
        Ok(RoutingKey(value))
    }

    /// The key as a string.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for RoutingKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn queue(value: &str) -> QueueName {
        match QueueName::new(value) {
            Ok(name) => name,
            Err(err) => panic!("{value:?} should be a valid queue name: {err}"),
        }
    }

    #[test]
    fn the_length_ceiling_is_255_bytes_for_both_types() {
        // A shortstr's length prefix is one octet, so 256 is unrepresentable.
        // Both encoders refuse it with no connection open, and a live broker
        // accepted 255 for a queue name and a routing key alike.
        assert_eq!(queue(&"q".repeat(255)).as_str().len(), 255);
        assert!(RoutingKey::new("r".repeat(255)).is_ok());

        assert_eq!(
            QueueName::new("q".repeat(256)),
            Err(AmqpNameError::TooLong {
                kind: NameKind::Queue,
                actual: 256,
            })
        );
        assert_eq!(
            RoutingKey::new("r".repeat(300)),
            Err(AmqpNameError::TooLong {
                kind: NameKind::RoutingKey,
                actual: 300,
            })
        );
    }

    #[test]
    fn the_ceiling_counts_bytes_not_characters() {
        // The distinction is load-bearing: `len()` and `chars().count()` differ
        // for non-ASCII, and the wire format counts bytes. 128 two-byte
        // characters is 256 bytes and was refused by the encoder itself; 127 of
        // them is 254 bytes and was accepted.
        let long = "é".repeat(128);
        assert_eq!(
            long.chars().count(),
            128,
            "under a character rule this fits"
        );
        assert_eq!(long.len(), 256);
        assert_eq!(
            QueueName::new(long),
            Err(AmqpNameError::TooLong {
                kind: NameKind::Queue,
                actual: 256,
            })
        );

        let fits = "é".repeat(127);
        assert_eq!(fits.len(), 254);
        assert!(QueueName::new(fits).is_ok());
    }

    #[test]
    fn the_amqp_spec_charset_is_deliberately_not_enforced() {
        // Each of these is forbidden by the AMQP 0-9-1 charset (letters,
        // digits, hyphen, underscore, period, colon) and accepted by a live
        // RabbitMQ. Encoding the spec would reject names that work today, so
        // this test exists to stop someone "fixing" that later.
        for accepted in [
            "with/slash",
            "with space",
            "with#hash",
            "with*star",
            "with>gt",
            "with\\backslash",
            "wíth-ünicode",
        ] {
            assert!(
                QueueName::new(accepted).is_ok(),
                "{accepted:?} is accepted by a real broker and must be accepted here"
            );
        }

        // These are *within* the spec charset, so they are not evidence of the
        // deviation -- but they are the shapes real topics take, so they are
        // worth pinning as accepted.
        for ordinary in ["with.dot", "with-dash", "with_underscore", "with:colon"] {
            assert!(QueueName::new(ordinary).is_ok(), "{ordinary:?}");
        }
    }

    #[test]
    fn the_reserved_amq_prefix_is_rejected_for_queue_names() {
        // The one queue-name content rule RabbitMQ does enforce. Measured:
        //   amq.reserved-looking -> 403 ACCESS_REFUSED ... reserved prefix 'amq.*'
        //   amq.gen-test         -> 403
        //   amq.                 -> 403
        // An earlier version of this crate asserted the opposite, citing a
        // measurement that had never been taken.
        for reserved in ["amq.reserved-looking", "amq.gen-test", "amq."] {
            assert_eq!(
                QueueName::new(reserved),
                Err(AmqpNameError::ReservedPrefix),
                "{reserved:?} is refused by the broker"
            );
        }

        // The boundary, also measured: it is exactly this lowercase prefix.
        for allowed in ["amq", "amqz.ok", "xamq.foo", "AMQ.upper"] {
            assert!(
                QueueName::new(allowed).is_ok(),
                "{allowed:?} is accepted by the broker"
            );
        }

        // Declaration-only: a routing key is not a queue being declared, and
        // nothing measured refuses one carrying the prefix.
        assert!(RoutingKey::new("amq.anything").is_ok());

        assert_eq!(
            AmqpNameError::ReservedPrefix.to_string(),
            r#"a queue name may not begin with the reserved prefix "amq.""#
        );
    }

    #[test]
    fn empty_is_rejected_in_both_positions() {
        assert_eq!(
            QueueName::new(""),
            Err(AmqpNameError::Empty {
                kind: NameKind::Queue
            })
        );
        assert_eq!(
            RoutingKey::new(""),
            Err(AmqpNameError::Empty {
                kind: NameKind::RoutingKey
            })
        );
    }

    #[test]
    fn control_characters_are_rejected_including_the_ones_the_broker_accepts() {
        // \r and \n are the measured hazard: declaring a queue with either
        // produced NO response from the broker, and the client hung until it
        // was killed.
        for (input, expected) in [("a\nb", '\n'), ("a\rb", '\r')] {
            assert_eq!(
                QueueName::new(input),
                Err(AmqpNameError::ControlCharacter {
                    kind: NameKind::Queue,
                    character: expected,
                })
            );
        }

        // These three the broker accepted normally. Rejecting them is this
        // crate being stricter than the broker on purpose -- recorded in
        // the design notes, and pinned here so the deviation is visible rather
        // than looking like a measurement.
        for (input, expected) in [("a\tb", '\t'), ("a\0b", '\0'), ("a\x1bb", '\x1b')] {
            assert_eq!(
                QueueName::new(input),
                Err(AmqpNameError::ControlCharacter {
                    kind: NameKind::Queue,
                    character: expected,
                }),
                "{input:?} is accepted by the broker; we refuse it deliberately"
            );
        }
    }

    #[test]
    fn dead_letter_derivation_fails_exactly_where_the_broker_refuses_it() {
        // The suffix is 12 bytes, so 243 is the last topic length that derives
        // a legal name. Both sides verified against a live broker: a 244-byte
        // queue name is accepted, and its 256-byte derived name is refused.
        let ok = queue(&"t".repeat(243));
        let Ok(derived) = ok.dead_letter() else {
            panic!("243 + 12 = 255 fits");
        };
        assert_eq!(derived.as_str().len(), 255);
        assert!(derived.as_str().ends_with(DEAD_LETTER_SUFFIX));

        let overflows = queue(&"t".repeat(244));
        assert_eq!(
            overflows.dead_letter(),
            Err(AmqpNameError::TooLong {
                kind: NameKind::Queue,
                actual: 256,
            }),
            "the topic is valid; only the derived name is not"
        );
    }

    #[test]
    fn dead_letter_matches_the_reference_f_string() {
        assert_eq!(
            queue("pneuma.result").dead_letter().map(|q| q.to_string()),
            Ok("pneuma.result.dead_letter".to_owned())
        );
        assert_eq!(DEAD_LETTER_SUFFIX.len(), 12);
    }

    #[test]
    fn a_queue_name_is_its_own_routing_key_on_the_default_exchange() {
        let name = queue("pneuma.input");
        assert_eq!(name.as_routing_key().as_str(), "pneuma.input");
        assert_eq!(
            name.as_routing_key(),
            RoutingKey::new("pneuma.input").unwrap_or_else(|_| panic!("should build"))
        );
    }

    #[test]
    fn errors_name_which_kind_they_are_about() {
        assert_eq!(
            AmqpNameError::Empty {
                kind: NameKind::Queue
            }
            .to_string(),
            "a queue name may not be empty"
        );
        assert_eq!(
            AmqpNameError::TooLong {
                kind: NameKind::RoutingKey,
                actual: 300,
            }
            .to_string(),
            "a routing key is 300 bytes, over the AMQP shortstr limit of 255"
        );
        assert_eq!(
            AmqpNameError::ControlCharacter {
                kind: NameKind::Queue,
                character: '\n',
            }
            .to_string(),
            r"a queue name contains the control character '\n'"
        );
        assert_eq!(NameKind::Queue.to_string(), "a queue name");
        assert_eq!(NameKind::RoutingKey.to_string(), "a routing key");
    }

    #[test]
    fn display_round_trips_for_both_types() {
        assert_eq!(queue("q.name").to_string(), "q.name");
        let Ok(key) = RoutingKey::new("r.key") else {
            panic!("should build");
        };
        assert_eq!(key.to_string(), "r.key");
        assert_eq!(key.as_str(), "r.key");
    }

    proptest::proptest! {
        /// For any name this crate accepts, the dead-letter derivation succeeds
        /// exactly when the result still fits. Stated as an independent rule
        /// rather than by re-running the implementation's arithmetic.
        #[test]
        fn dead_letter_succeeds_iff_the_result_fits(
            body in proptest::string::string_regex("[a-z.]{1,260}").unwrap_or_else(|_| unreachable!()),
        ) {
            let Ok(name) = QueueName::new(body.as_str()) else {
                // Rejected inputs are out of scope for this property.
                return Ok(());
            };
            let fits = body.len() + DEAD_LETTER_SUFFIX.len() <= MAX_NAME_BYTES;
            proptest::prop_assert_eq!(name.dead_letter().is_ok(), fits);
        }
    }
}
