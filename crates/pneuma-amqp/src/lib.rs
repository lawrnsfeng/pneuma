//! AMQP names: what a broker will actually accept, and what pneuma may send.
//!
//! The sibling of `pneuma-nats`, and deliberately client-free in the same way —
//! no connection handling, no `lapin`, no runtime. It exists so that a name that
//! cannot work is rejected where it is built rather than at the broker, several
//! layers away from whoever chose it.
//!
//! # This crate is *not* a transcription of the AMQP spec
//!
//! AMQP 0-9-1 says a queue name may contain only letters, digits, hyphen,
//! underscore, period and colon. **RabbitMQ does not enforce that**, and
//! encoding it would have rejected names that work in production. Measured
//! against `rabbitmq:3-management`, every one of these was accepted:
//!
//! ```text
//! with/slash   with space   with#hash   with*star   with>gt
//! with\backslash            wíth-ünicode            with:colon
//! ```
//!
//! So the charset rule is not in this crate. There *is* one content rule
//! RabbitMQ enforces — a queue name may not begin with `amq.`, which it
//! reserves for itself — and that one is here, measured down to its boundary
//! ([`RESERVED_QUEUE_PREFIX`]).
//!
//! What is here, then, is what a broker or the wire format genuinely enforces,
//! plus one deliberate and documented exception.
//!
//! # What the wire format enforces
//!
//! **A 255-byte ceiling, counted in bytes.** This is the AMQP `shortstr`, whose
//! length prefix is a single octet, so 256 bytes is not representable at all —
//! it is refused before any broker is consulted. (RabbitMQ enforces it
//! independently too, as its management API shows: `queue should be less than
//! 255 bytes, actually was 256`. Both are true; the encoder is simply first.) Confirmed in the encoders with no connection open
//! at all: `pika` raises `ShortStringTooLong` and `pamqp` — which is what
//! `aiormq`, `aio_pika` and therefore the original encode with — raises
//! `TypeError: string exceeds maximum length of 255 bytes`. A 255-byte name
//! encodes to 256 wire bytes: one for the length, 255 for the text.
//!
//! It is bytes rather than characters: 128 two-byte characters is 256 bytes and
//! is refused, while 127 of them is 254 bytes and is fine. [`MAX_NAME_BYTES`]
//! applies to both types here.
//!
//! Because the limit lives in the wire format rather than in a broker, it holds
//! for every conformant client and every AMQP broker, which is what makes the
//! derived-name hazard below a certainty rather than a RabbitMQ quirk.
//!
//! # Where that ceiling actually bites
//!
//! Not on a name anyone types. `RabbitMQReceiver` derives its dead-letter queue
//! as `f"{topic}.dead_letter"`, twelve bytes
//! longer than the topic. A 244-byte topic is a perfectly legal queue name whose
//! derived DLQ name is 256 and cannot be encoded at all. That is
//! why [`QueueName::dead_letter`] returns a `Result`: the derivation is the one
//! place a valid name becomes an invalid one. See the defect notes for
//! what that costs today.
//!
//! # The one place this is stricter than the broker
//!
//! Carriage return and line feed are rejected, because they do not fail — they
//! **hang**. Declaring a queue whose name contains `\n` or `\r` produced no
//! response from the broker at all, and the client waited until it was killed;
//! `\t`, `\0` and `\x1b` in the same position were all accepted normally. A
//! failure mode that presents as a hung consumer rather than an error is worth
//! spending a little strictness to make unconstructable. The remaining control
//! characters are rejected alongside them — that part is a judgement call, not a
//! measurement, and is recorded in the design notes.

#![forbid(unsafe_code)]
#![deny(clippy::unwrap_used, clippy::expect_used)]
#![deny(missing_docs)]

pub mod name;

pub use name::{
    AmqpNameError, NameKind, QueueName, RoutingKey, DEAD_LETTER_SUFFIX, MAX_NAME_BYTES,
    RESERVED_QUEUE_PREFIX,
};
