//! The AMQP client, and the queue arguments that bound a redelivery loop.
//!
//! Thin on purpose. Every decision this file could make has been made
//! somewhere it can be tested without a broker: the names come from
//! `pneuma-amqp`, the queue arguments are a pure function below, the
//! settlement rule is [`crate::delivery`], and the reconnect rule is
//! [`crate::supervise`]. What is left is a sequence of awaits.
//!
//! # Prefetch is one, and that is what makes settling simple
//!
//! `RabbitMQReceiver.receive` sets `prefetch_count=1`.
//! Kept, and relied on: with
//! one delivery outstanding at a time, a settlement posted by
//! [`Delivery`]'s `Drop` can be applied when the next one is asked for, and
//! there is never a queue of them to reason about.
//!
//! # `x-delivery-limit` is set here rather than left to the server
//!
//! The original declares quorum queues without it.
//! On RabbitMQ 3.x that means *unlimited* redelivery,
//! so a message that fails in the handler and is returned to the queue comes
//! back for ever — a poison message that looks like throughput. RabbitMQ 4.0
//! introduced a default of 20, which means the behaviour of an unchanged
//! declaration depends on the broker's version. Setting it explicitly is what
//! makes [`Delivery`]'s requeue-on-drop safe: something has to end the loop,
//! and "whichever RabbitMQ the cluster happens to run" is not it.
//!
//! The dead-letter queue sets it too, to `-1`. "Unlimited" there is not the
//! absence of the argument — that is the version-dependent thing this paragraph
//! is about — and a DLQ has nowhere to dead-letter *to*, so a limit it inherits
//! from a 4.x default silently deletes the message after twenty deliveries. A
//! replay tool that crashes and reconnects twenty times is enough.
//!
//! # Publishing waits for the broker to say it has it
//!
//! `basic_publish` returns as soon as the frame is written. Without
//! `confirm_select` the `PublisherConfirm` it hands back resolves immediately
//! to `Confirmation::NotRequested`, so awaiting it *looks* like an
//! acknowledgement and is a no-op — and `mandatory: false` means a routing key
//! with no queue behind it is discarded by the default exchange in silence.
//! Both return `Ok(())`. `delivery_mode: persistent` bounds only what happens
//! after the message is on disk; it says nothing about whether it arrived. So
//! confirms are enabled once on the publishing channel, `mandatory` is set, and
//! only `Confirmation::Ack(None)` counts as delivered.

use std::sync::Arc;

use futures_lite::StreamExt;
use lapin::options::{
    BasicAckOptions, BasicConsumeOptions, BasicNackOptions, BasicPublishOptions, BasicQosOptions,
    ConfirmSelectOptions, QueueDeclareOptions,
};
use lapin::types::{AMQPValue, FieldTable, ShortString};
use lapin::{BasicProperties, Channel, Confirmation, Connection, ConnectionProperties};
use pneuma_amqp::{AmqpNameError, QueueName, RoutingKey};
use tokio::sync::mpsc;

use crate::delivery::{Delivery, Disposition, Settle};

/// The `x-queue-type` every queue here is declared with.
///
/// Quorum, as the original declares them. Not a style
/// choice: `x-delivery-limit` is a quorum-queue feature, so the poison-message
/// bound below does not exist on a classic queue.
pub const QUEUE_TYPE: &str = "quorum";

/// How many times a message may be delivered before it is dead-lettered.
///
/// Twenty, which is RabbitMQ 4.0's own default — chosen so that setting it
/// explicitly changes nothing for a cluster already on 4.x, and *adds* the
/// bound for one still on 3.x, where the absence of this argument means
/// unlimited.
pub const DELIVERY_LIMIT: i64 = 20;

/// How many unacknowledged deliveries a consumer may hold.
///
/// One, as the original sets it. Relied on rather than merely copied: with a
/// single delivery outstanding, a settlement posted by `Drop` can be applied
/// when the next message is asked for. Note that `0` here would mean
/// *unlimited*, not "none".
pub const PREFETCH: u16 = 1;

/// `x-delivery-limit` for a queue that must never discard a message.
pub const UNLIMITED: i64 = -1;

/// Why the broker could not be used.
#[derive(Debug, thiserror::Error)]
pub enum AmqpError {
    /// The broker could not be reached.
    ///
    /// Named separately from the rest because it is the one an operator can
    /// act on without reading a stack trace, and the one a misconfigured URI
    /// produces.
    #[error("could not reach the broker: {source}")]
    Connect {
        /// What the client said.
        source: lapin::Error,
    },

    /// A queue could not be declared as asked.
    ///
    /// Almost always `PRECONDITION_FAILED`: a queue that already exists with
    /// different arguments. Named because the cure is a decision about the
    /// existing queue rather than a retry, and a bare AMQP error does not say
    /// which of several queues it was.
    #[error("could not declare {queue}: {source}")]
    Declare {
        /// The queue that could not be declared.
        queue: String,
        /// What the broker said.
        source: lapin::Error,
    },

    /// The broker did not take a published message.
    ///
    /// Either it said so (`Nack`) or it had nowhere to route it, which with
    /// `mandatory` set comes back as an `Ack` carrying the returned message
    /// rather than as a refusal. Both are failures to publish, and neither is
    /// visible at all without confirms.
    #[error("the broker did not accept a message for {key}: {outcome}")]
    NotDelivered {
        /// The routing key that was published to.
        key: String,
        /// What the broker said instead of a plain acknowledgement.
        outcome: String,
    },

    /// Anything else the client reported.
    ///
    /// A single variant rather than one per operation, and deliberately: each
    /// `map_err` that adds context is a closure reached only on a failure this
    /// suite cannot arrange, so a variant per operation would be either an
    /// unmeasured line or a contrived test. The two above exist because they
    /// have real failing tests; the rest carry lapin's own message, which
    /// already names the operation and the AMQP reply code.
    #[error("{0}")]
    Amqp(#[from] lapin::Error),
}

/// What a consumed queue looks like on the broker.
///
/// The fields are private and [`QueueSpec::new`] is the only door, because two
/// of them carry invariants a struct literal would skip: the dead-letter name
/// is *derived* and the derivation can fail, and a prefetch of anything but one
/// breaks the assumption that makes `Drop`-posted settlement simple. In AMQP a
/// prefetch of `0` means *unlimited*, so the literal that looks like "no
/// prefetching" is the one that lets unbounded deliveries go outstanding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueueSpec {
    queue: QueueName,
    dead_letter: QueueName,
    delivery_limit: i64,
    prefetch: u16,
}

impl QueueSpec {
    /// The spec for `queue`, with the derived dead-letter name.
    ///
    /// Fallible because the derivation is the one place a valid name becomes an
    /// invalid one: `.dead_letter` is twelve bytes, and the AMQP `shortstr`
    /// ceiling is 255, so a 244-byte queue name is legal and its dead-letter
    /// name cannot be put on the wire at all.
    pub fn new(queue: QueueName) -> Result<Self, AmqpNameError> {
        let dead_letter = queue.dead_letter()?;
        Ok(QueueSpec {
            queue,
            dead_letter,
            delivery_limit: DELIVERY_LIMIT,
            prefetch: PREFETCH,
        })
    }

    /// The queue to consume.
    pub fn queue(&self) -> &QueueName {
        &self.queue
    }

    /// Where a message goes when it has failed too often, or was rejected.
    pub fn dead_letter(&self) -> &QueueName {
        &self.dead_letter
    }

    /// How many deliveries before it goes there.
    pub fn delivery_limit(&self) -> i64 {
        self.delivery_limit
    }

    /// How many unacknowledged deliveries to hold.
    pub fn prefetch(&self) -> u16 {
        self.prefetch
    }

    /// The arguments the consumed queue is declared with.
    ///
    /// Pure, so what a queue is declared as is a unit test rather than
    /// something read back off a broker. The empty dead-letter exchange is the
    /// default exchange, which routes by queue name — the same pair the
    /// original passes.
    pub fn arguments(&self) -> FieldTable {
        let mut arguments = FieldTable::default();
        arguments.insert(
            field("x-queue-type"),
            AMQPValue::LongString(QUEUE_TYPE.into()),
        );
        arguments.insert(
            field("x-dead-letter-exchange"),
            AMQPValue::LongString("".into()),
        );
        arguments.insert(
            field("x-dead-letter-routing-key"),
            AMQPValue::LongString(self.dead_letter.as_str().into()),
        );
        arguments.insert(
            field("x-delivery-limit"),
            AMQPValue::LongLongInt(self.delivery_limit),
        );
        arguments
    }

    /// The arguments the dead-letter queue is declared with.
    ///
    /// No dead-letter of its own, and delivery explicitly unlimited -- `-1`,
    /// rather than the argument's absence. A message here has nowhere further
    /// to go, so a limit deletes it, and *omitting* the argument does not mean
    /// unlimited on RabbitMQ 4.x, where the server default is twenty. Leaving
    /// it out is exactly the version-dependent behaviour this module refuses
    /// everywhere else.
    pub fn dead_letter_arguments(&self) -> FieldTable {
        let mut arguments = FieldTable::default();
        arguments.insert(
            field("x-queue-type"),
            AMQPValue::LongString(QUEUE_TYPE.into()),
        );
        arguments.insert(field("x-delivery-limit"), AMQPValue::LongLongInt(UNLIMITED));
        arguments
    }
}

/// An argument name, as lapin wants it.
fn field(name: &str) -> ShortString {
    name.into()
}

/// A connection to the broker.
#[derive(Debug, Clone)]
pub struct Amqp {
    connection: Arc<Connection>,
    /// Opened once, with confirms enabled.
    ///
    /// Not per publish. A channel per message costs `channel.open` and
    /// `channel.close` around every `basic.publish` and burns a channel id
    /// against `channel_max`; and confirms have to be switched on per channel,
    /// so a fresh one would have to negotiate them every time or go without.
    publishing: Channel,
}

impl Amqp {
    /// Opens a connection.
    pub async fn connect(uri: &str) -> Result<Self, AmqpError> {
        let connection = Connection::connect(uri, ConnectionProperties::default())
            .await
            .map_err(|source| AmqpError::Connect { source })?;
        let publishing = connection.create_channel().await?;
        publishing
            .confirm_select(ConfirmSelectOptions::default())
            .await?;
        Ok(Amqp {
            connection: Arc::new(connection),
            publishing,
        })
    }

    /// Declares `spec`'s two queues and starts consuming the first.
    ///
    /// The dead-letter queue is declared before the queue that points at it,
    /// because a `x-dead-letter-routing-key` naming a queue that does not exist
    /// is accepted at declare time and silently drops the message later.
    pub async fn consume(&self, spec: &QueueSpec, tag: &str) -> Result<Consuming, AmqpError> {
        let channel = self.connection.create_channel().await?;
        channel
            .basic_qos(spec.prefetch, BasicQosOptions::default())
            .await?;
        declare(&channel, &spec.dead_letter, spec.dead_letter_arguments()).await?;
        declare(&channel, &spec.queue, spec.arguments()).await?;
        let consumer = channel
            .basic_consume(
                spec.queue.as_str().into(),
                tag.into(),
                BasicConsumeOptions::default(),
                FieldTable::default(),
            )
            .await?;
        let (sender, settlements) = mpsc::unbounded_channel();
        Ok(Consuming {
            consumer,
            channel,
            settlements,
            acknowledger: Arc::new(Acknowledger { sender }),
        })
    }

    /// Publishes `body` to `key` on the default exchange, durably, and waits
    /// for the broker to say it has it.
    ///
    /// Persistent, as everything the original publishes is:
    /// a message that a broker restart loses is a run that
    /// nobody started and nobody was told about. Persistence alone is not
    /// enough for that promise, though -- it describes what happens once the
    /// message is on disk, not whether it got there. `mandatory` plus confirms
    /// are what make the promise true, and only `Ack(None)` keeps it: an `Ack`
    /// *carrying* a returned message is the broker saying it had nowhere to put
    /// it.
    pub async fn publish(&self, key: &RoutingKey, body: &[u8]) -> Result<(), AmqpError> {
        let confirmation = self
            .publishing
            .basic_publish(
                "".into(),
                key.as_str().into(),
                BasicPublishOptions {
                    mandatory: true,
                    ..BasicPublishOptions::default()
                },
                body,
                BasicProperties::default().with_delivery_mode(PERSISTENT),
            )
            .await?
            .await?;
        match confirmation {
            Confirmation::Ack(None) => Ok(()),
            // Everything else: a `Nack`, an `Ack` carrying the message back
            // because nothing was bound to that key, and `NotRequested`, which
            // would mean confirms were never switched on. One arm, because the
            // caller's answer to all three is the same -- this message did not
            // arrive -- and only the log needs to tell them apart.
            outcome => Err(AmqpError::NotDelivered {
                key: key.as_str().to_owned(),
                outcome: format!("{outcome:?}"),
            }),
        }
    }

    /// Closes the connection.
    ///
    /// With AMQP's `REPLY_SUCCESS`, not zero: zero is not a defined reply code,
    /// and RabbitMQ logs a disconnect carrying it as abnormal -- so every
    /// orderly shutdown would read as a fault in the broker's own logs.
    pub async fn close(&self) -> Result<(), AmqpError> {
        self.connection.close(REPLY_SUCCESS, "bye".into()).await?;
        Ok(())
    }
}

/// AMQP's `delivery_mode` for a message that survives a broker restart.
const PERSISTENT: u8 = 2;

/// AMQP's reply code for an orderly close.
const REPLY_SUCCESS: u16 = 200;

/// Declares one queue, naming it if the broker refuses.
async fn declare(
    channel: &Channel,
    queue: &QueueName,
    arguments: FieldTable,
) -> Result<(), AmqpError> {
    channel
        .queue_declare(
            queue.as_str().into(),
            QueueDeclareOptions {
                durable: true,
                ..QueueDeclareOptions::default()
            },
            arguments,
        )
        .await
        .map_err(|source| AmqpError::Declare {
            queue: queue.as_str().to_owned(),
            source,
        })?;
    Ok(())
}

/// Posts settlements for the consumer to apply.
///
/// Never panics, which is [`Settle`]'s contract: a send whose receiver has gone
/// is the ordinary case at the end of a process's life, and this is called from
/// a `Drop` that may already be unwinding.
#[derive(Debug)]
pub struct Acknowledger {
    sender: mpsc::UnboundedSender<(u64, Disposition)>,
}

impl Settle for Acknowledger {
    fn settle(&self, tag: u64, disposition: Disposition) {
        // The failure is deliberately discarded rather than reported: a send
        // whose receiver has gone means the consumer has already stopped, and
        // this is called from a `Drop` that cannot report anything anyway.
        let _ = self.sender.send((tag, disposition));
    }
}

/// A consumer, and the settlements owed to the broker.
#[derive(Debug)]
pub struct Consuming {
    consumer: lapin::Consumer,
    channel: Channel,
    settlements: mpsc::UnboundedReceiver<(u64, Disposition)>,
    acknowledger: Arc<Acknowledger>,
}

impl Consuming {
    /// The next message, once everything owed for the last one is paid.
    ///
    /// `Ok(None)` when the consumer has been cancelled or the channel closed.
    ///
    /// `Result<Option<_>>` rather than the `Option<Result<_>>` a stream would
    /// give, because the failures are not the same kind of thing: "the broker
    /// said no" ends the loop, "there are no more messages" ends the loop, and
    /// a caller writing `while let Some(Ok(..))` silently treats the first as
    /// the second.
    pub async fn next(&mut self) -> Result<Option<Delivery<Acknowledger>>, AmqpError> {
        self.flush().await?;
        match self.consumer.next().await {
            None => Ok(None),
            Some(received) => {
                let delivery = received?;
                Ok(Some(Delivery::new(
                    delivery.delivery_tag,
                    delivery.data,
                    Arc::clone(&self.acknowledger),
                )))
            }
        }
    }

    /// Applies every settlement posted since the last call.
    ///
    /// Public because a consumer that has stopped asking for messages still
    /// owes the broker an answer for the last one — a shutdown that skipped
    /// this would leave the final delivery unacknowledged until the connection
    /// dropped, which is the redelivery this crate exists to make deliberate.
    pub async fn flush(&mut self) -> Result<(), AmqpError> {
        while let Ok((tag, disposition)) = self.settlements.try_recv() {
            match disposition {
                Disposition::Ack => {
                    self.channel
                        .basic_ack(tag, BasicAckOptions::default())
                        .await?
                }
                Disposition::Nack { requeue } => {
                    self.channel
                        .basic_nack(
                            tag,
                            BasicNackOptions {
                                requeue,
                                ..BasicNackOptions::default()
                            },
                        )
                        .await?;
                }
            }
        }
        Ok(())
    }
}
