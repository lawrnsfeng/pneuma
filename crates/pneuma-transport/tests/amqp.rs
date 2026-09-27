//! The AMQP client, against a real RabbitMQ.
//!
//! A fake broker would agree with whatever this file asserts, which is worth
//! nothing for the two things being checked: that a queue declared with these
//! arguments is one RabbitMQ accepts, and that a delivery dropped without being
//! settled genuinely comes back. Both are properties of the broker.
//!
//! ```sh
//! docker run -d --name pn-rabbit -p 5673:5672 rabbitmq:3.13
//! PNEUMA_TEST_AMQP_URL='amqp://guest:guest@127.0.0.1:5673/%2f' \
//!     cargo test -p pneuma-transport --test amqp
//! ```

use lapin::options::{QueueDeclareOptions, QueueDeleteOptions};
use lapin::types::{AMQPValue, FieldTable, ShortString};
use lapin::{Connection, ConnectionProperties};
use pneuma_amqp::{QueueName, RoutingKey};
use pneuma_transport::{
    Amqp, AmqpError, QueueSpec, DELIVERY_LIMIT, PREFETCH, QUEUE_TYPE, UNLIMITED,
};

fn url() -> String {
    let Ok(url) = std::env::var("PNEUMA_TEST_AMQP_URL") else {
        panic!("PNEUMA_TEST_AMQP_URL is not set; see the header of this file");
    };
    url
}

fn queue(name: &str) -> QueueName {
    let Ok(queue) = QueueName::new(name) else {
        panic!("{name} is a queue name");
    };
    queue
}

fn spec(name: &str) -> QueueSpec {
    let Ok(spec) = QueueSpec::new(queue(name)) else {
        panic!("{name} has a dead-letter name");
    };
    spec
}

fn key(name: &str) -> RoutingKey {
    let Ok(key) = RoutingKey::new(name) else {
        panic!("{name} is a routing key");
    };
    key
}

async fn connected() -> Amqp {
    match Amqp::connect(&url()).await {
        Ok(amqp) => amqp,
        Err(error) => panic!("could not reach the broker: {error}"),
    }
}

/// A channel of the test's own, for setup and for reading queues back.
///
/// Deliberately not methods on `Amqp`: `delete_queue` is a foot-gun to hand a
/// service, and a message count is something a test asserts rather than
/// something this crate does.
struct Admin {
    /// Held only so the channel outlives it; a dropped connection closes it.
    _connection: Connection,
    channel: lapin::Channel,
}

async fn admin() -> Admin {
    let Ok(connection) = Connection::connect(&url(), ConnectionProperties::default()).await else {
        panic!("could not reach the broker at {}", url());
    };
    match connection.create_channel().await {
        Ok(channel) => Admin {
            _connection: connection,
            channel,
        },
        Err(error) => panic!("could not open a channel: {error}"),
    }
}

/// Removes both queues so a rerun declares them fresh.
///
/// Not a nicety: a queue's arguments are part of its identity, so one left over
/// from an earlier version of this file makes every later run fail with
/// `PRECONDITION_FAILED` and look like a bug in the arguments.
async fn wipe(spec: &QueueSpec) {
    let admin = admin().await;
    for name in [spec.queue().as_str(), spec.dead_letter().as_str()] {
        if let Err(error) = admin
            .channel
            .queue_delete(name.into(), QueueDeleteOptions::default())
            .await
        {
            panic!("could not remove {name}: {error}");
        }
    }
}

/// How many messages are sitting in `name`.
///
/// Read by declaring it passively, which is how AMQP answers the question.
async fn depth(name: &str) -> u32 {
    let admin = admin().await;
    let declared = admin
        .channel
        .queue_declare(
            name.into(),
            QueueDeclareOptions {
                passive: true,
                ..QueueDeclareOptions::default()
            },
            FieldTable::default(),
        )
        .await;
    match declared {
        Ok(queue) => queue.message_count(),
        Err(error) => panic!("could not read {name}: {error}"),
    }
}

#[test]
fn the_arguments_bound_a_redelivery_loop() {
    // The point of `x-delivery-limit`: without it, a quorum queue on RabbitMQ
    // 3.x redelivers for ever, so `Delivery`'s requeue-on-drop turns a poison
    // message into an infinite loop that looks like throughput. RabbitMQ 4.0
    // defaults it to 20 -- so an unchanged declaration behaves differently
    // depending on the broker's version, which is the reason to be explicit.
    let arguments = spec("pneuma.test.args").arguments();
    let expected: Vec<(ShortString, AMQPValue)> = vec![
        (
            "x-dead-letter-exchange".into(),
            AMQPValue::LongString("".into()),
        ),
        (
            "x-dead-letter-routing-key".into(),
            AMQPValue::LongString("pneuma.test.args.dead_letter".into()),
        ),
        (
            "x-delivery-limit".into(),
            AMQPValue::LongLongInt(DELIVERY_LIMIT),
        ),
        (
            "x-queue-type".into(),
            AMQPValue::LongString(QUEUE_TYPE.into()),
        ),
    ];
    let mut wanted = FieldTable::default();
    for (name, value) in expected {
        wanted.insert(name, value);
    }
    assert_eq!(arguments, wanted);

    // The dead-letter queue gets no dead-letter of its own, and `-1` rather
    // than the argument's absence. A message here has nowhere further to go, so
    // a limit deletes it -- and omitting the argument does *not* mean unlimited
    // on RabbitMQ 4.x, where the server default is twenty. A replay tool that
    // crashes and reconnects twenty times would be enough to lose it.
    let mut dlq = FieldTable::default();
    dlq.insert(
        "x-queue-type".into(),
        AMQPValue::LongString(QUEUE_TYPE.into()),
    );
    dlq.insert("x-delivery-limit".into(), AMQPValue::LongLongInt(UNLIMITED));
    assert_eq!(spec("pneuma.test.args").dead_letter_arguments(), dlq);
    assert_eq!(UNLIMITED, -1);

    // `0` for a prefetch would mean *unlimited*, not "none" -- which is why the
    // spec's fields are private and `new` is the only door.
    let spec = spec("pneuma.test.args");
    assert_eq!(spec.prefetch(), PREFETCH);
    assert_eq!(spec.delivery_limit(), DELIVERY_LIMIT);
    assert_eq!(spec.queue().as_str(), "pneuma.test.args");
    assert_eq!(spec.dead_letter().as_str(), "pneuma.test.args.dead_letter");

    assert_eq!(
        PREFETCH, 1,
        "one outstanding delivery is what makes settling simple"
    );
}

#[test]
fn a_queue_whose_dead_letter_name_would_not_fit_has_no_spec() {
    // The derivation is the one place a valid name becomes an invalid one:
    // `.dead_letter` is twelve bytes and the `shortstr` ceiling is 255, so a
    // 244-byte queue name is legal and its dead-letter name cannot be encoded.
    let long = "q".repeat(244);
    assert!(QueueName::new(&long).is_ok(), "244 bytes is a legal name");
    let Ok(name) = QueueName::new(&long) else {
        panic!("244 bytes is a legal name");
    };
    assert!(
        QueueSpec::new(name).is_err(),
        "and its dead-letter name is 256, which cannot go on the wire"
    );
}

#[tokio::test]
async fn nothing_listening_is_named_as_a_connection_failure() {
    let Err(error) = Amqp::connect("amqp://127.0.0.1:1/%2f").await else {
        panic!("nothing is listening on port 1");
    };
    assert!(
        matches!(error, AmqpError::Connect { .. }),
        "wrong variant: {error:?}"
    );
}

#[tokio::test]
async fn a_message_survives_the_round_trip_and_is_acked_once() {
    let amqp = connected().await;
    let spec = spec("pneuma.test.roundtrip");
    wipe(&spec).await;

    let Ok(mut consuming) = amqp.consume(&spec, "test-roundtrip").await else {
        panic!("the queues should declare");
    };
    if let Err(error) = amqp
        .publish(&key(spec.queue().as_str()), b"{\"job\":1}")
        .await
    {
        panic!("publishing should work: {error}");
    }

    let Ok(Some(delivery)) = consuming.next().await else {
        panic!("the message should arrive");
    };
    assert_eq!(delivery.body(), b"{\"job\":1}");
    delivery.ack();

    // Acked, so it does not come back. `flush` is what pays the broker, and it
    // is public precisely so a shutdown can settle the last delivery rather
    // than leaving it to a dropped connection.
    if let Err(error) = consuming.flush().await {
        panic!("the ack should reach the broker: {error}");
    }
    assert_eq!(
        depth(spec.queue().as_str()).await,
        0,
        "an acked message is gone"
    );
}

#[tokio::test]
async fn a_delivery_dropped_without_settling_really_does_come_back() {
    // The whole point of the type, checked against the broker rather than
    // against a recorder: it is RabbitMQ that has to agree the message was
    // returned, not this crate's own bookkeeping.
    let amqp = connected().await;
    let spec = spec("pneuma.test.requeue");
    wipe(&spec).await;

    let Ok(mut consuming) = amqp.consume(&spec, "test-requeue").await else {
        panic!("the queues should declare");
    };
    if let Err(error) = amqp.publish(&key(spec.queue().as_str()), b"dropped").await {
        panic!("publishing should work: {error}");
    }

    let Ok(Some(delivery)) = consuming.next().await else {
        panic!("the message should arrive");
    };
    drop(delivery);

    let Ok(Some(again)) = consuming.next().await else {
        panic!("a dropped delivery comes back");
    };
    assert_eq!(again.body(), b"dropped");
    again.ack();
    if let Err(error) = consuming.flush().await {
        panic!("the ack should reach the broker: {error}");
    }
}

#[tokio::test]
async fn a_rejected_delivery_lands_in_the_dead_letter_queue() {
    let amqp = connected().await;
    let spec = spec("pneuma.test.dlq");
    wipe(&spec).await;

    let Ok(mut consuming) = amqp.consume(&spec, "test-dlq").await else {
        panic!("the queues should declare");
    };
    if let Err(error) = amqp
        .publish(&key(spec.queue().as_str()), b"malformed")
        .await
    {
        panic!("publishing should work: {error}");
    }

    let Ok(Some(delivery)) = consuming.next().await else {
        panic!("the message should arrive");
    };
    // A body that does not parse does not parse on the third attempt either,
    // so retrying it is how a malformed message becomes a loop.
    delivery.nack(false);
    if let Err(error) = consuming.flush().await {
        panic!("the nack should reach the broker: {error}");
    }

    assert_eq!(depth(spec.queue().as_str()).await, 0);
    assert_eq!(
        depth(spec.dead_letter().as_str()).await,
        1,
        "rejected, not retried"
    );
}

#[tokio::test]
async fn a_queue_that_already_exists_differently_is_named_in_the_refusal() {
    // The commonest AMQP failure in a deploy: a queue left over from an earlier
    // version, whose arguments are part of its identity. Named, because the
    // cure is a decision about the existing queue rather than a retry, and a
    // bare `PRECONDITION_FAILED` does not say which of three queues it was.
    let amqp = connected().await;
    let spec = spec("pneuma.test.conflict");
    wipe(&spec).await;
    let admin = admin().await;
    if let Err(error) = admin
        .channel
        .queue_declare(
            spec.queue().as_str().into(),
            QueueDeclareOptions {
                durable: true,
                ..QueueDeclareOptions::default()
            },
            FieldTable::default(),
        )
        .await
    {
        panic!("setup should declare a classic queue: {error}");
    }

    let Err(error) = amqp.consume(&spec, "test-conflict").await else {
        panic!("a classic queue is not a quorum queue");
    };
    let AmqpError::Declare { queue, .. } = &error else {
        panic!("wrong variant: {error:?}");
    };
    assert_eq!(queue, "pneuma.test.conflict");

    wipe(&spec).await;
}

#[tokio::test]
async fn a_closed_connection_ends_the_consumer_rather_than_hanging() {
    let amqp = connected().await;
    let spec = spec("pneuma.test.closed");
    wipe(&spec).await;

    let Ok(mut consuming) = amqp.consume(&spec, "test-closed").await else {
        panic!("the queues should declare");
    };
    if let Err(error) = amqp.close().await {
        panic!("closing should work: {error}");
    }
    let Ok(None) = consuming.next().await else {
        panic!("a closed connection has no more messages");
    };
}

#[tokio::test]
async fn a_message_with_nowhere_to_go_is_a_failure_rather_than_a_silence() {
    // Without publisher confirms this is the quietest failure available:
    // `basic_publish` returns as soon as the frame is written, the
    // `PublisherConfirm` resolves to `NotRequested` so awaiting it is a no-op,
    // and `mandatory: false` lets the default exchange discard an unroutable
    // message without a word. Every one of those returns `Ok(())`.
    let amqp = connected().await;
    let Err(error) = amqp
        .publish(&key("pneuma.test.nothing.is.bound.here"), b"lost")
        .await
    else {
        panic!("nothing is bound to that key");
    };
    let AmqpError::NotDelivered {
        key: named,
        outcome,
    } = &error
    else {
        panic!("wrong variant: {error:?}");
    };
    assert_eq!(named, "pneuma.test.nothing.is.bound.here");
    assert!(!outcome.is_empty(), "the log gets the broker's own answer");
}

#[tokio::test]
async fn the_dead_letter_queue_is_declared_with_no_delivery_limit() {
    // Asserted against the broker rather than against the FieldTable, because
    // what matters is that RabbitMQ accepts `-1` and records it -- a value it
    // rejected would fail the declare, and a value it silently ignored would
    // leave the 4.x default of twenty in place.
    let amqp = connected().await;
    let spec = spec("pneuma.test.dlqargs");
    wipe(&spec).await;
    let Ok(_consuming) = amqp.consume(&spec, "test-dlqargs").await else {
        panic!("the queues should declare");
    };

    // Declaring again with the same arguments is how a broker is asked whether
    // it agrees: a mismatch is `PRECONDITION_FAILED`.
    let admin = admin().await;
    let declared = admin
        .channel
        .queue_declare(
            spec.dead_letter().as_str().into(),
            QueueDeclareOptions {
                durable: true,
                ..QueueDeclareOptions::default()
            },
            spec.dead_letter_arguments(),
        )
        .await;
    if let Err(error) = declared {
        panic!("the broker disagrees about the dead-letter queue: {error}");
    }
}
