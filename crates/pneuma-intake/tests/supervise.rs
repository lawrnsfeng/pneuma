//! The reconnect loop and the settlement path, against a broker that goes away.
//!
//! The rule itself is `pneuma_transport::decide` and is tested there against an
//! enum. What is left here is the part that has to touch a broker: that a
//! shutdown interrupts a consumer sitting on a quiet queue, that a queue
//! disappearing is a reconnect rather than an exit, and that a settlement owed
//! to a connection that has gone is reported rather than lost.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use lapin::options::{QueueDeclareOptions, QueueDeleteOptions};
use lapin::types::FieldTable;
use lapin::{Connection, ConnectionProperties};
use pneuma_amqp::{QueueName, RoutingKey};
use pneuma_intake::{attach, pump, supervise, Outcome};
use pneuma_transport::{Amqp, Backoff, QueueSpec};
use tokio_util::sync::CancellationToken;

fn url() -> String {
    let Ok(url) = std::env::var("PNEUMA_TEST_AMQP_URL") else {
        panic!("PNEUMA_TEST_AMQP_URL is not set; see tests/service.rs");
    };
    url
}

fn queue(name: &str) -> QueueName {
    let Ok(queue) = QueueName::new(name) else {
        panic!("{name} is a queue name");
    };
    queue
}

fn key(name: &str) -> RoutingKey {
    let Ok(key) = RoutingKey::new(name) else {
        panic!("{name} is a routing key");
    };
    key
}

fn fast() -> Backoff {
    let Ok(backoff) = Backoff::new(
        Duration::from_millis(10),
        Duration::from_millis(20),
        Duration::from_secs(60),
    ) else {
        panic!("that is a backoff");
    };
    backoff
}

/// A channel of the test's own, for creating and destroying queues.
struct Admin {
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

async fn wipe(name: &str) {
    let admin = admin().await;
    for queue in [name.to_owned(), format!("{name}.dead_letter")] {
        if let Err(error) = admin
            .channel
            .queue_delete(queue.as_str().into(), QueueDeleteOptions::default())
            .await
        {
            panic!("could not remove {queue}: {error}");
        }
    }
}

/// Cancels `token` after `delay`, from a task of its own.
///
/// The loop under test then runs in the *current* task, so it is certainly
/// polled. Spawning the loop instead and sleeping here is a race: under
/// coverage instrumentation the spawned task is not always scheduled before the
/// sleep elapses, and a loop cancelled before it did anything still satisfies
/// "it stopped when asked".
fn cancel_after(token: CancellationToken, delay: Duration) {
    tokio::spawn(async move {
        tokio::time::sleep(delay).await;
        token.cancel();
    });
}

#[tokio::test]
async fn a_shutdown_interrupts_a_consumer_on_a_quiet_queue() {
    // The bug this is here for, found by a test that hung: a consumer sits in
    // `next()` until the broker sends something, which on a quiet queue is for
    // ever -- so a loop that checked the token only between reconnections would
    // never notice a shutdown, and the process would hang until it was killed.
    let name = "pneuma.test.sup.quiet";
    wipe(name).await;
    let token = CancellationToken::new();
    cancel_after(token.clone(), Duration::from_millis(200));

    let stopped = tokio::time::timeout(
        Duration::from_secs(10),
        supervise(
            &url(),
            &queue(name),
            "test-quiet",
            fast(),
            token,
            |_body| async { Outcome::Handled },
        ),
    )
    .await;
    if stopped.is_err() {
        panic!("a cancelled consumer must stop, not hang");
    }
}

#[tokio::test]
async fn a_broker_that_is_not_there_backs_off_rather_than_exiting() {
    // Nothing is listening, so every attempt fails. The loop must keep trying
    // and must still stop when asked -- including while it is asleep between
    // attempts, which is where it spends almost all of its time.
    let token = CancellationToken::new();
    cancel_after(token.clone(), Duration::from_millis(150));

    let stopped = tokio::time::timeout(
        Duration::from_secs(10),
        supervise(
            "amqp://127.0.0.1:1/%2f",
            &queue("pneuma.test.sup.absent"),
            "test-absent",
            fast(),
            token,
            |_body| async { Outcome::Handled },
        ),
    )
    .await;
    if stopped.is_err() {
        panic!("a cancelled reconnect loop must stop, not hang");
    }
}

#[tokio::test]
async fn every_way_of_failing_to_attach_is_a_connect_failure() {
    // Called directly rather than through `supervise`, because driving these
    // through the loop means cancelling it after a sleep -- and a loop that was
    // cancelled before it got as far as attaching still passes that test.
    //
    // Three ways, all permanent-looking and all handled the same: the
    // supervisor's job is to keep running and say so, not to take the process
    // down or to exit leaving a pod that looks healthy and consumes nothing.
    let name = "pneuma.test.sup.conflict";
    wipe(name).await;
    let setup = admin().await;
    if let Err(error) = setup
        .channel
        .queue_declare(
            name.into(),
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
    let long = "q".repeat(244);

    for (what, uri, queue_name) in [
        ("a broker that is not there", "amqp://127.0.0.1:1/%2f", name),
        // 244 bytes is a legal queue name and its derived `.dead_letter` is
        // 256, which the AMQP `shortstr` cannot carry at all.
        (
            "a dead-letter name that will not fit",
            url().as_str(),
            long.as_str(),
        ),
        // The commonest AMQP failure in a deploy: a queue left over from an
        // earlier version, whose arguments are part of its identity.
        (
            "a queue that already exists differently",
            url().as_str(),
            name,
        ),
    ] {
        let event = attach(uri, &queue(queue_name), "test-attach", &mut |_body: Vec<
            u8,
        >| async {
            Outcome::Handled
        })
        .await;
        let pneuma_transport::Event::ConnectFailed(why) = event else {
            panic!("{what} is a connect failure: {event:?}");
        };
        assert!(!why.is_empty(), "{what} should say why");
    }
    wipe(name).await;
}

#[tokio::test]
async fn a_closed_connection_ends_the_attachment_cleanly() {
    // The other end of `pump`: not "the broker refused" but "there are no more
    // messages". A connection closed with nothing owed ends the stream rather
    // than failing it, and the supervisor treats that as a drop and reattaches.
    let name = "pneuma.test.sup.closed";
    wipe(name).await;
    let Ok(amqp) = Amqp::connect(&url()).await else {
        panic!("could not reach the broker");
    };
    let Ok(spec) = QueueSpec::new(queue(name)) else {
        panic!("that name has a dead-letter name");
    };
    let Ok(mut consuming) = amqp.consume(&spec, "test-closed").await else {
        panic!("the queues should declare");
    };
    if let Err(error) = amqp.close().await {
        panic!("closing should work: {error}");
    }

    let event = pump(&mut consuming, Instant::now(), |_body| async {
        Outcome::Handled
    })
    .await;
    let pneuma_transport::Event::Dropped { why, .. } = event else {
        panic!("a closed connection ends the attachment: {event:?}");
    };
    assert!(why.contains("cancelled"), "and says so plainly: {why}");
}

#[tokio::test]
async fn a_queue_that_disappears_ends_the_attachment_and_the_loop_reattaches() {
    // Deleting a consumed queue cancels the consumer: the stream ends, and this
    // is the `Ok(None)` half of `pump`. It is a *drop*, not a shutdown -- so the
    // supervisor reconnects, which is what redeclares the queue.
    let name = "pneuma.test.sup.deleted";
    wipe(name).await;
    let token = CancellationToken::new();
    let running = tokio::spawn({
        let token = token.clone();
        async move {
            supervise(
                &url(),
                &queue(name),
                "test-deleted",
                fast(),
                token,
                |_body| async { Outcome::Handled },
            )
            .await;
        }
    });

    tokio::time::sleep(Duration::from_millis(200)).await;
    let deleter = admin().await;
    if let Err(error) = deleter
        .channel
        .queue_delete(name.into(), QueueDeleteOptions::default())
        .await
    {
        panic!("could not delete the queue: {error}");
    }

    // It comes back, because reconnecting redeclares it. A fresh channel per
    // attempt, because a passive declare that 404s *closes* the channel it was
    // asked on -- so reusing one would report "still missing" for ever.
    let mut redeclared = false;
    for _ in 0..100_u32 {
        tokio::time::sleep(Duration::from_millis(20)).await;
        let probe = admin().await;
        let found = probe
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
        if found.is_ok() {
            redeclared = true;
            break;
        }
    }
    assert!(redeclared, "a dropped attachment reconnects and redeclares");

    token.cancel();
    match tokio::time::timeout(Duration::from_secs(5), running).await {
        Ok(Ok(())) => {}
        Ok(Err(error)) => panic!("the loop panicked: {error}"),
        Err(_) => panic!("it must stop when asked"),
    }
}

#[tokio::test]
async fn a_settlement_owed_to_a_connection_that_has_gone_is_reported() {
    // The other half of `pump`'s ending: not "the consumer was cancelled" but
    // "the broker refused". A delivery dropped without being settled posts a
    // requeue, and `next()` pays it before asking for anything more -- so a
    // connection closed in between makes that payment fail. Reporting it is
    // what lets the supervisor reconnect; swallowing it would leave a consumer
    // attached to a dead channel.
    let name = "pneuma.test.sup.owed";
    wipe(name).await;
    let Ok(amqp) = Amqp::connect(&url()).await else {
        panic!("could not reach the broker");
    };
    let Ok(spec) = QueueSpec::new(queue(name)) else {
        panic!("that name has a dead-letter name");
    };
    let Ok(mut consuming) = amqp.consume(&spec, "test-owed").await else {
        panic!("the queues should declare");
    };
    if let Err(error) = amqp.publish(&key(name), b"owed").await {
        panic!("publishing should work: {error}");
    }

    let Ok(Some(delivery)) = consuming.next().await else {
        panic!("the message should arrive");
    };
    // Dropped, not settled: that posts a requeue for the next call to pay.
    drop(delivery);
    if let Err(error) = amqp.close().await {
        panic!("closing should work: {error}");
    }

    let event = pump(&mut consuming, Instant::now(), |_body| async {
        Outcome::Handled
    })
    .await;
    let pneuma_transport::Event::Dropped { why, .. } = event else {
        panic!("a dead channel ends the attachment: {event:?}");
    };
    assert!(!why.is_empty(), "and says why: {why}");
}

/// A handler that records what it was given, for the settlement path.
#[derive(Default)]
struct Seen(Mutex<Vec<Vec<u8>>>);

#[tokio::test]
async fn a_rejected_message_goes_to_the_dead_letter_queue() {
    // The settlement table, end to end rather than against a recorder: a body
    // the handler calls permanently wrong must reach the dead-letter queue,
    // because retrying it is how one bad message becomes a loop.
    let name = "pneuma.test.sup.rejected";
    wipe(name).await;
    let seen = Arc::new(Seen::default());
    let token = CancellationToken::new();
    let running = tokio::spawn({
        let token = token.clone();
        let seen = Arc::clone(&seen);
        async move {
            supervise(&url(), &queue(name), "test-rejected", fast(), token, {
                move |body: Vec<u8>| {
                    let seen = Arc::clone(&seen);
                    async move {
                        match seen.0.lock() {
                            Ok(mut seen) => seen.push(body),
                            Err(poisoned) => poisoned.into_inner().push(body),
                        }
                        Outcome::Rejected("malformed".to_owned())
                    }
                }
            })
            .await;
        }
    });

    let Ok(amqp) = Amqp::connect(&url()).await else {
        panic!("could not reach the broker");
    };
    let mut published = false;
    for _ in 0..100_u32 {
        if amqp.publish(&key(name), b"bad").await.is_ok() {
            published = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(published, "the queue should be declared by the consumer");

    let admin = admin().await;
    let mut dead_lettered = false;
    for _ in 0..100_u32 {
        tokio::time::sleep(Duration::from_millis(20)).await;
        let found = admin
            .channel
            .queue_declare(
                format!("{name}.dead_letter").as_str().into(),
                QueueDeclareOptions {
                    passive: true,
                    ..QueueDeclareOptions::default()
                },
                FieldTable::default(),
            )
            .await;
        if matches!(found, Ok(ref queue) if queue.message_count() == 1) {
            dead_lettered = true;
            break;
        }
    }
    assert!(
        dead_lettered,
        "a permanent failure is dead-lettered, not retried"
    );

    token.cancel();
    match tokio::time::timeout(Duration::from_secs(5), running).await {
        Ok(Ok(())) => {}
        Ok(Err(error)) => panic!("the loop panicked: {error}"),
        Err(_) => panic!("it must stop when asked"),
    }
}
