//! The whole process, against a real RabbitMQ, a real MongoDB and a stub
//! admission service.
//!
//! Every other file here tests one half against a fake for the other. This one
//! starts the real thing and asks the question the halves cannot: whether a
//! message published to the input queue becomes a run document and a
//! submission without anybody calling a function in between.
//!
//! ```sh
//! PNEUMA_TEST_MONGO_URL=mongodb://127.0.0.1:27018 \
//! PNEUMA_TEST_AMQP_URL='amqp://guest:guest@127.0.0.1:5673/%2f' \
//!     cargo test -p pneuma-intake --test service
//! ```

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::routing::post;
use axum::Router;
use mongodb::bson::{doc, Document};
use mongodb::{Client as MongoClient, Collection};
use pneuma_amqp::{QueueName, RoutingKey};
use pneuma_intake::{boot, settle, Admission, Admits, BootError, Config, Events, Outcome};
use pneuma_transport::{Amqp, Backoff, Delivery, Disposition, Settle};
use secrecy::SecretString;
use serde_json::{json, Value};
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;

const PIPELINE: &str = include_str!("fixtures/pipeline1.yaml");

fn amqp_url() -> String {
    let Ok(url) = std::env::var("PNEUMA_TEST_AMQP_URL") else {
        panic!("PNEUMA_TEST_AMQP_URL is not set; see the header of this file");
    };
    url
}

fn mongo_url() -> String {
    let Ok(url) = std::env::var("PNEUMA_TEST_MONGO_URL") else {
        panic!("PNEUMA_TEST_MONGO_URL is not set; see the header of this file");
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

/// A recorder standing in for a broker, for the settlement table.
#[derive(Debug, Default)]
struct Recorder(Mutex<Vec<Disposition>>);

impl Settle for Recorder {
    fn settle(&self, _tag: u64, disposition: Disposition) {
        match self.0.lock() {
            Ok(mut seen) => seen.push(disposition),
            Err(poisoned) => poisoned.into_inner().push(disposition),
        }
    }
}

#[test]
fn the_settlement_table_is_the_whole_mapping() {
    // Three outcomes, three answers to the broker, and the correspondence is
    // exact. Getting `Rejected` wrong makes a malformed message a loop; getting
    // `Retry` wrong makes a database blip a dead letter somebody has to replay.
    for (outcome, expected) in [
        (Outcome::Handled, Disposition::Ack),
        (
            Outcome::Rejected("malformed".to_owned()),
            Disposition::Nack { requeue: false },
        ),
        (
            Outcome::Retry("mongo is down".to_owned()),
            Disposition::Nack { requeue: true },
        ),
    ] {
        let recorder = Arc::new(Recorder::default());
        let delivery = Delivery::new(1, Vec::new(), Arc::clone(&recorder));
        assert_eq!(settle(delivery, &outcome), expected, "{outcome:?}");
        let seen = match recorder.0.lock() {
            Ok(seen) => seen.clone(),
            Err(poisoned) => poisoned.into_inner().clone(),
        };
        assert_eq!(seen, vec![expected], "and posted exactly once");
    }
}

/// A stub admission service that remembers what it was offered.
async fn stub_admission(seen: Arc<Mutex<Vec<Value>>>) -> SocketAddr {
    let router = Router::new().route(
        "/pneuma-admission/runs",
        post(move |axum::Json(body): axum::Json<Value>| {
            let seen = Arc::clone(&seen);
            async move {
                match seen.lock() {
                    Ok(mut seen) => seen.push(body),
                    Err(poisoned) => poisoned.into_inner().push(body),
                }
                (
                    axum::http::StatusCode::ACCEPTED,
                    axum::Json(json!({"status": "queued"})),
                )
            }
        }),
    );
    let Ok(listener) = TcpListener::bind("127.0.0.1:0").await else {
        panic!("a loopback port");
    };
    let Ok(address) = listener.local_addr() else {
        panic!("a bound listener has an address");
    };
    tokio::spawn(async move {
        let _ = axum::serve(listener, router).await;
    });
    address
}

async fn collections(name: &str) -> (Collection<Document>, Collection<Document>) {
    let Ok(client) = MongoClient::with_uri_str(&mongo_url()).await else {
        panic!("could not connect to {}", mongo_url());
    };
    let database = client.database(name);
    let pipelines: Collection<Document> = database.collection("pipelines");
    let runs: Collection<Document> = database.collection("runs");
    for collection in [&pipelines, &runs] {
        if let Err(error) = collection.drop().await {
            panic!("could not clear a collection: {error}");
        }
    }
    (pipelines, runs)
}

fn config(database: &str, queues: &str, admission: SocketAddr) -> Config {
    let Some(listen) = "127.0.0.1:0".parse::<SocketAddr>().ok() else {
        panic!("a real address");
    };
    let Ok(backoff) = Backoff::new(
        Duration::from_millis(20),
        Duration::from_millis(50),
        Duration::from_secs(60),
    ) else {
        panic!("that is a backoff");
    };
    Config {
        mongodb_uri: SecretString::from(mongo_url()),
        mongodb_database: database.to_owned(),
        rabbitmq_uri: SecretString::from(amqp_url()),
        runs: queue(&format!("{queues}.input")),
        events: queue(&format!("{queues}.event")),
        definitions: queue(&format!("{queues}.create")),
        event_key: key(&format!("{queues}.forwarded")),
        admission: format!("http://{admission}"),
        listen,
        backoff,
    }
}

fn definition_body() -> Vec<u8> {
    let Ok(value) = serde_yaml::from_str::<Value>(PIPELINE) else {
        panic!("the corpus fixture parses");
    };
    match serde_json::to_vec(&value) {
        Ok(bytes) => bytes,
        Err(error) => panic!("and serialises: {error}"),
    }
}

fn run_body(pipeline_id: &str) -> Vec<u8> {
    let mut parts = pipeline_id.splitn(3, '.');
    let (Some(kind), Some(level), Some(name)) = (parts.next(), parts.next(), parts.next()) else {
        panic!("a pipeline id is three dotted parts");
    };
    let body = json!({
        "meta": {
            "job_id": "job-1", "tenant_id": "acme",
            "pipeline_type": kind, "pipeline_level": level, "pipeline_name": name,
        },
        "step_input": {"doc": "d"},
    });
    match serde_json::to_vec(&body) {
        Ok(bytes) => bytes,
        Err(error) => panic!("that serialises: {error}"),
    }
}

/// Polls until `check` answers, or gives up after a couple of seconds.
async fn until<F, Fut>(what: &str, mut check: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    for _ in 0..200_u32 {
        if check().await {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("timed out waiting for {what}");
}

#[tokio::test]
async fn a_definition_and_a_run_travel_from_amqp_to_mongo_and_admission() {
    let (pipelines, runs) = collections("pneuma_intake_test").await;
    let offered: Arc<Mutex<Vec<Value>>> = Arc::new(Mutex::new(Vec::new()));
    let admission = stub_admission(Arc::clone(&offered)).await;
    let config = config("pneuma_intake_test", "pneuma.test.boot", admission);

    // The queues are wiped first: a queue's arguments are part of its identity,
    // so one left over from an earlier version fails every declare with
    // `PRECONDITION_FAILED` and reads as a bug in the arguments.
    let Ok(publisher) = Amqp::connect(&amqp_url()).await else {
        panic!("could not reach the broker");
    };

    let token = CancellationToken::new();
    let (tx, rx) = oneshot::channel();
    let running = tokio::spawn({
        let config = config.clone();
        let token = token.clone();
        async move {
            boot::run(&config, token, |address| {
                let _ = tx.send(address);
            })
            .await
        }
    });
    let Ok(address) = rx.await else {
        panic!("the service should bind and say where");
    };

    // The health routes a probe hits, mounted by `run` rather than by a caller.
    let client = reqwest::Client::new();
    for path in ["healthz", "liveness"] {
        let url = format!("http://{address}/pneuma-intake/{path}");
        let Ok(response) = client.get(&url).send().await else {
            panic!("{url} should answer");
        };
        assert_eq!(response.status().as_u16(), 200, "{url}");
    }

    // Published in a loop until it lands, because `Amqp::publish` sets
    // `mandatory` and the queues do not exist until the consumers declare them:
    // a routing key with nothing behind it comes back `NO_ROUTE` rather than
    // being discarded in silence. That is the whole point of the confirms, and
    // the price is that a publisher has to wait for its reader.
    //
    // A definition first, because a run naming a pipeline that is not stored is
    // dead-lettered rather than retried -- right behaviour, wrong order here.
    until("the create queue to be declared", || async {
        publisher
            .publish(&key("pneuma.test.boot.create"), &definition_body())
            .await
            .is_ok()
    })
    .await;
    until("the definition to be stored", || async {
        matches!(pipelines.count_documents(doc! {}).await, Ok(1))
    })
    .await;

    let Ok(Some(stored)) = pipelines.find_one(doc! {}).await else {
        panic!("the definition should be readable back");
    };
    let Ok(pipeline_id) = stored.get_str("pipeline_id") else {
        panic!("the fixture has a pipeline_id");
    };

    let run = run_body(pipeline_id);
    until("the input queue to be declared", || async {
        publisher
            .publish(&key("pneuma.test.boot.input"), &run)
            .await
            .is_ok()
    })
    .await;
    until("the run document to be written", || async {
        matches!(runs.count_documents(doc! {}).await, Ok(1))
    })
    .await;

    let Ok(Some(written)) = runs.find_one(doc! { "run_id": "job-1" }).await else {
        panic!("the run should be readable back");
    };
    assert_eq!(written.get_str("status").ok(), Some("created"));
    let Ok(state) = written.get_document("state") else {
        panic!("state should be a document");
    };
    assert!(
        !state.contains_key("steps"),
        "flat, keyed by node id: {state:?}"
    );

    until("the run to be offered to admission", || async {
        match offered.lock() {
            Ok(seen) => !seen.is_empty(),
            Err(poisoned) => !poisoned.into_inner().is_empty(),
        }
    })
    .await;
    let seen = match offered.lock() {
        Ok(seen) => seen.clone(),
        Err(poisoned) => poisoned.into_inner().clone(),
    };
    assert_eq!(seen.len(), 1);
    assert!(seen[0].get("pipeline").is_some(), "{:?}", seen[0]);

    token.cancel();
    match running.await {
        Ok(Ok(())) => {}
        Ok(Err(error)) => panic!("a cancelled service exits cleanly, not with {error}"),
        Err(error) => panic!("the service task panicked: {error}"),
    }
}

#[tokio::test]
async fn an_event_is_forwarded_to_its_queue() {
    // The third loop. Its destination is somebody else's queue, so the test
    // declares it -- and that is the production shape too: `Amqp::publish` sets
    // `mandatory`, so forwarding to a key with nothing bound behind it fails
    // loudly rather than being discarded the way the original discards it.
    let (_pipelines, _runs) = collections("pneuma_intake_events").await;
    let offered: Arc<Mutex<Vec<Value>>> = Arc::new(Mutex::new(Vec::new()));
    let admission = stub_admission(Arc::clone(&offered)).await;
    let config = config("pneuma_intake_events", "pneuma.test.ev", admission);

    let Ok(connection) =
        lapin::Connection::connect(&amqp_url(), lapin::ConnectionProperties::default()).await
    else {
        panic!("could not reach the broker");
    };
    let Ok(channel) = connection.create_channel().await else {
        panic!("could not open a channel");
    };
    // Deleted and redeclared, not just declared: a message left over from an
    // earlier run would satisfy the assertion below without this run forwarding
    // anything, which is a test that passes while the loop is broken.
    if let Err(error) = channel
        .queue_delete(
            "pneuma.test.ev.forwarded".into(),
            lapin::options::QueueDeleteOptions::default(),
        )
        .await
    {
        panic!("the destination should be removable: {error}");
    }
    if let Err(error) = channel
        .queue_declare(
            "pneuma.test.ev.forwarded".into(),
            lapin::options::QueueDeclareOptions {
                durable: true,
                ..lapin::options::QueueDeclareOptions::default()
            },
            lapin::types::FieldTable::default(),
        )
        .await
    {
        panic!("the destination should declare: {error}");
    }

    let token = CancellationToken::new();
    let (tx, rx) = oneshot::channel();
    let running = tokio::spawn({
        let config = config.clone();
        let token = token.clone();
        async move {
            boot::run(&config, token, |address| {
                let _ = tx.send(address);
            })
            .await
        }
    });
    let Ok(_address) = rx.await else {
        panic!("the service should bind");
    };

    let Ok(publisher) = Amqp::connect(&amqp_url()).await else {
        panic!("could not reach the broker");
    };
    let event = json!({
        "event": "finished",
        "meta": {
            "job_id": "job-1", "tenant_id": "acme",
            "pipeline_type": "invoice", "pipeline_level": "page", "pipeline_name": "default",
        },
    });
    let Ok(body) = serde_json::to_vec(&event) else {
        panic!("that serialises");
    };
    until("the event queue to be declared", || async {
        publisher
            .publish(&key("pneuma.test.ev.event"), &body)
            .await
            .is_ok()
    })
    .await;

    until("the event to be forwarded", || async {
        let declared = channel
            .queue_declare(
                "pneuma.test.ev.forwarded".into(),
                lapin::options::QueueDeclareOptions {
                    passive: true,
                    ..lapin::options::QueueDeclareOptions::default()
                },
                lapin::types::FieldTable::default(),
            )
            .await;
        matches!(declared, Ok(ref queue) if queue.message_count() >= 1)
    })
    .await;

    token.cancel();
    match running.await {
        Ok(Ok(())) => {}
        Ok(Err(error)) => panic!("a cancelled service exits cleanly, not with {error}"),
        Err(error) => panic!("the service task panicked: {error}"),
    }
}

#[tokio::test]
async fn forwarding_to_a_queue_nobody_declared_is_reported() {
    // Louder than the original, which publishes without `mandatory` and loses
    // the event in silence. An event nobody is listening for is a
    // misconfiguration, and the alternative is finding out when somebody asks
    // why a run never reported finishing.
    let Ok(amqp) = Amqp::connect(&amqp_url()).await else {
        panic!("could not reach the broker");
    };
    let forwarder = pneuma_intake::Forwarder::new(amqp, key("pneuma.test.nothing.bound"));
    let Err(why) = forwarder.forward(&json!({"event": "finished"})).await else {
        panic!("nothing is bound to that key");
    };
    assert!(!why.is_empty(), "the operator is told: {why}");
}

#[tokio::test]
async fn an_address_already_in_use_stops_the_whole_process() {
    // The property is not the bind failure. It is that the three consume loops
    // stop with the server: a replica that kept consuming after failing to
    // serve is one nobody can drain, and this test would hang rather than fail
    // if they were not joined.
    let (_pipelines, _runs) = collections("pneuma_intake_bind").await;
    let offered: Arc<Mutex<Vec<Value>>> = Arc::new(Mutex::new(Vec::new()));
    let admission = stub_admission(offered).await;
    let mut config = config("pneuma_intake_bind", "pneuma.test.bind", admission);
    let Ok(occupied) = TcpListener::bind("127.0.0.1:0").await else {
        panic!("a loopback port");
    };
    let Ok(taken) = occupied.local_addr() else {
        panic!("a bound listener has an address");
    };
    config.listen = taken;

    let Err(error) = boot::run(&config, CancellationToken::new(), |_| {
        panic!("nothing should bind");
    })
    .await
    else {
        panic!("{taken} is already listening");
    };
    let BootError::Serve { address, .. } = &error else {
        panic!("wrong variant: {error:?}");
    };
    assert_eq!(*address, taken);
}

#[tokio::test]
async fn admission_answering_badly_is_reported_with_its_status() {
    // A non-2xx is a `Retry`, and the run document is already written -- so a
    // redelivery re-announces it rather than starting again, which is exactly
    // what should happen.
    let router = Router::new().route(
        "/pneuma-admission/runs",
        post(|| async { axum::http::StatusCode::SERVICE_UNAVAILABLE }),
    );
    let Ok(listener) = TcpListener::bind("127.0.0.1:0").await else {
        panic!("a loopback port");
    };
    let Ok(address) = listener.local_addr() else {
        panic!("a bound listener has an address");
    };
    tokio::spawn(async move {
        let _ = axum::serve(listener, router).await;
    });

    let admission = Admission::new(reqwest::Client::new(), &format!("http://{address}/"));
    assert_eq!(
        admission.url(),
        format!("http://{address}/pneuma-admission/runs"),
        "a trailing slash on the base does not double up"
    );
    let Err(why) = admission.submit("job-1", &json!({})).await else {
        panic!("503 is not an acceptance");
    };
    assert!(why.contains("503"), "the status reaches the log: {why}");
    assert!(why.contains("job-1"), "and so does the run: {why}");
}

#[tokio::test]
async fn a_broker_that_is_not_there_is_a_startup_failure() {
    // Before anything binds. A wrong AMQP URI is wrong for ever, and a pod that
    // never becomes ready says so more usefully than one that reports healthy
    // and reconnects in a loop.
    let offered: Arc<Mutex<Vec<Value>>> = Arc::new(Mutex::new(Vec::new()));
    let admission = stub_admission(offered).await;
    let mut config = config("pneuma_intake_test", "pneuma.test.nobroker", admission);
    config.rabbitmq_uri = SecretString::from("amqp://127.0.0.1:1/%2f".to_owned());

    let Err(error) = boot::run(&config, CancellationToken::new(), |_| {
        panic!("nothing should bind");
    })
    .await
    else {
        panic!("nothing is listening on port 1");
    };
    assert!(
        matches!(error, BootError::Broker(_)),
        "wrong variant: {error:?}"
    );
}
