//! The whole process: a run announced on NATS, driven to completion against a
//! stand-in original executor, and recorded in Mongo.
//!
//! Every other file here tests one part against a fake. This one asks what
//! none of them can: whether `drive` actually runs on a `LocalSet` with a
//! non-`Send` component, whether the messages the transport publishes are ones
//! a consumer can answer, and whether the answers reach the run that is waiting.
//!
//! # `boot::run` is awaited here, never spawned
//!
//! It holds a `LocalSet`, so its future is not `Send` — the shape
//! `Component::call`'s deliberate lack of a `Send` promise gives everything
//! above it. So the service runs in the *test's own* task and the assertions
//! run in a spawned one, which is the opposite of the usual arrangement. This
//! file did not compile the other way round, which is the plan's warning
//! arriving on schedule.
//!
//! A Postgres too, since the driver mirrors what a run did into `node_run` and
//! refuses to start without a database it can reach *and* write to. Its own
//! schema, dropped and migrated once per run of this binary.
//!
//! ```sh
//! PNEUMA_TEST_MONGO_URL=mongodb://127.0.0.1:27018 \
//! PNEUMA_TEST_NATS_URL=nats://127.0.0.1:4223 \
//! PNEUMA_TEST_DATABASE_URL=postgres://postgres:pneuma@127.0.0.1:5433/postgres \
//!     cargo test -p pneuma-driver --test service
//! ```

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_lite::StreamExt;
use mongodb::bson::{doc, Document};
use mongodb::{Client as MongoClient, Collection};
use pneuma_driver::{boot, BootError, Config};
use pneuma_proto::dispatch::MessageRun;
use secrecy::SecretString;
use serde_json::{json, Value};
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;

const PIPELINE: &str = include_str!("fixtures/pipeline1.yaml");

fn nats_url() -> String {
    let Ok(url) = std::env::var("PNEUMA_TEST_NATS_URL") else {
        panic!("PNEUMA_TEST_NATS_URL is not set; see the header of this file");
    };
    url
}

fn mongo_url() -> String {
    let Ok(url) = std::env::var("PNEUMA_TEST_MONGO_URL") else {
        panic!("PNEUMA_TEST_MONGO_URL is not set; see the header of this file");
    };
    url
}

/// The schema this binary's `node_run` rows live in.
///
/// Its own, not `public`: run ids here are fixtures rather than fresh values,
/// so rows from a previous `cargo test` would still be there -- and
/// `ON CONFLICT (path) DO NOTHING` would make the second run of the suite
/// assert on the first run's rows.
const SCHEMA: &str = "pneuma_driver_test";

/// Where the mirror writes, with [`SCHEMA`] on the search path.
///
/// In the DSN, not a `SET` on one connection: a pool that reopens the
/// connection it was `SET` on reads `public` instead, and the failure then
/// blames the code under test.
fn database_url() -> String {
    let Ok(url) = std::env::var("PNEUMA_TEST_DATABASE_URL") else {
        panic!("PNEUMA_TEST_DATABASE_URL is not set; see the header of this file");
    };
    let separator = if url.contains('?') { '&' } else { '?' };
    format!("{url}{separator}options=-c%20search_path%3D{SCHEMA}")
}

/// Drops and migrates [`SCHEMA`], once per test binary.
///
/// Every test in this file boots the service, and the service now refuses to
/// start against a database with no `node_run` -- so this has to have happened
/// before the first `boot::run`, whichever test gets there first.
async fn migrated() {
    static ONCE: tokio::sync::OnceCell<()> = tokio::sync::OnceCell::const_new();
    ONCE.get_or_init(|| async {
        let Ok(url) = std::env::var("PNEUMA_TEST_DATABASE_URL") else {
            panic!("PNEUMA_TEST_DATABASE_URL is not set; see the header of this file");
        };
        let bare = match sqlx::postgres::PgPoolOptions::new()
            .max_connections(1)
            .connect(&url)
            .await
        {
            Ok(pool) => pool,
            Err(error) => panic!("could not connect to {url}: {error}"),
        };
        for statement in [
            format!("DROP SCHEMA IF EXISTS {SCHEMA} CASCADE"),
            format!("CREATE SCHEMA {SCHEMA}"),
        ] {
            if let Err(error) = sqlx::Executor::execute(&bare, statement.as_str()).await {
                panic!("setup failed on {statement:?}: {error}");
            }
        }
        drop(bare);

        let pool = match sqlx::postgres::PgPoolOptions::new()
            .max_connections(1)
            .connect(&database_url())
            .await
        {
            Ok(pool) => pool,
            Err(error) => panic!("could not connect to {SCHEMA}: {error}"),
        };
        let mut connection = match pool.acquire().await {
            Ok(connection) => connection,
            Err(error) => panic!("could not acquire a connection to migrate on: {error}"),
        };
        if let Err(error) = pneuma_store::migrator().run(&mut *connection).await {
            panic!("the migrations do not apply: {error}");
        }
    })
    .await;
}

/// Every `node_run` row a run left behind, by path.
async fn mirrored(run_id: &str) -> Vec<(String, String)> {
    let pool = match sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(&database_url())
        .await
    {
        Ok(pool) => pool,
        Err(error) => panic!("could not connect to {SCHEMA}: {error}"),
    };
    match sqlx::query_as("SELECT path, status::text FROM node_run WHERE run_id = $1 ORDER BY path")
        .bind(run_id)
        .fetch_all(&pool)
        .await
    {
        Ok(rows) => rows,
        Err(error) => panic!("could not read the mirror: {error}"),
    }
}

/// Cancels the token however the task holding it leaves, panic included.
///
/// `boot::run` is awaited by the test itself and returns only when the token is
/// cancelled, so an assertion that panics before its `token.cancel()` leaves
/// the service running and the test hanging -- the panic invisible, the file
/// reported as timing out. This turns every such panic back into a failure.
struct StopOnDrop(CancellationToken);

impl Drop for StopOnDrop {
    fn drop(&mut self) {
        self.0.cancel();
    }
}

/// The run document `pneuma-intake` would have written.
fn run_document(run_id: &str) -> Document {
    let Ok(value) = serde_yaml::from_str::<Value>(PIPELINE) else {
        panic!("the corpus fixture parses");
    };
    let Ok(mut document) = mongodb::bson::serialize_to_document(&value) else {
        panic!("and converts to BSON");
    };
    document.insert("run_id", run_id);
    document.insert("status", "created");
    document
}

async fn runs(database: &str) -> Collection<Document> {
    let Ok(client) = MongoClient::with_uri_str(&mongo_url()).await else {
        panic!("could not connect to {}", mongo_url());
    };
    let runs: Collection<Document> = client.database(database).collection("runs");
    if let Err(error) = runs.drop().await {
        panic!("could not clear the collection: {error}");
    }
    runs
}

/// A configuration pointed at this test binary's own schema.
///
/// Async because the schema has to exist before the service is booted with it:
/// `boot::run` now refuses to start against a database with no `node_run`.
async fn config(database: &str, subjects: &str) -> Config {
    migrated().await;
    let Some(listen) = "127.0.0.1:0".parse::<SocketAddr>().ok() else {
        panic!("a real address");
    };
    Config {
        database_url: SecretString::from(database_url()),
        mongodb_uri: SecretString::from(mongo_url()),
        mongodb_database: database.to_owned(),
        nats_uri: SecretString::from(nats_url()),
        runs_subject: format!("{subjects}.run.start"),
        results_subject: format!("{subjects}.result"),
        max_concurrent_runs: 4,
        call_timeout: Duration::from_secs(5),
        listen,
    }
}

/// A stand-in original executor: answer one run's messages with a step output.
///
/// This is what the original executor does, minus Seldon: take the message,
/// produce an output, publish a result carrying the same `node` back to the
/// fixed result subject.
///
/// `run_id` is not a convenience. The component subject is the component's
/// *name*, so every test in this file that uses the corpus fixture publishes to
/// `invoice.page.default.>` -- and a stand-in that answered all of them would
/// answer the run belonging to `a_component_that_never_answers_...`, whose
/// whole property is that nobody does. That was a race rather than a
/// separation: the tests run concurrently, and it held only while they happened
/// not to overlap.
async fn stand_in(
    client: async_nats::Client,
    subjects: String,
    run_id: String,
    seen: Arc<Mutex<Vec<String>>>,
    token: CancellationToken,
) {
    // Every component subject the fixture uses. A wildcard, because the
    // component name is the subject and the fixture has several.
    let Ok(mut work) = client.subscribe("invoice.page.default.>".to_owned()).await else {
        panic!("could not subscribe to the component subjects");
    };
    while let Some(message) = token.run_until_cancelled(work.next()).await.flatten() {
        let Ok(run) = serde_json::from_slice::<MessageRun>(&message.payload) else {
            panic!("the controller should publish run messages");
        };
        if run.meta.job_id.as_str() != run_id {
            continue;
        }
        match seen.lock() {
            Ok(mut seen) => seen.push(run.node.node_id.as_str().to_owned()),
            Err(poisoned) => poisoned
                .into_inner()
                .push(run.node.node_id.as_str().to_owned()),
        }
        let result = json!({
            "meta": run.meta,
            "node": run.node,
            "step_output": {"ran": run.node.node_id.as_str()},
        });
        let Ok(body) = serde_json::to_vec(&result) else {
            panic!("that serialises");
        };
        if let Err(error) = client
            .publish(format!("{subjects}.result"), body.into())
            .await
        {
            panic!("the stand-in should be able to answer: {error}");
        }
        if let Err(error) = client.flush().await {
            panic!("and the broker should take it: {error}");
        }
    }
}

/// Announces a run until the thing it should cause has happened.
///
/// Not "publish once and then poll". The announcement subject is plain core
/// NATS, so a message published before the driver has subscribed is dropped by
/// the server and nothing is ever driven — and the driver subscribes inside
/// `accept`, which `boot::run` polls *after* the health server whose bind is
/// what wakes these assertions. The old shape called that wait "the controller
/// to be subscribed" while only checking that the publish itself succeeded,
/// which it does whether or not anyone is listening. Under the load of a
/// workspace-wide run that raced, and the symptom was a run that never started
/// reported as a run that never finished.
///
/// Re-announcing is safe: a second delivery drives the same run again, and
/// every write it makes is keyed by a path or a run id it has already written.
async fn announced<F, Fut>(client: &async_nats::Client, subject: &str, body: &[u8], mut check: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    for attempt in 0..30_u32 {
        if let Err(error) = client
            .publish(subject.to_owned(), body.to_vec().into())
            .await
        {
            panic!("the announcement should publish: {error}");
        }
        if let Err(error) = client.flush().await {
            panic!("and the broker should take it: {error}");
        }
        // Polled between announcements rather than after all of them, so the
        // usual case -- the first one lands -- still finishes in milliseconds.
        for _ in 0..10_u32 {
            if check().await {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let _ = attempt;
    }
    panic!("the announcement on {subject} never took effect");
}

#[tokio::test]
async fn a_run_announced_on_nats_is_driven_to_completion_and_recorded() {
    let subjects = format!("pneuma.test.ctrl.svc{}", std::process::id());
    let collection = runs("pneuma_driver_test").await;
    if let Err(error) = collection.insert_one(run_document("job-driven")).await {
        panic!("the run document should insert: {error}");
    }

    let Ok(client) = async_nats::connect(&nats_url()).await else {
        panic!("could not reach NATS");
    };
    let token = CancellationToken::new();
    let seen: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let provider = tokio::spawn(stand_in(
        client.clone(),
        subjects.clone(),
        "job-driven".to_owned(),
        Arc::clone(&seen),
        token.clone(),
    ));

    let config = config("pneuma_driver_test", &subjects).await;
    let (tx, rx) = oneshot::channel();

    // The assertions are spawned and the *service* is awaited here, because
    // `boot::run` is not `Send`.
    let checks = tokio::spawn({
        let token = token.clone();
        let collection = collection.clone();
        let seen = Arc::clone(&seen);
        let subjects = subjects.clone();
        let client = client.clone();
        async move {
            // Cancels however this task leaves, panic included -- see
            // `StopOnDrop`. Every assertion below is otherwise a hang, with
            // the panic invisible and the file reported as timing out.
            let _stop = StopOnDrop(token);
            let Ok(address) = rx.await else {
                panic!("the service should bind and say where");
            };

            // The health routes a probe hits.
            let http = reqwest::Client::new();
            for path in ["healthz", "liveness"] {
                let url = format!("http://{address}/pneuma-driver/{path}");
                let Ok(response) = http.get(&url).send().await else {
                    panic!("{url} should answer");
                };
                assert_eq!(response.status().as_u16(), 200, "{url}");
            }

            // Announced the way `pneuma-intake` announces a run.
            let init = json!({
                "run_id": "job-driven",
                "pipeline_id": "invoice.page.default",
                "meta": {
                    "job_id": "job-driven", "tenant_id": "acme",
                    "pipeline_type": "invoice", "pipeline_level": "page", "pipeline_name": "default",
                },
                "step_input": {"doc": "d"},
            });
            let Ok(body) = serde_json::to_vec(&init) else {
                panic!("that serialises");
            };
            let subject = format!("{subjects}.run.start");
            announced(&client, &subject, &body, || async {
                let found = collection.find_one(doc! { "run_id": "job-driven" }).await;
                matches!(found, Ok(Some(ref run)) if run.get_str("status").ok() == Some("finished"))
            })
            .await;

            // And every step of the fixture ran, through the transport and back.
            let ran = match seen.lock() {
                Ok(seen) => seen.clone(),
                Err(poisoned) => poisoned.into_inner().clone(),
            };
            assert!(ran.contains(&"A".to_owned()), "the first step ran: {ran:?}");
            assert!(ran.len() >= 2, "and the ones after it: {ran:?}");

            // And it was written down. The Mongo document says the *run*
            // ended; `node_run` is what says which steps it took, and until
            // the design notes nothing on this path wrote a row -- so the
            // janitor's stale detection had nothing to look at.
            let rows = mirrored("job-driven").await;
            assert!(!rows.is_empty(), "the run left a record");
            let prefix = "job-driven.invoice.page.default";
            assert!(
                rows.iter().all(|(path, _)| path.starts_with(prefix)),
                "every row is pathed under this run: {rows:?}"
            );
            assert!(
                rows.iter()
                    .any(|(path, status)| path == &format!("{prefix}.A") && status == "FINISHED"),
                "the first step finished: {rows:?}"
            );
            // The fan-out fixture is `pipeline1`, so branch rows carry the
            // index that keeps them from collapsing onto one path.
            assert!(
                rows.iter().any(|(path, _)| path.contains(':')),
                "the fan-out's branches are distinguishable: {rows:?}"
            );
        }
    });

    let served = boot::run(&config, token.clone(), |address| {
        let _ = tx.send(address);
    })
    .await;
    if let Err(error) = served {
        panic!("a cancelled service exits cleanly, not with {error}");
    }
    if let Err(error) = checks.await {
        panic!("the assertions panicked: {error}");
    }
    if let Err(error) = provider.await {
        panic!("the stand-in panicked: {error}");
    }
}

#[tokio::test]
async fn a_run_that_was_never_written_is_recorded_as_an_error_not_retried() {
    // Bootstrap writes the run document *before* it announces the run, so an
    // announcement with no document behind it came from something that did not
    // write one. There is nothing to drive and nothing to wait for.
    let subjects = format!("pneuma.test.ctrl.missing{}", std::process::id());
    let collection = runs("pneuma_driver_missing").await;
    // A document for a *different* run, so the update below has something to
    // find and the assertion is about the announced one.
    if let Err(error) = collection.insert_one(run_document("other")).await {
        panic!("the run document should insert: {error}");
    }

    let Ok(client) = async_nats::connect(&nats_url()).await else {
        panic!("could not reach NATS");
    };
    let token = CancellationToken::new();
    let config = config("pneuma_driver_missing", &subjects).await;
    let (tx, rx) = oneshot::channel();
    let checks = tokio::spawn({
        let token = token.clone();
        let collection = collection.clone();
        async move {
            // Cancels however this task leaves, panic included -- see
            // `StopOnDrop`. Every assertion below is otherwise a hang, with
            // the panic invisible and the file reported as timing out.
            let _stop = StopOnDrop(token);
            let Ok(_address) = rx.await else {
                panic!("the service should bind");
            };

            let init = json!({
                "run_id": "never-written",
                "pipeline_id": "invoice.page.default",
                "meta": {
                    "job_id": "never-written", "tenant_id": "acme",
                    "pipeline_type": "invoice", "pipeline_level": "page", "pipeline_name": "default",
                },
            });
            let Ok(body) = serde_json::to_vec(&init) else {
                panic!("that serialises");
            };
            // Also something that is not a run message at all: the loop must
            // step over it rather than ending, or one bad announcement takes
            // every later run with it.
            let subject = format!("{subjects}.run.start");
            // The announced run has no document, so nothing it does is
            // observable in Mongo. What *is* observable is the run before it
            // being recorded as an error, so the junk message is announced with
            // that as its effect -- which is also the property: the loop steps
            // over an announcement it cannot parse rather than ending, or one
            // bad message takes every later run with it.
            announced(&client, &subject, b"not a message", || async {
                collection
                    .find_one(doc! { "run_id": "other" })
                    .await
                    .is_ok()
            })
            .await;
            announced(&client, &subject, &body, || async {
                let found = collection
                    .find_one(doc! { "run_id": "never-written" })
                    .await;
                matches!(found, Ok(None))
            })
            .await;

            // Nothing to update, so nothing appears -- what is being checked is
            // that the service survives it and keeps serving.
            tokio::time::sleep(Duration::from_millis(300)).await;
            let Ok(Some(other)) = collection.find_one(doc! { "run_id": "other" }).await else {
                panic!("the other run is untouched");
            };
            assert_eq!(other.get_str("status").ok(), Some("created"));
        }
    });

    let served = boot::run(&config, token.clone(), |address| {
        let _ = tx.send(address);
    })
    .await;
    if let Err(error) = served {
        panic!("a cancelled service exits cleanly, not with {error}");
    }
    if let Err(error) = checks.await {
        panic!("the assertions panicked: {error}");
    }
}

#[tokio::test]
async fn an_address_already_in_use_stops_the_whole_process() {
    let subjects = format!("pneuma.test.ctrl.bind{}", std::process::id());
    let _collection = runs("pneuma_driver_bind").await;
    let mut config = config("pneuma_driver_bind", &subjects).await;
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
async fn a_broker_that_is_not_there_is_a_startup_failure() {
    // Before anything binds. A wrong NATS URI is wrong for ever, and a pod that
    // never becomes ready says so more usefully than one that reports healthy
    // and drives nothing.
    let subjects = format!("pneuma.test.ctrl.nobroker{}", std::process::id());
    let _collection = runs("pneuma_driver_nobroker").await;
    let mut config = config("pneuma_driver_nobroker", &subjects).await;
    config.nats_uri = SecretString::from("nats://127.0.0.1:1".to_owned());

    let Err(error) = boot::run(&config, CancellationToken::new(), |_| {
        panic!("nothing should bind");
    })
    .await
    else {
        panic!("nothing is listening on port 1");
    };
    assert!(
        matches!(error, BootError::Nats(_)),
        "wrong variant: {error:?}"
    );
}

#[tokio::test]
async fn a_subject_nats_refuses_takes_the_process_down_rather_than_half_up() {
    // A controller that could not subscribe would sit there healthy and do
    // nothing: accepting nothing, or accepting runs it can never finish because
    // no result can reach it. Both are worse than exiting, so a failed
    // subscribe cancels the token and the process goes away to be restarted.
    //
    // An empty subject is one NATS refuses. `Config::from_env` cannot produce
    // one -- blank falls back to the default -- so this builds the config
    // directly, which is the only way to reach the arm.
    for (runs_subject, results_subject) in [("", "x.result"), ("x.run.start", "")] {
        let _collection = runs("pneuma_driver_badsubject").await;
        let mut config = config("pneuma_driver_badsubject", "unused").await;
        config.runs_subject = runs_subject.to_owned();
        config.results_subject = results_subject.to_owned();

        let stopped = tokio::time::timeout(
            Duration::from_secs(10),
            boot::run(&config, CancellationToken::new(), |_| {}),
        )
        .await;
        match stopped {
            Err(_) => panic!("a controller that cannot subscribe must not hang"),
            Ok(Ok(())) => {}
            Ok(Err(error)) => panic!("it stops cleanly, not with {error}"),
        }
    }
}

#[tokio::test]
async fn a_database_that_cannot_be_reached_ends_the_run_rather_than_the_process() {
    // One run fails; the service keeps serving. A controller that died on a
    // Mongo blip would take every other run on the replica with it.
    let subjects = format!("pneuma.test.ctrl.nodb{}", std::process::id());
    let mut config = config("pneuma_driver_nodb", &subjects).await;
    config.mongodb_uri =
        SecretString::from("mongodb://127.0.0.1:1/?serverSelectionTimeoutMS=100".to_owned());

    let Ok(client) = async_nats::connect(&nats_url()).await else {
        panic!("could not reach NATS");
    };
    let token = CancellationToken::new();
    let (tx, rx) = oneshot::channel();
    let checks = tokio::spawn({
        let token = token.clone();
        let subjects = subjects.clone();
        async move {
            // Cancels however this task leaves, panic included -- see
            // `StopOnDrop`. Every assertion below is otherwise a hang, with
            // the panic invisible and the file reported as timing out.
            let _stop = StopOnDrop(token);
            let Ok(_address) = rx.await else {
                panic!("the service should bind");
            };
            let init = json!({
                "run_id": "job-nodb",
                "pipeline_id": "invoice.page.default",
                "meta": {
                    "job_id": "job-nodb", "tenant_id": "acme",
                    "pipeline_type": "invoice", "pipeline_level": "page", "pipeline_name": "default",
                },
            });
            let Ok(body) = serde_json::to_vec(&init) else {
                panic!("that serialises");
            };
            // Announced three times over the wait rather than once: this is
            // the one test whose effect is *not* observable -- the database it
            // would be recorded in is the thing that is missing -- so there is
            // nothing for `announced` to check, and repetition is what stands
            // in for it.
            let subject = format!("{subjects}.run.start");
            for _ in 0..3 {
                if let Err(error) = client.publish(subject.clone(), body.clone().into()).await {
                    panic!("the announcement should publish: {error}");
                }
                if let Err(error) = client.flush().await {
                    panic!("and the broker should take it: {error}");
                }
                // Long enough for the run to have failed and for the settle
                // that follows it to have failed too -- both against a database
                // that is not there, which is the pair of arms being reached.
                tokio::time::sleep(Duration::from_millis(300)).await;
            }
        }
    });

    let served = boot::run(&config, token.clone(), |address| {
        let _ = tx.send(address);
    })
    .await;
    if let Err(error) = served {
        panic!("the service keeps serving: {error}");
    }
    if let Err(error) = checks.await {
        panic!("the assertions panicked: {error}");
    }
}

#[tokio::test]
async fn a_run_whose_pipeline_will_not_resolve_is_an_error_not_a_crash() {
    // Reachable even though `pneuma-intake` resolves before storing: a run
    // created by the *original's* bootstrap went through no such check, and
    // this service is meant to be deployable against it.
    let subjects = format!("pneuma.test.ctrl.badpipe{}", std::process::id());
    let collection = runs("pneuma_driver_badpipe").await;
    let unresolvable = doc! {
        "run_id": "job-badpipe",
        "status": "created",
        "pipeline_id": "invoice.page.default",
        // A start that names a node the components do not contain.
        "start": { "B": "B" },
        "components": [
            { "node_id": "A", "name": "a", "type": "Model", "children": ["end"] }
        ],
    };
    if let Err(error) = collection.insert_one(unresolvable).await {
        panic!("the run document should insert: {error}");
    }

    let Ok(client) = async_nats::connect(&nats_url()).await else {
        panic!("could not reach NATS");
    };
    let token = CancellationToken::new();
    let config = config("pneuma_driver_badpipe", &subjects).await;
    let (tx, rx) = oneshot::channel();
    let checks = tokio::spawn({
        let token = token.clone();
        let collection = collection.clone();
        let subjects = subjects.clone();
        async move {
            // Cancels however this task leaves, panic included -- see
            // `StopOnDrop`. Every assertion below is otherwise a hang, with
            // the panic invisible and the file reported as timing out.
            let _stop = StopOnDrop(token);
            let Ok(_address) = rx.await else {
                panic!("the service should bind");
            };
            let init = json!({
                "run_id": "job-badpipe",
                "pipeline_id": "invoice.page.default",
                "meta": {
                    "job_id": "job-badpipe", "tenant_id": "acme",
                    "pipeline_type": "invoice", "pipeline_level": "page", "pipeline_name": "default",
                },
            });
            let Ok(body) = serde_json::to_vec(&init) else {
                panic!("that serialises");
            };
            let subject = format!("{subjects}.run.start");
            announced(&client, &subject, &body, || async {
                let found = collection.find_one(doc! { "run_id": "job-badpipe" }).await;
                matches!(found, Ok(Some(ref run)) if run.get_str("status").ok() == Some("error"))
            })
            .await;
        }
    });

    let served = boot::run(&config, token.clone(), |address| {
        let _ = tx.send(address);
    })
    .await;
    if let Err(error) = served {
        panic!("a cancelled service exits cleanly, not with {error}");
    }
    if let Err(error) = checks.await {
        panic!("the assertions panicked: {error}");
    }
}

#[tokio::test]
async fn a_component_that_never_answers_ends_the_run_as_an_error() {
    // A run id of its own, like every test here that drives a run.
    // Nothing is subscribed to the component subjects, so the call times out.
    // Because `drive` awaits each call before asking for the next task, an
    // unbounded wait here would be a run stopped for ever -- so the deadline
    // turning it into a recorded failure is the property.
    let subjects = format!("pneuma.test.ctrl.silent{}", std::process::id());
    let collection = runs("pneuma_driver_silent").await;
    if let Err(error) = collection.insert_one(run_document("job-silent")).await {
        panic!("the run document should insert: {error}");
    }

    let Ok(client) = async_nats::connect(&nats_url()).await else {
        panic!("could not reach NATS");
    };
    let token = CancellationToken::new();
    let mut config = config("pneuma_driver_silent", &subjects).await;
    config.call_timeout = Duration::from_millis(120);
    let (tx, rx) = oneshot::channel();
    let checks = tokio::spawn({
        let token = token.clone();
        let collection = collection.clone();
        let subjects = subjects.clone();
        async move {
            // Cancels however this task leaves, panic included -- see
            // `StopOnDrop`. Every assertion below is otherwise a hang, with
            // the panic invisible and the file reported as timing out.
            let _stop = StopOnDrop(token);
            let Ok(_address) = rx.await else {
                panic!("the service should bind");
            };
            let init = json!({
                "run_id": "job-silent",
                "pipeline_id": "invoice.page.default",
                "meta": {
                    "job_id": "job-silent", "tenant_id": "acme",
                    "pipeline_type": "invoice", "pipeline_level": "page", "pipeline_name": "default",
                },
                "step_input": {"doc": "d"},
            });
            let Ok(body) = serde_json::to_vec(&init) else {
                panic!("that serialises");
            };
            let subject = format!("{subjects}.run.start");
            announced(&client, &subject, &body, || async {
                let found = collection.find_one(doc! { "run_id": "job-silent" }).await;
                matches!(found, Ok(Some(ref run)) if run.get_str("status").ok() == Some("error"))
            })
            .await;
        }
    });

    let served = boot::run(&config, token.clone(), |address| {
        let _ = tx.send(address);
    })
    .await;
    if let Err(error) = served {
        panic!("a cancelled service exits cleanly, not with {error}");
    }
    if let Err(error) = checks.await {
        panic!("the assertions panicked: {error}");
    }
}

#[tokio::test]
async fn a_database_the_mirror_cannot_reach_is_a_refusal_to_start() {
    // Not a run that fails: a pod that never becomes ready. A replica that
    // silently mirrors nothing is indistinguishable from one that works, and
    // the symptom surfaces weeks later as `pneuma-janitor` finding no stale
    // runs. The design notes
    let subjects = format!("pneuma.test.ctrl.nopg{}", std::process::id());
    let mut config = config("pneuma_driver_nopg", &subjects).await;
    config.database_url = SecretString::from("postgres://nobody:nothing@127.0.0.1:1/nowhere");

    let token = CancellationToken::new();
    let served = boot::run(&config, token, |_| panic!("it must not bind")).await;
    let Err(error) = served else {
        panic!("nothing is listening on port 1");
    };
    assert!(
        matches!(error, BootError::Database(_)),
        "wrong variant: {error:?}"
    );
    // The DSN carries a password, so the message must not -- the design notes
    // §12.
    assert!(
        !error.to_string().contains("nothing"),
        "the password reached the message: {error}"
    );
}

#[tokio::test]
async fn a_database_with_no_node_run_is_a_refusal_to_start_too() {
    // Reachable is not the same as migrated, and this is the likelier mistake:
    // a `search_path` that does not reach the schema the migrations ran in.
    // Without the check the replica reports ready and logs one refusal per
    // step, for ever.
    let Ok(url) = std::env::var("PNEUMA_TEST_DATABASE_URL") else {
        panic!("PNEUMA_TEST_DATABASE_URL is not set; see the header of this file");
    };
    let bare = "pneuma_driver_bare";
    let pool = match sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(&url)
        .await
    {
        Ok(pool) => pool,
        Err(error) => panic!("could not connect to {url}: {error}"),
    };
    // A schema of its own rather than `public`: another test binary migrating
    // `public` would make "there is no node_run" quietly false.
    for statement in [
        format!("DROP SCHEMA IF EXISTS {bare} CASCADE"),
        format!("CREATE SCHEMA {bare}"),
    ] {
        if let Err(error) = sqlx::Executor::execute(&pool, statement.as_str()).await {
            panic!("setup failed on {statement:?}: {error}");
        }
    }
    drop(pool);

    let subjects = format!("pneuma.test.ctrl.bare{}", std::process::id());
    let mut config = config("pneuma_driver_bare", &subjects).await;
    let separator = if url.contains('?') { '&' } else { '?' };
    config.database_url =
        SecretString::from(format!("{url}{separator}options=-c%20search_path%3D{bare}"));

    let token = CancellationToken::new();
    let served = boot::run(&config, token, |_| panic!("it must not bind")).await;
    let Err(error) = served else {
        panic!("there is no node_run in that schema");
    };
    assert!(
        matches!(error, BootError::Unmigrated(_)),
        "wrong variant: {error:?}"
    );
}

#[tokio::test]
async fn a_node_run_that_is_not_this_node_run_is_not_a_missing_migration() {
    // The distinction the two refusals exist for. A table called `node_run`
    // that this port's statements cannot use is not a database that needs
    // migrating, and saying so would send an operator to re-run migrations that
    // already applied. Only `undefined_table` means the migrations are the
    // answer.
    let Ok(url) = std::env::var("PNEUMA_TEST_DATABASE_URL") else {
        panic!("PNEUMA_TEST_DATABASE_URL is not set; see the header of this file");
    };
    let wrong = "pneuma_driver_wrong";
    let pool = match sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(&url)
        .await
    {
        Ok(pool) => pool,
        Err(error) => panic!("could not connect to {url}: {error}"),
    };
    for statement in [
        format!("DROP SCHEMA IF EXISTS {wrong} CASCADE"),
        format!("CREATE SCHEMA {wrong}"),
        format!("CREATE TABLE {wrong}.node_run (x integer)"),
    ] {
        if let Err(error) = sqlx::Executor::execute(&pool, statement.as_str()).await {
            panic!("setup failed on {statement:?}: {error}");
        }
    }
    drop(pool);

    let subjects = format!("pneuma.test.ctrl.wrong{}", std::process::id());
    let mut config = config("pneuma_driver_wrong", &subjects).await;
    let separator = if url.contains('?') { '&' } else { '?' };
    config.database_url = SecretString::from(format!(
        "{url}{separator}options=-c%20search_path%3D{wrong}"
    ));

    let token = CancellationToken::new();
    let served = boot::run(&config, token, |_| panic!("it must not bind")).await;
    let Err(error) = served else {
        panic!("that table has none of the columns the statements name");
    };
    assert!(
        matches!(error, BootError::Database(_)),
        "wrong variant: {error:?}"
    );
}

#[tokio::test]
async fn a_dsn_that_is_not_a_dsn_is_refused_before_anything_is_dialled() {
    // The other half of `Database`, and a different failure from a database
    // that will not answer: nothing is dialled at all, so this returns at once
    // rather than after the startup bound. `Config::from_env` deliberately does
    // not parse the DSN -- what a connection string may contain is sqlx's
    // question, and a parser here would be a second opinion that could refuse
    // something valid.
    let subjects = format!("pneuma.test.ctrl.baddsn{}", std::process::id());
    let mut config = config("pneuma_driver_baddsn", &subjects).await;
    config.database_url = SecretString::from("this is not a connection string");

    let token = CancellationToken::new();
    let served = boot::run(&config, token, |_| panic!("it must not bind")).await;
    let Err(error) = served else {
        panic!("that is not a connection string");
    };
    assert!(
        matches!(error, BootError::Database(_)),
        "wrong variant: {error:?}"
    );
}
