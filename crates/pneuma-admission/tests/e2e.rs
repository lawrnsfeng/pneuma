//! The whole path, with nothing stubbed but the model.
//!
//! Every other file in this workspace joins two things and stubs the third.
//! `boot.rs` posts to the real door and a stub Restate; `pneuma-restate`'s
//! `handler.rs` invokes a real Restate and never goes through the door. The
//! seam between them — that what admission submits is what the runner accepts,
//! and that a real Restate carries it across — is only asserted by shape until
//! here.
//!
//! That seam is worth a real test twice over: the request body is a *wire*
//! contract between two crates that do not depend on each other, and
//! the design notes have just renamed every key in it.
//!
//! What is stubbed is the component, because a model is not ours and
//! `docs/as-built/component-api.md` is the contract in both directions.
//!
//! ```sh
//! docker run -d --name pn-pg -p 5433:5432 -e POSTGRES_PASSWORD=pneuma postgres:16
//! docker run -d --name pn-restate --add-host=host.docker.internal:host-gateway \
//!     -p 18080:8080 -p 19070:9070 restatedev/restate:1.7.8
//! PNEUMA_TEST_DATABASE_URL=postgres://postgres:pneuma@127.0.0.1:5433/postgres \
//!     cargo test -p pneuma-admission --test e2e
//! ```

use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use axum::routing::post;
use axum::Router;
use chrono::TimeDelta;
use pneuma_admission::{boot, Config};
use secrecy::SecretString;
use serde_json::{json, Value};
use sqlx::{Executor, PgPool};
use tokio_util::sync::CancellationToken;

/// The Restate container, as mapped onto the host.
const INGRESS: &str = "http://127.0.0.1:18080";
const ADMIN: &str = "http://127.0.0.1:19070";

/// This binary's own ports, deliberately not `pneuma-restate`'s 9080/9081.
///
/// `cargo test` runs test targets one at a time, so sharing them would work
/// today and stop working under a runner that parallelises targets — and the
/// failure would be this suite registering *that* binary's endpoint as the
/// deployment and reporting on runs it did not drive.
const SDK_PORT: u16 = 9180;
const COMPONENT_PORT: u16 = 9181;

/// How many times the stub component was called.
static CALLS: AtomicUsize = AtomicUsize::new(0);

/// The `step_input` and `job_id` of the most recent call.
///
/// The job id is what makes the call attributable to *this* run. Without it any
/// invocation Restate delivers to the endpoint counts -- including a retry of a
/// leftover invocation from an earlier aborted run -- and since the submission
/// carries a constant `input`, the step input alone would match that stale call
/// too. So a genuine wire-contract break could still pass.
static LAST_CALL: std::sync::Mutex<Option<(Value, Value)>> = std::sync::Mutex::new(None);

/// A component that echoes its input, in the shape the contract requires.
async fn predict(axum::Json(body): axum::Json<Value>) -> axum::Json<Value> {
    // Read through the published contract, not around it: a body that is the
    // bare step input -- which is what the executor sent until a review caught
    // it -- yields `null` here and fails the assertion at the end rather than
    // passing silently.
    let step_input = body.pointer("/step_input").cloned().unwrap_or(Value::Null);
    let tenant = body
        .pointer("/meta/tenant_id")
        .cloned()
        .unwrap_or(Value::Null);
    let job = body.pointer("/meta/job_id").cloned().unwrap_or(Value::Null);
    let call = Some((step_input.clone(), job));
    match LAST_CALL.lock() {
        Ok(mut seen) => *seen = call,
        Err(poisoned) => *poisoned.into_inner() = call,
    }
    // Incremented last, so a test waiting on it cannot see the count move
    // before the body it is about to read has been recorded.
    CALLS.fetch_add(1, Ordering::SeqCst);
    axum::Json(json!({
        "step_output": { "echo": step_input, "for": tenant }
    }))
}

/// The SDK endpoint and the component, started once.
///
/// `dsn` is where the runner mirrors what it ran into `node_run`. It is this
/// test's own migrated schema rather than a lazy pool pointed nowhere, because
/// the row at the far end is half of what this file exists to prove: a
/// submission that reaches a component and leaves no record behind is the state
/// the port found the table in.
async fn serving(dsn: &str) {
    // Held and released, not held: the SDK binds inside `listen_and_serve`, so
    // this narrows the window rather than closing it -- it catches the case
    // that actually happens, a `pneuma-restate` left running, and a readiness
    // probe cannot tell that apart because a foreign endpoint answers
    // `/discover` just as happily.
    match std::net::TcpListener::bind(("0.0.0.0", SDK_PORT)) {
        Ok(probe) => drop(probe),
        Err(error) => panic!(
            "cannot take {SDK_PORT} for the SDK endpoint: {error}. Something else \
             is holding it, and this suite would register *that* as the deployment."
        ),
    }

    let app = Router::new().route("/predict", post(predict));
    let Ok(listener) = tokio::net::TcpListener::bind(("127.0.0.1", COMPONENT_PORT)).await else {
        panic!("cannot take {COMPONENT_PORT} for the stub component");
    };
    tokio::spawn(async move {
        if let Err(error) = axum::serve(listener, app).await {
            panic!("the stub component stopped: {error}");
        }
    });

    let dsn = dsn.to_owned();
    tokio::spawn(async move {
        // `127.0.0.1` for the component, because *this* process dials it. Only
        // the deployment URI is `host.docker.internal`, because that is the
        // container reaching back. Getting it the other way round fails as
        // "error sending request", which Restate retries with a growing
        // backoff -- so the symptom is a hang rather than a failure.
        let template = format!("http://127.0.0.1:{COMPONENT_PORT}/{{component}}");
        let Ok(endpoint) = pneuma_restate::Endpoint::new(&template) else {
            panic!("{template} is an endpoint template");
        };
        let Ok(pool) = sqlx::postgres::PgPoolOptions::new()
            .max_connections(2)
            .connect_lazy(&dsn)
        else {
            panic!("a lazy pool does not connect yet");
        };
        let mirror = pneuma_mirror::Mirror::new(pool, "pneuma-admission-e2e");
        let Ok(runner) = pneuma_restate::Runner::new(endpoint, mirror) else {
            panic!("a runner must build");
        };
        pneuma_restate::serve(runner, ([0, 0, 0, 0], SDK_PORT).into()).await;
    });

    // The endpoint speaks h2c, so an HTTP/1.1 probe never gets an answer and
    // the loop would simply run every iteration -- readiness that checks
    // nothing.
    let Ok(probe) = reqwest::Client::builder().http2_prior_knowledge().build() else {
        panic!("an h2-prior-knowledge client must build");
    };
    let mut ready = false;
    for _ in 0..100 {
        if probe
            .get(format!("http://127.0.0.1:{SDK_PORT}/discover"))
            .header("accept", "application/vnd.restate.endpointmanifest.v3+json")
            .send()
            .await
            .is_ok()
        {
            ready = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(
        ready,
        "the SDK endpoint on {SDK_PORT} never answered discovery"
    );

    register().await;
}

/// Points the Restate server at this process's SDK endpoint.
async fn register() {
    let client = reqwest::Client::new();
    let body = json!({
        "uri": format!("http://host.docker.internal:{SDK_PORT}"),
        "force": true,
    });
    let mut last = String::new();
    for _ in 0..50 {
        // A non-2xx is retried as well as a transport error: a server that is
        // up but not ready answers with a *status*, and so does a registration
        // that fails because the container cannot yet dial back.
        match client
            .post(format!("{ADMIN}/deployments"))
            .json(&body)
            .send()
            .await
        {
            Ok(response) if response.status().is_success() => return,
            Ok(response) => {
                let status = response.status();
                let text = response.text().await.unwrap_or_default();
                last = format!("{status}: {text}");
            }
            Err(error) => last = error.to_string(),
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    panic!(
        "could not register with the Restate admin API at {ADMIN} after 10s: {last}\n\
         See the header of this file for how to start it."
    );
}

/// Cancels the token however the task leaves — including by panicking.
///
/// Without it every assertion in the spawned task is a *hang* rather than a
/// failure: the panic skips `token.cancel()`, `boot::run` never returns, and CI
/// blocks on the job timeout with no message. The same trap `pneuma-janitor`'s
/// boot test hit; carried here rather than left to be hit a third time.
struct StopOnDrop(CancellationToken);

impl Drop for StopOnDrop {
    fn drop(&mut self) {
        self.0.cancel();
    }
}

/// A schema of this test's own, and a DSN that points at it.
async fn fresh() -> (String, PgPool) {
    let Ok(url) = std::env::var("PNEUMA_TEST_DATABASE_URL") else {
        panic!("PNEUMA_TEST_DATABASE_URL is not set; see the header of this file");
    };
    // Per process, not a fixed name: two runs against one
    // `PNEUMA_TEST_DATABASE_URL` would otherwise drop the schema out from under
    // each other, which is the shape of a failure nobody reproduces.
    let schema = format!("pneuma_e2e_{}", std::process::id());
    let Ok(bare) = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(&url)
        .await
    else {
        panic!("could not connect to {url}");
    };
    for statement in [
        format!("DROP SCHEMA IF EXISTS {schema} CASCADE"),
        format!("CREATE SCHEMA {schema}"),
    ] {
        if let Err(error) = bare.execute(statement.as_str()).await {
            panic!("setup failed on {statement:?}: {error}");
        }
    }
    drop(bare);

    let separator = if url.contains('?') { '&' } else { '?' };
    let dsn = format!("{url}{separator}options=-c%20search_path%3D{schema}");

    // The search path travels in the DSN, not in a `SET` on one connection.
    // That is what `boot::run` is handed and it is also what makes the read
    // pool below correct whatever sqlx does with its connections: a pool that
    // reopens the one it was `SET` on reads `public.submission` instead, which
    // other tests in this crate populate -- so the poll finds nothing and the
    // failure blames the dispatcher.
    let Ok(pool) = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(&dsn)
        .await
    else {
        panic!("could not connect to {dsn}");
    };
    // The migrator, not one hand-picked file: `submission` is what this needs,
    // and a list that names it is a list that drifts the next time a migration
    // lands.
    let Ok(mut connection) = pool.acquire().await else {
        panic!("could not acquire a connection to migrate on");
    };
    if let Err(error) = pneuma_store::migrator().run(&mut *connection).await {
        panic!("the migrations do not apply: {error}");
    }
    drop(connection);

    (dsn, pool)
}

/// A run id no earlier run of this test has used.
///
/// Not a constant, and the reason is the design notes rather than tidiness.
/// Restate keys on the idempotency key alone and does **not** compare request
/// bodies, with a retention of a day by default -- so a fixed id makes the
/// second run of this test return the *first* run's report, with `200`, no
/// component call, and a `submission` row that settles `done`. Measured: the
/// test passed once and then failed for thirty seconds on every later run,
/// which is the same trap the deviation says binds on production. A run id is
/// minted once and never reused, here as there.
fn a_fresh_run_id() -> String {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|since| since.as_nanos())
        .unwrap_or_default();
    format!("e2e-{}-{now}", std::process::id())
}

/// The submission body, exactly as a producer would post it.
fn submission(run: &str) -> Value {
    json!({
        "pipeline": {
            "pipeline_id": "invoice.page.default",
            "start": "A",
            "components": [
                {"node_id": "A", "name": "predict", "type": "Model", "children": ["end"]}
            ],
        },
        "meta": {
            "job_id": run,
            "tenant_id": "acme",
            "pipeline_type": "invoice",
            "pipeline_level": "page",
            "pipeline_name": "default",
        },
        "input": {"doc": "invoice-0001.pdf", "page": 1},
    })
}

#[tokio::test]
async fn a_run_posted_to_the_door_is_executed_by_a_real_restate() {
    // The schema first, because the SDK endpoint mirrors into it.
    let (dsn, pool) = fresh().await;
    serving(&dsn).await;
    let before = CALLS.load(Ordering::SeqCst);
    let run_id = a_fresh_run_id();

    let Some(listen) = "127.0.0.1:0".parse::<SocketAddr>().ok() else {
        panic!("a real address");
    };
    let config = Config {
        database_url: SecretString::from(dsn),
        listen,
        ingress: INGRESS.to_owned(),
        handler: "PneumaRunner/run".to_owned(),
        batch_size: 10,
        per_tenant: 10,
        // Short, because the test waits for a round rather than driving one.
        interval: Duration::from_millis(200),
        reclaim_after: TimeDelta::seconds(300),
        weights: std::collections::BTreeMap::new(),
    };

    let token = CancellationToken::new();
    let (tx, rx) = tokio::sync::oneshot::channel();
    let checks = tokio::spawn({
        let token = token.clone();
        let pool = pool.clone();
        let run_id = run_id.clone();
        async move {
            // Cancels however this task leaves, panic included -- see
            // `StopOnDrop`. Every assertion below is otherwise a hang.
            let _stop = StopOnDrop(token);
            let Ok(address) = rx.await else {
                panic!("the service should bind and say where");
            };
            let http = reqwest::Client::new();
            let posted = http
                .post(format!("http://{address}/pneuma-admission/runs"))
                .json(&submission(&run_id))
                .send()
                .await;
            let Ok(response) = posted else {
                panic!("the door should answer");
            };
            let status = response.status().as_u16();
            let body: Value = response.json().await.unwrap_or(Value::Null);
            assert_eq!(status, 202, "{body}");
            assert_eq!(body["run_id"], json!(run_id));

            // The dispatcher claims it, submits it to Restate, and settles it.
            // Polled rather than slept on: the round is 200ms and the run is
            // one component call, but a loaded machine is a loaded machine.
            let mut settled = None;
            for _ in 0..300 {
                // The query error is distinguished from "no such row". Folding
                // them together polls silently for thirty seconds against a
                // missing table or a dead connection and then reports "the
                // submission was never settled" -- the one explanation that is
                // certainly wrong.
                let row: Option<(String, Option<String>)> = match sqlx::query_as(
                    "SELECT state::text, detail FROM submission WHERE run_id = $1",
                )
                .bind(&run_id)
                .fetch_optional(&pool)
                .await
                {
                    Ok(row) => row,
                    Err(error) => panic!("could not read the submission: {error}"),
                };
                if let Some((state, detail)) = row {
                    if state != "queued" && state != "claimed" {
                        settled = Some((state, detail));
                        break;
                    }
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            let Some((state, detail)) = settled else {
                panic!("the submission was never settled");
            };
            assert_eq!(state, "done", "settled {state}: {detail:?}");
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

    // Waited for, not asserted directly. `done` on the row means Restate
    // *accepted* the submission -- `/send` is fire-and-forget, which is the
    // whole reason the dispatcher can settle a row without waiting for a run --
    // so the component call happens after. Asserting straight away passes
    // whenever the machine is fast and fails whenever it is not, which is the
    // shape of a test that will be quarantined rather than read.
    // Attributed to *this* run, not merely counted. Restate retains
    // invocations across runs, so a retry of a leftover one would move the
    // counter -- and the submission carries a constant input, so the step input
    // alone would match that stale call too.
    let mut seen = None;
    for _ in 0..300 {
        if CALLS.load(Ordering::SeqCst) > before {
            seen = match LAST_CALL.lock() {
                Ok(call) => call.clone(),
                Err(poisoned) => poisoned.into_inner().clone(),
            };
            if seen.is_some() {
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    // Reached through the door, the queue, the ingress, the server and the
    // handler -- and the component read `step_input` and `meta` out of the body
    // it was sent, which is only true if what admission submits is what the
    // runner accepts. A bare step input, which is what the executor sent until
    // a review caught it, would have answered `{"echo": null}` and still moved
    // the counter, so the body itself is what is checked.
    let Some((step_input, job_id)) = seen else {
        panic!("the component was never called");
    };
    assert_eq!(job_id, json!(run_id), "and it was called for *this* run");
    assert_eq!(
        step_input,
        json!({"doc": "invoice-0001.pdf", "page": 1}),
        "the component saw the step input the submission carried"
    );

    // And the run left a record. Polled for the same reason the component call
    // is: the mirror's last write happens after the component answers, and the
    // handler is still finishing when the assertion above passes.
    let mut mirrored = Vec::new();
    for _ in 0..300 {
        let rows: Vec<(String, String)> = match sqlx::query_as(
            "SELECT path, status::text FROM node_run WHERE run_id = $1 ORDER BY path",
        )
        .bind(&run_id)
        .fetch_all(&pool)
        .await
        {
            Ok(rows) => rows,
            Err(error) => panic!("could not read the mirror: {error}"),
        };
        if rows.iter().any(|(_, status)| status == "FINISHED") {
            mirrored = rows;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(
        mirrored,
        vec![(
            format!("{run_id}.invoice.page.default.A"),
            "FINISHED".to_owned()
        )],
        "the step the run executed is in `node_run`, pathed under this run"
    );
}
