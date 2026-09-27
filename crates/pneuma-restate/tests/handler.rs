//! Drives the handler through a real Restate server.
//!
//! This is the only way the adopted branch can be checked. `HttpComponent`
//! journals its call through `ctx.run`, and the whole claim of
//! `spikes/restate/VERDICT.md` — that the barrier collapses because one
//! replayed handler owns the run — is a property of the server, not of this
//! code. A mock context would assert that the mock behaves as written.
//!
//! The component here is a plain axum app that has never heard of Restate,
//! which is §8's requirement rather than a testing convenience.
//!
//! A real Postgres too, because the handler now mirrors what a run did into
//! `node_run` and a mirror pointed at nothing looks exactly like one that
//! works. Its own schema, dropped and migrated per run of this binary.
//!
//! ```sh
//! docker run -d --name pn-restate --add-host=host.docker.internal:host-gateway \
//!   -p 18080:8080 -p 19070:9070 restatedev/restate:latest
//! docker run -d --name pn-pg -p 5433:5432 -e POSTGRES_PASSWORD=pneuma postgres:16
//! PNEUMA_TEST_DATABASE_URL=postgres://postgres:pneuma@127.0.0.1:5433/postgres \
//!     cargo test -p pneuma-restate --test handler
//! ```

use std::time::Duration;

use axum::{routing::post, Json as AxumJson, Router};
use pneuma_core::node::NodeKind;
use pneuma_core::status::NodeStatus;
use pneuma_restate::{Endpoint, Runner};
use restate_sdk::prelude::{Endpoint as RestateEndpoint, HttpServer};
use serde_json::{json, Value};

/// The SDK endpoint's port, which the Restate container dials back on.
const SDK_PORT: u16 = 9080;
/// The stub component's port.
const COMPONENT_PORT: u16 = 9081;
/// The container's admin API and ingress, as mapped onto the host.
const ADMIN: &str = "http://localhost:19070";
const INGRESS: &str = "http://localhost:18080";

/// How many times the flaky route has been called, so it can recover.
static FLAKY_CALLS: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
/// How many times the permanently-refusing route has been called.
static REFUSE_CALLS: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

/// Always refuses, permanently.
async fn refuses() -> (axum::http::StatusCode, &'static str) {
    REFUSE_CALLS.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    (axum::http::StatusCode::BAD_REQUEST, "no")
}

/// How many times the not-JSON route has been called.
static NOT_JSON_CALLS: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

/// Answers 200 with something that is not JSON.
async fn not_json() -> (axum::http::StatusCode, &'static str) {
    NOT_JSON_CALLS.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    (axum::http::StatusCode::OK, "<html>not json</html>")
}

/// Fails once with a 503, then succeeds — a component restarting.
async fn flaky(AxumJson(body): AxumJson<Value>) -> axum::response::Response {
    use axum::response::IntoResponse;
    if FLAKY_CALLS.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0 {
        return (axum::http::StatusCode::SERVICE_UNAVAILABLE, "restarting").into_response();
    }
    predict(AxumJson(body)).await.into_response()
}

/// How many times the truncating route has been called.
static TRUNCATED_CALLS: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

/// Answers 200, sends part of a body, then fails.
///
/// A component whose pod is killed mid-response. The status says success, so
/// the failure is in *reading*, which reqwest reports with the same error kind
/// as malformed JSON -- the distinction `call_once` now draws.
async fn truncated() -> axum::response::Response {
    TRUNCATED_CALLS.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    let chunks: Vec<Result<axum::body::Bytes, std::io::Error>> = vec![
        Ok(axum::body::Bytes::from_static(b"{\"step_output\":")),
        Err(std::io::Error::other("connection reset")),
    ];
    axum::response::Response::builder()
        .status(axum::http::StatusCode::OK)
        .header("content-type", "application/json")
        .body(axum::body::Body::from_stream(futures_util::stream::iter(
            chunks,
        )))
        .unwrap_or_default()
}

/// How many times the stalling route has been entered.
static STALL_CALLS: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

/// Accepts the request and then never answers within the test's patience.
///
/// The failure a component client without a request timeout cannot survive:
/// the connection is established, so a connect timeout does not fire, and
/// nothing ever arrives. Before the client had a timeout at all this hung the
/// call for ever, and `RunRetryPolicy::max_duration` could not help -- it
/// bounds the interval *across* attempts, and an attempt that never returns
/// never yields to one.
async fn stalls() -> axum::response::Response {
    STALL_CALLS.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    tokio::time::sleep(Duration::from_secs(3600)).await;
    axum::response::Response::default()
}

/// Refuses permanently, from inside a fan-out.
///
/// A route of its own rather than `refuses`: that one's call count is the
/// assertion of the test it belongs to, and these run concurrently against one
/// server.
async fn refuses_branch() -> (axum::http::StatusCode, &'static str) {
    (axum::http::StatusCode::BAD_REQUEST, "no")
}

/// Answers with a list, so a `ListAggregator` downstream has something to fan
/// out over.
async fn list() -> AxumJson<Value> {
    AxumJson(json!({"step_output": [{"i": 1}, {"i": 2}, {"i": 3}]}))
}

/// Echoes its input back in the shape a component must answer in.
async fn predict(AxumJson(body): AxumJson<Value>) -> AxumJson<Value> {
    let step_input = body.pointer("/step_input").cloned().unwrap_or(Value::Null);
    // Enough for the interpreter: echo the object so conditionals can read it,
    // and report what the component was told so the test can check the wire.
    AxumJson(json!({
        "step_output": {
            "echo": step_input,
            "node_env_vars": body.pointer("/node_env_vars").cloned(),
            "custom_data": body.pointer("/custom_data").cloned(),
        }
    }))
}

/// The schema this file's rows live in.
///
/// Its own, not `public`: the mirror is written by a server shared across every
/// test in this binary, and a run's paths are derived from its `job_id`, so
/// rows from a previous `cargo test` would otherwise still be there — and
/// `ON CONFLICT (path) DO NOTHING` would make the second run of the suite
/// assert on the first run's rows.
const SCHEMA: &str = "pneuma_restate_handler";

/// Where the mirror writes, with [`SCHEMA`] on the search path.
///
/// Required rather than optional. A mirror pointed at nothing looks exactly
/// like a mirror that works — which is the defect this whole path exists to
/// close — so a suite that cannot check the rows says so instead of passing.
fn dsn() -> String {
    let Ok(url) = std::env::var("PNEUMA_TEST_DATABASE_URL") else {
        panic!("PNEUMA_TEST_DATABASE_URL is not set; see the header of this file");
    };
    // In the DSN, not a `SET` on one connection: a pool that reopens the
    // connection it was `SET` on reads `public` instead, and the failure then
    // blames the code under test.
    let separator = if url.contains('?') { '&' } else { '?' };
    format!("{url}{separator}options=-c%20search_path%3D{SCHEMA}")
}

/// A lazy pool onto [`dsn`].
fn mirror_pool() -> sqlx::PgPool {
    match sqlx::postgres::PgPoolOptions::new()
        .max_connections(2)
        .connect_lazy(&dsn())
    {
        Ok(pool) => pool,
        Err(error) => panic!("a lazy pool does not connect yet: {error}"),
    }
}

/// Drops and migrates [`SCHEMA`], once per test binary.
async fn migrated() {
    let url = match std::env::var("PNEUMA_TEST_DATABASE_URL") {
        Ok(url) => url,
        Err(error) => panic!("PNEUMA_TEST_DATABASE_URL: {error}"),
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

    let pool = mirror_pool();
    let mut connection = match pool.acquire().await {
        Ok(connection) => connection,
        Err(error) => panic!("could not acquire a connection to migrate on: {error}"),
    };
    if let Err(error) = pneuma_store::migrator().run(&mut *connection).await {
        panic!("the migrations do not apply: {error}");
    }
}

/// Every row a run left behind, by path.
async fn rows(run_id: &str) -> Vec<pneuma_store::NodeRun> {
    let store = pneuma_store::NodeRunStore::new(mirror_pool());
    match store.get_by_run_id(run_id).await {
        Ok(mut rows) => {
            rows.sort_by(|a, b| a.path.cmp(&b.path));
            rows
        }
        Err(error) => panic!("could not read the mirror: {error}"),
    }
}

/// A `job_id` no other test uses.
///
/// A run's paths are derived from it, and `node_run.path` is unique, so two
/// tests sharing one would have the second silently assert on the first's rows.
fn a_job(name: &str) -> String {
    format!("job-{name}")
}

/// Brings up the component and the SDK endpoint, once, on a runtime of their
/// own.
///
/// The runtime is the point. Each `#[tokio::test]` builds its own, and a task
/// spawned inside one dies when that test's runtime drops -- so servers started
/// by the first test are gone by the second, while a `OnceCell` still reports
/// them started. Every test then passes alone and the suite hangs together,
/// which is what happened here: three passes at 0.57s each, and no completion
/// when run as a file.
///
/// A runtime in a `static` is never dropped, so the servers outlive whichever
/// test happened to start them.
fn servers() {
    static SERVERS: std::sync::OnceLock<tokio::runtime::Runtime> = std::sync::OnceLock::new();
    SERVERS.get_or_init(|| {
        let runtime = match tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
        {
            Ok(runtime) => runtime,
            Err(error) => panic!("could not build the server runtime: {error}"),
        };
        // Bind *before* spawning, on this thread, so "address already in use"
        // is a panic here rather than inside a task whose `JoinHandle` is
        // dropped. It was spawned, and the panic was invisible: another
        // process holding 9081/9080 left the suite talking to *that* one, and
        // every test reported a plausible result for a run it did not drive.
        // `std`, not `tokio`: `servers()` is reached from inside a test's own
        // runtime, and `block_on` on a second runtime from there panics with
        // "cannot start a runtime from within a runtime".
        let component_listener = match std::net::TcpListener::bind(("0.0.0.0", COMPONENT_PORT)) {
            Ok(listener) => listener,
            Err(error) => panic!(
                "could not bind the stub component on {COMPONENT_PORT}: {error}. \
                 Something else is holding the port; the suite would otherwise \
                 run against it."
            ),
        };
        if let Err(error) = component_listener.set_nonblocking(true) {
            panic!("the stub component's listener must be non-blocking: {error}");
        }
        runtime.spawn(async move {
            let app = Router::new()
                .route("/predict", post(predict))
                .route("/list", post(list))
                .route("/refuses-branch", post(refuses_branch))
                .route("/refuses", post(refuses))
                .route("/not-json", post(not_json))
                .route("/flaky", post(flaky))
                .route("/truncated", post(truncated))
                .route("/stalls", post(stalls));
            let component_listener = match tokio::net::TcpListener::from_std(component_listener) {
                Ok(listener) => listener,
                Err(error) => panic!("the stub component's listener must convert: {error}"),
            };
            if let Err(error) = axum::serve(component_listener, app).await {
                panic!("the stub component stopped: {error}");
            }
        });
        // The SDK binds inside `listen_and_serve`, so unlike the component's
        // listener above this one cannot be *held* -- it is taken and released,
        // and the endpoint binds it again some time later on another runtime.
        //
        // So this narrows the window rather than closing it: it catches the
        // case that actually happened, a `pneuma-restate` already running on
        // 9080, and it does not catch one that appears between the release
        // here and `listen_and_serve` there. The readiness probe cannot cover
        // the difference either -- a foreign endpoint answers `/discover` just
        // as happily. Closing it properly needs the SDK to accept a listener,
        // which 0.11.1's `HttpServer` does not expose.
        match std::net::TcpListener::bind(("0.0.0.0", SDK_PORT)) {
            Ok(probe) => drop(probe),
            Err(error) => panic!(
                "cannot take {SDK_PORT} for the SDK endpoint: {error}. Something \
                 else is holding it -- most likely a `pneuma-restate` left \
                 running -- and the suite would register *that* as the \
                 deployment and report on runs it did not drive."
            ),
        }
        runtime.spawn(async {
            // `127.0.0.1`, not `host.docker.internal`. The component is dialled
            // by *this* process -- the SDK endpoint runs on the host -- so the
            // container's view of the host is the wrong one. Only the
            // deployment URI is `host.docker.internal`, because that is the
            // container reaching back. Getting it backwards fails as "error
            // sending request", which Restate retries with a growing backoff,
            // so the symptom is a hang rather than a failure.
            // The component name in *path* position, so each test can point a
            // pipeline at a different stub route by naming its component.
            let endpoint =
                match Endpoint::new(format!("http://127.0.0.1:{COMPONENT_PORT}/{{component}}")) {
                    Ok(endpoint) => endpoint,
                    Err(error) => panic!("the template must be valid: {error}"),
                };
            // A fast retry policy. The default is bounded by ten minutes, which
            // is right for production and unwatchable in a test; this still
            // exercises the same path -- a transient answer is retried, a
            // permanent one is not.
            // `default()`, not `new()`: `new()` is factor 1.0 with no ceiling,
            // i.e. a constant delay, which is the same defect the production
            // policy had. Only the bound differs from production here.
            let retry = restate_sdk::context::RunRetryPolicy::default()
                .max_duration(Duration::from_secs(5));
            // Two seconds, not the production 300: the stalling route sleeps an
            // hour, so this is what makes "the client gives up" observable in a
            // test rather than a wait.
            let timeout = Duration::from_secs(2);
            // The mirror writes into this file's own schema, migrated by
            // `serving()` before any run is submitted. Lazy, so the pool is
            // built here and connects later -- the schema does not exist yet
            // at this point.
            let mirror = pneuma_mirror::Mirror::new(mirror_pool(), "pneuma-restate-test");
            HttpServer::new(
                RestateEndpoint::builder()
                    .bind(match Runner::with_retry(endpoint, retry, timeout, mirror) {
                        Ok(runner) => runner,
                        Err(error) => panic!("the default client must build: {error}"),
                    })
                    .build(),
            )
            .listen_and_serve(([0, 0, 0, 0], SDK_PORT).into())
            .await;
        });
        runtime
    });
}

/// Starts the servers and registers them, once per test binary.
async fn serving() {
    servers();
    static REGISTERED: tokio::sync::OnceCell<()> = tokio::sync::OnceCell::const_new();
    REGISTERED
        .get_or_init(|| async {
            // The servers are on another runtime and bind asynchronously, so
            // the registration has to wait for the SDK endpoint to answer
            // discovery rather than assume it.
            // An h2-prior-knowledge client: the SDK endpoint speaks h2c, so the
            // default HTTP/1.1 client used here never got a response and this
            // loop simply ran all fifty iterations every time -- five seconds
            // of "readiness" that checked nothing. Registration still worked
            // because it targets the Restate admin API, which is HTTP/1.1.
            let Ok(probe) = reqwest::Client::builder().http2_prior_knowledge().build() else {
                panic!("an h2-prior-knowledge client must build");
            };
            let mut ready = false;
            for _ in 0..50 {
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
            // Refused, not shrugged at. Falling through to `register` after
            // fifty failures is a readiness check that reports ready when it
            // is not, and the failure it hides -- registering a deployment
            // that cannot be discovered -- surfaces as every test failing for
            // a reason none of them is about.
            assert!(
                ready,
                "the SDK endpoint on {SDK_PORT} never answered discovery"
            );
            // Before the first run rather than beside the pool: the pool is
            // built lazily inside `servers()`, at a point where this schema
            // does not exist yet.
            migrated().await;
            register().await;
        })
        .await;
}

/// Points the server at this process's SDK endpoint.
async fn register() {
    let client = reqwest::Client::new();
    let body = json!({
        "uri": format!("http://host.docker.internal:{SDK_PORT}"),
        "force": true,
    });
    // Bounded retry rather than one attempt. The server may still be opening
    // its admin port when a cached build reaches this, and failing on that is
    // failing on timing rather than on anything real. Bounded, so a server that
    // genuinely is not there still fails instead of hanging.
    let mut last = String::new();
    let mut attempt = None;
    for _ in 0..50 {
        match client
            .post(format!("{ADMIN}/deployments"))
            .json(&body)
            .send()
            .await
        {
            // A non-2xx is retried as well. A server that is up but not ready
            // answers with a *status*, not a transport error -- and so does a
            // registration that fails because the container cannot yet dial
            // back, which is the case the file's header flags as unverified on
            // a runner. Breaking out on any `Ok` left the loop covering only
            // "connection refused", which is the one case that was already
            // unlikely.
            Ok(response) if response.status().is_success() => {
                attempt = Some(response);
                break;
            }
            Ok(response) => {
                let status = response.status();
                let text = response.text().await.unwrap_or_default();
                last = format!("{status}: {text}");
            }
            Err(error) => last = error.to_string(),
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    let Some(response) = attempt else {
        panic!(
            "could not reach the Restate admin API at {ADMIN} after 10s: {last}\n\
             See the header of this file for how to start it."
        );
    };
    // Only a success leaves the loop, so nothing to re-check here.
    let _ = response;
}

fn pipeline() -> Value {
    one_step_each("predict", "predict")
}

/// A two-step pipeline naming one stub route per step.
fn one_step_each(first: &str, second: &str) -> Value {
    let yaml = format!(
        "pipeline_id: p\nstart: A\ncomponents:\n\
         \x20 - node_id: A\n    name: {first}\n    type: Model\n    children: [B]\n\
         \x20   params:\n      key_1: value_1\n\
         \x20 - node_id: B\n    name: {second}\n    type: Model\n    children: [end]\n"
    );
    let yaml = yaml.as_str();
    match serde_yaml::from_str::<serde_yaml::Value>(yaml) {
        Ok(value) => match serde_json::to_value(value) {
            Ok(json) => json,
            Err(error) => panic!("the fixture must convert: {error}"),
        },
        Err(error) => panic!("the fixture must parse: {error}"),
    }
}

/// A step, a list aggregator over one step, and a step after it.
///
/// `A` answers a three-element list, so `X` fans out three branches of `B`,
/// and `D` runs on what `X` aggregated. The shape the mirror's hardest case
/// needs: `node_run.path` is unique and all three branches dispatch the same
/// `node_id`, so a scheme without a branch index records one row where there
/// should be three -- silently, because the insert is
/// `ON CONFLICT (path) DO NOTHING`.
fn fanning_out() -> Value {
    fanning_out_to("predict")
}

/// The same, with the branch pointed at a stub route of the caller's choosing.
fn fanning_out_to(branch: &str) -> Value {
    let yaml = format!(
        "pipeline_id: p\nstart: A\ncomponents:\n\
         \x20 - node_id: A\n    name: list\n    type: Model\n    children: [X]\n\
         \x20 - node_id: X\n    name: fan\n    type: ListAggregator\n    start: B\n\
         \x20   components:\n\
         \x20     - node_id: B\n        name: {branch}\n        type: Model\n\
         \x20       children: [end]\n\
         \x20   children: [D]\n\
         \x20 - node_id: D\n    name: predict\n    type: Model\n    children: [end]\n"
    );
    let yaml = yaml.as_str();
    match serde_yaml::from_str::<serde_yaml::Value>(yaml) {
        Ok(value) => match serde_json::to_value(value) {
            Ok(json) => json,
            Err(error) => panic!("the fixture must convert: {error}"),
        },
        Err(error) => panic!("the fixture must parse: {error}"),
    }
}

fn meta() -> Value {
    meta_for("job-1")
}

/// The same envelope under a `job_id` of the caller's choosing.
fn meta_for(job_id: &str) -> Value {
    json!({
        "job_id": job_id,
        "tenant_id": "tenant-1",
        "pipeline_type": "invoice",
        "pipeline_level": "page",
        "pipeline_name": "default",
    })
}

/// How long a test will wait for the ingress before calling it a failure.
///
/// Without this the regression mode of every "fails terminally" test is a
/// **hang**, not a failure: if a permanent fault were reclassified as
/// retryable, the invoker's policy would retry it and the request would never
/// answer, so the suite would stop rather than report. Comfortably above the
/// 5 s retry bound the harness configures below.
const PATIENCE: Duration = Duration::from_secs(90);

async fn invoke(body: Value) -> (reqwest::StatusCode, Value) {
    serving().await;
    let client = match reqwest::Client::builder().timeout(PATIENCE).build() {
        Ok(client) => client,
        Err(error) => panic!("the ingress client must build: {error}"),
    };
    let response = match client
        .post(format!("{INGRESS}/PneumaRunner/run"))
        .json(&body)
        .send()
        .await
    {
        Ok(response) => response,
        Err(error) => panic!("could not reach the ingress at {INGRESS}: {error}"),
    };
    let status = response.status();
    let value = response.json::<Value>().await.unwrap_or(Value::Null);
    (status, value)
}

#[tokio::test]
async fn a_pipeline_runs_end_to_end_through_a_real_restate_server() {
    // The whole adopted branch in one assertion: a definition goes in, every
    // step is called on a component that knows nothing about Restate, and the
    // run reports what ended.
    let (status, report) = invoke(json!({
        "pipeline": pipeline(),
        "meta": meta(),
        "input": {"doc": "d"},
        "custom_data": {"tenant_hint": "acme"},
    }))
    .await;
    assert!(status.is_success(), "invocation failed: {status} {report}");

    let Some(ran) = report.get("ran").and_then(Value::as_array) else {
        panic!("the report names what ran: {report}");
    };
    let ran: Vec<&str> = ran.iter().filter_map(Value::as_str).collect();
    assert_eq!(ran, vec!["A", "B"], "both steps ran, in order");

    let Some(outputs) = report.get("outputs").and_then(Value::as_array) else {
        panic!("the report names what ended: {report}");
    };
    assert_eq!(outputs.len(), 1, "one ending: {report}");
    let Some(ended) = outputs
        .first()
        .and_then(|o| o.get(0))
        .and_then(Value::as_str)
    else {
        panic!("the ending is named: {report}");
    };
    assert_eq!(ended, "B");
}

#[tokio::test]
async fn the_component_receives_its_params_and_the_caller_passthrough() {
    // Proves the wire end to end rather than at the `Dispatch` boundary: these
    // travel through `ctx.run`, over HTTP, into a component that echoes them.
    let (status, report) = invoke(json!({
        "pipeline": pipeline(),
        "meta": meta(),
        "input": {"doc": "d"},
        "custom_data": {"tenant_hint": "acme"},
    }))
    .await;
    assert!(status.is_success(), "invocation failed: {status} {report}");

    // `B` is the ending, and its input is `A`'s echoed output -- so `A`'s view
    // of what it was sent is visible from the outside.
    let Some(echoed) = report.pointer("/outputs/0/1/echo") else {
        panic!("the ending carries A's output: {report}");
    };
    assert_eq!(
        echoed.pointer("/node_env_vars"),
        Some(&json!({"key_1": "value_1"})),
        "A's declared params reached the component: {report}"
    );
    assert_eq!(
        echoed.pointer("/custom_data"),
        Some(&json!({"tenant_hint": "acme"})),
        "and so did the caller passthrough: {report}"
    );
}

/// Asserts *which* failure happened, not merely that one did.
///
/// `is_client_error() || is_server_error()` is satisfied by any failure at all
/// -- including "deployment not registered", which is the state every one of
/// these tests would be in if the harness were broken. The status and a
/// fragment of the message are what distinguish the failure under test from
/// the suite not working.
fn failed_with(status: reqwest::StatusCode, body: &Value, code: u16, fragment: &str) {
    assert_eq!(
        status.as_u16(),
        code,
        "expected {code}, got {status}: {body}"
    );
    // The ingress reports a terminal failure as
    // `{"code":N,"message":"...","source":"invocation"}`. `source` is what
    // separates the handler failing from the ingress refusing the request.
    assert_eq!(
        body.get("source").and_then(Value::as_str),
        Some("invocation"),
        "the failure came from the handler, not the ingress: {body}"
    );
    let message = body.get("message").and_then(Value::as_str).unwrap_or("");
    assert!(
        message.contains(fragment),
        "expected {fragment:?} in: {message}"
    );
}

#[tokio::test]
async fn a_pipeline_that_cannot_resolve_fails_terminally_rather_than_retrying_forever() {
    // Resolution failure is the definition being wrong. Retrying it under the
    // invoker's default policy would retry indefinitely, so the handler raises
    // a `TerminalError` and the invocation fails instead of hanging.
    let broken = json!({
        "pipeline_id": "p",
        "start": "A",
        "components": [
            {"node_id": "A", "name": "comp-a", "type": "Model", "children": ["ghost"]}
        ],
    });
    let (status, body) = invoke(json!({
        "pipeline": broken,
        "meta": meta(),
        "input": {},
    }))
    .await;
    // 400, not 500: the definition arrives in the request body, so this is
    // the submission being wrong. A 500 would tell whoever sent it that this
    // service had broken, and invite a retry of something that fails
    // identically for ever.
    failed_with(status, &body, 400, "cannot resolve the pipeline");
    assert!(
        body.get("message")
            .and_then(Value::as_str)
            .unwrap_or("")
            .contains("ghost"),
        "and it names the step that does not exist: {body}"
    );
}

#[tokio::test]
async fn a_component_that_refuses_permanently_fails_the_run_instead_of_retrying_forever() {
    // The finding this exists for. Every non-2xx used to come back as
    // retryable, and the invoker's default policy retries indefinitely -- so a
    // component answering 400 produced a run that never completed and never
    // reported. It now fails, and fails quickly.
    serving().await;
    let (status, body) = invoke(json!({
        "pipeline": one_step_each("refuses", "predict"),
        "meta": meta(),
        "input": {},
    }))
    .await;
    // 400, the component's own status, carried through rather than flattened
    // to 500. A model answering 400 means the submission is wrong; reporting
    // 500 tells whoever submitted it to retry something that cannot succeed.
    failed_with(status, &body, 400, "will not change on a retry");
    // Counted, not timed. Wall clock was a proxy for "was not retried", and
    // these tests run concurrently against one container while a sibling test
    // deliberately drives a retry loop -- so a loaded machine could violate it
    // with nothing actually wrong. The component being called exactly once is
    // the property itself.
    assert_eq!(
        REFUSE_CALLS.load(std::sync::atomic::Ordering::SeqCst),
        1,
        "a permanent refusal is called once and not retried"
    );
}

#[tokio::test]
async fn a_component_answering_something_that_is_not_json_fails_the_run() {
    // A 200 with an HTML body. The status says success, so the failure is in
    // parsing -- which used to be a plain `?`, i.e. retryable, i.e. a hang.
    let (status, body) = invoke(json!({
        "pipeline": one_step_each("not-json", "predict"),
        "meta": meta(),
        "input": {},
    }))
    .await;
    // 502, not 500 and not the component's 200: the component answered, and
    // answered something the contract cannot use.
    failed_with(status, &body, 502, "is not JSON");
    // Counted, like its siblings. Without this the test passes whether the
    // parse failure is terminal or retryable -- which is the entire property
    // it is named for. Once: a body that is not JSON will not become JSON.
    assert_eq!(
        NOT_JSON_CALLS.load(std::sync::atomic::Ordering::SeqCst),
        1,
        "a body that is not JSON is not retried"
    );
}

#[tokio::test]
async fn a_component_that_recovers_is_retried_rather_than_failed() {
    // The other half, and the reason "everything is permanent" would be just as
    // wrong: a 503 from a restarting component is retried, and the run
    // completes. Proves the classification is a distinction rather than a
    // blanket.
    let (status, report) = invoke(json!({
        "pipeline": one_step_each("flaky", "predict"),
        "meta": meta(),
        "input": {"doc": "d"},
    }))
    .await;
    assert!(
        status.is_success(),
        "a component that comes back must not fail the run: {status} {report}"
    );
    let Some(ran) = report.get("ran").and_then(Value::as_array) else {
        panic!("the report names what ran: {report}");
    };
    assert_eq!(ran.len(), 2, "both steps ran despite the first failure");
}

#[tokio::test]
async fn a_body_that_fails_partway_is_retried_rather_than_failed_outright() {
    // The distinction `call_once` draws by splitting `bytes()` from
    // `from_slice`. reqwest reports a connection reset mid-body and malformed
    // JSON with the same error kind, so collapsing them made a component whose
    // pod was killed mid-response fail *terminally* -- the exact transient case
    // the retry policy exists for.
    //
    // The route always fails, so the run does end up failing once the bound is
    // reached. The property is that it was *retried* on the way there.
    let (status, body) = invoke(json!({
        "pipeline": one_step_each("truncated", "predict"),
        "meta": meta(),
        "input": {},
    }))
    .await;
    // 502, not 500: the retry loop was exhausted, and every failure that
    // reaches it that way is the component being down, slow or answering 5xx.
    // A 500 would page whoever owns this service for something only the
    // component's owner can fix.
    //
    // The *message* is deliberately only checked for the component it names.
    // An earlier version asserted "error sending request", which is what this
    // renders -- but that is a `send()` failure on a connection the previous
    // truncation killed, not the body-read failure the test is named for
    // (reqwest renders that as "error decoding response body"). Which of the
    // two the last attempt hits depends on connection-pool timing, so
    // asserting either is asserting a coincidence. The property is below: it
    // was retried.
    failed_with(status, &body, 502, "/truncated");
    assert!(
        TRUNCATED_CALLS.load(std::sync::atomic::Ordering::SeqCst) > 1,
        "but it was retried first, not treated as the component breaking its \
         contract: called {} time(s)",
        TRUNCATED_CALLS.load(std::sync::atomic::Ordering::SeqCst)
    );
}

#[tokio::test]
async fn a_component_that_accepts_and_never_answers_is_cut_off_by_the_client() {
    // Before the client carried a request timeout this hung for ever: the
    // connection is accepted, so no connect timeout fires, and no byte ever
    // arrives. Nothing else in the suite covers it -- `default_client` was
    // line-covered merely by being called, so a regression to
    // `reqwest::Client::new()` would have passed the whole gate including 100%
    // coverage.
    let started = std::time::Instant::now();
    let (status, body) = invoke(json!({
        "pipeline": one_step_each("stalls", "predict"),
        "meta": meta(),
        "input": {},
    }))
    .await;
    // Same shape, different cause: every attempt was cut off by the client's
    // own request timeout rather than by the component answering -- still the
    // component's failure, so still 502. The status and the named route are
    // what is stable; the reqwest error text is not the property, and the
    // timing assertion below is.
    failed_with(status, &body, 502, "/stalls");
    // The route sleeps an hour. Finishing at all is the assertion; finishing
    // inside the retry bound is what says the *client* cut it off rather than
    // something further out.
    assert!(
        started.elapsed() < Duration::from_secs(120),
        "the client gave up rather than waiting on the component: {:?}",
        started.elapsed()
    );
    assert!(
        STALL_CALLS.load(std::sync::atomic::Ordering::SeqCst) >= 1,
        "the component was actually reached"
    );
}

#[tokio::test]
async fn a_run_writes_every_step_it_ran_into_postgres() {
    // The wiring, end to end. `pneuma-mirror`'s own tests drive the statements
    // against Postgres and `pneuma-runner`'s drive the derivation against a
    // fake, and both passed while `node_run` had no producer at all -- which is
    // exactly the state this port found the table in. What is only assertable
    // here is that a real invocation, through a real server, leaves the rows.
    let job = a_job("mirrored");
    let (status, report) = invoke(json!({
        "pipeline": pipeline(),
        "meta": meta_for(&job),
        "input": {"doc": "d"},
    }))
    .await;
    assert!(status.is_success(), "invocation failed: {status} {report}");

    let rows = rows(&job).await;
    let paths: Vec<&str> = rows.iter().map(|row| row.path.as_str()).collect();
    let prefix = format!("{job}.invoice.page.default");
    assert_eq!(
        paths,
        vec![format!("{prefix}.A"), format!("{prefix}.B")],
        "one row per step, pathed under the run and its pipeline"
    );
    for row in &rows {
        assert_eq!(row.status, NodeStatus::Finished, "{}", row.path);
        assert_eq!(row.kind, NodeKind::Model);
        assert_eq!(row.run_id.as_str(), job);
        assert!(
            row.parent_path.is_none(),
            "neither step is inside a fan-out"
        );
        assert!(row.step_output.is_some(), "{} recorded nothing", row.path);
        // `started_at` is what the two-write cost buys. The mirror sees one
        // instant -- a step is dispatched -- and spends two writes on it,
        // `CREATED` then `PROCESSING`, because `node_run_update_status.sql`
        // stamps `started_at` on the move *into* `PROCESSING`. A row that went
        // straight to `FINISHED` would carry no start time at all.
        //
        // `finished_at` stays null, and that is the original's behaviour rather
        // than a gap: nothing in it writes `finished_at` on a noderun
        // (`node_run_record_output.sql:10-12`), and inventing a value here
        // would make this port's rows disagree with the ones already in the
        // table.
        assert!(row.started_at.is_some(), "{} never started", row.path);
        assert!(row.finished_at.is_none(), "{} invented a finish", row.path);
    }
    let Some(first) = rows.first() else {
        panic!("asserted above");
    };
    assert_eq!(
        first.step_input,
        Some(json!({"doc": "d"})),
        "the first step's input is the run's"
    );
}

#[tokio::test]
async fn a_fan_out_writes_one_row_per_branch_and_aggregates_its_parent() {
    // The case the unique `path` makes silent. All three branches dispatch the
    // node id `B`, so a path scheme without a branch index writes one row and
    // `ON CONFLICT (path) DO NOTHING` swallows the other two -- no error, and
    // the mirror under-reports precisely the runs that are hardest to reason
    // about.
    //
    // It is also the ordering test. `node_run.parent_path` is a non-deferrable
    // foreign key onto `path`, so `X`'s row must be written before any branch's
    // -- which is why the interpreter announces a fan-out rather than letting
    // the driver infer one from the steps it dispatches.
    let job = a_job("fanout");
    let (status, report) = invoke(json!({
        "pipeline": fanning_out(),
        "meta": meta_for(&job),
        "input": {"doc": "d"},
    }))
    .await;
    assert!(status.is_success(), "invocation failed: {status} {report}");

    let rows = rows(&job).await;
    let prefix = format!("{job}.invoice.page.default");
    let paths: Vec<&str> = rows.iter().map(|row| row.path.as_str()).collect();
    assert_eq!(
        paths,
        vec![
            format!("{prefix}.A"),
            format!("{prefix}.D"),
            format!("{prefix}.X"),
            format!("{prefix}.X:1.B"),
            format!("{prefix}.X:2.B"),
            format!("{prefix}.X:3.B"),
        ],
        "three branches, not one row for three"
    );

    let Some(aggregator) = rows.iter().find(|row| row.path == format!("{prefix}.X")) else {
        panic!("asserted above");
    };
    // `AGGREGATED`, which is only reachable *through* `FORKED`
    // (`pneuma_core::status`): a parent that never forked cannot aggregate, so
    // the terminal status proves the order the two writes happened in.
    assert_eq!(aggregator.status, NodeStatus::Aggregated);
    assert_eq!(aggregator.kind, NodeKind::ListAggregator);
    assert_eq!(
        aggregator
            .step_output
            .as_ref()
            .and_then(Value::as_array)
            .map(Vec::len),
        Some(3),
        "the aggregate is what the branches produced: {aggregator:?}"
    );

    let branches: Vec<&pneuma_store::NodeRun> = rows
        .iter()
        .filter(|row| row.parent_path.is_some())
        .collect();
    assert_eq!(branches.len(), 3);
    for (position, branch) in branches.iter().enumerate() {
        assert_eq!(branch.status, NodeStatus::Finished, "{}", branch.path);
        assert_eq!(branch.parent_path, Some(format!("{prefix}.X")));
        assert_eq!(branch.parent_kind.as_deref(), Some("ListAggregator"));
        assert_eq!(
            branch
                .child_index
                .map(pneuma_core::child_index::ChildIndex::get),
            u32::try_from(position + 1).ok(),
            "branches are indexed from one, in order: {}",
            branch.path
        );
    }
}

#[tokio::test]
async fn running_the_same_pipeline_twice_leaves_one_row_per_step() {
    // Idempotency of the whole path, which is what makes a replay safe. A row's
    // id is `uuid_v5(path)` rather than random, so a second write of a step is
    // the same row -- and `ON CONFLICT (path) DO NOTHING` is then a genuine
    // no-op rather than a swallowed duplicate.
    //
    // Two invocations rather than a forced replay: killing the handler
    // mid-run would kill this process, since the SDK endpoint is served from
    // it. What a replay *does* is re-execute the handler against the journal,
    // and a journalled `ctx.run` returns its recorded answer instead of running
    // again (`crate::component`'s module docs record that measurement), so the
    // duplicate-write case a replay could produce is the one asserted here.
    let job = a_job("twice");
    for attempt in 0..2 {
        let (status, report) = invoke(json!({
            "pipeline": pipeline(),
            "meta": meta_for(&job),
            "input": {"doc": "d"},
        }))
        .await;
        assert!(
            status.is_success(),
            "attempt {attempt} failed: {status} {report}"
        );
    }
    let rows = rows(&job).await;
    assert_eq!(rows.len(), 2, "two steps, two rows, two runs: {rows:?}");
}

#[tokio::test]
async fn a_failed_branch_marks_its_ancestors_and_not_only_itself() {
    // The recursion the original walks up `parent_slug`.
    // Without it a fan-out whose
    // branch failed leaves an aggregator sitting at `FORKED` for ever, which
    // reads as "still running" to anything looking at the table -- including
    // the janitor's stale detection, which would then archive a run it should
    // have reported.
    let job = a_job("childerror");
    let (status, body) = invoke(json!({
        "pipeline": fanning_out_to("refuses-branch"),
        "meta": meta_for(&job),
        "input": {"doc": "d"},
    }))
    .await;
    // The component's own 400, carried through: a branch refusing permanently
    // is the submission being wrong, exactly as it is at the top level.
    failed_with(status, &body, 400, "will not change on a retry");

    let rows = rows(&job).await;
    let prefix = format!("{job}.invoice.page.default");
    let Some(branch) = rows
        .iter()
        .find(|row| row.path == format!("{prefix}.X:1.B"))
    else {
        panic!("the branch that failed has a row: {rows:?}");
    };
    assert_eq!(branch.status, NodeStatus::Error);
    assert_eq!(
        branch.error_code.as_deref(),
        Some("PNEUMA_TRANSPORT_ERROR"),
        "and says why: {branch:?}"
    );

    let Some(aggregator) = rows.iter().find(|row| row.path == format!("{prefix}.X")) else {
        panic!("the aggregator has a row: {rows:?}");
    };
    assert_eq!(
        aggregator.status,
        NodeStatus::HasChildError,
        "the failure reached the parent: {aggregator:?}"
    );
}
