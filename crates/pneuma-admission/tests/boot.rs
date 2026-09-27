//! The whole process, wired the way `main` wires it.
//!
//! Every other file here tests one half against a fake for the other. This one
//! starts the real thing — a real pool, the real router, the real dispatch
//! loop, the real HTTP client — and asks the only question the halves cannot:
//! whether a submission posted to the door reaches Restate without anybody
//! calling a function in between.
//!
//! Restate itself is a stub, because what a real container answers is measured
//! in `restate.rs` and the design notes What is unmeasured until here is
//! the wiring.
//!
//! ```sh
//! PNEUMA_TEST_DATABASE_URL=postgres://postgres:pneuma@127.0.0.1:5433/postgres \
//!     cargo test -p pneuma-admission --test boot
//! ```

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::routing::post;
use axum::Router;
use chrono::TimeDelta;
use pneuma_admission::{boot, BootError, Config};
use secrecy::SecretString;
use serde_json::{json, Value};
use sqlx::{Executor, PgPool};
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;

const SUBMISSION: &str = include_str!("../../pneuma-store/migrations/0003_submission.sql");

/// A schema of this test's own, and a DSN that points at it.
///
/// The search path travels in the DSN rather than in an `after_connect` hook,
/// because the pool under test is built by `boot::run` from nothing but that
/// string — which is the point: a test that handed it a pre-configured pool
/// would not be testing the startup path.
async fn fresh(name: &str) -> (String, PgPool) {
    let Ok(url) = std::env::var("PNEUMA_TEST_DATABASE_URL") else {
        panic!("PNEUMA_TEST_DATABASE_URL is not set; see the header of this file");
    };
    let schema = format!("pneuma_boot_{name}");
    // One connection, because `SET search_path` is per session: with two, the
    // `CREATE TYPE` can land on a connection that never saw the `SET` and the
    // whole migration goes into `public`.
    let Ok(pool) = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(&url)
        .await
    else {
        panic!("could not connect to {url}");
    };
    for statement in [
        format!("DROP SCHEMA IF EXISTS {schema} CASCADE"),
        format!("CREATE SCHEMA {schema}"),
        format!("SET search_path TO {schema}"),
        SUBMISSION.to_owned(),
    ] {
        if let Err(error) = pool.execute(statement.as_str()).await {
            panic!("setup failed on {statement:?}: {error}");
        }
    }
    let separator = if url.contains('?') { '&' } else { '?' };
    (
        format!("{url}{separator}options=-c%20search_path%3D{schema}"),
        pool,
    )
}

/// What the stub Restate was asked to run.
type Seen = Arc<Mutex<Vec<(String, Value)>>>;

/// A Restate that accepts everything and remembers it.
async fn stub_restate(seen: Seen) -> SocketAddr {
    let router = Router::new().route(
        "/PneumaRunner/run/send",
        post(
            move |headers: axum::http::HeaderMap, axum::Json(body): axum::Json<Value>| {
                let seen = Arc::clone(&seen);
                async move {
                    let key = headers
                        .get("idempotency-key")
                        .and_then(|value| value.to_str().ok())
                        .unwrap_or("<absent>")
                        .to_owned();
                    match seen.lock() {
                        Ok(mut seen) => seen.push((key, body)),
                        Err(poisoned) => poisoned.into_inner().push((key, body)),
                    }
                    axum::Json(json!({"status": "Accepted"}))
                }
            },
        ),
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

fn config(database_url: &str, ingress: &str) -> Config {
    let Some(listen) = "127.0.0.1:0".parse::<SocketAddr>().ok() else {
        panic!("a real address");
    };
    let Some(reclaim_after) = TimeDelta::try_seconds(300) else {
        panic!("five minutes is a duration");
    };
    Config {
        database_url: SecretString::from(database_url.to_owned()),
        listen,
        ingress: ingress.to_owned(),
        handler: "PneumaRunner/run".to_owned(),
        batch_size: 10,
        per_tenant: 100,
        // Short, because the test waits for a round to happen. `every` runs
        // the first one immediately, so this only bounds the retry.
        interval: Duration::from_millis(50),
        reclaim_after,
        weights: BTreeMap::new(),
    }
}

fn submission(tenant: &str, job: &str) -> Value {
    json!({
        "pipeline": {
            "pipeline_id": "p",
            "start": "A",
            "components": [
                {"node_id": "A", "name": "comp-a", "type": "Model", "children": ["end"]}
            ],
        },
        "meta": {
            "job_id": job, "tenant_id": tenant,
            "pipeline_type": "invoice", "pipeline_level": "page", "pipeline_name": "default",
        },
        "input": {"doc": "d"},
    })
}

/// Polls `check` until it answers, or gives up after a second.
///
/// A bounded wait rather than a sleep: the round runs on its own schedule, and
/// a fixed sleep is either flaky or slow. Failing after a second means a broken
/// wiring fails the suite instead of hanging it.
async fn until<F>(what: &str, mut check: F)
where
    F: FnMut() -> bool,
{
    for _ in 0..200_u32 {
        if check() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    panic!("timed out waiting for {what}");
}

#[tokio::test]
async fn a_run_posted_to_the_door_reaches_restate_without_anybody_in_between() {
    let (dsn, pool) = fresh("endtoend").await;
    let seen: Seen = Arc::new(Mutex::new(Vec::new()));
    let restate = stub_restate(Arc::clone(&seen)).await;
    let config = config(&dsn, &format!("http://{restate}"));

    let token = CancellationToken::new();
    let (tx, rx) = oneshot::channel();
    let running = tokio::spawn({
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

    // The health routes are the ones a probe hits, and they are mounted by
    // `run` rather than by the caller -- so an unprefixed or missing one is a
    // deployment that never becomes ready.
    let client = reqwest::Client::new();
    for (path, expected) in [("healthz", 200_u16), ("liveness", 200)] {
        let url = format!("http://{address}/pneuma-admission/{path}");
        let Ok(response) = client.get(&url).send().await else {
            panic!("{url} should answer");
        };
        assert_eq!(response.status().as_u16(), expected, "{url}");
    }

    let Ok(response) = client
        .post(format!("http://{address}/pneuma-admission/runs"))
        .json(&submission("acme", "job-1"))
        .send()
        .await
    else {
        panic!("the ingress should answer");
    };
    assert_eq!(response.status().as_u16(), 202, "queued");

    until("the dispatch loop to submit the run", || {
        match seen.lock() {
            Ok(seen) => !seen.is_empty(),
            Err(poisoned) => !poisoned.into_inner().is_empty(),
        }
    })
    .await;
    let submitted = match seen.lock() {
        Ok(seen) => seen.clone(),
        Err(poisoned) => poisoned.into_inner().clone(),
    };
    assert_eq!(submitted.len(), 1);
    assert_eq!(
        submitted[0].0, "job-1",
        "the idempotency key is the run id, which is what makes a redelivery free"
    );

    // And the queue agrees it is finished, which is the half a stub cannot say:
    // the stub sees the submission arrive, the row is what says it was settled
    // afterwards rather than left claimed for the sweep.
    let sql = "SELECT state::text FROM submission WHERE run_id = $1";
    let mut state = String::new();
    for _ in 0..200_u32 {
        state = match sqlx::query_scalar(sql).bind("job-1").fetch_one(&pool).await {
            Ok(state) => state,
            Err(error) => panic!("could not read the row back: {error}"),
        };
        if state == "done" {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert_eq!(state, "done", "settled, not left for the sweep");

    token.cancel();
    match running.await {
        Ok(Ok(())) => {}
        Ok(Err(error)) => panic!("a cancelled service exits cleanly, not with {error}"),
        Err(error) => panic!("the service task panicked: {error}"),
    }
}

#[tokio::test]
async fn a_dsn_that_is_not_one_is_a_startup_failure() {
    // Before anything binds, and before the first submission -- which is the
    // difference between a pod that never becomes ready and one that accepts
    // work it cannot file.
    let config = config("not a dsn at all", "http://127.0.0.1:1");
    let Err(error) = boot::run(&config, CancellationToken::new(), |_| {
        panic!("nothing should bind");
    })
    .await
    else {
        panic!("that is not a connection string");
    };
    assert!(
        matches!(error, BootError::Database(_)),
        "wrong variant: {error:?}"
    );
}

#[tokio::test]
async fn an_address_that_cannot_be_bound_stops_the_whole_process() {
    // An address already in use, rather than a privileged port: port 1 is
    // unbindable only for an unprivileged process, so a container running as
    // root -- an ordinary dev setup -- would bind it, fire the `bound`
    // callback, and fail this test for the opposite of the reason it exists.
    //
    // And the property being checked is not the bind failure. It is that the
    // dispatch loop stops with the server: a replica that kept dispatching
    // after failing to serve is one nobody can drain, and this test would hang
    // rather than fail if the two were not joined.
    let (dsn, _pool) = fresh("unbindable").await;
    let Ok(occupied) = TcpListener::bind("127.0.0.1:0").await else {
        panic!("a loopback port");
    };
    let Ok(listen) = occupied.local_addr() else {
        panic!("a bound listener has an address");
    };
    let mut config = config(&dsn, "http://127.0.0.1:1");
    config.listen = listen;

    let Err(error) = boot::run(&config, CancellationToken::new(), |_| {
        panic!("nothing should bind");
    })
    .await
    else {
        panic!("{listen} is already listening");
    };
    let BootError::Serve { address, reason } = &error else {
        panic!("wrong variant: {error:?}");
    };
    assert_eq!(*address, listen);
    assert!(!reason.is_empty(), "the operating system said something");
}
