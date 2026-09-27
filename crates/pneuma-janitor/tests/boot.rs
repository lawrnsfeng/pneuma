//! Configuration, the line a pass reports, and the service end to end.
//!
//! ```sh
//! docker run -d --name pn-pg    -p 5433:5432 -e POSTGRES_PASSWORD=pneuma postgres:16
//! docker run -d --name pn-mongo -p 27018:27017 mongo:5.0.28
//! PNEUMA_TEST_DATABASE_URL=postgres://postgres:pneuma@127.0.0.1:5433/postgres \
//! PNEUMA_TEST_MONGO_URL=mongodb://127.0.0.1:27018 \
//!     cargo test -p pneuma-janitor --test boot
//! ```

use std::net::SocketAddr;
use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::routing::post;
use axum::Router;
use pneuma_config::Env;
use pneuma_janitor::boot::{self, Config, ConfigureError};
use pneuma_janitor::Endpoints;
use tokio_util::sync::CancellationToken;

/// The one variable with no default: where Postgres is.
fn minimal() -> Vec<(&'static str, &'static str)> {
    vec![("DATABASE_URL", "postgres://user:pass@db/pneuma")]
}

/// A gateway pointed at `base`, for a config built by hand.
fn a_gateway(base: &str) -> pneuma_janitor::Gateway {
    match pneuma_janitor::Gateway::new(base, Duration::from_secs(5)) {
        Ok(gateway) => gateway,
        Err(error) => panic!("{base} is a gateway address: {error}"),
    }
}

fn from(extra: &[(&'static str, &'static str)]) -> Result<Config, ConfigureError> {
    let mut pairs = minimal();
    pairs.extend_from_slice(extra);
    Config::from_env(&Env::from_pairs(pairs))
}

#[test]
fn every_variable_has_a_default_and_it_is_the_deployed_one() {
    let Ok(config) = from(&[]) else {
        panic!("one variable is enough to start");
    };
    assert_eq!(config.endpoints.mongo_database, "pneuma");
    assert_eq!(config.endpoints.runs_collection, "runs");
    assert_eq!(config.endpoints.history_collection, "run_history");
    assert_eq!(
        config.gateway.url(),
        "http://pneuma-gateway:8080/pneuma-gateway/api/v1/terminations"
    );
    // The original's `INTERVAL_MINUTES=*/5`, as a number of minutes.
    assert_eq!(config.interval, Duration::from_secs(5 * 60));
    assert_eq!(config.alert_grace, Duration::from_secs(30));
    assert_eq!(config.listen.to_string(), "0.0.0.0:9086");
    // And the pass settings, which come from `Settings` rather than from here.
    assert_eq!(config.settings.batch, 100);
    assert_eq!(config.settings.retention_days, 14);
    assert!(!config.settings.terminate_stale);
}

#[test]
fn two_names_for_one_collection_is_a_startup_failure() {
    // The refusal that matters most in this file. The archive is a `$merge`
    // from the live collection into the history one, so with a single name it
    // merges the collection into itself and the delete that follows removes
    // them -- a pass that exists to preserve work destroying it, from one typo.
    let result = from(&[
        ("PNEUMA_MONGODB_RUNS_COLLECTION", "runs"),
        ("PNEUMA_MONGODB_HISTORY_COLLECTION", "runs"),
    ]);
    let Err(ConfigureError::SameCollection { name }) = result else {
        panic!("wrong outcome: {result:?}");
    };
    assert_eq!(name, "runs");

    // Either one alone is fine: it is only the two agreeing that is fatal.
    assert!(from(&[("PNEUMA_MONGODB_RUNS_COLLECTION", "run_history")]).is_err());
    assert!(from(&[("PNEUMA_MONGODB_HISTORY_COLLECTION", "archive")]).is_ok());
    assert!(from(&[("PNEUMA_MONGODB_RUNS_COLLECTION", "live")]).is_ok());
}

#[test]
fn a_schedule_of_zero_is_refused_rather_than_run_as_fast_as_possible() {
    // `tokio::time::interval` panics on a zero period, so accepting this would
    // be a process that binds, reports ready, and dies at the first tick.
    for (key, max) in [
        ("PNEUMA_INTERVAL_MINUTES", boot::MAX_INTERVAL_MINUTES),
        ("PNEUMA_ALERT_MISS_GRACE_SECS", boot::MAX_GRACE_SECS),
        ("PNEUMA_GATEWAY_TIMEOUT_SECS", boot::MAX_GRACE_SECS),
    ] {
        for value in ["0", "-1"] {
            let result = from(&[(key, value)]);
            let Err(ConfigureError::OutOfRange { name, max: got, .. }) = result else {
                panic!("{key}={value} should be refused, got {result:?}");
            };
            assert_eq!(name, key);
            assert_eq!(got, max);
        }
        // And the other end. A pasted microsecond epoch is how this happens,
        // and the panic it would cause is past startup.
        let result = from(&[(key, "99999999999")]);
        assert!(
            matches!(result, Err(ConfigureError::OutOfRange { .. })),
            "{key} has no upper bound: {result:?}"
        );
    }
}

#[test]
fn a_listen_address_that_is_not_one_is_refused() {
    let result = from(&[("PNEUMA_LISTEN", "pneuma-janitor:9086")]);
    let Err(ConfigureError::Listen { value }) = result else {
        panic!("a hostname is not a socket address: {result:?}");
    };
    assert_eq!(value, "pneuma-janitor:9086");

    // Trimmed, because a value out of a rendered secret carries a newline.
    let Ok(config) = from(&[("PNEUMA_LISTEN", " 127.0.0.1:9999\n")]) else {
        panic!("a padded address is still an address");
    };
    assert_eq!(config.listen.to_string(), "127.0.0.1:9999");
}

#[test]
fn a_pass_setting_out_of_range_comes_through_as_itself() {
    // `Settings` owns its own refusals; this only checks they are not swallowed
    // on the way through `Config`.
    let result = from(&[("PNEUMA_STALE_AFTER_SECS", "0")]);
    assert!(
        matches!(result, Err(ConfigureError::Settings(_))),
        "{result:?}"
    );

    // As does a variable that is set to something unreadable.
    let result = from(&[("PNEUMA_RUN_BATCH_SIZE", "lots")]);
    assert!(
        matches!(result, Err(ConfigureError::Settings(_))),
        "{result:?}"
    );
}

#[test]
fn a_missing_postgres_is_a_startup_failure() {
    let result = Config::from_env(&Env::from_pairs(Vec::<(&str, &str)>::new()));
    assert!(
        matches!(result, Err(ConfigureError::Variable(_))),
        "{result:?}"
    );
}

#[test]
fn timezone_is_not_read() {
    // The one setting the port declines to carry, and the omission is asserted
    // rather than merely documented: the original parses `TIMEZONE` into a
    // `ZoneInfo`, uses it for `datetime.now(tz) - timedelta(days=N)` and then
    // converts the result back to UTC -- which cancels exactly, for every
    // value. Reading it here would reproduce a knob that appears to do
    // something and does nothing.
    let Ok(without) = from(&[]) else {
        panic!("one variable is enough to start");
    };
    let Ok(with) = from(&[("TIMEZONE", "Asia/Bangkok"), ("PNEUMA_TIMEZONE", "UTC-7")]) else {
        panic!("an unread variable cannot make the config fail");
    };
    assert_eq!(with.interval, without.interval);
    assert_eq!(with.settings, without.settings);
}

// --- the line a pass reports -------------------------------------------------

/// The stores, emptied, in a schema and database of this test's own.
async fn stores(name: &str) -> (Endpoints, String) {
    let Ok(pg_url) = std::env::var("PNEUMA_TEST_DATABASE_URL") else {
        panic!("PNEUMA_TEST_DATABASE_URL is not set; see the header of this file");
    };
    let Ok(mongo_url) = std::env::var("PNEUMA_TEST_MONGO_URL") else {
        panic!("PNEUMA_TEST_MONGO_URL is not set; see the header of this file");
    };
    let database = format!("pneuma_janitor_boot_{name}");

    // A Postgres schema of this test's own, carried in the DSN rather than set
    // on the connection. `boot::run` builds its own pool from the DSN and has
    // no way to be told a search path -- which is the production shape and not
    // something to bend for a test -- so `options=-c search_path=...` is how the
    // isolation every other fixture in this workspace gets is reached here.
    // Without it this binary migrates `public`, where `pneuma-store`'s suite is
    // already doing the same thing, and the two race on `CREATE TYPE`.
    let schema = format!("pneuma_janitor_boot_{name}");
    let dsn = format!("{pg_url}?options=-c%20search_path%3D{schema}");
    let Ok(bare) = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(&pg_url)
        .await
    else {
        panic!("could not connect to {pg_url}");
    };
    for statement in [
        format!("DROP SCHEMA IF EXISTS {schema} CASCADE"),
        format!("CREATE SCHEMA {schema}"),
    ] {
        if let Err(error) = sqlx::Executor::execute(&bare, statement.as_str()).await {
            panic!("setup failed on {statement:?}: {error}");
        }
    }
    drop(bare);

    let Ok(pool) = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(&dsn)
        .await
    else {
        panic!("could not connect to {dsn}");
    };
    let Ok(mut connection) = pool.acquire().await else {
        panic!("could not acquire a connection to migrate on");
    };
    if let Err(error) = pneuma_store::migrator().run(&mut *connection).await {
        panic!("the migrations do not apply: {error}");
    }
    drop(connection);
    drop(pool);

    let Ok(client) = mongodb::Client::with_uri_str(&mongo_url).await else {
        panic!("could not connect to {mongo_url}");
    };
    if client.database(&database).drop().await.is_err() {
        panic!("should drop the test database");
    }

    let endpoints = Endpoints {
        postgres: secrecy::SecretString::from(dsn),
        mongo: secrecy::SecretString::from(mongo_url),
        mongo_database: database.clone(),
        runs_collection: "runs".to_owned(),
        history_collection: "run_history".to_owned(),
    };
    (endpoints, database)
}

/// A gateway that answers with whatever status the test set.
async fn stub(status: Arc<AtomicU16>, seen: Arc<Mutex<Vec<String>>>) -> SocketAddr {
    let app = Router::new().route(
        "/pneuma-gateway/api/v1/terminations",
        post(move |axum::Json(body): axum::Json<serde_json::Value>| {
            let status = Arc::clone(&status);
            let seen = Arc::clone(&seen);
            async move {
                let job = body["job_id"].as_str().unwrap_or_default().to_owned();
                match seen.lock() {
                    Ok(mut seen) => seen.push(job),
                    Err(poisoned) => poisoned.into_inner().push(job),
                }
                axum::http::StatusCode::from_u16(status.load(Ordering::SeqCst))
                    .unwrap_or(axum::http::StatusCode::OK)
            }
        }),
    );
    let Ok(listener) = tokio::net::TcpListener::bind("127.0.0.1:0").await else {
        panic!("could not bind a stub gateway");
    };
    let Ok(address) = listener.local_addr() else {
        panic!("a bound listener has an address");
    };
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    address
}

#[tokio::test]
async fn the_service_runs_a_pass_and_reports_what_it_did() {
    let (endpoints, database) = stores("runs").await;
    let status = Arc::new(AtomicU16::new(201));
    let seen: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let gateway = stub(Arc::clone(&status), Arc::clone(&seen)).await;

    let Some(listen) = "127.0.0.1:0".parse::<SocketAddr>().ok() else {
        panic!("a real address");
    };
    let Ok(settings) =
        pneuma_janitor::Settings::from_env(&Env::from_pairs(Vec::<(&str, &str)>::new()))
    else {
        panic!("the defaults are enough");
    };
    let config = Config {
        endpoints,
        settings,
        gateway: a_gateway(&format!("http://{gateway}")),
        // Longer than the test, so exactly one pass runs: `every` runs the
        // first one immediately and the token is cancelled before the second.
        interval: Duration::from_secs(3600),
        alert_grace: Duration::from_secs(30),
        listen,
    };

    let token = CancellationToken::new();
    let (tx, rx) = tokio::sync::oneshot::channel();
    let checks = tokio::spawn({
        let token = token.clone();
        async move {
            let Ok(address) = rx.await else {
                panic!("the service should bind and say where");
            };
            let http = reqwest::Client::new();
            let mut answered = Vec::new();
            for path in ["healthz", "liveness"] {
                let url = format!("http://{address}/pneuma-janitor/{path}");
                let status = match http.get(&url).send().await {
                    Ok(response) => response.status().as_u16(),
                    Err(error) => panic!("{url} should answer: {error}"),
                };
                answered.push((path, status));
            }
            // Cancelled *before* asserting, and the connection pool dropped
            // with it. A panic here would otherwise leave the token uncancelled
            // and `serve`'s graceful shutdown waiting on a keep-alive
            // connection that nothing will close -- so a failing assertion
            // would present as a hanging suite rather than a failing test.
            drop(http);
            token.cancel();
            assert_eq!(answered, vec![("healthz", 200), ("liveness", 200)]);
        }
    });

    let served = boot::run(&config, boot::Mode::Live, token.clone(), |address| {
        let _ = tx.send(address);
    })
    .await;
    if let Err(error) = served {
        panic!("a cancelled service exits cleanly, not with {error}");
    }
    if let Err(error) = checks.await {
        panic!("the assertions panicked: {error}");
    }

    // Nothing was stale -- termination is off by default -- so the gateway was
    // never called.
    let asked = match seen.lock() {
        Ok(seen) => seen.clone(),
        Err(poisoned) => poisoned.into_inner().clone(),
    };
    assert!(asked.is_empty(), "{asked:?}");
    assert!(!database.is_empty());
}

#[tokio::test]
async fn a_database_that_is_not_there_is_a_startup_failure() {
    // A pod that never becomes ready, rather than one that reports healthy and
    // cleans nothing up. There is no reconnect loop under a janitor.
    let Some(listen) = "127.0.0.1:0".parse::<SocketAddr>().ok() else {
        panic!("a real address");
    };
    let Ok(settings) =
        pneuma_janitor::Settings::from_env(&Env::from_pairs(Vec::<(&str, &str)>::new()))
    else {
        panic!("the defaults are enough");
    };
    let config = Config {
        endpoints: Endpoints {
            postgres: secrecy::SecretString::from(
                "postgres://nobody:nothing@127.0.0.1:1/nowhere".to_owned(),
            ),
            mongo: secrecy::SecretString::from("mongodb://127.0.0.1:1".to_owned()),
            mongo_database: "pneuma".to_owned(),
            runs_collection: "runs".to_owned(),
            history_collection: "run_history".to_owned(),
        },
        settings,
        gateway: a_gateway("http://127.0.0.1:1"),
        interval: Duration::from_secs(3600),
        alert_grace: Duration::from_secs(30),
        listen,
    };
    let Err(error) = boot::run(&config, boot::Mode::Live, CancellationToken::new(), |_| {}).await
    else {
        panic!("there is no database there");
    };
    assert!(
        matches!(error, boot::BootError::Postgres(_)),
        "wrong variant: {error:?}"
    );
}

// --- what a pass reports -----------------------------------------------------

fn a_pass(stale: Vec<String>, expired: Option<pneuma_janitor::Expired>) -> pneuma_janitor::Pass {
    pneuma_janitor::Pass {
        cleaned: pneuma_janitor::Cleaned {
            runs: 3,
            noderuns_archived: 7,
            noderuns_deleted: 7,
            runs_deleted: 3,
        },
        expired,
        stale,
    }
}

#[test]
fn retention_being_off_is_said_rather_than_reported_as_zero() {
    // `0` would say a retention pass ran and found nothing. A non-positive
    // `PNEUMA_RETENTION_DAYS` means keep for ever, and no query is issued at
    // all -- which is what the operator diffing this against the original janitor
    // for a week needs to be able to tell apart.
    let off = boot::describe(&a_pass(Vec::new(), None));
    assert!(off.contains("expired nothing (retention is off)"), "{off}");

    let on = boot::describe(&a_pass(
        Vec::new(),
        Some(pneuma_janitor::Expired {
            runs: 2,
            node_runs: 5,
        }),
    ));
    assert!(on.contains("expired 2 runs and 5 node runs"), "{on}");
    // And the counts a real pass moved, which are the numbers being diffed.
    assert!(
        on.contains("archived 3 runs (7 node runs), deleted 7 node runs and 3 run documents"),
        "{on}"
    );
    assert!(on.contains("0 stale"), "{on}");
}

#[tokio::test]
async fn every_disposition_is_counted_and_only_a_refusal_is_named() {
    // A pass that finds a hundred stale runs because the gateway is down should
    // be one line, not a hundred -- but a refusal is the one outcome an
    // operator has to act on, so those carry their ids.
    let status = Arc::new(AtomicU16::new(201));
    let seen: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let address = stub(Arc::clone(&status), Arc::clone(&seen)).await;
    let Ok(gateway) =
        pneuma_janitor::Gateway::new(&format!("http://{address}"), Duration::from_secs(5))
    else {
        panic!("that is a gateway address");
    };

    let pass = a_pass(vec!["run-1".to_owned(), "run-2".to_owned()], None);

    let submitted = boot::terminate_stale(&gateway, &pass, Duration::from_secs(5)).await;
    assert!(
        submitted.starts_with("terminated 2, already terminating 0"),
        "{submitted}"
    );
    assert!(!submitted.contains("refused:"), "{submitted}");

    // 409 is success -- the run is already being cancelled, which is the
    // outcome asked for.
    status.store(409, Ordering::SeqCst);
    let already = boot::terminate_stale(&gateway, &pass, Duration::from_secs(5)).await;
    assert!(already.contains("already terminating 2"), "{already}");

    status.store(503, Ordering::SeqCst);
    let retryable = boot::terminate_stale(&gateway, &pass, Duration::from_secs(5)).await;
    assert!(retryable.contains("retryable 2"), "{retryable}");
    assert!(retryable.contains("deferred 0"), "{retryable}");

    status.store(422, Ordering::SeqCst);
    let refused = boot::terminate_stale(&gateway, &pass, Duration::from_secs(5)).await;
    assert!(refused.contains("refused 2"), "{refused}");
    assert!(
        refused.contains("refused: run-1 (422), run-2 (422)"),
        "a refusal names the runs: {refused}"
    );

    // Both stores of ids at once, so the two suffixes cannot shadow each other.
    let Ok(nowhere) =
        pneuma_janitor::Gateway::new("http://127.0.0.1:1", Duration::from_millis(500))
    else {
        panic!("that is a gateway address");
    };
    let unreachable = boot::terminate_stale(&nowhere, &pass, Duration::from_secs(5)).await;
    assert!(unreachable.contains("unreachable 2"), "{unreachable}");
    assert!(
        unreachable.contains("unreachable: run-1 ("),
        "{unreachable}"
    );

    // And nothing stale is not "terminated 0": there was no gateway call to
    // report the outcome of.
    let quiet =
        boot::terminate_stale(&gateway, &a_pass(Vec::new(), None), Duration::from_secs(5)).await;
    assert_eq!(quiet, "nothing to terminate");
}

/// A janitor over a schema and Mongo database of this test's own.
///
/// The name decides whether Postgres has the tables: `empty` gets a schema with
/// none of them, which is the cheapest way to make a pass fail.
async fn a_janitor(name: &str) -> (pneuma_janitor::Janitor, bool) {
    let Ok(pg_url) = std::env::var("PNEUMA_TEST_DATABASE_URL") else {
        panic!("PNEUMA_TEST_DATABASE_URL is not set; see the header of this file");
    };
    let Ok(mongo_url) = std::env::var("PNEUMA_TEST_MONGO_URL") else {
        panic!("PNEUMA_TEST_MONGO_URL is not set; see the header of this file");
    };
    let schema = format!("pneuma_janitor_cycle_{name}");
    let Ok(bare) = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(&pg_url)
        .await
    else {
        panic!("could not connect to {pg_url}");
    };
    for statement in [
        format!("DROP SCHEMA IF EXISTS {schema} CASCADE"),
        format!("CREATE SCHEMA {schema}"),
    ] {
        if let Err(error) = sqlx::Executor::execute(&bare, statement.as_str()).await {
            panic!("setup failed on {statement:?}: {error}");
        }
    }
    drop(bare);

    let Ok(pool) = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(&format!("{pg_url}?options=-c%20search_path%3D{schema}"))
        .await
    else {
        panic!("could not connect with a search path");
    };
    let migrated = name != "empty";
    if migrated {
        let Ok(mut connection) = pool.acquire().await else {
            panic!("could not acquire a connection to migrate on");
        };
        if let Err(error) = pneuma_store::migrator().run(&mut *connection).await {
            panic!("the migrations do not apply: {error}");
        }
    }

    let Ok(client) = mongodb::Client::with_uri_str(&mongo_url).await else {
        panic!("could not connect to {mongo_url}");
    };
    let database = client.database(&schema);
    if database.drop().await.is_err() {
        panic!("should drop the test database");
    }
    let runs = database.collection("runs");
    let history = database.collection("run_history");
    let Ok(archive) = pneuma_store::RunHistoryStore::new(runs.clone(), history) else {
        panic!("two different collections");
    };
    let Ok(janitor) = pneuma_janitor::Janitor::new(
        pneuma_store::RunStore::new(runs),
        archive,
        pneuma_store::NodeRunStore::new(pool),
    ) else {
        panic!("one collection, selected and archived from");
    };
    (janitor, migrated)
}

#[tokio::test]
async fn a_failed_pass_is_a_line_and_not_a_stop() {
    // The next pass is minutes away and repeats from the top, which is the
    // whole reason every write in a pass is idempotent.
    let (janitor, migrated) = a_janitor("empty").await;
    assert!(!migrated, "this one is meant to have no tables");
    let gateway = a_gateway("http://127.0.0.1:1");
    let Ok(settings) =
        pneuma_janitor::Settings::from_env(&Env::from_pairs(Vec::<(&str, &str)>::new()))
    else {
        panic!("the defaults are enough");
    };

    let report = boot::cycle(
        &janitor,
        &gateway,
        &settings,
        boot::Mode::Live,
        Duration::from_secs(5),
    )
    .await;
    assert!(report.starts_with("the pass failed: "), "{report}");
}

#[tokio::test]
async fn an_address_already_in_use_stops_the_whole_process() {
    // The health surface is not optional. A janitor that could not bind would
    // otherwise keep running its passes invisibly -- no probe, no way to tell
    // it apart from a pod that has crashed.
    //
    // Port 0 bound first and then handed over, rather than a privileged port:
    // this suite runs as root often enough that binding port 1 succeeds, and a
    // test that only fails for unprivileged users is a test that passes here
    // for the wrong reason.
    let Ok(taken) = tokio::net::TcpListener::bind("127.0.0.1:0").await else {
        panic!("could not take a port");
    };
    let Ok(address) = taken.local_addr() else {
        panic!("a bound listener has an address");
    };

    let (endpoints, _) = stores("inuse").await;
    let Ok(settings) =
        pneuma_janitor::Settings::from_env(&Env::from_pairs(Vec::<(&str, &str)>::new()))
    else {
        panic!("the defaults are enough");
    };
    let config = Config {
        endpoints,
        settings,
        gateway: a_gateway("http://127.0.0.1:1"),
        interval: Duration::from_secs(3600),
        alert_grace: Duration::from_secs(30),
        listen: address,
    };
    let Err(error) = boot::run(&config, boot::Mode::Live, CancellationToken::new(), |_| {}).await
    else {
        panic!("that port is taken");
    };
    let boot::BootError::Serve { address: asked, .. } = &error else {
        panic!("wrong variant: {error:?}");
    };
    assert_eq!(asked, &address);
}

#[tokio::test]
async fn a_termination_phase_that_runs_long_defers_the_rest() {
    // One pass must fit inside its own interval or the schedule stops meaning
    // anything, and `every` cannot tick while the body is awaited -- so an
    // unbounded loop against a gateway that black-holes connections stops the
    // archival and expiry that come first, not just the terminations. A run not
    // attempted this pass is still stale next pass.
    let Ok(nowhere) = pneuma_janitor::Gateway::new("http://127.0.0.1:1", Duration::from_secs(30))
    else {
        panic!("that is a gateway address");
    };
    let pass = a_pass(
        vec!["run-1".to_owned(), "run-2".to_owned(), "run-3".to_owned()],
        None,
    );
    // A budget of nothing, so the first check stops it: every id is deferred
    // and no call is made at all, which is the same rule at its limit.
    let line = boot::terminate_stale(&nowhere, &pass, Duration::ZERO).await;
    assert!(line.contains("deferred 3"), "{line}");
    assert!(line.contains("unreachable 0"), "{line}");
}

#[tokio::test]
async fn a_dry_run_selects_and_reports_and_calls_nothing() {
    // `Janitor::preview_pass` had no caller anywhere in the workspace until the
    // flag reached it, so the week-long production diff the original plan
    // asks for could not have been run.
    //
    // Driven through `cycle` rather than `run`: the pass loop's first tick
    // races the token, and a test that cancels as soon as the listener binds
    // proves nothing about a pass it may not have waited for.
    let (janitor, _) = a_janitor("dry").await;
    let status = Arc::new(AtomicU16::new(201));
    let seen: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let address = stub(Arc::clone(&status), Arc::clone(&seen)).await;
    let gateway = a_gateway(&format!("http://{address}"));

    // Stale termination *on*, which is what makes a preview worth reading: the
    // flag turns the selection on, and the dry run stops the acting. An earlier
    // runbook told operators to leave it off, which reports `0 stale` for a
    // week however much work is stuck.
    let Ok(settings) = pneuma_janitor::Settings::from_env(&Env::from_pairs(vec![(
        "PNEUMA_STALE_TERMINATION_ENABLED",
        "true",
    )])) else {
        panic!("that is a setting");
    };

    let report = boot::cycle(
        &janitor,
        &gateway,
        &settings,
        boot::Mode::DryRun,
        Duration::from_secs(5),
    )
    .await;
    assert!(report.contains("would terminate 0"), "{report}");
    // Distinguishable from a live pass in a week of logs, which is the whole
    // point of running one.
    assert!(!report.contains("terminated 0,"), "{report}");

    let asked = match seen.lock() {
        Ok(seen) => seen.clone(),
        Err(poisoned) => poisoned.into_inner().clone(),
    };
    assert!(asked.is_empty(), "a dry run calls nothing: {asked:?}");
}

#[tokio::test]
async fn a_schedule_that_stops_firing_degrades_liveness() {
    // The knob `PNEUMA_ALERT_MISS_GRACE_SECS` configures. Without this check it
    // was parsed, range-checked, documented and never read -- a knob that
    // appears to do something and does nothing, which is the shape this
    // module's header refuses to build for `TIMEZONE`.
    use pneuma_telemetry::HealthCheckable;

    // A pass has just "finished", so nothing is late.
    let fresh =
        pneuma_janitor::PassFreshness::new(Duration::from_secs(300), Duration::from_secs(30));
    assert!(fresh.ping().await.is_ok());

    // A period and grace of nothing: any elapsed time is a missed tick, which
    // is the same rule at its limit and needs no sleep to reach.
    let stale = pneuma_janitor::PassFreshness::new(Duration::ZERO, Duration::ZERO);
    tokio::time::sleep(Duration::from_millis(2)).await;
    let Err(reason) = stale.ping().await else {
        panic!("a pass that never finished is late");
    };
    assert!(
        reason.starts_with("no pass has finished since "),
        "{reason}"
    );
    assert_eq!(stale.name(), "passes");

    // And recording a pass clears it.
    let before = stale.last();
    tokio::time::sleep(Duration::from_millis(2)).await;
    stale.finished();
    assert!(stale.last() > before);
}

#[test]
fn the_only_flag_is_dry_run_and_anything_else_is_refused() {
    // A typo'd `--dry_run` that quietly ran a live pass against production is
    // the failure this flag exists to prevent, so an unknown argument stops the
    // process rather than being skipped.
    let args = |list: &[&str]| list.iter().map(|s| (*s).to_owned()).collect::<Vec<_>>();
    assert_eq!(boot::Mode::from_args(args(&[])), Ok(boot::Mode::Live));
    assert_eq!(
        boot::Mode::from_args(args(&["--dry-run"])),
        Ok(boot::Mode::DryRun)
    );
    // Repeating it is not an error: it says the same thing twice.
    assert_eq!(
        boot::Mode::from_args(args(&["--dry-run", "--dry-run"])),
        Ok(boot::Mode::DryRun)
    );
    let Err(reason) = boot::Mode::from_args(args(&["--dry_run"])) else {
        panic!("that is not the flag");
    };
    assert!(reason.contains("--dry_run"), "{reason}");
    assert!(reason.contains("the only one is --dry-run"), "{reason}");
    // And a good flag before a bad one does not rescue it.
    assert!(boot::Mode::from_args(args(&["--dry-run", "-n"])).is_err());
}
