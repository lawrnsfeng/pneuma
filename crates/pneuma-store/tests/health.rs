//! The two probes, against real servers and against a server that is not there.
//!
//! A fake cannot check either half of what these do. Whether `SELECT 1` reaches
//! Postgres is a question about Postgres, and whether an unreachable dependency
//! is reported rather than hung on is a question about the deadline — which a
//! stub answering instantly can never exercise.
//!
//! ```sh
//! PNEUMA_TEST_DATABASE_URL=postgres://postgres:pneuma@127.0.0.1:5433/postgres \
//! PNEUMA_TEST_MONGO_URL=mongodb://127.0.0.1:27018 \
//!     cargo test -p pneuma-store --test health
//! ```

use std::sync::Arc;
use std::time::{Duration, Instant};

use pneuma_store::{MongoHealth, PostgresHealth};
use pneuma_telemetry::{run_liveness_checks, HealthCheckable, HealthStatus};

fn pg_url() -> String {
    match std::env::var("PNEUMA_TEST_DATABASE_URL") {
        Ok(url) => url,
        Err(_) => panic!("PNEUMA_TEST_DATABASE_URL is not set; see the header of this file"),
    }
}

fn mongo_url() -> String {
    match std::env::var("PNEUMA_TEST_MONGO_URL") {
        Ok(url) => url,
        Err(_) => panic!("PNEUMA_TEST_MONGO_URL is not set; see the header of this file"),
    }
}

#[tokio::test]
async fn both_probes_reach_their_real_servers() {
    let Ok(pool) = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(&pg_url())
        .await
    else {
        panic!("could not connect to postgres");
    };
    let Ok(client) = mongodb::Client::with_uri_str(&mongo_url()).await else {
        panic!("could not connect to mongo");
    };

    let checks: Vec<Arc<dyn HealthCheckable>> = vec![
        Arc::new(PostgresHealth::new(pool)),
        Arc::new(MongoHealth::new(client.database("pneuma_health_test"))),
    ];
    // Through the aggregation rather than each `ping` alone: the report is what
    // `liveness` serves, and the names are its keys.
    let report = run_liveness_checks(&checks).await;
    assert_eq!(report.status, HealthStatus::Ok, "{report:?}");
    assert_eq!(
        report.checks.get("postgres").map(String::as_str),
        Some("ok")
    );
    assert_eq!(report.checks.get("mongodb").map(String::as_str), Some("ok"));
    assert!(
        report.duplicate_names.is_empty(),
        "two probes, two names: {:?}",
        report.duplicate_names
    );
}

#[tokio::test]
async fn a_dependency_that_never_answers_is_reported_rather_than_waited_on() {
    // The reason each probe carries a deadline. `run_liveness_checks` awaits
    // every ping in turn with no timeout of its own, so without one an
    // established-but-unresponsive dependency hangs the whole endpoint: the
    // report naming the *other* dependencies is never produced, and the failure
    // reads as a dead process. Kubernetes answers that by restarting a process
    // that is fine.
    //
    // A listener that accepts and never speaks is the shape that does it. A
    // closed port is refused immediately and proves nothing about the deadline.
    let Ok(silent) = tokio::net::TcpListener::bind(("127.0.0.1", 0)).await else {
        panic!("a port is available");
    };
    let Ok(address) = silent.local_addr() else {
        panic!("a bound listener has an address");
    };
    // Accept and hold, answering nothing.
    let _accepting = tokio::spawn(async move {
        let mut held = Vec::new();
        while let Ok((connection, _)) = silent.accept().await {
            held.push(connection);
        }
    });

    let url = format!("postgres://ignored:ignored@{address}/ignored");
    let Ok(pool) = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        // Longer than the probe's own deadline, so what is being measured is
        // the probe giving up and not the pool doing it first.
        .acquire_timeout(Duration::from_secs(60))
        .connect_lazy(&url)
    else {
        panic!("a lazy pool does not connect");
    };

    let probe = PostgresHealth::with_timeout(pool, Duration::from_millis(300));
    let started = Instant::now();
    let outcome = probe.ping().await;
    let elapsed = started.elapsed();

    let Err(message) = outcome else {
        panic!("a server that never answers is not healthy");
    };
    assert!(message.contains("did not answer"), "{message}");
    assert!(message.contains("postgres"), "{message}");
    assert!(
        elapsed < Duration::from_secs(5),
        "the probe gave up on its own deadline rather than waiting: {elapsed:?}"
    );

    // And through the aggregation it is a `degraded` report that still names
    // every other dependency -- which is the whole point of bounding it.
    let checks: Vec<Arc<dyn HealthCheckable>> = vec![Arc::new(probe)];
    let report = run_liveness_checks(&checks).await;
    assert_eq!(report.status, HealthStatus::Degraded);
    let Some(entry) = report.checks.get("postgres") else {
        panic!("the failing dependency is named: {report:?}");
    };
    assert!(entry.starts_with("error: "), "the wire prefix: {entry}");
}

#[tokio::test]
async fn a_mongo_that_never_answers_is_reported_rather_than_waited_on() {
    // The same property for the other store, and it needs its own test: the
    // deadline is per-probe, so Postgres having one says nothing about Mongo.
    // The driver's own server-selection timeout is thirty seconds by default,
    // which is far longer than any probe interval -- so without this the
    // `liveness` endpoint would stop answering for half a minute whenever
    // Mongo went quiet.
    let Ok(silent) = tokio::net::TcpListener::bind(("127.0.0.1", 0)).await else {
        panic!("a port is available");
    };
    let Ok(address) = silent.local_addr() else {
        panic!("a bound listener has an address");
    };
    let _accepting = tokio::spawn(async move {
        let mut held = Vec::new();
        while let Ok((connection, _)) = silent.accept().await {
            held.push(connection);
        }
    });

    let Ok(client) = mongodb::Client::with_uri_str(format!("mongodb://{address}")).await else {
        panic!("a client is built without connecting");
    };
    let probe = MongoHealth::with_timeout(
        client.database("pneuma_health_test"),
        Duration::from_millis(300),
    );
    let started = Instant::now();
    let Err(message) = probe.ping().await else {
        panic!("a server that never answers is not healthy");
    };
    assert!(message.contains("did not answer"), "{message}");
    assert!(message.contains("mongodb"), "{message}");
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "the probe gave up on its own deadline, well inside the driver's \
         thirty-second server selection: {:?}",
        started.elapsed()
    );
}

#[tokio::test]
async fn a_refused_dependency_reads_as_silence_unless_its_client_gives_up_first() {
    // Measured, because this module's earlier claim -- "a driver error is an
    // answer, and a timeout is the absence of one" -- is not true in
    // deployment, and a health check that mis-reports *why* is worse than one
    // that says less.
    //
    // Both drivers retry internally for about thirty seconds by default:
    // sqlx's pool until `acquire_timeout`, Mongo's until
    // `serverSelectionTimeoutMS`. Both are far longer than any sane probe
    // deadline, so the probe's own timeout fires first and a connection
    // *refused instantly* is reported as "did not answer".
    //
    // Port 1 is refused immediately, so anything but a prompt answer here is
    // the client's own retrying rather than the server's silence.
    let refused_pg = "postgres://ignored:ignored@127.0.0.1:1/ignored";
    let Ok(default_pool) = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect_lazy(refused_pg)
    else {
        panic!("a lazy pool does not connect");
    };
    let Err(message) = PostgresHealth::with_timeout(default_pool, Duration::from_millis(400))
        .ping()
        .await
    else {
        panic!("port 1 is not postgres");
    };
    assert!(
        message.contains("did not answer"),
        "a refusal reads as silence under the pool's default 30s acquire timeout: {message}"
    );

    // Shortening the *client's* own timeout below the probe's makes the answer
    // arrive in time to be reported -- and for sqlx that answer is still not
    // the cause: its pool reports its own timeout and drops the underlying
    // `Connection refused`. So for Postgres the cause is never named, whatever
    // is configured.
    let Ok(impatient_pool) = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .acquire_timeout(Duration::from_millis(200))
        .connect_lazy(refused_pg)
    else {
        panic!("a lazy pool does not connect");
    };
    let Err(message) = PostgresHealth::with_timeout(impatient_pool, Duration::from_secs(5))
        .ping()
        .await
    else {
        panic!("port 1 is not postgres");
    };
    assert!(
        message.contains("pool timed out"),
        "sqlx reports its own pool timeout, not the refusal beneath it: {message}"
    );

    // Mongo is the one that can name it, and only with a short
    // `serverSelectionTimeoutMS`. This is the deployment rule the module doc
    // states, demonstrated rather than asserted.
    let Ok(impatient) = mongodb::Client::with_uri_str(
        "mongodb://127.0.0.1:1/?serverSelectionTimeoutMS=500&connectTimeoutMS=500",
    )
    .await
    else {
        panic!("a client is built without connecting");
    };
    let Err(message) = MongoHealth::with_timeout(
        impatient.database("pneuma_health_test"),
        Duration::from_secs(5),
    )
    .ping()
    .await
    else {
        panic!("port 1 is not mongo");
    };
    assert!(
        message.contains("Connection refused"),
        "with a short server-selection timeout the driver names the cause: {message}"
    );
}
