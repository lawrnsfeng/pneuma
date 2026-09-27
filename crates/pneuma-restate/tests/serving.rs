//! Brings the service up the way the binary does.
//!
//! `serve` binds a port and never returns, so it cannot be called from a unit
//! test — but leaving it untested means the one path the binary actually takes
//! is the one path nothing exercises. Spawning it and asking it for its
//! discovery manifest is enough: if the endpoint is built wrong, or the service
//! is not bound into it, discovery is what says so.
//!
//! `main` reads the environment, calls [`configuration`], then [`connect`],
//! then [`serve`], and this file walks the same three. `connect` opens a real
//! pool, so it needs a real Postgres:
//!
//! ```sh
//! docker run -d --name pn-pg -p 5433:5432 -e POSTGRES_PASSWORD=pneuma postgres:16
//! PNEUMA_TEST_DATABASE_URL=postgres://postgres:pneuma@127.0.0.1:5433/postgres \
//!     cargo test -p pneuma-restate --test serving
//! ```

use std::time::Duration;

use pneuma_restate::serve::{configuration, connect, serve, ServeError};

/// A port of its own, so this cannot collide with `tests/handler.rs`.
const PORT: u16 = 9082;

// A multi-thread runtime, not the `#[tokio::test]` default. `listen_and_serve`
// does not make progress on a current-thread runtime while this task is parked
// on a request, so the server never binds and the probe reports connection
// refused -- which reads as "the server is broken" rather than "the test runs
// it wrong". `handler.rs` needs its own runtime for the same reason.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_server_binds_and_announces_the_service_it_serves() {
    prepared_schema("pneuma_restate_serving", true).await;
    let Ok(prepared) = configuration(|key| match key {
        "PNEUMA_COMPONENT_ENDPOINT" => Some("http://components.svc/{component}".to_owned()),
        "DATABASE_URL" => Some(database_url("pneuma_restate_serving")),
        "PNEUMA_LISTEN" => Some(format!("127.0.0.1:{PORT}")),
        "PNEUMA_COMPONENT_TIMEOUT_SECS" => Some("45".to_owned()),
        _ => None,
    }) else {
        panic!("this configuration is complete");
    };
    assert_eq!(prepared.1.port(), PORT, "it listens where it was told");

    let (runner, address) = match connect(prepared).await {
        Ok(connected) => connected,
        Err(error) => panic!("a reachable database must connect: {error}"),
    };
    // What the environment said, read back off the thing that will act on it.
    // Asserting that `configuration` returned `Ok` says only that nothing was
    // refused; these say the runner about to be served will dial where
    // `PNEUMA_COMPONENT_ENDPOINT` pointed, wait as long as
    // `PNEUMA_COMPONENT_TIMEOUT_SECS` said, and bound its retries by that same
    // timeout rather than by a constant beside it.
    let Ok(url) = runner.endpoint().url_for("ocr") else {
        panic!("the template resolves");
    };
    assert_eq!(url, "http://components.svc/ocr");
    assert_eq!(runner.timeout(), Duration::from_secs(45));
    assert_eq!(
        format!("{:?}", runner.retry()),
        format!(
            "{:?}",
            pneuma_restate::HttpComponent::retry_for(Duration::from_secs(45))
        ),
        "the bound follows the timeout"
    );

    tokio::spawn(async move { serve(runner, address).await });

    // The SDK endpoint speaks h2c: HTTP/2 with prior knowledge, no upgrade. A
    // default HTTP/1.1 client gets no response at all -- which presents as
    // "connection refused" and reads as "the server never bound", when the
    // server is bound and simply does not speak what is being spoken to it.
    let Ok(client) = reqwest::Client::builder().http2_prior_knowledge().build() else {
        panic!("an h2-prior-knowledge client must build");
    };
    let mut manifest = None;
    // The last thing that went wrong, so a failure says *what*. The first
    // version reported "did not answer" for a rejected `accept` header, which
    // is a different problem and sends you looking in the wrong place.
    let mut last = "nothing was attempted".to_owned();
    for _ in 0..50 {
        match client
            .get(format!("http://127.0.0.1:{PORT}/discover"))
            // v3, not v1. The SDK accepts v2, v3 and v4 and answers anything
            // else with `BadDiscoveryVersion` -- which the readiness probe in
            // `handler.rs` never noticed, because it only checked that a
            // response arrived at all.
            .header("accept", "application/vnd.restate.endpointmanifest.v3+json")
            .send()
            .await
        {
            Ok(response) if response.status().is_success() => {
                match response.json::<serde_json::Value>().await {
                    Ok(value) => {
                        manifest = Some(value);
                        break;
                    }
                    Err(error) => last = format!("body was not JSON: {error}"),
                }
            }
            Ok(response) => {
                let status = response.status();
                let body = response.text().await.unwrap_or_default();
                last = format!("{status}: {body}");
            }
            Err(error) => last = error.to_string(),
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let Some(manifest) = manifest else {
        panic!("the server did not answer discovery on {PORT} within 5s; last: {last}");
    };
    // The name a deployment registers under. Getting it wrong means Restate
    // accepts the deployment and every invocation goes to a service that is not
    // there.
    let rendered = manifest.to_string();
    assert!(
        rendered.contains("PneumaRunner"),
        "the manifest announces the service: {rendered}"
    );
    assert!(rendered.contains("run"), "and its handler: {rendered}");
}

/// Where `connect` opens its pool, with `schema` on the search path.
///
/// In the DSN, not a `SET` on one connection: a pool that reopens the
/// connection it was `SET` on reads `public` instead, and the failure then
/// blames the code under test.
fn database_url(schema: &str) -> String {
    let Ok(url) = std::env::var("PNEUMA_TEST_DATABASE_URL") else {
        panic!("PNEUMA_TEST_DATABASE_URL is not set; see the header of this file");
    };
    let separator = if url.contains('?') { '&' } else { '?' };
    format!("{url}{separator}options=-c%20search_path%3D{schema}")
}

/// Drops and recreates `schema`, migrating it only if asked.
///
/// The unmigrated case is a schema of its own rather than `public`: another
/// test binary migrating `public` would make "there is no `node_run`" quietly
/// false, and the test would then pass for the wrong reason.
async fn prepared_schema(schema: &str, migrate: bool) {
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
        format!("DROP SCHEMA IF EXISTS {schema} CASCADE"),
        format!("CREATE SCHEMA {schema}"),
    ] {
        if let Err(error) = sqlx::Executor::execute(&bare, statement.as_str()).await {
            panic!("setup failed on {statement:?}: {error}");
        }
    }
    drop(bare);
    if !migrate {
        return;
    }
    let pool = match sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(&database_url(schema))
        .await
    {
        Ok(pool) => pool,
        Err(error) => panic!("could not connect to {schema}: {error}"),
    };
    let mut connection = match pool.acquire().await {
        Ok(connection) => connection,
        Err(error) => panic!("could not acquire a connection to migrate on: {error}"),
    };
    if let Err(error) = pneuma_store::migrator().run(&mut *connection).await {
        panic!("the migrations do not apply: {error}");
    }
}

#[tokio::test]
async fn a_dsn_that_is_not_a_dsn_is_refused_before_anything_is_dialled() {
    // The other half of `Database`, and a different failure from a database
    // that will not answer: nothing is dialled at all, so this returns at once
    // rather than after the startup bound. `configuration` deliberately does
    // not parse the DSN -- what a connection string may contain is sqlx's
    // question, and a parser here would be a second opinion that could refuse
    // something valid.
    let Ok(prepared) = configuration(|key| match key {
        "PNEUMA_COMPONENT_ENDPOINT" => Some("http://components.svc/{component}".to_owned()),
        "DATABASE_URL" => Some("this is not a connection string".to_owned()),
        _ => None,
    }) else {
        panic!("a DSN is not inspected here");
    };
    let Err(error) = connect(prepared).await else {
        panic!("that is not a connection string");
    };
    assert!(
        matches!(error, ServeError::Database(_)),
        "wrong variant: {error:?}"
    );
}

#[tokio::test]
async fn a_database_with_no_node_run_is_a_startup_refusal_too() {
    // A DSN that opens is not a database that can be mirrored. Without this
    // check a replica pointed at an unmigrated database -- or at one whose
    // `search_path` misses the schema the migrations ran in, which is the
    // likelier mistake -- reports ready and then logs one refusal per step, for
    // ever. That is the silent mirror the required `DATABASE_URL` exists to
    // prevent, one step removed.
    prepared_schema("pneuma_restate_bare", false).await;
    let Ok(prepared) = configuration(|key| match key {
        "PNEUMA_COMPONENT_ENDPOINT" => Some("http://components.svc/{component}".to_owned()),
        "DATABASE_URL" => Some(database_url("pneuma_restate_bare")),
        _ => None,
    }) else {
        panic!("this configuration is complete");
    };
    let Err(error) = connect(prepared).await else {
        panic!("there is no node_run in that schema");
    };
    assert!(
        matches!(error, ServeError::Unmigrated(_)),
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
    let schema = "pneuma_restate_wrong";
    prepared_schema(schema, false).await;
    let Ok(url) = std::env::var("PNEUMA_TEST_DATABASE_URL") else {
        panic!("PNEUMA_TEST_DATABASE_URL is not set; see the header of this file");
    };
    let pool = match sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(&url)
        .await
    {
        Ok(pool) => pool,
        Err(error) => panic!("could not connect to {url}: {error}"),
    };
    let statement = format!("CREATE TABLE {schema}.node_run (x integer)");
    if let Err(error) = sqlx::Executor::execute(&pool, statement.as_str()).await {
        panic!("setup failed on {statement:?}: {error}");
    }
    drop(pool);

    let Ok(prepared) = configuration(|key| match key {
        "PNEUMA_COMPONENT_ENDPOINT" => Some("http://components.svc/{component}".to_owned()),
        "DATABASE_URL" => Some(database_url(schema)),
        _ => None,
    }) else {
        panic!("this configuration is complete");
    };
    let Err(error) = connect(prepared).await else {
        panic!("that table has none of the columns the statements name");
    };
    assert!(
        matches!(error, ServeError::Database(_)),
        "wrong variant: {error:?}"
    );
}

#[tokio::test]
async fn a_database_that_cannot_be_reached_is_a_refusal_to_start() {
    // The half of the decision that matters operationally. `DATABASE_URL` is
    // required rather than defaulted because a service that silently mirrors
    // nothing looks exactly like one that works -- and connecting *before*
    // serving is what turns an unreachable database into a pod that never
    // becomes ready, rather than one that reports healthy and writes nothing.
    let Ok(prepared) = configuration(|key| match key {
        "PNEUMA_COMPONENT_ENDPOINT" => Some("http://components.svc/{component}".to_owned()),
        "DATABASE_URL" => Some("postgres://nobody:nothing@127.0.0.1:1/nowhere".to_owned()),
        _ => None,
    }) else {
        panic!("this configuration is complete");
    };
    let Err(error) = connect(prepared).await else {
        panic!("nothing is listening on port 1");
    };
    assert!(
        matches!(error, ServeError::Database(_)),
        "wrong variant: {error:?}"
    );
    // The DSN carries a password, so the message must not -- the design notes
    // §12. `sqlx`'s `Display` names the transport failure, not the target.
    assert!(
        !error.to_string().contains("nothing"),
        "the password reached the message: {error}"
    );
}
