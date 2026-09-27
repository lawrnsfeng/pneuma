//! Drives the CLI's execution half against the real databases.
//!
//! `cli.rs` is pure and tested in place; this is `run.rs`, which connects. The
//! point of separating them is that everything decidable without a database
//! was already decided by the time these run — so what is left here is the
//! part that genuinely needs postgres:16 and mongo:5.0.28.
//!
//! ```sh
//! PNEUMA_TEST_DATABASE_URL=postgres://postgres:pneuma@127.0.0.1:5433/postgres \
//! PNEUMA_TEST_MONGO_URL=mongodb://127.0.0.1:27018 \
//!     cargo test -p pneuma-migrate --test cli
//! ```

use pneuma_migrate::cli::{exit_code, Baselining, Invocation, DEFAULT_SCRATCH};
use pneuma_migrate::run::{
    run, CliError, Report, DATABASE_URL, MONGODB_DATABASE, MONGODB_RUNS_COLLECTION, MONGODB_URL,
};
use pneuma_migrate::Plan;
use sqlx::Executor;

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

/// An environment reader over owned pairs, so each test can differ.
fn env(pairs: Vec<(String, String)>) -> impl Fn(&str) -> Option<String> {
    move |key| {
        pairs
            .iter()
            .find(|(name, _)| name == key)
            .map(|(_, value)| value.clone())
    }
}

fn postgres_env() -> impl Fn(&str) -> Option<String> {
    env(vec![(DATABASE_URL.to_owned(), pg_url())])
}

fn mongo_env(collection: &str) -> impl Fn(&str) -> Option<String> {
    env(vec![
        (MONGODB_URL.to_owned(), mongo_url()),
        (MONGODB_DATABASE.to_owned(), "pneuma_cli_test".to_owned()),
        (MONGODB_RUNS_COLLECTION.to_owned(), collection.to_owned()),
    ])
}

/// A schema in the state the original migration tool leaves behind: right tables, no sqlx rows.
async fn original_shaped(schema: &str) {
    let Ok(pool) = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(&pg_url())
        .await
    else {
        panic!("could not connect to {}", pg_url());
    };
    for statement in [
        format!("DROP SCHEMA IF EXISTS {schema} CASCADE"),
        format!("CREATE SCHEMA {schema}"),
        format!("SET search_path TO {schema}"),
        // The migrations `original_migrator` carries, applied by hand -- so the
        // schema is right and `_sqlx_migrations` is absent, which is the state
        // this fixture is named for. Taken from the migrator rather than from
        // two `include_str!`s, because "the shape the original migration tool leaves" now has a
        // definition (`pneuma_store::ORIGINAL_THROUGH`) and a hand-written list
        // beside it is a second definition that can disagree with the first.
        pneuma_store::original_migrator()
            .iter()
            .map(|migration| migration.sql.to_string())
            .collect::<Vec<_>>()
            .join(";\n"),
        "SET search_path TO public".to_owned(),
    ] {
        if let Err(error) = pool.execute(statement.as_str()).await {
            panic!("setup failed on {statement:?}: {error}");
        }
    }
}

#[tokio::test]
async fn baseline_records_the_migrations_and_says_which() {
    // the original plan:304's gate -- "`pneuma-migrate baseline` succeeds against a
    // production-shaped staging database" -- which could not be attempted at
    // all before this binary existed, because `baseline` was reachable only
    // from a `#[tokio::test]`.
    original_shaped("pn_cli_baseline").await;
    let outcome = run(
        Invocation::Baseline(Baselining {
            schema: "pn_cli_baseline".to_owned(),
            scratch: DEFAULT_SCRATCH.to_owned(),
            dry_run: false,
        }),
        postgres_env(),
    )
    .await;
    let Ok(Report::Baselined(Plan::Record(versions))) = &outcome else {
        panic!("a matching schema baselines: {outcome:?}");
    };
    assert_eq!(versions, &vec![1, 2]);
    assert_eq!(exit_code(&outcome), 0);
}

#[tokio::test]
async fn a_dry_run_decides_the_same_thing_and_writes_nothing() {
    // The dry run is a separate path, not a flag inside `baseline`: a function
    // that sometimes writes is one whose dry run is only as trustworthy as the
    // branch inside it. So the property worth asserting is that the two agree,
    // and that afterwards the real one still has work to do.
    original_shaped("pn_cli_dry").await;
    let previewed = run(
        Invocation::Baseline(Baselining {
            schema: "pn_cli_dry".to_owned(),
            scratch: "pn_cli_dry_scratch".to_owned(),
            dry_run: true,
        }),
        postgres_env(),
    )
    .await;
    let Ok(Report::WouldBaseline(would)) = &previewed else {
        panic!("a dry run reports a plan: {previewed:?}");
    };
    assert_eq!(would, &Plan::Record(vec![1, 2]));

    let done = run(
        Invocation::Baseline(Baselining {
            schema: "pn_cli_dry".to_owned(),
            scratch: "pn_cli_dry_scratch".to_owned(),
            dry_run: false,
        }),
        postgres_env(),
    )
    .await;
    let Ok(Report::Baselined(actually)) = &done else {
        panic!("should baseline: {done:?}");
    };
    assert_eq!(
        actually, would,
        "the dry run decided what the real one did -- which is the only thing \
         that makes a dry run worth running"
    );
}

#[tokio::test]
async fn an_unmigrated_schema_is_refused_with_its_own_exit_code() {
    let Ok(pool) = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(&pg_url())
        .await
    else {
        panic!("could not connect");
    };
    for statement in [
        "DROP SCHEMA IF EXISTS pn_cli_empty CASCADE",
        "CREATE SCHEMA pn_cli_empty",
    ] {
        if let Err(error) = pool.execute(statement).await {
            panic!("setup failed: {error}");
        }
    }
    let outcome = run(
        Invocation::Baseline(Baselining {
            schema: "pn_cli_empty".to_owned(),
            scratch: "pn_cli_empty_scratch".to_owned(),
            dry_run: false,
        }),
        postgres_env(),
    )
    .await;
    assert!(outcome.is_err(), "an empty schema is not baselineable");
    assert_eq!(
        exit_code(&outcome),
        3,
        "3 means 'this database is not what the migrations say', which is a \
         different problem from 'could not connect' and needs a human"
    );
}

#[tokio::test]
async fn fingerprint_counts_what_the_schema_holds() {
    original_shaped("pn_cli_fingerprint").await;
    let outcome = run(
        Invocation::Fingerprint {
            schema: "pn_cli_fingerprint".to_owned(),
        },
        postgres_env(),
    )
    .await;
    let Ok(Report::Fingerprint { tables, enums }) = &outcome else {
        panic!("should read the schema: {outcome:?}");
    };
    assert_eq!((*tables, *enums), (2, 2), "node_run + history, two enums");
}

#[tokio::test]
async fn the_mongo_subcommands_reach_mongo() {
    let collection = "cli_runs";
    let Ok(client) = mongodb::Client::with_uri_str(&mongo_url()).await else {
        panic!("could not connect to mongo");
    };
    let runs = client
        .database("pneuma_cli_test")
        .collection::<mongodb::bson::Document>(collection);
    if let Err(error) = runs.drop().await {
        panic!("could not drop: {error}");
    }
    for _ in 0..2 {
        if let Err(error) = runs
            .insert_one(mongodb::bson::doc! { "run_id": "forked" })
            .await
        {
            panic!("could not seed: {error}");
        }
    }

    let indexed = run(Invocation::MongoIndex, mongo_env(collection)).await;
    assert!(
        matches!(indexed, Ok(Report::IndexEnsured)),
        "the index is created: {indexed:?}"
    );

    let reported = run(Invocation::MongoDuplicates, mongo_env(collection)).await;
    let Ok(Report::Duplicates(found)) = &reported else {
        panic!("should report: {reported:?}");
    };
    assert_eq!(found.len(), 1, "the one duplicated run_id: {found:?}");
    assert_eq!(
        exit_code(&reported),
        4,
        "finding duplicates is gateable without reading the prose -- this is \
         the precondition check for making the index unique, and a unique \
         build over duplicates fails only *after* dropping the index it \
         replaces"
    );

    // And none found is success, not merely a different message.
    if let Err(error) = runs.delete_many(mongodb::bson::doc! {}).await {
        panic!("could not clear: {error}");
    }
    let clean = run(Invocation::MongoDuplicates, mongo_env(collection)).await;
    let Ok(Report::Duplicates(none)) = &clean else {
        panic!("should report: {clean:?}");
    };
    assert!(none.is_empty(), "{none:?}");
    assert_eq!(exit_code(&clean), 0);
}

#[tokio::test]
async fn a_missing_connection_variable_is_a_usage_error_not_a_connection_error() {
    // Exit 2, not 1. The operator asked for something impossible; nothing was
    // ever going to be reached, and a retry will not help.
    let outcome = run(
        Invocation::Fingerprint {
            schema: "public".to_owned(),
        },
        env(vec![]),
    )
    .await;
    let Err(CliError::Usage(message)) = &outcome else {
        panic!("an absent DATABASE_URL is a usage error: {outcome:?}");
    };
    assert!(message.contains(DATABASE_URL), "{message}");
    assert_eq!(exit_code(&outcome), 2);

    let mongo = run(Invocation::MongoIndex, env(vec![])).await;
    assert_eq!(exit_code(&mongo), 2, "and the same for MONGODB_URL");
}
