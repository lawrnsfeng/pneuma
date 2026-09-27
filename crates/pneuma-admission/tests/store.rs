//! The whole admission path against the real queue.
//!
//! Everything else here runs against fakes, which is right for the rules and
//! wrong for the wiring: until this existed the only implementor of either
//! trait was a test fake, so the router and the dispatcher were exported and
//! unreachable from anything that runs. Dead wiring with a thorough test looks
//! exactly like live wiring, and coverage cannot tell them apart.
//!
//! Restate is still a fake. What is being checked is that
//! `SubmissionStore` satisfies both traits and that a round moves a row
//! through the states it should — which is a property of this crate and
//! Postgres. What Restate answers is `restate.rs`'s question and is measured
//! against a real container there.
//!
//! ```sh
//! PNEUMA_TEST_DATABASE_URL=postgres://postgres:pneuma@127.0.0.1:5433/postgres \
//!     cargo test -p pneuma-admission --test store
//! ```

use std::collections::BTreeMap;

use async_trait::async_trait;
use pneuma_admission::{accept, round, Invoker, Settings, Submissions};
use pneuma_store::{Accepted, SubmissionStore};
use serde_json::{json, Value};
use sqlx::{Executor, PgPool};

const SUBMISSION: &str = include_str!("../../pneuma-store/migrations/0003_submission.sql");

/// A schema of this test's own. Per test, not per binary: the migration
/// creates a type as well as a table, so a shared schema means tests racing on
/// `CREATE TYPE`.
async fn fresh(name: &str) -> PgPool {
    let Ok(url) = std::env::var("PNEUMA_TEST_DATABASE_URL") else {
        panic!("PNEUMA_TEST_DATABASE_URL is not set; see the header of this file");
    };
    let schema = format!("pneuma_admission_{name}");
    let search_path = format!("SET search_path TO {schema}");
    let Ok(pool) = sqlx::postgres::PgPoolOptions::new()
        .max_connections(2)
        .after_connect({
            let search_path = search_path.clone();
            move |conn, _| {
                let search_path = search_path.clone();
                Box::pin(async move {
                    conn.execute(search_path.as_str()).await?;
                    Ok(())
                })
            }
        })
        .connect(&url)
        .await
    else {
        panic!("could not connect to {url}");
    };
    for statement in [
        format!("DROP SCHEMA IF EXISTS {schema} CASCADE"),
        format!("CREATE SCHEMA {schema}"),
        search_path,
        SUBMISSION.to_owned(),
    ] {
        if let Err(error) = pool.execute(statement.as_str()).await {
            panic!("setup failed on {statement:?}: {error}");
        }
    }
    pool
}

/// A Restate that accepts everything.
struct Accepts;

#[async_trait]
impl Invoker for Accepts {
    async fn send(&self, _key: &str, _payload: &Value) -> Result<(u16, Value), String> {
        Ok((202, json!({"status": "Accepted"})))
    }
}

/// A Restate that cannot be reached.
struct Unreachable;

#[async_trait]
impl Invoker for Unreachable {
    async fn send(&self, _key: &str, _payload: &Value) -> Result<(u16, Value), String> {
        Err("connection refused".to_owned())
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

/// Admits a document and puts it on the real queue, the way the ingress does.
async fn enqueue(store: &SubmissionStore, tenant: &str, job: &str) -> Accepted {
    let body = submission(tenant, job);
    let Ok(typed) = serde_json::from_value(body.clone()) else {
        panic!("that is a submission");
    };
    let Ok(admitted) = accept(&typed) else {
        panic!("it is admissible");
    };
    match Submissions::enqueue(store, &admitted, &body).await {
        Ok(accepted) => accepted,
        Err(error) => panic!("the real store should enqueue: {error}"),
    }
}

async fn state_of(pool: &PgPool, run_id: &str) -> String {
    let sql = "SELECT state::text FROM submission WHERE run_id = $1";
    match sqlx::query_scalar(sql).bind(run_id).fetch_one(pool).await {
        Ok(state) => state,
        Err(error) => panic!("could not read {run_id}: {error}"),
    }
}

fn settings() -> Settings {
    Settings {
        batch_size: 10,
        per_tenant: 100,
        weights: BTreeMap::new(),
    }
}

#[tokio::test]
async fn a_submission_goes_from_the_door_to_done_through_the_real_queue() {
    let pool = fresh("endtoend").await;
    let store = SubmissionStore::new(pool.clone());

    assert_eq!(enqueue(&store, "acme", "job-1").await, Accepted::Queued);
    assert_eq!(
        enqueue(&store, "acme", "job-1").await,
        Accepted::AlreadyQueued,
        "a redelivery through the real store is still a redelivery"
    );
    assert_eq!(state_of(&pool, "job-1").await, "queued");

    let Ok(report) = round(&store, &Accepts, &settings()).await else {
        panic!("a round against the real store should run");
    };
    assert_eq!(
        report.considered, 1,
        "one queued row, read back through the trait"
    );
    assert_eq!(report.claimed, 1);
    assert_eq!(report.submitted, 1);
    assert_eq!(report.unknown, 0);
    assert_eq!(state_of(&pool, "job-1").await, "done");

    // And the next round finds nothing, because a settled row leaves the
    // queue -- which is what stops a dispatcher resubmitting for ever.
    let Ok(quiet) = round(&store, &Accepts, &settings()).await else {
        panic!("an idle round should run");
    };
    assert_eq!((quiet.considered, quiet.claimed), (0, 0));
    assert!(quiet.round > report.round, "the sequence advanced");
}

#[tokio::test]
async fn an_unreachable_restate_leaves_the_row_claimed_for_reclaim() {
    // The reason `reclaim` exists, through the real states. Settling either
    // way would be a guess, so the row stays `claimed` -- invisible to
    // `queued_backlogs`, and recovered later by the sweep.
    let pool = fresh("unreachable").await;
    let store = SubmissionStore::new(pool.clone());
    enqueue(&store, "acme", "job-1").await;

    let Ok(report) = round(&store, &Unreachable, &settings()).await else {
        panic!("an unreachable Restate is not a failed round");
    };
    assert_eq!(report.claimed, 1);
    assert_eq!(report.submitted, 0);
    assert_eq!(report.unknown, 1);
    assert_eq!(state_of(&pool, "job-1").await, "claimed");

    // A second round sees nothing: it is claimed, not queued. Without
    // `reclaim` this row would wait for ever, which is what that function is
    // for and why it is not optional.
    let Ok(next) = round(&store, &Accepts, &settings()).await else {
        panic!("a round should run");
    };
    assert_eq!(next.considered, 0, "a claimed row is not in the backlog");

    let Ok(recovered) = store.reclaim(chrono::Utc::now()).await else {
        panic!("the sweep should run");
    };
    assert_eq!(recovered, vec!["job-1".to_owned()]);

    let Ok(after) = round(&store, &Accepts, &settings()).await else {
        panic!("a round should run");
    };
    assert_eq!(
        after.submitted, 1,
        "and it is dispatched once Restate is back"
    );
    assert_eq!(state_of(&pool, "job-1").await, "done");
}

#[tokio::test]
async fn two_tenants_share_a_round_rather_than_one_taking_it() {
    // Fair dispatch through the real queue: a tenant with a large backlog does
    // not crowd out a tenant with one row, which is the whole reason the queue
    // and the selection exist.
    let pool = fresh("fairness").await;
    let store = SubmissionStore::new(pool.clone());
    for n in 0..8 {
        enqueue(&store, "loud", &format!("loud-{n}")).await;
    }
    enqueue(&store, "quiet", "quiet-0").await;

    let mut settings = settings();
    settings.batch_size = 4;
    let Ok(report) = round(&store, &Accepts, &settings).await else {
        panic!("a round should run");
    };
    assert_eq!(report.selected, 4);
    assert_eq!(
        state_of(&pool, "quiet-0").await,
        "done",
        "the quiet tenant's one row was dispatched in the first round, not \
         after the loud tenant's eight"
    );
}
