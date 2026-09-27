//! The submission queue, against a real Postgres.
//!
//! Everything here is a property of the database rather than of this code: that
//! a second insert of one `run_id` does nothing, that two dispatchers claiming
//! the same row produce one winner, that a settle from the wrong state changes
//! nothing. A fake would agree with whatever this file asserted.
//!
//! ```sh
//! PNEUMA_TEST_DATABASE_URL=postgres://postgres:pneuma@127.0.0.1:5433/postgres \
//!     cargo test -p pneuma-store --test submission
//! ```

use chrono::{Duration, Utc};
use pneuma_store::{Accepted, Outcome, SubmissionStore};
use serde_json::json;
use sqlx::{Executor, PgPool, Row};

const SUBMISSION: &str = include_str!("../migrations/0003_submission.sql");

/// A schema of this test's own, emptied.
///
/// Per *test*, not per binary: `cargo test` runs the tests in one target
/// concurrently, and the migration creates a type as well as a table -- so a
/// shared schema means several tests racing on `CREATE TYPE` and all but one
/// losing to a duplicate key on `pg_type`. Which is what happened.
async fn fresh(name: &str) -> PgPool {
    let schema = format!("pneuma_test_submission_{name}");
    let search_path = format!("SET search_path TO {schema}");
    let Ok(url) = std::env::var("PNEUMA_TEST_DATABASE_URL") else {
        panic!("PNEUMA_TEST_DATABASE_URL is not set; see the header of this file");
    };
    let Ok(pool) = sqlx::postgres::PgPoolOptions::new()
        // More than one, because two of these tests deliberately run two
        // claims at once and a single-connection pool would serialise the race
        // out of existence -- the test would pass while proving nothing.
        .max_connections(5)
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

async fn state_of(pool: &PgPool, run_id: &str) -> String {
    let sql = "SELECT state::text FROM submission WHERE run_id = $1";
    match sqlx::query_scalar(sql).bind(run_id).fetch_one(pool).await {
        Ok(state) => state,
        Err(error) => panic!("could not read {run_id}: {error}"),
    }
}

#[tokio::test]
async fn a_redelivered_submission_is_accepted_and_changes_nothing() {
    // At-least-once delivery is the normal case, not the exception. A queue
    // that errors on a redelivery makes its caller deduplicate, and the caller
    // has less to deduplicate with -- while a queue that *runs it twice* pays
    // for the same pipeline twice.
    let pool = fresh("redeliver").await;
    let store = SubmissionStore::new(pool.clone());

    let Ok(first) = store.enqueue("r1", "acme", &json!({"doc": 1})).await else {
        panic!("the first submission is accepted");
    };
    assert_eq!(first, Accepted::Queued);

    // A different payload under the same run id, to show the first is kept.
    let Ok(again) = store.enqueue("r1", "acme", &json!({"doc": 999})).await else {
        panic!("a redelivery is not an error");
    };
    assert_eq!(again, Accepted::AlreadyQueued);

    let Ok(rows) = store.queued_backlogs(10).await else {
        panic!("should read the backlog");
    };
    assert_eq!(rows.len(), 1, "one row, not two: {rows:?}");
    assert_eq!(
        rows[0].payload,
        json!({"doc": 1}),
        "the first submission is what stands; a redelivery does not overwrite it"
    );
}

#[tokio::test]
async fn a_backlog_is_bounded_per_tenant_not_in_total() {
    // The bound that makes fair selection possible. A global limit would let a
    // noisy tenant crowd a quiet one out of the *input*, and `select_batch`
    // would then be provably fair over a sample that was not.
    let pool = fresh("backlog").await;
    let store = SubmissionStore::new(pool.clone());
    for n in 0..10 {
        let Ok(_) = store
            .enqueue(&format!("loud-{n}"), "loud", &json!({"n": n}))
            .await
        else {
            panic!("should enqueue");
        };
    }
    let Ok(_) = store.enqueue("quiet-0", "quiet", &json!({})).await else {
        panic!("should enqueue");
    };

    let Ok(rows) = store.queued_backlogs(3).await else {
        panic!("should read the backlog");
    };
    let loud: Vec<&str> = rows
        .iter()
        .filter(|r| r.tenant_id == "loud")
        .map(|r| r.run_id.as_str())
        .collect();
    let quiet: Vec<&str> = rows
        .iter()
        .filter(|r| r.tenant_id == "quiet")
        .map(|r| r.run_id.as_str())
        .collect();
    assert_eq!(loud.len(), 3, "the noisy tenant is capped: {loud:?}");
    assert_eq!(
        loud,
        vec!["loud-0", "loud-1", "loud-2"],
        "and capped at its *oldest*, so nothing is starved by newer work"
    );
    assert_eq!(
        quiet,
        vec!["quiet-0"],
        "the quiet tenant survives the cap, which a global limit would not have \
         guaranteed"
    );
}

#[tokio::test]
async fn two_dispatchers_claiming_one_run_produce_exactly_one_winner() {
    // What `claim` exists for. The fair choice is made outside the database --
    // `select_batch` needs a flow's whole backlog, which a `SKIP LOCKED` query
    // handing out whatever it reaches first cannot give it -- so this UPDATE is
    // the atomic arbiter, and `state = 'queued'` is the whole of it.
    let pool = fresh("claim").await;
    let store = SubmissionStore::new(pool.clone());
    let contested: Vec<String> = (0..20).map(|n| format!("r{n}")).collect();
    for run_id in &contested {
        let Ok(_) = store.enqueue(run_id, "acme", &json!({})).await else {
            panic!("should enqueue");
        };
    }

    // Both dispatchers select the same twenty rows, as two that ran the same
    // fair selection against the same backlog would.
    let one = SubmissionStore::new(pool.clone());
    let two = SubmissionStore::new(pool.clone());
    let (left, right) = tokio::join!(one.claim(&contested), two.claim(&contested));
    let (Ok(left), Ok(right)) = (left, right) else {
        panic!("both claims should succeed");
    };

    assert_eq!(
        left.len() + right.len(),
        contested.len(),
        "every row went to exactly one of them: {} and {}",
        left.len(),
        right.len()
    );
    let mut both: Vec<&String> = left.iter().chain(right.iter()).collect();
    both.sort();
    both.dedup();
    assert_eq!(both.len(), contested.len(), "and none to both");

    // Nothing is left queued, and a third claim gets nothing -- a short list is
    // this working rather than a fault.
    let Ok(nothing) = store.claim(&contested).await else {
        panic!("a claim over already-claimed rows is not an error");
    };
    assert!(nothing.is_empty(), "{nothing:?}");
    let Ok(backlog) = store.queued_backlogs(100).await else {
        panic!("should read the backlog");
    };
    assert!(
        backlog.is_empty(),
        "claimed rows leave the queue: {backlog:?}"
    );
}

#[tokio::test]
async fn only_a_claimed_submission_can_be_settled() {
    // Settling a queued row would mark work done that no dispatcher took;
    // settling a settled one would let a duplicate overwrite the first
    // outcome. Both are `false` rather than an error, so a caller's own
    // redelivery does not fail.
    let pool = fresh("settle").await;
    let store = SubmissionStore::new(pool.clone());
    let Ok(_) = store.enqueue("r1", "acme", &json!({})).await else {
        panic!("should enqueue");
    };

    let too_early = match store.settle("r1", Outcome::Done, None).await {
        Ok(settled) => settled,
        Err(error) => panic!("settling the wrong state is not an error: {error}"),
    };
    assert!(!too_early, "a queued row is not settleable");
    assert_eq!(state_of(&pool, "r1").await, "queued", "and was not touched");

    let Ok(claimed) = store.claim(&["r1".to_owned()]).await else {
        panic!("should claim");
    };
    assert_eq!(claimed, vec!["r1".to_owned()]);

    let Ok(settled) = store
        .settle("r1", Outcome::Failed, Some("ingress said 503"))
        .await
    else {
        panic!("should settle");
    };
    assert!(settled);
    assert_eq!(state_of(&pool, "r1").await, "failed");

    let sql = "SELECT detail, settled_at IS NOT NULL AS stamped FROM submission WHERE run_id = $1";
    let Ok(row) = sqlx::query(sql).bind("r1").fetch_one(&pool).await else {
        panic!("should read back");
    };
    let (Ok(detail), Ok(stamped)) = (
        row.try_get::<Option<String>, _>("detail"),
        row.try_get::<bool, _>("stamped"),
    ) else {
        panic!("columns read");
    };
    assert_eq!(detail.as_deref(), Some("ingress said 503"));
    assert!(stamped, "and when it ended");

    // A second settle, as a duplicate delivery would produce.
    let Ok(again) = store.settle("r1", Outcome::Done, None).await else {
        panic!("a duplicate settle is not an error");
    };
    assert!(!again, "the first outcome stands");
    assert_eq!(
        state_of(&pool, "r1").await,
        "failed",
        "and is not overwritten"
    );

    // A run nobody ever submitted.
    let Ok(absent) = store.settle("ghost", Outcome::Done, None).await else {
        panic!("settling an unknown run is not an error");
    };
    assert!(!absent);
}

#[tokio::test]
async fn the_round_advances_and_is_shared_between_dispatchers() {
    // `select_batch` rotates its tie-break by this number, so a round that does
    // not advance hands the remainder of every uneven batch to the same flow
    // for ever -- measured in `pneuma-fairness` at 800 items against 600 over
    // 200 rounds while each individual batch was provably fair. A sequence is
    // what makes it shared across replicas rather than per process.
    let pool = fresh("round").await;
    let one = SubmissionStore::new(pool.clone());
    let two = SubmissionStore::new(pool.clone());

    let mut seen = Vec::new();
    for _ in 0..3 {
        let (Ok(a), Ok(b)) = (one.next_round().await, two.next_round().await) else {
            panic!("the round should advance");
        };
        seen.push(a);
        seen.push(b);
    }
    let mut sorted = seen.clone();
    sorted.sort_unstable();
    assert_eq!(seen, sorted, "monotonic: {seen:?}");
    let mut unique = seen.clone();
    unique.dedup();
    assert_eq!(
        unique.len(),
        seen.len(),
        "and never handed to two dispatchers at once: {seen:?}"
    );
}

#[tokio::test]
async fn an_empty_claim_is_answered_without_asking_the_database() {
    // The idle case: a dispatcher whose fair selection came back empty should
    // not pay a round trip to be told nothing happened.
    let pool = fresh("empty").await;
    let Ok(claimed) = SubmissionStore::new(pool).claim(&[]).await else {
        panic!("an empty claim is not an error");
    };
    assert!(claimed.is_empty());
}

#[tokio::test]
async fn a_submission_that_already_ran_is_not_reported_as_merely_queued() {
    // `done` and `failed` rows are kept for ever, so "something was already
    // here" covered a run waiting to be dispatched *and* one that finished last
    // month. A caller retrying a failed submission was told `AlreadyQueued` and
    // dropped it -- a success-shaped answer to a question it got wrong.
    let pool = fresh("states").await;
    let store = SubmissionStore::new(pool.clone());
    let payload = json!({});

    let Ok(first) = store.enqueue("r1", "acme", &payload).await else {
        panic!("should enqueue");
    };
    assert_eq!(first, Accepted::Queued);
    let Ok(waiting) = store.enqueue("r1", "acme", &payload).await else {
        panic!("should answer");
    };
    assert_eq!(waiting, Accepted::AlreadyQueued);

    let Ok(_) = store.claim(&["r1".to_owned()]).await else {
        panic!("should claim");
    };
    let Ok(taken) = store.enqueue("r1", "acme", &payload).await else {
        panic!("should answer");
    };
    assert_eq!(
        taken,
        Accepted::AlreadyClaimed,
        "a dispatcher has it, which is not the same as it waiting"
    );

    let Ok(_) = store.settle("r1", Outcome::Failed, Some("503")).await else {
        panic!("should settle");
    };
    let Ok(ran) = store.enqueue("r1", "acme", &payload).await else {
        panic!("should answer");
    };
    assert_eq!(
        ran,
        Accepted::AlreadyRan { succeeded: false },
        "and a run that already failed is not something waiting to be dispatched"
    );

    // The row is not resurrected by any of that.
    assert_eq!(state_of(&pool, "r1").await, "failed");
    let Ok(backlog) = store.queued_backlogs(10).await else {
        panic!("should read the backlog");
    };
    assert!(backlog.is_empty(), "{backlog:?}");
}

#[tokio::test]
async fn a_claim_nobody_settled_goes_back_to_the_queue() {
    // A dispatcher that dies between `claim` and `settle` used to strand its
    // rows for ever: `claim` only moves `queued -> claimed`, `settle` only
    // moves `claimed -> done|failed`, and nothing looked at `claimed` at all.
    // The work was invisible -- no query found it, and re-submitting the same
    // `run_id` collided with the primary key. Silently losing work is the one
    // thing a durable queue must not do.
    let pool = fresh("reclaim").await;
    let store = SubmissionStore::new(pool.clone());
    for run_id in ["stranded", "in-flight"] {
        let Ok(_) = store.enqueue(run_id, "acme", &json!({})).await else {
            panic!("should enqueue");
        };
    }
    let Ok(claimed) = store
        .claim(&["stranded".to_owned(), "in-flight".to_owned()])
        .await
    else {
        panic!("should claim");
    };
    assert_eq!(claimed.len(), 2);

    // Nothing is old enough yet, so a sweep takes nothing -- a dispatch that is
    // merely slow must not be reclaimed, because reclaiming it runs it twice.
    let Ok(none) = store.reclaim(Utc::now() - Duration::hours(1)).await else {
        panic!("should sweep");
    };
    assert!(none.is_empty(), "a fresh claim is not stranded: {none:?}");

    // Age one of them, as a dispatcher dying an hour ago would leave it.
    let sql = "UPDATE submission SET claimed_at = now() - interval '2 hours' \
               WHERE run_id = 'stranded'";
    if let Err(error) = pool.execute(sql).await {
        panic!("could not age the claim: {error}");
    }

    let Ok(recovered) = store.reclaim(Utc::now() - Duration::hours(1)).await else {
        panic!("should sweep");
    };
    assert_eq!(
        recovered,
        vec!["stranded".to_owned()],
        "the stranded one, and only it"
    );
    assert_eq!(state_of(&pool, "stranded").await, "queued");
    assert_eq!(
        state_of(&pool, "in-flight").await,
        "claimed",
        "a live dispatch is left alone"
    );

    // And it is dispatchable again, which is the whole point.
    let Ok(backlog) = store.queued_backlogs(10).await else {
        panic!("should read the backlog");
    };
    let ids: Vec<&str> = backlog.iter().map(|q| q.run_id.as_str()).collect();
    assert_eq!(ids, vec!["stranded"]);
    let Ok(again) = store.claim(&["stranded".to_owned()]).await else {
        panic!("should claim");
    };
    assert_eq!(again, vec!["stranded".to_owned()]);
}
