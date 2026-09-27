//! The order `stale_inprogress_run_ids` returns runs in, which is a contract.
//!
//! Its own test binary, and so its own Postgres schema: the crate's convention
//! is one test per binary because two binaries creating the same tables in the
//! same database would race under a runner that parallelises test targets.
//!
//! The query has a `LIMIT`, and until this was written it had no `ORDER BY`.
//! That is not a tidiness point. Two things depended on the order being stable:
//!
//! - `pneuma-janitor`'s `preview_pass` and `pass` each execute it once and are
//!   supposed to report the same list. Against more stale runs than the batch
//!   size, an unordered `LIMIT` lets the two disagree, which is exactly what
//!   the week-long production diff before cutover is there to detect and
//!   exactly what it would then be reporting on itself.
//! - A run outside the limit could be passed over for ever while other stale
//!   runs were chosen ahead of it.
//!
//! ```sh
//! PNEUMA_TEST_DATABASE_URL=postgres://postgres:pneuma@127.0.0.1:5433/postgres \
//!     cargo test -p pneuma-store --test stale
//! ```

use chrono::Utc;
use pneuma_store::NodeRunStore;
use sqlx::{Executor, PgPool};

async fn fresh() -> PgPool {
    let Ok(url) = std::env::var("PNEUMA_TEST_DATABASE_URL") else {
        panic!("PNEUMA_TEST_DATABASE_URL is not set; see the header of this file");
    };
    let Ok(pool) = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .after_connect(|conn, _| {
            Box::pin(async move {
                conn.execute("SET search_path TO pneuma_test_stale").await?;
                Ok(())
            })
        })
        .connect(&url)
        .await
    else {
        panic!("could not connect to {url}");
    };
    for statement in [
        "DROP SCHEMA IF EXISTS pneuma_test_stale CASCADE".to_owned(),
        "CREATE SCHEMA pneuma_test_stale".to_owned(),
        "SET search_path TO pneuma_test_stale".to_owned(),
    ] {
        if let Err(error) = pool.execute(statement.as_str()).await {
            panic!("setup failed on {statement:?}: {error}");
        }
    }
    let Ok(mut connection) = pool.acquire().await else {
        panic!("could not acquire a connection to migrate on");
    };
    if let Err(error) = pneuma_store::migrator().run(&mut *connection).await {
        panic!("the migrations do not apply to a real Postgres: {error}");
    }
    drop(connection);
    pool
}

/// One unfinished row for `run_id`, last touched `age_days` ago.
///
/// The path carries the age because `uq_noderun_slug` is unique and a run may
/// need more than one stuck row.
async fn stuck(pool: &PgPool, run_id: &str, age_days: i32) {
    let sql = "INSERT INTO node_run (id, path, node_id, name, kind, pipeline_id, run_id, \
               sibling_index, status, created_at, updated_at) \
               VALUES ($1, $2, 'A', 'a', 'Model', 'p', $3, 1, 'PROCESSING', \
               now() - make_interval(days => $4), now() - make_interval(days => $4))";
    if let Err(error) = sqlx::query(sql)
        .bind(uuid::Uuid::new_v4())
        .bind(format!("{run_id}.A{age_days}"))
        .bind(run_id)
        .bind(age_days)
        .execute(pool)
        .await
    {
        panic!("could not seed {run_id}: {error}");
    }
}

#[tokio::test]
async fn the_stalest_runs_come_first_and_the_same_query_twice_agrees() {
    let pool = fresh().await;
    let store = NodeRunStore::new(pool.clone());

    // Deliberately inserted newest-first, so insertion order is the opposite of
    // the expected answer and cannot be what produces it.
    for (run_id, age) in [("newest", 2), ("middle", 20), ("oldest", 200)] {
        stuck(&pool, run_id, age).await;
    }

    let Ok(all) = store.stale_inprogress_run_ids(Utc::now(), 10).await else {
        panic!("stale query failed");
    };
    assert_eq!(
        all,
        vec![
            "oldest".to_owned(),
            "middle".to_owned(),
            "newest".to_owned()
        ],
        "stalest first: the run stuck longest is the one to terminate first"
    );

    // The property the janitor's dry-run depends on. Under a limit smaller than
    // the number of stale runs, two executions must choose the same runs --
    // `preview_pass` runs this once and `pass` runs it again.
    let Ok(first) = store.stale_inprogress_run_ids(Utc::now(), 2).await else {
        panic!("stale query failed");
    };
    let Ok(second) = store.stale_inprogress_run_ids(Utc::now(), 2).await else {
        panic!("stale query failed");
    };
    assert_eq!(first, second, "the same query twice gives the same answer");
    assert_eq!(
        first,
        vec!["oldest".to_owned(), "middle".to_owned()],
        "and the limit takes the stalest, so nothing is starved by newer work"
    );

    // A run with several stuck rows is still one entry, ranked by its oldest.
    stuck(&pool, "newest", 500).await;
    let Ok(reranked) = store.stale_inprogress_run_ids(Utc::now(), 10).await else {
        panic!("stale query failed");
    };
    assert_eq!(
        reranked,
        vec![
            "newest".to_owned(),
            "oldest".to_owned(),
            "middle".to_owned()
        ],
        "grouping dedupes, and a run is as stale as its stalest row"
    );
}
