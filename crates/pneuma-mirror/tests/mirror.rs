//! Drives the mirror against a real Postgres.
//!
//! The three things worth checking here are all properties of the database
//! rather than of this code: that a fan-out's rows satisfy the self-referential
//! foreign key, that the statements' guards refuse what they are meant to
//! refuse, and that writing the same run twice is a no-op rather than a
//! violation. A fake would agree with whatever this file believed about all
//! three.
//!
//! ```sh
//! docker run -d --name pn-pg -p 5433:5432 -e POSTGRES_PASSWORD=pneuma postgres:16
//! PNEUMA_TEST_DATABASE_URL=postgres://postgres:pneuma@127.0.0.1:5433/postgres \
//!     cargo test -p pneuma-mirror
//! ```

use pneuma_core::child_index::ChildIndex;
use pneuma_core::ids::{NodeId, PipelineId, RunId};
use pneuma_core::node::NodeKind;
use pneuma_core::status::NodeStatus;
use pneuma_mirror::Mirror;
use pneuma_runner::{Failure, NewStep, Record, Recorder};
use pneuma_store::NodeRunStore;
use sqlx::{Executor, PgPool};

/// A schema of this test's own, migrated.
async fn fresh(name: &str) -> PgPool {
    let Ok(url) = std::env::var("PNEUMA_TEST_DATABASE_URL") else {
        panic!("PNEUMA_TEST_DATABASE_URL is not set; see the header of this file");
    };
    let schema = format!("pneuma_mirror_{name}");
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

    // The search path in the DSN, not a `SET` on one connection: a pool that
    // reopens the connection it was `SET` on reads `public` instead, and the
    // failure then blames the code under test.
    let separator = if url.contains('?') { '&' } else { '?' };
    let dsn = format!("{url}{separator}options=-c%20search_path%3D{schema}");
    let Ok(pool) = sqlx::postgres::PgPoolOptions::new()
        .max_connections(2)
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
    pool
}

/// A step, with everything a caller usually leaves alone.
fn step(path: &str, node: &str, kind: NodeKind) -> NewStep {
    NewStep {
        path: path.to_owned(),
        node_id: NodeId::new(node),
        name: format!("component.{node}"),
        kind,
        pipeline_id: PipelineId::new("invoice.page.default"),
        run_id: RunId::new("run-1"),
        parent_id: None,
        parent_path: None,
        parent_kind: None,
        child_index: None,
        sibling_index: None,
        step_input: Some(serde_json::json!({"doc": "d"})),
    }
}

/// A step inside a fan-out.
fn child(path: &str, node: &str, parent: &str, index: u32) -> NewStep {
    let Ok(child_index) = ChildIndex::new(index) else {
        panic!("{index} is a valid 1-based index");
    };
    NewStep {
        parent_id: Some(NodeId::new("Agg")),
        parent_path: Some(parent.to_owned()),
        parent_kind: Some("ListAggregator".to_owned()),
        child_index: Some(child_index),
        ..step(path, node, NodeKind::Model)
    }
}

#[tokio::test]
async fn a_run_is_written_and_can_be_read_back() {
    let pool = fresh("basic").await;
    let mirror = Mirror::new(pool.clone(), "test");
    let store = NodeRunStore::new(pool);

    let path = "run-1.invoice.page.default.A";
    mirror
        .record(Record::Created(Box::new(step(path, "A", NodeKind::Model))))
        .await;
    let Ok(Some(created)) = store.get_by_path(path).await else {
        panic!("the row exists");
    };
    assert_eq!(created.status, NodeStatus::Created);
    assert_eq!(created.node_id.as_str(), "A");
    assert_eq!(created.step_input, Some(serde_json::json!({"doc": "d"})));

    mirror
        .record(Record::Moved {
            path: path.to_owned(),
            status: NodeStatus::Processing,
            failure: None,
        })
        .await;
    let Ok(Some(processing)) = store.get_by_path(path).await else {
        panic!("still there");
    };
    assert_eq!(processing.status, NodeStatus::Processing);
    // `started_at` is stamped by the statement's `CASE`, not by this crate.
    assert!(processing.started_at.is_some());

    mirror
        .record(Record::Produced {
            path: path.to_owned(),
            status: NodeStatus::Finished,
            output: serde_json::json!({"pages": 3}),
        })
        .await;
    let Ok(Some(finished)) = store.get_by_path(path).await else {
        panic!("still there");
    };
    assert_eq!(finished.status, NodeStatus::Finished);
    assert_eq!(finished.step_output, Some(serde_json::json!({"pages": 3})));
}

#[tokio::test]
async fn a_fan_out_satisfies_the_foreign_key_when_written_in_order() {
    // `node_run.parent_path` is a non-deferrable foreign key onto `path`, so a
    // branch's row cannot be written before its aggregator's. `Happening`
    // guarantees the order; this is the half that proves the database agrees.
    let pool = fresh("fanout").await;
    let mirror = Mirror::new(pool.clone(), "test");
    let store = NodeRunStore::new(pool);

    let agg = "run-1.invoice.page.default.Agg";
    mirror
        .record(Record::Created(Box::new(step(
            agg,
            "Agg",
            NodeKind::ListAggregator,
        ))))
        .await;
    for index in 1..=3 {
        let path = format!("{agg}:{index}.Inner");
        mirror
            .record(Record::Created(Box::new(child(&path, "Inner", agg, index))))
            .await;
    }

    // Three distinct rows, not one — which is what the unique `path` would
    // have silently reduced them to without the branch index.
    let Ok(children) = store.get_by_parent_path(agg, None, None).await else {
        panic!("should read the children");
    };
    assert_eq!(children.len(), 3, "{children:?}");
    let indices: Vec<u32> = children
        .iter()
        .filter_map(|row| row.child_index.map(ChildIndex::get))
        .collect();
    assert_eq!(indices, vec![1, 2, 3], "ordered by child_index");
    for row in &children {
        assert_eq!(row.parent_path.as_deref(), Some(agg));
        assert_eq!(row.parent_kind.as_deref(), Some("ListAggregator"));
    }
}

#[tokio::test]
async fn a_child_written_before_its_parent_is_a_failure_and_not_a_panic() {
    // The ordering is guaranteed upstream, so this cannot happen through
    // `drive` — but the mirror is a public type and a caller could. It has to
    // be a logged failure rather than an unwrap, because the whole contract is
    // that recording cannot fail a run.
    let pool = fresh("orphan").await;
    let mirror = Mirror::new(pool.clone(), "test");
    let store = NodeRunStore::new(pool);

    let orphan = "run-1.invoice.page.default.Missing:1.Inner";
    mirror
        .record(Record::Created(Box::new(child(
            orphan,
            "Inner",
            "run-1.invoice.page.default.Missing",
            1,
        ))))
        .await;
    let Ok(row) = store.get_by_path(orphan).await else {
        panic!("the query itself must work");
    };
    assert!(row.is_none(), "the foreign key refused it: {row:?}");
}

#[tokio::test]
async fn writing_the_same_step_twice_is_a_no_op() {
    // A Restate replay re-runs the handler, and every write it makes is one it
    // has made before. The row's id is derived from its path for this reason:
    // a random one would make the second attempt a different row, and
    // `ON CONFLICT (path)` would hide the duplicate rather than prevent it.
    let pool = fresh("replay").await;
    let mirror = Mirror::new(pool.clone(), "test");
    let store = NodeRunStore::new(pool);

    let path = "run-1.invoice.page.default.A";
    let first = step(path, "A", NodeKind::Model);
    assert_eq!(
        Mirror::row(&first).id,
        Mirror::row(&first).id,
        "the id is a function of the step, not of when it was built"
    );

    for _ in 0..3 {
        mirror
            .record(Record::Created(Box::new(step(path, "A", NodeKind::Model))))
            .await;
    }
    let Ok(rows) = store.get_by_run_id("run-1").await else {
        panic!("should read");
    };
    assert_eq!(rows.len(), 1, "one row, however many attempts: {rows:?}");
}

#[tokio::test]
async fn a_terminal_row_does_not_move_and_the_mirror_does_not_mind() {
    // The design notes: every statement that moves a row refuses a terminal
    // one, `CANCELLED` included. The mirror emits moves unconditionally — an
    // ancestor is told twice that a child started — so the refusal is the
    // ordinary case rather than an error, and it must not be logged as one.
    let pool = fresh("terminal").await;
    let mirror = Mirror::new(pool.clone(), "test");
    let store = NodeRunStore::new(pool);

    let path = "run-1.invoice.page.default.A";
    mirror
        .record(Record::Created(Box::new(step(path, "A", NodeKind::Model))))
        .await;
    mirror
        .record(Record::Produced {
            path: path.to_owned(),
            status: NodeStatus::Finished,
            output: serde_json::json!({"done": true}),
        })
        .await;
    // Now try to move it, twice, the way an ancestor would be told.
    for status in [NodeStatus::Processing, NodeStatus::Forked] {
        mirror
            .record(Record::Moved {
                path: path.to_owned(),
                status,
                failure: None,
            })
            .await;
    }
    let Ok(Some(row)) = store.get_by_path(path).await else {
        panic!("still there");
    };
    assert_eq!(row.status, NodeStatus::Finished, "a terminal row stays put");
}

#[tokio::test]
async fn a_failure_records_its_code_and_message() {
    let pool = fresh("failure").await;
    let mirror = Mirror::new(pool.clone(), "test");
    let store = NodeRunStore::new(pool);

    let path = "run-1.invoice.page.default.A";
    mirror
        .record(Record::Created(Box::new(step(path, "A", NodeKind::Model))))
        .await;
    mirror
        .record(Record::Moved {
            path: path.to_owned(),
            status: NodeStatus::Error,
            failure: Some(Failure {
                code: "PNEUMA_TRANSPORT_ERROR".to_owned(),
                message: "connection reset".to_owned(),
            }),
        })
        .await;

    let Ok(Some(row)) = store.get_by_path(path).await else {
        panic!("still there");
    };
    assert_eq!(row.status, NodeStatus::Error);
    assert_eq!(row.error_code.as_deref(), Some("PNEUMA_TRANSPORT_ERROR"));
    assert_eq!(row.error_message.as_deref(), Some("connection reset"));
}

#[tokio::test]
async fn a_database_that_is_not_there_costs_the_mirror_and_not_the_run() {
    // The contract, exercised: `record` returns `()` whatever happens, so a
    // store that cannot be reached is a log line. Losing a completed model call
    // because an audit table was down is the outcome that shape prevents.
    let Ok(pool) = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .acquire_timeout(std::time::Duration::from_millis(300))
        .connect_lazy("postgres://nobody:nothing@127.0.0.1:1/nowhere")
    else {
        panic!("a lazy pool does not connect yet");
    };
    let mirror = Mirror::new(pool, "test");
    let path = "run-1.invoice.page.default.A";

    // All three arms, against nothing at all.
    mirror
        .record(Record::Created(Box::new(step(path, "A", NodeKind::Model))))
        .await;
    mirror
        .record(Record::Moved {
            path: path.to_owned(),
            status: NodeStatus::Processing,
            failure: None,
        })
        .await;
    mirror
        .record(Record::Produced {
            path: path.to_owned(),
            status: NodeStatus::Finished,
            output: serde_json::json!({}),
        })
        .await;
    // Reaching here is the assertion.
}

#[tokio::test]
async fn a_mirror_says_whether_it_can_mirror_at_all() {
    // What a caller asks before it starts serving. A DSN that opens is not a
    // database that can be mirrored: an unmigrated one -- or one whose
    // `search_path` misses the schema the migrations ran in, which is the
    // likelier mistake -- accepts the connection and refuses every statement,
    // so a replica reports ready and then logs one line per step for ever.
    let pool = fresh("ready").await;
    // Through `ready_within`, because that is what a caller's startup calls:
    // the deadline must not turn a healthy database into a refusal.
    let ready = Mirror::new(pool, "test")
        .ready_within(std::time::Duration::from_secs(5))
        .await;
    if let Err(error) = ready {
        panic!("a migrated schema is ready: {error}");
    }

    let Ok(nowhere) = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .acquire_timeout(std::time::Duration::from_millis(300))
        .connect_lazy("postgres://nobody:nothing@127.0.0.1:1/nowhere")
    else {
        panic!("a lazy pool does not connect yet");
    };
    let Err(error) = Mirror::new(nowhere, "test").ready().await else {
        panic!("nothing is listening on port 1");
    };
    // `Unusable`, not `NoTable`: a database that cannot be reached is not a
    // database that needs migrating, and reporting it as one sends an operator
    // to re-run migrations that already applied.
    assert!(
        matches!(error, pneuma_mirror::NotReady::Unusable(_)),
        "wrong variant: {error:?}"
    );
    // A string inside rather than the `sqlx::Error`, so a DSN cannot reach a
    // log through a `Debug` -- the design notes
    assert!(
        !error.to_string().contains("nothing"),
        "the password reached the message: {error}"
    );
}

#[tokio::test]
async fn an_unmigrated_database_is_told_apart_from_an_unreachable_one() {
    // The distinction the two variants exist for. A role that may connect but
    // not read `node_run` is a common least-privilege setup, and reporting it
    // as a missing migration sends an operator to re-run migrations that
    // already applied -- so the SQLSTATE decides, not "the probe failed".
    let Ok(url) = std::env::var("PNEUMA_TEST_DATABASE_URL") else {
        panic!("PNEUMA_TEST_DATABASE_URL is not set; see the header of this file");
    };
    let schema = "pneuma_mirror_bare";
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
    let Ok(pool) = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(&dsn)
        .await
    else {
        panic!("could not connect to {dsn}");
    };
    let Err(error) = Mirror::new(pool.clone(), "test").ready().await else {
        panic!("there is no node_run in that schema");
    };
    assert!(
        matches!(error, pneuma_mirror::NotReady::NoTable(_)),
        "wrong variant: {error:?}"
    );

    // A `node_run` that is not *this* `node_run`: the table is there, so the
    // migrations are not the answer, and telling an operator to run them would
    // send them somewhere the problem is not.
    if let Err(error) = pool.execute("CREATE TABLE node_run (x integer)").await {
        panic!("setup failed: {error}");
    }
    let Err(error) = Mirror::new(pool, "test").ready().await else {
        panic!("that table has none of the columns the statement names");
    };
    assert!(
        matches!(error, pneuma_mirror::NotReady::Unusable(_)),
        "wrong variant: {error:?}"
    );
}

#[tokio::test]
async fn a_probe_that_does_not_answer_in_time_is_unusable_rather_than_a_hang() {
    // `ready_within` is what a caller's startup depends on, and the failure it
    // exists for is not a refusal: a database that completes the handshake and
    // then stalls -- a catalog lock held by a concurrent `ALTER`, a hung
    // standby -- would block a boot with nothing bound and no line logged.
    let Ok(pool) = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .acquire_timeout(std::time::Duration::from_secs(30))
        .connect_lazy("postgres://nobody:nothing@127.0.0.1:1/nowhere")
    else {
        panic!("a lazy pool does not connect yet");
    };
    let mirror = Mirror::new(pool, "test");
    let Err(error) = mirror
        .ready_within(std::time::Duration::from_millis(50))
        .await
    else {
        panic!("nothing is listening on port 1");
    };
    assert!(
        matches!(error, pneuma_mirror::NotReady::Unusable(_)),
        "wrong variant: {error:?}"
    );
    assert!(error.to_string().contains("50ms"), "{error}");
}

/// A component that accepts a step and never answers it.
///
/// The failure the janitor exists for: not a run that fails, a run that stops.
struct Silent;

#[derive(Debug, thiserror::Error)]
#[error("never")]
struct Never;

impl pneuma_runner::Component for Silent {
    type Error = Never;

    async fn call(&self, _dispatch: pneuma_runner::Dispatch) -> Result<serde_json::Value, Never> {
        std::future::pending().await
    }
}

#[tokio::test]
async fn a_run_that_stopped_mid_step_is_what_the_janitor_finds() {
    // The claim this closes. `pneuma-janitor`'s `stale_inprogress_run_ids` has
    // been in the repository since `node_run` was, and every test of it until
    // now seeded the rows by hand -- so what it was verified against was a
    // fixture's idea of a stuck row rather than one this port would produce.
    // Nothing wrote `node_run` at all, so in production the query had nothing
    // to find and `PNEUMA_STALE_TERMINATION_ENABLED=true` would have reported
    // `0 stale` for ever.
    //
    // Here the rows come from `drive_recording` through the real mirror, and
    // the run stops the way a replica dying mid-step stops it: the drive future
    // is dropped while a component call is outstanding, leaving the row at
    // `PROCESSING` with nothing that will ever move it.
    let pool = fresh("stale").await;
    let mirror = Mirror::new(pool.clone(), "test");
    let store = NodeRunStore::new(pool.clone());

    let yaml = "pipeline_id: p\nstart: A\ncomponents:\n\
                \x20 - node_id: A\n    name: comp-a\n    type: Model\n    children: [end]\n";
    let Ok(definition) = serde_yaml::from_str(yaml) else {
        panic!("the fixture parses");
    };
    let Ok(registry) = pneuma_core::resolver::resolve(&definition) else {
        panic!("and resolves");
    };
    let meta = serde_json::json!({
        "job_id": "run-stuck",
        "tenant_id": "acme",
        "pipeline_type": "invoice",
        "pipeline_level": "page",
        "pipeline_name": "default",
    });
    let Ok(meta) = serde_json::from_value::<pneuma_proto::meta::Meta>(meta) else {
        panic!("the envelope parses");
    };
    let run = pneuma_runner::RunInput {
        meta,
        input: serde_json::json!({"doc": "d"}),
        custom_data: None,
    };

    let driving = pneuma_runner::drive_recording(&registry, &run, &Silent, &mirror);
    let stopped = tokio::time::timeout(std::time::Duration::from_millis(300), driving).await;
    assert!(stopped.is_err(), "the component never answers");

    let path = "run-stuck.invoice.page.default.A";
    let Ok(Some(row)) = store.get_by_path(path).await else {
        panic!("the dispatched step has a row");
    };
    assert_eq!(
        row.status,
        NodeStatus::Processing,
        "and nothing will ever move it: {row:?}"
    );

    // Aged rather than waited for. The threshold is a deployment property and
    // `stale_inprogress_run_ids` compares `updated_at` against a caller-supplied
    // instant, so moving the instant is the same test as moving the clock.
    let Ok(stale) = store
        .stale_inprogress_run_ids(chrono::Utc::now() + chrono::Duration::seconds(1), 10)
        .await
    else {
        panic!("the query itself must work");
    };
    assert_eq!(
        stale,
        vec!["run-stuck".to_owned()],
        "the janitor sees the run this port stopped driving"
    );
}
