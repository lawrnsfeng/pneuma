//! What each queue's messages mean, against fakes for both stores.
//!
//! The distinction worth the most attention is the three-way one: a body that
//! will never parse must be dead-lettered, a database that did not answer must
//! not be. The original conflates them in the direction that loses work --
//! `handle_dead_message` sets `should_requeue = False` for a
//! `DatabaseOperationalError`, so
//! a Mongo blip dead-letters the run and a person has to find and replay it.

use std::sync::Mutex;

use async_trait::async_trait;
use chrono::{TimeZone, Utc};
use mongodb::bson::{doc, Document};
use pneuma_intake::{
    handle_definition, handle_event, handle_run, Admits, Events, Outcome, Pipelines, Runs,
};
use pneuma_store::Created;
use serde_json::{json, Value};

const PIPELINE: &str = include_str!("fixtures/pipeline1.yaml");

/// The corpus fixture, as a stored definition with the `_id` Mongo gives it.
fn stored() -> Document {
    let Ok(value) = serde_yaml::from_str::<Value>(PIPELINE) else {
        panic!("the corpus fixture parses");
    };
    let Ok(mut document) = mongodb::bson::serialize_to_document(&value) else {
        panic!("and converts to BSON");
    };
    document.insert("_id", mongodb::bson::oid::ObjectId::new());
    document
}

/// The pipeline id the fixture actually declares, so the message can name it.
fn stored_pipeline_id() -> String {
    match stored().get_str("pipeline_id") {
        Ok(id) => id.to_owned(),
        Err(error) => panic!("the fixture has a pipeline_id: {error}"),
    }
}

/// A run message naming `pipeline_id`, split into the three parts `Meta`
/// derives it from.
fn run_message(pipeline_id: &str) -> Vec<u8> {
    let mut parts = pipeline_id.splitn(3, '.');
    let (Some(kind), Some(level), Some(name)) = (parts.next(), parts.next(), parts.next()) else {
        panic!("a pipeline id is three dotted parts, got {pipeline_id:?}");
    };
    let body = json!({
        "meta": {
            "job_id": "job-1", "tenant_id": "acme",
            "pipeline_type": kind, "pipeline_level": level, "pipeline_name": name,
        },
        "step_input": {"doc": "d"},
    });
    match serde_json::to_vec(&body) {
        Ok(bytes) => bytes,
        Err(error) => panic!("that serialises: {error}"),
    }
}

/// Stores that answer from a script and record what they were given.
#[derive(Default)]
struct Fake {
    definition: Option<Document>,
    fails: bool,
    written: Mutex<Vec<Document>>,
    submitted: Mutex<Vec<(String, Value)>>,
    forwarded: Mutex<Vec<Value>>,
}

impl Fake {
    fn holding(definition: Document) -> Self {
        Fake {
            definition: Some(definition),
            ..Fake::default()
        }
    }

    fn broken() -> Self {
        Fake {
            fails: true,
            ..Fake::default()
        }
    }

    fn taken<T: Clone>(slot: &Mutex<Vec<T>>) -> Vec<T> {
        match slot.lock() {
            Ok(seen) => seen.clone(),
            Err(poisoned) => poisoned.into_inner().clone(),
        }
    }

    fn push<T>(slot: &Mutex<Vec<T>>, value: T) {
        match slot.lock() {
            Ok(mut seen) => seen.push(value),
            Err(poisoned) => poisoned.into_inner().push(value),
        }
    }
}

#[async_trait]
impl Pipelines for Fake {
    async fn by_pipeline_id(&self, _pipeline_id: &str) -> Result<Option<Document>, String> {
        if self.fails {
            return Err("connection refused".to_owned());
        }
        Ok(self.definition.clone())
    }
    async fn create(&self, document: Document) -> Result<Created, String> {
        if self.fails {
            return Err("connection refused".to_owned());
        }
        Fake::push(&self.written, document);
        Ok(Created::Inserted)
    }
}

#[async_trait]
impl Runs for Fake {
    async fn create(&self, document: Document) -> Result<Created, String> {
        if self.fails {
            return Err("connection refused".to_owned());
        }
        Fake::push(&self.written, document);
        Ok(Created::Inserted)
    }
}

#[async_trait]
impl Admits for Fake {
    async fn submit(&self, run_id: &str, body: &Value) -> Result<(), String> {
        if self.fails {
            return Err("connection refused".to_owned());
        }
        Fake::push(&self.submitted, (run_id.to_owned(), body.clone()));
        Ok(())
    }
}

#[async_trait]
impl Events for Fake {
    async fn forward(&self, event: &Value) -> Result<(), String> {
        if self.fails {
            return Err("connection refused".to_owned());
        }
        Fake::push(&self.forwarded, event.clone());
        Ok(())
    }
}

fn instant() -> chrono::DateTime<Utc> {
    let Some(at) = Utc.with_ymd_and_hms(2026, 3, 1, 12, 0, 0).single() else {
        panic!("a real instant");
    };
    at
}

#[tokio::test]
async fn a_run_is_written_before_it_is_announced() {
    // The order is the durability. A submission announced before the run
    // document exists is a run the rest of the system can be asked about and
    // has nothing for; a run written and not announced comes back, because the
    // message was never acknowledged.
    let pipelines = Fake::holding(stored());
    let runs = Fake::default();
    let admits = Fake::default();

    let outcome = handle_run(
        &run_message(&stored_pipeline_id()),
        &pipelines,
        &runs,
        &admits,
        instant(),
    )
    .await;
    assert_eq!(outcome, Outcome::Handled);

    let written = Fake::taken(&runs.written);
    assert_eq!(written.len(), 1, "one run document");
    assert_eq!(written[0].get_str("run_id").ok(), Some("job-1"));
    assert!(!written[0].contains_key("_id"), "not the pipeline's key");

    let submitted = Fake::taken(&admits.submitted);
    assert_eq!(submitted.len(), 1);
    assert_eq!(submitted[0].0, "job-1");
    assert!(
        submitted[0].1.get("pipeline").is_some(),
        "the definition travels with the submission: {:?}",
        submitted[0].1
    );
}

#[tokio::test]
async fn a_body_that_will_never_parse_is_dead_lettered() {
    let fake = Fake::holding(stored());
    for body in [&b"not json"[..], b"{}", b"{\"meta\": 3}"] {
        let outcome = handle_run(body, &fake, &fake, &fake, instant()).await;
        assert!(
            matches!(outcome, Outcome::Rejected(_)),
            "{body:?} gave {outcome:?}"
        );
    }
}

#[tokio::test]
async fn a_pipeline_that_is_not_there_is_dead_lettered_too() {
    // A statement about the submission, not about the database. It will not
    // become true because the message came back.
    let empty = Fake::default();
    let outcome = handle_run(
        &run_message("no.such.pipeline"),
        &empty,
        &empty,
        &empty,
        instant(),
    )
    .await;
    let Outcome::Rejected(why) = outcome else {
        panic!("an unknown pipeline is permanent: {outcome:?}");
    };
    assert!(why.contains("no.such.pipeline"), "{why}");
}

#[tokio::test]
async fn a_database_that_did_not_answer_is_retried_rather_than_dead_lettered() {
    // The original dead-letters this: `should_requeue = False` for a
    // `DatabaseOperationalError`. A Mongo blip should not cost a run a person
    // has to find and replay.
    let broken = Fake::broken();
    let outcome = handle_run(
        &run_message(&stored_pipeline_id()),
        &broken,
        &broken,
        &broken,
        instant(),
    )
    .await;
    assert!(matches!(outcome, Outcome::Retry(_)), "{outcome:?}");

    // And each later step fails the same way rather than differently.
    let pipelines = Fake::holding(stored());
    let outcome = handle_run(
        &run_message(&stored_pipeline_id()),
        &pipelines,
        &Fake::broken(),
        &Fake::default(),
        instant(),
    )
    .await;
    assert!(
        matches!(outcome, Outcome::Retry(_)),
        "the write: {outcome:?}"
    );

    let outcome = handle_run(
        &run_message(&stored_pipeline_id()),
        &pipelines,
        &Fake::default(),
        &Fake::broken(),
        instant(),
    )
    .await;
    assert!(
        matches!(outcome, Outcome::Retry(_)),
        "the announcement: {outcome:?}"
    );
}

#[tokio::test]
async fn a_stored_definition_that_cannot_resolve_is_rejected_at_the_run() {
    // The definition parses -- so nothing refused it on the way in -- and names
    // a start node that does not exist. Every run of it fails, and each failure
    // is permanent, so retrying is how one bad definition becomes a loop.
    let Ok(document) = mongodb::bson::serialize_to_document(&json!({
        "pipeline_id": "a.b.c",
        "start": {"B": "B"},
        "components": [
            {"node_id": "A", "name": "comp-a", "type": "Model", "children": ["end"]}
        ],
    })) else {
        panic!("that converts to BSON");
    };
    let fake = Fake::holding(document);
    let outcome = handle_run(&run_message("a.b.c"), &fake, &fake, &fake, instant()).await;
    let Outcome::Rejected(why) = outcome else {
        panic!("an unresolvable definition is permanent: {outcome:?}");
    };
    assert!(why.contains("does not resolve"), "{why}");
}

#[tokio::test]
async fn an_input_bson_cannot_hold_is_rejected_rather_than_written_wrong() {
    // Reachable from real wire data: `serde_json` holds integers to `u64::MAX`
    // and BSON's largest is an `i64`. Truncating it or dropping the key would
    // both be found by somebody reading the run months later.
    let pipelines = Fake::holding(stored());
    let pipeline_id = stored_pipeline_id();
    let mut parts = pipeline_id.splitn(3, '.');
    let (Some(kind), Some(level), Some(name)) = (parts.next(), parts.next(), parts.next()) else {
        panic!("a pipeline id is three dotted parts");
    };
    let body = json!({
        "meta": {
            "job_id": "job-1", "tenant_id": "acme",
            "pipeline_type": kind, "pipeline_level": level, "pipeline_name": name,
        },
        "step_input": {"pages": u64::MAX},
    });
    let Ok(bytes) = serde_json::to_vec(&body) else {
        panic!("that serialises");
    };
    let outcome = handle_run(
        &bytes,
        &pipelines,
        &Fake::default(),
        &Fake::default(),
        instant(),
    )
    .await;
    let Outcome::Rejected(why) = outcome else {
        panic!("that number does not fit in BSON: {outcome:?}");
    };
    assert!(why.contains("will not encode"), "{why}");
}

#[tokio::test]
async fn a_stored_definition_that_does_not_parse_is_rejected_loudly() {
    // It means something wrote a definition no run can ever use, and no number
    // of redeliveries changes that.
    let nonsense = Fake::holding(doc! { "pipeline_id": "a.b.c", "components": "not a list" });
    let outcome = handle_run(
        &run_message("a.b.c"),
        &nonsense,
        &nonsense,
        &nonsense,
        instant(),
    )
    .await;
    assert!(matches!(outcome, Outcome::Rejected(_)), "{outcome:?}");
}

#[tokio::test]
async fn a_definition_is_resolved_before_it_is_stored() {
    // The original validates the model and keeps whatever passes. A definition
    // that cannot resolve fails *every* run that names it, each time producing
    // the same error further down than here -- so one resolution at the door
    // turns a recurring runtime failure into a single rejected message.
    let store = Fake::default();
    let Ok(good) = serde_yaml::from_str::<Value>(PIPELINE) else {
        panic!("the corpus fixture parses");
    };
    let Ok(body) = serde_json::to_vec(&good) else {
        panic!("and serialises");
    };
    assert_eq!(handle_definition(&body, &store).await, Outcome::Handled);
    assert_eq!(Fake::taken(&store.written).len(), 1);

    // A definition that parses and does not resolve: its start names a node
    // that is not among its components.
    let unresolvable = json!({
        "pipeline_id": "a.b.c",
        "start": {"B": "B"},
        "components": [
            {"node_id": "A", "name": "comp-a", "type": "Model", "children": ["end"]}
        ],
    });
    let Ok(body) = serde_json::to_vec(&unresolvable) else {
        panic!("that serialises");
    };
    let outcome = handle_definition(&body, &store).await;
    assert!(matches!(outcome, Outcome::Rejected(_)), "{outcome:?}");
    assert_eq!(
        Fake::taken(&store.written).len(),
        1,
        "and nothing more was stored"
    );

    for body in [&b"not json"[..], b"[1,2,3]", b"{\"pipeline_id\": 4}"] {
        let outcome = handle_definition(body, &store).await;
        assert!(
            matches!(outcome, Outcome::Rejected(_)),
            "{body:?} gave {outcome:?}"
        );
    }

    let outcome = handle_definition(
        &serde_json::to_vec(&good).unwrap_or_default(),
        &Fake::broken(),
    )
    .await;
    assert!(matches!(outcome, Outcome::Retry(_)), "{outcome:?}");
}

#[tokio::test]
async fn an_event_is_validated_then_forwarded_unchanged() {
    // The validation is what stops a malformed event being relayed to every
    // downstream consumer to fail there instead.
    let events = Fake::default();
    let body = json!({
        "event": "finished",
        "meta": {
            "job_id": "job-1", "tenant_id": "acme",
            "pipeline_type": "invoice", "pipeline_level": "page", "pipeline_name": "default",
        },
    });
    let Ok(bytes) = serde_json::to_vec(&body) else {
        panic!("that serialises");
    };
    assert_eq!(handle_event(&bytes, &events).await, Outcome::Handled);
    let forwarded = Fake::taken(&events.forwarded);
    assert_eq!(forwarded.len(), 1);
    assert_eq!(
        forwarded[0].get("event").and_then(Value::as_str),
        Some("finished")
    );

    for bad in [&b"not json"[..], b"{}"] {
        let outcome = handle_event(bad, &events).await;
        assert!(
            matches!(outcome, Outcome::Rejected(_)),
            "{bad:?} gave {outcome:?}"
        );
    }

    let outcome = handle_event(&bytes, &Fake::broken()).await;
    assert!(matches!(outcome, Outcome::Retry(_)), "{outcome:?}");
}
