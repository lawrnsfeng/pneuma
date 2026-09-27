//! What each of the three queues means, decided without a broker.
//!
//! The original has one handler per queue; here the work is one
//! function per queue over three small traits, so every arm — a body that does
//! not parse, a pipeline that is not there, a database that is not answering —
//! is a unit test against a fake rather than a broker outage somebody has to
//! arrange.
//!
//! # The three answers, and why they are not two
//!
//! [`Outcome`] separates "wrong, and wrong next time" from "not now". The
//! original conflates them in the direction that loses work: `handle_dead_message`
//! sets `should_requeue = False` for a `DatabaseOperationalError`,
//! so a Mongo blip
//! dead-letters the run rather than retrying it — and a dead-lettered
//! submission is one a person has to find and replay. Here a database that did
//! not answer is [`Outcome::Retry`], and the redelivery limit on the queue is
//! what stops that from being unbounded.
//!
//! # The order inside a run, and why it is that order
//!
//! Look up, resolve, **write**, then announce. A submission announced before
//! the run document exists is a run the rest of the system can be asked about
//! and has nothing for; a run document written and not announced is
//! recoverable, because the message was never acknowledged and comes back. On
//! that second delivery the insert finds the document already there, which is
//! [`pneuma_store::Created::AlreadyExists`] and not a failure — and the run id
//! used from then on is the one this function already holds, never one read
//! back off an insert result. The defect notes names that trap: the
//! original fix written literally reads `result.model.run_id` from a `result`
//! that is unbound on exactly the redelivery path the fix exists to serve.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use mongodb::bson::Document;
use pneuma_core::node::Pipeline;
use pneuma_core::resolver::resolve;
use pneuma_proto::envelope::Message;
use pneuma_proto::event::MessageEvent;
use pneuma_store::Created;
use serde_json::Value;

use crate::document::{run_document, ID};

/// What a handler decided.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    /// Done. The message can be acknowledged.
    Handled,
    /// Wrong, and wrong the next time too. Dead-letter it.
    ///
    /// A body that does not parse does not parse on the third attempt, and a
    /// pipeline that does not exist will not exist because the message came
    /// back. Retrying either is how a bad message becomes a loop.
    Rejected(String),
    /// Not now. Return it to the queue.
    Retry(String),
}

/// The pipeline definitions, as these handlers need them.
#[async_trait]
pub trait Pipelines: Send + Sync {
    /// The definition with this id, if there is one.
    async fn by_pipeline_id(&self, pipeline_id: &str) -> Result<Option<Document>, String>;
    /// Stores a definition; an existing one is success.
    async fn create(&self, document: Document) -> Result<Created, String>;
}

/// The runs collection, as these handlers need it.
#[async_trait]
pub trait Runs: Send + Sync {
    /// Writes a run; an existing one is success.
    async fn create(&self, document: Document) -> Result<Created, String>;
}

/// Where an admitted run is announced.
#[async_trait]
pub trait Admits: Send + Sync {
    /// Offers a run to the admission service.
    async fn submit(&self, run_id: &str, body: &Value) -> Result<(), String>;
}

/// Where a lifecycle event is forwarded.
#[async_trait]
pub trait Events: Send + Sync {
    /// Passes an event on, unchanged.
    async fn forward(&self, event: &Value) -> Result<(), String>;
}

/// Handles one message from the input queue.
pub async fn handle_run<P, R, A>(
    body: &[u8],
    pipelines: &P,
    runs: &R,
    admits: &A,
    now: DateTime<Utc>,
) -> Outcome
where
    P: Pipelines + ?Sized,
    R: Runs + ?Sized,
    A: Admits + ?Sized,
{
    let message: Message = match serde_json::from_slice(body) {
        Ok(message) => message,
        Err(error) => return Outcome::Rejected(format!("not a run message: {error}")),
    };
    let run_id = message.meta.job_id.as_str().to_owned();
    let pipeline_id = message.meta.pipeline_id();

    let stored = match pipelines.by_pipeline_id(pipeline_id.as_str()).await {
        Err(error) => return Outcome::Retry(format!("could not read pipelines: {error}")),
        // A statement about the submission, not about the database. It will not
        // become true because the message came back.
        Ok(None) => return Outcome::Rejected(format!("no pipeline {pipeline_id}")),
        Ok(Some(stored)) => stored,
    };

    let definition = match definition(stored.clone()) {
        Ok(definition) => definition,
        Err(why) => return Outcome::Rejected(why),
    };
    let registry = match resolve(&definition) {
        Ok(registry) => registry,
        // Resolution is pure and deterministic, so a definition that does not
        // resolve will not resolve later either. Rejecting here is also what
        // keeps a broken definition from consuming a share of somebody's fair
        // dispatch quota to produce the same error further down.
        Err(error) => return Outcome::Rejected(format!("{pipeline_id} does not resolve: {error}")),
    };

    let document = match run_document(stored, &registry, &run_id, &message, now) {
        Ok(document) => document,
        Err(error) => {
            return Outcome::Rejected(format!("the run document will not encode: {error}"))
        }
    };
    // Written before it is announced, and a conflict is not a failure. See the
    // module docs.
    if let Err(error) = runs.create(document).await {
        return Outcome::Retry(format!("could not write the run: {error}"));
    }

    let submission = submission(&definition, &message);
    match admits.submit(&run_id, &submission).await {
        Ok(()) => Outcome::Handled,
        // The run document is already written, so a redelivery re-announces it
        // rather than starting again -- which is exactly what should happen.
        Err(error) => Outcome::Retry(format!("could not submit {run_id}: {error}")),
    }
}

/// Handles one message from the create-pipeline queue.
pub async fn handle_definition<P: Pipelines + ?Sized>(body: &[u8], pipelines: &P) -> Outcome {
    let document: Document = match serde_json::from_slice::<Value>(body) {
        Err(error) => return Outcome::Rejected(format!("not JSON: {error}")),
        Ok(value) => match mongodb::bson::serialize_to_document(&value) {
            Ok(document) => document,
            Err(error) => return Outcome::Rejected(format!("not a definition: {error}")),
        },
    };

    // Parsed *and resolved* before it is stored, which the original does not do
    // -- it validates the model and keeps whatever passes.
    // A definition that cannot resolve
    // is one that fails every run that ever names it, each time producing the
    // same error further down the pipeline than here. Refusing it at the door
    // costs one resolution and turns a recurring runtime failure into a single
    // rejected message.
    let definition = match definition(document.clone()) {
        Ok(definition) => definition,
        Err(why) => return Outcome::Rejected(why),
    };
    // The id is bound first so the refusal fits on one line: an argument on its
    // own line of a multi-line call is not attributed to the call that ran.
    let id = definition.pipeline_id.clone();
    if let Err(error) = resolve(&definition) {
        return Outcome::Rejected(format!("{id} does not resolve: {error}"));
    }

    match pipelines.create(document).await {
        Ok(_) => Outcome::Handled,
        Err(error) => Outcome::Retry(format!("could not store the definition: {error}")),
    }
}

/// Handles one message from the event queue.
///
/// Validated, then forwarded unchanged. The original does the same,
/// and the validation is what stops a
/// malformed event being relayed to every downstream consumer to fail there
/// instead.
pub async fn handle_event<E: Events + ?Sized>(body: &[u8], events: &E) -> Outcome {
    let value: Value = match serde_json::from_slice(body) {
        Ok(value) => value,
        Err(error) => return Outcome::Rejected(format!("not JSON: {error}")),
    };
    // Typed only to check it, then thrown away: what goes on is what came in.
    // The original forwards `model_dump()` of the parsed model, which is a
    // round trip -- and a round trip through a type is where an unmodelled key
    // silently stops existing. Validating without re-encoding gets the check
    // and keeps the bytes' meaning.
    if let Err(error) = serde_json::from_value::<MessageEvent>(value.clone()) {
        return Outcome::Rejected(format!("not an event: {error}"));
    }
    match events.forward(&value).await {
        Ok(()) => Outcome::Handled,
        Err(error) => Outcome::Retry(format!("could not forward the event: {error}")),
    }
}

/// A stored definition, as the typed pipeline the resolver takes.
///
/// `_id` is removed first. It is Mongo's key for the *definition*, it is not
/// part of the pipeline, and `Pipeline::extra` would otherwise carry it into
/// every submission as a `{"$oid": ...}` nobody downstream has a use for.
fn definition(mut stored: Document) -> Result<Pipeline, String> {
    stored.remove(ID);
    match mongodb::bson::deserialize_from_document(stored) {
        Ok(definition) => Ok(definition),
        // A stored definition that will not parse is not going to start
        // parsing, so this is a rejection rather than a retry -- and it is
        // worth saying loudly, because it means something wrote a definition
        // that no run can use.
        Err(error) => Err(format!("the stored definition will not parse: {error}")),
    }
}

/// What `pneuma-admission` is offered.
///
/// The definition travels with the submission rather than as an id, because
/// that is the shape admission's door takes: it resolves the pipeline again
/// before queueing, so a definition naming a step that does not exist is
/// refused before it consumes a share of anybody's dispatch quota.
fn submission(definition: &Pipeline, message: &Message) -> Value {
    serde_json::json!({
        "pipeline": definition,
        "meta": message.meta,
        "input": message.step_input,
        "custom_data": message.custom_data,
    })
}
