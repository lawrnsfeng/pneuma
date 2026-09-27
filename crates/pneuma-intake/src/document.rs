//! Building the run document, without a database.
//!
//! Pure, and the most consequential thing in this crate, because everything
//! downstream reads what it writes: the gateway shows a run's per-node status
//! out of `state`, the janitor finds abandoned runs by `status`, and the
//! archive keeps whatever is here for ever.
//!
//! # `state` is a flat map, not a registry
//!
//! The original writes `state` as `dict[str, Step]` — node id to step —
//! because that is what `RunResolver.step_registry` is,
//! and it is what the gateway
//! indexes into to show a node's status. `pneuma_core::step::StepRegistry`
//! serialises as `{"steps": {...}, "start": ...}`, which is a better shape and
//! the wrong one: written as-is, every consumer looking for `state.<node_id>`
//! finds nothing, and a run's per-node display goes blank with no error
//! anywhere. So the registry is flattened here, once, at the only place that
//! writes it.
//!
//! # `_id` is removed, and that is not tidiness
//!
//! The run document starts as the *pipeline* document, exactly as the original
//! starts from `pipeline.model_dump()`.
//! A document read from MongoDB
//! carries its `_id`, and copying that into the run would give every run of
//! the same pipeline the same primary key. The first insert would succeed and
//! every later one would fail with `DuplicateKey` — which this crate reads as
//! [`pneuma_store::Created::AlreadyExists`], the "a redelivery already handled
//! it" case. So the second and every subsequent run of a pipeline would be
//! silently dropped: acked, never written, never dispatched, nothing logged.
//! The `_id` that matters is the one Mongo generates per run.

use chrono::{DateTime, Utc};
use mongodb::bson::{Bson, DateTime as BsonDateTime, Document};
use pneuma_core::status::RunStatus;
use pneuma_core::step::StepRegistry;
use pneuma_proto::envelope::Message;

/// The run's own id. Unique once the defect notes are done.
pub const RUN_ID: &str = "run_id";
/// Where the per-node state lives, keyed by node id.
pub const STATE: &str = "state";
/// The run's status.
pub const STATUS: &str = "status";
/// When the run was created.
pub const CREATED_AT: &str = "created_at";
/// What the first steps receive.
pub const STEP_INPUT: &str = "step_input";
/// MongoDB's own primary key, which a run must not inherit from its pipeline.
pub const ID: &str = "_id";

/// Why a run document could not be built.
#[derive(Debug, thiserror::Error)]
pub enum DocumentError {
    /// A value would not encode as BSON.
    ///
    /// Reachable for a step carrying an unmodelled field that BSON has no
    /// representation for — a floating-point NaN, say, which JSON allows
    /// through `serde_json::Value` and BSON does not.
    #[error("{what} would not encode as BSON: {source}")]
    Encode {
        /// Which part of the document.
        what: &'static str,
        /// What the encoder said.
        source: mongodb::bson::error::Error,
    },
}

/// The run document for `message`, built from the stored pipeline definition.
///
/// `pipeline` is consumed and added to rather than copied field by field, so
/// anything a definition carries that this port does not model still reaches
/// the run — which is what the original does by starting from `model_dump()`.
///
/// `run_id` is passed in rather than derived here, because the caller already
/// holds it and has to go on holding it: the defect notes names the
/// trap of reading it back off the insert result, which is unbound on exactly
/// the redelivery path the conflict handling exists to serve.
pub fn run_document(
    mut pipeline: Document,
    registry: &StepRegistry,
    run_id: &str,
    message: &Message,
    created_at: DateTime<Utc>,
) -> Result<Document, DocumentError> {
    // See the module docs: inheriting the pipeline's `_id` makes every run of
    // one pipeline collide, and the collision is read as a redelivery.
    pipeline.remove(ID);

    pipeline.insert(STATE, state(registry)?);
    pipeline.insert(RUN_ID, run_id);
    pipeline.insert(STATUS, encode(&RunStatus::Created, "status")?);
    // Milliseconds rather than `DateTime::from_chrono`, which is behind a bson
    // feature this workspace does not enable -- the same conversion
    // `pneuma_store::history` makes, for the same reason.
    pipeline.insert(
        CREATED_AT,
        Bson::DateTime(BsonDateTime::from_millis(created_at.timestamp_millis())),
    );
    pipeline.insert(STEP_INPUT, encode(&message.step_input, "step_input")?);
    for (key, topic) in [
        ("reply_to_result", message.reply_to_result.as_ref()),
        ("reply_to_error", message.reply_to_error.as_ref()),
        ("reply_to_event", message.reply_to_event.as_ref()),
    ] {
        // Written even when absent, as `Null`. The original sets all three
        // unconditionally from values that may be `None`, so a run document
        // there always has the keys -- and a consumer distinguishing "no reply
        // address" from "an older run document that predates the field" needs
        // them to keep being there.
        let value = match topic {
            None => Bson::Null,
            Some(topic) => Bson::String(topic.to_string()),
        };
        pipeline.insert(key, value);
    }
    Ok(pipeline)
}

/// The resolved steps, flattened to the map the rest of the system reads.
fn state(registry: &StepRegistry) -> Result<Bson, DocumentError> {
    let mut state = Document::new();
    for step in registry.iter() {
        let node_id = step.common().node_id.as_str().to_owned();
        state.insert(node_id, encode(step, "state")?);
    }
    Ok(Bson::Document(state))
}

/// Encodes one value, naming what it was if it will not go.
fn encode<T: serde::Serialize>(value: &T, what: &'static str) -> Result<Bson, DocumentError> {
    mongodb::bson::serialize_to_bson(value).map_err(|source| DocumentError::Encode { what, source })
}
