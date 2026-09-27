//! The `pipelines` collection: definitions, looked up by their own id.
//!
//! Read-mostly, and read by exactly one thing — `pneuma-intake`, which resolves
//! a submitted `pipeline_id` into the definition a run is built from.
//! The writes are the
//! `create_pipeline` message, which is how a definition gets there in the
//! first place.
//!
//! # Documents, not models
//!
//! Both methods deal in `Document`. A pipeline definition is the *input* to
//! `pneuma_core::resolver::resolve`, and everything it carries that this port
//! does not model still has to reach a run document unchanged — the original
//! builds a run by `model_dump()`ing the pipeline and adding to it. Typing the
//! collection here would silently drop whatever a definition holds that the
//! Rust structs do not, which is a wire regression dressed as a tidy-up. The
//! resolver takes its own typed view of the parts it needs.

use mongodb::bson::{doc, Document};
use mongodb::Collection;

use crate::barrier::BarrierError;
use crate::conflict::{is_duplicate_key, Created};

/// The field a pipeline is looked up by.
///
/// Not `_id`. The original's filter is `{"pipeline_id": pipeline_id}`,
/// and the two are different
/// keys — a definition can be replaced, getting a new `_id`, and still be the
/// same pipeline.
pub const PIPELINE_ID: &str = "pipeline_id";

/// Reads and writes over the `pipelines` collection.
#[derive(Debug, Clone)]
pub struct PipelineStore {
    pipelines: Collection<Document>,
}

impl PipelineStore {
    /// Wraps the `pipelines` collection.
    pub fn new(pipelines: Collection<Document>) -> Self {
        PipelineStore { pipelines }
    }

    /// Which collection this reads.
    pub fn namespace(&self) -> mongodb::Namespace {
        self.pipelines.namespace()
    }

    /// The definition with this `pipeline_id`, if there is one.
    ///
    /// `None` rather than an error, because "no such pipeline" is a statement
    /// about the submission rather than about the database: the caller answers
    /// it by rejecting the message, not by retrying.
    pub async fn by_pipeline_id(
        &self,
        pipeline_id: &str,
    ) -> Result<Option<Document>, BarrierError> {
        let found = self
            .pipelines
            .find_one(doc! { PIPELINE_ID: pipeline_id })
            .await?;
        Ok(found)
    }

    /// Stores a definition, treating an existing one as success.
    ///
    /// Same bargain as [`crate::run::RunStore::create`]: a redelivered
    /// `create_pipeline` message is a message that was already handled, and
    /// failing it would dead-letter a definition that is already there.
    pub async fn create(&self, document: Document) -> Result<Created, BarrierError> {
        match self.pipelines.insert_one(document).await {
            Ok(_) => Ok(Created::Inserted),
            Err(error) if is_duplicate_key(&error) => Ok(Created::AlreadyExists),
            Err(error) => Err(BarrierError::Mongo(error)),
        }
    }
}
