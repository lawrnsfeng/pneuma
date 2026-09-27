//! Driving one run, and deciding what its outcome means.
//!
//! # Where a run comes from
//!
//! A `MessageInit`, in the shape the original's bootstrap published for its
//! controller to pick up — the same
//! message, on a renamed subject, with renamed keys.
//!
//! **It is no longer deployable against the original.** That was the point of
//! this service when it was written: consume the same message on the same
//! subject, so a broker-only deployment could run beside the original controller.
//! The design notes gave that up deliberately — the subject is now
//! `pneuma.run.start` and the envelope's keys are pneuma's own — so what
//! remains is the broker-portability half: a run can be driven with nothing but
//! a broker, which is why this does not read from `pneuma-admission` instead.
//!
//! # Where the pipeline comes from
//!
//! `MessageInit` names a pipeline; it does not carry one. The run document
//! does, because `pneuma-intake` builds it *as* the pipeline document plus a
//! few keys — so the definition is read back out of the run rather than the
//! `pipelines` collection. That matters: a definition edited between the run
//! being created and being driven would otherwise change what the run does
//! halfway through, and a run should execute the pipeline it was created from.
//!
//! # Why the outcome is decided separately
//!
//! `drive` reports a run's ending as a `Result`, and the two halves of that go
//! to different places: the outputs go back to whoever asked, and the *status*
//! goes into the run document for the janitor and the gateway to read.
//! [`status_for`] is that mapping, pure, so "does a stalled run count as an
//! error" is a test rather than an inline `match` in a function that also
//! talks to Mongo.

use mongodb::bson::Document;
use pneuma_core::node::Pipeline;
use pneuma_core::status::RunStatus;
use pneuma_runner::driver::{Completed, DriveError};

use crate::nats::CallError;

/// MongoDB's own key, which is the definition's and not the pipeline's.
const ID: &str = "_id";

/// Why a run could not be driven.
#[derive(Debug, thiserror::Error)]
pub enum HostError {
    /// No run document with that id.
    ///
    /// The ordinary shape of a race rather than a corruption: `pneuma-intake`
    /// writes the run document *before* the run is announced, so this means the
    /// announcement arrived from something that did not write one — an older
    /// intake, or a hand-published message.
    #[error("no run {run_id}")]
    NoSuchRun {
        /// The run that was announced.
        run_id: String,
    },

    /// The run document does not contain a pipeline this port can read.
    #[error("run {run_id} does not carry a readable pipeline: {reason}")]
    NotAPipeline {
        /// The run whose document it is.
        run_id: String,
        /// What the decoder said.
        reason: String,
    },

    /// The pipeline does not resolve.
    ///
    /// Reachable even though `pneuma-intake` resolves before storing: a run
    /// created by the *original's* bootstrap went through no such check, and a
    /// run document that predates the cutover is still readable.
    #[error("run {run_id} does not resolve: {reason}")]
    Unresolvable {
        /// The run whose pipeline it is.
        run_id: String,
        /// What the resolver said.
        reason: String,
    },

    /// The database refused, or was unreachable.
    #[error("could not read run {run_id}: {reason}")]
    Database {
        /// The run that was being read.
        run_id: String,
        /// What the driver said.
        reason: String,
    },
}

/// What driving a run produced.
#[derive(Debug)]
pub enum RunOutcome {
    /// It ran to completion.
    Finished(Completed),
    /// It did not, and this is why.
    ///
    /// One variant for every way of failing, because the *run's* answer to all
    /// of them is the same — it is over and it did not succeed — and the
    /// difference is a log line. Separating them here would be inventing a
    /// taxonomy the run document has no column for.
    Failed(String),
}

/// The status a run document should be left in.
///
/// Pure, so "what does a stalled run count as" is a test rather than a `match`
/// inside a function that also talks to Mongo.
///
/// There is no `Cancelled` here, and that is deliberate: cancellation is not
/// something `drive` reports. It arrives as an event on a different path
/// entirely, and a driver that guessed at it from a failure message would
/// mark a genuinely failed run as cancelled the first time an error string
/// happened to contain the word.
pub fn status_for(outcome: &RunOutcome) -> RunStatus {
    match outcome {
        RunOutcome::Finished(_) => RunStatus::Finished,
        RunOutcome::Failed(_) => RunStatus::Error,
    }
}

/// What a `drive` failure means, as one sentence.
///
/// Separate from [`status_for`] because the status is what the system reads and
/// this is what a person reads, and only one of them should be allowed to
/// change without a migration.
pub fn describe(error: &DriveError<CallError>) -> String {
    error.to_string()
}

/// The pipeline a run was created from, read back out of its own document.
///
/// Out of the *run*, not the `pipelines` collection, and that is the point: a
/// definition edited between a run being created and being driven would
/// otherwise change what the run does halfway through.
pub fn pipeline_of(run_id: &str, mut run: Document) -> Result<Pipeline, HostError> {
    // The run document's `_id` is the run's, and `Pipeline::extra` would carry
    // it into every dispatch as a `{"$oid": ...}` nobody downstream reads.
    run.remove(ID);
    mongodb::bson::deserialize_from_document(run).map_err(|error| HostError::NotAPipeline {
        run_id: run_id.to_owned(),
        reason: error.to_string(),
    })
}

/// A status, as the run document spells it.
///
/// Hand-written and public rather than reached through `serde_json::to_value`,
/// for the reason [`crate::message::wire_kind`] is: that conversion is
/// infallible in fact and fallible in type, so using it puts an arm in the
/// writing path that no input can reach. This pins the same spelling with no
/// arm at all, and a test holds it to serde's.
pub fn wire_status(status: RunStatus) -> &'static str {
    match status {
        RunStatus::Created => "created",
        RunStatus::Processing => "processing",
        RunStatus::Finished => "finished",
        RunStatus::Error => "error",
        RunStatus::TimedOut => "timed_out",
        RunStatus::Cancelled => "cancelled",
    }
}
