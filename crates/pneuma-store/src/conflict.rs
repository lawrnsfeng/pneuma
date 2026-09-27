//! Telling "somebody else already inserted this" apart from a real failure.
//!
//! # Why this is a whole module
//!
//! The defect notes turns on it. The `run_id` index is non-unique
//! today, with a comment saying it "should be unique, however we have retry"
//! —
//! and the premise is right while the remedy is backwards. A duplicate *is*
//! reachable: the original's bootstrap consumer acks by leaving the
//! `message.process()`
//! block that wraps the whole handler, so a process that dies between the
//! insert and the end of that block leaves the message unacked, AMQP redelivers
//! it, and the handler runs again against a collection that already holds the
//! document. With the index non-unique the second insert succeeds, the run's
//! state is split across two documents, and every later read picks one
//! arbitrarily with nothing logged.
//!
//! Tolerating the duplicate is what turns a clean conflict into a silent fork.
//! Refusing it — a unique index — is only safe once the one caller that can hit
//! it treats the conflict as **success**, because a redelivery of a message
//! that was already handled is exactly what it means. That is what [`Created`]
//! is for.
//!
//! The original has the same distinction and cannot use it:
//! the original translates `DuplicateKeyError` into
//! `ConflictError`, and searching the package finds **no `except
//! ConflictError` anywhere**. The type exists to be caught and nothing catches
//! it.

use mongodb::error::{Error, ErrorKind, WriteFailure};

/// MongoDB's error code for a unique-index violation.
///
/// Measured rather than assumed: building a unique index over duplicates fails
/// with this code, naming the offending value (the defect notes).
pub const DUPLICATE_KEY: i32 = 11000;

/// What an insert did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Created {
    /// The document was written.
    Inserted,
    /// A document with that key was already there.
    ///
    /// Not an error. For a run this is what a broker redelivery looks like,
    /// and the correct response is to carry on to whatever the first attempt
    /// did next — which is why the caller must go on using the id it already
    /// holds rather than reading one back off the insert result. §25 names that
    /// trap precisely: the original fix, written literally, reads
    /// `result.model.run_id` from a `result` that was never bound on the
    /// conflict path, and so raises `NameError` on the one path the fix exists
    /// to serve.
    AlreadyExists,
}

/// Whether an error is a unique-index violation.
///
/// Pure, and separate from the inserts that use it, because "which failures
/// mean the row is already there" is a rule rather than a step: it is the same
/// rule for every collection, and the alternative is each caller matching on
/// driver internals and one of them getting it wrong.
pub fn is_duplicate_key(error: &Error) -> bool {
    match *error.kind {
        ErrorKind::Write(WriteFailure::WriteError(ref failure)) => failure.code == DUPLICATE_KEY,
        // Everything else, including a write-concern failure: the write may
        // well have happened, and reporting "already there" for a database
        // that could not be reached would turn an outage into a silently
        // skipped run.
        _ => false,
    }
}
