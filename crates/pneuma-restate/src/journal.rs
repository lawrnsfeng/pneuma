//! Writing a run's record down from inside the journal.
//!
//! `pneuma-mirror` knows how to put a step into the `node_run` table.
//! Everything here is about *when* that write is allowed to happen under
//! Restate, and there is exactly one right answer.
//!
//! # A write outside `ctx.run` happens on every replay
//!
//! Restate resumes a crashed run by re-executing the handler against the
//! journal: a `ctx.run` side effect returns its recorded answer rather than
//! running again ([`crate::component`]'s module docs record the measurement),
//! and anything *not* inside one runs afresh. A bare `sqlx` await in the
//! handler would therefore re-stamp `PROCESSING` on every step of a run, every
//! time it resumed — turning the audit mirror into a record of how many times
//! the handler restarted rather than of what the run did.
//!
//! So every write is wrapped, individually. Not the whole recording in one
//! `ctx.run`: an entry is journalled when it completes, so a single wrapper
//! spanning a run would record nothing until the run ended, and a crash
//! part-way — the case this exists for — would leave no rows at all.
//!
//! # The journal's shape changes when this is deployed
//!
//! Adding entries changes the sequence a replay expects, so a run that was
//! in flight when mirroring was deployed replays against a journal it was not
//! started with. That is a cutover concern rather than a code one, and
//! `docs/runbook.md` carries it: the runbook already drains before deploying,
//! and this rides along with that drain.

use pneuma_mirror::Mirror;
use pneuma_runner::{Record, Recorder};
use restate_sdk::prelude::*;

/// A [`Mirror`] whose every write is a journal entry.
///
/// Borrows the context for the life of one handler invocation, exactly as
/// [`crate::component::HttpComponent`] does, and for the same reason: the
/// context is the invocation, not the service.
pub struct Journalled<'a, 'ctx> {
    context: &'a Context<'ctx>,
    mirror: Mirror,
}

impl<'a, 'ctx> Journalled<'a, 'ctx> {
    /// Wraps `mirror` for one invocation.
    pub fn new(context: &'a Context<'ctx>, mirror: Mirror) -> Self {
        Journalled { context, mirror }
    }
}

/// What a record is called in the journal.
///
/// An operator reading a run's journal sees which step each entry belongs to
/// rather than a list of anonymous side effects — the same reason
/// [`crate::component::HttpComponent`] names its calls `call:{node}`.
///
/// Not required to be unique, and deliberately not made so: a replay matches
/// entries by position, so two identical names are two entries rather than a
/// collision. An ancestor told twice that a child started produces exactly
/// that, and inventing a counter to tell them apart would add state whose only
/// purpose was to make a label prettier.
fn name_of(record: &Record) -> String {
    match record {
        Record::Created(step) => format!("record:created:{}", step.path),
        Record::Moved { path, status, .. } => format!("record:{status:?}:{path}"),
        Record::Produced { path, status, .. } => format!("record:{status:?}:{path}"),
    }
}

impl Recorder for Journalled<'_, '_> {
    async fn record(&self, record: Record) {
        let name = name_of(&record);
        let mirror = self.mirror.clone();
        // The closure captures clones and calls one function, which is the
        // discipline `component.rs` documents: nothing captured may differ
        // between attempts, or a retry would journal a different answer from
        // the one it replayed.
        let written = self
            .context
            .run(move || write_once(mirror.clone(), record.clone()))
            .name(&name)
            .await;
        report(&name, written);
    }
}

/// What a failed journal entry costs.
///
/// A log line, not a failure. [`Recorder::record`] returns `()` by signature
/// because an audit mirror must not be able to fail the run it audits, and a
/// `TerminalError` returned from here would do exactly that.
///
/// A function of its own rather than an `if let` in the handler, because the
/// only way `ctx.run` yields an error at all is a cancelled invocation --
/// [`write_once`] never constructs one -- and a decision reachable only by
/// cancelling a live run is a decision nothing would check. Here it is two
/// lines and a unit test.
fn report(name: &str, written: Result<(), TerminalError>) {
    if let Err(error) = written {
        eprintln!("pneuma-restate: could not journal {name}: {error}");
    }
}

/// One write, as a journalled side effect.
///
/// Always `Ok`. [`Mirror`] logs its own failures and returns nothing, so there
/// is no error for this to propagate — and propagating one would make Restate
/// retry a write whose failure has already been accepted, then fail the
/// invocation when the retries ran out.
///
/// `HandlerError` rather than `TerminalError` only because that is what
/// `ContextSideEffects::run` requires of a closure; nothing here ever
/// constructs one.
async fn write_once(mirror: Mirror, record: Record) -> Result<(), HandlerError> {
    mirror.record(record).await;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use pneuma_core::ids::{NodeId, PipelineId, RunId};
    use pneuma_core::node::NodeKind;
    use pneuma_core::status::NodeStatus;
    use pneuma_runner::NewStep;

    fn step(path: &str) -> Record {
        Record::Created(Box::new(NewStep {
            path: path.to_owned(),
            node_id: NodeId::new("A"),
            name: "component.a".to_owned(),
            kind: NodeKind::Model,
            pipeline_id: PipelineId::new("invoice.page.default"),
            run_id: RunId::new("run-1"),
            parent_id: None,
            parent_path: None,
            parent_kind: None,
            child_index: None,
            sibling_index: None,
            step_input: None,
        }))
    }

    #[test]
    fn an_entry_is_named_after_the_step_and_the_moment() {
        // An operator reading a run's journal sees which step each entry
        // belongs to rather than a list of anonymous side effects.
        assert_eq!(
            name_of(&step("run-1.p.A")),
            "record:created:run-1.p.A",
            "a creation names the path"
        );
        assert_eq!(
            name_of(&Record::Moved {
                path: "run-1.p.A".to_owned(),
                status: NodeStatus::Processing,
                failure: None,
            }),
            "record:Processing:run-1.p.A"
        );
        assert_eq!(
            name_of(&Record::Produced {
                path: "run-1.p.A".to_owned(),
                status: NodeStatus::Finished,
                output: serde_json::Value::Null,
            }),
            "record:Finished:run-1.p.A"
        );
    }

    #[test]
    fn a_journal_entry_that_failed_is_reported_and_not_raised() {
        // Both arms, because the contract is that neither of them fails the
        // run: `report` returns `()`, so what is assertable is that it accepts
        // an error at all rather than propagating one.
        report("record:created:run-1.p.A", Ok(()));
        report(
            "record:created:run-1.p.A",
            Err(TerminalError::new("the invocation was cancelled")),
        );
    }

    #[tokio::test]
    async fn a_write_that_reached_nothing_still_answers_ok() {
        // `write_once` never constructs an error, which is what makes
        // `report`'s failing arm unreachable in production: `Mirror` logs its
        // own failures and returns nothing, so there is no error to propagate
        // -- and propagating one would make Restate retry a write whose failure
        // has already been accepted.
        let Ok(pool) = sqlx::postgres::PgPoolOptions::new()
            .max_connections(1)
            .acquire_timeout(std::time::Duration::from_millis(200))
            .connect_lazy("postgres://nobody:nothing@127.0.0.1:1/nowhere")
        else {
            panic!("a lazy pool does not connect yet");
        };
        let mirror = Mirror::new(pool, "test");
        assert!(write_once(mirror, step("run-1.p.A")).await.is_ok());
    }
}
