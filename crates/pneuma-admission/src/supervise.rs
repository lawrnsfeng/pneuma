//! Noticing a run that will never finish on its own.
//!
//! Restate does not give up. The spike measured an invocation whose component
//! failed every time staying `backing-off` with a climbing `retry_count` and
//! no exhaustion within the window observed — nothing was discarded, nothing
//! was lost, and nothing complained (`spikes/restate/VERDICT.md`).
//!
//! That trade is favourable and it is not free. The old system's poison message
//! eventually exhausts `MaxDeliver` and lands in a dead-letter queue, when the
//! dead-letter queue works (the defect notes); here it retries visibly
//! until someone intervenes. **There is no automatic dead-letter equivalent**,
//! so something has to watch for `status = 'backing-off'` with a growing
//! `retry_count` or a poison run waits for ever without complaining. The
//! VERDICT says exactly that, and this is the rule it asks for.
//!
//! # Why two observations rather than one
//!
//! A single sample cannot tell a run that is stuck from one that is merely
//! retrying: transient failures are what the retry policy is *for*, and a
//! component restarting produces a handful of attempts that then stop. Two
//! samples of the same invocation, both backing off, is what separates "still
//! going" from "caught it mid-recovery".
//!
//! # Pure, so the rule is not tangled with the sampling
//!
//! No clock, no query, no client. What Restate is asked and how often is a
//! deployment question; what its answers *mean* is this, and it is a table of
//! cases with a test each.

/// One row of Restate's `sys_invocation`, as a watchdog reads it.
///
/// The four columns the VERDICT's own query selects:
/// `SELECT id, target, status, retry_count FROM sys_invocation`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Invocation {
    /// Restate's invocation id, e.g. `inv_1bnLw8…`.
    pub id: String,
    /// What is being invoked, e.g. `PneumaRunner/run`.
    pub target: String,
    /// Restate's own status word. `backing-off` is the one that matters.
    pub status: String,
    /// How many attempts have been made.
    pub retry_count: u32,
}

/// The status Restate reports for an invocation between failed attempts.
///
/// Restate's wire value, not a rendering of anything here — the VERDICT's
/// query returned it literally.
pub const BACKING_OFF: &str = "backing-off";

impl Invocation {
    /// Whether Restate is currently waiting to try this again.
    ///
    /// Anything else — including a status this code has never seen — is *not*
    /// backing off. A new Restate word must not be able to raise an alarm by
    /// being unrecognised: the failure mode of a watchdog is crying wolf, and
    /// the one that gets it switched off.
    pub fn is_backing_off(&self) -> bool {
        self.status == BACKING_OFF
    }
}

/// A run that is not going to finish without someone doing something.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Wedged {
    /// Which invocation, so it can be looked at — or killed, which the spike
    /// measured working: `DELETE /invocations/{id}?mode=kill`.
    pub id: String,
    /// What it was trying to run.
    pub target: String,
    /// How many attempts it had made when this was decided.
    pub retry_count: u32,
}

/// Whether two observations of one invocation say it is wedged.
///
/// Every arm answers `None` for a reason worth stating, because a watchdog that
/// alarms wrongly is one that gets muted:
///
/// - **Different ids.** Not two observations of one thing, so there is nothing
///   to compare. Alarming here would name a run that is fine.
/// - **Not backing off now.** It moved on — finished, was killed, or is
///   running. Whatever the previous sample said, the current one is the truth.
/// - **Not backing off before.** One sample of a retry is what a transient
///   failure looks like, and retrying is what the policy is for.
/// - **The count fell.** The id was reused, or Restate reset the counter.
///   Evidence that contradicts itself is not evidence.
/// - **Below the threshold.** How many attempts are too many is a deployment
///   question, so it is the caller's number rather than one invented here.
///
/// An *unchanged* count is deliberately **not** a reason to stay quiet, and an
/// earlier version of this got that wrong in a way that would have silenced the
/// watchdog for exactly the runs it exists for. Restate's backoff is
/// exponential up to a ceiling, so the longer a run is stuck the further apart
/// its attempts get — the spike watched the gap widen inside forty seconds.
/// Requiring the count to *grow* between samples therefore makes the alarm
/// depend on polling more slowly than the current backoff interval: a watchdog
/// sampling every minute against a ten-minute ceiling sees the same
/// `retry_count` in every consecutive pair and answers "not wedged" for ever,
/// which is precisely the poison run waiting silently that the VERDICT asked
/// this to prevent. Two `backing-off` samples of one id *is* the evidence; the
/// count growing is a bonus, not a requirement.
pub fn wedged(previous: &Invocation, current: &Invocation, threshold: u32) -> Option<Wedged> {
    if previous.id != current.id {
        return None;
    }
    if !previous.is_backing_off() || !current.is_backing_off() {
        return None;
    }
    // Strictly falling only. See the note above on why an unchanged count must
    // still raise the alarm.
    if current.retry_count < previous.retry_count {
        return None;
    }
    if current.retry_count < threshold {
        return None;
    }
    Some(Wedged {
        id: current.id.clone(),
        target: current.target.clone(),
        retry_count: current.retry_count,
    })
}
