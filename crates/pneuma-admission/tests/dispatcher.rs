//! One round, against fakes for both ends.
//!
//! The fair selection is tested in `dispatch.rs` without a database, and the
//! classification in `restate.rs` without a container. What is left here is the
//! *sequencing* — what gets claimed, what gets settled, and what deliberately
//! does not — which is the part a fake can check because it is this crate's
//! own logic rather than a property of Postgres or Restate.
//!
//! The case worth the most attention is the one that settles nothing:
//! a submission whose outcome is unknown must be left claimed, because
//! settling it either way is a guess.

use std::collections::BTreeMap;
use std::sync::Mutex;

use async_trait::async_trait;
use chrono::{DateTime, TimeZone, Utc};
use pneuma_admission::{
    report_line, round, tick, Dispatchable, Invoker, RoundReport, Settings, Tick,
};
use pneuma_store::{Outcome, Queued};
use serde_json::{json, Value};

fn queued(tenant: &str, run: &str, nth: u32) -> Queued {
    let Some(at) = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).single() else {
        panic!("a real instant");
    };
    Queued {
        run_id: run.to_owned(),
        tenant_id: tenant.to_owned(),
        payload: json!({"run": run}),
        enqueued_at: at + chrono::Duration::seconds(i64::from(nth)),
    }
}

/// A queue that answers from a script and records what it was told.
struct Queue {
    rows: Vec<Queued>,
    /// Which run ids `claim` will hand back. `None` means "all of them".
    claims: Option<Vec<String>>,
    settled: Mutex<Vec<(String, Outcome, Option<String>)>>,
    claimed: Mutex<Vec<String>>,
    /// An id `claim` returns that was never asked for, so the defensive path
    /// for a misbehaving queue is reachable.
    extra_claim: Option<String>,
    /// What the sweep hands back, and what cutoff it was asked for.
    reclaims: Vec<String>,
    swept: Mutex<Vec<DateTime<Utc>>>,
}

impl Queue {
    fn holding(rows: Vec<Queued>) -> Self {
        Queue {
            rows,
            claims: None,
            settled: Mutex::new(Vec::new()),
            claimed: Mutex::new(Vec::new()),
            extra_claim: None,
            reclaims: Vec::new(),
            swept: Mutex::new(Vec::new()),
        }
    }

    fn settled(&self) -> Vec<(String, Outcome, Option<String>)> {
        match self.settled.lock() {
            Ok(settled) => settled.clone(),
            Err(poisoned) => poisoned.into_inner().clone(),
        }
    }
}

#[async_trait]
impl Dispatchable for Queue {
    async fn next_round(&self) -> Result<i64, String> {
        Ok(7)
    }
    async fn queued_backlogs(&self, _per_tenant: i64) -> Result<Vec<Queued>, String> {
        Ok(self.rows.clone())
    }
    async fn claim(&self, run_ids: &[String]) -> Result<Vec<String>, String> {
        let mut taken: Vec<String> = match &self.claims {
            Some(only) => run_ids
                .iter()
                .filter(|id| only.contains(id))
                .cloned()
                .collect(),
            None => run_ids.to_vec(),
        };
        if let Some(extra) = &self.extra_claim {
            taken.push(extra.clone());
        }
        match self.claimed.lock() {
            Ok(mut seen) => seen.extend(taken.iter().cloned()),
            Err(poisoned) => poisoned.into_inner().extend(taken.iter().cloned()),
        }
        Ok(taken)
    }
    async fn settle(
        &self,
        run_id: &str,
        outcome: Outcome,
        detail: Option<&str>,
    ) -> Result<bool, String> {
        let entry = (run_id.to_owned(), outcome, detail.map(ToOwned::to_owned));
        match self.settled.lock() {
            Ok(mut settled) => settled.push(entry),
            Err(poisoned) => poisoned.into_inner().push(entry),
        }
        Ok(true)
    }
    async fn reclaim(&self, older_than: DateTime<Utc>) -> Result<Vec<String>, String> {
        match self.swept.lock() {
            Ok(mut swept) => swept.push(older_than),
            Err(poisoned) => poisoned.into_inner().push(older_than),
        }
        Ok(self.reclaims.clone())
    }
}

/// A Restate that answers the same thing every time.
struct Always(Result<(u16, Value), String>);

#[async_trait]
impl Invoker for Always {
    async fn send(&self, _key: &str, _payload: &Value) -> Result<(u16, Value), String> {
        self.0.clone()
    }
}

fn settings(batch_size: usize) -> Settings {
    Settings {
        batch_size,
        per_tenant: 100,
        weights: BTreeMap::new(),
    }
}

fn accepted() -> Always {
    Always(Ok((202, json!({"status": "Accepted"}))))
}

#[tokio::test]
async fn a_round_claims_what_it_chose_and_settles_what_it_submitted() {
    let queue = Queue::holding(vec![
        queued("acme", "a1", 0),
        queued("acme", "a2", 1),
        queued("globex", "g1", 0),
    ]);
    let Ok(report) = round(&queue, &accepted(), &settings(10)).await else {
        panic!("a round should run");
    };
    assert_eq!(report.round, 7, "the round the sequence handed out");
    assert_eq!(report.considered, 3);
    assert_eq!(report.selected, 3, "the batch is bigger than the backlog");
    assert_eq!(report.claimed, 3);
    assert_eq!(report.submitted, 3);
    assert_eq!(report.unknown, 0);
    assert_eq!(report.duplicate_flows, None);

    let settled = queue.settled();
    assert_eq!(settled.len(), 3);
    assert!(
        settled
            .iter()
            .all(|(_, outcome, _)| *outcome == Outcome::Done),
        "a submission Restate accepted has done its job: {settled:?}"
    );
}

#[tokio::test]
async fn an_unknown_outcome_is_left_claimed_rather_than_guessed_at() {
    // The case that matters most. A transport failure means the submission may
    // or may not have landed -- settling `done` loses a run that was never
    // submitted, settling `failed` gives up on one that may already be
    // running. Leaving it claimed is what `reclaim` exists for, and the
    // idempotency key makes resubmitting free if it did land.
    let queue = Queue::holding(vec![queued("acme", "a1", 0)]);
    let unreachable = Always(Err("connection refused".to_owned()));
    let Ok(report) = round(&queue, &unreachable, &settings(10)).await else {
        panic!("an unreachable Restate is not a failed round");
    };
    assert_eq!(report.claimed, 1);
    assert_eq!(report.submitted, 0);
    assert_eq!(report.unknown, 1);
    assert!(
        queue.settled().is_empty(),
        "nothing is settled on a guess: {:?}",
        queue.settled()
    );
}

#[tokio::test]
async fn a_refusal_and_a_failed_run_are_both_settled_failed_with_the_reason() {
    for (answer, fragment) in [
        (
            Always(Ok((400, json!({"message": "malformed"})))),
            "malformed",
        ),
        (
            Always(Ok((
                500,
                json!({"message": "calling A failed", "source": "invocation"}),
            ))),
            "calling A failed",
        ),
    ] {
        let queue = Queue::holding(vec![queued("acme", "a1", 0)]);
        let Ok(report) = round(&queue, &answer, &settings(10)).await else {
            panic!("a refused submission is not a failed round");
        };
        assert_eq!(report.submitted, 1);
        assert_eq!(report.unknown, 0);
        let settled = queue.settled();
        let [(run_id, outcome, detail)] = settled.as_slice() else {
            panic!("one settle: {settled:?}");
        };
        assert_eq!(run_id, "a1");
        assert_eq!(*outcome, Outcome::Failed);
        let detail = detail.clone().unwrap_or_default();
        assert!(
            detail.contains(fragment),
            "the reason reaches the row: {detail}"
        );
    }
}

#[tokio::test]
async fn losing_a_claim_to_another_dispatcher_is_not_a_failure() {
    // `claim` is the atomic arbiter, so a shorter list than was chosen means
    // another dispatcher got there first. That is the mechanism working.
    let mut queue = Queue::holding(vec![queued("acme", "a1", 0), queued("acme", "a2", 1)]);
    queue.claims = Some(vec!["a1".to_owned()]);
    let Ok(report) = round(&queue, &accepted(), &settings(10)).await else {
        panic!("a contested round still runs");
    };
    assert_eq!(report.selected, 2, "both were chosen");
    assert_eq!(report.claimed, 1, "one was taken");
    assert_eq!(report.submitted, 1, "and only that one was submitted");
    assert_eq!(queue.settled().len(), 1);
}

#[tokio::test]
async fn an_idle_round_still_takes_a_round_number() {
    // Not an early return. `select_batch` rotates its tie-break by the round,
    // so a dispatcher that skipped the increment when idle would advance it
    // only when there was work -- and two dispatchers with different idle
    // patterns would disagree about whose turn it is.
    let queue = Queue::holding(Vec::new());
    let Ok(report) = round(&queue, &accepted(), &settings(10)).await else {
        panic!("an empty queue is not an error");
    };
    assert_eq!(report.round, 7, "the number was still taken");
    assert_eq!(
        (
            report.considered,
            report.selected,
            report.claimed,
            report.submitted
        ),
        (0, 0, 0, 0)
    );
    assert!(queue.settled().is_empty());
}

#[tokio::test]
async fn the_batch_size_bounds_what_one_round_dispatches() {
    let rows: Vec<Queued> = (0..10)
        .map(|n| queued("acme", &format!("a{n}"), n))
        .collect();
    let queue = Queue::holding(rows);
    let Ok(report) = round(&queue, &accepted(), &settings(4)).await else {
        panic!("a round should run");
    };
    assert_eq!(report.considered, 10, "the whole backlog was read");
    assert_eq!(report.selected, 4, "and four of it dispatched");
    assert_eq!(report.submitted, 4);
}

#[tokio::test]
async fn a_claim_the_backlog_never_offered_is_left_alone_not_guessed_at() {
    // A queue that hands back an id this round did not ask for is
    // misbehaving. There is no payload to submit, and inventing one would be
    // worse than leaving it -- so it is counted as unknown and `reclaim`
    // returns it later, exactly like a submission whose outcome never came
    // back.
    let mut queue = Queue::holding(vec![queued("acme", "a1", 0)]);
    queue.claims = Some(vec!["a1".to_owned(), "a-stranger".to_owned()]);
    let Ok(report) = round(&queue, &accepted(), &settings(10)).await else {
        panic!("a misbehaving queue is not a failed round");
    };
    assert_eq!(report.claimed, 1, "only what was asked for is claimable");

    // Now the fake returns an id outright, regardless of what was chosen.
    let mut rogue = Queue::holding(vec![queued("acme", "a1", 0)]);
    rogue.claims = None;
    rogue.rows = vec![queued("acme", "a1", 0)];
    let mut stranger = Queue::holding(vec![queued("acme", "a1", 0)]);
    stranger.claims = Some(vec!["a1".to_owned()]);
    stranger.extra_claim = Some("a-stranger".to_owned());
    let Ok(report) = round(&stranger, &accepted(), &settings(10)).await else {
        panic!("a round should run");
    };
    assert_eq!(report.claimed, 2, "the queue insisted on two");
    assert_eq!(report.submitted, 1, "one had a payload");
    assert_eq!(report.unknown, 1, "and the stranger was left alone");
    let settled = stranger.settled();
    assert_eq!(
        settled.len(),
        1,
        "nothing was settled on a guess: {settled:?}"
    );
    assert_eq!(settled[0].0, "a1");
}

/// A queue that cannot be reached at all.
struct Broken;

#[async_trait]
impl Dispatchable for Broken {
    async fn next_round(&self) -> Result<i64, String> {
        Err("the pool is closed".to_owned())
    }
    async fn queued_backlogs(&self, _per_tenant: i64) -> Result<Vec<Queued>, String> {
        Err("the pool is closed".to_owned())
    }
    async fn claim(&self, _run_ids: &[String]) -> Result<Vec<String>, String> {
        Err("the pool is closed".to_owned())
    }
    async fn settle(
        &self,
        _run_id: &str,
        _outcome: Outcome,
        _detail: Option<&str>,
    ) -> Result<bool, String> {
        Err("the pool is closed".to_owned())
    }
    async fn reclaim(&self, _older_than: DateTime<Utc>) -> Result<Vec<String>, String> {
        Err("the pool is closed".to_owned())
    }
}

fn instant() -> DateTime<Utc> {
    let Some(at) = Utc.with_ymd_and_hms(2026, 3, 1, 12, 0, 0).single() else {
        panic!("a real instant");
    };
    at
}

#[tokio::test]
async fn a_tick_sweeps_before_it_dispatches() {
    // The ordering is the property. A submission reclaimed on this tick has to
    // be eligible for the round that follows it, or a Restate outage costs two
    // intervals to recover from instead of one.
    let mut queue = Queue::holding(vec![queued("acme", "a1", 0)]);
    queue.reclaims = vec!["stuck".to_owned()];

    let Ok(done) = tick(&queue, &accepted(), &settings(10), instant()).await else {
        panic!("a tick should run");
    };
    assert_eq!(done.reclaimed, vec!["stuck".to_owned()]);
    assert_eq!(done.round.selected, 1);
    let swept = match queue.swept.lock() {
        Ok(swept) => swept.clone(),
        Err(poisoned) => poisoned.into_inner().clone(),
    };
    assert_eq!(
        swept,
        vec![instant()],
        "the cutoff is passed through, not invented"
    );
}

#[tokio::test]
async fn a_tick_that_cannot_reach_the_queue_reports_rather_than_dispatching() {
    let Err(error) = tick(&Broken, &accepted(), &settings(10), instant()).await else {
        panic!("a closed pool is not a round");
    };
    assert!(error.contains("the pool is closed"), "{error}");
}

#[test]
fn a_tick_renders_to_one_line_whichever_way_it_went() {
    // The loop that calls this has no branch in it, deliberately: the failing
    // arm would otherwise need a database that is down in order to be measured.
    let failed: Result<Tick, String> = Err("the pool is closed".to_owned());
    assert_eq!(
        report_line(&failed),
        "dispatch round failed: the pool is closed"
    );

    let report = RoundReport {
        round: 12,
        considered: 5,
        selected: 4,
        claimed: 3,
        submitted: 2,
        unknown: 1,
        duplicate_flows: None,
    };
    let quiet = Ok(Tick {
        reclaimed: vec!["stuck".to_owned()],
        round: report.clone(),
    });
    assert_eq!(
        report_line(&quiet),
        "round 12 reclaimed=1 considered=5 selected=4 claimed=3 submitted=2 unknown=1"
    );

    // The alarm is spelled out rather than folded into a count. It must never
    // fire -- two backlogs for one flow would mean the grouping is broken --
    // so what it needs is to be legible, not countable.
    let alarmed = Ok(Tick {
        reclaimed: Vec::new(),
        round: RoundReport {
            duplicate_flows: Some(vec!["acme".to_owned(), "globex".to_owned()]),
            ..report
        },
    });
    let line = report_line(&alarmed);
    assert!(
        line.ends_with(" ALARM duplicate flows: acme, globex"),
        "{line}"
    );
}
