//! `pneuma-store`'s queue, as the two traits above want it.
//!
//! Until this existed the only implementor of either trait was a test fake, so
//! the router and the dispatcher were exported and unreachable from anything
//! that runs. This is the join.
//!
//! # Why the traits take `String` errors and this throws the type away
//!
//! Neither caller does anything with a `StoreError`'s structure: the ingress
//! turns every failure to reach the queue into the same 503, and the
//! dispatcher stops the round. A typed error would be a type every fake has to
//! construct in order to say "the database was down", which is the one thing a
//! fake needs to say most often. The message survives, which is what reaches
//! the log.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use pneuma_store::{Accepted, Outcome, Queued, SubmissionStore};
use serde_json::Value;

use crate::accept::Admitted;
use crate::dispatcher::Dispatchable;
use crate::ingress::Submissions;

#[async_trait]
impl Submissions for SubmissionStore {
    async fn enqueue(&self, admitted: &Admitted, payload: &Value) -> Result<Accepted, String> {
        SubmissionStore::enqueue(
            self,
            admitted.run_id.as_str(),
            admitted.tenant_id.as_str(),
            payload,
        )
        .await
        .map_err(|error| error.to_string())
    }
}

#[async_trait]
impl Dispatchable for SubmissionStore {
    async fn next_round(&self) -> Result<i64, String> {
        SubmissionStore::next_round(self)
            .await
            .map_err(|error| error.to_string())
    }

    async fn queued_backlogs(&self, per_tenant: i64) -> Result<Vec<Queued>, String> {
        SubmissionStore::queued_backlogs(self, per_tenant)
            .await
            .map_err(|error| error.to_string())
    }

    async fn claim(&self, run_ids: &[String]) -> Result<Vec<String>, String> {
        SubmissionStore::claim(self, run_ids)
            .await
            .map_err(|error| error.to_string())
    }

    async fn settle(
        &self,
        run_id: &str,
        outcome: Outcome,
        detail: Option<&str>,
    ) -> Result<bool, String> {
        SubmissionStore::settle(self, run_id, outcome, detail)
            .await
            .map_err(|error| error.to_string())
    }

    async fn reclaim(&self, older_than: DateTime<Utc>) -> Result<Vec<String>, String> {
        SubmissionStore::reclaim(self, older_than)
            .await
            .map_err(|error| error.to_string())
    }
}
