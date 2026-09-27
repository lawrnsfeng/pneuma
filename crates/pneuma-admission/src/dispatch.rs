//! Choosing what to run next, fairly.
//!
//! The first consumer `pneuma-fairness` has ever had. It has been complete,
//! pure and property-tested since it was written, with nothing calling it —
//! and a pillar with no consumer is a pillar nobody has checked the shape of.
//!
//! # The shape of a round
//!
//! `next_round()` → `queued_backlogs()` → [`group`] → [`backlogs`] →
//! `select_batch` → `claim` → submit. Only the ends touch the database; the
//! middle is pure, which is what lets the fair selection be tested without one.
//!
//! # Quotas must over-subscribe the batch
//!
//! `round_base × Σweights` has to comfortably exceed `batch_size` or work
//! conservation never happens — `pneuma-fairness`'s own header says so, and a
//! caller reaching for the obvious `round_base = batch_size / flows` gets a
//! batch that runs short while a flow sits on a hundred items. [`round_base`]
//! is the one place that arithmetic lives.

use std::collections::BTreeMap;
use std::ops::Range;

use pneuma_fairness::{Batch, Dimension, FlowBacklog, FlowKey, FlowKeyError, Weight};
use pneuma_store::Queued;

use crate::accept::TENANT_DIMENSION;

/// One tenant's contiguous run of queued rows.
///
/// A span rather than a copy: `queued_backlogs` returns rows ordered by
/// `(tenant_id, enqueued_at, run_id)`, so one tenant's rows are already
/// adjacent and `select_batch` can borrow straight into them. It also means
/// the arrival order the query established is the order the selection sees,
/// rather than one this module re-imposed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Group {
    /// The flow these compete as.
    pub flow: FlowKey,
    /// Its share of a round.
    pub weight: Weight,
    /// Where its rows are in the slice this was built from.
    pub span: Range<usize>,
}

/// Groups queued rows into flows, with each flow's configured weight.
///
/// Relies on the query's ordering rather than sorting again: rows arrive
/// grouped by tenant, so a change of `tenant_id` is a change of group. If that
/// ordering were ever dropped from the SQL this would silently produce several
/// groups per tenant — which `select_batch` reports as `duplicate_flows`, and
/// which [`alarm`] exists to notice.
///
/// An unknown tenant gets [`Weight::ONE`] rather than being dropped. A tenant
/// that appears in the queue and not in the configuration is a new customer or
/// a config that has not caught up, and refusing to dispatch their work until
/// someone edits a file is a worse answer than giving them an ordinary share.
///
/// Fallible only in a way that cannot currently happen: the dimension name is a
/// constant and one dimension is never empty, so `FlowKey::new` has nothing to
/// refuse. It is propagated rather than swallowed because the alternative —
/// skipping a flow whose key would not build — silently drops that tenant's
/// entire backlog out of a *fair* selection, which is the unfairness this
/// whole path exists to prevent.
pub fn group(
    queued: &[Queued],
    weights: &BTreeMap<String, Weight>,
) -> Result<Vec<Group>, FlowKeyError> {
    let mut groups: Vec<Group> = Vec::new();
    for (index, row) in queued.iter().enumerate() {
        match groups.last_mut() {
            // Same tenant as the row before: extend the span rather than
            // starting a second group for one flow.
            Some(open) if queued[open.span.start].tenant_id == row.tenant_id => {
                open.span.end = index + 1;
            }
            _ => {
                let flow = FlowKey::new(&[Dimension::new(TENANT_DIMENSION, &row.tenant_id)?])?;
                let weight = weights.get(&row.tenant_id).copied().unwrap_or(Weight::ONE);
                groups.push(Group {
                    flow,
                    weight,
                    span: index..index + 1,
                });
            }
        }
    }
    Ok(groups)
}

/// Borrows the groups as the backlogs `select_batch` takes.
///
/// Separate from [`group`] only because `FlowBacklog` borrows both the key and
/// the rows, so something has to own them first.
pub fn backlogs<'a>(queued: &'a [Queued], groups: &'a [Group]) -> Vec<FlowBacklog<'a, Queued>> {
    let mut borrowed = Vec::with_capacity(groups.len());
    for group in groups {
        borrowed.push(FlowBacklog {
            flow: &group.flow,
            weight: group.weight,
            items: &queued[group.span.clone()],
        });
    }
    borrowed
}

/// A round's quota base, chosen so quotas over-subscribe the batch.
///
/// Work conservation is a precondition, not a guarantee: `pneuma-fairness`
/// hands each flow at most `weight × round_base` items and never gives one
/// flow another's unused share, so a batch fills only because the busy flows
/// are still under their own ceilings. With `round_base × Σweights` equal to
/// `batch_size`, a quiet flow's share is simply lost and the batch runs short
/// while somebody holds a hundred items.
///
/// The whole batch per flow, therefore. That is the largest useful ceiling —
/// `select_batch` truncates each flow to the batch size anyway — so no flow is
/// ever the reason a round came back short, while the per-flow cap still stops
/// one flow taking more than a whole batch.
pub fn round_base(batch_size: usize) -> std::num::NonZeroU32 {
    let clamped = u32::try_from(batch_size).unwrap_or(u32::MAX);
    std::num::NonZeroU32::new(clamped).unwrap_or(std::num::NonZeroU32::MIN)
}

/// One flow appearing in a round more than once, which must never happen.
///
/// `select_batch` reports it, and the report is worth acting on: a flow with
/// two backlogs takes its quota twice, so the round was unfair and this names
/// who benefited.
///
/// A function rather than a check inside the loop, and that is the point.
/// [`group`] groups by tenant, so in production this set is always empty — an
/// inline `if` would be unreachable code inside an I/O function, testable only
/// by breaking the grouping. As a pure function over a `Batch` it is reached by
/// handing `select_batch` a duplicated backlog, which is three lines in a test.
pub fn alarm<T>(batch: &Batch<'_, T>) -> Option<Vec<String>> {
    if batch.duplicate_flows.is_empty() {
        return None;
    }
    // A loop rather than an iterator chain, for the same reason as `backlogs`
    // above: a line that is only a link in a multi-line chain is not
    // attributed to the call that ran, so this read as dead while the test
    // below was asserting what it returned.
    let mut named = Vec::with_capacity(batch.duplicate_flows.len());
    for flow in &batch.duplicate_flows {
        named.push(flow.to_string());
    }
    Some(named)
}
