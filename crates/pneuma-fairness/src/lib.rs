//! Work-conserving weighted fair dispatch.
//!
//! This is the one pillar nothing in the landscape provides. Temporal has no
//! notion of it; the Restate spike found no `priority`, `weight`, `quota`, or
//! `rate_limit` anywhere in its SDK surface — only a `limit_key`, which caps
//! per-key concurrency and so gives isolation rather than work conservation
//! (`spikes/restate/VERDICT.md` §4). Whatever engine ends up underneath, this
//! stays pneuma's own work, which is why the crate depends on none of them.
//!
//! Designed in `FRAMEWORK-FOUNDATIONS.md` §5, after Kubernetes' API Priority
//! and Fairness: classify work into *flows*, give each a proportional share
//! rather than a fixed quota, and redistribute an idle flow's unused share to
//! active flows immediately. That last property — **work conservation** — is
//! what makes "fair" and "fast" stop being a tradeoff.
//!
//! # Work conservation is a precondition, not a guarantee
//!
//! This module said work conservation happens, full stop. It does not: it holds
//! only while the quotas **over-subscribe** the batch, that is while
//! `round_base × Σweights` comfortably exceeds `batch_size`. A quota is a
//! ceiling on what one flow may contribute, and nothing hands one flow another
//! flow's unused share — a batch fills only because the flows that do have work
//! are still under their own ceilings.
//!
//! So with `round_base × Σweights == batch_size`, an idle flow's share is
//! simply lost and the batch runs short, however much work the others are
//! holding. `select_batch`'s caller is what makes this true, by configuring
//! `round_base` well above `batch_size / flows`. Both cases have a test, named
//! for which one they show.

#![deny(clippy::unwrap_used, clippy::expect_used)]
#![forbid(unsafe_code)]

pub mod batch;
pub mod flow;

pub use batch::{select_batch, Batch, FlowBacklog, Weight, WeightError};
pub use flow::{Dimension, FlowKey, FlowKeyError};
