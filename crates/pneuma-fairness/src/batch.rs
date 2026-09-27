//! Selecting one dispatch batch, fairly, without wasting capacity.
//!
//! The pure form of the query in `FRAMEWORK-FOUNDATIONS.md` §5. Postgres runs
//! it in production; this runs it in a test, which is how the fairness
//! properties get asserted at all — "no flow starves" is not something a
//! `SKIP LOCKED` query proves about itself.
//!
//! # The algorithm
//!
//! Each flow has a backlog ordered oldest-first, and a [`Weight`]. A round
//! admits at most `weight × round_base` items from any one flow — the quota —
//! and the batch is filled in `(round, pass, rotated flow rank)` order: pass `k` of a
//! round gives one item to every flow whose weight exceeds `k`, so a weight-3
//! flow takes three turns per round and a weight-1 flow takes one.
//!
//! Filling by rank alone — every flow's oldest, then every flow's second — is
//! what this file described before, and it is the unweighted algorithm the
//! sections below exist to reject.
//!
//! Weights work through **both** the quota and the interleaving, and it has to
//! be both. The quota caps how much a flow may take in a round; the
//! interleaving gives a weight-3 flow three turns for every one a weight-1 flow
//! gets, so the served ratio is 3:1 *while the batch limit binds*.
//!
//! Weighting only the quota is not enough, and fails silently. One item per
//! flow per rank serves every flow equally no matter its weight — weight then
//! merely decides the rank at which a flow runs out, so the ratio appears only
//! once `batch_size >= Σ quotas`. That is the non-over-subscribed case work
//! conservation forbids, so the weight column would be inert under exactly the
//! load it exists for, while looking correct in a test that picked a generous
//! batch. It did: the first version of this module measured 10/10 for weights
//! 1 and 3 at a binding batch, and its test passed because the batch size it
//! chose happened to equal the quota sum.
//!
//! # Two properties, and why the ordering decides both
//!
//! **Work conservation.** A flow with less backlog than its quota contributes
//! fewer candidates, and the batch fills from whoever else has work. Nothing is
//! reserved and left idle. This is what makes fair and fast compatible rather
//! than opposed — but it requires the quotas to *over-subscribe* the batch,
//! because quotas that sum to exactly the batch size leave an idle flow's share
//! unused by anyone.
//!
//! **No starvation.** Because quotas over-subscribe, something must arbitrate,
//! and that something is the batch limit. Filling rank-major means the limit
//! cuts *across* flows. Filling flow-major means it cuts *down* one, and the
//! flows sorted later get nothing.
//!
//! The design document originally specified `ORDER BY flow_key, rn`, which is
//! the flow-major form: two equal-weight flows with full backlogs and a batch
//! the size of one quota would give `acme` everything and `zeta` nothing, every
//! round, forever. The correction is recorded there; here it is a test —
//! `a_later_sorting_flow_is_not_starved` fails against the original ordering.
//!
//! `FlowKey` remains in the ordering as a deterministic tie-break, so a batch is
//! reproducible *for a given implementation*. It decides only who is served
//! first **within a single pass**, never how much anyone gets across a round —
//! which is the guarantee that matters, and a narrower one than "it no longer
//! decides who eats". An earlier revision claimed the broader version while the
//! implementation chunked by weight, and under that chunking key order did
//! decide the split of a partial round.
//!
//! That qualifier is deliberate. The tie-break here sorts by `FlowKey`'s derived
//! `Ord` — raw byte order — while Postgres sorts under the database collation,
//! and `en_US.UTF-8` reorders punctuation, so `a|b` and `ab` can compare
//! differently in the two. Within-rank order then diverges for keys differing
//! only in separators or case. Harmless for starvation and for the served
//! ratio, which is why it is a note rather than a defect, but a batch
//! reproduced here is not byte-identical to production unless the query pins
//! `ORDER BY ... flow_key COLLATE "C"`.
//!
//! # One flow, one backlog — surfaced, not assumed
//!
//! Each [`FlowKey`] should appear at most once. The query cannot violate this,
//! because `PARTITION BY flow_key` groups by definition; a Rust caller
//! assembling the slice can — from two paged queries, or by unioning a retry
//! backlog with a fresh one — and a flow listed twice **receives its quota
//! twice**, doubling its share at every other flow's expense.
//!
//! An earlier revision left that undetectable and called it a caller contract,
//! citing `pneuma-telemetry`'s duplicate-name collision as the same shape. It
//! is the same shape, and that citation was an argument against the choice
//! rather than for it: telemetry *reports* its collision, on the grounds that a
//! silent overwrite is otherwise unexplainable. So does this — see
//! [`Batch::duplicate_flows`]. Detection scans adjacent entries of the
//! already-sorted order, so it costs one comparison per flow and allocates only
//! when there is something to report — a caller that ignores the field pays
//! nothing.

use std::collections::{BTreeSet, BinaryHeap};
use std::num::NonZeroU32;

use crate::flow::FlowKey;

/// A flow's proportional share of a round.
///
/// Zero is unrepresentable. A weight of zero is not a tuning value, it is
/// starvation written as configuration: the flow would accrue backlog forever
/// while looking correctly configured. Pausing a flow should be an explicit,
/// visible action, not a number that reads like a dial.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Weight(NonZeroU32);

impl Weight {
    /// The default share.
    pub const ONE: Weight = Weight(NonZeroU32::MIN);

    /// Builds a weight, rejecting zero.
    pub fn new(value: u32) -> Result<Self, WeightError> {
        NonZeroU32::new(value).map(Weight).ok_or(WeightError::Zero)
    }

    /// The underlying share.
    pub fn get(self) -> u32 {
        self.0.get()
    }
}

/// The only way a weight can be invalid.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum WeightError {
    #[error("a flow weight must be at least 1; zero would starve the flow silently")]
    Zero,
}

/// One flow's pending work, oldest first.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FlowBacklog<'a, T> {
    /// Which flow this backlog belongs to.
    pub flow: &'a FlowKey,
    /// The flow's share of a round.
    pub weight: Weight,
    /// Pending items, oldest first — the `ORDER BY created_at` of the query.
    pub items: &'a [T],
}

impl<T> FlowBacklog<'_, T> {
    /// How many items this flow may contribute to one round.
    ///
    /// `weight × round_base`, saturating: the product of two `u32`s can exceed
    /// `u32`, and a quota larger than any real backlog is indistinguishable
    /// from an unbounded one, so saturation loses nothing.
    fn quota(&self, round_base: NonZeroU32) -> usize {
        (self.weight.get() as usize).saturating_mul(round_base.get() as usize)
    }
}

/// A candidate's place in the dispatch order:
/// `(round, pass, rotated flow rank, position in flow)`.
///
/// The first three are the sort key — the pure form of
/// `ORDER BY (rn - 1) / w, (rn - 1) % w, <rotated flow rank>`. The fourth is
/// not part of the ordering; it rides along so the item can be recovered.
type ScheduleKey = (usize, usize, usize, usize);

/// One round's dispatch decision.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Batch<'a, T> {
    /// The items to dispatch, in order. Never longer than `batch_size`.
    pub items: Vec<&'a T>,
    /// Flow keys that appeared in more than one backlog, and so took their
    /// quota more than once — see the module docs.
    ///
    /// Empty in normal operation. A non-empty set means this round was unfair
    /// and names who benefited.
    pub duplicate_flows: BTreeSet<FlowKey>,
}

/// Chooses the items to dispatch this round.
///
/// `round` **must advance between calls** — a dispatcher's monotonically
/// increasing cycle counter is the intended source. It rotates the tie-break so
/// the remainder of a partial pass circulates instead of always landing on the
/// same flow.
///
/// Passing a constant is the one way to misuse this that nothing detects. Every
/// individual batch remains provably fair, so the failure is invisible at the
/// timescale anyone would test at, while the earlier-sorting flow quietly
/// collects one extra item per round: measured at 800/600 over 200 rounds
/// before the rotation existed. It is a bare `u64` rather than a newtype
/// because a dispatcher already has such a counter and wrapping one would be
/// ceremony — but it is the parameter to look at first if fairness drifts.
pub fn select_batch<'a, T>(
    backlogs: &[FlowBacklog<'a, T>],
    round_base: NonZeroU32,
    batch_size: usize,
    round: u64,
) -> Batch<'a, T> {
    // Sorted by flow key so the tie-break within a pass is deterministic, and
    // so duplicate keys land adjacent for `repeated_flows`.
    let mut order: Vec<usize> = (0..backlogs.len()).collect();
    order.sort_by(|left, right| backlogs[*left].flow.cmp(backlogs[*right].flow));

    // Duplicates are found here, BEFORE the rotation below. The scan relies on
    // equal keys being adjacent, and a rotation whose offset lands inside a run
    // of equal keys splits it across the wrap point, so the scan sees no
    // adjacent pair and reports nothing. The unfairness still happens; only the
    // evidence disappears, and it blinks in and out at round-counter frequency.
    // Measured before this was moved: dup=1, dup=0, dup=1 over three rounds.
    let duplicate_flows = repeated_flows(backlogs, &order);

    // Rotated by the round counter so the tie-break moves. A batch that does
    // not divide the round total must hand the remainder to somebody, and with
    // a fixed order that is the same flow every time: measured at 800 vs 600
    // over 200 rounds of a 7-item batch, one extra item per round forever,
    // while every individual batch was provably fair. Rotating circulates it.
    if !order.is_empty() {
        let offset = (round % order.len() as u64) as usize;
        order.rotate_left(offset);
    }

    if batch_size == 0 {
        return Batch {
            items: Vec::new(),
            duplicate_flows,
        };
    }

    let mut candidates: Vec<&[T]> = Vec::with_capacity(backlogs.len());

    // Each flow's candidates: its backlog truncated to its quota, and to the
    // batch size — no flow can contribute more than a whole batch, and the cap
    // bounds the schedule below to `flows x batch_size` regardless of how large
    // a quota is configured.
    let mut total_candidates = 0usize;
    for backlog in backlogs {
        let take = backlog
            .quota(round_base)
            .min(backlog.items.len())
            .min(batch_size);
        total_candidates = total_candidates.saturating_add(take);
        candidates.push(&backlog.items[..take]);
    }

    // The dispatch order, built as a sort key rather than walked as a loop nest.
    //
    // Candidate `j` of a flow with weight `w` belongs to round `j / w` and to
    // pass `j % w` within it, so ordering by `(round, pass, flow rank)` produces
    // `ORDER BY (rn - 1) / w, (rn - 1) % w, <rotated flow rank>` — the query
    // this module is the pure form of, with the tie-break rotated per round.
    //
    // `rank` is the flow's position in the key-sorted order **after rotation**,
    // not the key itself. Porting this to SQL with a plain `flow_key` tie-break
    // reintroduces the 800/600 bias the rotation exists to remove.
    //
    // Three earlier versions were each a denial-of-service vector of the same
    // family, and the progression is worth keeping. Two walked
    // `rounds x passes x flows`, visiting combinations that hold no item: the
    // first ran to the largest weight, so `u32::MAX` spun four billion times for
    // a ten-item backlog; clamping that to the deepest backlog only made the
    // spin quadratic, measured at 0.90s in release for 20,001 useful items. The
    // third fixed the spin but materialised and sorted the entire schedule,
    // which is `O(flows x batch_size)` held at once — 512 flows and a batch of
    // 5,000 retained roughly 82MB to return 5,000 items.
    //
    // Streaming through a bounded heap holds at most `batch_size` entries,
    // evicting the largest key whenever a smaller one arrives, which is exactly
    // the set a full sort would have kept. The same 512-flow case now runs in
    // 6ms.
    let capacity = total_candidates.min(batch_size);
    let mut keep: BinaryHeap<ScheduleKey> = BinaryHeap::with_capacity(capacity);
    for (rank, index) in order.iter().enumerate() {
        let share = backlogs[*index].weight.get() as usize;
        for position in 0..candidates[*index].len() {
            let key: ScheduleKey = (position / share, position % share, rank, position);
            if keep.len() < batch_size {
                keep.push(key);
            } else if keep.peek().is_some_and(|largest| key < *largest) {
                keep.pop();
                keep.push(key);
            }
        }
    }

    let mut ordered = keep.into_vec();
    ordered.sort_unstable();
    let chosen: Vec<&'a T> = ordered
        .into_iter()
        .map(|(_, _, rank, position)| &candidates[order[rank]][position])
        .collect();

    Batch {
        items: chosen,
        duplicate_flows,
    }
}

/// Flow keys appearing in more than one backlog.
///
/// Takes the key-sorted `order` the caller has already built, so duplicates are
/// adjacent and a single scan finds them. Allocates only when there is
/// something to report.
fn repeated_flows<T>(backlogs: &[FlowBacklog<'_, T>], order: &[usize]) -> BTreeSet<FlowKey> {
    let mut repeated = BTreeSet::new();
    for pair in order.windows(2) {
        let (left, right) = (backlogs[pair[0]].flow, backlogs[pair[1]].flow);
        if left == right {
            repeated.insert(left.clone());
        }
    }
    repeated
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;
    use crate::flow::Dimension;

    fn flow(name: &str) -> FlowKey {
        let Ok(dimension) = Dimension::new("tenant_id", name) else {
            panic!("{name} should be a valid dimension value");
        };
        match FlowKey::new(&[dimension]) {
            Ok(key) => key,
            Err(err) => panic!("should build a key: {err}"),
        }
    }

    fn base(n: u32) -> NonZeroU32 {
        match NonZeroU32::new(n) {
            Some(value) => value,
            None => panic!("round base must be non-zero"),
        }
    }

    fn weight(n: u32) -> Weight {
        match Weight::new(n) {
            Ok(w) => w,
            Err(err) => panic!("{n} should be a valid weight: {err}"),
        }
    }

    /// Labels each item with its flow so a batch can be counted per flow.
    fn items(prefix: &str, count: usize) -> Vec<String> {
        (0..count).map(|i| format!("{prefix}{i}")).collect()
    }

    fn count_by_prefix(batch: &[&String], prefix: &str) -> usize {
        batch.iter().filter(|item| item.starts_with(prefix)).count()
    }

    #[test]
    fn a_later_sorting_flow_is_not_starved() {
        // THE test for this module. The design document originally specified
        // ORDER BY flow_key, rn -- flow-major -- which gives `acme` the entire
        // batch and `zeta` nothing, every round, forever. Rank-major splits it.
        let (acme, zeta) = (flow("acme"), flow("zeta"));
        let (acme_items, zeta_items) = (items("acme-", 100), items("zeta-", 100));
        let backlogs = [
            FlowBacklog {
                flow: &acme,
                weight: Weight::ONE,
                items: &acme_items,
            },
            FlowBacklog {
                flow: &zeta,
                weight: Weight::ONE,
                items: &zeta_items,
            },
        ];

        // Quotas deliberately over-subscribe the batch: 100 each, batch of 100.
        // That over-subscription is what work conservation requires, and it is
        // exactly when the flow-major ordering starves.
        let batch = select_batch(&backlogs, base(100), 100, 0);

        assert_eq!(batch.items.len(), 100);
        assert_eq!(count_by_prefix(&batch.items, "acme-"), 50);
        assert_eq!(count_by_prefix(&batch.items, "zeta-"), 50);
    }

    #[test]
    fn equal_weights_split_evenly_at_every_weight_not_just_one() {
        // The starvation test above uses Weight::ONE, where a per-flow chunk of
        // size 1 is indistinguishable from a fair interleave -- so it could not
        // see the regression that chunking introduced. At weight 3 the first
        // implementation of weighted interleaving gave 2/0 for a batch of 2.
        for share in [1u32, 2, 3, 5] {
            let (first, second) = (flow("aaa"), flow("zzz"));
            let (first_items, second_items) = (items("a-", 100), items("z-", 100));
            let backlogs = [
                FlowBacklog {
                    flow: &first,
                    weight: weight(share),
                    items: &first_items,
                },
                FlowBacklog {
                    flow: &second,
                    weight: weight(share),
                    items: &second_items,
                },
            ];

            for batch_size in [2usize, 4, 10, 20] {
                let batch = select_batch(&backlogs, base(10), batch_size, 0);
                let (a, z) = (
                    count_by_prefix(&batch.items, "a-"),
                    count_by_prefix(&batch.items, "z-"),
                );
                assert_eq!(
                    a,
                    batch_size / 2,
                    "weight {share}, batch {batch_size}: got {a}/{z}"
                );
                assert_eq!(z, batch_size / 2, "weight {share}, batch {batch_size}");
            }
        }
    }

    #[test]
    fn a_partial_round_is_dealt_round_robin_not_given_to_the_first_flow() {
        // The precise shape of the regression: a batch that does not divide the
        // round total must not hand the remainder to whoever sorts first.
        let (first, second) = (flow("aaa"), flow("zzz"));
        let (first_items, second_items) = (items("a-", 100), items("z-", 100));
        let backlogs = [
            FlowBacklog {
                flow: &first,
                weight: weight(3),
                items: &first_items,
            },
            FlowBacklog {
                flow: &second,
                weight: weight(3),
                items: &second_items,
            },
        ];

        // 5 is odd, so one flow must get the extra -- but only one.
        let batch = select_batch(&backlogs, base(10), 5, 0);
        let (a, z) = (
            count_by_prefix(&batch.items, "a-"),
            count_by_prefix(&batch.items, "z-"),
        );
        assert_eq!(batch.items.len(), 5);
        assert!(
            a.abs_diff(z) <= 1,
            "a partial round must not skew by more than one item: {a}/{z}"
        );
    }

    #[test]
    fn the_batch_interleaves_rather_than_draining_one_flow() {
        let (acme, zeta) = (flow("acme"), flow("zeta"));
        let (acme_items, zeta_items) = (items("acme-", 3), items("zeta-", 3));
        let backlogs = [
            FlowBacklog {
                flow: &acme,
                weight: Weight::ONE,
                items: &acme_items,
            },
            FlowBacklog {
                flow: &zeta,
                weight: Weight::ONE,
                items: &zeta_items,
            },
        ];

        let batch = select_batch(&backlogs, base(10), 6, 0);
        let order: Vec<&str> = batch.items.iter().map(|item| item.as_str()).collect();
        assert_eq!(
            order,
            vec!["acme-0", "zeta-0", "acme-1", "zeta-1", "acme-2", "zeta-2"]
        );
    }

    #[test]
    fn weight_sets_the_served_ratio_while_the_batch_limit_binds() {
        // The regime that matters. Quotas 10 and 30 over-subscribe a batch of
        // 20, which is what work conservation requires -- so the batch limit
        // arbitrates, and the ratio must still be 1:3.
        //
        // The first version of this module scored 10/10 here. Its test passed
        // only because it chose batch_size = 40, exactly the quota sum, the one
        // size at which weights show without weighted interleaving.
        let (light, heavy) = (flow("aaa"), flow("bbb"));
        let (light_items, heavy_items) = (items("light-", 100), items("heavy-", 100));
        let backlogs = [
            FlowBacklog {
                flow: &light,
                weight: Weight::ONE,
                items: &light_items,
            },
            FlowBacklog {
                flow: &heavy,
                weight: weight(3),
                items: &heavy_items,
            },
        ];

        let batch = select_batch(&backlogs, base(10), 20, 0);
        assert_eq!(batch.items.len(), 20);
        assert_eq!(count_by_prefix(&batch.items, "light-"), 5, "1 share of 4");
        assert_eq!(count_by_prefix(&batch.items, "heavy-"), 15, "3 shares of 4");
    }

    #[test]
    fn the_quota_still_caps_a_flow_when_the_batch_does_not() {
        // The other half: with room to spare, weights stop mattering and the
        // quota is what bounds each flow.
        let (light, heavy) = (flow("aaa"), flow("bbb"));
        let (light_items, heavy_items) = (items("light-", 100), items("heavy-", 100));
        let backlogs = [
            FlowBacklog {
                flow: &light,
                weight: Weight::ONE,
                items: &light_items,
            },
            FlowBacklog {
                flow: &heavy,
                weight: weight(3),
                items: &heavy_items,
            },
        ];

        let batch = select_batch(&backlogs, base(10), 1000, 0);
        assert_eq!(count_by_prefix(&batch.items, "light-"), 10, "quota 1 x 10");
        assert_eq!(count_by_prefix(&batch.items, "heavy-"), 30, "quota 3 x 10");
    }

    #[test]
    fn a_heavier_flow_is_interleaved_not_batched_at_the_end() {
        // Weighted interleaving, visible in the order: the weight-3 flow takes
        // three turns for every one the weight-1 flow takes, rather than being
        // appended after it.
        let (light, heavy) = (flow("aaa"), flow("bbb"));
        let (light_items, heavy_items) = (items("l", 4), items("h", 12));
        let backlogs = [
            FlowBacklog {
                flow: &light,
                weight: Weight::ONE,
                items: &light_items,
            },
            FlowBacklog {
                flow: &heavy,
                weight: weight(3),
                items: &heavy_items,
            },
        ];

        let batch = select_batch(&backlogs, base(10), 8, 0);
        let order: Vec<&str> = batch.items.iter().map(|item| item.as_str()).collect();
        assert_eq!(order, vec!["l0", "h0", "h1", "h2", "l1", "h3", "h4", "h5"]);
    }

    #[test]
    fn an_over_subscribed_round_fills_the_batch_despite_a_quiet_flow() {
        // Work conservation, and the condition it depends on. Quotas here are
        // 10 each against a batch of 12, so they over-subscribe it: the quiet
        // flow contributes the 2 it has, the busy one is still under its own
        // ceiling of 10, and the batch fills.
        //
        // The earlier name for this was
        // `an_idle_flows_share_is_redistributed_not_wasted`, which is not what
        // happens. The busy flow takes exactly its own quota of 10 -- none of
        // the quiet flow's unused 8 moves anywhere. The batch is full because
        // 10 + 10 > 12, not because a share was redistributed, and the test
        // below shows what happens when that stops being true.
        let (busy, quiet) = (flow("aaa"), flow("bbb"));
        let (busy_items, quiet_items) = (items("busy-", 100), items("quiet-", 2));
        let backlogs = [
            FlowBacklog {
                flow: &busy,
                weight: Weight::ONE,
                items: &busy_items,
            },
            FlowBacklog {
                flow: &quiet,
                weight: Weight::ONE,
                items: &quiet_items,
            },
        ];

        let batch = select_batch(&backlogs, base(10), 12, 0);
        assert_eq!(count_by_prefix(&batch.items, "quiet-"), 2, "all it had");
        assert_eq!(
            count_by_prefix(&batch.items, "busy-"),
            10,
            "the unused share went to work, not to waste"
        );
    }

    #[test]
    fn a_batch_runs_short_when_the_quotas_only_just_cover_it() {
        // The precondition failing, which is the half the module used to claim
        // could not happen. Quotas of 6 each against a batch of 12: they sum to
        // exactly the batch size rather than over-subscribing it. The quiet
        // flow has 2 items, the busy one has 100 -- and the batch comes back
        // with 8, not 12, because a quota is a ceiling and nothing hands the
        // quiet flow's unused 4 to anybody.
        //
        // This is a real configuration hazard rather than a curiosity: it is
        // what a caller gets by setting `round_base = batch_size / flows`,
        // which is the obvious thing to reach for.
        let (busy, quiet) = (flow("aaa"), flow("bbb"));
        let (busy_items, quiet_items) = (items("busy-", 100), items("quiet-", 2));
        let backlogs = [
            FlowBacklog {
                flow: &busy,
                weight: Weight::ONE,
                items: &busy_items,
            },
            FlowBacklog {
                flow: &quiet,
                weight: Weight::ONE,
                items: &quiet_items,
            },
        ];

        let batch = select_batch(&backlogs, base(6), 12, 0);
        assert_eq!(count_by_prefix(&batch.items, "quiet-"), 2, "all it had");
        assert_eq!(
            count_by_prefix(&batch.items, "busy-"),
            6,
            "its own quota, and not one item of the quiet flow's unused share"
        );
        assert_eq!(
            batch.items.len(),
            8,
            "the batch runs short: work conservation needs the quotas to \
             over-subscribe the batch, and here they do not"
        );
    }

    #[test]
    fn a_flow_never_exceeds_its_quota_even_with_capacity_to_spare() {
        // The other half of fairness: a busy flow cannot take a quiet flow's
        // future share just because the batch has room this round.
        let only = flow("aaa");
        let only_items = items("only-", 100);
        let backlogs = [FlowBacklog {
            flow: &only,
            weight: Weight::ONE,
            items: &only_items,
        }];

        let batch = select_batch(&backlogs, base(5), 50, 0);
        assert_eq!(
            batch.items.len(),
            5,
            "quota bounds the round, not the batch size"
        );
    }

    #[test]
    fn a_repeated_flow_key_takes_its_quota_twice() {
        // A caller contract violation, pinned so the consequence is known
        // rather than discovered. The same flow listed twice gets two quotas
        // and doubles its share -- the exact failure this module prevents,
        // reintroduced by the caller. Same shape as the duplicate-name
        // collision in pneuma-telemetry's health report.
        let (repeated, other) = (flow("aaa"), flow("bbb"));
        let (first_half, second_half) = (items("dup-a", 10), items("dup-b", 10));
        let other_items = items("other-", 10);
        let backlogs = [
            FlowBacklog {
                flow: &repeated,
                weight: Weight::ONE,
                items: &first_half,
            },
            FlowBacklog {
                flow: &repeated,
                weight: Weight::ONE,
                items: &second_half,
            },
            FlowBacklog {
                flow: &other,
                weight: Weight::ONE,
                items: &other_items,
            },
        ];

        // Swept across rounds, not fixed at 0. The rotation is applied to the
        // same `order` this detection scans, and an offset landing inside a run
        // of equal keys splits it across the wrap point -- so at round 0 the
        // report was correct and at round 1 it was empty while the unfairness
        // continued. A test pinned at round 0 saw none of that.
        for round in 0..6u64 {
            let batch = select_batch(&backlogs, base(2), 100, round);
            assert!(
                batch.duplicate_flows.contains(&repeated),
                "round {round}: the collision must be reported every round, \
                 not only when the rotation happens to keep the keys adjacent"
            );
        }

        let batch = select_batch(&backlogs, base(2), 100, 0);
        let duplicated = count_by_prefix(&batch.items, "dup-");
        assert_eq!(duplicated, 4, "two entries, two quotas of 2");
        assert_eq!(
            count_by_prefix(&batch.items, "other-"),
            2,
            "the honest flow still gets exactly one quota"
        );
        assert!(
            duplicated > count_by_prefix(&batch.items, "other-"),
            "which is the unfairness: listing a flow twice doubles its share"
        );
    }

    #[test]
    fn a_huge_weight_beside_a_deep_backlog_does_not_spin() {
        // Both earlier implementations walked rounds x passes x flows and spun
        // over combinations holding no item: the first ran to the largest
        // weight (u32::MAX iterations for ten items), the second was quadratic
        // in the deepest backlog (0.90s in release for 20,001 items, rising
        // with the square). Enumerating the items that exist cannot spin, so
        // this completes in the time it takes to sort them.
        let (deep, huge) = (flow("aaa"), flow("bbb"));
        let deep_items = items("deep-", 20_000);
        let huge_items = items("huge-", 1);
        let backlogs = [
            FlowBacklog {
                flow: &deep,
                weight: Weight::ONE,
                items: &deep_items,
            },
            FlowBacklog {
                flow: &huge,
                weight: weight(u32::MAX),
                items: &huge_items,
            },
        ];

        let batch = select_batch(&backlogs, base(20_000), 100_000, 0);
        assert_eq!(batch.items.len(), 20_001, "every item, none dropped");
        assert_eq!(count_by_prefix(&batch.items, "huge-"), 1);
    }

    #[test]
    fn a_flow_cannot_contribute_more_than_one_batch() {
        // The cap that bounds the schedule. A quota far larger than the batch
        // must not make the schedule larger than the batch either.
        let only = flow("aaa");
        let only_items = items("only-", 10_000);
        let backlogs = [FlowBacklog {
            flow: &only,
            weight: Weight::ONE,
            items: &only_items,
        }];

        let batch = select_batch(&backlogs, base(10_000), 5, 0);
        assert_eq!(batch.items.len(), 5);
        let order: Vec<&str> = batch.items.iter().map(|item| item.as_str()).collect();
        assert_eq!(
            order,
            vec!["only-0", "only-1", "only-2", "only-3", "only-4"],
            "still the oldest five"
        );
    }

    #[test]
    fn repeated_rounds_do_not_accumulate_a_bias_toward_the_first_key() {
        // The property no single-batch test can see, and the one most likely to
        // bite in production: a batch that cuts mid-pass gives the extra item to
        // whoever sorts first, and the tie-break is identical every round. If
        // that skew persists, the earlier-sorting flow collects one extra item
        // per round forever while every individual batch looks provably fair.
        //
        // This drains real backlogs over many rounds and checks the cumulative
        // split, which is the timescale a starving tenant actually experiences.
        let (first, second) = (flow("aaa"), flow("zzz"));
        let mut first_left: Vec<String> = items("a-", 2_000);
        let mut second_left: Vec<String> = items("z-", 2_000);
        let (mut served_first, mut served_second) = (0usize, 0usize);

        // An odd batch size, so every round must hand someone the extra item.
        for round in 0..200u64 {
            let backlogs = [
                FlowBacklog {
                    flow: &first,
                    weight: Weight::ONE,
                    items: &first_left,
                },
                FlowBacklog {
                    flow: &second,
                    weight: Weight::ONE,
                    items: &second_left,
                },
            ];
            let batch = select_batch(&backlogs, base(5), 7, round);
            let took_first = count_by_prefix(&batch.items, "a-");
            let took_second = count_by_prefix(&batch.items, "z-");
            served_first += took_first;
            served_second += took_second;
            first_left.drain(..took_first);
            second_left.drain(..took_second);
        }

        let skew = served_first.abs_diff(served_second);
        let total = served_first + served_second;
        assert!(
            total > 1_000,
            "the simulation must actually do work: {total}"
        );
        assert!(
            skew * 20 <= total,
            "cumulative skew {skew} over {total} served ({served_first} vs \
             {served_second}) -- a per-round bias is accumulating"
        );
    }

    #[test]
    fn an_empty_input_yields_an_empty_batch() {
        let backlogs: [FlowBacklog<'_, String>; 0] = [];
        assert!(select_batch(&backlogs, base(10), 10, 0).items.is_empty());
    }

    #[test]
    fn a_zero_batch_takes_nothing() {
        let only = flow("aaa");
        let only_items = items("only-", 10);
        let backlogs = [FlowBacklog {
            flow: &only,
            weight: Weight::ONE,
            items: &only_items,
        }];
        assert!(select_batch(&backlogs, base(10), 0, 0).items.is_empty());
    }

    #[test]
    fn a_flow_with_no_backlog_is_skipped_without_disturbing_the_others() {
        let (empty, busy) = (flow("aaa"), flow("bbb"));
        let (no_items, busy_items) = (Vec::<String>::new(), items("busy-", 4));
        let backlogs = [
            FlowBacklog {
                flow: &empty,
                weight: Weight::ONE,
                items: &no_items,
            },
            FlowBacklog {
                flow: &busy,
                weight: Weight::ONE,
                items: &busy_items,
            },
        ];

        let batch = select_batch(&backlogs, base(10), 10, 0);
        assert_eq!(batch.items.len(), 4);
    }

    #[test]
    fn selection_is_deterministic_regardless_of_input_order() {
        // FlowKey stays in the ordering as a tie-break, so a batch is
        // reproducible even if the caller hands the backlogs over in a
        // different order -- which a SKIP LOCKED scan may well do.
        let (acme, zeta) = (flow("acme"), flow("zeta"));
        let (acme_items, zeta_items) = (items("acme-", 3), items("zeta-", 3));
        let forward = [
            FlowBacklog {
                flow: &acme,
                weight: Weight::ONE,
                items: &acme_items,
            },
            FlowBacklog {
                flow: &zeta,
                weight: Weight::ONE,
                items: &zeta_items,
            },
        ];
        let reversed = [
            FlowBacklog {
                flow: &zeta,
                weight: Weight::ONE,
                items: &zeta_items,
            },
            FlowBacklog {
                flow: &acme,
                weight: Weight::ONE,
                items: &acme_items,
            },
        ];

        assert_eq!(
            select_batch(&forward, base(10), 6, 0).items,
            select_batch(&reversed, base(10), 6, 0).items
        );
    }

    #[test]
    fn the_oldest_items_go_first_within_a_flow() {
        let only = flow("aaa");
        let only_items = items("item-", 5);
        let backlogs = [FlowBacklog {
            flow: &only,
            weight: Weight::ONE,
            items: &only_items,
        }];

        let batch = select_batch(&backlogs, base(10), 3, 0);
        let order: Vec<&str> = batch.items.iter().map(|item| item.as_str()).collect();
        assert_eq!(order, vec!["item-0", "item-1", "item-2"]);
    }

    #[test]
    fn an_enormous_quota_saturates_instead_of_overflowing() {
        // weight x round_base is a u32 product that does not fit in u32. A
        // quota larger than any real backlog is indistinguishable from an
        // unbounded one, so saturation loses nothing -- but wrapping would
        // silently produce a tiny quota.
        let only = flow("aaa");
        let only_items = items("only-", 10);
        let backlogs = [FlowBacklog {
            flow: &only,
            weight: weight(u32::MAX),
            items: &only_items,
        }];

        let batch = select_batch(&backlogs, base(u32::MAX), 100, 0);
        assert_eq!(
            batch.items.len(),
            10,
            "the whole backlog, not a wrapped remnant"
        );
    }

    #[test]
    fn a_zero_weight_is_not_representable() {
        // Starvation written as configuration: the flow would accrue backlog
        // forever while looking correctly configured.
        assert_eq!(Weight::new(0), Err(WeightError::Zero));
        assert_eq!(
            WeightError::Zero.to_string(),
            "a flow weight must be at least 1; zero would starve the flow silently"
        );
        assert_eq!(Weight::ONE.get(), 1);
        assert!(Weight::new(7).is_ok_and(|w| w.get() == 7));
    }

    proptest! {
        /// The fair-share invariant, asserted over generated parameters rather
        /// than chosen ones.
        ///
        /// Both of this module's regressions were invisible to their own tests
        /// for the same reason: each test fixed every parameter but one, at a
        /// value where the defect vanished. The first chose a batch equal to the
        /// quota sum, the only size at which unweighted fill looks right; the
        /// second chose weight 1, the only weight at which chunking looks fair.
        ///
        /// This assertion has no such value to hide behind. For any two flows
        /// that both still have candidates left over, the served amounts must be
        /// proportional to their weights to within one round — which is the best
        /// any batch limit can do, and is exactly the property both bugs broke.
        #[test]
        fn service_is_proportional_to_weight(
            shares in prop::collection::vec(1u32..5, 2..5),
            depth in 1usize..40,
            round_base in 1u32..6,
            batch_size in 1usize..60,
            // Generated, not fixed. An earlier version passed a literal 0 here
            // while generating everything else -- in the one test written
            // specifically to escape fixed-parameter blindness. Both of the
            // round-dependent defects in this module would have been invisible
            // to it.
            round in 0u64..64,
        ) {
            let keys: Vec<FlowKey> = (0..shares.len()).map(|i| flow(&format!("f{i}"))).collect();
            let backlogs_items: Vec<Vec<String>> = (0..shares.len())
                .map(|i| items(&format!("f{i}-"), depth))
                .collect();
            let backlogs: Vec<FlowBacklog<'_, String>> = shares
                .iter()
                .enumerate()
                .map(|(i, share)| FlowBacklog {
                    flow: &keys[i],
                    weight: weight(*share),
                    items: &backlogs_items[i],
                })
                .collect();

            let batch = select_batch(&backlogs, base(round_base), batch_size, round);
            prop_assert!(batch.items.len() <= batch_size);

            let served: Vec<usize> = (0..shares.len())
                .map(|i| count_by_prefix(&batch.items, &format!("f{i}-")))
                .collect();

            // How much each flow could possibly have taken: its quota, but no
            // more than its backlog holds and no more than one batch. Getting
            // this wrong is what made the first draft of this test fail against
            // correct code -- it compared a flow capped by its own backlog as
            // though it had been capped by its weight.
            let available: Vec<usize> = shares
                .iter()
                .map(|share| {
                    (*share as usize)
                        .saturating_mul(round_base as usize)
                        .min(depth)
                        .min(batch_size)
                })
                .collect();

            for (i, share) in shares.iter().enumerate() {
                // Nobody exceeds what they were entitled to.
                prop_assert!(served[i] <= available[i]);

                // Proportionality, among flows that could have taken more.
                // A flow that ran out of work is not being treated unfairly.
                let i_capped = served[i] >= available[i];
                for (j, other) in shares.iter().enumerate() {
                    let j_capped = served[j] >= available[j];
                    if i_capped || j_capped {
                        continue;
                    }
                    let left = served[i] * (*other as usize);
                    let right = served[j] * (*share as usize);
                    let slack = (*share as usize) * (*other as usize);
                    prop_assert!(
                        left.abs_diff(right) <= slack,
                        "flows {i} and {j} served {}/{} at weights {share}/{other}",
                        served[i], served[j]
                    );

                    // The sharper property, and the one that matters: at equal
                    // weight the two must differ by at most one ITEM, not one
                    // round. The proportional bound above tolerates a whole
                    // round of skew and so does not catch chunked dealing --
                    // verified by re-introducing it, which the bound above
                    // passes and this assertion fails. Fairness at the
                    // granularity of a round is not fairness when the batch
                    // limit cuts inside one.
                    if share == other {
                        prop_assert!(
                            served[i].abs_diff(served[j]) <= 1,
                            "equal weights {share} served {}/{} for flows {i} and {j}",
                            served[i], served[j]
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn weight_derives() {
        assert_eq!(Weight::ONE, Weight::ONE);
        assert!(Weight::ONE < weight(2));
        assert!(format!("{:?}", Weight::ONE).contains('1'));

        use std::collections::HashSet;
        let set: HashSet<_> = [Weight::ONE, Weight::ONE].into();
        assert_eq!(set.len(), 1);
    }

    #[test]
    fn backlog_derives() {
        let only = flow("aaa");
        let only_items = items("only-", 1);
        let backlog = FlowBacklog {
            flow: &only,
            weight: Weight::ONE,
            items: &only_items,
        };
        assert_eq!(backlog.clone(), backlog);
        assert!(format!("{backlog:?}").contains("only-0"));
    }
}
