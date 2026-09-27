//! The fair selection, without a database.
//!
//! Everything between the two queries is pure, which is what lets a round be
//! examined here: the grouping, the weights, the quota base, and the alarm for
//! a condition that cannot happen in production and must still be checked.

use std::collections::BTreeMap;

use chrono::{TimeZone, Utc};
use pneuma_admission::{alarm, backlogs, group, round_base};
use pneuma_fairness::{select_batch, FlowBacklog, Weight};
use pneuma_store::Queued;
use serde_json::json;

/// A queued row, in the order `queued_backlogs` returns them.
fn queued(tenant: &str, run: &str, nth: u32) -> Queued {
    let Some(enqueued_at) = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).single() else {
        panic!("a real instant");
    };
    Queued {
        run_id: run.to_owned(),
        tenant_id: tenant.to_owned(),
        payload: json!({}),
        enqueued_at: enqueued_at + chrono::Duration::seconds(i64::from(nth)),
    }
}

#[test]
fn rows_are_grouped_by_tenant_in_the_order_the_query_returned_them() {
    // `queued_backlogs` orders by `(tenant_id, enqueued_at, run_id)`, so one
    // tenant's rows are already adjacent. Grouping on a change of tenant keeps
    // the arrival order the query established rather than imposing another.
    let rows = vec![
        queued("acme", "a1", 0),
        queued("acme", "a2", 1),
        queued("globex", "g1", 0),
    ];
    let Ok(groups) = group(&rows, &BTreeMap::new()) else {
        panic!("one dimension with a constant name cannot fail");
    };
    assert_eq!(groups.len(), 2, "{groups:?}");
    assert_eq!(groups[0].flow.as_str(), "tenant=acme");
    assert_eq!(groups[0].span, 0..2);
    assert_eq!(groups[1].flow.as_str(), "tenant=globex");
    assert_eq!(groups[1].span, 2..3);

    let borrowed = backlogs(&rows, &groups);
    assert_eq!(borrowed.len(), 2);
    assert_eq!(borrowed[0].items.len(), 2);
    assert_eq!(borrowed[0].items[0].run_id, "a1", "oldest first");
    assert_eq!(borrowed[1].items.len(), 1);
}

#[test]
fn an_empty_queue_is_no_groups_rather_than_an_error() {
    let Ok(groups) = group(&[], &BTreeMap::new()) else {
        panic!("nothing queued is not a failure");
    };
    assert!(groups.is_empty());
    assert!(backlogs(&[], &groups).is_empty());
}

#[test]
fn an_unknown_tenant_gets_an_ordinary_share_rather_than_none() {
    // A tenant in the queue and not in the configuration is a new customer, or
    // a config that has not caught up. Refusing to dispatch their work until
    // someone edits a file is a worse answer than an ordinary share.
    let Ok(heavy) = Weight::new(5) else {
        panic!("5 is a weight");
    };
    let mut weights = BTreeMap::new();
    weights.insert("acme".to_owned(), heavy);

    let rows = vec![queued("acme", "a1", 0), queued("newcomer", "n1", 0)];
    let Ok(groups) = group(&rows, &weights) else {
        panic!("should group");
    };
    assert_eq!(groups[0].weight, heavy, "the configured one is honoured");
    assert_eq!(
        groups[1].weight,
        Weight::ONE,
        "and the unknown one still runs"
    );
}

#[test]
fn the_quota_base_over_subscribes_the_batch_so_a_round_can_fill() {
    // Work conservation is a precondition, not a guarantee: no flow is ever
    // given another's unused share, so a batch fills only while the busy flows
    // are under their own ceilings. `round_base = batch_size / flows` -- the
    // obvious choice -- makes the quotas sum to exactly the batch, and a quiet
    // flow's share is then simply lost.
    let busy = queued("busy", "b", 0);
    let quiet = queued("quiet", "q", 0);
    let busy_items: Vec<Queued> = (0..100)
        .map(|n| queued("busy", &format!("b{n}"), n))
        .collect();
    let quiet_items = vec![quiet.clone()];
    let Ok(busy_flow) =
        pneuma_fairness::FlowKey::new(&[
            pneuma_fairness::Dimension::new("tenant", "busy").unwrap_or_else(|_| unreachable())
        ])
    else {
        panic!("a key");
    };
    let Ok(quiet_flow) =
        pneuma_fairness::FlowKey::new(&[
            pneuma_fairness::Dimension::new("tenant", "quiet").unwrap_or_else(|_| unreachable())
        ])
    else {
        panic!("a key");
    };
    let _ = busy;

    let backlogs = [
        FlowBacklog {
            flow: &busy_flow,
            weight: Weight::ONE,
            items: &busy_items,
        },
        FlowBacklog {
            flow: &quiet_flow,
            weight: Weight::ONE,
            items: &quiet_items,
        },
    ];

    // The obvious base: two flows, batch of 12, six each. The quotas sum to
    // exactly 12 and the round comes back with 7.
    let Some(naive) = std::num::NonZeroU32::new(6) else {
        panic!("6 is nonzero");
    };
    let short = select_batch(&backlogs, naive, 12, 0);
    assert_eq!(short.items.len(), 7, "a quiet flow's share is lost");

    // What `round_base` chooses instead: the whole batch per flow, so no flow
    // is ever the reason a round came back short.
    let full = select_batch(&backlogs, round_base(12), 12, 0);
    assert_eq!(full.items.len(), 12, "and the round fills");
}

/// Only reachable if a constant dimension name became invalid.
fn unreachable() -> pneuma_fairness::Dimension {
    panic!("`tenant` is a valid dimension name")
}

#[test]
fn a_flow_appearing_twice_in_one_round_raises_an_alarm() {
    // Cannot happen in production: `group` groups by tenant, so a flow has one
    // backlog. That is exactly why this is a pure function over the `Batch`
    // rather than an `if` inside the dispatch loop -- inline it would be
    // unreachable code inside an I/O function, testable only by breaking the
    // grouping. Here it is three lines.
    //
    // Worth checking because the consequence is silent: a flow with two
    // backlogs takes its quota twice, so the round is unfair and every
    // individual batch still looks correct.
    let items = vec![queued("acme", "a1", 0)];
    let Ok(flow) =
        pneuma_fairness::FlowKey::new(&[
            pneuma_fairness::Dimension::new("tenant", "acme").unwrap_or_else(|_| unreachable())
        ])
    else {
        panic!("a key");
    };
    let doubled = [
        FlowBacklog {
            flow: &flow,
            weight: Weight::ONE,
            items: &items,
        },
        FlowBacklog {
            flow: &flow,
            weight: Weight::ONE,
            items: &items,
        },
    ];
    let batch = select_batch(&doubled, round_base(10), 10, 0);
    let Some(named) = alarm(&batch) else {
        panic!("a flow taking its quota twice is worth saying out loud");
    };
    assert_eq!(named, vec!["tenant=acme".to_owned()]);

    // And an ordinary round says nothing.
    let single = [FlowBacklog {
        flow: &flow,
        weight: Weight::ONE,
        items: &items,
    }];
    assert_eq!(alarm(&select_batch(&single, round_base(10), 10, 0)), None);
}

#[test]
fn interleaved_rows_split_one_tenant_and_the_alarm_says_so() {
    // The failure `group`'s doc names and nothing was checking: grouping keys
    // off a *change* of tenant, so it depends on `queued_backlogs` returning
    // one tenant's rows contiguously. Drop that `ORDER BY` and each run of
    // rows becomes its own group -- and a flow with two backlogs takes its
    // quota twice, so the round is unfair while every individual batch still
    // looks provably correct.
    //
    // This drives the whole chain rather than asserting the grouping alone,
    // because the claim being checked is that the condition *reaches* the
    // alarm: interleaved input, several groups for one tenant, `select_batch`
    // reporting the duplicate, `alarm` naming it.
    let interleaved = vec![
        queued("acme", "a1", 0),
        queued("globex", "g1", 0),
        queued("acme", "a2", 1),
    ];
    let Ok(groups) = group(&interleaved, &BTreeMap::new()) else {
        panic!("should group");
    };
    assert_eq!(
        groups.len(),
        3,
        "three runs of rows, so three groups -- acme twice: {groups:?}"
    );

    let borrowed = backlogs(&interleaved, &groups);
    let batch = select_batch(&borrowed, round_base(10), 10, 0);
    let Some(named) = alarm(&batch) else {
        panic!("one tenant with two backlogs is exactly what the alarm is for");
    };
    assert_eq!(named, vec!["tenant=acme".to_owned()]);
}

#[test]
fn a_batch_size_of_zero_is_a_base_of_one_rather_than_a_panic() {
    // `NonZeroU32` has to be given something. Zero means "select nothing",
    // which `select_batch` already answers with an empty batch, so the base is
    // irrelevant -- but it must not be the thing that brings the dispatcher
    // down on a misconfigured setting.
    assert_eq!(round_base(0).get(), 1);
    // And a batch larger than a u32 clamps rather than wrapping to something
    // small, which would silently make every quota tiny.
    assert_eq!(round_base(usize::MAX).get(), u32::MAX);
    assert_eq!(round_base(12).get(), 12);
}
