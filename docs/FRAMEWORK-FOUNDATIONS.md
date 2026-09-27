# pneuma as a framework — the five pillars, and what's actually missing

> Answers: are idempotency + survivability + stateless scale + an open DSL + fair distribution enough grounding for a framework? Verdict: **yes for the substrate, plus two concrete additions this document specifies** — a pluggable node-type extension point (§4) and a work-conserving fair dispatch algorithm (§5). Pillars 1–3 synthesize prior docs; §4 and §5 are new design work.

## The verdict, precisely

Pillars 1–3 (idempotency, survivability, stateless scale) are properties of the *substrate* — they hold uniformly regardless of what a workflow computes, and they're largely already designed across `CONCURRENCY-AND-DIRECTION.md`, `PREGEL-NOTES.md`, and `ARCHITECTURE-V2.md`. Pillar 4 as stated ("registered DSL graph") implies an extension point that doesn't exist yet — today's Engine hardcodes four node types. Pillar 5 needed a specific algorithm, not a principle, and the requirement (fair across *workflow types*, not just tenants, without sacrificing throughput) points to a known family: weighted, work-conserving fair queuing.

Add §4 and §5 concretely and this is a sound foundation — comparable to what Temporal/DBOS/Restate offer architecturally for this problem class, for reasons specific to the workload rather than borrowed credibility.

## 1. Idempotency — one reusable primitive, not four bespoke mechanisms

Already specified: deterministic `Nats-Msg-Id`, `ON CONFLICT DO NOTHING` on noderun creation, PK-idempotent barrier arrival (`PREGEL-NOTES.md`), lease/`attempt_id` rejecting late results from superseded attempts.

**Framework-grade addition:** expose one primitive — `execute_once(idempotency_key, effect)`, where `idempotency_key = (execution_id, node_id, attempt_generation)` always, backed by the same outbox + unique-constraint mechanics — so every future node type (§4) gets exactly-once semantics by construction, not by re-deriving it per node type.

## 2. Graceful shutdown — an optimization, never a correctness requirement

The precise test: **does the system behave identically under `SIGKILL` and under `SIGTERM`-then-drain?** If SIGKILL loses something graceful shutdown would have saved, survivability isn't actually done.

- **Baseline (mandatory, zero cooperation required):** lease expiry → redispatch with a new `attempt_id` → old attempt's result rejected by the idempotency key. Must be correct on its own.
- **Optimization (reduces the window, never the correctness):** `CancellationToken`-driven drain — stop accepting new work, release in-flight leases early instead of waiting out the timeout, NAK immediately. Shrinks recovery latency; its absence costs speed, never correctness.

## 3. Stateless scale — read replicas as an underpriced lever

`ARCHITECTURE-V2.md`'s two-tier model stands (Tier 1: clone stateless compute via `SKIP LOCKED`; Tier 2: partition by `tenant_id`, only on measured need). Addition: Gateway's queries and Housekeeper's sweeps are read-heavy and don't need primary-consistent reads — Postgres read replicas absorb that traffic for free, extending Tier 1's runway before Tier 2 is ever needed. Cheap, real, not yet priced into the earlier scaling discussion.

## 4. The node-type extension point — corrected on review

**The original framing here conflated two different things: *closed* (a fixed, enumerable set of node types) and *hard to extend* (adding one requires jumping through hoops). Those aren't the same property, and closed isn't automatically a defect.** The real question is *who authors new node types* — a team extending its own repo, or genuine third parties shipping independently-versioned handler code the core team never compiles against. Those call for different designs, and a dynamic registry is only the right answer to the second one.

**Closed-world (the default, and probably the actual case here):** a `match` over a fixed enum buys something a registry structurally can't — the compiler forces every dispatch site to handle every variant. Add a 5th `StepKind` and the build fails everywhere it isn't handled yet (`init_next_step`, `_try_aggregate`, the resolver's `isinstance(step, AggregatorStep)` branch — the three sites named below). That's the safety net working, not friction. A registry trades this for a *runtime* "no handler found" instead of a compile error — a strictly weaker guarantee, for a team that doesn't need the flexibility it buys.

The design that keeps the same substrate/per-type separation without losing exhaustiveness:

```rust
trait NodeBehavior {
    fn on_dispatch(&self, ctx: &DispatchCtx) -> DispatchInstruction;
    fn on_result(&self, ctx: &ResultCtx) -> ResultInstruction;
}

impl NodeBehavior for StepKind {
    fn on_dispatch(&self, ctx: &DispatchCtx) -> DispatchInstruction {
        match self {
            StepKind::Model(m)          => m.on_dispatch(ctx),
            StepKind::ListAggregator(a) => a.on_dispatch(ctx),
            StepKind::DictAggregator(a) => a.on_dispatch(ctx),
            StepKind::Condition(c)      => c.on_dispatch(ctx),
            // add StepKind::Timer(t) here and every non-exhaustive match
            // over StepKind fails to compile until it's handled — that IS
            // the extension mechanism, not a workaround for lacking one.
        }
    }
}
```

Zero indirection, zero vtable, zero "handler missing at runtime" failure class. This is the default recommendation now.

**Open-world (only if external, independently-versioned authorship becomes a real requirement):** *then* the registry below is necessary, not optional — you genuinely cannot compile code you don't have. Kept as the documented fallback, not the default.

**Core insight, unchanged by this correction (the Pregel parallel):** the substrate — barriers, leases, idempotency, dispatch, fairness — is 100% node-type-agnostic either way. Only "what does dispatch mean" and "what does a result mean" vary per kind of node. The disagreement was only ever about *how* that variance should be expressed, not *whether* it should be isolated from the substrate.

```rust
trait NodeHandler: Send + Sync {
    fn kind(&self) -> NodeKind;                       // "Model" | "Timer" | "HumanApproval" | "SubWorkflow" | custom
    fn on_dispatch(&self, ctx: DispatchCtx) -> DispatchInstruction;
    fn on_result(&self, ctx: ResultCtx) -> ResultInstruction;
}

enum DispatchInstruction {
    Publish    { subject: SubjectName, payload: Bytes },     // today's Model node
    Inline     { compute: fn(Input) -> Output },              // Conditional — pure, no dispatch
    Await      { until: WallClock | ExternalSignalId },       // durable timer / human-in-the-loop — GAP TODAY
    SpawnChild { pipeline_id: PipelineId, input: Json },      // sub-workflow composition — GAP TODAY
}
```

This is [Airflow's extensibility model](https://airflow.apache.org/docs/apache-airflow/stable/howto/custom-operator.html), directly: subclass `BaseOperator`, override the constructor and `execute()`, the scheduler never needs to know what a custom operator does. Same shape: today's four node types become the *first four* registered handlers, not the only possible ones.

**The three closure points that would each need to become a registry lookup, cited precisely rather than described abstractly:**

1. The type itself — `NodeType` is a closed 4-value enum; `Node`/`Step` are discriminated unions over exactly those four. the original model library rejects a fifth kind at deserialization, before any dispatch logic runs at all.
2. `Controller.init_next_step` — `match step: case ModelStep(): ... case ListAggregatorStep(): ... case DictAggregatorStep(): ... case ConditionalStep(): ... case _: raise ValueError` (the `raise` is the proof this is closed, not open — a new kind is a hard error, not a registry miss).
3. `Controller._try_aggregate` — a second, narrower `match parent_step: case ListAggregatorStep(): ... case DictAggregatorStep(): ...`, specific to aggregation completion.
4. `RunResolver` — not a `match`, but the same closure via `isinstance(step, AggregatorStep)`: whether a node recurses into `.components` during resolution is decided by checking against the base class the two aggregator types share. A future kind needing different resolution semantics (e.g. `SpawnChild`, no nested `components` at all) isn't handled by this branch either.

Worth noting in fairness to the existing design: `gather_input_and_init_next_step`'s `match step.num_prerequisites: case 0 | 1 | _` is *already* appropriately generic — it dispatches on prerequisite count, not node type, so join logic doesn't need to know what kind of node it's joining. That's the shape the other three sites need to move toward; not everything in the Engine needs to change for `NodeHandler` to work.

**Honest gap this surfaces, not to be glossed over:** if "any kind of workflow" is the real target, today's DSL genuinely lacks primitives Temporal/DBOS/Restate all have — durable timers, external-signal/human-approval waits, sub-workflow composition, and true iteration (`ListAggregator` fans out over an already-known list; it can't express "repeat until condition" or "for N unknown until runtime"). `NodeHandler` is the *mechanism* to add these without touching the Engine. It doesn't build them. State this plainly rather than let "registered DSL graph" imply the gap is already closed.

**Concrete candidates to fill that gap, each mapped onto existing substrate rather than requiring new infrastructure:**

- **`Timer`/`Delay`** — wait until a wall-clock time or duration. Dispatch is a `wake_at` row, not a broker publish; completion is a scheduled sweep (Housekeeper or a dedicated one) checking `wake_at <= now()`, feeding into the same transactional-advance path as every other result.
- **`Signal`/wait-for-external-event** — pause until an external call arrives with a correlation token. Directly motivated by the domain, not borrowed from Temporal for its own sake: document-AI pipelines routinely need "route a low-confidence extraction to a human reviewer before finalizing." Dispatch creates an "awaiting signal" row keyed by a token; completion is a new Gateway endpoint (`POST /signals/{token}`), reusing the component that already owns the external HTTP surface rather than inventing a new one.
- **`SubWorkflow`/compose** — invoke another registered pipeline as one node, wait for its conclusion, treat its output as this node's. Dispatch reuses Ingress's existing run-creation path for a child run with a `parent_slug` pointer back; completion is the child run's conclusion — the same checkable invariant from `PREGEL-NOTES.md`'s vote-to-halt discussion, not a new mechanism.
- **`Loop`/repeat-until — the hard one, worth a real design pass before committing.** Not the same gap as dynamic fan-out — `ListAggregator` already handles a runtime-determined list length; that's not missing. What's missing is iteration where the loop body's *iteration count itself* isn't known ahead of time (retry-with-adjustment until confidence exceeds a threshold, capped at N). `RunResolver` assumes a static, acyclic, fully-resolved-at-bootstrap graph, so this doesn't fit cleanly. Two honest options: bound it at definition time (resolver pre-unrolls up to `max_iterations`, stays inside the static-resolution model, avoids the "no cycle detection" hazard from the survey notes entirely) or genuinely dynamic re-entrant scheduling (the resolver re-invoked per iteration at runtime — more powerful, a materially bigger change, deserves its own design doc rather than a bullet here).

**Two non-examples, worth naming because the distinction generalizes:** retry-with-backoff and k-of-n barrier completion ("proceed once 3 of 5 children finish, don't wait for stragglers") both look like candidates for new node types and aren't. Retry is orthogonal to node kind — any node might want configurable retry policy, and the lease/`attempt_id` mechanism already gives the substrate the hooks generically; making it a node type would duplicate the logic per type instead of writing it once. k-of-n is a different *combine policy* on the existing aggregator, not a new primitive — ties directly back to the Pregel combiner framing: "combine on all-arrived" vs. "combine on first-k-arrived" are two policies for one mechanism. **The rule this converges on:** a new node type is warranted when dispatch/result semantics genuinely differ; a parameter or substrate policy is warranted when only a threshold or a cross-cutting concern differs.

**Consequence, worth taking seriously if this ships to third parties:** self-service debuggability stops being optional. You can't always be the one debugging someone else's registered node type. The run-inspector (`STRATEGY.md` Phase 3) needs to expose *substrate* decisions generically — why a barrier didn't complete, why a dispatch was fairness-throttled, why a lease expired — not just the four built-in node types' state.

## 5. Fair distribution — work-conserving weighted fair queuing

The requirement, stated precisely per the clarified ask: fair across *flows* — tenant, workflow type, priority tier, or any combination — **without sacrificing throughput.** That combination is what work-conserving fair queuing exists for, and it has a directly relevant, production-proven reference: [Kubernetes' API Priority and Fairness](https://kubernetes.io/docs/concepts/cluster-administration/flow-control/) (stable since v1.29) — classifies requests into flows via `FlowSchema`, assigns **concurrency shares** (proportional, not fixed quotas) per priority level, applies fair queuing within a level so no flow starves another.

**Work-conserving** is the property that resolves "fair vs. fast" as a false tradeoff: an idle flow's unused share is redistributed to active flows immediately rather than wasted. That's what delivers both properties at once.

Design, staying consistent with "Postgres does the coordination, no new infrastructure":

```sql
WITH ranked AS (
    SELECT id, flow_key,
           ROW_NUMBER() OVER (PARTITION BY flow_key ORDER BY created_at) AS rn
    FROM outbox WHERE dispatched_at IS NULL
)
SELECT id FROM ranked
JOIN flow_weight USING (flow_key)
WHERE rn <= flow_weight.weight * round_base_quota
ORDER BY flow_key, rn
FOR UPDATE SKIP LOCKED
LIMIT :batch_size;
```

> **Correction — `ORDER BY flow_key, rn` starves, and it starves precisely when
> fairness is needed.** Found while implementing `pneuma-fairness`; the SQL
> above is left as written so the correction is legible.
>
> `LIMIT :batch_size` applied to `ORDER BY flow_key, rn` fills the batch **one
> flow at a time, in key order**. Two flows `acme` and `zeta`, equal weight,
> each with a full backlog and each contributing 100 candidates, with
> `batch_size = 100`: `acme` takes the entire batch and `zeta` gets nothing.
> Next cycle, the same. `zeta` is starved by its name.
>
> This is not avoidable by tuning, because it collides with the property the
> design exists for. Work conservation *requires* the quotas to over-subscribe
> the batch — if `round_base_quota × Σweights ≈ batch_size`, then an idle flow's
> unused share is simply not taken by anyone, which is the waste the design set
> out to avoid. So the quotas must over-subscribe, and `LIMIT` must arbitrate —
> and under this `ORDER BY`, arbitration is alphabetical.
>
> The fix is one clause: **`ORDER BY rn, flow_key`**. That interleaves — every
> flow's first item, then every flow's second — so `LIMIT` cuts across flows
> rather than down one. `acme` and `zeta` get 50 each. `flow_key` stays in the
> ordering only as a deterministic tie-break, which matters for reproducibility
> but no longer decides who eats.
>
> **Weights need the sort too, and this is where a first attempt at the
> correction got it wrong.** It claimed a weight-3 flow "appears three times as
> often in the interleaved order" because it contributes three times as many
> ranked rows. Under `ORDER BY rn, flow_key` it does not: it appears exactly
> once per rank, like everyone else, and its extra rows only come into play at
> ranks past the lighter flows' quotas. So the ratio materialises only when
> `batch_size >= Σ quotas` — the non-over-subscribed case this very correction
> says must not happen. The weight column would be inert under load while
> looking correct at low load. Measured: weights 1 and 3 with quotas 10 and 30
> served 10/10 at a batch of 20.
>
> The working form buckets the rank by the weight:
>
> ```sql
> ORDER BY (rn - 1) / flow_weight.weight, flow_key, rn
> ```
>
> Integer division, so a weight-3 flow's ranks 1-3 all fall in bucket 0 and it
> contributes three rows where a weight-1 flow contributes one.
>
> **That sort is still not enough, and the reason is the same starvation one
> level down.** Within a bucket it orders by `flow_key, rn`, so each flow's rows
> arrive as a contiguous chunk — and a `LIMIT` that cuts inside a bucket gives
> the remainder to whoever sorts first. Two flows of equal weight 3 with
> `LIMIT 4` split 3/1, and with `LIMIT 2` the second flow gets nothing at all.
> Measured against the Rust implementation of exactly this sort before it was
> corrected.
>
> The position *within* the bucket has to come before the flow key:
>
> ```sql
> ORDER BY (rn - 1) / w, (rn - 1) % w, flow_key      -- w = flow_weight.weight
> ```
>
> Now bucket 0 pass 0 holds every flow's `rn = 1`, pass 1 every weight-≥2 flow's
> `rn = 2`, and so on. A cut anywhere lands mid-pass and skews by at most one
> item, which is the best any batch limit can do. That gives 5/15 for weights
> 1 and 3 at a batch of 20, and an even split for equal weights at *every*
> weight rather than only at weight 1.
>
> `pneuma-fairness` implements this, and its tests assert the ratio at a
> **binding** batch size and across several weights. Both earlier versions
> passed their own tests: the first used a batch equal to the quota sum, the one
> size at which unweighted fill looks right; the second tested equal weights
> only at weight 1, the one weight at which chunking looks fair.
>
> `pneuma-fairness` implements the corrected ordering, and its tests assert the
> starvation case directly rather than trusting this paragraph.

`flow_key` is computed at write time from a *pluggable* set of dimensions (default `(tenant_id, workflow_type)`; add priority tier, cost class, SLA class without touching core dispatch logic — the composition lives in configuration, not scattered code). Each flow gets up to `weight × round_base_quota` rows per Dispatcher cycle; an under-quota flow contributes fewer candidates, letting `LIMIT :batch_size` fill from other flows — approximate work-conservation for free, using the same `SKIP LOCKED` chokepoint the Dispatcher already needed (`ARCHITECTURE-V2.md` §3), not a new one.

> **Measured: the simple form accumulates a per-round bias, and needs at least a
> rotating tie-break.** A batch that does not divide the round total must hand
> the remainder to somebody, and with a fixed `flow_key` tie-break that is the
> same flow every round. Simulated over 200 rounds of a 7-item batch, two
> equal-weight flows split **800 / 600** — one extra item per round, forever —
> while every individual batch was provably fair at 4/3.
>
> That is the third form of "the flow key decides who eats" this design has had,
> and the first two were caught only because a test asserted about a single
> batch. This one is invisible at that timescale by construction.
>
> **The dispatch query's locking never worked, and the rotation note as first
> written did not run.** Both were found by executing the SQL against
> PostgreSQL 16.15 rather than reading it. The working form, run and checked:
>
> ```sql
> WITH ranked AS (
>     SELECT id, flow_key,
>            ROW_NUMBER() OVER (PARTITION BY flow_key ORDER BY created_at) AS rn,
>            DENSE_RANK() OVER (ORDER BY flow_key) - 1                     AS flow_rank
>     FROM outbox WHERE dispatched_at IS NULL
> )
> SELECT o.id
> FROM outbox o
> JOIN ranked r      ON r.id = o.id
> LEFT JOIN flow_weight w ON w.flow_key = r.flow_key
> WHERE r.rn <= COALESCE(w.weight, 1) * :round_base_quota
> ORDER BY (r.rn - 1) / COALESCE(w.weight, 1),
>          (r.rn - 1) % COALESCE(w.weight, 1),
>          (r.flow_rank + :round) % :flows
> FOR UPDATE OF o SKIP LOCKED
> LIMIT :batch_size;
> ```
>
> **This is a defect in unbuilt design, not in production.** Checked before
> raising the alarm: the live system's only row locking is
> the original, a plain
> `select(Schema).where(slug == ...).with_for_update()` against one table with
> no CTE, so its lock lands correctly. Nothing deployed needs changing, and this
> is recorded in the design doc rather than the defect notes for that
> reason.
>
> Three things it fixes, each measured.
>
> **1. `FOR UPDATE` was locking the wrong table.** With no `OF` list, Postgres
> applies the locking clause to lockable relations and silently ignores CTE
> references — so in the original query the row locks landed on `flow_weight`
> and `outbox` took only an `AccessShareLock`:
>
> ```text
>  flow_weight | RowShareLock
>  outbox      | AccessShareLock
> ```
>
> Observed consequences with two concurrent sessions. With a single flow, the
> second dispatcher returned **0 rows** — it skipped the one locked
> `flow_weight` tuple, so the whole fleet serialises to one worker. With three
> flows, the two dispatchers partitioned by *flow* rather than by row, so more
> dispatchers than flows leaves the excess idle. And because `outbox` rows are
> never locked, nothing prevents the same row being claimed twice across
> non-overlapping cycles, which is the normal case since these transactions are
> short.
>
> The fix is to put `outbox` in the outer `FROM` and name it: `FOR UPDATE OF o`.
> Verified disjoint — session A took ids 1-5 and session B took 6-10
> concurrently, with `outbox | RowShareLock`.
>
> Note that pulling the ranking into a subquery with the `LIMIT` inside does
> *not* work either: the candidate set is fixed before locking, so both workers
> compete for the same rows and the loser gets nothing. That form was tried and
> measured at 0 rows for the second session.
>
> **2. A window function cannot go in the outer `ORDER BY`.** `DENSE_RANK() OVER
> (...)` there sets the statement's window-function flag and Postgres rejects
> the lock outright with `FOR UPDATE is not allowed with window functions`. The
> rank must be computed in the CTE and referenced as a plain column.
>
> **3. `JOIN flow_weight` was an inner join**, so a flow with outbox rows but no
> weight row contributes no candidates in any round, forever, silently. That is
> the same "starvation written as configuration" that `Weight::new(0)` is
> designed to make unrepresentable in the Rust; a missing row reintroduced it.
> `LEFT JOIN` with `COALESCE(weight, 1)` gives a new flow the default share.
>
> **`:flows` must equal the number of distinct `flow_key`s with undispatched
> rows this cycle** — not an upper bound, which an earlier draft of this note
> wrongly allowed. `(flow_rank + :round) % :flows` is a rotation only at the
> exact count: bind 100 where there are 3 flows and rounds 0-96 all produce the
> identical order, so the rotation is inert and the earlier-sorting flow
> collects its extra item again.
>
> An earlier draft also suggested `(hashtext(flow_key) + :round) % :flows`. That
> collides distinct flows onto one bucket, leaving their order to the planner,
> and `hashtext` returns a signed `int4` whose `%` can be negative.
>
> Note the rotation runs opposite to the Rust's `rotate_left`, which maps sorted
> index `i` to `(i - round) mod n` where this gives `(i + round) mod n`. Both
> circulate the remainder equally; batches will not match for the same `:round`.
>
> Rotation is not a substitute for Deficit Round Robin: it equalises the
> *remainder*, not accumulated debt, so a flow starved by quota exhaustion in
> one round is not compensated in the next. It is the cheap fix for the bias
> that was measured; DRR remains the answer if a weighted deficit is needed.

**Upgrade path, only on measured need:** persisted **Deficit Round Robin** — one small `flow_state(flow_key, deficit)` table, updated per round (`deficit += weight; take = min(deficit, backlog); deficit -= take`). Textbook algorithm, one more transactional Postgres update, still no new infrastructure. Don't build this speculatively — measure fairness violations under real bursty load first, same discipline as the Tier 1/Tier 2 scaling decision.

## What's still not covered by these five pillars, if this genuinely becomes a shared framework

Not gaps in this design — gaps in scope, worth naming so they aren't mistaken for solved:

- **Contract stability for `NodeHandler` itself.** Once third parties register handlers against it, the trait needs the same versioning discipline any public API needs. Not designed here.
- **A testing SDK for handler authors**, not just for the core team — the deterministic-simulation harness (the original plan's testing strategy) needs to be usable *by* someone writing a custom `NodeHandler`, not only by the people building the Engine.
- **Multi-DSL namespacing**, if independent teams register unrelated node-type vocabularies — probably `org/team/node-type@version`, not a single flat enum. Not designed here, flagged for when it's actually needed.
