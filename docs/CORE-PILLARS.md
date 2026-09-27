# pneuma — the core pillars, consolidated

> The final, clean statement. Pulls together `FRAMEWORK-FOUNDATIONS.md`, `ARCHITECTURE-V2.md`, `PREGEL-NOTES.md` — read those for implementation detail; this is the entry point.

## Autoscaling — settled, not speculative

KEDA on the sidecar pair, using the `kedaSpec`-on-predictor shape (Prometheus-scraped queue-depth trigger, uniform `min=1/max=4` across every stage) — swap the trigger for KEDA's native NATS JetStream scaler (consumer lag) rather than a Prometheus proxy metric. The deliberate difference from deployments of that shape: **scale the orchestration layer too, not only the leaf workers.** Their chainer pods are typically hardcoded to one replica with no `kedaSpec` at all. Pillar 3 exists specifically so pneuma doesn't repeat that.

## The six pillars

**1. Idempotency.** One primitive, `execute_once(idempotency_key, effect)`, keyed by `(execution_id, node_id, attempt_generation)`. Every node kind gets exactly-once semantics by using it, not by re-deriving it per kind.

**2. Survivability.** The test: does behavior differ under `SIGKILL` vs. `SIGTERM`-then-drain? If yes, it isn't done. Lease expiry is the mandatory baseline, correct with zero process cooperation. Graceful drain only ever changes *how fast* recovery happens, never *whether* it's correct.

**3. Stateless horizontal scale.** The precondition that makes KEDA-driven autoscaling trivially safe — nothing in the compute layer holds anything expensive to lose, so replicas can be added or removed with zero rebalancing logic, for the Engine exactly as much as for Workers. Tier 1 (`SKIP LOCKED`, clone freely) covers essentially all realistic load; Tier 2 (partition by `tenant_id`, only on measured need) is the answer if it doesn't.

**4. Fair distribution.** Work-conserving weighted dispatch by default (`SKIP LOCKED` + window-function quota per flow), Deficit Round Robin if bursty load demands more precision, hash-ring placement composed underneath as a *scale* layer, never a substitute for the fairness layer. Flow key defaults to `(tenant_id, workflow_type)`, pluggable to whatever dimensions matter — priority tier, cost class, SLA.

**5. Open graph vocabulary.** Closed, compiler-checked `match` by default — the right call for in-house extension, buying exhaustiveness a dynamic registry can't. A registry is the documented fallback only if genuine third-party, independently-compiled authorship becomes real. Recursive composition (`components: Sequence["Node"]`) already supports arbitrary depth by construction — nothing to add there.

**6. Declarative fidelity, tracing, and reasoned failure diagnosis.** Detailed below — this is the newest and least previously-specified pillar.

## Pillar 6, in detail

**Structural fidelity** — the execution must be a *provable* realization of the declared graph, not a probable one. Reframes nearly every defect in the survey notes precisely: the DictAggregator hang, the aggregation race producing duplicate emission, the `lstrip` corruption merging unrelated paths — each one is exactly "the run did not correspond to the graph." Made testable, not just aspirational, via the deterministic-simulation property already specified in the original plan: for any registered graph, the execution trace visits exactly the implied nodes and edges, exactly once each in the appropriate sense, with no silent divergence — run 10,000 adversarial seeds against it, pass/fail.

**Contractual fidelity** — a distinct failure mode from structural: node A's declared output not matching node B's declared input, discovered as a runtime failure instead of a load-time one. Typed I/O schemas per `NodeHandler`, validated at registration (adjacency check) *and* at dispatch (payload check).

**Tracing** — unchanged: OTel, in-band W3C propagation across every hop, kept deliberately even past what the newer sibling repo's plain-`tracing` baseline would suggest, because this pipeline is genuinely multi-hop in a way that baseline doesn't serve.

**Reasoned failure diagnosis** — the bar is answering *why*, in the graph's own vocabulary, without needing to know the Postgres schema or NATS internals. A query surface on Gateway (extending the run-inspector from `STRATEGY.md`), projecting substrate state — barrier arrivals, lease expiries, typed error codes — back into graph-relative terms:

```
GET /runs/{id}/diagnosis

{
  "status": "stuck",
  "failing_node": { "path": "invoice.page.default.C", "kind": "Model",
                     "error_code": "MODEL_TIMEOUT", "attempts": 3 },
  "blocks": {
    "aggregator": "invoice.page.default.X",
    "expected_children": 2, "arrived": 1, "missing": ["C"]
  },
  "cascade": "run cannot conclude until this resolves or is explicitly skipped"
}
```

Three questions, every time: which node, in graph terms; why, from a closed typed set of reasons (the `Coded` trait from the original plan), never a stringly-typed exception; and what's the blast radius — local and retryable, or cascading to block an aggregator, which blocks the run from concluding. Nothing structurally new — the barrier/mailbox table already knows expected-vs-arrived. What's new is committing to expose it as a first-class, graph-shaped surface instead of leaving it implicit in tables an author was never meant to query.

## Simplicity and depth — not actually in tension, but be precise about the line

**Design principle: complexity scales with what's declared, never with what's merely possible.** A three-node linear pipeline gets trivial YAML, a trivial resolved graph, a trivial diagnosis output — none of the barrier/combiner/fairness machinery visible or costly if unexercised. A deeply nested pipeline uses the exact same primitives, composed recursively, no separate "advanced mode."

**Where this claim's honest limit is:** execution capability for depth is already structurally sound — no depth limit anywhere in the design. **Authoring comprehensibility is a genuinely separate concern** and claiming the substrate solves it would be dishonest — a human still has to read a six-level-deep nested YAML. That's not a substrate problem, it's exactly what the Toolkit (`STRATEGY.md` Phase 3) and generated diagrams (replacing the hand-drawn `.d2` files found in the survey notes) exist for. Keep the line clear: the engine doesn't cap power; the tooling is what makes power manageable, and that's distinct work, not a free side effect of getting the substrate right.
