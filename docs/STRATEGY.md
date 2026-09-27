# pneuma — strategic positioning: lean into strengths, stop relitigating the engine question

> Answers: how to focus on what already works and build something Temporal genuinely can't do right now, without getting stuck re-evaluating engines forever. Builds on `CONCURRENCY-AND-DIRECTION.md` (the fix) and `LANDSCAPE-AND-EVOLUTION.md` (the landscape).

---

## Restate/DBOS, precisely — because it changes the recommendation

Neither is a "swarm of independently-scaled workers pulling named durable subjects" platform. But they differ from each other in how far that is:

- **[DBOS](https://docs.dbos.dev/architecture)** — a library in your app process; no separate server. Its **queues are Postgres-backed and multiple app replicas pull from them to distribute work across a cluster** — structurally close to what you have, Postgres-as-broker instead of NATS-as-broker, workflow logic as decorated functions instead of YAML data.
- **[Restate](https://docs.restate.dev/foundations/key-concepts)** — push/invocation model. The Restate Server (a clustered, journal-and-state-colocated binary) receives invocations via HTTP or Kafka and *pushes* to your handlers. Actor/RPC-shaped, not queue-consumer-shaped, even though it can be Kafka-triggered.

**Verdict:** don't adopt either as a replacement platform. **Study DBOS's durable-Postgres-queue-plus-checkpoint pattern as the design reference for the outbox/lease work you already need** (`CONCURRENCY-AND-DIRECTION.md` §Path A) — it's the same problem, solved. Neither product replaces tenant-routing, YAML-DAG-as-data, or the zero-SDK component contract, which is where your actual advantage lives (below).

---

## One positioning statement, not a feature list

Everything defensible about your design compounds into a single thesis:

> **pneuma runs workflows as declarative data, on a horizontally-scaled swarm of stateless workers, with zero determinism constraints on step logic and zero SDK lock-in for the services that do the work.**

Four structural properties make that true, each one something Temporal cannot do *right now* — not "hasn't gotten to yet," but ruled out by its own architecture. I verified each against Temporal's own docs rather than asserting it.

### 1. No determinism constraint — because there's no workflow code

[Temporal workflow functions must be deterministic](https://docs.temporal.io/workflow-definition): no direct `time.Now()`, no direct randomness, no direct I/O — because crash recovery works by **replaying the function against recorded history**. Break it, and you get a non-determinism error that "will result in failures during replays, which are difficult to debug," often surfacing long after the line that caused it. This is a real, constant source of production incidents for Temporal users, and it exists specifically because workflow logic is *code*.

Your DAG is not code — it's YAML data interpreted by a fixed engine. There is no function to replay, so there is no determinism constraint to violate, ever. This isn't "we avoided the bug" — it's "the bug's precondition doesn't exist in our architecture." Say this to engineers evaluating you against Temporal; it's the single most concrete technical advantage you have.

**Corollary — no versioning API.** Temporal ships `GetVersion`/Patching APIs specifically to let you change workflow code while old event histories are still replaying against it. You already sidestep this by snapshotting the resolved graph into the run document at bootstrap (`CONCURRENCY-AND-DIRECTION.md` correctly flagged this as one of the genuinely good decisions in the codebase) — new pipeline definitions affect only new runs, with no API for it, because there's no code to version.

### 2. Zero-SDK component contract

A Temporal activity, in any language, requires linking a Temporal SDK and running as a persistent worker process polling a task queue. Your AI component contract is: **expose one HTTP endpoint accepting `{"jsonData": {...}}`, return `{"jsonData": {"step_output": ...}}`.** That's it. Any existing model server — Seldon, Triton, BentoML, a bare Flask app someone wrote in an afternoon — is a valid pneuma component with zero framework buy-in and zero language lock-in.

For a platform whose users are ML engineers standing up inference containers, not distributed-systems engineers, this is real integration friction removed. Lean into it explicitly: publish it as a one-page contract spec, not something a developer has to reverse-engineer from the original source (which is the survey notes's finding — right now it's undiscoverable, and your one sample payload is in the wrong wire format).

### 3. No architectural fan-out ceiling

Temporal caps at [2,000 pending activities and ~50K events per workflow execution](https://docs.temporal.io/design-patterns/parallel-execution), with a 4MB gRPC message size limit forcing a claim-check pattern for anything but small payloads. Beyond the cap you need continue-as-new or child-workflow sharding — real engineering work pushed onto every user with wide fan-out.

Your flat noderun table plus a message broker has no equivalent structural wall. A 10,000-page document is a scaling and tuning question, not an architecture-busting one. This maps directly onto your literal workload — page-level document fan-out — in a way none of Temporal, Restate, or DBOS's workflow-execution model does natively.

### 4. Tenant isolation at the transport layer, natively, at high cardinality

Temporal's own guidance is explicit: [namespace-per-tenant is "only practical for a smaller number of high-value tenants... most teams find this manageable for fewer than 50 tenants," and "not a good fit if you expect a very large number of tenants (10,000+)."](https://docs.temporal.io/production-deployment/multi-tenant-patterns) The recommended pattern instead is a shared namespace with per-tenant task queues — which is a workaround, not a native primitive.

Your subject-per-tenant model (`{type}.{level}.{name}.tenant_{id}`) gives you this natively and cheaply at cardinality Temporal has to work around. This is currently underbuilt — no per-tenant quotas, no rate limiting, no isolation enforcement beyond a subject suffix — but the *substrate* is already the right shape. This is your highest-leverage differentiator to actually finish, because you're not fighting the platform's grain to get it, unlike every competitor in this comparison.

---

## What this rules out, so you stop relitigating it

- **Stop evaluating whether to adopt Temporal, Restate, or DBOS wholesale.** None of the three gives you all four properties above; two of the four (determinism-free steps, tenant-native routing) are structurally impossible for Temporal specifically, not just unbuilt. The only reason to keep touching this question is the substrate-pattern study in DBOS, which is bounded and small.
- **Stop treating the storage/coordination fix as a side project.** It's the precondition for every one of the four properties being *trustworthy* rather than merely *possible*. Right now property 1 is true architecturally but the engine has races that make "workflow as safe data" not actually safe in practice (`CONCURRENCY-AND-DIRECTION.md` Part 1). Fix that first — it's bounded, specified, and not a research question anymore.
- **Stop reading this as "compete with Temporal on durability."** You will not win that fight and don't need to. The fight worth having is "workflow-as-data with no SDK tax, at page-level fan-out, natively multi-tenant" — a different, smaller, winnable claim that happens to be true.

---

## Build order

**Phase 0 — make property 1 actually true (not optional, but bounded).**
The transactional-advance design from `CONCURRENCY-AND-DIRECTION.md` §Path A: one serialisable Postgres transaction per advance, an outbox for publish, leases instead of ack-window guessing. This is already fully specified — it's implementation work, not a decision. Do it before anything below, because every differentiator is a liability instead of an asset until duplicate/lost work stops happening.

**Phase 1 — make property 2 a real product surface.**
Publish the component contract as a spec, independent of source code. Ship a reference implementation in two languages (original decorator, one HTTP framework example) that's just documentation of the existing envelope — not a required SDK, since requiring one would undermine the "zero lock-in" claim. Add a contract-conformance test harness a component author can run against a fixture without any pneuma infrastructure running.

**Phase 2 — make property 4 real.**
Finish broker's job properly (or fold it into the controller's publish path, per the open question in the original plan) — actual per-tenant rate limiting and quota enforcement, not just subject routing with no isolation. This is the differentiator competitors structurally cannot match without a rearchitecture, so it's worth being the first thing a prospective adopter sees.

**Phase 3 — make property 1 visible, not just true.**
Local runner (in-process, no infra, stub components) and a validator (unreachable nodes, cycles, aggregators with no terminal child, contract mismatches at load time, not runtime). This is only possible *because* there's no workflow code — it's the direct product payoff of property 1, and it's the single highest-leverage fix from the earlier framework assessment (`CONCURRENCY-AND-DIRECTION.md` Part 3).

**Phase 4 — lean into property 3 explicitly.**
Once Phase 0 lands, benchmark fan-out width honestly — find where you actually start to strain (DB write throughput, not an artificial cap) and publish that number next to Temporal's 2,000. This is a concrete, verifiable claim a prospective adopter can check, unlike most vendor positioning in this space.

Everything else from the earlier documents (metrics, health endpoints, the Rust port itself) is infrastructure that makes this positioning credible under load — necessary, but not what makes the pitch true. The four properties above are what makes it true; the rest is what makes it survive contact with production.
