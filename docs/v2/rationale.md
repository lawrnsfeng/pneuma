# V2 — why the boundaries differ, and what the argument is worth

The useful thing about having two architectures written down is not choosing
between them. It is that each one names a cost the other pays, and both costs
are real.

## The one-sentence difference

**As-built distributes the decision; V2 centralises it in a transaction.**

A run in the as-built system advances because a specific process is driving it —
`pneuma-restate` inside a Restate invocation, or `pneuma-driver` on a `LocalSet`
holding that run's `Execution`. A run in V2 advances because *some* Engine
replica picked up a message and committed one Postgres transaction. Everything
else follows from that.

| | As-built | V2 |
|---|---|---|
| Per-run affinity | yes — the driving process holds the run | **none** — any replica, any message |
| Durability of a step | Restate's journal, or a re-publish | a Postgres transaction plus an outbox row |
| Dual write | present at intake, closed by hand | absent by construction |
| Stores | Postgres + MongoDB | Postgres only |
| Broker | RabbitMQ + NATS | NATS only |
| Failure of the broker | runs stop advancing on the NATS path | advance still commits; work backs up |
| Failure of Postgres | admission and the janitor stop; runs continue | **everything stops** |
| Services | 8 | 6 |
| Built? | **yes** | no |

## Where V2 is straightforwardly better

**The outbox removes a class of bug rather than an instance.** The as-built
intake writes the run document and then submits — and the gap between the two is
a run that exists and never starts. That is closed carefully, by hand, in one
place. V2 has no such gap anywhere, because there is no "and then publish": the
publish *is* a row in the same transaction. Ingress is not special-cased for the
first dispatch; it uses the same mechanism as every later one.

**Deleting broker is not simplification for its own sake.** It is one
`format!` call. As a separate deployment it is also a network hop, a durable
consumer, a JSON round trip that can lose keys, a health endpoint, a CI job and
a rollout. Folding it into the Dispatcher puts it where the routing decision was
already being made.

**Per-tenant quota only works at a chokepoint.** As-built has fairness at
admission and nothing at dispatch, which means a run already admitted competes
freely. V2's Dispatcher is the one place every dispatch passes through, which is
the only place enforcement is cheap.

**No per-run affinity means autoscaling is uneventful.** Nothing to warm,
nothing to rebalance, no replica whose death costs a specific run anything. The
as-built driver's affinity is why its result subscription cannot be a queue
group.

## Where as-built is straightforwardly better

**It exists.** It is implemented, gated, and verified against real Postgres,
MongoDB, RabbitMQ, NATS and Restate. V2 is a document.

**Restate is stronger than an outbox for the thing an outbox is worst at.** An
outbox guarantees the message is eventually published. It does not remember that
a component was *already called* — so a redelivery re-calls it, and every
component has to be idempotent. Restate's journal remembers the call's result,
so a crash mid-run resumes rather than repeats. V2 pushes that cost onto every
component author, which is in tension with the zero-SDK promise.

**Postgres becomes a hard single point of failure.** `ARCHITECTURE-V2.md` says
this plainly: previously either database going down partially degraded the
system; in V2 it is everything. HA Postgres becomes load-bearing infrastructure
from day one, not later hardening. As-built's two stores are a complication that
is also a blast-radius reduction.

**One Tier-2 ceiling for the whole platform.** Because the ledger, the barriers
and the outbox are deliberately in one database to get transactional atomicity,
Ingress, Engine, Dispatcher, Gateway and Housekeeper are all independently
Tier-1-scalable and share one Tier-2 ceiling. You do not get to shard the outbox
without sharding its neighbours, because the advance transaction touches several
of them together.

**Two workloads on one instance is a real operational risk.** The Engine's
advance transactions are complex, multi-statement and lock-heavy; the
Dispatcher's outbox drain is high-churn `SKIP LOCKED` polling with heavy
insert/delete turnover. If autovacuum falls behind on the outbox table, *both*
workloads degrade — not just the one causing it. `ARCHITECTURE-V2.md` proposes a
"Tier 1.5": separate instances by workload shape, before any sharding.

## The six pillars, scored against both

From [`CORE-PILLARS.md`](../CORE-PILLARS.md) and
[`STRATEGY.md`](../STRATEGY.md).

| Pillar | As-built | V2 |
|---|---|---|
| Workflow as declarative data | yes — and the resolved graph is snapshotted per run | yes, unchanged |
| Zero-SDK components | yes — one HTTP endpoint, contract documented | yes, but idempotency becomes the component's problem |
| No fan-out ceiling | yes — a flat row table plus a broker | yes, plus a combiner barrier that is atomic rather than re-read |
| Tenant isolation | subject-per-tenant, no enforcement | subject-per-tenant **with** quota at the Dispatcher |
| Durable execution | Restate journal (primary), correlation table (fallback) | outbox plus leases |
| Operational simplicity | 8 services, 2 stores, 2 brokers | 6 services, 1 store, 1 broker |

The honest reading: V2 wins on pillars 4 and 6, as-built wins on 5, and 1–3 are
a draw.

## The claim about statelessness, stated precisely

It is tempting to frame this as "Temporal polls, we do not". That is checkable
and wrong: V2's Workers use JetStream **pull** consumers, `Fetch(batch,
timeout)`, which is the same shape as a long poll. Both systems have a worker
loop asking for work.

The real difference is *what is being polled and what state it holds*.
Temporal's poll target is a smart, centralised, stateful scheduler that owns
per-execution event history and makes sticky-routing decisions — that server is
itself the thing that must scale and be made highly available. V2's poll target
is a domain-ignorant NATS stream; all state lives in Postgres, touched
transactionally by whichever replica picked the message up.

**No process caches per-run state in memory, and no message is preferentially
routed to whichever replica handled the last one for that run.** That is the
falsifiable claim, and it buys trivial autoscaling, no sticky-worker failure
domain, and uneventful rolling deploys.

It is worth noting what this costs Temporal specifically, because the cost is
documented rather than alleged: the sticky workflow cache defaults to 10,000
cached workflows in the Go SDK and 600 per host in the Java SDK. It is not
*synchronicity* that costs memory; it is the event-sourcing-plus-replay recovery
model, which needs a worker-side cache to make replay affordable.

And two unrelated projects converged on the same answer. DBOS re-runs the
workflow function but skips already-checkpointed steps via cheap Postgres
lookups — no event-history reconstruction, no sized cache. Restate externalises
state into its own server cluster, tuned as shared infrastructure rather than
per replica. Both rejected "cache execution state inside your own horizontally
scaled compute". The credible claim is not "we invented statelessness" — it is
"we made the same call the newer generation of durable-execution systems
independently made, for the same reason."

Which is also the reason the as-built system adopted Restate rather than
building an engine: it is that shared infrastructure, and it already exists.

## What is genuinely still open

- **When does Tier 2 become necessary?** There is no measured throughput number
  for this schema. "A well-tuned primary plausibly sustains tens of thousands of
  simple indexed writes per second on a table this shape" is an estimate, not a
  benchmark. Measure before assuming.
- **The Dispatcher's cancellation check needs a cache, not a query per
  dispatch.** As specified it is one extra read per dispatch on the same primary
  already serving the Engine's writes. The fix is a targeted index lookup held
  in a short-TTL in-memory cache per replica — the mechanism was specified
  without being priced.
- **RabbitMQ's quorum queues plus DLX back the client callback path today.**
  That guarantee has to be re-verified as achievable on a JetStream durable
  stream with explicit ack and a real DLQ policy, rather than assumed
  equivalent. Likely fine; worth confirming once rather than discovering a gap
  in production.
