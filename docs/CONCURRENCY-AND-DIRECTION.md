# pneuma — concurrency report, storage decision, and framework assessment

> Companion to the survey notes (which covers general defects). This document answers three questions: **(1)** what exactly goes wrong under concurrent execution and autoscaling, **(2)** whether to consolidate onto one database, and **(3)** whether this is ready to become a shared framework.
>
> Claims are marked **[verified]** (traced in source, cited `file:line`) or **[inferred]** (strongly implied; needs a production check).

---

# Part 1 — Concurrent execution: the complete picture

Your diagnosis is correct, and it is more specific than "NATS tuning". There are **four independent defects** that compose into the exact symptom you described — *"duplicate result processing triggered the next node, but we can't tell if the next node was done or failed, so we trigger it again."* Tuning ack/retry will reduce how often you hit them; it cannot remove them, because the state model is missing the fact you need.

## 1.1 The root cause: there is no record of "work was dispatched"

`NodeStatus` has `CREATED, PROCESSING, FINISHED, ERROR, TIMED_OUT, CANCELLED, FORKED, AGGREGATED, HAS_CHILD_*`. **None of them means "the MessageRun for this node has been published to NATS."**

`_process_init_node` does:
```reference
noderun = await self.construct_noderun(...)   # 1. Postgres INSERT (status=CREATED)
...
await self.sender.send_dict(message_sent, topic=step.name)   # 2. NATS publish
```

Two systems, two operations, no transaction between them. `CREATED` therefore means *"a row exists"* — it does **not** distinguish:

| Actual situation | Stored status |
|---|---|
| Row inserted, publish crashed before it happened | `CREATED` |
| Row inserted, published, the original executor hasn't picked it up yet | `CREATED` |
| Row inserted, published, the original executor is running inference **right now** | `CREATED` |

All three are indistinguishable. So on redelivery you genuinely cannot answer "was it sent?", and the only safe-looking choice is to send again. **That is not a tuning problem; it is a missing state.** This is the classic dual-write / transactional-outbox problem, and it is unaddressed.

> `PROCESSING` does get set — but by `process_event`, from an event the original executor publishes *after* it picks the message up. So it tells you the original executor started, not that you dispatched. Between publish and that event there is a window of unbounded length (the message sits in the stream until a consumer is free) where the state is indistinguishable from "never sent".

## 1.2 The redelivery detector is itself a race — **[verified]**

`update_noderun_output` is how the controller decides whether a result is a duplicate:

```reference
noderun = await self.noderun_repo.get_by_slug(node_slug)    # READ
if noderun.status == NodeStatus.FINISHED:
    return noderun, False                                    # -> redelivered = True
return await self.noderun_repo.update(...FINISHED...), True  # WRITE
```

Read, decide, write — **no lock, no compare-and-swap**. Two deliveries of the same `MessageResult` landing on two controller replicas both read a non-`FINISHED` status, both write `FINISHED`, and **both return `updated=True`**. Neither is flagged as redelivered. Both fan out to the next steps.

The detector fails precisely in the situation it exists to detect.

**And note the asymmetry** — the same repository has a *correctly locked* update path. `NodeRunRepository.update_status` does:
```reference
select(self.Schema).where(self.Schema.slug == slug).with_for_update()
```
That one takes a row lock and applies `should_update_status_from`. But `update_noderun_output` calls the original's generic `EntityRepository.update` instead, which has **no locking and no status guard** (the original, and `for_update` appears nowhere in the base it inherits from). Two update paths in one class — one safe, one not — and the result hot path uses the unsafe one.

**Consequence beyond duplicates:** because it bypasses `should_update_status_from`, a late result arriving for a `CANCELLED` noderun overwrites it with `FINISHED`. Cancelled work resurrects itself.

## 1.3 The guard you added only covers one of three cases — **[verified]**

the original:
```reference
if redelivered and noderun.status == NodeStatus.FINISHED:
    logger.warning("Redelivered noderun already finished, skipping it!", slug=noderun.slug)
    return
```

This is the guard against re-triggering. It fails in two of the three states that matter:

| Next node's state when a duplicate result arrives | Guard fires? | Result |
|---|---|---|
| `FINISHED` | yes | correctly skipped |
| `PROCESSING` (inference running right now) | **no** | **re-published → the model runs twice concurrently** |
| `CREATED` (dispatched, not yet picked up) | **no** | **re-published → duplicate queued** |

And it is gated on `redelivered`, which §1.2 shows is unreliable. So under true concurrency both copies have `redelivered=False`, the guard is not even consulted, and you get a full duplicate fan-out.

This is exactly the behaviour you described. The guard is not wrong so much as *unable to be right* — it is asking a question (`is this done?`) when the question it needs answered is `has this already been dispatched?`, which §1.1 shows is not recorded anywhere.

## 1.4 No idempotency key on the publish — **[verified]**

There is no `Nats-Msg-Id` header set anywhere, and no `duplicate_window` configured on any stream. So even when the controller re-publishes a byte-identical `MessageRun`, JetStream treats it as a brand-new message. There is no layer below the application that can absorb the duplicate.

Three cheap layers are all missing: message-ID dedup (NATS), a dispatch record (Postgres), and a status guard on the result write (§1.2).

---

## 1.5 Barrier races

### Aggregation barrier discards its own atomicity — **[verified]**

`increase_refcount` is **correct**: atomic `find_one_and_update` + `$addToSet` + `return_document=True`, returning the post-state set.

The caller throws that away and re-reads the document; then `_try_aggregate` reads a **third** time and decides completion from that separate read:
```reference
refcount = await self.run_repo.get_refcount(...)
...
if refcount != num_children:
    return None
```

Two children completing concurrently both `$addToSet`, both then read `2`, both see `num_children == 2`, **both aggregate**. Both mark the parent `AGGREGATED`, both walk up the parent chain, both emit every downstream step.

The prerequisite barrier twenty lines away does it **correctly** by using the returned set. Same file, two barriers, one right.

**Reproduced against a real MongoDB, not only read.** Two threads run the same
sequence the code does — the atomic `find_one_and_update` with `$addToSet` and
`return_document=AFTER`, its result discarded, then a separate `find_one` that
decides completion from the length of the set — against `mongo:5.0.28`, the
version the compose file pins:

```text
  trials: 200
  trials where MORE THAN ONE child decided to aggregate: 200

  the prerequisite barrier, deciding from the RETURNED set instead:
  trials where the count was not exactly one: 0
```

Both barriers, same database, same concurrency, 200 trials each. The one that
uses the value the atomic update handed back is correct every time; the one that
re-reads is wrong every time.

**What that does and does not establish.** The two threads synchronise on a
barrier between the update and the read, so the interleaving is forced rather
than stumbled into. That is deliberate: the question is whether the sequence
*permits* two children to both observe a complete set, and it does —
unconditionally, because after both `$addToSet`es have landed, any subsequent
read by either child sees `2`. It says nothing about how often the window is hit
in production, which depends on arrival timing and is not measurable from here.
What it removes is the possibility that some ordering property makes the race
unreachable.

### `set_children_number` is a lost update — **[verified, and measured]**
the original computes `(parent_step.num_children or 0) - len(skip_node)` from a document read earlier, and `set_children_number` is itself read-modify-write. Two conditionals resolving concurrently under one DictAggregator both read 5, both write 4; the answer is 3. The aggregator then waits forever.

**Reproduced against `mongo:5.0.28`**, two concurrent skips against a count of 5,
200 trials each:

```text
  read-compute-$set   trials with the WRONG count: 200/200
  $inc with a delta                  trials with the WRONG count:   0/200
```

The `$set` itself is atomic; the arithmetic in front of it is not. `$inc`
composes because each caller states only what *it* changed — but a bare `$inc`
is not idempotent, and the call site is **not** gated by the `redelivered` flag
computed the original. A redelivered conditional would decrement
twice, `num_children` would fall below the true child count, and the aggregator
would fire *early*, with a child's output missing. So the port records the **set
of skipped node ids** and derives the count from it: adding the same id twice
changes nothing, two different ids both land. Idempotent and composable at once,
which a counter cannot be. This is the second measured instance of one pattern — the first is
the aggregation barrier above — and the pattern is the same: an atomic primitive
used correctly, then its result thrown away in favour of a value computed
elsewhere.

### The prerequisite barrier re-fires — **[verified]**
`if len(completed_prerequisites) != step.num_prerequisites: return` uses `!=`. `$addToSet` is idempotent, so redelivery of the *final* prerequisite leaves the length equal and the join **fires a second time**. Bounded only by `ON CONFLICT DO NOTHING` on the noderun row — which does not stop the NATS publish.

---

## 1.6 Autoscaling: six specific problems

This is where your instinct is most correct — several of these only appear when replica count > 1.

### A. Scaling the controller does not increase throughput — **[verified]**
`MAX_ACK_PENDING=100` (`config/controller/.env`) is a property of the **durable consumer**, not of a replica. All controller replicas join one queue group against one durable, so 100 in-flight messages is a **global** ceiling. Adding replicas adds CPU and adds race exposure, but does not raise the ceiling. (The router's is worse: `MAX_ACK_PENDING=1` — see the survey notes)

### B. Scaling up monotonically increases corruption probability — **[verified]**
Every race in §1.2 and §1.5 is between two messages *for the same run*. With one replica, asyncio still interleaves them at `await` points, so the races are real but narrow. With N replicas they execute in true parallel across separate processes with no shared memory and no locks. **Every additional replica widens every race window.** This is why your failures correlate with autoscaling.

### C. Scale-down manufactures duplicates — **[verified]**
the original, in `OnShutdown`:
```go
if consumer.InProgressMessage != nil {
    _, err := consumer.NatsMessageHandler.JetStream.Publish(
        consumer.InProgressMessage.Subject, consumer.InProgressMessage.Data)
```
This **publishes a new message**, not a NAK. JetStream has no way to know it is the same work. So every scale-down event creates one genuine duplicate per in-flight message per terminating pod — while the original is *also* still pending redelivery. Git history shows this being reverted and partially restored twice, which suggests it has already caused visible pain.

The `NatsIngressTimeConsumer` variant does this correctly (NAK + `Drain()`, the original), but it is behind `INGRESS_TIME_STRATEGY_ENABLED`, default **false**.

### D. No graceful shutdown → multi-minute stalls — **[verified]**
`signal.NotifyContext` is created in both the original mains and never passed to the consume loops; the original services have no shutdown path at all. A killed pod's in-flight messages simply wait out `AckWait` — **300 seconds** for the controller. Every scale-down stalls that work for up to 5 minutes.

### E. Connection-pool exhaustion is a feedback loop — **[verified]**
the original's pool defaults are `POOL_SIZE=5, MAX_OVERFLOW=10` → **15 Postgres connections per replica**, with the original ORM's default 30s checkout timeout (no override).

Meanwhile the controller's processing semaphores default to **disabled** (the original, all `False`), and `on_message` fire-and-forgets via `asyncio.create_task`. So a replica can run up to `MAX_ACK_PENDING = 100` concurrent handlers.

Worse, the original's `ensure_session` opens a **new session per repository method call** when no session is injected — and `process_result` makes many such calls (`get_by_slug`, `update`, `get_by_parent_slug`, …). So 100 concurrent handlers each checking out and returning connections repeatedly, against a pool of 15.

The loop: load rises → pool saturates → 30s checkout timeouts → handler exceptions → messages unacked (and §1.2's DLQ is dead) → redelivery after 300s → **more load**. Autoscaling on CPU makes this worse, because the pool, not CPU, is the bottleneck.

**Turning on `BACKEND_*_PROCESSING_SEMAPHORE_ENABLED` and setting `MAX_CONCURRENT` at or below the pool size is a one-line mitigation you can ship today.**

### F. There is nothing correct to autoscale on — **[verified]**
No metrics exist on any pipeline service. HPA on CPU is actively misleading here: a consumer blocked on a 300-second HTTP call to a model uses ~0% CPU while being maximally busy. The correct signal is **consumer lag / `NumPending`**, which nothing exports. **[inferred]** you are likely scaling on the wrong signal, which would explain scaling behaviour that seems to make throughput worse rather than better.

---

## 1.7 Ordering and cross-store atomicity

- **No cross-subject ordering.** the original `result` subject and the original `event` subject are separate subjects with separate consumers. A node's `result` can be processed before its `PROCESSING` event. `process_event` and `process_result` then write the same noderun row concurrently via different paths — one locked (`update_status`), one not (`update`). **[verified]**
- **Cross-database non-atomicity.** The noderun row is Postgres; the barrier is Mongo. A crash between them leaves permanently inconsistent state with no reconciliation path. There is no operation in the system that can update both atomically. **[verified]**
- **Mongo is standalone in compose** — no `--replSet`. On a standalone mongod, `WriteConcern("majority")` is equivalent to `w:1` and **multi-document transactions are unavailable**. Single-document atomicity (`$addToSet`) still holds, so the primitives work — but the "majority" concerns sprinkled through the original are decorative. **[verified for dev compose; needs a production check]**

## 1.8 Why tuning alone will not fix this

| Symptom | Tuning helps? | Why not sufficient |
|---|---|---|
| Duplicate inference on slow steps | partly | Raising `AckWait` above the model timeout removes the *trigger*, but §1.2's unlocked read-write still duplicates under genuine concurrency |
| Next node triggered twice | no | Requires a "dispatched" state that does not exist (§1.1) |
| Duplicate aggregation | no | Requires using the atomic return value (§1.5) |
| Message loss | no | `defer DeleteMsg` and the dead DLQ are logic bugs |
| Scale-down duplicates | no | `OnShutdown` re-publishes by design |
| Pool exhaustion collapse | **yes** | Enable the semaphores; this one really is config |

**Minimum correct set, in order:**
1. `AckWait` > max inference time, or `msg.InProgress()` heartbeats — removes the main duplicate trigger.
2. Enable the processing semaphores at ≤ pool size — stops the collapse loop.
3. Set `Nats-Msg-Id` + a stream `duplicate_window` — absorbs residual duplicates for free.
4. Make the result write a guarded, locked transition (reuse `update_status`'s `FOR UPDATE` + `should_update_status_from`) — makes "already processed" a fact rather than a guess.
5. Add a `dispatched_at` / `DISPATCHED` state written **in the same transaction** as the noderun insert, with the publish driven from an outbox — makes §1.3's guard answerable.
6. Use `increase_refcount`'s returned set instead of re-reading — makes aggregation exactly-once.
7. Fix `OnShutdown` to NAK + drain rather than re-publish.

Items 1–3 are config and a few lines. Items 4–6 are the real fix and are exactly what the Rust port should encode structurally.

---

# Part 2 — One database, or two?

## Your original reasoning, re-examined

You chose Mongo for developer experience: dynamic schema, no unit-of-work ceremony, no migrations, "no need to care much about transactions and race conditions."

The first three delivered. **The fourth inverted.** Not needing to *care* about transactions is only true if you don't need them. This system needs them: it has barriers, counters, status machines, and parent-chain walks — all of which are concurrent read-modify-write. Removing transactions from the toolbox didn't remove the requirement; it relocated it into application code, where it was implemented **twice, inconsistently, and one of the two is wrong** (§1.5). The bugs in Part 1 are not incidental to that choice; they are largely downstream of it.

That's the honest accounting: Mongo bought real velocity on schema, and charged for it in coordination correctness.

## The options

### Option A — Postgres only ★ recommended

**What moves:** `pipelines`, `runs`, `runhistory` become Postgres tables with `JSONB` columns. Barriers become the `noderun_barrier` table from the original plan. Everything else already lives there.

**What you gain**
- **One transaction covers the whole logical operation.** Noderun status transition + barrier arrival + dispatch record commit together or not at all. This single change kills §1.2, §1.5, §1.7's cross-store split, and makes §1.1's outbox implementable.
- `INSERT … ON CONFLICT DO NOTHING` for idempotency (already in use).
- `SELECT … FOR UPDATE` for guarded transitions (already in use in one place — this makes it uniform).
- `SELECT … FOR UPDATE SKIP LOCKED` is an excellent durable-queue primitive if you ever want to reduce NATS surface.
- Real foreign keys on the self-referential noderun tree, which is genuinely relational (`parent_slug`, queries by parent/run/status).
- One datastore to run, back up, monitor, and tune. The gateway already uses sqlx.

**What you give up**
- JSONB querying is more awkward than Mongo's query language for deep document access. Mitigated by the fact that `state` should be **write-once** (see below) and is fetched whole, not queried into.
- Migrations become mandatory for schema changes. But you already run the original migration tool for Postgres — this consolidates onto tooling you already operate rather than adding new burden.
- Dynamic-schema authoring for pipeline definitions: `JSONB` handles this fine; you lose Mongo's native document validation, which you weren't using.

**The DX objection, answered:** the "no migrations" benefit largely evaporates once you notice that `state` should never be mutated after bootstrap. Today it is mutated (`num_children`, `refcounts`, `completed_prerequisites`) — which is *why* it feels like it needs a flexible schema. Move those three to real tables and the run document becomes an immutable blob, at which point JSONB is strictly as convenient as Mongo, and cacheable besides.

### Option B — Mongo only

**Viable but weaker.** Multi-document transactions exist from Mongo 4.0 **on a replica set** — and your compose runs a standalone (§1.7), so you'd have to stand one up regardless. Mongo's transactions carry a 60-second default limit and a real performance cliff, and the official guidance is to design to avoid them rather than lean on them.

The noderun tree is relational: self-referential parent links, filtered queries by parent and run, status aggregates. You can model it in Mongo, but you lose referential integrity, and the janitor's and gateway's queries get harder rather than easier. You'd also be moving *away* from the one store the already-shipped Rust gateway uses for its writes.

Choose this only if document-shaped access dominates and you're prepared to operate a replica set and live inside transaction limits.

### Option C — a different database

Not compelling. CockroachDB/Yugabyte buy distribution you don't need. FoundationDB is a correctness dream and an operational tax. Redis is not durable enough for run state. None of these address the actual problem, which is that coordination and the ledger live in different stores.

### Option D — the question you should ask before either ★ read this one

**What you have built is a durable workflow engine.** Not "a system that uses queues" — an engine with DAG resolution, fan-out/fan-in barriers, conditional branching, cancellation propagation, retry policy, and run history. That is precisely the category Temporal, Restate, and DBOS occupy.

Every single defect in Part 1 is something those systems solve *by construction*:

| Your problem | How an engine removes it |
|---|---|
| No "dispatched" state (§1.1) | Durable execution — the engine records what was scheduled |
| Duplicate next-node trigger (§1.3) | Activity dedup / exactly-once activity semantics |
| Racy barriers (§1.5) | Fan-out/fan-in is a first-class primitive |
| Ack vs. inference timeout (§1.6) | Heartbeats + per-activity timeouts, built in |
| Scale-down duplicates (§1.6C) | Worker drain is part of the protocol |
| Cross-store atomicity (§1.7) | One durable log |
| No run visibility | A UI, out of the box |

**This matters right now because you are about to rewrite it in Rust.** Porting a hand-rolled workflow engine is a large investment in *owning* this problem forever. It is worth consciously deciding to own it rather than arriving there by default.

Honest counter-arguments for keeping your own:
- **YAML-authored DAGs.** Temporal wants workflows as code. If non-engineers author pipelines, a declarative DAG is a genuine product requirement. You can run a YAML interpreter *on* Temporal, but you lose some of its ergonomics.
- **Operational weight.** A Temporal cluster is real infrastructure. Restate is lighter and Rust-native. **DBOS is Postgres-backed durable execution**, which is interesting here because it would answer Part 2 and Part 3 with one decision.
- **The Rust SDK story is the weakest part.** Temporal's Rust SDK is less mature than Go/Java/TS. If you're committed to Rust, this cuts against adoption.
- Your domain has specifics (per-tenant subject routing, Seldon contract, page-level fan-out) that you'd still own.

**My recommendation:** timebox a genuine spike — one week, one representative pipeline (nested aggregator + conditional) on Restate or Temporal — *before* committing to Phase 8 of the port. Phases 0–4 of the original plan (core types, transport, broker, the original executor) are useful either way. Phase 8 is the controller, and it's ~45% of the effort; that's the one you'd be able to delete.

## Recommendation

**Consolidate onto Postgres**, and make the run document immutable. It resolves the largest class of bug in Part 1, unifies you with the gateway's existing sqlx usage, and costs you a JSONB ergonomics tax that is smaller than it looks once `state` stops being mutated.

**But run the Option D spike first**, because if an engine wins, the storage question mostly dissolves — and so does the most expensive phase of the port.

---

# Part 3 — Would this work as a shared framework?

Short answer: **the problem is absolutely worth a framework; this is not one yet.** It's roughly 80% of an orchestrator and close to 0% of a developer product. And critically — **shipping it as a framework before fixing Part 1 would be actively harmful.**

## Why "not too simple"

To answer your question directly: no, it is not too simple to be worth doing. Durable, cancellable, observable fan-out over document pages with automatic retry is genuinely hard, and most teams get it wrong. The value proposition is real.

The trouble is that the hard part you've solved (orchestration) is invisible to your users, while the parts you haven't (contracts, validation, local iteration, error messages) are the entire surface they touch.

## Two personas, both underserved

### Pipeline authors — write the YAML DAG

Concrete failures in what exists today:

- **No validation whatsoever.** A typo in a `children` reference produces `logger.warning("child = %s not found in registry")` and then continues — silently under-counting `num_prerequisites`, so the run hangs forever in production. A typo should be a load-time error, not an unbounded stall. **[verified]**
- **No cycle detection.** `_update_nextnode` has no visited set. A cyclic definition is accepted and becomes either infinite recursion (conditionals recurse synchronously via the original) or an infinite message loop burning inference budget with nothing to stop it. **[verified]**
- **`start`/`end` are unvalidated magic strings**. Name a node `end` and the graph silently misbehaves.
- **The same field means different things in different node types.** `ListAggregator` uses only `start_ids[0]` and silently discards the rest; `DictAggregator` iterates all of them. Undocumented. **[verified]**
- **`start` accepts a bare string or a dict** via an undocumented coercion.
- **No local execution.** Testing a pipeline requires Mongo, Postgres, NATS, RabbitMQ, and live model containers. There is no dry-run, no in-memory runner, no `validate` command.
- **Errors are internal exceptions.** A misconfigured DictAggregator surfaces as `NumChildrenNotFoundError` in controller logs — not "node X: DictAggregator has no reachable terminal children".
- **No visualisation.** The `.d2` files in `tests/assets/` are hand-drawn, which tells you the need is real and unmet.

> Your own canonical fixture, `tests/assets/pipeline1.yaml`, is a DictAggregator with no Conditional child — which per the survey notes are exactly the shape that cannot complete. And **no test executes a pipeline** (`tests/` covers cancel, stale, gateway, YAML loading only). That is how a broken core shape stayed invisible.

### Component authors — write the AI model container

- **The contract is undiscoverable.** `{"jsonData": {...}}` → `{"jsonData": {"step_output": ...}}` is documented nowhere; you learn it by reading the original source. **[verified]**
- **The one sample is actively misleading.** the original sample is in the legacy V1 format and does **not** match the V2 structs the code parses. A developer following it writes a broken component. **[verified]**
- **No SDK.** Every component re-implements envelope parsing and error shaping.
- **Undocumented magic status codes.** HTTP **449** (non-standard) means prediction error; 400 means internal error; 500 triggers retry. A component author cannot know this.
- **`step_input`/`step_output` are untyped** (`interface{}` / `Any`). A contract mismatch between adjacent nodes is a runtime failure with no schema to check against — arguably the single biggest daily pain in a pipeline system.
- **No local harness** to run a component against a recorded fixture.

## The blocking objection

**A framework amplifies the cost of core defects.** Right now, when a run hangs, you can go read the original. Your users cannot. They will attribute silent hangs and duplicate inference to their own pipelines, they will have no tools to diagnose it (no metrics, no run inspector, dead DLQ), and every one of those becomes a support conversation you have to have.

Duplicate inference is especially bad to ship: it's invisible (both copies return valid results) and it costs money on someone else's budget.

**Do not productise this until Part 1 items 1–6 are done.**

## What would make it genuinely valuable

If you fix the core, the framework is worth building, in this order:

1. **Pipeline validation + dry-run.** `pneuma validate pipeline.yaml` catching unknown children, cycles, reserved names, unreachable nodes, aggregators with no terminal child, and type mismatches between connected steps. **Highest DX return per unit of effort by a wide margin** — it converts your worst failure mode (silent production hang) into a build-time error.
2. **Local runner.** Execute a pipeline in-process with stub components, no infrastructure. This is what makes iteration feel fast, and it is only possible if the core logic is separable from I/O — which is exactly what the original plan's `pneuma-core` / `pneuma-engine` split buys you. **These two goals are the same work.**
3. **Typed step contracts.** Declare each node's input/output schema; validate adjacency at load time and payloads at runtime. Turns the most common daily error into a checked one.
4. **Component SDK** (original first) handling the envelope, error codes, and heartbeats, so component authors write a function, not a protocol.
5. **Run inspector.** "Where is my run and why is it stuck" — the parent chain, barrier state, which prerequisites are outstanding. You have the data; nothing surfaces it.
6. **Generated diagrams** from the YAML, replacing the hand-drawn `.d2` files.

## The strategic tension

Note what items 1–5 actually are: **validation, local execution, typed contracts, an SDK, and a run inspector.** Temporal, Restate, and DBOS ship most of that in the box. If you go the framework route, you are committing to building a developer product on top of a workflow engine you also maintain — that is two products, and the second one is where your differentiation actually lives (document pipelines, page fan-out, model-serving integration), not the first.

**The strongest version of your framework idea is probably: your YAML DAG model, your component SDK, your validation and local runner — on top of someone else's durable execution engine.** That is Option D from Part 2, and it is the same decision.

---

# What I would do next, concretely

**This week — stop the bleeding (config + small patches, no architecture change):**
1. `CONSUMER_ACK_WAIT` above your p99 inference time, or add `msg.InProgress()` heartbeats.
2. Enable `BACKEND_*_PROCESSING_SEMAPHORE_ENABLED` with `MAX_CONCURRENT` ≤ 15 (the pool size), or raise the pool size.
3. Raise the router's `MAX_ACK_PENDING` off `1`.
4. Fix the termination parser (`{data}` not `{items}`) — cancellation is currently inert.
5. Disable the `OnShutdown` re-publish; enable `INGRESS_TIME_STRATEGY_ENABLED` for its correct drain path.
6. Rename `__handle_dead_message` → `_handle_dead_message` — you already wrote a working DLQ.
7. Export `NumPending` per consumer, and autoscale on that rather than CPU.

**Next — make duplicates harmless rather than rare:**
8. `Nats-Msg-Id` + stream `duplicate_window`.
9. Route the result write through the locked, guarded path (`update_status`'s `FOR UPDATE` + `should_update_status_from`).
10. Use `increase_refcount`'s returned set instead of re-reading.

**Then — decide the big questions before Phase 8 of the port:**
11. One-week spike: representative pipeline on Restate or Temporal.
12. If you keep ownership: consolidate onto Postgres, make `state` immutable, add a `DISPATCHED` state written transactionally with an outbox.
13. Only then consider productising as a framework, starting with validation and the local runner.
