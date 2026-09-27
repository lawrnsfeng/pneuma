# pneuma vs. the ecosystem — and how it should evolve

> Answers three things: (1) do your two Temporal objections still hold, with concrete pointers; (2) what exists in OSS that overlaps what you built, and what to steal from it; (3) a concrete evolution path to fully-async execution that survives autoscaling and concurrency.

---

# Part 1 — Your two Temporal objections, re-checked

You rejected Temporal for two reasons. **One doesn't hold up. The other does — but not for the reason you gave, and it has a standard fix.**

## Objection A: "Temporal can't do full-async execution from a graph like mine"

**This is not correct, and there is a project that does precisely what your controller does.**

### Parallel activity execution is a first-class pattern

Temporal's [Parallel Execution pattern](https://docs.temporal.io/design-patterns/parallel-execution) launches activities asynchronously and awaits them together — `asyncio.gather()` in original, `Promise.all()` in TypeScript, `Promise.allOf()` in Java, `Future.Get()` in Go. Fan-out/fan-in is a documented core pattern, not a workaround.

Ironically, Temporal's own docs recommend `asyncio.gather(*tasks, return_exceptions=True)` for partial results — **the exact call your controller uses the original**. The difference is that in Temporal the activity results are durably recorded, so discarding the exception loses nothing; in yours it silently drops a branch forever.

### Dynamic DAG interpretation already exists

[**temporalgraph**](https://temporal.io/code-exchange/temporalgraph-graph-based-orchestration) ([github.com/Nickqiaoo/temporalgraph](https://github.com/Nickqiaoo/temporalgraph/)) is a Go library that composes Temporal workflows as DAGs with typed I/O, branching, and merge semantics. You call `AddNode` / `AddEdge` / `Compile`, and it produces a deterministic runner that — quoting the description — *"submits ready nodes as activities in parallel, waits for completion, propagates outputs, and calculates next tasks until END."*

That sentence **is your controller's algorithm**, including the `END` sentinel. Fan-out/fan-in are described as first-class citizens with a parallel task submission and completion loop.

Caveat worth stating: it's a community project, not officially supported by Temporal. But it demonstrates the pattern is natural, not fought-for — you write one generic interpreter workflow that reads your YAML and schedules activities from it. Your DSL survives; the engine underneath changes.

### The real architectural difference (which you may have been sensing)

There *is* a genuine distinction, and it's worth naming precisely because it's the strongest thing about your design:

- **Temporal centralises a run.** One workflow execution owns the DAG state. Its event history is the source of truth, and workflow tasks for a given run are processed with affinity to one worker at a time.
- **You decentralised it.** There is no orchestrator process holding a run. Every controller replica can advance any run, because state lives in the database and each message independently moves the graph forward.

Your model is closer to a **distributed dataflow engine** than to a workflow orchestrator, and that is a legitimate, deliberate-looking design with real benefits (§4). The catch is that decentralised coordination demands exactly the rigorous concurrency primitives that `CONCURRENCY-AND-DIRECTION.md` shows are missing. Temporal centralises specifically so those problems can't arise.

## Objection B: "It can't do much with sidecar model serving"

**This one doesn't hold either — and Temporal's mechanism is a strict improvement over what you built.**

The mapping is almost one-to-one:

| Your design | Temporal equivalent |
|---|---|
| One the original executor deployment per AI component | One worker pool per component |
| `MATCH_PATTERN` (which subjects this pod consumes) | **Task Queue** name |
| `MESSAGE_ENDPOINT` → `POST /predict` on the sidecar | Activity body calling `localhost:8000/predict` |
| broker appending `.tenant_<id>` | Task queue routing / search attributes |
| `CONSUMER_ACK_WAIT` guesswork | `HeartbeatTimeout` + `StartToCloseTimeout` |

An activity is just code — calling a model sidecar over localhost HTTP is unremarkable. Temporal's [ML/AI workflows post](https://temporal.io/blog/ai-ml-and-data-engineering-workflows-with-temporal) describes routing activities to dedicated infrastructure explicitly: *"you can define activities that are restricted to other infrastructure, such as a high-powered GPU server, through the use of Task Queues"* — and notes teams *"fire up a GPU node, run models, and then tear down the node."* Descript is cited running video enhancement, voice training, and transcript generation this way.

### The part that matters most for you

[Activity heartbeats](https://docs.temporal.io/encyclopedia/detecting-activity-failures) solve your single most expensive bug by construction. The guidance is exactly the fix I recommended independently:

- `StartToCloseTimeout` must exceed the maximum possible execution — *"if an Activity Execution can take anywhere from 5 minutes to 5 hours, you need to set Start-To-Close to be longer than 5 hours."*
- `RecordHeartbeat()` periodically during long inference, with `HeartbeatTimeout` ≈ 2× the heartbeat interval.

Your `CONSUMER_ACK_WAIT=30` vs `REQUEST_TIMEOUT=300` is that exact problem, and heartbeats are the exact answer. Temporal separates "is the worker alive?" (heartbeat) from "how long may this take?" (start-to-close) — you have one knob doing both jobs, badly.

## The objection you *should* have raised

Neither of your stated objections holds. But there's a real one you didn't mention, and it's the strongest argument against Temporal for **your specific workload**:

**Payload size.** Temporal records activity inputs and outputs in the workflow's event history. There is a 4 MB gRPC message limit, a ~50K event history limit, and a cap of **2,000 pending activities per workflow execution**.

Your `step_input`/`step_output` are arbitrary JSON blobs — OCR output for a page, layout analysis, extracted fields. Those get large. Pushing them through event history is the wrong shape, and a big document fan-out plus many steps will approach the event limit.

This is a genuine constraint — but it has a standard answer (the claim-check pattern, §3.1), and honestly **you should adopt that answer regardless of which engine you use**, because right now those same blobs are travelling through NATS messages *and* being stored in Postgres `step_output` *and* echoed back in `MessageOutputV2`. Passing references instead of payloads is a win in your current architecture too.

---

# Part 2 — Where your system actually sits

Your system straddles two ecosystems that mostly don't talk to each other.

## Category 1 — Durable execution engines

**Temporal, Cadence, Restate, DBOS, Hatchet, Inngest.**

They solve: exactly-once step execution, durable state, automatic retry with backoff, timers, cancellation propagation, versioning, run visibility. Every defect in `CONCURRENCY-AND-DIRECTION.md` Part 1 is something this category solves by construction.

The 2026 landscape has shifted since you evaluated: [Restate, Inngest, Hatchet and DBOS have all crowded into the category Temporal pioneered](https://tiarebalbi.com/en/blog/dbos-vs-temporal-postgres-durable-execution). The notable argument is [DBOS's "Postgres is all you need"](https://www.dbos.dev/blog/postgres-is-all-you-need-for-durable-execution) — durable execution as a *library* writing checkpoints to Postgres, rather than a separate orchestration service. Their [comparison](https://www.dbos.dev/compare/dbos-vs-temporal) claims adding durability takes ~7 lines versus >100 plus rearchitecting into worker/server plus running a Temporal cluster, with each checkpoint being a single Postgres write against Temporal's async dispatch overhead.

Treat vendor comparison numbers with suspicion, but the structural point stands and it's directly relevant to your Part 2 storage decision: **if you consolidate onto Postgres anyway, a Postgres-backed durable execution library is nearly free to adopt.**

## Category 2 — ML/data pipeline orchestrators

**Airflow, Flyte, Prefect, Dagster, Metaflow, Kubeflow, Argo Workflows.**

[Argo Workflows](https://kestra.io/resources/infrastructure/argo-workflows-alternatives) is the closest on *authoring model* — YAML-defined DAGs as Kubernetes CRDs, which is your YAML DSL with a K8s control plane. [Flyte](https://mlai.qa/blog/prefect-vs-metaflow-vs-flyte-vs-airflow-mlops-2026/) is the closest on *developer experience* — strong typing, caching/memoisation, versioned reproducible pipelines, per-task resource control. That typed-and-cached story is precisely the DX gap I flagged in your framework assessment.

Why they don't simply replace you: this category is batch/scheduled-oriented with per-run overhead measured in seconds, and generally one-container-per-step. You're doing per-request document processing at volume against long-lived model servers. Flyte also carries real operational commitment — the same source notes that for teams under ~10 ML engineers, lighter tools deliver most of the value at a fraction of the overhead.

## Category 3 — Inference graphs / model serving

**This is where your closest sibling lives, and you should look at it hard.**

### Seldon Core v2 Pipelines — you have rebuilt this

[Seldon Core 2](https://docs.seldon.ai/seldon-core-2/about/concepts) is built around a **dataflow paradigm**. From their [architecture docs](https://docs.seldon.ai/seldon-core-2/about/architecture) and [dataflow docs](https://docs.seldon.ai/seldon-core-2/user-guide/data-science-monitoring/dataflow):

- A `Pipeline` CRD wires models into a **DAG of steps**.
- It **runs on Kafka**: an inference request lands on a pipeline input topic, which triggers evaluation.
- Each node in the graph is *"a service running in its own container fronted by a **model gateway** that listens to a corresponding input Kafka topic, reads data from it, calls the service and puts the received response to an output Kafka topic."*
- It natively supports **both synchronous and asynchronous** operation, async via streaming.

Read that model-gateway description again. **That is the original executor.** Kafka instead of NATS, a `Pipeline` CRD instead of your YAML, `MATCH_PATTERN` instead of topic-per-step. You have independently reimplemented Seldon Core v2's Pipeline feature — while already using Seldon-style model serving (`/predict`, the `jsonData` envelope).

This is the single most important finding in this document. Before writing any Rust, spend a day with Seldon Core 2's Pipelines and work out honestly what you have that it doesn't. My read of the gap: **per-tenant routing, page-level fan-out with aggregation, and conditional branching.** Those are real. But sequential/parallel model composition over a streaming substrate is solved there, in production, by a team maintaining it full-time.

Also in this category: **Ray Serve** (deployment graphs, model composition, async, autoscaling), **KServe** (inference graphs), **NVIDIA Triton** (ensembles / business-logic scripting), **BentoML**.

---

# Part 3 — What to learn, concretely

## 3.1 Claim-check: never move payloads through the control plane

Every mature engine passes **references**, not blobs. Today your OCR output travels in a NATS message, is written to Postgres `step_output`, and is **echoed back again** inside `MessageOutputV2` (the original — the output message re-includes `step_input`). That's three copies of the same payload per step.

Store blobs in object storage keyed by `{run_id}/{slug}`; move `{uri, size, content_type, checksum}`. This shrinks messages, removes the Temporal event-history objection, makes Mongo/Postgres rows small, and makes replay cheap. **Do this regardless of every other decision here.**

## 3.2 Separate liveness from duration

Heartbeat + `HeartbeatTimeout` answers *"is the worker alive?"*; `StartToCloseTimeout` answers *"how long may this legitimately take?"* You have one knob (`AckWait`) doing both, which is why it is impossible to set correctly — too short and slow inference duplicates, too long and crashes stall for minutes.

In NATS terms: call `msg.InProgress()` on a ticker to extend the ack deadline, and keep `AckWait` short (say 30s) as a genuine liveness signal. This is a small change with a large payoff.

## 3.3 Route by queue, not by subject-name pattern

Temporal task queues do declaratively what your `MATCH_PATTERN` + subject-scanning producer + broker hop do imperatively. A worker pool subscribes to a named queue; the scheduler puts work on it. No stream-metadata polling, no `.tenant_<id>` string concatenation service, no subject-cardinality growth with tenant count.

You can have this in NATS today: one durable consumer per component with an explicit filter, pull-based, plus tenant as a **message header or a database column** rather than a subject-name suffix. That deletes broker entirely and removes the `O(tenants × steps)` scan from your hot loop.

## 3.4 Typed step contracts and caching (from Flyte)

Declare each node's input/output schema; validate adjacency at pipeline-load time. This converts your most common daily failure — a contract mismatch between adjacent nodes — from a production runtime error into a load-time one. Flyte additionally memoises by content hash, which for deterministic OCR on an unchanged page is a direct cost saving on retries and reruns.

## 3.5 Idempotency as a substrate property, not application logic

Every engine in Category 1 makes duplicate suppression structural. You do it nowhere. Minimum viable version: a deterministic message ID plus a dedup window at the broker, and a `(slug, attempt_id)` unique key at the database. Then duplicates become *harmless* rather than *rare* — which is the only durable state to be in, because at-least-once delivery guarantees they will happen.

## 3.6 Run visibility

Every engine ships a UI showing where a run is and why it's stuck. You have the data (noderun tree, barrier state, parent chain) and surface none of it. This is the highest-leverage operability gap and is mostly a read-only query problem.

## 3.7 Explicit versioning of in-flight definitions

Temporal has versioning APIs precisely because changing a workflow mid-flight is dangerous. **You already solved this well** — snapshotting the resolved `state` into the run document means a definition change can't corrupt in-flight runs. Keep this. It's one of the genuinely good decisions in the codebase.

---

# Part 4 — Credit where it's due: what your design does better

A fair comparison has to include this, and it should shape what you keep.

1. **Genuinely decentralised orchestration.** No workflow-to-worker affinity, no orchestrator owning a run. Any controller replica can advance any run. That is a real horizontal-scaling property Temporal doesn't have (it has per-run affinity for workflow tasks). Your ceiling is the database, not a scheduler.
2. **Very low per-step overhead.** A NATS publish plus a couple of DB writes is cheaper than a workflow-task round trip. For a high-volume, many-small-steps pipeline that matters.
3. **YAML-authored DAGs for non-engineers.** Temporal requires code. If pipeline authors aren't Go/original engineers, that's a hard product requirement, and Argo/Seldon (both YAML CRDs) are the only Category 1/3 tools that match you here.
4. **Definition snapshotting per run** (§3.7).
5. **First-class multi-tenancy.** Most engines bolt tenancy on via namespaces or labels. You designed around it — even if the current implementation is just a subject suffix with no enforcement.
6. **No hard fan-out ceiling.** Temporal caps at 2,000 pending activities per execution and ~50K history events. Your design has no equivalent structural limit — a 10,000-page document is a scaling question, not an architectural wall.

**The honest summary: your architecture is a reasonable distributed dataflow engine whose coordination layer was never finished.** The design instincts are sound. What's missing is the two dozen lines of transactional discipline that make decentralised coordination correct — which is exactly what Category 1 engines exist to provide, and exactly what you'd be building in the Rust port.

---

# Part 5 — Evolution proposal

Three viable paths. I recommend **A**, with a mandatory spike on **B** first.

## Path A — Finish the decentralised engine (recommended)

Keep the architecture. Add the coordination primitives it always needed. The whole thing reduces to one idea:

> **Advancing a run must be a single serialisable transaction, and publishing must be driven from an outbox in that same transaction.**

### A1. The advance transaction

Everything the controller does per result becomes one Postgres transaction:

```sql
BEGIN ISOLATION LEVEL READ COMMITTED;

-- 1. Idempotency gate. Duplicate results stop here, cheaply.
INSERT INTO node_attempt_result (slug, attempt_id, output_ref)
VALUES ($1, $2, $3)
ON CONFLICT (slug, attempt_id) DO NOTHING;
--    0 rows => already processed; COMMIT and return.

-- 2. Guarded transition (replaces the unlocked read-then-write)
UPDATE noderun SET status='FINISHED', output_ref=$3, finished_at=now()
WHERE slug=$1 AND status NOT IN ('FINISHED','CANCELLED','ERROR','TIMED_OUT')
RETURNING id;

-- 3. Record barrier arrival for every successor, idempotently
INSERT INTO barrier (target_slug, kind, member_slug)
SELECT ... ON CONFLICT DO NOTHING;

-- 4. Find successors whose barrier is now complete, locking them
SELECT target_slug FROM barrier b
 WHERE ... GROUP BY target_slug
HAVING count(*) = (SELECT expected FROM noderun WHERE slug=b.target_slug)
   FOR UPDATE;

-- 5. Create their noderuns AND their outbox rows together
INSERT INTO noderun (...) ON CONFLICT (slug) DO NOTHING;
INSERT INTO outbox (msg_id, subject, payload_ref, created_at) VALUES (...);

COMMIT;
```

A separate publisher drains `outbox` to NATS with `Nats-Msg-Id = msg_id` and a stream duplicate window, deleting rows after PubAck. At-least-once publish, broker-side dedup, exactly-once *effect*.

What this kills outright:
- §1.2 unlocked read-then-write → step 2 is guarded and atomic
- §1.3 "is it dispatched?" → the outbox row **is** the dispatch record; `CREATED + outbox row exists` is unambiguous
- §1.5 racy aggregation → step 4's `HAVING … FOR UPDATE` makes exactly one transaction observe completion
- §1.5 lost update on `set_children_number` → `expected_children` is a column set once, in the transaction that creates the children
- §1.7 cross-store non-atomicity → there is one store

Requires the Postgres consolidation from `CONCURRENCY-AND-DIRECTION.md` Part 2. That's the point: **the storage decision and the correctness fix are the same decision.**

### A2. Leases instead of ack windows

Give each dispatched node an `attempt_id` (UUID) and a `lease_expires_at`. The worker heartbeats to extend it. A sweeper reclaims expired leases and re-dispatches with a **new** `attempt_id`. Results carry their `attempt_id`, so a zombie worker's late result is rejected by step 1's unique key — it belongs to a superseded attempt.

This is the correct decomposition of your `AckWait` problem, and unlike ack tuning it is robust to autoscaling, pod eviction, and inference times you can't predict.

### A3. Fully async execution — the actual throughput work

| Change | Fixes |
|---|---|
| `asyncio.gather` the ListAggregator fan-out | 100 sequential round trips before page 1 starts |
| Batch barrier inserts — one multi-row `INSERT`, not N | N round trips per fan-in |
| Delete broker; publish tenant-suffixed directly | one hop, one `MAX_ACK_PENDING=1` ceiling, one lossy re-marshal |
| Replace the original executor's subject-scan producer with pull consumers on a filtered durable | `O(tenants × steps)` polling in the hot loop |
| Payload by reference (§3.1) | 3× payload copies per step |
| Cache the immutable run document (`moka`, keyed by run_id) | full-document read per message |
| Bound handler concurrency at ≈ DB pool size | the pool-exhaustion collapse loop |

### A4. Autoscaling survival

- Export `NumPending` per consumer; scale on **queue depth**, never CPU.
- Graceful drain: `CancellationToken` → stop fetching → finish in-flight → NAK the rest → exit. Delete the original executor's `OnShutdown` re-publish.
- Concurrency per replica = min(pool size, configured max) — enforced, not optional.
- `MAX_ACK_PENDING` sized per replica-count, not left at 1.
- Because every advance is idempotent (A1) and every execution is leased (A2), a pod dying mid-flight is *routine*, not exceptional.

### A5. Then the framework

Only after A1–A4: pipeline validation + dry-run, local runner, typed contracts, component SDK, run inspector. `pneuma-core`'s I/O-free design makes the local runner nearly free — same work, two payoffs.

## Path B — Adopt an engine, keep your DSL

Keep the YAML DSL, the tenant model, the component SDK, the validation. Replace the controller with a generic interpreter workflow (the temporalgraph pattern). the original executor becomes an activity plus a task queue. broker disappears.

You delete: the controller state machine, all barrier logic, retry channel, termination plumbing, janitor. That's ~45% of the Rust port — Phase 8 in the original plan.

**Preconditions:** claim-check payloads (§3.1); confirm fan-out width against the 2,000-pending-activity cap; accept an operational dependency.

**Engine choice for a Rust shop matters here.** Temporal's [Rust SDK entered public preview in May 2026](https://temporal.io/changelog/rust-sdk-public-preview) with fairly stable APIs, but is **explicitly not production-ready and has no 1.0 date**. That's a real risk for a Rust rewrite. [Restate](https://restate.dev) is Rust-native. DBOS is Postgres-backed and lightest to adopt, and shipped a Go SDK in 2026.

## Path C — Hybrid (what I'd actually expect to happen)

Path A's transaction and lease model, plus §3.1–3.6's patterns, plus a deliberate decision to own the engine. This is Path A done with the humility of having read how the others solved it.

## Recommendation

1. **This week:** the config fixes from `CONCURRENCY-AND-DIRECTION.md` — heartbeats/`AckWait`, semaphores at pool size, router `MAX_ACK_PENDING`, termination parser, `OnShutdown`, the two-underscore DLQ fix. Independent of everything below.
2. **Next:** a one-week spike — one representative pipeline (nested aggregator + conditional + page fan-out) on **Restate or DBOS**, not Temporal, because the Rust SDK risk makes Temporal the wrong first probe for your stack.
3. **In parallel, a one-day honest evaluation of Seldon Core v2 Pipelines.** You are already a Seldon shop. If its Pipelines cover 80% of your DAG needs, the remaining 20% (tenancy, page fan-out, conditionals) is a much smaller thing to build and own than a whole engine.
4. **Then decide**, and only then start Phase 8 of the port. Phases 0–4 (core types, transport, broker, the original executor) are useful under every path — start them now if you want momentum.
5. **Do §3.1 (claim-check) regardless.** It's a win in the current architecture and a precondition for two of the three paths.

The thing I'd most want you to avoid is spending the Rust port rebuilding, carefully and in a faster language, a workflow engine you could have adopted — while the actual differentiators (document pipelines, page fan-out, tenancy, the authoring DX) stay unbuilt.

---

## Sources

- [Parallel Execution | Temporal Platform Documentation](https://docs.temporal.io/design-patterns/parallel-execution)
- [temporalgraph — graph-based orchestration | Temporal Code Exchange](https://temporal.io/code-exchange/temporalgraph-graph-based-orchestration) · [repo](https://github.com/Nickqiaoo/temporalgraph/)
- [Detecting Activity failures | Temporal Platform Documentation](https://docs.temporal.io/encyclopedia/detecting-activity-failures)
- [The four types of Activity timeouts | Temporal](https://temporal.io/blog/activity-timeouts)
- [ML Workflows with Temporal | Temporal](https://temporal.io/blog/ai-ml-and-data-engineering-workflows-with-temporal)
- [Temporal's Rust SDK is now in Public Preview | Temporal](https://temporal.io/changelog/rust-sdk-public-preview)
- [Why Rust powers Temporal's new Core SDK | Temporal](https://temporal.io/blog/why-rust-powers-core-sdk)
- [Concepts | Seldon Core 2](https://docs.seldon.ai/seldon-core-2/about/concepts)
- [Architecture | Seldon Core 2](https://docs.seldon.ai/seldon-core-2/about/architecture)
- [Dataflow with Kafka | Seldon Core 2](https://docs.seldon.ai/seldon-core-2/user-guide/data-science-monitoring/dataflow)
- [Postgres-backed Durable Workflow Execution | DBOS](https://www.dbos.dev/blog/postgres-is-all-you-need-for-durable-execution)
- [DBOS vs. Temporal](https://www.dbos.dev/compare/dbos-vs-temporal)
- [DBOS vs Temporal: Choosing Durable Execution in 2026](https://tiarebalbi.com/en/blog/dbos-vs-temporal-postgres-durable-execution)
- [Argo Workflows Alternatives for K8s Orchestration | Kestra](https://kestra.io/resources/infrastructure/argo-workflows-alternatives)
- [Prefect vs Metaflow vs Flyte vs Airflow 2026](https://mlai.qa/blog/prefect-vs-metaflow-vs-flyte-vs-airflow-mlops-2026/)
