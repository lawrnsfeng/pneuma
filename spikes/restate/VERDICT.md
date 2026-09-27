# Restate spike — verdict

**Decision: ADOPT**, against the rule fixed in the plan before anything was run (rows 1–5 pass; 6–8 pass or pass-with-work).

Every row below is backed by something executed against a real Restate server (`restatedev/restate:latest`, admin `:19070`, ingress `:18080`) driving the **real corpus fixtures** — the same YAML `pneuma-core` is tested against, not invented examples. Where a measurement turned out to be wrong, that is recorded rather than quietly corrected.

## Rubric

| # | Capability | Result | Evidence |
|---|---|---|---|
| 1 | Data-driven DAG (YAML, not code) | **Pass**, with a caveat | All four corpus pipelines executed from YAML via `pneuma-core::resolve`. The interpreter is code; the graph stays data. Caveat in §1. |
| 2 | Dynamic fan-out | **Pass** to 2,000 | `n=1000` and `n=2000` completed; `n=3000` did not finish in 400 s. Degradation, not an error or documented cap. |
| 3 | Aggregation barrier, exactly-once | **Pass**, decisively | The barrier collapsed to a local counter. See §2 — the most consequential finding here. |
| 4 | Nested aggregators, 4 levels | **Pass** | `pipeline_nested_list_dict` — 33 steps, 21 model calls, correct nested output. |
| 5 | Conditional branching | **Pass** | `pipeline_condition_list` on both branches: 10 steps/5 calls vs 5 steps/3 calls — measurably different paths. |
| 6 | Large payloads | **Pass** | 32 MB journaled in a single value. My first measurement was **wrong**; see §3. |
| 7 | Tenant fairness | **Pass-with-work** | No fairness/priority/weight/quota anywhere in the SDK surface. `limit_key` exists — per-key concurrency limiting. See §4. |
| 8 | Zero-SDK component contract | **Pass** | The stub AI service is a plain axum app that has never heard of Restate, called through `ctx.run(|| reqwest …)`. |
| 9 | Operational cost | **Pass** | One container, no external database. Registration is a single `POST /deployments`. |
| 10 | Rust SDK maturity at 0.11.1 | **Pass-with-work** | Everything attempted worked, mostly first try. Pre-1.0 API churn is the standing risk. See §5. |

**Durability itself was verified, not assumed.** `kill -9` on the service mid-invocation, then restart with no re-registration and no re-invocation: the original request returned `"completed 1500 of 1500"` on its own. This is the core claim of the whole product category, and it holds.

---

## §1 — The DAG stays data, but the determinism constraint does not vanish

`STRATEGY.md` differentiator #1 says pneuma has *no determinism constraint because there is no workflow code*. That survives the spike, but needs a correction that matters.

Restate replays handlers against a journal, so the **interpreter** must be deterministic. The DAG being data does not change that — it relocates the constraint. What it *does* change is scope: instead of every pipeline author being able to break replay, there is exactly one interpreter to write carefully and audit once. That is a real and large improvement, but it is not zero, and the earlier framing overstated it.

Concretely, the spike's own interpreter has a latent version of this: it uses `HashMap` for bookkeeping. It happens to be safe because control flow is driven by an ordered `Vec`, not by map iteration — but a future edit that iterated a `HashMap` to decide the order of `ctx.run` calls would corrupt replay silently. In production this wants a deterministic map and a written rule, not luck.

## §2 — The aggregation barrier disappears entirely

The most consequential result.

`CONCURRENCY-AND-DIRECTION.md` §1.5 documents pneuma's aggregation barrier as racy: `increase_refcount` is atomic, but the caller discards its return value and re-reads, so two children completing concurrently both observe "complete" and both fan out. `ARCHITECTURE-V2.md` designs a `noderun_barrier` table, a `SKIP LOCKED` dispatcher, and an outbox to fix it.

Inside a Restate durable handler, **none of that machinery is needed**. The whole barrier is:

```rust
let counter = arrived.entry(next.clone()).or_insert(0);
*counter += 1;
if *counter >= next_step.common().num_prerequisites.max(1) as usize {
    ready.push((next, out.clone()));
}
```

A local counter in a `HashMap`. There is no distributed state to race on, because the handler is one logical thread that Restate replays rather than distributes. The `pipeline_params` diamond confirmed it: node `D` has two prerequisites and executed exactly once.

The defect class does not get fixed — it becomes **unrepresentable**. That alone is most of the argument for adopting.

## §3 — A correction: my first payload measurement was wrong

Worth recording, because it would have become a false finding.

Running the corpus pipeline with increasing padding, 2 MB passed and 3 MB failed. The obvious conclusion — "Restate caps journal values somewhere near 2–3 MB, exactly the objection raised against Temporal" — was **wrong**.

Reading the actual error showed `"error decoding response body"` attributed to a `RunCommand` — a *reqwest* error inside my own closure, not a Restate rejection. Two checks settled it:

1. Calling the stub directly with `curl`: 4 MB downloaded fine, so neither the stub nor curl was the limit.
2. An HTTP-free probe journaling a generated string: **8 MB, then 16 MB, then 32 MB all journaled without complaint.**

So the ceiling was in the spike's HTTP client, and Restate's journal handled 32 MB. The payload objection that genuinely applies to Temporal (4 MB gRPC, ~50 K history events) **does not transfer to Restate** at the sizes tested.

The claim-check pattern is still worth adopting — moving OCR-scale blobs by reference rather than by value is right regardless of engine, as `LANDSCAPE-AND-EVOLUTION.md` §3.1 argues — but it is no longer a *precondition* for this engine.

## §4 — Fairness stays pneuma's own work

A grep of the entire SDK surface for `priority|fairness|weight|quota|rate.?limit` returns **nothing**.

There is one adjacent primitive: an invocation can be submitted with a **`limit_key`**, readable from the handler. A tenant id maps onto it naturally, and it is more than nothing. But a per-key concurrency *cap* is isolation, not work-conserving weighted fair queuing — an idle tenant's unused share is not redistributed. Its precise semantics could not be verified: web search was unavailable for the whole spike, so this rests on the SDK source alone.

This is consistent with, not contrary to, `STRATEGY.md`: fairness is the differentiator **nothing** in the landscape does natively, and it remains pneuma's to build regardless of which engine sits underneath. The `SKIP LOCKED` + weighted-quota dispatcher from `FRAMEWORK-FOUNDATIONS.md` §5 does not become unnecessary — it moves in front of Restate instead of in front of NATS.

## §5 — SDK maturity

`restate-sdk 0.11.1`, MIT, `rust-version 1.90.0`. Everything attempted worked, and mostly first try: `#[service]`, `#[object]`, `#[workflow]`, `ctx.run`, `ctx.sleep`, `DurableFuturesUnordered`, `TerminalError` vs retryable errors. The in-crate examples were sufficient documentation with web search unavailable — which is itself a positive signal.

No SDK bugs were hit. The standing risk is pre-1.0 API churn, the same caution that ruled Temporal out for a Rust shop. The difference is that Temporal's Rust SDK is explicitly *not production-ready* with no 1.0 date, while this one is a shipped 0.11 on a 7.x shared core.

---

## What was NOT tested

Stated plainly, because a spike that overclaims is worse than none:

- **Concurrency and contention.** Every run was a single workflow instance. No parallel tenants, no load, no contention. The fairness question is therefore untested in practice, not merely unsupported.
- **Fan-out concurrency in the interpreter.** The pipeline interpreter fans out **sequentially** (a `for` loop). `DurableFuturesUnordered` was exercised only in a separate probe. Real page-level fan-out would use the latter, and its behaviour inside a deep nested walk is unknown.
- **Cancellation**, timers as a workflow feature, and anything resembling the janitor's retention sweep.
- **Failure semantics beyond one `kill -9`.** No partial failure, no poison message, no retry-exhaustion path, no DLQ equivalent.
- **Multi-node Restate.** Single container, embedded storage. Nothing about clustering, failover, or the partition leadership model was touched.
- **Anything about operating it long-term** — upgrades, backup, observability integration.

## Recommendation

Adopt Restate for execution, and drop `pneuma-engine` from the port. Concretely, that removes the transactional-advance design, the `noderun_barrier` table, the outbox, the lease/heartbeat machinery, and the `SKIP LOCKED` dispatcher **for execution purposes** — roughly the 45% of remaining effort that carried all the concurrency risk.

What does **not** go away, and should not be assumed away:

- **`pneuma-core`** — the DSL, resolver, evaluator, slug and status types. The spike consumed it directly as a library and it did its job; this is the first time it has been exercised from outside its own test suite.
- **Tenant fairness** (§4) — pneuma's own, in front of Restate.
- **The component contract** — preserved exactly, and now demonstrated (§8).
- **`pneuma-proto`** — needed either way.

Before committing irreversibly, the two gaps most worth closing are **concurrency/fairness under real load** and **failure semantics beyond a single kill**. Both are cheap to test now that the harness exists, and either could change the answer.

---

## Reproducing

```sh
docker run -d --name restate-spike --add-host=host.docker.internal:host-gateway \
  -p 18080:8080 -p 19070:9070 restatedev/restate:latest
cd spikes/restate && cargo build && ./target/debug/restate-spike &
curl -X POST http://localhost:19070/deployments -H 'content-type: application/json' \
  -d '{"uri":"http://host.docker.internal:9080","force":true}'
curl -X POST http://localhost:18080/Runner/demo/run -H 'content-type: application/json' \
  -d '{"pipeline":"pipeline_nested_list_dict","tenant_id":"t1","input":{"doc":"d"}}'
```

Port 8080 on this host was already taken by an unrelated process, hence the remapping. The spike crate is deliberately **outside the main workspace** (own `[workspace]` table), so it never touches the 100% coverage gate or `--workspace` lints.

---

## Addendum — the concurrency gap, closed

The recommendation above says two gaps should be closed *before committing
irreversibly*, and that either could change the answer. The next crate in the
port is that commitment, so the first of them was closed before starting it.
This is not a re-run of the evaluation; it is the precondition the verdict set,
using the harness the verdict left behind.

### Concurrency and contention — **pass, and it settles a design question**

60 invocations across 60 distinct workflow keys and 6 tenants, fired at
concurrency 20 against the same single-container Restate:

```text
  wall                                   903 ms
  responses with steps_executed = 4      60 / 60
  distinct outputs                       60 / 60
  errors                                 0
```

Every run completed correctly and no output was crossed between keys. The
"single workflow instance, no contention" caveat in *What was NOT tested* no
longer applies to the parallel-keys case.

### What contention on **one** key does — the part worth knowing

Invoking the same workflow key five times concurrently:

```text
  1 x  the run's result
  4 x  {"code":409,"message":"the workflow method was already invoked"}
```

And after completion, a further invocation of that key returns `409` too, while
the result remains retrievable:

```sh
GET /restate/workflow/Runner/<key>/output   -> the original result
```

**A workflow key is single-use, permanently.** That is a stronger guarantee than
anything the current system has: a redelivered message cannot re-execute a run,
by construction. It is exactly the property the aggregation barrier fails to
provide today — and it is provided by the engine rather than by discipline in
every caller.

It is also a constraint the service crate must be built around, so it is stated
here rather than discovered later:

- A run id maps to a workflow key, and **retry means fetch the output, not
  invoke again**. A caller that treats `409` as an error will turn every
  redelivery into a failure.
- A run that must genuinely be re-executed needs a *new* key. "Re-run this job"
  is therefore a new run id, not a re-invocation — which matches how the
  platform already numbers runs, but is now load-bearing rather than incidental.

### Still open

**Failure semantics beyond one `kill -9`** — partial failure, poison messages,
retry exhaustion, DLQ equivalent. Untouched, and still worth closing. The
concurrency result does not bear on it.

### Failure semantics — **pass, with one operational consequence**

The second of the two gaps. A failure mode was added to the stub component
(`fail_times` in the step input makes that many attempts return `500`), which is
the zero-SDK contract failing the way a real component would.

**A transient failure is invisible to the caller.** Three injected `500`s:

```text
  run completed                          yes
  elapsed                                4 s
  component attempts                     7   (3 failed + 4 steps)
```

Restate retried the `ctx.run` block and the workflow completed. Nothing in the
interpreter handles this, and no caller sees it.

**A permanent failure backs off rather than hammering.** With every attempt
failing:

```text
  attempts after 12 s                    5
  attempts after 30 s                    6
  attempts after ~40 s                   7
```

**It does not give up on its own** — within the window observed, the invocation
stayed `backing-off` and kept retrying. No retry-exhaustion was reached and
nothing was discarded.

**The stuck invocation is fully visible**, which is more than the current system
offers for a wedged message:

```sql
SELECT id, target, status, retry_count FROM sys_invocation
-- inv_1bnLw8...  Runner/failforever/run  backing-off  7
```

**And it can be stopped.** `DELETE /invocations/{id}?mode=kill` returned `202`,
and the component attempt count was unchanged six seconds later — the retries
genuinely stopped rather than merely being hidden.

#### The consequence worth planning for

There is **no automatic dead-letter equivalent**. Today a poison message
eventually exhausts `MaxDeliver` and lands in a DLQ (when the DLQ works —
The defect notes). Here it retries, visibly, until someone intervenes.

That trade is favourable — nothing is silently lost, and `sys_invocation` names
exactly what is stuck and how many times it has tried, where the current system
gives a log line and a message that has already been deleted
(the defect notes). But it is an *operational* difference, not a free
win: something has to watch for `status = 'backing-off'` with a growing
`retry_count`, or a poison run waits forever without complaining. That belongs
in the same place the queue-depth metrics of the defect notes belong.

#### Still not tested

Multi-node Restate, cancellation as a workflow feature, timers, and long-term
operation. None of those bear on the adopt/reject decision; they are
deployment questions.

**Both gaps the recommendation named are now closed, and neither changed the
answer.**
