# Pregel — what actually transfers to pneuma

> Companion to `CONCURRENCY-AND-DIRECTION.md`. Google's Pregel ([Malewicz et al., SIGMOD 2010](https://blog.acolyer.org/2015/05/26/pregel-a-system-for-large-scale-graph-processing/)) is a distributed graph-computation model, not a workflow engine, and applying it literally would be a bad fit. But one of its primitives — the **combiner** — is exactly the missing piece in your aggregator barrier, and it's worth stealing deliberately rather than reinventing worse.

## The mechanism, verified

Pregel implements the **Bulk Synchronous Parallel** (BSP) model over a graph:

- Computation proceeds in **supersteps**. In superstep *S*, the framework calls a user `compute()` function on every *active* vertex, passing it every message sent to it during superstep *S-1*. `compute()` can send messages to neighbors (delivered in superstep *S+1*) and call **`vote_to_halt()`**.
- A **global barrier** separates supersteps: no vertex enters superstep *S+1* until every vertex — across every machine — has finished superstep *S*.
- **Vote to halt / reactivation:** a halted vertex is woken up automatically if it receives a message in a later superstep. The whole computation terminates when every vertex has voted to halt **and** no messages are in flight.
- **Combiners:** when multiple messages target the same vertex in the same superstep, a user-supplied associative/commutative `combine()` function (sum, min, max, …) merges them into one before delivery — the framework does this, not the vertex's own code.
- **Aggregators:** a separate, framework-wide reduction visible to all vertices at the start of the next superstep (global stats, termination flags).
- **Fault tolerance:** periodic checkpointing of the whole graph state at superstep boundaries; on worker failure, roll back to the last checkpoint and replay.

## Why applying it literally would be wrong

Pregel's target shape is: **one huge static graph, billions of cheap homogeneous vertices, many rounds until convergence** (PageRank, connected components). Yours is the opposite on every axis: **many small heterogeneous DAG instances (one per document), each executed once, vertices that are expensive external HTTP calls to AI models taking up to minutes.**

The disqualifying problem is the **global barrier**. If you synchronized supersteps across the whole system, every concurrent run would have to wait for the slowest document's slowest AI call before *any* run advanced — a straggler blocking every other tenant's unrelated work. This is not a hypothetical: it's [the exact failure mode](https://blog.acolyer.org/2015/05/29/powergraph-distributed-graph-parallel-computation-on-natural-graphs/) that drove GraphLab/PowerGraph to build an asynchronous alternative, and later work ([Giraph Unchained, VLDB 2015](http://www.vldb.org/pvldb/vol8/p950-han.pdf)) to remove Pregel's barrier entirely while keeping its programming model. **Don't adopt BSP. The graph-processing community already learned this lesson for you.**

## What does transfer — the combiner, precisely

Here is the mapping, made concrete against your own code.

**Your `_try_aggregate` today** (the original, and the race in `CONCURRENCY-AND-DIRECTION.md` §1.5): each child result triggers an atomic `$addToSet` (correct), then the caller **re-reads** the set separately and compares its length to `num_children` in application code. Two children finishing concurrently both observe "complete" and both proceed — because the *decision of completeness* lives in racy app-level read-then-compare code, not in the delivery mechanism itself.

**A Pregel combiner solves exactly this, by construction:** an aggregator node is a vertex expecting *N* messages. The framework — not the vertex, not application code — holds arriving messages, and delivers `compute()` to the vertex **exactly once**, atomically, only when the combine is complete. There is no window where two callers can both observe "ready" and both proceed, because the combine-and-deliver step is one operation owned by the substrate, not two reads separated in time.

This is precisely what the `noderun_barrier` table in the original plan/`CONCURRENCY-AND-DIRECTION.md` is trying to be — but naming it "a Pregel combiner, scoped per run, per aggregator vertex" gives you a well-specified contract to implement against instead of ad hoc SQL:

```sql
-- delivery IS the combine: arrival and completeness-check are one atomic statement.
WITH arrival AS (
    INSERT INTO vertex_mailbox (vertex_slug, message_slug, payload_ref)
    VALUES ($1, $2, $3) ON CONFLICT DO NOTHING RETURNING 1
)
SELECT count(*) = (SELECT expected FROM noderun WHERE slug = $1) AS combine_complete
FROM vertex_mailbox WHERE vertex_slug = $1
FOR UPDATE;
-- only the caller that observes combine_complete = true, inside this same
-- transaction, is permitted to call compute() (init_next_step) for $1.
```

That's the same primitive already specified — this note just gives it the correct name and the discipline that comes with it: **the aggregator vertex's `compute()` may only ever be invoked by whichever transaction wins the atomic completeness check, and never by a separate re-read.** State that as an invariant in the design doc; it's the one-sentence fix for the whole bug class.

## What else transfers

**Vote-to-halt / reactivate-on-message as your termination rule.** Your current `process_aggregation_and_next_steps` terminates a run via an ad hoc `while` loop walking up the parent chain until something yields `next_nodes` or hits root. Pregel's termination condition is a clean, checkable invariant instead: **a run is concluded when every reachable vertex has voted to halt (reached a terminal `NodeStatus`) and no messages for that run are in flight (the outbox/mailbox is empty).** That's a query you can run to *verify* a run is actually done, not just a traversal you hope terminated correctly — directly useful for the janitor's stale-run detection and for a debugging "why is this run stuck" tool (`STRATEGY.md`'s run-inspector item).

**Per-run local supersteps, not a global one.** The adaptation that makes this safe: scope the BSP model to a *single run's* DAG, not the whole system. Run A's "superstep 3" and run B's "superstep 1" have no relationship and no synchronization between them — each run is its own tiny, independently-progressing BSP computation. This is what avoids the straggler problem above, and it's also just a description of what a correctly-fixed pneuma controller already does: each run advances independently, driven by its own messages. Pregel gives you the vocabulary to reason about *within* a run cleanly (superstep = "the wave of steps whose prerequisites the previous wave just satisfied") without importing the global barrier that makes Pregel wrong for you across runs.

**Checkpointing validates, doesn't add to, the outbox design.** Pregel checkpoints at superstep boundaries so a crashed worker resumes from the last consistent point rather than the beginning. Your transactional-advance-plus-outbox design (`CONCURRENCY-AND-DIRECTION.md` §Path A) is the finer-grained, continuously-checkpointing analog of this — every advance transaction *is* a checkpoint. No new work here; take it as confirmation the design is pointed the right way.

## What I would not bother with

- **Aggregators** (Pregel's global-reduction primitive) — designed for whole-graph statistics across billions of vertices in one computation. You have no equivalent need; per-run state is already what your noderun tree is.
- **Adopting Giraph, GraphX, or any actual Pregel implementation as a dependency.** These are batch analytics engines for large static graphs computed by data teams, not long-lived services executing continuous streams of small heterogeneous workflow instances with slow, externally side-effecting vertex computation. Wrong tool, same verdict as Restate/DBOS in `STRATEGY.md`: steal the pattern, not the platform.

## One honest caveat

I could not find literature that makes this exact combiner-as-workflow-barrier connection explicitly — the mapping above is my own synthesis from the mechanism, not a cited established pattern. It holds up structurally against your code, but treat it as a design analogy you're choosing to adopt, not as "the graph-processing field already validated this for workflow engines." The `Barriers` port design in the original plan and the atomic-arrival transaction in `CONCURRENCY-AND-DIRECTION.md` are correct on their own merits regardless of the Pregel framing — this note just gives you the vocabulary and the invariant to state precisely when you implement and review it.

## Sources

- [Pregel: A System for Large-Scale Graph Processing (the morning paper)](https://blog.acolyer.org/2015/05/26/pregel-a-system-for-large-scale-graph-processing/) — supersteps, `compute()`, vote-to-halt, combiners, aggregators
- [PowerGraph: Distributed Graph-Parallel Computation on Natural Graphs (the morning paper)](https://blog.acolyer.org/2015/05/29/powergraph-distributed-graph-parallel-computation-on-natural-graphs/) — synchronous vs. asynchronous GAS, why strict BSP wastes time on stragglers
- [Giraph Unchained: Barrierless Asynchronous Parallel Execution in Pregel-like Graph Processing Systems (VLDB 2015)](http://www.vldb.org/pvldb/vol8/p950-han.pdf) — removing the global barrier while keeping the vertex-centric/combiner model
