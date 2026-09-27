# Flows — how a run actually happens

Five diagrams: the primary path end to end, the fallback path, a component
failure, cancellation, and the two background loops that keep the system honest.

## 1. Submission to completion — the Restate path

```mermaid
sequenceDiagram
    autonumber
    participant P as Producer
    participant A as pneuma-admission
    participant PG as PostgreSQL
    participant R as Restate server
    participant H as pneuma-restate
    participant C as component

    P->>A: POST /pneuma-admission/runs
    A->>A: resolve the pipeline
    Note over A: refused here if it cannot run —<br/>before it costs a quota slot
    A->>PG: INSERT submission ON CONFLICT DO NOTHING
    A-->>P: 202 {run_id, status: queued}

    loop every PNEUMA_DISPATCH_INTERVAL_SECS
        A->>PG: nextval(dispatch_round)
        A->>PG: queued backlogs, per tenant
        A->>A: select_batch — weighted, rotated by round
        A->>PG: claim those rows
        loop each claimed submission
            A->>R: POST /PneumaRunner/run/send<br/>idempotency-key: run_id
            R-->>A: Accepted | PreviouslyAccepted
            A->>PG: settle done
        end
    end

    R->>H: invoke PneumaRunner/run
    loop each superstep
        H->>H: which steps are runnable now?
        par each runnable step
            H->>R: ctx.run(call component)
            R->>C: POST {meta, step_input, …}
            C-->>R: {step_output}
            R-->>H: journalled result
        end
        H->>H: record outputs, recompute
    end
    H-->>R: the run's outputs
```

**Why a claim, and why it can be reclaimed.** Between `claim` and `settle` the
dispatcher can die. The row stays `claimed` with a `claimed_at`, and after
`PNEUMA_RECLAIM_AFTER_SECS` the sweep returns it to `queued`. Without that, a
dispatcher crash strands every submission it had in flight.

**Why the round comes from a sequence.** `select_batch` rotates its tie-break by
the round number, and a batch that does not divide evenly has to hand the
remainder to somebody. With a per-process counter that is the same tenant every
time — measured at 800 items against 600 over 200 rounds, while every individual
batch was provably fair. Two replicas with their own counters both sit near zero
and favour the same tenant. A sequence is the one counter shared and monotonic
across both.

## 2. Submission to completion — the NATS path

```mermaid
sequenceDiagram
    autonumber
    participant P as Producer
    participant N as NATS
    participant D as pneuma-driver
    participant MG as MongoDB
    participant E as pneuma-executor
    participant C as component

    P->>N: publish pneuma.run.start
    N->>D: delivery
    D->>MG: load the run document
    D->>D: start an Execution on a LocalSet

    loop each runnable step
        D->>D: register (run_id, node_id) in the correlation table
        D->>N: publish pneuma.step (MessageRun)
        Note over D,N: flush, bounded — a publish into<br/>a local buffer is not a publish
        N->>E: delivery (queue group)
        E->>C: POST {meta, step_input, …}
        C-->>E: {step_output}
        E->>N: publish pneuma.result (MessageResult)
        E->>N: publish pneuma.event (finished)
        N-->>D: result, to every replica
        D->>D: claim it, or report Unclaimed
        D->>MG: record the node's output
    end

    D->>MG: finish the run
```

**Why every replica sees every result.** The executor publishes to a *fixed
configured subject*, not to a reply inbox, so NATS request–reply is not
available. A run's `Execution` lives in the replica that started it, so the
subscription is plain rather than a queue group: the replica holding the run
claims the result and the others say `Unclaimed`. A queue group here would
deliver a result to a replica that has never heard of the run.

**Why a `LocalSet`.** `Component::call` deliberately promises no `Send`, because
the primary implementation is Restate's `ctx.run`, which returns a future with
no `Send` bound. Requiring `Send` on the trait would have made the *primary*
transport unimplementable in order to make the fallback more convenient.

## 3. A component that fails

```mermaid
sequenceDiagram
    autonumber
    participant E as pneuma-executor
    participant C as component
    participant N as NATS

    E->>C: POST (attempt 1)
    C--xE: connection reset
    Note over E: typed predicate → retryable
    E->>C: POST (attempt 2)
    C-->>E: 503
    Note over E: 5xx → retryable
    E->>C: POST (attempt 3)
    C-->>E: 200 {step_output}
    E->>N: pneuma.result
    E->>N: pneuma.event finished

    Note over E,C: and when the attempts run out
    E->>C: POST (final attempt)
    C-->>E: 400
    Note over E: 4xx → not retryable, ever
    E->>N: pneuma.event error (error_code, error_message)
    Note over E,N: no result — the run is told the step<br/>ended rather than left waiting
```

The distinction that matters: an error event **without** a result still advances
the run. The original's failure mode was a node that stayed `PROCESSING` for
ever because the only thing that would have moved it was a result that was never
coming.

## 4. Cancellation, and why a late result cannot undo it

```mermaid
stateDiagram-v2
    [*] --> CREATED
    CREATED --> PROCESSING: dispatched
    CREATED --> FORKED: fanned out
    PROCESSING --> FINISHED: result
    PROCESSING --> ERROR: failed
    PROCESSING --> TIMED_OUT: no answer
    FORKED --> AGGREGATED: every child landed
    FORKED --> HAS_CHILD_ERROR: a child failed
    FORKED --> HAS_CHILD_TIMED_OUT: a child timed out

    CREATED --> CANCELLED
    PROCESSING --> CANCELLED

    FINISHED --> [*]
    ERROR --> [*]
    TIMED_OUT --> [*]
    CANCELLED --> [*]
    AGGREGATED --> [*]
    HAS_CHILD_ERROR --> [*]
    HAS_CHILD_TIMED_OUT --> [*]

    note right of CANCELLED
        Terminal. Every UPDATE that
        moves a node carries
        `status NOT IN (…, 'CANCELLED')`
        in its WHERE clause.
    end note
```

The guard is the `WHERE` clause, not a read-then-write. The original read the
row `FOR UPDATE`, decided in original, then issued a separate `UPDATE` — and
`CANCELLED` was missing from its terminal list, so a result arriving after a
cancellation resurrected the node. Here the decision and the write are one
statement and there is no window between them
(the design notes).

`FORKED` has a second rule: a node may only become `FORKED` **from** `CREATED`.
That too is in the `WHERE`.

### Who writes these rows, and when

The diagram above was aspirational until the design notes: `node_run` had a
schema, a store and a janitor, and no producer at all. Both drive paths now
write it, through one `Recorder` (`pneuma-mirror`) over derivation that is pure
and shared (`pneuma_runner::record`), so a fan-out records the same way under
Restate as under the broker.

```mermaid
sequenceDiagram
    autonumber
    participant D as drive
    participant R as Recorder
    participant PG as node_run

    D->>R: dispatching(step)
    R->>PG: create(CREATED)
    R->>PG: update_status(PROCESSING)
    Note right of PG: `started_at` is stamped<br/>by this second write
    D->>D: component call
    alt the component answered
        D->>R: finishing(step, output)
        R->>PG: record_output(FINISHED)
    else it did not
        D->>R: failing(step, why)
        R->>PG: update_status(ERROR)
        loop each enclosing aggregator
            R->>PG: update_status(HAS_CHILD_ERROR)
        end
    end

    Note over D,PG: and what the interpreter did on its own
    D->>R: happened(FannedOut)
    R->>PG: create(aggregator, CREATED)
    R->>PG: update_status(FORKED)
    Note right of PG: once per aggregator,<br/>before any branch's row —<br/>`parent_path` is a<br/>non-deferrable foreign key
    D->>R: happened(Aggregated)
    R->>PG: record_output(AGGREGATED)
```

Two writes for one dispatch, because both instants collapse into one here and
`FORKED` is admitted only *from* `CREATED` — so a step that went straight to
`PROCESSING` would leave its aggregator unable to fork. `TIMED_OUT` and
`HAS_CHILD_TIMED_OUT` appear in the diagram above and are never written: the
transport does not say whether a failure was a timeout or a refusal, so every
failure is `ERROR`. Both are §25.

## 5. The dispatch round

```mermaid
flowchart TD
    START(["tick"]) --> ROUND["next_round<br/>nextval(dispatch_round)"]
    ROUND --> BACKLOG["queued_backlogs<br/>per tenant, oldest first"]
    BACKLOG --> EMPTY{"anything queued?"}
    EMPTY -->|no| DONE(["wait for the next tick"])
    EMPTY -->|yes| SELECT["select_batch<br/>weighted quota, rotated by round"]
    SELECT --> CLAIM["claim — queued → claimed,<br/>stamping claimed_at"]
    CLAIM --> LOOP{"for each claimed row"}
    LOOP --> SUBMIT["POST to the Restate ingress<br/>idempotency-key: run_id"]
    SUBMIT --> OK{"accepted?"}
    OK -->|yes| SETTLE["settle done"]
    OK -->|"no — 4xx"| FAILED["settle failed, with the detail"]
    OK -->|"no — unreachable"| LEAVE["leave it claimed"]
    LEAVE -.->|"after RECLAIM_AFTER_SECS"| RECLAIM["reclaim → queued"]
    SETTLE --> LOOP
    FAILED --> LOOP
    LOOP -->|done| DONE
```

**An unreachable Restate leaves the row claimed on purpose.** Settling it
`failed` would be a permanent verdict on a transient problem; the reclaim sweep
is what turns "we could not reach it" back into "try again".

## 6. Reconnect supervision

Both brokers share one supervisor, because the decision is the same for both.

```mermaid
stateDiagram-v2
    [*] --> Connecting
    Connecting --> Up: connected
    Connecting --> Backoff: refused
    Up --> Dropped: connection lost
    Dropped --> Backoff: decide
    Backoff --> Connecting: after the delay
    Up --> [*]: shutdown requested

    note right of Dropped
        The attempt counter resets on
        *survival*, not on attachment.
        A broker that accepts and
        immediately drops would
        otherwise never back off.
    end note
```

That note is a real bug that was caught in review: resetting the counter on
`Up` meant a flapping broker was reconnected to as fast as the loop could spin.
The reset is now gated on the connection having *lasted*, which is what
`Dropped { after }` carries.

## 7. Delivery settlement

```mermaid
flowchart TD
    D["a delivery arrives"] --> H["hand it to the handler"]
    H --> R{"what did it say?"}
    R -->|"handled"| ACK["ack"]
    R -->|"unrecoverable"| DLQ["dead-letter"]
    R -->|"transient"| NACK["nack, requeue"]
    H -->|"the handler panicked"| DROP["drop without acking"]
    DROP --> REDELIVER["the broker redelivers"]

    ACK --> DONE(["done"])
    DLQ --> DONE
    NACK --> REDELIVER
    REDELIVER --> D
```

A `Settle` that panics **during unwinding** aborts the process, so the drop path
catches. That is not defensive programming for its own sake: the drop path runs
inside `Drop`, and a panic there while already panicking is an immediate abort
with no chance to log why.
