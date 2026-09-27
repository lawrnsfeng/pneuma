# Protocols — specification

**This page is a specification, not a description.** The port deliberately gave
up compatibility with the systems it replaces
(the design notes), so this is the only
statement of the current contract that exists anywhere. Anything that produces
or consumes a pneuma message is written against this page.

Every key here is **exactly** what goes on the wire. Where a key changed, the
old spelling is named so a reader porting a producer can see what to edit.

## The two paths

```mermaid
flowchart LR
    SUB["submission"] --> ADM["pneuma-admission"]

    subgraph P["Primary — Restate"]
        direction TB
        RS[("Restate server")] -->|invoke| RST["pneuma-restate"]
        RST -->|"HTTP POST"| C1["component"]
    end

    subgraph F["Fallback — NATS"]
        direction TB
        DRV["pneuma-driver"] -->|"publish pneuma.step"| N[("NATS")]
        N --> EXE["pneuma-executor"]
        EXE -->|"HTTP POST"| C2["component"]
        EXE -->|"publish pneuma.result"| N
        N --> DRV
    end

    ADM -->|"POST /PneumaRunner/run/send<br/>idempotency-key: run_id"| RS
    ADM -.->|"deployment without Restate"| DRV
```

They are **not** redundant. Restate is the primary: a component call is a
journalled `ctx.run`, so a crash resumes rather than re-calls. The NATS path is
for a deployment that cannot run a Restate server, per
[`BROKER-PORTABILITY.md`](../BROKER-PORTABILITY.md); it buys horizontal
scale-out of the component callers and pays for it with a correlation table
instead of a journal.

Both drive the **same** interpreter (`pneuma-runner`) over the same
`Component` trait. Only the transport differs, which is the property that keeps
them from drifting.

## NATS subjects

| Subject | Published by | Consumed by | Carries |
|---|---|---|---|
| `pneuma.input` | producers | `pneuma-broker` (queue group `pneuma-broker`) | `MessageRun` |
| `{type}.{level}.{name}.tenant_{id}` | `pneuma-broker` | per-tenant consumers | `MessageRun` |
| `pneuma.run.start` | producers | `pneuma-driver` | run announcement |
| `pneuma.step` | `pneuma-driver` | `pneuma-executor` (queue group) | `MessageRun` |
| `pneuma.result` | `pneuma-executor` | `pneuma-driver` (**plain** subscription) | `MessageResult` |
| `pneuma.event` | `pneuma-executor` | status consumers | `MessageEvent` |
| `pneuma.pipeline.create` | producers | `pneuma-intake` (AMQP) | pipeline definition |
| `pneuma.dlq` | any | dead-letter consumers | the message that failed |
| `pneuma.termination` | `pneuma-janitor` | termination consumers | termination request |

Every subject carries the `pneuma.` prefix.

Two things are deliberately **not** renamed: the `tenant_` prefix on a
per-tenant subject, and the `.dead_letter` suffix on an AMQP queue. The suffix's
twelve bytes are load-bearing — 244 + 12 = 256 overflows the AMQP `shortstr`,
which is the entire reason `QueueName::dead_letter` returns a `Result`.

### `pneuma.result` is not a queue group, and that is deliberate

The executor publishes a result to a **fixed configured subject**, not to a
reply inbox — so NATS request–reply cannot be used. A run's `Execution` lives in
the driver replica that started it, so every replica subscribes plainly, sees
every result, and only the one holding that run claims it. The rest report
`Unclaimed` and move on.

## Envelopes

### `MessageRun` — a step to execute

Field order is part of the contract: base fields first, then subclass fields,
with `headers` appended last. `extra` keys are emitted after the named ones, and
a caller extra that collides with a reserved key is **dropped** rather than
emitted twice.

```json
{
  "meta": {
    "job_id": "job-1",
    "tenant_id": "acme",
    "pipeline_type": "invoice",
    "pipeline_level": "page",
    "pipeline_name": "default",
    "pipeline_id": "invoice.page.default"
  },
  "step_input": { "doc": "invoice-0001.pdf", "page": 1 },
  "custom_data": null,
  "reply_to_result": "pneuma.result",
  "reply_to_error": "pneuma.dlq",
  "reply_to_event": "pneuma.event",
  "node": {
    "path": "job-1.invoice.page.default.extract",
    "node_id": "extract",
    "name": "invoice-extract",
    "kind": "Model",
    "pipeline_id": "invoice.page.default",
    "run_id": "job-1",
    "parent_id": null,
    "parent_path": null,
    "parent_kind": null,
    "child_index": 2,
    "sibling_index": 1,
    "parent_index": null
  },
  "node_env_vars": { "MODEL_VARIANT": "large" },
  "headers": { "traceparent": "00-…-01" }
}
```

`node.name` is the **subject to publish this step to** for a `Model`,
`ListAggregator` or `DictAggregator`. For a `Condition` there is no subject at
all — a condition is evaluated in-process and produces a `MessageResult`
directly. A dispatcher written as "the name is the subject" would publish to
`"condition"` and drop the work silently.

### `MessageResult` — what a component produced

```json
{
  "meta": { "…as above…" },
  "step_input": { "doc": "invoice-0001.pdf", "page": 1 },
  "custom_data": null,
  "reply_to_result": "pneuma.result",
  "reply_to_error": "pneuma.dlq",
  "reply_to_event": "pneuma.event",
  "node": { "…as above…" },
  "step_output": { "total": "129.90", "currency": "EUR" },
  "headers": { "traceparent": "00-…-01" }
}
```

`step_output` is required and must be an **object or an array** — never a
scalar. A component returning `{"step_output": "hello"}` is valid by the
component contract and rejected here, which is a real divergence in the original
system recorded in the protocol notes rather
than papered over.

### `MessageEvent` — a status report

```json
{
  "meta": { "…as above…" },
  "node": { "…as above…" },
  "event": "finished",
  "error_code": "TIMEOUT",
  "error_message": "component did not answer in 300s",
  "headers": { "traceparent": "00-…-01" }
}
```

`error_code` and `error_message` are omitted entirely when absent. `event` is
one of `created`, `processing`, `finished`, `error`, `timed_out`, `cancelled`,
`forked`, `aggregated`, `has_child_error`, `has_child_timed_out`. The legacy
inbound spelling `timeout` is **no longer accepted**: it existed only because
the original executor emitted it.

Only four of those statuses are written back to a node's stored record —
`finished`, `error`, `timed_out`, `cancelled`. The rest are derived by the
controller, and writing them back would record states no component ever sends.

### `MessageRetry` — a dispatch to try again

```json
{
  "subject": "invoice.page.default.tenant_acme",
  "message": { "…a whole MessageRun…" },
  "retry_count": 2
}
```

`subject` was `topic` and `message` was `content`. The wrapped message is an
opaque map, round-tripped without being parsed.

## The key rename, in full

| Old key | New key |
|---|---|
| `runinfo` | `node` |
| `slug` | `path` |
| `parent_slug` | `parent_path` |
| `type` (in `node`) | `kind` |
| `parent_type` | `parent_kind` |
| `child_idx` / `child_id` | `child_index` |
| `nth` | `sibling_index` |
| `type` (in `meta`) | `pipeline_type` |
| `level` (in `meta`) | `pipeline_level` |
| `name` (in `meta`) | `pipeline_name` |
| `topic_output` | `reply_to_result` |
| `topic_error` | `reply_to_error` |
| `topic_event` | `reply_to_event` |
| `topic` (in retry) | `subject` |
| `content` (in retry) | `message` |

Unchanged, deliberately: `meta`, `job_id`, `run_id`, `tenant_id`,
`pipeline_id`, `node_id`, `parent_id`, `parent_index`, `step_input`,
`step_output`, `custom_data`, `node_env_vars`, `headers`, `event`,
`error_code`, `error_message`, `retry_count`.

Also unchanged: the **pipeline definition**'s `type` discriminant. A definition
is authored outside this system and stored as written; the envelope is ours, the
definition is not.

### Two rules a producer must follow

- **An index of `0` means absent.** These indices are 1-based, so a producer in
  a language with non-nullable integers writes `0` for "nobody told me". `0`,
  `null` and an absent key all decode identically. Encoding omits them rather
  than writing `0`.
- **An empty string means absent** for `parent_id`, `parent_path` and
  `parent_kind`. A `parent_id` of `""` would otherwise satisfy "this node is
  nested" and send the driver looking for a parent that does not exist.

## HTTP surfaces

### `pneuma-admission`

```
POST /pneuma-admission/runs
Content-Type: application/json

{ "pipeline": { … }, "meta": { … }, "input": { … }, "custom_data": … }
```

| Status | Meaning |
|---|---|
| `202` | queued, or already queued — a redelivery is still in |
| `400` | the submission cannot be admitted, and the body says why |
| `409` | that run already happened, and the body says how it went |
| `503` | the queue could not be reached |

`202` carries `{"run_id": …, "status": "queued"}`. The whole submission is
stored, including keys pneuma does not know — a producer's extra survives.

### Health, on every long-running binary

```
GET /{service}/healthz   → 200 {"status":"ok"} — the process is running
GET /{service}/liveness  → 200 when every dependency answers, 503 otherwise
```

Prefixed with the service name because these are reached through one ingress,
and an unprefixed `/healthz` on six services is one route six ways.

### Restate ingress, as admission calls it

```
POST {PNEUMA_RESTATE_INGRESS}/{PNEUMA_RESTATE_HANDLER}/send
idempotency-key: {run_id}
```

`PNEUMA_RESTATE_HANDLER` defaults to `PneumaRunner/run`. The response
distinguishes `Accepted` from `PreviouslyAccepted`, which is why the dispatcher
needs no `409` branch.

**One hazard, measured.** Restate keys on the idempotency key alone and does not
compare request bodies: submitting a different pipeline under a key already used
returns the *first* submission's report, with `200` and nothing to indicate the
body was ignored. That binds on admission — a run id is minted once and never
reused. See the design notes

## Environment variables

Every variable carries the `PNEUMA_` prefix, except `DATABASE_URL`, which is a
de-facto convention (sqlx, Rails, Django) rather than an inheritance.

### `pneuma-admission`

| Variable | Default |
|---|---|
| `DATABASE_URL` | required |
| `PNEUMA_LISTEN` | `0.0.0.0:9081` |
| `PNEUMA_RESTATE_INGRESS` | required |
| `PNEUMA_RESTATE_HANDLER` | `PneumaRunner/run` |
| `PNEUMA_BATCH_SIZE` | `50` |
| `PNEUMA_PER_TENANT` | `500` |
| `PNEUMA_DISPATCH_INTERVAL_SECS` | `2` |
| `PNEUMA_RECLAIM_AFTER_SECS` | `300` |
| `PNEUMA_TENANT_WEIGHTS` | empty — every tenant weight 1 |

### `pneuma-intake`

| Variable | Default |
|---|---|
| `PNEUMA_MONGODB_URL` | `mongodb://admin:password@metadb:27017` |
| `PNEUMA_MONGODB_DATABASE` | `pneuma` |
| `PNEUMA_AMQP_URL` | `amqp://guest:guest@rmq` |
| `PNEUMA_RUNS_QUEUE` | `pneuma.input` |
| `PNEUMA_EVENTS_QUEUE` | `pneuma.event` |
| `PNEUMA_PIPELINES_QUEUE` | `pneuma.pipeline.create` |
| `PNEUMA_RECONNECT_DELAY_SECS` | — |
| `PNEUMA_ADMISSION_URL` | required |
| `PNEUMA_EVENT_ROUTING_KEY` | — |
| `PNEUMA_LISTEN` | `0.0.0.0:9082` |

### `pneuma-driver`

| Variable | Default |
|---|---|
| `DATABASE_URL` | **required — no default** |
| `PNEUMA_MONGODB_URL` | `mongodb://admin:password@metadb:27017` |
| `PNEUMA_MONGODB_DATABASE` | `pneuma` |
| `PNEUMA_NATS_URL` | `nats://nats:4222` |
| `PNEUMA_RUNS_SUBJECT` | `pneuma.run.start` |
| `PNEUMA_RESULT_SUBJECT` | `pneuma.result` |
| `PNEUMA_MAX_CONCURRENT_RUNS` | — |
| `PNEUMA_COMPONENT_TIMEOUT_SECS` | — |
| `PNEUMA_LISTEN` | `0.0.0.0:9083` |

### `pneuma-executor`

| Variable | Default |
|---|---|
| `PNEUMA_NATS_URL` | `nats://nats:4222` |
| `PNEUMA_WORK_SUBJECT` | `pneuma.step` |
| `PNEUMA_RESULT_SUBJECT` | `pneuma.result` |
| `PNEUMA_EVENT_SUBJECT` | `pneuma.event` |
| `PNEUMA_COMPONENT_ENDPOINT` | **required — no default** |
| `PNEUMA_COMPONENT_TIMEOUT_SECS` | `300` |
| `PNEUMA_COMPONENT_ATTEMPTS` | `3` |
| `PNEUMA_MAX_CONCURRENT_STEPS` | `8` |
| `PNEUMA_LISTEN` | `0.0.0.0:9085` |

`PNEUMA_COMPONENT_ENDPOINT` has no default on purpose: defaulting it would give
a manifest that looks migrated and calls nothing.

### `pneuma-broker`

| Variable | Default |
|---|---|
| `PNEUMA_NATS_URL` | `nats://nats:4222` |
| `PNEUMA_INPUT_SUBJECTS` | `pneuma.input` |
| `PNEUMA_QUEUE_GROUP` | `pneuma-broker` |
| `PNEUMA_LISTEN` | `0.0.0.0:9084` |

### `pneuma-restate`

| Variable | Default |
|---|---|
| `PNEUMA_COMPONENT_ENDPOINT` | required |
| `PNEUMA_COMPONENT_TIMEOUT_SECS` | — |
| `DATABASE_URL` | **required — no default** |
| `PNEUMA_LISTEN` | `0.0.0.0:9080` |

`pneuma-restate` and `pneuma-executor` read the **same** two variables for the
same two things, which they did not before the rename.

`DATABASE_URL` is required by both drive paths, and neither starts without it —
that is the design notes A default would give a service that runs, reports
healthy, and mirrors nothing, and the only visible symptom would be
`pneuma-janitor` finding no stale runs, weeks later, and attributed to the
janitor. A database that opens but has no `node_run` is refused for the same
reason: migrations not run, or a `search_path` that does not reach the schema
they ran in, is the likelier mistake and looks identical from outside.

### `pneuma-janitor`

| Variable | Default |
|---|---|
| `DATABASE_URL`, or `PNEUMA_POSTGRES_{HOST,USER,PASSWORD,DB}` | the four parts, assembled |
| `PNEUMA_MONGODB_URL` | `mongodb://admin:password@metadb:27017` |
| `PNEUMA_MONGODB_DATABASE` | `pneuma` |
| `PNEUMA_MONGODB_RUNS_COLLECTION` | `runs` |
| `PNEUMA_MONGODB_HISTORY_COLLECTION` | `run_history` |
| `PNEUMA_RUN_BATCH_SIZE` | `100` |
| `PNEUMA_STALE_AFTER_SECS` | `3600` |
| `PNEUMA_RETENTION_DAYS` | `14` |
| `PNEUMA_STALE_TERMINATION_ENABLED` | `false` |
| `PNEUMA_INTERVAL_MINUTES` | `5` |
| `PNEUMA_ALERT_MISS_GRACE_SECS` | `30` |
| `PNEUMA_GATEWAY_URL` | `http://pneuma-gateway:8080` |
| `PNEUMA_GATEWAY_TIMEOUT_SECS` | `10` |
| `PNEUMA_LISTEN` | `0.0.0.0:9086` |

The two collection names must differ, and that is checked at startup: the
archive is a `$merge` from the live collection into the history one, so one name
for both merges the collection into itself and the delete that follows removes
it.

`PNEUMA_INTERVAL_MINUTES` is a number of minutes, where the original's
`INTERVAL_MINUTES` was a cron minute field (`*/5`). Together with
`PNEUMA_ALERT_MISS_GRACE_SECS` it is the deadline the `passes` liveness check
uses: past it, `/pneuma-janitor/liveness` answers 503.

`TIMEZONE` is deliberately not read — see
The design notes

The binary takes one flag, `--dry-run`: it selects and reports and changes
nothing.

The four-part Postgres form is percent-encoded rather than interpolated. A
password containing `@`, `/`, `:` or `?` — which a secret manager produces
routinely — otherwise turns `user:p@ss@host` into a URL whose host is `ss@host`,
and the failure is a connection error naming a host nobody configured.

### `pneuma-migrate`

| Variable | Default |
|---|---|
| `DATABASE_URL` | required |
| `PNEUMA_MONGODB_URL` | — |
| `PNEUMA_MONGODB_DATABASE` | `pneuma` |
| `PNEUMA_MONGODB_RUNS_COLLECTION` | `runs` |

## Delivery guarantees

```mermaid
stateDiagram-v2
    [*] --> Received
    Received --> Handled: the handler answered
    Received --> Failed: the handler refused

    Handled --> Acked: ack
    Failed --> DeadLettered: unrecoverable
    Failed --> Requeued: transient
    Requeued --> Received: redelivered
    DeadLettered --> [*]
    Acked --> [*]

    note right of Failed
        Three answers, not two.
        The original had ack-or-drop,
        which loses work on a blip.
    end note
```

**A publish into a local buffer is not a publish.** Measured on both transports:

- **AMQP** needs `confirm_select` and `mandatory`, and only
  `Confirmation::Ack(None)` counts. Without confirms, `basic_publish` returns
  `Ok` for a message the broker never saw.
- **NATS** `publish` returns `Ok` even on a *drained* client. `flush` is the
  strongest guarantee core NATS offers, and it must be bounded — an unbounded
  flush against a wedged server is a hang, not a guarantee.

`x-delivery-limit` is always set explicitly on a quorum queue: absent means
unlimited on RabbitMQ 3.x and 20 on 4.0, so a manifest that says nothing means
different things on two versions of the same broker. A dead-letter queue takes
`-1`.
