# Changelog

All notable changes to this project are documented in this file.

## [Unreleased]

### Added

- `pneuma-core`, the pure I/O-free domain kernel: `ids`, `status`,
  `child_idx`, `condition`, `evaluator`, `slug`, `child_ref`, `start_set`,
  `node`, `step`, `resolver`.
- A differential test against the original's own resolver output over
  all 11 corpus fixtures.
- CI gating `pneuma-core` at 100% line coverage, plus a check that the crate
  pulls in no async runtime.
- `pneuma-proto`, the wire messages exchanged between components. Separate from
  `pneuma-core` on purpose: a domain type is free to be renamed for clarity, a
  wire type is not, because a peer that was not rebuilt is still sending the old
  field name. Modules: `backend`, `dispatch`, `envelope`, `event`, `headers`, `meta`,
  `payload`, `runinfo`, `status_wire`, `timestamp`.
- `pneuma-nats`. Subject construction, subscription patterns, and stream
  topology. Implements
  The defect notes at the type: a tenant id that would break the subject
  grammar cannot reach a subject at all, rather than being unchecked at each of
  the three sites that interpolate it today.
- `pneuma-fairness`, the work-conserving weighted fair dispatch that nothing in
  the landscape provides natively — the Restate spike found no priority, weight
  or quota primitive at all. Independent of any engine by construction. Modules: `FlowKey`, whose encoding must be injective because a collision
  means two tenants silently sharing one quota, and `select_batch`, the fair
  round selection.
- `pneuma-telemetry`, starting with the dependency health checks behind the
  `liveness` endpoint. Deliberately HTTP-free so the aggregation is testable
  without binding a port; the wire shape is `pneuma-gateway`'s exactly,
  including the `error: ` prefix on a failed check.
- `pneuma-config`, typed environment-variable helpers — a pattern matching
  `pneuma-gateway`'s hand-rolled `Config::from_env()`, not a deserialization
  framework. Two deliberate departures, recorded in the design notes: a
  non-UTF-8 value is an error rather than a silent default, and credentials are
  `SecretString` so a `Debug` of a config cannot print a DSN.
- The `BackendMessage` callbacks in `pneuma-proto` — the only messages that
  leave the system. `BackendCallback` encodes the routing table from
  `_send_to_backend` in the type, and its variant order is load-bearing and
  pinned by a test.
- `MessageEvent` and `MessageRetry` in `pneuma-proto`. `MessageRetry::has_reached`
  is the comparison the original service never makes — its `retry_count` is
  incremented and read by nothing, so the retry loop is unbounded
  (the defect notes).
- `IsoTimestamp` in `pneuma-proto`, formatting an instant exactly as the original's
  `datetime.isoformat()` does — `+00:00` rather than `Z`, and six fractional
  digits or none. Neither obvious chrono format matches; both are pinned as
  failing by mutation tests against golden values from the interpreter.
- `MessageRun` and `MessageResult` in `pneuma-proto`, the dispatch pair. Both
  subclass `Message` in original and so inherit its catch-all; both carry the
  reserved-key guard that keeps a caller extra from duplicating a named wire key.
- `Message` and `MessageInit` in `pneuma-proto`, the ingress envelopes.
  `MessageInit` keeps the `headers` key original discards, so the trace no longer
  breaks at the boot-to-controller hop.
- `StepPayload` in `pneuma-proto`, enforcing that a step payload is an object or
  an array as original does, rather than the `interface{}` the original services accept.
  Recorded a live defect this exposes: a component may return a scalar
  `step_output` that is valid by the component contract, is forwarded by
  the original executor, and is then rejected by the controller.
- `TraceHeaders` in `pneuma-proto`, declaring the trace-context map that rides
  on nearly every message while being declared on no original model — it is
  injected into the dumped dict just before serialization, so it never appears
  in a model definition. Tolerates an explicit `null`, which the obvious
  modelling rejects — defensively, since no producer is known to emit one.
- `NodeStatusWire` in `pneuma-proto`, accepting the `"timeout"` spelling
  the original executor emits for the state the original calls `"timed_out"`.
  A wrapper rather than a change to `NodeStatus`, because original applies that
  coercion to exactly two fields and widening the domain type would hide which
  boundaries are lenient.
- `SiblingIndex` in `pneuma-core`, separating a node's static position among its
  parent's declared components (`nth`) from the runtime fan-out index
  (`child_idx`). The two were one type; the split immediately caught two call
  sites.
- `NodeKind` gains the Postgres `nodetype` mapping `pneuma-store` needs, pinned
  the same way `NodeStatus`'s is — against the labels the original migration tool actually created,
  without a live database. Worth noting that the two enums in the same table use
  *different* conventions: `nodetype` is `PascalCase` and `nodestatus` is
  `SCREAMING_SNAKE_CASE`, so neither spelling can be derived from the other.
- `NodeKind` in `pneuma-core` — the node discriminant on its own, mirroring the
  original's `NodeType`, with a test pinning that its wire names cannot
  drift from `Node`'s serde tags.
- `SubjectPattern::overlaps` and self-overlap detection in
  `StreamTopology::validate`. JetStream refuses a stream whose own subject list
  overlaps itself — verified against a live `nats:2`, which rejects both
  `a.b,a.b` and `x.*,x.b` with error `10052` — while `diff` reports such a
  topology as equal to itself. Overlap is not containment: `a.*` and `*.b`
  match neither each other nor anything textually, yet both match `a.b`. The
  algorithm is checked against a brute-force oracle over every pattern pair in
  a closed universe, not against a restatement of itself. The separate
  cross-stream rejection (`10065`) is documented as out of scope, since one
  topology cannot see it.
- `StreamTopology::validate` additionally rejects a subject that reaches into
  JetStream's `$JS.>` namespace, and the capture-all subject `>`. JetStream
  reports these, self-overlap, *and* nothing else under the single code
  `10052`, distinguished only by message text — which is exactly what made them
  easy to miss. The boundary is genuine overlap against `$JS.>` rather than a
  prefix test: `$JS` alone is accepted (one token cannot reach a pattern
  needing two) and `$JSX.a` is accepted, while `*.a` is not. Validation also
  reproduces the server's *order-dependent* choice of which simultaneous
  violation to name, so the message an operator gets here is the one they would
  get from `AddStream`.
- `scripts/forbid-deps.sh`, one implementation of the "this crate is
  deliberately I/O-free" CI guard for the two crates that assert it.
- `pneuma-amqp`, the AMQP counterpart to `pneuma-nats` — queue names and
  routing keys, client-free. Every rule was measured against a live
  `rabbitmq:3-management` rather than taken from the spec, which mattered: AMQP
  0-9-1 restricts queue names to letters, digits, hyphen, underscore, period and
  colon, and RabbitMQ enforces none of it. `with/slash`, `with space`,
  `with#hash`, `with*star` and `wíth-ünicode` are all accepted, so encoding the
  spec would have rejected names that work in production. What *is* enforced is
  a 255-**byte** ceiling, and it comes from the wire format rather than the
  broker: a `shortstr`'s length prefix is one octet, so 256 is unrepresentable.
  Confirmed in `pamqp` — the encoder beneath `aio_pika`, and so beneath
  the original — with no connection open. Bytes, not characters: 128 two-byte
  characters is 256 bytes and is refused.
  The one queue-name content rule RabbitMQ *does* enforce is the reserved
  `amq.` prefix (`403 ACCESS_REFUSED`), measured down to its boundary — `amq`,
  `amqz.ok`, `xamq.foo` and `AMQ.upper` are all fine — and that one is enforced
  here, on queue names only.
  `QueueName::dead_letter()` returns a `Result` because the `.dead_letter`
  derivation is where a valid name becomes an invalid one; that is
  The defect notes
- `pneuma-gateway-client`, the gateway's HTTP contract as paths and wire types
  with no HTTP client. It exists because of the defect notes: the
  original client interpolates a caller-supplied `job_id` into a URL path with an
  f-string, and because the gateway deletes by LIKE prefix rather than equality,
  an id that gets *truncated* on the way into a URL deletes more than it was
  asked to. Here a path segment cannot be assembled except through
  `percent_encode_segment`, whose output is pinned against
  `urllib.parse.quote(value, safe="")` — the same fix §13 prescribes for the
  original side, so the two clients cannot address different rows. Encoding alone
  turns out to be insufficient, and the original fix has the same hole: `.` and
  `..` are RFC 3986 unreserved, so both encoders leave them untouched and the
  HTTP client then strips them as dot-segments. `JobId` refuses those two values
  for that reason, and refuses nothing else that encoding can carry.
- The Seldon component contract in `pneuma-proto` — `SeldonRequest` and
  `extract_step_output`, the entire integration surface for a zero-SDK
  component. Modelled from what the original executor **parses**, not what it
  declares: its `MessageSeldonOutputV2` / `JsonDataOutputV2` are referenced only
  by each other, and the real read is untyped map access requiring a `jsonData`
  object and a `step_output` key of any JSON type. A present-but-`null`
  `step_output` is accepted, matching the original's presence-only check. The two failure
  modes are kept distinct because the original executor treats them differently — a missing
  `jsonData` is dead-lettered, a missing `step_output` is not, which is one of
  the paths in the defect notes that loses work outright. The request's
  `meta` is a separate five-key `SeldonMeta` rather than the controller's
  `Meta`: the original's `MetaV2` has no catch-all and no `pipeline_id` field, so a
  component sees exactly five keys and reusing `Meta` would have widened the one
  surface the module exists to pin.
- `pneuma-store`, beginning with the Postgres half — the `noderun` schema, the
  row/domain split, and the nine queries the original's the original
  performs, plus one the original reaches through its generic repository, and
  the `noderun_history` table with its backup and retention queries. Two of
  those carries a fix rather than a transcription: the backup is idempotent
  (the defect notes, where a repeated backup is a primary-key violation
  that wedges the janitor permanently), asserted against a real server and
  checked by removing the `ON CONFLICT` and watching the test fail. The
  retention cutoff is bound as an aware instant, which is the honest type but
  not a fix: §16 claimed a naive one would drift by the session's UTC offset and
  is retracted.
  `NodeRunStore` is the async layer over those twelve statements — thin on
  purpose, since the decisions live in the SQL. Every method is driven against a
  real Postgres, including the guard refusing a transition (`Ok(None)`, not an
  error), the `ON CONFLICT` fallback returning the existing row on a redelivery,
  and the empty-input short circuits that answer without a query.
  `RunHistoryStore` completes the Mongo side: the `runs` → `runhistory` copy and
  the two deletes after it. The copy is a server-side `$merge` rather than the
  original's read-modify-write, which is the same choice already made for
  Postgres and removes rather than reimplements a category of bookkeeping —
  the original reconstructs which documents of a partially-failed `bulk_write`
  landed, where an idempotent copy just runs again. Retention measures
  `archived_at` where it exists and the `ObjectId`'s own timestamp where it
  does not (the defect notes, the design notes). The original's
  cutoff is age-since-*created*, so a run alive longer than the window is
  archived and expired on the same janitor pass and keeps no history at all;
  the second clause is what keeps every document the original janitor already
  wrote — verbatim copies with no stamp — from becoming permanently
  unexpirable at cutover. The merge is a pipeline rather than one of `$merge`'s
  named modes, because refreshing the archived payload and holding the
  retention clock still are both wanted and no named mode does both.
  Every property the design leans on was measured against `mongo:5.0.28`
  first, and several came back other than expected: `$merge` yields no
  documents (so unlike the Postgres half there is no affected-count),
  resolves a bare `into` against the *aggregation's* database, and accepts a
  self-merge — which writes, rather than doing nothing, so a store built from
  one collection twice would archive nowhere while reporting success.
  The design notes gates the *shape of the resolved graph* in the Mongo
  `state` blob, which is `pneuma-intake`'s single write and not this
  crate's — `update_step_status` moves one step's status within `state` and
  never writes the graph — which is why this could start. `migrations/0001_noderun.sql` is the original migration tool's
  existing table transcribed, not a new design, so `pneuma-migrate` can baseline
  a live database against it. Every query is checked by asking a real Postgres
  to `PREPARE` it, which resolves each column against the real schema and infers
  each parameter type without the repository layer existing yet; the same test
  asserts both enums carry the original migration tool's exact labels and that `parent_slug` really
  is a self-referential foreign key.
- `pneuma-interpreter`, the scheduling decisions: which step runs next and with
  what input, with no I/O and no engine. A driver asks what is ready, runs it,
  and reports back — so the rules are testable without a component, a queue or a
  workflow runtime, and survive a change of engine, which this port has already
  reconsidered once. Prerequisites are tracked as a **set**, not a tally: the
  spike used a counter, which is safe only because a durable journal never
  replays a completed step, and that makes the rule a property of the engine
  rather than of the scheduler.
  Aggregator fan-out is a *nested execution*, not a scheduling edge, and
  `execution` owns it: a **tree** of schedulers, each branch bounded to the
  aggregator's `component_ids`. A tree rather than recursion because branches
  must be able to run at the same time — the original runs each as an
  independent message, and a recursive driver, where a branch is one stack
  frame, cannot. Aggregators and conditionals are never handed to the driver at
  all; the original resolves both itself, and dispatching one would ask an AI
  component to evaluate `equals`. Every corpus pipeline runs to completion,
  pinned by exact counts — including the two that reach fewer nodes than they
  have, because a conditional's untaken branch never runs, and the one that
  executes 33 steps across 24 nodes, because a fan-out runs its children once
  per branch.
  Three states are kept distinct that the original conflates into "the run is
  still going": finished, working, and wedged. A step that was begun and never
  finished — abandoned, or a completion that failed and was not retried — is
  reported by `stalled()` rather than leaving the run quiet, and a branch in
  that state is **not** aggregated: `branch_outputs` would filter the missing
  output away and hand back a silently short array. A zero-width fan-out
  completes immediately instead of wedging (the defect notes), while a
  fan-out with no way in — every declared start blocked — is reported rather
  than given a fabricated `[]`, because those two empties mean opposite things.
  A join's input is the **merge** of every prerequisite's output, not whichever
  one released it, and the merge order is deterministic — which the original's
  is not, because it merges with a `ChainMap` over rows fetched with no
  `ORDER BY` (the defect notes).
- `pneuma-janitor`, the original plan — which the Restate verdict left
  standing, having removed phase 8 alone. The queries were already
  `pneuma-store`'s; this owns the thing that decides which follows which.
  `cleanup_runs` copies both records before deleting either, so nothing is
  deleted that was not copied first and any step failing abandons the pass for
  the next one to repeat. That retry is only safe because both copies are
  idempotent, which is exactly what the defect notes says is untrue in
  the original — so the test seeds what an interrupted pass leaves behind and
  requires a second pass to get past the copy, rather than asserting §17 is
  fixed. Zero rows newly archived is the expected answer there, and the field's
  doc says so, because it is the kind of suspicious zero a later reader turns
  into an error.
  Errors propagate instead of being logged and swallowed, and `JanitorError`
  keeps the two stores apart because which one failed says where the pass
  stopped. Stale-run handling splits at the I/O boundary: selection here,
  termination over HTTP in the binary, which is also why `pneuma-gateway-client`
  ships an endpoint and wire types with no client.
  `preview_cleanup` and `preview_expiry` are the `--dry-run` phase 7 asks for,
  so the janitor can run a week against production and be diffed against the
  original one. They report from the same predicates the real passes use — shared
  outright on the Mongo side, and held to it on the Postgres side by a test that
  compares count against delete at the exact boundary timestamp, since cutoffs
  away from the data agree under either `<` or `<=` and cannot see that
  spelling drift.
- The protocol notes, recording the places where the four live services
  disagree about the same message — including a fan-out index that is called
  `child_id` in original and `child_idx` in the original and therefore survives a round trip
  in neither direction, and a `headers` field that is present on nearly every
  message while being declared on no original model.
- `pneuma-migrate` gains a command line -- `baseline [--dry-run]`,
  `fingerprint`, `mongo index|duplicates` -- which closes the original plan:304`,
  a gate that could not previously be attempted because `baseline` was
  reachable only from a `#[tokio::test]`. It is also the first production
  caller `pneuma_store::migrator()` has ever had. Exit codes are contract and
  live in a pure `exit_code()`: **3** for a schema mismatch, so a deploy
  pipeline can tell "this database is not what the migrations say", which needs
  a human, from "could not connect", which needs a retry; and **4** for
  duplicate `run_id` values, so the precondition check for making that index
  unique is gateable without parsing prose.
- `--dry-run` and the real thing share one decision function. They were written
  as two, and the duplicate immediately drifted -- the preview omitted a
  qualifier and reported a mismatch against a database the real path baselines
  cleanly. Sharing the comparison is what makes "would record" mean it.

- The port ran a real pipeline end to end for the first time: the
  `pneuma-restate` release binary, configured from the environment, registered
  as a deployment with `restatedev/restate:1.7.8`, driving
  the original -- an unmodified corpus fixture with a
  nested `DictAggregator` -- against a component in another process and another
  language that knows nothing about Restate, over a Postgres schema baselined
  by the `pneuma-migrate` binary. `A -> B -> X{C, D}` ran in that order, the
  aggregator combined both children, and each component saw its predecessor's
  output. Until this the only thing that had ever driven a pipeline to
  completion was a `#[tokio::test]`.
- That run also settled the idempotency fork the plan refused to assume, and
  the whole measured table is the design notes A redelivered submission
  under the same key returns the identical report in under 6 ms and pays for
  **zero** component calls; a fresh client can attach for a run's output
  without the original request, blocking while it is in flight; and `/send`
  distinguishes `Accepted` from `PreviouslyAccepted`. So the unkeyed service
  stands and the `#[workflow]` conversion is not needed. It also found a
  hazard: Restate keys on the idempotency key alone and ignores the request
  body, so a *different* pipeline submitted under a used key silently returns
  the first one's answer.
- **`node_run` has a producer.** The table had a schema, four migrations, a
  rename, a complete store API and a janitor that archived and deleted it, and
  nothing wrote a row — `NodeRunStore`'s every write method had zero production
  callers, so `pneuma-janitor`'s stale-run detection could never fire. Both
  drive paths now mirror what a run did: `pneuma-restate` through the journal,
  `pneuma-driver` directly.
  - `pneuma_interpreter::Happening` — the interpreter says what it did, not only
    what to run next. Aggregators and conditionals never become tasks, so a
    driver watching the dispatch loop cannot infer their rows; and the order is
    load-bearing, because `node_run.parent_path` is a non-deferrable foreign key
    onto `path`. A `Frame` now also carries the `child_index` it used to
    discard, without which two branches of a fan-out would derive one path and
    the second insert would be swallowed by `ON CONFLICT (path) DO NOTHING` —
    silently.
  - `pneuma_runner::Recorder` — a port beside `Component`, with the same
    non-`Send` shape and no error type. An audit mirror must not be able to fail
    the run it audits: losing a completed model call, paid for, because an audit
    table was down is the outcome that signature prevents.
  - `pneuma-mirror` — the one implementation, a three-arm match onto statements
    that already existed. A crate rather than a module because `pneuma-runner`
    may not link a database and `pneuma-store` has no business knowing what a
    run is, and two copies of it in two binaries would drift.
  - Every write under Restate goes inside its own `ctx.run(..).name(..)`. A
    write outside the journal re-executes on every replay, which would turn the
    mirror into a record of how many times the handler restarted.
  - `DATABASE_URL` is now required by `pneuma-restate` and `pneuma-driver`, and
    a database that opens but has no `node_run` is refused too. `pneuma-driver`
    also gains a `PostgresHealth` probe beside its Mongo one, so a database that
    goes away *after* startup shows on `liveness` rather than only in the logs;
    `pneuma-restate` has no health surface of its own to add it to — the SDK
    endpoint answers discovery and nothing else. The design notes have the three deliberate divergences from the original: the branch index
    moves onto the branch's shared prefix, `FORKED` is written once per
    aggregator instead of once per child start, and a failure is `ERROR` and
    never `TIMED_OUT`.

### Changed

- **BREAKING — two crates renamed.** `pneuma-tenant-broker` is now
  `pneuma-broker`, and the library that held that name (the broker traits
  `pneuma-nats` and `pneuma-amqp` may not hold) is now `pneuma-transport`.
  Runtime-visible: the service's health routes move to `/pneuma-broker/`, and
  the default NATS queue group is `pneuma-broker`. A deployment that pinned the
  old queue group must set it explicitly or its replicas will not share a group
  with the new ones during a rolling deploy. The sibling repository
  the original broker is unaffected and keeps its name.

- **BREAKING — the wire.** Every inherited message key whose name was wrong or
  opaque now carries pneuma's own: `runinfo` → `node`, `slug` → `path`,
  `parent_slug` → `parent_path`, `type` → `kind` (in the node) and
  `pipeline_type` (in the meta), `level` → `pipeline_level`, `name` →
  `pipeline_name`, `parent_type` → `parent_kind`, `nth` → `sibling_index`,
  `child_idx`/`child_id` → `child_index`, `topic_output`/`topic_error`/
  `topic_event` → `reply_to_result`/`reply_to_error`/`reply_to_event`, and a
  retry's `topic`/`content` → `subject`/`message`. The filter was *rename a key
  whose name is wrong or opaque, not merely inherited* — which is why `meta`,
  `job_id`, `run_id`, `step_input`, `step_output` and the rest are untouched,
  and why the **pipeline definition**'s `type` discriminant is untouched too: a
  definition is authored outside this system and stored as written. The full
  table, and the last byte-compatible commit, are in the design notes
- **BREAKING — the component protocol.** The `jsonData` wrapper is gone from
  both the request and the response. A component is now sent
  `{meta, step_input, custom_data, node_env_vars, headers}` and answers
  `{"step_output": …}`. The wrapper was Seldon v1's, inherited along with a
  module named `seldon.rs`; it carried no information, since a request has
  exactly one body. Dropped rather than renamed, and with no dual-read: a shim
  for a protocol being deliberately replaced is a shim nobody removes. Every
  deployed model must be rebuilt against `docs/as-built/component-api.md`.
- **BREAKING — the storage names.** `noderun` → `node_run`, `noderun_history` →
  `node_run_history`, the enums `nodetype`/`nodestatus` → `node_kind`/
  `node_status`, and the columns `slug`/`parent_slug`/`type`/`parent_type`/
  `child_idx`/`nth` → `path`/`parent_path`/`kind`/`parent_kind`/`child_index`/
  `sibling_index`, with every index and constraint renamed explicitly because
  Postgres does not rename them with their table. Carried by
  `crates/pneuma-store/migrations/0004_rename.sql`, which uses `ALTER … RENAME`
  throughout: a catalogue update that rewrites no rows.
- **BREAKING — the Mongo names.** The default database is `pneuma`, and the `runs.run_id` index is
  `ix_runs_run_id`, renamed from the original's. `ensure_run_id_index` performs that rename itself — dropping
  the old index before building the new one — because Mongo has no `ALTER INDEX`
  and refuses a second index over the same key under a different name, so
  creating alongside would make `pneuma-migrate mongo unique` an error on
  exactly the databases it is meant to be run against. It is **not** called
  `run_id_unique`: this index is deliberately not unique (the defect notes), and a name asserting otherwise would have an operator drop the old index
  believing they had kept a guarantee they never had. The `runhistory` →
  `run_history` collection rename is a `renameCollection` in
  `docs/runbook.md`, not something any code does.
- `ORIGINAL_THROUGH` stays at **2**, and now matters more: an original-era
  database has the old names, so it can only match an expectation built from
  `0001` and `0002` alone. `pneuma-migrate baseline` still adopts one, and
  `migrate run` then carries it through `0003` and `0004` to the schema this
  port queries — asserted end to end against a real Postgres.
- Every test fixture that stood up a schema by `include_str!`-ing two migration
  files now runs the **migrator**. That list was hand-maintained and had already
  drifted once; a rename is exactly the change it cannot survive.
- `pneuma-core`'s `ChildIdx` is `ChildIndex`, and its module with it, so the
  pair with `SiblingIndex` reads the same way. The `idx` abbreviation was the
  last of the originals' spellings left in the domain kernel.
- `pneuma-janitor` reads its environment through `pub const`s like every other
  crate, rather than inline string literals in two modules.

- **The originals' spellings come off the wire.** Every NATS subject and every
  environment variable that carried a name from the systems being replaced now
  carries pneuma's own. `pneuma.termination` already existed as a migrated
  subject, so `pneuma.` was the established prefix rather than a new invention.
  - Subjects: every subject now carries the `pneuma.` prefix — `pneuma.input`,
    `pneuma.event`, `pneuma.pipeline.create`, `pneuma.run.start`,
    `pneuma.result`, `pneuma.step` and `pneuma.dlq`; the broker's queue group is
    `pneuma-broker`.
  - Every environment variable gains the `PNEUMA_` prefix, **except
    `DATABASE_URL`**, which is a de-facto convention (sqlx, Rails, Django)
    rather than an inheritance from this system.
  - The rename also made two services agree. `pneuma-restate` already read
    `PNEUMA_COMPONENT_ENDPOINT` and `PNEUMA_COMPONENT_TIMEOUT_SECS`;
    `pneuma-executor` read `MESSAGE_ENDPOINT` and `REQUEST_TIMEOUT` for exactly
    the same two things. The two services that call components now read the same
    variables -- a consistency the port did not have.
  - `pneuma-janitor` kept its environment names as inline string literals rather
    than constants, alone among the seven services. Normalised while renaming.
  - The constant *identifiers* moved with their values: a
    `pub const RABBITMQ_URI: &str = "PNEUMA_AMQP_URL"` would have been worse than
    either name alone.
  - **What this costs, deliberately.** These services can no longer run beside
    the originals one process at a time, which was the original plan's stated
    reason for building `pneuma-broker` and the executor early. The port
    becomes a cutover. Recorded in the design notes.
  - Unchanged: the `tenant_` subject prefix and the `.dead_letter` suffix. The
    latter's twelve bytes are load-bearing -- 244 + 12 = 256 overflows the AMQP
    `shortstr`, which is the entire reason `QueueName::dead_letter` returns a
    `Result`.

- `pneuma-executor`, the seventh of the plan's eight binaries: NATS in, a model
  call out, an event and a result back.
  - **The retry decision comes off typed predicates, not a printed message.**
    the original decides whether a
    transport failure is worth retrying by matching the original's error *text* for
    `dial tcp`, `connection refused`, `EOF` and `connection reset by peer` --
    with the strings copied from a terminal into the comment above, which is
    the honest admission that nothing pins them. An original upgrade that rewords one
    turns a retryable failure into a permanent one, silently. Here it is
    `reqwest::Error`'s own `is_timeout`, `is_connect` and `is_body`.
  - A body that stops mid-read is retried, not failed. That is the same event
    as the original's `EOF` and `connection reset by peer`: the component took the
    request and died, and another worker may well answer.
  - **An unexpected status is a named failure.** The original's `default` branch
 logs and returns `nil` *without setting `result`*, so
    the zero `SendResult` survives -- `Success` false, `Response` nil -- and the
    handler's `GetStringFromMap(nil, "error_message")` fails. The message is
    dead-lettered complaining about a missing map key rather than about the
    status nobody expected.
  - A timeout is its own verdict, not a generic failure: the original does not
    retry it either, and making a run wait for a slow model twice is worse than
    telling it what happened.
  - The step is announced *before* the component is called, unconditionally, so
    a step that never finishes still shows as having started. A broker that will
    not take that announcement stops the message there -- calling the component
    anyway would spend a model call nobody can be told the result of.
  - Tested **as a pair with `pneuma-broker`** against real NATS, which is
    what the plan asked for: each service alone is only proved against a fake,
    and the thing worth knowing is that the subject one writes is the subject
    the other reads. A message published to the broker's input is routed to the
    tenant subject, consumed by the executor, sent to a stub component, and its
    result published.

- `pneuma-broker`, which **closes the original plan's Phase 3 gate**. The
  sixth of the plan's eight binaries, and one of the two the plan calls
  independently deployable against the live the originals: the variable names are
  the original's, so the same manifest runs it.
  - **A tenant id that would break the subject grammar cannot reach a subject.**
    The defect notes: the id is interpolated at three sites with no
    validation, so a `.` invents a token and a `*` or `>` makes the message go
    to *everybody* subscribed below that point — with nothing rejecting it at
    any boundary, so the misrouting is silent. Here the id must become a
    `SubjectToken` first, and `Subject::tenant_scoped` takes nothing else.
  - The queue group is validated too, which the original does not do. NATS
    applies the subject-token grammar to it, and the refusal arrives at the
    *subscribe* — after the process has reported ready, with a consumer that
    never attaches and a pod that looks fine.
  - Both halves of the original's `IsValid` are told apart. It checks the job id
    and the tenant id together and answers with a bare `false`, so its log says
    "Invalid message" and a person has to guess which half.
  - The message is republished **byte for byte**. A broker that re-encoded could
    silently drop a field it does not model, and everything downstream reads
    fields this service has no opinion about.
  - Subscribing happens *before* anything reports ready. That is both better
    behaviour and the removal of an arm nothing could reach: attempted the other
    way round, a failing `queue_subscribe` is not something a test can arrange —
    `Subject::parse` refuses every subject NATS would, `Config::from_env`
    refuses every queue group NATS would, and a *drained* async-nats client
    subscribes successfully anyway. Measured.
  - The publish is bounded, the same way `pneuma-driver`'s is and for the
    same measured reason. It was not, at first, and the test hung — `flush`
    against a broker that is not answering waits for ever, which would hang the
    whole demux loop on one message. The duplication with the controller is
    deliberate: the obvious home is `pneuma-transport`, but that crate holds
    `lapin`, and moving it there would make every NATS-only service link an
    AMQP client. A third call site is the point to split that crate by
    transport.

- `pneuma-driver` becomes a process -- the fifth of the plan's eight
  binaries, and the deployment for where Restate cannot go.
  - **`boot::run` is not `Send`, and that is the design showing through.**
    `Component::call` deliberately promises no `Send`, because `restate_sdk`'s
    `ContextSideEffects::run` makes its caller non-`Send` and a bound there
    would have excluded the one transport this port adopted. The cost lands
    here: `drive` cannot be `tokio::spawn`ed, so runs are `spawn_local`ed onto a
    `LocalSet` and the whole service future is `!Send`. The plan warned that
    discovering this at wiring time costs a rewrite; the test file did not
    compile the usual way round, and now awaits the service in its own task and
    spawns the *assertions*.
  - It consumes `MessageInit` on the subject the original's bootstrap publishes
    to, so it is deployable against the existing bootstrap rather than only
    against this port's. That is what "broker-portability driver" has to mean.
  - The pipeline is read back out of the **run document**, not the `pipelines`
    collection. A definition edited between a run being created and being driven
    would otherwise change what the run does halfway through.
  - A pipeline that will not resolve is a recorded error rather than a crash,
    and it is reachable even though `pneuma-intake` resolves before storing:
    a run created by the *original's* bootstrap went through no such check.
  - A semaphore bounds runs in flight, because a replica holds one `Execution`
    per run in memory. One thread is not the bottleneck it looks like: a run in
    flight is a run waiting on a component, since `drive` awaits each call
    before asking for the next task.
  - A failed subscribe cancels the token and takes the process down. A
    controller that could not subscribe would sit there healthy and either
    accept nothing or accept runs no result can ever reach — both worse than
    exiting to be restarted.
  - `wire_status` is hand-written and public for the reason `wire_kind` is:
    `serde_json::to_value` is infallible in fact and fallible in type, so using
    it puts an arm in the *writing* path that no input can reach. A test holds
    the spelling to serde's.

- The controller's NATS transport: publish to a component's own subject, wait
  for its result on the shared one.
  - **A publish that only reached a local buffer is not a publish.** Measured,
    after two wrong guesses: `async_nats::Client::publish` writes into a local
    buffer and returns `Ok` even on a *drained* client, and an empty subject
    behaves the same. Core NATS has no per-message acknowledgement, so `flush`
    is the strongest guarantee there is -- a `PING` the server answers only
    after everything written before it. Without it a broker that has gone away
    yields success and the run then waits its whole timeout for an answer
    nobody was ever asked for. The same lesson as the AMQP side's publisher
    confirms, reached independently.
  - **The publish is bounded separately from the call.** A component thinking
    for five minutes is normal; a broker taking ten seconds to admit a publish
    is a broker that is not there. The deadline used is the smaller of the two,
    since a publish can never usefully take longer than the call it belongs to.
  - **A call has a deadline at all**, which the original does not. It publishes
    and returns, so a run whose component never answers stays `processing` until
    the janitor's stale sweep finds it (the defect notes). Here, because
    `drive` awaits each call before asking for the next task, an unbounded wait
    is not one lost step but a run stopped for ever, holding its execution and
    its place in the correlation table. Five minutes, matching the original's
    own model timeout and `pneuma-restate`'s default.
  - The call registers *before* it publishes. The result can arrive while the
    publish is still returning, and a table not yet expecting it would drop it
    as another replica's -- then wait the whole timeout for an answer that had
    already come and gone.
  - A failed publish undoes its registration, or the entry sits in the table
    until the process ends and the next attempt for that node looks like a
    duplicate.
  - "Abandoned" is distinct from "timed out": the first means the table gave up
    on the call -- a cancelled run, a shutdown -- and nothing was waited *for*.
    A run told "timeout" for that would look like a slow component.
  - Two error variants were removed rather than left unreachable. Encoding a
    `MessageRun` cannot fail, and neither can turning a `StepPayload` into a
    `Value`; a variant no input can produce is a promise the type does not keep.
  - `async-nats` is pinned to 0.50, not the 0.42 `cargo add` first resolved to.
    The gate caught it: 0.42 depends non-optionally on `rustls-native-certs
    0.7`, which pulls the unmaintained `rustls-pemfile` (RUSTSEC-2025-0134) and
    `rustls-webpki 0.102.8` (RUSTSEC-2026-0049, faulty CRL distribution-point
    matching). Neither could be feature-gated away. Its default features are
    `ring` rather than `aws-lc-rs`, which matters for the same reason lapin's
    do: two crypto providers enabled at once make `ClientConfig::builder()`
    panic.

- The message a component is sent, and what identifies its answer. Pure, and
  built from the corpus fixture through the real resolver.
  - `pneuma_runner::Dispatch` deliberately carries only what a *component* is
    entitled to see -- a node id, a component name, the narrowed Seldon body.
    The original's `MessageRun` carries a whole `NodeRunInfo`, because that is
    what its result listener correlates on, so the extra is rebuilt from the
    registry the driver already holds rather than widening `Dispatch`.
  - The slug is `f"{run_id}.{step.full_path}"`, as the original builds it. The
    fan-out suffix it appends for a child index is deliberately absent: this
    controller's fan-out lives inside `pneuma_interpreter::Execution` as frames,
    and a frame is not a separate node run -- so there is no child index, and
    inventing one would put a coordinate on the wire nothing else agrees with.
    `child_index`, `sibling_index` and `parent_index` stay `None` for the same
    reason, and `None` is not the same as position one.
  - A nested node's `parent_slug` is built by the same rule its parent was
    dispatched under, which is the one place a mistake would be invisible: a
    slug built two ways correlates against nothing.
  - The step output is re-wrapped into `jsonData.step_output` before it goes
    back to the driver. `Component::call` returns a component's raw response and
    `extract_step_output` unwraps the Seldon shape; over NATS the message
    provider has already unwrapped it, so the transport puts it back rather than
    the driver learning a second shape.
  - `wire_kind` and `kind_of` are public and table-tested over `NodeKind::ALL`
    rather than checked through whichever pipeline a fixture happens to contain.
    Only an aggregator can be a parent, so the `Model` and `Condition` arms are
    unreachable through their caller -- a spelling only ever checked through its
    callers is a spelling only half checked.

- `pneuma-driver`, starting with the correlation table -- and with
  `scripts/forbid-symbols.sh`, the guard that keeps it from becoming a second
  engine.
  - **The guard first, because the temptation is specific.** This crate holds a
    run's `Execution` in memory, which is lost on a crash, and the obvious cure
    -- write the barrier state down so a restart can resume -- reinstates
    exactly what the port removed: refcounts, prerequisite completion,
    expected-children counts, and the `CONCURRENCY-AND-DIRECTION.md` §1.5 race
    that comes with them. That temptation arrives months from now, in a hurry,
    from somebody who has not read the crate docs; a failing build is the only
    kind of note that gets read then. The script was proved to fail when it
    should, to refuse a malformed pattern rather than silently passing, and to
    refuse a crate that does not exist -- the same discipline
    `forbid-nondeterminism.sh` got.
  - **Why a correlation table and not request-reply.** The original's
    controller does not wait: it publishes a `MessageRun` to the component's own
    subject and returns, and the result arrives later on a different subject.
    The original executor publishes that result to a *fixed configured* subject
, not to a reply inbox -- so NATS's own request-reply
    is unavailable without changing a service this port must stay
    wire-compatible with. The round trip is reassembled here instead.
  - **Every replica sees every result.** A run's execution lives in the replica
    that started it, so a queue group would deliver each result to exactly one
    replica -- usually the wrong one. The subscription is plain, and a result
    nobody here is waiting for is the ordinary case rather than an error.
  - A second call under a key that is already outstanding is **refused, not
    substituted**. Replacing the waiter drops the first one's sender, so the
    call that registered it never hears anything and waits for ever -- a run
    wedged with no error anywhere. It is reachable: the ingress is not
    acknowledged until a run completes, so a redelivery while the first attempt
    is still running starts a second execution for the same run id.
  - The node is in the key even though `drive` awaits each call before asking
    for the next task, so a run id alone would identify the one call in flight.
    A result arriving *after* its call was abandoned would otherwise be handed
    to whatever is outstanding now -- one step's output delivered as another's,
    which is a wrong answer rather than an error.
  - A `tokio::sync::Mutex` rather than `std`'s, and not because the guard is
    held across an `await`. `std`'s poisons, and the only honest handling here
    is `into_inner()` -- but that arm cannot be reached from outside the module,
    so it would be an untestable branch defending against a state that cannot
    arise. A mutex that does not poison removes the question rather than
    answering it where nobody can check.

- `pneuma-intake` becomes a process: three supervised AMQP consumers, the
  real stores, an HTTP client for admission, and health endpoints -- the fourth
  of the plan's eight binaries.
  - **A shutdown interrupts a consumer sitting on a quiet queue**, which is a
    bug this found by hanging. A consumer waits in `next()` until the broker
    sends something, so a loop that checked its cancellation token only between
    reconnections would never notice a shutdown at all and the process would
    hang until it was killed. The token races the whole attach-and-consume, and
    a cancelled wait becomes the next attach's cancellation rather than a second
    exit only a cancelled sleep can reach.
  - Each queue's loop is `pneuma_transport::decide` plus three lines. The branches
    that only a misbehaving broker reaches are tested there against an enum; the
    branches only a *real* broker reaches are tested here against one -- a queue
    deleted under a live consumer, a queue that already exists with different
    arguments, a connection closed with a settlement still owed, and a name
    whose derived dead-letter name cannot be encoded.
  - The settlement table is checked end to end as well as against a recorder: a
    body the handler calls permanently wrong really does reach the dead-letter
    queue rather than being retried.
  - Forwarding an event to a routing key nobody declared **fails** rather than
    being discarded, because `Amqp::publish` sets `mandatory`. That is louder
    than the original, and loud is the right end of the trade: an event nobody
    is listening for is a misconfiguration, and the alternative is finding out
    when somebody asks why a run never reported finishing.
  - `attach` is public so its failure paths are tested directly. Driving them
    through `supervise` means spawning the loop and cancelling it after a sleep
    -- and under coverage instrumentation the spawned task is not always
    scheduled before that sleep elapses, so the loop is cancelled having done
    nothing and the test still passes. It did, on a later run, and the gate
    caught it: three failure paths reported unreached by a suite that was
    green. The shutdown tests now run the loop in the *current* task and have a
    timer task cancel it, which removes the race rather than widening the sleep.
  - Two more tarpaulin attribution traps, both found by the gate disagreeing
    with the log: a bare `return` in a match arm, and a match arm whose whole
    body is an enum constructor. The second ran thousands of times in the test
    output while being reported unreached. Both restructured rather than
    excluded -- `?` for the first, a named function for the second.

- The three handlers behind `pneuma-intake`'s queues, decided without a
  broker: one function per queue over three small traits, so a body that will
  not parse, a pipeline that is not there and a database that is not answering
  are each a unit test rather than an outage somebody has to arrange.
  - **Three answers, not two.** The original conflates "wrong" with "not now"
    in the direction that loses work: `handle_dead_message` sets
    `should_requeue = False` for a `DatabaseOperationalError`, so a Mongo blip dead-letters
    the run and a person has to find and replay it. Here a database that did
    not answer is retried, and the queue's redelivery limit is what bounds it.
  - **Look up, resolve, write, then announce.** A submission announced before
    the run document exists is a run the rest of the system can be asked about
    and has nothing for; a run written and not announced is recoverable, because
    the message was never acknowledged. On the redelivery the insert finds the
    document already there, which is `Created::AlreadyExists` and not a failure
    -- and the run id used from then on is the one the handler already holds,
    never one read back off an insert result, which is the trap
    The defect notes names.
  - A definition is **resolved before it is stored**, which the original does
    not do -- it validates the model and keeps whatever passes. A definition
    that cannot resolve fails every run that ever names it, each time producing
    the same error further down than here, so one resolution at the door turns a
    recurring runtime failure into a single rejected message.
  - An event is validated and forwarded **as it arrived**, not re-encoded. The
    original forwards `model_dump()` of the parsed model, and a round trip
    through a type is where an unmodelled key silently stops existing.
  - The definition travels with the submission rather than as an id, because
    that is the shape admission's door takes -- it resolves the pipeline again
    before queueing, so a definition naming a step that does not exist is
    refused before it consumes a share of anybody's dispatch quota.

- `pneuma-intake`, starting with the run document -- built purely, because
  everything downstream reads what it writes: the gateway shows per-node status
  out of `state`, the janitor finds abandoned runs by `status`, and the archive
  keeps whatever is here for ever.
  - **`state` is written as a flat map, not as a registry.** The original writes
    `dict[str, Step]` -- node id to step -- because that is what
    `RunResolver.step_registry` is, and it is what the gateway indexes into.
    `pneuma_core::step::StepRegistry` serialises as
    `{"steps": {...}, "start": ...}`, which is a better shape and the wrong one:
    written as-is, every consumer looking for `state.<node_id>` finds nothing
    and a run's per-node display goes blank with no error anywhere. Flattened
    once, at the only place that writes it.
  - **The pipeline's `_id` is removed, and that is not tidiness.** The run
    document starts as the pipeline document, exactly as the original starts
    from `model_dump()`, and a document read from Mongo carries its `_id`.
    Copying it in gives every run of one pipeline the same primary key -- so the
    first insert succeeds and every later one fails with `DuplicateKey`, which
    this port reads as "a redelivery already handled it". Every run of that
    pipeline after the first would be acked, never written, never dispatched,
    and nothing would log it.
  - Everything else the definition carried survives, because a definition field
    this port does not model still has to reach the run document -- typing it
    would quietly narrow what a pipeline can say.
  - All three `topic_*` keys are written even when absent, as `Null`, matching
    the original: a consumer telling "no reply address" apart from "a run
    document that predates the field" needs them to keep being there.
  - A value BSON cannot hold is named rather than dropped. Reachable from real
    wire data: `serde_json` holds integers to `u64::MAX` and BSON's largest is
    an `i64`, so an unmodelled step field above `i64::MAX` cannot be encoded --
    and the alternatives, dropping the field or truncating the number, are both
    discovered by somebody reading a run months later.

- `pneuma-store` gains what a bootstrap needs: `PipelineStore` over the
  `pipelines` collection, `RunStore::create`, and a `conflict` module that says
  what "already there" means.
  - `Created::AlreadyExists` rather than an error, which is the whole of
    The defect notes's first step. The `run_id` index is non-unique
    today with a comment saying it "should be unique, however we have retry" --
    the premise is right and the remedy is backwards. A duplicate *is*
    reachable: the bootstrap consumer acks by leaving the `message.process()`
    block that wraps the whole handler, so a process that dies between the
    insert and the end of that block leaves the message unacked, AMQP redelivers
    it, and the handler runs again against a collection that already holds the
    document. With the index non-unique the second insert succeeds and the run's
    state is split across two documents that later reads pick between
    arbitrarily, with nothing logged. Tolerating the duplicate is what turns a
    clean conflict into a silent fork; refusing it is safe only once the caller
    treats the conflict as success.
  - The original has the same distinction and cannot use it: the original
    translates `DuplicateKeyError` into `ConflictError`, and searching the
    package finds **no `except ConflictError` anywhere** -- a type that exists
    to be caught and nothing catches.
  - `is_duplicate_key` says no to everything that is not a write conflict,
    including a write-concern failure. Reporting "already there" for a database
    that never answered would turn an outage into runs silently skipped: acked,
    never inserted, never dispatched, with nothing recording it. Both halves are
    tested against a real MongoDB, the conflict one behind the unique index §25
    argues for.
  - Both stores deal in `Document` rather than typed models. A pipeline
    definition is the input to `resolve`, and the original builds a run by
    `model_dump()`ing the pipeline and adding to it -- so anything a definition
    carries that this port does not model still has to reach the run document.
    Typing the collection would drop those fields silently, which is a wire
    regression dressed as a tidy-up.

- The AMQP half of `pneuma-transport`: connect, declare, consume, settle, publish.
  Thin on purpose -- every decision it could make has been made somewhere
  testable without a broker, so what is left is a sequence of awaits.
  - **`x-delivery-limit` is set explicitly**, and that is what makes
    `Delivery`'s requeue-on-drop safe. The original declares quorum queues
    without it, which on RabbitMQ 3.x means *unlimited* redelivery -- a poison
    message that comes back for ever and looks like throughput. RabbitMQ 4.0
    introduced a default of 20, so the behaviour of an unchanged declaration
    depends on the broker's version. Something has to end the loop, and
    "whichever RabbitMQ the cluster happens to run" is not it. The value chosen
    is 20, so a cluster already on 4.x sees no change.
  - `prefetch_count=1` is kept from the original and *relied on*: with one
    delivery outstanding, a settlement posted by `Drop` can be applied when the
    next message is asked for, and there is never a queue of them to reason
    about.
    The dead-letter queue sets `-1` for the same reason: "unlimited" there is
    not the argument's absence, and a DLQ has nowhere to dead-letter *to*, so a
    limit inherited from a 4.x default silently deletes a message after twenty
    deliveries -- a replay tool that crashes and reconnects twenty times is
    enough.
  - **Publishing waits for the broker to say it has the message.** Without
    `confirm_select` the `PublisherConfirm` that `basic_publish` returns
    resolves immediately to `NotRequested`, so awaiting it *looks* like an
    acknowledgement and is a no-op; and with `mandatory: false` a routing key
    with nothing bound to it is discarded by the default exchange in silence.
    Both returned `Ok(())`. `delivery_mode: persistent` bounds only what happens
    once the message is on disk. So confirms are enabled once on a cached
    publishing channel, `mandatory` is set, and only `Confirmation::Ack(None)`
    counts -- an `Ack` *carrying* the message back is the broker saying it had
    nowhere to put it.
  - The dead-letter queue is declared before the queue that points at it,
    because an `x-dead-letter-routing-key` naming a queue that does not exist is
    accepted at declare time and silently drops the message later.
  - `lapin` is pinned to `rustls--ring`, not the plain `rustls` feature. That
    feature pulls `tcp-stream`'s default `rustls--aws_lc_rs`, which turns on
    rustls's `aws_lc_rs` *alongside* the `ring` that sqlx, reqwest and mongodb
    already enable -- and with both on, `ClientConfig::builder()` cannot pick a
    provider and **panics**. Any binary linking this crate and a database client
    would have aborted on its first `amqps://` connection, which is not
    something a reconnect supervisor can retry. It also keeps `aws-lc-sys` and
    its cmake/C build out of CI and the image.
  - `QueueSpec`'s fields are private, so `new` is the only door. Two of them
    carry invariants a struct literal skips: the dead-letter name is derived and
    the derivation can fail, and a prefetch of anything but one breaks the
    assumption that makes `Drop`-posted settlement simple -- and in AMQP `0`
    means *unlimited*, so the literal that reads as "no prefetching" is the one
    that lets unbounded deliveries go outstanding.
  - `next()` returns `Result<Option<_>>`, not the `Option<Result<_>>` a stream
    would give: "the broker said no" and "there are no more messages" are not
    the same kind of thing, and a caller writing `while let Some(Ok(..))`
    silently treats the first as the second.
  - Two error variants carry context and the rest do not, deliberately. A
    `map_err` that names what failed is a closure reached only on a failure the
    suite has to be able to arrange -- so `Connect` and `Declare` exist because
    they have real failing tests (nothing listening; a queue that already exists
    as a classic queue), and everything else carries lapin's own message, which
    already names the operation and the AMQP reply code.
  - Tested against a real RabbitMQ, because both interesting properties belong
    to the broker rather than to this crate: that it accepts these arguments,
    and that a delivery dropped without being settled genuinely comes back.
    `verify.sh` and CI gained the container and `PNEUMA_TEST_AMQP_URL`, required
    rather than skipped.
  - `deny.toml` allows BSD-2-Clause. The original plan predicted every new pin
    would be MIT/Apache-2.0 and that this file would need no change; lapin
    brings the seven-crate `amq-rs` family, all BSD-2-Clause, which is
    BSD-3-Clause minus the no-endorsement clause -- strictly fewer obligations
    than a licence already allowed. The prediction was an assumption; the gate
    was the check.

- `pneuma-transport`, starting with the two rules a broker client gets wrong,
  decided without a broker. It has no dependencies at all in this commit; the
  clients `pneuma-nats` and `pneuma-amqp` are CI-forbidden from holding arrive
  with the code that needs them.
  - `Delivery` makes "a message that was neither acknowledged nor returned"
    unrepresentable. That is the shape of a live defect in
    the original executor: `defer func() { _ = js.DeleteMsg(...) }()` runs on
    *every* path out of the handler, including the failing ones, so work that
    failed is deleted exactly as if it had succeeded -- not retried, not
    dead-lettered, not recorded. The bug is not a wrong line; it is that the
    right line and the wrong line look identical. Here settling **consumes**
    the delivery so the compiler tracks it, the type is `#[must_use]`, and a
    delivery dropped unsettled is *returned* to the broker -- covering the
    early return, the `?`, and the panic, with the safe direction as the
    default. Requeue is only safe while something bounds redelivery, so the
    queue declaration will set `x-delivery-limit` explicitly rather than trust
    a server default that has changed between RabbitMQ releases.
  - `decide`, the reconnect supervisor, split into a pure decision and a
    three-line application -- the plan's highest coverage risk, because every
    interesting branch is reached by a broker misbehaving, which no test can
    arrange. The original's loop is kept, and its comment says why: `aio_pika`'s
    `connect_robust` restores the TCP connection but not the consumer, so a
    cancelled `Channel.Open` leaves a process that is connected and consuming
    nothing. Its **fixed** five-second delay is not kept -- a broker that just
    restarted is one every consumer reconnects to at once, and a constant
    interval is what synchronises them -- so the delay doubles to a ceiling
    from that same five seconds as a floor. Deliberately unjittered: jitter
    needs a random source, which the determinism gate exists to keep out.
  - The reset is gated on **survival, not attachment**, and that distinction is
    two bugs rather than one. Never resetting means a consumer up for a week
    whose broker restarts waits the *maximum* backoff before its first retry.
    Resetting the moment it attaches reinstates the fixed delay it was meant to
    remove: a broker that accepts the connection and then closes the channel --
    the exact failure the loop was written for -- gives `Up, Dropped, Up,
    Dropped, ...`, and a counter zeroed at every `Up` never exceeds one, so the
    ceiling is unreachable and every consumer of a flapping broker retries in
    lockstep. So `Dropped` carries how long the connection lasted, and only one
    that lasted past `stable` starts a fresh episode.
  - `Backoff` is built through a checked constructor, because two of the three
    ways of filling it in fail silently: a zero floor makes every delay zero
    however many failures there have been -- the unbounded reconnect loop a
    backoff exists to prevent, arrived at by configuring one -- and a ceiling
    below the floor collapses every delay to the ceiling, so raising it into
    the wrong variable gives a *shorter* wait than the default with nothing to
    say so.
  - A `Settle` implementation must not panic, and the drop path contains it if
    it does. The delivery is dropped *while unwinding* on the handler-panicked
    path, so a `send(..).expect(..)` in a shutdown race would be a double panic
    -- and the one type whose purpose is to make a handler panic survivable
    would turn it into a `SIGABRT`. Contained there and nowhere else: on `ack`
    a broken settler stays as loud as any other bug.

- **`pneuma-admission` becomes a process.** The plan's topology lists it as one
  of eight binaries; until this commit the crate had every part of a service
  and nothing that started them, so the port had two binaries against a target
  of eight.
  - `Config::from_env` reads the whole deployment once, and refuses to start on
    anything it cannot use. Four knobs are held to at least 1: three would
    otherwise be a service that does nothing, and the fourth is worse --
    `tokio::time::interval` **panics** on a zero period, so a mistyped dispatch
    interval would take the process down at the first tick, after it had
    reported ready. A negative sweep cutoff is refused for a quieter reason:
    `now - (-300s)` is an instant in the future, so every claim in the table is
    older than it and the sweep hands back work claimed a moment ago -- runs
    submitted twice, on a timer. Each has a ceiling as well as a floor, and for
    the same reason from the other side: `Utc::now() + delta` and
    `tokio::time::interval` both **panic** past chrono's range, so an unbounded
    value -- a pasted microsecond epoch -- is a pod that binds, reports ready,
    and dies on its first tick.
  - A variable set to blank falls back to its default rather than being taken
    literally. `PNEUMA_RESTATE_HANDLER=` is what `${HANDLER}` renders to when
    `HANDLER` is unset, and taken literally it gave a submission URL of
    `http://restate:8080//send`, a 404, and every submission settled `failed`
    permanently.
  - A mistyped tenant weight is refused rather than defaulted. Falling back to
    `Weight::ONE` gives a *smaller* share than intended to anyone configured
    above one, so the deployment would run, look healthy, and under-serve the
    customer somebody went to the trouble of prioritising.
  - `Ingress`, the real `Invoker`: one `reqwest::Client` with a connect and a
    request timeout, and the `/send` URL **parsed** once at startup rather than
    formatted per submission. Parsed, not merely built: `Client::post` parses
    lazily, so `PNEUMA_RESTATE_INGRESS=restate:8080` -- an ordinary compose
    typo -- would bind, report ready, and then fail every submission with a
    builder error, which is a transport failure, which is `Retry` for ever with
    nothing ever reaching Restate. The scheme and host are checked too, because
    `restate:8080` *does* parse: as a URL whose scheme is `restate`.
  - Redirects are not followed. reqwest's default follows up to ten, and on a
    301/302/303 it rewrites the POST to a GET and drops the body -- so an
    `http` → `https` redirect on a load balancer would turn a submission into a
    bodiless GET, Restate would answer 404, and the run would be settled
    `failed` without ever having been offered.
  - The response body is read as text and parsed leniently, because reading it
    as JSON would turn a proxy's HTML error page into a transport failure --
    which classifies as `Retry` for ever, while the status that page carried
    may well have been a permanent rejection.
  - `reclaim` joins the dispatcher's trait rather than sitting beside it. It is
    the other half of what a round deliberately leaves undone: a submission
    whose outcome is unknown stays `claimed`, and without a sweep it stays that
    way for ever, so a dispatcher that could claim but not reclaim is a queue
    with a slow leak. The sweep runs *before* the round on each tick, which
    keeps recovery from a Restate outage to one interval instead of two.
  - The dispatch loop's body has no branch in it. `report_line` renders both
    arms of a tick purely, so the failing arm is a unit test rather than a
    region of the loop only a database that is down could reach.
  - The HTTP surface and the dispatch loop are peers on one cancellation token,
    awaited together rather than spawned. Either stopping stops the other --
    including a bind that never succeeded, which would otherwise leave a failed
    start dispatching for ever instead of exiting non-zero -- and a panic in
    either reaches the process instead of arriving as a `JoinError` nobody is
    obliged to read.
  - Tested as a process: a real pool, the real router, the real dispatch loop
    and the real HTTP client, with a run posted to `/pneuma-admission/runs`
    arriving at a stub Restate under the run id as its idempotency key, and the
    queue row ending `done`. Plus the two startup refusals -- an unusable DSN,
    and an address that cannot be bound -- the second of which would hang
    rather than fail if the loops were not joined.

- The dispatch loop, and the wiring that makes `pneuma-admission` a service
  rather than a set of parts. Until this commit the only implementor of either
  trait was a test fake, so the router and the dispatcher were exported and
  unreachable from anything that runs — and dead wiring with a thorough test
  looks exactly like live wiring.
  - **A submission whose outcome is unknown is left claimed, not settled.**
    Settling it either way is a guess: `done` loses a run that was never
    submitted, `failed` gives up on one that may already be running. Leaving it
    claimed is what `reclaim` exists for, and the idempotency key makes
    resubmitting free if it did land after all. A round therefore reports
    `unknown` separately from `submitted` — a number that keeps growing means
    Restate is unreachable, not that runs are failing.
  - An idle round still takes a round number. `select_batch` rotates its
    tie-break by it, so a dispatcher that skipped the increment when idle would
    advance it only when there was work, and two dispatchers with different
    idle patterns would disagree about whose turn it is.
  - A claim shorter than the selection is not a failure — it is `claim` working
    as the atomic arbiter, with another dispatcher having got there first.
  - Tested against the real queue as well as fakes: a submission goes from the
    door through `accept`, the real `SubmissionStore`, a fair round and out to
    a stubbed Restate, ending `done`; an unreachable Restate leaves the row
    `claimed`, invisible to the next round, until `reclaim` returns it; and a
    tenant with one row is dispatched in the same round as a tenant with eight
    rather than after them.

- The fair half of a dispatch round, and **the first consumer
  `pneuma-fairness` has ever had**. It has been complete, pure and
  property-tested since it was written with nothing calling it, and a pillar
  with no consumer is a pillar nobody has checked the shape of.
  - Everything between the two queries is pure — grouping rows into flows,
    attaching weights, choosing the quota base — so a round can be examined
    without a database.
  - `round_base` gives each flow the **whole batch** as its ceiling, because
    work conservation is a precondition rather than a guarantee: no flow is
    ever handed another's unused share, so a batch fills only while the busy
    flows are under their own ceilings. Measured in the tests: the obvious
    `round_base = batch_size / flows` returns **7 items of 12** while a flow
    sits on a hundred; the whole-batch ceiling fills the round.
  - An unknown tenant gets `Weight::ONE` rather than being dropped. A tenant in
    the queue and not in the configuration is a new customer, or a config that
    has not caught up, and refusing to dispatch their work until someone edits
    a file is the worse answer.
  - `duplicate_flows` is checked by a **pure `alarm(&batch)`**, not by an `if`
    in the loop. Grouping is by tenant, so in production the set is always
    empty — inline it would be unreachable code inside an I/O function,
    reachable only by breaking the grouping. As a function over the `Batch` it
    is three lines in a test, and worth having because the consequence is
    silent: a flow with two backlogs takes its quota twice, so the round is
    unfair while every individual batch still looks correct.
  - `forbid-deps.sh pneuma-fairness "${IO_FREE}|axum|restate"` lands with the
    consumer, not before. A pure crate nobody depends on stays pure by
    accident; now that something uses it, the pressure to reach for a database
    "just to look up a weight" is real, and the engine it runs under must not
    be able to leak into the thing meant to outlive it.

- `pneuma-admission`, starting with the pure `accept()`: a submission is
  refused at the door if its pipeline does not resolve or its tenant id does
  not identify a flow.
  - **A blank tenant id is refused here because nothing below refuses it.**
    `TenantId::new` takes any string, `Dimension::new` validates only the
    dimension *name* and escapes the value, and `FlowKey::new` fails only on an
    empty dimension list -- so a blank id renders the perfectly valid key
    `tenant=`, and every submission missing a tenant is silently merged into
    one flow sharing one quota. That is the exact failure fair dispatch exists
    to prevent, arriving through the one field nobody validates. Whitespace
    counts as blank, since `"  "` would render `tenant=%20`: a distinct flow
    from every real tenant and from every other whitespace variant, so neither
    the blank case nor a real one.
  - The pipeline is resolved **before** anything is queued. The Restate handler
    resolves it too and answers 400 -- but by then the submission has been
    queued, selected against a tenant's quota, claimed and sent, so a
    definition naming a step that does not exist consumes a slot in somebody's
    fair share to produce an error that was knowable when it arrived.
    Resolution is pure, so doing it twice costs nothing.
  - **A blank job id is refused too, and it is the worse of the two.**
    `JobId::new` takes any string and `RunId::from_job` copies it, and the run
    id is the submission queue's primary key under `ON CONFLICT DO NOTHING` --
    so the first blank-job submission is queued and every later one, from any
    tenant and for any pipeline, comes back `AlreadyQueued` and is discarded as
    a redelivery of unrelated work. A merged quota is unfair; this drops runs.
  - Surrounding whitespace is removed from both ids rather than only tested
    for. Trimming to test and storing untrimmed admitted `"acme "` as flow
    `tenant=acme ` -- a different flow *and* a different grouping key from
    `"acme"` -- which is one tenant split in two by a template artefact.
  - The ingress: `POST /{service}/runs`, over a `Submissions` trait so every
    status is a unit test against a hand-rolled fake — finding out what a
    rejected pipeline answers should not need Postgres, and one of the four
    outcomes (`already_ran`) would otherwise mean claiming and settling a
    submission first. The trait is a seam for testing, not a dependency
    boundary: this crate depends on `pneuma-store` regardless, rather than
    inventing a second copy of `Accepted` to dodge it.
    - **202** for queued, already queued, and already claimed — all three mean
      "in, and not run yet", which is what a caller acts on; answering them
      differently makes every caller write a branch that does the same thing on
      both sides. **409** for a run that already happened, because 202 promises
      it will run and nothing will run that id again. **400** for a submission
      that is wrong and will be wrong next time. **503**, not 500, for a queue
      that cannot be reached: not the caller's fault and worth retrying.
    - The body is stored **verbatim except for the two ids**. It is taken as a
      `Value` and typed in the handler rather than extracted as a
      `Submission`, because round-tripping through that type drops any
      top-level key it does not know — silently stripping, between this door
      and the runner, a field the runner might understand, when the two
      crates' shapes are coupled by nothing but convention. The ids are the
      exception because they are not just data: `accept` trims them and
      derives the queue's primary key from the result, while the runner
      re-derives run identity from the *payload* (`Meta::run_id` is
      `RunId::from_job(&self.job_id)`). Left untrimmed, a submission with
      `"job-1\n"` is filed under `job-1` and executed under `job-1\n` — the run
      happens under an id the queue never knew, and a later genuine `"job-1"`
      is answered `AlreadyQueued` against a row whose payload says otherwise.
    - The receipt carries `succeeded` for a run that already happened, and is
      parseable. `status` was a `&'static str`, which makes a derived
      `Deserialize` demand a `'static` input — a response type no client could
      read back. And collapsing both `AlreadyRan` outcomes into one 409 threw
      away the distinction the store keeps them apart for: "it already
      succeeded, stop" and "it failed, and nothing will retry it under this
      id" are different actions.
    - The two statuses axum's extractor answers before this module sees the
      request — 415 for a wrong `content-type`, 400 for a body that is not
      JSON, both `text/plain` — are documented, because "an error body, in one
      shape" was not true of the whole surface.
  - `restate.rs`: a pure `disposition(status, body)` written **against the
    measurements in the design notes** rather than against expectations, and
    two of its arms are ones a classifier reasoned out from first principles
    gets wrong. `/send` answers **202 for both** a fresh submission and a
    redelivery, distinguishing them in the body's `status` field — so reading
    only the status counts a redelivery as new work, and reaching for 409
    produces a branch Restate never takes. An unrecognised label is still
    `Accepted`, because a 202 means Restate took it whatever it called the
    state, and treating a future version's label as a failure would retry a
    submission against a key that already has it.
  - **A run that failed is told apart from a Restate that is unwell**, by the
    `source` field rather than the status, since both are 5xx. A terminal
    handler failure comes back as `source: "invocation"`; the ingress's own
    answers say `"ingress"`. It matters because Restate *caches* the failure:
    under the deployment's `idempotency_retention` — a day by default — every
    resubmission under that key returns the same 500 in milliseconds with no
    work done, so calling it retryable means looping until the window expires,
    making no progress and never recording the run as failed.
  - 408, 425 and 429 are retried rather than dropped. A blanket
    `400..=499 => Rejected` swallowed them, and a gateway shedding load or a
    proxy timing out the ingress POST is transient — retrying is free, because
    the idempotency key is what makes a second attempt cost nothing.
  - What is *not* measured is named rather than left to be discovered: §22
    measured the cached failure on a blocking call and `/send` against a key
    whose run **succeeded**, so whether `/send` against a *failed* key answers
    202 or the cached 500 is unknown. Both are handled.
  - Tested in two halves that do not replace each other: a stub covers every
    arm, including a transport failure with no status to classify, which a real
    container will not produce on demand; and one test really talks to the
    container, because a stub agrees with whatever the file asserts and would
    otherwise pin this crate's beliefs rather than Restate's behaviour. That
    test says which it is when it fails, so "the container is not running"
    cannot read as "Restate changed its mind".
  - `supervise.rs`: the watchdog rule for the one gap the Restate spike's
    verdict named. Restate **does not give up** — the spike measured an
    invocation whose component failed every time staying `backing-off` with a
    climbing `retry_count`, nothing discarded and nothing complaining. That
    trade is favourable and it is not free: there is no automatic dead-letter
    equivalent, so a poison run waits for ever unless something watches for
    exactly that pattern.
    - Two observations, not one, because a single sample cannot tell a stuck
      run from a recovering one — transient failure is what the retry policy is
      *for*. But an **unchanged** retry count still raises the alarm: Restate's
      backoff is exponential up to a ceiling, so the longer a run is stuck the
      further apart its attempts get, and requiring the count to *grow* between
      samples would make the alarm depend on polling more slowly than the
      current backoff interval. A watchdog sampling every minute against a
      ten-minute ceiling would see the same count in every consecutive pair and
      stay quiet for ever — the silent poison run this rule exists to prevent.
      Only a *falling* count is disqualifying, and that means the id was reused
      or the counter reset.
    - Every arm that answers "not wedged" is tested, and they matter more than
      the one that answers "wedged": a watchdog that cries wolf gets switched
      off, and then the poison run it existed for waits for ever anyway. A
      status this code has never seen is *not* backing off, so a Restate
      release that adds or renames one cannot raise an alarm by being
      unrecognised.
  - Tested against a real corpus pipeline rather than hand-written definitions,
    **vendored** under `tests/fixtures/` the way `pneuma-core` already does. It
    was `include_str!`d from the sibling the original checkout, which resolves at
    build time: CI checks out this repository and nothing else, so `lint` and
    every coverage job behind it could not have compiled the test target at
    all. Green on one machine and red everywhere else is the failure this
    repository's gate exists to make impossible.

- The durable submission queue: `0003_submission.sql` and `SubmissionStore`
  with `enqueue`, `queued_backlogs`, `claim`, `settle` and `next_round`.
  Nothing in the original has this table -- the original accepts a run and
  dispatches it in the same breath, so a submission arriving while the
  controller is down never happened. It is the durable half of what
  `pneuma-fairness` needs to be usable at all: a flow's backlog has to survive
  a restart, or "fair over time" means "fair until the next deploy".
  - **A redelivery is not an error.** `run_id` is the primary key and `enqueue`
    is `ON CONFLICT DO NOTHING`, so the ordinary case for an at-least-once
    transport is `AlreadyQueued` rather than a failure -- and the first payload
    stands, so a redelivery cannot overwrite what is already queued.
  - **`claim` is deliberately not `SKIP LOCKED`.** The usual work-queue shape
    hands out whatever rows a locking query reaches first, which is the
    opposite of a fair selection: `select_batch` needs a flow's whole bounded
    backlog to compute its quota. The choice is made outside the database and
    the `UPDATE ... WHERE state = 'queued'` is the atomic arbiter -- two
    dispatchers that chose the same run race there and exactly one wins, which
    is tested with twenty contested rows and two concurrent claims.
  - **The backlog is bounded per tenant, not in total**, because a global limit
    would let a noisy tenant crowd a quiet one out of the *input* and the
    selection would then be provably fair over a sample that was not.
  - **A claim nobody settles goes back to the queue.** `claim` only moves
    `queued -> claimed` and `settle` only `claimed -> done|failed`, so a
    dispatcher dying between them stranded its rows for ever: no query found
    them, and re-submitting the same `run_id` collided with the primary key.
    `reclaim` sweeps claims older than a caller-chosen cutoff -- the caller's,
    because reclaiming a dispatch that is merely slow runs it twice.
  - **`enqueue` says *what* was already there.** `done` and `failed` rows are
    kept for ever, so a single "already here" answer covered a run waiting to
    be dispatched and one that finished last month, and a retry of a failed
    submission was dropped as a duplicate. Four answers now, discriminated by
    the `xmax = 0` an `ON CONFLICT DO UPDATE` returns.
  - **`claim` locks in `run_id` order**, through an `ORDER BY` inside a
    `FOR UPDATE` subquery. A bare `WHERE run_id = ANY($1)` locks in whatever
    order the plan visits, so two dispatchers with overlapping selections and
    different array sizes can take different plans, lock in opposite orders and
    deadlock -- which Postgres resolves by aborting one with 40P01, arriving as
    an error rather than the empty list a loser is supposed to get.
  - **The round is a Postgres sequence**, so it is shared across replicas and
    survives a restart. `select_batch` rotates its tie-break by it, and a round
    that does not advance hands the remainder of every uneven batch to the same
    flow for ever.
- `baseline` adopts a database only up to `ORIGINAL_THROUGH`. Adding
  `0003_submission` made this concrete: it creates a table the original migration tool never
  created, so no production database has it, and comparing a live schema
  against all three migrations refuses a database that is in fact correct.
  Recording all three would have been the worse outcome -- `sqlx migrate run`
  would see `0003` applied, skip creating `submission`, and every submission
  query would fail at run time on a deployment whose baseline reported success.
  The adoption now uses `original_migrator()` and everything after that line is
  an ordinary forward migration. "Already adopted" is decided **before** the
  schema comparison, which is a reversal: checking the schema first was right
  while the expectation was every migration in the repository, and wrong once
  it is only the adoption subset, because an adopted database goes on migrating
  and is *supposed* to be ahead. Left as it was, `baseline` refused a correct
  database with `SchemaMismatch` on the second run of any deploy script that
  baselines before migrating -- every deploy after the first. A test walks the whole sequence: baseline
  records `[1, 2]` and `submission` does not exist; `migrate run` creates it;
  all three end up recorded.
- `tests/schema.rs` builds its fixture by running the migrator rather than from
  a hand-maintained list of files, which is what drifted: `queries::ALL` caught
  `SUBMISSION_ENQUEUE` failing to prepare against a schema missing its table,
  but only after the fact.

- `PostgresHealth` and `MongoHealth`, the first implementations
  `pneuma_telemetry::HealthCheckable` has ever had -- the trait was written,
  and `liveness` had a shape with nothing to report on.
  - **Each probe carries its own deadline**, which neither original does
. `run_liveness_checks` awaits every ping in turn and
    imposes no timeout, deliberately, because the right value is known at the
    probe and not at the aggregation -- so without one an
    established-but-unresponsive dependency hangs the whole endpoint: the
    report naming the *other* dependencies is never produced, and the failure
    reads as a dead process. Kubernetes answers that by restarting a process
    that is fine. Mongo makes it concrete: its driver's own server selection is
    thirty seconds by default, far longer than any probe interval.
  - The names are the gateway's exactly -- `postgres` and `mongodb` -- and are
    asserted as *keys of the report*, since that is what makes them a wire
    contract rather than a return value.
  - Tested against a listener that accepts and never speaks, which is the shape
    that hangs a probe; a closed port is refused immediately and proves nothing
    about a deadline.
  - What the message **cannot** say is recorded too, because a first draft
    claimed a distinction that measurement disproved. Both clients retry
    internally for about thirty seconds by default -- sqlx's pool until
    `acquire_timeout`, Mongo's until `serverSelectionTimeoutMS` -- so a port
    refused *instantly* still reports `did not answer within 20s`. Shortening
    the client's own timeout below the probe's recovers the cause on Mongo
    (`Connection refused (os error 111)`) and never on Postgres, where sqlx
    reports its own `pool timed out` and discards the refusal beneath it. The
    deadline is what keeps the endpoint answering, which is the module's job;
    naming the cause is a property of the handle it was given.

- `pneuma-serve`, the HTTP surface, shutdown and scheduling every binary
  shares: `/{service}/healthz` and `/{service}/liveness` in `pneuma-gateway`'s
  exact wire shape, a `serve` that stops when a future completes rather than
  when it is killed, and an interval loop with a pure `missed` predicate for
  "a pass should have happened by now and did not".
  - **Not folded into `pneuma-telemetry`**, whose doc says it is deliberately
    HTTP-free -- which is what lets its aggregation be tested without binding a
    port. This is the HTTP half and depends on it, so the split holds in the
    direction it was drawn.
  - The service name must be a plain path segment. It is interpolated into an
    axum route *pattern*, so `"{svc}"` built a router answering
    `GET /anything/healthz` with 200 -- a wildcard shadowing every route it is
    merged with -- and `"{"` made `Router::route` panic, which is a fallible
    constructor aborting the process instead of returning its error.
  - Two checks reporting the same name are refused **before the process
    starts**. `run_liveness_checks` keys its report by name, so a collision
    means one check silently overwrites the other and `liveness` reports on a
    dependency nobody is watching, while answering `200`.
  - SIGTERM as well as SIGINT, and both handlers installed before either is
    awaited. Kubernetes sends SIGTERM and waits before SIGKILL, so a process
    that ignores it has its in-flight work destroyed on a timer every deploy --
    and for a janitor mid-pass that is an archive written and the matching
    delete not. The Restate SDK's own `listen_and_serve` handles SIGINT and not
    SIGTERM, which is right for a terminal and wrong for a cluster. Tested by
    sending this process a real SIGTERM.
  - The interval loop asks the token again *after* waiting for a tick.
    `CancellationToken::run_until_cancelled` is biased towards the future, not
    towards cancellation -- its `poll` tries the inner future first, and
    tokio-util's own doc says so -- so a due tick beats a cancellation arriving
    in the same poll and one more pass starts after the process was told to
    stop. Measured: two passes where there should be one. A comment here
    asserted the opposite bias and relied on it.
  - Ticks are `Skip`, not `Delay`: both drop what was missed rather than
    running it all at once, but `Delay` reschedules from *now*, so an hourly
    janitor whose pass takes twenty minutes drifts to 01:20, 02:40 and onwards
    while `missed` judges it against a fixed period.
  - Everything about routing is tested through `ServiceExt::oneshot` against a
    `Router`, so no test binds a port to check a status code; the interval loop
    is tested on `tokio::time::pause()`, so an hourly schedule is a microsecond
    test rather than a shorter period that proves nothing about the real one.

- A Restate spike (`spikes/restate/`) evaluating whether an existing durable
  execution engine can replace the `pneuma-engine` crate, which was ~45% of
  the remaining effort and carried all the concurrency risk. Verdict: adopt,
  with named gaps — see `spikes/restate/VERDICT.md`. The spike drives the real
  corpus fixtures through `pneuma-core` as a library, which is the first time
  that crate has been exercised from outside its own test suite.

- **`pneuma-janitor` is the eighth binary.** It was a library of passes with no
  process to run them; now it reads its environment, connects to both stores,
  serves `/pneuma-janitor/healthz` and `/pneuma-janitor/liveness` on `:9086`,
  and runs a pass every `PNEUMA_INTERVAL_MINUTES`.
  - `409` from the gateway is **success**: a termination for that run already
    exists, which means the run this pass found stale is already being
    cancelled. `disposition` is pure, so all four classes of answer are a test
    rather than something only a running gateway can show. A `404` is a wrong
    base URL rather than a missing termination — there is nothing to be missing
    on a create — so it is refused rather than retried for ever.
  - Two names for one Mongo collection is a **startup failure**. The archive is
    a `$merge` from the live collection into the history one, so with a single
    name it merges the collection into itself and the delete that follows
    removes them: a pass that exists to preserve work destroying it, from one
    typo in a manifest.
  - `INTERVAL_MINUTES` becomes a number of minutes rather than a cron minute
    field. `*/5` is the default and the only value anything sets, and reading
    the full grammar would mean carrying a cron parser for a schedule nobody
    writes; a value a plain interval cannot express is refused at startup rather
    than silently rounded.
  - `TIMEZONE` is **not** read, and the design notes works through why: the
    original's only use of it computes `datetime.now(tz) - timedelta(days=N)`
    and converts back to UTC, which cancels exactly, and its scheduler passes
    `timezone=UTC` explicitly. A test asserts the omission.
  - `--dry-run` selects and reports and changes nothing. `Janitor::preview_pass`
    was written for the original plan's week-long production diff and had
    no caller anywhere in the workspace until the flag reached it, so the diff
    the phase gate asks for could not have been run. It is **not**
    `PNEUMA_STALE_TERMINATION_ENABLED=false`, which the runbook had confused it
    with: that flag turns the stale *selection* off, so a week of passes with it
    reports `0 stale` however much work is stuck.
  - The termination phase is bounded at half the interval, reporting
    `deferred N` for what it did not attempt. Unbounded, one pass against an
    unresponsive gateway is `batch × timeout` — seventeen minutes at the
    defaults, inside a five-minute schedule — and `pneuma_serve::every` cannot
    tick while the body is awaited, so the archival and retention work that runs
    *first* stops happening too.
  - `PNEUMA_ALERT_MISS_GRACE_SECS` reaches a health check rather than nothing.
    With `PNEUMA_INTERVAL_MINUTES` it is the deadline a `passes` check measures
    against: past it, `liveness` reports degraded while `healthz` stays 200,
    because the process is running and a restart is not the remedy for a slow
    pass. Both knobs were parsed, range-checked and documented, and read by
    nothing — the shape the same module refuses to build for `TIMEZONE`.
  - A blank variable is an unset one, as everywhere else. `PNEUMA_LISTEN=` in a
    compose file is what `${JANITOR_LISTEN}` renders to when the outer variable
    is not set, and taking it literally was a pod that never started.
  - The gateway address is validated in `Config::from_env` rather than in
    `run`, so a typo is a configuration refusal (exit 2) before any pool exists
    rather than a startup failure two database handshakes later.

- **An end-to-end test that stubs nothing but the model.**
  `crates/pneuma-admission/tests/e2e.rs` drives a submission from the HTTP door,
  through the queue and a real dispatch round, into the real Restate ingress,
  the registered SDK endpoint and the handler, and asserts the component saw the
  `step_input` the submission carried. Every other integration test in this
  workspace joins two things and stubs the third; this is the seam between
  `pneuma-admission` and `pneuma-restate`, two crates that do not depend on each
  other and share a wire contract every key of which just changed.

- **The other coverage engine was measured, and rejected.** `cargo-tarpaulin`'s
  `--engine llvm` was run against all 21 crates. Thirteen read 100%; the other
  eight reported 22 lines the ptrace gate calls covered. Reading those lines is
  what settles it: about seven are the *opening* line of a multi-line
  `Ok(Constructor(` expression — `pneuma-runner`'s `Ok(Completed {` is reported
  uncovered while `tests/drive.rs:127` destructures it out of a successful
  `drive`. So llvm does not remove the line-attribution problem, it relocates
  it: ptrace loses the continuation lines, llvm loses the opening one. Ptrace's
  false negatives can be fixed by restructuring; llvm's cannot be fixed by
  testing at all, because the line already runs. `docs/verification.md` carries
  the table and the worked examples, and `scripts/verify.sh`'s header no longer
  implies the engine was never considered.
- **Eleven lines llvm was right about, recorded as a known gap.** The bodies of
  the `.map_err(|error| error.to_string())` closures in
  `pneuma-admission/src/store.rs` and `pneuma-intake/src/adapt.rs` — adapters
  that implement a port trait over a real store. The closure runs only when the
  underlying call fails, and every test drives the happy path against a real
  server; ptrace folds the closure into the line above it. Named rather than
  closed, because some are unreachable by construction — `serde_json::to_vec`
  on a `serde_json::Value` has no failing input — and contorting code to reach a
  branch a type forbids is how an exclusion list starts.
- **A provably dead branch in `pneuma-core`'s resolver, deleted rather than
  tested.** `Step::is_aggregator` *is* `aggregator_refs().is_some()`, so
  filtering on the first and then asking the second left an `else { continue }`
  nothing could take. `filter_map` takes the refs from the filter itself and the
  branch is gone. Found by the llvm run — the one thing in this measurement that
  was unambiguously a defect rather than an artefact.
- **The plans the port outgrew moved to `docs/history/`.** the original plan and
  The survey notes, with a status banner each and an index saying why they
  are kept. Nothing else was edited: both carry retractions, and a retraction is
  evidence. Rewriting an archived document to agree with what was learned later
  destroys the one thing it is still good for. The ~80 references to them by
  name were left alone — they name a document, not a path.

### Fixed

- `pneuma-migrate`'s schema fingerprint compared two schemas built from
  identical SQL as **unequal** once `0003_submission` landed. Its normalisation
  stripped a schema qualifier only from the token after ` ON ` and after
  `REFERENCES `, and a partial index's predicate and an enum column default
  render it as `'queued'::<schema>.submission_state` — neither position. The old
  version recorded that as a known limit; left in place it would have made
  `baseline` refuse every correct database. `strip_schema_qualifier` now matches
  the schema's own name, which is exact rather than positional and is safe
  inside a `CHECK`, where the positional rule provably was not.

### Changed

- The workspace carries a `[profile.dev]`, because it did not and the default
  filled a disk. Every test binary statically links its dependencies *and*
  embedded a full copy of their DWARF, so 15 crates' worth of integration tests
  each carried their own copy of mongodb, sqlx and tokio: **63 artifacts over
  10 MB, 3.0 GB between them**, and a gate run peaking at 5.6 GB. Twice that
  exhausted the disk, which surfaces as `No space left on device` followed by
  `ld terminated with signal 7 [Bus error]` -- a linker killed mid-write, which
  reads as a compiler bug rather than a full disk, twenty minutes into a run.
  `debug = "line-tables-only"` with `split-debuginfo = "unpacked"` keeps every
  line number -- what panics, backtraces and tarpaulin's line attribution read
  -- and drops what only a step debugger uses. Measured: **63 large artifacts
  totalling 3.0 GB became 5 totalling 0.09 GB**, and the peak fell from 5.6 GB
  to 3.95 GB, with the coverage gate still enforcing 100% on every crate --
  re-run to confirm that rather than assumed, since a coverage engine that
  quietly loses line attribution would report a different number for the same
  code.
- `verify.sh` refuses to start without 8 GiB free, the way it already refuses
  without the databases: this run cannot succeed without it, and a full disk is
  otherwise diagnosed as a linker crash after twenty minutes of work. Proved to
  fail when it should before being wired in.


- `pneuma-config` takes its environment as a value: `Env::from_process()` for a
  binary, `Env::from_pairs(...)` for a test. The helpers were free functions
  over `std::env`, which can only be tested by mutating a process global --
  a data race under any parallel runner, and why `std::env::set_var` is
  `unsafe` from edition 2024. `pneuma-janitor` had grown a crate-wide `Mutex`
  for it, and a lock only works while every test cooperates; six binaries and
  two config modules make that a matter of time, and the failure mode is a
  flake rather than a failure. `testenv.rs` and the lock are deleted, both
  janitor config modules read an injected `Env`, and their tests no longer have
  to name every key and unset it first so that a value in the developer's own
  shell cannot decide a result. The crate is back to `#![forbid(unsafe_code)]`.
- `Env` is one concrete type rather than generic over its reader. It was
  generic first, and the parameter leaked into every signature that touches
  configuration -- which would have spread through every service still to be
  written. Configuration is read a few dozen times at startup, so the boxed
  closure costs nothing measurable, and `&Env` is a type anyone can write down.
- The `VarError` classification is a pure function, so `NotUnicode` is
  reachable at all: producing one through the process needs a non-UTF-8 value
  *set* on it, which is the mutation this change removes -- and an arm that
  cannot be reached is an arm nobody has checked.

- `Message::has_reply_topic` no longer counts an empty string as a supplied
  address, and its rationale is corrected: the mechanism is not an empty routing
  key. Recorded as the defect notes — two guards on one path use
  different notions of "absent" (`if topic is None` in the controller,
  `if not topic` in every sender), so an empty topic raises
  `NoTopicSpecifiedError`, goes uncaught, and dead-letters the result of a run
  that has already been paid for. the original's guard is `if topic is None`, so `topic_output: ""` passes it
  and reaches `basic_publish` as the routing key; an empty routing key is not an
  address anyone receives on, so the predicate was answering "was the field
  populated" rather than "will results reach someone", which is what it exists
  to be asked. Found while reading the AMQP publish path, not by a test.
- Two claims made earlier in the design docs are corrected by measurement in
  `spikes/restate/VERDICT.md`: a payload size ceiling that turned out to be an
  artifact of the spike's own HTTP client rather than the engine, and "no
  determinism constraint", which overstated the case — replay still constrains
  the interpreter, just one interpreter rather than every pipeline author.

- The Restate component client is built once on `Runner` and cloned, with a
  connect timeout and a request timeout (`PNEUMA_COMPONENT_TIMEOUT_SECS`,
  default 300 s -- the original's model timeout). Previously it was
  `reqwest::Client::new()` per call with no timeout of any kind, so a component
  that accepted a connection and never answered hung the handler forever;
  `max_duration` bounds attempts, not one attempt. The default is arithmetic
  rather than taste, and the design notes show the working: the SDK tests
  `max_duration` *before* scheduling the next attempt, so a request timeout past
  half the retry bound authorises a second attempt that runs beyond it and
  charges for the component twice. The bound is therefore *derived* from the
  timeout rather than being a constant beside it -- otherwise
  `PNEUMA_COMPONENT_TIMEOUT_SECS` could reintroduce from the environment the
  defect the source no longer contains.

- A run's failure is now reported under a status that says what went wrong.
  Every terminal failure was a 500, because `TerminalError::new` is
  `new_with_code(500, ...)`, and that lost two distinctions the caller needs: a
  **cancellation** arrives as code 409, and reported as 500 it is
  indistinguishable from the run having broken, so a dispatcher retries what a
  human deliberately stopped; and a component's own 4xx says the *submission*
  is wrong, while 500 says this service is. A component's status is carried
  through, a response the contract cannot use is 502, a submission that is
  wrong -- a pipeline that does not resolve, a step with no component, a
  component name that cannot go in a URL -- is 400, and this service or this
  deployment failing is 500. The rule lives in one function, because applying
  it at some sites and not others is worse than not applying it: a component
  returning `<html>` failed 502 while the same component returning `{"foo":1}`
  failed 500, two neighbouring contract violations pointing at two different
  people. Three 4xx are deliberately not carried through, because the transport
  speaks them for itself: 409 is how the SDK reports a cancelled invocation, and
  401/403 are the ingress's own -- a client seeing either would re-authenticate
  against Restate when the truth is that a model refused the submission. All
  three become 502, with the component's own status still in the message. And a
  call that exhausts its retries is 502 rather than 500: the SDK wraps a
  retryable failure as 500, but every retryable failure on that path is a
  component that is down, slow, or answering 5xx, so a 500 would page whoever
  owns this service for something only the component's owner can fix. The one
  failure on that path which *is* this service's -- a request it could not
  serialise -- is raised before `ctx.run` with its own 500, for the same reason
  the endpoint template is resolved there: the bytes are identical on every
  attempt, so building them inside the retried closure repeats work for
  nothing, and a request this service cannot build is not a transient fault.
- The Restate suite's own harness could report on runs it did not drive. Both
  servers were started in tasks whose `JoinHandle`s were dropped, so a bind
  failure was invisible: with another process holding either port, the suite
  registered *that* one as the deployment and every test returned a plausible
  result for someone else's work. Found by it happening. Both ports are now
  taken on the calling thread, and the readiness probe refuses instead of
  falling through to registration after fifty failed attempts.
- The tests that assert a run fails now assert *which* failure -- the status
  and a fragment of the message -- rather than `is_client_error() ||
  is_server_error()`, which is satisfied by any failure at all, including the
  deployment not being registered. `not-json` gained the call counter its
  siblings have, without which it passed whether the parse failure was terminal
  or retryable, which is the entire property it is named for. And the ingress
  client has a timeout, so the regression mode of every one of them is a
  failure rather than a hang.

- `preview_pass` is now a faithful dry-run of `pass`. `pass` drops from its
  stale list the runs the same cycle is about to archive and delete -- otherwise
  it names runs whose records then exist nowhere, and the binary submits
  terminations for them -- and `preview_pass` did not. So the `--dry-run` that
  the original plan runs against production for a week disagreed with the
  pass it previews, which is the one thing that diff cannot afford; the comment
  saying so was three lines above the code that did it. The filter is now a
  pure function both call, and a test compares the two lists. `pneuma-migrate`'s
  `preview` and `baseline` had the identical bug for the identical reason: two
  callers, one decision, written twice.
- `baseline` refuses a scratch schema that already holds relations. The name
  checks caught `public`, `pg_*` and the schema being baselined; they did not
  catch a real application schema under any other name, and the first statement
  of a derivation is `DROP SCHEMA ... CASCADE`. One mistyped `--scratch` and it
  was gone, with the run reporting success. The check reads `pg_class` rather
  than `information_schema`, because the latter shows only relations the current
  user has a privilege on -- a schema full of another owner's tables would have
  read as empty and been dropped anyway, and it counts types and functions as
  well as relations -- `0001_noderun.sql` creates two enums before its first
  table, so "a schema of only types" is exactly what a half-applied migration
  leaves, and counting `pg_class` alone read that as empty.
  The scratch schema a derivation creates is stamped with a comment, and a
  stamped schema is still reusable: without that, the guard would have removed
  the recovery `DROP SCHEMA IF EXISTS` on the way in exists for, and one
  `baseline` killed mid-derivation would refuse every later run until an
  operator intervened.
- `stale_inprogress_run_ids` is ordered, stalest first. It had a `LIMIT` and no
  `ORDER BY`, so it returned an arbitrary subset -- measured at
  `["middle", "newest", "oldest"]` against rows inserted newest-first. Two
  things depended on that not happening: the janitor's `preview_pass` and `pass`
  each run it once and are supposed to report the same list, so the dry-run diff
  could disagree with the pass it previews; and a run outside the limit could be
  passed over for ever while newer stale runs were chosen ahead of it. Ordering
  by the run's oldest stuck row also puts the work in the order worth doing it.
- "There are no migrations to record" is now refused *before* the scratch schema
  is dropped and rebuilt. The check existed, but ran after the derivation, so
  the tool did all of its destructive work and then reported that there was
  nothing to do.
- `pneuma-fairness` states that work conservation is a **precondition, not a
  guarantee**: it holds only while `round_base × Σweights` over-subscribes
  `batch_size`. The module asserted it unconditionally, and the test named
  `an_idle_flows_share_is_redistributed_not_wasted` did not demonstrate it --
  no share is redistributed, the batch fills because the busy flow is still
  under its own ceiling. Both tests are renamed for what they show, and the
  failing case has one: with quotas summing to exactly the batch size, a batch
  of 12 comes back with 8 while a flow holds 100 items. That is what a caller
  gets from the obvious `round_base = batch_size / flows`.
- `pneuma-migrate`'s schema fingerprint states what it does not see -- RLS and
  its policies, `indisvalid`, triggers, grants, ownership, column order, views,
  functions, extensions. `baseline` reports "the live schema matches the
  migrations" on the strength of it, and a reader has to know how much that
  sentence covers.

### Fixed — defects carried over from the original

These are live in the original service; see the design notes for evidence and
verification of each.

- `str.lstrip` used as a prefix strip, silently aliasing distinct coordination
  keys, and a second aliasing route where a node id containing `:` collided
  with a fan-out suffix. Written up as the defect notes with the
  consequence spelled out: the aliased string is the map key for `refcounts`
  and `completed_prerequisites`, so two nodes reduced to one key share a
  prerequisite set — a join can release counting another node's prerequisite
  and then run on that node's output. Confirmed by running it against real
  corpus pipeline ids, where `'detect'` strips to `''` and `'preprocess'` and
  `'ocr'` both strip to `''`.
- An aggregator with nothing under it wedging the run permanently, two ways
  (the defect notes). A list aggregator over `[]` starts no children,
  and aggregation is attempted only when a child finishes — so the run stops
  for good with no error and no log line. Separately, `if not
  aggregated_output: break` treats a legitimately empty aggregate the same as
  "not ready yet", where `is None` is meant.
- A conditional handed a non-dict input reporting itself as
  `IncompatibleListAggregatorInputError` — the wrong class, one line from the
  right one, on a rare failure where the operator has no prior to correct it
  against (the defect notes).
- `CANCELLED` not treated as terminal, letting a late result resurrect
  cancelled work.
- An unknown successor logged a warning and continued, leaving a downstream
  join waiting forever for a prerequisite that could never arrive.
- No cycle detection at all — including the case where a component's successor
  is its own enclosing aggregator.
- A node id colliding with the `end` sentinel counted toward an aggregator's
  expected children while being unreferenceable.
- An aggregator starting outside its own components, leaving its barrier
  waiting on children the fan-out never reaches.
