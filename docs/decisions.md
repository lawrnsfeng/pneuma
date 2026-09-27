# Decisions

Every load-bearing decision in this system, with the evidence for it and where
that evidence lives. One paragraph each. Where a decision was settled by running
something rather than by reading something, that is said explicitly — those are
the ones worth trusting most.

## Architecture

### D1 — Adopt Restate rather than build a durable engine

**Decision:** durable execution comes from Restate; pneuma does not build a
journal, a replay mechanism or a barrier table.
**Evidence:** a spike, scored against a rubric fixed *before* anything was run.
Verdict **ADOPT**: rows 1–5 pass outright, 6–8 pass or pass with work.
**Where:** `spikes/restate/VERDICT.md`. The decisive finding was §2 — the
aggregation barrier disappears entirely, which removes the single most
race-prone piece of the original system rather than reimplementing it more
carefully.

### D2 — Keep a NATS path as a genuine fallback, not a demo

**Decision:** `pneuma-driver` plus `pneuma-executor` can run the same
interpreter over a plain broker, for a deployment that cannot run a Restate
server.
**Evidence:** [`BROKER-PORTABILITY.md`](BROKER-PORTABILITY.md).
**Why it holds:** both paths drive the *same* `pneuma-runner` over the same
`Component` trait. A fallback that shared no code with the primary would drift
until it was wrong, and nobody would find out until it was needed.

### D3 — The fan-in is a local counter, never a barrier table

**Decision:** an aggregator fires when the process driving the run has seen
every child. There is no `BarrierStore`, no `complete_prerequisite`, no
`expected_children`, no `skip_child` and no refcount in the database.
**Evidence:** [`CONCURRENCY-AND-DIRECTION.md`](CONCURRENCY-AND-DIRECTION.md)
§1.5, which enumerates the barrier races in the original.
**Enforced by:** `scripts/forbid-symbols.sh`, run in the gate. The guard exits
non-zero if the crate it checks does not exist, because a guard that silently
checks nothing is worse than no guard.

### D4 — Fairness belongs to pneuma, and to admission specifically

**Decision:** weighted per-tenant selection, in `pneuma-fairness`, applied at the
dispatch round.
**Evidence:** [`STRATEGY.md`](STRATEGY.md) — nothing in the landscape
provides it. Temporal's own guidance says namespace-per-tenant is "only
practical for a smaller number of high-value tenants" and "not a good fit if you
expect a very large number of tenants (10,000+)". The recommended alternative is
a shared namespace with per-tenant task queues, which is a workaround rather
than a primitive.
**Where:** `crates/pneuma-fairness`.

## Measured, not assumed

### D5 — An idempotency key, not `#[workflow]`

**Decision:** admission submits to Restate with the run id as an
`idempotency-key` on an **unkeyed** service, rather than modelling a run as a
Restate workflow.
**Evidence:** measured against a running Restate 1.7.8.
The design notes — attach and output *are* exposed for
idempotency-keyed invocations on an unkeyed service; they block for an in-flight
run and return immediately for a finished one; a redelivered submission costs
zero component calls.
**The hazard that came with it:** Restate keys on the idempotency key alone and
does **not** compare request bodies. A different pipeline submitted under a used
key returns the *first* submission's report, `200`, with nothing to indicate the
body was ignored. That binds on admission: a run id is minted once and never
reused.

### D6 — `abort-timeout`, not `inactivity-timeout`, bounds a `ctx.run`

**Decision:** the component timeout is set against `abort-timeout`.
**Evidence:** measured. The design notes
**Why it matters:** getting it backwards means a legitimately slow inference
call is killed by a setting nobody thought applied to it — and the symptom is a
timeout at a duration that appears in no configuration.

### D7 — A publish into a local buffer is not a publish

**Decision:** every publish is confirmed before it is reported as done.
**Evidence:** measured on both transports.

- **AMQP:** `basic_publish` returns `Ok` for a message the broker never saw.
  `confirm_select` plus `mandatory` is required, and only
  `Confirmation::Ack(None)` counts as delivered.
- **NATS:** `publish` returns `Ok` even on a **drained** client. `flush` is the
  strongest guarantee core NATS offers, and it must be bounded — an unbounded
  flush against a wedged server is a hang, not a guarantee.

**Where:** `crates/pneuma-amqp`, `crates/pneuma-nats`.

### D8 — The dispatch round comes from a Postgres sequence

**Decision:** `dispatch_round` is a `CYCLE` sequence, not a column and not a
per-process counter.
**Evidence:** measured — 800 items against 600 over 200 rounds, while every
individual batch was provably fair. `select_batch` rotates its tie-break by the
round, and a batch that does not divide evenly hands the remainder to somebody;
with a fixed round that is the same tenant every time. A per-process counter
resets on restart *and is per replica*, so two dispatchers both sit near zero
and favour the same tenant.

### D9 — `Component::call` promises no `Send`

**Decision:** the trait's returned future is deliberately not `Send`.
**Evidence:** it returned `+ Send` first, on the reasoning that a transport is
shared and real ones would be. Wiring the actual one disproved it:
`restate_sdk`'s `ContextSideEffects::run` returns `impl RunFuture<…>` with no
`Send` promise, so awaiting it makes the caller non-`Send`.
**What it costs:** `drive` cannot be `tokio::spawn`ed, so `pneuma-driver` runs
each run on a `LocalSet` and `boot::run` is itself `!Send`. That is the price of
not making the *primary* transport unimplementable in order to make the fallback
more convenient.

## Correctness

### D10 — A duplicate insert is success

**Decision:** `enqueue` is `ON CONFLICT DO NOTHING` and reports the submission as
in; `create` falls back to reading the existing row.
**Evidence:** the defect notes — the
original left the `run_id` index non-unique to tolerate a duplicate that should
have been refused, and a redelivered bootstrap silently forked a run in two.
**Why it is not merely tolerance:** a redelivery is the ordinary case for any
at-least-once transport. Treating it as an error means the normal operation of
the broker produces errors.

### D11 — `CANCELLED` is terminal, in the `WHERE` clause

**Decision:** every statement that moves a node carries
`status NOT IN (…, 'CANCELLED')`, and the decision and the write are one
statement.
**Evidence:** the design notes The original read the
row `FOR UPDATE`, decided in original, then issued a separate `UPDATE` — and its
`NODERUN_FINISHED_STATUSES` omitted `CANCELLED`, so a late result resurrected a
cancelled node.
**Pinned by:** a test that scans every statement for the terminal-status list
and requires `CANCELLED` in each. That is mechanical because memory already
failed once: `UPDATE_STATUS` shipped without it while
`STALE_INPROGRESS_RUN_IDS` had it, and the disagreement was written up as
deliberate.

### D12 — Three answers, not two

**Decision:** a delivery is acked, dead-lettered, or requeued.
**Evidence:** the original's consumers had ack-or-drop. A transport that only
knows ack-and-drop loses work on a network blip; one that only knows
ack-and-requeue spins for ever on a poison message.
**Where:** `crates/pneuma-transport`.

### D13 — A tenant id cannot become a subject

**Decision:** a tenant id is validated before it is interpolated into a NATS
subject.
**Evidence:** the defect notes — the
original interpolates it unvalidated, and a tenant id containing `.` or `>` is
how one customer reads another's traffic.

### D14 — Retry classification from typed errors, not printed ones

**Decision:** whether a component failure is retryable is decided by typed
predicates on the HTTP client.
**Evidence:** the original matched substrings in a formatted error string. A
library upgrade that rewords an error silently changes which failures are
retried, and nothing fails to compile.
**Where:** `crates/pneuma-executor/src/verdict.rs`.

### D15 — `x-delivery-limit` is always explicit

**Decision:** every quorum queue declares it; a dead-letter queue declares `-1`.
**Evidence:** absent means unlimited on RabbitMQ 3.x and 20 on 4.0. A manifest
that says nothing means two different things on two versions of the same broker,
and the difference only shows up as messages disappearing.

### D16 — The run document's `state` is a flat map, and the pipeline's `_id` is stripped

**Decision:** both, in `pneuma-intake`.
**Evidence:** `pneuma-gateway` indexes `state.<node_id>`; nesting the map a level
deeper leaves every one of those indexes matching nothing, and the symptom is a
run that exists and reports no progress. Embedding the pipeline definition with
its own `_id` intact makes every run after the first collide on insert, and the
collision is silently read as a redelivery.

### D17 — The correlation table, because request–reply is not available

**Decision:** `pneuma-driver` subscribes to `pneuma.result` **plainly**, not in a
queue group, and reassembles the round trip through a correlation table.
**Evidence:** the original executor publishes results to a *fixed configured
subject*, not a reply inbox, so NATS request–reply cannot be used. A run's
`Execution` lives in the replica that started it, so every replica must see every
result; the one holding the run claims it and the rest report `Unclaimed`.

## The wire

### D18 — The wire diverged deliberately, at a named commit

**Decision:** every inherited key whose name is wrong or opaque was renamed; the
component protocol's `jsonData` wrapper was dropped.
**Evidence and the full table:** the design notes The
last byte-compatible commit is named there.
**The filter:** rename a key whose name is *wrong or opaque*, not merely
inherited — the same rule that keeps `DATABASE_URL`.
**What it costs:** every deployed model must be rebuilt, with no dual-read and
no shim. [`as-built/component-api.md`](as-built/component-api.md) is the
deliverable that makes that survivable.

### D19 — One name for the fan-out index

**Decision:** `child_index`, replacing `child_id` (original) and `child_idx`.
**Evidence:** the protocol notes, and a check of
both sources: `child_id` appears exactly once in the original — the field
declaration — and is never set, so original emitted `child_id: null` on every
message. No original code touched its own field outside the struct definitions. The
two names were a bridge between producers that never read either one; the real
carrier is the `:N` suffix on the path.

### D20 — The pipeline definition's `type` was **not** renamed

**Decision:** the envelope's keys changed; the pipeline definition's
discriminant did not.
**Evidence:** a definition is authored outside this system and stored as
written. Renaming its keys would invalidate every pipeline already in Mongo and
the corpus the resolver is verified against. The envelope is ours; the
definition is not.

## Storage

### D21 — Rename, never drop-and-create

**Decision:** `0004_rename.sql` uses `ALTER … RENAME` throughout.
**Evidence:** a rename is a catalogue update — an `ACCESS EXCLUSIVE` lock for
the instant it runs, no rows rewritten, every index, constraint and grant kept.
On a table of any production size the difference from create-copy-drop is the
whole maintenance window.
**The part that is easy to miss:** Postgres does *not* rename indexes and
constraints when their table is renamed, and `pneuma-migrate` fingerprints both
by name — so each one is renamed explicitly or the database mismatches for ever.

### D22 — `ORIGINAL_THROUGH` stays at 2

**Decision:** `baseline` adopts a database by comparing it against migrations 1
and 2 only.
**Evidence:** `0003` creates `submission`, which the original migration tool never made and no
existing database has; `0004` renames what 1 and 2 created, so an original-era
database can only match an expectation built from 1 and 2 alone. Baselining
against all four would refuse a correct database, or — worse — record `0003` as
applied against a database with no `submission` table, so `migrate run` would
skip creating it and every submission query would fail at run time on a
deployment whose baseline reported success.

### D23 — The fingerprint strips the schema's own name, everywhere

**Decision:** `strip_schema_qualifier` removes `<schema>.` from every rendered
definition, rather than from the token after ` ON ` or `REFERENCES `.
**Evidence:** the positional rule worked for `0001` and `0002` and stopped
working the moment `0003` landed a partial index and an enum default —
`'queued'::<schema>.submission_state` is in neither position. The old version
recorded that as a known limit; two schemas built from identical SQL compared
unequal, which would make `baseline` refuse every correct database. Matching the
schema's own name is exact rather than positional, and is safe inside a `CHECK`,
which the positional rule was not.

### D24 — Two stores, on purpose

**Decision:** Postgres for steps, MongoDB for runs.
**Evidence:** [`DATABASE-PORTABILITY.md`](DATABASE-PORTABILITY.md). A whole
run's state is read and written in one round trip; a step needs relational
integrity, a self-referential parent key and partial indexes.
**The counter-argument is real** and is V2's: one store means transactional
atomicity across the ledger, the barriers and the outbox. See
[`v2/rationale.md`](v2/rationale.md).

## Process

### D25 — 100% line coverage, verified by running it

**Decision:** every crate in `src/` is held at 100%, and the number comes from
`./scripts/coverage.sh`, not from an estimate.
**Evidence:** it has repeatedly found real gaps. The recurring hazard is
`cargo-tarpaulin`'s attribution blind spots — a multi-line struct pattern, a
multi-line call-argument continuation, a bare `return`/`break`/`continue` in a
match arm, a `tokio::select!`, or a match arm whose whole body is an enum
constructor. Each of those reads as covered when it is not.
**The rule that follows:** restructure the code rather than exclude it. A line
that cannot be reached is usually a line that should not exist. That rule earns
its keep: measuring the alternative engine turned up one branch in
`pneuma-core`'s resolver that was provably dead — `is_aggregator()` *is*
`aggregator_refs().is_some()`, so asking the second question after filtering on
the first had an `else` nothing could take — and it is now gone rather than
tested.

**The other engine was measured and rejected**, on 2026-09-08, against all 21
crates. `--engine llvm` does not retire the blind spots; it relocates them.
Ptrace loses the continuation lines of a multi-line expression, llvm loses the
opening one — and llvm's false negatives land on lines that demonstrably run,
which no test can fix. `verification.md` carries the table and the two worked
examples. Eleven of the lines it flagged *are* real untested error paths, and
those are recorded there as a known gap.

### D26 — Coverage flakes are test-design bugs

**Decision:** a test that spawns a task and cancels it after a fixed sleep is
rewritten to run the loop in the current task.
**Evidence:** measured. Under coverage instrumentation the spawned task was not
always scheduled, so three failure paths never ran while the suite stayed green.
The fix was making `attach` public and driving the loop directly.

### D28 — The toolchain is pinned in a file

**Decision:** `rust-toolchain.toml`, against the original plan's decision not
to.
**Evidence:** the design notes
