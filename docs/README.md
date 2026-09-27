# pneuma — technical wiki

This is the entry point to the design of pneuma. Fifteen documents in the
repository root record decisions as they were made; this directory is the
reading order that turns them into an architecture.

## Which architecture is real

Two are described here, and confusing them is the single most likely way to
misread this repository.

| | **As-built** | **V2** |
|---|---|---|
| Status | **what is implemented, tested and gated** | a design, unbuilt |
| Services | 8 binaries | 6 components |
| Durable execution | Restate, with a NATS path as the fallback | none — Postgres only |
| Stores | Postgres + MongoDB | Postgres only |
| Source | this directory's [`as-built/`](as-built/) | [`ARCHITECTURE-V2.md`](ARCHITECTURE-V2.md), summarised in [`v2/`](v2/) |

`ARCHITECTURE-V2.md` does not
describe the running system: it describes what a rebuild would look like if the
boundaries were drawn again from scratch, knowing what the port learned. Both
are documented here in equal depth, because the argument between them is the
most useful thing either one contains — see [`v2/rationale.md`](v2/rationale.md).

**If you only need one page:** [`as-built/overview.md`](as-built/overview.md).

## How to read this

1. [`context.md`](context.md) — what the system does, what it replaces, and the
   four things it is trying to be better at.
2. [`as-built/overview.md`](as-built/overview.md) — the eight services and how
   they fit together.
3. [`as-built/protocols.md`](as-built/protocols.md) — **specification.** Every
   subject, every envelope, every HTTP surface.
4. [`as-built/component-api.md`](as-built/component-api.md) — **specification.**
   What an AI component receives and must return.
5. [`as-built/flows.md`](as-built/flows.md) — submission to completion, on both
   execution paths, plus failure and cancellation.
6. [`as-built/storage.md`](as-built/storage.md) — the Postgres schema, the Mongo
   documents, and the rename migration.
7. [`v2/overview.md`](v2/overview.md), [`v2/rationale.md`](v2/rationale.md),
   [`v2/migration.md`](v2/migration.md) — the other architecture.
8. [`decisions.md`](decisions.md) — every load-bearing decision, with where it
   was measured.
9. [`verification.md`](verification.md) — the gate, and how to run it.
10. [`runbook.md`](runbook.md) — the cutover, running it, and what to do when
    something is wrong.

## Two pages are specifications, not descriptions

[`protocols.md`](as-built/protocols.md) and
[`component-api.md`](as-built/component-api.md) are the **only** statement of
the new contract that exists anywhere. The port deliberately broke compatibility
with the earlier services and the deployed models, and those two pages are what
makes that survivable. Whoever rebuilds a component or a producer reads them, not the
Rust.

## The documents behind this one

| Document | What it holds |
|---|---|
| [`STRATEGY.md`](STRATEGY.md) | why this system exists, and the four differentiators |
| [`CORE-PILLARS.md`](CORE-PILLARS.md) | the pillars the design is measured against |
| [`ARCHITECTURE-V2.md`](ARCHITECTURE-V2.md) | the six-component redesign |
| [`CONCURRENCY-AND-DIRECTION.md`](CONCURRENCY-AND-DIRECTION.md) | how a run advances, and what must never be a barrier table |
| [`BROKER-PORTABILITY.md`](BROKER-PORTABILITY.md) | why two transports, and what each guarantees |
| [`DATABASE-PORTABILITY.md`](DATABASE-PORTABILITY.md) | what the two stores are each for |
| [`FRAMEWORK-FOUNDATIONS.md`](FRAMEWORK-FOUNDATIONS.md) | the crate layout and why the boundaries fall where they do |
| [`LANDSCAPE-AND-EVOLUTION.md`](LANDSCAPE-AND-EVOLUTION.md) | what else exists, and why none of it was adopted whole |
| [`PREGEL-NOTES.md`](PREGEL-NOTES.md) | the superstep model the interpreter is built on |
| [`CHANGELOG.md`](CHANGELOG.md) | what landed, in order |
