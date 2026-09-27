# pneuma

A workflow engine for AI pipelines: a directed graph of components, expressed as
declarative YAML, interpreted by a fixed engine. A Rust rewrite of a
legacy system, with the defects that rewrite found closed rather than
carried over.

**Start here: [`docs/`](docs/README.md).** That is the architecture wiki — the
services, the protocols, the flows, the storage, the decisions and the runbook.

```
docs/README.md                  index, and which architecture is real
docs/context.md                 what this is and what it replaces
docs/as-built/overview.md       the eight services
docs/as-built/protocols.md      SPECIFICATION — subjects, envelopes, HTTP
docs/as-built/component-api.md  SPECIFICATION — what a model receives and returns
docs/as-built/flows.md          sequence, activity and state diagrams
docs/as-built/storage.md        the schemas, and the rename migration
docs/v2/                        the six-component redesign, unbuilt
docs/decisions.md               every decision, with where it was measured
docs/verification.md            the gate
docs/runbook.md                 cutover and operations
docs/history/                   the plans the port outgrew, kept as records
```

## The crates

Twenty-one crates. The binaries are thin; almost all the logic is in libraries
that can be tested without any infrastructure at all.

| Crate | Binary | What it is |
|---|---|---|
| `pneuma-core` | | the pure domain kernel: ids, graph, status, resolver, evaluator |
| `pneuma-proto` | | the wire types |
| `pneuma-interpreter` | | which steps run next |
| `pneuma-runner` | | drives a run over a `Component` |
| `pneuma-fairness` | | weighted fair selection |
| `pneuma-store` | | PostgreSQL and MongoDB |
| `pneuma-nats` | | subject grammar and stream topology — **not** a client |
| `pneuma-amqp` | | queue naming and topology — **not** a client |
| `pneuma-transport` | | connections, reconnection, and delivery settlement |
| `pneuma-serve` | | health, shutdown, and the interval ticker |
| `pneuma-config` | | typed environment reading |
| `pneuma-telemetry` | | dependency health, deliberately HTTP-free |
| `pneuma-gateway-client` | | the termination API of `pneuma-gateway` |
| `pneuma-admission` | ✔ | the front door and the fair dispatcher |
| `pneuma-intake` | ✔ | the AMQP door |
| `pneuma-restate` | ✔ | durable execution |
| `pneuma-driver` | ✔ | the NATS-path run driver |
| `pneuma-broker` | ✔ | per-tenant subject routing |
| `pneuma-executor` | ✔ | calls a component, publishes the result |
| `pneuma-janitor` | ✔ | archival, retention, stale-run cleanup |
| `pneuma-migrate` | ✔ | schema adoption and migration |

## The dependency-direction rule

**Dependencies point inward, and the inner crates own no I/O.**

`pneuma-core` and `pneuma-interpreter` link no async runtime and no client — no
`tokio`, no `reqwest`, no `mongodb`, no `sqlx`, no broker. `pneuma-runner` adds
`restate` and `axum` to that list. `pneuma-fairness` adds both. `pneuma-nats`
and `pneuma-amqp` are *naming* crates: they decide what a subject or a queue is
called and what topology it needs, and something else connects.

That is enforced, not documented: `scripts/forbid-deps.sh` runs in the gate and
fails on a forbidden transitive dependency. It is what keeps the domain testable
without infrastructure and what let a durable-execution engine be adopted
underneath the interpreter without touching it.

## Building and testing

```sh
cargo build --workspace
cargo test --workspace          # needs the containers below
./scripts/verify.sh             # the full gate — commit only on "all gates pass"
```

The test suite talks to real infrastructure rather than fakes, because what a
fake could not answer is whether the broker accepts these arguments and whether
a publish that reached only a local buffer counts as sent. See
[`docs/verification.md`](docs/verification.md) for the containers, the
environment variables, and what each of the eleven gate steps checks.

## Licence

Dual-licensed under [MIT](LICENSE-MIT) or [Apache 2.0](LICENSE-APACHE), at your
option.
