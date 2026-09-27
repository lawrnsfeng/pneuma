# pneuma — database portability by design

> Answers: could the framework support an OLTP RDBMS other than Postgres, without breaking any mechanism already designed, while keeping "one database, sharded only if data outgrows it"? Same rigor as `BROKER-PORTABILITY.md` — verified claims, not assumed compatibility from "speaks the wire protocol."

## The crux insight, parallel to the broker case

Most of this portability was already bought by earlier restraint, not new work: the design deliberately uses `READ COMMITTED` + explicit row locking, never `SERIALIZABLE` or anything exotic; UUIDs are generated client-side in Rust (`uuid` v7), never by a DB-side function; the design avoids `LISTEN/NOTIFY` as a load-bearing mechanism (mentioned once, as an optional Tier-2 optimization, explicitly not required). None of those choices were made for portability — they were made for simplicity and correctness — but they mean the honest remaining risk concentrates in exactly two features, both load-bearing and both worth verifying per-database rather than assuming: **`RETURNING`** and **`SKIP LOCKED`**.

## Two corrections from verification, not assumption

**CockroachDB's `SKIP LOCKED` under `READ COMMITTED` is not a clean drop-in**, despite Postgres wire-protocol compatibility. [An open, reported issue](https://github.com/cockroachdb/cockroach/issues/121917): workers frequently fail to find jobs using `SELECT FOR UPDATE SKIP LOCKED` under `READ COMMITTED`, plus documented slowness at high concurrent-query counts. Exactly the primitive Tier-1 dispatch scaling depends on most (`ARCHITECTURE-V2.md` §3). Not disqualifying, but "needs real validation against a current version," not "obviously fine, same driver."

**`sqlx` dropped MSSQL support entirely** — supported prior to 0.7, removed pending a rewrite. SQL Server's SQL-level story is fine (`OUTPUT` ≈ `RETURNING`, `READPAST` ≈ `SKIP LOCKED`), but a SQL Server adapter needs `tiberius` directly, losing `sqlx`'s compile-time query checking and offline-metadata story. A Rust-toolchain cost, not a SQL-semantics one — keep the two axes separate.

**MySQL proper has no `RETURNING` clause**, still, as of 8.0. MariaDB (a fork, not the same codebase) added it in 10.5. Worth naming precisely — these two are often conflated as interchangeable and aren't, on exactly this feature.

## Mechanism-by-mechanism dependency table

| Mechanism | Postgres | Citus (on Postgres) | CockroachDB/Yugabyte | MariaDB | MySQL | SQL Server | Oracle | SQLite |
|---|---|---|---|---|---|---|---|---|
| `SELECT FOR UPDATE` | yes | yes | yes | yes | yes | yes | yes | no (single-writer) |
| `FOR UPDATE SKIP LOCKED` | yes | yes | **needs validation** | yes (8.0+) | yes (8.0+) | via `READPAST` | yes, long-standing | no |
| `INSERT ... RETURNING` | yes | yes | yes | yes (10.5+) | **no — needs 2-step pattern** | via `OUTPUT` | yes | yes |
| `ON CONFLICT DO NOTHING` | yes | yes | yes | `ON DUPLICATE KEY UPDATE` (different pattern) | same | `MERGE` (different pattern) | `MERGE` | yes |
| Window functions | yes | yes | yes | yes (8.0+) | yes (8.0+) | yes | yes | yes |
| JSONB-equivalent | native, indexable | native (Postgres) | native | JSON type, weaker indexing | JSON type, weaker indexing | native (recent versions) | native (21c+) | JSON1 extension |
| Native ENUM | yes | yes | yes (PG-compatible) | yes (different reorder semantics) | yes | no — `CHECK`/lookup table | no — `CHECK`/lookup table | no |
| `sqlx` first-class | yes | yes (same driver) | yes (same driver) | yes | yes | **no — needs `tiberius`** | no | yes |
| Multi-writer production fit | yes | yes | yes | yes | yes | yes | yes | **no** |

## Ranked by actual risk, given what's already designed

**Tier 0 — PostgreSQL.** Baseline. Zero friction, everything already verified against it.

**Tier 0.5 — Postgres + Citus, for sharding.** The conservative answer to "shard if data outgrows one node, keep one database." Citus is an extension on literal Postgres — same driver, same SQL, same already-verified `SKIP LOCKED` behavior — sharding added transparently rather than by switching engines. Direct continuation of the Tier 1/Tier 2 story in `ARCHITECTURE-V2.md`, not a new database.

**Tier 1 — CockroachDB / YugabyteDB.** The most *interesting* option on paper — near-zero SQL porting via the same `sqlx::Postgres` driver, and native transparent sharding that could make the manual `tenant_id`-partitioning design unnecessary entirely. But given the `SKIP LOCKED`/`READ COMMITTED` finding, needs a real load test before being trusted, not an assumption. (Yugabyte not independently verified to share the identical issue — different underlying storage engine, same general distributed-consensus category — flagged as unverified, not assumed inherited.) Fallback if `SKIP LOCKED` proves unreliable: CRDB's native `SERIALIZABLE` with optimistic concurrency and retry-on-conflict — a real redesign of the dispatcher's concurrency model, not a config flag.

**Tier 2 — MariaDB.** A genuinely different RDBMS family, `sqlx`-native, has `RETURNING`. The better choice within this family if it's ever picked at all.

**Tier 3 — MySQL.** Same family, missing `RETURNING` — a two-step insert-then-select pattern instead, more adapter code, a transaction-scoping detail to get right (the insert and the follow-up select need to be in one transaction with appropriate locking to avoid a race), not just different syntax.

**Tier 4 — SQL Server.** SQL-capable, Rust-ecosystem-expensive given the dropped `sqlx` support.

**Tier 5 — Oracle.** Technically capable (arguably the original implementer of `SKIP LOCKED`), low practical fit given the project's open-source, Rust-native, cloud-native direction. Not worth deep investment.

**Not a production candidate, but the right fit elsewhere — SQLite.** Fundamentally single-writer; no true multi-process competing-consumer pattern, so it can't serve as the production substrate. The obvious, correct choice for the Toolkit's local runner (`STRATEGY.md` Phase 3) — zero infrastructure, `sqlx`-native, matches "run it on a laptop" exactly.

## What this means for the abstraction, concretely

Same discipline as the broker layer: design `pneuma-store`'s query layer around the *portable* subset (`SELECT FOR UPDATE`, window functions, `ON CONFLICT`-shaped upsert as an abstracted operation rather than literal syntax) and treat `RETURNING` and `SKIP LOCKED` as the two named risk points requiring an explicit compatibility check per target database — not features to assume transfer because "it's SQL." If CockroachDB or Yugabyte is ever seriously pursued for the native-sharding payoff, the `SKIP LOCKED` validation is the one prerequisite that gates the decision, not a footnote to discover after committing.
