# `pneuma-core`

The pure, I/O-free domain kernel of the pneuma workflow framework: the types a
pipeline is written in, the resolver that turns an authored definition into an
executable graph, and the evaluator that decides conditional branches.

## The dependency-direction rule

**This crate compiles without an async runtime.** No `tokio`, no `async-nats`,
no `mongodb`, no `sqlx` runtime or TLS feature. The one apparent exception is
`sqlx`'s `derive` + `postgres` features, which give `NodeStatus` its
compile-time `Type` mapping without pulling in a runtime — CI asserts this with
`cargo tree`.

That constraint is not incidental. It is what makes a 100% coverage gate
achievable rather than aspirational, and it is what will let the controller's
state machine be tested without containers when it is built on top.

## Module map, in dependency order

| Module | Holds |
|---|---|
| `ids` | Newtype identifiers. `RunId`/`JobId` stay distinct despite being equal today |
| `status` | `NodeStatus` and its `admit()` transition guard |
| `child_idx` | `ChildIdx(NonZeroU32)` — a 1-based index where zero is unrepresentable |
| `condition` | Branch conditions, operand inside each variant |
| `evaluator` | Pure evaluation of conditions against a JSON input |
| `slug` | Run-scoped hierarchical step identifiers |
| `child_ref` | `ChildRef{End, Node}` — the `end` sentinel as a variant |
| `start_set` | Non-empty, ordered start directive |
| `node` | The authored pipeline — the wire format |
| `step` | The resolved pipeline — what the engine executes |
| `resolver` | Flattens `node` into `step` |

`node` is what an author writes; `step` is what runs. Keeping them separate
types is why resolution can be a total function with typed failures.

## Testing

Three disciplines, applied where each actually fits rather than uniformly:

- **Exhaustive tables** where the input space is small and enumerable — the
  100-pair `admit()` matrix, the 24-cell operator × JSON-kind evaluator table.
  Line coverage saturates long before these are complete, so the table is what
  proves correctness; the coverage number only proves nothing was left unrun.
- **Property tests** where an invariant should hold over generated input —
  slug non-aliasing (with a generator deliberately biased toward the collision
  alphabet, since a uniform one would pass vacuously), resolver never-panics,
  and injected-cycle-always-rejected.
- **Example-based tests** everywhere else.

Plus `tests/reference_differential.rs`, the only test whose expected values were
not written by hand: it compares this resolver against the original's
own recorded output over all 11 real corpus fixtures.

```sh
cargo test -p pneuma-core
cargo tarpaulin -p pneuma-core \
  --exclude-files '*/pneuma-core/src/lib.rs' \
  --exclude-files '*/pneuma-core/tests/*' \
  --fail-under 100
```

## Making the coverage gate binding

CI defines three jobs — `lint`, `deny`, and `core-coverage` — but a workflow
that merely *runs* is advisory. To make the 100% gate actually block a merge,
mark all three as **required status checks** on the default branch:

> Settings → Branches → Add branch protection rule → Require status checks to
> pass before merging → select `lint`, `deny`, `core-coverage`.

One wrinkle worth knowing: the repository's default branch is currently
`master`, while the workflow triggers on `[main, master]`. That is deliberate
so either name works, but the protection rule must name the branch that
actually exists.

Until this is configured, a direct push bypasses the gate entirely.

## Deviations from the original

Every behavioural difference from the original is recorded in the design notes at
the workspace root, with evidence (`file:line`), risk, and the test that
verifies it. Several fix defects that are live in production today — read that
file before assuming a difference is accidental.
