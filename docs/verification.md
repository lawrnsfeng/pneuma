# Verification — the gate

Nothing lands in this repository without `./scripts/verify.sh` printing
**`all gates pass`**. This page says what it checks, why each check exists, and
how to run it.

## Running it

Five containers, four environment variables, one command.

```sh
docker run -d --name pn-pg     -p 5433:5432 -e POSTGRES_PASSWORD=pneuma postgres:16
docker run -d --name pn-mongo  -p 27018:27017 mongo:5.0.28
docker run -d --name pn-restate --add-host=host.docker.internal:host-gateway \
    -p 18080:8080 -p 19070:9070 restatedev/restate:1.7.8
docker run -d --name pn-rabbit -p 5673:5672 rabbitmq:3.13
docker run -d --name pn-nats   -p 4223:4222 nats:2.10 -js

export PNEUMA_TEST_DATABASE_URL=postgres://postgres:pneuma@127.0.0.1:5433/postgres
export PNEUMA_TEST_MONGO_URL=mongodb://127.0.0.1:27018
export PNEUMA_TEST_AMQP_URL='amqp://guest:guest@127.0.0.1:5673/%2f'
export PNEUMA_TEST_NATS_URL=nats://127.0.0.1:4223

./scripts/verify.sh              # three test runs, the default
./scripts/verify.sh 1            # one, while iterating
./scripts/coverage.sh <crate> --out stdout    # one crate, must print 100.00%
```

**Real infrastructure, not fakes.** The four URLs are *required*, not optional
with a skip. What a fake could not answer is whether RabbitMQ accepts these
queue arguments, whether a dropped delivery really does come back, whether a
message published to a subject reaches a subscriber on it, and whether a publish
that only reached a local buffer counts as sent. Every one of those is a
property of a real client and a real server, and every one of them was measured
wrong first.

## The ten steps

```mermaid
flowchart TD
    P0["preflight:<br/>4 URLs · curl · cargo-deny · 8 GiB free"] --> S1
    S1["1 · formatting"] --> S2
    S2["2 · clippy, warnings are errors"] --> S3
    S3["3 · docs, broken intra-doc links are errors"] --> S4
    S4["4 · determinism"] --> S5
    S5["5 · CI gates every crate"] --> S6
    S6["6 · forbidden dependencies"] --> S7
    S7["7 · dependency licences and advisories"] --> S8
    S8["8 · doctests"] --> P1
    P1["Restate admin answers?"] --> S9
    S9["9 · tests, 3 consecutive runs"] --> S10
    S10["10 · coverage, 100% per crate"] --> OK(["all gates pass"])
```

The order is not arbitrary. Everything portable runs first, so a developer on
macOS gets eight green steps before the parts that need Linux and a container.
The licence check sits above the three-times test sweep and the coverage run
because it takes seconds — learning about a licence violation twenty minutes in
is the same slow failure the Restate probe exists to avoid.

### 4 — determinism

`forbid-nondeterminism.sh`. Restate replays handlers in a **fresh process**, so
a container whose iteration order comes from a per-process hash seed corrupts
replay silently. The step registry was one, and it had been patched at three
call sites without anyone fixing the container.

### 5 — CI gates every crate

`ci-covers-every-crate.sh`. `verify.sh` derives its coverage list from
`crates/*/`; the GitHub workflow enumerates jobs by hand. The gap is silent in
the direction that matters — `pneuma-runner` was added with no CI coverage job
at all and every local run still said "all gates pass". It has since caught a
half-created crate directory mid-rename.

**The sequencing rule that follows:** any new crate lands with its CI job in the
same commit.

### 6 — forbidden dependencies

Six crates are checked for what they may not link, and one for what it may not
*write*.

| Crate | May not link |
|---|---|
| `pneuma-core` | any async runtime, any client |
| `pneuma-nats` | the same — it is the *naming* crate, not the client |
| `pneuma-amqp` | the same |
| `pneuma-gateway-client` | the same, plus `axum` |
| `pneuma-interpreter` | the same, plus `restate` |
| `pneuma-fairness` | the same, plus `axum` and `restate` |
| `pneuma-runner` | the same, plus `restate` and `axum` |

`pneuma-fairness` was guarded from the moment it acquired a consumer, not
before: a pure crate nobody depends on stays pure by accident. Now that
`pneuma-admission` uses it, the pressure to reach for a database "just to look
up a weight" is real — and the whole claim is that fair dispatch is decidable
without one.

`pneuma-runner`'s guard is what keeps `Component` a trait, which is what keeps
every branch of the drive loop reachable from a fake.

And `forbid-symbols.sh pneuma-driver` forbids
`BarrierStore|complete_prerequisite|expected_children|skip_child|refcount`. The
driver holds a run's `Execution` in memory, which is lost on a crash — and the
obvious cure, writing the barrier state down so a restart can resume,
reinstates the design the port removed, races included. That temptation arrives
months from now, in a hurry, from somebody who has not read the crate docs. The
script exits non-zero if the crate does not exist, because a guard that silently
checks nothing is worse than none.

### 9 — three consecutive runs

A suite that passes once is not a suite that passes. The scheduler's test helper
read `HashMap` iteration order and was green most of the time; one run would not
have found it.

### 10 — 100% coverage, per crate

Derived from the directory, so a new crate is covered without anyone remembering
to add it — the same reason the run-status list is checked for exhaustiveness
rather than trusted.

**Restructure rather than exclude.** A line that coverage cannot reach is
usually a line that should not exist. Exclusions accumulate and nobody audits
them.

**What tarpaulin does not attribute correctly** — every one of these has caused
a false 100% in this repository at least once:

| Shape | What happens |
|---|---|
| a multi-line struct pattern | attributed to the first line only |
| a multi-line call-argument continuation | the continuation lines are invisible |
| a bare `return` / `break` / `continue` in a match arm | not counted |
| `tokio::select!` | branches are not attributed |
| a match arm whose whole body is an enum constructor | reads as covered |

The fix in every case is the same: bind the value before the expression so the
interesting line is one line.

### The other engine was measured, and is not better

`cargo-tarpaulin` also has an `--engine llvm` backend, and the obvious question
is whether it would retire the table above. It was run against all 21 crates on
2026-09-08. **It would not, and the gate stays on ptrace.**

Thirteen crates read 100% under llvm. The other eight reported 22 lines the
ptrace gate calls covered — which sounds like a strict improvement until the
lines are read. They fall into three groups:

| Group | Lines | Verdict |
|---|---|---|
| `.map_err(\|error\| error.to_string())` closure bodies in `pneuma-admission/src/store.rs` and `pneuma-intake/src/adapt.rs` | 11 | **llvm is right.** See below. |
| The opening line of a multi-line `Ok(Constructor(` expression | ~7 | **llvm is wrong**, and demonstrably so. |
| Shutdown-only branches and one genuinely dead one | 4 | mixed; the dead one is now gone |

The second group is what settles it. `crates/pneuma-runner/src/driver.rs`'s
`Ok(Completed {` is reported uncovered, and
`crates/pneuma-runner/tests/drive.rs:127` destructures
`Ok(Completed { outputs, ran })` out of a successful `drive` — the line runs in
every passing test in that file. `crates/pneuma-migrate/src/run.rs`'s
`Ok(Report::WouldBaseline(` is reported uncovered, and
`crates/pneuma-migrate/tests/cli.rs:128` destructures it.

So llvm does not remove this problem, it **relocates** it: ptrace loses the
continuation lines of a multi-line expression, llvm loses the opening one. The
difference that decides it is that ptrace's false negatives can be fixed by
restructuring — bind the value, make the interesting line one line — while
llvm's cannot be fixed by testing at all, because the line already runs. A gate
whose failures have no honest remedy is a gate that gets excluded around.

Two other things worth recording, since the run cost an afternoon:

- **No crate fell back.** llvm silently downgrades to ptrace when unsupported
, and none did; stable
  1.97.1 supports it with no extra flags and no `llvm-tools-preview`.
- The three documented llvm caveats — no data from a test that exits non-zero,
  thread-unsafety, and profraw collision across forked processes — did **not**
  bite here. They were the expected risk and were not the deciding one.

### The two workarounds that were going to be reverted, and were not

The plan that ordered this measurement said that whichever engine won, two of
the eighteen ptrace workarounds should be undone — the ones held to have
*distorted a public type* rather than merely rearranged a line:
`pneuma-restate`'s unit error variants and `pneuma-core`'s deliberately
non-generic `unsupported()`. Reading them again, neither is a distortion, and
undoing them would take information out rather than put it back.

- `ServeError::Missing` and `EndpointError::EmptyComponent` are unit variants
  whose field-carrying versions carried a constant. `Missing` could only ever
  name `PNEUMA_COMPONENT_ENDPOINT`, because it is the only variable that can be
  missing, and the `Display` still names it from the constant; `EmptyComponent`
  would carry the empty name that is the whole complaint. A field that is always
  the same value is not information the caller lost.
- `pneuma_core::evaluator::unsupported` is a private helper with three call
  sites, all of which pass a `&Value`. Generic over what it takes, it would
  accept types no caller has, and every one of them would format identically.

So the count stands at eighteen, and this is not a workaround left in place for
convenience: it is two sites where the premise turned out to be wrong. The
sixteen the plan already declined to touch are unaffected.

### What llvm was right about, and what is being done with it

Eleven of the twenty-two are real, and they are the same shape in both places:
the body of a `.map_err(|error| error.to_string())` closure in an adapter that
implements a port trait over a real store —
`crates/pneuma-admission/src/store.rs` (six) and
`crates/pneuma-intake/src/adapt.rs` (five). The closure runs only when the
underlying call *fails*, and every test drives the happy path against a real
server. Ptrace folds the closure into the covered line above it; llvm does not.

These are recorded rather than closed, and the reason is not shrugging. Some are
reachable with a stub — an HTTP adapter pointed at a dead port. Others are not:
`adapt.rs`'s `serde_json::to_vec(event)` fails only for a value that cannot
serialise, and the input is already a `serde_json::Value`. Contorting the code
to reach a branch that a type makes impossible is how an exclusion list starts.
The honest position is the one this file takes elsewhere: name the gap, so it is
a known limit rather than an invisible one.

**And a coverage flake is a test-design bug, not a flake.** A test that spawned
`supervise` and cancelled it after a fixed sleep passed under normal `cargo
test` and, under instrumentation, sometimes never scheduled the spawned task —
so three failure paths never ran while the suite stayed green. It was fixed by
making `attach` public and running the loop in the current task.

## The one test that stubs nothing but the model

`crates/pneuma-admission/tests/e2e.rs`. Every other integration test in this
workspace joins two things and stubs the third: `pneuma-admission`'s `boot.rs`
posts to the real door and a stub Restate, `pneuma-restate`'s `handler.rs`
invokes a real Restate and never goes through the door.

The seam between them — that what admission submits is what the runner
accepts — was only asserted by shape. This drives a submission from the HTTP
door, through the queue, a real dispatch round, the real Restate ingress, the
registered SDK endpoint and the handler, into a component; then asserts the row
settled `done` **and** that the component saw the `step_input` the submission
carried.

Two things it had to learn, both of them real:

- **`done` on the row is not "the run finished".** `/send` is fire-and-forget,
  which is the whole reason the dispatcher can settle a row without waiting —
  so the component call happens after. Asserting straight away passes on a fast
  machine and fails on a slow one.
- **The run id cannot be a constant.** Restate keys on the idempotency key alone
  and does not compare request bodies, with a day's retention — so a fixed id
  makes the *second* run of the test return the first run's report, with no
  component call and a row that settles `done` anyway. Measured: it passed once
  and then failed for thirty seconds on every later run. That is
  The design notes's hazard, met in a test rather than
  in production.

## Two guards that are not in `verify.sh`

**`ORIGINAL_THROUGH` and the migration list** are asserted in
`pneuma-store`'s own tests: the embedded versions, the adoptable subset, and the
requirement that a migration exists *after* the original migration tool line — because without
one the two lists agree and the test proves nothing.

**The reserved wire keys** are asserted in `pneuma-proto`: each key is planted
in the `extra` catch-all and must be emitted exactly once and never carry the
planted value. That is the one place the encoder, the decoder and the
duplicate-key guard have to agree, and a missed rename there is silent on
encode — Rust refuses to read the result back, `orjson` takes last-wins.

## The rule for a module

Every module ends the same way: 100% coverage on the crate, `/code-review
medium` with **all** findings fixed before the next module starts, the full gate
green, one commit.

The reason findings are fixed before moving on rather than batched is that they
have repeatedly not been style points. Among them: a redirect policy that would
settle runs `failed` permanently; a `send_url` that was never parsed, so
`restate:8080` retried for ever; an `Event::Up` that reset the backoff attempt
counter unconditionally, so a flapping broker was never backed off from; a
panicking `Settle` on the unwind path, which is an abort; and a publish reported
as successful without confirms.
