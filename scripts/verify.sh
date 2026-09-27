#!/usr/bin/env bash
#
# The whole gate, in one command that cannot be misread.
#
# This exists because reading gate output and moving on has failed repeatedly:
# a clippy failure committed over twice, a "583 tests, 0 failed" reported from a
# single run of an order-dependent suite, and an edit assumed applied that had
# not. Each time the evidence was on screen and the wrong conclusion was drawn
# from it.
#
# So: `set -e` rather than eyeballing, every crate's coverage derived rather
# than listed, the suite run more than once rather than once, and one verdict
# at the end.
#
# Usage:  scripts/verify.sh [runs]      # runs defaults to 3
# `-E` so the ERR trap below is inherited by functions and subshells. Without
# it the trap is silently not installed where most of the work happens, which
# is how a trap added to announce failure announced nothing.
# Requires three containers. The gate covers every crate in `crates/`, and
# three of them are tested against real servers rather than mocks:
#
#   docker run -d --name pn-pg    -p 5433:5432 -e POSTGRES_PASSWORD=pneuma postgres:16
#   docker run -d --name pn-mongo -p 27018:27017 mongo:5.0.28
#   docker run -d --name pn-restate --add-host=host.docker.internal:host-gateway \
#       -p 18080:8080 -p 19070:9070 restatedev/restate:1.7.8
#   docker run -d --name pn-rabbit -p 5673:5672 rabbitmq:3.13
#   docker run -d --name pn-nats   -p 4223:4222 nats:2.10 -js
#
#   export PNEUMA_TEST_DATABASE_URL=postgres://postgres:pneuma@127.0.0.1:5433/postgres
#   export PNEUMA_TEST_MONGO_URL=mongodb://127.0.0.1:27018
#   export PNEUMA_TEST_AMQP_URL='amqp://guest:guest@127.0.0.1:5673/%2f'
#   export PNEUMA_TEST_NATS_URL=nats://127.0.0.1:4223
#
# They fail loudly rather than skipping. A suite that quietly skips its
# integration tests reports success while checking nothing, which is the
# failure mode this whole directory exists to avoid.

set -Eeuo pipefail

# The two databases the store's gate needs, and the URLs that match them:
#
#     docker run -d --name pn-pg    -p 5433:5432 -e POSTGRES_PASSWORD=pneuma postgres:16
#     docker run -d --name pn-mongo -p 27018:27017 mongo:5.0.28
#
#     docker run -d --name pn-restate --add-host=host.docker.internal:host-gateway \
#         -p 18080:8080 -p 19070:9070 restatedev/restate:1.7.8
#     docker run -d --name pn-rabbit -p 5673:5672 rabbitmq:3.13
#     docker run -d --name pn-nats   -p 4223:4222 nats:2.10 -js
#
#     export PNEUMA_TEST_DATABASE_URL=postgres://postgres:pneuma@127.0.0.1:5433/postgres
#     export PNEUMA_TEST_MONGO_URL=mongodb://127.0.0.1:27018
#     export PNEUMA_TEST_AMQP_URL='amqp://guest:guest@127.0.0.1:5673/%2f'
#     export PNEUMA_TEST_NATS_URL=nats://127.0.0.1:4223
#
# Spelled out here because they are otherwise only in the header comments of
# three different test files, and finding them again is a five-minute detour
# every time.
#
# Two prerequisites worth knowing before starting:
#
#   * The coverage step needs Linux -- cargo-tarpaulin's default engine on
#     x86_64 Linux is ptrace, which does not build on macOS. Everything up to
#     it runs anywhere. tarpaulin's `--engine llvm` would lift that, and was
#     measured against all 21 crates and rejected: it relocates the line
#     attribution problem rather than removing it, and its false negatives land
#     on lines that demonstrably run. `docs/verification.md` carries the table.
#   * `curl` and `cargo-deny` must be on PATH. Both are checked for by name
#     below rather than being allowed to fail as something else.

cd "$(dirname "${BASH_SOURCE[0]}")/.."
RUNS="${1:-3}"
# Validated, because `set -e` does not abort on a failure inside a `for` word
# list. `verify.sh abc` printed seq's error, ran *zero* test iterations and
# then "all gates pass"; `verify.sh 0` did the same with no error text at all,
# since `seq 1 0` succeeds and yields nothing. Both were green. A gate that
# runs no tests must not be able to say it passed.
if ! [[ "${RUNS}" =~ ^[1-9][0-9]*$ ]]; then
    printf 'runs must be a positive integer, got %s\n' "${RUNS}" >&2
    exit 1
fi

: "${PNEUMA_TEST_DATABASE_URL:?set it -- see the header of this script for the exact command}"
: "${PNEUMA_TEST_MONGO_URL:?set it -- see the header of this script for the exact command}"
# The broker `pneuma-transport`'s AMQP tests need. Required rather than skipped for
# the same reason as the two above: the two things those tests check -- that
# RabbitMQ accepts these queue arguments, and that a dropped delivery really
# does come back -- are properties of the broker, and a fake would agree with
# whatever the file asserted.
: "${PNEUMA_TEST_AMQP_URL:?set it -- see the header of this script for the exact command}"
# The broker `pneuma-driver`'s transport tests need. Required for the same
# reason: what a fake could not answer is whether a message published to a
# component's own subject is one a subscriber on that subject receives, and
# whether a publish that only reached a local buffer counts as sent. Both are
# properties of the client and the server, and both were measured wrong first.
: "${PNEUMA_TEST_NATS_URL:?set it -- see the header of this script for the exact command}"

# Tool prerequisites, by name. `if ! curl ...` alone would report a missing
# curl (exit 127) as a missing Restate container and point the reader at a
# docker command that cannot help.
for tool in curl cargo-deny; do
    if ! command -v "${tool}" >/dev/null 2>&1; then
        printf '%s is required and is not on PATH\n' "${tool}" >&2
        exit 1
    fi
done

# Free disk, checked the same way and for the same reason as the databases
# above: this run cannot succeed without it, and finding that out twenty
# minutes in is worse than being told now.
#
# It is not hypothetical. A run that exhausted the disk reported
# `No space left on device`, then `collect2: fatal error: ld terminated with
# signal 7 [Bus error]` -- a linker killed mid-write, which reads as a compiler
# bug rather than as a full disk, and it wasted the twenty minutes it took to
# get there. Twice.
#
# 8 GiB because that is the observed high-water mark of one run plus headroom:
# `cargo test --workspace` alone builds one statically linked binary per test
# target, and the coverage step then rebuilds each crate again under
# tarpaulin's instrumentation. The `[profile.dev]` settings in `Cargo.toml`
# exist to keep that number down; this is what notices when it creeps back up
# or when something else on the machine has taken the room.
# `df -Pk`, not `-BM`: `-B` is a GNU extension that BSD and macOS `df` reject,
# and this check runs before every step -- including the six this script's
# header promises work anywhere. Worse, under `set -e` the assignment would
# have taken the failure and aborted with `df: illegal option -- B`, so the
# "could not read" message below was unreachable in exactly the case it was
# written for. `-P` and `-k` are both POSIX, and the `if !` is what keeps a
# failure here reportable rather than fatal.
REQUIRED_FREE_MIB=8192
if ! FREE_KIB="$(df -P -k . | awk 'NR==2 {print $4}')"; then
    printf 'could not read free disk space for %s\n' "$(pwd)" >&2
    exit 1
fi
if ! [[ "${FREE_KIB}" =~ ^[0-9]+$ ]]; then
    printf 'could not read free disk space for %s (got %s)\n' "$(pwd)" "${FREE_KIB}" >&2
    exit 1
fi
FREE_MIB=$((FREE_KIB / 1024))
if [[ "${FREE_MIB}" -lt "${REQUIRED_FREE_MIB}" ]]; then
    printf 'only %s MiB free on the filesystem holding %s; this run needs about %s MiB\n' \
        "${FREE_MIB}" "$(pwd)" "${REQUIRED_FREE_MIB}" >&2
    printf 'a full disk fails this as a linker crash twenty minutes in, not as a disk error\n' >&2
    printf 'try: cargo clean, here or in whichever checkout is holding the space\n' >&2
    exit 1
fi

# The step currently running, for the failure trap below.
CURRENT_STEP="startup"
step() { CURRENT_STEP="$1"; printf '\n=== %s\n' "$1"; }

# Announce failure as loudly as success. This script already printed
# "=== all gates pass" on the way out and nothing at all on the way down, so a
# failing run looked like a passing one to anybody reading the tail -- which is
# exactly how 42 unqualified citations survived for the whole life of this
# script while it was reported as green. A gate that only speaks when it agrees
# with you is not a gate.
trap 'printf "\n=== GATE FAILED during: %s\n" "$CURRENT_STEP" >&2' ERR

step "formatting"
cargo fmt --all -- --check

step "clippy (warnings are errors)"
cargo clippy --workspace --all-targets --all-features -- -D warnings

step "docs (broken intra-doc links are errors)"
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps

# VERDICT.md §1: Restate replays handlers in a fresh process, so a container
# whose order comes from a per-process hash seed corrupts replay silently. The
# registry was one, and it had been patched at three call sites without anyone
# fixing the container.
step "determinism (no randomly-ordered containers)"
./scripts/forbid-nondeterminism.sh

# `verify.sh` derives its coverage list from `crates/*/`; the workflow
# enumerates jobs by hand. The gap is silent in the direction that matters --
# `pneuma-runner` was added with no CI coverage job at all and every local run
# still said "all gates pass".
step "CI gates every crate"
./scripts/ci-covers-every-crate.sh

# These ran in CI and not here, so `verify.sh` could print "all gates pass"
# while a constraint the workflow enforces went unchecked locally -- the exact
# split `scripts/coverage.sh` exists to prevent ("the command CI runs and the
# command a developer runs are the same command"). The pattern is duplicated
# from ci.yml deliberately: it is the one place both can be read side by side,
# and a drift between them shows up as a local pass and a CI failure rather
# than silence.
step "forbidden dependencies"
IO_FREE='tokio|async-std|smol|async-global-executor|futures-executor|async-nats|nats|amqp|lapin|mongodb|redis|reqwest|hyper'
./scripts/forbid-deps.sh pneuma-core "${IO_FREE}"
./scripts/forbid-deps.sh pneuma-nats "${IO_FREE}"
./scripts/forbid-deps.sh pneuma-amqp "${IO_FREE}"
./scripts/forbid-deps.sh pneuma-gateway-client "${IO_FREE}|axum"
./scripts/forbid-deps.sh pneuma-interpreter "${IO_FREE}|restate"
# Guarded from the moment it acquired a consumer, not before. `pneuma-fairness`
# was complete, pure and property-tested with nothing calling it -- and a pure
# crate nobody depends on stays pure by accident. Now that `pneuma-admission`
# uses it, the pressure to reach for a database "just to look up a weight" is
# real, and the whole claim of `STRATEGY.md` is that fair dispatch is decidable
# without one. `axum` and `restate` as well as the I/O set: the engine this runs
# under must not be able to leak into the thing that is meant to outlive it.
./scripts/forbid-deps.sh pneuma-fairness "${IO_FREE}|axum|restate"
# The driver's whole design is that it owns no transport: `Component` is a
# trait so the same loop runs under Restate and under a plain client, and so
# every branch of it is reachable from a fake. That is only true while this
# holds.
./scripts/forbid-deps.sh pneuma-runner "${IO_FREE}|restate|axum"

# What `pneuma-driver` may not *write*, as opposed to what it may not link.
# It holds a run's `Execution` in memory, which is lost on a crash, and the
# obvious cure -- write the barrier state down so a restart can resume --
# reinstates the design the port removed: refcounts, prerequisite completion,
# expected-children counts, and the `CONCURRENCY-AND-DIRECTION.md` §1.5 race
# that comes with them. That temptation arrives months from now, in a hurry,
# from somebody who has not read the crate docs.
./scripts/forbid-symbols.sh pneuma-driver \
    'BarrierStore|complete_prerequisite|expected_children|skip_child|refcount'


# Repeated, because a suite that passes once is not a suite that passes. The
# scheduler's test helper read HashMap iteration order and was green most of the
# time; one run would not have found it.
# Seconds each, and they were below a three-times test sweep and a fourteen
# crate tarpaulin run -- so a licence violation was learned twenty minutes in,
# which is the same slow way the Restate probe below exists to avoid.
step "dependency licences and advisories"
cargo deny check

step "doctests"
cargo test --workspace --doc

# Reaching it is the check; there is no env var to validate, because the port
# is a constant in the test files. Placed here rather than at the top so that
# everything portable has already run: a macOS developer gets six green steps
# before the parts that need Linux and a container.
RESTATE_ADMIN="${PNEUMA_TEST_RESTATE_ADMIN:-http://localhost:19070}"
if ! curl -sf -o /dev/null --max-time 5 "${RESTATE_ADMIN}/health"; then
    printf 'the Restate admin API is not answering at %s\n' "${RESTATE_ADMIN}" >&2
    printf 'see the header of this script for the exact docker command\n' >&2
    exit 1
fi

step "tests, ${RUNS} consecutive runs"
for run in $(seq 1 "${RUNS}"); do
    printf '  run %s of %s\n' "${run}" "${RUNS}"
    cargo test --workspace --quiet
done

# Derived from the directory, so a new crate is covered without anyone
# remembering to add it here -- the same reason ALL_RUN_STATUSES is checked for
# exhaustiveness rather than trusted.
step "coverage, 100% per crate"
for dir in crates/*/; do
    crate="$(basename "${dir}")"
    printf '  %s\n' "${crate}"
    ./scripts/coverage.sh "${crate}" --out stdout >/dev/null
done

printf '\n=== all gates pass\n'
