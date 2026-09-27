#!/usr/bin/env bash
#
# Fail if a crate's dependency tree contains anything matching a forbidden
# pattern. Used to turn "this crate is deliberately I/O-free" from a claim in a
# doc comment into something CI enforces.
#
# Usage:  scripts/forbid-deps.sh <crate-name> <extended-regex>
#
# Two things this gets right that the inline `cargo tree | grep ... || true`
# it replaces did not:
#
#   1. A `cargo tree` failure is not a pass. GitHub Actions runs `run:` blocks
#      as `bash -e {0}` without `pipefail`, so in `cargo tree | grep -E ... &&
#      exit 1 || true` the pipeline's status is grep's. If `cargo tree` errors
#      -- crate renamed, moved out of the workspace, lockfile or registry
#      failure, a `--edges` flag that changed meaning -- grep reads empty
#      input, exits 1, `|| true` swallows it, and the step reports success with
#      the constraint entirely unenforced and no signal that anything happened.
#      Here cargo runs on its own line, so a failure fails the job.
#
#   2. The root line is dropped, so a bare pattern is safe. `cargo tree` prints
#      the crate under test as its first line, so `pneuma-nats` matches a `nats`
#      pattern and the guard fails every run. The previous fix for that was to
#      narrow the pattern to `async-nats|tokio` -- which made the guard pass,
#      and also made it miss the synchronous `nats` client and every non-tokio
#      runtime, so `nats = "0.25"` could be added with CI staying green while
#      the crate doc still claimed to be client-free. Stripping the root line
#      instead means the pattern can stay broad.
#   3. It checks a package's OWN resolution, not the whole-workspace build.
#      `cargo tree -p X` resolves features for X alone. Cargo unifies features
#      across a workspace build, so a sibling crate enabling `runtime-tokio` on
#      a shared dependency does change what X links there -- verifiable with
#      `cargo tree -i tokio` from the root. That is a real limit: this guard
#      proves the crate's dependency contract and that it builds and tests
#      standalone without the forbidden crates, not that no artifact anywhere
#      contains them.
set -euo pipefail

CRATE="${1:?usage: scripts/forbid-deps.sh <crate-name> <extended-regex>}"
PATTERN="${2:?usage: scripts/forbid-deps.sh <crate-name> <extended-regex>}"

cd "$(dirname "${BASH_SOURCE[0]}")/.."

# `--edges normal` excludes dev- and build-dependencies: test-only use of a
# runtime is fine and expected, it is the shipped artifact that must stay pure.
TREE="$(cargo tree -p "${CRATE}" --edges normal)"

# Only the crate name, not the whole line. `cargo tree` prints a workspace
# member as `pneuma-core v0.1.0 (/home/you/relic/pneuma/crates/pneuma-core)`,
# so matching the line means a checkout path is part of the haystack: a clone
# into `~/hyperion/pneuma` fails the `hyper` guard on every crate. Field 2 of
# the de-drawn line is the version, field 1 the name; taking the name alone is
# both the thing the pattern is about and immune to where the repo lives.
NAMES="$(printf '%s\n' "${TREE}" \
  | tail -n +2 \
  | sed -E 's/^[^A-Za-z0-9_-]*//; s/[[:space:]].*$//' \
  | grep -v '^$' || true)"

# grep exits 0 on a match, 1 on none, and **2 on an error** -- a malformed
# pattern, an unreadable input. `if grep ...; then` collapses 1 and 2 into "no
# match", so a typo in the caller's regex printed `ok:` and exited 0 with the
# constraint entirely unenforced. This script's own header claims to have fixed
# that class of exit-code masking; it fixed the `|| true` half and left this
# one. Reproduced before the fix:
#
#     $ scripts/forbid-deps.sh pneuma-core 'tokio|('
#     grep: Unmatched ( or \(
#     ok: pneuma-core pulls in nothing matching /tokio|(/
#
# So the status is captured and each of the three outcomes handled by name.
set +e
MATCHES="$(printf '%s\n' "${NAMES}" | grep -Ei -- "${PATTERN}")"
STATUS=$?
set -e

case "${STATUS}" in
  0)
    echo "error: ${CRATE} depends on something it is documented not to." >&2
    echo "       forbidden pattern: ${PATTERN}" >&2
    printf '%s\n' "${MATCHES}" >&2
    exit 1
    ;;
  1)
    echo "ok: ${CRATE} pulls in nothing matching /${PATTERN}/"
    ;;
  *)
    echo "error: grep failed (status ${STATUS}) checking ${CRATE}." >&2
    echo "       The pattern is probably malformed: ${PATTERN}" >&2
    echo "       This is a broken guard, not a passing one." >&2
    exit 2
    ;;
esac
