#!/usr/bin/env bash
#
# Fail if a crate's own sources mention any of a set of forbidden symbols.
# The sibling of forbid-deps.sh: that one guards what a crate may *link*, this
# one guards what it may *write*.
#
# Usage:  scripts/forbid-symbols.sh <crate-name> <extended-regex>
#
# # Why a grep guard rather than a code review
#
# `pneuma-driver` is the one crate that will be tempted to grow a barrier
# table. It holds a run's `Execution` in memory, which is lost on a crash, and
# the obvious cure -- write the barrier state down so a restart can resume --
# reinstates exactly the design the port removed: refcounts, prerequisite
# completion, expected-children counts, and the `CONCURRENCY-AND-DIRECTION.md`
# §1.5 race that comes with them. The fan-in is a local counter inside
# `pneuma_interpreter::Execution` precisely so that race is unrepresentable.
#
# That temptation arrives months from now, in a hurry, from somebody who did
# not read this file. A guard that fails the build is the only kind of note
# that gets read at that moment.
#
# # What it looks at
#
# `crates/<crate>/src/**.rs` only -- not tests, which legitimately name the
# thing they are asserting does not happen, and not comments, which is how the
# constraint gets *explained*. Comments are blanked in place rather than
# stripped, so reported line numbers are the file's own; the same technique
# `coverage.sh` uses on `main.rs` and for the same reason.
set -euo pipefail

CRATE="${1:?usage: scripts/forbid-symbols.sh <crate-name> <extended-regex>}"
PATTERN="${2:?usage: scripts/forbid-symbols.sh <crate-name> <extended-regex>}"

cd "$(dirname "${BASH_SOURCE[0]}")/.."

SRC="crates/${CRATE}/src"
if [ ! -d "${SRC}" ]; then
    echo "error: ${SRC} does not exist; ${CRATE} is not a crate here." >&2
    echo "       A guard that silently checks nothing is worse than no guard." >&2
    exit 2
fi

# Every source file, comments blanked. `sed` handles the three comment forms
# this repository uses -- `//`, `///` and `//!` all start with `//`, and a
# block comment's continuation lines start with `*`.
FOUND=""
while IFS= read -r file; do
    BODY="$(sed -E 's#^[[:space:]]*(//|/\*|\*).*$##' "${file}")"
    # The same exit-code discipline as forbid-deps.sh: grep's 2 is an error,
    # not "no match", and collapsing them is how a typo in the caller's
    # pattern turns a guard into a no-op that reports success.
    set +e
    HITS="$(printf '%s\n' "${BODY}" | grep -nE -- "${PATTERN}")"
    STATUS=$?
    set -e
    case "${STATUS}" in
        0) FOUND="${FOUND}$(printf '%s\n' "${HITS}" | sed "s#^#${file}:#")"$'\n' ;;
        1) ;;
        *)
            echo "error: grep failed (status ${STATUS}) reading ${file}." >&2
            echo "       The pattern is probably malformed: ${PATTERN}" >&2
            echo "       This is a broken guard, not a passing one." >&2
            exit 2
            ;;
    esac
done < <(find "${SRC}" -name '*.rs' -type f | sort)

if [ -n "${FOUND}" ]; then
    echo "error: ${CRATE} names a symbol it is documented never to use." >&2
    echo "       forbidden pattern: ${PATTERN}" >&2
    printf '%s' "${FOUND}" >&2
    echo "       These are the barrier design the port removed. The fan-in is" >&2
    echo "       a local counter inside pneuma_interpreter::Execution so that" >&2
    echo "       CONCURRENCY-AND-DIRECTION.md §1.5's race cannot be written." >&2
    exit 1
fi

echo "ok: ${CRATE} names nothing matching /${PATTERN}/"
