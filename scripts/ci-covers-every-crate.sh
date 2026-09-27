#!/usr/bin/env bash
#
# Fail if a crate has no coverage job in the CI workflow.
#
# `scripts/verify.sh` derives its coverage list from `crates/*/`, so a new crate
# is gated locally the moment it exists. The workflow enumerates jobs by hand,
# so it is not -- and the difference is silent in the direction that matters:
# the local gate passes, the pull request is green, and the crate is only ever
# type-checked by the workspace-wide clippy.
#
# That is not hypothetical. `pneuma-runner` was added with a `forbid-deps` step
# appended to the *interpreter's* job and no coverage job of its own. Every
# local run said "all gates pass".
#
# Regex rather than a YAML parser on purpose: the citation checker and the rest
# of this directory run on a stock python3, and adding PyYAML to the gate to
# check the gate is the wrong trade. The pattern is `coverage.sh <crate>`, which
# is how the workflow invokes it and the only way it can.
#
set -Eeuo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

WORKFLOW=".github/workflows/ci.yml"
[ -f "${WORKFLOW}" ] || { echo "error: ${WORKFLOW} not found" >&2; exit 1; }

covered="$(grep -oE 'coverage\.sh[[:space:]]+[A-Za-z0-9_-]+' "${WORKFLOW}" \
    | awk '{print $2}' | sort -u)"
[ -n "${covered}" ] || {
    echo "error: no 'coverage.sh <crate>' invocations found in ${WORKFLOW}." >&2
    echo "       Either the workflow stopped gating coverage, or this check's" >&2
    echo "       pattern no longer matches how it is invoked. Both are bugs." >&2
    exit 1
}

# `sed`, not `-printf`: the latter is a GNU extension and BSD/macOS `find`
# errors on it. Under `set -Eeuo pipefail` in `verify.sh` that aborts the gate
# before tests or coverage run -- a check for the gate breaking the gate.
# `forbid-nondeterminism.sh` stays portable for the same reason.
present="$(find crates -mindepth 1 -maxdepth 1 -type d | sed 's#.*/##' | sort)"

missing="$(comm -23 <(printf '%s\n' "${present}") <(printf '%s\n' "${covered}"))"
if [ -n "${missing}" ]; then
    echo "error: these crates have no coverage job in ${WORKFLOW}:" >&2
    printf '  %s\n' ${missing} >&2
    echo "       Add one, or CI will merge a change that breaks them." >&2
    exit 1
fi

ghost="$(comm -13 <(printf '%s\n' "${present}") <(printf '%s\n' "${covered}"))"
if [ -n "${ghost}" ]; then
    echo "error: ${WORKFLOW} gates crates that do not exist:" >&2
    printf '  %s\n' ${ghost} >&2
    echo "       A job naming a missing crate fails loudly, but a *renamed*" >&2
    echo "       crate leaves the new name ungated, which does not." >&2
    exit 1
fi

printf 'every crate has a CI coverage job (%s)\n' "$(printf '%s\n' "${present}" | wc -l)"
