#!/usr/bin/env bash
#
# Fail if production code uses a randomly-ordered container.
#
# VERDICT.md §1: adopting Restate relocates the determinism constraint rather
# than removing it. Handlers replay against a journal in a *fresh process*, so
# anything whose order feeds the sequence of journalled calls must not depend on
# a per-process hash seed. §1 asks for "a deterministic map and a written rule".
# This is the rule, checked rather than remembered.
#
# Not hypothetical. `StepRegistry::iter` was `HashMap::values` and
# `Execution::run_output` walked it, so a run's outputs came back in an order
# that differed on every process -- measured, four orders in four runs. It had
# already been patched at three separate call sites without anyone fixing the
# container.
#
# `HashMap` is not banned for being wrong; it is banned because choosing one
# deserves a sentence. Where iteration order genuinely cannot escape, say so on
# the line and the check passes:
#
#     let seen: HashSet<_> = ...; // determinism-ok: membership only, never iterated
#
# How test code is excluded, and why it is not a guess. A `#[cfg(test)]` block
# is entered only when the attribute is at column zero **and the next line is a
# braced `mod NAME {`**, and left at the next line that is exactly `}` in column
# zero. `cargo fmt --check` is part of the same gate, so rustfmt guarantees a
# top-level item closes there -- the rule holds because another gate enforces
# it, not because the code happens to look that way today.
#
# The `mod NAME {` requirement is not belt-and-braces. Without it the attribute
# alone armed the skip, so `#[cfg(test)] mod testenv;` -- which has no closing
# brace -- skipped from there to EOF, and an indented `#[cfg(test)]` inside an
# `impl` skipped to that impl's closing brace, hiding every method after it.
# Neither shape announced itself. Anything that is not a braced module is now
# scanned like ordinary code rather than trusted.
#
# Scanning *resumes* after the block, which is the part that matters:
# `pneuma-store/src/refpath.rs` declares `pub struct StepId` and its whole impl
# *after* its test module. A checker that simply stopped at the first
# `#[cfg(test)]` -- the obvious shortcut, and the one written first here --
# would have silently skipped all of it.
#
set -Eeuo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

files=$(find crates -path '*/src/*' -name '*.rs' | sort)
[ -n "${files}" ] || { echo "error: no source files found; wrong directory?" >&2; exit 1; }

# shellcheck disable=SC2086
offenders=$(awk '
    FNR == 1 { in_test = 0; pending = 0 }

    # A `#[cfg(test)]` at column zero *may* open a test module -- but only if a
    # braced `mod` follows. An earlier version entered test scope on the
    # attribute alone and left it at the next `}` in column zero, which is
    # unsound for every non-braced item: `#[cfg(test)] mod testenv;` (a real
    # instance, crates/pneuma-janitor/src/lib.rs) has no closing brace at all,
    # so the checker skipped from there to end of file. Silently. In a script
    # whose entire purpose is refusing to skip things silently.
    #
    # So the attribute only arms; the *next* line decides.
    /^#\[cfg\(test\)\]/ { pending = 1; next }

    pending {
        pending = 0
        if ($0 ~ /^(pub[[:space:]]+)?mod[[:space:]]+[A-Za-z0-9_]+[[:space:]]*\{/) {
            in_test = 1
            next
        }
        # Anything else: an indented attribute, a non-braced `mod foo;`, a
        # `use`, a `const`, a bare `fn`. Not a scope we can bound, so it is not
        # skipped -- it is scanned like any other line, and falls through to
        # the checks below.
    }

    in_test && /^\}/ { in_test = 0; next }
    in_test { next }
    {
        stripped = $0
        sub(/^[[:space:]]+/, "", stripped)
        if (stripped ~ /^(\/\/|\/\*|\*)/) next      # a comment about them is fine
        if ($0 ~ /determinism-ok/) next                # explicitly justified
        if ($0 ~ /(HashMap|HashSet)/) printf "%s:%d: %s\n", FILENAME, FNR, stripped
    }
' ${files})

if [ -n "${offenders}" ]; then
    printf 'error: randomly-ordered container in production code.\n' >&2
    printf '       Use BTreeMap/BTreeSet, or annotate the line with\n' >&2
    printf '       `// determinism-ok: <why the order cannot escape>`.\n' >&2
    printf '       See VERDICT.md §1 and the header of this script.\n\n' >&2
    printf '%s\n' "${offenders}" >&2
    exit 1
fi

printf 'no randomly-ordered containers in production code\n'
