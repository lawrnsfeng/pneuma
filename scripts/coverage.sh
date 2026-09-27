#!/usr/bin/env bash
#
# Run the 100% line-coverage gate for one crate.
#
# This exists so the command CI runs and the command a developer runs are the
# same command. They drifted once already: the Restate spike was added, and
# because the `pneuma-core` job spelled its exclusions out by hand, the spike's
# sources were counted as uncovered and the gate silently fell to ~90% while
# still being described as a 100% gate. Anything that must stay in sync across
# jobs belongs here rather than in the workflow file.
#
# Usage:  scripts/coverage.sh <crate-name> [extra cargo-tarpaulin args...]
#
set -euo pipefail

CRATE="${1:?usage: scripts/coverage.sh <crate-name> [extra tarpaulin args...]}"
shift

cd "$(dirname "${BASH_SOURCE[0]}")/.."

# cargo-tarpaulin discovers source files by walking the repository root, not by
# following the dependency graph of the package under test. So every sibling
# crate and the out-of-workspace spike get counted as uncovered lines unless
# they are excluded explicitly. Deriving the list means adding a crate cannot
# quietly break another crate's gate.
EXCLUDES=(
  # Module declarations and crate docs, no logic.
  --exclude-files "*/${CRATE}/src/lib.rs"
  # The integration harness itself. Its `panic!` arms are failure messages that
  # by definition cannot run while the suite passes; measuring them would be
  # measuring the ruler. Inline `#[cfg(test)]` modules inside src/ ARE still
  # measured, and are held to 100%.
  --exclude-files "*/${CRATE}/tests/*"
  # A binary's `main` is never executed by `cargo test`, so every line in it
  # reports uncovered and a crate with a binary can never reach 100%. Measured:
  # a five-line probe `main.rs` took `pneuma-restate` from 100% to 93.24%.
  # Excluded like `lib.rs`, and guarded like it too -- see below.
  --exclude-files "*/${CRATE}/src/main.rs"
  # The throwaway Restate spike. Deliberately outside the workspace (its own
  # [workspace] table), never built or linted here, and explicitly not held to
  # production standards — see spikes/restate/VERDICT.md.
  --exclude-files '*spikes*'
)

# Sibling crates. Each has its own gate in its own job; counting them here would
# both double-count and measure lines unreachable from this crate's tests.
for dir in crates/*/; do
  name="$(basename "${dir}")"
  [ "${name}" = "${CRATE}" ] && continue
  EXCLUDES+=(--exclude-files "*/${name}/*")
done

# The `src/lib.rs` exclusion above is only honest while that file really is
# declarations-only. If logic lives there it is silently unmeasured -- and the
# gate still reports 100%, because the other modules collect fine. (A crate with
# ALL its code in lib.rs fails loudly with "No coverage results collected", which
# is how this was noticed; the dangerous case is the mixed one, which does not.)
# Same bargain as `lib.rs`, and the same danger: excluding a file is only honest
# while nothing can hide in it.
#
# The rule is the same one `lib.rs` gets -- no definitions -- rather than "no
# control flow". Forbidding an `if` or a `match` here sounds stricter and is
# worse: startup wiring genuinely has to branch on whether the configuration
# parsed, and a rule that bans that pushes the branch into a *measured* file
# where it cannot be tested either, because it ends in `exit` or in a server
# that never returns. Definitions are the thing that must not accumulate.
#
# The line cap is the second half. A `main` that only wires cannot grow past a
# screen; one that has grown past a screen is doing something that belongs in
# the library.
MAIN="crates/${CRATE}/src/main.rs"
if [ -f "${MAIN}" ]; then
  # `grep -n` over a *filtered* stream numbers the filtered stream, so a guard
  # that stripped comments first reported line numbers nobody could look up.
  # Comments are blanked in place instead: the numbering stays the file's.
  BODY="$(sed -E 's#^[[:space:]]*(//|/\*|\*).*$##' "${MAIN}")"

  # `mod`, `macro_rules!` and `type` are here because each is a way to put
  # unmeasured substance in an excluded file. The `fn` filter matches on the
  # *definition*, not the line: filtering lines containing `fn main(` anywhere
  # meant a helper with a trailing `// called from fn main()` comment was
  # invisible to the guard.
  DEF_RE='^[[:space:]]*(pub[[:space:]]+)?(async[[:space:]]+)?(struct|enum|trait|impl|const|static|type|mod)[[:space:]]|^[[:space:]]*macro_rules![[:space:]]*'
  FN_RE='^[[:space:]]*(pub[[:space:]]+)?(async[[:space:]]+)?fn[[:space:]]+'
  MAIN_RE='^[[:space:]]*(pub[[:space:]]+)?(async[[:space:]]+)?fn[[:space:]]+main[[:space:]]*\('

  OTHER_FN="$(printf '%s\n' "${BODY}" | grep -nE "${FN_RE}" | grep -vE "^[0-9]+:${MAIN_RE#^}" || true)"
  DEFS="$(printf '%s\n' "${BODY}" | grep -nE "${DEF_RE}" || true)"
  if [ -n "${OTHER_FN}" ] || [ -n "${DEFS}" ]; then
    echo "error: ${MAIN} contains definitions, but the coverage gate excludes it." >&2
    echo "       A binary's \`main\` is never run by the test suite, so anything" >&2
    echo "       defined here is unmeasured. Move it into the library and leave" >&2
    echo "       \`main\` to wiring." >&2
    printf '%s\n%s\n' "${OTHER_FN}" "${DEFS}" | grep -v '^$' >&2
    exit 1
  fi
  LINES="$(wc -l < "${MAIN}")"
  if [ "${LINES}" -gt 40 ]; then
    echo "error: ${MAIN} is ${LINES} lines, and the gate excludes it." >&2
    echo "       A \`main\` that only wires does not reach 40 lines; one that" >&2
    echo "       does is doing work that belongs in the library." >&2
    exit 1
  fi
fi

LIB="crates/${CRATE}/src/lib.rs"
if [ -f "${LIB}" ]; then
  # `mod NAME {` -- a braced, inline module -- is a definition; `mod name;`, a
  # declaration pointing at another file that IS measured, is the whole point of
  # lib.rs. So the two are distinguished rather than both banned.
  LIB_RE='^[[:space:]]*(pub[[:space:]]+)?(async[[:space:]]+)?(fn|struct|enum|trait|impl|const|static|type)[[:space:]]|^[[:space:]]*macro_rules![[:space:]]*|^[[:space:]]*(pub[[:space:]]+)?mod[[:space:]]+[A-Za-z0-9_]+[[:space:]]*\{'
  # `if grep; then` treats grep's error exit 2 as "no match" -- the same
  # masking fixed in forbid-deps.sh. Capture the status instead.
  set +e
  LIB_DEFS="$(grep -nE "${LIB_RE}" "${LIB}")"
  LIB_STATUS=$?
  set -e
  if [ "${LIB_STATUS}" -gt 1 ]; then
    echo "error: could not scan ${LIB} (grep status ${LIB_STATUS})." >&2
    exit 2
  fi
  if [ -n "${LIB_DEFS}" ]; then
    echo "error: ${LIB} contains definitions, but the coverage gate excludes it." >&2
    echo "       Move them into a module so they are measured; keep lib.rs to" >&2
    echo "       crate docs, lint attributes, \`pub mod\`, and \`pub use\`." >&2
    printf '%s\n' "${LIB_DEFS}" >&2
    exit 1
  fi
fi

echo "coverage gate: ${CRATE} (100% required)"
exec cargo tarpaulin -p "${CRATE}" "${EXCLUDES[@]}" --fail-under 100 "$@"
