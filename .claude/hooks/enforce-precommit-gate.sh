#!/usr/bin/env bash
# enforce-precommit-gate.sh — pre-commit guard for the Phase 3 ticket-hygiene
# contract. Blocks any commit whose staged diff contains a placeholder shape
# OR a TODO/FIXME citing a Linear ID that is NOT in `.linear-allowlist`.
#
# Placeholder shapes blocked (per Phase 3 plan §C):
#   - <linear-id>
#   - <issue-id>
#   - APP-NNN
#   - APP-???-[A-Z]
#   - bare "Issue [A-Z]\b"
#
# Linear ID validation:
#   - Every `TODO(APP-<digits>)` or `FIXME(APP-<digits>)` cited in a staged
#     diff must appear as a verbatim line in `.linear-allowlist`.
#   - Comment lines (`#`) and blank lines in the allowlist are ignored.
#
# Self-test: `.claude/hooks/test/enforce-precommit-gate.test.sh` pipes
# fixture diffs through this script and asserts the expected exit codes.
#
# Exit codes:
#   0  — diff is clean
#   1  — placeholder shape found
#   2  — TODO/FIXME cites a Linear ID not in the allowlist
#   3  — `.linear-allowlist` is missing or unreadable

set -Eeuo pipefail

REPO_ROOT="$(git rev-parse --show-toplevel)"
ALLOWLIST="$REPO_ROOT/.linear-allowlist"

if [[ ! -r "$ALLOWLIST" ]]; then
  echo "enforce-precommit-gate: missing or unreadable .linear-allowlist at $ALLOWLIST" >&2
  exit 3
fi

# Build the set of allowed IDs (strip comments + blanks).
ALLOWED_IDS="$(grep -v '^\s*#' "$ALLOWLIST" | grep -v '^\s*$' | tr -d ' \t' | sort -u)"

# Capture the staged diff (added lines only — leading `+`, excluding `+++` headers).
# If invoked without git context (self-test), accept the diff on stdin.
if [[ -t 0 ]]; then
  DIFF="$(git diff --cached -U0 --no-color | grep -E '^\+' | grep -vE '^\+\+\+')"
else
  DIFF="$(cat | grep -E '^\+' | grep -vE '^\+\+\+' || true)"
fi

if [[ -z "$DIFF" ]]; then
  exit 0
fi

# Placeholder-shape scan.
PLACEHOLDER_RE='(<linear-id>|<issue-id>|APP-NNN|APP-\?\?\?-[A-Z]|\bIssue [A-Z]\b)'
if echo "$DIFF" | grep -E "$PLACEHOLDER_RE" >/dev/null; then
  echo "enforce-precommit-gate: BLOCKED — placeholder shape in staged diff:" >&2
  echo "$DIFF" | grep -nE "$PLACEHOLDER_RE" >&2
  exit 1
fi

# Linear-ID scan: every `TODO(APP-<digits>)` or `FIXME(APP-<digits>)` must
# cite an allowlisted ID.
CITED_IDS="$(echo "$DIFF" | grep -oE '(TODO|FIXME)\(APP-[0-9]+\)' | grep -oE 'APP-[0-9]+' | sort -u || true)"

if [[ -n "$CITED_IDS" ]]; then
  while IFS= read -r id; do
    if ! echo "$ALLOWED_IDS" | grep -Fxq "$id"; then
      echo "enforce-precommit-gate: BLOCKED — TODO/FIXME cites $id which is NOT in .linear-allowlist." >&2
      echo "  Allowed IDs: $(echo "$ALLOWED_IDS" | tr '\n' ' ')" >&2
      echo "  Did you cite a Done ticket, or a stale handoff-doc ID? See Phase 3 plan §C." >&2
      exit 2
    fi
  done <<<"$CITED_IDS"
fi

exit 0
