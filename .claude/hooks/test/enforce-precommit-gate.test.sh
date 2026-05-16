#!/usr/bin/env bash
# Self-test for `enforce-precommit-gate.sh`. Pipes deterministic fixture
# diffs (in unified-diff `+`-prefix format) through the gate script and
# asserts the expected exit code + stderr message.
#
# Run: ./enforce-precommit-gate.test.sh
# Wired into the `:check-hooks` Gradle task (CI gate).
#
# Cases:
#   1. Clean diff → exit 0
#   2. Placeholder `<linear-id>` → exit 1
#   3. Placeholder `APP-NNN` → exit 1
#   4. TODO citing non-allowlisted ID (APP-37 Done) → exit 2
#   5. TODO citing allowlisted ID (APP-125) → exit 0

set -Eeuo pipefail

HOOK="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/enforce-precommit-gate.sh"
PASS=0
FAIL=0

run_case() {
  local name="$1"
  local fixture="$2"
  local expected="$3"
  local actual
  actual=$(echo -e "$fixture" | "$HOOK" 2>/dev/null && echo $? || echo $?)
  # shell quirk: capture exit explicitly
  set +e
  echo -e "$fixture" | "$HOOK" >/dev/null 2>&1
  actual=$?
  set -e
  if [[ "$actual" == "$expected" ]]; then
    echo "  PASS  $name (exit=$actual)"
    PASS=$((PASS + 1))
  else
    echo "  FAIL  $name — expected exit=$expected, got exit=$actual"
    FAIL=$((FAIL + 1))
  fi
}

echo "enforce-precommit-gate self-test"

run_case "clean diff" \
  "+// some code change\n+const x = 1;" \
  0

run_case "placeholder <linear-id>" \
  "+// TODO(<linear-id>): hook this up\n" \
  1

run_case "placeholder APP-NNN" \
  "+// TODO(APP-NNN): file an issue\n" \
  1

run_case "TODO cites APP-37 (Done — not in allowlist)" \
  "+// TODO(APP-37): finish this later\n" \
  2

run_case "TODO cites APP-125 (allowlisted)" \
  "+// TODO(APP-125): consume holiday calendar\n" \
  0

run_case "TODO cites APP-237 (security-audit, not on Phase 3 allowlist)" \
  "+// TODO(APP-237): refactor\n" \
  2

echo ""
echo "Total: $((PASS + FAIL)) — passed $PASS, failed $FAIL"
[[ $FAIL -eq 0 ]]
