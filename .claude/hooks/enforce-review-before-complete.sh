#!/usr/bin/env bash
# PreToolUse hook for TaskUpdate. Blocks task completion unless LOCAL review loop passed.
# Orchestrate (Gemini) runs in parallel and does NOT block — it's advisory.
#
# LOCAL LOOP (BLOCKING — must pass before task completion):
# 1. /review on ALL branch changes main...HEAD (review_done)
# 2. All 3 test suites passed (test_done, integration_done, e2e_done)
# 3. spotlessCheck passed (spotless_done)
#
# GEMINI LOOP (NON-BLOCKING — runs in parallel, Gemini findings fixed inline):
# - /orchestrate commits + pushes + polls Gemini
# - Gemini findings are fixed in the current or next phase's work
# - Does NOT need to complete before marking a task done
#
# Uses /tmp/claude_gates/ (NOT $TMPDIR) — hook runner uses macOS system TMPDIR,
# Bash tool uses sandbox TMPDIR. Fixed path ensures both can read/write.
#
# Markers are cleared on successful completion so each phase must re-earn them.
set -euo pipefail

GATE_DIR="/tmp/claude_gates"

input=$(cat)
status=$(printf '%s' "$input" | jq -r '.tool_input.status // ""')

# Only gate on "completed" status — allow in_progress, pending, deleted, etc.
if [ "$status" != "completed" ]; then
  exit 0
fi

missing=""

# Local loop markers (BLOCKING)
[ ! -f "${GATE_DIR}/review_done" ] && missing="${missing}\n  - /review not run (2 agents on ALL main...HEAD changes)"
[ ! -f "${GATE_DIR}/test_done" ] && missing="${missing}\n  - ./gradlew test not run"
[ ! -f "${GATE_DIR}/integration_done" ] && missing="${missing}\n  - ./gradlew :integration-tests:test not run"
[ ! -f "${GATE_DIR}/e2e_done" ] && missing="${missing}\n  - ./gradlew e2e not run"
[ ! -f "${GATE_DIR}/spotless_done" ] && missing="${missing}\n  - ./gradlew spotlessCheck not run"

# NOTE: orchestrate_done is NOT checked here — Gemini loop is non-blocking.
# It runs in parallel and findings are fixed inline in the current/next phase.

if [ -n "$missing" ]; then
  jq -nc --arg msg "BLOCKED: Cannot mark task as completed. Local review loop not done:${missing}

Run these first (Gemini/orchestrate runs in parallel, does NOT block):
  mkdir -p /tmp/claude_gates
  /review (ALL changes)    → touch /tmp/claude_gates/review_done
  ./gradlew test           → touch /tmp/claude_gates/test_done
  ./gradlew :integration-tests:test → touch /tmp/claude_gates/integration_done
  ./gradlew e2e            → touch /tmp/claude_gates/e2e_done
  ./gradlew spotlessCheck  → touch /tmp/claude_gates/spotless_done" '{
    hookSpecificOutput: {
      hookEventName: "PreToolUse",
      permissionDecision: "deny",
      permissionDecisionReason: $msg
    }
  }'
  exit 0
fi

# All local gates passed — clear markers for next phase
rm -f "${GATE_DIR}/review_done" "${GATE_DIR}/test_done" \
      "${GATE_DIR}/integration_done" "${GATE_DIR}/e2e_done" \
      "${GATE_DIR}/spotless_done"
exit 0
