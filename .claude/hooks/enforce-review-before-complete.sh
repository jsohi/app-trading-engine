#!/usr/bin/env bash
# PreToolUse hook for TaskUpdate. Blocks task completion unless:
# /orchestrate was run for this phase (marker: /tmp/claude_gates/orchestrate_done)
#
# /orchestrate internally runs: /review (2 agents) → fix → all 3 test suites →
# spotlessCheck → commit → push → Gemini poll → fix → loop until convergence.
# A single marker replaces the 5 individual gates because orchestrate covers all of them.
#
# Uses /tmp/claude_gates/ (NOT $TMPDIR) because Claude Code's hook runner
# uses macOS system TMPDIR while Bash tool uses sandbox TMPDIR.
#
# Markers are cleared on successful completion so each phase must re-earn them.
#
# Stdin: Claude Code hook JSON
# Stdout: empty (allow) or hookSpecificOutput JSON (deny)
set -euo pipefail

GATE_DIR="/tmp/claude_gates"

input=$(cat)
status=$(printf '%s' "$input" | jq -r '.tool_input.status // ""')

# Only gate on "completed" status — allow in_progress, pending, deleted, etc.
if [ "$status" != "completed" ]; then
  exit 0
fi

if [ ! -f "${GATE_DIR}/orchestrate_done" ]; then
  jq -nc '{
    hookSpecificOutput: {
      hookEventName: "PreToolUse",
      permissionDecision: "deny",
      permissionDecisionReason: "BLOCKED: Cannot mark task as completed. /orchestrate has not been run for this phase.\n\nYou MUST run the full orchestrate loop before completing any task:\n  1. Invoke /orchestrate via the Skill tool\n  2. It will run: /review → fix → 3 test suites → spotless → commit → push → Gemini poll → fix → loop\n  3. After convergence, run: mkdir -p /tmp/claude_gates && touch /tmp/claude_gates/orchestrate_done\n  4. Then retry TaskUpdate(completed)\n\nDo NOT skip /orchestrate. Do NOT mark complete without it."
    }
  }'
  exit 0
fi

# Gate passed — clear marker for next phase
rm -f "${GATE_DIR}/orchestrate_done"
exit 0
