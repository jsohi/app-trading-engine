#!/usr/bin/env bash
# PreToolUse hook for ExitPlanMode. Blocks plan exit unless:
# 1. Plan review agents ran with zero findings (marker: /tmp/claude_gates/plan_review_done)
#
# Uses /tmp/claude_gates/ (NOT $TMPDIR) because Claude Code's hook runner
# uses macOS system TMPDIR while Bash tool uses sandbox TMPDIR.
#
# Stdin: Claude Code hook JSON
# Stdout: empty (allow) or hookSpecificOutput JSON (deny)
set -euo pipefail

marker="/tmp/claude_gates/plan_review_done"

if [ ! -f "$marker" ]; then
  jq -nc '{
    hookSpecificOutput: {
      hookEventName: "PreToolUse",
      permissionDecision: "deny",
      permissionDecisionReason: "BLOCKED: Cannot exit plan mode. Plan review loop not completed.\n\nYou MUST:\n1. Spawn 2 review agents IN PARALLEL on the ENTIRE plan file:\n   - Agent A (Industry Design Checker): CLAUDE.md conventions\n   - Agent B (Completeness + Quality): edge cases, error handling, test plan\n2. Fix ALL findings in the plan file\n3. Re-run BOTH agents until zero findings\n4. Print the compliance report — every finding count must be MEASURED from agent output, NEVER guessed, estimated, summarized from memory, or inferred. Cite the exact agent IDs and finding lists. If you cannot cite, you have not measured.\n5. Run: mkdir -p /tmp/claude_gates && touch /tmp/claude_gates/plan_review_done\n6. Then retry ExitPlanMode"
    }
  }'
  exit 0
fi

# Gate passed — clear marker for next plan session
rm -f "$marker"
exit 0
