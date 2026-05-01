#!/usr/bin/env bash
# PreToolUse hook for ExitPlanMode. Blocks plan exit unless:
# 1. A Plan-subtype subagent was invoked during this plan-mode session
#    (marker: /tmp/claude_gates/plan_design_done)
#
# The marker is set by record-plan-agent-done.sh (PostToolUse on Agent
# when tool_input.subagent_type == "Plan"). Pairs with
# enforce-plan-review-before-exit.sh — both gates must pass.
#
# Uses /tmp/claude_gates/ (NOT $TMPDIR) because Claude Code's hook runner
# uses macOS system TMPDIR while Bash tool uses sandbox TMPDIR.
#
# Stdin: Claude Code hook JSON
# Stdout: empty (allow) or hookSpecificOutput JSON (deny)
set -euo pipefail

marker="/tmp/claude_gates/plan_design_done"

if [ ! -f "$marker" ]; then
  jq -nc '{
    hookSpecificOutput: {
      hookEventName: "PreToolUse",
      permissionDecision: "deny",
      permissionDecisionReason: "BLOCKED: Cannot exit plan mode. Plan-agent design pass not completed.\n\nPhase 2 of the plan workflow REQUIRES launching at least one Plan subagent (Agent tool with subagent_type: \"Plan\") to design the implementation. Skipping this step via judgment call is not allowed — the harness enforces the workflow.\n\nYou MUST:\n1. Spawn at least one Plan agent (Agent tool, subagent_type: \"Plan\") with a prompt covering: comprehensive context from Phase 1 exploration (file paths, traces, constraints), requirements, and a request for a detailed implementation plan.\n2. Apply the Plan agent recommendations to your plan file (or document explicitly in the plan why each was rejected).\n3. The PostToolUse hook will set /tmp/claude_gates/plan_design_done automatically when the Plan agent completes successfully.\n4. Re-run the review agents (the previous plan_review_done marker was cleared on the last allowed exit attempt) and retry ExitPlanMode."
    }
  }'
  exit 0
fi

# Gate passed — clear marker for next plan session
rm -f "$marker"
exit 0
