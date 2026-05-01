#!/usr/bin/env bash
# PostToolUse hook on Agent. When a Plan-subtype subagent completes
# successfully, set /tmp/claude_gates/plan_design_done.
#
# This is the marker enforce-plan-design-before-exit.sh checks before
# allowing ExitPlanMode. Other subagent_type values (general-purpose,
# Explore, implementer, etc.) are ignored — they don't satisfy Phase 2.
#
# Stdin: Claude Code hook JSON (tool_name=Agent, tool_input.subagent_type, tool_response)
# Stdout: empty (always allow — this hook only sets state)
set -euo pipefail

input=$(cat)
subagent=$(printf '%s' "$input" | jq -r '.tool_input.subagent_type // ""')

if [ "$subagent" = "Plan" ]; then
  mkdir -p /tmp/claude_gates
  touch /tmp/claude_gates/plan_design_done
fi

exit 0
