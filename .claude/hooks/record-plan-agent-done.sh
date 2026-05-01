#!/usr/bin/env bash
# PostToolUse hook on Agent. When a Plan-subtype subagent completes
# successfully, set /tmp/claude_gates/plan_design_done.
#
# This is the marker enforce-plan-design-before-exit.sh checks before
# allowing ExitPlanMode. Other subagent_type values (general-purpose,
# Explore, implementer, etc.) are ignored — they don't satisfy Phase 2.
#
# A "successful completion" requires both:
#   1. tool_input.subagent_type == "Plan"
#   2. tool_response.is_error != true (the Agent run did not error out)
# Without the second check, a failing or aborted Plan agent would silently
# arm the gate, letting the user exit plan mode without a real design pass.
#
# Stdin: Claude Code hook JSON (tool_name=Agent, tool_input.subagent_type, tool_response)
# Stdout: empty (always allow — this hook only sets state)
set -euo pipefail

input=$(cat)
subagent=$(printf '%s' "$input" | jq -r '.tool_input.subagent_type // ""')
is_error=$(printf '%s' "$input" | jq -r '.tool_response.is_error // false')

if [ "$subagent" = "Plan" ] && [ "$is_error" != "true" ]; then
  mkdir -p /tmp/claude_gates
  touch /tmp/claude_gates/plan_design_done
fi

exit 0
