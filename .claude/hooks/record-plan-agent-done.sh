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
# CWE-377 mitigation: the marker directory and file are created with
# restrictive permissions (mode 0700) so a co-tenant cannot read or
# overwrite the marker. The reader hook additionally verifies the
# marker is owned by the current user — a planted file from another
# user is rejected even if perms drift.
#
# Stdin: Claude Code hook JSON (tool_name=Agent, tool_input.subagent_type, tool_response)
# Stdout: empty (always allow — this hook only sets state)
set -euo pipefail

input=$(cat)
subagent=$(printf '%s' "$input" | jq -r '.tool_input.subagent_type // ""')
is_error=$(printf '%s' "$input" | jq -r '.tool_response.is_error // false')

if [ "$subagent" = "Plan" ] && [ "$is_error" != "true" ]; then
  # mkdir -m 0700 only sets the mode if the directory is actually created;
  # if it already exists with looser perms, a co-tenant could already have
  # planted the marker. Force-tighten in that case.
  mkdir -p -m 0700 /tmp/claude_gates
  chmod 0700 /tmp/claude_gates
  umask 077
  touch /tmp/claude_gates/plan_design_done
fi

exit 0
