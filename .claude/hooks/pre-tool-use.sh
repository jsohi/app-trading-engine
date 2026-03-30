#!/usr/bin/env bash
# PreToolUse hook: enforce git conventions (branch naming, commit messages, push-to-main block)

cmd=$(jq -r '.tool_input.command // ""')

deny() {
  printf '{"hookSpecificOutput":{"hookEventName":"PreToolUse","permissionDecision":"deny","permissionDecisionReason":"%s"}}' "$1"
  exit 0
}

case "$cmd" in
  git\ push*)
    # Block explicit pushes targeting main (any remote, any refspec syntax)
    echo "$cmd" | grep -qE -- '(:|[[:space:]])(refs/heads/)?main$' \
      && deny 'No direct pushes to main — all changes must go via PRs'

    # Block implicit pushes (bare "git push" or "git push <remote>") when on main
    branch=$(git rev-parse --abbrev-ref HEAD 2>/dev/null || echo none)
    if [ "$branch" = "main" ]; then
      echo "$cmd" | grep -qE -- '^git push([[:space:]]+\S+)?$' \
        && deny 'No direct pushes to main — all changes must go via PRs'
    fi
    ;;

  git\ commit*)
    # Commit message must reference a Linear issue
    echo "$cmd" | grep -qE 'APP-[0-9]+: ' \
      || deny 'Commit message must start with APP-{N}: prefix (Linear issue required)'

    # Branch name must follow convention (unless on main for fixups)
    branch=$(git rev-parse --abbrev-ref HEAD 2>/dev/null || echo main)
    [ "$branch" = "main" ] || echo "$branch" | grep -qE '^feat/app-[0-9]+-' \
      || deny 'Branch name must match feat/app-{N}-* pattern (Linear issue required)'
    ;;

  git\ checkout\ -b\ *|git\ switch\ -c\ *)
    name=$(echo "$cmd" | sed -E 's/^git (checkout -b|switch -c) //; s/ .*//')
    echo "$name" | grep -qE '^feat/app-[0-9]+-' \
      || deny 'Branch name must match feat/app-{N}-* pattern (Linear issue required)'
    ;;

  git\ branch\ *)
    # Extract branch name, skipping any flags (--track, etc.)
    name=$(echo "$cmd" | sed -E 's/^git branch //; s/--[^ ]+ *//g; s/ .*//')
    [ -z "$name" ] && exit 0
    echo "$name" | grep -qE '^feat/app-[0-9]+-' \
      || deny 'Branch name must match feat/app-{N}-* pattern (Linear issue required)'
    ;;
esac
