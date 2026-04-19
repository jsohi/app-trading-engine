#!/usr/bin/env bash
# PreToolUse hook for the Bash tool. Blocks `git push`, `make push`, and
# `gh pr create` unless the command is prefixed with
# LOCALLOOM_REVIEW_VERIFIED=1 to assert that the /review skill was run.
#
# Enforces the memory rule feedback_review_before_push.md so it cannot be
# bypassed by judgment.
#
# Stdin: Claude Code hook JSON ({"tool_name": "Bash", "tool_input": {"command": "..."}})
# Stdout: empty (allow) or hookSpecificOutput JSON (deny)

set -euo pipefail

cmd=$(jq -r '.tool_input.command // ""')

# Strip single- and double-quoted content from the command before pattern
# matching. Without this, commit messages like
#   git commit -m "fix: remove the make push suppression"
# would false-match on the text "make push" inside the message body.
# This is an imperfect heuristic (doesn't handle nested or escaped quotes
# perfectly) but correctly handles the common case of `git commit -m "..."`
# and `git commit -m '...'` invocations, where the message contains
# protected-verb text as literal content.
stripped=$(printf '%s' "$cmd" | sed -E "s/'[^']*'//g" | sed -E 's/"[^"]*"//g')

# Match the protected verbs ANCHORED at the start of the stripped command,
# allowing optional environment-variable prefixes:
#
#   ^ — start of (stripped) command
#   ([A-Za-z_][A-Za-z0-9_]*=\S+\s+)* — zero or more env var assignments
#   (git push | make push | gh pr create) — the protected verb
#   (\s|$) — word boundary after the verb
#
# This prevents false-matches on:
#   - text inside quoted strings (stripped above)
#   - substring matches like `git pushhh` or `make-pushy` (word boundary)
#   - `git commit -m "... make push ..."` (anchored at start)
PROTECTED_RE='^([A-Za-z_][A-Za-z0-9_]*=[^[:space:]]*[[:space:]]+)*((git[[:space:]]+push)|(make[[:space:]]+push)|(gh[[:space:]]+pr[[:space:]]+create))([[:space:]]|$)'

if ! printf '%s' "$stripped" | grep -qE "$PROTECTED_RE"; then
  exit 0
fi

# Allow `git push origin --delete <branch>` — deleting a remote branch is not
# a code push and should never be blocked by the review gate.
DELETE_BRANCH_RE='^([A-Za-z_][A-Za-z0-9_]*=[^[:space:]]*[[:space:]]+)*git[[:space:]]+push[[:space:]]+[^[:space:]]+[[:space:]]+--delete([[:space:]]|$)'
if printf '%s' "$stripped" | grep -qE "$DELETE_BRANCH_RE"; then
  exit 0
fi

# `git push` handling: if the repo has `make push` (scripts/push.sh or Makefile
# push target), block bare git push and require `make push`. Otherwise (e.g.
# Java/Gradle repos with no make push), allow `git push` with the
# LOCALLOOM_REVIEW_VERIFIED=1 gate — same as `gh pr create`.
GIT_PUSH_RE='^([A-Za-z_][A-Za-z0-9_]*=[^[:space:]]*[[:space:]]+)*git[[:space:]]+push([[:space:]]|$)'
if printf '%s' "$stripped" | grep -qE "$GIT_PUSH_RE"; then
  # Check if `make push` is available in this repo
  if [ -f scripts/push.sh ] || ([ -f Makefile ] && grep -q '^push:' Makefile 2>/dev/null); then
    # Repo has make push — block bare git push unconditionally
    jq -nc --arg msg "BLOCKED: bare \`git push\` is forbidden in this repo.

Use \`make push\` instead. \`make push\` runs Spotless, ruff, ESLint, and all
three test suites (api, ml-sidecar, frontend) before pushing. Bare \`git push\`
skips all of that and the repo-side hook scripts/hooks/pre-push will refuse it
anyway.

If you have already run /review (simplify) and E2E tests, invoke:

  LOCALLOOM_REVIEW_VERIFIED=1 LOCALLOOM_E2E_VERIFIED=1 make push

This enforces the memory rules feedback_review_before_push.md and
feedback_build_both.md so they cannot be bypassed by judgment." '{
      hookSpecificOutput: {
        hookEventName: "PreToolUse",
        permissionDecision: "deny",
        permissionDecisionReason: $msg
      }
    }'
    exit 0
  fi
  # Repo has no make push — fall through to the LOCALLOOM_REVIEW_VERIFIED=1 gate below
fi

# For `make push` and `gh pr create`: allow if the command was prefixed with
# LOCALLOOM_REVIEW_VERIFIED=1. The flag is always outside quotes (it's an env
# var, not a message), so we check the stripped version too.
has_review=false
has_e2e=false
if printf '%s' "$stripped" | grep -qE '(^|[[:space:]])LOCALLOOM_REVIEW_VERIFIED=1[[:space:]]'; then
  has_review=true
fi
if printf '%s' "$stripped" | grep -qE '(^|[[:space:]])LOCALLOOM_E2E_VERIFIED=1[[:space:]]'; then
  has_e2e=true
fi

if $has_review && $has_e2e; then
  exit 0
fi

# Build a specific deny message based on what's missing.
missing=""
if ! $has_review; then
  missing="  - Run /review (simplify) and fix any blocking findings"
fi
if ! $has_e2e; then
  if [ -n "$missing" ]; then missing="$missing
"; fi
  missing="${missing}  - Run E2E / integration tests: ./gradlew :integration-tests:test"
fi

# Block.
jq -nc --arg msg "BLOCKED by pre-push review hook (~/.claude/hooks/enforce-review-before-push.sh).

Before pushing or creating a PR, you MUST complete:
$missing

Then re-issue the command with BOTH env var prefixes:

  LOCALLOOM_REVIEW_VERIFIED=1 LOCALLOOM_E2E_VERIFIED=1 make push
  LOCALLOOM_REVIEW_VERIFIED=1 LOCALLOOM_E2E_VERIFIED=1 gh pr create --title '...' --body '...'

This enforces the memory rules feedback_review_before_push.md and ensures E2E tests are run before any PR is raised." '{
  hookSpecificOutput: {
    hookEventName: "PreToolUse",
    permissionDecision: "deny",
    permissionDecisionReason: $msg
  }
}'
exit 0
