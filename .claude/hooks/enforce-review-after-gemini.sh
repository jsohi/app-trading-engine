#!/usr/bin/env bash
# PreToolUse hook for the Bash tool. Blocks `git push` / `make push` /
# `gh pr create` if the most-recent local commit titled "Gemini" /
# "Gemini review fixes" / "Gemini round" was not followed by a
# `/review` run that produced zero findings.
#
# The convergence loop semantics:
#   1. Gemini posts findings on a PR.
#   2. Agent fixes them and commits with "Gemini fixes" / "Gemini R{N}"
#      in the message.
#   3. BEFORE pushing the next round, the agent MUST re-run /review
#      (parallel two-agent pass) on the fixed branch — Gemini fixes
#      can introduce drift, and the user wants the same /review gate
#      that protected the original push.
#   4. The /review skill (or the orchestrate loop) creates the marker
#      file `/tmp/claude_gates/post_gemini_review_done` on a clean
#      review (zero findings) and removes it on the next Gemini round.
#   5. This hook checks for the marker and BLOCKS the push if a
#      Gemini-fix commit is at HEAD without the marker.
#
# Bypass: prefix the command with POST_GEMINI_REVIEW_VERIFIED=1 to
# assert the gate is satisfied (mirrors LOCALLOOM_PRECOMMIT_VERIFIED=1
# pattern). Use only after running /review and confirming zero findings.
#
# Stdin: Claude Code hook JSON ({"tool_name": "Bash", "tool_input": {"command": "..."}})
# Stdout: empty (allow) or hookSpecificOutput JSON (deny)

set -euo pipefail

cmd=$(jq -r '.tool_input.command // ""')

# Strip quoted content so commit-message text doesn't trigger the gate.
stripped=$(printf '%s' "$cmd" | sed -E "s/'[^']*'//g" | sed -E 's/"[^"]*"//g')

# Same protected verbs as enforce-review-before-push.sh.
PROTECTED_RE='^([A-Za-z_][A-Za-z0-9_]*=[^[:space:]]*[[:space:]]+)*((git[[:space:]]+push)|(make[[:space:]]+push)|(gh[[:space:]]+pr[[:space:]]+create))([[:space:]]|$)'

if ! printf '%s' "$stripped" | grep -qE "$PROTECTED_RE"; then
  exit 0
fi

# Bypass flag.
if printf '%s' "$stripped" | grep -qE '(^|[[:space:]])POST_GEMINI_REVIEW_VERIFIED=1[[:space:]]'; then
  exit 0
fi

# Walk back from HEAD until we find a non-fix-on-fix commit. Any commit
# whose subject mentions "Gemini" (case-insensitive) is treated as a
# Gemini-fix commit; we only gate when the LATEST such commit exists
# without a corresponding /review marker.
#
# Resolve the base ref defensively — local "main" may not exist (forks
# named "master", or freshly-cloned worktrees that only fetched the
# default branch). Try in order: local main → origin/HEAD's symbolic
# target → origin/main → origin/master. If none resolve, fall back to
# the last 5 HEAD subjects so the gate fails closed (any Gemini commit
# in the recent window still triggers).
base_ref=""
for candidate in main origin/HEAD origin/main origin/master; do
  if git rev-parse --verify --quiet "$candidate" >/dev/null 2>&1; then
    base_ref="$candidate"
    break
  fi
done

if [ -n "$base_ref" ]; then
  last_subjects=$(git log --pretty=%s "${base_ref}..HEAD" 2>/dev/null | head -5 || true)
else
  last_subjects=$(git log --pretty=%s -5 HEAD 2>/dev/null || true)
fi

if printf '%s' "$last_subjects" | grep -qiE 'gemini'; then
  marker=/tmp/claude_gates/post_gemini_review_done
  if [ ! -f "$marker" ]; then
    jq -nc --arg msg "BLOCKED by post-Gemini /review gate (~/.claude/hooks/enforce-review-after-gemini.sh).

The most recent commit(s) on this branch include Gemini-fix work but no /review run has produced zero findings since.

You MUST:
  (1) Run /review (parallel Agent A + Agent B) on the current branch.
  (2) Fix every finding the agents produce.
  (3) Re-run /review until both agents report 0/0/0/0 (zero findings).
  (4) Run: mkdir -p /tmp/claude_gates && touch /tmp/claude_gates/post_gemini_review_done
  (5) Re-issue the push command.

The marker is consumed (deleted) on the next Gemini-fix commit so the gate fires fresh on every Gemini round.

Why this exists: Gemini fixes can silently introduce regressions (broken types, drift from project conventions, lost test coverage). /review re-running is the second-opinion gate that the original push had. Without it, fixes layer on top of fixes and quality decays.

To bypass once (only after running /review with zero findings):
  POST_GEMINI_REVIEW_VERIFIED=1 <your command>" '{
  hookSpecificOutput: {
    hookEventName: "PreToolUse",
    permissionDecision: "deny",
    permissionDecisionReason: $msg
  }
}'
    exit 0
  fi

  # Marker exists — allow push, then delete the marker so the next
  # Gemini fix re-arms the gate.
  rm -f "$marker"
fi

exit 0
