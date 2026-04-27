---
description: "Full review-test-fix-push-review loop until convergence with compliance report"
allowed-tools: ["Bash", "Glob", "Grep", "Read", "Write", "Edit", "Agent", "Skill"]
---

# Orchestrate — Automated Review-Fix-Push-Review Convergence Loop

Run the full quality loop until zero changes across all gates: local review, tests, formatting, push, and Gemini review. Produces a session report with every comment's status and industry compliance percentage.

## MANDATORY EXECUTION RULES — READ FIRST

These rules are non-negotiable and OVERRIDE any judgment about efficiency or shortcuts:

1. **ALWAYS run /review every iteration** — spawns 2 fresh agents on ALL changes. Never skip. Never do "quick delta check."
2. **ALWAYS run ALL 3 test suites** — `./gradlew test`, `./gradlew :integration-tests:test`, `./gradlew e2e`. Never skip even if "nothing changed."
3. **NEVER accept or defer ANY comment** — dev phase means fix everything. No "accepted," no "out of scope," no "good enough."
4. **ALWAYS review ALL changes** — every iteration reviews the full diff `main...HEAD`, not just the delta since last review.
5. **ALWAYS wait for and address Gemini review** — after push, wait 270s, poll, fix all findings.
6. **NEVER terminate early** — loop until zero changes in a full pass, or hit max 10 iterations.
7. **ALWAYS produce the final report** — comment ledger + compliance dashboard + accepted/out-of-scope table (must be empty).
8. **ALWAYS write the report to a file** — `docs/review-reports/{issue}-session-{date}.md` for persistence.

## Step 0: Initialize

1. Determine the Linear issue number from the branch name or argument (e.g., `APP-35`).
2. Initialize the comment ledger — an empty list that accumulates every finding across all iterations:
   ```
   commentLedger = []
   iteration = 0
   converged = false
   startTime = now
   agentsSpawned = 0
   geminiRounds = 0
   ```
3. Record the start time for the session report.
4. **Record the rollback checkpoint** — save the current commit SHA so we can revert if the loop makes things worse:
   ```bash
   BASELINE_SHA=$(git rev-parse HEAD)
   ```
   If the loop hits max iterations without converging, offer to revert to this SHA.

## Step 1: Local Review (every iteration)

Run these in parallel to gather context:
```bash
git log --oneline main..HEAD
git diff main...HEAD --stat
git diff main...HEAD --name-only
```

Then invoke `/review` via the Skill tool. This spawns:
- **Agent A**: Trading Engine Constraint Checker (10 blocking rules including `final var`)
- **Agent B**: General Code Quality Review

Both agents review ALL changes (`main...HEAD`), not just the delta.

Collect all findings. For each finding, append to the comment ledger:
```
{ iteration, source: "LocalReview-AgentA" or "LocalReview-AgentB",
  severity: "blocking" or "critical" or "medium" or "low",
  file, line, description, status: "OPEN", fixCommit: null }
```

If there are findings: fix ALL of them (edit the files), then run `./gradlew spotlessApply`.

## Step 2: Test Suites — ALL THREE, NO EXCEPTIONS

**MANDATORY: Run ALL 3 test suites EVERY iteration. No exceptions. No "already passing." No "UP-TO-DATE means skip." No "only markdown changed." Execute each command and report the actual result.**

```bash
./gradlew test
```
If failures: fix them, add to ledger (source: "UnitTest"), run `./gradlew spotlessApply`, commit, and **restart from Step 1**.

```bash
./gradlew :integration-tests:test
```
If failures: fix them, add to ledger (source: "IntegrationTest"), run `./gradlew spotlessApply`, commit, and **restart from Step 1**.

```bash
./gradlew e2e
```
If failures: first run `./gradlew e2eClean`, then retry. If still failing, fix, add to ledger (source: "E2E"), commit, and **restart from Step 1**.

**NEVER skip any suite. NEVER claim a suite was "already run." Execute all three and report pass/fail with actual output.**

## Step 2b: Compliance Score — MEASURED, NEVER GUESSED

**After tests pass each iteration, run the FULL compliance measurement. NEVER estimate or guess a score. ALWAYS execute every scan command and report actual numbers.**

Run these exact commands and report the actual output:

```
── Tools Run Every Iteration ─────────────────────────────────
 TOOL                              │ PURPOSE                    │ CATEGORY
──────────────────────────────────┼────────────────────────────┼──────────
 Skill(/review)                    │ 2 fresh agents, 10 rules   │ All
 ./gradlew test                    │ Unit tests                 │ Cat 1
 ./gradlew :integration-tests:test │ Integration tests          │ Cat 1
 ./gradlew e2e                     │ Full 3-node cluster E2E    │ Cat 1
 ./gradlew spotlessApply           │ Auto-format                │ Cat 8
 ./gradlew spotlessCheck           │ Verify format (must pass)  │ Cat 8
 grep -rEn (determinism)           │ Cluster non-determinism    │ Cat 4
 grep -rEn (collections)           │ java.util.* in hot-path    │ Cat 5
 grep -rl  (logging SLF4J)        │ SLF4J anywhere             │ Cat 9
 grep -rl  (logging Log4j2)       │ Log4j2 in hot-path         │ Cat 9
 grep -rEn (clock)                 │ Wall-clock outside cluster │ Cat 10
 Agent A (docs + threading)        │ Javadoc + thread-safety    │ Cat 3, 13
 Agent B (alloc + autobox + var)   │ Zero-alloc + final var     │ Cat 2, 6, 11
 ./gradlew dependencyCheckAnalyze  │ OWASP CVE scan             │ Cat 12
 gh api (Gemini poll)              │ External review            │ Gemini
──────────────────────────────────┴────────────────────────────┴──────────
```

After executing ALL scans, print the MEASURED score:

```
── Iteration {N} Compliance (MEASURED) ───────────────────
 #  │ Category                    │ Score │ Status
────┼─────────────────────────────┼───────┼────────
 1  │ Test Coverage               │ {X}%  │ {PASS/SUBPAR}
 2  │ Zero-Allocation             │ {X}%  │ {PASS/SUBPAR}
 3  │ Code Documentation          │ {X}%  │ {PASS/SUBPAR}
 4  │ Determinism                 │ {X}%  │ {PASS/SUBPAR}
 5  │ Collection Compliance       │ {X}%  │ {PASS/SUBPAR}
 6  │ Autoboxing Compliance       │ {X}%  │ {PASS/SUBPAR}
 7  │ FIX Protocol                │ {X}%  │ {PASS/SUBPAR}
 8  │ Formatting                  │ {X}%  │ {PASS/SUBPAR}
 9  │ Logging Compliance          │ {X}%  │ {PASS/SUBPAR}
 10 │ Clock Discipline            │ {X}%  │ {PASS/SUBPAR}
 11 │ final var Usage             │ {X}%  │ {PASS/SUBPAR}
 12 │ Security (OWASP)            │ {X}%  │ {PASS/SUBPAR}
 13 │ Thread-Safety Docs          │ {X}%  │ {PASS/SUBPAR}
────┼─────────────────────────────┼───────┼────────
    │ OVERALL                     │ {X}%  │
──────────────────────────────────────────────────────────
 Review:  {N} found, {N} fixed
 Gemini:  {N} found, {N} fixed
 Accepted: 0 | Out of scope: 0
──────────────────────────────────────────────────────────
```

**NEVER write "Estimated", "~", "approximately", or "about" for any score. Every number must come from an actual command execution.**

## Step 3: Formatting

```bash
./gradlew spotlessApply
./gradlew spotlessCheck
```

If `spotlessCheck` fails after `spotlessApply`, something is wrong — investigate, fix, add to ledger (source: "Spotless"), and **restart from Step 1**.

## Step 4: Commit and Push

If there are any uncommitted changes from Steps 1-3:
```bash
git add -u
git commit -m "APP-{N}: orchestrate R{iteration} — review fixes"
```

**Hook interaction:** The post-commit hook will fire and demand 3 test suites. Since you already ran them in Step 2 this iteration, acknowledge the hook demand: "Tests were run in Step 2 above — all 3 suites passed." Proceed without re-running.

Record the push timestamp with a 30s clock-skew buffer (for Gemini polling in Step 5), then push:
```bash
PUSH_TIME=$(date -u -d '30 seconds ago' +%Y-%m-%dT%H:%M:%SZ 2>/dev/null || date -u -v-30S +%Y-%m-%dT%H:%M:%SZ)  # portable 30s clock-skew buffer (GNU || BSD)
LOCALLOOM_REVIEW_VERIFIED=1 LOCALLOOM_E2E_VERIFIED=1 git push origin HEAD
```

The PostToolUse hook will auto-comment `@gemini review` on the PR. Do NOT duplicate this.

## Step 5: Poll Gemini Review

Record the push timestamp. Use `ScheduleWakeup` with `delaySeconds: 270` to wait (stays within Claude prompt cache 5-min TTL). If `ScheduleWakeup` is unavailable, use `Bash(sleep 270)` with `timeout: 300000`.

Then poll for Gemini's review:

```bash
# Get PR number
pr=$(gh pr view --json number -q .number)

# Get owner/repo
repo=$(gh repo view --json nameWithOwner -q .nameWithOwner)

# Get Gemini review comments created after the push
# NOTE: PUSH_TIME must be recorded BEFORE the git push in Step 4, not here.
# Record it in Step 4 like: PUSH_TIME=$(date -u +%Y-%m-%dT%H:%M:%SZ)
# Then use it here to filter only NEW comments:

gh api --paginate "repos/${repo}/pulls/${pr}/comments" --jq "
  .[] | select(.user.login == \"gemini-code-assist[bot]\" and .created_at > \"$PUSH_TIME\") |
  {path, line, body, created_at}
"

# Also get review-level summary
gh api --paginate "repos/${repo}/pulls/${pr}/reviews" --jq "
  .[] | select(.user.login == \"gemini-code-assist[bot]\" and .submitted_at > \"$PUSH_TIME\") |
  {body, submitted_at}
"
```

If no Gemini review found, wait another 120 seconds and poll again. Maximum 3 polls (total ~8.5 min).

Parse each Gemini comment's severity from SVG badges in the body:
- `security-high-priority.svg` → severity: "security-high" (blocking)
- `high-priority.svg` → severity: "high" (blocking)
- `medium-priority.svg` → severity: "medium" (fix in dev phase)
- `low-priority.svg` → severity: "low" (fix in dev phase)
- No badge → severity: "info"

Add ALL Gemini comments to the ledger (source: "GeminiReview").

## Step 6: Fix Gemini Comments

If there are any high, security-high, medium, or low Gemini comments:
1. Fix ALL of them (edit files)
2. Run `./gradlew spotlessApply`
3. Commit: `"APP-{N}: orchestrate R{iteration} — Gemini review fixes"`
4. **Restart from Step 1** (the fix may have introduced new issues)

If zero actionable Gemini comments → **CONVERGED**. Proceed to Step 7.

## Step 7: Final Report

### 7a: Run Compliance Scoring

Invoke `/compliance` via the Skill tool. This produces the 13-category industry compliance dashboard.

### 7b: Compile Session Report

Generate the full report and write it to `docs/review-reports/APP-{N}-session-{date}.md`:

```markdown
# Session Report — APP-{N} — {date}

## Summary
- **Branch:** `{branch_name}`
- **Iterations:** {N} (converged | hit max)
- **Total comments found:** {count}
- **Total fixes applied:** {count}
- **Final verdict:** ALL CLEAR | NEEDS ATTENTION

## Comment Ledger

| # | Iter | Source | Severity | File:Line | Description | Status | Fix Commit |
|---|------|--------|----------|-----------|-------------|--------|------------|
| 1 | 1 | LocalReview-A | blocking | cluster/Foo.java:42 | new ArrayList in hot path | FIXED | abc1234 |
| ... | ... | ... | ... | ... | ... | ... | ... |

## Compliance Dashboard

(output from /compliance — 13 metrics table)

## Accepted / Out-of-Scope / Not Prod-Ready

| # | Source | Description | Reason | Status |
|---|--------|-------------|--------|--------|
| (This table MUST be empty in dev phase.) |

**TOTAL: 0 accepted, 0 out-of-scope, 0 subpar**

## Test Results

| Suite | Status | Duration | Details |
|-------|--------|----------|---------|
| Unit Tests | {PASS/FAIL} | {time} | {count} tests, {failures} failures |
| Integration Tests | {PASS/FAIL} | {time} | {count} tests, {failures} failures |
| E2E | {PASS/FAIL} | {time} | 3-node cluster + FIX validation |
```

### 7c: Append to Session Report Index

Append a one-line entry to `docs/review-reports/INDEX.md` (create if it doesn't exist):

```markdown
| {date} | APP-{N} | {iterations} | {total_comments} | {fixed}/{total} | {compliance_pct}% | [report](APP-{N}-session-{date}.md) |
```

### 7d: Track Cost and Duration

Add to the session report:

```markdown
## Session Metrics
- **Wall time:** {minutes}m {seconds}s
- **Iterations:** {N}
- **Agents spawned:** {count} (review: {N}, compliance: {N}, plan: {N})
- **Gemini rounds:** {N}
- **Commits:** {N}
```

### 7e: Save to Cross-Session Memory

After each session, check if common patterns emerge. If the same finding type appeared in 3+ sessions, write a memory file:

```
File: ~/.claude/projects/{project}/memory/orchestrator_common_findings.md
Content: Top recurring findings across sessions with file paths and fix patterns
```

This lets future `/review` runs prioritize checking patterns that commonly fail.

### 7f: Print Summary

Print the comment ledger and compliance dashboard to the conversation so the user sees it immediately.

## Convergence Criteria

The loop converges when a FULL pass through Steps 1-6 produces ZERO changes:
- `/review` finds 0 blocking + 0 quality issues
- All 3 test suites pass without any fixes needed
- `spotlessCheck` passes
- Gemini review has 0 actionable comments (high/medium/low)

## Hook Interaction Reference

| Action | PreToolUse Hooks | PostToolUse Hooks | Your Response |
|--------|-----------------|-------------------|---------------|
| `git commit -m "APP-N: ..."` | pre-tool-use.sh validates prefix + branch | Post-commit hook demands 3 test suites | Tests already ran this iteration; acknowledge |
| `git push origin HEAD` | enforce-review-before-push.sh checks env vars | Auto `@gemini review` on PR | Provide both env vars; leverage auto-trigger |
| `/review` via Skill | None | None | Fresh agents spawned by review.md |

## Safety Limits

- **Max iterations:** 10. If not converged after 10, produce the report with status "HIT MAX ITERATIONS" and list all unresolved items.
- **Max commits per iteration:** 2 (one for local review fixes, one for Gemini fixes).
- **E2E failure recovery:** Run `./gradlew e2eClean` before retrying E2E if the previous run failed (kills stale Aeron processes).
- **Context management:** Keep the comment ledger as a compact markdown table. Summarize test output (pass/fail + failure count) rather than including raw logs.
- **Rollback safety net:** If the loop hits max iterations without converging, show the user:
  ```
  ⚠ Orchestrator did not converge after 10 iterations.
  Baseline: {BASELINE_SHA} ({baseline_commit_msg})
  Current:  {HEAD_SHA}
  Changes:  git diff {BASELINE_SHA}..HEAD --stat

  Options:
  1. Keep changes and review manually
  2. Revert to baseline: git reset --hard {BASELINE_SHA}
  ```
  NEVER auto-revert — always ask the user. Show the diff stat so they can make an informed decision.
