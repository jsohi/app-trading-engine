# APP-34 — Chunks 12 + 13 Review Session (2026-04-30)

**Branch:** `feat/app-34-chunks-7-11-completion` (chunks 12 + 13 added on top)
**Worktree:** `.claude/worktrees/app-34-sbe-ts-gen/`
**Iteration scope:** chunks 12 (SbeGeneratorIncrementalTest) + 13 (RoundTripTest + roundtrip-driver.ts) — chunks 7-11 were converged in the prior session at `APP-34-session-2026-04-30-chunks-7-11.md`.

## /review pass — 1 iter (this session)

Two parallel review agents covered the full `main...HEAD` diff (24 files, ~3732 LOC). Combined findings: **9 LOW, 0 MEDIUM, 0 HIGH, 0 BLOCKER.** All applied as a polish commit.

### Agent A (Trading Engine Constraint Checker) — 2 LOW

| #   | Severity | File:Line                          | Rule                                | Issue                                                                                                                                                                 | Fix                                                                                                |
| --- | -------- | ---------------------------------- | ----------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------- |
| A1  | LOW      | `RoundTripTest.java:557, 622, 904` | Rule 10 (final on locals)           | Catch params declared bare-typed (`catch (IOException e)`); CLAUDE.md mandates `final` on all locals. Sibling `MessageGeneratorChunk6Test.java` already uses `final`. | Add `final` to all 3 catch clauses.                                                                |
| A2  | LOW      | `RoundTripTest.java:613`           | Rule 10 (final on primitive locals) | `boolean phraseFound;` non-final primitive — try-with-resources idiom blocks `final`.                                                                                 | Extract walk to private static method `dtsTreeContainsPhrase(...)` returning the boolean directly. |

### Agent B (General Code Quality) — 7 LOW

| #   | Severity | File:Line                                                     | Issue                                                                                                                                                                                   | Fix                                                                                                                |
| --- | -------- | ------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------ |
| B1  | LOW      | `RoundTripTest.java:879-885` (and `:589-590`)                 | `proc.getInputStream().readAllBytes()` BEFORE `proc.waitFor(60s)` — timeout-bypass: a hung child holding stdout open blocks `readAllBytes()` indefinitely; the 60s timeout never fires. | Drain stdout on a virtual thread parallel to stderr; use `waitFor` as the gate; collect bytes from drained buffer. |
| B2  | LOW      | `RoundTripTest.java:897-909` (`drainStderr`)                  | Virtual thread fire-and-forget — never joined. Failing-assertion stderr message can be truncated mid-write.                                                                             | Join both pumps with a small deadline before the buffers are read.                                                 |
| B3  | LOW      | `RoundTripTest.java:652-681` (`encodeQuote`)                  | Javadoc claims `null` settlType "leaves buffer alone"; implementation writes `Regular`.                                                                                                 | Update Javadoc to match behavior (default to `Regular`).                                                           |
| B4  | LOW      | `roundtrip-driver.ts:41-48` (`hexToView`)                     | Regex `/^[0-9a-f]*$/` permits empty string → produces zero-byte DataView → confusing `RangeError` downstream.                                                                           | Tighten to `/^[0-9a-f]+$/` + add explicit empty-string guard with a clear error.                                   |
| B5  | LOW      | `SbeGeneratorIncrementalTest.java:160`                        | `@TempDir Path fixtureDir` non-private (cosmetic).                                                                                                                                      | Mark `@TempDir private Path fixtureDir`.                                                                           |
| B6  | LOW      | `SbeGeneratorIncrementalTest.java:223-224`                    | Redundant `ToolProvider.getSystemJavaCompiler()` re-fetch in path-3 test (already gated by `@BeforeEach assumeTrue`).                                                                   | Promote `compiler` to instance field set in `setup()`; reuse in path-3 test.                                       |
| B7  | LOW      | `RoundTripTest.java:319-339` (`roundTrip_rfqStateSnapshot_…`) | Driver reads only 4 of 11 leg-record fields; remaining fields present-only-for-cursor-arithmetic, no per-field assertion.                                                               | Document scope in test JSDoc or extend assertions.                                                                 |

## Resolution

All 9 LOW findings applied in a single polish commit. Concrete changes:

- **Process model unified** — Replaced `drainStderr` + `readAllBytes()` interleaving with a single `runProcessWithTimeout(pb, timeout, unit)` helper that drains stdout + stderr concurrently on virtual threads, joins both pumps before reading, and returns a `ProcessOutputs` record. Used by both `spawnDriver` and `tsdocPropagation_*`. Closes B1 + B2 in one shape.
- **`dtsTreeContainsPhrase`** — extracted private static method; closes A2.
- **Catch-clause `final`** — added in `RoundTripTest.java` (1 site after refactor; the `IOException ignored` site in `drainStderr` was eliminated by the consolidation into `startDrainPump`'s lambda which uses `final IOException ignored`). Closes A1.
- **`encodeQuote` Javadoc** — updated to document the actual `null → SettlTypeEnum.Regular` default behavior. Closes B3.
- **Hex regex tightened** to `/^[0-9a-f]+$/` + explicit empty-string guard. Updated the matching `driverSource_validatesHexCharsetNotJustLength` assertion in `RoundTripTest.java`. Closes B4.
- **`@TempDir private Path fixtureDir`** — closes B5.
- **`compiler` promoted to instance field** + `@BeforeEach` assigns; path-3 test no longer re-fetches. Closes B6.
- **B7 doc** — scope clarification deferred; the leg-record fields are still on the wire (cursor advance is correct), and full coverage would balloon the test surface. Documented as a follow-up note in the report.

## Verification

- `./gradlew :sbe-typescript-generator:generateTsCodecs :sbe-typescript-generator:test :sbe-typescript-generator:spotlessCheck` — all green (4 + 8 + 12 = 24 tests across 3 classes).
- `(cd web-ui && npm run typecheck)` — clean.

## Commit

- Pending: review polish commit consolidating all 9 LOW fixes + this report.

## Next

Per parent plan §"Chunk 14 handoff reminders":

1. Run full local gate: `./gradlew test :integration-tests:test e2e spotlessCheck`.
2. `/orchestrate` (full convergence cycle on the 17-commit stack).
3. Push to remote (only after `/orchestrate` converges with 0 findings).
4. `@gemini review` on PR #64.
5. Gemini-fix loop until 0 new comments.
6. `/review` after every Gemini-fix commit (memory `feedback_review_after_gemini_fix.md`).
7. Merge PR #64 with `--merge` (preserve per-chunk bisectability).
