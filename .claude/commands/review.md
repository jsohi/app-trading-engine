---
description: "Review branch changes against trading engine rules before pushing"
allowed-tools: ["Bash", "Glob", "Grep", "Read", "Agent"]
---

# Trading Engine Branch Review

Review all changes on the current branch before pushing. This wraps a general code review with trading-engine-specific checks that are **blocking**.

## MANDATORY EXECUTION RULES — READ FIRST

These rules are non-negotiable and OVERRIDE any judgment about efficiency, context reuse, or delta size:

1. **ALWAYS spawn both review agents in parallel via the Agent tool** — Agent A (Trading Engine Constraint Checker) AND Agent B (General Code Quality Review). Never skip either. Never do "just one this time."
2. **ALWAYS scope the review to the full `main...HEAD` diff — NEVER to a single commit, single file, or the delta since the previous review.** Every agent prompt must list **every** file in `git diff main...HEAD --name-only`. If the user says "review this commit" or "review this file," expand it to the full branch diff and tell them that's what you're doing. Reviewing a subset misses cross-file regressions and the cumulative state.
3. **NEVER do an inline delta-only review** — even if you reviewed this exact branch seconds ago, even if the delta since the last review is a one-line comment, even if you "already have full context," even if the changes are "obviously clean." Spawn fresh agents every single invocation.
4. **NEVER offer to shortcut** — do not propose "I can do a quick delta check instead," "single-commit review," "single-file review," or "the previous review already covered this." Go straight to Step 1 → Step 2 (parallel agent spawn) → Step 3 without asking.
5. **The value of /review is the independent second opinion from fresh agent context covering the full branch surface area** — your own in-conversation review is not a substitute and defeats the purpose of invoking the command.

If you catch yourself about to write any of the following — stop and spawn the agents on the full `main...HEAD` diff:

- "the delta is small so..."
- "I already reviewed this..."
- "let me just review this one file..."
- "let me just review this one commit..."
- "let me just review the latest commit..."
- "since only one commit was added since the last review..."
- "the diff for this commit is..."
- any framing that scopes the review to fewer than all changes in `main...HEAD`

## Step 1: Gather Context

Run these in parallel:

- `git log --oneline main..HEAD` — list commits on this branch
- `git diff main...HEAD --name-only` — list all changed files
- `git diff main...HEAD --stat` — summary of changes
- `git diff` and `git diff --cached` — any uncommitted work

If there are no changes vs main, stop and report "Nothing to review."

## Step 2: Launch Review Agents in Parallel

Launch these agents in parallel:

### Agent A: Trading Engine Constraint Checker (BLOCKING)

Read every changed file in full. Check for these **blocking violations** — any match means the branch is NOT ready to push:

1. **No heap allocation in cluster/gateway hot path** — flag any `new` keyword in files under `cluster/` or `gateway/` packages (except in snapshot restore, startup, or test code). Look for `new ArrayList`, `new HashMap`, `new String`, boxing of primitives, string concatenation with `+`, autoboxing, `String.format`, `Arrays.asList`, `List.of`, `Map.of`, `stream()`, `collect()`, lambdas that capture variables.

    **Iterator and garbage-creating patterns (MUST be flagged for approval):**
    - **Enhanced for-each** (`for (final var x : collection)`) — allocates an `Iterator` object on every invocation when used with `Iterable` types (arrays are exempt — the compiler generates index-based access). On hot path, use index-based loops (`for (int i = 0; i < size; i++)`) or Agrona's reusable iterator pattern instead.
    - **`collection.iterator()`** — allocates a new `Iterator` instance. Use Agrona's `ObjectHashSet.iterator()` only if the iterator is reset/reused; otherwise use index-based iteration or `forEachInt()`/`forEachLong()`.
    - **`Iterable.forEach(lambda)`** — allocates a lambda/closure if it captures local variables. Acceptable only if the lambda is a non-capturing method reference to a `final` field.
    - **`Map.entrySet()`** — allocates `Map.Entry` wrappers on every iteration for most implementations. Use Agrona's `Long2ObjectHashMap.EntryIterator` with `reset()` or iterate keys + `get()` instead.
    - **`String.split()`, `String.substring()`, `Pattern.compile()`** — allocate arrays or regex objects. Pre-compile patterns at construction; avoid split on hot path.
    - **`toArray()`, `Arrays.copyOf()`, `Arrays.stream()`** — array allocation. Pre-allocate and reuse.
    - **`try-with-resources`** — may allocate a suppressed-exceptions list if exceptions occur. Acceptable for I/O-bound code but flag on hot path.
    - **Varargs calls** (`method(T... args)`) — allocate an `Object[]` per invocation. Use overloaded fixed-arity methods instead.

    Any of these patterns in hot-path code (cluster/, gateway/, orchestrator/, pricing-service/ production code, excluding snapshot restore, startup, and tests) MUST be flagged. If the pattern appears on a non-hot path (startup, snapshot, diagnostic), flag as WARNING with a note that it should be documented with an `// Allocation: ...` comment justifying why it is acceptable.

2. **No `java.util.*` collections in cluster/gateway modules** — flag any import of `java.util.HashMap`, `java.util.ArrayList`, `java.util.Map`, `java.util.List`, `java.util.Set`, etc. in files under cluster or gateway packages. Must use Agrona: `Object2ObjectHashMap`, `Long2ObjectHashMap`, `Int2ObjectHashMap`, `ObjectHashSet`, etc.

3. **No floating-point for prices/quantities** — flag any `double`, `float`, `BigDecimal`, or `Double` used for price, quantity, amount, or notional fields. Must use `long` with fixed-point scaling (× 10⁻⁸).

4. **No wall-clock time in cluster service** — flag `System.currentTimeMillis()`, `System.nanoTime()`, `Instant.now()`, `LocalDateTime.now()`, `Clock.systemUTC()`, or any `java.time` clock in cluster service code. Must use the cluster timestamp from `onSessionMessage` / `onTimerEvent`.

5. **No randomness in cluster service** — flag `Math.random()`, `new Random()`, `ThreadLocalRandom`, `UUID.randomUUID()`, `SecureRandom` in cluster service code. Must be deterministic for Aeron log replay.

6. **SBE field IDs must use FIX tag numbers** — if `trading-schema.xml` is modified, check that field `id=` values correspond to FIX tag numbers (e.g., ClOrdID=11, OrderQty=38, Price=44, Side=54, Symbol=55, OrdType=40, etc.).

7. **Schema sequential merge warning** — if `trading-schema.xml` is modified, flag a warning that schema changes must be merged sequentially to avoid conflicts.

8. **No autoboxing of primitives** — flag any code path that boxes a primitive to its wrapper type (`Long`, `Integer`, `Boolean`, `Double`, `Float`, `Short`, `Byte`, `Character`). Common offenders:
    - `Long`, `Integer`, etc. as field types, parameter types, return types, or generic type arguments where a primitive would do (e.g., `Map<Long, X>`, `List<Integer>`, `Optional<Long>`)
    - `Long.valueOf(...)`, `Integer.valueOf(...)`, `Boolean.valueOf(...)` calls
    - Implicit boxing in method calls: `someMap.put(longVar, value)` where the map signature wants `Object` / `Long`
    - Implicit boxing in collection operations: `list.add(intVar)`, `list.contains(longVar)` on `List<Integer>`/`List<Long>`
    - **Iterating Agrona primitive collections via enhanced-for**: `for (final long id : longHashMap.keySet())` calls `Iterator<Long>.next()` and boxes — use the primitive iterator instead (`Long2ObjectHashMap.KeyIterator.nextLong()`, `Int2ObjectHashMap.KeyIterator.nextInt()`)
    - `IntArrayList.addAll(Collection<Integer>)` / `LongArrayList.addAll(Collection<Long>)` — these box every element on the way in
    - Returning a primitive from a method whose declared return type is the wrapper
    - Auto-boxing into `Object[]` (e.g., `Stream.of(1, 2, 3)` → `Stream<Integer>`)

    The hot path is the strict prohibition. Diagnostic / snapshot / startup paths may box if explicitly documented and justified — if you encounter boxing on those paths, flag it as a warning rather than blocking, but still call it out.

9. **No direct wall-clock calls outside cluster** — flag `System.currentTimeMillis()`, `Instant.now()`, `LocalDateTime.now()`, `new Date()`, `System.nanoTime()` in any non-test module. Must use injected `EpochNanoClock` (epoch time) or `NanoClock` (monotonic time). Exception: test code.

10. **`final var` for all reference-type local variables** — flag any local variable declaration in changed files that declares a reference type explicitly instead of using `var`. All reference-type locals must use `final var x = ...`.

    Primitives (`int`, `long`, `byte`, `boolean`, `short`, `char`, `float`, `double`) MUST keep explicit types with `final` — explicit primitive types make zero-allocation intent self-documenting and prevent silent type drift if a return type changes from primitive to wrapper (which would introduce autoboxing per Rule #8).

    **Violations (blocking):**
    - `final String msg = ex.getMessage()` → should be `final var msg = ex.getMessage()`
    - `String s = "hello"` → should be `final var s = "hello"`
    - `final Map<Long, Order> map = ...` → should be `final var map = ...`
    - `List<String> items = getItems()` → should be `final var items = getItems()`
    - `Iterator<X> it = collection.iterator()` → should be `final var it = collection.iterator()`
    - Any non-final local variable of reference type: `var x = ...` without `final`

    **NOT violations (correct):**
    - `final long price = decoder.price()` — primitive, explicit type required
    - `final int count = 0` — primitive
    - `final boolean active = true` — primitive
    - `final var account = accountStore.get(id)` — already correct
    - Method/constructor parameters (var not allowed in parameters by Java spec)
    - Field declarations (var not allowed for fields by Java spec)
    - Catch clause variables (`catch (final IOException ex)` — var not allowed)
    - Enhanced for-each loop variables with primitives (`for (final long id : ids)`) — note: while the explicit type is correct per Rule #10, enhanced-for over Agrona primitive collections still causes autoboxing per Rule #8; use primitive iterators instead
    - Cases where var genuinely cannot infer the correct type (null literal, diamond with ambiguous target, intersection types)
    - Resource declarations in try-with-resources where the type is needed for clarity

Report each violation with: file path, line number, the offending code, which rule it violates, and a suggested fix.

### Agent B: General Code Quality Review

Review the diff for:

- Correctness: logic bugs, off-by-one, null/edge cases
- Thread safety: shared mutable state outside cluster
- Resource leaks: unclosed streams, missing try-with-resources
- Error handling: swallowed exceptions, missing validation at boundaries
- Test quality: meaningful assertions, edge cases, no flaky patterns
- Naming and clarity

Only report issues with confidence ≥ 80.

### Mandatory output for BOTH agents

Each agent MUST report THREE distinct sections, not just findings:

1. **Findings (TABULAR)** — violations / issues in a markdown table. **Tabular format is mandatory; bulleted/prose lists are rejected by Step 3.** Required columns:

    | # | Severity | File:Line | Code/Pattern | Issue | Rule Violated | Suggested Fix |

    Severities: BLOCKER / HIGH / MEDIUM / LOW. End the table with a measured count line: `Total: B BLOCKER, H HIGH, M MEDIUM, L LOW.` If there are zero findings, emit a one-row table with `—` placeholders so the schema is preserved, then the count line.

2. **Considered and accepted (TABULAR)** — every rule the agent CHECKED that returned PASS or N/A, in a markdown table. **Tabular format is mandatory.** Required columns:

    | # | Rule / Dimension | Status (PASS/N/A) | Rationale (one line) |

    This is positive coverage: the user must see what was reviewed and not flagged, not just what failed. Never empty — if the agent checked nothing, the output isn't a review.

3. **Out of scope (deferred) (TABULAR)** — items the agent recognized but explicitly did NOT review because they belong to another ticket / phase / wave. **Tabular format is mandatory.** Required columns:

    | # | Item | Deferring Chunk / Ticket | Where it's tracked (file:line, TODO, plan §) |

    Cite the deferring ticket (e.g., "CSP enforcement → APP-246", "RUM dashboards → APP-245", "TS project references → deferred until 3+ TS workspaces").

Without all three sections in the required tabular format, the agent's report is incomplete and Step 3 must reject it and re-spawn the agent.

## Step 3: Compile Results

Combine both agent reports into a single summary:

```markdown
## Branch Review: `<branch-name>`

**Commits:** <count> commits on branch
**Files changed:** <count>

### Industry-Standard Score (MEASURED, weighted)

Compute by weighted dimension, citing each agent's coverage:

| Dimension                                                                           | Weight | Score   | Notes                                      |
| ----------------------------------------------------------------------------------- | ------ | ------- | ------------------------------------------ |
| Convention compliance (CLAUDE.md rules: bigint, clock, allocation, naming, etc.)    | 25%    | <0–100> | <Agent A coverage matrix cited>            |
| Tooling currency (current LTS / stable versions, no deprecated APIs)                | 15%    | <0–100> | <e.g., Vite 7, Node 22 LTS, ESLint 9 flat> |
| Documentation quality (Javadoc/JSDoc, design rationale, threading model)            | 15%    | <0–100> | <Agent B observation>                      |
| Test coverage (unit + integration + e2e where applicable; lint-fixture regressions) | 15%    | <0–100> | <test count + categories>                  |
| CI / build hygiene (formatting, lint clean, type-safe, gates wired)                 | 10%    | <0–100> | <Agent B observation>                      |
| Industry idioms (named imports, useSyncExternalStore, canonical Vite worker, etc.)  | 10%    | <0–100> | <Agent A + B cross-reference>              |
| Supply chain (SBOM, dep-pin, engines, lockfile)                                     | 5%     | <0–100> | <observation>                              |
| Observability seams (telemetry SDK wired, span contracts)                           | 5%     | <0–100> | <observation>                              |

**Industry-Standard Score: <weighted total>%** — measured, not guessed. Cite the agent IDs that contributed each row.

If a dimension is genuinely N/A for this branch (e.g., observability seams for a pure docs PR), mark the row "N/A" and redistribute its weight proportionally across the remaining dimensions. Document the redistribution.

### Production-Ready Score (MEASURED, weighted)

For Phase 1 / foundation tickets this is OFTEN intentionally low — production hardening lives in separate tickets (e.g., APP-244 umbrella). Mark items "out of scope (APP-NNN)" rather than failing them.

| Dimension                                                   | Weight | Score                 | Notes |
| ----------------------------------------------------------- | ------ | --------------------- | ----- |
| Auth UX (refresh, idle, logout, MFA)                        | 15%    | <0–100 / OOS:APP-NNN> |       |
| Operational observability (RUM, error tracking, dashboards) | 15%    | <0–100 / OOS>         |       |
| Resilience tested (chaos, recovery, slow-consumer)          | 15%    | <0–100 / OOS>         |       |
| Performance budgets (Lighthouse, bundle, FPS)               | 10%    | <0–100 / OOS>         |       |
| Browser/platform matrix declared + enforced                 | 10%    | <0–100 / OOS>         |       |
| Deployment topology (CDN, blue/green, cache headers)        | 10%    | <0–100 / OOS>         |       |
| Feature flags + kill switch                                 | 10%    | <0–100 / OOS>         |       |
| Security headers (CSP, SRI, COOP/COEP)                      | 10%    | <0–100 / OOS>         |       |
| I18n / locale-aware formatting                              | 5%     | <0–100 / OOS>         |       |

**Production-Ready Score: <weighted total>%** (excluding OOS rows). Phase 1 foundation tickets typically score 50–70% with the rest tracked elsewhere.

### Blocking Issues (Trading Engine Rules)

<list from Agent A, or "None found">

### Code Quality Issues

<list from Agent B, grouped by severity>

### Considered and accepted

<positive-coverage merge from Agent A + Agent B — every rule that returned PASS / N/A, deduplicated and grouped by topic>

### Out of scope (deferred)

<merged list from Agent A + Agent B, each item citing the deferring ticket>

### Verdict

**READY TO PUSH** | **FIX BEFORE PUSHING** | **NEEDS DISCUSSION**
```

Rules:

- Any blocking issue from Agent A → verdict is **FIX BEFORE PUSHING**
- Any critical code quality issue → verdict is **FIX BEFORE PUSHING**
- Only warnings/suggestions → verdict is **READY TO PUSH**
- Ambiguous cases → verdict is **NEEDS DISCUSSION**
- Industry-Standard Score is informational; it does NOT gate the verdict (unless it's catastrophically low, e.g., <60% — then verdict is **NEEDS DISCUSSION**)
- Production-Ready Score is informational; foundation tickets are EXPECTED to be partial

## Step 4: Persist the report

If the user invoked `/review` for a branch with an open Linear ticket reference (`APP-NNN` in the branch name or commit), the compiled report SHOULD be posted as a comment on that ticket via `mcp__linear__save_comment` so the score history is auditable. Do this only on explicit request or when running inside `/orchestrate`; do not silently spam tickets.

## Step 5: Post-Gemini gate marker

If the most recent commit on the branch (HEAD) has a subject line containing `Gemini` (case-insensitive — covers `Gemini fixes`, `Gemini R{N}`, `Gemini round 1`, etc.), and this `/review` run produced **zero findings across both agents**, then create the marker file that the `enforce-review-after-gemini.sh` push hook checks:

```bash
mkdir -p /tmp/claude_gates && touch /tmp/claude_gates/post_gemini_review_done
```

Why: the hook gates `git push` / `make push` / `gh pr create` after every Gemini-fix commit. Without the marker, the next push is denied — forcing /review to re-run after every Gemini round (per `feedback_review_after_gemini_fix.md`). The marker is consumed (deleted) on the next push, so the gate fires fresh on every Gemini round.

Only create the marker when:

- The HEAD commit subject mentions Gemini (otherwise the gate doesn't fire and the marker is irrelevant)
- BOTH review agents reported zero findings in this run
- Findings count is MEASURED from the agents' actual output (cited agent IDs), not estimated

If findings exist, do NOT create the marker — the loop must converge before the next push.
