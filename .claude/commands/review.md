---
description: "Review branch changes against trading engine rules before pushing"
allowed-tools: ["Bash", "Glob", "Grep", "Read", "Agent"]
---

# Trading Engine Branch Review

Review all changes on the current branch before pushing. This wraps a general code review with trading-engine-specific checks that are **blocking**.

## MANDATORY EXECUTION RULES — READ FIRST

These rules are non-negotiable and OVERRIDE any judgment about efficiency, context reuse, or delta size:

1. **ALWAYS spawn both review agents in parallel via the Agent tool** — Agent A (Trading Engine Constraint Checker) AND Agent B (General Code Quality Review). Never skip either. Never do "just one this time."
2. **NEVER do an inline delta-only review** — even if you reviewed this exact branch seconds ago, even if the delta since the last review is a one-line comment, even if you "already have full context," even if the changes are "obviously clean." Spawn fresh agents every single invocation.
3. **NEVER offer to shortcut** — do not propose "I can do a quick delta check instead" or "the previous review already covered this." Go straight to Step 1 → Step 2 (parallel agent spawn) → Step 3 without asking.
4. **The value of /review is the independent second opinion from fresh agent context** — your own in-conversation review is not a substitute and defeats the purpose of invoking the command.

If you catch yourself about to write "the delta is small so..." or "I already reviewed this..." — stop and spawn the agents.

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

## Step 3: Compile Results

Combine both agent reports into a single summary:

```markdown
## Branch Review: `<branch-name>`

**Commits:** <count> commits on branch
**Files changed:** <count>

### Blocking Issues (Trading Engine Rules)
<list from Agent A, or "None found">

### Code Quality Issues
<list from Agent B, grouped by severity>

### Verdict
**READY TO PUSH** | **FIX BEFORE PUSHING** | **NEEDS DISCUSSION**
```

Rules:
- Any blocking issue from Agent A → verdict is **FIX BEFORE PUSHING**
- Any critical code quality issue → verdict is **FIX BEFORE PUSHING**
- Only warnings/suggestions → verdict is **READY TO PUSH**
- Ambiguous cases → verdict is **NEEDS DISCUSSION**
