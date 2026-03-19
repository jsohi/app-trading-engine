---
description: "Review branch changes against trading engine rules before pushing"
allowed-tools: ["Bash", "Glob", "Grep", "Read", "Agent"]
---

# Trading Engine Branch Review

Review all changes on the current branch before pushing. This wraps a general code review with trading-engine-specific checks that are **blocking**.

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

1. **No heap allocation in cluster hot path** — flag any `new` keyword inside cluster service code (except in snapshot restore or startup). Look for `new ArrayList`, `new HashMap`, `new String`, boxing of primitives, string concatenation with `+`, autoboxing, `String.format`, `Arrays.asList`, `List.of`, `Map.of`, `stream()`, `collect()`, lambdas that capture variables.

2. **No `java.util.*` collections in cluster/gateway modules** — flag any import of `java.util.HashMap`, `java.util.ArrayList`, `java.util.Map`, `java.util.List`, `java.util.Set`, etc. in files under cluster or gateway packages. Must use Agrona: `Object2ObjectHashMap`, `Long2ObjectHashMap`, `Int2ObjectHashMap`, `ObjectHashSet`, etc.

3. **No floating-point for prices/quantities** — flag any `double`, `float`, `BigDecimal`, or `Double` used for price, quantity, amount, or notional fields. Must use `long` with fixed-point scaling (× 10⁻⁸).

4. **No wall-clock time in cluster service** — flag `System.currentTimeMillis()`, `System.nanoTime()`, `Instant.now()`, `LocalDateTime.now()`, `Clock.systemUTC()`, or any `java.time` clock in cluster service code. Must use the cluster timestamp from `onSessionMessage` / `onTimerEvent`.

5. **No randomness in cluster service** — flag `Math.random()`, `new Random()`, `ThreadLocalRandom`, `UUID.randomUUID()`, `SecureRandom` in cluster service code. Must be deterministic for Aeron log replay.

6. **SBE field IDs must use FIX tag numbers** — if `trading-schema.xml` is modified, check that field `id=` values correspond to FIX tag numbers (e.g., ClOrdID=11, OrderQty=38, Price=44, Side=54, Symbol=55, OrdType=40, etc.).

7. **Schema sequential merge warning** — if `trading-schema.xml` is modified, flag a warning that schema changes must be merged sequentially to avoid conflicts.

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
