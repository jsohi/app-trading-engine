---
description: "Industry-standard compliance scoring across 13 weighted metrics"
allowed-tools: ["Bash", "Glob", "Grep", "Read", "Agent"]
---

# Compliance — Industry-Standard Scoring Dashboard

Score the codebase against 13 weighted industry metrics for a production trading engine. Produces a single percentage (0-100%) and a tabular report with per-category breakdown.

## MANDATORY RULES

1. **NEVER mark anything as "accepted" or "out of scope"** — dev phase means everything must meet the target.
2. **Score the ENTIRE codebase** — not just the current branch's changes. This is a health check on all production code.
3. **Run ALL automated scans** — do not skip any scan even if you "already know" the result.
4. **Spawn BOTH review agents** — Agent A (Documentation + Thread-Safety) and Agent B (Allocation + Autoboxing + `final var`).

## Scoring Rubric — 13 Categories, 100 Points

| # | Category | Weight | Target | Scope |
|---|----------|--------|--------|-------|
| 1 | Test Coverage (line + branch) | 15 | >= 80% combined | All Java modules |
| 2 | Zero-Allocation Compliance | 14 | 100% | Hot-path: cluster, gateway, orchestrator, pricing-service, projections |
| 3 | Code Documentation Coverage | 12 | 100% class + method | All production Java |
| 4 | Determinism Compliance | 10 | 100% | cluster module |
| 5 | Collection Compliance | 8 | 100% | Hot-path modules |
| 6 | Autoboxing Compliance | 8 | 100% | Hot-path modules |
| 7 | FIX Protocol Compliance | 7 | 100% | trading-schema.xml |
| 8 | Formatting Compliance | 5 | Pass | All Java |
| 9 | Logging Compliance | 5 | 100% | All modules |
| 10 | Clock Discipline | 5 | 100% | All non-test modules |
| 11 | `final var` Usage | 4 | >= 90% | All production Java |
| 12 | Security (OWASP CVE) | 4 | 0 findings >= 7.0 CVSS | Full dependency tree |
| 13 | Thread-Safety Documentation | 3 | >= 90% | All production public classes |

**Formula:** `overall = sum(category_compliance_rate * category_weight)`

## Phase 1: Automated Scans (run in parallel)

Run all of these via Bash in parallel:

### Category 8: Formatting
```bash
./gradlew spotlessCheck
# Pass = 100%, Fail = 0%
```

### Category 12: Security
```bash
./gradlew dependencyCheckAnalyze
# Pass (exit 0) = 100%, Fail = 0%
```

### Category 1: Test Coverage
```bash
./gradlew test jacocoTestReport
# Then parse each module's JaCoCo XML:
# {module}/build/reports/jacoco/test/jacocoTestReport.xml
# Extract: <counter type="INSTRUCTION" missed="X" covered="Y"/>
#           <counter type="BRANCH" missed="X" covered="Y"/>
# line_coverage = covered / (covered + missed)
# branch_coverage = covered / (covered + missed)
# combined = (line_coverage * 0.6) + (branch_coverage * 0.4)
```

### Category 4: Determinism (cluster module)
```bash
# Search for non-deterministic operations in cluster production code
grep -rEn 'System\.(currentTimeMillis|nanoTime)|Instant\.now|LocalDateTime\.now|OffsetDateTime\.now|ZonedDateTime\.now|Clock\.system(UTC|DefaultZone)|Math\.random|new (Random|Date)\(|ThreadLocalRandom|SecureRandom|UUID\.randomUUID' \
  cluster/src/main --include="*.java"
# 0 hits = 100%, each hit = violation
```

### Category 5: Collection Compliance (hot-path modules)
```bash
grep -rEn 'import java\.util\.(concurrent\.)?(\*|HashMap|ArrayList|LinkedList|HashSet|TreeMap|LinkedHashMap|ArrayDeque|PriorityQueue|EnumSet|ConcurrentHashMap|CopyOnWriteArrayList|Map;|List;|Set;|Collection;)' \
  cluster/src/main gateway/src/main orchestrator/src/main pricing-service/src/main projections/src/main \
  --include="*.java"
# 0 hits = 100%, each file with hits = violation
# Exceptions: java.util.Objects (utility), java.util.Arrays (sort), java.util.zip.CRC32C
```

### Category 9: Logging Compliance
```bash
# SLF4J anywhere (violation)
grep -rl 'import org\.slf4j' */src/main --include="*.java"

# Log4j2 in hot-path modules (violation — should use GFLog)
grep -rl 'import org\.apache\.logging\.log4j' \
  cluster/src/main gateway/src/main orchestrator/src/main pricing-service/src/main projections/src/main \
  messages/src/main fix-codecs/src/main event-logger/src/main \
  --include="*.java"

# GFLog in infra modules (violation — should use Log4j2)
grep -rl 'import com\.epam\.deltix\.gflog' \
  launcher/src/main websocket-server/src/main reference-data/src/main \
  query-service/src/main fix-client-bridge/src/main media-driver/src/main \
  --include="*.java"
# 0 total violations = 100%
```

### Category 10: Clock Discipline (all non-test code)
```bash
grep -rEn 'System\.(currentTimeMillis|nanoTime)|Instant\.now|LocalDateTime\.now|OffsetDateTime\.now|ZonedDateTime\.now|new Date\(\)|Clock\.system(UTC|DefaultZone)' \
  */src/main --include="*.java" | grep -v 'TradingClocks\.java\|OffsetEpochNanoClock'
# 0 hits = 100%, each hit = violation
# Exception: TradingClocks.java itself (it's the blessed wrapper)
```

## Phase 2: Agent Reviews (spawn 2 agents in parallel)

### Agent A: Documentation + Thread-Safety Audit (Categories 3, 13)

Prompt for Agent A:
```
Audit ALL production Java files (src/main/java, excluding build/ and generated code) for documentation compliance.

For EVERY public class/interface/enum/record:
1. Does it have a class-level Javadoc (/** ... */ before the declaration)?
2. Does the Javadoc include a threading model statement? (e.g., "Not thread-safe", "Thread-safe via CAS", "single-threaded cluster duty cycle")
3. Does it include allocation behavior for hot-path classes? (e.g., "Zero allocation after construction")

For EVERY public method:
1. Does it have method-level Javadoc?
2. Does it include @param, @return, @throws where applicable?

Report:
- Total public classes scanned
- Classes WITH class-level Javadoc (count + %)
- Classes WITH threading annotation (count + %)
- Total public methods scanned
- Methods WITH method-level Javadoc (count + %)
- List of classes MISSING Javadoc (file path + class name)
- List of classes MISSING threading annotation (file path + class name)
```

### Agent B: Allocation + Autoboxing + `final var` Audit (Categories 2, 6, 11)

Prompt for Agent B:
```
Audit hot-path production code (cluster, gateway, orchestrator, pricing-service, projections — src/main/java only) for allocation, autoboxing, and final var compliance.

ZERO-ALLOCATION CHECK:
For every file in hot-path modules, check for:
- `new` keyword outside constructors/static initializers (except pre-allocated buffers)
- String concatenation with `+` in non-constant expressions
- `String.format`, `String.getBytes`, `Arrays.asList`, `List.of`, `Map.of`, `Set.of`
- `stream()`, `collect()`, `.map()`, `.filter()`, `Optional.of()`
- Lambda expressions that capture local variables (allocate a closure)
- **Garbage-creating iterator patterns:**
  - Enhanced for-each (`for (final var x : collection)`) — allocates Iterator
  - `collection.iterator()` — allocates Iterator (use index-based or Agrona reusable)
  - `Iterable.forEach(lambda)` — allocates closure if capturing locals
  - `Map.entrySet()` — allocates Entry wrappers per iteration
  - `String.split()`, `Pattern.compile()` on hot path
  - `toArray()`, `Arrays.copyOf()`, `Arrays.stream()`
  - Varargs calls (`method(T... args)`) — allocate Object[] per invocation
Report: total hot-path methods, methods with violations, compliance %.

AUTOBOXING CHECK:
For every file in hot-path modules:
- Wrapper types as field/param/return types where primitive would do (Long, Integer, Boolean)
- `Long.valueOf()`, `Integer.valueOf()` calls
- Enhanced-for over Agrona primitive collections (boxes via Iterator<Long>.next())
Report: total violations, file:line for each.

FINAL VAR CHECK:
Sample 20 production files across ALL modules (not just hot-path). For each:
- Count local variable declarations using `final var` (correct for references)
- Count local variable declarations using explicit reference type (violation)
- Count local variable declarations using explicit primitive type with `final` (correct)
- Count non-final local variables (violation unless reassigned)
Report: total declarations sampled, correct count, violation count, compliance %.
```

## Phase 3: Compile Report

Combine Phase 1 automated results + Phase 2 agent results into the final table.

### Scoring per Category

| Category | How to Score |
|----------|-------------|
| 1. Test Coverage | `combined_coverage * 15` |
| 2. Zero-Allocation | `(1 - violations/total_methods) * 14` |
| 3. Documentation | `((class_javadoc_rate * 4) + (method_javadoc_rate * 4) + (threading_rate * 4))` |
| 4. Determinism | `violations == 0 ? 10 : max(0, 10 - violations*2)` |
| 5. Collections | `(1 - violating_files/total_hot_path_files) * 8` |
| 6. Autoboxing | `(1 - confirmed_violations / total_checked_items) * 8` |
| 7. FIX Protocol | `(correct_field_ids / total_fix_fields) * 7` |
| 8. Formatting | `spotlessCheck_pass ? 5 : 0` |
| 9. Logging | `total_violations == 0 ? 5 : max(0, 5 - total_violations)` |
| 10. Clock | `violations == 0 ? 5 : max(0, 5 - violations)` |
| 11. `final var` | `compliance_rate * 4` |
| 12. Security | `owasp_pass ? 4 : 0` |
| 13. Thread-Safety | `threading_doc_rate * 3` |

### Status Thresholds

| Status | Meaning |
|--------|---------|
| **PASS** | Meets or exceeds target |
| **SUBPAR** | Below target but > 0; must fix (dev phase: nothing accepted) |
| **FAIL** | Critical deficiency; score = 0 or binary check failed |

### Output Format

```
═══════════════════════════════════════════════════════════════
              TRADING ENGINE COMPLIANCE REPORT
              Date: {YYYY-MM-DD}  Branch: {branch}
═══════════════════════════════════════════════════════════════

 #  │ Category                    │ Weight │ Score  │ %     │ Status
────┼─────────────────────────────┼────────┼────────┼───────┼────────
 1  │ Test Coverage               │   15   │ XX.X   │ XX.X% │ PASS
 2  │ Zero-Allocation Compliance  │   14   │ XX.X   │ XX.X% │ PASS
 3  │ Code Documentation Coverage │   12   │ XX.X   │ XX.X% │ PASS
 4  │ Determinism Compliance      │   10   │ XX.X   │ XX.X% │ PASS
 5  │ Collection Compliance       │    8   │ XX.X   │ XX.X% │ PASS
 6  │ Autoboxing Compliance       │    8   │ XX.X   │ XX.X% │ PASS
 7  │ FIX Protocol Compliance     │    7   │ XX.X   │ XX.X% │ PASS
 8  │ Formatting Compliance       │    5   │ XX.X   │ XX.X% │ PASS
 9  │ Logging Compliance          │    5   │ XX.X   │ XX.X% │ PASS
 10 │ Clock Discipline            │    5   │ XX.X   │ XX.X% │ PASS
 11 │ final var Usage             │    4   │ XX.X   │ XX.X% │ PASS
 12 │ Security (OWASP CVE)       │    4   │ XX.X   │ XX.X% │ PASS
 13 │ Thread-Safety Documentation │    3   │ XX.X   │ XX.X% │ PASS
────┼─────────────────────────────┼────────┼────────┼───────┼────────
    │ OVERALL                     │  100   │ XX.X   │ XX.X% │
═══════════════════════════════════════════════════════════════

ITEMS REQUIRING ACTION:
────────────────────────────────────────────────────────────────
 [SUBPAR] #{N} {Category} ({X}%)
   - {file}:{line} — {description}
   Details: {count} of {total} {items} do not meet target
────────────────────────────────────────────────────────────────

ACCEPTED ITEMS:     0  (dev phase: nothing should be accepted)
OUT OF SCOPE ITEMS: 0  (dev phase: nothing should be out of scope)
```

Print the report to the conversation AND return it so the caller (e.g., `/orchestrate`) can include it in the session report file.
