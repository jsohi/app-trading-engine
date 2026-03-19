# Multi-Agent Implementation Plan — Trading Engine

## Context

Plan the sequence, agent roles, and verification strategy for implementing 50 Linear issues (APP-5 through APP-54) using AI coding agents. The project is greenfield (no code exists). The goal is to maximize parallelism, ensure quality via review/test agents, and deliver a working trading engine end-to-end.

---

## Agent Roles

### 1. Implementer Agent
- Writes code for a single issue
- Runs in an **isolated worktree** (git branch per issue)
- Has full context: issue description, CLAUDE.md, dependency code from merged branches
- Creates a PR when done

### 2. Reviewer Agent
- Reviews PRs created by Implementer agents
- Checks: correctness, patterns consistency, CLAUDE.md conventions, security, performance
- Posts review comments or approves
- Flags: allocation in hot path, non-deterministic code in cluster, floating-point instead of fixed-point

### 3. Test Agent
- Runs after Implementer completes, before PR merge
- Executes: `./gradlew build`, `./gradlew test`, type-checks
- For integration test issues: spins up the full stack and runs the test suite
- Reports pass/fail with logs

### 4. Orchestrator (You / Human)
- Manages the wave sequence below
- Merges approved PRs in dependency order
- Triggers next wave when current wave's PRs are merged
- Resolves conflicts between parallel branches

---

## Implementation Waves

### Wave 1 — Foundation (1 issue, serial)
**Must complete first. Everything depends on this.**

| Issue | Title | Agent Config |
|-------|-------|-------------|
| APP-5 | Scaffold Gradle multi-module project | Implementer → Reviewer → Test (`./gradlew projects && ./gradlew build`) |

**Verification:** `./gradlew projects` shows all 14 subprojects, `./gradlew build` succeeds, CLAUDE.md exists.

---

### Wave 2 — Schema + Infrastructure (5 issues, parallel)
**All depend only on APP-5. Run 5 Implementer agents simultaneously.**

| Issue | Title | Branch | Parallel Group |
|-------|-------|--------|----------------|
| APP-6 | SBE XML schema (7 messages: 5 original + CancelOrder + MassQuote) | `feat/app-6-sbe-schema` | A |
| APP-17 | Standalone Aeron Media Driver | `feat/app-17-media-driver` | B |
| APP-19 | Domain event SBE message types | `feat/app-19-event-messages` | A (after APP-6) |
| APP-28 | Pricing SBE messages | `feat/app-28-pricing-messages` | A (after APP-6) |
| APP-44 | FX product types + NoLegs schema | `feat/app-44-fx-multileg-schema` | A (after APP-6) |

**Note:** APP-19, APP-28, APP-44 all modify `trading-schema.xml` so they must be **sequenced within Group A**: APP-6 → APP-19 → APP-28 → APP-44 (each building on the previous). APP-17 is fully independent.

**Verification per issue:**
- APP-6: `./gradlew :messages:generateCodecs && ./gradlew :messages:compileJava` — 7 encoder/decoder pairs generated (QuoteRequest, Quote, QuoteRequestReject, NewOrderSingle, ExecutionReport, CancelOrderRequest, MassQuote)
- APP-17: `./gradlew :media-driver:run --args="--aeron-dir=$TMPDIR/aeron-test"` — CnC file created
- APP-19: `./gradlew :messages:generateCodecs` — 10 event + 1 snapshot message codecs (OrderCreated, OrderRejected, OrderFilled, OrderCancelled, QuoteRequested, QuoteCreated, QuoteRejected, QuoteExpired, PriceRequested, PriceReceived, SnapshotHeader)
- APP-28: `./gradlew :messages:generateCodecs` — 4 pricing message codecs
- APP-44: `./gradlew :messages:generateCodecs` — ProductTypeEnum, TenorEnum, NoLegs groups generated

---

### Wave 3 — Core Utilities (6 issues, parallel)
**Depend on schema (APP-6). Run after Wave 2 Group A completes.**

| Issue | Title | Parallel? |
|-------|-------|-----------|
| APP-7 | IdGenerator + OrderBook | Yes |
| APP-10 | FixToSbeTranslator + SbeToFixTranslator | Yes |
| APP-20 | EventSequencer | Yes (after APP-7) |
| APP-21 | EventJournal | Yes |
| APP-24 | Projections module + Projection interface | Yes |
| APP-11 | Unit tests for FIX-SBE translators | After APP-10 |

**Verification:** `./gradlew :cluster:test` (APP-7, APP-20), `./gradlew :gateway:test` (APP-10, APP-11), `./gradlew :projections:compileJava` (APP-24)

---

### Wave 4 — Cluster Service + Gateway (6 issues, partially parallel)

| Issue | Title | Depends On | Parallel? |
|-------|-------|-----------|-----------|
| APP-8 | TradingClusteredService | APP-7 | Yes |
| APP-12 | ClusterClient + EgressListener | APP-10 | Yes |
| APP-9 | Unit tests for cluster service | APP-8 | After APP-8 |
| APP-13 | FixGateway + FixSessionHandler | APP-10, APP-12 | After APP-12 |
| APP-14 | ClusterNodeLauncher (3-node) | APP-8, APP-17 | After APP-8 |
| APP-25 | OrderProjection + PositionProjection | APP-24 | Yes |

**Agent strategy:** Run APP-8, APP-12, APP-25 in parallel. Then APP-9, APP-13, APP-14 in a second sub-wave.

---

### Wave 5 — Launchers + Services (6 issues)

| Issue | Title | Depends On |
|-------|-------|-----------|
| APP-15 | GatewayLauncher + TradingEngineLauncher | APP-13, APP-14 |
| APP-22 | Refactor to CommandHandler/EventSink pattern | APP-8, APP-20, APP-21 |
| APP-26 | QuoteProjection + QueryService | APP-25 |
| APP-29 | Pricing Service | APP-14, APP-28 |
| APP-30 | Orchestrator + RFQ state machine | APP-12, APP-13, APP-28 |
| APP-39 | FIX Client Bridge | APP-5 (can start earlier) |

**Parallel groups:**
- Group A: APP-15, APP-29, APP-30 (launcher + services)
- Group B: APP-22, APP-26 (event sourcing + projections)
- Group C: APP-39 (independent — can actually start in Wave 2)

---

### Wave 6 — Integration Tests + Wiring (5 issues)

| Issue | Title | Depends On |
|-------|-------|-----------|
| APP-16 | Integration: FIX NOS → Cluster → ExecReport | APP-15 |
| APP-31 | Wire Gateway → Orchestrator → Pricing → Cluster | APP-29, APP-30 |
| APP-32 | Unit tests: Pricing, Orchestrator, RFQ | APP-29, APP-30 |
| APP-23 | Integration: gapless event sequencing | APP-22, APP-18 |
| APP-18 | Integration: leader failover | APP-16 |

**Agent strategy:** APP-16 first (validates basic E2E), then APP-31, then APP-18, APP-23, APP-32 in parallel.

**Verification:** `./gradlew :integration-tests:test` — all tests pass within 90s timeout.

---

### Wave 7 — Web UI (6 issues, sequential pipeline)

| Issue | Title | Depends On |
|-------|-------|-----------|
| APP-34 | SBE TypeScript code generator | APP-6, APP-19, APP-28 |
| APP-35 | Babl WebSocket server | APP-14, APP-19 |
| APP-36 | Web Worker + RxJS plumbing | APP-34, APP-35 |
| APP-37 | React + AG Grid streaming blotters | APP-36 |
| APP-42 | Event Log viewer panel | APP-36, APP-37 |
| APP-40 | RFQ trading panel + FIX log | APP-37, APP-39 |

**Parallel:** APP-34 and APP-35 simultaneously. Then APP-36 → APP-37 → (APP-42 ∥ APP-40).

---

### Wave 8 — FX Multi-Leg Extensions (5 issues, parallel)

| Issue | Title | Depends On |
|-------|-------|-----------|
| APP-45 | Update FIX-SBE translators for FX | APP-10, APP-44 |
| APP-46 | Update CommandHandlers for multi-leg | APP-22, APP-44 |
| APP-47 | Update Orchestrator for multi-product | APP-30, APP-44 |
| APP-48 | Update projections for multi-leg | APP-25, APP-26, APP-44 |
| APP-33 | Integration: full RFQ flow via Orchestrator | APP-31 |

**All 5 can run in parallel** since they touch different modules.

---

### Wave 9 — Observability (4 issues)

| Issue | Title | Depends On |
|-------|-------|-----------|
| APP-41 | EventLogger module | APP-19, APP-24 |
| APP-49 | Micrometer metrics on EventLogger | APP-41 |
| APP-50 | Loki log shipping | APP-41 |
| APP-51 | AeronMetricsAgent | APP-17 |

**Parallel:** APP-41 and APP-51 simultaneously. Then APP-49 ∥ APP-50 after APP-41.

---

### Wave 10 — Final Assembly (4 issues)

| Issue | Title | Depends On |
|-------|-------|-----------|
| APP-43 | Integration: EventLogger captures all events | APP-41, APP-22 |
| APP-27 | Integration: CQRS read models consistent | APP-26, APP-22 |
| APP-52 | Docker Compose observability stack | APP-49, APP-50, APP-51 |
| APP-53 | Full Stack Dev Launcher (`devAll`) | APP-15, APP-29, APP-30, APP-35, APP-39 |

---

### Wave 11 — Final Deliverable (2 issues)

| Issue | Title | Depends On |
|-------|-------|-----------|
| APP-54 | Docker Compose full stack | APP-52, APP-53 |
| APP-38 | Integration: E2E WebSocket streaming | APP-31, APP-34, APP-35, APP-37 |

---

## Multi-Agent Best Practices

### 0. No Direct Pushes to Main
- **NEVER push directly to `main`** — all changes go through feature branches + PRs
- Branch naming: `feat/app-{N}-short-description`
- Only the Orchestrator merges approved PRs into `main`
- A pre-push git hook enforces this locally (see `.githooks/pre-push`)
- This is enforced by convention — treat it as a hard rule

### 1. One Issue = One Branch = One Agent
- Each Implementer agent works in an **isolated git worktree**
- Branch naming: `feat/app-{N}-short-description`
- Agent gets: issue description + CLAUDE.md + all merged code from prior waves
- Never let two agents edit the same file simultaneously

### 2. Schema Issues Are Serial
APP-6 → APP-19 → APP-28 → APP-44 all modify `trading-schema.xml`. These MUST be sequential within their wave to avoid merge conflicts.

### 3. Review Agent Checklist
Every PR gets a Reviewer agent that checks:
- [ ] No heap allocation in cluster hot path (use flyweight pattern)
- [ ] No `java.util.*` collections in cluster/gateway (use Agrona)
- [ ] No floating-point for prices/quantities (use int64 fixed-point × 10^8)
- [ ] No wall-clock time in cluster service (use cluster timestamp)
- [ ] No randomness in cluster service (deterministic for log replay)
- [ ] All SBE fields use FIX tag numbers as field IDs
- [ ] Tests exist and pass
- [ ] No unused imports or dead code

### 4. Test Agent Strategy
```
For each PR:
  1. ./gradlew build              # Compilation check
  2. ./gradlew test               # Unit tests
  3. ./gradlew :MODULE:test       # Module-specific tests
  4. For integration issues:
     ./gradlew :integration-tests:test  # Full E2E
  5. For web-ui issues:
     cd web-ui && npm run typecheck && npm run build
```

### 5. Conflict Resolution Protocol
When two parallel agents produce conflicting changes:
1. Merge the **earlier-numbered issue** first (it's closer to the dependency root)
2. Rebase the later issue's branch onto the updated main
3. Re-run the Test agent on the rebased branch

### 6. Agent Context Management
- **CLAUDE.md** is the single source of truth for conventions — agents read it first
- Each agent gets a **focused prompt**: issue description + relevant module code only
- Don't load entire codebase into agent context — only the module being changed + its dependencies

### 7. Parallelism Budget

| Wave | Max Parallel Agents | Duration Estimate |
|------|-------------------|-------------------|
| 1 | 1 | Short (scaffold) |
| 2 | 2 (schema serial + driver parallel) | Medium |
| 3 | 5 | Medium |
| 4 | 3 + 3 (two sub-waves) | Medium |
| 5 | 3 groups | Medium |
| 6 | 1 → 4 | Long (integration) |
| 7 | 2 → 1 → 2 | Long (full UI) |
| 8 | 5 | Medium |
| 9 | 2 → 2 | Medium |
| 10 | 4 | Medium |
| 11 | 2 | Short |

**Total: 50 issues across 11 waves, ~16 serial steps on critical path.**

---

## Agent Prompt Templates

### Implementer Agent Prompt
```
You are implementing issue {APP-N} for the Trading Engine project.

## Issue Description
{paste issue description from Linear}

## Conventions (from CLAUDE.md)
- JDK 25, Gradle Kotlin DSL
- Aeron Cluster + SBE + Artio FIX
- Fixed-point pricing: int64 × 10^-8
- Agrona collections only (no java.util in hot path)
- Deterministic cluster operations (no randomness, no wall-clock)
- Event sourcing: commands → events → projections

## Your Task
1. Create branch: feat/app-{N}-{short-name}
2. Implement the code described in the issue
3. Write tests as specified in the AC
4. Run: ./gradlew :MODULE:test
5. Commit with descriptive message referencing APP-{N}

## Available Code
{list of already-merged modules/files from prior waves}
```

### Reviewer Agent Prompt
```
Review PR for APP-{N}. Check against these rules:
1. No heap allocation in hot path
2. Agrona collections only (no java.util.HashMap etc.)
3. Fixed-point pricing (int64 × 10^-8), no floating-point
4. Cluster code: deterministic, no wall-clock, no randomness
5. SBE fields use FIX tag numbers
6. Tests cover AC items
7. Code follows patterns established in prior modules
Post specific line comments for issues. Approve if clean.
```

### Test Agent Prompt
```
Run verification for APP-{N}:
1. ./gradlew build
2. ./gradlew :MODULE:test
3. {issue-specific verification command}
Report: PASS/FAIL with relevant output.
```

---

## Verification — End-to-End

After all 11 waves complete:

1. `./gradlew build` — full project compiles
2. `./gradlew test` — all unit tests pass
3. `./gradlew :integration-tests:test` — all integration tests pass
4. `./gradlew devAll` — full stack starts with single command
5. `cd web-ui && npm run dev` — UI connects, prices stream, orders flow
6. `docker compose --profile observability up` — Grafana dashboards show live data
7. Send FIX QuoteRequest → receive Quote → accept → ExecutionReport (full RFQ flow)
8. Send SWAP QuoteRequest with 2 legs → multi-leg flow works
