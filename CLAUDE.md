# Trading Engine — Developer Conventions

## Build Commands

```bash
./gradlew build                          # Full build (all modules)
./gradlew test                           # Run all unit tests
./gradlew :messages:generateCodecs       # Generate SBE codecs from trading-schema.xml
./gradlew :messages:compileJava          # Compile generated codecs
./gradlew :MODULE:test                   # Run tests for a specific module
./gradlew :integration-tests:test        # Run integration tests
./gradlew spotlessApply                  # Auto-format all source files
./gradlew spotlessCheck                  # Check formatting (CI)
./gradlew jacocoTestReport               # Generate code coverage (HTML + XML)
./gradlew dependencyCheckAnalyze         # OWASP CVE scan (fails on CVSS >= 7)
./gradlew projects                       # List all subprojects
```

## Architecture

- **Aeron Cluster** — 3-node Raft consensus for deterministic order matching
- **Artio FIX Gateway** — FIX 4.4 session handling (acceptor)
- **SBE (Simple Binary Encoding)** — zero-allocation message codec
- **CQRS + Event Sourcing** — commands → events → projections
- **External Media Driver** — shared memory transport between processes

## Module Layout

```text
messages              — SBE schema + generated codecs (no internal deps)
media-driver          — Standalone Aeron Media Driver process
cluster               — TradingClusteredService (order matching, state machine)
gateway               — Artio FIX acceptor + ClusterClient
projections           — Read-side projections (Order, Position, Quote)
pricing-service       — Dummy price generation over Aeron IPC
websocket-server      — Babl WebSocket server for browser clients
fix-client-bridge     — Artio initiator + WebSocket JSON API
event-logger          — Structured event logging + metrics
sbe-typescript-generator — SBE → TypeScript code generator
query-service         — Unified read API over projections
launcher              — Top-level process launchers (cluster + gateway + services)
integration-tests     — End-to-end tests (FIX → cluster → response)
web-ui                — React + AG Grid browser UI (Node project)
```

## Conventions

### Pricing
- **Fixed-point only**: `long` with scale factor `100_000_000L` (10^-8)
- **No floating-point** (`double`, `float`, `BigDecimal`) for prices, quantities, or amounts
- Constants: `public static final long PRICE_SCALE = 100_000_000L;`

### Cluster Service (Deterministic)
- **No wall-clock time** — use cluster timestamp from `onSessionMessage` / `onTimerEvent`
- **No randomness** — no `Math.random()`, `UUID.randomUUID()`, `SecureRandom`
- **No heap allocation in hot path** — use flyweight pattern, pre-allocated buffers
- **No `java.util.*` collections** — use Agrona: `Long2ObjectHashMap`, `Object2ObjectHashMap`, `ObjectHashSet`, `Int2ObjectHashMap`
- All operations must be deterministic for Aeron log replay

### SBE Schema
- Field `id=` values must correspond to FIX tag numbers (e.g., ClOrdID=11, OrderQty=38, Price=44, Side=54, Symbol=55)
- Schema changes to `trading-schema.xml` must be merged sequentially (no parallel merges)
- Template IDs: commands 1-19, events 100-119, snapshots 200-206 (200=SnapshotTaken, 201=Account, 202=OrderBook, 203=RfqState, 204=Position, 205=IdGenerator, 206=EventSequencer)

### Event Sourcing
- Commands are validated and produce events
- Events are the source of truth (immutable, sequenced)
- Projections consume events to build read models
- Snapshots taken periodically for fast write-model recovery (cluster state only)
- **Projections never snapshot** — they replay all events from Aeron Archive position 0 on recovery. Archive log is never truncated.
- **RFQ snapshot recovery** — after restoring RfqStateMachine from snapshot, immediately expire any RFQ in REQUESTED/QUOTED state whose TTL has elapsed relative to recovery cluster timestamp

### Testing
- Unit tests: `./gradlew :MODULE:test`
- Integration tests: `./gradlew :integration-tests:test` (spins up full 3-node cluster)
- All tests use JUnit 6

### Git
- Branch naming: `feat/app-{N}-short-description`
- No direct pushes to `main` — all changes via PRs
- Commit messages reference Linear issue: `APP-{N}: description`
- Always run `/review` before creating a PR — fix all blocking issues first

### Logging
- **Hot-path modules** (`cluster`, `gateway`): GFLog 3.0.7 — zero-allocation, builder API: `log.info().append("Order ").append(orderId).commit()`
- **Infra modules** (`launcher`, `websocket-server`, `media-driver`, etc.): Log4j2 2.25.3 Async + LMAX Disruptor — garbage-free mode with `AsyncLoggerContextSelector`
- **No SLF4J on hot path** — Aeron/Artio use native error handling (CnC counters, `ErrorHandler`), not SLF4J
- GFLog config: `src/main/resources/gflog.xml`; Log4j2 config: `src/main/resources/log4j2.xml`

## Dependencies

| Library | Version | Purpose |
|---------|---------|---------|
| Aeron | 1.50.4 | Cluster, driver, archive |
| SBE | 1.37.1 | Message codec generation |
| Artio | 0.175 | FIX 4.4 engine |
| Agrona | 2.4.0 | Off-heap collections, buffers |
| JUnit | 6.0.3 | Testing framework |
| GFLog | 3.0.7 | Zero-alloc logging (hot path) |
| Log4j2 | 2.25.3 | Async logging (infra modules) |
| Disruptor | 3.4.4 | LMAX ring buffer for Log4j2 Async |
| JDK | 25 | Runtime |
