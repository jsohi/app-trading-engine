# Zero-Allocation Strategy

## Why It Matters

In a low-latency trading engine, the primary enemy is **unpredictable pauses**. The #1 source of those in Java is garbage collection (GC).

- **GC pauses cause tail latency spikes.** A single young-gen collection can stall the cluster duty cycle for 1-10ms — the difference between your order hitting the book first or being stale. These pauses are non-deterministic: you cannot predict when they will occur.
- **Aeron Cluster requires deterministic replay.** If a node crashes, it replays the log to rebuild state. GC pauses during replay slow recovery and can trigger leader election timeouts (default heartbeat budget: 500ms).
- **External media driver topology.** The media driver runs as a separate JVM process specifically to isolate its GC from application logic. The same principle applies within the application: eliminate allocation so GC never runs.

The goal is not raw speed — it is **eliminating the worst case**. In trading, you are measured by your slowest tick, not your fastest.

## Allocation Zones

| Zone | Modules | Rule | Rationale |
|------|---------|------|-----------|
| Strict zero-alloc | `cluster`, `gateway` | No `new` after construction on hot path | Deterministic replay, sub-100us latency |
| Bounded per-entity | `projections` | One object per order/position, no per-event alloc | Read model performance, bounded memory |
| Free | `launcher`, `media-driver`, `websocket-server`, `reference-data` | Standard Java permitted | Startup/config path, not latency-sensitive |

**Strict zero-alloc** means: after the constructor returns, the hot-path methods (`onSessionMessage`, `doWork`, `onMessage`, `translate*`) must not allocate a single byte on the Java heap. All buffers, flyweights, scratch arrays, and collections are pre-allocated during construction and reused on every call.

**Bounded per-entity** means: projections may allocate one view object per order, position, or account (e.g., `OrderView`, `PositionView`). Per-event allocation (allocating on every event consumed) is prohibited. The `ByteArrayKey.emptyForLookup()` probe pattern enables zero-allocation map lookups on the event-processing path.

## Patterns

### Flyweight Pattern (SBE decoders/encoders)

SBE (Simple Binary Encoding) generates flyweight classes that wrap a `DirectBuffer` at a given offset without creating new objects. A single decoder/encoder instance is pre-allocated at construction and re-wrapped on each message:

```java
// Pre-allocated once at construction
private final NewOrderSingleDecoder nosDecoder = new NewOrderSingleDecoder();

// Re-wrapped on every message — zero allocation
nosDecoder.wrap(buffer, offset + HDR_LEN, blockLength, version);
long orderQty = nosDecoder.orderQty();  // Direct read from buffer, no object created
```

`TradingClusteredService` holds one `MessageHeaderDecoder` reused for every dispatch. A separate `journalHeaderDecoder` is used by the journal path to avoid clobbering the dispatch decoder's wrap state.

### Object Pool with LIFO Free-List (OrderBook)

`OrderBook` manages a fixed-capacity pool of `OrderState` objects:

- **Pre-allocated array** of `OrderState[capacity]` at construction — no per-order allocation
- **LIFO free-list** via `int[] freeList` and `freeTop` stack pointer — O(1) acquire/release
- **Primitive-keyed** `Long2ObjectHashMap` — no boxing on lookup
- Pool size: 65,534 (`MAX_CAPACITY`, matches SBE `uint16` group count limit)

```java
// O(1) acquire — pops from free-list stack, no allocation
OrderState state = orderBook.acquire(orderKey);

// O(1) release — pushes back to free-list stack
orderBook.release(orderKey);

// O(1) lookup — primitive long key, no boxing
OrderState existing = orderBook.get(orderKey);
```

### Primitive Collections (Agrona)

Agrona provides collections keyed by primitives, avoiding the autoboxing overhead of `java.util.HashMap<Long, V>`:

| Agrona Collection | Replaces | Benefit |
|---|---|---|
| `Long2ObjectHashMap` | `HashMap<Long, V>` | No `Long` boxing on put/get |
| `Int2ObjectHashMap` | `HashMap<Integer, V>` | No `Integer` boxing |
| `Long2LongHashMap` | `HashMap<Long, Long>` | No boxing at all — primitive-to-primitive |
| `ObjectHashSet` | `HashSet<V>` | No `Entry` allocation on add |

**Iterator warning:** Agrona map iterators are reusable flyweights. They must be consumed immediately within a single loop and never stored in a field or collection. In some Agrona versions, `for (T item : map.values())` allocates a new `ValueIterator` — prefer `map.values().forEach()` or `forEachLong` to avoid this trap.

### Scratch Buffers

SBE fixed-length character fields (e.g., `ClOrdID char[20]`, `Symbol char[8]`) are decoded into pre-allocated `byte[]` scratch arrays:

```java
// Pre-allocated once at construction — reused on every message
private final byte[] clOrdIdScratch = new byte[20];
private final byte[] symbolScratch = new byte[8];

// Zero-alloc decode — copies bytes into existing array
nosDecoder.getClOrdId(clOrdIdScratch, 0);
nosDecoder.getSymbol(symbolScratch, 0);
```

No `String` objects are created. Scratch arrays are zero-padded to the SBE field length. The `trimTrailingZeros` helper finds the logical string length without allocation.

### Probe Keys (ByteArrayKey)

`ByteArrayKey` enables zero-allocation `HashMap` lookups via a mutable probe key:

```java
// Pre-allocated probe key — reused for every lookup
private final ByteArrayKey probeKey = ByteArrayKey.emptyForLookup(16);

// Zero-alloc lookup: set probe, look up, consume immediately
probeKey.set(accountCodeBytes, 0, accountCodeLen);
AccountState account = accountMap.get(probeKey);
```

**Critical rule:** Probe keys **MUST NOT** be stored as map keys. They are mutable and shared. Use `ByteArrayKey.copyOf()` (which allocates) only when inserting a new map entry.

### Fixed-Point Arithmetic

All prices, quantities, and monetary amounts use `long` with a scale factor of `100_000_000L` (10^-8):

```java
public static final long PRICE_SCALE = 100_000_000L;

// 1.5 USD = 150_000_000L
long price = 150_000_000L;

// No double, no float, no BigDecimal anywhere in the codebase
```

`DecimalFloat` (from Artio) is used only at the FIX wire boundary for encoding/decoding FIX price fields. It is never used for computation.

### Zero-Alloc Logging (GFLog)

Hot-path modules (`cluster`, `gateway`) use GFLog's builder API:

```java
// Zero allocation — no varargs, no String.format(), no boxing
log.info().append("Order ").append(orderId).append(" filled at ").append(price).commit();
```

Infrastructure modules (`launcher`, `websocket-server`, `media-driver`) use Log4j2 Async with `AsyncLoggerContextSelector` and LMAX Disruptor in garbage-free mode.

**No SLF4J on the hot path.** Aeron and Artio use native error handling (CnC counters, `ErrorHandler`), not SLF4J.

## Library Choices

| Library | Version | Role | Why This One |
|---------|---------|------|--------------|
| SBE | 1.37.1 | Message codec | Zero-copy flyweight encode/decode, fixed-size messages, no reflection |
| Agrona | 2.4.0 | Collections + buffers | Primitive-keyed maps (no boxing), `UnsafeBuffer`, reusable iterators |
| GFLog | 3.0.7 | Hot-path logging | Zero-alloc builder API, no SLF4J overhead, no varargs |
| Aeron | 1.50.4 | Transport + cluster | Shared-memory IPC, no serialization overhead, deterministic replay |
| Artio | 0.175 | FIX 4.4 engine | Pre-allocated FIX codecs, reusable decoder/encoder flyweights |

## Verification

### Allocation Regression Tests (NoAllocationTest)

Two test suites guard against allocation regressions:

**Gateway (`NoAllocationTest`):**
- FIX-to-SBE NewOrderSingle translation path
- SBE-to-FIX ExecutionReport translation path
- Technique: `GarbageCollectorMXBean.getCollectionCount()` tripwire
- 5,000 JIT warmup iterations + 10,000 measured iterations
- Gated by `-DrunAllocTests=true` to avoid CI flakiness

**Cluster (`ClusterNoAllocationTest`):**
- NewOrderSingle happy path (OrderCreated event)
- NewOrderSingle reject path (OrderRejected event)
- Reference-data dispatch path (LoadCurrency command)
- Unknown templateId path (silent drop)
- Primary technique: Epsilon GC (`-XX:+UseEpsilonGC`) — deterministic, no false passes
- Fallback: GC bean tripwire with 100K measured iterations

```bash
# Run both suites
./gradlew :gateway:test :cluster:test --tests '*NoAllocationTest' -DrunAllocTests=true
```

### JMH Benchmarks (`benchmarks` module — planned, APP-67)

| Benchmark | Mode | Target |
|---|---|---|
| `ClusterBenchmark` | SampleTime | p50 <10us, p99 <50us, p999 <100us |
| `ClusterThroughputBenchmark` | Throughput | >100K NOS/sec |
| `SbeBenchmark` | AverageTime | <200ns per encode/decode |
| `TranslatorBenchmark` | AverageTime | <500ns per translation |
| `OrderBookBenchmark` | AverageTime | O(1) at 100/1K/10K book sizes |

```bash
# Run all benchmarks (fork=2, warmup=5, measurement=15)
./gradlew :benchmarks:jmh

# Verify zero allocation rate
./gradlew :benchmarks:jmh -PjmhArgs="-prof gc"

# Flame graph with async-profiler
./gradlew :benchmarks:jmh -PjmhArgs="-prof async:libPath=/path/to/libasyncProfiler.so;output=flamegraph"
```

### Diagnosing Allocation Regressions

When a `NoAllocationTest` fails, use these tools to find the allocating code:

1. **JFR allocation profiling:**
   ```
   java -XX:StartFlightRecording=filename=alloc.jfr,settings=profile ...
   ```
   Look for `jdk.ObjectAllocationInNewTLAB` and `jdk.ObjectAllocationOutsideTLAB` events in JDK Mission Control.

2. **JMH GC profiler:**
   ```bash
   ./gradlew :benchmarks:jmh -PjmhArgs="-prof gc"
   ```
   Reports `gc.alloc.rate` (MB/sec) and `gc.alloc.rate.norm` (bytes/op). Both must be 0 for hot-path benchmarks.

3. **async-profiler allocation mode:**
   ```bash
   ./gradlew :benchmarks:jmh -PjmhArgs="-prof async:event=alloc;output=flamegraph"
   ```
   Produces a flame graph showing allocation call stacks — the fastest way to find the offending `new`.

4. **JIT compilation log:**
   ```
   -XX:+UnlockDiagnosticVMOptions -XX:+LogCompilation
   ```
   Check for inlining failures that prevent escape analysis. If C2 cannot inline a method, it cannot scalar-replace objects returned from it.

## Rules for Contributors

### Never on hot path

- `new` (any object creation)
- `java.util.*` collections (`HashMap`, `ArrayList`, `HashSet`, etc.)
- `String` creation or concatenation (`"Order " + orderId`)
- `double`, `float`, `BigDecimal` for prices/quantities/amounts
- `System.currentTimeMillis()`, `Instant.now()`, `LocalDateTime.now()`
- Autoboxing (`Integer`, `Long`, `Double` wrapper types)
- `String.format()`, `String.getBytes()`, varargs methods
- `Arrays.asList()`, `List.of()`, `Map.of()` (allocate wrapper objects)
- `EnumSet.of()` (allocates — use raw bitmask instead)
- `Optional.of()` (allocates — return `null` or sentinel on hot path)

### Always on hot path

- Pre-allocated SBE flyweights, re-wrapped per message
- Agrona primitive-keyed collections (`Long2ObjectHashMap`, `Int2ObjectHashMap`)
- `byte[]` scratch buffers for character field decode
- `long` fixed-point for all monetary values (scale factor `100_000_000L`)
- Cluster timestamp from `onSessionMessage` / `onTimerEvent` — never wall clock
- GFLog builder API for logging (`log.info().append(...).commit()`)

### Common allocation traps

| Trap | Why it allocates | Zero-alloc alternative |
|------|-----------------|----------------------|
| `for (T item : map.values())` on Agrona maps | Some versions create a new `ValueIterator` | `map.values().forEach()` or `forEachLong` |
| `"Order " + orderId` | Creates `StringBuilder` + `String` | GFLog builder: `log.info().append("Order ").append(orderId)` |
| `Arrays.asList(a, b, c)` | Allocates wrapper `List` + `Object[]` | Pass elements directly or use pre-allocated array |
| `EnumSet.of(A, B)` | Allocates `RegularEnumSet` | Use `int` bitmask: `FLAG_A \| FLAG_B` |
| `Optional.of(value)` | Allocates `Optional` wrapper | Return `null` or sentinel value |
| `ByteArrayKey.copyOf()` | Allocates new key (intentional for map insert) | Use `ByteArrayKey.emptyForLookup()` probe for reads |

### Class Javadoc annotation

Every hot-path class MUST document in its class-level Javadoc:

- **Allocation behavior:** "Zero allocation after construction" or explicitly note where allocation occurs and why
- **Threading model:** "Not thread-safe — single-threaded cluster duty cycle only" or equivalent

### Escape analysis awareness

- Keep method call depth shallow on hot path. C2's default inlining limit is `-XX:MaxInlineLevel=15`. Beyond that depth, objects cannot be scalar-replaced.
- Avoid extracting helper methods that return newly created objects. If C2 cannot inline the method, the object escapes and must be heap-allocated.
- Prefer passing primitives over wrapper objects.

### Contributor workflow

1. **Every new `CommandHandler` implementation MUST have a corresponding allocation test** added to `ClusterNoAllocationTest`. This is a merge-blocking requirement.
2. **Run allocation tests locally** before every PR that touches `cluster/` or `gateway/`:
   ```bash
   ./gradlew :gateway:test :cluster:test --tests '*NoAllocationTest' -DrunAllocTests=true
   ```
3. **Run JMH with `-prof gc`** to verify `gc.alloc.rate == 0` for any new benchmark.

## Further Reading

- Gil Tene, "Understanding Latency" (Azul Systems) — coordinated omission, percentile measurement
- Martin Thompson, "Mechanical Sympathy" blog — LMAX Disruptor design, cache-friendly data structures
- Aeron Design Principles (Real Logic wiki) — shared-memory IPC, zero-copy transport
- exchange-core project (GitHub) — open-source matching engine with JMH benchmarks and Epsilon GC allocation tests
