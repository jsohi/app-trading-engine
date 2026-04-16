# Publisher SAM Pattern

## Why It Matters

Aeron's `io.aeron.ExclusivePublication` and `io.aeron.Subscription` are **`final` classes**. They cannot be subclassed for test fakes, and the project does not depend on a bytecode-mocking framework (no `mockito-inline`, no `byte-buddy`). Without an indirection layer, services that own outbound publications could only be exercised via an embedded media driver — slow (~1-3s startup per test class) and overkill for unit tests of pure business logic.

The `Publisher` SAM (`com.trading.engine.orchestrator.Publisher`) is a one-method functional interface that wraps the publication's `offer(DirectBuffer, int, int)` call. Production code binds `gatewayPublication::offer` once at construction; tests inject a `RecordingPublisher` fake. Same shape, same return semantics, swappable.

This pattern is established in the codebase by `com.trading.refdata.ClusterCommandSender` (the cluster-ingress equivalent for reference-data writes). `Publisher` mirrors it exactly.

## Pattern

```java
@FunctionalInterface
public interface Publisher {
  long publish(DirectBuffer buffer, int offset, int length);
}
```

The single method's return-value contract **mirrors `io.aeron.Publication.offer` exactly**:

| Return value | Meaning |
|---|---|
| Positive | New stream position; publication succeeded |
| `Publication.NOT_CONNECTED` (-1) | No subscribers connected (terminal — caller drops) |
| `Publication.BACK_PRESSURED` (-2) | Subscriber slow, retry later (transient — caller retries) |
| `Publication.ADMIN_ACTION` (-3) | Term rotation in progress, retry later (transient) |
| `Publication.CLOSED` (-4) | Publication closed (terminal) |
| `Publication.MAX_POSITION_EXCEEDED` (-5) | Stream position overflow (terminal) |

Callers must handle these per Aeron semantics. `OrchestratorService.offerWithRetry` is the canonical mapper: positive → `CONTINUE`, transient (-2/-3) → bounded retry then `ABORT`, terminal → `CONTINUE` (drop, avoid spin loop).

## Binding Idiom (zero allocation after construction)

The recommended idiom is to bind the SAM via a method reference to a `final` field:

```java
// In the launcher (OrchestratorLauncher.java)
final ExclusivePublication gatewayPublication =
    aeron.addExclusivePublication(IPC_CHANNEL, GATEWAY_RESPONSE_STREAM_ID);
final ExclusivePublication pricingPublication =
    aeron.addExclusivePublication(IPC_CHANNEL, PRICING_REQUEST_STREAM_ID);

// SAM bind via method reference — captured ONCE at construction
final OrchestratorService service =
    new OrchestratorService(
        gatewaySubscription,
        gatewayPublication::offer,   // Publisher SAM
        pricingSubscription,
        pricingPublication::offer,   // Publisher SAM
        ...);
```

### Why this is zero-alloc per call

- **JLS §15.27.4** guarantees the lambda/method-reference instance is created at expression evaluation. The JVM is permitted but not required to cache identical lambdas.
- Assigning the binding to a `final` field at construction means the SAM instance lives for the lifetime of the holder.
- After JIT warmup, calls to `publisher.publish(...)` are **inlined** through the SAM directly to `gatewayPublication.offer(...)` — no per-call allocation, no virtual-dispatch overhead.

This mirrors the `reapCallback` field in `OrchestratorService`: `this.reapCallback = this::onRfqExpired` is captured ONCE in the constructor and reused for every duty cycle's `reapExpired` call.

### What NOT to do

- **Inline lambdas inside hot loops** — every iteration creates a new SAM instance. Allocates.
  ```java
  // BAD: allocates a new Publisher instance every iteration
  for (int i = 0; i < 1_000_000; i++) {
    callSomethingThatTakesPublisher((b, o, l) -> publication.offer(b, o, l));
  }
  ```
- **Reassigning the binding field** — defeats the JIT inlining and breaks the regression guarantee.
- **Closing the binding's `Publication` before the SAM holder** — calls will start returning `Publication.CLOSED`. Lifecycle is owned by `OrchestratorComponents.close()` in the canonical wiring.

## Existing Usages

| SAM | Module | File |
|---|---|---|
| `Publisher` (this pattern) | `orchestrator` | `orchestrator/src/main/java/com/trading/engine/orchestrator/Publisher.java` |
| `ClusterCommandSender` (precedent) | `reference-data` | `reference-data/src/main/java/com/trading/refdata/ClusterCommandSender.java` |

Both follow the same idiom: `@FunctionalInterface`, single method whose return-value semantics mirror the underlying Aeron call, bound via method reference at construction.

## When NOT to Use the Pattern

- **Per-call dynamic publication selection** (e.g., choosing among many publications keyed by symbol). Method-reference capture is fixed at bind time; for dynamic dispatch, hold the typed `ExclusivePublication`s directly and select inline.
- **Code that needs `Publication`-specific methods** beyond `offer` (e.g., `tryClaim`, `addDestination`, `position`). The SAM only exposes `publish`; reach for the concrete type when those are needed.

## Testing

The complementary fake is `com.trading.engine.orchestrator.RecordingPublisher`:

```java
final RecordingPublisher gw = new RecordingPublisher();
final OrchestratorService service =
    new OrchestratorService(unusedSubscription(), gw, ...);

// Drive the system under test
service.someHandler(decoder);

// Assert what the SUT published
assertEquals(1, gw.callCount());
final byte[] captured = gw.capturedBufferBytes(0);
// decode `captured` with the appropriate SBE decoder and assert fields
```

`RecordingPublisher` defensively copies each offered byte slice (the SUT may reuse its encoding buffer between calls) and exposes a configurable return value via `setReturnValue(long)` for tests that need to drive the retry/terminal path (e.g., `setReturnValue(Publication.BACK_PRESSURED)`).

**Note:** `RecordingPublisher` is itself **not** zero-allocation (uses `ArrayList` + per-call `byte[]` copies). Do not exercise it from `OrchestratorNoAllocationTest`; that test uses a captured no-op SAM (`(b, o, l) -> 1L`) to measure the steady-state alloc profile.

## See Also

- `docs/zero-allocation.md` — the broader zero-allocation strategy
- `orchestrator/src/test/java/com/trading/engine/orchestrator/OrchestratorServicePartialTest.java` — canonical example of `RecordingPublisher`-driven unit testing
- `orchestrator/src/test/java/com/trading/engine/orchestrator/OrchestratorNoAllocationTest.java` — proves the bound SAM is zero-alloc after warmup
