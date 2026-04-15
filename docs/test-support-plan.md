# Plan: Shared `test-support` Module for Trading Engine — APP-222

## Context

The trading engine has 16 Java modules with 55+ test files. Test fixture code is heavily duplicated across modules — SBE encoding/decoding boilerplate appears in 48+ call sites across 30+ files, reference data builders are copy-pasted across 10+ tests, and fake Aeron session doubles are trapped as inner classes in individual test files. No module shares test code with any other.

The `messages` module has the `java-test-fixtures` plugin applied and declares `testFixturesImplementation(libs.agrona)`, but the `src/testFixtures/java/` directory was never created. `ControllableNanoClock` sits in `messages/src/test/java/` and is therefore NOT published — no module can consume it.

This duplication violates DRY, makes test maintenance expensive, and creates drift risk where the same encoding pattern diverges subtly across modules. Industry precedent: Aeron provides `aeron-test-support`, LMAX Disruptor centralizes test utilities, exchange-core uses shared test harnesses.

**Goal:** Create a standalone `test-support` module that consolidates all cross-module test fixtures into a single dependency, following the same conventions as comparable low-latency Java projects.

---

## Architecture Decisions

### Decision 1: Standalone `test-support` module (not `java-test-fixtures` expansion)

A dedicated `java-library` module that exports shared test utilities as its production code. Consumer modules add `testImplementation(project(":test-support"))`.

**Why:**
- SBE helpers, fake Aeron sessions, and buffer utilities span multiple domain boundaries — they don't belong in `messages`
- Matches Aeron's own `aeron-test-support` pattern
- Full control over package structure, dependencies, and lifecycle
- Avoids circular dependency risk

### Decision 2: Refdata fixtures stay in `cluster` module (NOT in `test-support`)

`AccountStore`, `CurrencyStore`, `RiskLimitStore`, `AccountState`, `CurrencyState`, and `RiskLimitState` all live in the `cluster` module. Putting fixtures for these types in `test-support` would require `test-support` → `:cluster`, creating a circular dependency since `cluster` → `testImplementation(:test-support)`.

**Solution:** Apply `java-test-fixtures` plugin to the `cluster` module. Place `AccountFixtures`, `CurrencyFixtures`, `RiskLimitFixtures`, and `ReferenceDataSeeder` in `cluster/src/testFixtures/java/`. Cluster's own tests consume them automatically; other modules (if ever needed) can add `testImplementation(testFixtures(project(":cluster")))`.

### Decision 3: Move `ControllableNanoClock` into `test-support`

The clock currently sits in `messages/src/test/java/` (not published). The `java-test-fixtures` plugin on `messages` has an empty `testFixtures` source set. Rather than creating `messages/src/testFixtures/java/` for a single file, move the clock into `test-support` where it logically belongs alongside other shared test utilities. Remove the unused `java-test-fixtures` plugin from `messages/build.gradle.kts`.

### Decision 4: `FakeGatewaySession` stays in gateway tests

`GatewaySession.trySend()` takes `uk.co.real_logic.artio.builder.Encoder` — a hard dependency on Artio. Moving `GatewaySession` to `messages` is infeasible. `FakeGatewaySession` is used by exactly 4 test files, all within `gateway/src/test/`. No cross-module consumers exist. Keep it where it is.

### Decision 5: No static shared `MessageHeaderEncoder`

The existing codebase creates `new MessageHeaderEncoder()` per method call or per test instance — never a shared static. Although technically safe in single-threaded JUnit, a shared static flyweight creates a trap if parallel test execution is ever enabled. Use a local `new MessageHeaderEncoder()` per encode method call. The allocation cost (16 bytes) is irrelevant in test code.

### Decision 6: Use Java records for batch parameters (not `Object[][]`)

The existing `Object[][]` pattern for batch encoding is type-unsafe. Shared utilities should improve on scattered code, not consolidate its flaws. Use typed records:

```java
/**
 * Typed record for batch account encoding. Carries all fields that vary per entry.
 * The {@code capabilities} field avoids a subtle default mismatch between single
 * ({@code CAN_TRADE|CAN_RFQ}) and batch ({@code CAN_TRADE} only) encoders found
 * in the original test code.
 */
public record AccountRecord(long id, String code, String baseCcy, long capabilities) {
    /** CAN_TRADE capability flag (mirrors AccountState.Capabilities.CAN_TRADE). */
    public static final long CAN_TRADE = 1L;
    /** CAN_RFQ capability flag (mirrors AccountState.Capabilities.CAN_RFQ). */
    public static final long CAN_RFQ = 2L;

    /** Convenience constructor — defaults capabilities to CAN_TRADE only. */
    public AccountRecord(long id, String code, String baseCcy) {
        this(id, code, baseCcy, CAN_TRADE);
    }
}
public record CurrencyRecord(String code, int isoNumeric, String name, int decimals,
                             CurrencyClassEnum cls, AccountStatusEnum status) {}
public record RiskLimitRecord(long accountId, long maxOrderSize, long maxOrderNotional,
                              long maxDailyVolume, long maxDailyLossBps) {}
```

### Decision 7: Test naming convention — defer to separate pass

CLAUDE.md mandates `methodUnderTest_scenario_expectedBehavior`. The codebase has mixed conventions (some camelCase, some underscore). Renaming during migration increases scope and PR diff noise. This plan migrates fixture code only; test method renaming is a separate, independently-tracked task.

### Decision 8: Use `aeron-test-support` for integration infrastructure, not unit test doubles

`io.aeron:aeron-test-support:1.50.4` is available on Maven Central and provides `TestMediaDriver`, `TestCluster`, `TestNode`, and `ClusterTests` for spinning up real embedded Aeron infrastructure in tests.

**Where to use it:**
- **`integration-tests` module** (Phase 3) — `TestCluster` and `TestMediaDriver` are the standard way to run end-to-end cluster tests. This replaces the need to manually configure and launch a 3-node cluster in integration tests.
- **`launcher` tests** (Phase 2) — `TestMediaDriver` can simplify tests that validate cluster startup configuration, replacing manual `@TempDir` Aeron directory setup.

**Where NOT to use it:**
- **`test-support` module** — our `FakeClientSession` and `FakeCluster` are intentionally minimal unit test doubles that run in microseconds with zero I/O. `aeron-test-support`'s `TestCluster` spins up real Raft consensus with shared memory — correct for integration tests, far too heavy for unit tests. Both serve distinct purposes and coexist.

**Action:** Add `aeron-test-support` to `libs.versions.toml`. Wire it into `integration-tests/build.gradle.kts` in Phase 3 and `launcher/build.gradle.kts` in Phase 2. Keep custom `FakeClientSession`/`FakeCluster` in `test-support` for unit tests.

---

## Module Structure

### `test-support` Build Configuration

**File: `settings.gradle.kts`** — add `"test-support"` to the `include(...)` block.

**File: `gradle/libs.versions.toml`** — add `aeron-test-support` library:
```toml
aeron-test-support = { module = "io.aeron:aeron-test-support", version.ref = "aeron" }
```

**File: `test-support/build.gradle.kts`**
```kotlin
plugins {
    `java-library`
}

dependencies {
    api(project(":messages"))
    api(libs.agrona)
    api(libs.aeron.cluster)   // Cluster, ClientSession interfaces
    api(libs.junit.jupiter)
}
```

**Design decisions:**
- `api` scope for deps consumers need (messages, agrona, junit) — avoids forcing consumers to re-declare
- No `:cluster` dependency — refdata fixtures live in `cluster/src/testFixtures/`
- No logging framework — pure utility module
- No `java-test-fixtures` plugin — this IS the test fixture module

**File: `build.gradle.kts` (root)** — add `"test-support"` to the `hotPathModules` set (line ~79). This is cosmetic — prevents unnecessary SLF4J/Log4j2/Disruptor injection into a module that has no loggers. Functionally harmless either way.

### `cluster` Test Fixtures Configuration

**File: `cluster/build.gradle.kts`** — add `java-test-fixtures` plugin and dependencies:
```kotlin
plugins {
    `java-test-fixtures`
}

dependencies {
    // existing deps...
    testImplementation(project(":test-support"))
    testFixturesImplementation(project(":messages"))
    testFixturesImplementation(libs.agrona)
}
```

### Package Structure

```
test-support/src/main/java/com/trading/engine/testsupport/
    sbe/
        SbeTestEncoder.java          — Encode any SBE command/event into a buffer
        SbeTestDecoder.java          — Decode any SBE message with header unwrapping
        SbeMessageAssertions.java    — JUnit assertion helpers for SBE messages
        AccountRecord.java           — Typed record for batch account encoding
        CurrencyRecord.java          — Typed record for batch currency encoding
        RiskLimitRecord.java         — Typed record for batch risk-limit encoding
    aeron/
        FakeClientSession.java       — Extracted from TradingClusteredServiceTest
        FakeCluster.java             — Extracted from TradingClusteredServiceTest
    clock/
        ControllableNanoClock.java   — NEW (write from scratch)
    buffer/
        TestBuffers.java             — Pre-sized ExpandableArrayBuffer factory methods
        SbeFieldUtil.java            — Fixed-width field padding (zeroPad, spacePad, wrapSymbol)
    FixedPointTestUtil.java          — Price/quantity helpers using PRICE_SCALE

test-support/src/test/java/com/trading/engine/testsupport/
    sbe/
        SbeEncoderDecoderRoundTripTest.java  — Round-trip verification for every message type
        SbeMessageAssertionsTest.java        — Assertion helpers fail correctly on wrong data
    clock/
        ControllableNanoClockTest.java       — NEW
    buffer/
        TestBuffersTest.java                 — Verify buffer sizes and types

cluster/src/testFixtures/java/com/trading/engine/cluster/refdata/
    AccountFixtures.java             — AccountState factory methods
    CurrencyFixtures.java            — CurrencyState factory methods
    RiskLimitFixtures.java           — RiskLimitState factory methods
    ReferenceDataSeeder.java         — Seeds stores with standard test dataset
```

---

## Class Designs

All classes follow CLAUDE.md documentation standards: class-level Javadoc with purpose, threading model, and allocation behavior; method-level `@param`, `@return`, `@throws` on every public method; FIX tag references where applicable.

### 1. `SbeTestEncoder`

Consolidates the 48 instances of `wrapAndApplyHeader` boilerplate across cluster, projections, gateway, and reference-data tests.

```java
/**
 * Shared SBE message encoding utilities for unit tests.
 *
 * <p>Provides factory methods that encode complete SBE messages (header + body) into a
 * {@link MutableDirectBuffer} and return the total encoded length. Each method mirrors
 * a single SBE template and accepts only the fields that vary across tests — sensible
 * defaults are applied for boilerplate fields ({@code timeInForce=Day},
 * {@code transactTime=0L}, {@code acctIdSource=Internal}).
 *
 * <p>Thread-safe — all methods are stateless static functions that create a local
 * {@link MessageHeaderEncoder} per call. No shared mutable state.
 *
 * <p>Allocates freely on every call (new encoder instances, string-to-byte conversions).
 * This is test infrastructure, not hot-path code — do not copy these patterns into
 * production handlers.
 *
 * @see SbeTestDecoder for the corresponding decode/assert utilities
 */
public final class SbeTestEncoder { ... }
```

**Method categories and signatures:**

#### Commands

```java
/**
 * Encodes a NewOrderSingle command (template ID 4) with default spot/FX fields.
 *
 * <p>Defaults: {@code timeInForce=Day}, {@code transactTime=0L},
 * {@code productType=Spot}, {@code settlType=Regular}, {@code tenor=SN},
 * {@code settlDate="20260101"}, {@code settlCurrency} = same as {@code currency}.
 *
 * @param dst         destination buffer; must have sufficient capacity at {@code offset}
 * @param offset      starting position in the buffer
 * @param clOrdId     client order identifier (FIX tag 11); max 20 ASCII characters
 * @param symbol      instrument symbol (FIX tag 55); max 8 ASCII characters
 * @param side        order side (FIX tag 54)
 * @param ordType     order type (FIX tag 40)
 * @param price       limit price in fixed-point scale 10^8 (FIX tag 44)
 * @param orderQty    order quantity in fixed-point scale 10^8 (FIX tag 38); must be > 0
 * @param accountCode account identifier string (FIX tag 1)
 * @param currency    order currency ISO code (FIX tag 15); 3 ASCII characters
 * @return total encoded length including SBE header
 */
public static int encodeNewOrderSingle(
    MutableDirectBuffer dst, int offset,
    String clOrdId, String symbol, SideEnum side, OrdTypeEnum ordType,
    long price, long orderQty, String accountCode, String currency);

/**
 * Encodes a NewOrderSingle command with explicit FX product fields.
 *
 * <p>Use this overload when the test asserts on FX-specific fields like
 * {@code productType}, {@code settlDate}, {@code tenor}, or {@code settlCurrency}.
 *
 * @param productType product classification (FIX tag 460)
 * @param settlDate   settlement date YYYYMMDD (FIX tag 64)
 * @param settlType   settlement type (FIX tag 63)
 * @param tenor       tenor enum (e.g., SN, TN, ON)
 * @param settlCurrency settlement currency ISO code (FIX tag 120); 3 ASCII characters
 * @return total encoded length including SBE header
 */
public static int encodeNewOrderSingle(
    MutableDirectBuffer dst, int offset,
    String clOrdId, String symbol, SideEnum side, OrdTypeEnum ordType,
    long price, long orderQty, String accountCode, String currency,
    ProductTypeEnum productType, String settlDate,
    SettlTypeEnum settlType, TenorEnum tenor, String settlCurrency);

/**
 * Encodes an OrderCancelRequest command (template ID 6).
 *
 * @param dst          destination buffer
 * @param offset       starting position in the buffer
 * @param clOrdId      client order identifier for this cancel (FIX tag 11)
 * @param origClOrdId  original order's ClOrdID being cancelled (FIX tag 41)
 * @param symbol       instrument symbol (FIX tag 55)
 * @param side         order side (FIX tag 54)
 * @return total encoded length including SBE header
 */
public static int encodeOrderCancelRequest(
    MutableDirectBuffer dst, int offset,
    String clOrdId, String origClOrdId, String symbol, SideEnum side);
```

#### Reference Data Commands

```java
/**
 * Encodes a LoadAccount command (template ID 11).
 *
 * <p>Defaults: {@code parentAccountId=0}, {@code acctIdSource=Internal},
 * {@code accountType=Client}, {@code status=Active},
 * {@code complianceStatus=OK}, {@code capabilities=CAN_TRADE|CAN_RFQ},
 * {@code transactTime=0L}.
 *
 * @param dst       destination buffer
 * @param offset    starting position in the buffer
 * @param accountId unique account identifier
 * @param code      account code string; max 16 ASCII characters
 * @param name      human-readable account name; max 64 ASCII characters
 * @param baseCcy   base currency ISO code; 3 ASCII characters
 * @return total encoded length including SBE header
 */
public static int encodeLoadAccount(
    MutableDirectBuffer dst, int offset,
    long accountId, String code, String name, String baseCcy);

/**
 * Encodes a LoadAccountBatch command (template ID 12) using typed records.
 *
 * @param dst     destination buffer; must have capacity for header + all group entries
 * @param offset  starting position in the buffer
 * @param records account records to encode as repeating group entries
 * @return total encoded length including SBE header
 */
public static int encodeLoadAccountBatch(
    MutableDirectBuffer dst, int offset,
    AccountRecord... records);

/**
 * Encodes a LoadCurrency command (template ID 13).
 *
 * <p>Default: {@code transactTime=0L}.
 *
 * @param dst        destination buffer
 * @param offset     starting position in the buffer
 * @param code       currency ISO code; 3 ASCII characters
 * @param isoNumeric ISO 4217 numeric code (e.g., 840 for USD)
 * @param name       currency display name
 * @param decimals   decimal precision (e.g., 2 for USD)
 * @param cls        currency classification (Fiat, Crypto, etc.)
 * @param status     currency status (Active, Suspended)
 * @return total encoded length including SBE header
 */
public static int encodeLoadCurrency(
    MutableDirectBuffer dst, int offset,
    String code, int isoNumeric, String name, int decimals,
    CurrencyClassEnum cls, AccountStatusEnum status);

/** Overload with explicit {@code transactTime} for snapshot round-trip tests. */
public static int encodeLoadCurrency(
    MutableDirectBuffer dst, int offset,
    String code, int isoNumeric, String name, int decimals,
    CurrencyClassEnum cls, AccountStatusEnum status, long transactTime);

public static int encodeLoadCurrencyBatch(
    MutableDirectBuffer dst, int offset,
    CurrencyRecord... records);

public static int encodeLoadRiskLimit(
    MutableDirectBuffer dst, int offset,
    long accountId, long maxOrderSize, long maxOrderNotional,
    long maxDailyVolume, long maxDailyLossBps);

public static int encodeLoadRiskLimitBatch(
    MutableDirectBuffer dst, int offset,
    RiskLimitRecord... records);
```

#### Events (for projection and egress tests)

```java
/**
 * Encodes an OrderCreatedEvent (template ID 100) with explicit FX fields.
 *
 * @param dst         destination buffer
 * @param offset      starting position
 * @param seqNo       event sequence number
 * @param timestamp   cluster timestamp (epoch nanoseconds)
 * @param orderId     server-assigned order ID (20 chars)
 * @param clOrdId     client order ID (FIX tag 11)
 * @param symbol      instrument symbol (FIX tag 55)
 * @param side        order side (FIX tag 54)
 * @param price       limit price in fixed-point (FIX tag 44)
 * @param orderQty    order quantity in fixed-point (FIX tag 38)
 * @param accountCode account identifier (FIX tag 1)
 * @param productType product classification (FIX tag 460)
 * @param settlDate   settlement date YYYYMMDD (FIX tag 64)
 * @param currency    order currency ISO code (FIX tag 15)
 * @return total encoded length including SBE header
 */
public static int encodeOrderCreatedEvent(
    MutableDirectBuffer dst, int offset,
    long seqNo, long timestamp, String orderId, String clOrdId,
    String symbol, SideEnum side, long price, long orderQty,
    String accountCode, ProductTypeEnum productType,
    String settlDate, String currency);

/** Convenience overload with spot/FX defaults for cluster handler tests. */
public static int encodeOrderCreatedEvent(
    MutableDirectBuffer dst, int offset,
    long seqNo, long timestamp, String orderId, String clOrdId,
    String symbol, SideEnum side, long price, long orderQty,
    String accountCode);

/**
 * Encodes an OrderFilledEvent (template ID 102) with explicit FX fields.
 *
 * @param leavesQty   remaining quantity after this fill (FIX tag 151); 0 = fully filled
 * @param cumQty      total filled quantity (FIX tag 14)
 * @param productType product classification (FIX tag 460)
 * @param settlDate   settlement date YYYYMMDD (FIX tag 64)
 * @param settlType   settlement type (FIX tag 63)
 * @param tenor       tenor enum
 * @return total encoded length including SBE header
 */
public static int encodeOrderFilledEvent(
    MutableDirectBuffer dst, int offset,
    long seqNo, long timestamp, String execId, String orderId,
    String clOrdId, String symbol, SideEnum side,
    long lastPx, long lastQty, long leavesQty, long cumQty,
    String accountCode, String currency, String settlCurrency,
    ProductTypeEnum productType, String settlDate,
    SettlTypeEnum settlType, TenorEnum tenor);

/** Convenience overload with spot/FX defaults. */
/**
 * Convenience overload with spot/FX defaults and zero legs.
 *
 * <p><b>Implementation note:</b> Must call {@code enc.noLegsCount(0)} after setting
 * fixed fields. SBE requires repeating group counts to be explicitly set, even for
 * zero entries. Omitting this call produces a malformed message that will fail on decode.
 */
public static int encodeOrderFilledEvent(
    MutableDirectBuffer dst, int offset,
    long seqNo, long timestamp, String execId, String orderId,
    String clOrdId, String symbol, SideEnum side,
    long lastPx, long lastQty, long leavesQty, long cumQty,
    String accountCode, String currency);

public static int encodeOrderCanceledEvent(
    MutableDirectBuffer dst, int offset,
    long seqNo, long timestamp, String orderId, String clOrdId,
    String origClOrdId, String symbol, SideEnum side,
    ProductTypeEnum productType);

/** Full-parameter overload for projection tests that assert on symbol/side/account. */
public static int encodeOrderRejectedEvent(
    MutableDirectBuffer dst, int offset,
    long seqNo, long timestamp, String clOrdId, String symbol,
    SideEnum side, RejectReasonEnum rejectReason,
    String accountCode, String currency);

/** Convenience overload — sets symbol/side/account to defaults. For cluster handler tests. */
public static int encodeOrderRejectedEvent(
    MutableDirectBuffer dst, int offset,
    long seqNo, long timestamp, String clOrdId,
    RejectReasonEnum rejectReason);

/** Reference-data event encoders for egress bridge and projection tests. */
public static int encodeAccountLoadedEvent(
    MutableDirectBuffer dst, int offset,
    long seqNo, long timestamp, long accountId, String code,
    String baseCcy, AccountStatusEnum status);

public static int encodeAccountLoadRejectedEvent(
    MutableDirectBuffer dst, int offset,
    long seqNo, long timestamp, String accountCode,
    RejectReasonEnum reason, String text);

public static int encodeCurrencyLoadedEvent(
    MutableDirectBuffer dst, int offset,
    long seqNo, long timestamp, String code, int isoNumeric);

public static int encodeRiskLimitLoadedEvent(
    MutableDirectBuffer dst, int offset,
    long seqNo, long timestamp, long accountId, long maxOrderSize);

public static int encodeExecutionReport(
    MutableDirectBuffer dst, int offset,
    String orderId, String execId, String clOrdId,
    ExecTypeEnum execType, OrdStatusEnum ordStatus,
    String symbol, SideEnum side,
    long leavesQty, long cumQty, long avgPx, long timestamp);
```

### 2. `SbeTestDecoder`

```java
/**
 * Shared SBE message decoding utilities for unit tests.
 *
 * <p>Provides type-safe decode methods that handle header unwrapping, template ID
 * verification, and decoder positioning in a single call. Eliminates the 4-line
 * boilerplate pattern of wrap-header, assert-templateId, wrap-decoder, return.
 *
 * <p>Thread-safe — all methods are stateless static functions.
 *
 * <p><b>Flyweight ownership:</b> The returned decoder wraps the supplied buffer via
 * the SBE flyweight pattern. Modifying the buffer after this call invalidates the
 * decoder's state. This matches SBE's standard usage contract.
 *
 * <p>Allocates a new {@link MessageHeaderDecoder} and message decoder per call.
 * This is test infrastructure, not hot-path code.
 */
public final class SbeTestDecoder {

    private SbeTestDecoder() {}

    // --- Header inspection ---

    /** Reads template ID from the SBE header at the given offset. */
    public static int templateId(DirectBuffer buffer, int offset);

    /** Reads template ID from a byte array (wraps in UnsafeBuffer at offset 0). */
    public static int templateId(byte[] bytes);

    // --- Order event decoders ---

    public static OrderCreatedEventDecoder decodeOrderCreated(DirectBuffer buffer, int offset);
    public static OrderCreatedEventDecoder decodeOrderCreated(byte[] bytes);

    public static OrderRejectedEventDecoder decodeOrderRejected(DirectBuffer buffer, int offset);
    public static OrderRejectedEventDecoder decodeOrderRejected(byte[] bytes);

    public static OrderFilledEventDecoder decodeOrderFilled(DirectBuffer buffer, int offset);
    public static OrderFilledEventDecoder decodeOrderFilled(byte[] bytes);

    public static OrderCanceledEventDecoder decodeOrderCanceled(DirectBuffer buffer, int offset);
    public static OrderCanceledEventDecoder decodeOrderCanceled(byte[] bytes);

    // --- Reference data event decoders ---

    public static AccountLoadedEventDecoder decodeAccountLoaded(DirectBuffer buffer, int offset);
    public static AccountLoadedEventDecoder decodeAccountLoaded(byte[] bytes);

    public static AccountLoadRejectedEventDecoder decodeAccountLoadRejected(DirectBuffer buffer, int offset);
    public static AccountLoadRejectedEventDecoder decodeAccountLoadRejected(byte[] bytes);

    public static CurrencyLoadedEventDecoder decodeCurrencyLoaded(DirectBuffer buffer, int offset);
    public static CurrencyLoadedEventDecoder decodeCurrencyLoaded(byte[] bytes);

    public static CurrencyLoadRejectedEventDecoder decodeCurrencyLoadRejected(DirectBuffer buffer, int offset);
    public static CurrencyLoadRejectedEventDecoder decodeCurrencyLoadRejected(byte[] bytes);

    public static RiskLimitLoadedEventDecoder decodeRiskLimitLoaded(DirectBuffer buffer, int offset);
    public static RiskLimitLoadedEventDecoder decodeRiskLimitLoaded(byte[] bytes);

    public static RiskLimitLoadRejectedEventDecoder decodeRiskLimitLoadRejected(DirectBuffer buffer, int offset);
    public static RiskLimitLoadRejectedEventDecoder decodeRiskLimitLoadRejected(byte[] bytes);

    // --- Gateway/ExecutionReport decoder ---

    public static ExecutionReportDecoder decodeExecutionReport(DirectBuffer buffer, int offset);
    public static ExecutionReportDecoder decodeExecutionReport(byte[] bytes);

    // --- Generic header for sequential multi-event buffer iteration ---

    /**
     * Wraps a MessageHeaderDecoder at the given offset for manual iteration over
     * concatenated events in a single buffer.
     */
    public static MessageHeaderDecoder wrapHeader(DirectBuffer buffer, int offset);
}
```

All typed decode methods verify the template ID and throw `AssertionError` on mismatch. Full `@param`/`@return` Javadoc on every method (omitted from plan for brevity — see `SbeTestEncoder` for the documentation standard).

### 3. `SbeMessageAssertions`

```java
/**
 * JUnit assertion helpers for SBE-encoded messages.
 *
 * <p>Provides domain-specific assertions that produce clear failure messages
 * referencing FIX tag names and SBE template IDs.
 *
 * <p>Thread-safe — all methods are stateless static functions.
 */
public final class SbeMessageAssertions {

    private SbeMessageAssertions() {}

    /**
     * Asserts the SBE header templateId equals {@code expected} at offset 0.
     *
     * @param expected expected template ID
     * @param bytes    SBE-encoded message bytes
     * @throws AssertionError if the template ID does not match
     */
    public static void assertTemplateId(int expected, byte[] bytes);

    /**
     * Asserts the SBE header templateId at the given buffer offset.
     *
     * @param expected expected template ID
     * @param buffer   buffer containing the SBE message
     * @param offset   offset where the SBE header begins
     * @throws AssertionError if the template ID does not match
     */
    public static void assertTemplateId(int expected, DirectBuffer buffer, int offset);

    /**
     * Decodes an OrderRejectedEvent from {@code bytes} and asserts the reject reason.
     *
     * @param bytes          SBE-encoded OrderRejectedEvent (template 101)
     * @param expectedReason expected rejection reason enum value
     * @throws AssertionError if template ID is not 101 or reason does not match
     */
    public static void assertRejected(byte[] bytes, RejectReasonEnum expectedReason);

    /**
     * Asserts that a buffer contains exactly {@code expectedCount} concatenated
     * SBE messages, all with the given template ID.
     *
     * @param buffer        buffer containing concatenated SBE messages
     * @param totalLength   total encoded length of all messages in the buffer
     * @param expectedId    expected template ID for every message
     * @param expectedCount expected number of messages
     * @throws AssertionError if count or any template ID does not match
     */
    public static void assertMessageCount(DirectBuffer buffer, int totalLength,
                                          int expectedId, int expectedCount);
}
```

### 4. `SbeFieldUtil` (NEW — addresses missing padding helpers)

```java
/**
 * Utilities for constructing fixed-width SBE field values in tests.
 *
 * <p>SBE schema fields like Symbol (8 bytes, space-padded) and ClOrdID (20 bytes,
 * zero-padded) require specific padding. These helpers eliminate the 4+ duplicated
 * {@code rightPadSymbol} / {@code padBytes} methods scattered across test files.
 *
 * <p>Thread-safe — all methods are pure functions with no mutable state.
 *
 * <p>Allocates byte arrays on every call. Test infrastructure only.
 */
public final class SbeFieldUtil {

    private SbeFieldUtil() {}

    /**
     * Right-pads a string with zero bytes to the specified length.
     * Used for SBE fixed-length char fields (ClOrdID, OrderID, ExecID).
     *
     * @param value source string (ASCII)
     * @param len   target byte array length
     * @return zero-padded byte array of exactly {@code len} bytes
     */
    public static byte[] zeroPad(String value, int len);

    /**
     * Right-pads a string with space bytes (0x20) to the specified length.
     * Used for SBE Symbol fields (8 bytes, space-padded per FIX convention).
     *
     * @param value source string (ASCII)
     * @param len   target byte array length
     * @return space-padded byte array of exactly {@code len} bytes
     */
    public static byte[] spacePad(String value, int len);

    /**
     * Wraps a string as an {@link UnsafeBuffer} for SBE DirectBuffer fields.
     *
     * @param value ASCII string to wrap
     * @return buffer wrapping the string's byte representation
     */
    public static UnsafeBuffer wrapAscii(String value);

    /**
     * Space-pads a symbol to 8 bytes and wraps as {@link UnsafeBuffer}.
     * Convenience for the most common fixed-width field pattern.
     *
     * @param symbol instrument symbol (FIX tag 55)
     * @return 8-byte space-padded buffer
     */
    public static UnsafeBuffer wrapSymbol(String symbol);
}
```

### 5. `TestBuffers`

```java
/**
 * Pre-sized buffer factory methods for common test scenarios.
 *
 * <p>All methods return {@link ExpandableArrayBuffer} instances (heap-backed,
 * auto-growing, debugger-friendly). Buffer sizes are initial capacities chosen to
 * match typical SBE message requirements without reallocation:
 * <ul>
 *   <li>{@link #command()} — 256 bytes, sufficient for any single SBE command</li>
 *   <li>{@link #event()} — 512 bytes, sufficient for any single SBE event</li>
 *   <li>{@link #batch()} — 4096 bytes, sufficient for batch commands/events</li>
 *   <li>{@link #snapshot()} — 65536 bytes, sufficient for snapshot concatenation</li>
 * </ul>
 *
 * <p>Thread-safe — each call allocates a new buffer instance.
 *
 * <p>For tests requiring fixed-size (non-growing) buffers, use
 * {@code new UnsafeBuffer(new byte[size])} directly.
 */
public final class TestBuffers { ... }
```

### 6. `FixedPointTestUtil`

```java
/**
 * Fixed-point arithmetic helpers for test readability.
 *
 * <p>Converts human-readable whole-number values to the engine's fixed-point
 * representation (scale factor {@value #PRICE_SCALE}) used for prices and quantities.
 *
 * <p>Example: {@code price(100)} returns {@code 10_000_000_000L}.
 *
 * <p>Thread-safe — all methods are pure functions. No mutable state.
 */
public final class FixedPointTestUtil { ... }
```

### 7. `FakeClientSession` (extracted from TradingClusteredServiceTest)

```java
/**
 * Test double for Aeron {@link ClientSession} that captures all offered messages
 * as defensive byte-array copies.
 *
 * <p>Supports configurable back-pressure simulation via {@link #pendingBackpressures}
 * and {@link #alwaysBackpressured}.
 *
 * <p>Not thread-safe — intended for single-threaded cluster service tests.
 *
 * <p>Allocates a new {@code byte[]} copy per {@link #offer} call (defensive copy).
 *
 * <p><b>Field visibility:</b> All tracking fields ({@code messages},
 * {@code pendingBackpressures}, {@code alwaysBackpressured}, {@code closed}) are
 * {@code public} to preserve the existing direct-field-access pattern from test
 * call sites. This is deliberate for test-only code — no getters needed.
 *
 * @see FakeCluster
 */
public final class FakeClientSession implements ClientSession {
    /** Captured messages — each entry is a defensive byte[] copy of an offer() call. */
    public final List<byte[]> messages = new ArrayList<>();
    /** Number of remaining back-pressure responses before normal flow resumes. */
    public int pendingBackpressures;
    /** When true, all offer() calls return BACK_PRESSURED unconditionally. */
    public boolean alwaysBackpressured;
    /** Set to true when close() is called. */
    public boolean closed;
    // ... ClientSession method implementations extracted from
    // TradingClusteredServiceTest lines 1213-1273
}
```

### 8. `FakeCluster` (extracted from TradingClusteredServiceTest)

```java
/**
 * Minimal test double for Aeron {@link Cluster} that returns a fixed timestamp
 * and tracks idle-strategy invocations.
 *
 * <p>Suitable for unit-testing {@link ClusteredService} implementations without
 * spinning up a real Aeron cluster.
 *
 * <p>Not thread-safe — intended for single-threaded cluster service tests.
 *
 * <p><b>Field visibility:</b> {@code idleCount} and {@code errorHandler} are
 * {@code public} to preserve direct-field-access from existing test call sites.
 *
 * @see FakeClientSession
 */
public final class FakeCluster implements Cluster {
    /** Tracks the number of idle() invocations via the wrapped IdleStrategy. */
    public int idleCount;
    /** Optional error handler for snapshot warning tests (APP-150). */
    public ErrorHandler errorHandler;
    // ... Cluster method implementations extracted from
    // TradingClusteredServiceTest lines 1276-1391
}
```

### 9. `ControllableNanoClock` (NEW — written from scratch)

The `messages/build.gradle.kts` references a `ControllableNanoClock` in its testFixtures comment, but
the file was never created. Write it from scratch in `test-support`.

```java
/**
 * Deterministic test clock implementing both {@link NanoClock} and {@link EpochNanoClock}.
 *
 * <p>Allows tests to control time precisely without wall-clock dependency.
 * Both interfaces return the same controlled value — this deliberately collapses
 * monotonic (relative) and epoch-anchored (absolute) time into a single controllable
 * value, which is the correct simplification for deterministic testing.
 *
 * <p>Not thread-safe — intended for single-threaded JUnit test methods.
 *
 * <p>Example usage:
 * <pre>{@code
 * var clock = new ControllableNanoClock(1_000_000_000L); // 1 second
 * clock.advanceNanos(5_000_000_000L);                    // now 6 seconds
 * assertEquals(6_000_000_000L, clock.nanoTime());
 * }</pre>
 */
public final class ControllableNanoClock implements NanoClock, EpochNanoClock {

    private long nanos;

    /** @param initialNanos starting time in nanoseconds */
    public ControllableNanoClock(final long initialNanos) { this.nanos = initialNanos; }

    /** Starts at time zero. */
    public ControllableNanoClock() { this(0L); }

    /** @return current controlled time in nanoseconds */
    @Override public long nanoTime() { return nanos; }

    /** @param deltaNanos nanoseconds to advance (must be >= 0) */
    public void advanceNanos(final long deltaNanos) { nanos += deltaNanos; }

    /** @param deltaMillis milliseconds to advance */
    public void advanceMillis(final long deltaMillis) { nanos += deltaMillis * 1_000_000L; }

    /** @param deltaSeconds seconds to advance */
    public void advanceSeconds(final long deltaSeconds) { nanos += deltaSeconds * 1_000_000_000L; }

    /** @param nanos absolute time to set */
    public void setNanos(final long nanos) { this.nanos = nanos; }
}
```

### 10. `AccountFixtures` (in `cluster/src/testFixtures/`)

```java
/**
 * Factory methods for {@link AccountState} test instances.
 *
 * <p>Each method produces a fully-populated, valid {@code AccountState} with sensible
 * defaults. Three overloads cover the two distinct factory patterns found in the
 * codebase: {@code AccountStoreTest.makeState()} (explicit name and currency) and
 * {@code TradingClusteredServiceTest.makeAccount()} (explicit status and capabilities).
 *
 * <p>Thread-safe — each call allocates a new {@code AccountState} instance.
 * The returned instances are mutable (not thread-safe themselves).
 */
public final class AccountFixtures {

    public static final long DEFAULT_ACCOUNT_ID = 1L;
    public static final String DEFAULT_CODE = "ACC-001";
    public static final String DEFAULT_NAME = "Test Account 1";
    public static final String DEFAULT_CURRENCY = "USD";
    public static final long DEFAULT_CAPABILITIES =
        AccountState.Capabilities.CAN_TRADE | AccountState.Capabilities.CAN_RFQ;

    private AccountFixtures() {}

    /**
     * Full-parameter factory — all fields explicitly set.
     *
     * @param id           unique account identifier
     * @param code         account code string
     * @param name         human-readable account name
     * @param baseCcy      base currency ISO code (3 chars)
     * @param status       account status (Active, Suspended, etc.)
     * @param capabilities bitfield of account capabilities
     * @return new fully-populated {@code AccountState}
     */
    public static AccountState account(long id, String code, String name, String baseCcy,
                                       AccountStatusEnum status, long capabilities);

    /**
     * Convenience — Active status with {@code CAN_TRADE | CAN_RFQ} capabilities.
     * Matches the pattern from {@code AccountStoreTest.makeState()}.
     */
    public static AccountState account(long id, String code, String name, String baseCcy);

    /**
     * Convenience — auto-generated name ({@code "Account " + code}), USD currency.
     * Matches the pattern from {@code TradingClusteredServiceTest.makeAccount()}.
     */
    public static AccountState account(long id, String code,
                                       AccountStatusEnum status, long capabilities);

    /** Minimal default account for simple tests. */
    public static AccountState defaultAccount();
}
```

### 11. `CurrencyFixtures` (in `cluster/src/testFixtures/`)

```java
/**
 * Factory methods for {@link CurrencyState} test instances.
 *
 * <p>Thread-safe — each call allocates a new {@code CurrencyState} instance.
 */
public final class CurrencyFixtures {

    private CurrencyFixtures() {}

    /**
     * Full-parameter factory.
     *
     * @param code       currency ISO code (3 chars, e.g., "USD")
     * @param isoNumeric ISO 4217 numeric code (e.g., 840)
     * @param name       display name (e.g., "US Dollar")
     * @param decimals   decimal precision (e.g., 2)
     * @param cls        currency classification
     * @param status     currency status
     * @return new {@code CurrencyState}
     */
    public static CurrencyState currency(String code, int isoNumeric, String name,
                                         int decimals, CurrencyClassEnum cls,
                                         AccountStatusEnum status);

    /** Convenience — Fiat, Active, 2 decimals. */
    public static CurrencyState currency(String code, int isoNumeric);

    /** Pre-built USD instance. */
    public static CurrencyState usd();

    /** Pre-built EUR instance. */
    public static CurrencyState eur();
}
```

### 12. `RiskLimitFixtures` (in `cluster/src/testFixtures/`)

```java
/**
 * Factory methods for {@link RiskLimitState} test instances.
 *
 * <p>Thread-safe — each call allocates a new {@code RiskLimitState} instance.
 */
public final class RiskLimitFixtures {

    private RiskLimitFixtures() {}

    /**
     * Creates a risk limit with all fields set.
     *
     * @param accountId       account this limit applies to
     * @param maxOrderSize    maximum single-order size in fixed-point
     * @param maxOrderNotional maximum single-order notional in fixed-point
     * @param maxDailyVolume  maximum daily volume in fixed-point
     * @param maxDailyLossBps maximum daily loss in basis points
     * @return new {@code RiskLimitState}
     */
    public static RiskLimitState riskLimit(long accountId, long maxOrderSize,
                                          long maxOrderNotional, long maxDailyVolume,
                                          long maxDailyLossBps);

    /** Convenience — permissive limit (10 units max order, no notional/volume/loss caps). */
    public static RiskLimitState permissive(long accountId);
}
```

### 13. `ReferenceDataSeeder` (in `cluster/src/testFixtures/`)

```java
/**
 * Seeds reference data stores with a standard dataset for integration-style unit tests.
 *
 * <p>Provides the canonical test dataset currently duplicated in
 * {@code TradingClusteredServiceTest.seedReferenceData()}. The dataset matches the
 * existing test expectations exactly:
 * <ul>
 *   <li>Account 1 (ACME): Active, USD, CAN_TRADE only (capabilities=1)</li>
 *   <li>Account 2 (LOCKED): Suspended, USD, CAN_TRADE (capabilities=1)</li>
 *   <li>Account 3 (QUOTEONLY): Active, USD, no capabilities (capabilities=0)</li>
 *   <li>Currency USD (840): Fiat, 2 decimals</li>
 *   <li>Currency EUR (978): Fiat, 2 decimals</li>
 *   <li>Risk limit for Account 1: maxOrderSize = 10 units (fixed-point)</li>
 * </ul>
 *
 * <p>Thread-safe — creates new state instances per call.
 *
 * @see AccountFixtures
 * @see CurrencyFixtures
 * @see RiskLimitFixtures
 */
public final class ReferenceDataSeeder {

    private ReferenceDataSeeder() {}

    /**
     * Seeds all three stores with the standard test dataset.
     *
     * @param accounts  account store to populate
     * @param currencies currency store to populate
     * @param limits    risk-limit store to populate
     */
    public static void seed(AccountStore accounts, CurrencyStore currencies,
                            RiskLimitStore limits);

    /** Seeds only the account store with 3 standard accounts. */
    public static void seedAccounts(AccountStore accounts);

    /** Seeds only the currency store with USD and EUR. */
    public static void seedCurrencies(CurrencyStore currencies);
}
```

---

## Migration Plan

### Phase 1: Module skeleton + core utilities + cluster migration (this PR)

**Step 1 — Module infrastructure:**
1. Create `test-support/build.gradle.kts`
2. Add `"test-support"` to `settings.gradle.kts`
3. Add `"test-support"` to `hotPathModules` in root `build.gradle.kts`
4. Add `java-test-fixtures` plugin to `cluster/build.gradle.kts`
5. Remove `java-test-fixtures` plugin from `messages/build.gradle.kts`

**Step 2 — Implement `test-support` classes:**
6. `SbeTestEncoder` — all encode methods with full Javadoc
7. `SbeTestDecoder` — all decode methods with flyweight ownership note
8. `SbeMessageAssertions` — template ID and rejection assertions
9. `SbeFieldUtil` — zeroPad, spacePad, wrapAscii, wrapSymbol
10. `TestBuffers` — command(), event(), batch(), snapshot(), of(int)
11. `FixedPointTestUtil` — price(long), qty(long), PRICE_SCALE constant
12. `FakeClientSession` — extracted from TradingClusteredServiceTest (public fields)
13. `FakeCluster` — extracted from TradingClusteredServiceTest (public fields)
14. `ControllableNanoClock` — NEW (write from scratch; file never existed in messages despite build comment)
15. `AccountRecord`, `CurrencyRecord`, `RiskLimitRecord` — batch encoding records

**Step 3 — Implement `cluster/src/testFixtures/` classes:**
16. `AccountFixtures` — 3 overloads + defaultAccount()
17. `CurrencyFixtures` — full + convenience + usd()/eur()
18. `RiskLimitFixtures` — full + permissive()
19. `ReferenceDataSeeder` — seed(), seedAccounts(), seedCurrencies()

**Step 4 — Add `test-support` self-tests:**
20. `SbeEncoderDecoderRoundTripTest` — encode each message type, decode, verify every field
21. `SbeMessageAssertionsTest` — verify assertions fail correctly on wrong data
22. `TestBuffersTest` — verify buffer sizes and concrete type
23. `ControllableNanoClockTest` — NEW test (no existing test file exists in messages; write from scratch)

**Step 5 — Verify Phase 1a:**
24. `./gradlew :test-support:test` — self-tests pass
25. `./gradlew :cluster:compileTestFixturesJava` — cluster testFixtures compile
26. `./gradlew spotlessApply` then `spotlessCheck`

### Phase 1b: Migrate cluster + projections tests (same or follow-up PR)

Scope: ~17 files modified. Can be same PR as 1a if total stays reviewable, or a follow-up.

**Step 1 — Migrate cluster module tests:**
1. Add `testImplementation(project(":test-support"))` to `cluster/build.gradle.kts`
2. Migrate `TradingClusteredServiceTest` — replace inline fakes, encoders, decoders, makeAccount, makeCurrency, seedReferenceData with shared fixtures
3. Migrate `AccountStoreTest` — replace `makeState()` with `AccountFixtures.account()`
4. Migrate `LoadAccountHandlerTest`, `LoadCurrencyHandlerTest`, `LoadRiskLimitHandlerTest` — replace encode/decode helpers with `SbeTestEncoder`/`SbeTestDecoder`
5. Migrate `LoadAccountBatchHandlerTest`, `LoadCurrencyBatchHandlerTest`, `LoadRiskLimitBatchHandlerTest` — replace `Object[][]` with typed records
6. Migrate `ReferenceDataRegistryTest` — update `AccountStoreTest.makeState()` references to `AccountFixtures.account()`
7. Migrate `RefDataIntegrationTest` — replace encode helpers

**Step 2 — Migrate projections module tests:**
8. Add `testImplementation(project(":test-support"))` to `projections/build.gradle.kts`
9. Migrate `OrderProjectionTest` — replace inline encoders with `SbeTestEncoder`
10. Migrate `PositionProjectionTest` — replace inline encoders with `SbeTestEncoder`
11. Migrate `AccountProjectionTest` — replace inline encoders with `SbeTestEncoder`

**Step 3 — Verify Phase 1b:**
12. `./gradlew test` — all tests pass
13. `./gradlew spotlessApply` then `spotlessCheck`

### Phase 2: Remaining module migrations (follow-up PR)
- Add `testImplementation(project(":test-support"))` to gateway, pricing-service, launcher
- Add `testImplementation(libs.aeron.test.support)` to `launcher/build.gradle.kts` — use `TestMediaDriver` for cluster startup config tests that currently use manual `@TempDir` setup
- Migrate gateway tests (FixGatewayTest, SbeToFixTranslatorTest — where applicable)
- Migrate pricing-service tests (replace duplicated rightPadSymbol with SbeFieldUtil)
- Migrate launcher tests (RefDataEgressBridgeTest — replace encode helpers)

### Phase 3: Integration test support (when integration-tests is implemented)
- Add `testImplementation(project(":test-support"))` to integration-tests
- Add `testImplementation(libs.aeron.test.support)` to `integration-tests/build.gradle.kts` — use `TestCluster`, `TestMediaDriver`, `TestNode` for end-to-end cluster tests (FIX → 3-node Raft → response)
- Build integration test harness on top of both `test-support` (SBE helpers, assertions) and `aeron-test-support` (real cluster infrastructure)

---

## Files to Create

| File | Purpose |
|------|---------|
| `test-support/build.gradle.kts` | Module build configuration |
| `test-support/src/main/java/com/trading/engine/testsupport/sbe/SbeTestEncoder.java` | SBE encoding factories |
| `test-support/src/main/java/com/trading/engine/testsupport/sbe/SbeTestDecoder.java` | SBE decoding + header unwrap |
| `test-support/src/main/java/com/trading/engine/testsupport/sbe/SbeMessageAssertions.java` | SBE assertion helpers |
| `test-support/src/main/java/com/trading/engine/testsupport/sbe/AccountRecord.java` | Typed batch record |
| `test-support/src/main/java/com/trading/engine/testsupport/sbe/CurrencyRecord.java` | Typed batch record |
| `test-support/src/main/java/com/trading/engine/testsupport/sbe/RiskLimitRecord.java` | Typed batch record |
| `test-support/src/main/java/com/trading/engine/testsupport/aeron/FakeClientSession.java` | Extracted Aeron session double |
| `test-support/src/main/java/com/trading/engine/testsupport/aeron/FakeCluster.java` | Extracted Aeron cluster double |
| `test-support/src/main/java/com/trading/engine/testsupport/clock/ControllableNanoClock.java` | NEW — write from scratch |
| `test-support/src/main/java/com/trading/engine/testsupport/buffer/TestBuffers.java` | Buffer factory methods |
| `test-support/src/main/java/com/trading/engine/testsupport/buffer/SbeFieldUtil.java` | Fixed-width field padding |
| `test-support/src/main/java/com/trading/engine/testsupport/FixedPointTestUtil.java` | Price/qty scaling helpers |
| `test-support/src/test/java/.../SbeEncoderDecoderRoundTripTest.java` | Round-trip encode/decode tests |
| `test-support/src/test/java/.../SbeMessageAssertionsTest.java` | Assertion correctness tests |
| `test-support/src/test/java/.../TestBuffersTest.java` | Buffer factory tests |
| `test-support/src/test/java/.../ControllableNanoClockTest.java` | NEW — no existing test in messages |
| `cluster/src/testFixtures/java/.../refdata/AccountFixtures.java` | AccountState factories |
| `cluster/src/testFixtures/java/.../refdata/CurrencyFixtures.java` | CurrencyState factories |
| `cluster/src/testFixtures/java/.../refdata/RiskLimitFixtures.java` | RiskLimitState factories |
| `cluster/src/testFixtures/java/.../refdata/ReferenceDataSeeder.java` | Standard dataset seeder |

## Files to Modify

| File | Change |
|------|--------|
| `gradle/libs.versions.toml` | Add `aeron-test-support` library definition |
| `settings.gradle.kts` | Add `"test-support"` to `include(...)` |
| `build.gradle.kts` (root) | Add `"test-support"` to `hotPathModules` set |
| `messages/build.gradle.kts` | Remove `java-test-fixtures` plugin and `testFixturesImplementation` |
| `cluster/build.gradle.kts` | Add `java-test-fixtures` plugin, `testImplementation(:test-support)`, `testFixturesImplementation` deps |
| `projections/build.gradle.kts` | Add `testImplementation(project(":test-support"))` |
| `cluster/.../TradingClusteredServiceTest.java` | Replace inline fakes, encoders, decoders, seedReferenceData with shared fixtures |
| `cluster/.../refdata/AccountStoreTest.java` | Replace `makeState()` with `AccountFixtures.account()` |
| `cluster/.../refdata/LoadAccountHandlerTest.java` | Replace encode/decode helpers with `SbeTestEncoder`/`SbeTestDecoder` |
| `cluster/.../refdata/LoadCurrencyHandlerTest.java` | Replace encode helpers with `SbeTestEncoder` |
| `cluster/.../refdata/LoadRiskLimitHandlerTest.java` | Replace encode helpers with `SbeTestEncoder` |
| `cluster/.../refdata/LoadAccountBatchHandlerTest.java` | Replace `Object[][]` with `AccountRecord...` |
| `cluster/.../refdata/LoadCurrencyBatchHandlerTest.java` | Replace with `CurrencyRecord...` |
| `cluster/.../refdata/LoadRiskLimitBatchHandlerTest.java` | Replace with `RiskLimitRecord...` |
| `cluster/.../refdata/ReferenceDataRegistryTest.java` | Replace `AccountStoreTest.makeState()` → `AccountFixtures.account()` |
| `cluster/.../refdata/RefDataIntegrationTest.java` | Replace encode helpers with `SbeTestEncoder` |
| `projections/.../OrderProjectionTest.java` | Replace inline encoders with `SbeTestEncoder` |
| `projections/.../PositionProjectionTest.java` | Replace inline encoders with `SbeTestEncoder` |
| `projections/.../AccountProjectionTest.java` | Replace inline encoders with `SbeTestEncoder` |

---

## Verification

```bash
# 1. Build new module compiles cleanly
./gradlew :test-support:compileJava

# 2. Test-support self-tests pass
./gradlew :test-support:test

# 3. Cluster testFixtures compile
./gradlew :cluster:compileTestFixturesJava

# 4. ALL existing tests still pass
./gradlew test

# 5. Formatting is clean
./gradlew spotlessCheck

# 6. New module appears in project list
./gradlew projects | grep test-support

# 7. No circular dependencies
./gradlew :test-support:dependencies --configuration compileClasspath
# Should show: messages, agrona, aeron-cluster, junit — NO cluster

# 8. Cluster testFixtures resolve correctly
./gradlew :cluster:dependencies --configuration testFixturesCompileClasspath

# 9. Consumer modules can resolve test-support
./gradlew :cluster:dependencies --configuration testCompileClasspath | grep test-support
./gradlew :projections:dependencies --configuration testCompileClasspath | grep test-support

# 10. Code coverage still works
./gradlew jacocoTestReport

# 11. OWASP scan passes
./gradlew dependencyCheckAnalyze
```
