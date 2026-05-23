package com.trading.engine.cluster.handler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.trading.engine.cluster.IdGenerator;
import com.trading.engine.cluster.OrderBook;
import com.trading.engine.cluster.TradingClusteredServiceFactory;
import com.trading.engine.cluster.metrics.RfqMetrics;
import com.trading.engine.cluster.refdata.AccountFixtures;
import com.trading.engine.cluster.refdata.AccountState;
import com.trading.engine.cluster.refdata.AccountStore;
import com.trading.engine.cluster.refdata.CurrencyFixtures;
import com.trading.engine.cluster.refdata.CurrencyStore;
import com.trading.engine.cluster.refdata.RiskLimitFixtures;
import com.trading.engine.cluster.refdata.RiskLimitStore;
import com.trading.engine.cluster.state.RfqStateMachine;
import com.trading.engine.cluster.state.TradingState;
import com.trading.engine.messages.sbe.AccountStatusEnum;
import com.trading.engine.messages.sbe.ClOrdIdDedupSnapshotDecoder;
import com.trading.engine.messages.sbe.MessageHeaderDecoder;
import com.trading.engine.messages.sbe.MessageHeaderEncoder;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Long2LongHashMap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for the ClOrdID dedup snapshot round-trip on {@link NewOrderSingleHandler}: the {@link
 * NewOrderSingleHandler#snapshotDedupTo} → {@link NewOrderSingleHandler#restoreDedupFrom} pair
 * introduced in APP-225. Each test encodes the dedup registry (and eviction timestamp) into an SBE
 * buffer and restores into a fresh handler, then asserts full fidelity.
 *
 * <p>Registry state is injected via reflection (see note below) so no public setters are added to
 * production code:
 *
 * <ul>
 *   <li>{@code clOrdIdRegistry} — package-private; accessible without {@code setAccessible(true)}
 *   <li>{@code lastEvictionTimestampNanos} — private; requires {@code setAccessible(true)} for
 *       direct write; tests that target it use reflection only for pre-conditions; the observable
 *       side is always tested through {@code snapshotDedupTo} + {@code restoreDedupFrom}
 * </ul>
 *
 * <p><b>Threading:</b> single-threaded — matches the cluster duty-cycle invariant.
 *
 * <p><b>Allocation:</b> tests are not on the hot path; {@link ExpandableArrayBuffer} heap
 * allocation is acceptable.
 */
class NewOrderSingleHandlerDedupSnapshotTest {

  // -------------------------------------------------------------------------
  // Constants
  // -------------------------------------------------------------------------

  /** SBE header length — 8 bytes (blockLength=2, templateId=2, schemaId=2, version=2). */
  private static final int SBE_HDR = MessageHeaderEncoder.ENCODED_LENGTH;

  /**
   * SBE body block for {@code ClOrdIdDedupSnapshot}: just {@code lastEvictionTimestampNanos} (8
   * bytes).
   */
  private static final int BODY_BLOCK = ClOrdIdDedupSnapshotDecoder.BLOCK_LENGTH;

  /** SBE group-dimension header for {@code noEntries}: 4 bytes (blockLength=2, count=2). */
  private static final int GROUP_HDR = ClOrdIdDedupSnapshotDecoder.NoEntriesDecoder.HEADER_SIZE;

  /** SBE block per group row: {@code dedupKey} (8) + {@code firstSeenTimestamp} (8) = 16 bytes. */
  private static final int ENTRY_BLOCK =
      ClOrdIdDedupSnapshotDecoder.NoEntriesDecoder.sbeBlockLength();

  /** Sentinel used by {@link Long2LongHashMap} when a key is not found. */
  private static final long MISSING = NewOrderSingleHandler.CLORDID_DEDUP_MISSING;

  // -------------------------------------------------------------------------
  // Fixtures
  // -------------------------------------------------------------------------

  private NewOrderSingleHandler handler;
  private AccountStore accountStore;
  private CurrencyStore currencyStore;
  private RiskLimitStore riskLimitStore;

  @BeforeEach
  void setUp() {
    accountStore = new AccountStore();
    currencyStore = new CurrencyStore();
    riskLimitStore = new RiskLimitStore();

    accountStore.put(
        AccountFixtures.account(
            1L, "ACME", AccountStatusEnum.Active, AccountState.Capabilities.CAN_TRADE));
    currencyStore.put(
        CurrencyStore.packCode((byte) 'U', (byte) 'S', (byte) 'D'), CurrencyFixtures.usd());
    currencyStore.put(
        CurrencyStore.packCode((byte) 'E', (byte) 'U', (byte) 'R'), CurrencyFixtures.eur());
    riskLimitStore.put(RiskLimitFixtures.permissive(1L));

    handler = buildFreshHandler();
  }

  // -------------------------------------------------------------------------
  // Build helpers
  // -------------------------------------------------------------------------

  /**
   * Builds a fresh {@link NewOrderSingleHandler} wired with a minimal {@link RfqStateMachine} —
   * mirrors the fixture setup in {@link NewOrderSingleHandlerClOrdIdDedupTest}.
   *
   * @return a ready-to-use handler with an empty dedup registry
   */
  private NewOrderSingleHandler buildFreshHandler() {
    final var orderBook = new OrderBook(128);
    final var orderIdGen = new IdGenerator("ORD");
    final var execIdGen = new IdGenerator("EXE");
    final var quoteIdGen = new IdGenerator("QTE");
    final var tradingState = new TradingState(orderBook, orderIdGen, execIdGen, quoteIdGen);
    final var h =
        new NewOrderSingleHandler(tradingState, accountStore, currencyStore, riskLimitStore);
    final var rfqMetrics = new RfqMetrics();
    final var rfqStateMachine =
        new RfqStateMachine(
            256,
            TradingClusteredServiceFactory.DEFAULT_RFQ_TTL_NANOS,
            TradingClusteredServiceFactory.DEFAULT_RFQ_TTL_NANOS,
            TradingClusteredServiceFactory.DEFAULT_RFQ_TTL_NANOS,
            TradingClusteredServiceFactory.DEFAULT_RFQ_TTL_NANOS,
            TradingClusteredServiceFactory.DEFAULT_RFQ_REQUEST_TIMEOUT_NANOS,
            TradingClusteredServiceFactory.DEFAULT_RFQ_RATE_LIMIT_PER_SESSION,
            TradingClusteredServiceFactory.DEFAULT_RFQ_RATE_LIMIT_WINDOW_NANOS,
            0,
            0,
            accountStore,
            rfqMetrics);
    h.wireRfqStateMachine(rfqStateMachine, rfqMetrics);
    return h;
  }

  /**
   * Allocates an {@link ExpandableArrayBuffer} large enough for a dedup snapshot holding {@code
   * entryCount} entries.
   *
   * @param entryCount the number of (key, value) pairs to be encoded
   * @return a zeroed buffer with sufficient capacity
   */
  private static MutableDirectBuffer snapshotBuf(final int entryCount) {
    // SBE header (8) + body block (8) + group header (4) + N × entry block (16)
    final int capacity = SBE_HDR + BODY_BLOCK + GROUP_HDR + entryCount * ENTRY_BLOCK + 64;
    return new ExpandableArrayBuffer(capacity);
  }

  /**
   * Reads the private {@code lastEvictionTimestampNanos} field from {@code h} via reflection.
   *
   * @param h the handler instance to inspect
   * @return the current value of {@code lastEvictionTimestampNanos}
   */
  private static long readLastEviction(final NewOrderSingleHandler h) {
    try {
      final var f = NewOrderSingleHandler.class.getDeclaredField("lastEvictionTimestampNanos");
      f.setAccessible(true);
      return (long) f.get(h);
    } catch (final ReflectiveOperationException e) {
      throw new AssertionError("reflection failed on lastEvictionTimestampNanos", e);
    }
  }

  /**
   * Writes {@code value} into the private {@code lastEvictionTimestampNanos} field of {@code h} via
   * reflection.
   *
   * @param h the handler instance to modify
   * @param value the value to set
   */
  private static void writeLastEviction(final NewOrderSingleHandler h, final long value) {
    try {
      final var f = NewOrderSingleHandler.class.getDeclaredField("lastEvictionTimestampNanos");
      f.setAccessible(true);
      f.set(h, value);
    } catch (final ReflectiveOperationException e) {
      throw new AssertionError("reflection failed writing lastEvictionTimestampNanos", e);
    }
  }

  /**
   * Reads the package-private {@code tradingState} field from {@code h} via reflection. Used to
   * verify the {@code tradingHalted} flag round-trips through the snapshot wire.
   *
   * @param h the handler to inspect
   * @return the handler's {@link TradingState} instance
   */
  private static TradingState getTradingState(final NewOrderSingleHandler h) {
    try {
      final var f = NewOrderSingleHandler.class.getDeclaredField("tradingState");
      f.setAccessible(true);
      return (TradingState) f.get(h);
    } catch (final ReflectiveOperationException e) {
      throw new AssertionError("reflection failed on tradingState", e);
    }
  }

  /**
   * Verifies the {@code tradingHalted} flag rides the ClOrdIdDedupSnapshot template (header field,
   * id=10058). With the flag set to {@code true} on the source handler's {@link TradingState}, a
   * snapshot encode + restore into a fresh handler (whose initial tradingHalted is {@code false})
   * must leave the destination handler's tradingState reporting halted. This is the safety-critical
   * invariant from Gemini iter-3 review (HIGH): an operator-set halt must survive cluster restart.
   */
  @Test
  void tradingHalted_true_restored_viaSnapshotHeader() {
    final var sourceState = getTradingState(handler);
    sourceState.setTradingHalted(true);

    final var buf = snapshotBuf(0);
    final var restored = buildFreshHandler();
    final var restoredState = getTradingState(restored);
    assertFalse(restoredState.isTradingHalted(), "fresh handler must default to admitting");
    roundTrip(handler, restored, buf);

    assertTrue(
        restoredState.isTradingHalted(),
        "tradingHalted=true must round-trip through ClOrdIdDedupSnapshot.tradingHalted");
  }

  /**
   * Mirror of {@link #tradingHalted_true_restored_viaSnapshotHeader} for the {@code false} → {@code
   * false} path. Defends against a regression where the restore code forces halted=true
   * unconditionally.
   */
  @Test
  void tradingHalted_false_restored_viaSnapshotHeader() {
    final var sourceState = getTradingState(handler);
    sourceState.setTradingHalted(false);

    final var buf = snapshotBuf(0);
    final var restored = buildFreshHandler();
    final var restoredState = getTradingState(restored);
    // Pre-set restored to halted; restore must explicitly CLEAR it.
    restoredState.setTradingHalted(true);
    roundTrip(handler, restored, buf);

    assertFalse(
        restoredState.isTradingHalted(),
        "tradingHalted=false must round-trip through ClOrdIdDedupSnapshot.tradingHalted "
            + "(restore must clear, not skip)");
  }

  /**
   * Performs a full encode → decode round-trip: calls {@link NewOrderSingleHandler#snapshotDedupTo}
   * on {@code src}, then calls {@link NewOrderSingleHandler#restoreDedupFrom} on {@code dst},
   * decoding the SBE header first to extract {@code blockLength} and {@code version} exactly as
   * {@link com.trading.engine.cluster.TradingClusteredService#applySnapshotFragment} does in
   * production.
   *
   * @param src the handler whose registry to snapshot
   * @param dst the handler to restore into
   * @param buf an ExpandableArrayBuffer with sufficient capacity
   * @return the total bytes returned by {@code snapshotDedupTo}
   */
  private static int roundTrip(
      final NewOrderSingleHandler src,
      final NewOrderSingleHandler dst,
      final MutableDirectBuffer buf) {
    // Encode.
    final int totalBytes = src.snapshotDedupTo(buf, 0);

    // Decode header to extract SBE blockLength and version — mirrors the production call site.
    final var hdrDec = new MessageHeaderDecoder();
    hdrDec.wrap(buf, 0);
    final int blockLength = hdrDec.blockLength();
    final int version = hdrDec.version();

    // Restore from body (header already consumed by hdrDec.wrap).
    dst.restoreDedupFrom(buf, MessageHeaderDecoder.ENCODED_LENGTH, blockLength, version);
    return totalBytes;
  }

  // =========================================================================
  // Test 1 — emptyRegistry_roundTrip
  // =========================================================================

  /**
   * Encoding an empty registry and restoring into a fresh handler must yield a registry of size 0.
   * {@code lastEvictionTimestampNanos} must also be restored verbatim — set to a non-zero value
   * before encoding so the test distinguishes "restored correctly" from "default stays at 0".
   */
  @Test
  void emptyRegistry_roundTrip_restoresSizeZeroAndLastEviction() {
    // Pre-condition: set a non-zero eviction timestamp on the source handler.
    final long presetEviction = 9_876_543_210_000L;
    writeLastEviction(handler, presetEviction);
    assertEquals(0, handler.clOrdIdRegistry.size(), "source registry must be empty");

    final var buf = snapshotBuf(0);
    final var restored = buildFreshHandler();
    roundTrip(handler, restored, buf);

    assertEquals(0, restored.clOrdIdRegistry.size(), "restored registry must be empty");
    assertEquals(
        presetEviction,
        readLastEviction(restored),
        "lastEvictionTimestampNanos must be restored verbatim");
  }

  // =========================================================================
  // Test 2 — singleEntry_roundTrip
  // =========================================================================

  /**
   * A registry with one entry must survive the encode → restore cycle: the entry's value must be
   * retrievable by the same key in the restored handler.
   */
  @Test
  void singleEntry_roundTrip_entryRestoredWithCorrectValue() {
    final long key = 0xDEAD_BEEF_CAFE_1234L;
    final long firstSeen = 1_700_000_000_123_456_789L;
    handler.clOrdIdRegistry.put(key, firstSeen);

    final var buf = snapshotBuf(1);
    final var restored = buildFreshHandler();
    roundTrip(handler, restored, buf);

    assertEquals(1, restored.clOrdIdRegistry.size(), "restored registry must have 1 entry");
    assertEquals(
        firstSeen,
        restored.clOrdIdRegistry.get(key),
        "restored entry value must match the original firstSeen timestamp");
    assertTrue(
        restored.clOrdIdRegistry.get(key) != MISSING,
        "key must be present in the restored registry");
  }

  // =========================================================================
  // Test 3 — manyEntries_roundTrip
  // =========================================================================

  /**
   * A registry with 1000 entries must survive the encode → restore cycle: size must be 1000 and
   * every original (key, value) pair must be retrievable in the restored handler.
   */
  @Test
  void manyEntries_roundTrip_allEntriesRestoredWithCorrectValues() {
    final int count = 1_000;
    // Build a reference map alongside so we can assert every (k,v) pair after restore.
    final var expected = new Long2LongHashMap(count * 2, 0.65f, MISSING);
    for (int i = 0; i < count; i++) {
      final long key = 0x0F00_0000_0000_0000L | (long) i;
      final long value = 1_700_000_000_000_000_000L + i;
      handler.clOrdIdRegistry.put(key, value);
      expected.put(key, value);
    }
    assertEquals(
        count, handler.clOrdIdRegistry.size(), "pre-condition: source must hold 1000 entries");

    final var buf = snapshotBuf(count);
    final var restored = buildFreshHandler();
    roundTrip(handler, restored, buf);

    assertEquals(
        count, restored.clOrdIdRegistry.size(), "restored registry must hold 1000 entries");
    final Long2LongHashMap.KeyIterator iter = expected.keySet().iterator();
    while (iter.hasNext()) {
      final long key = iter.nextValue();
      final long expectedValue = expected.get(key);
      assertEquals(
          expectedValue, restored.clOrdIdRegistry.get(key), () -> "value mismatch for key " + key);
    }
  }

  // =========================================================================
  // Test 4 — lastEvictionTimestamp_restored
  // =========================================================================

  /**
   * {@code lastEvictionTimestampNanos} must be restored verbatim from the snapshot. After
   * restoring, the handler must NOT trigger an immediate eviction scan for a NOS whose
   * clusterTimestamp is within the same 60-second eviction interval — i.e., the restored throttle
   * anchor must behave as if the eviction ran at the snapshotted time.
   *
   * <p>The test confirms this via direct reflection read rather than submitting a NOS (which would
   * require full cluster wiring) — the invariant is: {@code readLastEviction(restored) ==
   * snapshotted value}.
   */
  @Test
  void lastEvictionTimestamp_restored_verbatim() {
    final long evictionTs = 12_345_678_901_234L;
    writeLastEviction(handler, evictionTs);

    final var buf = snapshotBuf(0);
    final var restored = buildFreshHandler();
    roundTrip(handler, restored, buf);

    assertEquals(
        evictionTs,
        readLastEviction(restored),
        "lastEvictionTimestampNanos must be restored verbatim");
  }

  // =========================================================================
  // Test 5 — restoreReplacesPriorState
  // =========================================================================

  /**
   * Restoring from a snapshot must completely replace any prior state in the destination handler.
   * Pre-populating the destination with 5 entries, then restoring a snapshot containing 3 different
   * entries, must yield a registry of exactly 3 entries — no residual entries from the pre-restore
   * state.
   */
  @Test
  void restoreReplacesPriorState_oldEntriesErasedNewEntriesInstalled() {
    // Source handler: 3 distinct entries that will be in the snapshot.
    final long keyA = 0xAAAA_0000_0000_0001L;
    final long keyB = 0xBBBB_0000_0000_0002L;
    final long keyC = 0xCCCC_0000_0000_0003L;
    handler.clOrdIdRegistry.put(keyA, 1_000L);
    handler.clOrdIdRegistry.put(keyB, 2_000L);
    handler.clOrdIdRegistry.put(keyC, 3_000L);

    final var buf = snapshotBuf(3);
    final int totalBytes = handler.snapshotDedupTo(buf, 0);
    assertTrue(totalBytes > 0, "snapshotDedupTo must return a positive byte count");

    // Destination handler: pre-populate with 5 different keys — none of which appear in snapshot.
    final var restored = buildFreshHandler();
    for (int i = 1; i <= 5; i++) {
      restored.clOrdIdRegistry.put(0xDDDD_0000_0000_0000L | (long) i, 9_000L + i);
    }
    assertEquals(
        5, restored.clOrdIdRegistry.size(), "pre-condition: destination must have 5 entries");

    // Restore — must erase the 5 pre-existing entries and install exactly the 3 snapshot entries.
    final var hdrDec = new MessageHeaderDecoder();
    hdrDec.wrap(buf, 0);
    restored.restoreDedupFrom(
        buf, MessageHeaderDecoder.ENCODED_LENGTH, hdrDec.blockLength(), hdrDec.version());

    assertEquals(
        3,
        restored.clOrdIdRegistry.size(),
        "restored registry must contain exactly the 3 snapshot entries");
    assertEquals(1_000L, restored.clOrdIdRegistry.get(keyA), "keyA value must match snapshot");
    assertEquals(2_000L, restored.clOrdIdRegistry.get(keyB), "keyB value must match snapshot");
    assertEquals(3_000L, restored.clOrdIdRegistry.get(keyC), "keyC value must match snapshot");
    // Verify old keys are gone.
    for (int i = 1; i <= 5; i++) {
      assertEquals(
          MISSING,
          restored.clOrdIdRegistry.get(0xDDDD_0000_0000_0000L | (long) i),
          () -> "pre-restore key must not be present after restore");
    }
  }

  // =========================================================================
  // Test 6 — encodedLength_matchesReturnedLength
  // =========================================================================

  /**
   * The {@code int} returned by {@link NewOrderSingleHandler#snapshotDedupTo} must equal the
   * SBE-computed total length: {@code SBE_HDR + BODY_BLOCK + GROUP_HDR + N * ENTRY_BLOCK}. Verified
   * for both the empty-registry case (N=0) and a non-empty case (N=7).
   */
  @Test
  void encodedLength_matchesReturnedLength_forBothEmptyAndNonEmpty() {
    // Case 1: empty registry.
    {
      final var buf = snapshotBuf(0);
      final int returned = handler.snapshotDedupTo(buf, 0);
      final int expected = SBE_HDR + BODY_BLOCK + GROUP_HDR + 0 * ENTRY_BLOCK;
      assertEquals(
          expected,
          returned,
          "empty snapshot: returned length must equal SBE_HDR + BODY_BLOCK + GROUP_HDR");
    }

    // Case 2: 7 entries.
    {
      for (int i = 1; i <= 7; i++) {
        handler.clOrdIdRegistry.put(0xEEEE_0000_0000_0000L | (long) i, 100L * i);
      }
      final var buf = snapshotBuf(7);
      final int returned = handler.snapshotDedupTo(buf, 0);
      final int expected = SBE_HDR + BODY_BLOCK + GROUP_HDR + 7 * ENTRY_BLOCK;
      assertEquals(
          expected,
          returned,
          "7-entry snapshot: returned length must equal SBE_HDR + BODY_BLOCK + GROUP_HDR + 7 * ENTRY_BLOCK");
    }
  }
}
