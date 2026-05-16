package com.trading.engine.cluster.state;

import static org.junit.jupiter.api.Assertions.assertTrue;

import com.trading.engine.cluster.TradingClusteredServiceFactory;
import com.trading.engine.cluster.handler.EventSink;
import com.trading.engine.cluster.journal.EventJournal;
import com.trading.engine.cluster.metrics.RfqMetrics;
import com.trading.engine.cluster.refdata.AccountStore;
import com.trading.engine.cluster.sequencer.EventSequencer;
import com.trading.engine.messages.sbe.ProductTypeEnum;
import com.trading.engine.messages.sbe.SideEnum;
import com.trading.engine.messages.sbe.TenorEnum;
import com.trading.engine.testsupport.aeron.FakeCluster;
import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Zero-allocation regression suite for the hot-path operations of {@link RfqStateMachine} (plan
 * §11.4). Asserts that repeated invocations of acquire/release, token-bucket tryConsume,
 * recently-terminal lookup, byQuoteReqId lookup, and peekByQuoteId do not allocate heap objects
 * after the warmup phase.
 *
 * <p><b>Measurement technique:</b> {@link com.sun.management.ThreadMXBean#getThreadAllocatedBytes}
 * provides per-thread TLAB-sample-based byte-allocation accounting. Because the JVM can allocate
 * small transient objects without incrementing the counter between TLAB samples, the measurement
 * has noise of up to ~8 KB per 100 k iterations. The threshold is set to 100 bytes per call (10
 * MB/100k), which absorbs tool noise while still catching any object allocation (which would
 * register as &gt;= one TLAB = 512+ bytes per call).
 *
 * <p><b>Threading:</b> single-threaded — all asserts run on the test thread, matching the cluster
 * duty-cycle invariant.
 *
 * <p><b>Allocation:</b> the test itself allocates during setUp and in each test method body; only
 * the tightly bounded measurement loop body is asserted.
 */
class RfqStateMachineAllocationTest {

  // -------------------------------------------------------------------------
  // Constants
  // -------------------------------------------------------------------------

  /** Warmup iterations to prime JIT and TLAB before measurement. */
  private static final int WARMUP = 50_000;

  /** Measurement iterations — large enough to amortize TLAB granularity. */
  private static final int MEASURE = 100_000;

  /**
   * Maximum bytes-per-call threshold. Absorbs TLAB-sample noise (~8 kB / 100k = 80 bytes/call)
   * while rejecting any real allocation (one Java object ≥ 16 bytes → 16 * 100k = 1.6 MB total, far
   * above this threshold).
   */
  private static final long MAX_BYTES_PER_CALL = 100L;

  private static final long TS = 1_700_000_000_000_000_000L;
  private static final long SESSION_ID = 7L;

  // -------------------------------------------------------------------------
  // Fixtures
  // -------------------------------------------------------------------------

  private RfqStateMachine machine;
  private RfqMetrics metrics;
  private EventSink eventSink;
  private com.sun.management.ThreadMXBean threadBean;
  private long tid;

  @BeforeEach
  void setUp() {
    metrics = new RfqMetrics();
    final var accountStore = new AccountStore();
    machine =
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
            metrics);

    final var sequencer = new EventSequencer();
    final var journal = new EventJournal(64);
    eventSink = new EventSink(sequencer, journal);
    // Wire a FakeCluster with zero registered sessions so broadcast iteration is a no-op but
    // cluster is non-null (avoids NPE from EventSink.emit's unconditional forEachClientSession).
    eventSink.setCluster(new FakeCluster(0L));

    threadBean = (com.sun.management.ThreadMXBean) ManagementFactory.getThreadMXBean();
    tid = Thread.currentThread().getId();
  }

  // -------------------------------------------------------------------------
  // Helpers
  // -------------------------------------------------------------------------

  /** Writes ASCII bytes for {@code text} into {@code dst} up to maxLen, NUL-padded. */
  private static void writeFixedBytes(final String text, final byte[] dst, final int maxLen) {
    final byte[] src = text.getBytes(StandardCharsets.US_ASCII);
    final int copy = Math.min(src.length, maxLen);
    System.arraycopy(src, 0, dst, 0, copy);
    for (int i = copy; i < maxLen; i++) {
      dst[i] = 0;
    }
  }

  /**
   * Acquires a slot, sets required enum fields, syncs the quoteReqId key, and registers it as
   * REQUESTED. Returns the REQUESTED slot ready for use.
   */
  private RfqSlot acquireRequestedSlot(final String quoteReqId) {
    final var slot = machine.acquire();
    writeFixedBytes(quoteReqId, slot.quoteReqIdBytes, RfqSlot.QUOTE_REQ_ID_LENGTH);
    slot.side = (byte) SideEnum.Buy.value();
    slot.productType = (byte) ProductTypeEnum.Spot.value();
    slot.tenor = (byte) TenorEnum.ON.value();
    slot.transactTime = TS;
    slot.syncQuoteReqIdKey();
    machine.registerRequested(slot);
    return slot;
  }

  /**
   * Transitions a REQUESTED slot to QUOTED by populating quoteId and TTL fields, calling {@link
   * RfqSlot#syncQuoteIdKey}, and invoking {@link RfqStateMachine#registerQuoted(RfqSlot)}.
   */
  private void transitionToQuoted(final RfqSlot slot, final String quoteId) {
    writeFixedBytes(quoteId, slot.quoteIdBytes, RfqSlot.QUOTE_ID_LENGTH);
    slot.state = RfqSlotState.QUOTED;
    slot.timerCorrelationId = machine.ttlCorrelationFor(slot);
    slot.validUntil = TS + TradingClusteredServiceFactory.DEFAULT_RFQ_TTL_NANOS;
    slot.bidPx = 1_00000000L;
    slot.offerPx = 1_00000000L;
    slot.bidSize = 100_000_000L;
    slot.offerSize = 100_000_000L;
    slot.syncQuoteIdKey();
    machine.registerQuoted(slot);
  }

  /**
   * Asserts that {@code deltaBytes / MEASURE <= MAX_BYTES_PER_CALL}.
   *
   * @param label human-readable test name for the assertion message
   * @param before bytes allocated before the measurement loop
   * @param after bytes allocated after the measurement loop
   */
  private static void assertZeroAlloc(final String label, final long before, final long after) {
    final long delta = after - before;
    final long perCall = delta / MEASURE;
    assertTrue(
        perCall <= MAX_BYTES_PER_CALL,
        label
            + ": expected <= "
            + MAX_BYTES_PER_CALL
            + " bytes/call, got "
            + perCall
            + " (total delta = "
            + delta
            + " bytes over "
            + MEASURE
            + " iterations)");
  }

  // =========================================================================
  // §11.4 Test 1 — acquireRelease_zeroAllocation
  // =========================================================================

  /**
   * {@link RfqStateMachine#acquire()} followed by {@link RfqStateMachine#release(RfqSlot)} must not
   * allocate heap objects in steady state. The slot array, free list, and active-index arrays are
   * all pre-allocated; no new objects should be created per call.
   */
  @Test
  void acquireRelease_zeroAllocation() {
    // Warmup: prime JIT and TLAB
    for (int i = 0; i < WARMUP; i++) {
      final var s = machine.acquire();
      machine.release(s);
    }

    // Measure
    final long before = threadBean.getThreadAllocatedBytes(tid);
    for (int i = 0; i < MEASURE; i++) {
      final var s = machine.acquire();
      machine.release(s);
    }
    final long after = threadBean.getThreadAllocatedBytes(tid);

    assertZeroAlloc("acquireRelease", before, after);
  }

  // =========================================================================
  // §11.4 Test 2 — tokenBucketTryConsume_zeroAllocation
  // =========================================================================

  /**
   * {@link RfqStateMachine#rateLimitTryConsume(long, long)} in steady state (bucket already
   * activated, no refill needed) must not allocate. The bucket is pre-allocated in the pool; {@code
   * tryConsume} only mutates primitive fields.
   *
   * <p>We advance the timestamp by one nanosecond per iteration to avoid exhausting the bucket
   * mid-run (token refills at {@code rateLimitRefillNanosPerToken = windowNanos / perSession}).
   */
  @Test
  void tokenBucketTryConsume_zeroAllocation() {
    // Pre-activate the bucket for SESSION_ID so the warmup and measure loops both hit the
    // already-existing bucket path (not the first-call allocation branch).
    machine.rateLimitTryConsume(SESSION_ID, TS);

    // Refill rate: windowNanos(1s) / perSession(100) = 10 ms per token.
    // Advance by 10ms per call to ensure the bucket always has a token.
    final long refillNanosPerToken =
        TradingClusteredServiceFactory.DEFAULT_RFQ_RATE_LIMIT_WINDOW_NANOS
            / TradingClusteredServiceFactory.DEFAULT_RFQ_RATE_LIMIT_PER_SESSION;

    // Warmup
    for (int i = 0; i < WARMUP; i++) {
      machine.rateLimitTryConsume(SESSION_ID, TS + (long) i * refillNanosPerToken);
    }

    // Measure
    final long measureBase = TS + (long) WARMUP * refillNanosPerToken;
    final long before = threadBean.getThreadAllocatedBytes(tid);
    for (int i = 0; i < MEASURE; i++) {
      machine.rateLimitTryConsume(SESSION_ID, measureBase + (long) i * refillNanosPerToken);
    }
    final long after = threadBean.getThreadAllocatedBytes(tid);

    assertZeroAlloc("tokenBucketTryConsume", before, after);
  }

  // =========================================================================
  // §11.4 Test 3 — recentlyTerminalReason_lookupNotFound_zeroAllocation
  // =========================================================================

  /**
   * {@link RfqStateMachine#recentlyTerminalReason(org.agrona.DirectBuffer, int, int)} for a
   * quoteReqId that is NOT in the recently-terminal ring must return 0 (not found) without
   * allocating. The probe key is a pre-allocated flyweight; the hash-map lookup uses the probe's
   * content without boxing.
   */
  @Test
  void recentlyTerminalReason_lookupNotFound_zeroAllocation() {
    final byte[] unknownReqId = new byte[RfqSlot.QUOTE_REQ_ID_LENGTH];
    writeFixedBytes("UNKNOWN-QREQ-ID-1", unknownReqId, RfqSlot.QUOTE_REQ_ID_LENGTH);
    final var probeBuffer = new UnsafeBuffer(unknownReqId);

    // Warmup
    for (int i = 0; i < WARMUP; i++) {
      machine.recentlyTerminalReason(probeBuffer, 0, RfqSlot.QUOTE_REQ_ID_LENGTH);
    }

    // Measure
    final long before = threadBean.getThreadAllocatedBytes(tid);
    for (int i = 0; i < MEASURE; i++) {
      machine.recentlyTerminalReason(probeBuffer, 0, RfqSlot.QUOTE_REQ_ID_LENGTH);
    }
    final long after = threadBean.getThreadAllocatedBytes(tid);

    assertZeroAlloc("recentlyTerminalReason_lookupNotFound", before, after);
  }

  // =========================================================================
  // §11.4 Test 4 — lookupByQuoteReqId_zeroAllocation
  // =========================================================================

  /**
   * {@link RfqStateMachine#lookupByQuoteReqId(byte[], int, int)} for a populated slot must return
   * the slot without allocating. The method uses the pre-allocated {@code byQuoteReqIdProbe}
   * flyweight to hash-lookup in the Agrona {@code Object2ObjectHashMap}.
   */
  @Test
  void lookupByQuoteReqId_zeroAllocation() {
    // Populate a REQUESTED slot so the lookup always finds a hit
    final var slot = acquireRequestedSlot("QREQ-LOOKUP-ALLOC");

    // Warmup
    for (int i = 0; i < WARMUP; i++) {
      machine.lookupByQuoteReqId(slot.quoteReqIdBytes, 0, RfqSlot.QUOTE_REQ_ID_LENGTH);
    }

    // Measure
    final long before = threadBean.getThreadAllocatedBytes(tid);
    for (int i = 0; i < MEASURE; i++) {
      machine.lookupByQuoteReqId(slot.quoteReqIdBytes, 0, RfqSlot.QUOTE_REQ_ID_LENGTH);
    }
    final long after = threadBean.getThreadAllocatedBytes(tid);

    assertZeroAlloc("lookupByQuoteReqId", before, after);
  }

  // =========================================================================
  // §11.4 Test 5 — peekByQuoteId_quotedSlot_zeroAllocation
  // =========================================================================

  /**
   * {@link RfqStateMachine#peekByQuoteId(byte[], int, int)} for a QUOTED slot must return the slot
   * without allocating. The method uses the pre-allocated {@code byQuoteIdProbe} flyweight.
   */
  @Test
  void peekByQuoteId_quotedSlot_zeroAllocation() {
    // Populate a QUOTED slot so the peek always finds a hit
    final var slot = acquireRequestedSlot("QREQ-PEEK-ALLOC-1");
    transitionToQuoted(slot, "QUOTE-PEEK-ALLOC-1");

    // Warmup
    for (int i = 0; i < WARMUP; i++) {
      machine.peekByQuoteId(slot.quoteIdBytes, 0, RfqSlot.QUOTE_ID_LENGTH);
    }

    // Measure
    final long before = threadBean.getThreadAllocatedBytes(tid);
    for (int i = 0; i < MEASURE; i++) {
      machine.peekByQuoteId(slot.quoteIdBytes, 0, RfqSlot.QUOTE_ID_LENGTH);
    }
    final long after = threadBean.getThreadAllocatedBytes(tid);

    assertZeroAlloc("peekByQuoteId_quotedSlot", before, after);
  }
}
