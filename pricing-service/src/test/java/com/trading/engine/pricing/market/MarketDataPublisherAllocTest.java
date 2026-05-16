package com.trading.engine.pricing.market;

import static org.junit.jupiter.api.Assertions.assertTrue;

import com.trading.engine.testsupport.clock.ControllableNanoClock;
import org.junit.jupiter.api.Test;

/**
 * Zero-allocation regression test for the {@link MarketDataPublisher} hot path.
 *
 * <p><b>Purpose.</b> Asserts that the steady-state {@code onTick} + {@code doWork} cycle allocates
 * zero bytes on the agent thread (≤ {@link #MAX_BYTES_PER_CALL} bytes/call to absorb TLAB-sample
 * noise). A pre-warm phase exhausts all lazy initialisation paths so the measurement loop only
 * exercises already-hot JIT code:
 *
 * <ol>
 *   <li>Every symbol in the cohort pre-populated via an initial {@code onTick} so {@code
 *       computeIfAbsent} is in the cached-hit path.
 *   <li>Rate-limiter {@code long[] lastLogNs} pre-warmed by one rejected tick per {@link
 *       RejectReason} ordinal.
 *   <li>Heartbeat path warmed by one heartbeat cycle (advance clock past interval, drain).
 * </ol>
 *
 * <p>After the three-step pre-warm, 10 000 additional warmup cycles prime the JIT, then 100 000
 * measured cycles verify the zero-alloc invariant.
 *
 * <p><b>Measurement technique.</b> {@link com.sun.management.ThreadMXBean#getThreadAllocatedBytes}
 * provides per-thread TLAB-sample-based byte accounting. The 100 bytes/call threshold absorbs
 * granularity noise (~1–2 kB / 100 k = 10–20 bytes/call) while reliably rejecting any real object
 * allocation (one Java object ≥ 16 bytes → 16 × 100 k = 1.6 MB total, far above the threshold).
 * Mirrors the technique in {@code EventSinkAllocTest} and {@code RfqStateMachineAllocationTest}.
 *
 * <p><b>Threading model.</b> Single-threaded — all calls run on the JUnit test thread, matching the
 * single-writer invariant of {@link MarketDataPublisher}.
 *
 * <p><b>Allocation.</b> This test asserts the production {@code onTick} + drain path allocates zero
 * bytes per call at steady state. The test itself allocates during setup; only the measurement loop
 * is asserted. The {@link NonAllocatingFakePublication} inner class returns {@code 1L} on every
 * offer call and does NOT copy bytes, keeping the fake off the allocation critical path.
 *
 * <p><b>Dependencies.</b> {@link ControllableNanoClock}, {@link com.sun.management.ThreadMXBean}.
 */
final class MarketDataPublisherAllocTest {

  // ─── Alloc-test constants ─────────────────────────────────────────────────

  /**
   * Additional warmup iterations after the three-step pre-warm. Chosen to be at least 2× the slot
   * map INITIAL_SLOT_CAPACITY (16) so all map-entry paths are in the JIT's inline-cache before
   * measurement.
   */
  private static final int WARMUP = 10_000;

  /** Measurement iterations — large enough to amortise TLAB granularity noise. */
  private static final int MEASURE = 100_000;

  /**
   * Maximum bytes-per-call threshold. Absorbs TLAB-sample noise (~10–20 bytes/call) while rejecting
   * any real allocation (one Java object ≥ 16 bytes → 16 × 100 k = 1.6 MB total).
   */
  private static final long MAX_BYTES_PER_CALL = 100L;

  // ─── Symbols + prices ────────────────────────────────────────────────────

  private static final long BID = 118_500_000_000L;
  private static final long ASK = 118_510_000_000L;
  private static final long SIZE = 1_000_000L * 100_000_000L;
  private static final long INGRESS = 1_700_000_000_000_000_000L;
  private static final long CADENCE_MICROS = 5_000L;
  private static final long HEARTBEAT_BASE_MS = 1_000L;

  private static final long EURUSD = pack("EURUSD  ");
  private static final long GBPUSD = pack("GBPUSD  ");
  private static final long USDJPY = pack("USDJPY  ");
  private static final long AUDUSD = pack("AUDUSD  ");

  private static final long[] SYMBOLS = {EURUSD, GBPUSD, USDJPY, AUDUSD};

  private static long pack(final String s) {
    long packed = 0L;
    for (int i = 0; i < 8; i++) {
      final long b = i < s.length() ? (byte) s.charAt(i) : (byte) ' ';
      packed |= (b & 0xFFL) << (i * 8);
    }
    return packed;
  }

  // ─── Non-allocating fake ─────────────────────────────────────────────────

  /**
   * Non-allocating fake implementation of {@link BroadcastPublisher} for use in the measurement
   * loop. Does NOT copy bytes (avoids {@code byte[]} allocation on every offer) — correctness of
   * the published bytes is asserted by {@link MarketDataPublisherTest}, not here.
   *
   * <p><b>Allocation.</b> Zero per call: every method is a constant-return with no object creation.
   */
  private static final class NonAllocatingFakePublication implements BroadcastPublisher {

    @Override
    public long offer(final org.agrona.DirectBuffer buffer, final int offset, final int length) {
      return 1L;
    }

    @Override
    public long position() {
      return 1L;
    }

    @Override
    public int termBufferLength() {
      return 16 * 1_024 * 1_024;
    }
  }

  // ─── Helpers ─────────────────────────────────────────────────────────────

  /**
   * Asserts {@code deltaBytes / MEASURE <= MAX_BYTES_PER_CALL}. Matches the helper convention in
   * {@code RfqStateMachineAllocationTest}.
   *
   * @param label human-readable test name
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
            + " (total delta="
            + delta
            + " over "
            + MEASURE
            + " iterations)");
  }

  // =========================================================================
  // §1 — onTick + drain cycle allocates zero bytes at steady state
  // =========================================================================

  /**
   * Pre-warms all three lazy-init paths (slot map, rate-limiter log array, heartbeat PRNG) and
   * asserts that 100 000 measured {@code onTick} + {@code doWork} cycles allocate at most {@link
   * #MAX_BYTES_PER_CALL} bytes/call on the agent thread.
   */
  @Test
  void onTickDrain_steadyState_zeroAllocation() {
    final var fake = new NonAllocatingFakePublication();
    final var clock = new ControllableNanoClock(1_000_000_000L);
    final var config =
        new MarketDataPublisherConfig(
            MarketDataPublisherConfig.AdapterKind.DETERMINISTIC, CADENCE_MICROS, HEARTBEAT_BASE_MS);
    final var publisher = new MarketDataPublisher(fake, null, clock, clock, config);
    publisher.onStart();

    // ── Step 1: pre-populate all symbol slots (computeIfAbsent first-touch) ──
    for (final long sym : SYMBOLS) {
      publisher.onTick(sym, BID, ASK, SIZE, SIZE, INGRESS);
    }
    clock.advanceMillis(10L);
    publisher.doWork();

    // ── Step 2: warm the rate-limiter log array (one rejected tick per reason ordinal) ──
    final long packed = EURUSD;
    publisher.onTick(packed, ASK, ASK, SIZE, SIZE, INGRESS); // CROSSED
    publisher.onTick(packed, 0L, ASK, SIZE, SIZE, INGRESS); // NON_POSITIVE
    clock.advanceMillis(10L);
    publisher.doWork();

    // ── Step 3: warm the heartbeat path ──
    clock.advanceMillis(1_100L); // exceeds ±10% heartbeat jitter band
    publisher.doWork();

    // ── JIT warmup: 10 000 additional onTick + doWork cycles ─────────────────
    for (int i = 0; i < WARMUP; i++) {
      publisher.onTick(EURUSD, BID, ASK, SIZE, SIZE, INGRESS + i);
      clock.advanceMillis(10L);
      publisher.doWork();
    }

    // ── Measurement ──────────────────────────────────────────────────────────
    final var bean =
        (com.sun.management.ThreadMXBean) java.lang.management.ManagementFactory.getThreadMXBean();
    final long threadId = Thread.currentThread().getId();
    final long before = bean.getThreadAllocatedBytes(threadId);

    for (int i = 0; i < MEASURE; i++) {
      publisher.onTick(EURUSD, BID, ASK, SIZE, SIZE, INGRESS + i);
      clock.advanceMillis(10L);
      publisher.doWork();
    }

    final long after = bean.getThreadAllocatedBytes(threadId);
    assertZeroAlloc("onTick+doWork steady state", before, after);
  }
}
