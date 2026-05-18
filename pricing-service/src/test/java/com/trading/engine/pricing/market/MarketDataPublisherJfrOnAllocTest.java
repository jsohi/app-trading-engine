package com.trading.engine.pricing.market;

import static org.junit.jupiter.api.Assertions.assertTrue;

import com.trading.engine.testsupport.clock.ControllableNanoClock;
import java.time.Duration;
import jdk.jfr.Recording;
import org.junit.jupiter.api.Test;

/**
 * JFR-recording-on zero-allocation regression test for the {@link MarketDataPublisher} hot path.
 *
 * <p><b>Purpose.</b> Complements {@link MarketDataPublisherAllocTest} (which runs with JFR
 * <em>off</em>) by asserting that the steady-state {@code onTick} + {@code doWork} cycle remains
 * allocation-free even when a JFR {@link Recording} is actively running and the publisher's three
 * custom events ({@code trading.MarketDataTickPublished}, {@code trading.MarketDataTickRejected},
 * {@code trading.MarketDataFeedStateTransition}) are enabled.
 *
 * <p><b>Emit-site pattern.</b> The Phase 3 C.4 commits adopted the cheap pre-construction {@link
 * jdk.jfr.EventType#isEnabled()} gate recommended by the OpenJDK JFR team:
 *
 * <pre>{@code
 * if (MarketDataTickPublished.TYPE.isEnabled()) {
 *     final var e = new MarketDataTickPublished();
 *     e.symbol = ...;
 *     e.commit();
 * }
 * }</pre>
 *
 * <p>The previous shape — {@code final var e = new ...(); if (e.shouldCommit())} — relied on
 * HotSpot escape analysis to scalar-replace the {@code new Event()} allocation when the
 * post-construction {@code shouldCommit()} call returned {@code false}. EA cannot prove that the
 * native-method-backed {@code shouldCommit()} is pure, so the {@code Event} object escapes to the
 * heap on every emit; under JFR recording this leaked ~480 bytes/call across the 4-symbol cohort,
 * tripping the earlier 250 bytes/call budget. {@link jdk.jfr.EventType#isEnabled()} is a cheap
 * volatile read that returns {@code false} when no recording has subscribed to the event type — so
 * when disabled (the production case AND the JFR-OFF sibling test) NO {@code Event} is allocated.
 * When enabled (this test), we pay the {@code Event} subclass allocation plus the JFR commit-path
 * ring-buffer housekeeping on every emit; that is the price of the recording.
 *
 * <p><b>Budget rationale (measured).</b> The JFR-OFF sibling test uses 100 bytes/call. With JFR
 * recording and three custom event types subscribed, the steady-state {@code onTick} + {@code
 * doWork} loop measures {@code ~480 bytes/call} on JDK 25. The breakdown:
 *
 * <ul>
 *   <li>One {@link MarketDataTickPublished} subclass allocation per drain (~96 B header + fields).
 *   <li>One {@code byte[8]} + one {@code String} allocation per drain from {@link
 *       MarketDataPublisher#unpackSymbol(long)} (~64 B together) — only executed inside the
 *       enabled-gate.
 *   <li>JFR commit-path housekeeping (chunk-checkpoint allocations, metadata records) inside {@code
 *       jdk.jfr.internal} (~300 B / commit on JDK 25).
 *   <li>TLAB-sample granularity noise (~10–20 bytes/call) inherited from the JFR-OFF test.
 * </ul>
 *
 * <p>The 600-byte ceiling is the measured ~480 plus ~25 % headroom to absorb JDK minor-version
 * drift in the {@code jdk.jfr.internal} commit-path; anything above this indicates EITHER the
 * pre-construction gate has regressed (e.g. someone deleted the {@code TYPE.isEnabled()} check), OR
 * the JDK has materially regressed the commit path. Both deserve investigation. The budget may need
 * re-tuning on JDK upgrade — escape analysis heuristics and JFR commit-path internals evolve
 * between LTS releases.
 *
 * <p><b>Measurement technique.</b> Identical to {@link MarketDataPublisherAllocTest}: {@code
 * com.sun.management.ThreadMXBean#getThreadAllocatedBytes} samples the test thread's allocation
 * counter before and after the measurement loop. Single-threaded — the recording is started on the
 * same JUnit thread that drives the publisher.
 *
 * <p><b>Threading model.</b> Single-threaded — all calls run on the JUnit test thread, matching the
 * single-writer invariant of {@link MarketDataPublisher}. The JFR runtime owns its own background
 * flush thread; that thread's allocations are not attributed to the test thread by {@code
 * getThreadAllocatedBytes}, so the measurement is a clean per-thread view.
 *
 * <p><b>Dependencies.</b> {@link ControllableNanoClock}, {@link com.sun.management.ThreadMXBean},
 * {@link Recording}.
 *
 * @see MarketDataPublisherAllocTest
 */
final class MarketDataPublisherJfrOnAllocTest {

  // ─── Alloc-test constants ─────────────────────────────────────────────────

  /** Additional warmup iterations after the three-step pre-warm — mirrors the JFR-OFF test. */
  private static final int WARMUP = 10_000;

  /** Measurement iterations — large enough to amortise TLAB granularity noise. */
  private static final int MEASURE = 100_000;

  /**
   * Maximum bytes-per-call threshold when JFR is recording. Measured at ~480 bytes/call on JDK 25
   * for the steady-state {@code onTick} + {@code doWork} loop with all three publisher JFR events
   * subscribed: one {@code MarketDataTickPublished} subclass allocation per drain (~96 B), one
   * {@code byte[8]} + {@code String} from {@code unpackSymbol} (~64 B together) inside the {@link
   * jdk.jfr.EventType#isEnabled()} gate, plus ~300 B of {@code jdk.jfr.internal} commit-path
   * housekeeping. The 600-byte ceiling adds ~25 % headroom over the measured floor to absorb JDK
   * minor-version drift in the commit path while still loudly rejecting any regression that deletes
   * the {@link jdk.jfr.EventType#isEnabled()} pre-construction gate (which would roughly double the
   * per-call cost on the JFR-off case AND introduce per-call alloc when JFR is off — caught by the
   * sibling {@link MarketDataPublisherAllocTest}).
   */
  private static final long BUDGET_BYTES_PER_CALL = 600L;

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
   * Non-allocating fake implementation of {@link BroadcastPublisher}. Identical to the sibling
   * defined in {@link MarketDataPublisherAllocTest} — copied here rather than shared so each
   * alloc-test file is self-contained and a refactor of one will not silently corrupt the other.
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
   * Asserts {@code (after - before) / MEASURE <= BUDGET_BYTES_PER_CALL}. On failure the message
   * includes the measured per-call bytes so a JFR escape-analysis regression is immediately
   * diagnosable from the test output.
   *
   * @param label human-readable test name
   * @param before bytes allocated before the measurement loop
   * @param after bytes allocated after the measurement loop
   */
  private static void assertWithinBudget(final String label, final long before, final long after) {
    final long delta = after - before;
    final long perCall = delta / MEASURE;
    assertTrue(
        perCall <= BUDGET_BYTES_PER_CALL,
        label
            + ": expected <= "
            + BUDGET_BYTES_PER_CALL
            + " bytes/call with JFR recording on, got "
            + perCall
            + " (total delta="
            + delta
            + " over "
            + MEASURE
            + " iterations). A value ≫ budget indicates EITHER the pre-construction"
            + " EventType.isEnabled() gate has regressed (someone replaced it with the post-construction"
            + " Event.shouldCommit() pattern, allowing the Event allocation to escape) OR the JDK"
            + " has materially regressed the jdk.jfr.internal commit path — investigate both before"
            + " loosening the budget.");
  }

  // =========================================================================
  // §1 — onTick + drain cycle allocates zero bytes at steady state with JFR ON
  // =========================================================================

  /**
   * Pre-warms all three lazy-init paths (slot map, rate-limiter log array, heartbeat PRNG), starts
   * a JFR {@link Recording} that enables every custom event the publisher emits, and asserts that
   * 100 000 measured {@code onTick} + {@code doWork} cycles allocate at most {@link
   * #BUDGET_BYTES_PER_CALL} bytes/call on the agent thread.
   *
   * <p>If this test fails, the most likely cause is removal of the {@link
   * jdk.jfr.EventType#isEnabled()} pre-construction gate at an emit site — see the class-level
   * Javadoc for diagnostic guidance.
   */
  @Test
  void publishHotPath_zeroAllocation_evenWithJfrRecording() {
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

    // ── Measurement (JFR ON) ─────────────────────────────────────────────────
    final var bean =
        (com.sun.management.ThreadMXBean) java.lang.management.ManagementFactory.getThreadMXBean();
    final long threadId = Thread.currentThread().getId();

    try (final var recording = new Recording()) {
      // Enable all three custom events the publisher emits. Use the @Period-driven sampling for
      // the publish event (matches the @Period("100 ms") on the Event class) and a zero-duration
      // threshold for the point-in-time events so every commit() lands in the buffer when the
      // shouldCommit() guard returns true.
      recording.enable("trading.MarketDataTickPublished").withPeriod(Duration.ofMillis(100));
      recording.enable("trading.MarketDataTickRejected").withThreshold(Duration.ZERO);
      recording.enable("trading.MarketDataFeedStateTransition");
      recording.start();

      // A brief warmup post-start lets the JFR runtime install its sampling timer and finish any
      // first-flight bookkeeping — without this, the first ~hundred iterations would attribute the
      // JFR install cost to the measurement window.
      for (int i = 0; i < 1_000; i++) {
        publisher.onTick(EURUSD, BID, ASK, SIZE, SIZE, INGRESS + WARMUP + i);
        clock.advanceMillis(10L);
        publisher.doWork();
      }

      final long before = bean.getThreadAllocatedBytes(threadId);

      for (int i = 0; i < MEASURE; i++) {
        publisher.onTick(EURUSD, BID, ASK, SIZE, SIZE, INGRESS + i);
        clock.advanceMillis(10L);
        publisher.doWork();
      }

      final long after = bean.getThreadAllocatedBytes(threadId);

      recording.stop();
      assertWithinBudget("onTick+doWork steady state with JFR ON", before, after);
    }
  }
}
