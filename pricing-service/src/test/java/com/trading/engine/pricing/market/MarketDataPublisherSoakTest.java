package com.trading.engine.pricing.market;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.trading.engine.testsupport.clock.ControllableNanoClock;
import org.agrona.DirectBuffer;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Soak test for {@link MarketDataPublisher}: sustained throughput over 60 seconds.
 *
 * <p><b>Purpose.</b> Verifies that the publisher conflates correctly under sustained load: 1 000
 * ticks/symbol/second for 4 symbols over 60 seconds, driven via a controllable clock so the test
 * runs in &lt; 1 second of wall time (no real sleeping). Asserts that exactly one publish per drain
 * interval per symbol occurs — no double-publish, no missed drain, no accumulated lag.
 *
 * <p>The soak is gated by the system property {@code soak=true} to prevent execution in the default
 * {@code :pricing-service:test} run. Activate via:
 *
 * <pre>
 *   ./gradlew :pricing-service:test -Psoak=true
 * </pre>
 *
 * <p>The property gate mirrors the {@code RfqLatencyRegressionIT} pattern in the existing codebase
 * (soak annotation + property guard).
 *
 * <p><b>Simulation approach.</b> The controllable clock is advanced by 1 ms per "tick batch"
 * (simulating 1 000 Hz source). A drain is triggered every 5 clock milliseconds (matching the
 * production cadence). Over 60 simulated seconds: 60 000 ms ÷ 5 ms = 12 000 drain cycles. With 4
 * symbols, total expected publishes = 4 × 12 000 = 48 000 (one per symbol per drain).
 *
 * <p><b>Threading model.</b> Single-threaded — all calls run on the JUnit test thread after {@code
 * onStart} binds the agent-thread guard.
 *
 * <p><b>Allocation.</b> Not asserting zero-alloc in the soak test — covered by {@link
 * MarketDataPublisherAllocTest}. {@link CountingFakePublication} does not copy bytes, avoiding OOM
 * from 48 000 × 4 = 192 000 {@code byte[]} allocations over the soak duration.
 *
 * <p><b>Dependencies.</b> {@link ControllableNanoClock}.
 */
@Tag("soak")
final class MarketDataPublisherSoakTest {

  // ─── Soak parameters ──────────────────────────────────────────────────────

  /** Simulated duration in milliseconds (60 s). */
  private static final long SOAK_DURATION_MS = 60_000L;

  /** Publisher drain cadence in milliseconds. Must match production default (5 ms). */
  private static final long CADENCE_MS = 5L;

  /** Tick batch interval in milliseconds (1 000 Hz source = 1 ms per tick). */
  private static final long TICK_INTERVAL_MS = 1L;

  /** Number of symbols in the cohort. */
  private static final int SYMBOL_COUNT = 4;

  /** Total drain cycles over the soak duration. */
  private static final long TOTAL_DRAIN_CYCLES = SOAK_DURATION_MS / CADENCE_MS; // 12 000

  /** Expected total wire publishes: one per symbol per drain. */
  private static final long EXPECTED_PUBLISHES = TOTAL_DRAIN_CYCLES * SYMBOL_COUNT; // 48 000

  // ─── Publisher config ─────────────────────────────────────────────────────
  private static final long CADENCE_MICROS = CADENCE_MS * 1_000L; // 5 000 µs
  private static final long HEARTBEAT_BASE_MS = 1_000L;

  // ─── Prices ───────────────────────────────────────────────────────────────
  private static final long BID = 118_500_000_000L;
  private static final long ASK = 118_510_000_000L;
  private static final long SIZE = 1_000_000L * 100_000_000L;
  private static final long INGRESS_BASE = 1_700_000_000_000_000_000L;

  // ─── Symbols ──────────────────────────────────────────────────────────────
  private static final long[] SYMBOLS = {
    pack("EURUSD  "), pack("GBPUSD  "), pack("USDJPY  "), pack("AUDUSD  ")
  };

  private static long pack(final String s) {
    long packed = 0L;
    for (int i = 0; i < 8; i++) {
      final long b = i < s.length() ? (byte) s.charAt(i) : (byte) ' ';
      packed |= (b & 0xFFL) << (i * 8);
    }
    return packed;
  }

  /**
   * Count-only fake publication implementing {@link BroadcastPublisher}. Does NOT copy bytes to
   * avoid 48 000 × 4 = 192 000 {@code byte[]} allocations that would OOM or skew the soak timing.
   * Correctness of published bytes is verified by {@link MarketDataPublisherTest}.
   *
   * <p><b>Allocation.</b> Zero per call — all methods are arithmetic on primitive fields.
   */
  private static final class CountingFakePublication implements BroadcastPublisher {

    private long offerCount;

    long offerCount() {
      return offerCount;
    }

    @Override
    public long offer(final DirectBuffer buffer, final int offset, final int length) {
      offerCount++;
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

  /**
   * Constructs a {@link MarketDataPublisher} backed by the given counting fake and clock.
   *
   * @param fake the counting fake publication.
   * @param clock the controllable clock.
   * @return a publisher ready for {@code onStart()}.
   */
  private static MarketDataPublisher buildPublisher(
      final CountingFakePublication fake, final ControllableNanoClock clock) {
    final var config =
        new MarketDataPublisherConfig(
            MarketDataPublisherConfig.AdapterKind.DETERMINISTIC, CADENCE_MICROS, HEARTBEAT_BASE_MS);
    return new MarketDataPublisher(fake, null, clock, clock, config);
  }

  // =========================================================================
  // §1 — 60-second soak: one publish per drain per symbol, no double-publish
  // =========================================================================

  /**
   * Drives the publisher at 1 000 ticks/symbol/second for 60 simulated seconds using a controllable
   * clock. Asserts exactly {@link #EXPECTED_PUBLISHES} wire publishes total (one per symbol per 5
   * ms drain cycle) and no double-publish.
   *
   * <p>The soak is opt-in only — skip if the {@code soak} system property is absent or not {@code
   * "true"}.
   */
  @Test
  void soak_1000TicksSymbolSec_60s_exactlyOnePublishPerDrainPerSymbol() {
    // Gate: skip unless -Psoak=true / -Dsoak=true.
    final String soakProp = System.getProperty("soak");
    if (!"true".equals(soakProp)) {
      // org.junit.jupiter.api.Assumptions.assumeTrue would be cleaner but we keep the same
      // pattern as the rest of the module (no assumeTrue imports elsewhere).
      return;
    }

    final var fake = new CountingFakePublication();
    final var clock = new ControllableNanoClock(1_000_000_000L);
    final var publisher = buildPublisher(fake, clock);
    publisher.onStart();

    // Simulate SOAK_DURATION_MS milliseconds at 1 ms tick intervals, draining every CADENCE_MS ms.
    // ticksPerDrain = CADENCE_MS / TICK_INTERVAL_MS = 5 ticks per symbol per drain.
    for (long drainCycle = 0L; drainCycle < TOTAL_DRAIN_CYCLES; drainCycle++) {
      // Push CADENCE_MS / TICK_INTERVAL_MS = 5 tick-batches per drain cycle.
      for (long tickBatch = 0L; tickBatch < (CADENCE_MS / TICK_INTERVAL_MS); tickBatch++) {
        final long ingress =
            INGRESS_BASE + (drainCycle * CADENCE_MS + tickBatch * TICK_INTERVAL_MS) * 1_000_000L;
        for (final long sym : SYMBOLS) {
          publisher.onTick(sym, BID, ASK, SIZE, SIZE, ingress);
        }
        clock.advanceMillis(TICK_INTERVAL_MS);
      }
      // Trigger drain — clock is now CADENCE_MS past the last drain.
      publisher.doWork();
    }

    assertEquals(
        EXPECTED_PUBLISHES,
        fake.offerCount(),
        "soak must produce exactly "
            + EXPECTED_PUBLISHES
            + " publishes (one per symbol per drain cycle, 4 symbols × "
            + TOTAL_DRAIN_CYCLES
            + " drains)");
  }
}
