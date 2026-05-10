package com.trading.engine.fixbridge.ratelimit;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.trading.engine.fixbridge.ratelimit.PerTypeRateLimiter.CommandType;
import com.trading.engine.fixbridge.ratelimit.PerTypeRateLimiter.Outcome;
import java.lang.management.ManagementFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

/**
 * Allocation regression tripwire for {@link PerTypeRateLimiter}. Mirrors the {@code
 * GarbageCollectorMXBean.getCollectionCount()} delta pattern from other {@code *AllocTest}s in
 * this module: warm the JIT, sample GC count, run {@link PerTypeRateLimiter#tryConsume} in a tight
 * loop, sample again, assert no GC advanced.
 *
 * <p>Gated by {@code -DrunAllocTests=true} (locked §21, §23) — opt-in only because GC counts can
 * be advanced by unrelated background processes on a shared CI host.
 *
 * <p>Threading: single-threaded. The {@link PerTypeRateLimiter} is not thread-safe per its
 * contract; the test owns it exclusively.
 */
@EnabledIfSystemProperty(named = "runAllocTests", matches = "true")
final class PerTypeRateLimiterAllocTest {

  private static final int WARMUP_ITERATIONS = 5_000;
  private static final int MEASURED_ITERATIONS = 100_000;

  /**
   * Auth anchor at a fixed offset to keep arithmetic deterministic.
   * Using zero would risk underflow in nanosecond subtraction during warmup.
   */
  private static final long AUTH_NANOS = 1_000_000_000_000L; // 1000 s in nanos

  /**
   * Advance 200 ms per iteration — 0.2 s × 5 tokens/s = 1.0 token refilled per step.
   * This keeps the QuoteRequest normal bucket from depleting, ensuring every iteration
   * takes the ALLOWED path (the hot path).
   */
  private static final long STEP_NS = 200_000_000L; // 200 ms

  // ---------------------------------------------------------------------------
  // Test: repeated ALLOWED calls do not allocate.
  // ---------------------------------------------------------------------------

  @Test
  void tryConsume_repeatedAllowed_zeroAlloc() {
    // Start after the initial window so the normal bucket (burst=10, sustained=5/s) is used.
    final long windowEnd = AUTH_NANOS + PerTypeRateLimiter.FIRST_60S_WINDOW_NANOS;
    final var limiter = new PerTypeRateLimiter(AUTH_NANOS);

    // Warmup — let JIT compile the hot tryConsume path before we measure.
    long nowNs = windowEnd;
    for (int i = 0; i < WARMUP_ITERATIONS; i++) {
      final var outcome = limiter.tryConsume(CommandType.QUOTE_REQUEST, nowNs);
      // Suppress dead-code elimination: consume result.
      if (outcome == Outcome.REJECTED_RATE_LIMIT) {
        // Advance clock to trigger refill and stay on the ALLOWED path.
        nowNs += STEP_NS;
      } else {
        nowNs += STEP_NS;
      }
    }

    // Measured phase.
    nowNs = windowEnd + (long) WARMUP_ITERATIONS * STEP_NS;
    final long beforeGc = totalGcCount();
    for (int i = 0; i < MEASURED_ITERATIONS; i++) {
      final var outcome = limiter.tryConsume(CommandType.QUOTE_REQUEST, nowNs);
      // Suppress dead-code elimination.
      if (outcome != Outcome.ALLOWED && outcome != Outcome.REJECTED_RATE_LIMIT) {
        throw new AssertionError("unexpected outcome: " + outcome);
      }
      nowNs += STEP_NS;
    }
    final long afterGc = totalGcCount();

    assertEquals(
        beforeGc,
        afterGc,
        "PerTypeRateLimiter.tryConsume advanced GC count from " + beforeGc + " to " + afterGc);
  }

  // ---------------------------------------------------------------------------
  // GC count helper — shared with other *AllocTest classes.
  // ---------------------------------------------------------------------------

  private static long totalGcCount() {
    long total = 0L;
    final var beans = ManagementFactory.getGarbageCollectorMXBeans();
    for (final var bean : beans) {
      final long c = bean.getCollectionCount();
      if (c >= 0L) {
        total += c;
      }
    }
    return total;
  }
}
