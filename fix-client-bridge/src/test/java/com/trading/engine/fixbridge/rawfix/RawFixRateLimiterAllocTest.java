package com.trading.engine.fixbridge.rawfix;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.lang.management.ManagementFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

/**
 * Allocation regression tripwire for {@link RawFixRateLimiter#tryConsume(long)}.
 *
 * <p>Mirrors the {@code GarbageCollectorMXBean.getCollectionCount()} delta pattern used by other
 * {@code *AllocTest}s in this module: warm the JIT, sample GC count, run the hot path in a tight
 * loop, sample again, assert no GC collection occurred.
 *
 * <p>Gated by {@code -DrunAllocTests=true} — opt-in only because GC counts can be advanced by
 * unrelated background processes on a shared CI host (locked §21, §23).
 *
 * <p>Threading: single-threaded. {@link RawFixRateLimiter} is not thread-safe per its contract; the
 * test owns the instance exclusively.
 */
@EnabledIfSystemProperty(named = "runAllocTests", matches = "true")
final class RawFixRateLimiterAllocTest {

  private static final int WARMUP_ITERATIONS = 5_000;
  private static final int MEASURED_ITERATIONS = 100_000;

  /** Fixed construction timestamp — non-zero to avoid underflow in elapsed arithmetic. */
  private static final long T0 = 1_000_000_000_000L; // 1000 s in nanos

  /**
   * Advance 1 ms per iteration. At DEFAULT_RATE_PER_SEC = 1000 tokens/s and 1 ms steps, each step
   * refills exactly 1.0 token. This keeps the bucket from depleting so every iteration takes the
   * ALLOWED path — the hot path — throughout the measured phase.
   */
  private static final long STEP_NS = 1_000_000L; // 1 ms

  /**
   * Run {@link RawFixRateLimiter#tryConsume} 100k times advancing the clock 1 ms per call (refills
   * 1 token per step at 1000/s, keeping the ALLOWED path hot) and assert that no GC collection
   * fires after JIT warmup. Confirms the zero-alloc guarantee documented on the class.
   */
  @Test
  void tryConsume_repeatedAllowed_zeroAlloc() {
    final var limiter = new RawFixRateLimiter(T0);

    // Warmup — let the JIT compile the hot tryConsume/refill path before measuring.
    long nowNs = T0;
    for (int i = 0; i < WARMUP_ITERATIONS; i++) {
      nowNs += STEP_NS;
      // Consume result to suppress dead-code elimination.
      if (!limiter.tryConsume(nowNs)) {
        // Should not happen given 1 refill per step, but keep the loop live if it does.
        nowNs += STEP_NS;
      }
    }

    // Measured phase.
    final long beforeGc = totalGcCount();
    for (int i = 0; i < MEASURED_ITERATIONS; i++) {
      nowNs += STEP_NS;
      // Consume result to suppress dead-code elimination.
      if (!limiter.tryConsume(nowNs)) {
        nowNs += STEP_NS;
      }
    }
    final long afterGc = totalGcCount();

    assertEquals(
        beforeGc,
        afterGc,
        "RawFixRateLimiter.tryConsume advanced GC count from " + beforeGc + " to " + afterGc);
  }

  // ---------------------------------------------------------------------------
  // GC count helper — shared pattern across *AllocTest classes in this module.
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
