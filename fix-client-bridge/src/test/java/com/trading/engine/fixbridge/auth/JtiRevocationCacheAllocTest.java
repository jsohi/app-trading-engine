package com.trading.engine.fixbridge.auth;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.lang.management.ManagementFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

/**
 * Allocation regression tripwire for {@link JtiRevocationCache#isRevoked(String, long)}.
 *
 * <p>Asserts {@code GarbageCollectorMXBean.getCollectionCount()} does not advance during
 * {@code 100_000} steady-state iterations of {@code isRevoked} on a pre-revoked JTI. The
 * production hot path ({@code isRevoked}) is documented as zero-alloc; this test enforces that
 * invariant after JIT warm-up.
 *
 * <p>Gated by {@code -DrunAllocTests=true} so the regular {@code test} task skips it (locked
 * §21, §23).
 *
 * <p><b>Threading.</b> Single-threaded. {@link JtiRevocationCache} is not thread-safe per its
 * contract; the test owns it exclusively.
 */
@EnabledIfSystemProperty(named = "runAllocTests", matches = "true")
final class JtiRevocationCacheAllocTest {

  private static final int WARMUP_ITERATIONS = 5_000;
  private static final int MEASURED_ITERATIONS = 100_000;

  /** A fixed "now" that is safely before the entry's expiry so no lazy eviction fires. */
  private static final long NOW_NS = 1_000L;

  /** Expiry set far in the future to prevent lazy eviction during the measured loop. */
  private static final long EXP_NS = Long.MAX_VALUE;

  @Test
  void isRevoked_repeated_zeroAlloc() {
    final var cache = new JtiRevocationCache();
    cache.revoke("jti1", EXP_NS);

    // Warmup — let the JIT compile the hot isRevoked path before measuring.
    for (int i = 0; i < WARMUP_ITERATIONS; i++) {
      final boolean revoked = cache.isRevoked("jti1", NOW_NS);
      // Prevent dead-code elimination: consume result.
      if (!revoked) {
        throw new AssertionError("jti1 should be revoked during warmup at now=" + NOW_NS);
      }
    }

    // Measured phase.
    final long beforeGc = totalGcCount();
    for (int i = 0; i < MEASURED_ITERATIONS; i++) {
      final boolean revoked = cache.isRevoked("jti1", NOW_NS);
      // Prevent dead-code elimination: consume result.
      if (!revoked) {
        throw new AssertionError("jti1 should be revoked during measured phase at now=" + NOW_NS);
      }
    }
    final long afterGc = totalGcCount();

    assertEquals(
        beforeGc,
        afterGc,
        "JtiRevocationCache.isRevoked advanced GC count from " + beforeGc + " to " + afterGc);
  }

  // ---------------------------------------------------------------------------
  // GC count helper — mirrors pattern from other *AllocTest classes.
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
