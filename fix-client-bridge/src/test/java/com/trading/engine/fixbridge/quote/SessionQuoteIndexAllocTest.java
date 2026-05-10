package com.trading.engine.fixbridge.quote;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.lang.management.ManagementFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

/**
 * Allocation regression tripwire for the read-only hot-path methods of {@link SessionQuoteIndex}.
 *
 * <p><b>Scope.</b> Only truly read-only, zero-allocating methods are covered here:
 * {@link SessionQuoteIndex#isOwnedBy}, {@link SessionQuoteIndex#subFor}, and
 * {@link SessionQuoteIndex#sessionsForSub}. Methods that allocate by design
 * ({@link SessionQuoteIndex#onQuoteRequest}, {@link SessionQuoteIndex#onQuoteEmitted},
 * {@link SessionQuoteIndex#onSessionAuthenticated}) are explicitly excluded.
 *
 * <p><b>Gating.</b> Enabled only when the JVM system property {@code runAllocTests=true} is set
 * (set automatically by the {@code allocTest} Gradle task; absent from the default {@code test}
 * task). This matches the pattern used by {@code JsonToFixTranslatorAllocTest} and other
 * {@code *AllocTest} suites in this module.
 *
 * <p><b>Measurement.</b> Uses total GC collection count across all collectors as the zero-alloc
 * proxy, identical to the existing alloc-test pattern in this module. Warmup drives JIT
 * compilation before measurement begins so JIT-internal allocations do not pollute the count.
 *
 * <p><b>Threading.</b> Single-threaded; {@link SessionQuoteIndex} is not thread-safe by design.
 */
@EnabledIfSystemProperty(named = "runAllocTests", matches = "true")
final class SessionQuoteIndexAllocTest {

  private static final int WARMUP_ITERATIONS = 5_000;
  private static final int MEASURED_ITERATIONS = 100_000;

  /** Arbitrary stable timestamp used across all alloc tests. */
  private static final long FIXED_NOW_NS = 1_712_491_200_000_000_000L;

  // ---------------------------------------------------------------------------
  // isOwnedBy — zero-alloc on the accept path
  // ---------------------------------------------------------------------------

  @Test
  void isOwnedBy_validQuote_zeroAlloc() {
    final var index = new SessionQuoteIndex();
    final var session = new SessionId("ALLOC-SESSION-1");
    index.onSessionAuthenticated(session, "alloc-user");
    index.onQuoteRequest("ALLOC-REQ-1", session, FIXED_NOW_NS);
    index.onQuoteEmitted("ALLOC-REQ-1", "ALLOC-QUOTE-1", FIXED_NOW_NS + 1_000_000L);

    // Warmup: drive JIT compilation
    for (int i = 0; i < WARMUP_ITERATIONS; i++) {
      index.isOwnedBy("ALLOC-QUOTE-1", session);
    }

    final long beforeGc = totalGcCount();
    for (int i = 0; i < MEASURED_ITERATIONS; i++) {
      index.isOwnedBy("ALLOC-QUOTE-1", session);
    }
    final long afterGc = totalGcCount();

    assertEquals(beforeGc, afterGc,
        "isOwnedBy advanced GC count " + beforeGc + " -> " + afterGc
            + "; indicates unexpected allocation on the hot path");
  }

  // ---------------------------------------------------------------------------
  // subFor — zero-alloc reverse lookup
  // ---------------------------------------------------------------------------

  @Test
  void subFor_validSession_zeroAlloc() {
    final var index = new SessionQuoteIndex();
    final var session = new SessionId("ALLOC-SESSION-2");
    index.onSessionAuthenticated(session, "alloc-user-2");

    // Warmup: drive JIT compilation
    for (int i = 0; i < WARMUP_ITERATIONS; i++) {
      index.subFor(session);
    }

    final long beforeGc = totalGcCount();
    for (int i = 0; i < MEASURED_ITERATIONS; i++) {
      index.subFor(session);
    }
    final long afterGc = totalGcCount();

    assertEquals(beforeGc, afterGc,
        "subFor advanced GC count " + beforeGc + " -> " + afterGc
            + "; indicates unexpected allocation on the hot path");
  }

  // ---------------------------------------------------------------------------
  // sessionsForSub — zero-alloc live-view lookup (no bucket allocation)
  // ---------------------------------------------------------------------------

  @Test
  void sessionsForSub_existingSub_zeroAlloc() {
    final var index = new SessionQuoteIndex();
    final var session = new SessionId("ALLOC-SESSION-3");
    index.onSessionAuthenticated(session, "alloc-user-3");

    // Warmup: drive JIT compilation
    for (int i = 0; i < WARMUP_ITERATIONS; i++) {
      index.sessionsForSub("alloc-user-3");
    }

    final long beforeGc = totalGcCount();
    for (int i = 0; i < MEASURED_ITERATIONS; i++) {
      index.sessionsForSub("alloc-user-3");
    }
    final long afterGc = totalGcCount();

    assertEquals(beforeGc, afterGc,
        "sessionsForSub advanced GC count " + beforeGc + " -> " + afterGc
            + "; indicates unexpected allocation on the hot path");
  }

  // ---------------------------------------------------------------------------
  // Helper
  // ---------------------------------------------------------------------------

  /**
   * Sum of {@link java.lang.management.GarbageCollectorMXBean#getCollectionCount()} across all
   * registered GC beans. Returns a stable monotonic counter for the zero-alloc proxy: if the
   * measured loop triggers no GC, the count must not advance.
   *
   * @return total GC collection count
   */
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
