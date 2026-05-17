package com.trading.engine.websocket;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.trading.engine.messages.sbe.OrdTypeEnum;
import com.trading.engine.messages.sbe.SideEnum;
import com.trading.engine.projections.SymbolPacker;
import com.trading.engine.testsupport.sbe.SbeTestEncoder;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.LongHashSet;
import org.junit.jupiter.api.Test;

/**
 * Edge-case tests for {@link SubscriptionFilter} verifying the construct-then-publish discipline
 * when the entitlement set is explicitly EMPTY (zero entitled symbols). Every symbol that passes
 * subscription and event-bit matching must still be denied by the entitlement guard once the empty
 * set has been published.
 *
 * <p>This covers the security invariant: a session whose JWT {@code accounts} claim maps to zero
 * entitled symbols MUST NOT receive any market-data events, even if the client subscribes via
 * {@code WebSocketSubscribe}. The empty set published by an auth path that finds no entitled
 * symbols therefore acts as a total block.
 *
 * <p><b>Threading model.</b> Single-threaded — all calls on the test runner thread. The volatile
 * semantics in {@link SubscriptionFilter} are designed for cross-thread safety but are correct
 * within a single thread.
 *
 * <p><b>Allocation note.</b> The correctness variant of the zero-allocation test drives {@link
 * SubscriptionFilter#matches} 100,000 times. If the JVM thread-allocated-bytes API is accessible
 * via the {@code com.sun.management.ThreadMXBean} cast, allocation bytes are also verified. On JVMs
 * where the cast is unavailable the correctness assertion is still performed and the allocation
 * assertion is explicitly skipped with a comment explaining why.
 */
final class SubscriptionFilterEmptyTest {

  private static final int MAX_SUBSCRIPTIONS = 100;

  private final MutableDirectBuffer buffer = new ExpandableArrayBuffer(512);

  /**
   * After publishing an EMPTY {@link LongHashSet}, any subscribed symbol with a matching event bit
   * must still be denied by the entitlement guard. The denial counter must increment once per
   * blocked match call.
   *
   * <p>Wiring:
   *
   * <ol>
   *   <li>Subscribe EURUSD with all event bits.
   *   <li>Publish {@code new LongHashSet(0)} (empty — no entitled symbols).
   *   <li>Call {@link SubscriptionFilter#matches} with a valid EURUSD OrderCreated event.
   *   <li>Assert return value is {@code false}.
   *   <li>Assert {@code websocket.subscription.entitlement.denied} increments by 1.
   * </ol>
   */
  @Test
  void matches_emptyEntitlementSet_deniesEverything() {
    final var registry = new SimpleMeterRegistry();
    final var metrics = new WebSocketMetrics(registry);
    final var filter = new SubscriptionFilter(MAX_SUBSCRIPTIONS, metrics);

    final long packedEur = SymbolPacker.pack("EURUSD");
    filter.addSubscription(packedEur, 0x1F); // all event bits

    // Publish empty entitlement set — no account is entitled to any symbol.
    filter.publishEntitledSymbols(new LongHashSet(0));

    final int len = encodeOrderCreated("EURUSD");
    final byte[] bytes = toByteArray(buffer, len);

    final double before =
        registry.get("websocket.subscription.entitlement.denied").counter().count();
    final boolean result = filter.matches(100, bytes, 0, len);
    final double after =
        registry.get("websocket.subscription.entitlement.denied").counter().count();

    assertFalse(result, "matches must return false when entitlement set is empty");
    assertEquals(
        before + 1.0,
        after,
        1e-9,
        "websocket.subscription.entitlement.denied counter must increment by 1");
  }

  /**
   * Zero-allocation regression for the entitlement-denial hot path. Drives {@link
   * SubscriptionFilter#matches} 100,000 times against an empty entitlement set and asserts:
   *
   * <ol>
   *   <li>Every call returns {@code false} (correctness).
   *   <li>Zero bytes are allocated by the calling thread during the hot loop (allocation check via
   *       {@code com.sun.management.ThreadMXBean}, skipped with a comment when unavailable).
   * </ol>
   *
   * <p>The metrics sink is omitted (1-arg ctor) so the entitlement-denial counter call-path does
   * not interfere with the allocation measurement. The {@code metrics == null} branch in {@link
   * SubscriptionFilter#matches} is the production no-metric code path used before a metrics
   * instance is available, and must itself be zero-allocation.
   */
  @Test
  void matches_emptyEntitlementSet_doesNotAllocate() {
    // Use the no-metrics ctor so the hot path exercises the null-metrics branch
    // (no counter allocation on deny). Entitlement still enforced because
    // publishEntitledSymbols() flips the latch regardless of the metrics sink.
    final var filter = new SubscriptionFilter(MAX_SUBSCRIPTIONS);
    final long packedEur = SymbolPacker.pack("EURUSD");
    filter.addSubscription(packedEur, 0x1F);
    filter.publishEntitledSymbols(new LongHashSet(0));

    final int len = encodeOrderCreated("EURUSD");
    final byte[] bytes = toByteArray(buffer, len);

    // Warm up to trigger JIT compilation before measuring.
    for (int i = 0; i < 5_000; i++) {
      filter.matches(100, bytes, 0, len);
    }

    // --- Allocation measurement ---
    // Attempt to access com.sun.management.ThreadMXBean for per-thread byte accounting.
    // This API is present on HotSpot / OpenJDK but is not part of the standard JDK spec,
    // so the cast may fail on non-HotSpot JVMs (GraalVM native, OpenJ9). If unavailable
    // we still assert correctness and skip the byte assertion with an explanatory comment.
    com.sun.management.ThreadMXBean sunBean = null;
    final var mxBean = java.lang.management.ManagementFactory.getThreadMXBean();
    if (mxBean instanceof com.sun.management.ThreadMXBean candidate
        && candidate.isThreadAllocatedMemoryEnabled()) {
      sunBean = candidate;
    }

    final long tid = Thread.currentThread().getId();
    final long bytesBefore = sunBean != null ? sunBean.getThreadAllocatedBytes(tid) : -1L;

    // Hot loop — 100,000 iterations, all must deny.
    int denials = 0;
    for (int i = 0; i < 100_000; i++) {
      if (!filter.matches(100, bytes, 0, len)) {
        denials++;
      }
    }

    final long bytesAfter = sunBean != null ? sunBean.getThreadAllocatedBytes(tid) : -1L;

    assertEquals(100_000, denials, "Every call must return false against an empty entitlement set");

    if (sunBean != null) {
      // Allow up to 4 KiB of JVM bookkeeping overhead across 100,000 iterations. The
      // matches() call itself is zero-allocation per design; however the JIT deoptimisation
      // mechanism, profiling counters, and GC safepoint polling can each flush a few bytes of
      // metadata during a measured window even on a fully-warmed code path. An allowance of
      // 4 096 bytes over 100,000 calls is ~0.04 bytes/call — orders of magnitude below the
      // threshold that would indicate an allocation regression in the production code path
      // (a single Object allocation is at minimum 16 bytes = 1,600,000 bytes/100k calls).
      final long allocatedBytes = bytesAfter - bytesBefore;
      assertTrue(
          allocatedBytes <= 4_096L,
          "matches() with empty entitlement set must not allocate meaningful memory "
              + "(allocated="
              + allocatedBytes
              + " bytes over 100,000 iterations; "
              + "threshold=4096 bytes)");
    }
    // If sunBean == null: com.sun.management.ThreadMXBean cast was unavailable on this JVM
    // (non-HotSpot or thread-alloc tracking disabled). Correctness assertion above still ran.
  }

  // --- Helpers ---

  private int encodeOrderCreated(final String symbol) {
    return SbeTestEncoder.encodeOrderCreatedEvent(
        buffer,
        0,
        1L,
        1_000L,
        "ORD001",
        "EXEC001",
        "CLORD001",
        symbol,
        SideEnum.Buy,
        OrdTypeEnum.Limit,
        110_000_000L,
        100_000_000L,
        "ACME-001");
  }

  private static byte[] toByteArray(final MutableDirectBuffer buf, final int length) {
    final byte[] bytes = new byte[length];
    buf.getBytes(0, bytes);
    return bytes;
  }
}
