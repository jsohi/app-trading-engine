package com.trading.engine.cluster;

import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/**
 * Regression test for the APP-151 phase 4 idle-scan timer's correlation-id space disjointness from
 * {@code RfqStateMachine}'s TTL and request-timeout namespaces.
 *
 * <p>Bug history:
 *
 * <ul>
 *   <li>v1 used {@link Long#MIN_VALUE} — collided with RFQ request-timeout for slot 0 generation 0
 *       ({@code REQUEST_TIMEOUT_NAMESPACE_BIT | 0 == Long.MIN_VALUE}).
 *   <li>v2 switched to {@link Long#MAX_VALUE} — disjoint from RFQ but collided with Aeron's
 *       internal {@code WheelTimerService} {@code Long2LongHashMap} whose missingValue sentinel is
 *       {@code Long.MAX_VALUE}.
 *   <li>v3 (current) uses {@code -1L} — above the RFQ request-timeout range AND distinct from both
 *       sentinels above.
 * </ul>
 *
 * <p>This test asserts the v3 invariant holds.
 *
 * <p><b>Threading:</b> test-only — runs on the JUnit worker thread.
 *
 * @see com.trading.engine.cluster.TradingClusteredService#IDLE_SCAN_TIMER_CORRELATION_ID
 */
class TradingClusteredServiceCorrelationIdTest {

  /** Asserts the current chosen value is {@code -1L} (v3). */
  @Test
  void idleScanCorrelationId_isMinusOne() {
    assertNotEquals(Long.MIN_VALUE, TradingClusteredService.IDLE_SCAN_TIMER_CORRELATION_ID);
    assertNotEquals(Long.MAX_VALUE, TradingClusteredService.IDLE_SCAN_TIMER_CORRELATION_ID);
    org.junit.jupiter.api.Assertions.assertEquals(
        -1L, TradingClusteredService.IDLE_SCAN_TIMER_CORRELATION_ID);
  }

  /**
   * Verifies the idle-scan correlation id does NOT collide with RFQ's request-timeout namespace for
   * any reachable {@code (gen, poolIndex)}. RFQ request-timeout ids equal {@code
   * REQUEST_TIMEOUT_NAMESPACE_BIT | ttlCorrelation}; the top of that range is {@code
   * 0xBFFF_FFFF_FFFF_FFFFL}.
   */
  @Test
  void idleScanCorrelationId_disjointFromRfqRequestTimeoutNamespace() {
    // RfqStateMachine.REQUEST_TIMEOUT_NAMESPACE_BIT is package-private; mirror as a literal.
    final long requestTimeoutNamespaceBit = 0x8000_0000_0000_0000L;

    // Boundary: slot 0 gen 0 — original v1 collision exact value.
    final long slot0Gen0Timeout = requestTimeoutNamespaceBit | 0L;
    assertNotEquals(
        slot0Gen0Timeout,
        TradingClusteredService.IDLE_SCAN_TIMER_CORRELATION_ID,
        "must not collide with RFQ request-timeout for slot 0 generation 0 (the original v1 bug)");

    // Boundary: max possible RFQ request-timeout id.
    final long maxTtlCorrelation = ((long) Integer.MAX_VALUE << 31) | (long) Integer.MAX_VALUE;
    final long maxRequestTimeoutId = requestTimeoutNamespaceBit | maxTtlCorrelation;
    assertTrue(
        TradingClusteredService.IDLE_SCAN_TIMER_CORRELATION_ID > maxRequestTimeoutId,
        "idle-scan correlation id ("
            + TradingClusteredService.IDLE_SCAN_TIMER_CORRELATION_ID
            + ") must be above the entire RFQ request-timeout range; max request-timeout id is "
            + maxRequestTimeoutId);
  }

  /**
   * Verifies the idle-scan correlation id is not {@link Long#MAX_VALUE} — which is Aeron's internal
   * {@code WheelTimerService} {@code Long2LongHashMap} missingValue sentinel. {@code
   * scheduleTimer(Long.MAX_VALUE, …)} throws {@code IllegalArgumentException: "cannot accept
   * missingValue"}; this regression check prevents anyone from re-choosing that value.
   */
  @Test
  void idleScanCorrelationId_notAeronWheelTimerSentinel() {
    assertNotEquals(
        Long.MAX_VALUE,
        TradingClusteredService.IDLE_SCAN_TIMER_CORRELATION_ID,
        "must not equal Long.MAX_VALUE — Aeron's WheelTimerService rejects it as its internal"
            + " Long2LongHashMap missingValue sentinel (the v2 bug)");
  }

  /**
   * Verifies the idle-scan correlation id is also not in RFQ's positive-TTL range (defensive). RFQ
   * TTL ids are {@code (gen << 31) | poolIndex} — all non-negative.
   */
  @Test
  void idleScanCorrelationId_disjointFromRfqTtlNamespace() {
    // The chosen -1L is negative; RFQ TTL ids are non-negative, so any negative value is disjoint.
    assertTrue(
        TradingClusteredService.IDLE_SCAN_TIMER_CORRELATION_ID < 0L
            || TradingClusteredService.IDLE_SCAN_TIMER_CORRELATION_ID
                > (((long) Integer.MAX_VALUE << 31) | (long) Integer.MAX_VALUE),
        "idle-scan correlation id must lie outside RFQ TTL range (non-negative longs ≤"
            + " 0x3FFF_FFFF_FFFF_FFFFL)");
  }
}
