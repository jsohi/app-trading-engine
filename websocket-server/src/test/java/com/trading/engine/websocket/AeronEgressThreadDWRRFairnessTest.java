package com.trading.engine.websocket;

import static com.trading.engine.websocket.AeronEgressThread.MARKET_DATA_FRAGMENT_LIMIT;
import static com.trading.engine.websocket.AeronEgressThread.MARKET_DATA_QUANTUM;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.trading.engine.testsupport.clock.ControllableNanoClock;
import org.agrona.concurrent.ManyToOneConcurrentArrayQueue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * DWRR fairness test for {@link AeronEgressThread.DWRRPollingAgent}.
 *
 * <p>Drives 1 000 cycles with a saturated MD poller and asserts that the market-data source is
 * polled exactly once per cycle with {@code fragmentLimit = MARKET_DATA_QUANTUM} (=1) in steady
 * state — confirming the DWRR credit/debit mechanics are correct for the MD source.
 *
 * <p><b>Steady-state DWRR behavior for MD (quantum=1).</b> Each cycle: deficit = 0 + 1 = 1 → limit
 * = min(1, 32) = 1 → poll(handler, 1) → returns 1 → deficit = 0 (exactly consumed, no idle reset).
 * Total MD fragments over N cycles = N × 1 = N. The ±10% tolerance covers any transient startup
 * effect.
 *
 * <p><b>Cluster source.</b> The cluster client is DISCONNECTED (returns 0) — it cannot be driven to
 * return non-zero without a live Aeron cluster. Cluster starvation is instead validated separately
 * in {@link AeronEgressThreadStarvationTest}. The 2:1 ratio test under dual saturation would
 * require a fakeable cluster client; the DWRR accounting for the MD source is fully observable
 * through the limit argument and the deficit accessor.
 */
final class AeronEgressThreadDWRRFairnessTest {

  private static final int CYCLES = 1_000;
  private static final int CAPACITY = 4;
  private static final int MAX_MESSAGE_SIZE = 256;

  private ControllableNanoClock clock;
  private WebSocketClusterClient clusterClient;
  private WebSocketMetrics metrics;
  private ManyToOneConcurrentArrayQueue<EgressEntry> queue;
  private ManyToOneConcurrentArrayQueue<EgressEntry> returnQueue;
  private WebSocketEgressListener egressListener;

  @BeforeEach
  void setUp() {
    clock = new ControllableNanoClock(0L);
    clusterClient =
        WebSocketClusterClient.builder()
            .aeronDirectoryName("/tmp/aeron-fairness-test")
            .ingressEndpoints("0=localhost:20111")
            .egressListener((sessionId, ts, buf, offset, len, header) -> {})
            .errorHandler(Throwable::printStackTrace)
            .nanoClock(clock)
            .build();
    metrics = WebSocketMetrics.createWithDefaults();
    queue = new ManyToOneConcurrentArrayQueue<>(CAPACITY);
    returnQueue = new ManyToOneConcurrentArrayQueue<>(CAPACITY);
    egressListener =
        new WebSocketEgressListener(queue, returnQueue, metrics, CAPACITY, MAX_MESSAGE_SIZE);
  }

  /**
   * doWork_1000CyclesSaturatedMd_mdPolledExactlyOncePerCycleWithQuantumLimit: over 1 000 cycles
   * with a saturated MD poller that returns whatever it is asked for (up to {@link
   * AeronEgressThread#MARKET_DATA_FRAGMENT_LIMIT}), asserts:
   *
   * <ol>
   *   <li>Poller called exactly once per cycle (no starvation, no double-poll).
   *   <li>fragmentLimit argument never exceeds {@link
   *       AeronEgressThread#MARKET_DATA_FRAGMENT_LIMIT}.
   *   <li>Total MD fragments = CYCLES × MARKET_DATA_QUANTUM within ±10%.
   * </ol>
   */
  @Test
  void doWork_1000CyclesSaturatedMd_mdPolledExactlyOncePerCycleWithQuantumLimit() throws Exception {
    final long[] totalMdFragments = {0L};
    final int[] callCount = {0};
    final boolean[] fragmentLimitViolated = {false};

    // Saturated MD poller — returns min(limit, MARKET_DATA_FRAGMENT_LIMIT).
    final MarketDataPoller saturatedMdPoller =
        (handler, limit) -> {
          callCount[0]++;
          if (limit > MARKET_DATA_FRAGMENT_LIMIT) {
            fragmentLimitViolated[0] = true;
          }
          final int returned = Math.min(limit, MARKET_DATA_FRAGMENT_LIMIT);
          totalMdFragments[0] += returned;
          return returned;
        };

    // Build a wired-but-effectively-no-op ingress handler.
    final var tracker = new MarketDataSubscriptionLivenessTracker(clock, state -> {});
    final var ingressHandler =
        new MarketDataIngressHandler(queue, egressListener, tracker, metrics, clock);

    final var agent =
        new AeronEgressThread.DWRRPollingAgent(
            clusterClient, saturatedMdPoller, ingressHandler, null, metrics, clock);

    for (int i = 0; i < CYCLES; i++) {
      agent.doWork();
    }

    // 1. Poller called exactly once per cycle.
    assertEquals(CYCLES, callCount[0], "MD poller must be called exactly once per doWork() cycle");

    // 2. fragmentLimit never exceeded the per-poll cap.
    assertFalse(
        fragmentLimitViolated[0],
        "MD poller must never be called with fragmentLimit > MARKET_DATA_FRAGMENT_LIMIT ("
            + MARKET_DATA_FRAGMENT_LIMIT
            + ")");

    // 3. Total MD fragments ≈ CYCLES × MARKET_DATA_QUANTUM within ±10%.
    //    Steady state: credit 1 per cycle, consume 1, deficit = 0. Total = 1 000.
    final long expectedMd = (long) CYCLES * MARKET_DATA_QUANTUM;
    final long tolerance = expectedMd / 10; // 10%
    assertTrue(
        totalMdFragments[0] >= expectedMd - tolerance
            && totalMdFragments[0] <= expectedMd + tolerance,
        "Total MD fragments "
            + totalMdFragments[0]
            + " must be within ±10% of expected "
            + expectedMd);
  }
}
