package com.trading.engine.websocket;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.trading.engine.testsupport.clock.ControllableNanoClock;
import org.agrona.concurrent.ManyToOneConcurrentArrayQueue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Starvation guard tests for {@link AeronEgressThread.DWRRPollingAgent}.
 *
 * <p>Drives cycles with various MD poller patterns and asserts that neither source is permanently
 * starved. The cluster client is DISCONNECTED (not fakeable for non-zero returns), so the cluster
 * starvation guard is verified indirectly through the deficit arithmetic:
 *
 * <ul>
 *   <li>Cluster deficit is always credited {@link AeronEgressThread#CLUSTER_QUANTUM} = 2 per cycle
 *       and then idle-reset to 0 when it returns 0 (DISCONNECTED state). This proves the cluster
 *       path runs every cycle — if it did not run, the deficit would not be reset.
 *   <li>MD poller is called exactly once per cycle across 100 cycles — no starvation, no
 *       double-poll — proving {@link AeronEgressThread#MARKET_DATA_QUANTUM} = 1 always credits at
 *       least one fragment limit.
 * </ul>
 *
 * <p>{@link MarketDataIngressHandler} is a concrete class (not a SAM), so tests wire a real handler
 * constructed via {@link #buildNoopIngressHandler()} — the handler is never invoked by the lambda
 * pollers because they capture the fragment count directly.
 */
final class AeronEgressThreadStarvationTest {

  private static final int CYCLES = 100;
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
            .aeronDirectoryName("/tmp/aeron-starvation-test")
            .ingressEndpoints("0=localhost:20112")
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
   * doWork_mdSaturated_mdPolledExactlyOncePerCycle: with cluster DISCONNECTED (0 fragments) and MD
   * saturated (returning the requested limit), the MD poller must be called exactly once per cycle
   * across 100 cycles — MARKET_DATA_QUANTUM=1 always credits at least one fragment limit so the MD
   * source is never starved.
   */
  @Test
  void doWork_mdSaturated_mdPolledExactlyOncePerCycle() throws Exception {
    final int[] mdCallCount = {0};

    final MarketDataPoller saturatedMd =
        (handler, limit) -> {
          mdCallCount[0]++;
          return limit; // saturated: return exactly the requested limit
        };

    final var agent =
        new AeronEgressThread.DWRRPollingAgent(
            clusterClient, saturatedMd, buildNoopIngressHandler(), null, metrics, clock);

    for (int i = 0; i < CYCLES; i++) {
      agent.doWork();
    }

    assertEquals(
        CYCLES,
        mdCallCount[0],
        "MD poller must be called exactly once per cycle — MARKET_DATA_QUANTUM=1 always credits "
            + "at least one fragment limit so the MD source can never be starved");
  }

  /**
   * doWork_clusterPolledEveryCycle_clusterDeficitNeverAccumulatesUnbounded: the cluster deficit is
   * credited {@link AeronEgressThread#CLUSTER_QUANTUM} = 2 every cycle and idle-reset to 0 when the
   * cluster is DISCONNECTED (returns 0). The deficit stays at 0 across 100 cycles — proving the
   * cluster path runs every cycle without accumulating unbounded deficit.
   */
  @Test
  void doWork_clusterPolledEveryCycle_clusterDeficitNeverAccumulatesUnbounded() throws Exception {
    final var agent =
        new AeronEgressThread.DWRRPollingAgent(
            clusterClient,
            (handler, limit) -> 0, // idle MD poller
            buildNoopIngressHandler(),
            null,
            metrics,
            clock);

    for (int i = 0; i < CYCLES; i++) {
      agent.doWork();
      // After each cycle: cluster credit = 2, clusterClient.doWork() returned 0 →
      // idle-reset → clusterDeficit() must be 0 every cycle.
      assertEquals(
          0L,
          agent.clusterDeficit(),
          "Cluster deficit must be 0 after idle-reset (DISCONNECTED → 0 fragments, cycle "
              + i
              + ")");
    }
  }

  /**
   * doWork_mdIdleEveryOtherCycle_mdPolledOnSaturatedCycles: alternating idle/saturated MD cycles.
   * On idle cycles the deficit resets to 0; on the next saturated cycle the MD source gets exactly
   * one credit (MARKET_DATA_QUANTUM = 1) and is polled. Verifies that prior-cycle idle does NOT
   * prevent the MD source from getting a turn on the saturated cycle.
   */
  @Test
  void doWork_mdIdleEveryOtherCycle_mdPolledOnSaturatedCycles() throws Exception {
    final int[] saturatedCallCount = {0};
    final int[] cycleNumber = {0};

    // Even cycles: idle (returns 0). Odd cycles: saturated (returns limit).
    final MarketDataPoller alternatingPoller =
        (handler, limit) -> {
          final int cycle = cycleNumber[0];
          if (cycle % 2 == 1) {
            saturatedCallCount[0]++;
            return limit;
          }
          return 0; // idle cycle
        };

    final var agent =
        new AeronEgressThread.DWRRPollingAgent(
            clusterClient, alternatingPoller, buildNoopIngressHandler(), null, metrics, clock);

    for (int i = 0; i < CYCLES; i++) {
      cycleNumber[0] = i;
      agent.doWork();
    }

    // Odd cycles: 1, 3, 5, ... → CYCLES/2 = 50 saturated calls.
    final int expectedSaturatedCycles = CYCLES / 2;
    assertEquals(
        expectedSaturatedCycles,
        saturatedCallCount[0],
        "MD poller must be called on every saturated cycle — idle-reset on prior cycle clears "
            + "accumulated deficit so each new cycle starts fresh with exactly one quantum credit");
  }

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  /**
   * Build a no-op {@link MarketDataIngressHandler}. This handler is never actually invoked because
   * the lambda pollers in these tests return scripted fragment counts without dispatching to the
   * underlying handler — it is constructed only to satisfy the {@link
   * AeronEgressThread.DWRRPollingAgent} non-null pre-condition when {@code marketDataPoller !=
   * null}.
   */
  private MarketDataIngressHandler buildNoopIngressHandler() {
    final var noopTracker = new MarketDataSubscriptionLivenessTracker(clock, state -> {});
    return new MarketDataIngressHandler(queue, egressListener, noopTracker, metrics, clock);
  }
}
