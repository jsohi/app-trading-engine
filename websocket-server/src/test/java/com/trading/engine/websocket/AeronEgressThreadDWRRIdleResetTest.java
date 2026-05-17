package com.trading.engine.websocket;

import static com.trading.engine.websocket.AeronEgressThread.MARKET_DATA_FRAGMENT_LIMIT;
import static com.trading.engine.websocket.AeronEgressThread.MARKET_DATA_QUANTUM;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.trading.engine.testsupport.clock.ControllableNanoClock;
import java.util.ArrayList;
import java.util.List;
import org.agrona.concurrent.ManyToOneConcurrentArrayQueue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Idle-reset behaviour tests for {@link AeronEgressThread.DWRRPollingAgent}.
 *
 * <p>RFC 8290 §4.3 specifies the anti-burst idle-reset rule: when a source returns zero fragments,
 * its deficit is reset to zero so that accumulated unused credit cannot be claimed as a burst on
 * the next saturated cycle. The DWRR polling agent implements this by setting {@code
 * marketDataDeficit = 0} when {@code marketDataFragments == 0}.
 *
 * <p>This test drives 100 idle cycles (MD poller returns 0) followed by one saturated cycle (MD
 * poller can return up to {@link AeronEgressThread#MARKET_DATA_FRAGMENT_LIMIT} = 32 if asked). The
 * key assertion: on the saturated cycle the poller is called with {@code fragmentLimit = 1} — NOT
 * 101 (100 idle cycles × quantum = 100 accumulated credit without idle-reset) and NOT 32 (the cap).
 * This proves that idle-reset zeros the deficit after each idle cycle, leaving exactly one quantum
 * credit on the next active cycle.
 *
 * <p>{@link MarketDataIngressHandler} is a concrete class (not a SAM), so tests wire a real handler
 * constructed via {@link #buildNoopIngressHandler()} — the handler is never invoked because lambda
 * pollers capture the fragment count without dispatching to it.
 */
final class AeronEgressThreadDWRRIdleResetTest {

  private static final int IDLE_CYCLES = 100;
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
            .aeronDirectoryName("/tmp/aeron-idlereset-test")
            .ingressEndpoints("0=localhost:20113")
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
   * doWork_100IdleCyclesThenSaturated_pollerCalledWithFragmentLimitOne: after 100 idle cycles the
   * MD deficit is zero (idle-reset every cycle). On the saturated cycle the credit is exactly
   * MARKET_DATA_QUANTUM = 1, so the poller is called with {@code fragmentLimit = 1}.
   *
   * <p>If the idle-reset had NOT fired for the 100 idle cycles, the deficit would have accumulated
   * to 100 × MARKET_DATA_QUANTUM = 100, and the limit on the saturated cycle would be {@code
   * min(100, MARKET_DATA_FRAGMENT_LIMIT) = 32} — NOT 1. Getting exactly 1 proves that idle-reset
   * zeroed the deficit every idle cycle.
   */
  @Test
  void doWork_100IdleCyclesThenSaturated_pollerCalledWithFragmentLimitOne() throws Exception {
    final List<Integer> limitOnSaturatedCycle = new ArrayList<>();
    final int[] cycleNumber = {0};

    // Poller: idle for cycles 0..IDLE_CYCLES-1, saturated on cycle IDLE_CYCLES.
    final MarketDataPoller poller =
        (handler, limit) -> {
          final int cycle = cycleNumber[0];
          if (cycle < IDLE_CYCLES) {
            return 0; // idle — triggers idle-reset
          }
          limitOnSaturatedCycle.add(limit);
          return Math.min(limit, MARKET_DATA_FRAGMENT_LIMIT);
        };

    final var agent =
        new AeronEgressThread.DWRRPollingAgent(
            clusterClient, poller, buildNoopIngressHandler(), null, metrics, clock);

    // Drive 100 idle cycles and verify idle-reset fires every time.
    for (int i = 0; i < IDLE_CYCLES; i++) {
      cycleNumber[0] = i;
      agent.doWork();
      assertEquals(
          0L, agent.marketDataDeficit(), "MD deficit must be 0 after idle-reset (cycle " + i + ")");
    }

    // Verify deficit is exactly 0 entering the saturated cycle.
    assertEquals(
        0L,
        agent.marketDataDeficit(),
        "MD deficit must be zero before the saturated cycle — confirms no carry-forward credit");

    // Drive the saturated cycle (cycle 100).
    cycleNumber[0] = IDLE_CYCLES;
    agent.doWork();

    // Poller called exactly once on the saturated cycle.
    assertEquals(
        1,
        limitOnSaturatedCycle.size(),
        "Poller must be called exactly once on the saturated cycle");

    // The key assertion: fragmentLimit must be exactly MARKET_DATA_QUANTUM = 1, NOT 32 or 100.
    assertEquals(
        MARKET_DATA_QUANTUM,
        (int) limitOnSaturatedCycle.get(0),
        "Poller must be called with fragmentLimit = MARKET_DATA_QUANTUM (=1) on the saturated "
            + "cycle — idle-reset must have zeroed accumulated credit, NOT carried it forward as "
            + "burst (without idle-reset the limit would be min(100, 32) = 32)");
  }

  /**
   * doWork_singleIdleThenSaturated_noAccumulatedCredit: simpler 1-idle then 1-saturated sequence
   * verifying that even a single idle cycle prevents any carry-forward deficit.
   */
  @Test
  void doWork_singleIdleThenSaturated_noAccumulatedCredit() throws Exception {
    final List<Integer> capturedLimits = new ArrayList<>();
    final int[] cycle = {0};

    final MarketDataPoller poller =
        (handler, limit) -> {
          if (cycle[0] == 0) {
            return 0; // idle — triggers idle-reset
          }
          capturedLimits.add(limit);
          return limit;
        };

    final var agent =
        new AeronEgressThread.DWRRPollingAgent(
            clusterClient, poller, buildNoopIngressHandler(), null, metrics, clock);

    cycle[0] = 0;
    agent.doWork(); // idle cycle

    assertEquals(
        0L, agent.marketDataDeficit(), "Deficit must be zero after single idle cycle (idle-reset)");

    cycle[0] = 1;
    agent.doWork(); // saturated cycle

    assertEquals(1, capturedLimits.size(), "Poller must be called once on the saturated cycle");
    assertEquals(
        MARKET_DATA_QUANTUM,
        (int) capturedLimits.get(0),
        "Limit must be exactly MARKET_DATA_QUANTUM=1 — single idle cycle's credit does not carry forward");
  }

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  /**
   * Build a no-op {@link MarketDataIngressHandler}. This handler is never actually invoked because
   * the lambda pollers in these tests return scripted fragment counts without dispatching to it —
   * constructed only to satisfy the {@link AeronEgressThread.DWRRPollingAgent} non-null
   * pre-condition when {@code marketDataPoller != null}.
   */
  private MarketDataIngressHandler buildNoopIngressHandler() {
    final var noopTracker = new MarketDataSubscriptionLivenessTracker(clock, state -> {});
    return new MarketDataIngressHandler(queue, egressListener, noopTracker, metrics, clock);
  }
}
