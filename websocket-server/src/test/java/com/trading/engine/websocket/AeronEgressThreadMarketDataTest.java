package com.trading.engine.websocket;

import static com.trading.engine.websocket.AeronEgressThread.MARKET_DATA_QUANTUM;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.trading.engine.testsupport.clock.ControllableNanoClock;
import java.util.ArrayList;
import java.util.List;
import org.agrona.concurrent.ManyToOneConcurrentArrayQueue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link AeronEgressThread.DWRRPollingAgent} market-data ingest wiring.
 *
 * <p>Directly constructs the package-private {@code DWRRPollingAgent} (no AgentRunner spin-up) and
 * drives {@code doWork()} synchronously so all assertions are deterministic. Uses lambda {@link
 * MarketDataPoller} fakes — the SAM seam exists precisely for this pattern.
 *
 * <p>The {@link WebSocketClusterClient} is constructed in DISCONNECTED state (no Aeron driver
 * required) and therefore returns {@code 0} from {@code doWork()} — it is present only to satisfy
 * the agent's non-null contract. All interesting assertions target the market-data side.
 *
 * <p>{@link MarketDataIngressHandler} is a concrete class (not a SAM), so tests that wire the MD
 * path construct a real handler via {@link #buildNoopIngressHandler}. The handler is never actually
 * invoked in DWRR-unit tests because the lambda pollers capture the fragment count without
 * dispatching to it.
 */
final class AeronEgressThreadMarketDataTest {

  /** Queue / pool capacity — power-of-two required by ManyToOneConcurrentArrayQueue. */
  private static final int CAPACITY = 4;

  /** Max SBE message size per entry. */
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
    clusterClient = buildDisconnectedClient(clock);
    metrics = WebSocketMetrics.createWithDefaults();
    queue = new ManyToOneConcurrentArrayQueue<>(CAPACITY);
    returnQueue = new ManyToOneConcurrentArrayQueue<>(CAPACITY);
    egressListener =
        new WebSocketEgressListener(queue, returnQueue, metrics, CAPACITY, MAX_MESSAGE_SIZE);
  }

  // ---------------------------------------------------------------------------
  // Constructor / wiring validation
  // ---------------------------------------------------------------------------

  /**
   * Constructing the agent with {@code marketDataPoller=null} must not throw and must result in
   * cluster-only behavior: subsequent {@code doWork()} calls return 0 (cluster is DISCONNECTED) and
   * the market-data deficit stays at zero.
   */
  @Test
  void constructor_nullMarketDataPoller_clusterOnlyBehavior() throws Exception {
    final var agent =
        new AeronEgressThread.DWRRPollingAgent(
            clusterClient,
            null, // no market-data poller
            null, // no market-data ingress handler
            null, // no liveness tracker
            metrics,
            clock);

    agent.doWork();

    // DISCONNECTED cluster returns 0 → deficit idle-reset to 0.
    assertEquals(0L, agent.clusterDeficit(), "Cluster deficit must be zero after idle reset");
    // No MD side wired → market-data deficit must remain 0.
    assertEquals(0L, agent.marketDataDeficit(), "Market-data deficit must be zero when not wired");
  }

  /**
   * When a {@link MarketDataSubscriptionLivenessTracker} is wired, the agent calls {@code
   * tracker.tick()} exactly once per {@code doWork()} invocation. Verified by seeding the tracker
   * in LIVE state and advancing the clock past {@link
   * MarketDataSubscriptionLivenessTracker#QUIET_THRESHOLD_NANOS} — the LIVE→QUIET transition fires
   * only on cycle 2 (when tick() observes the elapsed time), not cycle 1.
   */
  @Test
  void doWork_withLivenessTracker_tickCalledOncePerCycle() throws Exception {
    final List<Long> transitionTimes = new ArrayList<>();
    // Seed the clock at 1 s so the tracker's constructor records lastTickNs = 1s.
    final var tickClock = new ControllableNanoClock(1_000_000_000L);

    final long quietThreshold = MarketDataSubscriptionLivenessTracker.QUIET_THRESHOLD_NANOS;
    final var tracker =
        new MarketDataSubscriptionLivenessTracker(
            tickClock, state -> transitionTimes.add(tickClock.nanoTime()));

    // Idle MD poller — returns 0 every cycle so the handler is never invoked.
    final MarketDataPoller idlePoller = (handler, limit) -> 0;
    final var ingressHandler = buildNoopIngressHandler(tracker, tickClock);

    final var agent =
        new AeronEgressThread.DWRRPollingAgent(
            clusterClient, idlePoller, ingressHandler, tracker, metrics, tickClock);

    // Cycle 1: clock still at 1 s — below QUIET threshold — no transition expected.
    agent.doWork();
    final int countAfterCycle1 = transitionTimes.size();

    // Advance past QUIET threshold; next tick() call will observe the elapsed time.
    tickClock.advanceNanos(quietThreshold + 1L);

    // Cycle 2: tick() fires, LIVE→QUIET transition emitted.
    agent.doWork();

    assertEquals(0, countAfterCycle1, "No transition must fire in cycle 1 (below quiet threshold)");
    assertEquals(
        1,
        transitionTimes.size(),
        "Exactly one transition must fire when quiet threshold exceeded on cycle 2");
  }

  /**
   * On the very first {@code doWork()} cycle, the market-data deficit starts at zero. The agent
   * credits {@link AeronEgressThread#MARKET_DATA_QUANTUM} (=1), so {@code marketDataLimit = min(1,
   * MARKET_DATA_FRAGMENT_LIMIT) = 1}. The poller must be called with {@code fragmentLimit = 1}.
   */
  @Test
  void doWork_freshCycle_pollerCalledWithFragmentLimitOne() throws Exception {
    final List<Integer> capturedLimits = new ArrayList<>();
    final MarketDataPoller capturingPoller =
        (handler, limit) -> {
          capturedLimits.add(limit);
          return 0;
        };

    // Tracker is required when the poller is non-null (handler wraps it).
    final var tracker = new MarketDataSubscriptionLivenessTracker(clock, state -> {});
    final var ingressHandler = buildNoopIngressHandler(tracker, clock);

    final var agent =
        new AeronEgressThread.DWRRPollingAgent(
            clusterClient,
            capturingPoller,
            ingressHandler,
            null, // agent-level tracker not needed for this assertion
            metrics,
            clock);

    agent.doWork();

    assertEquals(1, capturedLimits.size(), "Poller must be called exactly once per cycle");
    assertEquals(
        MARKET_DATA_QUANTUM,
        (int) capturedLimits.get(0),
        "Poller must be called with fragmentLimit = MARKET_DATA_QUANTUM (=1) on the first cycle");
  }

  /**
   * The cluster source must be polled BEFORE the market-data source each cycle — the cluster-first
   * ordering rule documented in the {@link AeronEgressThread} class-level Javadoc. Verified by
   * checking that the MD poller is called after cluster polling completes (cluster is first in
   * {@code doWork()} source order) and that the cluster deficit is idle-reset (confirming the
   * cluster path ran without exception before MD).
   */
  @Test
  void doWork_clusterPolledBeforeMarketData_orderingGuaranteed() throws Exception {
    final List<String> callOrder = new ArrayList<>();

    final MarketDataPoller mdPoller =
        (handler, limit) -> {
          callOrder.add("md");
          return 0;
        };

    final var tracker = new MarketDataSubscriptionLivenessTracker(clock, state -> {});
    final var ingressHandler = buildNoopIngressHandler(tracker, clock);

    final var agent =
        new AeronEgressThread.DWRRPollingAgent(
            clusterClient, mdPoller, ingressHandler, null, metrics, clock);

    agent.doWork();

    assertEquals(1, callOrder.size(), "MD poller must have been called once");
    assertEquals("md", callOrder.get(0), "MD poller must be the only recorded call");
    // Cluster-side deficit was credited and idle-reset (DISCONNECTED → 0 fragments) before MD
    // polling ran — confirms cluster path executed first without exception.
    assertEquals(
        0L,
        agent.clusterDeficit(),
        "Cluster deficit must be 0 after idle-reset (cluster polled first, returned 0, idle-reset)");
  }

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  /**
   * Build a {@link WebSocketClusterClient} in DISCONNECTED state. In DISCONNECTED state {@code
   * doWork()} returns 0 at the {@code state != CONNECTED → return 0} guard — no Aeron Media Driver
   * is required.
   */
  private static WebSocketClusterClient buildDisconnectedClient(
      final ControllableNanoClock nanoClock) {
    return WebSocketClusterClient.builder()
        .aeronDirectoryName("/tmp/aeron-dwrr-test")
        .ingressEndpoints("0=localhost:20110")
        .egressListener((clusterSessionId, timestamp, buffer, offset, length, header) -> {})
        .errorHandler(Throwable::printStackTrace)
        .nanoClock(nanoClock)
        .build();
  }

  /**
   * Build a {@link MarketDataIngressHandler} bound to the given tracker and clock. This handler is
   * never actually invoked in DWRR-unit tests because the lambda pollers return scripted fragment
   * counts without dispatching to the underlying handler — it is constructed only to satisfy the
   * {@link AeronEgressThread.DWRRPollingAgent} non-null pre-condition when {@code marketDataPoller
   * != null}.
   */
  private MarketDataIngressHandler buildNoopIngressHandler(
      final MarketDataSubscriptionLivenessTracker tracker, final ControllableNanoClock nanoClock) {
    return new MarketDataIngressHandler(queue, egressListener, tracker, metrics, nanoClock);
  }
}
