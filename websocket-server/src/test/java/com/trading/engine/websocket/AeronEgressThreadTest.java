package com.trading.engine.websocket;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.trading.engine.testsupport.clock.ControllableNanoClock;
import io.aeron.cluster.client.EgressListener;
import io.aeron.cluster.codecs.EventCode;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.ManyToOneConcurrentArrayQueue;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link AeronEgressThread} — verifies constructor null-guards, start lifecycle, and
 * double-start rejection using the canonical 12-arg constructor.
 *
 * <p>The {@link AeronEgressThread#start()} method launches a real thread that invokes the cluster
 * client's {@code onStart()} which calls {@code connect()}. Without a running Aeron Media Driver,
 * the connection fails and the client enters RECONNECTING state. This is expected behavior — we
 * verify that start() itself does not throw.
 */
final class AeronEgressThreadTest {

  /** No-op egress listener for builder satisfaction. */
  private static final EgressListener NOOP_EGRESS_LISTENER =
      new EgressListener() {
        @Override
        public void onMessage(
            final long clusterSessionId,
            final long timestamp,
            final DirectBuffer buffer,
            final int offset,
            final int length,
            final Header header) {}

        @Override
        public void onSessionEvent(
            final long correlationId,
            final long clusterSessionId,
            final long leadershipTermId,
            final int leaderMemberId,
            final EventCode code,
            final String detail) {}

        @Override
        public void onNewLeader(
            final long clusterSessionId,
            final long leadershipTermId,
            final int leaderMemberId,
            final String ingressEndpoints) {}
      };

  /** Queue capacity — power of 2 for ManyToOneConcurrentArrayQueue. */
  private static final int QUEUE_CAPACITY = 4;

  /** Max SBE message size per pool entry. */
  private static final int MAX_MESSAGE_SIZE = 256;

  private ControllableNanoClock clock;
  private WebSocketClusterClient clusterClient;
  private ManyToOneConcurrentArrayQueue<EgressEntry> queue;
  private ManyToOneConcurrentArrayQueue<EgressEntry> commandQueue;
  private ManyToOneConcurrentArrayQueue<EgressEntry> ackQueue;
  private CommandEntryPool commandEntryPool;
  private WebSocketMetrics metrics;
  private WebSocketEgressListener egressListener;
  private MarketDataSubscriptionLivenessTracker livenessTracker;
  private MarketDataIngressHandler marketDataIngressHandler;

  /** Track the thread for cleanup. */
  private AeronEgressThread egressThread;

  @BeforeEach
  void setUp() {
    clock = new ControllableNanoClock(1_000_000_000L);
    clusterClient =
        WebSocketClusterClient.builder()
            .aeronDirectoryName("/tmp/aeron-test-egress")
            .ingressEndpoints("0=localhost:20000")
            .egressListener(NOOP_EGRESS_LISTENER)
            .errorHandler(Throwable::printStackTrace)
            .nanoClock(clock)
            .build();
    queue = new ManyToOneConcurrentArrayQueue<>(QUEUE_CAPACITY);
    commandQueue = new ManyToOneConcurrentArrayQueue<>(QUEUE_CAPACITY);
    ackQueue = new ManyToOneConcurrentArrayQueue<>(QUEUE_CAPACITY);
    commandEntryPool = new CommandEntryPool(QUEUE_CAPACITY, MAX_MESSAGE_SIZE);
    metrics = WebSocketMetrics.createWithDefaults();
    final var returnQueue = new ManyToOneConcurrentArrayQueue<EgressEntry>(QUEUE_CAPACITY);
    egressListener =
        new WebSocketEgressListener(queue, returnQueue, metrics, QUEUE_CAPACITY, MAX_MESSAGE_SIZE);
    livenessTracker = new MarketDataSubscriptionLivenessTracker(clock, state -> {});
    marketDataIngressHandler =
        new MarketDataIngressHandler(queue, egressListener, livenessTracker, metrics, clock);
  }

  @AfterEach
  void tearDown() {
    if (egressThread != null) {
      egressThread.close();
    }
    if (clusterClient != null) {
      clusterClient.close();
    }
  }

  /**
   * Build a complete canonical {@link AeronEgressThread} using all pre-wired fields. Callers may
   * substitute {@code null} for a specific argument to exercise null-guard paths.
   */
  private AeronEgressThread build(
      final WebSocketClusterClient client,
      final ManyToOneConcurrentArrayQueue<EgressEntry> mainQueue,
      final WebSocketMetrics metricsArg) {
    final MarketDataPoller noopPoller = (handler, limit) -> 0;
    return new AeronEgressThread(
        client,
        mainQueue,
        commandQueue,
        ackQueue,
        commandEntryPool,
        metricsArg,
        QUEUE_CAPACITY,
        noopPoller,
        marketDataIngressHandler,
        livenessTracker,
        egressListener,
        clock);
  }

  @Test
  void constructor_nullClusterClient_throwsNullPointerException() {
    assertThrows(
        NullPointerException.class,
        () -> build(null, queue, metrics),
        "Constructor with null clusterClient must throw NullPointerException");
  }

  @Test
  void constructor_nullQueue_throwsNullPointerException() {
    assertThrows(
        NullPointerException.class,
        () -> build(clusterClient, null, metrics),
        "Constructor with null queue must throw NullPointerException");
  }

  @Test
  void constructor_nullMetrics_throwsNullPointerException() {
    assertThrows(
        NullPointerException.class,
        () -> build(clusterClient, queue, null),
        "Constructor with null metrics must throw NullPointerException");
  }

  @Test
  void isStarted_beforeStart_returnsFalse() {
    egressThread = build(clusterClient, queue, metrics);

    assertFalse(egressThread.isStarted(), "isStarted() must return false before start() is called");
  }

  @Test
  void start_calledOnce_startsSuccessfully() {
    egressThread = build(clusterClient, queue, metrics);

    // start() launches a thread that calls connect(). Without a running cluster, the
    // connection will fail and the client will enter RECONNECTING state. That's fine.
    egressThread.start();

    assertTrue(egressThread.isStarted(), "isStarted() must return true after start()");
  }

  @Test
  void start_calledTwice_throwsIllegalStateException() {
    egressThread = build(clusterClient, queue, metrics);
    egressThread.start();

    assertThrows(
        IllegalStateException.class,
        () -> egressThread.start(),
        "Calling start() twice must throw IllegalStateException");
  }
}
