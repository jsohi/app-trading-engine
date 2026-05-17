package com.trading.engine.websocket;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.trading.engine.testsupport.clock.ControllableNanoClock;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.netty.channel.DefaultChannelId;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import io.netty.util.ResourceLeakDetector;
import java.util.Set;
import org.agrona.concurrent.ManyToOneConcurrentArrayQueue;
import org.agrona.concurrent.SystemNanoClock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Race-scenario tests for the stale-epoch drop guard in {@link WebSocketDrainHandler#drain()} —
 * specifically the {@code writeAckToTargetChannel} path that checks {@link
 * EgressEntry#sessionEpoch()} against {@link WebSocketSession#currentEpoch()}.
 *
 * <p><b>Scenario A — stale epoch drops all entries.</b> Enqueue 50 ack-back-channel entries stamped
 * with epoch 0, then call {@code session.resume()} (bumping epoch to 1), then drain. All 50 must be
 * dropped and the {@code egress.dropped.stale-epoch} counter must equal 50. No {@link
 * BinaryWebSocketFrame} must be written to the session channel.
 *
 * <p><b>Scenario B — EPOCH_ANY bypasses the check.</b> Enqueue 50 ack-back-channel entries stamped
 * with {@link EgressEntry#EPOCH_ANY}, then call resume(), then drain. All 50 must be delivered
 * (EPOCH_ANY is the "always-valid" sentinel used by broadcasts and legacy paths).
 *
 * <p><b>Wiring.</b> Uses the full-arity {@link WebSocketDrainHandler} constructor with a real
 * {@link CommandEntryPool} and ack queue so the {@code writeAckToTargetChannel} branch executes.
 * The main egress queue is left empty; all entries go via the ack back-channel.
 *
 * <p><b>Threading model.</b> Single-threaded — all setup and drain calls happen on the JUnit runner
 * thread, matching the Netty-event-loop ownership contract of the production drain handler.
 *
 * <p><b>Allocation.</b> {@link EmbeddedChannel} allocates on construction; cleaned up in {@link
 * #tearDown()} via {@link EmbeddedChannel#finishAndReleaseAll()}.
 */
final class WebSocketSessionResumeRaceTest {

  /** Power-of-2 queue capacity — large enough for 50 ack entries + headroom. */
  private static final int QUEUE_CAPACITY = 64;

  /** Max SBE message size used across entries. */
  private static final int MAX_MESSAGE_SIZE = 256;

  /** Number of ack entries used in each scenario. */
  private static final int ENTRY_COUNT = 50;

  private SimpleMeterRegistry registry;
  private WebSocketMetrics metrics;
  private ManyToOneConcurrentArrayQueue<EgressEntry> egressQueue;
  private ManyToOneConcurrentArrayQueue<EgressEntry> returnQueue;
  private ManyToOneConcurrentArrayQueue<EgressEntry> ackQueue;
  private WebSocketEgressListener egressListener;
  private CommandEntryPool commandEntryPool;
  private WebSocketSessionManager sessionManager;
  private WebSocketDrainHandler drainHandler;
  private EmbeddedChannel channel;
  private WebSocketSession session;

  @BeforeAll
  static void enableLeakDetection() {
    ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID);
  }

  @BeforeEach
  void setUp() {
    registry = new SimpleMeterRegistry();
    metrics = new WebSocketMetrics(registry);

    egressQueue = new ManyToOneConcurrentArrayQueue<>(QUEUE_CAPACITY);
    returnQueue = new ManyToOneConcurrentArrayQueue<>(QUEUE_CAPACITY);
    ackQueue = new ManyToOneConcurrentArrayQueue<>(QUEUE_CAPACITY);

    egressListener =
        new WebSocketEgressListener(
            egressQueue, returnQueue, metrics, QUEUE_CAPACITY, MAX_MESSAGE_SIZE);
    commandEntryPool = new CommandEntryPool(QUEUE_CAPACITY, MAX_MESSAGE_SIZE);

    final var config = WebSocketServerConfig.builder().build();
    final var clock = new ControllableNanoClock(1_000_000_000L);
    sessionManager = new WebSocketSessionManager(config, metrics, clock);

    drainHandler =
        new WebSocketDrainHandler(
            egressQueue,
            ackQueue,
            commandEntryPool,
            egressListener,
            sessionManager,
            metrics,
            SystemNanoClock.INSTANCE);

    // Register a session with a unique channel so it has a non-zero UUID that findById can locate.
    channel = new EmbeddedChannel(DefaultChannelId.newInstance());
    session = sessionManager.tryRegister(channel);

    // Init subscription filter with a broad subscription so reliable delivery is not blocked
    // by an empty filter (the ack path calls writeReliableToSession which checks isWritable but
    // not the filter directly; the filter check is on the writeReliableToAllChannels path).
    session.initSubscriptionFilter(100);
    session.subscriptionFilter().addSubscription(0L, 0x1F);
    session.entitledAccounts(Set.of("TEST"));
    session.initReliableStreamTracker(16, MAX_MESSAGE_SIZE, metrics);
    session.initSnapshotTokenBucket(clock.nanoTime());
  }

  @AfterEach
  void tearDown() {
    channel.finishAndReleaseAll();
  }

  // ── Helpers ──────────────────────────────────────────────────────────────

  /**
   * Build a minimal valid CommandAck SBE byte array (template 57 header + 1-byte state + 8-byte
   * serverNanos = 17 bytes total) repurposed here purely to carry routing fields — the actual bytes
   * are not decoded by {@code writeAckToTargetChannel}; we just need a non-zero length.
   *
   * <p>The ack entry format requires: sessionIdMsb/lsb to route to the target session, an epoch
   * stamp, and any non-empty payload so {@code writeReliableToSession} writes a frame.
   */
  private EgressEntry buildAckEntry(final long epochStamp) {
    // Borrow from the command pool so the pool has the right ownership for the drain handler's
    // commandEntryPool.release() call at the end of writeAckToTargetChannel.
    final var entry = commandEntryPool.tryAcquire();
    if (entry == null) {
      throw new IllegalStateException("Command pool exhausted during test setup");
    }
    // Write a minimal SBE header (8 bytes) + 1 data byte = 9 bytes so length > 0.
    // The exact content doesn't matter — drain only checks routing and epoch fields.
    final byte[] payload = buildMinimalSbeBytes();
    entry.fillAckBackChannel(payload, 0, payload.length, session.sessionId());
    entry.sessionEpoch(epochStamp);
    return entry;
  }

  /**
   * Return a 9-byte buffer that satisfies the SBE header layout so {@code
   * EgressEntry.extractTemplateId} can read it without ArrayIndexOutOfBoundsException. Layout:
   * [blockLength:2LE][templateId:2LE][schemaId:2LE][version:2LE][payload:1] = 9 bytes. We use
   * templateId=70 (CommandAck) which is reliable.
   */
  private static byte[] buildMinimalSbeBytes() {
    final byte[] b = new byte[9];
    // blockLength = 1 (LE): b[0]=1, b[1]=0
    b[0] = 1;
    // templateId = 70 (LE): b[2]=70, b[3]=0
    b[2] = 70;
    // remaining header bytes stay 0
    return b;
  }

  // ── Scenario A: stale-epoch entries are dropped ──────────────────────────

  /**
   * All 50 ack entries stamped with epoch 0 must be dropped when the session has been resumed
   * (epoch = 1). The {@code egress.dropped.stale-epoch} counter must equal exactly 50, and no
   * {@link BinaryWebSocketFrame} must be written to the session channel.
   */
  @Test
  void drain_staleEpochEntries_dropsAllFiftyAndIncrementsCounter() {
    // Stamp entries with epoch 0 and enqueue them.
    for (int i = 0; i < ENTRY_COUNT; i++) {
      final var entry = buildAckEntry(0L);
      assertTrue(ackQueue.offer(entry), "ack queue must accept entry " + i);
    }

    // Bump session epoch to 1 — entries are now stale.
    final long newEpoch = session.resume();
    assertEquals(1L, newEpoch, "resume() must return 1");

    // Drain — all 50 entries must be dropped.
    drainHandler.drain();

    // No BinaryWebSocketFrame must have been written.
    assertNull(
        channel.readOutbound(),
        "No frame must be written to the channel when all entries are stale-epoch");

    // The stale-epoch counter must equal exactly 50.
    final double dropped = registry.get("websocket.egress.dropped.stale-epoch").counter().count();
    assertEquals(50.0, dropped, "egress.dropped.stale-epoch counter must be exactly 50");
  }

  // ── Scenario B: EPOCH_ANY entries bypass the check ───────────────────────

  /**
   * All 50 ack entries stamped with {@link EgressEntry#EPOCH_ANY} must be delivered even after
   * {@code resume()} bumps the epoch. EPOCH_ANY is the "always-valid" sentinel; the drain handler
   * must NOT drop them. Exactly 50 {@link BinaryWebSocketFrame} objects must be written.
   */
  @Test
  void drain_epochAnyEntries_deliversAllFifty() {
    // Stamp entries with EPOCH_ANY and enqueue them.
    for (int i = 0; i < ENTRY_COUNT; i++) {
      final var entry = buildAckEntry(EgressEntry.EPOCH_ANY);
      assertTrue(ackQueue.offer(entry), "ack queue must accept EPOCH_ANY entry " + i);
    }

    // Bump session epoch — EPOCH_ANY entries must still pass.
    session.resume();

    // Drain.
    drainHandler.drain();

    // Count delivered frames.
    int frames = 0;
    BinaryWebSocketFrame frame;
    while ((frame = channel.readOutbound()) != null) {
      frame.release();
      frames++;
    }
    assertEquals(
        ENTRY_COUNT,
        frames,
        "All 50 EPOCH_ANY entries must be delivered regardless of session epoch");

    // The stale-epoch counter must remain zero.
    final double dropped = registry.get("websocket.egress.dropped.stale-epoch").counter().count();
    assertEquals(0.0, dropped, "No stale-epoch drops must occur for EPOCH_ANY entries");
  }
}
