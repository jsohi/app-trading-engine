package com.trading.engine.websocket;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.trading.engine.messages.sbe.CommandAckStatus;
import com.trading.engine.testsupport.clock.ControllableNanoClock;
import com.trading.engine.testsupport.sbe.SbeTestEncoder;
import io.netty.channel.DefaultChannelId;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import io.netty.util.ResourceLeakDetector;
import java.util.ArrayList;
import java.util.List;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.ManyToOneConcurrentArrayQueue;
import org.agrona.concurrent.SystemNanoClock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link WebSocketDrainHandler} — verifies queue draining, fan-out to active sessions,
 * reliable vs. best-effort header encoding, entry pool recycling, and null-guard validation.
 *
 * <p>Uses {@link EmbeddedChannel} for Netty channels and small queue/pool capacities (4) for
 * controllable test scenarios. {@link ResourceLeakDetector} is set to PARANOID to catch any
 * unreleased ByteBuf allocations.
 */
final class WebSocketDrainHandlerTest {

  /** Queue/pool capacity — power of 2 for ManyToOneConcurrentArrayQueue. */
  private static final int CAPACITY = 4;

  /** Maximum SBE message size per entry. */
  private static final int MAX_MESSAGE_SIZE = 1024;

  private ManyToOneConcurrentArrayQueue<EgressEntry> queue;
  private ManyToOneConcurrentArrayQueue<EgressEntry> returnQueue;
  private WebSocketMetrics metrics;
  private WebSocketEgressListener egressListener;
  private WebSocketSessionManager sessionManager;
  private WebSocketDrainHandler drainHandler;
  private MutableDirectBuffer sbeBuffer;

  /** Channels opened during the test — closed in {@link #tearDown()}. */
  private final List<EmbeddedChannel> openChannels = new ArrayList<>();

  @BeforeAll
  static void enableLeakDetection() {
    ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID);
  }

  @BeforeEach
  void setUp() {
    queue = new ManyToOneConcurrentArrayQueue<>(CAPACITY);
    returnQueue = new ManyToOneConcurrentArrayQueue<>(CAPACITY);
    metrics = WebSocketMetrics.createWithDefaults();
    egressListener =
        new WebSocketEgressListener(queue, returnQueue, metrics, CAPACITY, MAX_MESSAGE_SIZE);
    sbeBuffer = new ExpandableArrayBuffer(MAX_MESSAGE_SIZE);

    final var config = WebSocketServerConfig.builder().build();
    final var clock = new ControllableNanoClock(1_000_000_000L);
    sessionManager = new WebSocketSessionManager(config, metrics, clock);
    drainHandler =
        new WebSocketDrainHandler(
            queue, egressListener, sessionManager, metrics, SystemNanoClock.INSTANCE);
  }

  @AfterEach
  void tearDown() {
    for (final var ch : openChannels) {
      ch.finishAndReleaseAll();
    }
    openChannels.clear();
  }

  /**
   * Create an {@link EmbeddedChannel} with a unique {@link DefaultChannelId} and register it as a
   * session. Unique IDs prevent key collisions in the session manager's {@code Long2ObjectHashMap}
   * — default EmbeddedChannel instances share the same ID hash (0).
   *
   * @return the created EmbeddedChannel
   */
  private EmbeddedChannel createSessionChannel() {
    final var ch = new EmbeddedChannel(DefaultChannelId.newInstance());
    openChannels.add(ch);
    final var session = sessionManager.tryRegister(ch);
    // Init subscription filter with a broad match so drain handler doesn't skip this session.
    // SubscriptionFilter.matches() returns false if no subscriptions — add a wildcard-like sub
    // that matches the CommandAck templateId used in most tests (bit 0 = orders).
    session.initSubscriptionFilter(100);
    // Subscribe with all event types for both the "any" sentinel and "EURUSD" (used in tests).
    // packedSymbol=0L covers no-symbol templates via globalEventBitMask.
    // "EURUSD" covers PriceResponse (template 51) tests.
    session.subscriptionFilter().addSubscription(0L, 0x1F);
    session
        .subscriptionFilter()
        .addSubscription(com.trading.engine.projections.SymbolPacker.pack("EURUSD  "), 0x1F);
    session.entitledAccounts(java.util.Set.of("TEST-ACCT"));
    return ch;
  }

  /**
   * Manually create and enqueue an EgressEntry with the given templateId and length. Uses the raw
   * SBE buffer content.
   *
   * @param templateId the SBE templateId to set on the entry
   * @param length the message length
   */
  private void enqueueEntry(final int templateId, final int length) {
    final var entry = new EgressEntry(MAX_MESSAGE_SIZE);
    // Copy the SBE bytes from the shared buffer
    sbeBuffer.getBytes(0, entry.bytes(), 0, length);
    entry.setMetadata(length, templateId);
    final boolean offered = queue.offer(entry);
    assertTrue(offered, "Must be able to offer entry to queue");
  }

  @Test
  void drain_emptyQueue_doesNothing() {
    final var ch = createSessionChannel();

    drainHandler.drain();

    final var msg = ch.readOutbound();
    assertEquals(null, msg, "No messages must be written when the queue is empty");
  }

  @Test
  void drain_oneEntry_writesToAllActiveSessions() {
    final var ch1 = createSessionChannel();
    final var ch2 = createSessionChannel();

    // Encode a CommandAck (templateId=70, reliable) and enqueue
    final int length = SbeTestEncoder.encodeCommandAck(sbeBuffer, 0, 1L, CommandAckStatus.Accepted);
    enqueueEntry(70, length);

    drainHandler.drain();

    // Both channels should have received a BinaryWebSocketFrame
    final var frame1 = (BinaryWebSocketFrame) ch1.readOutbound();
    final var frame2 = (BinaryWebSocketFrame) ch2.readOutbound();

    assertTrue(frame1 != null, "Channel 1 must receive a BinaryWebSocketFrame");
    assertTrue(frame2 != null, "Channel 2 must receive a BinaryWebSocketFrame");

    // Verify frame content is non-empty
    assertTrue(frame1.content().readableBytes() > 0, "Frame 1 must have non-empty content");
    assertTrue(frame2.content().readableBytes() > 0, "Frame 2 must have non-empty content");

    // Release frames
    frame1.release();
    frame2.release();
  }

  @Test
  void drain_reliableMessage_usesReliableHeader() {
    final var ch = createSessionChannel();

    // CommandAck (templateId=70) is reliable
    final int length = SbeTestEncoder.encodeCommandAck(sbeBuffer, 0, 1L, CommandAckStatus.Accepted);
    enqueueEntry(70, length);

    drainHandler.drain();

    final var frame = (BinaryWebSocketFrame) ch.readOutbound();
    assertTrue(frame != null, "Channel must receive a frame");

    // Reliable header is 17 bytes + SBE payload
    final int expectedSize = FrameParser.RELIABLE_HEADER_SIZE + length;
    assertEquals(
        expectedSize,
        frame.content().readableBytes(),
        "Reliable message must have 17-byte header + SBE payload");

    frame.release();
  }

  @Test
  void drain_bestEffortMessage_usesBestEffortHeader() {
    final var ch = createSessionChannel();

    // PriceResponse (templateId=51) is best-effort
    final int length =
        SbeTestEncoder.encodePriceResponse(
            sbeBuffer, 0, "RFQ-001", "EURUSD  ", true, 110_000_000L, 111_000_000L, 1_000_000_000L);
    enqueueEntry(51, length);

    drainHandler.drain();

    final var frame = (BinaryWebSocketFrame) ch.readOutbound();
    assertTrue(frame != null, "Channel must receive a frame");

    // Best-effort header is 13 bytes + SBE payload
    final int expectedSize = FrameParser.BEST_EFFORT_HEADER_SIZE + length;
    assertEquals(
        expectedSize,
        frame.content().readableBytes(),
        "Best-effort message must have 13-byte header + SBE payload");

    frame.release();
  }

  @Test
  void drain_returnsEntryToPool_afterProcessing() {
    createSessionChannel();

    // Encode and enqueue a message via the listener's onMessage (uses the listener's pool)
    final int length = SbeTestEncoder.encodeCommandAck(sbeBuffer, 0, 1L, CommandAckStatus.Accepted);
    egressListener.onMessage(1L, 1_000_000_000L, sbeBuffer, 0, length, null);

    final int poolBefore = egressListener.poolAvailable();

    // Drain — this calls returnToPool on the listener for each processed entry
    drainHandler.drain();

    // The return queue now has one entry; it will be reclaimed on the next onMessage.
    // Trigger reclamation by calling onMessage again.
    egressListener.onMessage(1L, 2_000_000_000L, sbeBuffer, 0, length, null);

    final int poolAfter = egressListener.poolAvailable();
    assertEquals(
        poolBefore,
        poolAfter,
        "Pool count must be restored after drain returns the entry and a new message consumes one");
  }

  @Test
  void constructor_nullQueue_throwsNullPointerException() {
    assertThrows(
        NullPointerException.class,
        () ->
            new WebSocketDrainHandler(
                null, egressListener, sessionManager, metrics, SystemNanoClock.INSTANCE),
        "Constructor with null queue must throw NullPointerException");
  }
}
