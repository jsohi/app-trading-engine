package com.trading.engine.websocket;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.trading.engine.messages.sbe.CommandAckEncoder;
import com.trading.engine.messages.sbe.CommandAckStatus;
import com.trading.engine.messages.sbe.MessageHeaderEncoder;
import com.trading.engine.projections.SymbolPacker;
import com.trading.engine.testsupport.clock.ControllableNanoClock;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.netty.buffer.Unpooled;
import io.netty.channel.DefaultChannelId;
import io.netty.channel.WriteBufferWaterMark;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import io.netty.util.ReferenceCounted;
import java.util.Set;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.ManyToOneConcurrentArrayQueue;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Regression guard for the Gemini cloud-review R2 G-3 fix. Asserts the corrected contract on {@link
 * WebSocketDrainHandler}'s reliable path: a reliable frame MUST be written to the channel even when
 * {@code ch.isWritable() == false}, so the reliable-stream protocol's seqNo invariant holds (no
 * client-undetectable gap). The {@link SlowConsumerHandler} ladder is the correct mechanism for
 * handling sustained back-pressure on the reliable path — silent drop is forbidden.
 *
 * <p>Best-effort path retains its pre-existing {@code !isWritable()} skip; that skip now increments
 * the {@code websocket.egress.dropped.channel-not-writable} counter. This test verifies BOTH halves
 * of the contract:
 *
 * <ul>
 *   <li>Reliable frame written despite {@code !isWritable()}; reliable seq counter advances;
 *       counter NOT incremented for the reliable drop scenario.
 *   <li>Best-effort frame dropped on {@code !isWritable()}; counter IS incremented; no frame
 *       reaches the channel.
 * </ul>
 *
 * <p><b>Making the channel non-writable.</b> Set {@code WriteBufferWaterMark(0, 0)} so any pending
 * byte trips the high water mark, then prime the channel with one unflushed write.
 *
 * <p><b>Threading.</b> Single-threaded — all I/O on the test thread via {@link EmbeddedChannel}.
 *
 * @see WebSocketDrainHandler#writeReliableToSession
 * @see WebSocketDrainHandler#writeBestEffortToAllChannels
 */
final class WebSocketDrainHandlerReliableNoDropTest {

  private static final int CAPACITY = 8;
  private static final int MAX_MESSAGE_SIZE = 512;
  private static final int TRACKER_CAPACITY = 4;
  private static final String METRIC_DROP = "websocket.egress.dropped.channel-not-writable";

  private SimpleMeterRegistry registry;
  private WebSocketMetrics metrics;
  private ManyToOneConcurrentArrayQueue<EgressEntry> queue;
  private ManyToOneConcurrentArrayQueue<EgressEntry> returnQueue;
  private WebSocketEgressListener egressListener;
  private WebSocketSessionManager sessionManager;
  private WebSocketDrainHandler drainHandler;
  private ControllableNanoClock clock;
  private EmbeddedChannel channel;
  private WebSocketSession session;
  private MutableDirectBuffer sbeBuf;

  @BeforeEach
  void setUp() {
    registry = new SimpleMeterRegistry();
    metrics = new WebSocketMetrics(registry);
    clock = new ControllableNanoClock(1_000_000_000L);
    queue = new ManyToOneConcurrentArrayQueue<>(CAPACITY);
    returnQueue = new ManyToOneConcurrentArrayQueue<>(CAPACITY);
    egressListener =
        new WebSocketEgressListener(queue, returnQueue, metrics, CAPACITY, MAX_MESSAGE_SIZE);
    final var config = WebSocketServerConfig.builder().build();
    sessionManager = new WebSocketSessionManager(config, metrics, clock);
    final var ackQueue = new ManyToOneConcurrentArrayQueue<EgressEntry>(CAPACITY);
    final var commandEntryPool = new CommandEntryPool(CAPACITY, MAX_MESSAGE_SIZE);
    drainHandler =
        new WebSocketDrainHandler(
            queue, ackQueue, commandEntryPool, egressListener, sessionManager, metrics, clock);
    sbeBuf = new ExpandableArrayBuffer(MAX_MESSAGE_SIZE);

    channel = new EmbeddedChannel(DefaultChannelId.newInstance());
    session = sessionManager.tryRegister(channel);
    assertNotNull(session, "Session must be created");
    session.initSubscriptionFilter(100, metrics);
    session.subscriptionFilter().addSubscription(0L, 0x1F);
    session.subscriptionFilter().addSubscription(SymbolPacker.pack("EURUSD  "), 0x1F);
    session.entitledAccounts(Set.of("TEST-ACCT"));
    session.initReliableStreamTracker(TRACKER_CAPACITY, MAX_MESSAGE_SIZE, metrics);
  }

  @AfterEach
  void tearDown() {
    channel.finishAndReleaseAll();
  }

  /**
   * Reliable path: with {@code ch.isWritable() == false}, the drain handler MUST still write the
   * frame (Netty buffers it; the SlowConsumerHandler will eventually disconnect if backpressure
   * persists). The reliable seq counter MUST advance. The {@code
   * websocket.egress.dropped.channel-not-writable} counter MUST NOT increment for the reliable
   * scenario.
   */
  @Test
  void drain_reliableFrame_channelNotWritable_isWrittenAndCounterStays() {
    makeNonWritable(channel);
    assertFalse(channel.isWritable(), "Channel must be non-writable for the precondition");

    final long seqBefore = session.reliableSeqCounter();
    final double dropBefore = registry.get(METRIC_DROP).counter().count();

    // Enqueue a reliable CommandAck (templateId 70 — reliable per EgressEntry.isReliable()).
    enqueueReliableCommandAck();

    drainHandler.drain();

    final double dropAfter = registry.get(METRIC_DROP).counter().count();
    final long seqAfter = session.reliableSeqCounter();

    // Drain BinaryWebSocketFrames from the outbound buffer past the priming placeholder byte.
    int reliableFramesWritten = 0;
    Object outbound;
    while ((outbound = channel.readOutbound()) != null) {
      if (outbound instanceof BinaryWebSocketFrame bf) {
        reliableFramesWritten++;
        bf.release();
      } else if (outbound instanceof ReferenceCounted rc) {
        rc.release();
      }
    }

    assertEquals(
        1,
        reliableFramesWritten,
        "Reliable frame must be written to the channel even when isWritable()==false");
    assertEquals(
        seqBefore + 1L,
        seqAfter,
        "Reliable seq counter must advance — seqNo is burned on every reliable frame");
    assertEquals(
        dropBefore,
        dropAfter,
        1e-9,
        "egress.dropped.channel-not-writable must NOT increment on the reliable path");
  }

  /**
   * Best-effort path: with {@code ch.isWritable() == false}, the drain handler MUST drop the frame,
   * increment the counter, and NOT write to the channel.
   */
  @Test
  void drain_bestEffortFrame_channelNotWritable_isDroppedAndCounterIncrements() {
    makeNonWritable(channel);
    final double dropBefore = registry.get(METRIC_DROP).counter().count();

    // Enqueue a best-effort frame — template 64 (WebSocketHeartbeat) — to trigger the
    // writeBestEffortToAllChannels path. Use a simple raw byte payload sized like an SBE header.
    enqueueBestEffortHeartbeat();

    drainHandler.drain();

    final double dropAfter = registry.get(METRIC_DROP).counter().count();

    // Outbound queue must contain ONLY the priming placeholder byte — no BinaryWebSocketFrame
    // for the best-effort drop.
    int bestEffortFramesWritten = 0;
    Object outbound;
    while ((outbound = channel.readOutbound()) != null) {
      if (outbound instanceof BinaryWebSocketFrame bf) {
        bestEffortFramesWritten++;
        bf.release();
      } else if (outbound instanceof ReferenceCounted rc) {
        rc.release();
      }
    }

    assertEquals(
        0,
        bestEffortFramesWritten,
        "Best-effort frame must NOT be written to the channel when isWritable()==false");
    assertTrue(
        dropAfter > dropBefore,
        "egress.dropped.channel-not-writable must increment on the best-effort drop path");
  }

  // ────────────────────────────────────────────────────────────────────────
  // Helpers

  private static void makeNonWritable(final EmbeddedChannel ch) {
    ch.config().setWriteBufferWaterMark(new WriteBufferWaterMark(0, 0));
    ch.write(Unpooled.wrappedBuffer(new byte[] {0x00}));
  }

  private void enqueueReliableCommandAck() {
    final var headerEncoder = new MessageHeaderEncoder();
    final var ackEncoder = new CommandAckEncoder();
    ackEncoder.wrapAndApplyHeader(sbeBuf, 0, headerEncoder);
    ackEncoder.clientCmdSeqNo(42L);
    ackEncoder.status(CommandAckStatus.Accepted);
    final int encodedLen = MessageHeaderEncoder.ENCODED_LENGTH + ackEncoder.encodedLength();
    final var entry = egressListener.borrowForAck();
    assertNotNull(entry, "Test pool must not be exhausted");
    System.arraycopy(sbeBuf.byteArray(), 0, entry.bytes(), 0, encodedLen);
    entry.setMetadata(encodedLen, CommandAckEncoder.TEMPLATE_ID);
    assertTrue(queue.offer(entry), "Queue offer must succeed");
  }

  private void enqueueBestEffortHeartbeat() {
    // Minimal SBE header (8 bytes) with templateId=64 (WebSocketHeartbeat — best-effort per
    // EgressEntry.isReliable: templateId 64 is NOT in the reliable set).
    final var entry = egressListener.borrowForAck();
    assertNotNull(entry, "Test pool must not be exhausted");
    // SBE header: blockLength(2) || templateId(2) || schemaId(2) || version(2) all LE.
    final byte[] hdr = entry.bytes();
    hdr[0] = 0;
    hdr[1] = 0; // blockLength = 0
    hdr[2] = 64;
    hdr[3] = 0; // templateId = 64
    hdr[4] = 1;
    hdr[5] = 0; // schemaId = 1
    hdr[6] = 1;
    hdr[7] = 0; // version = 1
    entry.setMetadata(8, 64);
    assertTrue(queue.offer(entry), "Queue offer must succeed");
  }
}
