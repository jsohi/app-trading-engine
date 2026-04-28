package com.trading.engine.websocket;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.trading.engine.messages.sbe.CommandAckDecoder;
import com.trading.engine.messages.sbe.CommandAckStatus;
import com.trading.engine.messages.sbe.MessageHeaderDecoder;
import com.trading.engine.messages.sbe.MessageHeaderEncoder;
import com.trading.engine.messages.sbe.NewOrderSingleEncoder;
import com.trading.engine.testsupport.clock.ControllableNanoClock;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import java.nio.charset.StandardCharsets;
import java.util.Set;
import java.util.UUID;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.ManyToOneConcurrentArrayQueue;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link CommandDispatcher}. */
final class CommandDispatcherTest {

  private WebSocketServerConfig config;
  private WebSocketMetrics metrics;
  private ControllableNanoClock clock;
  private ManyToOneConcurrentArrayQueue<EgressEntry> commandQueue;
  private CommandEntryPool pool;
  private CommandDispatcher dispatcher;
  private EmbeddedChannel channel;
  private WebSocketSession session;
  private WebSocketSessionManager sessionManager;

  @BeforeEach
  void setUp() {
    config =
        WebSocketServerConfig.builder()
            .commandsPerSecSustained(2)
            .commandsBurst(3)
            .clOrdIdDedupCapacity(8)
            .clOrdIdDedupTtlMs(60_000L)
            .clOrdIdDedupMaxUsers(8)
            .dedupTryLockMicros(50L)
            .commandQueueCapacity(16)
            .commandAckQueueCapacity(8)
            .replayBufferFrames(8)
            .replayBufferFrameSize(256)
            .build();
    metrics = WebSocketMetrics.createWithDefaults();
    clock = new ControllableNanoClock();
    sessionManager = new WebSocketSessionManager(config, metrics, clock);
    commandQueue = new ManyToOneConcurrentArrayQueue<>(16);
    pool = new CommandEntryPool(16, 256);
    dispatcher =
        new CommandDispatcher(
            config,
            metrics,
            clock,
            commandQueue,
            new CommandDispatcher.EgressEntryAllocator() {
              @Override
              public EgressEntry tryAcquire() {
                return pool.tryAcquire();
              }

              @Override
              public void release(final EgressEntry entry) {
                pool.release(entry);
              }
            });
    channel = new EmbeddedChannel(new ChannelInboundHandlerAdapter());
    session = sessionManager.tryRegister(channel);
    sessionManager.setUserId(session, "user-001");
    session.entitledAccounts(Set.of("ACME-001"));
    session.initSubscriptionFilter(100);
    session.initReliableStreamTracker(8, 256, metrics);
  }

  @AfterEach
  void tearDown() {
    dispatcher.close();
    if (channel.isOpen()) {
      channel.close();
    }
    channel.finishAndReleaseAll();
  }

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  private ByteBuf encodeNewOrderSingle(final String clOrdId, final String accountCode) {
    final var buf = new ExpandableArrayBuffer(256);
    final var enc = new NewOrderSingleEncoder();
    final var hdr = new MessageHeaderEncoder();
    enc.wrapAndApplyHeader(buf, 0, hdr);
    enc.clOrdId(clOrdId);
    enc.accountCode(accountCode);
    enc.orderQty(100L);
    enc.price(10000L);
    final int totalLen = MessageHeaderEncoder.ENCODED_LENGTH + enc.encodedLength();
    final var nettyBuf = Unpooled.buffer(totalLen);
    nettyBuf.writeBytes(buf.byteArray(), 0, totalLen);
    return nettyBuf;
  }

  private CommandAckStatus readLastAckStatus() {
    BinaryWebSocketFrame ack = null;
    Object next;
    while ((next = channel.readOutbound()) != null) {
      if (next instanceof BinaryWebSocketFrame f) {
        if (ack != null) {
          ack.release();
        }
        ack = f;
      } else if (next instanceof ByteBuf bb) {
        bb.release();
      }
    }
    if (ack == null) {
      return null;
    }
    try {
      // Strip 17-byte reliable header
      final var payload =
          new byte[ack.content().readableBytes() - FrameParser.RELIABLE_HEADER_SIZE];
      ack.content().getBytes(FrameParser.RELIABLE_HEADER_SIZE, payload, 0, payload.length);
      final var wrap = new UnsafeBuffer(payload);
      final var hdr = new MessageHeaderDecoder();
      hdr.wrap(wrap, 0);
      assertEquals(CommandAckDecoder.TEMPLATE_ID, hdr.templateId());
      final var dec = new CommandAckDecoder();
      dec.wrap(wrap, MessageHeaderDecoder.ENCODED_LENGTH, hdr.blockLength(), hdr.version());
      return dec.status();
    } finally {
      ack.release();
    }
  }

  // ---------------------------------------------------------------------------
  // Tests
  // ---------------------------------------------------------------------------

  @Test
  void dispatch_validCommand_enqueuesAndAcksAccepted() {
    final ByteBuf frame = encodeNewOrderSingle("ORD-1", "ACME-001");
    try {
      dispatcher.dispatch(
          channel.pipeline().firstContext(),
          session,
          frame,
          4,
          NewOrderSingleEncoder.BLOCK_LENGTH,
          1);
      channel.flushOutbound();
      assertEquals(CommandAckStatus.Accepted, readLastAckStatus());
      assertEquals(1, commandQueue.size());
    } finally {
      frame.release();
    }
  }

  @Test
  void dispatch_unentitledAccount_rejected() {
    final ByteBuf frame = encodeNewOrderSingle("ORD-1", "OTHER-001");
    try {
      dispatcher.dispatch(
          channel.pipeline().firstContext(),
          session,
          frame,
          4,
          NewOrderSingleEncoder.BLOCK_LENGTH,
          1);
      channel.flushOutbound();
      assertEquals(CommandAckStatus.Rejected, readLastAckStatus());
      assertEquals(0, commandQueue.size());
    } finally {
      frame.release();
    }
  }

  @Test
  void dispatch_duplicateClOrdId_returnsDuplicate() {
    final ByteBuf f1 = encodeNewOrderSingle("ORD-DUP", "ACME-001");
    final ByteBuf f2 = encodeNewOrderSingle("ORD-DUP", "ACME-001");
    try {
      dispatcher.dispatch(
          channel.pipeline().firstContext(), session, f1, 4, NewOrderSingleEncoder.BLOCK_LENGTH, 1);
      channel.flushOutbound();
      assertEquals(CommandAckStatus.Accepted, readLastAckStatus());

      dispatcher.dispatch(
          channel.pipeline().firstContext(), session, f2, 4, NewOrderSingleEncoder.BLOCK_LENGTH, 1);
      channel.flushOutbound();
      assertEquals(CommandAckStatus.Duplicate, readLastAckStatus());
    } finally {
      f1.release();
      f2.release();
    }
  }

  @Test
  void dispatch_dedupTtlExpiry_acceptsAfterTtl() {
    final ByteBuf f1 = encodeNewOrderSingle("ORD-TTL", "ACME-001");
    final ByteBuf f2 = encodeNewOrderSingle("ORD-TTL", "ACME-001");
    try {
      dispatcher.dispatch(
          channel.pipeline().firstContext(), session, f1, 4, NewOrderSingleEncoder.BLOCK_LENGTH, 1);
      channel.flushOutbound();
      readLastAckStatus(); // drain

      // Advance past TTL.
      clock.advanceMillis(config.clOrdIdDedupTtlMs() + 1);

      dispatcher.dispatch(
          channel.pipeline().firstContext(), session, f2, 4, NewOrderSingleEncoder.BLOCK_LENGTH, 1);
      channel.flushOutbound();
      assertEquals(CommandAckStatus.Accepted, readLastAckStatus());
    } finally {
      f1.release();
      f2.release();
    }
  }

  @Test
  void dispatch_rateLimitExceeded_returnsThrottled() {
    // Burst=3, sustained=2 → after 3 ACCEPTED the 4th must be THROTTLED.
    for (int i = 0; i < 3; i++) {
      final ByteBuf frame = encodeNewOrderSingle("ORD-RL-" + i, "ACME-001");
      try {
        dispatcher.dispatch(
            channel.pipeline().firstContext(),
            session,
            frame,
            4,
            NewOrderSingleEncoder.BLOCK_LENGTH,
            1);
        channel.flushOutbound();
        assertEquals(CommandAckStatus.Accepted, readLastAckStatus(), "command #" + i);
      } finally {
        frame.release();
      }
    }
    final ByteBuf frame4 = encodeNewOrderSingle("ORD-RL-4", "ACME-001");
    try {
      dispatcher.dispatch(
          channel.pipeline().firstContext(),
          session,
          frame4,
          4,
          NewOrderSingleEncoder.BLOCK_LENGTH,
          1);
      channel.flushOutbound();
      assertEquals(CommandAckStatus.Throttled, readLastAckStatus());
    } finally {
      frame4.release();
    }
  }

  @Test
  void dispatch_acceptedCommand_routedThroughCommandQueue() {
    final ByteBuf frame = encodeNewOrderSingle("ORD-Q", "ACME-001");
    try {
      dispatcher.dispatch(
          channel.pipeline().firstContext(),
          session,
          frame,
          4,
          NewOrderSingleEncoder.BLOCK_LENGTH,
          1);
    } finally {
      frame.release();
    }
    final var entry = commandQueue.poll();
    assertNotNull(entry);
    assertEquals(EgressEntry.Direction.BROWSER_TO_CLUSTER, entry.direction());
    assertEquals(4, entry.templateId());
    assertEquals(session.sessionId(), new UUID(entry.sessionIdMsb(), entry.sessionIdLsb()));
    pool.release(entry);
  }

  @Test
  void dispatch_truncatedFrame_doesNotEnqueue() {
    // Frame too small for any account field — synthesizes truncation.
    final var truncated = new byte[10];
    truncated[2] = 4; // templateId LSB
    truncated[3] = 0; // templateId MSB
    final var frame = Unpooled.wrappedBuffer(truncated);
    try {
      dispatcher.dispatch(
          channel.pipeline().firstContext(),
          session,
          frame,
          4,
          NewOrderSingleEncoder.BLOCK_LENGTH,
          1);
    } finally {
      frame.release();
    }
    assertEquals(0, commandQueue.size());
  }

  @Test
  void dispatch_userWithoutEntitledAccount_rejected() {
    session.entitledAccounts(Set.of()); // strip entitlements
    final ByteBuf frame = encodeNewOrderSingle("ORD-NE", "ACME-001");
    try {
      dispatcher.dispatch(
          channel.pipeline().firstContext(),
          session,
          frame,
          4,
          NewOrderSingleEncoder.BLOCK_LENGTH,
          1);
      channel.flushOutbound();
      assertEquals(CommandAckStatus.Rejected, readLastAckStatus());
    } finally {
      frame.release();
    }
  }

  @Test
  void sweep_evictsExpiredDedupEntries() {
    final ByteBuf frame = encodeNewOrderSingle("ORD-S", "ACME-001");
    try {
      dispatcher.dispatch(
          channel.pipeline().firstContext(),
          session,
          frame,
          4,
          NewOrderSingleEncoder.BLOCK_LENGTH,
          1);
    } finally {
      frame.release();
    }
    assertTrue(dispatcher.dedupUserCount() >= 1);
    clock.advanceMillis(config.clOrdIdDedupTtlMs() + 1);
    dispatcher.sweepForTest();
    assertEquals(0, dispatcher.dedupUserCount());
  }

  @Test
  void responseAcks_areCapturedInReliableTracker() {
    final ByteBuf frame = encodeNewOrderSingle("ORD-Cap", "ACME-001");
    try {
      dispatcher.dispatch(
          channel.pipeline().firstContext(),
          session,
          frame,
          4,
          NewOrderSingleEncoder.BLOCK_LENGTH,
          1);
    } finally {
      frame.release();
    }
    // The tracker must have captured the ack at seqNo 1.
    assertTrue(session.reliableStreamTracker().lookupLength(1L) > 0);
  }

  @Test
  void rejectedCommand_releasesEntryToPoolAndDoesNotEnqueue() {
    final int initialAvailable = pool.available();
    final ByteBuf frame = encodeNewOrderSingle("ORD-RE", "OTHER-ACCOUNT");
    try {
      dispatcher.dispatch(
          channel.pipeline().firstContext(),
          session,
          frame,
          4,
          NewOrderSingleEncoder.BLOCK_LENGTH,
          1);
    } finally {
      frame.release();
    }
    assertEquals(0, commandQueue.size());
    assertEquals(initialAvailable, pool.available(), "rejected command must not consume pool");
  }

  @Test
  void reauth_doesNotChangeOriginalAuthJti() {
    session.originalAuthJti("first-jti");
    session.jti("rotated-jti");
    session.originalAuthJti("second-jti"); // attempt to update — must be no-op
    assertEquals("first-jti", session.originalAuthJti());
  }

  @Test
  void byteArrayAccountField_isExtractedAndCheckedAgainstEntitlements() {
    // Ensures account scratch byte buffer is correctly populated from ByteBuf.getBytes.
    session.entitledAccounts(Set.of("FOO-XYZ"));
    final ByteBuf good = encodeNewOrderSingle("ORD-G", "FOO-XYZ");
    final ByteBuf bad = encodeNewOrderSingle("ORD-B", "BAR-XYZ");
    try {
      dispatcher.dispatch(
          channel.pipeline().firstContext(),
          session,
          good,
          4,
          NewOrderSingleEncoder.BLOCK_LENGTH,
          1);
      channel.flushOutbound();
      assertEquals(CommandAckStatus.Accepted, readLastAckStatus());

      dispatcher.dispatch(
          channel.pipeline().firstContext(),
          session,
          bad,
          4,
          NewOrderSingleEncoder.BLOCK_LENGTH,
          1);
      channel.flushOutbound();
      assertEquals(CommandAckStatus.Rejected, readLastAckStatus());
    } finally {
      good.release();
      bad.release();
    }
  }

  // SBE schema's NewOrderSingleEncoder uses a Latin-1-ish encoding via putAccountCode(byte[]) too;
  // ensure the String setter (which putAccountCode wraps) works for both the unit test path and
  // production.
  @Test
  void sbeFieldEncoding_byteWiseAccountSetter_alsoMatches() {
    session.entitledAccounts(Set.of("BYT-1"));
    final var buf = new ExpandableArrayBuffer(256);
    final var enc = new NewOrderSingleEncoder();
    final var hdr = new MessageHeaderEncoder();
    enc.wrapAndApplyHeader(buf, 0, hdr);
    enc.clOrdId("ORD-Byte");
    final var accountBytes = "BYT-1".getBytes(StandardCharsets.US_ASCII);
    final var padded = new byte[16];
    System.arraycopy(accountBytes, 0, padded, 0, accountBytes.length);
    enc.putAccountCode(padded, 0);
    enc.orderQty(1L);
    enc.price(1L);
    final int totalLen = MessageHeaderEncoder.ENCODED_LENGTH + enc.encodedLength();
    final var nettyBuf = Unpooled.buffer(totalLen);
    nettyBuf.writeBytes(buf.byteArray(), 0, totalLen);
    try {
      dispatcher.dispatch(
          channel.pipeline().firstContext(),
          session,
          nettyBuf,
          4,
          NewOrderSingleEncoder.BLOCK_LENGTH,
          1);
      channel.flushOutbound();
      assertEquals(CommandAckStatus.Accepted, readLastAckStatus());
    } finally {
      nettyBuf.release();
    }
  }
}
