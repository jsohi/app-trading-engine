package com.trading.engine.websocket;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link WriteByteCounterHandler}. */
final class WriteByteCounterHandlerTest {

  private EmbeddedChannel channel;
  private WriteByteCounterHandler handler;

  @BeforeEach
  void setUp() {
    handler = new WriteByteCounterHandler();
    channel = new EmbeddedChannel(handler);
  }

  @AfterEach
  void tearDown() {
    channel.finishAndReleaseAll();
  }

  @Test
  void write_thenFlush_decrementsCounter() {
    final ByteBuf payload = Unpooled.buffer().writeBytes(new byte[100]);
    final var frame = new BinaryWebSocketFrame(payload);

    channel.write(frame);
    assertEquals(100L, handler.pendingBytes(), "tally tracks pending writes pre-flush");

    channel.flush();
    // EmbeddedChannel completes the promise on flush — counter should drop.
    assertEquals(0L, handler.pendingBytes(), "tally clears once promise completes");
  }

  @Test
  void write_multipleFrames_sumsBeforeFlush() {
    channel.write(new BinaryWebSocketFrame(Unpooled.buffer().writeBytes(new byte[40])));
    channel.write(new BinaryWebSocketFrame(Unpooled.buffer().writeBytes(new byte[60])));
    assertEquals(100L, handler.pendingBytes());
    channel.flush();
    assertEquals(0L, handler.pendingBytes());
  }

  @Test
  void write_nonFrameMessage_doesNotAffectCounter() {
    // A plain Object should not advance the byte counter.
    channel.write("hello"); // Strings are skipped by readableBytes()
    assertEquals(0L, handler.pendingBytes());
    // EmbeddedChannel will fail the write (no codec for String), but the counter must remain 0.
    channel.flush();
    assertEquals(0L, handler.pendingBytes());
  }

  @Test
  void pendingBytesRef_returnsLiveReference() {
    final var ref = handler.pendingBytesRef();
    channel.write(new BinaryWebSocketFrame(Unpooled.buffer().writeBytes(new byte[10])));
    assertEquals(10L, ref.get(), "shared AtomicLong reflects writes");
    channel.flush();
    assertEquals(0L, ref.get());
  }
}
