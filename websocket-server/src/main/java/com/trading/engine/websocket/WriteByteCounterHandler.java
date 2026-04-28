package com.trading.engine.websocket;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.websocketx.WebSocketFrame;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Per-pipeline outbound handler that tallies bytes pending in Netty's outbound write queue between
 * {@link #write(ChannelHandlerContext, Object, ChannelPromise)} and the channel's final OS-level
 * handoff (signalled by {@link ChannelPromise} completion).
 *
 * <p><b>Purpose.</b> Provides absolute-byte visibility into per-channel write-queue depth, used by
 * {@link SlowConsumerHandler} to apply graduated 4-level backpressure thresholds. Avoids Netty
 * internal {@code Channel.unsafe()} (which is private API) by observing public {@code write} +
 * {@code promise.addListener} hooks.
 *
 * <p><b>Semantics.</b> {@code pendingBytes} reflects bytes Netty has accepted for write but not yet
 * fully flushed to the OS kernel. It does NOT measure:
 *
 * <ul>
 *   <li>Bytes already handed to the kernel but not yet ACKed by the peer (TCP send-window state).
 *   <li>Bytes the peer has received but not yet processed (application-layer lag).
 * </ul>
 *
 * <p>Despite this caveat, Netty queue depth is a reliable early-warning signal for slow consumers
 * because the queue grows whenever the kernel cannot accept more bytes (which is the symptom of TCP
 * back-pressure from a slow peer).
 *
 * <p><b>Threading.</b> One instance per pipeline (NOT {@code @Sharable}). All {@code write} calls
 * happen on the channel's event loop thread; promise-completion callbacks are dispatched on the
 * channel's event loop too. {@link AtomicLong} is used purely for cross-thread visibility of {@link
 * #pendingBytes()} reads from the SlowConsumerHandler scan loop (which runs on the drain thread, a
 * different worker event loop than the per-channel one).
 *
 * <p><b>Allocation.</b> Zero allocation per write — relies on the existing {@link ChannelPromise}
 * for the listener registration; no closure capture of mutable state beyond the {@code int} payload
 * size.
 *
 * @see SlowConsumerHandler
 */
public final class WriteByteCounterHandler extends ChannelOutboundHandlerAdapter {

  private final AtomicLong pendingBytes = new AtomicLong(0L);

  /**
   * Returns the {@link AtomicLong} backing the byte counter so external readers (the {@link
   * SlowConsumerHandler}) can observe the value without invoking a method on the handler instance
   * directly. The returned reference is shared and must not be replaced by the caller.
   *
   * @return the live byte counter (read-only by convention; only this handler mutates it)
   */
  public AtomicLong pendingBytesRef() {
    return pendingBytes;
  }

  /**
   * @return the current number of bytes accepted for write but not yet flushed to the OS kernel
   */
  public long pendingBytes() {
    return pendingBytes.get();
  }

  /**
   * Tallies the bytes about to be written, then registers a listener on the supplied promise that
   * decrements the tally when the kernel handoff completes (success or failure).
   *
   * <p>{@link WebSocketFrame} delegates to its inner {@link ByteBuf} via {@code content()}; raw
   * {@link ByteBuf} writes are handled directly. Other message types (e.g. {@code Object}) are
   * passed through without accounting — the WebSocket pipeline only writes frames or buffers
   * downstream of the dispatcher.
   *
   * @param ctx the channel handler context
   * @param msg the outbound message
   * @param promise the channel promise; receives a listener that decrements the tally
   * @throws Exception if the super call throws
   */
  @Override
  public void write(final ChannelHandlerContext ctx, final Object msg, final ChannelPromise promise)
      throws Exception {
    final int delta = readableBytes(msg);
    if (delta > 0) {
      pendingBytes.addAndGet(delta);
      promise.addListener(future -> pendingBytes.addAndGet(-delta));
    }
    super.write(ctx, msg, promise);
  }

  private static int readableBytes(final Object msg) {
    if (msg instanceof WebSocketFrame frame) {
      final var content = frame.content();
      return content == null ? 0 : content.readableBytes();
    }
    if (msg instanceof ByteBuf buf) {
      return buf.readableBytes();
    }
    return 0;
  }
}
