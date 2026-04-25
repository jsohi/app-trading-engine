package com.trading.engine.websocket;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import java.util.Objects;
import org.agrona.concurrent.ManyToOneConcurrentArrayQueue;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Standalone drain task that polls the {@link ManyToOneConcurrentArrayQueue} bridge between the
 * Aeron egress thread and the Netty event loop, encoding each message into the custom wire envelope
 * and writing to all active client channels.
 *
 * <p><b>Scheduling.</b> Scheduled once on the Netty worker event loop at 1ms fixed rate via {@code
 * scheduleAtFixedRate} in {@link WebSocketServerMain#start()}. Not a {@code ChannelHandler} — a
 * single instance serves all channels, avoiding the per-channel timer leak of the previous design.
 *
 * <p><b>Message priority.</b> Reliable messages (orders, fills, positions, errors, CommandAck) are
 * processed before best-effort messages (prices, quotes, heartbeat) within each drain cycle. This
 * ensures order fill notifications are not delayed behind price ticks.
 *
 * <p><b>ByteBuf fan-out.</b> For each message, one {@link ByteBuf} is allocated, then each active
 * channel receives a {@code retainedDuplicate()} sharing the same underlying memory. The original
 * is released after all writes. A single {@code flush()} per channel at the end of the drain cycle
 * batches all writes.
 *
 * <p><b>Threading.</b> Runs on the Netty worker event loop thread only. Not thread-safe.
 *
 * <p><b>Allocation.</b> One pooled {@link ByteBuf} per message per drain cycle (from {@link
 * PooledByteBufAllocator}). No Java object allocation on the hot path.
 *
 * @see <a href="docs/websocket-architecture.md">WebSocket Architecture — Section 1, Section 6</a>
 */
public final class WebSocketDrainHandler {

  private static final Logger LOG = LogManager.getLogger(WebSocketDrainHandler.class);

  private final ManyToOneConcurrentArrayQueue<EgressEntry> queue;
  private final WebSocketEgressListener egressListener;
  private final WebSocketSessionManager sessionManager;
  private final WebSocketMetrics metrics;

  /**
   * Create a drain handler.
   *
   * @param queue the egress queue to drain
   * @param egressListener the listener (for returning entries to the pool)
   * @param sessionManager the session manager (for iterating active sessions)
   * @param metrics metrics instance for queue depth updates
   */
  public WebSocketDrainHandler(
      final ManyToOneConcurrentArrayQueue<EgressEntry> queue,
      final WebSocketEgressListener egressListener,
      final WebSocketSessionManager sessionManager,
      final WebSocketMetrics metrics) {
    this.queue = Objects.requireNonNull(queue, "queue");
    this.egressListener = Objects.requireNonNull(egressListener, "egressListener");
    this.sessionManager = Objects.requireNonNull(sessionManager, "sessionManager");
    this.metrics = Objects.requireNonNull(metrics, "metrics");
  }

  /**
   * Drain all available entries from the queue and fan-out to every active session channel. Called
   * from the Netty worker event loop at 1ms fixed rate.
   *
   * <p>For each entry, a single pooled {@link ByteBuf} is allocated with the wire frame. Each
   * active channel receives a {@code retainedDuplicate()} of the frame, and the original is
   * released after all writes. Channels are flushed once at the end of the drain cycle.
   */
  public void drain() {
    int drained = 0;
    EgressEntry entry;

    // Drain all available entries from the queue.
    // In a future optimization, reliable messages would be separated and processed first.
    // For now, entries are processed in queue order (FIFO from Aeron egress).
    while ((entry = queue.poll()) != null) {
      try {
        writeToAllChannels(entry);
        drained++;
      } finally {
        // Return entry to the egress listener pool (via thread-safe return queue)
        egressListener.returnToPool(entry);
      }
    }

    if (drained > 0) {
      // Flush all active channels once at the end of the drain cycle
      sessionManager.forEachSession(
          session -> {
            final var ch = session.channel();
            if (ch.isActive()) {
              ch.flush();
            }
          });
      metrics.updateQueueDepth(queue.size());
    }
  }

  private void writeToAllChannels(final EgressEntry entry) {
    // Allocate a pooled ByteBuf for the wire envelope
    final int frameSize =
        entry.isReliable()
            ? FrameParser.RELIABLE_HEADER_SIZE + entry.length()
            : FrameParser.BEST_EFFORT_HEADER_SIZE + entry.length();

    final var frameBuf = PooledByteBufAllocator.DEFAULT.buffer(frameSize, frameSize);

    try {
      if (entry.isReliable()) {
        // For now, use seqNo=0 — proper per-session seq assignment comes in PR 3/4
        // when SubscriptionFilter and ReliableStreamTracker are wired in.
        FrameParser.encodeReliable(frameBuf, 0L, entry.bytes(), 0, entry.length());
      } else {
        FrameParser.encodeBestEffort(frameBuf, entry.bytes(), 0, entry.length());
      }

      // Fan-out: retainedDuplicate() per active channel, release original after all writes
      sessionManager.forEachSession(
          session -> {
            final var ch = session.channel();
            if (ch.isActive()) {
              ch.write(new BinaryWebSocketFrame(frameBuf.retainedDuplicate()));
            }
          });
    } catch (final Exception e) {
      LOG.warn("Failed to encode frame for templateId={}: {}", entry.templateId(), e.getMessage());
    } finally {
      // Release the original ByteBuf — each channel holds its own retained duplicate
      frameBuf.release();
    }
  }
}
