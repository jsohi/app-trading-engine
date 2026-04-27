package com.trading.engine.websocket;

import io.netty.buffer.PooledByteBufAllocator;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import java.util.Objects;
import org.agrona.concurrent.ManyToOneConcurrentArrayQueue;
import org.agrona.concurrent.NanoClock;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Standalone drain task that polls the {@link ManyToOneConcurrentArrayQueue} bridge between the
 * Aeron egress thread and the Netty event loop, encoding each message into the custom wire envelope
 * and writing to all active client channels.
 *
 * <p><b>Scheduling.</b> Scheduled once on the Netty worker event loop at 1ms fixed rate via {@code
 * scheduleWithFixedDelay} in {@link WebSocketServerMain#start()}. Not a {@code ChannelHandler} — a
 * single instance serves all channels, avoiding the per-channel timer leak of the previous design.
 *
 * <p><b>Filtering.</b> Per-session {@link SubscriptionFilter} checks symbol + event type. Per-
 * session account entitlement checks via zero-allocation packed long comparison through {@link
 * AccountExtractor#extractPackedAccount} and {@link WebSocketSession#isEntitledAccount}. Messages
 * that don't match a session's subscriptions or entitlements are skipped (O(M*S) reduced to O(M*S')
 * where S' is the matching subset).
 *
 * <p><b>Reliable vs best-effort.</b> Reliable messages get per-session ByteBuf (different seqNo →
 * different CRC32C → can't share). Best-effort messages share one ByteBuf via {@code
 * retainedDuplicate()} (seqNo=0 for all).
 *
 * <p><b>Threading.</b> Runs on the Netty worker event loop thread only. Not thread-safe.
 *
 * <p><b>Allocation.</b> Per-session pooled ByteBuf for reliable messages (acceptable per CLAUDE.md
 * WebSocket exception). Shared ByteBuf with retainedDuplicate for best-effort. Pre-allocated {@code
 * long[2]} flyweight for zero-alloc packed account extraction.
 *
 * @see <a href="docs/websocket-architecture.md">WebSocket Architecture — Section 1, Section 6</a>
 */
public final class WebSocketDrainHandler {

  private static final Logger LOG = LogManager.getLogger(WebSocketDrainHandler.class);

  private final ManyToOneConcurrentArrayQueue<EgressEntry> queue;
  private final WebSocketEgressListener egressListener;
  private final WebSocketSessionManager sessionManager;
  private final WebSocketMetrics metrics;
  private final NanoClock nanoClock;

  /** Pre-allocated flyweight for zero-alloc packed account extraction on the drain hot path. */
  private final long[] packedAccountBuf = new long[2];

  /**
   * Create a drain handler.
   *
   * @param queue the egress queue to drain
   * @param egressListener the listener (for returning entries to the pool)
   * @param sessionManager the session manager (for iterating active sessions)
   * @param metrics metrics instance for queue depth and filter metrics
   * @param nanoClock monotonic clock for drain cycle latency measurement
   */
  public WebSocketDrainHandler(
      final ManyToOneConcurrentArrayQueue<EgressEntry> queue,
      final WebSocketEgressListener egressListener,
      final WebSocketSessionManager sessionManager,
      final WebSocketMetrics metrics,
      final NanoClock nanoClock) {
    this.queue = Objects.requireNonNull(queue, "queue");
    this.egressListener = Objects.requireNonNull(egressListener, "egressListener");
    this.sessionManager = Objects.requireNonNull(sessionManager, "sessionManager");
    this.metrics = Objects.requireNonNull(metrics, "metrics");
    this.nanoClock = Objects.requireNonNull(nanoClock, "nanoClock");
  }

  /**
   * Drain all available entries from the queue and fan-out to matching session channels. Called
   * from the Netty worker event loop at 1ms fixed rate.
   *
   * <p>Per-session {@link SubscriptionFilter} and account entitlement checks reduce fan-out to only
   * matching sessions. Reliable messages use per-session ByteBuf with per-session seqNo.
   * Best-effort messages share a single ByteBuf via {@code retainedDuplicate()}.
   */
  public void drain() {
    final long cycleStartNs = nanoClock.nanoTime();
    int drained = 0;
    EgressEntry entry;

    while ((entry = queue.poll()) != null) {
      try {
        if (entry.isReliable()) {
          writeReliableToAllChannels(entry);
        } else {
          writeBestEffortToAllChannels(entry);
        }
        drained++;
      } finally {
        egressListener.returnToPool(entry);
      }
    }

    if (drained > 0) {
      // Flush all active channels once at the end of the drain cycle (for-loop, not lambda)
      for (final var session : sessionManager.sessions()) {
        final var ch = session.channel();
        if (ch.isActive()) {
          ch.flush();
        }
      }
      metrics.updateQueueDepth(queue.size());

      // Record drain cycle latency using injected NanoClock (not System.nanoTime)
      final long cycleNs = nanoClock.nanoTime() - cycleStartNs;
      metrics.recordDrainCycleNanos(cycleNs);
    }
  }

  /**
   * Fan out a reliable message to all matching sessions with per-session sequence numbers. Each
   * session gets its own ByteBuf because different seqNo values produce different CRC32C checksums.
   */
  private void writeReliableToAllChannels(final EgressEntry entry) {
    final var bytes = entry.bytes();
    final int length = entry.length();
    final int templateId = entry.templateId();

    for (final var session : sessionManager.sessions()) {
      final var filter = session.subscriptionFilter();
      if (filter == null) {
        continue; // pre-auth session — no subscriptions yet
      }
      // SubscriptionFilter only applies to mapped event templates (orders, prices, quotes,
      // positions, accounts). Unmapped templates (CommandAck=70, WebSocketError=67, etc.)
      // are control messages that bypass filtering and are delivered to all sessions.
      final int eventBit = SubscriptionFilter.templateIdToEventBit(templateId);
      if (eventBit >= 0) {
        if (!filter.matches(templateId, bytes, 0, length)) {
          metrics.filterFiltered();
          continue;
        }

        // Zero-alloc account entitlement check (single-call packed long extraction)
        if (AccountExtractor.extractPackedAccount(templateId, bytes, 0, length, packedAccountBuf)
            && !session.isEntitledAccount(packedAccountBuf[0], packedAccountBuf[1])) {
          metrics.filterFiltered();
          continue;
        }
      }

      metrics.filterMatched();

      final var buf =
          PooledByteBufAllocator.DEFAULT.buffer(
              FrameParser.RELIABLE_HEADER_SIZE + length, FrameParser.RELIABLE_HEADER_SIZE + length);
      boolean written = false;
      try {
        FrameParser.encodeReliable(buf, session.nextReliableSeqNo(), bytes, 0, length);
        session.channel().write(new BinaryWebSocketFrame(buf));
        written = true;
      } finally {
        if (!written) {
          buf.release();
        }
      }
    }
  }

  /**
   * Fan out a best-effort message to all matching sessions. Shared ByteBuf with {@code
   * retainedDuplicate()} — seqNo=0 for all sessions (no per-session CRC variation).
   */
  private void writeBestEffortToAllChannels(final EgressEntry entry) {
    final var bytes = entry.bytes();
    final int length = entry.length();
    final int templateId = entry.templateId();
    final int frameSize = FrameParser.BEST_EFFORT_HEADER_SIZE + length;

    final var frameBuf = PooledByteBufAllocator.DEFAULT.buffer(frameSize, frameSize);
    try {
      FrameParser.encodeBestEffort(frameBuf, bytes, 0, length);

      for (final var session : sessionManager.sessions()) {
        final var filter = session.subscriptionFilter();
        if (filter == null) {
          continue;
        }

        final var ch = session.channel();
        if (!ch.isActive() || !ch.isWritable()) {
          continue;
        }

        // Same filter bypass as reliable path — unmapped templates are control messages.
        final int bestEffortEventBit = SubscriptionFilter.templateIdToEventBit(templateId);
        if (bestEffortEventBit >= 0) {
          if (!filter.matches(templateId, bytes, 0, length)) {
            metrics.filterFiltered();
            continue;
          }

          if (AccountExtractor.extractPackedAccount(templateId, bytes, 0, length, packedAccountBuf)
              && !session.isEntitledAccount(packedAccountBuf[0], packedAccountBuf[1])) {
            metrics.filterFiltered();
            continue;
          }
        }

        metrics.filterMatched();
        ch.write(new BinaryWebSocketFrame(frameBuf.retainedDuplicate()));
      }
    } catch (final Exception e) {
      LOG.warn("Failed to encode best-effort frame for templateId={}", templateId);
    } finally {
      frameBuf.release();
    }
  }
}
