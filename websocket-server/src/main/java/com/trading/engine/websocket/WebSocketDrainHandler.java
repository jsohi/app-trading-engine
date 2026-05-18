package com.trading.engine.websocket;

import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import java.util.Objects;
import java.util.UUID;
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
 * <p><b>Slow-consumer interaction.</b> When {@link WebSocketSession#isDropBestEffort()} is true
 * (set by {@link SlowConsumerHandler} on level-2 entry), best-effort frames are skipped for that
 * session even if the channel reports {@code isWritable}.
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
  private final ManyToOneConcurrentArrayQueue<EgressEntry> ackQueue;
  private final CommandEntryPool commandEntryPool;
  private final WebSocketEgressListener egressListener;
  private final WebSocketSessionManager sessionManager;
  private final WebSocketMetrics metrics;
  private final NanoClock nanoClock;

  /** Pre-allocated flyweight for zero-alloc packed account extraction on the drain hot path. */
  private final long[] packedAccountBuf = new long[2];

  /**
   * Create a drain handler with full command/ack wiring. All collaborators required.
   *
   * @param queue the egress queue to drain
   * @param ackQueue the back-channel queue for CommandAck(THROTTLED) entries originating from
   *     {@link AeronEgressThread} after BACK_PRESSURED retries
   * @param commandEntryPool the pool that owns ack entries (for release after consume)
   * @param egressListener the listener (for returning entries to the pool)
   * @param sessionManager the session manager (for iterating active sessions)
   * @param metrics metrics instance for queue depth and filter metrics
   * @param nanoClock monotonic clock for drain cycle latency measurement
   */
  public WebSocketDrainHandler(
      final ManyToOneConcurrentArrayQueue<EgressEntry> queue,
      final ManyToOneConcurrentArrayQueue<EgressEntry> ackQueue,
      final CommandEntryPool commandEntryPool,
      final WebSocketEgressListener egressListener,
      final WebSocketSessionManager sessionManager,
      final WebSocketMetrics metrics,
      final NanoClock nanoClock) {
    this.queue = Objects.requireNonNull(queue, "queue");
    this.ackQueue = Objects.requireNonNull(ackQueue, "ackQueue");
    this.commandEntryPool = Objects.requireNonNull(commandEntryPool, "commandEntryPool");
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

    // Drain ack back-channel: CommandAck frames bound for a single session (carried by sessionId).
    while ((entry = ackQueue.poll()) != null) {
      try {
        writeAckToTargetChannel(entry);
        drained++;
      } finally {
        commandEntryPool.release(entry);
      }
    }

    if (drained > 0) {
      // Flush all active channels once at the end of the drain cycle (for-loop, not lambda).
      // sessionManager.sessions() returns ConcurrentHashMap.values() — weakly-consistent
      // iterator, safe for cross-thread iteration without external synchronization.
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

    // Hoist per-message invariants out of the session loop to avoid redundant computation.
    final int eventBit = SubscriptionFilter.templateIdToEventBit(templateId);
    final boolean hasAccount =
        eventBit >= 0
            && AccountExtractor.extractPackedAccount(
                templateId, bytes, 0, length, packedAccountBuf);

    for (final var session : sessionManager.sessions()) {
      final var ch = session.channel();
      if (!ch.isActive()) {
        continue;
      }
      final var filter = session.subscriptionFilter();
      if (filter == null) {
        continue; // pre-auth session — no subscriptions yet
      }
      // SubscriptionFilter only applies to mapped event templates (orders, prices, quotes,
      // positions, accounts). Unmapped templates (CommandAck=70, WebSocketError=67, etc.)
      // are control messages that bypass filtering and are delivered to all sessions.
      if (eventBit >= 0) {
        if (!filter.matches(templateId, bytes, 0, length)) {
          metrics.filterFiltered();
          continue;
        }

        // Zero-alloc account entitlement check using pre-extracted packed values
        if (hasAccount && !session.isEntitledAccount(packedAccountBuf[0], packedAccountBuf[1])) {
          metrics.filterFiltered();
          continue;
        }
      }

      metrics.filterMatched();

      writeReliableToSession(session, bytes, length, templateId, ch);
    }
  }

  /**
   * Encode and write a reliable frame for a single session, capturing it in the session's {@link
   * ReliableStreamTracker} so a later gap-request or session-resume can replay it. On any failure
   * (write throws, capture throws) the captured slot is evicted so replay never serves a frame the
   * client did not receive (which would produce phantom-gap on next ClientAck).
   *
   * <p><b>No back-pressure drop on the reliable path.</b> Gemini cloud-review R2 G-3 correctly
   * identified that dropping a reliable frame when {@code !ch.isWritable()} would violate the
   * reliable-stream contract: no seqNo would be burned, so the client could not detect the gap via
   * {@code ClientAck}, and the message would be permanently lost. The reliable path therefore
   * ALWAYS proceeds to write the frame; Netty buffers it past the water-mark, and the existing
   * {@link SlowConsumerHandler} ladder (level 1 → 2 → 3 → 4 → disconnect) is the correct mechanism
   * for handling sustained slow consumers — it eventually disconnects the session, after which the
   * client reconnects and replays from its last-acknowledged seqNo. Best-effort frames retain the
   * pre-existing {@code isWritable()} skip in {@link #writeBestEffortToAllChannels} because
   * dropping a best-effort frame is by definition acceptable.
   */
  private void writeReliableToSession(
      final WebSocketSession session,
      final byte[] bytes,
      final int length,
      final int templateId,
      final Channel ch) {
    final long seqNo = session.nextReliableSeqNo();
    final var tracker = session.reliableStreamTracker();
    final var buf =
        ch.alloc()
            .buffer(
                FrameParser.RELIABLE_HEADER_SIZE + length,
                FrameParser.RELIABLE_HEADER_SIZE + length);
    // Agent B R3 F-3: catch+rethrow instead of mutable `boolean captured` + `boolean
    // written` flags (CLAUDE.md §Local Variable Style — try-finally guard flags fall
    // outside the carve-out). The single catch arm always evicts when `tracker != null`:
    // the original two-branch logic (`captured && tracker != null` vs `tracker != null`)
    // collapsed to the same body because `evict()` is idempotent on miss, so a single
    // unconditional evict on the failure path preserves the same semantics. Netty
    // takes ownership of `buf` on a successful `ch.write`; release only on exception.
    try {
      FrameParser.encodeReliable(buf, seqNo, bytes, 0, length);
      if (tracker != null) {
        tracker.capture(seqNo, templateId, bytes, 0, length);
      }
      ch.write(new BinaryWebSocketFrame(buf));
    } catch (final Throwable t) {
      if (tracker != null) {
        // evict() is idempotent on miss — safe whether capture() ran or threw.
        tracker.evict(seqNo);
      }
      buf.release();
      throw t;
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

    // Hoist per-message invariants out of the session loop (same as reliable path).
    final int bestEffortEventBit = SubscriptionFilter.templateIdToEventBit(templateId);
    final boolean bestEffortHasAccount =
        bestEffortEventBit >= 0
            && AccountExtractor.extractPackedAccount(
                templateId, bytes, 0, length, packedAccountBuf);

    // Best-effort: shared ByteBuf allocated before the session loop (retainedDuplicate per
    // session). Using DEFAULT allocator because the buffer is not channel-specific — it is
    // shared across all channels via retainedDuplicate(). Channel allocators would be
    // arbitrary since any active session's allocator would work equivalently.
    final var frameBuf = PooledByteBufAllocator.DEFAULT.buffer(frameSize, frameSize);
    try {
      FrameParser.encodeBestEffort(frameBuf, bytes, 0, length);

      for (final var session : sessionManager.sessions()) {
        final var filter = session.subscriptionFilter();
        if (filter == null) {
          continue;
        }

        final var ch = session.channel();
        if (!ch.isActive()) {
          continue;
        }
        if (!ch.isWritable()) {
          // Acceptable drop site: best-effort frames have no replay contract, so dropping when
          // the channel is back-pressured is correct (the SlowConsumerHandler ladder will
          // disconnect persistent slow consumers separately). Increment the metric so dashboards
          // can detect sustained back-pressure on best-effort flow.
          metrics.egressDroppedChannelNotWritable();
          continue;
        }
        // SlowConsumerHandler may have flagged this session for best-effort drop at level 2.
        if (session.isDropBestEffort()) {
          metrics.messageDropped();
          continue;
        }

        if (bestEffortEventBit >= 0) {
          if (!filter.matches(templateId, bytes, 0, length)) {
            metrics.filterFiltered();
            continue;
          }

          if (bestEffortHasAccount
              && !session.isEntitledAccount(packedAccountBuf[0], packedAccountBuf[1])) {
            metrics.filterFiltered();
            continue;
          }
        }

        metrics.filterMatched();
        final var dup = frameBuf.retainedDuplicate();
        try {
          ch.write(new BinaryWebSocketFrame(dup));
        } catch (final Exception writeEx) {
          dup.release(); // prevent leak if write throws
        }
      }
    } catch (final Exception e) {
      // Pass the throwable so the stack trace lands in the log — without it the encoder failure
      // (truncated buffer, malformed templateId, etc.) reaches ops as a one-line "failed" with
      // zero diagnostic surface (Agent B review F-2).
      LOG.warn("Failed to encode best-effort frame for templateId={}", templateId, e);
    } finally {
      frameBuf.release();
    }
  }

  /**
   * Route an ack back-channel entry to the originating session. The entry's payload is a
   * pre-encoded {@code CommandAck} SBE message; we wrap it in a reliable envelope (capturing it in
   * the tracker) and write it to that session only.
   *
   * <p><b>Stale-epoch guard.</b> If the entry was captured with a specific session epoch (i.e.
   * {@code epoch != EPOCH_ANY}) and that epoch no longer matches the session's current epoch
   * (because the session was {@code resume()}d after enqueue), the entry is dropped with the {@code
   * egress.dropped.stale-epoch} counter incremented. Prevents the resumed session from receiving an
   * ack intended for a prior session epoch's command.
   *
   * <p><b>Pool-release contract.</b> The {@code entry} is NOT released to {@code commandEntryPool}
   * inside this method on any of its early-return branches. Release is the responsibility of the
   * SOLE caller — the {@code drain()} loop wraps every invocation in {@code try { ... } finally {
   * commandEntryPool.release(entry); }} (see {@link #drain()} lines around the ack-queue poll).
   * This ensures correct release on every return path including exceptions thrown by {@link
   * #writeReliableToSession}. Gemini cloud-review R1 flagged a potential leak here — the finding
   * was a false positive because Gemini read this method in isolation; this comment documents the
   * contract so future reviewers don't re-trip on the same issue.
   */
  private void writeAckToTargetChannel(final EgressEntry entry) {
    final var sessionId = new UUID(entry.sessionIdMsb(), entry.sessionIdLsb());
    final var session = sessionManager.findById(sessionId);
    if (session == null) {
      return; // session disappeared — drop the ack silently (caller's finally releases entry)
    }
    final long capturedEpoch = entry.sessionEpoch();
    if (capturedEpoch != EgressEntry.EPOCH_ANY && capturedEpoch != session.currentEpoch()) {
      metrics.egressDroppedStaleEpoch();
      return; // stale epoch — caller's finally releases entry
    }
    final var ch = session.channel();
    if (!ch.isActive()) {
      return; // channel closed — caller's finally releases entry
    }
    writeReliableToSession(session, entry.bytes(), entry.length(), entry.templateId(), ch);
    metrics.filterMatched();
  }
}
