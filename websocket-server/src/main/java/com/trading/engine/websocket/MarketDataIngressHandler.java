package com.trading.engine.websocket;

import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import java.nio.ByteOrder;
import java.util.Objects;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.ManyToOneConcurrentArrayQueue;
import org.agrona.concurrent.NanoClock;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Fragment handler for the Aeron IPC market-data ingest stream ({@link
 * com.trading.engine.messages.MarketDataConstants#MARKET_DATA_STREAM_ID stream 204}).
 *
 * <p>Receives {@code MarketDataTick} (template 54) and {@code MarketDataHeartbeat} (template 55)
 * fragments published by the {@code pricing-service.MarketDataPublisher}, dispatches each fragment
 * to the {@link MarketDataSubscriptionLivenessTracker} for LIVE/QUIET/STALE accounting, and copies
 * the raw SBE bytes into a borrowed {@link EgressEntry} which is enqueued onto the shared egress
 * {@link ManyToOneConcurrentArrayQueue} for fan-out to subscribed WebSocket sessions by the drain
 * handler.
 *
 * <p><b>Threading.</b> Single-threaded — installed under a {@link io.aeron.FragmentAssembler} and
 * invoked only from the {@code AeronEgressThread}'s {@code Subscription.poll(...)} call. Same
 * thread that owns the {@link WebSocketEgressListener#borrowForMarketData() pool} — direct LIFO
 * borrow / release is safe.
 *
 * <p><b>Allocation.</b> Zero allocation on the hot path. All fields are final references bound at
 * construction; the {@code DirectBuffer.getBytes(...)} copy is into a pre-allocated entry's backing
 * array. Drop paths increment a metric counter and release the entry directly back to the LIFO pool
 * (no cross-thread return queue overhead).
 *
 * <p><b>Drop semantics.</b>
 *
 * <ul>
 *   <li><b>Unknown templateId</b> — increment {@link WebSocketMetrics#marketDataDropped()} with
 *       reason {@code unknown-template} and skip. Defensive: the pricing-service must only emit
 *       54/55 on this stream; a future schema extension that adds another template id would be a
 *       silent regression if we routed it without explicit allow-listing.
 *   <li><b>Pool exhaustion</b> — drain queue not keeping up with the publisher; increment the drop
 *       counter and skip. The next fragment may succeed once the drain handler frees entries.
 *   <li><b>Oversize fragment</b> — fragment exceeds the entry's backing array. SBE wire encoding is
 *       fixed-size per template (template 54 = 8+64 = 72 bytes, template 55 = 8+8+var); the entry
 *       capacity (typically equal to the cluster path's {@code replayBufferFrameSize}) is sized
 *       well above this. An oversize fragment indicates a schema mismatch and is dropped fail-loud
 *       with a {@code WARN} log + counter.
 *   <li><b>Queue full</b> — the egress queue is back-pressured by a slow Netty drain. The fragment
 *       is dropped, the entry is released directly to the pool, and the drop counter is
 *       incremented. The publisher's 5 ms cadence + conflation makes this acceptable for ticks; the
 *       liveness tracker may eventually transition to STALE if the back-pressure persists.
 * </ul>
 *
 * <p><b>Reliable vs best-effort routing.</b> Template 54 / 55 are best-effort ({@link
 * EgressEntry#isReliable()} returns false). The drain handler uses {@code retainedDuplicate()}
 * fan-out and skips slow-consumer sessions with {@code dropBestEffort}. Template 57
 * (MarketDataFeedStateChange) is NOT ingested here — it is synthesised by the {@link
 * MarketDataSubscriptionLivenessTracker}'s transition callback and enqueued directly by the caller.
 */
public final class MarketDataIngressHandler implements FragmentHandler {

  private static final Logger LOG = LogManager.getLogger(MarketDataIngressHandler.class);

  /** Template id for {@code MarketDataTick} — drives {@code onTickFragment}. */
  static final int TEMPLATE_TICK = 54;

  /** Template id for {@code MarketDataHeartbeat} — drives {@code onHeartbeatFragment}. */
  static final int TEMPLATE_HEARTBEAT = 55;

  private final ManyToOneConcurrentArrayQueue<EgressEntry> queue;
  private final WebSocketEgressListener egressListener;
  private final MarketDataSubscriptionLivenessTracker livenessTracker;
  private final WebSocketMetrics metrics;
  private final NanoClock nanoClock;

  /**
   * Construct a handler bound to the shared egress queue, the egress listener (for pool borrow /
   * release), and the liveness tracker (for fragment-arrival accounting).
   *
   * @param queue the shared {@link ManyToOneConcurrentArrayQueue} drained by the Netty event loop
   * @param egressListener owns the pre-allocated {@link EgressEntry} LIFO pool that this handler
   *     borrows from — must be the same instance wired into the cluster egress path so a single
   *     pool budget covers both ingest sources
   * @param livenessTracker receives {@code onTickFragment} / {@code onHeartbeatFragment} callbacks
   *     to drive the LIVE/QUIET/STALE transitions
   * @param metrics metrics instance for drop / counter accounting
   * @param nanoClock monotonic clock used to stamp {@code nowNanos} for the liveness tracker
   *     callbacks; injected for test-time determinism
   */
  public MarketDataIngressHandler(
      final ManyToOneConcurrentArrayQueue<EgressEntry> queue,
      final WebSocketEgressListener egressListener,
      final MarketDataSubscriptionLivenessTracker livenessTracker,
      final WebSocketMetrics metrics,
      final NanoClock nanoClock) {
    this.queue = Objects.requireNonNull(queue, "queue");
    this.egressListener = Objects.requireNonNull(egressListener, "egressListener");
    this.livenessTracker = Objects.requireNonNull(livenessTracker, "livenessTracker");
    this.metrics = Objects.requireNonNull(metrics, "metrics");
    this.nanoClock = Objects.requireNonNull(nanoClock, "nanoClock");
  }

  /**
   * Aeron fragment callback. Extracts the SBE templateId, dispatches to the liveness tracker,
   * borrows a pool entry, copies the raw bytes, and enqueues for fan-out. All branches honour
   * single-thread invariants and zero-alloc discipline.
   *
   * @param buffer Aeron {@link DirectBuffer} valid only for the duration of this callback
   * @param offset start of the SBE message header within {@code buffer}
   * @param length total length of the SBE message in bytes
   * @param header Aeron log header (unused — included for the {@link FragmentHandler} contract)
   */
  @Override
  public void onFragment(
      final DirectBuffer buffer, final int offset, final int length, final Header header) {

    // Template id is at HEADER offset + 2 (blockLength[2] || templateId[2], little-endian).
    final int templateId = extractTemplateId(buffer, offset);

    if (templateId != TEMPLATE_TICK && templateId != TEMPLATE_HEARTBEAT) {
      metrics.marketDataDropped();
      LOG.warn("Market-data fragment with unexpected templateId={} — dropped", templateId);
      return;
    }

    final long nowNanos = nanoClock.nanoTime();
    if (templateId == TEMPLATE_TICK) {
      livenessTracker.onTickFragment(nowNanos);
    } else {
      livenessTracker.onHeartbeatFragment(nowNanos);
    }

    final EgressEntry entry = egressListener.borrowForMarketData();
    if (entry == null) {
      metrics.marketDataDropped();
      LOG.warn("Market-data pool exhausted — fragment dropped (templateId={})", templateId);
      return;
    }

    if (length > entry.bytes().length) {
      egressListener.releaseDirectly(entry);
      metrics.marketDataDropped();
      LOG.warn(
          "Market-data fragment exceeds entry capacity ({} > {}) — dropped",
          length,
          entry.bytes().length);
      return;
    }

    buffer.getBytes(offset, entry.bytes(), 0, length);
    entry.setMetadata(length, templateId);

    if (!queue.offer(entry)) {
      egressListener.releaseDirectly(entry);
      metrics.marketDataDropped();
      LOG.warn("Egress queue full — market-data fragment dropped (templateId={})", templateId);
    }
  }

  // ────────────────────────────────────────────────────────────────────────
  // Helpers

  /**
   * Extract the SBE templateId from a {@link DirectBuffer} at the given message-header offset.
   * Mirrors {@link EgressEntry#extractTemplateId(byte[], int)} but operates on a {@code
   * DirectBuffer} (the Aeron fragment buffer) rather than a byte array.
   *
   * @param buffer the Aeron fragment buffer
   * @param offset the start of the SBE message header within {@code buffer}
   * @return the unsigned 16-bit templateId, little-endian
   */
  private static int extractTemplateId(final DirectBuffer buffer, final int offset) {
    // SBE header: blockLength[2] || templateId[2] || schemaId[2] || version[2], little-endian.
    return buffer.getShort(offset + 2, ByteOrder.LITTLE_ENDIAN) & 0xFFFF;
  }
}
