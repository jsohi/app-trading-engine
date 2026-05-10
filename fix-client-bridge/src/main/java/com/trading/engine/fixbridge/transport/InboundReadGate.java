package com.trading.engine.fixbridge.transport;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import java.util.Objects;

/**
 * Inbound auto-read backpressure gate (§3.1 step 4).
 *
 * <p>Pauses TCP-level reads on the channel when the per-session {@link OutboundQueue} crosses the
 * <b>pause</b> high-water mark (>80% of capacity by default), and resumes reads once the queue
 * drains back below the <b>resume</b> low-water mark (<50% of capacity by default). The hysteresis
 * band prevents thrashing under bursty load.
 *
 * <p>The gate observes queue depth at two well-defined points:
 *
 * <ul>
 *   <li>{@link #onAfterInboundDispatch(ChannelHandlerContext)} — invoked by {@code WsListener}
 *       <i>after</i> it processes one inbound frame; this is the upward-edge check that must latch
 *       the pause if processing the frame produced new outbound work.
 *   <li>{@link #onAfterDrain(ChannelHandlerContext)} — invoked by the outbound flusher each time it
 *       drains one or more events; this is the downward-edge check that lifts the pause once the
 *       queue has receded.
 * </ul>
 *
 * <p>The handler holds no queue depth itself — depth is read from {@link OutboundQueue#size()} on
 * each check so there is no cache to invalidate. Auto-read state is owned by Netty ({@code
 * channel.config().setAutoRead}); the gate caches the last value it set so it only flips the
 * channel option on actual transitions.
 *
 * <p><b>Threading.</b> Per-channel instance, NOT {@code @Sharable}. All checks happen on the
 * channel's event loop.
 *
 * <p><b>Allocation.</b> Zero post-construction.
 */
public final class InboundReadGate extends ChannelInboundHandlerAdapter {

  /**
   * Default pause high-water as a percentage of {@link OutboundQueue#capacity()}. {@code 80} per
   * §3.1 step 4.
   */
  public static final int DEFAULT_PAUSE_PERCENT = 80;

  /**
   * Default resume low-water as a percentage of {@link OutboundQueue#capacity()}. {@code 50} per
   * §3.1 step 4. The gap between pause and resume is the hysteresis band.
   */
  public static final int DEFAULT_RESUME_PERCENT = 50;

  private final OutboundQueue queue;
  private final int pauseAtSize;
  private final int resumeAtSize;

  /**
   * Tracks the gate's last-applied auto-read state. Initialised to {@code true} to match Netty's
   * default. Mutating this field is restricted to the channel event loop (per {@link
   * ChannelInboundHandlerAdapter} contract).
   */
  private boolean autoReadEnabled = true;

  /**
   * Construct a gate with the {@link #DEFAULT_PAUSE_PERCENT} / {@link #DEFAULT_RESUME_PERCENT}
   * thresholds.
   *
   * @param queue the per-session outbound queue whose depth drives the gate
   */
  public InboundReadGate(final OutboundQueue queue) {
    this(queue, DEFAULT_PAUSE_PERCENT, DEFAULT_RESUME_PERCENT);
  }

  /**
   * Construct a gate with custom thresholds.
   *
   * @param queue the per-session outbound queue
   * @param pausePercent pause high-water as a percent of capacity (must be {@code 1..100})
   * @param resumePercent resume low-water as a percent of capacity (must be {@code 0..pausePercent
   *     - 1} so the hysteresis band is non-empty)
   * @throws IllegalArgumentException if either threshold is out of range or the band is empty
   */
  public InboundReadGate(
      final OutboundQueue queue, final int pausePercent, final int resumePercent) {
    Objects.requireNonNull(queue, "queue");
    if (pausePercent <= 0 || pausePercent > 100) {
      throw new IllegalArgumentException("pausePercent out of range 1..100: " + pausePercent);
    }
    if (resumePercent < 0 || resumePercent >= pausePercent) {
      throw new IllegalArgumentException(
          "resumePercent must satisfy 0 <= resumePercent < pausePercent, got "
              + resumePercent
              + " vs "
              + pausePercent);
    }
    this.queue = queue;
    final int cap = queue.capacity();
    // Round up so the pause threshold for tiny capacities (e.g. test fixtures of 16 entries)
    // isn't silently zero'd by integer division.
    this.pauseAtSize = Math.max(1, (int) ((long) cap * pausePercent / 100L));
    this.resumeAtSize = (int) ((long) cap * resumePercent / 100L);
  }

  /**
   * Hook invoked by {@code WsListener} after it processes one inbound frame. Must be called on the
   * channel event loop. If the queue depth has crossed the pause high-water mark, autoRead is
   * latched OFF so the next TCP-level read is suppressed.
   *
   * @param ctx the channel context (auto-read flipped via {@code ctx.channel().config()})
   */
  public void onAfterInboundDispatch(final ChannelHandlerContext ctx) {
    if (autoReadEnabled && queue.size() >= pauseAtSize) {
      autoReadEnabled = false;
      ctx.channel().config().setAutoRead(false);
    }
  }

  /**
   * Hook invoked by the outbound flusher after each drain pass. Must be called on the channel event
   * loop. If the queue depth has receded below the resume low-water mark, autoRead is re-enabled
   * and a single read is issued so a paused channel doesn't sit idle waiting for the next TCP
   * packet.
   *
   * @param ctx the channel context
   */
  public void onAfterDrain(final ChannelHandlerContext ctx) {
    if (!autoReadEnabled && queue.size() <= resumeAtSize) {
      autoReadEnabled = true;
      ctx.channel().config().setAutoRead(true);
      // After re-enabling autoRead, a single explicit read kicks the loop in the case where the
      // channel has buffered data ready but no new packet has arrived yet. Without this nudge the
      // gate could deadlock on a fully drained queue with the peer waiting for a server response.
      ctx.read();
    }
  }

  /**
   * @return {@code true} iff this gate's last-applied auto-read state is enabled
   */
  public boolean isAutoReadEnabled() {
    return autoReadEnabled;
  }

  /**
   * @return the absolute size threshold at which the gate pauses inbound reads
   */
  public int pauseAtSize() {
    return pauseAtSize;
  }

  /**
   * @return the absolute size threshold at which the gate resumes inbound reads
   */
  public int resumeAtSize() {
    return resumeAtSize;
  }
}
