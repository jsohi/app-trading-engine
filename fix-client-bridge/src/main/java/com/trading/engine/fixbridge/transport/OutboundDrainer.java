package com.trading.engine.fixbridge.transport;

import com.trading.engine.fixbridge.json.BrowserEvent;
import com.trading.engine.fixbridge.json.BrowserEventWriter;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import java.util.Objects;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.agrona.concurrent.NanoClock;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Per-channel outbound drain loop (§3.1).
 *
 * <p>Schedules a fixed-delay task on the channel's event loop that drains the per-session {@link
 * OutboundQueue} into the WebSocket: each event is serialised by {@link
 * BrowserEventWriter#writeAny} into a fresh {@link TextWebSocketFrame} payload and written to the
 * channel. After each drain pass, the {@link InboundReadGate#onAfterDrain} hook is invoked so
 * backpressure can latch the channel back to {@code autoRead=true} once the queue has receded.
 *
 * <p><b>Stall escalation (§3.1 step 6).</b> If the queue depth stays at or above {@link
 * #STALL_PERCENT}% of capacity for {@link #STALL_TIMEOUT_NANOS} nanoseconds (30 s), the drainer
 * enqueues a fatal {@link BrowserEvent.BridgeStatus} ({@code reason="outbound-stall"}) and closes
 * the channel.
 *
 * <p><b>Threading.</b> Per-channel; runs on the channel's event loop. NOT thread-safe.
 *
 * <p><b>Allocation.</b> Per-frame allocation: one {@link TextWebSocketFrame} (Netty-pooled {@link
 * ByteBuf} backing). The {@link BrowserEventWriter} is per-instance and zero-alloc on the write
 * path. Stall-state is two primitive longs.
 *
 * <p><b>Lifecycle.</b> Started by {@code BridgeNettyBootstrap.ChannelInitializer} after the channel
 * becomes active and the auth handler has minted the session. Cancelled on {@code channelInactive}.
 */
public final class OutboundDrainer {

  private static final Logger LOG = LogManager.getLogger(OutboundDrainer.class);

  /** Drain task period (matches the {@code :websocket-server} drain handler cadence). */
  static final long DRAIN_PERIOD_MS = 1L;

  /** Queue-depth threshold above which the stall-timeout starts ticking. */
  static final int STALL_PERCENT = 90;

  /** Sustained-overflow window before the drainer emits a fatal close. */
  static final long STALL_TIMEOUT_NANOS = 30L * 1_000_000_000L;

  /** Maximum events drained per pass — caps event-loop time per tick on a flooded queue. */
  static final int DRAIN_BATCH_LIMIT = 64;

  private final ChannelHandlerContext ctx;
  private final BridgeSession session;
  private final InboundReadGate readGate;
  private final BrowserEventWriter writer;
  private final NanoClock nanoClock;
  private final int stallAtSize;

  /**
   * Monotonic timestamp at which the queue first crossed {@link #stallAtSize}, or {@link
   * Long#MIN_VALUE} when not in stall-watch state. Reset to {@link Long#MIN_VALUE} on every drain
   * pass that brings the queue below the stall threshold.
   */
  private long stallSinceNs = Long.MIN_VALUE;

  /** Marker so a stall-fatal close fires exactly once per drainer instance. */
  private boolean stallTerminated;

  /** Handle to the scheduled drain task, used by {@link #stop()} to cancel cleanly. */
  private ScheduledFuture<?> drainTask;

  /**
   * Construct a per-channel drainer.
   *
   * @param ctx the channel context (drain task scheduled on its event loop)
   * @param session the per-channel session record
   * @param readGate the per-channel inbound read gate (drain hook callback)
   * @param writer the per-handler outbound JSON writer (zero-alloc on serialisation)
   * @param nanoClock monotonic clock used for stall detection
   */
  public OutboundDrainer(
      final ChannelHandlerContext ctx,
      final BridgeSession session,
      final InboundReadGate readGate,
      final BrowserEventWriter writer,
      final NanoClock nanoClock) {
    this.ctx = Objects.requireNonNull(ctx, "ctx");
    this.session = Objects.requireNonNull(session, "session");
    this.readGate = Objects.requireNonNull(readGate, "readGate");
    this.writer = Objects.requireNonNull(writer, "writer");
    this.nanoClock = Objects.requireNonNull(nanoClock, "nanoClock");
    final int cap = session.outboundQueue().capacity();
    this.stallAtSize = Math.max(1, (int) ((long) cap * STALL_PERCENT / 100L));
  }

  /** Schedule the drain loop on the channel's event loop. Idempotent. */
  public void start() {
    if (drainTask != null) {
      return;
    }
    drainTask =
        ctx.executor()
            .scheduleWithFixedDelay(this::safeDrain, 0L, DRAIN_PERIOD_MS, TimeUnit.MILLISECONDS);
  }

  /** Cancel the drain loop. Safe to call from {@code channelInactive}. */
  public void stop() {
    if (drainTask != null && !drainTask.isCancelled()) {
      drainTask.cancel(false);
    }
  }

  /**
   * One drain pass — invoked on every {@link #DRAIN_PERIOD_MS} tick. Public-package for direct test
   * invocation; production code MUST use {@link #start()} which schedules this on the event loop.
   */
  void runOnce() {
    if (stallTerminated || !ctx.channel().isActive()) {
      return;
    }
    final var queue = session.outboundQueue();
    final var ch = ctx.channel();

    int drained = 0;
    while (drained < DRAIN_BATCH_LIMIT && ch.isWritable()) {
      final BrowserEvent event = queue.poll();
      if (event == null) {
        break;
      }
      final ByteBuf buf = ctx.alloc().buffer();
      try {
        writer.writeAny(event, buf);
        ctx.writeAndFlush(new TextWebSocketFrame(buf));
      } catch (final RuntimeException ex) {
        // Writer rejected the event (e.g. forbidden character in a String field). The buffer
        // hasn't been wrapped in a frame yet, so release it ourselves to avoid a leak. The event
        // is dropped on the floor; logging is best-effort because the audit path is upstream.
        buf.release();
        LOG.error(
            "OutboundDrainer: writer rejected event for session={}, dropping",
            session.sessionId(),
            ex);
      }
      drained++;
    }

    updateStallState(queue.size(), drained);
    readGate.onAfterDrain(ctx);
  }

  /**
   * Update the stall-watch state machine. Called once per drain pass.
   *
   * <ul>
   *   <li>Queue >= {@link #stallAtSize} and not yet watching → start the watch.
   *   <li>Queue >= {@link #stallAtSize} and watching for {@link #STALL_TIMEOUT_NANOS} → escalate.
   *   <li>Queue &lt; {@link #stallAtSize} and watching → cancel the watch.
   * </ul>
   *
   * @param queueSize current outbound-queue depth (after the drain pass)
   * @param drainedThisPass count drained on this pass — informational only, kept for future metrics
   *     emission (APP-40b)
   */
  private void updateStallState(final int queueSize, final int drainedThisPass) {
    final long nowNs = nanoClock.nanoTime();
    if (queueSize < stallAtSize) {
      stallSinceNs = Long.MIN_VALUE;
      return;
    }
    if (stallSinceNs == Long.MIN_VALUE) {
      stallSinceNs = nowNs;
      return;
    }
    if (nowNs - stallSinceNs >= STALL_TIMEOUT_NANOS) {
      escalateStall();
    }
  }

  /**
   * Emit a fatal {@link BrowserEvent.BridgeStatus} and close the channel. Fires exactly once per
   * drainer instance — repeated invocations during the close handshake are no-ops.
   */
  private void escalateStall() {
    if (stallTerminated) {
      return;
    }
    stallTerminated = true;
    LOG.warn(
        "OutboundDrainer: outbound-stall fatal close for session={} (queue size={} >= stall threshold {})",
        session.sessionId(),
        session.outboundQueue().size(),
        stallAtSize);
    final var fatal =
        new BrowserEvent.BridgeStatus(true, true, "outbound-stall", false, false, 1, 0L);
    final ByteBuf buf = ctx.alloc().buffer();
    try {
      writer.writeBridgeStatus(fatal, buf);
      ctx.writeAndFlush(new TextWebSocketFrame(buf))
          .addListener(
              future -> {
                final var close =
                    new CloseWebSocketFrame(BridgeCloseCodes.POLICY_VIOLATION, "outbound-stall");
                ctx.writeAndFlush(close).addListener(closeFuture -> ctx.close());
              });
    } catch (final RuntimeException ex) {
      buf.release();
      LOG.error(
          "OutboundDrainer: stall-fatal write failed for session={}, forcing close",
          session.sessionId(),
          ex);
      ctx.close();
    }
  }

  /** Wraps {@link #runOnce} with the same defensive try/catch pattern as the WS server drainer. */
  private void safeDrain() {
    try {
      runOnce();
    } catch (final Exception e) {
      LOG.error(
          "OutboundDrainer: drain pass exception for session={} — task continues",
          session.sessionId(),
          e);
    }
  }

  // --- Test-visible state -------------------------------------------------------------------

  /** Visible for testing — true once the stall-fatal close has fired. */
  boolean isStallTerminated() {
    return stallTerminated;
  }

  /** Visible for testing — the absolute size threshold for the stall watch. */
  int stallAtSize() {
    return stallAtSize;
  }
}
