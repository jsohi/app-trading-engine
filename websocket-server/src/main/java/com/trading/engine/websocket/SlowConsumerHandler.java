package com.trading.engine.websocket;

import com.trading.engine.messages.sbe.MessageHeaderEncoder;
import com.trading.engine.messages.sbe.WebSocketErrorCode;
import com.trading.engine.messages.sbe.WebSocketErrorEncoder;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.NanoClock;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Graduated 4-level slow-consumer detector. Runs piggybacking on the 1ms drain cycle (no separate
 * scheduler). Reads {@link WebSocketSession#pendingBytes()} (the Netty outbound queue depth tally
 * maintained by {@link WriteByteCounterHandler}) and applies hysteresis-based action ladder:
 *
 * <ul>
 *   <li>0 – L1: clear, no action.
 *   <li>L1 – L2: counter increment + warn.
 *   <li>L2 – L3: counter increment + set {@code dropBestEffort=true} (drain handler honors this).
 *   <li>L3 – L4: counter increment + send {@code WebSocketError(SlowConsumer)} once on entry.
 *   <li>{@code &gt;} L4 sustained for {@code slowConsumerDisconnectMs}: counter + close channel.
 * </ul>
 *
 * <p><b>Replay exemption.</b> Levels 3 and 4 are suppressed while {@link
 * WebSocketSession#isReplayInProgress()} is true. A replay legitimately spikes pendingBytes;
 * disconnecting mid-replay would re-enter the failure mode on reconnect. Levels 1 and 2 still fire
 * (cheap + non-destructive). After replay completes the next cycle resumes full classification.
 *
 * <p><b>Hysteresis.</b> The handler tracks {@code lastLagLevel} and {@code levelEnteredNs} on the
 * session; it only fires actions when the level changes upward (L1→L2, etc.) or when level 4 dwell
 * exceeds the disconnect threshold. Downward transitions clear {@code dropBestEffort} and reset
 * {@code lastLagLevel}. This prevents flapping at threshold boundaries.
 *
 * <p><b>Threading.</b> Single-threaded — {@link #scan()} is invoked from the drain handler's worker
 * event loop after each drain pass. Per-session lag fields are volatile so visibility is maintained
 * across reads from the channel's own event loop.
 *
 * <p><b>Allocation.</b> Pre-allocated {@link ExpandableArrayBuffer} for the rare {@code
 * WebSocketError} encoding. Zero allocation on the no-op clear path.
 *
 * @see WriteByteCounterHandler
 * @see <a href="docs/websocket-architecture.md">WebSocket Architecture — Section 5</a>
 */
public final class SlowConsumerHandler {

  private static final Logger LOG = LogManager.getLogger(SlowConsumerHandler.class);

  private final WebSocketSessionManager sessionManager;
  private final WebSocketServerConfig config;
  private final WebSocketMetrics metrics;
  private final NanoClock nanoClock;
  private final long disconnectNs;
  private final ExpandableArrayBuffer responseBuf = new ExpandableArrayBuffer(64);

  /**
   * Create a slow-consumer handler.
   *
   * @param sessionManager session registry to iterate during {@link #scan}
   * @param config server config (level thresholds, disconnect timeout)
   * @param metrics metrics instance
   * @param nanoClock monotonic clock for level-4 dwell measurement
   */
  public SlowConsumerHandler(
      final WebSocketSessionManager sessionManager,
      final WebSocketServerConfig config,
      final WebSocketMetrics metrics,
      final NanoClock nanoClock) {
    this.sessionManager = Objects.requireNonNull(sessionManager, "sessionManager");
    this.config = Objects.requireNonNull(config, "config");
    this.metrics = Objects.requireNonNull(metrics, "metrics");
    this.nanoClock = Objects.requireNonNull(nanoClock, "nanoClock");
    this.disconnectNs = TimeUnit.MILLISECONDS.toNanos(config.slowConsumerDisconnectMs());
  }

  /**
   * Iterate all active sessions, classify pendingBytes against the level ladder, and apply
   * hysteresis-aware actions. Idempotent — safe to call repeatedly.
   */
  public void scan() {
    final long nowNs = nanoClock.nanoTime();
    final int level1 = config.slowConsumerLevel1Bytes();
    final int level2 = config.slowConsumerLevel2Bytes();
    final int level3 = config.slowConsumerLevel3Bytes();
    final int level4 = config.slowConsumerLevel4Bytes();

    long maxLag = 0L;
    for (final var session : sessionManager.sessions()) {
      final var ch = session.channel();
      if (!ch.isActive()) {
        continue;
      }
      final long pending = session.pendingBytes();
      if (pending > maxLag) {
        maxLag = pending;
      }
      final int newLevel = classify(pending, level1, level2, level3, level4);
      final int oldLevel = session.lastLagLevel();
      if (newLevel != oldLevel) {
        applyTransition(session, oldLevel, newLevel, nowNs);
      } else if (newLevel == 3
          && session.isSlowConsumerErrorPending()
          && !session.isReplayInProgress()) {
        // Replay-suppressed L3 error needs to fire now that replay has ended (Gemini round 2).
        sendSlowConsumerError(session);
        session.slowConsumerErrorPending(false);
      } else if (newLevel == 4) {
        // Sustained level 4 — disconnect after dwell timeout.
        final long enteredNs = session.levelEnteredNs();
        if (!session.isReplayInProgress() && nowNs - enteredNs >= disconnectNs) {
          metrics.slowConsumerDisconnect();
          LOG.warn(
              "Slow consumer disconnect: sessionId={} pendingBytes={}",
              session.sessionId(),
              pending);
          // Send a final error frame, then close the channel.
          sendSlowConsumerError(session);
          ch.close();
          // Reset level so a re-iteration before close completes doesn't re-fire.
          session.recordLagLevel(0, nowNs);
        }
      }
    }
    metrics.updateMaxClientLag(maxLag);
  }

  private static int classify(
      final long pending, final int l1, final int l2, final int l3, final int l4) {
    if (pending < l1) {
      return 0;
    }
    if (pending < l2) {
      return 1;
    }
    if (pending < l3) {
      return 2;
    }
    if (pending < l4) {
      return 3;
    }
    return 4;
  }

  private void applyTransition(
      final WebSocketSession session, final int oldLevel, final int newLevel, final long nowNs) {
    // Downward transition: clear dropBestEffort if returning below L2; clear pending L3 error
    // if returning below L3 (it's no longer applicable to the current state).
    if (newLevel < oldLevel) {
      if (newLevel < 2) {
        session.dropBestEffort(false);
      }
      if (newLevel < 3) {
        session.slowConsumerErrorPending(false);
      }
      session.recordLagLevel(newLevel, nowNs);
      return;
    }

    // Upward transitions: per-level action.
    switch (newLevel) {
      case 1 -> {
        metrics.slowConsumerLevel1();
        LOG.info(
            "Slow consumer L1 entered: sessionId={} pendingBytes={}",
            session.sessionId(),
            session.pendingBytes());
      }
      case 2 -> {
        metrics.slowConsumerLevel2();
        session.dropBestEffort(true);
        LOG.warn(
            "Slow consumer L2 entered (drop best-effort): sessionId={} pendingBytes={}",
            session.sessionId(),
            session.pendingBytes());
      }
      case 3 -> {
        metrics.slowConsumerLevel3();
        if (session.isReplayInProgress()) {
          // Replay legitimately spikes pendingBytes; defer the error until replay completes so
          // the resuming session is not flagged. The scan loop fires it on the next pass once
          // isReplayInProgress() flips false (Gemini PR #62 round 2).
          session.slowConsumerErrorPending(true);
        } else {
          sendSlowConsumerError(session);
        }
        LOG.warn(
            "Slow consumer L3 entered: sessionId={} pendingBytes={} replay={}",
            session.sessionId(),
            session.pendingBytes(),
            session.isReplayInProgress());
      }
      case 4 -> {
        metrics.slowConsumerLevel4();
        LOG.warn(
            "Slow consumer L4 entered: sessionId={} pendingBytes={} replay={}",
            session.sessionId(),
            session.pendingBytes(),
            session.isReplayInProgress());
      }
      default -> {}
    }
    session.recordLagLevel(newLevel, nowNs);
  }

  private void sendSlowConsumerError(final WebSocketSession session) {
    final var ch = session.channel();
    if (!ch.isActive()) {
      return;
    }
    final var errorText = ErrorTextRegistry.textFor(WebSocketErrorCode.SlowConsumer);
    final var enc = new WebSocketErrorEncoder();
    final var header = new MessageHeaderEncoder();
    enc.wrapAndApplyHeader(responseBuf, 0, header);
    enc.errorCode(WebSocketErrorCode.SlowConsumer);
    enc.putErrorText(errorText, 0, errorText.length);
    final int encodedLen = MessageHeaderEncoder.ENCODED_LENGTH + enc.encodedLength();
    final var nettyBuf = ch.alloc().buffer(encodedLen);
    boolean written = false;
    try {
      nettyBuf.writeBytes(responseBuf.byteArray(), 0, encodedLen);
      ch.writeAndFlush(new BinaryWebSocketFrame(nettyBuf));
      written = true;
    } finally {
      if (!written) {
        nettyBuf.release();
      }
    }
  }
}
