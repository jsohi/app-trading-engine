package com.trading.engine.websocket;

import com.trading.engine.messages.sbe.MessageHeaderEncoder;
import com.trading.engine.messages.sbe.WebSocketErrorCode;
import com.trading.engine.messages.sbe.WebSocketErrorEncoder;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.EpochNanoClock;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Mid-session JWT expiry sweeper — periodically scans active sessions, comparing the JWT {@code
 * exp} (stored in nano-precision on {@link WebSocketSession#expEpochNanos()}) against the injected
 * {@link EpochNanoClock}. Emits a soft {@code AuthExpiringSoon} warning frame (template 67, SBE
 * enum value 18) when the session enters the {@link #WARN_LEAD_NANOS} window ahead of hard expiry,
 * and closes the channel when hard expiry is reached.
 *
 * <p>Plan §Commit 8 — JWT expiry mid-session:
 *
 * <ul>
 *   <li><b>Nano-precision comparison.</b> Never truncating integer division to epoch-seconds (would
 *       otherwise create a worst-case 999 ms window of accepted expired tokens — CME iLink / EBS
 *       Direct fail closed at sub-millisecond precision). The conversion from RFC 7519 {@code exp}
 *       (epoch-seconds) to epoch-nanos happens once at auth time in {@link
 *       JwtAuthHandler#continueAuthOnEventLoop} as {@code expSec * 1_000_000_000L}; this sweeper
 *       reads the pre-converted nano value.
 *   <li><b>Warn once.</b> Once the warning is emitted for a session, a volatile latch ({@link
 *       WebSocketSession#expiringWarningSent}) prevents subsequent ticks within the window from
 *       spamming the client. Hot-spot FX / EBS pattern: warn once.
 *   <li><b>Closes on expiry.</b> When {@code nowNanos >= expEpochNanos}, the channel is closed bare
 *       (no close-status frame — matches the existing slow-consumer disconnect discipline; the
 *       client observes disconnect and re-auths on reconnect through the very first {@code
 *       WebSocketAuth} frame, same code path as a cold connect).
 * </ul>
 *
 * <p><b>Cadence.</b> Piggybacks on the drain handler's 1 ms scheduled task in {@link
 * WebSocketServerMain#start()}. The sweeper carries an internal {@link #SCAN_INTERVAL_NANOS} guard
 * so the actual session-iteration only fires roughly once per second — the 1 ms outer loop's no-op
 * overhead is one volatile read + one comparison per cycle.
 *
 * <p><b>Threading.</b> Single-threaded — {@link #scan(long)} runs on the drain worker event loop.
 * Per-session fields ({@code expEpochNanos}, {@code expiringWarningSent}) are {@code volatile} so
 * visibility is maintained for the channel's own event loop that wrote them at auth time.
 *
 * <p><b>Allocation.</b> Pre-allocated {@link ExpandableArrayBuffer} + {@link MessageHeaderEncoder}
 * + {@link WebSocketErrorEncoder} for the rare warning-frame encode. Zero allocation on the no-op
 * fast path (either the cadence-guard early-return, or the "no session past warn boundary"
 * iteration).
 *
 * @see JwtAuthHandler#continueAuthOnEventLoop
 * @see WebSocketSession#expEpochNanos(long)
 */
public final class JwtExpirySweeper {

  private static final Logger LOG = LogManager.getLogger(JwtExpirySweeper.class);

  /**
   * Soft-expiry warning lead time — {@code 60} seconds in nanoseconds. When {@code nowNanos >=
   * expEpochNanos - WARN_LEAD_NANOS} the sweeper emits one {@code AuthExpiringSoon} warning frame
   * and latches {@link WebSocketSession#expiringWarningSent} so subsequent ticks within the window
   * stay quiet. The client's {@link com.trading.engine.websocket.JwtAuthHandler}-installed reauth
   * handler then triggers an in-session reauth before the hard expiry boundary (Plan §Commit 8).
   */
  public static final long WARN_LEAD_NANOS = TimeUnit.SECONDS.toNanos(60L);

  /**
   * Internal cadence guard — the sweeper only iterates sessions once every {@code 1} second despite
   * being called from the drain handler's 1 ms task. Keeps the per-tick overhead negligible (one
   * volatile read + one comparison per outer-loop invocation) while preserving sub-second
   * responsiveness on the actual sweep.
   */
  public static final long SCAN_INTERVAL_NANOS = TimeUnit.SECONDS.toNanos(1L);

  private final WebSocketSessionManager sessionManager;
  private final WebSocketMetrics metrics;
  private final EpochNanoClock epochNanoClock;

  // Pre-allocated encode-side fields. The sweeper is a singleton (one instance per server
  // process), so a single buffer + encoder pair is safe — the sweeper runs single-threaded
  // on the drain worker event loop.
  private final ExpandableArrayBuffer responseBuf = new ExpandableArrayBuffer(64);
  private final MessageHeaderEncoder responseHeaderEncoder = new MessageHeaderEncoder();
  private final WebSocketErrorEncoder errorEncoder = new WebSocketErrorEncoder();

  /**
   * Last sweep time in epoch-nanos; updated at the start of each iteration that passes the cadence
   * guard. Initialised to {@code 0L} so the first call always iterates. Not {@code volatile} — the
   * sweeper is single-threaded on the drain worker event loop.
   */
  private long lastScanEpochNanos;

  /**
   * Create a sweeper.
   *
   * @param sessionManager session registry to iterate during {@link #scan(long)}
   * @param metrics metrics instance for the expired + emitted counters
   * @param epochNanoClock epoch-nanos clock; compared against {@link
   *     WebSocketSession#expEpochNanos()} on each tick. {@code EpochNanoClock} (not a monotonic
   *     {@code NanoClock}) because the JWT {@code exp} claim is wall-clock by RFC 7519 definition —
   *     comparison would be meaningless across a monotonic source that does not track epoch.
   */
  public JwtExpirySweeper(
      final WebSocketSessionManager sessionManager,
      final WebSocketMetrics metrics,
      final EpochNanoClock epochNanoClock) {
    this.sessionManager = Objects.requireNonNull(sessionManager, "sessionManager");
    this.metrics = Objects.requireNonNull(metrics, "metrics");
    this.epochNanoClock = Objects.requireNonNull(epochNanoClock, "epochNanoClock");
  }

  /**
   * Run one sweeper tick. Called from the drain handler's scheduled task in {@link
   * WebSocketServerMain#start()} after {@code drainHandler.drain()} and {@code
   * slowConsumerHandler.scan()}.
   *
   * <p>Cadence-guarded: returns immediately unless at least {@link #SCAN_INTERVAL_NANOS} have
   * elapsed since the previous full iteration. The {@code nowEpochNanos} argument is sampled from
   * the injected {@link EpochNanoClock} once per outer call.
   *
   * <p>Idempotent — safe to call repeatedly even within a single second; the cadence guard makes
   * the in-window invocations a no-op.
   */
  public void scan() {
    final long nowEpochNanos = epochNanoClock.nanoTime();
    if (nowEpochNanos - lastScanEpochNanos < SCAN_INTERVAL_NANOS) {
      return;
    }
    lastScanEpochNanos = nowEpochNanos;

    for (final var session : sessionManager.sessions()) {
      final long expEpochNanos = session.expEpochNanos();
      if (expEpochNanos == 0L) {
        // Pre-auth session — no expiry to enforce yet.
        continue;
      }
      final var ch = session.channel();
      if (!ch.isActive()) {
        continue;
      }
      if (nowEpochNanos >= expEpochNanos) {
        // Hard expiry — close the channel. The client observes the disconnect and
        // re-auths on reconnect through the standard cold-auth path.
        metrics.authSessionExpired();
        LOG.warn(
            "JWT expired mid-session — closing: sessionId={} expEpochNanos={}",
            session.sessionId(),
            expEpochNanos);
        ch.close();
        continue;
      }
      if (nowEpochNanos >= expEpochNanos - WARN_LEAD_NANOS && !session.expiringWarningSent()) {
        // First crossing of the soft-expiry window — emit one AuthExpiringSoon and latch.
        sendAuthExpiringSoonWarning(session);
        session.markExpiringWarningSent();
      }
    }
  }

  /**
   * Encode + write a single {@code WebSocketError(AuthExpiringSoon)} frame to the session's
   * channel. Cold path — called at most once per session per token. Catch+rethrow protects against
   * Netty buffer leaks on the failure path (matches the JwtAuthHandler / WebSocketFrameDispatcher
   * idiom).
   */
  private void sendAuthExpiringSoonWarning(final WebSocketSession session) {
    final var ch = session.channel();
    final var errorText = ErrorTextRegistry.textFor(WebSocketErrorCode.AuthExpiringSoon);
    errorEncoder.wrapAndApplyHeader(responseBuf, 0, responseHeaderEncoder);
    errorEncoder.errorCode(WebSocketErrorCode.AuthExpiringSoon);
    errorEncoder.putErrorText(errorText, 0, errorText.length);
    final int encodedLen = MessageHeaderEncoder.ENCODED_LENGTH + errorEncoder.encodedLength();
    final var nettyBuf = ch.alloc().buffer(encodedLen);
    try {
      nettyBuf.writeBytes(responseBuf.byteArray(), 0, encodedLen);
      ch.writeAndFlush(new BinaryWebSocketFrame(nettyBuf));
      metrics.authExpiringSoonEmitted();
      LOG.info(
          "AuthExpiringSoon emitted: sessionId={} expEpochNanos={}",
          session.sessionId(),
          session.expEpochNanos());
    } catch (final Throwable t) {
      nettyBuf.release();
      throw t;
    }
  }
}
