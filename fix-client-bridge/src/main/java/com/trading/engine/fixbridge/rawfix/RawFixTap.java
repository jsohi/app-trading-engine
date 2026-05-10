package com.trading.engine.fixbridge.rawfix;

import com.trading.engine.fixbridge.audit.AuditAction;
import com.trading.engine.fixbridge.audit.AuditLogger;
import com.trading.engine.fixbridge.json.BrowserEvent;
import com.trading.engine.fixbridge.transport.BridgeSession;
import com.trading.engine.fixbridge.transport.OutboundQueue;
import java.util.Objects;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Per-session FIX-tap (§3.5).
 *
 * <p>Receives raw FIX messages from the Artio session attached to one bridge session and emits a
 * {@link BrowserEvent.RawFix} on the per-session outbound queue, but only when:
 *
 * <ul>
 *   <li>{@code bridgeDebug} is currently {@code true} (volatile runtime toggle), AND
 *   <li>The session's JWT carries the configured audit-view role (from {@link
 *       com.trading.engine.fixbridge.FixClientBridgeConfig#auditViewRole()}).
 * </ul>
 *
 * <p>If either gate is closed the tap is a silent no-op.
 *
 * <p>If both gates are open the FIX bytes are first masked through {@link PiiMask}
 * (byte-length-preserving redaction of GDPR/FINRA/MiFID II PII tags) and then admitted through
 * {@link RawFixRateLimiter} (1000 frames/s/session). Drops are silent at the limiter; this class
 * surfaces the drop reason via the supplied {@link DropCounter} hook so the caller can update a
 * metrics counter without coupling this class to a metrics framework.
 *
 * <p><b>Direction.</b> Encoded as a single ASCII byte ({@code 'i'} or {@code 'o'}) on the public
 * API. The constants {@link #DIRECTION_IN} / {@link #DIRECTION_OUT} are for caller convenience.
 *
 * <p><b>Threading.</b> NOT thread-safe. Owned by the bridge's per-session Netty handler. The Artio
 * session must marshal its inbound/outbound callbacks onto the bridge channel's event loop before
 * invoking the tap (this is the responsibility of the Artio session integration that lands in
 * subsequent days; the tap itself does no marshalling).
 *
 * <p><b>Allocation.</b> One {@link BrowserEvent.RawFixSlice} record per emitted frame (unavoidable
 * since the queue holds boxed events). No {@link String} is allocated — the slice references the
 * per-instance {@code maskScratch} buffer directly. SOH→{@code '|'} substitution and JSON-escape
 * are deferred to {@link com.trading.engine.fixbridge.json.BrowserEventWriter#writeRawFixSlice} at
 * write time, which reads the byte slice without constructing an intermediate {@link String}.
 */
public final class RawFixTap {

  private static final Logger LOG = LogManager.getLogger(RawFixTap.class);

  /** Direction byte for an inbound FIX message (gateway → bridge → browser-tap). */
  public static final byte DIRECTION_IN = (byte) 'i';

  /** Direction byte for an outbound FIX message (bridge → gateway). */
  public static final byte DIRECTION_OUT = (byte) 'o';

  /**
   * Caller-provided counter hook — invoked once per drop with a stable {@link DropReason}. Lets the
   * integration code wire a Micrometer counter without coupling this class to Micrometer.
   */
  @FunctionalInterface
  public interface DropCounter {
    /**
     * Increment the {@code fixbridge_rawfix_dropped_total{session,reason}} counter (or its adapter
     * equivalent).
     *
     * @param session the session whose tap dropped a frame
     * @param reason why the drop occurred
     */
    void incrementDrop(BridgeSession session, DropReason reason);

    /** No-op counter for tests / bring-up before metrics wiring lands. */
    DropCounter NOOP = (session, reason) -> {};
  }

  /** Stable reason codes so the metric carries a useful low-cardinality label. */
  public enum DropReason {
    /** Tap is currently disabled (bridgeDebug=false or audit_view role missing). */
    DISABLED,
    /** Rate limiter rejected the frame (1000 fps/session token bucket exhausted). */
    RATE_LIMIT,
    /** Outbound queue overflowed and the priority drop policy bumped this RawFix. */
    OUTBOUND_QUEUE_FULL
  }

  private final BridgeSession session;
  private final PiiMask piiMask;
  private final RawFixRateLimiter rateLimiter;
  private final AuditLogger auditLogger;
  private final DropCounter dropCounter;
  private final String auditViewRole;

  /**
   * {@code volatile} so the bridge's reload endpoint can flip the flag from any thread and the
   * Artio session-loop sees the change without lock acquisition. Reading a volatile boolean on the
   * hot path is essentially free on x86.
   */
  private volatile boolean bridgeDebug;

  /**
   * Reusable mask scratch — sized to the largest expected FIX message (4 KiB covers every realistic
   * FIX 4.4 message, including the longest SecurityList responses). If the input exceeds this the
   * tap drops the frame with {@link DropReason#OUTBOUND_QUEUE_FULL} (re-using the most appropriate
   * existing reason code; a future {@code FRAME_TOO_LARGE} reason can split this once observability
   * shows it matters).
   */
  private final byte[] maskScratch = new byte[4096];

  /**
   * Construct the tap.
   *
   * @param session the bridge session this tap is bound to (one tap per session)
   * @param piiMask mask configuration (typically {@link PiiMask#withDefaultMask()})
   * @param rateLimiter per-session token-bucket limiter
   * @param auditLogger audit sink (BRIDGE_DEBUG_TOGGLE entries on each {@link
   *     #setBridgeDebug(boolean, long)})
   * @param dropCounter metrics hook for drops
   * @param auditViewRole role identifier required in the session's JWT before frames are emitted
   * @param initialBridgeDebug initial value of the {@code bridgeDebug} flag (typically from {@code
   *     FixClientBridgeConfig.bridgeDebug()})
   */
  public RawFixTap(
      final BridgeSession session,
      final PiiMask piiMask,
      final RawFixRateLimiter rateLimiter,
      final AuditLogger auditLogger,
      final DropCounter dropCounter,
      final String auditViewRole,
      final boolean initialBridgeDebug) {
    this.session = Objects.requireNonNull(session, "session");
    this.piiMask = Objects.requireNonNull(piiMask, "piiMask");
    this.rateLimiter = Objects.requireNonNull(rateLimiter, "rateLimiter");
    this.auditLogger = Objects.requireNonNull(auditLogger, "auditLogger");
    this.dropCounter = Objects.requireNonNull(dropCounter, "dropCounter");
    this.auditViewRole = Objects.requireNonNull(auditViewRole, "auditViewRole");
    this.bridgeDebug = initialBridgeDebug;
  }

  /**
   * Toggle the {@code bridgeDebug} flag at runtime. Every transition records a {@link
   * AuditAction#BRIDGE_DEBUG_TOGGLE} entry so the audit trail captures every operator action that
   * could expose tap output (per §3.5 audit-log requirement).
   *
   * @param newValue desired flag state
   * @param nowNs monotonic timestamp (audit log clock source)
   */
  public void setBridgeDebug(final boolean newValue, final long nowNs) {
    final boolean prior = this.bridgeDebug;
    if (prior == newValue) {
      return;
    }
    this.bridgeDebug = newValue;
    if (auditLogger.isWritable()) {
      auditLogger.record(
          nowNs,
          session.claims().sub(),
          session.claims().jti(),
          null,
          AuditAction.BRIDGE_DEBUG_TOGGLE,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          newValue ? "enabled" : "disabled",
          null,
          null);
    }
    LOG.info("bridgeDebug toggled {} -> {} for session={}", prior, newValue, session.sessionId());
  }

  /**
   * @return current value of the {@code bridgeDebug} flag
   */
  public boolean isBridgeDebugEnabled() {
    return bridgeDebug;
  }

  /**
   * Tap a FIX message. Caller supplies the SOH-delimited bytes verbatim (no masking, no SOH
   * substitution); this method applies {@link PiiMask}, the rate limiter, and finally enqueues a
   * {@link BrowserEvent.RawFix} on the session's outbound queue.
   *
   * @param direction {@link #DIRECTION_IN} or {@link #DIRECTION_OUT}
   * @param fixBytes source byte array
   * @param off start offset within {@code fixBytes}
   * @param len byte length to tap
   * @param nowNs monotonic clock for the rate limiter (typically {@code nanoClock.nanoTime()})
   */
  public void tap(
      final byte direction, final byte[] fixBytes, final int off, final int len, final long nowNs) {
    Objects.requireNonNull(fixBytes, "fixBytes");
    if (direction != DIRECTION_IN && direction != DIRECTION_OUT) {
      throw new IllegalArgumentException("direction must be 'i' or 'o', got: " + (char) direction);
    }
    if (off < 0 || len < 0 || off + len > fixBytes.length) {
      throw new IndexOutOfBoundsException(
          "slice out of bounds: off=" + off + " len=" + len + " arrayLen=" + fixBytes.length);
    }

    // Gate 1: bridgeDebug
    if (!bridgeDebug) {
      dropCounter.incrementDrop(session, DropReason.DISABLED);
      return;
    }
    // Gate 2: audit_view role
    if (!session.hasAuditViewRole(auditViewRole)) {
      dropCounter.incrementDrop(session, DropReason.DISABLED);
      return;
    }
    // Gate 3: scratch capacity
    if (len > maskScratch.length) {
      dropCounter.incrementDrop(session, DropReason.OUTBOUND_QUEUE_FULL);
      return;
    }
    // Gate 4: rate limiter
    if (!rateLimiter.tryConsume(nowNs)) {
      dropCounter.incrementDrop(session, DropReason.RATE_LIMIT);
      return;
    }

    // Apply PII mask in-place into the per-tap scratch buffer. PiiMask preserves byte length, so
    // the masked slice length equals the source length. The RawFixSlice carrier holds a reference
    // to maskScratch directly — the per-frame String allocation is eliminated (APP-40a Day 5).
    final int maskedLen = piiMask.mask(fixBytes, off, len, maskScratch, 0);
    // RawFixSlice shares maskScratch without copying — the queue consumer (BrowserEventWriter) MUST
    // serialise this record before the next tap() call mutates the buffer. This is guaranteed
    // because both the drainer and the tap run on the same channel event loop (single-threaded).
    final var event =
        new BrowserEvent.RawFixSlice(direction == DIRECTION_IN, maskScratch, 0, maskedLen);

    final var result = session.outboundQueue().offer(event);
    if (result == OutboundQueue.OfferResult.TERMINAL) {
      // Should be impossible because RawFix events are themselves the priority-drop target — the
      // queue can never report TERMINAL while there is a RawFix in residence. Defensive: surface
      // the drop and let the caller escalate to a fatal BridgeStatus elsewhere.
      dropCounter.incrementDrop(session, DropReason.OUTBOUND_QUEUE_FULL);
      LOG.error(
          "RawFixTap: terminal outbound overflow for session={} (no RawFix to drop)",
          session.sessionId());
    }
  }
}
