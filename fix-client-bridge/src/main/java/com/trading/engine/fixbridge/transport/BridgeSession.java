package com.trading.engine.fixbridge.transport;

import com.trading.engine.fixbridge.json.BrowserEvent;
import com.trading.engine.fixbridge.quote.SessionId;
import com.trading.engine.fixbridge.ratelimit.PerTypeRateLimiter;
import com.trading.engine.websocket.JwtValidator.ValidatedClaims;
import io.netty.util.AttributeKey;
import java.net.InetAddress;
import java.util.Objects;

/**
 * Per-channel state carried by the FIX client bridge for the lifetime of one authenticated
 * WebSocket session. Holds the validated JWT claims, minted {@link SessionId}, pinned remote IP
 * (when {@code ip_pinned=true}), per-session rate limiter, and the bounded {@link OutboundQueue}.
 *
 * <p><b>Threading.</b> NOT thread-safe. Owned exclusively by the channel's Netty event loop — every
 * read and mutation must occur on that loop. Cross-loop access is a programming error.
 *
 * <p><b>Allocation.</b> One instance per authenticated session, allocated by the auth handler on
 * success. Per-message hot paths read fields off the session without allocating.
 *
 * <p><b>Lifecycle.</b> Constructed by {@code JwtAuthHandler} after JWT verification; reachable via
 * {@link #ATTRIBUTE_KEY} on the channel; cleared on {@code channelInactive}.
 *
 * <p><b>Dependencies.</b> Pure data holder — no Netty pipeline calls.
 */
public final class BridgeSession {

  /**
   * Channel attribute key — handlers downstream of {@code JwtAuthHandler} look up the session via
   * {@code ctx.channel().attr(BridgeSession.ATTRIBUTE_KEY).get()}. The key name is namespaced to
   * avoid collisions with any third-party Netty handlers in the pipeline.
   */
  public static final AttributeKey<BridgeSession> ATTRIBUTE_KEY =
      AttributeKey.valueOf("com.trading.engine.fixbridge.BridgeSession");

  private final SessionId sessionId;
  private final ValidatedClaims claims;
  private final InetAddress pinnedRemoteAddress;
  private final OutboundQueue outboundQueue;
  private final PerTypeRateLimiter perTypeRateLimiter;

  /**
   * Construct a session record.
   *
   * @param sessionId minted session identifier (one per successful Auth)
   * @param claims validated JWT claims captured at auth time — {@code claims.ipPinned()} drives
   *     whether {@link #pinnedRemoteAddress} is consulted on subsequent frames
   * @param pinnedRemoteAddress remote IP captured at handshake (only enforced when {@code
   *     claims.ipPinned()} is {@code true}); never {@code null}
   * @param outboundQueue the per-session bounded outbound queue (§3.1)
   * @param perTypeRateLimiter the per-session command rate limiter pinned to the auth nano-time
   */
  public BridgeSession(
      final SessionId sessionId,
      final ValidatedClaims claims,
      final InetAddress pinnedRemoteAddress,
      final OutboundQueue outboundQueue,
      final PerTypeRateLimiter perTypeRateLimiter) {
    this.sessionId = Objects.requireNonNull(sessionId, "sessionId");
    this.claims = Objects.requireNonNull(claims, "claims");
    this.pinnedRemoteAddress = Objects.requireNonNull(pinnedRemoteAddress, "pinnedRemoteAddress");
    this.outboundQueue = Objects.requireNonNull(outboundQueue, "outboundQueue");
    this.perTypeRateLimiter = Objects.requireNonNull(perTypeRateLimiter, "perTypeRateLimiter");
  }

  /**
   * @return the minted session id
   */
  public SessionId sessionId() {
    return sessionId;
  }

  /**
   * @return the validated JWT claims captured at auth time
   */
  public ValidatedClaims claims() {
    return claims;
  }

  /**
   * @return the remote address captured at handshake — only enforced as a pinning constraint when
   *     {@code claims().ipPinned()} returns {@code true}
   */
  public InetAddress pinnedRemoteAddress() {
    return pinnedRemoteAddress;
  }

  /**
   * @return the bounded outbound queue for this session
   */
  public OutboundQueue outboundQueue() {
    return outboundQueue;
  }

  /**
   * @return the per-session command rate limiter
   */
  public PerTypeRateLimiter perTypeRateLimiter() {
    return perTypeRateLimiter;
  }

  /**
   * Convenience: check whether the validated claims include the configured audit-view role, gating
   * {@link com.trading.engine.fixbridge.rawfix.RawFixTap} emission.
   *
   * @param auditViewRole the role identifier from {@code FixClientBridgeConfig.auditViewRole()}
   * @return {@code true} iff the role appears in the JWT's {@code roles} claim
   */
  public boolean hasAuditViewRole(final String auditViewRole) {
    if (auditViewRole == null) {
      return false;
    }
    final var roles = claims.roles();
    for (int i = 0, n = roles.size(); i < n; i++) {
      if (auditViewRole.equals(roles.get(i))) {
        return true;
      }
    }
    return false;
  }

  /**
   * Convenience: enqueue the supplied event on the per-session outbound queue. Returns the same
   * {@link OutboundQueue.OfferResult} as the queue so the caller can react to overflow or
   * RawFix-drop bookkeeping.
   *
   * @param event event to enqueue
   * @return queue offer outcome
   */
  public OutboundQueue.OfferResult enqueue(final BrowserEvent event) {
    return outboundQueue.offer(event);
  }
}
