package com.trading.engine.fixbridge.transport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.trading.engine.fixbridge.json.BrowserEvent;
import com.trading.engine.fixbridge.quote.SessionId;
import com.trading.engine.fixbridge.ratelimit.PerTypeRateLimiter;
import com.trading.engine.websocket.JwtValidator.ValidatedClaims;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link BridgeSession}.
 *
 * <p>Verifies constructor null-checks, field accessors, {@code hasAuditViewRole}, and the {@code
 * enqueue} delegation path.
 *
 * <p><b>Threading.</b> Single-threaded — all Netty event-loop invariants trivially hold.
 *
 * <p><b>Allocation.</b> Test-only — allocation is acceptable.
 */
final class BridgeSessionTest {

  private static final String AUDIT_ROLE = "audit_view";

  private SessionId sessionId;
  private ValidatedClaims claimsWithRole;
  private ValidatedClaims claimsWithoutRole;
  private ValidatedClaims claimsIpNotPinned;
  private InetAddress loopback;
  private OutboundQueue queue;
  private PerTypeRateLimiter limiter;

  @BeforeEach
  void setUp() throws UnknownHostException {
    sessionId = new SessionId("test-session-001");
    claimsWithRole =
        new ValidatedClaims(
            "user-1",
            "jti-1",
            List.of("ACME-001"),
            9999999999L,
            true,
            List.of(AUDIT_ROLE, "trader"));
    claimsWithoutRole =
        new ValidatedClaims(
            "user-2", "jti-2", List.of("ACME-001"), 9999999999L, true, List.of("trader"));
    claimsIpNotPinned =
        new ValidatedClaims("user-3", "jti-3", List.of("ACME-001"), 9999999999L, false, List.of());
    loopback = InetAddress.getLoopbackAddress();
    queue = new OutboundQueue(16);
    limiter = new PerTypeRateLimiter(0L);
  }

  // --- Constructor null-checks ---

  @Test
  void constructor_nullSessionId_throws() {
    assertThrows(
        NullPointerException.class,
        () -> new BridgeSession(null, claimsWithRole, loopback, queue, limiter));
  }

  @Test
  void constructor_nullClaims_throws() {
    assertThrows(
        NullPointerException.class,
        () -> new BridgeSession(sessionId, null, loopback, queue, limiter));
  }

  @Test
  void constructor_nullPinnedAddress_throws() {
    assertThrows(
        NullPointerException.class,
        () -> new BridgeSession(sessionId, claimsWithRole, null, queue, limiter));
  }

  @Test
  void constructor_nullOutboundQueue_throws() {
    assertThrows(
        NullPointerException.class,
        () -> new BridgeSession(sessionId, claimsWithRole, loopback, null, limiter));
  }

  @Test
  void constructor_nullRateLimiter_throws() {
    assertThrows(
        NullPointerException.class,
        () -> new BridgeSession(sessionId, claimsWithRole, loopback, queue, null));
  }

  // --- Accessors ---

  @Test
  void sessionId_returnsInjectedValue() {
    final var session = new BridgeSession(sessionId, claimsWithRole, loopback, queue, limiter);
    assertSame(sessionId, session.sessionId());
  }

  @Test
  void claims_returnsInjectedValue() {
    final var session = new BridgeSession(sessionId, claimsWithRole, loopback, queue, limiter);
    assertSame(claimsWithRole, session.claims());
  }

  @Test
  void pinnedRemoteAddress_returnsInjectedValue() throws UnknownHostException {
    final var addr = InetAddress.getByName("192.168.1.1");
    final var session = new BridgeSession(sessionId, claimsWithRole, addr, queue, limiter);
    assertSame(addr, session.pinnedRemoteAddress());
  }

  @Test
  void outboundQueue_returnsInjectedValue() {
    final var session = new BridgeSession(sessionId, claimsWithRole, loopback, queue, limiter);
    assertSame(queue, session.outboundQueue());
  }

  @Test
  void perTypeRateLimiter_returnsInjectedValue() {
    final var session = new BridgeSession(sessionId, claimsWithRole, loopback, queue, limiter);
    assertSame(limiter, session.perTypeRateLimiter());
  }

  // --- hasAuditViewRole ---

  @Test
  void hasAuditViewRole_rolePresent_returnsTrue() {
    final var session = new BridgeSession(sessionId, claimsWithRole, loopback, queue, limiter);
    assertTrue(session.hasAuditViewRole(AUDIT_ROLE));
  }

  @Test
  void hasAuditViewRole_roleAbsent_returnsFalse() {
    final var session = new BridgeSession(sessionId, claimsWithoutRole, loopback, queue, limiter);
    assertFalse(session.hasAuditViewRole(AUDIT_ROLE));
  }

  @Test
  void hasAuditViewRole_emptyRolesList_returnsFalse() {
    final var session = new BridgeSession(sessionId, claimsIpNotPinned, loopback, queue, limiter);
    assertFalse(session.hasAuditViewRole(AUDIT_ROLE));
  }

  @Test
  void hasAuditViewRole_nullRole_returnsFalse() {
    final var session = new BridgeSession(sessionId, claimsWithRole, loopback, queue, limiter);
    assertFalse(session.hasAuditViewRole(null));
  }

  @Test
  void hasAuditViewRole_differentRole_returnsFalse() {
    final var session = new BridgeSession(sessionId, claimsWithRole, loopback, queue, limiter);
    assertFalse(session.hasAuditViewRole("admin"));
  }

  // --- enqueue delegation ---

  @Test
  void enqueue_delegatesToOutboundQueueAndReturnsResult() {
    final var session = new BridgeSession(sessionId, claimsWithRole, loopback, queue, limiter);
    final var event = new BrowserEvent.Error("test-error");

    final var result = session.enqueue(event);

    assertEquals(OutboundQueue.OfferResult.ACCEPTED, result);
    assertEquals(1, queue.size());
    assertSame(event, queue.poll());
  }

  @Test
  void enqueue_fullQueueWithRawFix_returnsDROPPED() {
    final var session = new BridgeSession(sessionId, claimsWithRole, loopback, queue, limiter);
    // Fill queue with RawFix entries up to capacity.
    for (int i = 0; i < 16; i++) {
      session.enqueue(new BrowserEvent.RawFix("in", "fix-" + i));
    }
    // The next offer should displace the oldest RawFix.
    final var result = session.enqueue(new BrowserEvent.Error("overflow"));
    assertEquals(OutboundQueue.OfferResult.ACCEPTED_DROPPED_RAWFIX, result);
  }

  @Test
  void claims_ipPinnedReflectsClaimsRecord() {
    final var sessionPinned =
        new BridgeSession(sessionId, claimsWithRole, loopback, queue, limiter);
    assertTrue(sessionPinned.claims().ipPinned());

    final var sessionNotPinned =
        new BridgeSession(sessionId, claimsIpNotPinned, loopback, queue, limiter);
    assertFalse(sessionNotPinned.claims().ipPinned());
  }
}
