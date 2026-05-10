package com.trading.engine.fixbridge.rawfix;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.trading.engine.fixbridge.audit.AuditAction;
import com.trading.engine.fixbridge.audit.AuditLogger;
import com.trading.engine.fixbridge.json.BrowserEvent;
import com.trading.engine.fixbridge.quote.SessionId;
import com.trading.engine.fixbridge.ratelimit.PerTypeRateLimiter;
import com.trading.engine.fixbridge.transport.BridgeSession;
import com.trading.engine.fixbridge.transport.OutboundQueue;
import com.trading.engine.websocket.JwtValidator.ValidatedClaims;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link RawFixTap} — the per-session FIX-tap (§3.5).
 *
 * <p>Verifies the double-gate (bridgeDebug + audit_view role), PII masking, rate limiting, outbound
 * queue overflow, audit logging on toggle, and direction validation.
 *
 * <p><b>Threading.</b> Single-threaded — RawFixTap is not thread-safe; test isolation holds.
 *
 * <p><b>Allocation.</b> Test-only.
 */
final class RawFixTapTest {

  private static final String AUDIT_ROLE = "audit_view";

  // A minimal FIX 4.4 NewOrderSingle message with SOH→| substitution.
  // Tag 1 = Account (PII → masked); tag 11 = ClOrdID (not masked); tag 55 = Symbol (not masked).
  private static final byte[] SAMPLE_FIX =
      "8=FIX.4.4|35=D|49=BRIDGE|56=EXCH|1=SECRET|11=ORD-001|55=EUR/USD|"
          .getBytes(java.nio.charset.StandardCharsets.US_ASCII);

  // ─── Recording AuditLogger ────────────────────────────────────────────────

  private static final class RecordingAuditLogger implements AuditLogger {
    final List<AuditAction> actions = new ArrayList<>();

    @Override
    public void record(
        final long tsNs,
        final String userId,
        final String jti,
        final String sourceIp,
        final AuditAction action,
        final String symbol,
        final String side,
        final String qtyStr,
        final String priceStr,
        final String ordType,
        final String tif,
        final String account,
        final String clOrdId,
        final String origClOrdId,
        final String quoteId,
        final String result,
        final String failureReason,
        final String traceparent) {
      actions.add(action);
    }

    @Override
    public boolean isWritable() {
      return true;
    }
  }

  // ─── Recording DropCounter ────────────────────────────────────────────────

  private static final class RecordingDropCounter implements RawFixTap.DropCounter {
    final List<RawFixTap.DropReason> reasons = new ArrayList<>();

    @Override
    public void incrementDrop(final BridgeSession session, final RawFixTap.DropReason reason) {
      reasons.add(reason);
    }
  }

  // ─── Shared helpers ───────────────────────────────────────────────────────

  private RecordingAuditLogger auditLogger;
  private RecordingDropCounter dropCounter;

  @BeforeEach
  void setUp() {
    auditLogger = new RecordingAuditLogger();
    dropCounter = new RecordingDropCounter();
  }

  /** Build a BridgeSession with specific role list. */
  private static BridgeSession buildSession(final List<String> roles, final OutboundQueue queue)
      throws Exception {
    final var claims =
        new ValidatedClaims("user-1", "jti-1", List.of("ACME-001"), 9999999999L, true, roles);
    final var addr = InetAddress.getLoopbackAddress();
    return new BridgeSession(
        new SessionId("sess-1"), claims, addr, queue, new PerTypeRateLimiter(0L));
  }

  /** Build a fully-open RawFixRateLimiter that always admits. */
  private static RawFixRateLimiter fullBucket() {
    return new RawFixRateLimiter(0L);
  }

  /** Build an exhausted RawFixRateLimiter that always rejects. */
  private static RawFixRateLimiter exhaustedBucket() {
    // Burst of 1, refill negligible — consume the token first.
    final var limiter = new RawFixRateLimiter(1L, 0.000001, 0L);
    limiter.tryConsume(0L); // consume the single token
    return limiter;
  }

  // ─── Gate 1: bridgeDebug=false → DISABLED ────────────────────────────────

  @Test
  void tap_bridgeDebugFalse_rolePresent_dropsWithDISABLED() throws Exception {
    final var queue = new OutboundQueue(64);
    final var session = buildSession(List.of(AUDIT_ROLE), queue);
    final var tap =
        new RawFixTap(
            session,
            PiiMask.withDefaultMask(),
            fullBucket(),
            auditLogger,
            dropCounter,
            AUDIT_ROLE,
            false /* bridgeDebug */);

    tap.tap(RawFixTap.DIRECTION_IN, SAMPLE_FIX, 0, SAMPLE_FIX.length, 1_000_000L);

    assertEquals(0, queue.size(), "No event must be enqueued when bridgeDebug=false");
    assertEquals(1, dropCounter.reasons.size());
    assertEquals(RawFixTap.DropReason.DISABLED, dropCounter.reasons.get(0));
  }

  // ─── Gate 2: bridgeDebug=true, role missing → DISABLED ──────────────────

  @Test
  void tap_bridgeDebugTrue_roleMissing_dropsWithDISABLED() throws Exception {
    final var queue = new OutboundQueue(64);
    final var session = buildSession(List.of("trader" /* no audit_view */), queue);
    final var tap =
        new RawFixTap(
            session,
            PiiMask.withDefaultMask(),
            fullBucket(),
            auditLogger,
            dropCounter,
            AUDIT_ROLE,
            true /* bridgeDebug */);

    tap.tap(RawFixTap.DIRECTION_OUT, SAMPLE_FIX, 0, SAMPLE_FIX.length, 1_000_000L);

    assertEquals(0, queue.size());
    assertEquals(1, dropCounter.reasons.size());
    assertEquals(RawFixTap.DropReason.DISABLED, dropCounter.reasons.get(0));
  }

  // ─── Gate 4: rate limiter exhausted → RATE_LIMIT ─────────────────────────

  @Test
  void tap_bridgeDebugTrue_rolePresent_rateLimitExhausted_dropsWithRATE_LIMIT() throws Exception {
    final var queue = new OutboundQueue(64);
    final var session = buildSession(List.of(AUDIT_ROLE), queue);
    final var tap =
        new RawFixTap(
            session,
            PiiMask.withDefaultMask(),
            exhaustedBucket(),
            auditLogger,
            dropCounter,
            AUDIT_ROLE,
            true);

    tap.tap(RawFixTap.DIRECTION_IN, SAMPLE_FIX, 0, SAMPLE_FIX.length, 1_000_000L);

    assertEquals(0, queue.size());
    assertEquals(1, dropCounter.reasons.size());
    assertEquals(RawFixTap.DropReason.RATE_LIMIT, dropCounter.reasons.get(0));
  }

  // ─── Happy path: event enqueued with direction + masked content ───────────

  @Test
  void tap_allGatesOpen_directionIn_enqueuesRawFixWithMaskedContent() throws Exception {
    final var queue = new OutboundQueue(64);
    final var session = buildSession(List.of(AUDIT_ROLE), queue);
    final var tap =
        new RawFixTap(
            session,
            PiiMask.withDefaultMask(),
            fullBucket(),
            auditLogger,
            dropCounter,
            AUDIT_ROLE,
            true);

    tap.tap(RawFixTap.DIRECTION_IN, SAMPLE_FIX, 0, SAMPLE_FIX.length, 1_000_000L);

    assertEquals(1, queue.size());
    assertEquals(0, dropCounter.reasons.size());
    final var event = queue.poll();
    assertTrue(event instanceof BrowserEvent.RawFix, "Expected RawFix event");
    final var rawFix = (BrowserEvent.RawFix) event;
    assertEquals("in", rawFix.direction());
    // Account tag 1 value "SECRET" must be masked as "******".
    assertTrue(rawFix.fix().contains("1="), "Event must contain tag 1");
    assertTrue(rawFix.fix().contains("11=ORD-001"), "Non-PII tag 11 must be preserved");
    // Verify masking: the Account value bytes should all be '*'.
    assertTrue(rawFix.fix().contains("1=******"), "Account (tag 1) value must be masked");
  }

  @Test
  void tap_allGatesOpen_directionOut_enqueuesRawFixDirectionOut() throws Exception {
    final var queue = new OutboundQueue(64);
    final var session = buildSession(List.of(AUDIT_ROLE), queue);
    final var tap =
        new RawFixTap(
            session,
            PiiMask.withDefaultMask(),
            fullBucket(),
            auditLogger,
            dropCounter,
            AUDIT_ROLE,
            true);

    tap.tap(RawFixTap.DIRECTION_OUT, SAMPLE_FIX, 0, SAMPLE_FIX.length, 1_000_000L);

    final var rawFix = (BrowserEvent.RawFix) queue.poll();
    assertEquals("out", rawFix.direction());
  }

  // ─── Direction validation ─────────────────────────────────────────────────

  @Test
  void tap_invalidDirection_throwsIllegalArgument() throws Exception {
    final var queue = new OutboundQueue(64);
    final var session = buildSession(List.of(AUDIT_ROLE), queue);
    final var tap =
        new RawFixTap(
            session,
            PiiMask.withDefaultMask(),
            fullBucket(),
            auditLogger,
            dropCounter,
            AUDIT_ROLE,
            true);

    assertThrows(
        IllegalArgumentException.class,
        () -> tap.tap((byte) 'x', SAMPLE_FIX, 0, SAMPLE_FIX.length, 1_000_000L));
  }

  // ─── Bounds check ────────────────────────────────────────────────────────

  @Test
  void tap_negativeOffset_throwsIndexOutOfBounds() throws Exception {
    final var queue = new OutboundQueue(64);
    final var session = buildSession(List.of(AUDIT_ROLE), queue);
    final var tap =
        new RawFixTap(
            session,
            PiiMask.withDefaultMask(),
            fullBucket(),
            auditLogger,
            dropCounter,
            AUDIT_ROLE,
            true);

    assertThrows(
        IndexOutOfBoundsException.class,
        () -> tap.tap(RawFixTap.DIRECTION_IN, SAMPLE_FIX, -1, SAMPLE_FIX.length, 0L));
  }

  @Test
  void tap_oversizedLength_throwsIndexOutOfBounds() throws Exception {
    final var queue = new OutboundQueue(64);
    final var session = buildSession(List.of(AUDIT_ROLE), queue);
    final var tap =
        new RawFixTap(
            session,
            PiiMask.withDefaultMask(),
            fullBucket(),
            auditLogger,
            dropCounter,
            AUDIT_ROLE,
            true);

    assertThrows(
        IndexOutOfBoundsException.class,
        () -> tap.tap(RawFixTap.DIRECTION_IN, SAMPLE_FIX, 0, SAMPLE_FIX.length + 1, 0L));
  }

  // ─── Frame > 4096 → OUTBOUND_QUEUE_FULL ──────────────────────────────────

  @Test
  void tap_frameLargerThanMaskScratch_dropsWithOUTBOUND_QUEUE_FULL() throws Exception {
    final var queue = new OutboundQueue(64);
    final var session = buildSession(List.of(AUDIT_ROLE), queue);
    final var tap =
        new RawFixTap(
            session,
            PiiMask.withDefaultMask(),
            fullBucket(),
            auditLogger,
            dropCounter,
            AUDIT_ROLE,
            true);

    // maskScratch is 4096 bytes; supply exactly 4097.
    final var oversized = new byte[4097];
    java.util.Arrays.fill(oversized, (byte) 'A');

    tap.tap(RawFixTap.DIRECTION_IN, oversized, 0, 4097, 1_000_000L);

    assertEquals(0, queue.size());
    assertEquals(1, dropCounter.reasons.size());
    assertEquals(RawFixTap.DropReason.OUTBOUND_QUEUE_FULL, dropCounter.reasons.get(0));
  }

  // ─── setBridgeDebug auditing ──────────────────────────────────────────────

  @Test
  void setBridgeDebug_transitionsFalseToTrue_recordsAuditToggle() throws Exception {
    final var queue = new OutboundQueue(64);
    final var session = buildSession(List.of(AUDIT_ROLE), queue);
    final var tap =
        new RawFixTap(
            session,
            PiiMask.withDefaultMask(),
            fullBucket(),
            auditLogger,
            dropCounter,
            AUDIT_ROLE,
            false /* start disabled */);

    tap.setBridgeDebug(true, 100_000L);

    assertTrue(
        auditLogger.actions.contains(AuditAction.BRIDGE_DEBUG_TOGGLE),
        "Toggle must record BRIDGE_DEBUG_TOGGLE in audit");
    assertTrue(tap.isBridgeDebugEnabled());
  }

  @Test
  void setBridgeDebug_noTransition_doesNotAudit() throws Exception {
    final var queue = new OutboundQueue(64);
    final var session = buildSession(List.of(AUDIT_ROLE), queue);
    final var tap =
        new RawFixTap(
            session,
            PiiMask.withDefaultMask(),
            fullBucket(),
            auditLogger,
            dropCounter,
            AUDIT_ROLE,
            true /* start enabled */);

    tap.setBridgeDebug(true, 100_000L); // same value — no-op

    assertTrue(
        auditLogger.actions.isEmpty(),
        "No audit entry must be recorded when the flag does not change");
  }

  // ─── Outbound queue terminal overflow path ────────────────────────────────

  @Test
  void tap_terminalQueueOverflow_dropsWithOUTBOUND_QUEUE_FULL() throws Exception {
    // A queue of capacity 4 filled entirely with non-RawFix critical events.
    final var queue = new OutboundQueue(4);
    final var session = buildSession(List.of(AUDIT_ROLE), queue);

    // Fill with Error events (non-droppable — no RawFix to displace).
    for (int i = 0; i < 4; i++) {
      queue.offer(new BrowserEvent.Error("fill-" + i));
    }
    assertEquals(4, queue.size());

    final var tap =
        new RawFixTap(
            session,
            PiiMask.withDefaultMask(),
            fullBucket(),
            auditLogger,
            dropCounter,
            AUDIT_ROLE,
            true);

    tap.tap(RawFixTap.DIRECTION_IN, SAMPLE_FIX, 0, SAMPLE_FIX.length, 1_000_000L);

    // Queue is still 4 (TERMINAL — nothing was displaced).
    assertEquals(4, queue.size());
    assertEquals(1, dropCounter.reasons.size());
    assertEquals(RawFixTap.DropReason.OUTBOUND_QUEUE_FULL, dropCounter.reasons.get(0));
  }
}
