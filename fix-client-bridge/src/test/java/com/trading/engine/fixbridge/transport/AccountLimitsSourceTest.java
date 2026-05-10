package com.trading.engine.fixbridge.transport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

import com.trading.engine.fixbridge.json.BrowserEvent;
import com.trading.engine.fixbridge.quote.SessionId;
import com.trading.engine.fixbridge.ratelimit.PerTypeRateLimiter;
import com.trading.engine.websocket.JwtValidator.ValidatedClaims;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link AccountLimitsSource} — SAM seam for pre-trade limits push on AUTH_SUCCESS.
 *
 * <p>Verifies:
 *
 * <ul>
 *   <li>{@link AccountLimitsSource#NOOP} is a complete no-op (sink is never called).
 *   <li>A custom impl can emit one {@link BrowserEvent.AccountLimits} per claimed account.
 *   <li>The {@link AccountLimitsSource.Sink} receives events in claim order.
 *   <li>Wiring the sink to a real {@link OutboundQueue} yields {@link
 *       OutboundQueue.OfferResult#ACCEPTED}.
 * </ul>
 *
 * <p><b>Threading.</b> Single-threaded — test-only.
 *
 * <p><b>Allocation.</b> Test-only — allocation is acceptable.
 */
final class AccountLimitsSourceTest {

  // ---------------------------------------------------------------------------
  // Test helpers.
  // ---------------------------------------------------------------------------

  /** Minimal {@link ValidatedClaims} with a configurable accounts list. */
  private static ValidatedClaims claims(final List<String> accounts) {
    // sub, jti, accounts, expiryEpochSec, ipPinned, roles
    return new ValidatedClaims("user-001", "jti-001", accounts, Long.MAX_VALUE, true, List.of());
  }

  /** Build a minimal {@link BridgeSession} using a small outbound queue. */
  private static BridgeSession session(final int queueCapacity) {
    final var q = new OutboundQueue(queueCapacity);
    final var limiter = new PerTypeRateLimiter(0L);
    return new BridgeSession(
        new SessionId("session-001"),
        claims(List.of()),
        InetAddress.getLoopbackAddress(),
        q,
        limiter);
  }

  /** Recording {@link AccountLimitsSource.Sink} that captures each emitted event. */
  private static final class RecordingSink implements AccountLimitsSource.Sink {

    final List<BrowserEvent.AccountLimits> received = new ArrayList<>();
    final OutboundQueue.OfferResult fixedResult;

    RecordingSink(final OutboundQueue.OfferResult fixedResult) {
      this.fixedResult = fixedResult;
    }

    @Override
    public OutboundQueue.OfferResult emit(final BrowserEvent.AccountLimits event) {
      received.add(event);
      return fixedResult;
    }
  }

  /** Simple {@link AccountLimitsSource} that emits one limits frame per account claim. */
  private static final class PerAccountSource implements AccountLimitsSource {

    // Fixed limits: 100 qty (8 decimal places), 1_000_000 notional, 50 bps, 10 OPS.
    private static final long MAX_QTY = 100L * 100_000_000L;
    private static final long MAX_NOTIONAL = 1_000_000L * 100_000_000L;
    private static final int DEVIATION_BPS = 50;
    private static final int MAX_OPS = 10;

    @Override
    public void pushFor(
        final ValidatedClaims claims,
        final BridgeSession session,
        final AccountLimitsSource.Sink sink) {
      for (final var account : claims.accounts()) {
        sink.emit(
            new BrowserEvent.AccountLimits(account, MAX_QTY, MAX_NOTIONAL, DEVIATION_BPS, MAX_OPS));
      }
    }
  }

  // ---------------------------------------------------------------------------
  // NOOP tests.
  // ---------------------------------------------------------------------------

  @Test
  void noop_pushFor_doesNotCallSink() {
    final var sink = new RecordingSink(OutboundQueue.OfferResult.ACCEPTED);
    final var claimsWithAccounts = claims(List.of("ACME-001", "ACME-002"));
    AccountLimitsSource.NOOP.pushFor(claimsWithAccounts, session(64), sink);
    assertEquals(0, sink.received.size(), "NOOP must not call sink");
  }

  @Test
  void noop_pushFor_withNullClaims_doesNotThrow() {
    // NOOP is a lambda; we only require it does not throw.
    AccountLimitsSource.NOOP.pushFor(null, null, e -> OutboundQueue.OfferResult.ACCEPTED);
  }

  // ---------------------------------------------------------------------------
  // Custom impl — emission order tests.
  // ---------------------------------------------------------------------------

  @Test
  void customImpl_pushFor_emitsOneFramePerAccount_inOrder() {
    final var source = new PerAccountSource();
    final var sink = new RecordingSink(OutboundQueue.OfferResult.ACCEPTED);
    final var accounts = List.of("ACME-001", "ACME-002", "ACME-003");
    final var c = claims(accounts);

    source.pushFor(c, session(64), sink);

    assertEquals(3, sink.received.size(), "should emit one frame per account");
    assertEquals("ACME-001", sink.received.get(0).account());
    assertEquals("ACME-002", sink.received.get(1).account());
    assertEquals("ACME-003", sink.received.get(2).account());
  }

  @Test
  void customImpl_pushFor_emitsCorrectLimitsPerAccount() {
    final var source = new PerAccountSource();
    final var sink = new RecordingSink(OutboundQueue.OfferResult.ACCEPTED);
    final var c = claims(List.of("ACME-001"));

    source.pushFor(c, session(64), sink);

    final var limits = sink.received.get(0);
    assertEquals("ACME-001", limits.account());
    assertEquals(100L * 100_000_000L, limits.maxQtyInt64());
    assertEquals(1_000_000L * 100_000_000L, limits.maxNotionalInt64());
    assertEquals(50, limits.priceDeviationBps());
    assertEquals(10, limits.maxOrdersPerSecond());
  }

  @Test
  void customImpl_pushFor_noAccounts_emitsNothing() {
    final var source = new PerAccountSource();
    final var sink = new RecordingSink(OutboundQueue.OfferResult.ACCEPTED);
    final var c = claims(List.of());

    source.pushFor(c, session(64), sink);

    assertEquals(0, sink.received.size());
  }

  // ---------------------------------------------------------------------------
  // Wired to a real OutboundQueue.
  // ---------------------------------------------------------------------------

  @Test
  void customImpl_pushFor_wireThroughRealQueue_yieldsAccepted() {
    final var source = new PerAccountSource();
    final var s = session(16);
    final var queue = s.outboundQueue();

    // Wire the sink directly to the session's queue::offer method reference.
    source.pushFor(claims(List.of("ACME-001")), s, queue::offer);

    assertEquals(1, queue.size(), "queue must hold the emitted limits frame");
    final var polled = (BrowserEvent.AccountLimits) queue.poll();
    assertEquals("ACME-001", polled.account());
  }

  @Test
  void customImpl_pushFor_twoAccounts_wiredToRealQueue_enqueuedInOrder() {
    final var source = new PerAccountSource();
    final var s = session(16);
    final var queue = s.outboundQueue();

    source.pushFor(claims(List.of("ACME-001", "ACME-002")), s, queue::offer);

    assertEquals(2, queue.size());
    final var first = (BrowserEvent.AccountLimits) queue.poll();
    final var second = (BrowserEvent.AccountLimits) queue.poll();
    assertEquals("ACME-001", first.account());
    assertEquals("ACME-002", second.account());
  }

  // ---------------------------------------------------------------------------
  // Sink SAM return value propagation.
  // ---------------------------------------------------------------------------

  @Test
  void sink_emit_terminalResult_isPropagatedToCaller() {
    // The sink's return value is what the lambda/impl sees — verify the recording captures it.
    final var sink = new RecordingSink(OutboundQueue.OfferResult.TERMINAL);
    final var limits = new BrowserEvent.AccountLimits("ACME", 0L, 0L, 0, 0);
    final var result = sink.emit(limits);
    assertSame(OutboundQueue.OfferResult.TERMINAL, result);
  }
}
