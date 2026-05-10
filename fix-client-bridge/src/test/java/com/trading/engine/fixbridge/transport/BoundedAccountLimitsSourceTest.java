package com.trading.engine.fixbridge.transport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.trading.engine.fixbridge.json.BrowserEvent;
import com.trading.engine.fixbridge.quote.SessionId;
import com.trading.engine.fixbridge.ratelimit.PerTypeRateLimiter;
import com.trading.engine.websocket.JwtValidator.ValidatedClaims;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link BoundedAccountLimitsSource} — production {@link AccountLimitsSource} impl.
 *
 * <p>Verifies:
 *
 * <ul>
 *   <li>Two accounts in claims, both resolved by provider → 2 frames emitted in order.
 *   <li>One account in claims, provider returns {@code null} → pessimistic-default frame emitted
 *       (all numeric limits are {@code 0}).
 *   <li>Zero accounts in claims → no frames emitted.
 *   <li>Provider throws → exception propagated without swallowing.
 *   <li>Constructor null-check on provider.
 * </ul>
 *
 * <p><b>Threading.</b> Single-threaded — test-only.
 *
 * <p><b>Allocation.</b> Test-only — allocation is acceptable.
 */
final class BoundedAccountLimitsSourceTest {

  // ---------------------------------------------------------------------------
  // Test doubles.
  // ---------------------------------------------------------------------------

  /** Recording {@link AccountLimitsSource.Sink} that captures every emitted event. */
  private static final class RecordingSink implements AccountLimitsSource.Sink {

    final List<BrowserEvent.AccountLimits> received = new ArrayList<>();

    @Override
    public OutboundQueue.OfferResult emit(final BrowserEvent.AccountLimits event) {
      received.add(event);
      return OutboundQueue.OfferResult.ACCEPTED;
    }
  }

  /** Provider that returns canned limits for known accounts and {@code null} for unknown ones. */
  private static final class FakeProvider implements AccountLimitsProvider {

    private static final long MAX_QTY = 500L * 100_000_000L;
    private static final long MAX_NOTIONAL = 5_000_000L * 100_000_000L;
    private static final int DEVIATION_BPS = 25;
    private static final int MAX_OPS = 20;

    private final List<String> knownAccounts;

    FakeProvider(final List<String> knownAccounts) {
      this.knownAccounts = knownAccounts;
    }

    @Override
    public BrowserEvent.AccountLimits lookup(final String account) {
      if (knownAccounts.contains(account)) {
        return new BrowserEvent.AccountLimits(
            account, MAX_QTY, MAX_NOTIONAL, DEVIATION_BPS, MAX_OPS);
      }
      return null;
    }
  }

  /** Provider that always throws a {@link RuntimeException} — for propagation tests. */
  private static final class ThrowingProvider implements AccountLimitsProvider {

    @Override
    public BrowserEvent.AccountLimits lookup(final String account) {
      throw new RuntimeException("provider-failure: " + account);
    }
  }

  // ---------------------------------------------------------------------------
  // Fixtures.
  // ---------------------------------------------------------------------------

  private BridgeSession session;
  private RecordingSink sink;

  @BeforeEach
  void setUp() {
    final var queue = new OutboundQueue(32);
    final var claims =
        new ValidatedClaims("user-1", "jti-1", List.of(), Long.MAX_VALUE, true, List.of());
    session =
        new BridgeSession(
            new SessionId("session-001"),
            claims,
            InetAddress.getLoopbackAddress(),
            queue,
            new PerTypeRateLimiter(0L));
    sink = new RecordingSink();
  }

  /** Build claims with the given account list. */
  private static ValidatedClaims claims(final List<String> accounts) {
    return new ValidatedClaims("user-1", "jti-1", accounts, Long.MAX_VALUE, true, List.of());
  }

  // ---------------------------------------------------------------------------
  // Two accounts resolved.
  // ---------------------------------------------------------------------------

  @Test
  void pushFor_twoAccountsBothResolved_emitsTwoFramesInOrder() {
    final var accounts = List.of("ACME-001", "ACME-002");
    final var source = new BoundedAccountLimitsSource(new FakeProvider(accounts));

    source.pushFor(claims(accounts), session, sink);

    assertEquals(2, sink.received.size(), "must emit one frame per account");
    assertEquals("ACME-001", sink.received.get(0).account());
    assertEquals("ACME-002", sink.received.get(1).account());
  }

  @Test
  void pushFor_twoAccountsBothResolved_limitsMatchProvider() {
    final var accounts = List.of("ACME-001", "ACME-002");
    final var source = new BoundedAccountLimitsSource(new FakeProvider(accounts));

    source.pushFor(claims(accounts), session, sink);

    for (final var limits : sink.received) {
      assertEquals(FakeProvider.MAX_QTY, limits.maxQtyInt64());
      assertEquals(FakeProvider.MAX_NOTIONAL, limits.maxNotionalInt64());
      assertEquals(FakeProvider.DEVIATION_BPS, limits.priceDeviationBps());
      assertEquals(FakeProvider.MAX_OPS, limits.maxOrdersPerSecond());
    }
  }

  // ---------------------------------------------------------------------------
  // One account returns null → pessimistic defaults.
  // ---------------------------------------------------------------------------

  @Test
  void pushFor_oneAccountProviderReturnsNull_emitsPessimisticDefaults() {
    // Provider knows no accounts → lookup always returns null.
    final var source = new BoundedAccountLimitsSource(new FakeProvider(List.of()));

    source.pushFor(claims(List.of("UNKNOWN-001")), session, sink);

    assertEquals(1, sink.received.size(), "must still emit one frame even for unknown account");
    final var limits = sink.received.get(0);
    assertEquals("UNKNOWN-001", limits.account());
    assertEquals(0L, limits.maxQtyInt64(), "pessimistic qty must be 0");
    assertEquals(0L, limits.maxNotionalInt64(), "pessimistic notional must be 0");
    assertEquals(0, limits.priceDeviationBps(), "pessimistic deviationBps must be 0");
    assertEquals(0, limits.maxOrdersPerSecond(), "pessimistic maxOps must be 0");
  }

  @Test
  void pushFor_mixedResolvedAndNull_emitsPessimisticForUnknownAndRealForKnown() {
    // Only ACME-001 is known; ACME-999 gets pessimistic defaults.
    final var source = new BoundedAccountLimitsSource(new FakeProvider(List.of("ACME-001")));
    final var accounts = List.of("ACME-001", "ACME-999");

    source.pushFor(claims(accounts), session, sink);

    assertEquals(2, sink.received.size());
    final var first = sink.received.get(0);
    assertEquals("ACME-001", first.account());
    assertEquals(
        FakeProvider.MAX_QTY, first.maxQtyInt64(), "resolved account must have real limits");
    final var second = sink.received.get(1);
    assertEquals("ACME-999", second.account());
    assertEquals(0L, second.maxQtyInt64(), "unknown account must have pessimistic qty");
  }

  // ---------------------------------------------------------------------------
  // Zero accounts.
  // ---------------------------------------------------------------------------

  @Test
  void pushFor_zeroAccounts_emitsNothing() {
    final var source = new BoundedAccountLimitsSource(new FakeProvider(List.of()));

    source.pushFor(claims(List.of()), session, sink);

    assertEquals(0, sink.received.size(), "no frames must be emitted for empty account list");
  }

  // ---------------------------------------------------------------------------
  // Provider throws — exception propagates.
  // ---------------------------------------------------------------------------

  @Test
  void pushFor_providerThrows_exceptionPropagated() {
    final var source = new BoundedAccountLimitsSource(new ThrowingProvider());

    assertThrows(
        RuntimeException.class,
        () -> source.pushFor(claims(List.of("ACME-001")), session, sink),
        "provider exception must propagate without being swallowed");
  }

  // ---------------------------------------------------------------------------
  // Constructor null-check.
  // ---------------------------------------------------------------------------

  @Test
  void constructor_nullProvider_throws() {
    assertThrows(NullPointerException.class, () -> new BoundedAccountLimitsSource(null));
  }
}
