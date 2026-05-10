package com.trading.engine.fixbridge.ratelimit;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.trading.engine.fixbridge.ratelimit.PerTypeRateLimiter.CommandType;
import com.trading.engine.fixbridge.ratelimit.PerTypeRateLimiter.Outcome;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link PerTypeRateLimiter}.
 *
 * <p>Covers: construction, initial-window enforcement (first-60s tighter buckets), normal-mode
 * enforcement (steady-state), the 60s boundary transition (strict less-than), bucket isolation, and
 * the no-clock-advance edge case. Test names follow {@code methodUnderTest_scenario_expectedBehavior}.
 *
 * <p>Clock arithmetic: {@code authNanos = 0L}. All {@code nowNs} values are expressed as offsets
 * from zero so refill deltas are easy to compute manually.
 *
 * <p>Threading: not thread-safe per the class contract. Tests run serially on a single thread and
 * use independent limiter instances — no sharing.
 *
 * <p>Allocation: the hot path is zero-alloc; the tests themselves freely allocate (they are not
 * in the {@code *AllocTest} suite).
 */
final class PerTypeRateLimiterTest {

  /** Auth anchor pinned at epoch zero for deterministic arithmetic. */
  private static final long AUTH_NANOS = 0L;

  /** Any timestamp strictly inside the first-60s window. */
  private static final long WITHIN_WINDOW_NS = 1_000_000L; // 1 ms after auth

  /**
   * Any timestamp strictly outside (>= 60s after auth) the first-60s window.
   * Boundary = exactly 60 s → normal mode per the strict less-than.
   */
  private static final long AFTER_WINDOW_NS = PerTypeRateLimiter.FIRST_60S_WINDOW_NANOS; // = 60s

  // ---------------------------------------------------------------------------
  // Construction.
  // ---------------------------------------------------------------------------

  @Test
  void ctor_atAuthNanos_authNanosReturnsArgument() {
    final var limiter = new PerTypeRateLimiter(AUTH_NANOS);
    assertEquals(AUTH_NANOS, limiter.authNanos());
  }

  /**
   * Buckets start full, so the very first call to each command type must be ALLOWED regardless of
   * mode (initial-window or normal).
   */
  @Test
  void ctor_initialBucketsAreFull() {
    final var limiter = new PerTypeRateLimiter(AUTH_NANOS);
    // First call at auth time is still in initial window (0 < 60s).
    assertEquals(Outcome.ALLOWED, limiter.tryConsume(CommandType.QUOTE_REQUEST, AUTH_NANOS));
    assertEquals(Outcome.ALLOWED, limiter.tryConsume(CommandType.ACCEPT_QUOTE, AUTH_NANOS));
    assertEquals(Outcome.ALLOWED, limiter.tryConsume(CommandType.REJECT_QUOTE, AUTH_NANOS));
    assertEquals(Outcome.ALLOWED, limiter.tryConsume(CommandType.NEW_ORDER_SINGLE, AUTH_NANOS));
    assertEquals(Outcome.ALLOWED, limiter.tryConsume(CommandType.CANCEL_ORDER, AUTH_NANOS));
  }

  // ---------------------------------------------------------------------------
  // Initial-window enforcement.
  // ---------------------------------------------------------------------------

  /**
   * AcceptQuote initial-window burst cap is 2. First two calls ALLOWED; third is
   * REJECTED_INITIAL_WINDOW.
   */
  @Test
  void tryConsume_acceptQuoteThirdConsecutiveCallInInitialWindow_returnsRejectedInitialWindow() {
    final var limiter = new PerTypeRateLimiter(AUTH_NANOS);
    assertEquals(Outcome.ALLOWED, limiter.tryConsume(CommandType.ACCEPT_QUOTE, WITHIN_WINDOW_NS));
    assertEquals(Outcome.ALLOWED, limiter.tryConsume(CommandType.ACCEPT_QUOTE, WITHIN_WINDOW_NS));
    assertEquals(
        Outcome.REJECTED_INITIAL_WINDOW,
        limiter.tryConsume(CommandType.ACCEPT_QUOTE, WITHIN_WINDOW_NS));
  }

  /**
   * QuoteRequest initial-window burst cap is 2; sustained is 5/s. Drain the initial burst (2
   * calls), then verify third is REJECTED_INITIAL_WINDOW. Advance 200 ms (elapsed = 200_000_000 ns;
   * refill = 0.2 s × 5/s = 1.0 token) and verify one more ALLOWED.
   */
  @Test
  void tryConsume_quoteRequestSecondCallAtSameNanos_returnsAllowedThenRejectedInitialWindowAtThird_thenRefills() {
    final var limiter = new PerTypeRateLimiter(AUTH_NANOS);
    // Drain burst.
    assertEquals(Outcome.ALLOWED, limiter.tryConsume(CommandType.QUOTE_REQUEST, WITHIN_WINDOW_NS));
    assertEquals(Outcome.ALLOWED, limiter.tryConsume(CommandType.QUOTE_REQUEST, WITHIN_WINDOW_NS));
    assertEquals(
        Outcome.REJECTED_INITIAL_WINDOW,
        limiter.tryConsume(CommandType.QUOTE_REQUEST, WITHIN_WINDOW_NS));

    // Advance 200 ms within the initial window — refill = 0.2 × 5 = 1.0 token.
    final long nowNs200ms = WITHIN_WINDOW_NS + 200_000_000L;
    // Sanity: still in initial window.
    assert (nowNs200ms - AUTH_NANOS) < PerTypeRateLimiter.FIRST_60S_WINDOW_NANOS;
    assertEquals(Outcome.ALLOWED, limiter.tryConsume(CommandType.QUOTE_REQUEST, nowNs200ms));
  }

  @Test
  void tryConsume_newOrderSingleInitialBurst2_thirdRejected() {
    final var limiter = new PerTypeRateLimiter(AUTH_NANOS);
    assertEquals(Outcome.ALLOWED, limiter.tryConsume(CommandType.NEW_ORDER_SINGLE, WITHIN_WINDOW_NS));
    assertEquals(Outcome.ALLOWED, limiter.tryConsume(CommandType.NEW_ORDER_SINGLE, WITHIN_WINDOW_NS));
    assertEquals(
        Outcome.REJECTED_INITIAL_WINDOW,
        limiter.tryConsume(CommandType.NEW_ORDER_SINGLE, WITHIN_WINDOW_NS));
  }

  /**
   * RejectQuote initial-window burst cap is 20 (same as normal). 20 ALLOWED, 21st
   * REJECTED_INITIAL_WINDOW.
   */
  @Test
  void tryConsume_rejectQuoteInitialBurst20_higherCapacity() {
    final var limiter = new PerTypeRateLimiter(AUTH_NANOS);
    for (int i = 0; i < 20; i++) {
      assertEquals(
          Outcome.ALLOWED,
          limiter.tryConsume(CommandType.REJECT_QUOTE, WITHIN_WINDOW_NS),
          "call " + (i + 1) + " expected ALLOWED");
    }
    assertEquals(
        Outcome.REJECTED_INITIAL_WINDOW,
        limiter.tryConsume(CommandType.REJECT_QUOTE, WITHIN_WINDOW_NS));
  }

  /**
   * CancelOrder initial-window burst cap is 50 (same as normal). 50 ALLOWED, 51st
   * REJECTED_INITIAL_WINDOW.
   */
  @Test
  void tryConsume_cancelOrderInitialBurst50() {
    final var limiter = new PerTypeRateLimiter(AUTH_NANOS);
    for (int i = 0; i < 50; i++) {
      assertEquals(
          Outcome.ALLOWED,
          limiter.tryConsume(CommandType.CANCEL_ORDER, WITHIN_WINDOW_NS),
          "call " + (i + 1) + " expected ALLOWED");
    }
    assertEquals(
        Outcome.REJECTED_INITIAL_WINDOW,
        limiter.tryConsume(CommandType.CANCEL_ORDER, WITHIN_WINDOW_NS));
  }

  // ---------------------------------------------------------------------------
  // Normal-mode enforcement (after initial window).
  // ---------------------------------------------------------------------------

  /**
   * After the initial window, AcceptQuote uses the normal bucket (burst=10). First wave: 10 ALLOWED,
   * 11th REJECTED_RATE_LIMIT (not REJECTED_INITIAL_WINDOW — the label changes).
   */
  @Test
  void tryConsume_acceptQuoteAfterInitialWindow_usesNormalBurst10() {
    final var limiter = new PerTypeRateLimiter(AUTH_NANOS);
    final long nowNs = AFTER_WINDOW_NS; // exactly 60s → normal mode
    for (int i = 0; i < 10; i++) {
      assertEquals(
          Outcome.ALLOWED,
          limiter.tryConsume(CommandType.ACCEPT_QUOTE, nowNs),
          "call " + (i + 1) + " expected ALLOWED");
    }
    assertEquals(
        Outcome.REJECTED_RATE_LIMIT,
        limiter.tryConsume(CommandType.ACCEPT_QUOTE, nowNs));
  }

  /**
   * QuoteRequest normal sustained refill rate is 5/s. Exhaust the normal burst (10), then advance
   * 200 ms — refill = 0.2 × 5 = 1.0 token — and verify exactly one more ALLOWED.
   */
  @Test
  void tryConsume_normalSustainedRefillRate_quoteRequestRefillsAt5PerSec() {
    final var limiter = new PerTypeRateLimiter(AUTH_NANOS);
    final long t0 = AFTER_WINDOW_NS; // first call at exactly 60s boundary
    // Exhaust burst (10 tokens).
    for (int i = 0; i < 10; i++) {
      assertEquals(Outcome.ALLOWED, limiter.tryConsume(CommandType.QUOTE_REQUEST, t0));
    }
    assertEquals(Outcome.REJECTED_RATE_LIMIT, limiter.tryConsume(CommandType.QUOTE_REQUEST, t0));

    // Advance 200 ms: 0.2 s × 5 tokens/s = 1.0 token refilled.
    final long t200ms = t0 + 200_000_000L;
    assertEquals(Outcome.ALLOWED, limiter.tryConsume(CommandType.QUOTE_REQUEST, t200ms));
    // Second call at same time — no further refill, bucket empty again.
    assertEquals(Outcome.REJECTED_RATE_LIMIT, limiter.tryConsume(CommandType.QUOTE_REQUEST, t200ms));
  }

  // ---------------------------------------------------------------------------
  // 60s boundary.
  // ---------------------------------------------------------------------------

  /**
   * The guard is {@code (nowNs - authNanos) < 60s} — strict less-than. At exactly 60s the normal
   * bucket is consulted (REJECTED_RATE_LIMIT rather than REJECTED_INITIAL_WINDOW).
   *
   * <p>We drain the initial bucket down to zero first so that if the initial bucket were mistakenly
   * consulted its empty state would give REJECTED_INITIAL_WINDOW, making the test correctly detect a
   * wrong boundary.
   */
  @Test
  void tryConsume_atExactly60sBoundary_switchesToNormalMode() {
    final var limiter = new PerTypeRateLimiter(AUTH_NANOS);

    // Drain the initial AcceptQuote bucket (burst 2) completely inside the window.
    assertEquals(Outcome.ALLOWED, limiter.tryConsume(CommandType.ACCEPT_QUOTE, WITHIN_WINDOW_NS));
    assertEquals(Outcome.ALLOWED, limiter.tryConsume(CommandType.ACCEPT_QUOTE, WITHIN_WINDOW_NS));
    assertEquals(
        Outcome.REJECTED_INITIAL_WINDOW,
        limiter.tryConsume(CommandType.ACCEPT_QUOTE, WITHIN_WINDOW_NS));

    // At exactly 60s, the normal bucket (burst=10) is used — all 10 should be ALLOWED.
    final long boundary = AUTH_NANOS + PerTypeRateLimiter.FIRST_60S_WINDOW_NANOS; // = 60_000_000_000L
    for (int i = 0; i < 10; i++) {
      assertEquals(
          Outcome.ALLOWED,
          limiter.tryConsume(CommandType.ACCEPT_QUOTE, boundary),
          "call " + (i + 1) + " at boundary expected ALLOWED");
    }
    // 11th: normal bucket exhausted → REJECTED_RATE_LIMIT (not INITIAL_WINDOW).
    assertEquals(
        Outcome.REJECTED_RATE_LIMIT,
        limiter.tryConsume(CommandType.ACCEPT_QUOTE, boundary));
  }

  // ---------------------------------------------------------------------------
  // Bucket isolation.
  // ---------------------------------------------------------------------------

  /**
   * Exhausting the QuoteRequest initial bucket must NOT affect AcceptQuote.
   */
  @Test
  void tryConsume_quoteRequestExhausted_acceptQuoteUnaffected() {
    final var limiter = new PerTypeRateLimiter(AUTH_NANOS);
    // Drain QuoteRequest initial burst (2).
    limiter.tryConsume(CommandType.QUOTE_REQUEST, WITHIN_WINDOW_NS);
    limiter.tryConsume(CommandType.QUOTE_REQUEST, WITHIN_WINDOW_NS);
    assertEquals(
        Outcome.REJECTED_INITIAL_WINDOW,
        limiter.tryConsume(CommandType.QUOTE_REQUEST, WITHIN_WINDOW_NS));

    // AcceptQuote initial burst (2) is unaffected — first two calls ALLOWED.
    assertEquals(Outcome.ALLOWED, limiter.tryConsume(CommandType.ACCEPT_QUOTE, WITHIN_WINDOW_NS));
    assertEquals(Outcome.ALLOWED, limiter.tryConsume(CommandType.ACCEPT_QUOTE, WITHIN_WINDOW_NS));
  }

  /**
   * Draining the initial bucket heavily inside the first-60s window leaves the normal bucket
   * untouched (it refills from auth time). Once the window crosses, the normal bucket should be
   * full (up to its cap), not depleted.
   */
  @Test
  void tryConsume_separateBucketsBetweenInitialAndNormal() {
    final var limiter = new PerTypeRateLimiter(AUTH_NANOS);

    // Drain AcceptQuote initial bucket (2 calls) completely inside the window.
    assertEquals(Outcome.ALLOWED, limiter.tryConsume(CommandType.ACCEPT_QUOTE, WITHIN_WINDOW_NS));
    assertEquals(Outcome.ALLOWED, limiter.tryConsume(CommandType.ACCEPT_QUOTE, WITHIN_WINDOW_NS));
    assertEquals(
        Outcome.REJECTED_INITIAL_WINDOW,
        limiter.tryConsume(CommandType.ACCEPT_QUOTE, WITHIN_WINDOW_NS));

    // Cross into normal mode. The normal bucket (burst=10) was never touched — starts full.
    final long normalNs = AFTER_WINDOW_NS;
    for (int i = 0; i < 10; i++) {
      assertEquals(
          Outcome.ALLOWED,
          limiter.tryConsume(CommandType.ACCEPT_QUOTE, normalNs),
          "normal call " + (i + 1) + " expected ALLOWED");
    }
    assertEquals(Outcome.REJECTED_RATE_LIMIT, limiter.tryConsume(CommandType.ACCEPT_QUOTE, normalNs));
  }

  // ---------------------------------------------------------------------------
  // Clock-doesn't-advance edge case.
  // ---------------------------------------------------------------------------

  /**
   * When {@code nowNs} does not advance (elapsed = 0), the bucket can only drain — no refill
   * occurs. The normal QuoteRequest bucket starts with burst=10, so the first 10 calls at the same
   * timestamp (after the window) are ALLOWED; the 11th is REJECTED_RATE_LIMIT.
   */
  @Test
  void tryConsume_sameNowNsRepeatedly_consumesUntilEmpty_thenRejects() {
    final var limiter = new PerTypeRateLimiter(AUTH_NANOS);
    final long nowNs = AFTER_WINDOW_NS; // normal mode, no refill between calls
    for (int i = 0; i < 10; i++) {
      assertEquals(
          Outcome.ALLOWED,
          limiter.tryConsume(CommandType.QUOTE_REQUEST, nowNs),
          "call " + (i + 1) + " expected ALLOWED");
    }
    assertEquals(Outcome.REJECTED_RATE_LIMIT, limiter.tryConsume(CommandType.QUOTE_REQUEST, nowNs));
  }
}
