package com.trading.engine.fixbridge.rawfix;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link RawFixRateLimiter}.
 *
 * <p>Covers: construction with defaults (burst starts full), construction validation (non-positive
 * burst and rate rejected), token exhaustion ({@code tryConsume} returns false when empty), refill
 * arithmetic (token accumulation after clock advance), burst cap enforcement (refill never exceeds
 * burst capacity), and edge cases (no clock advance, backwards clock).
 *
 * <p>All arithmetic is verified against the token-bucket formula:
 *
 * <pre>
 *   tokens_added = elapsed_ns * (rate_per_sec / 1_000_000_000)
 *   tokens       = min(capacity, tokens + tokens_added)
 * </pre>
 *
 * <p>Threading: not thread-safe per the class contract. Each test constructs its own {@link
 * RawFixRateLimiter} instance — no sharing.
 *
 * <p>Allocation: tests freely allocate — zero-alloc constraint is verified in {@link
 * RawFixRateLimiterAllocTest}.
 */
final class RawFixRateLimiterTest {

  /** Fixed construction timestamp. Non-zero to avoid latent underflow in refill arithmetic. */
  private static final long T0 = 1_000_000_000_000L; // 1000 s in nanos

  // ---------------------------------------------------------------------------
  // Construction — default bucket.
  // ---------------------------------------------------------------------------

  /**
   * The bucket starts full (capacity == DEFAULT_BURST = 1000). The first 1000 consecutive {@code
   * tryConsume} calls at the construction timestamp must all return {@code true}.
   */
  @Test
  void ctor_defaults_burstStartsFull() {
    final var limiter = new RawFixRateLimiter(T0);

    for (int i = 0; i < (int) RawFixRateLimiter.DEFAULT_BURST; i++) {
      assertTrue(limiter.tryConsume(T0), "call " + (i + 1) + " expected true within burst");
    }
    // 1001st call: bucket empty.
    assertFalse(limiter.tryConsume(T0), "expected false after burst exhausted");
  }

  // ---------------------------------------------------------------------------
  // Construction — validation.
  // ---------------------------------------------------------------------------

  @Test
  void ctor_zeroBurst_throwsIAE() {
    assertThrows(IllegalArgumentException.class, () -> new RawFixRateLimiter(0L, 10.0, T0));
  }

  @Test
  void ctor_negativeBurst_throwsIAE() {
    assertThrows(IllegalArgumentException.class, () -> new RawFixRateLimiter(-1L, 10.0, T0));
  }

  @Test
  void ctor_zeroRate_throwsIAE() {
    assertThrows(IllegalArgumentException.class, () -> new RawFixRateLimiter(10L, 0.0, T0));
  }

  @Test
  void ctor_negativeRate_throwsIAE() {
    assertThrows(IllegalArgumentException.class, () -> new RawFixRateLimiter(10L, -1.0, T0));
  }

  // ---------------------------------------------------------------------------
  // tryConsume — exhaustion.
  // ---------------------------------------------------------------------------

  /**
   * burst=10, rate=10/sec. Exhaust 10 tokens at {@code T0}, then the 11th call at the same
   * timestamp must return false (no refill has occurred).
   */
  @Test
  void tryConsume_burstExhausted_returnsFalse() {
    final var limiter = new RawFixRateLimiter(10L, 10.0, T0);

    for (int i = 0; i < 10; i++) {
      assertTrue(limiter.tryConsume(T0), "call " + (i + 1) + " expected true");
    }
    assertFalse(limiter.tryConsume(T0), "11th call must be false — burst exhausted");
  }

  // ---------------------------------------------------------------------------
  // tryConsume — refill.
  // ---------------------------------------------------------------------------

  /**
   * burst=10, rate=10/sec. Exhaust the bucket, then advance 1 ms (1_000_000 ns). Tokens added =
   * 1_000_000 × (10 / 1_000_000_000) = 0.01 — well below 1.0. The 11th consecutive call must return
   * false.
   */
  @Test
  void tryConsume_refillAt1msNanos_doesNotAddTokens() {
    final var limiter = new RawFixRateLimiter(10L, 10.0, T0);

    for (int i = 0; i < 10; i++) {
      limiter.tryConsume(T0);
    }

    final long t1ms = T0 + 1_000_000L; // +1 ms
    assertFalse(limiter.tryConsume(t1ms), "0.01 tokens added — not enough for another consume");
  }

  /**
   * burst=10, rate=10/sec. Exhaust the bucket. Advance 200 ms (200_000_000 ns). Tokens added =
   * 200_000_000 × (10 / 1_000_000_000) = 2.0. Next 2 {@code tryConsume} calls succeed; 3rd fails.
   */
  @Test
  void tryConsume_refill200ms_addsTwoTokens() {
    final var limiter = new RawFixRateLimiter(10L, 10.0, T0);

    for (int i = 0; i < 10; i++) {
      limiter.tryConsume(T0);
    }

    final long t200ms = T0 + 200_000_000L; // +200 ms
    assertTrue(limiter.tryConsume(t200ms), "first refilled token — expected true");
    assertTrue(limiter.tryConsume(t200ms), "second refilled token — expected true");
    assertFalse(limiter.tryConsume(t200ms), "third call at same time — expected false (empty)");
  }

  /**
   * burst=10, rate=1000/sec. Consume 5 tokens, then advance 1 second. Without the cap: tokens = 5 +
   * (1_000_000_000 × 1000 / 1_000_000_000) = 5 + 1000 = 1005. With cap: tokens = min(10, 1005) =
   * 10. Only 10 more calls should succeed.
   */
  @Test
  void tryConsume_refillSaturatesAtBurst() {
    final var limiter = new RawFixRateLimiter(10L, 1000.0, T0);

    // Consume 5.
    for (int i = 0; i < 5; i++) {
      assertTrue(limiter.tryConsume(T0), "call " + (i + 1) + " must succeed");
    }

    // Advance 1 second — would overflow to 1005 without cap.
    final long t1sec = T0 + 1_000_000_000L;

    // Bucket must be capped at 10 (not 10 + 1000).
    for (int i = 0; i < 10; i++) {
      assertTrue(limiter.tryConsume(t1sec), "call " + (i + 1) + " after refill must succeed");
    }
    assertFalse(limiter.tryConsume(t1sec), "11th call at same time — must be false (at cap)");
  }

  // ---------------------------------------------------------------------------
  // tryConsume — clock edge cases.
  // ---------------------------------------------------------------------------

  /**
   * When the clock does not advance ({@code nowNs == lastRefillNs}), elapsed is 0 and the refill
   * method returns immediately. The bucket can only drain. Exhaust it at T0, then confirm the very
   * next call at T0 is also false.
   */
  @Test
  void tryConsume_clockDoesntAdvance_consumesUntilEmpty() {
    final var limiter = new RawFixRateLimiter(5L, 10.0, T0);

    for (int i = 0; i < 5; i++) {
      limiter.tryConsume(T0);
    }
    assertFalse(limiter.tryConsume(T0), "clock unchanged — expected false");
  }

  /**
   * A backwards clock call (nowNs < lastRefillNs) must not corrupt state. The refill guard treats
   * elapsed ≤ 0 as a no-op and leaves {@code lastRefillNs} alone. A subsequent forward call must
   * still correctly accumulate elapsed time from the previous {@code lastRefillNs}.
   *
   * <p>Sequence:
   *
   * <ol>
   *   <li>Exhaust burst=10 at T0.
   *   <li>Advance 100 ms (→ T0+100ms) — refill = 100ms × 10/s = 1.0 token. One consume succeeds.
   *   <li>Call with T0 (backwards) — no refill, state is unchanged.
   *   <li>Call again at T0+100ms — lastRefillNs was T0+100ms; elapsed=0 — false (empty).
   *   <li>Advance another 100 ms (→ T0+200ms) — refill another 1.0 token. Next call succeeds.
   * </ol>
   */
  @Test
  void tryConsume_clockGoesBackwards_doesNotMutateState() {
    final var limiter = new RawFixRateLimiter(10L, 10.0, T0);

    // Exhaust burst.
    for (int i = 0; i < 10; i++) {
      limiter.tryConsume(T0);
    }

    // Advance 100 ms — gain 1 token.
    final long t100ms = T0 + 100_000_000L;
    assertTrue(limiter.tryConsume(t100ms), "after +100ms: should have 1 refilled token");

    // Go backwards to T0 — elapsed ≤ 0 → no-op.
    assertFalse(limiter.tryConsume(T0), "backwards call: bucket is empty, no refill");

    // Verify lastRefillNs was not corrupted by the backwards call. Advance another 100 ms from
    // lastRefillNs (which remains T0+100ms) — we should gain another 1 token.
    final long t200ms = T0 + 200_000_000L;
    assertTrue(limiter.tryConsume(t200ms), "after +200ms total: should have 1 more refilled token");
    assertFalse(limiter.tryConsume(t200ms), "same timestamp again: empty after the one consume");
  }
}
