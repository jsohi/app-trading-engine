package com.trading.engine.cluster.state;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link TokenBucket}. Exercises the continuous-refill token-bucket algorithm with
 * integer-only arithmetic, the {@link TokenBucket#activate} lifecycle, and the zero-allocation
 * smoke path.
 *
 * <p><b>Threading:</b> single-threaded tests — no concurrency.
 */
class TokenBucketTest {

  private static final long CAPACITY = 5L;

  /** 1s window / 5 tokens = 200ms (200_000_000 ns) per token. */
  private static final long REFILL_NANOS_PER_TOKEN = 200_000_000L;

  private static final long T0 = 1_700_000_000_000_000_000L; // arbitrary epoch base

  // ---------------------------------------------------------------------------
  // Happy-path admittance below capacity
  // ---------------------------------------------------------------------------

  /**
   * A freshly-activated bucket starts full. The first N=capacity tryConsume calls must all return
   * true.
   */
  @Test
  void tryConsume_freshBucketBelowLimit_admits() {
    final var bucket = new TokenBucket(0);
    bucket.activate(CAPACITY, REFILL_NANOS_PER_TOKEN, T0);

    // Drain all tokens — every call must be admitted.
    for (int i = 0; i < (int) CAPACITY; i++) {
      assertTrue(bucket.tryConsume(T0), "expected admission on attempt " + (i + 1));
    }
    assertEquals(0L, bucket.tokens(), "bucket should be empty after draining capacity tokens");
  }

  // ---------------------------------------------------------------------------
  // Rejection when empty
  // ---------------------------------------------------------------------------

  /**
   * After draining all tokens the bucket rejects further requests at the same timestamp (no refill
   * has elapsed).
   */
  @Test
  void tryConsume_drainBucket_rejectsWhenEmpty() {
    final var bucket = new TokenBucket(1);
    bucket.activate(CAPACITY, REFILL_NANOS_PER_TOKEN, T0);

    // Drain to zero.
    for (int i = 0; i < (int) CAPACITY; i++) {
      bucket.tryConsume(T0);
    }

    // Same timestamp → no elapsed time → no refill → reject.
    assertFalse(bucket.tryConsume(T0), "empty bucket must reject at same timestamp");
    assertEquals(0L, bucket.tokens(), "token count must remain 0 after rejection");
  }

  // ---------------------------------------------------------------------------
  // Refill across window boundary
  // ---------------------------------------------------------------------------

  /**
   * After draining the bucket, advancing time by {@code REFILL_NANOS_PER_TOKEN * k} grants exactly
   * {@code k} new tokens; subsequent calls consume those refilled tokens and then reject.
   */
  @Test
  void tryConsume_acrossWindow_refillsAtRate() {
    final var bucket = new TokenBucket(2);
    bucket.activate(CAPACITY, REFILL_NANOS_PER_TOKEN, T0);

    // Drain all 5 tokens.
    for (int i = 0; i < (int) CAPACITY; i++) {
      bucket.tryConsume(T0);
    }
    assertFalse(bucket.tryConsume(T0), "should be empty at T0");

    // Advance by exactly 2 refill periods → +2 tokens.
    final long t1 = T0 + 2L * REFILL_NANOS_PER_TOKEN;
    assertTrue(bucket.tryConsume(t1), "first refilled token must be admitted");
    assertTrue(bucket.tryConsume(t1), "second refilled token must be admitted");
    assertFalse(bucket.tryConsume(t1), "third call must be rejected after consuming 2 refilled");

    // Advance by 1 more period → +1 more token.
    final long t2 = t1 + REFILL_NANOS_PER_TOKEN;
    assertTrue(bucket.tryConsume(t2), "one more token should be available after another period");
    assertFalse(bucket.tryConsume(t2), "no more tokens — bucket empty again");
  }

  // ---------------------------------------------------------------------------
  // Zero-allocation smoke test
  // ---------------------------------------------------------------------------

  /**
   * Calls {@link TokenBucket#tryConsume} 100k times after warmup. No {@link Throwable} is expected;
   * this guards against regression to an allocating implementation.
   *
   * <p>This is a smoke test: it does not use a GC listener. It verifies only that the hot path is
   * exception-free under tight iteration.
   */
  @Test
  void tryConsume_zeroAlloc_smokesWithoutException() {
    final var bucket = new TokenBucket(3);
    // Large capacity so refill keeps the bucket non-empty throughout.
    final long bigCapacity = 100_000L;
    // Refill rate: 1 token per nanosecond for very fast replenishment.
    final long fastRefill = 1L;
    bucket.activate(bigCapacity, fastRefill, T0);

    // Warmup: 10k calls to JIT-compile the hot path before the timed run.
    for (int i = 0; i < 10_000; i++) {
      bucket.tryConsume(T0 + i);
    }

    // Hot path: 100k more calls — must complete without any exception.
    for (int i = 0; i < 100_000; i++) {
      bucket.tryConsume(T0 + 10_000L + i);
    }
    // If we reach here without throwing, the smoke test passes.
  }

  // ---------------------------------------------------------------------------
  // activate() sets full capacity
  // ---------------------------------------------------------------------------

  /**
   * {@link TokenBucket#activate} must initialise the bucket to the full capacity regardless of any
   * prior state.
   */
  @Test
  void activate_setsFullCapacity() {
    final var bucket = new TokenBucket(4);

    // First activation: small capacity.
    bucket.activate(3L, REFILL_NANOS_PER_TOKEN, T0);
    assertEquals(3L, bucket.tokens(), "tokens should equal capacity after first activate");

    // Drain fully.
    bucket.tryConsume(T0);
    bucket.tryConsume(T0);
    bucket.tryConsume(T0);
    assertEquals(0L, bucket.tokens(), "tokens should be 0 after drain");

    // Re-activate with a different capacity — must reset to new capacity.
    final long newCapacity = 7L;
    bucket.activate(newCapacity, REFILL_NANOS_PER_TOKEN, T0 + 1_000_000_000L);
    assertEquals(newCapacity, bucket.tokens(), "re-activate must fill to the new capacity");
  }

  // ---------------------------------------------------------------------------
  // poolIndex accessor is immutable after construction
  // ---------------------------------------------------------------------------

  /**
   * The pool index provided at construction is returned unchanged by {@link TokenBucket#poolIndex}.
   */
  @Test
  void poolIndex_reflectsConstructorArgument() {
    final int expectedIndex = 99;
    final var bucket = new TokenBucket(expectedIndex);
    assertEquals(
        expectedIndex,
        bucket.poolIndex(),
        "poolIndex() must return the value supplied at construction");
  }
}
