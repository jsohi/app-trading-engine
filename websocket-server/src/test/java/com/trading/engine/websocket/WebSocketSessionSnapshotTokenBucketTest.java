package com.trading.engine.websocket;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for the per-session snapshot-request token-bucket implemented in {@link
 * WebSocketSession}: {@link WebSocketSession#initSnapshotTokenBucket(long)}, {@link
 * WebSocketSession#tryConsumeSnapshotToken(long)}, {@link WebSocketSession#refundSnapshotToken()},
 * and {@link WebSocketSession#snapshotTokensAvailable()}.
 *
 * <p><b>Design under test.</b> The bucket carries capacity = 10 tokens (from {@code
 * MarketDataConstants.MARKET_DATA_SNAPSHOT_REQUESTS_PER_SECOND}) and refills lazily at 10
 * tokens/second. Refill math: {@code tokensToAdd = floor(elapsedNs × 10 / 1_000_000_000)}; the
 * {@code lastRefillNanos} cursor advances by {@code (tokensToAdd × 1_000_000_000) / 10} so
 * fractional-second remainder carries over across consume calls. The bucket is capped at capacity
 * on refill and on refund, preventing burst above the steady-state rate.
 *
 * <p><b>Threading model.</b> Not thread-safe — each test constructs its own {@link
 * WebSocketSession} and drives it single-threadedly, matching the Netty event-loop ownership
 * contract of the production code.
 *
 * <p><b>Allocation.</b> {@link EmbeddedChannel} is Netty's standard in-process test fixture and
 * allocates on construction; tests call {@link EmbeddedChannel#finishAndReleaseAll()} to avoid
 * buffer leak warnings.
 *
 * <p><b>Test naming convention.</b> {@code methodUnderTest_scenario_expectedBehavior}.
 */
final class WebSocketSessionSnapshotTokenBucketTest {

  // -------------------------------------------------------------------------
  // 1. Pre-init guard rails
  // -------------------------------------------------------------------------

  @Test
  void tryConsume_beforeInit_throwsIllegalStateException() {
    final var channel = new EmbeddedChannel();
    final var session = new WebSocketSession(channel, 0L, "127.0.0.1");

    assertThrows(
        IllegalStateException.class,
        () -> session.tryConsumeSnapshotToken(0L),
        "tryConsumeSnapshotToken must throw before initSnapshotTokenBucket");

    channel.finishAndReleaseAll();
  }

  @Test
  void refund_beforeInit_throwsIllegalStateException() {
    final var channel = new EmbeddedChannel();
    final var session = new WebSocketSession(channel, 0L, "127.0.0.1");

    assertThrows(
        IllegalStateException.class,
        () -> session.refundSnapshotToken(),
        "refundSnapshotToken must throw before initSnapshotTokenBucket");

    channel.finishAndReleaseAll();
  }

  @Test
  void snapshotTokensAvailable_beforeInit_returnsMinusOne() {
    final var channel = new EmbeddedChannel();
    final var session = new WebSocketSession(channel, 0L, "127.0.0.1");

    assertEquals(-1L, session.snapshotTokensAvailable(), "Must return -1 before bucket init");

    channel.finishAndReleaseAll();
  }

  // -------------------------------------------------------------------------
  // 2. Initialisation
  // -------------------------------------------------------------------------

  @Test
  void initSnapshotTokenBucket_seedsFullCapacity() {
    final var channel = new EmbeddedChannel();
    final var session = new WebSocketSession(channel, 0L, "127.0.0.1");

    session.initSnapshotTokenBucket(0L);

    assertEquals(10L, session.snapshotTokensAvailable(), "Fresh bucket must start at capacity 10");

    channel.finishAndReleaseAll();
  }

  @Test
  void initSnapshotTokenBucket_idempotent_onSecondCall() {
    // Init at t=0 → full (10 tokens).
    // Consume 5 tokens at t=0 → 5 remain.
    // Re-init at t=999_999_999 (just under 1 s) must be a no-op — the bucket is already
    // allocated, so no re-allocation, no reset of tokens, no update of lastRefillNanos.
    // Verify: snapshotTokensAvailable() (no-refill read) still returns 5, NOT 10.
    // Then consume at t=0 (no elapsed time from lastRefillNanos=0 → no additional refill):
    // succeeds and leaves 4 tokens.
    final var channel = new EmbeddedChannel();
    final var session = new WebSocketSession(channel, 0L, "127.0.0.1");

    session.initSnapshotTokenBucket(0L);

    for (int i = 0; i < 5; i++) {
      assertTrue(session.tryConsumeSnapshotToken(0L), "Each of the first 5 consumes must succeed");
    }
    assertEquals(5L, session.snapshotTokensAvailable(), "Must have 5 tokens after 5 consumes");

    // Second init call — must be a no-op (bucket already allocated).
    session.initSnapshotTokenBucket(999_999_999L);

    // snapshotTokensAvailable() reads the raw token count without triggering a refill, so it
    // must still be 5 — not 10 (which would indicate a re-init from the new nowNanos seed).
    assertEquals(5L, session.snapshotTokensAvailable(), "Second init must NOT reset the bucket");

    // Consume at t=0 (same as lastRefillNanos=0 → no elapsed time, no additional refill):
    // succeeds and leaves 4.
    assertTrue(session.tryConsumeSnapshotToken(0L), "Consume after idempotent init must succeed");
    assertEquals(
        4L,
        session.snapshotTokensAvailable(),
        "Must have 4 tokens after consume post idempotent-init");

    channel.finishAndReleaseAll();
  }

  // -------------------------------------------------------------------------
  // 3. Basic consume / exhaustion
  // -------------------------------------------------------------------------

  @Test
  void tryConsume_capacity10_then11thReturnsFalse() {
    final var channel = new EmbeddedChannel();
    final var session = new WebSocketSession(channel, 0L, "127.0.0.1");
    session.initSnapshotTokenBucket(0L);

    // Consume all 10 tokens at t=0 (no elapsed time → no refill).
    for (int i = 0; i < 10; i++) {
      assertTrue(session.tryConsumeSnapshotToken(0L), "Token " + (i + 1) + " must succeed");
    }
    assertEquals(0L, session.snapshotTokensAvailable(), "Bucket must be empty after 10 consumes");

    // 11th consume must fail.
    assertFalse(session.tryConsumeSnapshotToken(0L), "11th consume must return false");
    assertEquals(
        0L, session.snapshotTokensAvailable(), "Bucket must remain at 0 after failed consume");

    channel.finishAndReleaseAll();
  }

  // -------------------------------------------------------------------------
  // 4. Refill behaviour
  // -------------------------------------------------------------------------

  @Test
  void tryConsume_afterOneSecond_refillsToCapacity() {
    // Drain to 0 at t=0; at t=1_000_000_000 (exactly 1 s) the refill adds 10 tokens (back to
    // capacity 10). One consume → 9 tokens remain.
    final var channel = new EmbeddedChannel();
    final var session = new WebSocketSession(channel, 0L, "127.0.0.1");
    session.initSnapshotTokenBucket(0L);

    for (int i = 0; i < 10; i++) {
      session.tryConsumeSnapshotToken(0L);
    }
    assertEquals(0L, session.snapshotTokensAvailable(), "Bucket must be empty before refill");

    final long oneSecondNs = 1_000_000_000L;
    assertTrue(
        session.tryConsumeSnapshotToken(oneSecondNs), "Consume at +1 s must succeed after refill");
    assertEquals(
        9L,
        session.snapshotTokensAvailable(),
        "Must have 9 tokens after refill-to-10 then one consume");

    channel.finishAndReleaseAll();
  }

  @Test
  void tryConsume_partialRefill_addsProportionalTokens() {
    // Drain at t=0; at t=500_000_000 (0.5 s) → floor(0.5 × 10) = 5 fresh tokens.
    // Consume 1 → 4 remain.
    // Consume 4 more (total 5) → 0 remain, all succeed.
    // 6th consume (would need a 6th token) → fails.
    final var channel = new EmbeddedChannel();
    final var session = new WebSocketSession(channel, 0L, "127.0.0.1");
    session.initSnapshotTokenBucket(0L);

    for (int i = 0; i < 10; i++) {
      session.tryConsumeSnapshotToken(0L);
    }
    assertEquals(0L, session.snapshotTokensAvailable());

    final long halfSecondNs = 500_000_000L;
    assertTrue(
        session.tryConsumeSnapshotToken(halfSecondNs), "First consume at +0.5 s must succeed");
    assertEquals(
        4L,
        session.snapshotTokensAvailable(),
        "Must have 4 tokens after 5-token refill and 1 consume");

    // Consume the remaining 4 — all must succeed.
    for (int i = 0; i < 4; i++) {
      assertTrue(
          session.tryConsumeSnapshotToken(halfSecondNs),
          "Consume " + (i + 2) + " at +0.5 s must succeed");
    }
    assertEquals(
        0L,
        session.snapshotTokensAvailable(),
        "Bucket must be empty after 5 total consumes at +0.5 s");

    // 6th consume at +0.5 s → no more tokens.
    assertFalse(session.tryConsumeSnapshotToken(halfSecondNs), "6th consume at +0.5 s must fail");

    channel.finishAndReleaseAll();
  }

  @Test
  void tryConsume_refillCapsAtCapacity_noBurst() {
    // Drain at t=0; at t=5_000_000_000 (5 s) the uncapped refill would be 50, but bucket is
    // capped at 10. One consume → 9 remain (NOT 49).
    final var channel = new EmbeddedChannel();
    final var session = new WebSocketSession(channel, 0L, "127.0.0.1");
    session.initSnapshotTokenBucket(0L);

    for (int i = 0; i < 10; i++) {
      session.tryConsumeSnapshotToken(0L);
    }
    assertEquals(0L, session.snapshotTokensAvailable());

    final long fiveSecondsNs = 5_000_000_000L;
    assertTrue(session.tryConsumeSnapshotToken(fiveSecondsNs), "Consume at +5 s must succeed");
    assertEquals(
        9L, session.snapshotTokensAvailable(), "Bucket must be capped at 10 then decremented to 9");

    channel.finishAndReleaseAll();
  }

  // -------------------------------------------------------------------------
  // 5. Refund behaviour
  // -------------------------------------------------------------------------

  @Test
  void refund_addsOneToken_capsAtCapacity() {
    // Fresh bucket at capacity (10). Refund must not push above 10.
    // Then: consume one → 9; refund → back to 10.
    final var channel = new EmbeddedChannel();
    final var session = new WebSocketSession(channel, 0L, "127.0.0.1");
    session.initSnapshotTokenBucket(0L);

    assertEquals(10L, session.snapshotTokensAvailable());

    // Refund on a full bucket — must stay capped at 10.
    session.refundSnapshotToken();
    assertEquals(10L, session.snapshotTokensAvailable(), "Refund on full bucket must cap at 10");

    // Consume one to create room, then refund restores.
    assertTrue(session.tryConsumeSnapshotToken(0L));
    assertEquals(9L, session.snapshotTokensAvailable());

    session.refundSnapshotToken();
    assertEquals(10L, session.snapshotTokensAvailable(), "Refund after consume must restore to 10");

    channel.finishAndReleaseAll();
  }

  @Test
  void refund_afterFullExhaustion_addsOne() {
    // Drain all 10 at t=0. Refund once → 1 token. One consume → succeeds. Next → fails.
    final var channel = new EmbeddedChannel();
    final var session = new WebSocketSession(channel, 0L, "127.0.0.1");
    session.initSnapshotTokenBucket(0L);

    for (int i = 0; i < 10; i++) {
      session.tryConsumeSnapshotToken(0L);
    }
    assertEquals(0L, session.snapshotTokensAvailable());

    session.refundSnapshotToken();
    assertEquals(
        1L, session.snapshotTokensAvailable(), "Refund after exhaustion must add exactly 1 token");

    assertTrue(session.tryConsumeSnapshotToken(0L), "Consume after refund must succeed");
    assertFalse(
        session.tryConsumeSnapshotToken(0L), "Second consume after single refund must fail");

    channel.finishAndReleaseAll();
  }

  // -------------------------------------------------------------------------
  // 6. Fractional-second carry-over
  // -------------------------------------------------------------------------

  @Test
  void tryConsume_fractionalRefillCarriesOver() {
    // Drain at t=0. At t=50_000_000 (50 ms):
    //   elapsedNs = 50_000_000; tokensToAdd = floor(50_000_000 × 10 / 1_000_000_000) = 0
    //   → cursor does NOT advance; bucket stays at 0; consume fails.
    //
    // At t=100_000_000 (100 ms from init, i.e. elapsed = 100_000_000 from unchanged cursor=0):
    //   elapsedNs = 100_000_000; tokensToAdd = floor(100_000_000 × 10 / 1_000_000_000) = 1
    //   cursorAdvance = (1 × 1_000_000_000) / 10 = 100_000_000
    //   lastRefillNanos = 0 + 100_000_000 = 100_000_000; tokens = 1
    //   consume succeeds; tokens = 0.
    final var channel = new EmbeddedChannel();
    final var session = new WebSocketSession(channel, 0L, "127.0.0.1");
    session.initSnapshotTokenBucket(0L);

    for (int i = 0; i < 10; i++) {
      session.tryConsumeSnapshotToken(0L);
    }
    assertEquals(0L, session.snapshotTokensAvailable());

    // 50 ms: sub-integer refill — bucket stays empty, consume must fail.
    final long fiftyMsNs = 50_000_000L;
    assertFalse(
        session.tryConsumeSnapshotToken(fiftyMsNs),
        "0.05 s = 0.5 tokens (floored to 0) — consume must fail, fractional remainder not yet redeemable");
    assertEquals(0L, session.snapshotTokensAvailable(), "Bucket must still be empty at 50 ms");

    // 100 ms: now elapsed from the still-unchanged cursor (0) is 100_000_000 ns → 1 token.
    // The fractional remainder from the 50 ms call was preserved because the cursor did not
    // advance.
    final long hundredMsNs = 100_000_000L;
    assertTrue(
        session.tryConsumeSnapshotToken(hundredMsNs),
        "0.1 s = 1.0 token (floored to 1) — consume must succeed");
    assertEquals(
        0L,
        session.snapshotTokensAvailable(),
        "Bucket must be empty after the one refilled token is consumed");

    channel.finishAndReleaseAll();
  }
}
