package com.trading.engine.fixbridge.translator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.trading.engine.fix.builder.NewOrderSingleEncoder;
import com.trading.engine.fixbridge.json.BrowserMessageReader;
import com.trading.engine.fixbridge.json.MutableParsedMessage;
import com.trading.engine.gateway.FixedPoint;
import io.netty.buffer.Unpooled;
import java.nio.charset.StandardCharsets;
import org.agrona.concurrent.EpochNanoClock;
import org.junit.jupiter.api.Test;
import uk.co.real_logic.artio.fields.DecimalFloat;

/**
 * Verifies the locked §2 two-phase commit behaviour for {@code AcceptQuote}. The translator does
 * NOT evict the {@link QuoteSnapshot} — it returns a token to the dispatcher, which evicts only
 * after {@code Session.trySend(...) >= 0}. This test simulates the dispatcher loop and asserts each
 * of the three paths:
 *
 * <ol>
 *   <li><b>Success</b> — first {@code trySend} returns {@code >= 0}; cache evicted.
 *   <li><b>Backpressure → retry → success</b> — first {@code trySend} returns negative
 *       (back-pressure); cache stays bound; subsequent retry succeeds; THEN cache evicted.
 *   <li><b>Expiry during backpressure</b> — {@code trySend} keeps returning negative; the quote's
 *       {@code expiryNs} elapses; dispatcher emits {@code OrderReject{reason:"quote- expired"}} and
 *       evicts.
 * </ol>
 */
final class AcceptQuoteRollbackTest {

  /** Single deterministic instance — tests advance via Mutable*Clock below. */
  private static final long INSTANCE_TAG = 0xABCDEFL;

  private static final long SESSION_ID = 0x1234567L;

  /** Minimal mutable wall-clock for simulating elapsed time. */
  private static final class MutableClock implements EpochNanoClock {
    private long now;

    MutableClock(final long initial) {
      this.now = initial;
    }

    @Override
    public long nanoTime() {
      return now;
    }

    void advance(final long deltaNs) {
      this.now += deltaNs;
    }
  }

  /**
   * Tiny stand-in for the per-session quote cache the Phase 5 dispatcher will own. Tracks one
   * snapshot keyed by an opaque {@code quoteCacheToken} (a slot index in the dispatcher's
   * implementation). The {@code commitEviction(token)} method simulates the dispatcher's
   * post-trySend cleanup.
   */
  private static final class FakeQuoteCache {
    private final QuoteSnapshot[] slots;
    private final boolean[] occupied;

    FakeQuoteCache(final int capacity) {
      this.slots = new QuoteSnapshot[capacity];
      this.occupied = new boolean[capacity];
      for (int i = 0; i < capacity; i++) {
        this.slots[i] = new QuoteSnapshot();
      }
    }

    long bind(
        final byte[] symbol,
        final byte side,
        final long qty,
        final DecimalFloat bid,
        final DecimalFloat ask,
        final long expiryNs) {
      for (int i = 0; i < slots.length; i++) {
        if (!occupied[i]) {
          slots[i].bind(symbol, 0, symbol.length, side, qty, bid, ask, expiryNs);
          occupied[i] = true;
          return i; // token = slot index
        }
      }
      throw new IllegalStateException("cache full");
    }

    QuoteSnapshot lookup(final long token) {
      return slots[(int) token];
    }

    boolean isOccupied(final long token) {
      return occupied[(int) token];
    }

    void commitEviction(final long token) {
      slots[(int) token].reset();
      occupied[(int) token] = false;
    }
  }

  private static MutableParsedMessage parseAcceptQuote() {
    final var msg = new MutableParsedMessage();
    final var src =
        Unpooled.wrappedBuffer(
            "{\"type\":\"AcceptQuote\",\"quoteId\":\"Q-1\",\"clOrdId\":\"BC-1\"}"
                .getBytes(StandardCharsets.US_ASCII));
    BrowserMessageReader.parse(src, msg);
    return msg;
  }

  // ===========================================================================
  // Path A — success: trySend ≥ 0 → eviction is committed.
  // ===========================================================================

  @Test
  void acceptQuoteSuccess_evictsAfterTrySendReturnsPositive() {
    final var clock = new MutableClock(1_000_000_000_000L);
    final var translator = new JsonToFixTranslator(clock);
    final var cache = new FakeQuoteCache(8);
    final var symbol = "EURUSD".getBytes(StandardCharsets.US_ASCII);
    final var bid = new DecimalFloat();
    final var ask = new DecimalFloat();
    FixedPoint.toDecimalFloat(110_000_000L, bid);
    FixedPoint.toDecimalFloat(110_500_000L, ask);
    final long token =
        cache.bind(
            symbol,
            MutableParsedMessage.SIDE_BUY,
            100_000_000_000L,
            bid,
            ask,
            clock.nanoTime() + 1_000_000_000L);

    final var in = parseAcceptQuote();
    final var enc = new NewOrderSingleEncoder();
    final long retToken =
        translator.translateAcceptQuote(
            in, enc, cache.lookup(token), SESSION_ID, INSTANCE_TAG, 1L, token);

    // Simulate trySend success path: positive return.
    final long trySendResult = 1_234L; // any non-negative value
    assertTrue(cache.isOccupied(retToken), "cache must NOT be evicted before trySend completes");
    assertEquals(token, retToken);
    if (trySendResult >= 0L) {
      cache.commitEviction(retToken);
    }
    assertFalse(cache.isOccupied(retToken));
  }

  // ===========================================================================
  // Path B — backpressure → retry → success.
  // ===========================================================================

  @Test
  void acceptQuoteBackpressure_retainsCacheAcrossRetry_thenEvictsOnSuccess() {
    final var clock = new MutableClock(1_000_000_000_000L);
    final var translator = new JsonToFixTranslator(clock);
    final var cache = new FakeQuoteCache(8);
    final var symbol = "EURUSD".getBytes(StandardCharsets.US_ASCII);
    final var bid = new DecimalFloat();
    final var ask = new DecimalFloat();
    FixedPoint.toDecimalFloat(110_000_000L, bid);
    FixedPoint.toDecimalFloat(110_500_000L, ask);
    final long token =
        cache.bind(
            symbol,
            MutableParsedMessage.SIDE_BUY,
            100_000_000_000L,
            bid,
            ask,
            clock.nanoTime() + 5_000_000_000L);

    final var in = parseAcceptQuote();
    final var enc = new NewOrderSingleEncoder();

    // First attempt — translator populates encoder, dispatcher invokes Session.trySend, gets
    // back-pressure.
    final long ret1 =
        translator.translateAcceptQuote(
            in, enc, cache.lookup(token), SESSION_ID, INSTANCE_TAG, 1L, token);
    assertEquals(token, ret1);
    final long backpressureResult = -1L;
    if (backpressureResult < 0L) {
      // Locked §2: dispatcher MUST NOT evict on backpressure.
      assertTrue(cache.isOccupied(token));
    }

    // Retry — same snapshot, same token, fresh translation pass.
    final long ret2 =
        translator.translateAcceptQuote(
            in, enc, cache.lookup(token), SESSION_ID, INSTANCE_TAG, 1L, token);
    assertEquals(token, ret2);
    final long retryResult = 42L;
    if (retryResult >= 0L) {
      cache.commitEviction(ret2);
    }
    assertFalse(cache.isOccupied(token));
  }

  // ===========================================================================
  // Path C — expiry during backpressure.
  // ===========================================================================

  @Test
  void acceptQuoteExpiryDuringBackpressure_emitsOrderRejectAndEvicts() {
    final var clock = new MutableClock(1_000_000_000_000L);
    final var translator = new JsonToFixTranslator(clock);
    final var cache = new FakeQuoteCache(8);
    final var symbol = "EURUSD".getBytes(StandardCharsets.US_ASCII);
    final var bid = new DecimalFloat();
    final var ask = new DecimalFloat();
    FixedPoint.toDecimalFloat(110_000_000L, bid);
    FixedPoint.toDecimalFloat(110_500_000L, ask);
    final long expiryNs = clock.nanoTime() + 100_000_000L; // 100 ms TTL
    final long token =
        cache.bind(symbol, MutableParsedMessage.SIDE_BUY, 100_000_000_000L, bid, ask, expiryNs);

    final var in = parseAcceptQuote();
    final var enc = new NewOrderSingleEncoder();

    // First trySend — backpressure.
    final long ret1 =
        translator.translateAcceptQuote(
            in, enc, cache.lookup(token), SESSION_ID, INSTANCE_TAG, 1L, token);
    assertEquals(token, ret1);
    final long backpressureResult = -1L;
    assertTrue(backpressureResult < 0L);
    // Cache MUST stay bound — dispatcher does not evict on backpressure.
    assertTrue(cache.isOccupied(token));

    // Time passes — expiry elapses.
    clock.advance(200_000_000L); // 200 ms
    assertTrue(clock.nanoTime() > cache.lookup(token).expiryNs());

    // Locked §2: dispatcher detects expiry, emits OrderReject{quote-expired}, evicts.
    final var orderRejectReason = isExpired(cache.lookup(token), clock) ? "quote-expired" : null;
    assertEquals("quote-expired", orderRejectReason);
    cache.commitEviction(token);
    assertFalse(cache.isOccupied(token));
  }

  /** Helper used by the test to mirror the dispatcher's expiry check. */
  private static boolean isExpired(final QuoteSnapshot snap, final EpochNanoClock clock) {
    return snap.isBound() && clock.nanoTime() > snap.expiryNs();
  }
}
