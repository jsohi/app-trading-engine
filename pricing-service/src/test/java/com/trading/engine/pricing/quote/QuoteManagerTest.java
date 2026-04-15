package com.trading.engine.pricing.quote;

import static com.trading.engine.testsupport.buffer.SbeFieldUtil.zeroPad;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.trading.engine.testsupport.buffer.SbeFieldUtil;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link QuoteManager} — the pre-allocated pool and lookup map that tracks active
 * quotes keyed by QuoteReqID (FIX tag 131).
 *
 * <p>Tests cover the core quote lifecycle: allocation and retrieval, quote refresh (same quoteReqId
 * replaces old entry), stale expiry, retention of active quotes, unknown key lookups, and FIFO pool
 * eviction when the pool is exhausted. The pool size is intentionally small ({@code maxActiveQuotes
 * = 3} for eviction tests) to exercise boundary conditions without requiring large data volumes.
 */
class QuoteManagerTest {

  /** Default pool size for most tests — large enough to avoid eviction. */
  private static final int DEFAULT_POOL_SIZE = 16;

  /** Small pool size to exercise eviction behaviour. */
  private static final int SMALL_POOL_SIZE = 3;

  /** Symbol bytes — 8-byte fixed-width SBE Symbol. */
  private static final byte[] SYMBOL_BYTES = zeroPad("EURUSD", SbeFieldUtil.SYMBOL_LENGTH);

  /** Account code bytes — 16-byte fixed-width SBE Account. */
  private static final byte[] ACCOUNT_BYTES = zeroPad("ACME-001", 16);

  private QuoteManager manager;

  @BeforeEach
  void setUp() {
    manager = new QuoteManager(DEFAULT_POOL_SIZE);
  }

  /**
   * Store a quote via allocateAndStore, populate it, then look it up by quoteReqId — the returned
   * entry must be the same object with the populated fields intact.
   */
  @Test
  void allocateAndStore_newQuote_retrievable() {
    final byte[] qrIdBytes = zeroPad("QR-001", QuoteEntry.QUOTE_REQ_ID_LENGTH);
    final UnsafeBuffer qrBuf = new UnsafeBuffer(qrIdBytes);
    final UnsafeBuffer symBuf = new UnsafeBuffer(SYMBOL_BYTES);
    final UnsafeBuffer accBuf = new UnsafeBuffer(ACCOUNT_BYTES);

    final QuoteEntry entry = manager.allocateAndStore(qrBuf, 0, qrIdBytes.length);
    entry.populate(
        qrBuf,
        0,
        qrIdBytes.length,
        symBuf,
        0,
        SYMBOL_BYTES.length,
        accBuf,
        0,
        ACCOUNT_BYTES.length,
        /* bidPx= */ 108_400_000L,
        /* offerPx= */ 108_600_000L,
        /* bidSize= */ 1_000_000_00_000_000L,
        /* offerSize= */ 1_000_000_00_000_000L,
        /* validUntil= */ Long.MAX_VALUE,
        /* midRateAtQuoteTime= */ 108_500_000L,
        /* creationNanos= */ 1_000_000L);

    final QuoteEntry looked = manager.lookup(qrBuf, 0, qrIdBytes.length);
    assertNotNull(looked, "Stored quote must be retrievable by quoteReqId");
    assertEquals(108_400_000L, looked.bidPx);
    assertEquals(108_600_000L, looked.offerPx);
    assertEquals(1, manager.size());
  }

  /**
   * Storing a second quote with the same quoteReqId replaces the first (FIX quote refresh
   * semantics). The map size remains 1 and the lookup returns the new entry's data.
   */
  @Test
  void allocateAndStore_refresh_replacesOld() {
    final byte[] qrIdBytes = zeroPad("QR-REFRESH", QuoteEntry.QUOTE_REQ_ID_LENGTH);
    final UnsafeBuffer qrBuf = new UnsafeBuffer(qrIdBytes);
    final UnsafeBuffer symBuf = new UnsafeBuffer(SYMBOL_BYTES);
    final UnsafeBuffer accBuf = new UnsafeBuffer(ACCOUNT_BYTES);

    // First store.
    final QuoteEntry first = manager.allocateAndStore(qrBuf, 0, qrIdBytes.length);
    first.populate(
        qrBuf,
        0,
        qrIdBytes.length,
        symBuf,
        0,
        SYMBOL_BYTES.length,
        accBuf,
        0,
        ACCOUNT_BYTES.length,
        /* bidPx= */ 100_000_000L,
        /* offerPx= */ 101_000_000L,
        /* bidSize= */ 500_000_00_000_000L,
        /* offerSize= */ 500_000_00_000_000L,
        /* validUntil= */ Long.MAX_VALUE,
        /* midRateAtQuoteTime= */ 100_500_000L,
        /* creationNanos= */ 1_000_000L);

    // Second store with same quoteReqId — refresh.
    final QuoteEntry second = manager.allocateAndStore(qrBuf, 0, qrIdBytes.length);
    second.populate(
        qrBuf,
        0,
        qrIdBytes.length,
        symBuf,
        0,
        SYMBOL_BYTES.length,
        accBuf,
        0,
        ACCOUNT_BYTES.length,
        /* bidPx= */ 200_000_000L,
        /* offerPx= */ 201_000_000L,
        /* bidSize= */ 750_000_00_000_000L,
        /* offerSize= */ 750_000_00_000_000L,
        /* validUntil= */ Long.MAX_VALUE,
        /* midRateAtQuoteTime= */ 200_500_000L,
        /* creationNanos= */ 2_000_000L);

    final QuoteEntry looked = manager.lookup(qrBuf, 0, qrIdBytes.length);
    assertNotNull(looked, "Refreshed quote must still be retrievable");
    assertEquals(200_000_000L, looked.bidPx, "Refreshed quote must have updated bidPx");
    assertEquals(1, manager.size(), "Refresh must not increase active quote count");
  }

  /**
   * A quote whose validUntil has elapsed is removed by expireStale. After expiry the lookup returns
   * null and the size decreases.
   */
  @Test
  void expireStale_expiredQuote_removed() {
    final byte[] qrIdBytes = zeroPad("QR-EXPIRED", QuoteEntry.QUOTE_REQ_ID_LENGTH);
    final UnsafeBuffer qrBuf = new UnsafeBuffer(qrIdBytes);
    final UnsafeBuffer symBuf = new UnsafeBuffer(SYMBOL_BYTES);
    final UnsafeBuffer accBuf = new UnsafeBuffer(ACCOUNT_BYTES);

    final QuoteEntry entry = manager.allocateAndStore(qrBuf, 0, qrIdBytes.length);
    // validUntil in the past: epoch nanos = 1000, we will call expireStale with now = 2000.
    entry.populate(
        qrBuf,
        0,
        qrIdBytes.length,
        symBuf,
        0,
        SYMBOL_BYTES.length,
        accBuf,
        0,
        ACCOUNT_BYTES.length,
        /* bidPx= */ 108_400_000L,
        /* offerPx= */ 108_600_000L,
        /* bidSize= */ 1_000_000_00_000_000L,
        /* offerSize= */ 1_000_000_00_000_000L,
        /* validUntil= */ 1_000L,
        /* midRateAtQuoteTime= */ 108_500_000L,
        /* creationNanos= */ 500L);

    assertEquals(1, manager.size());

    final int expired = manager.expireStale(2_000L);

    assertEquals(1, expired, "One quote should have been expired");
    assertEquals(0, manager.size(), "No active quotes should remain");
    assertNull(manager.lookup(qrBuf, 0, qrIdBytes.length), "Expired quote must not be retrievable");
  }

  /**
   * A quote whose validUntil is in the future is retained by expireStale — still retrievable and
   * counted.
   */
  @Test
  void expireStale_activeQuote_retained() {
    final byte[] qrIdBytes = zeroPad("QR-ACTIVE", QuoteEntry.QUOTE_REQ_ID_LENGTH);
    final UnsafeBuffer qrBuf = new UnsafeBuffer(qrIdBytes);
    final UnsafeBuffer symBuf = new UnsafeBuffer(SYMBOL_BYTES);
    final UnsafeBuffer accBuf = new UnsafeBuffer(ACCOUNT_BYTES);

    final QuoteEntry entry = manager.allocateAndStore(qrBuf, 0, qrIdBytes.length);
    // validUntil far in the future — this quote is still active.
    entry.populate(
        qrBuf,
        0,
        qrIdBytes.length,
        symBuf,
        0,
        SYMBOL_BYTES.length,
        accBuf,
        0,
        ACCOUNT_BYTES.length,
        /* bidPx= */ 108_400_000L,
        /* offerPx= */ 108_600_000L,
        /* bidSize= */ 1_000_000_00_000_000L,
        /* offerSize= */ 1_000_000_00_000_000L,
        /* validUntil= */ Long.MAX_VALUE,
        /* midRateAtQuoteTime= */ 108_500_000L,
        /* creationNanos= */ 500L);

    final int expired = manager.expireStale(2_000L);

    assertEquals(0, expired, "No quotes should have been expired");
    assertEquals(1, manager.size(), "Active quote must still be counted");
    assertNotNull(
        manager.lookup(qrBuf, 0, qrIdBytes.length), "Active quote must still be retrievable");
  }

  /**
   * Looking up a quoteReqId that was never stored returns null — the safe default for unknown
   * quotes.
   */
  @Test
  void lookup_unknownQuoteReqId_returnsNull() {
    final byte[] qrIdBytes = zeroPad("QR-UNKNOWN", QuoteEntry.QUOTE_REQ_ID_LENGTH);
    final UnsafeBuffer qrBuf = new UnsafeBuffer(qrIdBytes);

    final QuoteEntry result = manager.lookup(qrBuf, 0, qrIdBytes.length);

    assertNull(result, "Lookup of unknown quoteReqId must return null");
  }

  /**
   * When the pool is full (all slots in use), the next allocation evicts the oldest entry (FIFO
   * round-robin). With a pool size of 3, inserting a 4th quote should evict the 1st.
   */
  @Test
  void poolEviction_fullPool_evictsOldest() {
    final QuoteManager smallManager = new QuoteManager(SMALL_POOL_SIZE);
    final UnsafeBuffer symBuf = new UnsafeBuffer(SYMBOL_BYTES);
    final UnsafeBuffer accBuf = new UnsafeBuffer(ACCOUNT_BYTES);

    // Fill all 3 pool slots.
    for (int i = 0; i < SMALL_POOL_SIZE; i++) {
      final byte[] qrId = zeroPad("QR-" + i, QuoteEntry.QUOTE_REQ_ID_LENGTH);
      final UnsafeBuffer qrBuf = new UnsafeBuffer(qrId);
      final QuoteEntry entry = smallManager.allocateAndStore(qrBuf, 0, qrId.length);
      entry.populate(
          qrBuf,
          0,
          qrId.length,
          symBuf,
          0,
          SYMBOL_BYTES.length,
          accBuf,
          0,
          ACCOUNT_BYTES.length,
          /* bidPx= */ 100_000_000L + i,
          /* offerPx= */ 101_000_000L + i,
          /* bidSize= */ 1_000_000_00_000_000L,
          /* offerSize= */ 1_000_000_00_000_000L,
          /* validUntil= */ Long.MAX_VALUE,
          /* midRateAtQuoteTime= */ 100_500_000L,
          /* creationNanos= */ i * 1_000_000L);
    }

    assertEquals(SMALL_POOL_SIZE, smallManager.size(), "Pool should be full");

    // Insert a 4th quote — this must evict pool slot 0 (QR-0).
    final byte[] qr4Bytes = zeroPad("QR-NEW", QuoteEntry.QUOTE_REQ_ID_LENGTH);
    final UnsafeBuffer qr4Buf = new UnsafeBuffer(qr4Bytes);
    final QuoteEntry newEntry = smallManager.allocateAndStore(qr4Buf, 0, qr4Bytes.length);
    newEntry.populate(
        qr4Buf,
        0,
        qr4Bytes.length,
        symBuf,
        0,
        SYMBOL_BYTES.length,
        accBuf,
        0,
        ACCOUNT_BYTES.length,
        /* bidPx= */ 999_000_000L,
        /* offerPx= */ 999_100_000L,
        /* bidSize= */ 1_000_000_00_000_000L,
        /* offerSize= */ 1_000_000_00_000_000L,
        /* validUntil= */ Long.MAX_VALUE,
        /* midRateAtQuoteTime= */ 999_050_000L,
        /* creationNanos= */ 100_000_000L);

    // The evicted slot 0 (QR-0) should no longer be retrievable.
    final byte[] qr0Bytes = zeroPad("QR-0", QuoteEntry.QUOTE_REQ_ID_LENGTH);
    final UnsafeBuffer qr0Buf = new UnsafeBuffer(qr0Bytes);
    assertNull(
        smallManager.lookup(qr0Buf, 0, qr0Bytes.length),
        "Evicted quote (QR-0) must not be retrievable after pool wrap");

    // The new quote should be retrievable.
    assertNotNull(
        smallManager.lookup(qr4Buf, 0, qr4Bytes.length),
        "Newly inserted quote must be retrievable");

    // QR-1 and QR-2 should still be present.
    final byte[] qr1Bytes = zeroPad("QR-1", QuoteEntry.QUOTE_REQ_ID_LENGTH);
    final UnsafeBuffer qr1Buf = new UnsafeBuffer(qr1Bytes);
    assertNotNull(
        smallManager.lookup(qr1Buf, 0, qr1Bytes.length),
        "QR-1 must survive eviction (slot 1 not yet recycled)");

    final byte[] qr2Bytes = zeroPad("QR-2", QuoteEntry.QUOTE_REQ_ID_LENGTH);
    final UnsafeBuffer qr2Buf = new UnsafeBuffer(qr2Bytes);
    assertNotNull(
        smallManager.lookup(qr2Buf, 0, qr2Bytes.length),
        "QR-2 must survive eviction (slot 2 not yet recycled)");

    // Size should still be 3 — one evicted, one added.
    assertEquals(SMALL_POOL_SIZE, smallManager.size(), "Pool size must remain at capacity");
  }
}
