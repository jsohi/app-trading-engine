package com.trading.engine.pricing.quote;

import org.agrona.DirectBuffer;

/**
 * Mutable flyweight for an active quote. Pre-allocated in the {@link QuoteManager} pool -- never
 * constructed on the hot path.
 *
 * <p>Each instance holds the full state of a single outstanding quote: the originating request
 * identifiers (quoteReqId, symbol, accountCode), the two-way price (bid/offer), the quoted sizes,
 * the expiry timestamp, the mid-rate snapshot captured at quote time (for last-look validation),
 * and a monotonic creation timestamp used for FIFO eviction ordering when the pool is exhausted.
 *
 * <p>Fields are populated via {@link #populate} from SBE-decoded {@link DirectBuffer} contents. The
 * fixed-size byte arrays (quoteReqId, symbol, accountCode) match the SBE schema type lengths
 * exactly -- no heap allocation occurs after the pool is initialised.
 *
 * <h3>Design rationale</h3>
 *
 * <p>The flyweight/pool pattern avoids per-quote allocation on the hot path, following the same
 * idiom used by SBE decoders and Aeron's internal flyweights. The pricing service pre-allocates
 * {@code maxActiveQuotes} entries at startup and recycles them in a round-robin fashion. This is
 * the standard approach in low-latency FX pricing engines (e.g., exchange-core, LMAX).
 *
 * <h3>Threading model</h3>
 *
 * <p><b>Not thread-safe.</b> A single instance is accessed exclusively from the pricing-service
 * agent's single-threaded duty cycle. All field mutations ({@link #populate}, {@link #reset}) and
 * reads ({@link #isExpired}) occur on the same thread -- no synchronisation is required.
 *
 * <h3>Allocation behaviour</h3>
 *
 * <p>Zero allocation after construction. The three byte arrays are allocated once in the
 * constructor and reused across quote lifecycles via {@link #populate}. {@link
 * DirectBuffer#getBytes} copies directly into the pre-allocated arrays without intermediate
 * objects.
 *
 * @see QuoteManager
 * @see PriceValidator
 */
public final class QuoteEntry {

  /**
   * Fixed byte length of the SBE {@code QuoteReqID} type. Matches {@code <type name="QuoteReqID"
   * primitiveType="char" length="20"/>}.
   */
  static final int QUOTE_REQ_ID_LENGTH = 20;

  /**
   * Fixed byte length of the SBE {@code Symbol} type. Matches {@code <type name="Symbol"
   * primitiveType="char" length="8"/>}.
   */
  static final int SYMBOL_LENGTH = 8;

  /**
   * Fixed byte length of the SBE {@code Account} type. Matches {@code <type name="Account"
   * primitiveType="char" length="16"/>}.
   */
  static final int ACCOUNT_CODE_LENGTH = 16;

  /**
   * Quote request identifier -- 20-byte fixed-width field matching SBE QuoteReqID (FIX tag 131).
   * Populated by {@link #populate} from the decoded SBE buffer.
   */
  final byte[] quoteReqId = new byte[QUOTE_REQ_ID_LENGTH];

  /**
   * Instrument symbol -- 8-byte fixed-width field matching SBE Symbol (FIX tag 55). Populated by
   * {@link #populate} from the decoded SBE buffer.
   */
  final byte[] symbol = new byte[SYMBOL_LENGTH];

  /**
   * Account code -- 16-byte fixed-width field matching SBE Account (FIX tag 1). Populated by {@link
   * #populate} from the decoded SBE buffer.
   */
  final byte[] accountCode = new byte[ACCOUNT_CODE_LENGTH];

  /** Bid price in fixed-point {@code 10^-8} representation. */
  long bidPx;

  /** Offer price in fixed-point {@code 10^-8} representation. */
  long offerPx;

  /** Bid size (quantity available on the bid side) in fixed-point {@code 10^-8}. */
  long bidSize;

  /** Offer size (quantity available on the offer side) in fixed-point {@code 10^-8}. */
  long offerSize;

  /**
   * Epoch nanosecond timestamp after which this quote is no longer valid. Compared against the
   * {@link com.trading.engine.messages.clock.TradingClocks#epochNanoClock()} time supplied to
   * {@link #isExpired(long)}.
   */
  long validUntil;

  /**
   * Mid-rate snapshot captured at quote creation time, in fixed-point {@code 10^-8}. Used by {@link
   * PriceValidator} for last-look validation -- if the market mid has moved beyond the configured
   * tolerance since this snapshot, the quote execution is rejected.
   */
  long midRateAtQuoteTime;

  /**
   * Monotonic nanosecond timestamp (from {@link org.agrona.concurrent.NanoClock}) recorded when
   * this entry was populated. Used by {@link QuoteManager#expireStale(long)} and the pool eviction
   * scan to identify the oldest entry for FIFO replacement.
   */
  long creationNanos;

  /**
   * Actual length of meaningful bytes in {@link #quoteReqId}. The SBE field is fixed-width (20
   * bytes) but the identifier may be shorter, with trailing null/space padding. This length is
   * needed for correct {@link com.trading.engine.pricing.ByteArrayKey} comparison.
   */
  int quoteReqIdLength;

  /**
   * Actual length of meaningful bytes in {@link #symbol}. Stored for the same reason as {@link
   * #quoteReqIdLength}.
   */
  int symbolLength;

  /**
   * Actual length of meaningful bytes in {@link #accountCode}. Stored for the same reason as {@link
   * #quoteReqIdLength}.
   */
  int accountCodeLength;

  /**
   * Constructs a quote entry with pre-allocated byte arrays. All fields are zeroed. The entry is
   * not valid until {@link #populate} is called.
   *
   * <p><b>Allocation:</b> allocates three byte arrays (quoteReqId[20], symbol[8], accountCode[16]).
   * This constructor is called once per pool slot at startup.
   */
  public QuoteEntry() {
    // Byte arrays are already zero-initialised by the JVM.
  }

  /**
   * Fills all fields from SBE-decoded buffers and scalar values. Copies identifier bytes into the
   * pre-allocated arrays via {@link DirectBuffer#getBytes(int, byte[], int, int)}, avoiding any
   * heap allocation.
   *
   * <p>This method overwrites all state in the entry. The caller must not assume any prior state
   * survives a {@code populate} call -- the entry is fully reinitialised.
   *
   * <p><b>Allocation:</b> zero allocation.
   *
   * @param quoteReqIdBuf buffer containing the QuoteReqID bytes (FIX tag 131)
   * @param qrOff start offset of the QuoteReqID within the buffer
   * @param qrLen number of meaningful bytes in the QuoteReqID; must be {@code <=
   *     QUOTE_REQ_ID_LENGTH}
   * @param symbolBuf buffer containing the Symbol bytes (FIX tag 55)
   * @param sOff start offset of the Symbol within the buffer
   * @param sLen number of meaningful bytes in the Symbol; must be {@code <= SYMBOL_LENGTH}
   * @param accountBuf buffer containing the Account code bytes (FIX tag 1)
   * @param aOff start offset of the Account within the buffer
   * @param aLen number of meaningful bytes in the Account; must be {@code <= ACCOUNT_CODE_LENGTH}
   * @param bidPx bid price in fixed-point {@code 10^-8}
   * @param offerPx offer price in fixed-point {@code 10^-8}
   * @param bidSize bid size in fixed-point {@code 10^-8}
   * @param offerSize offer size in fixed-point {@code 10^-8}
   * @param validUntil epoch nanos after which this quote expires
   * @param midRateAtQuoteTime mid-rate snapshot in fixed-point {@code 10^-8} for last-look
   * @param creationNanos monotonic nanos (from {@link org.agrona.concurrent.NanoClock}) for FIFO
   *     ordering
   */
  public void populate(
      final DirectBuffer quoteReqIdBuf,
      final int qrOff,
      final int qrLen,
      final DirectBuffer symbolBuf,
      final int sOff,
      final int sLen,
      final DirectBuffer accountBuf,
      final int aOff,
      final int aLen,
      final long bidPx,
      final long offerPx,
      final long bidSize,
      final long offerSize,
      final long validUntil,
      final long midRateAtQuoteTime,
      final long creationNanos) {
    quoteReqIdBuf.getBytes(qrOff, this.quoteReqId, 0, qrLen);
    this.quoteReqIdLength = qrLen;

    symbolBuf.getBytes(sOff, this.symbol, 0, sLen);
    this.symbolLength = sLen;

    accountBuf.getBytes(aOff, this.accountCode, 0, aLen);
    this.accountCodeLength = aLen;

    this.bidPx = bidPx;
    this.offerPx = offerPx;
    this.bidSize = bidSize;
    this.offerSize = offerSize;
    this.validUntil = validUntil;
    this.midRateAtQuoteTime = midRateAtQuoteTime;
    this.creationNanos = creationNanos;
  }

  /**
   * Checks whether this quote has expired relative to the given wall-clock time.
   *
   * <p><b>Allocation:</b> zero allocation.
   *
   * @param nowEpochNanos the current epoch nanosecond time from {@link
   *     com.trading.engine.messages.clock.TradingClocks#epochNanoClock()}
   * @return {@code true} if the current time is at or past the quote's {@link #validUntil} deadline
   */
  public boolean isExpired(final long nowEpochNanos) {
    return nowEpochNanos >= validUntil;
  }

  /**
   * Resets all fields to their zero/default values. Called when the entry is returned to the pool
   * or evicted. Not strictly required (the next {@link #populate} overwrites everything) but
   * provides a clean state for debugging.
   *
   * <p><b>Allocation:</b> zero allocation.
   */
  public void reset() {
    java.util.Arrays.fill(quoteReqId, (byte) 0);
    java.util.Arrays.fill(symbol, (byte) 0);
    java.util.Arrays.fill(accountCode, (byte) 0);
    quoteReqIdLength = 0;
    symbolLength = 0;
    accountCodeLength = 0;
    bidPx = 0L;
    offerPx = 0L;
    bidSize = 0L;
    offerSize = 0L;
    validUntil = 0L;
    midRateAtQuoteTime = 0L;
    creationNanos = 0L;
  }
}
