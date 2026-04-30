package com.trading.engine.fixbridge.translator;

import uk.co.real_logic.artio.fields.DecimalFloat;

/**
 * Mutable per-session quote-cache entry. Holds the structural fields of an inbound dealer {@code
 * Quote} needed to translate a subsequent {@code AcceptQuote} into a FIX {@code NewOrderSingle}
 * with {@code OrdType=D (Previously Quoted)}.
 *
 * <p><b>Purpose.</b> Implements the per-session quote cache backing the locked §2 two-phase commit
 * for {@code AcceptQuote}: when a dealer {@code Quote} arrives the bridge populates a snapshot
 * keyed by {@code quoteId}; when the browser sends {@code AcceptQuote{quoteId}} the translator
 * pulls the snapshot, mints a {@code NewOrderSingle (35=D)} with all fields sourced from it, and
 * returns a token that lets the dispatcher evict the cache entry only AFTER {@code
 * Session.trySend(...) >= 0}. If {@code trySend} returns backpressure the entry stays in the cache
 * for the retry; if the entry's {@code expiryNs} elapses while waiting the dispatcher emits {@code
 * OrderReject{quoteId, reason:"quote-expired"}} and evicts.
 *
 * <p><b>Threading.</b> Not thread-safe. OWNED by exactly one {@code BrowserSession} which is in
 * turn pinned to a single Netty worker; concurrent access is a bug. Phase 5 wires the ownership.
 *
 * <p><b>Allocation.</b> Once at construction. The {@link #symbolBytes} array, the two {@link
 * DecimalFloat} instances, and all primitive headers are reused across rebinds. {@link #reset()} is
 * the public mutator that puts the snapshot in an "empty/free" state.
 *
 * <p><b>Lifecycle.</b> One pool of fixed size {@code quoteCacheCapacityPerSession} per browser
 * session, allocated at session creation in Phase 5. Released to GC when the session is closed.
 *
 * <p><b>Dependencies.</b> Artio's {@link DecimalFloat} only.
 *
 * <p><b>Symbol byte storage.</b> {@link #symbolBytes} is sized to the maximum FIX 4.4 {@code Symbol
 * (55)} length recorded in production protocols — 32 bytes covers every spot/forward FX pair
 * (typically 6 chars), every equity ticker (typically &le;12 chars), and every CME futures contract
 * spec (typically &le;20 chars). The actual length is recorded in {@link #symbolLen} so downstream
 * consumers slice {@code symbolBytes[0..symbolLen)}; bytes beyond that are stale.
 */
public final class QuoteSnapshot {

  /**
   * Maximum bytes reserved for the FIX {@code Symbol (55)} value. 32 is comfortably above the
   * widest production ticker length (~20 bytes for CME futures) and matches a power-of-two cache
   * line stride.
   */
  public static final int SYMBOL_CAPACITY = 32;

  /** Sentinel for {@link #symbolLen} indicating the snapshot is empty / free. */
  public static final int FREE = -1;

  // ---------------------------------------------------------------------------
  // Storage. Public final on the byte[] / DecimalFloat references because each
  // hot-path access is read-only after the snapshot is bound, and the lack of
  // accessor overhead matches the established RetainedEvent / MutableParsedMessage
  // idioms in this module. Mutable scalars are private with explicit getters.
  // ---------------------------------------------------------------------------

  /**
   * Symbol bytes scratch. Length valid in {@code [0, symbolLen)}; bytes beyond are stale and MUST
   * NOT be read.
   */
  public final byte[] symbolBytes = new byte[SYMBOL_CAPACITY];

  /**
   * FIX {@code BidPx (132)} flyweight (price the dealer is willing to BUY at). Reused across
   * snapshot rebinds via {@link DecimalFloat#set(long, int)}; never reallocated.
   */
  public final DecimalFloat bid = new DecimalFloat();

  /**
   * FIX {@code OfferPx (133)} flyweight (price the dealer is willing to SELL at). Reused across
   * snapshot rebinds via {@link DecimalFloat#set(long, int)}; never reallocated.
   */
  public final DecimalFloat ask = new DecimalFloat();

  /** Number of valid bytes in {@link #symbolBytes}; {@link #FREE} when the snapshot is empty. */
  private int symbolLen = FREE;

  /** FIX {@code Side (54)} byte ({@code '1'} = Buy, {@code '2'} = Sell); {@code 0} when free. */
  private byte side;

  /** Quantity, fixed-point int64 (scale {@code 10^-8}). */
  private long qtyInt64;

  /** Absolute epoch-nanosecond TTL after which {@code AcceptQuote} must be rejected. */
  private long expiryNs;

  /** Constructs an empty snapshot. Allocates only the immutable child references. */
  public QuoteSnapshot() {}

  /**
   * Bind this snapshot to the structural fields of an inbound {@code Quote}.
   *
   * @param symbolSrc source array carrying the FIX {@code Symbol} bytes
   * @param symbolOff offset into {@code symbolSrc}
   * @param symbolLen number of bytes ({@code 0 < symbolLen <= SYMBOL_CAPACITY})
   * @param side FIX {@code Side (54)} byte ({@code '1'} or {@code '2'})
   * @param qtyInt64 fixed-point int64 quantity (scale {@code 10^-8})
   * @param bidValue {@link DecimalFloat#value()} for the bid price
   * @param bidScale {@link DecimalFloat#scale()} for the bid price
   * @param askValue {@link DecimalFloat#value()} for the offer price
   * @param askScale {@link DecimalFloat#scale()} for the offer price
   * @param expiryNs absolute epoch-nanosecond expiry; the caller is responsible for ensuring it is
   *     positive and ahead of {@code wallClock.nanoTime()}. The snapshot stores the value verbatim;
   *     expiry semantics are evaluated by the dispatcher at AcceptQuote time
   * @throws IllegalArgumentException if {@code symbolLen} is non-positive or exceeds {@link
   *     #SYMBOL_CAPACITY}
   */
  public void bind(
      final byte[] symbolSrc,
      final int symbolOff,
      final int symbolLen,
      final byte side,
      final long qtyInt64,
      final long bidValue,
      final int bidScale,
      final long askValue,
      final int askScale,
      final long expiryNs) {
    if (symbolLen <= 0 || symbolLen > SYMBOL_CAPACITY) {
      throw new IllegalArgumentException(
          "symbolLen out of range: symbolLen=" + symbolLen + " capacity=" + SYMBOL_CAPACITY);
    }
    System.arraycopy(symbolSrc, symbolOff, this.symbolBytes, 0, symbolLen);
    this.symbolLen = symbolLen;
    this.side = side;
    this.qtyInt64 = qtyInt64;
    this.bid.set(bidValue, bidScale);
    this.ask.set(askValue, askScale);
    this.expiryNs = expiryNs;
  }

  /**
   * Bind this snapshot using two pre-populated {@link DecimalFloat} flyweights for the bid / ask
   * prices. Convenience overload — copies the {@code (value, scale)} pair from the supplied {@code
   * DecimalFloat}s into the snapshot's owned instances. The caller's {@code DecimalFloat}s may be
   * mutated freely after this call.
   *
   * @param symbolSrc source array carrying the FIX {@code Symbol} bytes
   * @param symbolOff offset into {@code symbolSrc}
   * @param symbolLen number of bytes ({@code 0 < symbolLen <= SYMBOL_CAPACITY})
   * @param side FIX {@code Side (54)} byte ({@code '1'} or {@code '2'})
   * @param qtyInt64 fixed-point int64 quantity (scale {@code 10^-8})
   * @param bid bid {@link DecimalFloat}; {@code null} treated as zero-value
   * @param ask ask {@link DecimalFloat}; {@code null} treated as zero-value
   * @param expiryNs absolute epoch-nanosecond expiry; the caller is responsible for ensuring it is
   *     positive and ahead of {@code wallClock.nanoTime()}. The snapshot stores the value verbatim;
   *     expiry semantics are evaluated by the dispatcher at AcceptQuote time
   * @throws IllegalArgumentException if {@code symbolLen} is non-positive or exceeds {@link
   *     #SYMBOL_CAPACITY}
   */
  public void bind(
      final byte[] symbolSrc,
      final int symbolOff,
      final int symbolLen,
      final byte side,
      final long qtyInt64,
      final DecimalFloat bid,
      final DecimalFloat ask,
      final long expiryNs) {
    final long bidValue = bid == null ? 0L : bid.value();
    final int bidScale = bid == null ? 0 : bid.scale();
    final long askValue = ask == null ? 0L : ask.value();
    final int askScale = ask == null ? 0 : ask.scale();
    bind(
        symbolSrc, symbolOff, symbolLen, side, qtyInt64, bidValue, bidScale, askValue, askScale,
        expiryNs);
  }

  /** Reset to the free / empty state. Reference fields ({@link DecimalFloat}) are reused. */
  public void reset() {
    this.symbolLen = FREE;
    this.side = 0;
    this.qtyInt64 = 0L;
    // DecimalFloat.set(0,0) puts it in canonical zero state — we cannot null it (it's a final
    // instance) and don't want to allocate a fresh one.
    this.bid.set(0L, 0);
    this.ask.set(0L, 0);
    this.expiryNs = 0L;
  }

  /**
   * @return {@code true} when this snapshot holds a bound quote (i.e. has been written to since the
   *     most recent {@link #reset()}).
   */
  public boolean isBound() {
    return symbolLen != FREE;
  }

  /**
   * @return number of valid bytes in {@link #symbolBytes}; {@link #FREE} when unbound.
   */
  public int symbolLen() {
    return symbolLen;
  }

  /**
   * @return FIX {@code Side (54)} byte; {@code 0} when unbound.
   */
  public byte side() {
    return side;
  }

  /**
   * @return fixed-point int64 quantity (scale {@code 10^-8}).
   */
  public long qtyInt64() {
    return qtyInt64;
  }

  /**
   * @return absolute epoch-nanosecond expiry; {@code 0} when unbound.
   */
  public long expiryNs() {
    return expiryNs;
  }
}
