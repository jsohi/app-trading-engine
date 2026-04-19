package com.trading.engine.orchestrator;

import com.trading.engine.messages.sbe.PriceResponseDecoder;
import com.trading.engine.messages.sbe.QuoteRequestDecoder;
import com.trading.engine.messages.util.ByteArrayKey;
import java.util.Arrays;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

/**
 * Pre-allocated mutable flyweight representing a single RFQ lifecycle within the orchestrator's
 * object pool. Each pool slot is an {@code RfqState} instance that is acquired when a QuoteRequest
 * arrives and released when the RFQ reaches a terminal state ({@link State#COMPLETED}, {@link
 * State#REJECTED}, {@link State#EXPIRED}).
 *
 * <p><b>Memory layout.</b> Uses the flat-buffer pattern for cache efficiency: all variable-length
 * byte-array fields (quoteReqId, quoteId, symbol, accountCode, settlDate, currency, settlCurrency,
 * and the NOS stash buffer) are packed into a single contiguous {@code byte[]} backing buffer at
 * named offset constants. Primitive fields (state, expiryNanos, orderQty, prices) remain as Java
 * fields. This reduces per-slot heap objects to 1 byte array + 2 {@link ByteArrayKey} objects = 3
 * objects (vs ~12 with individual arrays). At 10,000 pool entries: ~30K heap objects instead of
 * ~120K.
 *
 * <p><b>Threading:</b> not thread-safe — single-threaded orchestrator duty cycle only.
 *
 * <p><b>Allocation:</b> zero allocation after construction. All byte arrays and ByteArrayKey
 * instances are pre-allocated; {@code populateFromQuoteRequest}, {@code applyPriceResponse}, and
 * {@code stashNos} only write into the pre-existing flat buffer.
 *
 * @see RfqStateMachine
 * @see OrchestratorService
 */
public final class RfqState {

  // ===========================================================================
  // RFQ lifecycle states
  // ===========================================================================

  /**
   * RFQ lifecycle states, aligned with {@code docs/state-machines.md} naming.
   *
   * <ul>
   *   <li>{@link #FREE} — pool slot available for reuse (not in any map)
   *   <li>{@link #PENDING_PRICE} — maps to "Requested": QuoteRequest received, PriceRequest
   *       forwarded to pricing service, awaiting PriceResponse
   *   <li>{@link #QUOTED} — maps to "Quoted": PriceResponse accepted, Quote sent to client,
   *       awaiting NOS with quoteId (AcceptQuote)
   *   <li>{@link #PENDING_VALIDATION} — maps to "Accepted": NOS with quoteId received,
   *       PriceValidationRequest sent to pricing, awaiting PriceValidationResponse
   *   <li>{@link #COMPLETED} — maps to "Filled": validation passed, NOS forwarded to cluster
   *       (terminal)
   *   <li>{@link #REJECTED} — maps to "Rejected": pricing declined or validation failed (terminal)
   *   <li>{@link #EXPIRED} — maps to "Expired": per-state timeout elapsed (terminal)
   * </ul>
   */
  public enum State {
    FREE,
    PENDING_PRICE,
    QUOTED,
    PENDING_VALIDATION,
    COMPLETED,
    REJECTED,
    EXPIRED
  }

  // ===========================================================================
  // Flat buffer layout — named offset constants
  // ===========================================================================

  /** Offset into the flat buffer for the 20-byte QuoteReqID field (FIX tag 131). */
  static final int QUOTE_REQ_ID_OFFSET = 0;

  /** Length of the QuoteReqID field in the flat buffer. Matches SBE QuoteReqID char[20]. */
  static final int QUOTE_REQ_ID_LENGTH = 20;

  /** Offset into the flat buffer for the 20-byte QuoteID field (FIX tag 117). */
  static final int QUOTE_ID_OFFSET = 20;

  /** Length of the QuoteID field in the flat buffer. Matches SBE QuoteID char[20]. */
  static final int QUOTE_ID_LENGTH = 20;

  /** Offset into the flat buffer for the 8-byte Symbol field (FIX tag 55). */
  static final int SYMBOL_OFFSET = 40;

  /** Length of the Symbol field. Matches SBE Symbol char[8]. */
  static final int SYMBOL_LENGTH = 8;

  /** Offset into the flat buffer for the 16-byte AccountCode field (FIX tag 1). */
  static final int ACCOUNT_CODE_OFFSET = 48;

  /** Length of the AccountCode field. Matches SBE Account char[16]. */
  static final int ACCOUNT_CODE_LENGTH = 16;

  /** Offset into the flat buffer for the 8-byte SettlDate field (FIX tag 64). */
  static final int SETTL_DATE_OFFSET = 64;

  /** Length of the SettlDate field. Matches SBE SettlDate char[8]. */
  static final int SETTL_DATE_LENGTH = 8;

  /** Offset into the flat buffer for the 3-byte Currency field (FIX tag 15). */
  static final int CURRENCY_OFFSET = 72;

  /** Length of the Currency field. Matches SBE Currency char[3]. */
  static final int CURRENCY_LENGTH = 3;

  /** Offset into the flat buffer for the 3-byte SettlCurrency field (FIX tag 120). */
  static final int SETTL_CURRENCY_OFFSET = 75;

  /** Length of the SettlCurrency field. Matches SBE Currency char[3]. */
  static final int SETTL_CURRENCY_LENGTH = 3;

  /** Offset into the flat buffer for the NOS stash region. */
  static final int NOS_BUFFER_OFFSET = 78;

  /**
   * Total flat buffer size in bytes. All byte-array fields are packed contiguously: quoteReqId(20)
   * + quoteId(20) + symbol(8) + accountCode(16) + settlDate(8) + currency(3) + settlCurrency(3) +
   * nosStash(512) = 590.
   */
  static final int FLAT_BUFFER_SIZE =
      NOS_BUFFER_OFFSET + OrchestratorConstants.NOS_STASH_BUFFER_SIZE;

  // Static assertions — verify flat buffer layout at class load time (unconditional, no -ea needed)
  static {
    if (QUOTE_REQ_ID_LENGTH != QuoteRequestDecoder.quoteReqIdLength()) {
      throw new IllegalStateException("QUOTE_REQ_ID_LENGTH mismatch with SBE QuoteReqID");
    }
    if (SYMBOL_LENGTH != QuoteRequestDecoder.symbolLength()) {
      throw new IllegalStateException("SYMBOL_LENGTH mismatch with SBE Symbol");
    }
    if (ACCOUNT_CODE_LENGTH != QuoteRequestDecoder.accountCodeLength()) {
      throw new IllegalStateException("ACCOUNT_CODE_LENGTH mismatch with SBE Account");
    }
    if (SETTL_DATE_LENGTH != QuoteRequestDecoder.settlDateLength()) {
      throw new IllegalStateException("SETTL_DATE_LENGTH mismatch with SBE SettlDate");
    }
    if (CURRENCY_LENGTH != QuoteRequestDecoder.currencyLength()) {
      throw new IllegalStateException("CURRENCY_LENGTH mismatch with SBE Currency");
    }
    if (SETTL_CURRENCY_LENGTH != QuoteRequestDecoder.settlCurrencyLength()) {
      throw new IllegalStateException(
          "SETTL_CURRENCY_LENGTH mismatch with SBE Currency (settlCurrency)");
    }
    if (QUOTE_ID_LENGTH != PriceResponseDecoder.quoteReqIdLength()) {
      throw new IllegalStateException(
          "QUOTE_ID_LENGTH mismatch with SBE QuoteReqID (same 20-byte type)");
    }
  }

  // ===========================================================================
  // Instance fields — flat buffer + primitives
  // ===========================================================================

  /** Single contiguous backing buffer for all byte-array fields. */
  private final byte[] flatBuffer;

  /**
   * UnsafeBuffer wrapping {@link #flatBuffer} for zero-copy reads/writes via DirectBuffer API.
   * Pre-allocated at construction — never re-wrapped.
   */
  private final UnsafeBuffer flatBufferView;

  /**
   * Pre-allocated owned ByteArrayKey for the quoteReqId map. The backing array is overwritten from
   * the flat buffer slice at {@link #QUOTE_REQ_ID_OFFSET} via {@link ByteArrayKey#overwrite}. Since
   * the backing array is pre-allocated at 20 bytes (matching the SBE field length), {@code
   * overwrite()} never allocates.
   */
  private final ByteArrayKey quoteReqIdKey;

  /**
   * Pre-allocated owned ByteArrayKey for the quoteId map. Same pattern as {@link #quoteReqIdKey}.
   */
  private final ByteArrayKey quoteIdKey;

  // --- Primitive fields (Java fields, not in flat buffer) ---

  private State state;
  private long expiryNanos;
  private int quoteReqIdLen;
  private int quoteIdLen;
  private byte sideRaw;
  private long orderQty;
  private long bidPx;
  private long offerPx;
  private long bidSize;
  private long offerSize;
  private long validUntil;
  private long swapPoints;
  private byte productTypeRaw;
  private byte settlTypeRaw;
  private byte tenorRaw;
  private long transactTime;
  private int nosLength;

  /** Pool index — set once at construction, used for diagnostics and pool management. */
  private final int poolIndex;

  /**
   * Constructs a new RfqState in {@link State#FREE} state with all fields zeroed.
   *
   * @param poolIndex the index of this slot in the pool array (for diagnostics)
   */
  public RfqState(final int poolIndex) {
    this.poolIndex = poolIndex;
    this.flatBuffer = new byte[FLAT_BUFFER_SIZE];
    this.flatBufferView = new UnsafeBuffer(flatBuffer);
    this.quoteReqIdKey = ByteArrayKey.owned(flatBuffer, QUOTE_REQ_ID_OFFSET, QUOTE_REQ_ID_LENGTH);
    this.quoteIdKey = ByteArrayKey.owned(flatBuffer, QUOTE_ID_OFFSET, QUOTE_ID_LENGTH);
    this.state = State.FREE;
  }

  // ===========================================================================
  // Lifecycle
  // ===========================================================================

  /**
   * Resets this slot to {@link State#FREE}, zeroing all fields. Called when releasing the slot back
   * to the pool after a terminal state transition.
   */
  public void reset() {
    state = State.FREE;
    expiryNanos = 0L;
    quoteReqIdLen = 0;
    quoteIdLen = 0;
    sideRaw = 0;
    orderQty = 0L;
    bidPx = 0L;
    offerPx = 0L;
    bidSize = 0L;
    offerSize = 0L;
    validUntil = 0L;
    swapPoints = 0L;
    productTypeRaw = 0;
    settlTypeRaw = 0;
    tenorRaw = 0;
    transactTime = 0L;
    nosLength = 0;
    Arrays.fill(flatBuffer, (byte) 0);
  }

  /**
   * Returns {@code true} if this RFQ is in a terminal state ({@link State#COMPLETED}, {@link
   * State#REJECTED}, or {@link State#EXPIRED}).
   */
  public boolean isTerminal() {
    return state == State.COMPLETED || state == State.REJECTED || state == State.EXPIRED;
  }

  /** Returns {@code true} if this RFQ is in an active (non-FREE, non-terminal) state. */
  public boolean isActive() {
    return state != State.FREE && !isTerminal();
  }

  // ===========================================================================
  // Populate from inbound messages
  // ===========================================================================

  /**
   * Populates this slot from a decoded QuoteRequest. Sets state to {@link State#PENDING_PRICE} and
   * computes expiryNanos.
   *
   * @param decoder the pre-wrapped QuoteRequest decoder — must not be retained past this call
   * @param nowNanos current monotonic time from NanoClock
   * @param pendingPriceTimeoutNanos timeout for the PENDING_PRICE state
   */
  public void populateFromQuoteRequest(
      final QuoteRequestDecoder decoder, final long nowNanos, final long pendingPriceTimeoutNanos) {

    state = State.PENDING_PRICE;
    expiryNanos = nowNanos + pendingPriceTimeoutNanos;

    // Copy char fields into flat buffer. SBE getters take (byte[], dstOffset) — the field length
    // is fixed by the schema and the getter writes exactly that many bytes.
    decoder.getQuoteReqId(flatBuffer, QUOTE_REQ_ID_OFFSET);
    quoteReqIdLen = QUOTE_REQ_ID_LENGTH;
    decoder.getSymbol(flatBuffer, SYMBOL_OFFSET);
    decoder.getAccountCode(flatBuffer, ACCOUNT_CODE_OFFSET);
    decoder.getSettlDate(flatBuffer, SETTL_DATE_OFFSET);
    decoder.getCurrency(flatBuffer, CURRENCY_OFFSET);
    decoder.getSettlCurrency(flatBuffer, SETTL_CURRENCY_OFFSET);

    // Copy primitive fields
    sideRaw = (byte) decoder.side().value();
    orderQty = decoder.orderQty();
    productTypeRaw = (byte) decoder.productType().value();
    settlTypeRaw = (byte) decoder.settlType().value();
    tenorRaw = (byte) decoder.tenor().value();
    transactTime = decoder.transactTime();

    // Update the ByteArrayKey for map insertion — overwrite never allocates (20-byte backing)
    quoteReqIdKey.overwrite(flatBufferView, QUOTE_REQ_ID_OFFSET, QUOTE_REQ_ID_LENGTH);
  }

  /**
   * Applies pricing data from a PriceResponse. Transitions state to {@link State#QUOTED} and resets
   * expiryNanos for the quoted timeout.
   *
   * @param decoder the pre-wrapped PriceResponse decoder — must not be retained past this call
   * @param nowNanos current monotonic time from NanoClock
   * @param quotedTimeoutNanos timeout for the QUOTED state
   */
  public void applyPriceResponse(
      final PriceResponseDecoder decoder, final long nowNanos, final long quotedTimeoutNanos) {

    state = State.QUOTED;
    expiryNanos = nowNanos + quotedTimeoutNanos;

    bidPx = decoder.bidPx();
    offerPx = decoder.offerPx();
    bidSize = decoder.bidSize();
    offerSize = decoder.offerSize();
    validUntil = decoder.validUntil();
    swapPoints = decoder.swapPoints();
  }

  /**
   * Stores the generated quoteId bytes into the flat buffer and updates the quoteId ByteArrayKey
   * for map insertion.
   *
   * @param quoteIdBytes the generated quoteId bytes (e.g., from OrchestratorIdGenerator)
   * @param offset offset into quoteIdBytes
   * @param length number of bytes to copy (must be {@code <= QUOTE_ID_LENGTH})
   */
  public void setQuoteId(final byte[] quoteIdBytes, final int offset, final int length) {
    System.arraycopy(quoteIdBytes, offset, flatBuffer, QUOTE_ID_OFFSET, length);
    quoteIdLen = length;
    // Null-pad remainder if length < QUOTE_ID_LENGTH
    if (length < QUOTE_ID_LENGTH) {
      Arrays.fill(
          flatBuffer, QUOTE_ID_OFFSET + length, QUOTE_ID_OFFSET + QUOTE_ID_LENGTH, (byte) 0);
    }
    quoteIdKey.overwrite(flatBufferView, QUOTE_ID_OFFSET, QUOTE_ID_LENGTH);
  }

  /**
   * Stashes raw NOS fragment bytes into the flat buffer for later cluster forwarding (APP-31).
   *
   * @param buffer the source buffer containing the NOS fragment
   * @param offset offset into the source buffer
   * @param length byte length of the NOS fragment
   * @return {@code true} if stashed successfully, {@code false} if the NOS is too large for the
   *     stash buffer
   */
  public boolean stashNos(final DirectBuffer buffer, final int offset, final int length) {
    if (length > OrchestratorConstants.NOS_STASH_BUFFER_SIZE) {
      return false;
    }
    buffer.getBytes(offset, flatBuffer, NOS_BUFFER_OFFSET, length);
    nosLength = length;
    return true;
  }

  // ===========================================================================
  // State transitions (called by RfqStateMachine)
  // ===========================================================================

  /** Sets the state directly. Package-private — only {@link RfqStateMachine} should call this. */
  void setState(final State newState) {
    this.state = newState;
  }

  /** Sets the expiry timestamp. Package-private — only {@link RfqStateMachine} should call this. */
  void setExpiryNanos(final long expiryNanos) {
    this.expiryNanos = expiryNanos;
  }

  // ===========================================================================
  // Getters — zero allocation
  // ===========================================================================

  public State state() {
    return state;
  }

  public long expiryNanos() {
    return expiryNanos;
  }

  public int poolIndex() {
    return poolIndex;
  }

  public int quoteReqIdLen() {
    return quoteReqIdLen;
  }

  public int quoteIdLen() {
    return quoteIdLen;
  }

  public byte sideRaw() {
    return sideRaw;
  }

  public long orderQty() {
    return orderQty;
  }

  public long bidPx() {
    return bidPx;
  }

  public long offerPx() {
    return offerPx;
  }

  public long bidSize() {
    return bidSize;
  }

  public long offerSize() {
    return offerSize;
  }

  public long validUntil() {
    return validUntil;
  }

  public long swapPoints() {
    return swapPoints;
  }

  public byte productTypeRaw() {
    return productTypeRaw;
  }

  public byte settlTypeRaw() {
    return settlTypeRaw;
  }

  public byte tenorRaw() {
    return tenorRaw;
  }

  public long transactTime() {
    return transactTime;
  }

  public int nosLength() {
    return nosLength;
  }

  /** Returns the ByteArrayKey for the quoteReqId map. Do not retain past the current duty cycle. */
  public ByteArrayKey quoteReqIdKey() {
    return quoteReqIdKey;
  }

  /** Returns the ByteArrayKey for the quoteId map. Do not retain past the current duty cycle. */
  public ByteArrayKey quoteIdKey() {
    return quoteIdKey;
  }

  // ===========================================================================
  // Zero-copy field accessors — write into caller's buffer
  // ===========================================================================

  /**
   * Copies the quoteReqId bytes into the destination buffer at the given offset.
   *
   * @param dst destination buffer
   * @param dstOffset offset in the destination buffer
   * @return number of bytes written ({@link #QUOTE_REQ_ID_LENGTH})
   */
  public int putQuoteReqIdInto(final MutableDirectBuffer dst, final int dstOffset) {
    dst.putBytes(dstOffset, flatBuffer, QUOTE_REQ_ID_OFFSET, QUOTE_REQ_ID_LENGTH);
    return QUOTE_REQ_ID_LENGTH;
  }

  /**
   * Copies the quoteReqId bytes into the destination byte array.
   *
   * @param dst destination byte array
   * @param dstOffset offset in the destination array
   * @return number of bytes written
   */
  public int putQuoteReqIdInto(final byte[] dst, final int dstOffset) {
    System.arraycopy(flatBuffer, QUOTE_REQ_ID_OFFSET, dst, dstOffset, QUOTE_REQ_ID_LENGTH);
    return QUOTE_REQ_ID_LENGTH;
  }

  /**
   * Copies the quoteId bytes into the destination buffer at the given offset.
   *
   * @param dst destination buffer
   * @param dstOffset offset in the destination buffer
   * @return number of bytes written ({@link #QUOTE_ID_LENGTH})
   */
  public int putQuoteIdInto(final MutableDirectBuffer dst, final int dstOffset) {
    dst.putBytes(dstOffset, flatBuffer, QUOTE_ID_OFFSET, QUOTE_ID_LENGTH);
    return QUOTE_ID_LENGTH;
  }

  /**
   * Copies the quoteId bytes into the destination byte array.
   *
   * @param dst destination byte array
   * @param dstOffset offset in the destination array
   * @return number of bytes written
   */
  public int putQuoteIdInto(final byte[] dst, final int dstOffset) {
    System.arraycopy(flatBuffer, QUOTE_ID_OFFSET, dst, dstOffset, QUOTE_ID_LENGTH);
    return QUOTE_ID_LENGTH;
  }

  /**
   * Copies the symbol bytes into the destination buffer.
   *
   * @param dst destination buffer
   * @param dstOffset offset in the destination buffer
   * @return number of bytes written ({@link #SYMBOL_LENGTH})
   */
  public int putSymbolInto(final MutableDirectBuffer dst, final int dstOffset) {
    dst.putBytes(dstOffset, flatBuffer, SYMBOL_OFFSET, SYMBOL_LENGTH);
    return SYMBOL_LENGTH;
  }

  /**
   * Copies the symbol bytes into the destination byte array.
   *
   * @param dst destination byte array
   * @param dstOffset offset in the destination array
   * @return number of bytes written
   */
  public int putSymbolInto(final byte[] dst, final int dstOffset) {
    System.arraycopy(flatBuffer, SYMBOL_OFFSET, dst, dstOffset, SYMBOL_LENGTH);
    return SYMBOL_LENGTH;
  }

  /**
   * Copies the accountCode bytes into the destination buffer.
   *
   * @param dst destination buffer
   * @param dstOffset offset in the destination buffer
   * @return number of bytes written ({@link #ACCOUNT_CODE_LENGTH})
   */
  public int putAccountCodeInto(final MutableDirectBuffer dst, final int dstOffset) {
    dst.putBytes(dstOffset, flatBuffer, ACCOUNT_CODE_OFFSET, ACCOUNT_CODE_LENGTH);
    return ACCOUNT_CODE_LENGTH;
  }

  /**
   * Copies the accountCode bytes into the destination byte array.
   *
   * @param dst destination byte array
   * @param dstOffset offset in the destination array
   * @return number of bytes written
   */
  public int putAccountCodeInto(final byte[] dst, final int dstOffset) {
    System.arraycopy(flatBuffer, ACCOUNT_CODE_OFFSET, dst, dstOffset, ACCOUNT_CODE_LENGTH);
    return ACCOUNT_CODE_LENGTH;
  }

  /**
   * Copies the settlDate bytes into the destination buffer.
   *
   * @param dst destination buffer
   * @param dstOffset offset in the destination buffer
   * @return number of bytes written ({@link #SETTL_DATE_LENGTH})
   */
  public int putSettlDateInto(final MutableDirectBuffer dst, final int dstOffset) {
    dst.putBytes(dstOffset, flatBuffer, SETTL_DATE_OFFSET, SETTL_DATE_LENGTH);
    return SETTL_DATE_LENGTH;
  }

  /**
   * Copies the settlDate bytes into the destination byte array.
   *
   * @param dst destination byte array
   * @param dstOffset offset in the destination array
   * @return number of bytes written
   */
  public int putSettlDateInto(final byte[] dst, final int dstOffset) {
    System.arraycopy(flatBuffer, SETTL_DATE_OFFSET, dst, dstOffset, SETTL_DATE_LENGTH);
    return SETTL_DATE_LENGTH;
  }

  /**
   * Copies the currency bytes into the destination buffer.
   *
   * @param dst destination buffer
   * @param dstOffset offset in the destination buffer
   * @return number of bytes written ({@link #CURRENCY_LENGTH})
   */
  public int putCurrencyInto(final MutableDirectBuffer dst, final int dstOffset) {
    dst.putBytes(dstOffset, flatBuffer, CURRENCY_OFFSET, CURRENCY_LENGTH);
    return CURRENCY_LENGTH;
  }

  /**
   * Copies the currency bytes into the destination byte array.
   *
   * @param dst destination byte array
   * @param dstOffset offset in the destination array
   * @return number of bytes written
   */
  public int putCurrencyInto(final byte[] dst, final int dstOffset) {
    System.arraycopy(flatBuffer, CURRENCY_OFFSET, dst, dstOffset, CURRENCY_LENGTH);
    return CURRENCY_LENGTH;
  }

  /**
   * Copies the settlCurrency bytes into the destination buffer.
   *
   * @param dst destination buffer
   * @param dstOffset offset in the destination buffer
   * @return number of bytes written ({@link #SETTL_CURRENCY_LENGTH})
   */
  public int putSettlCurrencyInto(final MutableDirectBuffer dst, final int dstOffset) {
    dst.putBytes(dstOffset, flatBuffer, SETTL_CURRENCY_OFFSET, SETTL_CURRENCY_LENGTH);
    return SETTL_CURRENCY_LENGTH;
  }

  /**
   * Copies the settlCurrency bytes into the destination byte array.
   *
   * @param dst destination byte array
   * @param dstOffset offset in the destination array
   * @return number of bytes written
   */
  public int putSettlCurrencyInto(final byte[] dst, final int dstOffset) {
    System.arraycopy(flatBuffer, SETTL_CURRENCY_OFFSET, dst, dstOffset, SETTL_CURRENCY_LENGTH);
    return SETTL_CURRENCY_LENGTH;
  }

  /**
   * Copies the stashed NOS bytes into the destination buffer.
   *
   * @param dst destination buffer
   * @param dstOffset offset in the destination buffer
   * @return number of NOS bytes written ({@link #nosLength()})
   */
  public int putNosInto(final MutableDirectBuffer dst, final int dstOffset) {
    if (nosLength > 0) {
      dst.putBytes(dstOffset, flatBuffer, NOS_BUFFER_OFFSET, nosLength);
    }
    return nosLength;
  }

  /**
   * Copies the stashed NOS bytes into the destination byte array.
   *
   * @param dst destination byte array (must be at least {@code dstOffset + nosLength} bytes)
   * @param dstOffset offset in the destination array
   * @return number of NOS bytes written ({@link #nosLength()})
   */
  public int putNosInto(final byte[] dst, final int dstOffset) {
    if (nosLength > 0) {
      System.arraycopy(flatBuffer, NOS_BUFFER_OFFSET, dst, dstOffset, nosLength);
    }
    return nosLength;
  }

  /**
   * Returns the flat buffer view for direct read access. Package-private — used by the encoder for
   * efficient char-field copying via DirectBuffer API.
   *
   * @return the UnsafeBuffer wrapping the flat buffer
   */
  DirectBuffer flatBufferView() {
    return flatBufferView;
  }
}
