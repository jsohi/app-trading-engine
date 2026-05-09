package com.trading.engine.cluster.state;

import com.trading.engine.messages.util.ByteArrayKey;
import org.agrona.concurrent.UnsafeBuffer;

/**
 * Per-RFQ flyweight slot in the {@link RfqStateMachine} pool. One {@link RfqSlot} object is
 * allocated per pool entry at construction time; it is reused across multiple RFQ lifecycles by
 * resetting fields on {@link RfqStateMachine#release(RfqSlot)}.
 *
 * <p>The slot stores all fields required to:
 *
 * <ul>
 *   <li>Encode {@code QuoteRequestedEvent} (104), {@code QuoteCreatedEvent} (105), {@code
 *       QuoteRejectedEvent} (106), and {@code QuoteExpiredEvent} (107).
 *   <li>Snapshot the RFQ state via template 203 ({@code RfqStateSnapshot}).
 *   <li>Detect idempotent retransmits via CRC32C of the decoded request body with byte-for-byte
 *       fallback comparison.
 *   <li>Manage per-slot TTL and request-timeout timers via Aeron's {@code Cluster.scheduleTimer}.
 * </ul>
 *
 * <p><b>Field sizing notes:</b> all fixed-length fields match the SBE schema definitions in {@code
 * trading-schema.xml}. {@code QuoteReqID} (FIX tag 131) = 20 bytes; {@code QuoteID} (FIX tag 117) =
 * 20 bytes; {@code Symbol} (FIX tag 55) = 8 bytes; AccountCode (FIX tag 1) = 16 bytes; SettlDate
 * (FIX tag 64) = 8 bytes; Currency (FIX tag 15) = 3 bytes; SettlCurrency (FIX tag 120) = 3 bytes.
 *
 * <p><b>Threading:</b> not thread-safe — single-threaded cluster duty cycle only.
 *
 * <p><b>Allocation:</b> zero allocation after construction. All byte arrays and wrapper objects are
 * pre-allocated in the constructor.
 *
 * @see RfqStateMachine
 * @see RfqSlotState
 */
public final class RfqSlot {

  // -------------------------------------------------------------------------
  // Field length constants (match SBE schema trading-schema.xml)
  // -------------------------------------------------------------------------

  /** QuoteReqID (FIX tag 131) fixed-length: 20 bytes. */
  public static final int QUOTE_REQ_ID_LENGTH = 20;

  /** QuoteID (FIX tag 117) fixed-length: 20 bytes. */
  public static final int QUOTE_ID_LENGTH = 20;

  /** Symbol (FIX tag 55) fixed-length: 8 bytes. */
  public static final int SYMBOL_LENGTH = 8;

  /** AccountCode (FIX tag 1) fixed-length: 16 bytes. */
  public static final int ACCOUNT_CODE_LENGTH = 16;

  /** SettlDate (FIX tag 64) fixed-length: 8 bytes. */
  public static final int SETTL_DATE_LENGTH = 8;

  /** Currency (FIX tag 15) fixed-length: 3 bytes. */
  public static final int CURRENCY_LENGTH = 3;

  /** SettlCurrency (FIX tag 120) fixed-length: 3 bytes. */
  public static final int SETTL_CURRENCY_LENGTH = 3;

  /**
   * Maximum legs per RFQ swap. Schema 203 {@code noLegs} group supports up to 2 legs (Spot/Forward
   * legs of an FX swap). Single-leg (Spot/Forward) RFQs use {@code noLegs=0}.
   */
  public static final int MAX_LEGS = 2;

  /**
   * Size of the request body buffer used for idempotent retransmit detection. Sized generously to
   * accommodate the largest possible {@code QuoteRequest} command body including a full {@code
   * noLegs(2)} group with all leg fields, plus an 8-byte header guard. At 256 bytes this is well
   * above the actual maximum (~180 bytes for 2 legs) while remaining cache-line friendly.
   */
  public static final int REQUEST_BODY_SIZE = 256;

  // -------------------------------------------------------------------------
  // Pool management (set by RfqStateMachine only)
  // -------------------------------------------------------------------------

  /** Pool index within {@link RfqStateMachine}'s slot array. Immutable after construction. */
  public final int poolIndex;

  /**
   * Per-slot generation counter (31-bit). Incremented on each {@link RfqStateMachine#release}. Used
   * to detect stale timer correlations: a timer firing with generation {@code G} after the slot has
   * been released and reused at generation {@code G+1} is silently dropped. Retirement threshold:
   * {@code Integer.MAX_VALUE >> 1} (~1 billion reuses, ~100+ years at 10/sec).
   */
  public int generation;

  /** Current slot lifecycle state. */
  public RfqSlotState state;

  // -------------------------------------------------------------------------
  // Timer correlation IDs (set by RfqStateMachine only)
  // -------------------------------------------------------------------------

  /**
   * Correlation ID for the TTL timer (high bit clear). Computed as {@code (generation << 31) |
   * poolIndex}. Registered with {@code Cluster.scheduleTimer} when the slot transitions
   * REQUESTED→QUOTED. Fires → {@code QuoteExpiredEvent} (107).
   */
  public long timerCorrelationId;

  /**
   * Correlation ID for the request-timeout timer (high bit set). Computed as {@code
   * 0x8000_0000_0000_0000L | (generation << 31) | poolIndex}. Registered with {@code
   * Cluster.scheduleTimer} when the slot is first acquired (REQUESTED state). Fires → {@code
   * QuoteRejectedEvent} (106) with {@code text="request timeout"}.
   */
  public long requestTimeoutCorrelationId;

  // -------------------------------------------------------------------------
  // Timing fields
  // -------------------------------------------------------------------------

  /**
   * Epoch-nanos cluster timestamp when the slot reached REQUESTED state (set from {@code
   * onSessionMessage} timestamp). Used to compute the request-timeout deadline on recovery. Matches
   * the {@code transactTime} field in template 203.
   */
  public long transactTime;

  /**
   * Epoch-nanos deadline for the TTL timer. Set when transitioning REQUESTED→QUOTED. {@code
   * validUntil = clusterTimestamp + ttlForProduct(productType)}. Matches the {@code validUntil}
   * field in template 203 and is the {@code ValidUntilTime} (FIX tag 62) value in {@code
   * QuoteCreatedEvent} (105).
   */
  public long validUntil;

  // -------------------------------------------------------------------------
  // Account and session routing
  // -------------------------------------------------------------------------

  /**
   * Cluster session ID of the originating FIX gateway session. Used to release the rate-limit
   * {@link TokenBucket} on {@code onSessionClose} and to fast-fail in-flight slots originating from
   * a closed session.
   */
  public long sessionId;

  /**
   * Numeric account ID resolved from AccountCode at request time. Used during snapshot recovery to
   * rehydrate {@code accountCodeBytes} via {@code AccountStore.get(accountId)}.
   */
  public long accountId;

  // -------------------------------------------------------------------------
  // Idempotent retransmit detection
  // -------------------------------------------------------------------------

  /**
   * CRC32C of the decoded QuoteRequest body. Used as the first tier of idempotent retransmit
   * detection (fast path). On CRC match, the full byte-for-byte comparison against {@link
   * #requestBody} disambiguates the ~2^{-32} collision case.
   */
  public int bodyCrc;

  /**
   * Raw bytes of the decoded QuoteRequest body for byte-for-byte idempotent retransmit comparison
   * (second tier after CRC match). Sized to {@link #REQUEST_BODY_SIZE}.
   */
  public final byte[] requestBody;

  /** Number of valid bytes in {@link #requestBody}. */
  public int requestBodyLen;

  // -------------------------------------------------------------------------
  // Fixed-length identity fields (FIX tag refs in field Javadoc)
  // -------------------------------------------------------------------------

  /** QuoteReqID (FIX tag 131) — 20-byte fixed-length ASCII, NUL-padded. */
  public final byte[] quoteReqIdBytes;

  /** QuoteID (FIX tag 117) — 20-byte fixed-length ASCII, NUL-padded. Set on REQUESTED→QUOTED. */
  public final byte[] quoteIdBytes;

  /** Symbol (FIX tag 55) — 8-byte fixed-length ASCII, NUL-padded. */
  public final byte[] symbolBytes;

  /** AccountCode (FIX tag 1) — 16-byte fixed-length ASCII, NUL-padded. */
  public final byte[] accountCodeBytes;

  /** SettlDate (FIX tag 64) — 8-byte fixed-length ASCII, NUL-padded. */
  public final byte[] settlDateBytes;

  /** Currency (FIX tag 15) — 3 bytes. */
  public final byte[] currencyBytes;

  /** SettlCurrency (FIX tag 120) — 3 bytes. */
  public final byte[] settlCurrencyBytes;

  // -------------------------------------------------------------------------
  // Enum-typed fields (stored as bytes to avoid enum boxing on hot path)
  // -------------------------------------------------------------------------

  /** Side (FIX tag 54) — byte: 1=Buy, 2=Sell. */
  public byte side;

  /** ProductType (FIX tag 460) — byte: 1=Spot, 2=Forward, 3=Swap. */
  public byte productType;

  /** SettlType (FIX tag 63) — byte: 0=Regular (sentinel for unset), 1=Cash, 2=NextDay, etc. */
  public byte settlType;

  /** Tenor (byte enum). */
  public byte tenor;

  // -------------------------------------------------------------------------
  // Numeric price and quantity fields (fixed-point 10^-8)
  // -------------------------------------------------------------------------

  /** OrderQty (FIX tag 38) — fixed-point 10^-8. */
  public long orderQty;

  /** BidPx (FIX tag 132) — fixed-point 10^-8. Set on REQUESTED→QUOTED. */
  public long bidPx;

  /** OfferPx (FIX tag 133) — fixed-point 10^-8. Set on REQUESTED→QUOTED. */
  public long offerPx;

  /** BidSize (FIX tag 134) — fixed-point 10^-8. Set on REQUESTED→QUOTED. */
  public long bidSize;

  /** OfferSize (FIX tag 135) — fixed-point 10^-8. Set on REQUESTED→QUOTED. */
  public long offerSize;

  /** LastPx — the last traded/execution price (fixed-point 10^-8). */
  public long lastPx;

  /** SwapPoints — forward swap points (fixed-point 10^-8, may be negative). */
  public long swapPoints;

  // -------------------------------------------------------------------------
  // Leg fields (FX swap — up to MAX_LEGS legs)
  // -------------------------------------------------------------------------

  /** Number of legs (0 for Spot/Forward, 2 for Swap). Range [0, MAX_LEGS]. */
  public int noLegs;

  // Per-leg arrays: indexed [0..noLegs). Pre-allocated to MAX_LEGS depth.

  /** LegSide (FIX tag 624) — byte per leg. */
  public final byte[] legSide;

  /** LegSettlDate (FIX tag 588) — 8 bytes per leg. */
  public final byte[][] legSettlDate;

  /** LegSettlType (FIX tag 587) — byte per leg. */
  public final byte[] legSettlType;

  /** LegCurrency (FIX tag 556) — 3 bytes per leg. */
  public final byte[][] legCurrency;

  /** LegTenor — byte enum per leg. */
  public final byte[] legTenor;

  /** LegOrderQty (FIX tag 685) — fixed-point 10^-8 per leg. */
  public final long[] legOrderQty;

  /** LegPrice (FIX tag 566) — fixed-point 10^-8 per leg. */
  public final long[] legPrice;

  /** LegBidPx — fixed-point 10^-8 per leg. */
  public final long[] legBidPx;

  /** LegOfferPx — fixed-point 10^-8 per leg. */
  public final long[] legOfferPx;

  /** LegBidSize — fixed-point 10^-8 per leg. */
  public final long[] legBidSize;

  /** LegOfferSize — fixed-point 10^-8 per leg. */
  public final long[] legOfferSize;

  // -------------------------------------------------------------------------
  // Map key (per-slot, pre-allocated for zero-alloc byQuoteReqId lookup)
  // -------------------------------------------------------------------------

  /**
   * Pre-allocated owned {@link ByteArrayKey} wrapping {@link #quoteReqIdBytes}. Populated when the
   * slot transitions FREE→REQUESTED (commit step). Used as the key in {@code
   * RfqStateMachine.byQuoteReqId} and {@code byQuoteId} maps. Must be removed from all maps BEFORE
   * any byte mutation in {@link RfqStateMachine#release(RfqSlot)}.
   */
  public final ByteArrayKey quoteReqIdKey;

  /**
   * Pre-allocated owned {@link ByteArrayKey} wrapping {@link #quoteIdBytes}. Populated when the
   * slot transitions REQUESTED→QUOTED. Used as the key in {@code RfqStateMachine.byQuoteId} map.
   */
  public final ByteArrayKey quoteIdKey;

  // -------------------------------------------------------------------------
  // Buffer views for GFLog zero-alloc logging
  // -------------------------------------------------------------------------

  /**
   * Pre-allocated {@link UnsafeBuffer} view over {@link #quoteReqIdBytes}. Used by {@link
   * BufferAsAsciiCharSequence} for zero-alloc GFLog char-by-char append.
   */
  public final UnsafeBuffer quoteReqIdBuffer;

  /**
   * Pre-allocated {@link UnsafeBuffer} view over {@link #quoteIdBytes}. Used by {@link
   * BufferAsAsciiCharSequence} for zero-alloc GFLog char-by-char append.
   */
  public final UnsafeBuffer quoteIdBuffer;

  // -------------------------------------------------------------------------
  // Constructor
  // -------------------------------------------------------------------------

  /**
   * Constructs a fresh FREE slot at the given pool index. All arrays are pre-allocated; all
   * primitive fields are zero-initialized. Called once per pool entry at {@link RfqStateMachine}
   * construction time.
   *
   * @param poolIndex the immutable pool index within the slot array; must be &gt;= 0
   */
  public RfqSlot(final int poolIndex) {
    this.poolIndex = poolIndex;
    this.generation = 0;
    this.state = RfqSlotState.FREE;

    this.requestBody = new byte[REQUEST_BODY_SIZE];
    this.quoteReqIdBytes = new byte[QUOTE_REQ_ID_LENGTH];
    this.quoteIdBytes = new byte[QUOTE_ID_LENGTH];
    this.symbolBytes = new byte[SYMBOL_LENGTH];
    this.accountCodeBytes = new byte[ACCOUNT_CODE_LENGTH];
    this.settlDateBytes = new byte[SETTL_DATE_LENGTH];
    this.currencyBytes = new byte[CURRENCY_LENGTH];
    this.settlCurrencyBytes = new byte[SETTL_CURRENCY_LENGTH];

    this.legSide = new byte[MAX_LEGS];
    this.legSettlDate = new byte[MAX_LEGS][SETTL_DATE_LENGTH];
    this.legSettlType = new byte[MAX_LEGS];
    this.legCurrency = new byte[MAX_LEGS][CURRENCY_LENGTH];
    this.legTenor = new byte[MAX_LEGS];
    this.legOrderQty = new long[MAX_LEGS];
    this.legPrice = new long[MAX_LEGS];
    this.legBidPx = new long[MAX_LEGS];
    this.legOfferPx = new long[MAX_LEGS];
    this.legBidSize = new long[MAX_LEGS];
    this.legOfferSize = new long[MAX_LEGS];

    // The ByteArrayKey instances are owned keys wrapping the fixed-length byte arrays.
    // They are not inserted into any map until the commit step in RfqStateMachine.
    // Sized exactly to the SBE field lengths (no extra capacity needed — fixed-width SBE fields).
    this.quoteReqIdKey = ByteArrayKey.copyOf(quoteReqIdBytes, 0, QUOTE_REQ_ID_LENGTH);
    this.quoteIdKey = ByteArrayKey.copyOf(quoteIdBytes, 0, QUOTE_ID_LENGTH);

    this.quoteReqIdBuffer = new UnsafeBuffer(quoteReqIdBytes);
    this.quoteIdBuffer = new UnsafeBuffer(quoteIdBytes);
  }

  // -------------------------------------------------------------------------
  // Key synchronization helpers (called by RfqStateMachine before map operations)
  // -------------------------------------------------------------------------

  /**
   * Overwrites the {@link #quoteReqIdKey} content from the current {@link #quoteReqIdBytes}. Must
   * be called after populating {@code quoteReqIdBytes} and before inserting into {@code
   * byQuoteReqId}. Zero allocation — uses {@link ByteArrayKey#overwrite}.
   */
  public void syncQuoteReqIdKey() {
    quoteReqIdKey.overwrite(quoteReqIdBytes, 0, QUOTE_REQ_ID_LENGTH);
  }

  /**
   * Overwrites the {@link #quoteIdKey} content from the current {@link #quoteIdBytes}. Must be
   * called after populating {@code quoteIdBytes} (on REQUESTED→QUOTED transition) and before
   * inserting into {@code byQuoteId}. Zero allocation — uses {@link ByteArrayKey#overwrite}.
   */
  public void syncQuoteIdKey() {
    quoteIdKey.overwrite(quoteIdBytes, 0, QUOTE_ID_LENGTH);
  }

  // -------------------------------------------------------------------------
  // Accessors for metrics / logging (read-only, package-private visibility)
  // -------------------------------------------------------------------------

  /**
   * Returns the pool index.
   *
   * @return the pool index
   */
  public int poolIndex() {
    return poolIndex;
  }

  /**
   * Returns the current generation counter.
   *
   * @return the generation
   */
  public int generation() {
    return generation;
  }

  /**
   * Returns the current slot state.
   *
   * @return the slot state
   */
  public RfqSlotState state() {
    return state;
  }

  /**
   * Returns the TTL timer correlation ID.
   *
   * @return the TTL timer correlation ID
   */
  public long timerCorrelationId() {
    return timerCorrelationId;
  }

  /**
   * Returns the request-timeout timer correlation ID.
   *
   * @return the request-timeout timer correlation ID
   */
  public long requestTimeoutCorrelationId() {
    return requestTimeoutCorrelationId;
  }
}
