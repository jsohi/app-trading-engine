package com.trading.engine.fixbridge.json;

/**
 * Pooled per-session bounded-queue entry that carries the minimal set of fields needed to re-write
 * a backpressured outbound event when the channel becomes writable again.
 *
 * <p><b>Purpose.</b> When {@code Channel.isWritable()} returns {@code false}, lossy events ({@link
 * BrowserEvent.Quote}, {@link BrowserEvent.RawFix}) are dropped per locked §6, but correlated
 * events ({@link BrowserEvent.ExecutionReport}, {@link BrowserEvent.OrderReject}, {@link
 * BrowserEvent.AuthExpired}) MUST be queued for redelivery. The queue is bounded ({@code
 * outboundQueueCapacityPerSession=64}); on overflow the bridge writes a synchronous {@code
 * BridgeStatus(fatal=true,reason="outbound-overflow")} and closes WS code 1011.
 *
 * <p>This entry holds a {@code kind} discriminator and the structural fields of every retainable
 * event. Each {@code BrowserSession} owns a fixed-size {@code RetainedEvent[]} pool that is cycled
 * through as a free-list — entries are NEVER allocated on the hot path. Phase 5/6 wires the
 * pooling.
 *
 * <p><b>Threading.</b> Mutable; OWNED by exactly one Netty worker per session. NOT thread-safe.
 *
 * <p><b>Allocation.</b> The instance itself is allocated once at session creation. {@link #set*}
 * methods only mutate primitive / reference fields — no new allocation. String fields are stored by
 * reference; the caller must ensure they outlive the queued entry (in production, all such strings
 * are interned identifiers from FIX or the per-session quote cache, both of which pre-date the
 * retain entry).
 *
 * <p><b>Lifecycle.</b> One pool per browser session, sized to {@code
 * outboundQueueCapacityPerSession}. Released when the session closes.
 *
 * <p><b>Dependencies.</b> JDK only.
 */
public final class RetainedEvent {

  // Discriminator: only the three retainable event kinds + a "free" sentinel for pool entries.

  /** Sentinel: this entry is currently in the free-list and not bound to any event. */
  public static final int KIND_FREE = 0;

  /** Carries an {@link BrowserEvent.ExecutionReport}. */
  public static final int KIND_EXEC_REPORT = 1;

  /** Carries an {@link BrowserEvent.OrderReject}. */
  public static final int KIND_ORDER_REJECT = 2;

  /** Carries the singleton {@link BrowserEvent.AuthExpired}. */
  public static final int KIND_AUTH_EXPIRED = 3;

  // ---------------------------------------------------------------------------
  // Storage. Public final because each call-site iterates fixed fields and we
  // value the lack of accessor overhead. Mutator setters guarantee the
  // pre-flight kind validation so callers can't put the entry in an
  // inconsistent state.
  // ---------------------------------------------------------------------------

  private int kind = KIND_FREE;

  // ExecutionReport / OrderReject shared
  private String clOrdId;
  private String reason;

  // ExecutionReport-only
  private String execId;
  private char execType;
  private char ordStatus;
  private String symbol;
  private String side;
  private long cumQtyInt64;
  private long leavesQtyInt64;
  private long avgPxInt64;

  /** Constructs a fresh, free-listed entry. */
  public RetainedEvent() {}

  /**
   * @return current discriminator (one of {@code KIND_*}).
   */
  public int kind() {
    return kind;
  }

  /** Reset to the {@link #KIND_FREE} state and null out all reference fields. */
  public void release() {
    this.kind = KIND_FREE;
    this.clOrdId = null;
    this.reason = null;
    this.execId = null;
    this.symbol = null;
    this.side = null;
    this.execType = 0;
    this.ordStatus = 0;
    this.cumQtyInt64 = 0L;
    this.leavesQtyInt64 = 0L;
    this.avgPxInt64 = 0L;
  }

  /**
   * Bind this entry to an {@link BrowserEvent.OrderReject}. Caller must guarantee the entry was in
   * {@link #KIND_FREE}.
   *
   * @param clOrdId originating client order id (non-null)
   * @param reason short textual reason (non-null, ≤128 bytes recommended)
   * @throws IllegalStateException if the entry is not currently free
   */
  public void setOrderReject(final String clOrdId, final String reason) {
    if (this.kind != KIND_FREE) {
      throw new IllegalStateException("RetainedEvent already bound: kind=" + this.kind);
    }
    this.kind = KIND_ORDER_REJECT;
    this.clOrdId = clOrdId;
    this.reason = reason;
  }

  /**
   * Bind this entry to an {@link BrowserEvent.ExecutionReport}.
   *
   * @param clOrdId originating client order id
   * @param execId server-assigned execution id
   * @param execType FIX {@code ExecType (150)} char
   * @param ordStatus FIX {@code OrdStatus (39)} char
   * @param symbol FIX symbol
   * @param side FIX side ({@code "Buy"} or {@code "Sell"})
   * @param cumQtyInt64 fixed-point cumulative qty
   * @param leavesQtyInt64 fixed-point leaves qty
   * @param avgPxInt64 fixed-point avg price
   * @throws IllegalStateException if the entry is not currently free
   */
  public void setExecutionReport(
      final String clOrdId,
      final String execId,
      final char execType,
      final char ordStatus,
      final String symbol,
      final String side,
      final long cumQtyInt64,
      final long leavesQtyInt64,
      final long avgPxInt64) {
    if (this.kind != KIND_FREE) {
      throw new IllegalStateException("RetainedEvent already bound: kind=" + this.kind);
    }
    this.kind = KIND_EXEC_REPORT;
    this.clOrdId = clOrdId;
    this.execId = execId;
    this.execType = execType;
    this.ordStatus = ordStatus;
    this.symbol = symbol;
    this.side = side;
    this.cumQtyInt64 = cumQtyInt64;
    this.leavesQtyInt64 = leavesQtyInt64;
    this.avgPxInt64 = avgPxInt64;
  }

  /**
   * Bind this entry to the singleton {@link BrowserEvent.AuthExpired}.
   *
   * @throws IllegalStateException if the entry is not currently free
   */
  public void setAuthExpired() {
    if (this.kind != KIND_FREE) {
      throw new IllegalStateException("RetainedEvent already bound: kind=" + this.kind);
    }
    this.kind = KIND_AUTH_EXPIRED;
  }

  // ---------------------------------------------------------------------------
  // Read-side accessors.
  // ---------------------------------------------------------------------------

  /**
   * @return clOrdId (only valid if {@link #kind()} is exec report or order reject)
   */
  public String clOrdId() {
    return clOrdId;
  }

  /**
   * @return reason (only valid for order reject)
   */
  public String reason() {
    return reason;
  }

  /**
   * @return execId (only valid for exec report)
   */
  public String execId() {
    return execId;
  }

  /**
   * @return execType (only valid for exec report)
   */
  public char execType() {
    return execType;
  }

  /**
   * @return ordStatus (only valid for exec report)
   */
  public char ordStatus() {
    return ordStatus;
  }

  /**
   * @return symbol (only valid for exec report)
   */
  public String symbol() {
    return symbol;
  }

  /**
   * @return side (only valid for exec report)
   */
  public String side() {
    return side;
  }

  /**
   * @return cumQty fixed-point (only valid for exec report)
   */
  public long cumQtyInt64() {
    return cumQtyInt64;
  }

  /**
   * @return leavesQty fixed-point (only valid for exec report)
   */
  public long leavesQtyInt64() {
    return leavesQtyInt64;
  }

  /**
   * @return avgPx fixed-point (only valid for exec report)
   */
  public long avgPxInt64() {
    return avgPxInt64;
  }
}
