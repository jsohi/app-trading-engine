package com.trading.engine.fixbridge.json;

import com.trading.engine.fixbridge.translator.DecimalStringEmitter;
import io.netty.buffer.ByteBuf;
import java.nio.charset.StandardCharsets;
import org.agrona.concurrent.UnsafeBuffer;

/**
 * Zero-allocation outbound JSON event writer.
 *
 * <p><b>Purpose.</b> Serialise each of the seven outbound {@link BrowserEvent} kinds directly into
 * a Netty {@link ByteBuf} as a UTF-8 JSON object. The writer never instantiates a {@code String},
 * never uses a JSON library, and never allocates outside its own constructor. It is the byte-exact
 * complement of {@link BrowserMessageReader}.
 *
 * <p><b>Threading.</b> Per-instance — owns a {@link DecimalStringEmitter} and a small scratch
 * wrapper. NOT thread-safe; one per Netty worker (Phase 6 wires this).
 *
 * <p><b>Allocation.</b> Zero on the hot path. String fields are written via {@link
 * #writeJsonStringValue(String, ByteBuf)} which iterates the supplied {@link String} a {@code char}
 * at a time and writes each as a UTF-8 byte sequence directly into the destination buffer — no
 * intermediate {@code byte[]}. Fixed-point numerics flow through {@link DecimalStringEmitter}.
 *
 * <p><b>Lifecycle.</b> Per-handler instance, allocated alongside {@link DecimalStringEmitter}.
 *
 * <p><b>Dependencies.</b> Netty {@link ByteBuf}, Agrona ({@link UnsafeBuffer} via the emitter), and
 * {@link DecimalStringEmitter}.
 *
 * <p><b>Escaping.</b> Per locked wire-format constraint, string values are 7-bit ASCII without
 * embedded control characters or backslashes. The writer rejects an embedded {@code "} or {@code
 * \\} with {@link IllegalArgumentException} rather than emitting it escaped — bridge components
 * upstream of the writer must already have sanitised values (e.g. FIX {@code text} fields with
 * {@code SOH} stripped). Control characters {@code 0x00..0x1F} are likewise rejected.
 */
public final class BrowserEventWriter {

  // --- Pre-computed key prefixes for each event type. Stored as byte[] so each
  //     write is a single ByteBuf.writeBytes(byte[], int, int). Names use ASCII
  //     so multi-byte UTF-8 considerations don't apply.

  private static final byte[] HDR_QUOTE = ascii("{\"type\":\"Quote\"");
  private static final byte[] HDR_EXEC = ascii("{\"type\":\"ExecutionReport\"");
  private static final byte[] HDR_ORDER_REJECT = ascii("{\"type\":\"OrderReject\"");
  private static final byte[] HDR_BRIDGE_STATUS = ascii("{\"type\":\"BridgeStatus\"");
  private static final byte[] HDR_RAW_FIX = ascii("{\"type\":\"RawFix\"");
  private static final byte[] HDR_AUTH_EXPIRED = ascii("{\"type\":\"AuthExpired\"}");
  private static final byte[] HDR_ERROR = ascii("{\"type\":\"Error\"");
  private static final byte[] HDR_ACCOUNT_LIMITS = ascii("{\"type\":\"AccountLimits\"");
  private static final byte[] HDR_SESSION_TERMINATED = ascii("{\"type\":\"SessionTerminated\"}");
  private static final byte[] HDR_ORDER_RECONCILED = ascii("{\"type\":\"OrderReconciled\"");
  private static final byte[] HDR_ORDER_STATUS_REPLY = ascii("{\"type\":\"OrderStatusReply\"");

  private static final byte[] K_REQ_ID = ascii(",\"reqId\":\"");
  private static final byte[] K_QUOTE_ID = ascii(",\"quoteId\":\"");
  private static final byte[] K_SYMBOL = ascii(",\"symbol\":\"");
  private static final byte[] K_SIDE = ascii(",\"side\":\"");
  private static final byte[] K_QTY = ascii(",\"qty\":\"");
  private static final byte[] K_PRICE = ascii(",\"price\":\"");
  private static final byte[] K_EXPIRY = ascii(",\"expiryNs\":");
  private static final byte[] K_CL_ORD_ID = ascii(",\"clOrdId\":\"");
  private static final byte[] K_EXEC_ID = ascii(",\"execId\":\"");
  private static final byte[] K_EXEC_TYPE = ascii(",\"execType\":\"");
  private static final byte[] K_ORD_STATUS = ascii(",\"ordStatus\":\"");
  private static final byte[] K_CUM_QTY = ascii(",\"cumQty\":\"");
  private static final byte[] K_LEAVES_QTY = ascii(",\"leavesQty\":\"");
  private static final byte[] K_AVG_PX = ascii(",\"avgPx\":\"");
  private static final byte[] K_REASON = ascii(",\"reason\":\"");
  private static final byte[] K_FIX_SESSION_UP = ascii(",\"fixSessionUp\":");
  private static final byte[] K_FATAL = ascii(",\"fatal\":");
  private static final byte[] K_DIRECTION = ascii(",\"direction\":\"");
  private static final byte[] K_FIX = ascii(",\"fix\":\"");
  private static final byte[] K_RECEIVED = ascii(",\"received\":\"");
  private static final byte[] K_NEW_ORDERS = ascii(",\"newOrders\":");
  private static final byte[] K_NEW_QUOTES = ascii(",\"newQuotes\":");
  private static final byte[] K_PROTOCOL_VERSION = ascii(",\"protocolVersion\":");
  private static final byte[] K_SERVER_ORDER_TIMEOUT_MS = ascii(",\"serverOrderTimeoutMs\":");
  private static final byte[] K_ACCOUNT = ascii(",\"account\":\"");
  private static final byte[] K_MAX_QTY = ascii(",\"maxQty\":\"");
  private static final byte[] K_MAX_NOTIONAL = ascii(",\"maxNotional\":\"");
  private static final byte[] K_PRICE_DEVIATION_BPS = ascii(",\"priceDeviationBps\":");
  private static final byte[] K_MAX_ORDERS_PER_SECOND = ascii(",\"maxOrdersPerSecond\":");
  private static final byte[] K_STATUS = ascii(",\"status\":\"");
  private static final byte[] K_LAST_EXEC_ID = ascii(",\"lastExecId\":\"");

  private static final byte[] V_TRUE = ascii("true");
  private static final byte[] V_FALSE = ascii("false");

  private static final byte CLOSE_QUOTE = (byte) '"';
  private static final byte CLOSE_BRACE = (byte) '}';

  // --- Per-instance state ---

  private final DecimalStringEmitter decimalEmitter;
  // Scratch wrapper exposed so internal helpers can leverage Agrona's putLongAscii on a
  // byte[]. Sized to 32 bytes — aligned with DecimalStringEmitter.SCRATCH_CAPACITY — so the
  // worst-case "-9223372036854775808" (20 chars) plus a 2-byte safety padding fits with room
  // for cache-line alignment.
  private static final int LONG_SCRATCH_CAPACITY = 32;

  private final byte[] longScratch = new byte[LONG_SCRATCH_CAPACITY];
  private final UnsafeBuffer longView = new UnsafeBuffer(longScratch);

  /**
   * Constructs a writer using a caller-supplied emitter. Caller MUST NOT share the emitter with
   * another thread.
   *
   * @param decimalEmitter per-handler emitter (must not be null)
   */
  public BrowserEventWriter(final DecimalStringEmitter decimalEmitter) {
    if (decimalEmitter == null) {
      throw new IllegalArgumentException("decimalEmitter must not be null");
    }
    this.decimalEmitter = decimalEmitter;
  }

  // ---------------------------------------------------------------------------
  // Public API — one method per event kind.
  // ---------------------------------------------------------------------------

  /**
   * Serialise a {@link BrowserEvent.Quote} into {@code dst}.
   *
   * @param e event to serialise (non-null)
   * @param dst destination buffer
   * @return number of bytes appended to {@code dst}
   */
  public int writeQuote(final BrowserEvent.Quote e, final ByteBuf dst) {
    final int start = dst.writerIndex();
    try {
      dst.writeBytes(HDR_QUOTE);
      dst.writeBytes(K_REQ_ID);
      writeJsonStringValue(e.reqId(), dst);
      dst.writeByte(CLOSE_QUOTE);
      dst.writeBytes(K_QUOTE_ID);
      writeJsonStringValue(e.quoteId(), dst);
      dst.writeByte(CLOSE_QUOTE);
      dst.writeBytes(K_SYMBOL);
      writeJsonStringValue(e.symbol(), dst);
      dst.writeByte(CLOSE_QUOTE);
      dst.writeBytes(K_SIDE);
      writeJsonStringValue(e.side(), dst);
      dst.writeByte(CLOSE_QUOTE);
      dst.writeBytes(K_QTY);
      decimalEmitter.emitInt64FixedPoint(e.qtyInt64(), dst);
      dst.writeByte(CLOSE_QUOTE);
      dst.writeBytes(K_PRICE);
      decimalEmitter.emitInt64FixedPoint(e.priceInt64(), dst);
      dst.writeByte(CLOSE_QUOTE);
      dst.writeBytes(K_EXPIRY);
      writeLong(e.expiryNs(), dst);
      dst.writeByte(CLOSE_BRACE);
      return dst.writerIndex() - start;
    } catch (final RuntimeException ex) {
      // Roll back any partial write so the caller observes an all-or-nothing buffer state.
      // Without this, mid-write rejection (e.g. forbidden character on a later field) leaves the
      // outbound buffer with an incomplete JSON object that the WS framer would emit downstream.
      dst.writerIndex(start);
      throw ex;
    }
  }

  /**
   * Serialise an {@link BrowserEvent.ExecutionReport}.
   *
   * @param e event to serialise
   * @param dst destination buffer
   * @return number of bytes appended
   */
  public int writeExecutionReport(final BrowserEvent.ExecutionReport e, final ByteBuf dst) {
    final int start = dst.writerIndex();
    try {
      dst.writeBytes(HDR_EXEC);
      dst.writeBytes(K_CL_ORD_ID);
      writeJsonStringValue(e.clOrdId(), dst);
      dst.writeByte(CLOSE_QUOTE);
      dst.writeBytes(K_EXEC_ID);
      writeJsonStringValue(e.execId(), dst);
      dst.writeByte(CLOSE_QUOTE);
      dst.writeBytes(K_EXEC_TYPE);
      writeJsonChar(e.execType(), dst);
      dst.writeByte(CLOSE_QUOTE);
      dst.writeBytes(K_ORD_STATUS);
      writeJsonChar(e.ordStatus(), dst);
      dst.writeByte(CLOSE_QUOTE);
      dst.writeBytes(K_SYMBOL);
      writeJsonStringValue(e.symbol(), dst);
      dst.writeByte(CLOSE_QUOTE);
      dst.writeBytes(K_SIDE);
      writeJsonStringValue(e.side(), dst);
      dst.writeByte(CLOSE_QUOTE);
      dst.writeBytes(K_CUM_QTY);
      decimalEmitter.emitInt64FixedPoint(e.cumQtyInt64(), dst);
      dst.writeByte(CLOSE_QUOTE);
      dst.writeBytes(K_LEAVES_QTY);
      decimalEmitter.emitInt64FixedPoint(e.leavesQtyInt64(), dst);
      dst.writeByte(CLOSE_QUOTE);
      dst.writeBytes(K_AVG_PX);
      decimalEmitter.emitInt64FixedPoint(e.avgPxInt64(), dst);
      dst.writeByte(CLOSE_QUOTE);
      dst.writeByte(CLOSE_BRACE);
      return dst.writerIndex() - start;
    } catch (final RuntimeException ex) {
      dst.writerIndex(start);
      throw ex;
    }
  }

  /**
   * Serialise an {@link BrowserEvent.OrderReject}.
   *
   * @param e event to serialise
   * @param dst destination buffer
   * @return number of bytes appended
   */
  public int writeOrderReject(final BrowserEvent.OrderReject e, final ByteBuf dst) {
    final int start = dst.writerIndex();
    try {
      dst.writeBytes(HDR_ORDER_REJECT);
      dst.writeBytes(K_CL_ORD_ID);
      writeJsonStringValue(e.clOrdId(), dst);
      dst.writeByte(CLOSE_QUOTE);
      dst.writeBytes(K_REASON);
      writeJsonStringValue(e.reason(), dst);
      dst.writeByte(CLOSE_QUOTE);
      dst.writeByte(CLOSE_BRACE);
      return dst.writerIndex() - start;
    } catch (final RuntimeException ex) {
      dst.writerIndex(start);
      throw ex;
    }
  }

  /**
   * Serialise a {@link BrowserEvent.BridgeStatus} including all 7 fields (the runtime kill-switch
   * gates {@code newOrders}/{@code newQuotes}, the worker's {@code protocolVersion} probe, and the
   * cluster's {@code serverOrderTimeoutMs} for the UI's STUCK_LONG transition — Day 1 added these
   * to the record but Day 4-c left the writer emitting only the legacy 3-field form).
   *
   * @param e event to serialise
   * @param dst destination buffer
   * @return number of bytes appended
   */
  public int writeBridgeStatus(final BrowserEvent.BridgeStatus e, final ByteBuf dst) {
    final int start = dst.writerIndex();
    try {
      dst.writeBytes(HDR_BRIDGE_STATUS);
      dst.writeBytes(K_FIX_SESSION_UP);
      dst.writeBytes(e.fixSessionUp() ? V_TRUE : V_FALSE);
      dst.writeBytes(K_FATAL);
      dst.writeBytes(e.fatal() ? V_TRUE : V_FALSE);
      dst.writeBytes(K_REASON);
      writeJsonStringValue(e.reason(), dst);
      dst.writeByte(CLOSE_QUOTE);
      dst.writeBytes(K_NEW_ORDERS);
      dst.writeBytes(e.newOrders() ? V_TRUE : V_FALSE);
      dst.writeBytes(K_NEW_QUOTES);
      dst.writeBytes(e.newQuotes() ? V_TRUE : V_FALSE);
      dst.writeBytes(K_PROTOCOL_VERSION);
      writeLong(e.protocolVersion(), dst);
      dst.writeBytes(K_SERVER_ORDER_TIMEOUT_MS);
      writeLong(e.serverOrderTimeoutMs(), dst);
      dst.writeByte(CLOSE_BRACE);
      return dst.writerIndex() - start;
    } catch (final RuntimeException ex) {
      dst.writerIndex(start);
      throw ex;
    }
  }

  /**
   * Serialise an {@link BrowserEvent.AccountLimits} push frame (§3.14). Server-authoritative
   * pre-trade limits the UI uses to gate submit buttons.
   *
   * @param e event to serialise
   * @param dst destination buffer
   * @return number of bytes appended
   */
  public int writeAccountLimits(final BrowserEvent.AccountLimits e, final ByteBuf dst) {
    final int start = dst.writerIndex();
    try {
      dst.writeBytes(HDR_ACCOUNT_LIMITS);
      dst.writeBytes(K_ACCOUNT);
      writeJsonStringValue(e.account(), dst);
      dst.writeByte(CLOSE_QUOTE);
      dst.writeBytes(K_MAX_QTY);
      decimalEmitter.emitInt64FixedPoint(e.maxQtyInt64(), dst);
      dst.writeByte(CLOSE_QUOTE);
      dst.writeBytes(K_MAX_NOTIONAL);
      decimalEmitter.emitInt64FixedPoint(e.maxNotionalInt64(), dst);
      dst.writeByte(CLOSE_QUOTE);
      dst.writeBytes(K_PRICE_DEVIATION_BPS);
      writeLong(e.priceDeviationBps(), dst);
      dst.writeBytes(K_MAX_ORDERS_PER_SECOND);
      writeLong(e.maxOrdersPerSecond(), dst);
      dst.writeByte(CLOSE_BRACE);
      return dst.writerIndex() - start;
    } catch (final RuntimeException ex) {
      dst.writerIndex(start);
      throw ex;
    }
  }

  /**
   * Serialise the singleton {@link BrowserEvent.SessionTerminated} event — sent immediately before
   * WS close 4002 when {@code signOut()} fires on a sibling session of the same {@code sub} (§3.3 /
   * §3.7 / §4.9).
   *
   * @param dst destination buffer
   * @return number of bytes appended
   */
  public int writeSessionTerminated(final ByteBuf dst) {
    final int start = dst.writerIndex();
    try {
      dst.writeBytes(HDR_SESSION_TERMINATED);
      return dst.writerIndex() - start;
    } catch (final RuntimeException ex) {
      dst.writerIndex(start);
      throw ex;
    }
  }

  /**
   * Serialise an {@link BrowserEvent.OrderReconciled} event — emitted when the cluster's TTL fires
   * on a STUCK / STUCK_LONG order (§4.5) and the bridge forwards the authoritative final state.
   *
   * @param e event to serialise
   * @param dst destination buffer
   * @return number of bytes appended
   */
  public int writeOrderReconciled(final BrowserEvent.OrderReconciled e, final ByteBuf dst) {
    final int start = dst.writerIndex();
    try {
      dst.writeBytes(HDR_ORDER_RECONCILED);
      dst.writeBytes(K_CL_ORD_ID);
      writeJsonStringValue(e.clOrdId(), dst);
      dst.writeByte(CLOSE_QUOTE);
      dst.writeBytes(K_STATUS);
      writeJsonStringValue(e.status(), dst);
      dst.writeByte(CLOSE_QUOTE);
      dst.writeBytes(K_CUM_QTY);
      decimalEmitter.emitInt64FixedPoint(e.cumQtyInt64(), dst);
      dst.writeByte(CLOSE_QUOTE);
      dst.writeBytes(K_LEAVES_QTY);
      decimalEmitter.emitInt64FixedPoint(e.leavesQtyInt64(), dst);
      dst.writeByte(CLOSE_QUOTE);
      dst.writeBytes(K_AVG_PX);
      decimalEmitter.emitInt64FixedPoint(e.avgPxInt64(), dst);
      dst.writeByte(CLOSE_QUOTE);
      dst.writeByte(CLOSE_BRACE);
      return dst.writerIndex() - start;
    } catch (final RuntimeException ex) {
      dst.writerIndex(start);
      throw ex;
    }
  }

  /**
   * Serialise an {@link BrowserEvent.OrderStatusReply} — synthesised from the cluster's {@code
   * OrderQueryByClOrdId} projection result (§3.15). Emits the optional {@code lastExecId} key only
   * when non-null.
   *
   * @param e event to serialise
   * @param dst destination buffer
   * @return number of bytes appended
   */
  public int writeOrderStatusReply(final BrowserEvent.OrderStatusReply e, final ByteBuf dst) {
    final int start = dst.writerIndex();
    try {
      dst.writeBytes(HDR_ORDER_STATUS_REPLY);
      dst.writeBytes(K_CL_ORD_ID);
      writeJsonStringValue(e.clOrdId(), dst);
      dst.writeByte(CLOSE_QUOTE);
      dst.writeBytes(K_STATUS);
      writeJsonStringValue(e.status(), dst);
      dst.writeByte(CLOSE_QUOTE);
      dst.writeBytes(K_CUM_QTY);
      decimalEmitter.emitInt64FixedPoint(e.cumQtyInt64(), dst);
      dst.writeByte(CLOSE_QUOTE);
      dst.writeBytes(K_LEAVES_QTY);
      decimalEmitter.emitInt64FixedPoint(e.leavesQtyInt64(), dst);
      dst.writeByte(CLOSE_QUOTE);
      dst.writeBytes(K_AVG_PX);
      decimalEmitter.emitInt64FixedPoint(e.avgPxInt64(), dst);
      dst.writeByte(CLOSE_QUOTE);
      if (e.lastExecId() != null) {
        dst.writeBytes(K_LAST_EXEC_ID);
        writeJsonStringValue(e.lastExecId(), dst);
        dst.writeByte(CLOSE_QUOTE);
      }
      dst.writeByte(CLOSE_BRACE);
      return dst.writerIndex() - start;
    } catch (final RuntimeException ex) {
      dst.writerIndex(start);
      throw ex;
    }
  }

  /**
   * Polymorphic dispatch to the type-specific {@code write*} method. Used by the outbound drainer
   * which holds {@link BrowserEvent} references generically rather than per concrete subtype.
   *
   * @param e event to serialise
   * @param dst destination buffer
   * @return number of bytes appended
   */
  public int writeAny(final BrowserEvent e, final ByteBuf dst) {
    if (e instanceof BrowserEvent.Quote q) {
      return writeQuote(q, dst);
    }
    if (e instanceof BrowserEvent.ExecutionReport r) {
      return writeExecutionReport(r, dst);
    }
    if (e instanceof BrowserEvent.OrderReject r) {
      return writeOrderReject(r, dst);
    }
    if (e instanceof BrowserEvent.BridgeStatus s) {
      return writeBridgeStatus(s, dst);
    }
    if (e instanceof BrowserEvent.RawFix r) {
      return writeRawFix(r, dst);
    }
    if (e instanceof BrowserEvent.AuthExpired) {
      return writeAuthExpired(dst);
    }
    if (e instanceof BrowserEvent.Error err) {
      return writeError(err, dst);
    }
    if (e instanceof BrowserEvent.AccountLimits a) {
      return writeAccountLimits(a, dst);
    }
    if (e instanceof BrowserEvent.SessionTerminated) {
      return writeSessionTerminated(dst);
    }
    if (e instanceof BrowserEvent.OrderReconciled r) {
      return writeOrderReconciled(r, dst);
    }
    if (e instanceof BrowserEvent.OrderStatusReply r) {
      return writeOrderStatusReply(r, dst);
    }
    throw new IllegalArgumentException("unknown BrowserEvent: " + e.getClass().getName());
  }

  /**
   * Serialise a {@link BrowserEvent.RawFix}.
   *
   * @param e event to serialise
   * @param dst destination buffer
   * @return number of bytes appended
   */
  public int writeRawFix(final BrowserEvent.RawFix e, final ByteBuf dst) {
    final int start = dst.writerIndex();
    try {
      dst.writeBytes(HDR_RAW_FIX);
      dst.writeBytes(K_DIRECTION);
      writeJsonStringValue(e.direction(), dst);
      dst.writeByte(CLOSE_QUOTE);
      dst.writeBytes(K_FIX);
      writeJsonStringValue(e.fix(), dst);
      dst.writeByte(CLOSE_QUOTE);
      dst.writeByte(CLOSE_BRACE);
      return dst.writerIndex() - start;
    } catch (final RuntimeException ex) {
      dst.writerIndex(start);
      throw ex;
    }
  }

  /**
   * Serialise the singleton {@code AuthExpired} event.
   *
   * @param dst destination buffer
   * @return number of bytes appended
   */
  public int writeAuthExpired(final ByteBuf dst) {
    final int start = dst.writerIndex();
    try {
      dst.writeBytes(HDR_AUTH_EXPIRED);
      return dst.writerIndex() - start;
    } catch (final RuntimeException ex) {
      dst.writerIndex(start);
      throw ex;
    }
  }

  /**
   * Serialise a generic {@link BrowserEvent.Error}. Emits the optional {@code "received"} key only
   * when {@link BrowserEvent.Error#received()} is non-null — taxonomy-only errors omit it.
   *
   * @param e event to serialise
   * @param dst destination buffer
   * @return number of bytes appended
   */
  public int writeError(final BrowserEvent.Error e, final ByteBuf dst) {
    final int start = dst.writerIndex();
    try {
      dst.writeBytes(HDR_ERROR);
      dst.writeBytes(K_REASON);
      writeJsonStringValue(e.reason(), dst);
      dst.writeByte(CLOSE_QUOTE);
      if (e.received() != null) {
        dst.writeBytes(K_RECEIVED);
        writeJsonStringValue(e.received(), dst);
        dst.writeByte(CLOSE_QUOTE);
      }
      dst.writeByte(CLOSE_BRACE);
      return dst.writerIndex() - start;
    } catch (final RuntimeException ex) {
      dst.writerIndex(start);
      throw ex;
    }
  }

  // ---------------------------------------------------------------------------
  // Internals.
  // ---------------------------------------------------------------------------

  /**
   * Write the contents of {@code s} as a UTF-8 byte sequence into {@code dst}, NOT including the
   * surrounding quotes (callers handle those). Rejects {@code "} (0x22), {@code \\} (0x5C), and any
   * code point in {@code 0x00..0x1F} — the wire protocol forbids them.
   *
   * <p>Delegates to {@link Utf8JsonStringEmitter#appendStringValue(String, ByteBuf)} — single
   * implementation shared with {@code FixToJsonTranslator}.
   */
  private static void writeJsonStringValue(final String s, final ByteBuf dst) {
    Utf8JsonStringEmitter.appendStringValue(s, dst);
  }

  /**
   * Write a single FIX-style {@code char} (e.g. {@code ExecType}) as one ASCII byte, validating it
   * is printable. Stricter than {@link #writeJsonStringValue} because FIX enum chars are always a
   * single non-control ASCII byte.
   */
  private static void writeJsonChar(final char c, final ByteBuf dst) {
    if (c == '"' || c == '\\' || c < 0x20 || c >= 0x7F) {
      throw new IllegalArgumentException("non-printable FIX char: " + (int) c);
    }
    dst.writeByte((byte) c);
  }

  /**
   * Write the supplied {@code long} in decimal ASCII form into {@code dst} via Agrona's {@code
   * putLongAscii}. Zero allocation.
   */
  private void writeLong(final long value, final ByteBuf dst) {
    final int digits = longView.putLongAscii(0, value);
    dst.writeBytes(longScratch, 0, digits);
  }

  private static byte[] ascii(final String s) {
    return s.getBytes(StandardCharsets.US_ASCII);
  }
}
