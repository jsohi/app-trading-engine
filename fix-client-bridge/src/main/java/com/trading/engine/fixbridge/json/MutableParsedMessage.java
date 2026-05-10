package com.trading.engine.fixbridge.json;

import java.nio.charset.StandardCharsets;

/**
 * Hot-path flyweight that captures the parsed shape of a single inbound JSON frame WITHOUT
 * allocating any objects. The frame's bytes are copied once from the inbound Netty {@link
 * io.netty.buffer.ByteBuf} into the {@link #scratch} array; every string-valued field is then
 * recorded as an {@code (offset, length)} slice into that array. Numeric and enum-coded fields are
 * decoded eagerly into primitive members.
 *
 * <p><b>Purpose.</b> Eliminate per-frame allocation on the inbound JSON path. The translator layer
 * (Phase 4 — {@code JsonToFixTranslator}) reads the slices directly into Artio FIX encoder field
 * setters using the primitive {@code (byte[], int, int)} variants — no intermediate {@code String}
 * is created end-to-end on the dispatch path.
 *
 * <p><b>Threading.</b> Not thread-safe. Owned by exactly one Netty worker handler per channel; the
 * channel is bound to a single Netty {@code EventExecutor} which guarantees serial dispatch into
 * {@code BrowserSessionHandler.channelRead0}. Any cross-thread sharing is a bug.
 *
 * <p><b>Allocation.</b> Zero — the {@link #scratch} array is pre-allocated at construction ({@code
 * 64 * 1024} bytes per session, matching the per-frame cap in {@link
 * BrowserMessageReader#MAX_BYTES}). All other state is primitive. {@link #reset()} merely zeroes
 * the primitive headers; the underlying array is reused indefinitely.
 *
 * <p><b>Lifecycle.</b> One instance per browser channel, allocated by the per-channel {@code
 * BrowserSessionHandler} at handler creation, released to GC when the channel is closed and the
 * handler is removed from the pipeline.
 *
 * <p><b>Memory.</b> 64 KiB per channel × 256 max sessions ≈ 16 MiB resident; documented in {@code
 * FixClientBridgeConfig}.
 *
 * <p><b>Dependencies.</b> JDK only.
 *
 * <p><b>Slice convention.</b> An offset/length pair of {@code (-1, 0)} indicates the field was
 * absent from the parsed message. {@code (off, 0)} (where {@code off >= 0}) means the field was
 * present but empty (which the translator must reject as malformed for required fields).
 */
public final class MutableParsedMessage {

  // ---------------------------------------------------------------------------
  // Type tags. Stored as plain ints rather than an enum to make the dispatch
  // switch in JsonToFixTranslator a tableswitch — saving the loadEnum + bounds
  // check that Java's switch-on-enum compiles to.
  // ---------------------------------------------------------------------------

  /** Sentinel for {@link #type} indicating no message has been parsed yet. */
  public static final int TYPE_NONE = 0;

  public static final int TYPE_AUTH = 1;
  public static final int TYPE_QUOTE_REQUEST = 2;
  public static final int TYPE_ACCEPT_QUOTE = 3;
  public static final int TYPE_REJECT_QUOTE = 4;
  public static final int TYPE_NEW_ORDER_SINGLE = 5;
  public static final int TYPE_CANCEL_ORDER = 6;
  /** {@code OrderStatusRequest} — recovery path for STUCK/STUCK_LONG quote rows (§3.15). */
  public static final int TYPE_ORDER_STATUS_REQUEST = 7;

  // ---------------------------------------------------------------------------
  // Side, OrdType, and TimeInForce sentinels. Stored as bytes matching the FIX
  // 4.4 wire char so the translator can pass them straight through to the
  // Artio encoder. 0 means "absent / unset"; non-zero is the wire char.
  // ---------------------------------------------------------------------------

  /** Sentinel returned for {@link #side}/{@link #ordType}/{@link #timeInForce} when absent. */
  public static final byte ABSENT = 0;

  // FIX Side (54)
  public static final byte SIDE_BUY = '1';
  public static final byte SIDE_SELL = '2';

  // FIX OrdType (40)
  public static final byte ORDTYPE_MARKET = '1';
  public static final byte ORDTYPE_LIMIT = '2';
  public static final byte ORDTYPE_PREVIOUSLY_QUOTED = 'D';

  // FIX TimeInForce (59)
  public static final byte TIF_DAY = '0';
  public static final byte TIF_GTC = '1';
  public static final byte TIF_IOC = '3';
  public static final byte TIF_FOK = '4';
  public static final byte TIF_GTD = '6';

  // ---------------------------------------------------------------------------
  // Storage.
  // ---------------------------------------------------------------------------

  /**
   * Per-channel scratch buffer that receives the raw frame bytes. Sized to match {@link
   * BrowserMessageReader#MAX_BYTES} — frames larger than this are rejected before any copy occurs.
   *
   * <p>{@code public final} so the translator can read slices via {@code scratch[off+i]} without an
   * accessor call; the caller MUST treat it as read-only after parsing.
   */
  public final byte[] scratch;

  /**
   * One of the {@code TYPE_*} constants; {@link #TYPE_NONE} until {@link #reset()} has been called
   * and a parse has succeeded.
   */
  public int type;

  // String-valued fields stored as (offset, length) slices into scratch.
  // (-1, 0) = absent; (off, 0) = present-but-empty.

  public int reqIdOff = -1;
  public int reqIdLen;

  public int symbolOff = -1;
  public int symbolLen;

  public int priceOff = -1;
  public int priceLen;

  public int qtyOff = -1;
  public int qtyLen;

  public int clOrdIdOff = -1;
  public int clOrdIdLen;

  public int origClOrdIdOff = -1;
  public int origClOrdIdLen;

  public int quoteIdOff = -1;
  public int quoteIdLen;

  public int tokenOff = -1;
  public int tokenLen;

  public int accountOff = -1;
  public int accountLen;

  /**
   * W3C trace-context {@code traceparent} extracted from the optional inbound {@code _meta}
   * envelope (§3.6). Slice into {@link #scratch}; {@code (-1, 0)} when absent. The parser
   * accepts any string here; {@link BrowserMessageReader#isValidTraceparent} is a separate
   * validator the dispatcher runs to gate emission of {@code Error{reason:"malformed-traceparent"}}
   * — the parser deliberately does NOT throw on a malformed traceparent so the carrying command
   * still processes (per §3.6 "drop the trace context but keep processing the command").
   */
  public int traceparentOff = -1;

  public int traceparentLen;

  /**
   * Eagerly-decoded {@code OrderQty (38)} as fixed-point int64 (scale {@code 10^-8}). Set to {@link
   * Long#MIN_VALUE} when absent — the translator uses this as the absent-sentinel because {@code
   * Long.MIN_VALUE} is also rejected by {@code DecimalStringEmitter} (Phase 3) so it cannot collide
   * with a real value. The {@code priceOff/Len} slice carries the original ASCII price for the FIX
   * wire to avoid any double-rounding through the int64 representation.
   */
  public long qty;

  public byte side;
  public byte ordType;
  public byte timeInForce;

  /**
   * Constructs a flyweight whose {@link #scratch} buffer is sized to {@link
   * BrowserMessageReader#MAX_BYTES}. Allocates exactly one {@code byte[]} of that size — the only
   * allocation associated with the object. After construction the instance is in the same state as
   * {@link #reset()}.
   */
  public MutableParsedMessage() {
    this.scratch = new byte[BrowserMessageReader.MAX_BYTES];
    reset();
  }

  /**
   * Resets every primitive header to its absent / sentinel state. Does NOT zero the {@link
   * #scratch} array — the new parse will overwrite the prefix it actually uses, and stale tail
   * bytes are never read because every consumer respects the slice lengths.
   */
  public void reset() {
    type = TYPE_NONE;

    reqIdOff = -1;
    reqIdLen = 0;
    symbolOff = -1;
    symbolLen = 0;
    priceOff = -1;
    priceLen = 0;
    qtyOff = -1;
    qtyLen = 0;
    clOrdIdOff = -1;
    clOrdIdLen = 0;
    origClOrdIdOff = -1;
    origClOrdIdLen = 0;
    quoteIdOff = -1;
    quoteIdLen = 0;
    tokenOff = -1;
    tokenLen = 0;
    accountOff = -1;
    accountLen = 0;
    traceparentOff = -1;
    traceparentLen = 0;

    qty = Long.MIN_VALUE;
    side = ABSENT;
    ordType = ABSENT;
    timeInForce = ABSENT;
  }

  /**
   * Returns a heap-allocated {@link String} view of the slice for the given offset/length. ONLY for
   * test assertion convenience — production hot path NEVER calls this.
   *
   * @param off slice offset (must be {@code >= 0})
   * @param len slice length (must be {@code >= 0})
   * @return decoded UTF-8 string
   * @throws IndexOutOfBoundsException if the slice falls outside {@link #scratch}
   */
  String sliceAsString(final int off, final int len) {
    if (off < 0) {
      return null;
    }
    if (off + len > scratch.length || len < 0) {
      throw new IndexOutOfBoundsException(
          "slice out of bounds: off=" + off + " len=" + len + " scratchLen=" + scratch.length);
    }
    return new String(scratch, off, len, StandardCharsets.UTF_8);
  }
}
