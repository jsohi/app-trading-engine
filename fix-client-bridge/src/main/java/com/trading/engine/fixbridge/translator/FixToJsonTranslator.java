package com.trading.engine.fixbridge.translator;

import com.trading.engine.fix.decoder.BusinessMessageRejectDecoder;
import com.trading.engine.fix.decoder.ExecutionReportDecoder;
import com.trading.engine.fix.decoder.OrderCancelRejectDecoder;
import com.trading.engine.fix.decoder.QuoteDecoder;
import com.trading.engine.fix.decoder.QuoteRequestRejectDecoder;
import com.trading.engine.fix.decoder.RejectDecoder;
import com.trading.engine.fixbridge.json.Utf8JsonStringEmitter;
import com.trading.engine.gateway.FixedPoint;
import io.netty.buffer.ByteBuf;
import java.nio.charset.StandardCharsets;
import org.agrona.concurrent.EpochNanoClock;
import org.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.artio.fields.DecimalFloat;
import uk.co.real_logic.artio.fields.UtcTimestampDecoder;

/**
 * Zero-allocation translator from inbound Artio FIX 4.4 decoders to outbound browser JSON events,
 * written directly into a Netty {@link ByteBuf}.
 *
 * <p><b>Purpose.</b> Bridge the FIX 4.4 wire-protocol (gateway-side) into the JSON wire-protocol
 * (browser-side). Each {@code translateXxx} method consumes a fully-populated Artio decoder and
 * emits a UTF-8 JSON object as raw bytes appended to {@code dst}. The translator does NOT
 * instantiate any {@code BrowserEvent} record; the records exist only as a test-facing
 * specification (locked: zero alloc on hot path).
 *
 * <p><b>Coverage (locked §17).</b> Six inbound FIX message types: {@code ExecutionReport (35=8)},
 * {@code Quote (35=S)}, {@code OrderCancelReject (35=9)}, {@code QuoteRequestReject (35=AG)},
 * {@code BusinessMessageReject (35=j)}, {@code Reject (35=3)}. Anything else the dispatcher logs
 * once per (msgType, session) and drops.
 *
 * <p><b>Quote expiry handling (locked §8).</b> {@link #translateQuote} reads FIX {@code
 * ValidUntilTime (62)}; if absent OR malformed OR earlier than {@code wallClock.nanoTime() +
 * EXPIRY_SAFETY_MARGIN_NS}, the translator falls back to {@code wallClock.nanoTime() +
 * DEFAULT_EXPIRY_NS} as the canonical {@code expiryNs} on the outbound JSON event. Tests exercise
 * all four paths.
 *
 * <p><b>Threading.</b> Not thread-safe. OWNED by exactly one Netty / Artio worker. Each instance
 * owns a per-call {@link UtcTimestampDecoder}, a {@link DecimalStringEmitter}, and a {@link
 * DecimalFloat} scratch — concurrent invocations corrupt state.
 *
 * <p><b>Allocation.</b> Zero allocation per {@code translateXxx} call after construction — verified
 * by {@code FixToJsonTranslatorAllocTest}. Header / key byte[] constants are interned once at
 * class-init.
 *
 * <p><b>Lifecycle.</b> Per-handler instance, allocated alongside {@link DecimalStringEmitter} at
 * Phase 6 wire-up.
 *
 * <p><b>Dependencies.</b> Artio FIX 4.4 codecs (decoders), {@link UtcTimestampDecoder}, {@link
 * DecimalFloat}, gateway {@link FixedPoint}, and {@link DecimalStringEmitter} from this package.
 *
 * <p><b>String escaping.</b> FIX values containing un-escaped {@code "}, {@code \\}, or any byte in
 * {@code 0x00..0x1F} are written verbatim by {@link #writeJsonStringChars(char[], int, int,
 * ByteBuf)} only after passing through a sanitisation guard that rejects them with {@link
 * IllegalStateException}. The wire-protocol contract upstream of this translator is that gateway
 * FIX values are 7-bit ASCII without embedded control bytes; a violation is a gateway bug worth
 * surfacing.
 */
public final class FixToJsonTranslator {

  // ---------------------------------------------------------------------------
  // Quote expiry constants (locked §8).
  // ---------------------------------------------------------------------------

  /**
   * Safety margin used when validating an inbound {@code ValidUntilTime}. A parsed expiry less than
   * {@code wallClock + 50 ms} is treated as effectively expired (browser cannot meaningfully accept
   * it given network round-trip) and replaced with the default fallback.
   */
  public static final long EXPIRY_SAFETY_MARGIN_NS = 50_000_000L;

  /** Default outbound quote TTL when {@code ValidUntilTime} is absent / malformed (5 seconds). */
  public static final long DEFAULT_EXPIRY_NS = 5_000_000_000L;

  // ---------------------------------------------------------------------------
  // Pre-computed JSON header / key byte[] constants.
  // ---------------------------------------------------------------------------

  private static final byte[] HDR_QUOTE = ascii("{\"type\":\"Quote\"");
  private static final byte[] HDR_EXEC = ascii("{\"type\":\"ExecutionReport\"");
  private static final byte[] HDR_ORDER_REJECT = ascii("{\"type\":\"OrderReject\"");
  private static final byte[] HDR_ERROR = ascii("{\"type\":\"Error\"");

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
  private static final byte[] K_RECEIVED = ascii(",\"received\":\"");

  private static final byte[] V_BUY = ascii("Buy");
  private static final byte[] V_SELL = ascii("Sell");

  // Reason prefixes for the fault taxonomy (locked §17).
  private static final byte[] PFX_CANCEL_REJECT = ascii("cancel-reject:");
  private static final byte[] PFX_QUOTE_REJECTED = ascii("quote-rejected:");
  private static final byte[] PFX_FIX_REJECT = ascii("fix-reject:");
  private static final byte[] PFX_RECEIVED_QR = ascii("QuoteRequest:");

  private static final byte CLOSE_QUOTE = (byte) '"';
  private static final byte CLOSE_BRACE = (byte) '}';

  // ---------------------------------------------------------------------------
  // Per-instance state.
  // ---------------------------------------------------------------------------

  /** Decimal emitter for fixed-point quantities / prices. */
  private final DecimalStringEmitter emitter;

  /** Reusable timestamp decoder for {@code ValidUntilTime} parsing. */
  private final UtcTimestampDecoder utcTs = new UtcTimestampDecoder(false);

  /** Reusable {@link DecimalFloat} scratch for FixedPoint round-tripping outbound prices. */
  private final DecimalFloat priceScratch = new DecimalFloat();

  /**
   * 32-byte scratch buffer for long ASCII rendering. {@code expiryNs} (a signed long) takes at most
   * 20 ASCII bytes; sized to 32 to match {@link DecimalStringEmitter#scratchCapacity()} so all
   * three on-bridge scratch sizing constants align (worst-case {@code "-" + 19 digits + "." + 8
   * frac digits = 29}; round up to 32 for cache-line alignment).
   */
  private static final int LONG_SCRATCH_CAPACITY = 32;

  private final byte[] longScratch = new byte[LONG_SCRATCH_CAPACITY];

  /**
   * Wraps {@link #longScratch} for Agrona's {@code putLongAscii}. Allocated once, reused on every
   * long-write.
   */
  private final UnsafeBuffer longView = new UnsafeBuffer(longScratch);

  /** Wall-clock for expiry fallbacks. */
  private final EpochNanoClock wallClock;

  /**
   * Constructs a translator using the supplied emitter and wall clock.
   *
   * @param emitter caller-owned per-handler emitter (non-null)
   * @param wallClock injected wall-clock source (non-null)
   * @throws IllegalArgumentException if any argument is null
   */
  public FixToJsonTranslator(final DecimalStringEmitter emitter, final EpochNanoClock wallClock) {
    if (emitter == null) {
      throw new IllegalArgumentException("emitter must not be null");
    }
    if (wallClock == null) {
      throw new IllegalArgumentException("wallClock must not be null");
    }
    this.emitter = emitter;
    this.wallClock = wallClock;
  }

  // ---------------------------------------------------------------------------
  // ExecutionReport (35=8)
  // ---------------------------------------------------------------------------

  /**
   * Translate a FIX {@code ExecutionReport} into a {@code BrowserEvent.ExecutionReport} JSON object
   * appended to {@code dst}.
   *
   * @param in fully-populated decoder
   * @param dst destination buffer; bytes are appended at {@code dst.writerIndex()}
   * @return number of bytes appended to {@code dst}
   */
  public int translateExecutionReport(final ExecutionReportDecoder in, final ByteBuf dst) {
    final int start = dst.writerIndex();
    try {
      dst.writeBytes(HDR_EXEC);

      dst.writeBytes(K_CL_ORD_ID);
      writeJsonStringChars(in.clOrdID(), 0, in.clOrdIDLength(), dst);
      dst.writeByte(CLOSE_QUOTE);

      dst.writeBytes(K_EXEC_ID);
      writeJsonStringChars(in.execID(), 0, in.execIDLength(), dst);
      dst.writeByte(CLOSE_QUOTE);

      dst.writeBytes(K_EXEC_TYPE);
      writeJsonChar(in.execType(), dst);
      dst.writeByte(CLOSE_QUOTE);

      dst.writeBytes(K_ORD_STATUS);
      writeJsonChar(in.ordStatus(), dst);
      dst.writeByte(CLOSE_QUOTE);

      dst.writeBytes(K_SYMBOL);
      writeJsonStringChars(in.symbol(), 0, in.symbolLength(), dst);
      dst.writeByte(CLOSE_QUOTE);

      dst.writeBytes(K_SIDE);
      writeSideBytes((byte) in.side(), dst);
      dst.writeByte(CLOSE_QUOTE);

      dst.writeBytes(K_CUM_QTY);
      emitDecimalFloatRoundTripped(in.cumQty(), dst);
      dst.writeByte(CLOSE_QUOTE);

      dst.writeBytes(K_LEAVES_QTY);
      emitDecimalFloatRoundTripped(in.leavesQty(), dst);
      dst.writeByte(CLOSE_QUOTE);

      dst.writeBytes(K_AVG_PX);
      emitDecimalFloatRoundTripped(in.avgPx(), dst);
      dst.writeByte(CLOSE_QUOTE);

      dst.writeByte(CLOSE_BRACE);
      return dst.writerIndex() - start;
    } catch (final RuntimeException ex) {
      // Roll back any partial write — see {@link #translateQuote} for rationale.
      dst.writerIndex(start);
      throw ex;
    }
  }

  // ---------------------------------------------------------------------------
  // Quote (35=S)
  // ---------------------------------------------------------------------------

  /**
   * Translate a FIX {@code Quote} into a {@code BrowserEvent.Quote} JSON object appended to {@code
   * dst}.
   *
   * <p>The outbound {@code expiryNs} is computed per locked §8: parse {@code ValidUntilTime (62)}
   * as nanoseconds; if absent / malformed / less than {@code wallClock + EXPIRY_SAFETY_ MARGIN_NS},
   * fall back to {@code wallClock + DEFAULT_EXPIRY_NS}.
   *
   * <p>Side selection (locked: Quote.side maps to the dealer-quoted side from the perspective of
   * the requester): {@code Side (54)} is REQUIRED on the inbound FIX Quote — if it is absent, the
   * translator throws {@link IllegalStateException} rather than silently fabricating a default
   * (locked review LOW-MEDIUM-2). The Phase 6 dispatcher catches the exception, logs at WARN, and
   * surfaces a fault-taxonomy {@code Error} event to the browser. The {@code price} field is the
   * OFFER for a Buy-side quote and the BID for a Sell-side quote, matching the convention that an
   * {@code AcceptQuote} pays the dealer's offer when buying and lifts the dealer's bid when
   * selling.
   *
   * <p>The dealer's {@code OrderQty} carries the quoted size; if absent we default to {@code 0L}.
   * Locked §9: prices flow through {@code FixedPoint.toInt64} → {@code FixedPoint .toDecimalFloat}
   * for byte-stable round-trip via the emitter.
   *
   * @param in fully-populated decoder
   * @param dst destination buffer
   * @return number of bytes appended to {@code dst}
   */
  public int translateQuote(final QuoteDecoder in, final ByteBuf dst) {
    final int start = dst.writerIndex();
    try {
      dst.writeBytes(HDR_QUOTE);

      // reqId — echo of QuoteReqID.
      dst.writeBytes(K_REQ_ID);
      writeJsonStringChars(in.quoteReqID(), 0, in.quoteReqIDLength(), dst);
      dst.writeByte(CLOSE_QUOTE);

      // quoteId.
      dst.writeBytes(K_QUOTE_ID);
      writeJsonStringChars(in.quoteID(), 0, in.quoteIDLength(), dst);
      dst.writeByte(CLOSE_QUOTE);

      // symbol.
      dst.writeBytes(K_SYMBOL);
      writeJsonStringChars(in.symbol(), 0, in.symbolLength(), dst);
      dst.writeByte(CLOSE_QUOTE);

      // side: REQUIRED. Per locked review (MEDIUM-2) we do NOT fabricate a default — a Quote
      // with no Side from the gateway is a wire-protocol bug worth surfacing to the dispatcher.
      if (!in.hasSide()) {
        throw new IllegalStateException("FIX Quote missing required Side field");
      }
      dst.writeBytes(K_SIDE);
      final char sideChar = in.side();
      writeSideBytes((byte) sideChar, dst);
      dst.writeByte(CLOSE_QUOTE);

      // qty: emit OrderQty if present, else "0.00000000".
      dst.writeBytes(K_QTY);
      if (in.hasOrderQty()) {
        emitDecimalFloatRoundTripped(in.orderQty(), dst);
      } else {
        emitter.emitInt64FixedPoint(0L, dst);
      }
      dst.writeByte(CLOSE_QUOTE);

      // price: pick OfferPx for Buy, BidPx for Sell. Fall through to whichever side is
      // populated when the requested-side counterpart is absent.
      dst.writeBytes(K_PRICE);
      final boolean wantOffer = sideChar == '1';
      final var px = pickPrice(in, wantOffer);
      if (px == null) {
        emitter.emitInt64FixedPoint(0L, dst);
      } else {
        emitDecimalFloatRoundTripped(px, dst);
      }
      dst.writeByte(CLOSE_QUOTE);

      // expiryNs: per locked §8 fallback rules.
      dst.writeBytes(K_EXPIRY);
      final long expiryNs = computeExpiryNs(in);
      writeLong(expiryNs, dst);

      dst.writeByte(CLOSE_BRACE);
      return dst.writerIndex() - start;
    } catch (final RuntimeException ex) {
      // All-or-nothing: any mid-write rejection (forbidden chars, missing Side, etc.) leaves
      // the buffer untouched so the dispatcher does not emit a half-formed object on the wire.
      dst.writerIndex(start);
      throw ex;
    }
  }

  // ---------------------------------------------------------------------------
  // OrderCancelReject (35=9)
  // ---------------------------------------------------------------------------

  /**
   * Translate a FIX {@code OrderCancelReject} into a {@code BrowserEvent.OrderReject} JSON object
   * with {@code reason="cancel-reject:<text>"} per locked §17. {@code clOrdId} is sourced from the
   * incoming {@code ClOrdID (11)} (the rejected cancel-request's id).
   *
   * @param in fully-populated decoder
   * @param dst destination buffer
   * @return number of bytes appended to {@code dst}
   */
  public int translateOrderCancelReject(final OrderCancelRejectDecoder in, final ByteBuf dst) {
    final int start = dst.writerIndex();
    try {
      dst.writeBytes(HDR_ORDER_REJECT);

      dst.writeBytes(K_CL_ORD_ID);
      writeJsonStringChars(in.clOrdID(), 0, in.clOrdIDLength(), dst);
      dst.writeByte(CLOSE_QUOTE);

      dst.writeBytes(K_REASON);
      dst.writeBytes(PFX_CANCEL_REJECT);
      if (in.hasText()) {
        writeJsonStringChars(in.text(), 0, in.textLength(), dst);
      }
      dst.writeByte(CLOSE_QUOTE);

      dst.writeByte(CLOSE_BRACE);
      return dst.writerIndex() - start;
    } catch (final RuntimeException ex) {
      dst.writerIndex(start);
      throw ex;
    }
  }

  // ---------------------------------------------------------------------------
  // QuoteRequestReject (35=AG)
  // ---------------------------------------------------------------------------

  /**
   * Translate a FIX {@code QuoteRequestReject} into a {@code BrowserEvent.Error} JSON object with
   * {@code reason="quote-rejected:<text>"} and {@code received="QuoteRequest:<reqId>"} per locked
   * §17.
   *
   * @param in fully-populated decoder
   * @param dst destination buffer
   * @return number of bytes appended to {@code dst}
   */
  public int translateQuoteRequestReject(final QuoteRequestRejectDecoder in, final ByteBuf dst) {
    final int start = dst.writerIndex();
    try {
      dst.writeBytes(HDR_ERROR);

      dst.writeBytes(K_REASON);
      dst.writeBytes(PFX_QUOTE_REJECTED);
      if (in.hasText()) {
        writeJsonStringChars(in.text(), 0, in.textLength(), dst);
      }
      dst.writeByte(CLOSE_QUOTE);

      dst.writeBytes(K_RECEIVED);
      dst.writeBytes(PFX_RECEIVED_QR);
      writeJsonStringChars(in.quoteReqID(), 0, in.quoteReqIDLength(), dst);
      dst.writeByte(CLOSE_QUOTE);

      dst.writeByte(CLOSE_BRACE);
      return dst.writerIndex() - start;
    } catch (final RuntimeException ex) {
      dst.writerIndex(start);
      throw ex;
    }
  }

  // ---------------------------------------------------------------------------
  // BusinessMessageReject (35=j)
  // ---------------------------------------------------------------------------

  /**
   * Translate a FIX {@code BusinessMessageReject} into a {@code BrowserEvent.Error} JSON object
   * with {@code reason="fix-reject:<text>"}.
   *
   * @param in fully-populated decoder
   * @param dst destination buffer
   * @return number of bytes appended to {@code dst}
   */
  public int translateBusinessMessageReject(
      final BusinessMessageRejectDecoder in, final ByteBuf dst) {
    final int start = dst.writerIndex();
    try {
      dst.writeBytes(HDR_ERROR);

      dst.writeBytes(K_REASON);
      dst.writeBytes(PFX_FIX_REJECT);
      if (in.hasText()) {
        writeJsonStringChars(in.text(), 0, in.textLength(), dst);
      }
      dst.writeByte(CLOSE_QUOTE);

      dst.writeByte(CLOSE_BRACE);
      return dst.writerIndex() - start;
    } catch (final RuntimeException ex) {
      dst.writerIndex(start);
      throw ex;
    }
  }

  // ---------------------------------------------------------------------------
  // Reject (35=3)
  // ---------------------------------------------------------------------------

  /**
   * Translate a FIX session-level {@code Reject} into a {@code BrowserEvent.Error} JSON object with
   * {@code reason="fix-reject:<text>"}.
   *
   * @param in fully-populated decoder
   * @param dst destination buffer
   * @return number of bytes appended to {@code dst}
   */
  public int translateReject(final RejectDecoder in, final ByteBuf dst) {
    final int start = dst.writerIndex();
    try {
      dst.writeBytes(HDR_ERROR);

      dst.writeBytes(K_REASON);
      dst.writeBytes(PFX_FIX_REJECT);
      if (in.hasText()) {
        writeJsonStringChars(in.text(), 0, in.textLength(), dst);
      }
      dst.writeByte(CLOSE_QUOTE);

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
   * Picks the price flyweight matching the quoted side. {@code wantOffer=true} (Buy-side) prefers
   * {@code OfferPx (133)}; {@code wantOffer=false} prefers {@code BidPx (132)}. If the preferred
   * side is absent, falls back to whichever side is populated; if neither, returns null.
   */
  private static DecimalFloat pickPrice(final QuoteDecoder in, final boolean wantOffer) {
    if (wantOffer) {
      if (in.hasOfferPx()) {
        return in.offerPx();
      }
      if (in.hasBidPx()) {
        return in.bidPx();
      }
      return null;
    }
    if (in.hasBidPx()) {
      return in.bidPx();
    }
    if (in.hasOfferPx()) {
      return in.offerPx();
    }
    return null;
  }

  /**
   * Compute the outbound {@code expiryNs} per locked §8.
   *
   * <p>Reads {@code ValidUntilTime (62)}; if present and parseable, returns it as nanoseconds.
   * Falls back to {@code wallClock.nanoTime() + DEFAULT_EXPIRY_NS} if the field is absent,
   * malformed, or earlier than {@code wallClock.nanoTime() + EXPIRY_SAFETY_MARGIN_NS}.
   *
   * @param in fully-populated quote decoder; the {@code ValidUntilTime (62)} field is consulted via
   *     {@link QuoteDecoder#hasValidUntilTime()} / {@link QuoteDecoder#validUntilTime()}
   * @return the outbound JSON {@code expiryNs} value (epoch nanoseconds)
   */
  long computeExpiryNs(final QuoteDecoder in) {
    final long now = wallClock.nanoTime();
    if (!in.hasValidUntilTime()) {
      return now + DEFAULT_EXPIRY_NS;
    }
    final var vt = in.validUntilTime();
    final int vtLen = in.validUntilTimeLength();
    final long parsed;
    try {
      // UtcTimestampDecoder caps precision at the supplied resolution; even though FIX 4.4
      // ValidUntilTime is millisecond, decodeNanos returns nanoseconds (zero-padded the
      // sub-millisecond digits).
      parsed = utcTs.decodeNanos(vt, vtLen);
    } catch (final IllegalArgumentException ex) {
      // Narrow catch: Artio's UtcTimestampDecoder throws IllegalArgumentException (out-of-range
      // field) or NumberFormatException (non-digit char) on malformed input — both are caught
      // here since NumberFormatException extends IllegalArgumentException. Other RuntimeExceptions
      // would indicate a programming error and should propagate.
      //
      // The exception instance is intentionally dropped: logging is the caller's concern (the
      // dispatcher layer wires Log4j2 and will emit a WARN at the next layer; the translator
      // stays logging-agnostic to keep the alloc footprint flat).
      // TODO(APP-39 Phase 6): emit metric for malformed ValidUntilTime so operators can detect
      //   gateway misconfiguration without rummaging through logs.
      return now + DEFAULT_EXPIRY_NS;
    }
    if (parsed < now + EXPIRY_SAFETY_MARGIN_NS) {
      return now + DEFAULT_EXPIRY_NS;
    }
    return parsed;
  }

  /**
   * Round-trip {@code df} through {@code FixedPoint.toInt64} → {@code FixedPoint.toDecimalFloat} to
   * lock the wire-form scale to {@link FixedPoint#FIXED_POINT_SCALE} before emitting via {@link
   * DecimalStringEmitter}.
   *
   * <p>This single call site enforces the locked §9 invariant that every outbound price / quantity
   * flows through the FixedPoint round-trip. Without the round-trip, a {@link DecimalFloat}
   * carrying a non-canonical scale would emit a string with a different number of fractional digits
   * than the JSON wire-protocol contract requires.
   */
  private void emitDecimalFloatRoundTripped(final DecimalFloat df, final ByteBuf dst) {
    if (df == null) {
      emitter.emitInt64FixedPoint(0L, dst);
      return;
    }
    final long int64 = FixedPoint.toInt64(df);
    FixedPoint.toDecimalFloat(int64, priceScratch);
    emitter.emitDecimalFloat(priceScratch, dst);
  }

  /**
   * Write a single FIX {@code char} field as one ASCII byte. Strict guard: rejects any byte that
   * would require JSON escaping (the wire-protocol upstream of this translator is 7-bit printable
   * ASCII).
   */
  private static void writeJsonChar(final char c, final ByteBuf dst) {
    if (c == '"' || c == '\\' || c < 0x20 || c >= 0x7F) {
      throw new IllegalStateException("non-printable FIX char from gateway: " + (int) c);
    }
    dst.writeByte((byte) c);
  }

  /**
   * Write the canonical "Buy" / "Sell" mapping of a FIX {@code Side (54)} byte. {@code '1'} →
   * {@code Buy}, {@code '2'} → {@code Sell}; any other value is a wire-protocol upstream bug.
   */
  private static void writeSideBytes(final byte side, final ByteBuf dst) {
    if (side == '1') {
      dst.writeBytes(V_BUY);
      return;
    }
    if (side == '2') {
      dst.writeBytes(V_SELL);
      return;
    }
    throw new IllegalStateException("unsupported FIX Side byte: " + (int) side);
  }

  /**
   * Write {@code chars[off..off+len)} into {@code dst} as UTF-8 / ASCII, validating each character
   * is JSON-safe (no embedded {@code "}, {@code \\}, or control bytes). Zero allocation: writes one
   * byte at a time directly into {@code dst}.
   *
   * <p>Delegates to {@link Utf8JsonStringEmitter#appendCharSlice(char[], int, int, ByteBuf)} —
   * single implementation shared with {@code BrowserEventWriter}.
   */
  private static void writeJsonStringChars(
      final char[] chars, final int off, final int len, final ByteBuf dst) {
    Utf8JsonStringEmitter.appendCharSlice(chars, off, len, dst);
  }

  /**
   * Write {@code value} as a base-10 ASCII integer into {@code dst} via Agrona's {@code
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
