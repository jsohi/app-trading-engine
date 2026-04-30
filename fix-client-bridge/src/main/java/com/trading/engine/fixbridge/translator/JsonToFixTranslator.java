package com.trading.engine.fixbridge.translator;

import com.trading.engine.fix.builder.NewOrderSingleEncoder;
import com.trading.engine.fix.builder.OrderCancelRequestEncoder;
import com.trading.engine.fix.builder.QuoteRequestEncoder;
import com.trading.engine.fixbridge.json.JsonParseException;
import com.trading.engine.fixbridge.json.MutableParsedMessage;
import com.trading.engine.gateway.FixedPoint;
import java.util.concurrent.TimeUnit;
import org.agrona.concurrent.EpochNanoClock;
import uk.co.real_logic.artio.fields.DecimalFloat;
import uk.co.real_logic.artio.fields.UtcTimestampEncoder;

/**
 * Zero-allocation translator from inbound browser JSON ({@link MutableParsedMessage}) to Artio FIX
 * 4.4 encoders. The dispatcher (Phase 6) pre-populates the parsed message via {@code
 * BrowserMessageReader.parse}, calls one of the {@code translate*} methods here to populate the
 * encoder fields, and then hands the encoder to {@code Session.trySend(...)} which serialises and
 * dispatches the FIX bytes.
 *
 * <p><b>Purpose.</b> Bridge the JSON wire-protocol (browser-facing) to the FIX 4.4 wire-protocol
 * (gateway-facing) without a single allocation per message. All encoder field setters used here are
 * zero-allocation: they wrap (do NOT copy) the parsed-message scratch byte array, and the
 * fixed-point round-trip flows through {@link FixedPoint#toDecimalFloat} into a per-instance {@link
 * DecimalFloat} flyweight. ClOrdIDs are minted into a per-instance 20-byte scratch using Agrona /
 * java.lang primitives only.
 *
 * <p><b>Threading.</b> Not thread-safe. OWNED by exactly one Netty worker; one translator instance
 * per worker is the design intent (Phase 6 will allocate). Concurrent invocations on the same
 * instance will corrupt the shared {@link #clOrdIdScratch} buffer and the encoder flyweights.
 *
 * <p><b>Allocation.</b> Zero on every {@code translate*} method after construction. The encoders
 * passed in are caller-owned and reused; the translator only mutates fields. {@link
 * #parseDecimalToDecimalFloat} is verified zero-alloc by {@code JsonToFixTranslatorAllocTest}.
 *
 * <p><b>Lifecycle.</b> Per-handler instance, allocated alongside the inbound flyweight at handler
 * registration in Phase 6.
 *
 * <p><b>Dependencies.</b> Artio FIX 4.4 codecs (from {@code :fix-codecs}), Artio's {@link
 * UtcTimestampEncoder} / {@link DecimalFloat}, the gateway's {@link FixedPoint} round-trip helper,
 * and {@link MutableParsedMessage} from this module.
 *
 * <p><b>ClOrdID format (locked §4).</b> 20 bytes exactly: {@code
 * <6-hex-instanceTag>-<7-hex-sessionId>-<5-digit-counter>}. The instanceTag identifies the bridge
 * process at boot (clock-derived); the sessionId identifies the browser session within that
 * process; the counter is monotonic per session. The format is restart-stable so an inbound FIX
 * {@code ExecutionReport} can be routed back to its originating browser session by parsing the
 * ClOrdID with {@link Long#parseLong(CharSequence, int, int, int)} on the three slices.
 *
 * <p><b>AcceptQuote two-phase commit (locked §2).</b> {@link #translateAcceptQuote} reads from a
 * caller-supplied {@link QuoteSnapshot} but does NOT evict the snapshot — it returns a token
 * (currently the unmodified {@code quoteCacheToken} the caller passed in) that the dispatcher uses
 * to evict the cache slot only AFTER {@code Session.trySend} returns success. {@link
 * #handleRejectQuote} returns the sentinel {@link #NO_FIX_BYTES} so the dispatcher can skip {@code
 * trySend} entirely.
 *
 * <p><b>Browser-supplied ClOrdID.</b> When a browser supplies its own {@code clOrdId}, the
 * translator passes it through verbatim (after length validation ≤ 20 bytes per FIX 4.4 spec). When
 * the browser omits the field the translator mints a fresh one using {@link #mintClOrdId(long,
 * long, long, byte[], int)}.
 */
public final class JsonToFixTranslator {

  /**
   * Sentinel returned by {@link #handleRejectQuote} (and {@code translate*} no-op paths) to signal
   * the dispatcher that no FIX message should be sent. Negative-valued so it is unambiguous
   * relative to legitimate FIX byte counts which are always {@code > 0}.
   */
  public static final int NO_FIX_BYTES = -1;

  /** Locked §4: ClOrdID is exactly 20 bytes. */
  public static final int CLORDID_LENGTH = 20;

  /** Lower bound for a parseable browser-supplied ClOrdID — empty string is forbidden. */
  public static final int CLORDID_MIN_LENGTH = 1;

  /** ASCII hex digit lookup. Read-only — never mutated post class-init. */
  private static final byte[] HEX = {
    '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'
  };

  // ---------------------------------------------------------------------------
  // Per-instance scratch buffers and reusable Artio flyweights. None of these
  // are thread-safe; the translator is single-threaded by construction.
  // ---------------------------------------------------------------------------

  /**
   * 20-byte scratch for minted ClOrdIDs. The encoder's {@code clOrdID(byte[], int, int)} setter
   * wraps (does NOT copy) this reference; the buffer outlives every {@code Session.trySend} call
   * because the translator is single-threaded — by the time the next translate call mutates this
   * buffer, the previous {@code trySend} has already serialised and discarded it.
   */
  private final byte[] clOrdIdScratch = new byte[CLORDID_LENGTH];

  /** Reusable price flyweight; populated via {@link FixedPoint#toDecimalFloat}. */
  private final DecimalFloat priceDf = new DecimalFloat();

  /** Reusable quantity flyweight. */
  private final DecimalFloat qtyDf = new DecimalFloat();

  /** Artio UTC-timestamp encoder for FIX {@code TransactTime (60)} fields. */
  private final UtcTimestampEncoder utcTs = new UtcTimestampEncoder();

  /** Wall-clock source. Injected so tests can drive deterministic timestamps. */
  private final EpochNanoClock wallClock;

  /**
   * Constructs a translator using the supplied wall clock for FIX {@code TransactTime (60)}
   * stamping.
   *
   * @param wallClock injected clock; never {@code null}
   * @throws IllegalArgumentException if {@code wallClock} is null
   */
  public JsonToFixTranslator(final EpochNanoClock wallClock) {
    if (wallClock == null) {
      throw new IllegalArgumentException("wallClock must not be null");
    }
    this.wallClock = wallClock;
  }

  // ---------------------------------------------------------------------------
  // Public translation API.
  // ---------------------------------------------------------------------------

  /**
   * Translate {@code in} (already parsed as {@code TYPE_NEW_ORDER_SINGLE}) into the supplied {@link
   * NewOrderSingleEncoder}, populating every required FIX 4.4 field. The browser MAY supply its own
   * {@code clOrdId} via the JSON; if absent or empty, the translator mints a fresh one using {@code
   * (instanceTag, sessionId, counter)}.
   *
   * @param in parsed inbound JSON; must be type {@link MutableParsedMessage#TYPE_NEW_ORDER_SINGLE}
   * @param out caller-owned encoder; reset and field-populated by this method
   * @param sessionId browser session identifier (locked §4 ClOrdID format)
   * @param instanceTag bridge process instanceTag (locked §4)
   * @param counter pre-incremented per-session ClOrdID counter (locked §4)
   * @return number of bytes that will be on the FIX wire — sentinel value {@code 0} (positive) to
   *     signal the dispatcher should call {@code Session.trySend(out)}; the actual byte count is
   *     known only after {@code trySend}
   * @throws IllegalArgumentException if {@code in.type} is not {@link
   *     MutableParsedMessage#TYPE_NEW_ORDER_SINGLE}
   * @throws JsonParseException {@link JsonParseException#PRICE_PRECISION} if a decimal field has
   *     more than 8 fractional digits
   */
  public int translateNewOrderSingle(
      final MutableParsedMessage in,
      final NewOrderSingleEncoder out,
      final long sessionId,
      final long instanceTag,
      final long counter) {
    if (in.type != MutableParsedMessage.TYPE_NEW_ORDER_SINGLE) {
      throw new IllegalArgumentException("expected TYPE_NEW_ORDER_SINGLE but got type=" + in.type);
    }

    out.reset();

    // ClOrdID — browser-supplied passthrough, otherwise minted into clOrdIdScratch.
    setNewOrderClOrdId(in, out, sessionId, instanceTag, counter);

    // Account (1) — optional in FIX 4.4 but always supplied by the browser per wire protocol.
    if (in.accountOff >= 0 && in.accountLen > 0) {
      out.account(in.scratch, in.accountOff, in.accountLen);
    }

    // HandlInst (21) — required on FIX 4.4 NewOrderSingle. Browser does not supply this; we
    // pin to '1' (Automated execution, no broker intervention) which is the only value
    // appropriate for an electronic dealing channel.
    out.handlInst('1');

    // Symbol (55) on the embedded Instrument component. Wraps scratch — zero alloc.
    if (in.symbolOff >= 0 && in.symbolLen > 0) {
      out.instrument().symbol(in.scratch, in.symbolOff, in.symbolLen);
    }

    // Side (54).
    if (in.side != MutableParsedMessage.ABSENT) {
      out.side((char) in.side);
    }

    // TransactTime (60) — wall-clock UTC, microsecond precision (Artio default formatting).
    final long nowNs = wallClock.nanoTime();
    final int tsLen = utcTs.encodeFrom(nowNs, TimeUnit.NANOSECONDS);
    out.transactTime(utcTs.buffer(), 0, tsLen);

    // OrdType (40).
    if (in.ordType != MutableParsedMessage.ABSENT) {
      out.ordType((char) in.ordType);
    }

    // OrderQty (38) on OrderQtyData component. qty is pre-decoded to int64 by the parser; round
    // trip through FixedPoint.toDecimalFloat for a stable (value, scale) on the FIX wire.
    if (in.qty != Long.MIN_VALUE) {
      FixedPoint.toDecimalFloat(in.qty, qtyDf);
      out.orderQtyData().orderQty(qtyDf);
    }

    // Price (44) — only meaningful for Limit orders. The parser leaves the int64 value
    // un-decoded; we re-parse the slice into priceDf to preserve full precision through
    // FixedPoint round-trip (locked §9).
    if (in.priceOff >= 0 && in.priceLen > 0) {
      parseDecimalToDecimalFloat(in.scratch, in.priceOff, in.priceLen, priceDf);
      // Funnel through FixedPoint to lock the wire-form scale to PRICE_SCALE.
      final long priceInt64 = priceDfToInt64(priceDf);
      FixedPoint.toDecimalFloat(priceInt64, priceDf);
      out.price(priceDf);
    }

    // TimeInForce (59).
    if (in.timeInForce != MutableParsedMessage.ABSENT) {
      out.timeInForce((char) in.timeInForce);
    }

    // Encoder is fully populated; dispatcher invokes Session.trySend(out) next.
    return 0;
  }

  /**
   * Translate {@code in} (parsed as {@code TYPE_ACCEPT_QUOTE}) into a FIX {@code NewOrderSingle
   * (35=D)} with {@code OrdType=D (Previously Quoted)} and {@code QuoteID (117) = quoteId}, with
   * symbol/side/qty/price sourced from the per-session quote cache via {@code snapshot}.
   *
   * <p>Implements locked §2 (two-phase commit): the snapshot is read but NOT evicted. The caller
   * MUST evict the snapshot only after {@code Session.trySend(out)} returns {@code >= 0}. The
   * {@code quoteCacheToken} is round-tripped to the caller via the return value so a single
   * dispatcher call site can scope the eviction.
   *
   * @param in parsed inbound JSON; must be type {@link MutableParsedMessage#TYPE_ACCEPT_QUOTE}
   * @param out caller-owned encoder; reset and field-populated by this method
   * @param snapshot quote-cache entry referenced by {@code in.quoteId}; MUST already be bound
   * @param sessionId browser session identifier (locked §4)
   * @param instanceTag bridge process instanceTag (locked §4)
   * @param counter pre-incremented per-session ClOrdID counter (locked §4)
   * @param quoteCacheToken caller-supplied opaque eviction handle (round-tripped on return)
   * @return {@code quoteCacheToken} on success — the caller uses it to evict the snapshot only if
   *     {@code Session.trySend(out) >= 0}
   * @throws IllegalArgumentException if {@code in.type} is not {@code TYPE_ACCEPT_QUOTE}, if {@code
   *     snapshot} is null, or if {@code snapshot} is not bound
   */
  public long translateAcceptQuote(
      final MutableParsedMessage in,
      final NewOrderSingleEncoder out,
      final QuoteSnapshot snapshot,
      final long sessionId,
      final long instanceTag,
      final long counter,
      final long quoteCacheToken) {
    if (in.type != MutableParsedMessage.TYPE_ACCEPT_QUOTE) {
      throw new IllegalArgumentException("expected TYPE_ACCEPT_QUOTE but got type=" + in.type);
    }
    if (snapshot == null) {
      throw new IllegalArgumentException("snapshot must not be null");
    }
    if (!snapshot.isBound()) {
      throw new IllegalArgumentException("snapshot is not bound (cache miss path)");
    }

    out.reset();

    // ClOrdID — browser-supplied passthrough, otherwise minted into clOrdIdScratch.
    setNewOrderClOrdId(in, out, sessionId, instanceTag, counter);

    // QuoteID (117) — sliced from the inbound JSON.
    if (in.quoteIdOff >= 0 && in.quoteIdLen > 0) {
      out.quoteID(in.scratch, in.quoteIdOff, in.quoteIdLen);
    }

    // HandlInst (21) — required field; '1' = Automated execution (no broker intervention).
    out.handlInst('1');

    // Symbol from cache.
    out.instrument().symbol(snapshot.symbolBytes, 0, snapshot.symbolLen());

    // Side from cache.
    out.side((char) snapshot.side());

    // TransactTime (60) — wall-clock now.
    final long nowNs = wallClock.nanoTime();
    final int tsLen = utcTs.encodeFrom(nowNs, TimeUnit.NANOSECONDS);
    out.transactTime(utcTs.buffer(), 0, tsLen);

    // OrdType (40) = 'D' (Previously Quoted) — locked §2.
    out.ordType('D');

    // OrderQty (38) from cache.
    FixedPoint.toDecimalFloat(snapshot.qtyInt64(), qtyDf);
    out.orderQtyData().orderQty(qtyDf);

    // Price (44) from cache. Quote semantics: a Buy AcceptQuote takes the dealer's offer (ask);
    // a Sell AcceptQuote takes the dealer's bid. Locked §9: prices flow through FixedPoint
    // round-trip — go via int64 to lock the wire scale.
    final var takenPrice =
        (snapshot.side() == MutableParsedMessage.SIDE_BUY) ? snapshot.ask : snapshot.bid;
    final long priceInt64 = priceDfToInt64(takenPrice);
    FixedPoint.toDecimalFloat(priceInt64, priceDf);
    out.price(priceDf);

    // TimeInForce (59) — for Previously-Quoted orders the Artio reference dictionary
    // recommends IOC ('3') so the order does not rest. Browser cannot override.
    out.timeInForce('3');

    return quoteCacheToken;
  }

  /**
   * Translate {@code in} (parsed as {@code TYPE_CANCEL_ORDER}) into the supplied {@link
   * OrderCancelRequestEncoder}.
   *
   * @param in parsed inbound JSON; must be type {@link MutableParsedMessage#TYPE_CANCEL_ORDER}
   * @param out caller-owned encoder
   * @param sessionId browser session identifier (locked §4)
   * @param instanceTag bridge process instanceTag (locked §4)
   * @param counter pre-incremented per-session ClOrdID counter (locked §4)
   * @return {@code 0} on success (caller must invoke {@code Session.trySend(out)})
   * @throws IllegalArgumentException if {@code in.type} is not {@code TYPE_CANCEL_ORDER}
   */
  public int translateCancelOrder(
      final MutableParsedMessage in,
      final OrderCancelRequestEncoder out,
      final long sessionId,
      final long instanceTag,
      final long counter) {
    if (in.type != MutableParsedMessage.TYPE_CANCEL_ORDER) {
      throw new IllegalArgumentException("expected TYPE_CANCEL_ORDER but got type=" + in.type);
    }

    out.reset();

    // OrigClOrdID (41) — the ClOrdID of the order being cancelled.
    if (in.origClOrdIdOff >= 0 && in.origClOrdIdLen > 0) {
      out.origClOrdID(in.scratch, in.origClOrdIdOff, in.origClOrdIdLen);
    }

    // ClOrdID (11) — NEW id for the cancel request itself. Browser-supplied or minted.
    setCancelClOrdId(in, out, sessionId, instanceTag, counter);

    // Symbol (55) — Artio requires it on cancel.
    if (in.symbolOff >= 0 && in.symbolLen > 0) {
      out.instrument().symbol(in.scratch, in.symbolOff, in.symbolLen);
    }

    // Side (54).
    if (in.side != MutableParsedMessage.ABSENT) {
      out.side((char) in.side);
    }

    // TransactTime (60).
    final long nowNs = wallClock.nanoTime();
    final int tsLen = utcTs.encodeFrom(nowNs, TimeUnit.NANOSECONDS);
    out.transactTime(utcTs.buffer(), 0, tsLen);

    return 0;
  }

  /**
   * Translate {@code in} (parsed as {@code TYPE_QUOTE_REQUEST}) into the supplied {@link
   * QuoteRequestEncoder}. FIX 4.4 {@code QuoteRequest (35=R)} carries one {@code RelatedSym} group
   * entry per requested symbol; the browser wire protocol is a single (symbol, side, qty) tuple so
   * we always emit exactly one group entry.
   *
   * @param in parsed inbound JSON
   * @param out caller-owned encoder
   * @param sessionId browser session identifier (unused for QuoteRequest — kept for API symmetry)
   * @param instanceTag bridge process instanceTag (unused for QuoteRequest — kept for symmetry)
   * @param counter unused
   * @return {@code 0} on success
   * @throws IllegalArgumentException if {@code in.type} is not {@code TYPE_QUOTE_REQUEST}
   */
  public int translateQuoteRequest(
      final MutableParsedMessage in,
      final QuoteRequestEncoder out,
      final long sessionId,
      final long instanceTag,
      final long counter) {
    if (in.type != MutableParsedMessage.TYPE_QUOTE_REQUEST) {
      throw new IllegalArgumentException("expected TYPE_QUOTE_REQUEST but got type=" + in.type);
    }

    out.reset();

    // QuoteReqID (131) — required. The browser supplies this as `reqId`.
    if (in.reqIdOff >= 0 && in.reqIdLen > 0) {
      out.quoteReqID(in.scratch, in.reqIdOff, in.reqIdLen);
    }

    // RelatedSym group: one entry. The first call lazily allocates the inner encoder
    // (one-time construction cost paid at translator warm-up).
    final var leg = out.relatedSymGroup(1);

    // Symbol (55) on the embedded Instrument.
    if (in.symbolOff >= 0 && in.symbolLen > 0) {
      leg.instrument().symbol(in.scratch, in.symbolOff, in.symbolLen);
    }

    // Side (54).
    if (in.side != MutableParsedMessage.ABSENT) {
      leg.side((char) in.side);
    }

    // OrderQty (38) on OrderQtyData.
    if (in.qty != Long.MIN_VALUE) {
      FixedPoint.toDecimalFloat(in.qty, qtyDf);
      leg.orderQtyData().orderQty(qtyDf);
    }

    // TransactTime (60) on the leg — required for FIX QuoteRequest semantics.
    final long nowNs = wallClock.nanoTime();
    final int tsLen = utcTs.encodeFrom(nowNs, TimeUnit.NANOSECONDS);
    leg.transactTime(utcTs.buffer(), 0, tsLen);

    return 0;
  }

  /**
   * Handle a {@code RejectQuote} inbound message — locked §11: emit no FIX, the dispatcher skips
   * {@code Session.trySend} entirely.
   *
   * <p>This method does NOT touch the quote cache. The CALLER (Phase 5/6 dispatcher) MUST evict the
   * cache slot for {@code in.quoteId} after this method returns; the translator has no reference to
   * the per-session cache by design (single-responsibility — translation only).
   *
   * @param in parsed inbound JSON
   * @return the sentinel {@link #NO_FIX_BYTES} so the dispatcher knows to skip the wire send
   * @throws IllegalArgumentException if {@code in.type} is not {@code TYPE_REJECT_QUOTE}
   */
  public int handleRejectQuote(final MutableParsedMessage in) {
    if (in.type != MutableParsedMessage.TYPE_REJECT_QUOTE) {
      throw new IllegalArgumentException("expected TYPE_REJECT_QUOTE but got type=" + in.type);
    }
    return NO_FIX_BYTES;
  }

  // ---------------------------------------------------------------------------
  // Public helpers — used by the dispatcher and exercised by alloc tests.
  // ---------------------------------------------------------------------------

  /**
   * Mint a 20-byte ClOrdID into {@code dst} per the locked §4 format: {@code
   * <6-hex-instanceTag>-<7-hex-sessionId>-<5-digit-counter>}.
   *
   * <p>The caller-supplied {@code dst} is overwritten in place; no allocation occurs. {@code dst}
   * MUST be at least {@link #CLORDID_LENGTH} bytes from offset {@code 0}.
   *
   * @param instanceTag bridge process tag; {@code instanceTag & 0xFFFFFF} is the 24-bit value that
   *     occupies the first 6 hex digits
   * @param sessionId browser session id; {@code sessionId & 0xFFFFFFF} is the 28-bit value that
   *     occupies the next 7 hex digits
   * @param counter monotonic per-session counter; {@code counter % 100_000} is the 5-digit decimal
   *     that occupies the last 5 chars
   * @param dst destination scratch (size ≥ 20 bytes)
   * @param dstOffset offset in {@code dst} at which to write
   * @throws IllegalArgumentException if {@code dst} is too small from {@code dstOffset}
   */
  public static void mintClOrdId(
      final long instanceTag,
      final long sessionId,
      final long counter,
      final byte[] dst,
      final int dstOffset) {
    if (dstOffset < 0) {
      throw new IllegalArgumentException("dstOffset must be >= 0: " + dstOffset);
    }
    if (counter < 0L) {
      // The locked §4 spec mandates a monotonic non-negative per-session counter; a negative value
      // would silently produce a malformed ClOrdID via two's-complement modulo. Surface the
      // contract violation as a fail-fast IllegalArgumentException.
      throw new IllegalArgumentException("counter must be non-negative: " + counter);
    }
    if (dst.length - dstOffset < CLORDID_LENGTH) {
      throw new IllegalArgumentException(
          "dst too small: need "
              + CLORDID_LENGTH
              + " bytes from offset "
              + dstOffset
              + " but only "
              + (dst.length - dstOffset)
              + " available");
    }
    // 6 hex digits for instanceTag (24 bits).
    writeHexLowercase(instanceTag, 6, dst, dstOffset);
    dst[dstOffset + 6] = '-';
    // 7 hex digits for sessionId (28 bits).
    writeHexLowercase(sessionId, 7, dst, dstOffset + 7);
    dst[dstOffset + 14] = '-';
    // 5 decimal digits for counter (modulo 100000).
    writeFiveDigitDecimal(counter, dst, dstOffset + 15);
  }

  /**
   * Parse an ASCII decimal slice into {@code df}. Zero-allocation. Rejects fractional precision
   * finer than {@code 10^-8} with the singleton {@link JsonParseException#PRICE_PRECISION}; rejects
   * other malformed shapes with {@link JsonParseException#MALFORMED}.
   *
   * <p>The {@link DecimalFloat} is populated as {@code (value, scale)} preserving the EXACT input
   * scale up to {@link FixedPoint#FIXED_POINT_SCALE}; the caller may further normalise via {@link
   * FixedPoint#toDecimalFloat} after a fixed-point round-trip if a canonical scale is required.
   *
   * @param buf source byte array (UTF-8 / ASCII)
   * @param off slice offset
   * @param len slice length
   * @param df target flyweight; mutated in place
   * @throws JsonParseException one of the singletons (identity-comparable)
   */
  public static void parseDecimalToDecimalFloat(
      final byte[] buf, final int off, final int len, final DecimalFloat df) {
    if (len <= 0) {
      throw JsonParseException.MALFORMED;
    }
    // Loop accumulators below (p, mantissa, scale, sawDigit, sawDot) are deliberately non-final
    // because they advance / accumulate inside the parse loop. Per CLAUDE.md, this is the
    // documented exception to the all-locals-final convention; primitives keep explicit type for
    // zero-allocation intent.
    int p = off;
    final int end = off + len;
    final boolean negative;
    if (buf[p] == '-') {
      negative = true;
      p++;
    } else {
      negative = false;
    }
    if (p >= end) {
      throw JsonParseException.MALFORMED;
    }

    long mantissa = 0L;
    int scale = 0;
    boolean sawDigit = false;
    boolean sawDot = false;

    while (p < end) {
      final byte b = buf[p];
      if (b == '.') {
        if (sawDot) {
          throw JsonParseException.MALFORMED;
        }
        sawDot = true;
        p++;
        continue;
      }
      if (b < '0' || b > '9') {
        throw JsonParseException.MALFORMED;
      }
      final int d = b - '0';
      if (sawDot) {
        if (scale >= FixedPoint.FIXED_POINT_SCALE) {
          // Per locked §3 we tolerate trailing zeros at scale 9+ but reject any non-zero
          // digit beyond 8 fractional digits.
          if (d != 0) {
            throw JsonParseException.PRICE_PRECISION;
          }
          // Tolerate the trailing zero — do NOT extend the scale (avoid shrinking headroom in
          // the mantissa for no precision gain).
          p++;
          continue;
        }
        scale++;
      }
      // Overflow-checked accumulate. Since len ≤ 64 KiB and we cap scale at 8, mantissa stays
      // well within int64; multiplyExact is overkill but keeps the cost predictable.
      if (mantissa > (Long.MAX_VALUE - d) / 10L) {
        throw JsonParseException.MALFORMED;
      }
      mantissa = mantissa * 10L + d;
      sawDigit = true;
      p++;
    }

    if (!sawDigit) {
      throw JsonParseException.MALFORMED;
    }

    df.set(negative ? -mantissa : mantissa, scale);
  }

  // ---------------------------------------------------------------------------
  // Internal helpers.
  // ---------------------------------------------------------------------------

  /**
   * Populate the encoder's {@code clOrdID} either from the parsed message (browser passthrough) or
   * from a freshly-minted ID written into {@link #clOrdIdScratch}.
   *
   * <p>Browser passthrough wraps {@code in.scratch} (zero-copy). Mint path writes into the
   * per-instance {@link #clOrdIdScratch} which the encoder also wraps; the buffer survives until
   * the next {@code translate*} call by single-threaded contract.
   */
  private void setNewOrderClOrdId(
      final MutableParsedMessage in,
      final NewOrderSingleEncoder out,
      final long sessionId,
      final long instanceTag,
      final long counter) {
    if (in.clOrdIdOff >= 0 && in.clOrdIdLen >= CLORDID_MIN_LENGTH) {
      if (in.clOrdIdLen > CLORDID_LENGTH) {
        // FIX 4.4 spec caps ClOrdID at 20 bytes; reject overly long values up front.
        throw new IllegalArgumentException(
            "browser ClOrdID exceeds 20 bytes: clOrdIdLen=" + in.clOrdIdLen);
      }
      out.clOrdID(in.scratch, in.clOrdIdOff, in.clOrdIdLen);
      return;
    }
    mintClOrdId(instanceTag, sessionId, counter, clOrdIdScratch, 0);
    out.clOrdID(clOrdIdScratch, 0, CLORDID_LENGTH);
  }

  /**
   * Same passthrough/mint logic as {@link #setNewOrderClOrdId} but for the {@link
   * OrderCancelRequestEncoder}. The minted ID is the NEW ClOrdID for the cancel request itself (the
   * original ClOrdID is encoded separately as {@code OrigClOrdID}).
   */
  private void setCancelClOrdId(
      final MutableParsedMessage in,
      final OrderCancelRequestEncoder out,
      final long sessionId,
      final long instanceTag,
      final long counter) {
    if (in.clOrdIdOff >= 0 && in.clOrdIdLen >= CLORDID_MIN_LENGTH) {
      if (in.clOrdIdLen > CLORDID_LENGTH) {
        throw new IllegalArgumentException(
            "browser ClOrdID exceeds 20 bytes: clOrdIdLen=" + in.clOrdIdLen);
      }
      out.clOrdID(in.scratch, in.clOrdIdOff, in.clOrdIdLen);
      return;
    }
    mintClOrdId(instanceTag, sessionId, counter, clOrdIdScratch, 0);
    out.clOrdID(clOrdIdScratch, 0, CLORDID_LENGTH);
  }

  /**
   * Write {@code v & ((1<<(4*nibbles)) - 1)} as zero-padded lowercase hex into {@code dst} starting
   * at {@code dstOff}.
   *
   * <p>The bottom-up loop avoids any conditional branches inside the digit emit step. Mask is
   * applied implicitly by truncating the high bits after {@code 4 * nibbles} shifts.
   */
  private static void writeHexLowercase(
      final long v, final int nibbles, final byte[] dst, final int dstOff) {
    // `shifted` is a mutated loop accumulator (right-shifted on every iteration) — non-final by
    // design, see CLAUDE.md's loop-accumulator exception.
    long shifted = v;
    for (int i = nibbles - 1; i >= 0; i--) {
      dst[dstOff + i] = HEX[(int) (shifted & 0xFL)];
      shifted >>>= 4;
    }
  }

  /**
   * Write {@code (counter % 100_000)} as a 5-digit zero-padded decimal into {@code dst} at {@code
   * dstOff..dstOff+4}. Caller MUST supply a non-negative {@code counter}; the public entry {@link
   * #mintClOrdId} enforces this at the API boundary.
   */
  private static void writeFiveDigitDecimal(
      final long counter, final byte[] dst, final int dstOff) {
    // `value` is a mutated loop accumulator (divided by 10 each iteration) — non-final by design
    // (CLAUDE.md loop-accumulator carve-out).
    long value = counter % 100_000L;
    for (int i = 4; i >= 0; i--) {
      dst[dstOff + i] = (byte) ('0' + (value % 10L));
      value /= 10L;
    }
  }

  /**
   * Convert a {@link DecimalFloat} to int64 fixed-point (scale {@code 10^-8}). Wraps {@link
   * FixedPoint#toInt64} to keep the call site readable. Zero-allocation.
   */
  private static long priceDfToInt64(final DecimalFloat df) {
    return FixedPoint.toInt64(df);
  }
}
