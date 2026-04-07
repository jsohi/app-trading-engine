package com.trading.engine.gateway;

import com.trading.engine.fix.builder.ExecutionReportEncoder;
import com.trading.engine.fix.builder.OrderCancelRejectEncoder;
import com.trading.engine.fix.builder.QuoteEncoder;
import com.trading.engine.messages.sbe.CxlRejReasonEnum;
import com.trading.engine.messages.sbe.CxlRejResponseToEnum;
import com.trading.engine.messages.sbe.ExecTypeEnum;
import com.trading.engine.messages.sbe.ExecutionReportDecoder;
import com.trading.engine.messages.sbe.OrdStatusEnum;
import com.trading.engine.messages.sbe.OrderCancelRejectDecoder;
import com.trading.engine.messages.sbe.QuoteDecoder;
import com.trading.engine.messages.sbe.SettlTypeEnum;
import com.trading.engine.messages.sbe.SideEnum;
import java.util.concurrent.TimeUnit;
import uk.co.real_logic.artio.fields.DecimalFloat;
import uk.co.real_logic.artio.fields.UtcTimestampEncoder;

/**
 * Translator from SBE decoders to Artio FIX 4.4 encoders. Each public method consumes an SBE
 * decoder positioned over a complete message and writes the field values into a caller-supplied
 * Artio encoder. The caller is responsible for the Artio session header (sender, target, seq num,
 * sending time) and for calling {@code encode()} on the resulting FIX message.
 *
 * <p><b>Threading.</b> This class is <em>not</em> thread-safe. Each instance owns mutable scratch
 * buffers, a {@link DecimalFloat}, and a {@link UtcTimestampEncoder} that are reused across calls;
 * concurrent invocations on the same instance would corrupt them. The gateway is expected to
 * construct one {@code SbeToFixTranslator} per egress duty-cycle thread (typically one per FIX
 * session worker) and never share an instance across threads.
 *
 * <p>The instance-based design is the standard Aeron/Artio pattern: per-thread state lives on a
 * per-thread instance, and the cost of constructing the instance is paid once at startup, after
 * which every {@code translateXxx} call is zero-allocation.
 *
 * <p><b>Allocation.</b> Zero allocation on every method. (The single {@code java.util.concurrent
 * .TimeUnit} reference is enum-constant access, not a {@code java.util.*} collection.) The SBE
 * decoder's {@code getXxx(byte[], int)} accessors copy char fields into <em>per-field</em>
 * dedicated instance {@code byte[]} scratch buffers (one per char field per message type), and the
 * trailing null bytes are stripped before handing the actual length to the Artio encoder's {@code
 * xxx(byte[], int, int)} setter. Per-field buffers are required because Artio's non-{@code AsCopy}
 * setters store a <em>reference</em> to the byte array and only read it when {@code encode()} is
 * called — a single shared scratch would be silently overwritten between fields. The {@code AsCopy}
 * variants would allocate per call, violating the zero-allocation rule. Numeric fields flow through
 * {@link FixedPoint#toDecimalFloat} into a single shared {@link DecimalFloat}. Timestamps flow
 * through a single shared {@link UtcTimestampEncoder}.
 *
 * <p><b>Errors.</b> Unmapped enum values throw {@link IllegalStateException} with a string-literal
 * message naming the field. The gateway is expected to catch and surface as a session-level FIX
 * reject. {@code NULL_VAL} on a required field is treated as a fatal cluster bug and throws.
 *
 * <p><b>Multileg.</b> ExecutionReport and Quote both translate the {@code noLegs} repeating group
 * (FX swap fills and FX swap quotes). Per-leg scratch buffers are sliced from a single
 * pre-allocated byte array of size {@link #MAX_LEGS}; messages exceeding {@code MAX_LEGS} legs
 * throw {@link IllegalStateException}.
 */
public final class SbeToFixTranslator {

  // Per-field dedicated scratch buffers. Artio's FIX encoder xxx(byte[], int, int) setters
  // store a *reference* to the byte array (no copy) and only read it during encode(); using a
  // single shared scratch would let later setters silently overwrite earlier ones. The
  // alternative — Artio's xxxAsCopy variants — allocates per call, which violates the
  // zero-allocation rule. So each char field gets its own byte[] sized to the SBE field length.
  //
  // ExecutionReport char fields:
  private final byte[] erOrderId =
      new byte[com.trading.engine.messages.sbe.ExecutionReportDecoder.orderIdLength()];
  private final byte[] erExecId =
      new byte[com.trading.engine.messages.sbe.ExecutionReportDecoder.execIdLength()];
  private final byte[] erClOrdId =
      new byte[com.trading.engine.messages.sbe.ExecutionReportDecoder.clOrdIdLength()];
  private final byte[] erSymbol =
      new byte[com.trading.engine.messages.sbe.ExecutionReportDecoder.symbolLength()];
  private final byte[] erText =
      new byte[com.trading.engine.messages.sbe.ExecutionReportDecoder.textLength()];
  private final byte[] erSettlDate =
      new byte[com.trading.engine.messages.sbe.ExecutionReportDecoder.settlDateLength()];
  private final byte[] erCurrency =
      new byte[com.trading.engine.messages.sbe.ExecutionReportDecoder.currencyLength()];
  private final byte[] erSettlCurrency =
      new byte[com.trading.engine.messages.sbe.ExecutionReportDecoder.settlCurrencyLength()];

  // Quote leg-level char fields (per-leg sliced — same MAX_LEGS rationale as ER):
  private static final int Q_LEG_SETTL_DATE_LEN =
      com.trading.engine.messages.sbe.QuoteDecoder.NoLegsDecoder.legSettlDateLength();
  private static final int Q_LEG_CURRENCY_LEN =
      com.trading.engine.messages.sbe.QuoteDecoder.NoLegsDecoder.legCurrencyLength();
  private final byte[] qLegSettlDate = new byte[MAX_LEGS * Q_LEG_SETTL_DATE_LEN];
  private final byte[] qLegCurrency = new byte[MAX_LEGS * Q_LEG_CURRENCY_LEN];

  // Quote char fields:
  private final byte[] qQuoteReqId =
      new byte[com.trading.engine.messages.sbe.QuoteDecoder.quoteReqIdLength()];
  private final byte[] qQuoteId =
      new byte[com.trading.engine.messages.sbe.QuoteDecoder.quoteIdLength()];
  private final byte[] qSymbol =
      new byte[com.trading.engine.messages.sbe.QuoteDecoder.symbolLength()];
  private final byte[] qText = new byte[com.trading.engine.messages.sbe.QuoteDecoder.textLength()];
  private final byte[] qSettlDate =
      new byte[com.trading.engine.messages.sbe.QuoteDecoder.settlDateLength()];
  private final byte[] qCurrency =
      new byte[com.trading.engine.messages.sbe.QuoteDecoder.currencyLength()];
  // SettlCurrency intentionally absent — not in stock FIX 4.4 Quote(35=S). See translateQuote.

  // ExecutionReport leg-level char fields. Sized for up to MAX_LEGS legs sharing the buffer
  // contiguously: leg N uses [N*FIELD_LEN, (N+1)*FIELD_LEN). This is the per-leg-per-field
  // pattern needed because Artio's leg encoder setters store a *reference* to the byte array
  // and read it during encode(); two legs sharing the same byte[] offset would alias.
  //
  // MAX_LEGS=8 covers the FX product universe the engine actually speaks: spot (1 leg),
  // forward (1 leg), swap (2 legs near + far), butterfly/condor strategies (3-4 legs), and
  // headroom for exotic option spreads up to 8 legs. The SBE schema's noLegs group type-level
  // bound is `numInGroup=uint16` (65535) — far above any realistic usage — so this is an
  // engine-side cap, not a protocol cap. The `legCount > MAX_LEGS` guard in each leg loop
  // throws IllegalStateException at the boundary; bumping the cap just means widening the
  // per-leg scratch arrays.
  private static final int MAX_LEGS = 8;

  private static final int ER_LEG_SETTL_DATE_LEN =
      com.trading.engine.messages.sbe.ExecutionReportDecoder.NoLegsDecoder.legSettlDateLength();
  private static final int ER_LEG_CURRENCY_LEN =
      com.trading.engine.messages.sbe.ExecutionReportDecoder.NoLegsDecoder.legCurrencyLength();
  private final byte[] erLegSettlDate = new byte[MAX_LEGS * ER_LEG_SETTL_DATE_LEN];
  private final byte[] erLegCurrency = new byte[MAX_LEGS * ER_LEG_CURRENCY_LEN];

  // OrderCancelReject char fields:
  private final byte[] ocrOrderId =
      new byte[com.trading.engine.messages.sbe.OrderCancelRejectDecoder.orderIdLength()];
  private final byte[] ocrClOrdId =
      new byte[com.trading.engine.messages.sbe.OrderCancelRejectDecoder.clOrdIdLength()];
  private final byte[] ocrOrigClOrdId =
      new byte[com.trading.engine.messages.sbe.OrderCancelRejectDecoder.origClOrdIdLength()];
  private final byte[] ocrAccount =
      new byte[com.trading.engine.messages.sbe.OrderCancelRejectDecoder.accountCodeLength()];
  private final byte[] ocrText =
      new byte[com.trading.engine.messages.sbe.OrderCancelRejectDecoder.textLength()];

  /**
   * Per-field scratch for the encoded UTC timestamp. Same reference-aliasing concern as the other
   * char fields applies to {@link UtcTimestampEncoder#buffer()}: it's the encoder's internal buffer
   * and gets overwritten on the next {@code encodeFrom} call. We copy out into this dedicated
   * buffer so the FIX encoder can hold a stable reference until {@code encode()}. 32 bytes covers
   * FIX UTC timestamp formats up to nanosecond precision (27 chars).
   */
  private final byte[] erTransactTime = new byte[32];

  /** Same per-field rationale as {@link #erTransactTime}, for OrderCancelReject. */
  private final byte[] ocrTransactTime = new byte[32];

  /** Same per-field rationale as {@link #erTransactTime}, for Quote.transactTime. */
  private final byte[] qTransactTime = new byte[32];

  /**
   * Same per-field rationale as {@link #erTransactTime}, for Quote.validUntilTime — needs its own
   * buffer because it's encoded by the same {@link #tsEnc} as {@link #qTransactTime} during the
   * same translateQuote call, and the second encodeFrom() would overwrite the first.
   */
  private final byte[] qValidUntilTime = new byte[32];

  /** Reusable DecimalFloat for FIX decimal field setters. */
  private final DecimalFloat dec = new DecimalFloat();

  /** Reusable UTC timestamp encoder for transactTime fields. */
  private final UtcTimestampEncoder tsEnc = new UtcTimestampEncoder();

  public SbeToFixTranslator() {}

  // ---------------------------------------------------------------------------
  // ExecutionReport (35=8)
  // ---------------------------------------------------------------------------

  /**
   * Translate an SBE ExecutionReport into the supplied Artio FIX 4.4 encoder. The caller must have
   * wrapped {@code sbe} over a complete decoded ExecutionReport message and must populate the FIX
   * session header on {@code fix} before calling {@code encode}.
   */
  public void translateExecutionReport(ExecutionReportDecoder sbe, ExecutionReportEncoder fix) {
    // orderID — required. SBE field is null-padded; trim before passing to FIX.
    sbe.getOrderId(erOrderId, 0);
    fix.orderID(erOrderId, 0, trimNulls(erOrderId));

    // execID — required
    sbe.getExecId(erExecId, 0);
    fix.execID(erExecId, 0, trimNulls(erExecId));

    // clOrdID — required (per FIX 4.4 ER spec)
    sbe.getClOrdId(erClOrdId, 0);
    fix.clOrdID(erClOrdId, 0, trimNulls(erClOrdId));

    // execType — required, full enum exhaustion
    fix.execType(mapExecType(sbe.execType()));

    // ordStatus — required, full enum exhaustion
    fix.ordStatus(mapOrdStatus(sbe.ordStatus()));

    // symbol — required
    sbe.getSymbol(erSymbol, 0);
    fix.instrument().symbol(erSymbol, 0, trimNulls(erSymbol));

    // side — required
    fix.side(mapSide(sbe.side()));

    // leavesQty — required
    FixedPoint.toDecimalFloat(sbe.leavesQty(), dec);
    fix.leavesQty(dec);

    // cumQty — required
    FixedPoint.toDecimalFloat(sbe.cumQty(), dec);
    fix.cumQty(dec);

    // avgPx — required by FIX 4.4 ER, but SBE allows null on non-fill ExecTypes (New,
    // Rejected, Canceled, etc.) where there's nothing yet to average. The cross-field
    // invariant is simpler and stricter than gating on ExecType: if cumQty > 0, avgPx must
    // be populated regardless of which ExecType the cluster chose. Shipping a "filled at
    // zero" execution downstream would corrupt P&L tracking and audit trails. Throw at the
    // boundary if the invariant is violated; otherwise emit avgPx=0 (satisfies the FIX
    // wire-level required-field check for the no-fill statuses).
    long avg = sbe.avgPx();
    if (avg == ExecutionReportDecoder.avgPxNullValue()) {
      if (sbe.cumQty() != 0L) {
        throw new IllegalStateException(
            "ExecutionReport has cumQty="
                + sbe.cumQty()
                + " but null avgPx (execType="
                + sbe.execType()
                + ")");
      }
      avg = 0L;
    }
    FixedPoint.toDecimalFloat(avg, dec);
    fix.avgPx(dec);

    // transactTime — required. SBE is uint64 epoch nanos; encode as Artio's UTC timestamp ASCII.
    // Copy out of the encoder's internal buffer into a dedicated per-field buffer so the FIX
    // encoder's reference stays stable until encode() (same reasoning as the char fields).
    int tsLen = tsEnc.encodeFrom(sbe.transactTime(), TimeUnit.NANOSECONDS);
    System.arraycopy(tsEnc.buffer(), 0, erTransactTime, 0, tsLen);
    fix.transactTime(erTransactTime, 0, tsLen);

    // text — optional
    sbe.getText(erText, 0);
    int trimmed = trimNulls(erText);
    if (trimmed > 0) {
      fix.text(erText, 0, trimmed);
    }

    // settlType — optional
    if (sbe.settlType() != SettlTypeEnum.NULL_VAL) {
      fix.settlType(mapSettlTypeToFix(sbe.settlType()));
    }

    // settlDate — optional
    sbe.getSettlDate(erSettlDate, 0);
    trimmed = trimNulls(erSettlDate);
    if (trimmed > 0) {
      fix.settlDate(erSettlDate, 0, trimmed);
    }

    // currency — optional
    sbe.getCurrency(erCurrency, 0);
    trimmed = trimNulls(erCurrency);
    if (trimmed > 0) {
      fix.currency(erCurrency, 0, trimmed);
    }

    // settlCurrency — optional
    sbe.getSettlCurrency(erSettlCurrency, 0);
    trimmed = trimNulls(erSettlCurrency);
    if (trimmed > 0) {
      fix.settlCurrency(erSettlCurrency, 0, trimmed);
    }

    // productType / tenor — APP-45 (custom tags 10013/10001 not in stock FIX 4.4)

    // noLegs repeating group — for FX swap fills, the cluster reports per-leg execution
    // details. We map every leg's standard FIX 4.4 fields. The trading-engine custom leg
    // quantities (legLastQty=10010, legLeavesQty=10011, legCumQty=10012) are not in stock
    // FIX 4.4 and stay as APP-45 work; the standard legLastPx still propagates so a fill
    // count and last-trade price can round-trip even without the custom quantities.
    final ExecutionReportDecoder.NoLegsDecoder noLegs = sbe.noLegs();
    final int legCount = noLegs.count();
    if (legCount > MAX_LEGS) {
      throw new IllegalStateException(
          "ExecutionReport noLegs count " + legCount + " exceeds MAX_LEGS=" + MAX_LEGS);
    }
    if (legCount > 0) {
      // Artio's LegsGroupEncoder.next() returns a *new* LegsGroupEncoder (linked-list
      // pattern), NOT a self-reference. Each leg has its own encoder instance; advancing
      // means re-binding the local variable to next().
      ExecutionReportEncoder.LegsGroupEncoder leg = fix.legsGroup(legCount);
      int legIdx = 0;
      while (noLegs.hasNext()) {
        noLegs.next();
        if (legIdx > 0) {
          leg = leg.next();
        }
        // legSide (no NULL_VAL guard — required at the leg level when present)
        if (noLegs.legSide() != SideEnum.NULL_VAL) {
          leg.instrumentLeg().legSide(mapSide(noLegs.legSide()));
        }
        // legSettlDate — sliced into per-leg offset of erLegSettlDate
        final int sdOffset = legIdx * ER_LEG_SETTL_DATE_LEN;
        noLegs.getLegSettlDate(erLegSettlDate, sdOffset);
        final int sdTrim = trimNulls(erLegSettlDate, sdOffset, ER_LEG_SETTL_DATE_LEN);
        if (sdTrim > 0) {
          leg.legSettlDate(erLegSettlDate, sdOffset, sdTrim);
        }
        // legSettlType — optional
        if (noLegs.legSettlType() != SettlTypeEnum.NULL_VAL) {
          leg.legSettlType(mapSettlTypeToFix(noLegs.legSettlType()));
        }
        // legCurrency — sliced into per-leg offset of erLegCurrency
        final int curOffset = legIdx * ER_LEG_CURRENCY_LEN;
        noLegs.getLegCurrency(erLegCurrency, curOffset);
        final int curTrim = trimNulls(erLegCurrency, curOffset, ER_LEG_CURRENCY_LEN);
        if (curTrim > 0) {
          leg.instrumentLeg().legCurrency(erLegCurrency, curOffset, curTrim);
        }
        // legLastPx — required at fill time. SBE uses Long.MIN_VALUE as the null sentinel.
        if (noLegs.legLastPx() != ExecutionReportDecoder.NoLegsDecoder.legLastPxNullValue()) {
          FixedPoint.toDecimalFloat(noLegs.legLastPx(), dec);
          leg.legLastPx(dec);
        }
        // legLastQty / legLeavesQty / legCumQty — APP-45 (custom tags)
        legIdx++;
      }
    }
  }

  // ---------------------------------------------------------------------------
  // OrderCancelReject (35=9)
  // ---------------------------------------------------------------------------

  /**
   * Translate an SBE OrderCancelReject into the supplied Artio FIX 4.4 encoder. Caller wraps {@code
   * sbe} over the decoded message and populates the FIX session header on {@code fix}.
   */
  public void translateOrderCancelReject(
      OrderCancelRejectDecoder sbe, OrderCancelRejectEncoder fix) {
    // orderID — required (per FIX 4.4 OCR spec)
    sbe.getOrderId(ocrOrderId, 0);
    fix.orderID(ocrOrderId, 0, trimNulls(ocrOrderId));

    // clOrdID — required
    sbe.getClOrdId(ocrClOrdId, 0);
    fix.clOrdID(ocrClOrdId, 0, trimNulls(ocrClOrdId));

    // origClOrdID — required
    sbe.getOrigClOrdId(ocrOrigClOrdId, 0);
    fix.origClOrdID(ocrOrigClOrdId, 0, trimNulls(ocrOrigClOrdId));

    // ordStatus — required
    fix.ordStatus(mapOrdStatus(sbe.ordStatus()));

    // cxlRejResponseTo — required, char on the wire
    fix.cxlRejResponseTo(mapCxlRejResponseTo(sbe.cxlRejResponseTo()));

    // cxlRejReason — optional in FIX, int on the wire
    if (sbe.cxlRejReason() != CxlRejReasonEnum.NULL_VAL) {
      fix.cxlRejReason(mapCxlRejReason(sbe.cxlRejReason()));
    }

    // account — optional
    sbe.getAccountCode(ocrAccount, 0);
    int trimmed = trimNulls(ocrAccount);
    if (trimmed > 0) {
      fix.account(ocrAccount, 0, trimmed);
    }

    // transactTime — required
    int tsLen = tsEnc.encodeFrom(sbe.transactTime(), TimeUnit.NANOSECONDS);
    System.arraycopy(tsEnc.buffer(), 0, ocrTransactTime, 0, tsLen);
    fix.transactTime(ocrTransactTime, 0, tsLen);

    // text — optional
    sbe.getText(ocrText, 0);
    trimmed = trimNulls(ocrText);
    if (trimmed > 0) {
      fix.text(ocrText, 0, trimmed);
    }
  }

  // ---------------------------------------------------------------------------
  // Quote (35=S)
  // ---------------------------------------------------------------------------

  /**
   * Translate an SBE Quote into the supplied Artio FIX 4.4 encoder. Caller wraps {@code sbe} over
   * the decoded message and populates the FIX session header on {@code fix}.
   *
   * <p>The SBE {@code quoteStatus} field is informational only — FIX 4.4 Quote(35=S) has no
   * QuoteStatus tag (that lives on QuoteStatusReport 35=AI), so the field is dropped at the
   * boundary. The {@code noLegs} repeating group IS translated (for FX swap quotes); per-leg char
   * fields use sliced scratch buffers from {@link #qLegSettlDate}/{@link #qLegCurrency}.
   */
  public void translateQuote(QuoteDecoder sbe, QuoteEncoder fix) {
    // quoteReqID — optional
    sbe.getQuoteReqId(qQuoteReqId, 0);
    int trimmed = trimNulls(qQuoteReqId);
    if (trimmed > 0) {
      fix.quoteReqID(qQuoteReqId, 0, trimmed);
    }

    // quoteID — required
    sbe.getQuoteId(qQuoteId, 0);
    fix.quoteID(qQuoteId, 0, trimNulls(qQuoteId));

    // symbol — required, via Instrument component
    sbe.getSymbol(qSymbol, 0);
    fix.instrument().symbol(qSymbol, 0, trimNulls(qSymbol));

    // side — optional in FIX 4.4 Quote
    if (sbe.side() != SideEnum.NULL_VAL) {
      fix.side(mapSide(sbe.side()));
    }

    // bidPx, offerPx, bidSize, offerSize — all optional. NULL_VAL is encoded as Long.MIN_VALUE
    // by the SBE generator.
    if (sbe.bidPx() != QuoteDecoder.bidPxNullValue()) {
      FixedPoint.toDecimalFloat(sbe.bidPx(), dec);
      fix.bidPx(dec);
    }
    if (sbe.offerPx() != QuoteDecoder.offerPxNullValue()) {
      FixedPoint.toDecimalFloat(sbe.offerPx(), dec);
      fix.offerPx(dec);
    }
    if (sbe.bidSize() != QuoteDecoder.bidSizeNullValue()) {
      FixedPoint.toDecimalFloat(sbe.bidSize(), dec);
      fix.bidSize(dec);
    }
    if (sbe.offerSize() != QuoteDecoder.offerSizeNullValue()) {
      FixedPoint.toDecimalFloat(sbe.offerSize(), dec);
      fix.offerSize(dec);
    }

    // transactTime — required
    int tsLen = tsEnc.encodeFrom(sbe.transactTime(), TimeUnit.NANOSECONDS);
    System.arraycopy(tsEnc.buffer(), 0, qTransactTime, 0, tsLen);
    fix.transactTime(qTransactTime, 0, tsLen);

    // validUntilTime — optional. SBE uint64 nanos; FIX 4.4 ASCII UTC timestamp.
    if (sbe.validUntil() != QuoteDecoder.validUntilNullValue()) {
      int vtLen = tsEnc.encodeFrom(sbe.validUntil(), TimeUnit.NANOSECONDS);
      System.arraycopy(tsEnc.buffer(), 0, qValidUntilTime, 0, vtLen);
      fix.validUntilTime(qValidUntilTime, 0, vtLen);
    }

    // text — optional
    sbe.getText(qText, 0);
    trimmed = trimNulls(qText);
    if (trimmed > 0) {
      fix.text(qText, 0, trimmed);
    }

    // settlType — optional
    if (sbe.settlType() != SettlTypeEnum.NULL_VAL) {
      fix.settlType(mapSettlTypeToFix(sbe.settlType()));
    }

    // settlDate — optional
    sbe.getSettlDate(qSettlDate, 0);
    trimmed = trimNulls(qSettlDate);
    if (trimmed > 0) {
      fix.settlDate(qSettlDate, 0, trimmed);
    }

    // currency — optional
    sbe.getCurrency(qCurrency, 0);
    trimmed = trimNulls(qCurrency);
    if (trimmed > 0) {
      fix.currency(qCurrency, 0, trimmed);
    }

    // settlCurrency — not in stock FIX 4.4 Quote(35=S); the SBE field is dropped at the
    // boundary. APP-45 will add a custom tag if/when needed.

    // productType / tenor / swapPoints — APP-45 (custom tags 10001/10003/10013)

    // noLegs repeating group — for FX swap quotes the cluster sends per-leg pricing.
    // Custom leg sizes (legBidSize/legOfferSize) are SBE-only and don't exist in stock
    // FIX 4.4 NoLegs; standard fields (side/settl/currency/legBidPx/legOfferPx) propagate.
    final QuoteDecoder.NoLegsDecoder qLegs = sbe.noLegs();
    final int qLegCount = qLegs.count();
    if (qLegCount > MAX_LEGS) {
      throw new IllegalStateException(
          "Quote noLegs count " + qLegCount + " exceeds MAX_LEGS=" + MAX_LEGS);
    }
    if (qLegCount > 0) {
      // Same Artio linked-list pattern as ER above.
      QuoteEncoder.LegsGroupEncoder leg = fix.legsGroup(qLegCount);
      int legIdx = 0;
      while (qLegs.hasNext()) {
        qLegs.next();
        if (legIdx > 0) {
          leg = leg.next();
        }
        if (qLegs.legSide() != SideEnum.NULL_VAL) {
          leg.instrumentLeg().legSide(mapSide(qLegs.legSide()));
        }
        final int sdOffset = legIdx * Q_LEG_SETTL_DATE_LEN;
        qLegs.getLegSettlDate(qLegSettlDate, sdOffset);
        final int sdTrim = trimNulls(qLegSettlDate, sdOffset, Q_LEG_SETTL_DATE_LEN);
        if (sdTrim > 0) {
          leg.legSettlDate(qLegSettlDate, sdOffset, sdTrim);
        }
        if (qLegs.legSettlType() != SettlTypeEnum.NULL_VAL) {
          leg.legSettlType(mapSettlTypeToFix(qLegs.legSettlType()));
        }
        final int curOffset = legIdx * Q_LEG_CURRENCY_LEN;
        qLegs.getLegCurrency(qLegCurrency, curOffset);
        final int curTrim = trimNulls(qLegCurrency, curOffset, Q_LEG_CURRENCY_LEN);
        if (curTrim > 0) {
          leg.instrumentLeg().legCurrency(qLegCurrency, curOffset, curTrim);
        }
        if (qLegs.legBidPx() != QuoteDecoder.NoLegsDecoder.legBidPxNullValue()) {
          FixedPoint.toDecimalFloat(qLegs.legBidPx(), dec);
          leg.legBidPx(dec);
        }
        if (qLegs.legOfferPx() != QuoteDecoder.NoLegsDecoder.legOfferPxNullValue()) {
          FixedPoint.toDecimalFloat(qLegs.legOfferPx(), dec);
          leg.legOfferPx(dec);
        }
        // legBidSize / legOfferSize — not in stock FIX 4.4 Quote.NoLegs; SBE-only.
        legIdx++;
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  /**
   * Find the length of the non-null prefix of {@code field}. SBE char fields are fixed length and
   * null-padded; FIX wire fields are variable length, so we drop the padding before passing the
   * actual length to the Artio encoder.
   */
  private static int trimNulls(byte[] field) {
    int len = field.length;
    while (len > 0 && field[len - 1] == 0) {
      len--;
    }
    return len;
  }

  /**
   * Slice variant of {@link #trimNulls(byte[])} — finds the non-null prefix of the {@code [offset,
   * offset+length)} window inside {@code field}. Used by leg-level scratch buffers that pack
   * multiple legs contiguously into a single byte array.
   */
  private static int trimNulls(byte[] field, int offset, int length) {
    int end = length;
    while (end > 0 && field[offset + end - 1] == 0) {
      end--;
    }
    return end;
  }

  // ---------------------------------------------------------------------------
  // Enum mappings (SBE enum → FIX char). Throw on NULL_VAL or unmapped values.
  // ---------------------------------------------------------------------------

  private static char mapSide(SideEnum sbe) {
    return switch (sbe) {
      case Buy -> '1';
      case Sell -> '2';
      default -> throw new IllegalStateException("Unsupported SBE Side for FIX wire: " + sbe);
    };
  }

  private static char mapExecType(ExecTypeEnum sbe) {
    return switch (sbe) {
      case New -> '0';
      case PartialFill -> '1';
      case Fill -> '2';
      case DoneForDay -> '3';
      case Canceled -> '4';
      case Replaced -> '5';
      case PendingCancel -> '6';
      case Stopped -> '7';
      case Rejected -> '8';
      case Suspended -> '9';
      case PendingNew -> 'A';
      case Calculated -> 'B';
      case Expired -> 'C';
      case Restated -> 'D';
      case PendingReplace -> 'E';
      case Trade -> 'F';
      case TradeCorrect -> 'G';
      case TradeCancel -> 'H';
      case OrderStatus -> 'I';
      default -> throw new IllegalStateException("Unsupported SBE ExecType for FIX wire: " + sbe);
    };
  }

  private static char mapOrdStatus(OrdStatusEnum sbe) {
    return switch (sbe) {
      case New -> '0';
      case PartiallyFilled -> '1';
      case Filled -> '2';
      case DoneForDay -> '3';
      case Canceled -> '4';
      case Replaced -> '5';
      case PendingCancel -> '6';
      case Stopped -> '7';
      case Rejected -> '8';
      case Suspended -> '9';
      case PendingNew -> 'A';
      case Calculated -> 'B';
      case Expired -> 'C';
      case AcceptedForBidding -> 'D';
      case PendingReplace -> 'E';
      default -> throw new IllegalStateException("Unsupported SBE OrdStatus for FIX wire: " + sbe);
    };
  }

  private static char mapCxlRejResponseTo(CxlRejResponseToEnum sbe) {
    return switch (sbe) {
      case OrderCancelRequest -> '1';
      case OrderCancelReplaceRequest -> '2';
      default ->
          throw new IllegalStateException("Unsupported SBE CxlRejResponseTo for FIX wire: " + sbe);
    };
  }

  private static int mapCxlRejReason(CxlRejReasonEnum sbe) {
    return switch (sbe) {
      case TooLateToCancel -> 0;
      case UnknownOrder -> 1;
      case BrokerOption -> 2;
      case OrderAlreadyInPendingStatus -> 3;
      case UnableToProcessOrderMassCancelRequest -> 4;
      case OrigOrdModTime -> 5;
      case DuplicateClOrdID -> 6;
      case Other -> 99;
      default ->
          throw new IllegalStateException("Unsupported SBE CxlRejReason for FIX wire: " + sbe);
    };
  }

  private static char mapSettlTypeToFix(SettlTypeEnum sbe) {
    return switch (sbe) {
      case Regular -> '0';
      case Cash -> '1';
      case NextDay -> '2';
      case TPlus2 -> '3';
      case TPlus3 -> '4';
      case TPlus4 -> '5';
      case Future -> '6';
      case WhenAndIfIssued -> '7';
      case SellersOption -> '8';
      case TPlus5 -> '9';
      case BrokenDate -> 'B';
      case FXSpotNextDay -> 'C';
      default -> throw new IllegalStateException("Unsupported SBE SettlType for FIX wire: " + sbe);
    };
  }
}
