package com.trading.engine.gateway;

import com.trading.engine.fix.builder.ExecutionReportEncoder;
import com.trading.engine.fix.builder.OrderCancelRejectEncoder;
import com.trading.engine.fix.builder.QuoteEncoder;
import com.trading.engine.fix.builder.QuoteRequestRejectEncoder;
import com.trading.engine.messages.sbe.CancelReasonEnum;
import com.trading.engine.messages.sbe.CxlRejReasonEnum;
import com.trading.engine.messages.sbe.CxlRejResponseToEnum;
import com.trading.engine.messages.sbe.ExecTypeEnum;
import com.trading.engine.messages.sbe.ExecutionReportDecoder;
import com.trading.engine.messages.sbe.ExpireReasonEnum;
import com.trading.engine.messages.sbe.OrdStatusEnum;
import com.trading.engine.messages.sbe.OrdTypeEnum;
import com.trading.engine.messages.sbe.OrderCancelRejectDecoder;
import com.trading.engine.messages.sbe.OrderCanceledEventDecoder;
import com.trading.engine.messages.sbe.OrderCreatedEventDecoder;
import com.trading.engine.messages.sbe.OrderExpiredEventDecoder;
import com.trading.engine.messages.sbe.OrderRejectedEventDecoder;
import com.trading.engine.messages.sbe.QuoteDecoder;
import com.trading.engine.messages.sbe.QuoteRejectReasonEnum;
import com.trading.engine.messages.sbe.QuoteRequestRejectDecoder;
import com.trading.engine.messages.sbe.RejectReasonEnum;
import com.trading.engine.messages.sbe.SettlTypeEnum;
import com.trading.engine.messages.sbe.SideEnum;
import com.trading.engine.messages.sbe.TimeInForceEnum;
import java.nio.charset.StandardCharsets;
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
  private final byte[] erOrderId = new byte[ExecutionReportDecoder.orderIdLength()];
  private final byte[] erExecId = new byte[ExecutionReportDecoder.execIdLength()];
  private final byte[] erClOrdId = new byte[ExecutionReportDecoder.clOrdIdLength()];
  private final byte[] erSymbol = new byte[ExecutionReportDecoder.symbolLength()];
  private final byte[] erText = new byte[ExecutionReportDecoder.textLength()];
  private final byte[] erSettlDate = new byte[ExecutionReportDecoder.settlDateLength()];
  private final byte[] erCurrency = new byte[ExecutionReportDecoder.currencyLength()];
  private final byte[] erSettlCurrency = new byte[ExecutionReportDecoder.settlCurrencyLength()];

  // Quote leg-level char fields (per-leg sliced — same MAX_LEGS rationale as ER):
  private static final int Q_LEG_SETTL_DATE_LEN = QuoteDecoder.NoLegsDecoder.legSettlDateLength();
  private static final int Q_LEG_CURRENCY_LEN = QuoteDecoder.NoLegsDecoder.legCurrencyLength();
  private final byte[] qLegSettlDate = new byte[MAX_LEGS * Q_LEG_SETTL_DATE_LEN];
  private final byte[] qLegCurrency = new byte[MAX_LEGS * Q_LEG_CURRENCY_LEN];

  // Quote char fields:
  private final byte[] qQuoteReqId = new byte[QuoteDecoder.quoteReqIdLength()];
  private final byte[] qQuoteId = new byte[QuoteDecoder.quoteIdLength()];
  private final byte[] qSymbol = new byte[QuoteDecoder.symbolLength()];
  private final byte[] qText = new byte[QuoteDecoder.textLength()];
  private final byte[] qSettlDate = new byte[QuoteDecoder.settlDateLength()];
  private final byte[] qCurrency = new byte[QuoteDecoder.currencyLength()];
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
      ExecutionReportDecoder.NoLegsDecoder.legSettlDateLength();
  private static final int ER_LEG_CURRENCY_LEN =
      ExecutionReportDecoder.NoLegsDecoder.legCurrencyLength();
  private final byte[] erLegSettlDate = new byte[MAX_LEGS * ER_LEG_SETTL_DATE_LEN];
  private final byte[] erLegCurrency = new byte[MAX_LEGS * ER_LEG_CURRENCY_LEN];

  // OrderCancelReject char fields:
  private final byte[] ocrOrderId = new byte[OrderCancelRejectDecoder.orderIdLength()];
  private final byte[] ocrClOrdId = new byte[OrderCancelRejectDecoder.clOrdIdLength()];
  private final byte[] ocrOrigClOrdId = new byte[OrderCancelRejectDecoder.origClOrdIdLength()];
  private final byte[] ocrAccount = new byte[OrderCancelRejectDecoder.accountCodeLength()];
  private final byte[] ocrText = new byte[OrderCancelRejectDecoder.textLength()];

  // QuoteRequestReject char fields (prefix "qrr"):
  private final byte[] qrrQuoteReqId = new byte[QuoteRequestRejectDecoder.quoteReqIdLength()];
  private final byte[] qrrSymbol = new byte[QuoteRequestRejectDecoder.symbolLength()];
  private final byte[] qrrText = new byte[QuoteRequestRejectDecoder.textLength()];
  private final byte[] qrrTransactTime = new byte[32];

  // OrderCreatedEvent char fields (prefix "oc" = orderCreated):
  private final byte[] ocOrderId = new byte[OrderCreatedEventDecoder.orderIdLength()];
  private final byte[] ocExecId = new byte[OrderCreatedEventDecoder.execIdLength()];
  private final byte[] ocClOrdId = new byte[OrderCreatedEventDecoder.clOrdIdLength()];
  private final byte[] ocSymbol = new byte[OrderCreatedEventDecoder.symbolLength()];
  private final byte[] ocSettlDate = new byte[OrderCreatedEventDecoder.settlDateLength()];
  private final byte[] ocCurrency = new byte[OrderCreatedEventDecoder.currencyLength()];
  private final byte[] ocSettlCurrency = new byte[OrderCreatedEventDecoder.settlCurrencyLength()];
  private final byte[] ocAccountCode = new byte[OrderCreatedEventDecoder.accountCodeLength()];

  // OrderRejectedEvent char fields (prefix "or" = orderRejected):
  private final byte[] orClOrdId = new byte[OrderRejectedEventDecoder.clOrdIdLength()];
  private final byte[] orSymbol = new byte[OrderRejectedEventDecoder.symbolLength()];
  private final byte[] orAccountCode = new byte[OrderRejectedEventDecoder.accountCodeLength()];
  private final byte[] orCurrency = new byte[OrderRejectedEventDecoder.currencyLength()];
  private final byte[] orText = new byte[OrderRejectedEventDecoder.textLength()];

  // OrderCanceledEvent char fields (prefix "oxl" = orderCanceled — "oc" conflicts with
  // orderCreated above). APP-151 phase 2. Reuse-across-calls is safe because SBE's generated
  // {@code getXxx(byte[], 0)} accessors copy the full fixed field length on every call,
  // re-overwriting any prior bytes; the trailing nulls in the source wire bytes (zero-padded by
  // the encoder side at emit time) are copied across too, so {@code trimNulls} observes only the
  // current value — no stale-byte bleed from a longer prior call. Covered by {@code
  // translateOrderCanceledEvent_consecutiveCalls_noScratchBufferCorruption}.
  private final byte[] oxlOrderId = new byte[OrderCanceledEventDecoder.orderIdLength()];
  private final byte[] oxlClOrdId = new byte[OrderCanceledEventDecoder.clOrdIdLength()];
  private final byte[] oxlOrigClOrdId = new byte[OrderCanceledEventDecoder.origClOrdIdLength()];
  private final byte[] oxlSymbol = new byte[OrderCanceledEventDecoder.symbolLength()];

  /**
   * APP-151 phase 3: the cluster mints a real {@code execId} on every cancel event (template 103),
   * so the gateway just copies it from the wire — no more synthesised {@code "CXL-"+clOrdId}
   * sentinel. Per-field scratch reuse same as the other {@code oxl*} buffers.
   */
  private final byte[] oxlExecId = new byte[OrderCanceledEventDecoder.execIdLength()];

  // OrderExpiredEvent char fields (prefix "oxp" — APP-62 §J). Dedicated scratch arrays distinct
  // from the {@code oxl*} cancel buffers so a hypothetical interleaved cancel+expire dispatch
  // (e.g., a future emitter ordering that fires both events for the same order) can never
  // alias the Artio-encoder byte[] references that Artio captures by reference and reads only
  // at encode() time. Single-threaded gateway duty-cycle means we don't observe interleaving
  // today, but the per-event dedicated buffer is the safe industry-standard pattern and keeps
  // future-proofing aligned with the {@code oxl*} rationale above. OrderExpiredEvent does NOT
  // carry origClOrdId (FIX tag 41 has no semantics for server-initiated expiries), so no
  // origClOrdId scratch is allocated.
  private final byte[] oxpOrderId = new byte[OrderExpiredEventDecoder.orderIdLength()];
  private final byte[] oxpExecId = new byte[OrderExpiredEventDecoder.execIdLength()];
  private final byte[] oxpClOrdId = new byte[OrderExpiredEventDecoder.clOrdIdLength()];
  private final byte[] oxpSymbol = new byte[OrderExpiredEventDecoder.symbolLength()];

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

  /** Same per-field rationale as {@link #erTransactTime}, for OrderCreatedEvent. */
  private final byte[] ocTransactTime = new byte[32];

  /** Same per-field rationale as {@link #erTransactTime}, for OrderRejectedEvent. */
  private final byte[] orTransactTime = new byte[32];

  /** Same per-field rationale as {@link #erTransactTime}, for OrderCanceledEvent (APP-151 ph2). */
  private final byte[] oxlTransactTime = new byte[32];

  /** Same per-field rationale as {@link #erTransactTime}, for OrderExpiredEvent (APP-62 §J). */
  private final byte[] oxpTransactTime = new byte[32];

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
  // QuoteRequestReject (35=AG)
  // ---------------------------------------------------------------------------

  /**
   * Translate an SBE QuoteRequestReject (templateId=3) into the supplied Artio FIX 4.4
   * QuoteRequestReject (35=AG) encoder. The caller must have wrapped {@code sbe} over a complete
   * decoded QuoteRequestReject message and must populate the FIX session header on {@code fix}
   * before calling {@code encode}.
   *
   * <p><b>FIX 4.4 compliance:</b> The {@code NoRelatedSym} repeating group (tag 146) is REQUIRED on
   * QuoteRequestReject. This method emits a single group entry containing the Symbol (tag 55) from
   * the Instrument component, with Side (tag 54) optional inside the group.
   *
   * <p><b>FIX tag mapping note:</b> The SBE field {@code quoteRejectReason} has SBE {@code
   * id="658"} matching FIX tag 658 (QuoteRequestRejectReason). The SBE enum values
   * (1=UnknownSymbol, 2=ExchangeClosed, 3=QuoteExceedsLimit, 4=TooLateToEnter, 5=InvalidPrice,
   * 99=Other) match the FIX tag 658 value set.
   *
   * @param sbe the SBE decoder positioned over a complete QuoteRequestReject message
   * @param fix the Artio FIX encoder — caller must populate the session header before {@code
   *     encode}
   */
  public void translateQuoteRequestReject(
      QuoteRequestRejectDecoder sbe, QuoteRequestRejectEncoder fix) {

    // quoteReqID (tag 131) — required
    sbe.getQuoteReqId(qrrQuoteReqId, 0);
    fix.quoteReqID(qrrQuoteReqId, 0, trimNulls(qrrQuoteReqId));

    // quoteRequestRejectReason (tag 658) — required. SBE enum value maps directly to FIX int.
    fix.quoteRequestRejectReason(mapQuoteRejectReason(sbe.quoteRejectReason()));

    // NoRelatedSym repeating group (tag 146) — REQUIRED by FIX 4.4 QuoteRequestReject.
    // Emit 1 entry containing Symbol (via Instrument component) and optional Side.
    final QuoteRequestRejectEncoder.RelatedSymGroupEncoder relSym = fix.relatedSymGroup(1);

    // Symbol (tag 55) — required inside NoRelatedSym.Instrument
    sbe.getSymbol(qrrSymbol, 0);
    relSym.instrument().symbol(qrrSymbol, 0, trimNulls(qrrSymbol));

    // Side (tag 54) — optional inside NoRelatedSym
    if (sbe.side() != SideEnum.NULL_VAL) {
      relSym.side(mapSide(sbe.side()));
    }

    // transactTime (tag 60) — optional per FIX 4.4 but always populated for client clarity.
    int tsLen = tsEnc.encodeFrom(sbe.transactTime(), TimeUnit.NANOSECONDS);
    System.arraycopy(tsEnc.buffer(), 0, qrrTransactTime, 0, tsLen);
    fix.transactTime(qrrTransactTime, 0, tsLen);

    // text (tag 58) — optional
    sbe.getText(qrrText, 0);
    int trimmed = trimNulls(qrrText);
    if (trimmed > 0) {
      fix.text(qrrText, 0, trimmed);
    }

    // productType (tag 10013) — APP-45 (custom tag not in stock FIX 4.4)
  }

  /**
   * Map SBE {@link QuoteRejectReasonEnum} to FIX tag 658 (QuoteRequestRejectReason) int value. The
   * SBE enum values align with the FIX tag 658 value set.
   *
   * @param sbe the SBE enum value
   * @return the FIX int representation for tag 658
   * @throws IllegalStateException if the enum value is NULL_VAL or unmapped
   */
  private static int mapQuoteRejectReason(QuoteRejectReasonEnum sbe) {
    return switch (sbe) {
      case UnknownSymbol -> 1;
      case ExchangeClosed -> 2;
      case QuoteExceedsLimit -> 3;
      case TooLateToEnter -> 4;
      case InvalidPrice -> 5;
      case Other -> 99;
      default ->
          throw new IllegalStateException("Unsupported SBE QuoteRejectReason for FIX wire: " + sbe);
    };
  }

  // ---------------------------------------------------------------------------
  // OrderCreatedEvent → ExecutionReport (35=8, ExecType=New)
  // ---------------------------------------------------------------------------

  /**
   * Translate an SBE OrderCreatedEvent (template 100) into a FIX 4.4 ExecutionReport with
   * ExecType=New ('0') and OrdStatus=New ('0'). The cluster emits this domain event when a
   * NewOrderSingle is accepted into the order book. The FIX mapping synthesises the required ER
   * fields that do not exist on the domain event (LeavesQty=orderQty, CumQty=0, AvgPx=0).
   *
   * @param sbe the decoder positioned over a complete OrderCreatedEvent message
   * @param fix the Artio encoder — caller must populate the FIX session header before {@code
   *     encode}
   */
  public void translateOrderCreatedEvent(OrderCreatedEventDecoder sbe, ExecutionReportEncoder fix) {
    // orderId (tag 37) — required
    sbe.getOrderId(ocOrderId, 0);
    fix.orderID(ocOrderId, 0, trimNulls(ocOrderId));

    // execId (tag 17) — required
    sbe.getExecId(ocExecId, 0);
    fix.execID(ocExecId, 0, trimNulls(ocExecId));

    // clOrdId (tag 11) — required
    sbe.getClOrdId(ocClOrdId, 0);
    fix.clOrdID(ocClOrdId, 0, trimNulls(ocClOrdId));

    // execType (tag 150) — New ('0')
    fix.execType('0');

    // ordStatus (tag 39) — New ('0')
    fix.ordStatus('0');

    // symbol (tag 55) — required
    sbe.getSymbol(ocSymbol, 0);
    fix.instrument().symbol(ocSymbol, 0, trimNulls(ocSymbol));

    // side (tag 54) — required
    fix.side(mapSide(sbe.side()));

    // ordType (tag 40) — echo back the order type from the domain event
    fix.ordType(mapOrdType(sbe.ordType()));

    // price (tag 44) — optional. Limit orders have a price; market orders use null sentinel.
    long price = sbe.price();
    if (price != OrderCreatedEventDecoder.priceNullValue()) {
      FixedPoint.toDecimalFloat(price, dec);
      fix.price(dec);
    }

    // orderQty (tag 38) — required, via OrderQtyData component
    FixedPoint.toDecimalFloat(sbe.orderQty(), dec);
    fix.orderQtyData().orderQty(dec);

    // leavesQty (tag 151) — for a New ack, leavesQty = orderQty (nothing filled yet)
    FixedPoint.toDecimalFloat(sbe.orderQty(), dec);
    fix.leavesQty(dec);

    // cumQty (tag 14) — zero for a New ack
    FixedPoint.toDecimalFloat(0L, dec);
    fix.cumQty(dec);

    // avgPx (tag 6) — zero for a New ack (no fills yet). Required by FIX 4.4 ER.
    FixedPoint.toDecimalFloat(0L, dec);
    fix.avgPx(dec);

    // transactTime (tag 60) — required. SBE uint64 epoch nanos → FIX UTC timestamp ASCII.
    int tsLen = tsEnc.encodeFrom(sbe.timestamp(), TimeUnit.NANOSECONDS);
    System.arraycopy(tsEnc.buffer(), 0, ocTransactTime, 0, tsLen);
    fix.transactTime(ocTransactTime, 0, tsLen);

    // timeInForce (tag 59) — optional
    if (sbe.timeInForce() != TimeInForceEnum.NULL_VAL) {
      fix.timeInForce(mapTimeInForce(sbe.timeInForce()));
    }

    // settlType (tag 63) — optional
    if (sbe.settlType() != SettlTypeEnum.NULL_VAL) {
      fix.settlType(mapSettlTypeToFix(sbe.settlType()));
    }

    // settlDate (tag 64) — optional
    sbe.getSettlDate(ocSettlDate, 0);
    int trimmed = trimNulls(ocSettlDate);
    if (trimmed > 0) {
      fix.settlDate(ocSettlDate, 0, trimmed);
    }

    // currency (tag 15) — optional
    sbe.getCurrency(ocCurrency, 0);
    trimmed = trimNulls(ocCurrency);
    if (trimmed > 0) {
      fix.currency(ocCurrency, 0, trimmed);
    }

    // settlCurrency (tag 120) — optional
    sbe.getSettlCurrency(ocSettlCurrency, 0);
    trimmed = trimNulls(ocSettlCurrency);
    if (trimmed > 0) {
      fix.settlCurrency(ocSettlCurrency, 0, trimmed);
    }

    // account (tag 1) — optional
    sbe.getAccountCode(ocAccountCode, 0);
    trimmed = trimNulls(ocAccountCode);
    if (trimmed > 0) {
      fix.account(ocAccountCode, 0, trimmed);
    }

    // productType / tenor — APP-45 (custom tags 10013/10001 not in stock FIX 4.4)
  }

  // ---------------------------------------------------------------------------
  // OrderRejectedEvent → ExecutionReport (35=8, ExecType=Rejected)
  // ---------------------------------------------------------------------------

  /**
   * Translate an SBE OrderRejectedEvent (template 101) into a FIX 4.4 ExecutionReport with
   * ExecType=Rejected ('8') and OrdStatus=Rejected ('8'). The cluster emits this domain event when
   * a NewOrderSingle fails validation. OrderID and ExecID are zero-filled (the order was never
   * assigned an engine ID). The reject reason is mapped to FIX OrdRejReason (tag 103) and the
   * human-readable reason text goes to tag 58.
   *
   * @param sbe the decoder positioned over a complete OrderRejectedEvent message
   * @param fix the Artio encoder — caller must populate the FIX session header before {@code
   *     encode}
   */
  public void translateOrderRejectedEvent(
      OrderRejectedEventDecoder sbe, ExecutionReportEncoder fix) {
    // orderID (tag 37) — required but not assigned for a rejection. FIX 4.4 requires the field
    // to be present; use "NONE" as a sentinel per industry convention (CME iLink uses "0").
    fix.orderID(NONE_ORDER_ID, 0, NONE_ORDER_ID.length);

    // execID (tag 17) — required but not assigned for a rejection; same sentinel.
    fix.execID(NONE_EXEC_ID, 0, NONE_EXEC_ID.length);

    // clOrdId (tag 11) — required
    sbe.getClOrdId(orClOrdId, 0);
    fix.clOrdID(orClOrdId, 0, trimNulls(orClOrdId));

    // execType (tag 150) — Rejected ('8')
    fix.execType('8');

    // ordStatus (tag 39) — Rejected ('8')
    fix.ordStatus('8');

    // symbol (tag 55) — required
    sbe.getSymbol(orSymbol, 0);
    fix.instrument().symbol(orSymbol, 0, trimNulls(orSymbol));

    // side (tag 54) — required
    fix.side(mapSide(sbe.side()));

    // leavesQty (tag 151) — 0 for a rejection (no open quantity)
    FixedPoint.toDecimalFloat(0L, dec);
    fix.leavesQty(dec);

    // cumQty (tag 14) — 0 for a rejection
    FixedPoint.toDecimalFloat(0L, dec);
    fix.cumQty(dec);

    // avgPx (tag 6) — 0 for a rejection. Required by FIX 4.4 ER.
    FixedPoint.toDecimalFloat(0L, dec);
    fix.avgPx(dec);

    // ordRejReason (tag 103) — map domain RejectReasonEnum to FIX int
    if (sbe.rejectReason() != RejectReasonEnum.NULL_VAL) {
      fix.ordRejReason(mapRejectReason(sbe.rejectReason()));
    }

    // text (tag 58) — optional, human-readable rejection reason
    sbe.getText(orText, 0);
    int trimmed = trimNulls(orText);
    if (trimmed > 0) {
      fix.text(orText, 0, trimmed);
    }

    // transactTime (tag 60) — required
    int tsLen = tsEnc.encodeFrom(sbe.timestamp(), TimeUnit.NANOSECONDS);
    System.arraycopy(tsEnc.buffer(), 0, orTransactTime, 0, tsLen);
    fix.transactTime(orTransactTime, 0, tsLen);

    // currency (tag 15) — optional
    sbe.getCurrency(orCurrency, 0);
    trimmed = trimNulls(orCurrency);
    if (trimmed > 0) {
      fix.currency(orCurrency, 0, trimmed);
    }

    // account (tag 1) — optional
    sbe.getAccountCode(orAccountCode, 0);
    trimmed = trimNulls(orAccountCode);
    if (trimmed > 0) {
      fix.account(orAccountCode, 0, trimmed);
    }
  }

  // ---------------------------------------------------------------------------
  // OrderCanceledEvent → ExecutionReport (35=8, ExecType=Canceled) — APP-151 phase 2
  // ---------------------------------------------------------------------------

  /**
   * Translate an SBE {@code OrderCanceledEvent} (template 103) into a FIX 4.4 ExecutionReport with
   * {@code ExecType=Canceled('4')} and {@code OrdStatus=Canceled('4')}. The cluster emits this
   * domain event from session-disconnect orphan cancel (APP-151 phase 1) and idle-session timeout
   * (APP-151 phase 4); future cancel triggers (operator force-cancel via APP-153, explicit FIX 35=F
   * via APP-65) will reuse this same path.
   *
   * <p><b>FIX field mapping.</b>
   *
   * <ul>
   *   <li>OrderID (37) ← event's orderId
   *   <li>ExecID (17) ← event's execId (APP-151 phase 3 — cluster mints a real one per cancel via
   *       the same id-generator as OrderCreated)
   *   <li>ClOrdID (11) ← event's clOrdId
   *   <li>OrigClOrdID (41) ← event's origClOrdId (server-initiated cancel paths echo clOrdId per
   *       industry convention; APP-65 explicit cancel will set the original-id properly)
   *   <li>ExecType (150) ← '4' (Canceled)
   *   <li>OrdStatus (39) ← '4' (Canceled)
   *   <li>Symbol (55) ← event's symbol
   *   <li>Side (54) ← event's side mapped via {@link #mapSide}
   *   <li>LeavesQty (151) ← 0 (per FIX 4.4 — a canceled order has no open quantity)
   *   <li>CumQty (14) ← {@code sbe.cumQty()} (APP-151 phase 3 — schema added the field; today the
   *       value is 0 for session-disconnect / idle-timeout cancels of unfilled orders, but the wire
   *       is correct for partial-filled cancels once phase-4+ work enables that path)
   *   <li>AvgPx (6) ← 0 — phase-3 limitation: cluster does not retain avg-fill price on {@code
   *       OrderState} yet. Tracking lands alongside partial-fill cancel support.
   *   <li>TransactTime (60) ← event's timestamp (cluster epoch nanos → FIX UTC timestamp ASCII)
   *   <li>Text (58) ← human-readable cancel reason mapped from {@code sbe.cancelReason()} via
   *       {@link #mapCancelReasonToText} (APP-151 phase 3)
   * </ul>
   *
   * <p><b>ProductType.</b> Cluster populates {@code productType} from {@link OrderState} (APP-151
   * phase 3). FIX 4.4 has no stock ProductType tag, so this is informational only and not emitted
   * on the wire — APP-45 will add the custom tag if/when required by downstream consumers.
   *
   * @param sbe the decoder positioned over a complete OrderCanceledEvent message
   * @param fix the Artio encoder — caller must populate the FIX session header before {@code
   *     encode}
   * @throws IllegalStateException if {@code sbe.side()} is {@code NULL_VAL} or otherwise unmapped
   *     (per the class-level "Unmapped enum values throw" contract)
   */
  public void translateOrderCanceledEvent(
      OrderCanceledEventDecoder sbe, ExecutionReportEncoder fix) {
    // orderId (tag 37) — required
    sbe.getOrderId(oxlOrderId, 0);
    fix.orderID(oxlOrderId, 0, trimNulls(oxlOrderId));

    // execId (tag 17) — APP-151 phase 3: cluster now mints a real execId on every cancel via the
    // same id-generator as OrderCreated, so the gateway reads it straight from the event (drop
    // the phase-2 "CXL-" synthesis).
    sbe.getExecId(oxlExecId, 0);
    fix.execID(oxlExecId, 0, trimNulls(oxlExecId));

    // clOrdId (tag 11) — required
    sbe.getClOrdId(oxlClOrdId, 0);
    fix.clOrdID(oxlClOrdId, 0, trimNulls(oxlClOrdId));

    // origClOrdId (tag 41) — optional but standard on cancel ER
    sbe.getOrigClOrdId(oxlOrigClOrdId, 0);
    final int origClOrdIdLen = trimNulls(oxlOrigClOrdId);
    if (origClOrdIdLen > 0) {
      fix.origClOrdID(oxlOrigClOrdId, 0, origClOrdIdLen);
    }

    // execType (tag 150) — Canceled ('4')
    fix.execType('4');

    // ordStatus (tag 39) — Canceled ('4')
    fix.ordStatus('4');

    // symbol (tag 55) — required
    sbe.getSymbol(oxlSymbol, 0);
    fix.instrument().symbol(oxlSymbol, 0, trimNulls(oxlSymbol));

    // side (tag 54) — required
    fix.side(mapSide(sbe.side()));

    // leavesQty (tag 151) = 0 (canceled order has no open qty).
    // cumQty (tag 14) — APP-151 phase 3: cluster now carries cumQty on the cancel event, so emit
    // the real value instead of forcing 0. Non-zero only when phase 4+ enables cancel of
    // partial-filled orders; today the value is 0 for session-disconnect / idle-timeout cancels
    // of unfilled orders, but the wire is now correct for the partial-filled case as soon as
    // cluster-side supports it.
    // avgPx (tag 6) — still 0 in this slice (cluster does not retain avg-fill price on
    // OrderState; tracking that is phase 4+ work, paired with partial-fill cancels).
    //
    // Sequential reuse of {@code dec} across the three setters is safe: Artio's encoder
    // {@code xxx(ReadOnlyDecimalFloat)} setters COPY the value (via a per-field
    // {@code DecimalFloat.set(value, scale)}) at setter-call time, NOT by capturing a reference
    // resolved at encode() time. So we can rewrite {@code dec} between calls and each tag retains
    // its distinct value. (Verified by {@code translateOrderCanceledEvent_cumQty_propagatesToFix14}
    // which sets three different values and reads three different tags off the wire.)
    FixedPoint.toDecimalFloat(0L, dec);
    fix.leavesQty(dec);
    FixedPoint.toDecimalFloat(sbe.cumQty(), dec);
    fix.cumQty(dec);
    FixedPoint.toDecimalFloat(0L, dec);
    fix.avgPx(dec);

    // transactTime (tag 60) — required
    final int tsLen = tsEnc.encodeFrom(sbe.timestamp(), TimeUnit.NANOSECONDS);
    System.arraycopy(tsEnc.buffer(), 0, oxlTransactTime, 0, tsLen);
    fix.transactTime(oxlTransactTime, 0, tsLen);

    // text (tag 58) — APP-151 phase 3: human-readable cancel reason. Translates the SBE
    // CancelReasonEnum into FIX-side ASCII text. FIX 4.4 has no stock OrdRejReason-style tag
    // for cancel reasons (tag 102 is OrderCancelRejectReason on 35=9, not 35=8), so the
    // industry convention is to surface the reason via Text(58).
    final byte[] reasonText = mapCancelReasonToText(sbe.cancelReason());
    if (reasonText != null) {
      fix.text(reasonText, 0, reasonText.length);
    }
  }

  // ---------------------------------------------------------------------------
  // OrderExpiredEvent → ExecutionReport (35=8, ExecType=Expired) — APP-62 §J
  // ---------------------------------------------------------------------------

  /**
   * Translate an SBE {@code OrderExpiredEvent} (template 121, APP-62 §J) into a FIX 4.4
   * ExecutionReport with {@code ExecType=Expired('C', tag 150)} and {@code OrdStatus=Expired('C',
   * tag 39)}. The cluster emits this domain event for idle-session timeouts (today's {@link
   * NewOrderSingleHandler#onIdleScan} hook) and — once landed — TIF-driven expiries; both deserve
   * the FIX 4.4 semantic distinction from ExecType=Canceled ('4') per the spec's §4.5 "ExecType —
   * Execution Type" table.
   *
   * <p><b>FIX field mapping.</b>
   *
   * <ul>
   *   <li>OrderID (37) ← event's orderId
   *   <li>ExecID (17) ← event's execId (cluster mints a real one per expire via the same
   *       id-generator as OrderCreated / OrderCanceled — FIX 4.4 §4.4.5 ExecID uniqueness)
   *   <li>ClOrdID (11) ← event's clOrdId
   *   <li>OrigClOrdID (41) ← NOT emitted; FIX OrigClOrdID is for cancel-request paths only and the
   *       OrderExpiredEvent schema deliberately omits the field
   *   <li>ExecType (150) ← 'C' (Expired) — FIX 4.4 spec
   *   <li>OrdStatus (39) ← 'C' (Expired) — FIX 4.4 spec
   *   <li>Symbol (55) ← event's symbol
   *   <li>Side (54) ← event's side mapped via {@link #mapSide}
   *   <li>LeavesQty (151) ← 0 (per FIX 4.4 — an expired order has no open quantity)
   *   <li>CumQty (14) ← {@code sbe.cumQty()} (today's idle-timeout cancel-of-unfilled paths emit 0;
   *       partial-filled expires would carry the real value once that path lands)
   *   <li>AvgPx (6) ← 0 (cluster does not retain avg-fill price on {@code OrderState} yet; tracking
   *       lands alongside partial-fill cancel/expire work)
   *   <li>TransactTime (60) ← event's timestamp (cluster epoch nanos → FIX UTC timestamp ASCII)
   *   <li>Text (58) ← human-readable expire reason mapped from {@code sbe.expireReason()} via
   *       {@link #mapExpireReasonToText}. FIX 4.4 has no dedicated ExpireReason tag (only
   *       OrdRejReason 103 on rejected ERs), so industry convention is to carry the discriminator
   *       via Text(58) — same pattern as {@link #translateOrderCanceledEvent}.
   * </ul>
   *
   * <p><b>ProductType.</b> Cluster populates {@code productType} from {@link OrderState}. FIX 4.4
   * has no stock ProductType tag, so this is informational only and not emitted on the wire —
   * future ticket will add a custom tag if/when required by downstream consumers.
   *
   * @param sbe the decoder positioned over a complete OrderExpiredEvent message
   * @param fix the Artio encoder — caller must populate the FIX session header before {@code
   *     encode}
   * @throws IllegalStateException if {@code sbe.side()} is {@code NULL_VAL} or otherwise unmapped
   *     (per the class-level "Unmapped enum values throw" contract)
   */
  public void translateOrderExpiredEvent(OrderExpiredEventDecoder sbe, ExecutionReportEncoder fix) {
    // orderId (tag 37) — required
    sbe.getOrderId(oxpOrderId, 0);
    fix.orderID(oxpOrderId, 0, trimNulls(oxpOrderId));

    // execId (tag 17) — cluster-minted per APP-62 §J emit path (mirrors APP-151 phase 3 cancel).
    sbe.getExecId(oxpExecId, 0);
    fix.execID(oxpExecId, 0, trimNulls(oxpExecId));

    // clOrdId (tag 11) — required
    sbe.getClOrdId(oxpClOrdId, 0);
    fix.clOrdID(oxpClOrdId, 0, trimNulls(oxpClOrdId));

    // OrigClOrdID (tag 41) — NOT emitted: the OrderExpiredEvent schema omits origClOrdId because
    // expiries are server-initiated and have no paired counterparty cancel request to echo.

    // execType (tag 150) — Expired ('C') per FIX 4.4 §4.5.
    fix.execType('C');

    // ordStatus (tag 39) — Expired ('C') per FIX 4.4 §4.5.
    fix.ordStatus('C');

    // symbol (tag 55) — required
    sbe.getSymbol(oxpSymbol, 0);
    fix.instrument().symbol(oxpSymbol, 0, trimNulls(oxpSymbol));

    // side (tag 54) — required
    fix.side(mapSide(sbe.side()));

    // leavesQty (tag 151) = 0, cumQty (tag 14) from event, avgPx (tag 6) = 0.
    // Sequential reuse of {@code dec} across the three setters is safe — see same comment block
    // on translateOrderCanceledEvent: Artio's encoder setters COPY the DecimalFloat value at
    // setter-call time, not by reference resolved at encode().
    FixedPoint.toDecimalFloat(0L, dec);
    fix.leavesQty(dec);
    FixedPoint.toDecimalFloat(sbe.cumQty(), dec);
    fix.cumQty(dec);
    FixedPoint.toDecimalFloat(0L, dec);
    fix.avgPx(dec);

    // transactTime (tag 60) — required
    // Primitive local bare (no `final`) per memory rule feedback_final_primitives_autoboxing.md.
    int tsLen = tsEnc.encodeFrom(sbe.timestamp(), TimeUnit.NANOSECONDS);
    System.arraycopy(tsEnc.buffer(), 0, oxpTransactTime, 0, tsLen);
    fix.transactTime(oxpTransactTime, 0, tsLen);

    // text (tag 58) — human-readable expire reason. FIX 4.4 has no dedicated ExpireReason tag,
    // so the discriminator is surfaced via Text(58) following the same convention as the cancel
    // path (mapCancelReasonToText). null → tag 58 omitted (FIX 4.4 Text is optional).
    final byte[] reasonText = mapExpireReasonToText(sbe.expireReason());
    if (reasonText != null) {
      fix.text(reasonText, 0, reasonText.length);
    }
  }

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  /**
   * Sentinel OrderID for rejected orders that were never assigned an engine-side order ID. FIX 4.4
   * requires tag 37 to be present on every ExecutionReport; "NONE" follows the CME iLink convention
   * of using a placeholder when no real ID exists.
   */
  private static final byte[] NONE_ORDER_ID = "NONE".getBytes(StandardCharsets.US_ASCII);

  /**
   * Sentinel ExecID for rejected orders that were never assigned an execution ID. Same rationale as
   * {@link #NONE_ORDER_ID}.
   */
  private static final byte[] NONE_EXEC_ID = "NONE".getBytes(StandardCharsets.US_ASCII);

  // CANCEL_EXEC_ID_PREFIX removed in APP-151 phase 3 — cluster now mints a real execId on every
  // OrderCanceledEvent (template 103), so the gateway reads it from the wire instead of
  // synthesising a "CXL-"+clOrdId sentinel.

  /**
   * ASCII text for FIX Text(58) on a session-disconnect cancel ExecutionReport. APP-151 phase 3
   * maps {@link CancelReasonEnum} → human-readable text via {@link #mapCancelReasonToText}.
   */
  private static final byte[] CANCEL_TEXT_SESSION_DISCONNECT =
      "Cancelled: session disconnected".getBytes(StandardCharsets.US_ASCII);

  /** ASCII text for cancels initiated by an explicit FIX 35=F (APP-65 future work). */
  private static final byte[] CANCEL_TEXT_EXPLICIT =
      "Cancelled: explicit request".getBytes(StandardCharsets.US_ASCII);

  /** ASCII text for cancels triggered by the idle-session timer (APP-151 phase 4). */
  private static final byte[] CANCEL_TEXT_IDLE_TIMEOUT =
      "Cancelled: idle session timeout".getBytes(StandardCharsets.US_ASCII);

  /** ASCII text for operator-initiated force cancels (APP-153 future work). */
  private static final byte[] CANCEL_TEXT_OPERATOR_FORCE =
      "Cancelled: operator force".getBytes(StandardCharsets.US_ASCII);

  /**
   * Map a {@link CancelReasonEnum} value to its human-readable FIX Text(58) ASCII bytes. Returns
   * {@code null} for {@link CancelReasonEnum#NULL_VAL} or for a {@code null} input so the caller
   * can skip the tag (FIX 4.4 Text is optional). The null-input branch is defensive — SBE decoders
   * never produce a {@code null} enum on the wire — but it keeps the switch total and prevents an
   * NPE from leaking up the egress translator if a flyweight is misused.
   *
   * @param reason the cancel reason from the decoder (may be null)
   * @return ASCII text bytes, or {@code null} if {@code reason} is {@code null} or {@code NULL_VAL}
   */
  private static byte[] mapCancelReasonToText(final CancelReasonEnum reason) {
    if (reason == null) {
      return null;
    }
    return switch (reason) {
      case SessionDisconnect -> CANCEL_TEXT_SESSION_DISCONNECT;
      case ExplicitCancel -> CANCEL_TEXT_EXPLICIT;
      case IdleTimeout -> CANCEL_TEXT_IDLE_TIMEOUT;
      case OperatorForce -> CANCEL_TEXT_OPERATOR_FORCE;
      case NULL_VAL -> null;
    };
  }

  /**
   * ASCII text for FIX Text(58) on an idle-timeout expire ExecutionReport. APP-62 §J — the
   * idle-timeout reaper now routes through {@code OrderExpiredEvent} (template 121) so the wire
   * form is ExecType=Expired ('C'); the surfaced text is distinct from the canceled-path
   * "Cancelled: idle session timeout" so downstream consumers can distinguish the two.
   */
  private static final byte[] EXPIRE_TEXT_IDLE_TIMEOUT =
      "Expired: idle session timeout".getBytes(StandardCharsets.US_ASCII);

  /** ASCII text for FIX Text(58) when a TIF window elapsed (reserved future emitter). */
  private static final byte[] EXPIRE_TEXT_TIME_IN_FORCE =
      "Expired: time in force elapsed".getBytes(StandardCharsets.US_ASCII);

  /**
   * Map an {@link ExpireReasonEnum} value to its human-readable FIX Text(58) ASCII bytes for the
   * APP-62 §J OrderExpiredEvent translation path. Returns {@code null} for {@link
   * ExpireReasonEnum#NULL_VAL} or a {@code null} input so the caller can skip the tag (FIX 4.4 Text
   * is optional). The null-input branch is defensive — SBE decoders never produce a {@code null}
   * enum on the wire — but it keeps the switch total and prevents an NPE from leaking up the egress
   * translator if a flyweight is misused.
   *
   * @param reason the expire reason from the decoder (may be null)
   * @return ASCII text bytes, or {@code null} if {@code reason} is {@code null} or {@code NULL_VAL}
   */
  private static byte[] mapExpireReasonToText(final ExpireReasonEnum reason) {
    if (reason == null) {
      return null;
    }
    return switch (reason) {
      case IdleTimeout -> EXPIRE_TEXT_IDLE_TIMEOUT;
      case TimeInForceExpired -> EXPIRE_TEXT_TIME_IN_FORCE;
      case NULL_VAL -> null;
    };
  }

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

  /**
   * Map SBE {@link TimeInForceEnum} to FIX 4.4 TimeInForce (tag 59) char value. The SBE enum's
   * short values were chosen to match the FIX wire char's numeric value (Day=0 → '0', GTC=1 → '1',
   * IOC=3 → '3', FOK=4 → '4'), so the mapping is a direct cast to char after adding '0'.
   */
  private static char mapTimeInForce(TimeInForceEnum sbe) {
    return switch (sbe) {
      case Day -> '0';
      case GTC -> '1';
      case IOC -> '3';
      case FOK -> '4';
      default ->
          throw new IllegalStateException("Unsupported SBE TimeInForce for FIX wire: " + sbe);
    };
  }

  /**
   * Map SBE {@link OrdTypeEnum} to FIX 4.4 OrdType (tag 40) char value. Market='1', Limit='2',
   * PreviouslyQuoted='D'.
   */
  private static char mapOrdType(OrdTypeEnum sbe) {
    return switch (sbe) {
      case Market -> '1';
      case Limit -> '2';
      case PreviouslyQuoted -> 'D';
      default -> throw new IllegalStateException("Unsupported SBE OrdType for FIX wire: " + sbe);
    };
  }

  /**
   * Map SBE {@link RejectReasonEnum} to FIX 4.4 OrdRejReason (tag 103) int value. The domain enum
   * carries fine-grained reasons (e.g., AccountSuspended, BookFull) that have no direct FIX
   * counterpart; those map to the closest standard FIX value or to {@code 99} (Other). The mapping
   * preserves enough information for downstream clients to distinguish the major categories
   * (unknown symbol, duplicate, account issues) while the full detail lives in tag 58 (Text).
   */
  private static int mapRejectReason(RejectReasonEnum sbe) {
    return switch (sbe) {
      case UnknownSymbol -> 1; // FIX: Unknown symbol
      case InsufficientQuantity -> 13; // FIX: Incorrect quantity
      case InvalidPrice -> 99; // FIX: Other (no standard "invalid price" code)
      case InvalidQuantity -> 13; // FIX: Incorrect quantity
      case DuplicateClOrdId -> 6; // FIX: Duplicate Order
      case QuoteNotFound -> 99; // FIX: Other
      case QuoteExpired -> 99; // FIX: Other
      case OrderNotFound -> 5; // FIX: Unknown order
      case BookFull -> 99; // FIX: Other
      case AccountNotFound -> 15; // FIX: Unknown account
      case AccountSuspended -> 99; // FIX: Other
      case AccountNoTradePermission -> 99; // FIX: Other
      case AccountNoQuotePermission -> 99; // FIX: Other
      case OrderExceedsMaxSize -> 3; // FIX: Order exceeds limit
      case TradingHalted -> 99; // FIX: Other (FIX 4.4 has no direct OrdRejReason for trading halt;
      // the operator's free-text reason is preserved in tag 58 Text)
      case RateLimitExceeded -> 8; // FIX: Broker / Exchange option — closest FIX 4.4 value for a
      // throttle-side rejection (no dedicated rate-limit code). Free-text in tag 58 carries the
      // human-readable detail.
      case DailyVolumeExceeded -> 3; // FIX: Order exceeds limit — same FIX code as
      // OrderExceedsMaxSize since both are "you tried to push more than the desk's risk-limit
      // allows" rejections at the cluster level. Free-text in tag 58 distinguishes the two for
      // ops triage.
      case DuplicateAccountCode -> 99; // FIX: Other
      case UnknownCurrency -> 99; // FIX: Other
      case InvalidCurrencyCode -> 99; // FIX: Other
      case InvalidAccountId -> 15; // FIX: Unknown account
      case InvalidLimitValue -> 99; // FIX: Other
      case PositionLimitExceeded -> 3; // APP-62 §4 — FIX: Order exceeds limit. CME PTRM "Long Qty"
      // / "Short Qty" breach. Tag 58 carries the limit + projected values for ops triage.
      case PriceTooFarFromMarket -> 99; // APP-62 §5 — FIX: Other. FIX 4.4 has no dedicated
      // "price band" code (FIX 5.0 SP2 added value 16); 99 chosen to avoid colliding with
      // RateLimitExceeded (=8) on tag-103 ops triage. Tag 58 carries deviationBps.
      case RiskLimitsNotLoaded -> 3; // APP-62 §E — FIX: Order exceeds limit. Account has no
      // RiskLimitRecord; fail-closed boot semantic. Tag 58 carries the accountId.
      case RegulatoryRestriction -> 99; // APP-62 §G — FIX: Other. Symbol-eligibility breach
      // (Reg SHO restricted-symbol subset). Tag 58 carries the restriction kind.
      case FourEyesViolation -> 99; // APP-62 §H — FIX: Other. MiFID II RTS 6 §1(2) dual-control
      // failure on a LoadRiskLimit ingress; never reaches an OrderRejectedEvent today (lives on
      // the reference-data reject path) — case is here for switch-exhaustiveness only.
      default ->
          throw new IllegalStateException("Unsupported SBE RejectReason for FIX wire: " + sbe);
    };
  }
}
