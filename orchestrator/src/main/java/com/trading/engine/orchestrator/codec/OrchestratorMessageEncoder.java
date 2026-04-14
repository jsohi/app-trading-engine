package com.trading.engine.orchestrator.codec;

import com.trading.engine.messages.sbe.ExecTypeEnum;
import com.trading.engine.messages.sbe.ExecutionReportEncoder;
import com.trading.engine.messages.sbe.MessageHeaderEncoder;
import com.trading.engine.messages.sbe.NewOrderSingleDecoder;
import com.trading.engine.messages.sbe.OrdStatusEnum;
import com.trading.engine.messages.sbe.PriceRequestEncoder;
import com.trading.engine.messages.sbe.ProductTypeEnum;
import com.trading.engine.messages.sbe.QuoteEncoder;
import com.trading.engine.messages.sbe.QuoteRejectReasonEnum;
import com.trading.engine.messages.sbe.QuoteRequestDecoder;
import com.trading.engine.messages.sbe.QuoteRequestRejectEncoder;
import com.trading.engine.messages.sbe.QuoteStatusEnum;
import com.trading.engine.messages.sbe.SettlTypeEnum;
import com.trading.engine.messages.sbe.SideEnum;
import com.trading.engine.messages.sbe.TenorEnum;
import com.trading.engine.orchestrator.RfqState;
import java.util.Arrays;
import org.agrona.MutableDirectBuffer;

/**
 * Zero-allocation SBE message encoder for outbound orchestrator messages. Encodes messages to the
 * gateway (Quote, QuoteRequestReject, ExecutionReport) and to the pricing service (PriceRequest,
 * PriceValidationRequest).
 *
 * <p><b>Encoding pattern.</b> Each {@code encode*} method uses the SBE {@code wrapAndApplyHeader}
 * pattern: the header encoder writes the SBE message header at the given offset, then the body
 * encoder writes fields. For messages with repeating groups (NoLegs), the group is encoded with
 * count=0 (APP-47: swap/multileg deferred). The method returns the total encoded length.
 *
 * <p><b>Char-field copying.</b> SBE's generated {@code putXxx(byte[], int)} setters require a
 * {@code byte[]} source. The encoder copies fields from the {@link RfqState} flat buffer or
 * decoders into the pre-allocated {@link #charScratch} byte array, then passes the scratch to the
 * SBE setter.
 *
 * <p><b>Text field convention.</b> The {@code text} parameter is a {@code byte[]} (pre-allocated
 * ASCII constants in the caller). The encoder null-pads the remainder of the 64-byte SBE Text field
 * via the {@link #textScratch} buffer.
 *
 * <p><b>Allocation:</b> zero allocation after construction. All SBE encoder flyweights and scratch
 * buffers are pre-allocated.
 *
 * <p><b>Threading:</b> not thread-safe — single-threaded orchestrator duty-cycle thread only.
 *
 * @see GatewayMessageDispatcher
 * @see PricingResponseDispatcher
 */
public final class OrchestratorMessageEncoder {

  /** Scratch buffer for char-field copying. Sized for largest field: QuoteReqID/QuoteID = 20. */
  private static final int CHAR_SCRATCH_LEN = 24;

  /** Text scratch sized to the SBE Text field length (64 bytes). */
  private static final int TEXT_SCRATCH_LEN = QuoteRequestRejectEncoder.textLength();

  // Static init: validate scratch sizes against all SBE char fields written by this encoder
  static {
    final int maxCharField =
        Math.max(
            PriceRequestEncoder.quoteReqIdLength(),
            Math.max(
                QuoteEncoder.quoteIdLength(),
                Math.max(QuoteEncoder.symbolLength(), ExecutionReportEncoder.orderIdLength())));
    if (maxCharField > CHAR_SCRATCH_LEN) {
      throw new IllegalStateException(
          "OrchestratorMessageEncoder CHAR_SCRATCH_LEN="
              + CHAR_SCRATCH_LEN
              + " too small for SBE char field "
              + maxCharField);
    }
  }

  // --- Pre-allocated scratch buffers ---
  private final byte[] charScratch = new byte[CHAR_SCRATCH_LEN];
  private final byte[] textScratch = new byte[TEXT_SCRATCH_LEN];

  /** 20-byte null-padded sentinel for orderId/execId on orchestrator-generated rejects. */
  private final byte[] nullSentinel20 = new byte[20];

  // --- Pre-allocated SBE flyweight encoders ---
  private final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
  private final com.trading.engine.messages.sbe.PriceRequestEncoder priceRequestEncoder =
      new com.trading.engine.messages.sbe.PriceRequestEncoder();
  private final com.trading.engine.messages.sbe.QuoteEncoder quoteEncoder =
      new com.trading.engine.messages.sbe.QuoteEncoder();
  private final com.trading.engine.messages.sbe.QuoteRequestRejectEncoder
      quoteRequestRejectEncoder = new com.trading.engine.messages.sbe.QuoteRequestRejectEncoder();
  private final com.trading.engine.messages.sbe.PriceValidationRequestEncoder
      priceValidationRequestEncoder =
          new com.trading.engine.messages.sbe.PriceValidationRequestEncoder();
  private final com.trading.engine.messages.sbe.ExecutionReportEncoder executionReportEncoder =
      new com.trading.engine.messages.sbe.ExecutionReportEncoder();

  /** Construct an encoder with pre-allocated flyweights and scratch buffers. */
  public OrchestratorMessageEncoder() {}

  // ===========================================================================
  // 1. PriceRequest (templateId=50) — orchestrator → pricing
  // ===========================================================================

  /**
   * Encode a PriceRequest (templateId=50) transcribed from an RfqState (originally populated from a
   * QuoteRequest). The transactTime is passed through from the client's QuoteRequest for audit
   * trail purposes; the pricing service does not use it for staleness computation (it uses
   * nanoClock.nanoTime() instead).
   *
   * @param dst target buffer
   * @param offset byte offset to start encoding
   * @param state the RfqState in PENDING_PRICE state
   * @return total encoded length (header + body + empty noLegs group)
   */
  public int encodePriceRequest(
      final MutableDirectBuffer dst, final int offset, final RfqState state) {

    priceRequestEncoder.wrapAndApplyHeader(dst, offset, headerEncoder);

    state.putQuoteReqIdInto(charScratch, 0);
    priceRequestEncoder.putQuoteReqId(charScratch, 0);

    state.putSymbolInto(charScratch, 0);
    priceRequestEncoder.putSymbol(charScratch, 0);

    priceRequestEncoder.side(SideEnum.get(state.sideRaw()));
    priceRequestEncoder.orderQty(state.orderQty());

    state.putAccountCodeInto(charScratch, 0);
    priceRequestEncoder.putAccountCode(charScratch, 0);

    priceRequestEncoder.transactTime(state.transactTime());
    priceRequestEncoder.productType(ProductTypeEnum.get(state.productTypeRaw()));

    state.putSettlDateInto(charScratch, 0);
    priceRequestEncoder.putSettlDate(charScratch, 0);

    priceRequestEncoder.settlType(SettlTypeEnum.get(state.settlTypeRaw()));

    state.putCurrencyInto(charScratch, 0);
    priceRequestEncoder.putCurrency(charScratch, 0);

    state.putSettlCurrencyInto(charScratch, 0);
    priceRequestEncoder.putSettlCurrency(charScratch, 0);

    priceRequestEncoder.tenor(TenorEnum.get(state.tenorRaw()));

    // NoLegs: empty group (APP-47: swap/multileg support deferred)
    priceRequestEncoder.noLegsCount(0);

    return MessageHeaderEncoder.ENCODED_LENGTH + priceRequestEncoder.encodedLength();
  }

  /**
   * Encode a PriceRequest (templateId=50) directly from a {@link QuoteRequestDecoder}. Used in the
   * publish-before-mutate path of {@code onQuoteRequest}: the PriceRequest is encoded and published
   * BEFORE the pool slot is acquired, so there is no RfqState yet. All fields are read directly
   * from the decoder.
   *
   * @param dst target buffer
   * @param offset byte offset to start encoding
   * @param decoder the pre-wrapped QuoteRequest decoder — must not be retained past this call
   * @return total encoded length (header + body + empty noLegs group)
   */
  public int encodePriceRequest(
      final MutableDirectBuffer dst, final int offset, final QuoteRequestDecoder decoder) {

    priceRequestEncoder.wrapAndApplyHeader(dst, offset, headerEncoder);

    decoder.getQuoteReqId(charScratch, 0);
    priceRequestEncoder.putQuoteReqId(charScratch, 0);

    decoder.getSymbol(charScratch, 0);
    priceRequestEncoder.putSymbol(charScratch, 0);

    priceRequestEncoder.side(decoder.side());
    priceRequestEncoder.orderQty(decoder.orderQty());

    decoder.getAccountCode(charScratch, 0);
    priceRequestEncoder.putAccountCode(charScratch, 0);

    priceRequestEncoder.transactTime(decoder.transactTime());
    priceRequestEncoder.productType(decoder.productType());

    decoder.getSettlDate(charScratch, 0);
    priceRequestEncoder.putSettlDate(charScratch, 0);

    priceRequestEncoder.settlType(decoder.settlType());

    decoder.getCurrency(charScratch, 0);
    priceRequestEncoder.putCurrency(charScratch, 0);

    decoder.getSettlCurrency(charScratch, 0);
    priceRequestEncoder.putSettlCurrency(charScratch, 0);

    priceRequestEncoder.tenor(decoder.tenor());

    // NoLegs: empty group (APP-47: swap/multileg support deferred)
    priceRequestEncoder.noLegsCount(0);

    return MessageHeaderEncoder.ENCODED_LENGTH + priceRequestEncoder.encodedLength();
  }

  // ===========================================================================
  // 2. Quote (templateId=2) — orchestrator → gateway
  // ===========================================================================

  /**
   * Encode a Quote (templateId=2) from RfqState identity fields + pricing data.
   *
   * <p>The quoteId is passed as a parameter (NOT read from RfqState) because in the
   * publish-before-mutate pattern, the quoteId is generated before encoding but stored in RfqState
   * only after successful publication.
   *
   * <p>quoteStatus is set to {@link QuoteStatusEnum#Accepted} — FIX tag 297=0: the market maker
   * accepts the RFQ and provides a quote (not the client accepting; that is a NOS with quoteId).
   *
   * @param dst target buffer
   * @param offset byte offset to start encoding
   * @param state the RfqState with pricing data applied
   * @param quoteId generated quoteId bytes (from OrchestratorIdGenerator scratch)
   * @param quoteIdOffset offset into quoteId bytes
   * @param quoteIdLen length of quoteId bytes
   * @param transactTimeNanos epoch nanos from EpochNanoClock
   * @return total encoded length
   */
  public int encodeQuote(
      final MutableDirectBuffer dst,
      final int offset,
      final RfqState state,
      final byte[] quoteId,
      final int quoteIdOffset,
      final int quoteIdLen,
      final long transactTimeNanos) {

    quoteEncoder.wrapAndApplyHeader(dst, offset, headerEncoder);

    state.putQuoteReqIdInto(charScratch, 0);
    quoteEncoder.putQuoteReqId(charScratch, 0);

    // quoteId from parameter, null-pad remainder
    Arrays.fill(charScratch, 0, CHAR_SCRATCH_LEN, (byte) 0);
    System.arraycopy(quoteId, quoteIdOffset, charScratch, 0, quoteIdLen);
    quoteEncoder.putQuoteId(charScratch, 0);

    state.putSymbolInto(charScratch, 0);
    quoteEncoder.putSymbol(charScratch, 0);

    quoteEncoder.side(SideEnum.get(state.sideRaw()));
    quoteEncoder.bidPx(state.bidPx());
    quoteEncoder.offerPx(state.offerPx());
    quoteEncoder.bidSize(state.bidSize());
    quoteEncoder.offerSize(state.offerSize());
    quoteEncoder.transactTime(transactTimeNanos);
    quoteEncoder.quoteStatus(QuoteStatusEnum.Accepted);

    // text: empty (null-padded 64 bytes)
    Arrays.fill(textScratch, (byte) 0);
    quoteEncoder.putText(textScratch, 0);

    quoteEncoder.productType(ProductTypeEnum.get(state.productTypeRaw()));

    state.putSettlDateInto(charScratch, 0);
    quoteEncoder.putSettlDate(charScratch, 0);

    quoteEncoder.settlType(SettlTypeEnum.get(state.settlTypeRaw()));

    state.putCurrencyInto(charScratch, 0);
    quoteEncoder.putCurrency(charScratch, 0);

    state.putSettlCurrencyInto(charScratch, 0);
    quoteEncoder.putSettlCurrency(charScratch, 0);

    quoteEncoder.tenor(TenorEnum.get(state.tenorRaw()));
    quoteEncoder.validUntil(state.validUntil());
    quoteEncoder.swapPoints(state.swapPoints());

    // NoLegs: empty group (APP-47)
    quoteEncoder.noLegsCount(0);

    return MessageHeaderEncoder.ENCODED_LENGTH + quoteEncoder.encodedLength();
  }

  // ===========================================================================
  // 3a. QuoteRequestReject (templateId=3) — from RfqState
  // ===========================================================================

  /**
   * Encode a QuoteRequestReject (templateId=3) from RfqState identity fields. Used for:
   * pricing-rejected, reap-expiry, accepted-null-prices paths.
   *
   * @param dst target buffer
   * @param offset byte offset to start encoding
   * @param state the RfqState with identity fields
   * @param reason the rejection reason
   * @param text reject text bytes (pre-allocated ASCII constant)
   * @param textLen number of meaningful bytes in text
   * @param transactTimeNanos epoch nanos from EpochNanoClock
   * @return total encoded length
   */
  public int encodeQuoteRequestReject(
      final MutableDirectBuffer dst,
      final int offset,
      final RfqState state,
      final QuoteRejectReasonEnum reason,
      final byte[] text,
      final int textLen,
      final long transactTimeNanos) {

    quoteRequestRejectEncoder.wrapAndApplyHeader(dst, offset, headerEncoder);

    state.putQuoteReqIdInto(charScratch, 0);
    quoteRequestRejectEncoder.putQuoteReqId(charScratch, 0);

    quoteRequestRejectEncoder.quoteRejectReason(reason);

    state.putSymbolInto(charScratch, 0);
    quoteRequestRejectEncoder.putSymbol(charScratch, 0);

    quoteRequestRejectEncoder.side(SideEnum.get(state.sideRaw()));
    quoteRequestRejectEncoder.transactTime(transactTimeNanos);

    writeText(text, textLen);
    quoteRequestRejectEncoder.putText(textScratch, 0);

    quoteRequestRejectEncoder.productType(ProductTypeEnum.get(state.productTypeRaw()));

    return MessageHeaderEncoder.ENCODED_LENGTH + quoteRequestRejectEncoder.encodedLength();
  }

  // ===========================================================================
  // 3b. QuoteRequestReject (templateId=3) — from QuoteRequestDecoder (no RfqState)
  // ===========================================================================

  /**
   * Encode a QuoteRequestReject (templateId=3) directly from a QuoteRequestDecoder. Used for:
   * validation-failure (before pool acquisition), quoteReqId collision, pool-full paths where no
   * RfqState is populated. The reject echoes the NEW request's fields.
   *
   * @param dst target buffer
   * @param offset byte offset to start encoding
   * @param decoder the pre-wrapped QuoteRequest decoder
   * @param reason the rejection reason
   * @param text reject text bytes
   * @param textLen number of meaningful bytes in text
   * @param transactTimeNanos epoch nanos from EpochNanoClock
   * @return total encoded length
   */
  public int encodeQuoteRequestReject(
      final MutableDirectBuffer dst,
      final int offset,
      final QuoteRequestDecoder decoder,
      final QuoteRejectReasonEnum reason,
      final byte[] text,
      final int textLen,
      final long transactTimeNanos) {

    quoteRequestRejectEncoder.wrapAndApplyHeader(dst, offset, headerEncoder);

    decoder.getQuoteReqId(charScratch, 0);
    quoteRequestRejectEncoder.putQuoteReqId(charScratch, 0);

    quoteRequestRejectEncoder.quoteRejectReason(reason);

    decoder.getSymbol(charScratch, 0);
    quoteRequestRejectEncoder.putSymbol(charScratch, 0);

    quoteRequestRejectEncoder.side(decoder.side());
    quoteRequestRejectEncoder.transactTime(transactTimeNanos);

    writeText(text, textLen);
    quoteRequestRejectEncoder.putText(textScratch, 0);

    quoteRequestRejectEncoder.productType(decoder.productType());

    return MessageHeaderEncoder.ENCODED_LENGTH + quoteRequestRejectEncoder.encodedLength();
  }

  // ===========================================================================
  // 4. PriceValidationRequest (templateId=52) — orchestrator → pricing
  // ===========================================================================

  /**
   * Encode a PriceValidationRequest (templateId=52) from RfqState + NewOrderSingle fields. Both
   * quoteId AND quoteReqId are encoded — the pricing service keys stored quotes by quoteReqId.
   *
   * @param dst target buffer
   * @param offset byte offset to start encoding
   * @param state the RfqState in PENDING_VALIDATION state
   * @param nosDecoder the pre-wrapped NewOrderSingle decoder (for price, orderQty)
   * @param transactTimeNanos epoch nanos from EpochNanoClock
   * @return total encoded length
   */
  public int encodePriceValidationRequest(
      final MutableDirectBuffer dst,
      final int offset,
      final RfqState state,
      final NewOrderSingleDecoder nosDecoder,
      final long transactTimeNanos) {

    priceValidationRequestEncoder.wrapAndApplyHeader(dst, offset, headerEncoder);

    // quoteId from RfqState
    state.putQuoteIdInto(charScratch, 0);
    priceValidationRequestEncoder.putQuoteId(charScratch, 0);

    // quoteReqId from RfqState — required for pricing service quote lookup
    state.putQuoteReqIdInto(charScratch, 0);
    priceValidationRequestEncoder.putQuoteReqId(charScratch, 0);

    state.putSymbolInto(charScratch, 0);
    priceValidationRequestEncoder.putSymbol(charScratch, 0);

    priceValidationRequestEncoder.side(SideEnum.get(state.sideRaw()));

    // price and orderQty from the NOS decoder
    priceValidationRequestEncoder.price(nosDecoder.price());
    priceValidationRequestEncoder.orderQty(nosDecoder.orderQty());

    state.putAccountCodeInto(charScratch, 0);
    priceValidationRequestEncoder.putAccountCode(charScratch, 0);

    priceValidationRequestEncoder.transactTime(transactTimeNanos);
    priceValidationRequestEncoder.productType(ProductTypeEnum.get(state.productTypeRaw()));

    state.putSettlDateInto(charScratch, 0);
    priceValidationRequestEncoder.putSettlDate(charScratch, 0);

    priceValidationRequestEncoder.settlType(SettlTypeEnum.get(state.settlTypeRaw()));

    state.putCurrencyInto(charScratch, 0);
    priceValidationRequestEncoder.putCurrency(charScratch, 0);

    state.putSettlCurrencyInto(charScratch, 0);
    priceValidationRequestEncoder.putSettlCurrency(charScratch, 0);

    priceValidationRequestEncoder.tenor(TenorEnum.get(state.tenorRaw()));

    // NoLegs: empty group (APP-47)
    priceValidationRequestEncoder.noLegsCount(0);

    return MessageHeaderEncoder.ENCODED_LENGTH + priceValidationRequestEncoder.encodedLength();
  }

  // ===========================================================================
  // 5. Reject ExecutionReport (templateId=5) — orchestrator → gateway
  // ===========================================================================

  /**
   * Encode a reject ExecutionReport (templateId=5) for an NOS that failed price validation or had
   * an unknown quoteId. The orchestrator generates this reject BEFORE the order reaches the
   * cluster, so orderId and execId are null-padded sentinels (all 0x00 bytes).
   *
   * @param dst target buffer
   * @param offset byte offset to start encoding
   * @param clOrdId client order ID bytes
   * @param clOrdIdOffset offset into clOrdId
   * @param quoteId quote ID bytes
   * @param quoteIdOffset offset into quoteId
   * @param symbol symbol bytes (8 bytes)
   * @param symbolOffset offset into symbol
   * @param sideRaw SideEnum raw value
   * @param text reject text bytes
   * @param textLen number of meaningful bytes in text
   * @param transactTimeNanos epoch nanos from EpochNanoClock
   * @param productTypeRaw ProductTypeEnum raw value
   * @param settlDate settlDate bytes (8 bytes)
   * @param settlDateOffset offset into settlDate
   * @param settlTypeRaw SettlTypeEnum raw value
   * @param currency currency bytes (3 bytes)
   * @param currencyOffset offset into currency
   * @param settlCurrency settlCurrency bytes (3 bytes)
   * @param settlCurrencyOffset offset into settlCurrency
   * @param tenorRaw TenorEnum raw value
   * @return total encoded length
   */
  public int encodeRejectExecutionReport(
      final MutableDirectBuffer dst,
      final int offset,
      final byte[] clOrdId,
      final int clOrdIdOffset,
      final byte[] quoteId,
      final int quoteIdOffset,
      final byte[] symbol,
      final int symbolOffset,
      final byte sideRaw,
      final byte[] text,
      final int textLen,
      final long transactTimeNanos,
      final byte productTypeRaw,
      final byte[] settlDate,
      final int settlDateOffset,
      final byte settlTypeRaw,
      final byte[] currency,
      final int currencyOffset,
      final byte[] settlCurrency,
      final int settlCurrencyOffset,
      final byte tenorRaw) {

    executionReportEncoder.wrapAndApplyHeader(dst, offset, headerEncoder);

    // orderId: null-padded sentinel (no cluster orderId yet)
    executionReportEncoder.putOrderId(nullSentinel20, 0);
    // execId: null-padded sentinel
    executionReportEncoder.putExecId(nullSentinel20, 0);

    executionReportEncoder.putClOrdId(clOrdId, clOrdIdOffset);
    executionReportEncoder.putQuoteId(quoteId, quoteIdOffset);
    executionReportEncoder.execType(ExecTypeEnum.Rejected);
    executionReportEncoder.ordStatus(OrdStatusEnum.Rejected);
    executionReportEncoder.putSymbol(symbol, symbolOffset);
    executionReportEncoder.side(SideEnum.get(sideRaw));
    executionReportEncoder.leavesQty(0L);
    executionReportEncoder.cumQty(0L);
    executionReportEncoder.avgPx(executionReportEncoder.avgPxNullValue());
    executionReportEncoder.transactTime(transactTimeNanos);

    writeText(text, textLen);
    executionReportEncoder.putText(textScratch, 0);

    executionReportEncoder.productType(ProductTypeEnum.get(productTypeRaw));
    executionReportEncoder.putSettlDate(settlDate, settlDateOffset);
    executionReportEncoder.settlType(SettlTypeEnum.get(settlTypeRaw));
    executionReportEncoder.putCurrency(currency, currencyOffset);
    executionReportEncoder.putSettlCurrency(settlCurrency, settlCurrencyOffset);
    executionReportEncoder.tenor(TenorEnum.get(tenorRaw));

    // NoLegs: empty group
    executionReportEncoder.noLegsCount(0);

    return MessageHeaderEncoder.ENCODED_LENGTH + executionReportEncoder.encodedLength();
  }

  // ===========================================================================
  // Internal helpers
  // ===========================================================================

  /**
   * Copies the caller's text bytes into the textScratch buffer, null-padding the remainder to fill
   * the full SBE Text field length. Zero allocation.
   */
  private void writeText(final byte[] text, final int textLen) {
    final int copyLen = Math.min(textLen, TEXT_SCRATCH_LEN);
    System.arraycopy(text, 0, textScratch, 0, copyLen);
    if (copyLen < TEXT_SCRATCH_LEN) {
      Arrays.fill(textScratch, copyLen, TEXT_SCRATCH_LEN, (byte) 0);
    }
  }
}
