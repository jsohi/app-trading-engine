package com.trading.engine.orchestrator.codec;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.trading.engine.messages.sbe.ExecTypeEnum;
import com.trading.engine.messages.sbe.ExecutionReportDecoder;
import com.trading.engine.messages.sbe.MessageHeaderDecoder;
import com.trading.engine.messages.sbe.MessageHeaderEncoder;
import com.trading.engine.messages.sbe.NewOrderSingleDecoder;
import com.trading.engine.messages.sbe.OrdStatusEnum;
import com.trading.engine.messages.sbe.OrdTypeEnum;
import com.trading.engine.messages.sbe.PriceRequestDecoder;
import com.trading.engine.messages.sbe.PriceResponseDecoder;
import com.trading.engine.messages.sbe.PriceValidationRequestDecoder;
import com.trading.engine.messages.sbe.ProductTypeEnum;
import com.trading.engine.messages.sbe.QuoteDecoder;
import com.trading.engine.messages.sbe.QuoteRejectReasonEnum;
import com.trading.engine.messages.sbe.QuoteRequestDecoder;
import com.trading.engine.messages.sbe.QuoteRequestRejectDecoder;
import com.trading.engine.messages.sbe.QuoteStatusEnum;
import com.trading.engine.messages.sbe.SettlTypeEnum;
import com.trading.engine.messages.sbe.SideEnum;
import com.trading.engine.messages.sbe.TenorEnum;
import com.trading.engine.orchestrator.RfqState;
import com.trading.engine.orchestrator.RfqStateMachine;
import com.trading.engine.testsupport.buffer.SbeFieldUtil;
import com.trading.engine.testsupport.sbe.SbeTestEncoder;
import java.nio.charset.StandardCharsets;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Round-trip tests for {@link OrchestratorMessageEncoder}. Encodes messages via the encoder, then
 * decodes with the SBE decoder and verifies field values match. Tests both overloads (from RfqState
 * and from QuoteRequestDecoder) where applicable.
 *
 * <p><b>SBE char-field convention:</b> SBE char fields (e.g., {@code quoteId}, {@code quoteReqId})
 * are null-padded fixed-length byte arrays on the wire. The SBE-generated decoder's {@code
 * String}-returning accessors retain the trailing null bytes when the value is shorter than the
 * field. {@link #assertSbeCharField(String, String)} therefore uses {@code startsWith} to tolerate
 * decoder-emitted padding while still proving the leading bytes encode the expected value.
 *
 * <p><b>Defaults:</b> account code, settlement, currency and tenor are sourced from {@link
 * SbeTestEncoder#encodeQuoteRequest} fixed defaults (see helper Javadoc). Each encoder round-trip
 * verifies these are preserved end-to-end.
 */
class OrchestratorMessageEncoderTest {

  /** Arbitrary epoch-nanos timestamp for {@code transactTime}; chosen non-zero to surface bugs. */
  private static final long NOW = 1_000_000_000L;

  private static final String QUOTE_REQ_ID = "QR-000000000001";
  private static final String QUOTE_ID = "QTE-00000000001";
  private static final String SYMBOL = "EURUSD";

  /** Bid price in fixed-point 10^8 scale (= 1.10 USD/EUR). */
  private static final long BID_PX = 110_000_000L;

  /** Offer price in fixed-point 10^8 scale (= 1.11 USD/EUR). */
  private static final long OFFER_PX = 111_000_000L;

  /** Order quantity in fixed-point 10^8 scale (= 1.00 unit). */
  private static final long ORDER_QTY = 100_000_000L;

  /** Account code used by {@link SbeTestEncoder#encodeQuoteRequest} default helper. */
  private static final String ACCOUNT_CODE = "ACCT001";

  /** Default currency emitted by {@link SbeTestEncoder} helpers (matches FIX tag 15). */
  private static final String CURRENCY = "USD";

  /** Default settle currency emitted by {@link SbeTestEncoder} helpers (matches FIX tag 120). */
  private static final String SETTL_CURRENCY = "EUR";

  /** Default settle date emitted by {@link SbeTestEncoder} helpers (LocalMktDate, FIX tag 64). */
  private static final String SETTL_DATE = "20260101";

  /** Default bid/offer size emitted by {@link SbeTestEncoder#encodePriceResponse} when accepted. */
  private static final long DEFAULT_SIZE = 100_000_000L;

  /**
   * Default {@code validUntil} offset (30s in nanos) added to {@code transactTime} by {@link
   * SbeTestEncoder#encodePriceResponse} when accepted.
   */
  private static final long DEFAULT_VALID_UNTIL_OFFSET = 30_000_000_000L;

  private final MutableDirectBuffer srcBuf = new ExpandableArrayBuffer(512);
  private final MutableDirectBuffer dstBuf = new ExpandableArrayBuffer(512);
  private final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();
  private final QuoteRequestDecoder quoteReqDecoder = new QuoteRequestDecoder();
  private final PriceResponseDecoder priceRespDecoder = new PriceResponseDecoder();

  private OrchestratorMessageEncoder encoder;
  private RfqStateMachine sm;

  @BeforeEach
  void setUp() {
    encoder = new OrchestratorMessageEncoder();
    sm = new RfqStateMachine(4, 5_000_000_000L, 30_000_000_000L, 5_000_000_000L);
  }

  // ===========================================================================
  // PriceRequest (templateId=50)
  // ===========================================================================

  @Test
  void encodePriceRequest_fromRfqState_roundTrip() {
    final var rfq = acquireSlot();

    final int len = encoder.encodePriceRequest(dstBuf, 0, rfq);
    assertTrue(len > MessageHeaderEncoder.ENCODED_LENGTH);

    final var dec = new PriceRequestDecoder();
    headerDecoder.wrap(dstBuf, 0);
    assertEquals(PriceRequestDecoder.TEMPLATE_ID, headerDecoder.templateId());
    dec.wrap(
        dstBuf,
        MessageHeaderDecoder.ENCODED_LENGTH,
        headerDecoder.blockLength(),
        headerDecoder.version());

    assertSbeCharField(dec.quoteReqId(), QUOTE_REQ_ID);
    assertSbeCharField(dec.symbol(), SYMBOL);
    assertEquals(SideEnum.Buy, dec.side());
    assertEquals(ORDER_QTY, dec.orderQty());
    assertSbeCharField(dec.accountCode(), ACCOUNT_CODE);
    assertEquals(0L, dec.transactTime()); // SbeTestEncoder.encodeQuoteRequest defaults to 0
    assertEquals(ProductTypeEnum.Spot, dec.productType());
    assertSbeCharField(dec.settlDate(), SETTL_DATE);
    assertEquals(SettlTypeEnum.Regular, dec.settlType());
    assertSbeCharField(dec.currency(), CURRENCY);
    assertSbeCharField(dec.settlCurrency(), SETTL_CURRENCY);
    assertEquals(TenorEnum.SN, dec.tenor());
    assertEquals(0, dec.noLegs().count()); // APP-47: multileg deferred
  }

  @Test
  void encodePriceRequest_fromDecoder_roundTrip() {
    final var qrDecoder = wrapQuoteRequest();

    final int len = encoder.encodePriceRequest(dstBuf, 0, qrDecoder);
    assertTrue(len > MessageHeaderEncoder.ENCODED_LENGTH);

    final var dec = new PriceRequestDecoder();
    headerDecoder.wrap(dstBuf, 0);
    assertEquals(PriceRequestDecoder.TEMPLATE_ID, headerDecoder.templateId());
    dec.wrap(
        dstBuf,
        MessageHeaderDecoder.ENCODED_LENGTH,
        headerDecoder.blockLength(),
        headerDecoder.version());

    assertSbeCharField(dec.quoteReqId(), QUOTE_REQ_ID);
    assertSbeCharField(dec.symbol(), SYMBOL);
    assertEquals(SideEnum.Buy, dec.side());
    assertEquals(ORDER_QTY, dec.orderQty());
    assertSbeCharField(dec.accountCode(), ACCOUNT_CODE);
    assertEquals(0L, dec.transactTime()); // SbeTestEncoder.encodeQuoteRequest defaults to 0
    assertEquals(ProductTypeEnum.Spot, dec.productType());
    assertSbeCharField(dec.settlDate(), SETTL_DATE);
    assertEquals(SettlTypeEnum.Regular, dec.settlType());
    assertSbeCharField(dec.currency(), CURRENCY);
    assertSbeCharField(dec.settlCurrency(), SETTL_CURRENCY);
    assertEquals(TenorEnum.SN, dec.tenor());
    assertEquals(0, dec.noLegs().count());
  }

  // ===========================================================================
  // Quote (templateId=2)
  // ===========================================================================

  @Test
  void encodeQuote_roundTrip() {
    final var rfq = acquireSlot();
    applyPricing(rfq);

    final var quoteIdBytes = SbeFieldUtil.zeroPad(QUOTE_ID, QuoteDecoder.quoteIdLength());
    final int len = encoder.encodeQuote(dstBuf, 0, rfq, quoteIdBytes, 0, quoteIdBytes.length, NOW);
    assertTrue(len > MessageHeaderEncoder.ENCODED_LENGTH);

    final var dec = new QuoteDecoder();
    headerDecoder.wrap(dstBuf, 0);
    assertEquals(QuoteDecoder.TEMPLATE_ID, headerDecoder.templateId());
    dec.wrap(
        dstBuf,
        MessageHeaderDecoder.ENCODED_LENGTH,
        headerDecoder.blockLength(),
        headerDecoder.version());

    assertSbeCharField(dec.quoteId(), QUOTE_ID);
    assertSbeCharField(dec.quoteReqId(), QUOTE_REQ_ID);
    assertSbeCharField(dec.symbol(), SYMBOL);
    assertEquals(SideEnum.Buy, dec.side());
    assertEquals(QuoteStatusEnum.Accepted, dec.quoteStatus());
    assertEquals(BID_PX, dec.bidPx());
    assertEquals(OFFER_PX, dec.offerPx());
    assertEquals(DEFAULT_SIZE, dec.bidSize());
    assertEquals(DEFAULT_SIZE, dec.offerSize());
    assertEquals(NOW + DEFAULT_VALID_UNTIL_OFFSET, dec.validUntil());
    assertEquals(QuoteDecoder.swapPointsNullValue(), dec.swapPoints());
    assertEquals(NOW, dec.transactTime());
    assertEquals(ProductTypeEnum.Spot, dec.productType());
    assertSbeCharField(dec.settlDate(), SETTL_DATE);
    assertEquals(SettlTypeEnum.Regular, dec.settlType());
    assertSbeCharField(dec.currency(), CURRENCY);
    assertSbeCharField(dec.settlCurrency(), SETTL_CURRENCY);
    assertEquals(TenorEnum.SN, dec.tenor());
    // text is null-padded empty (encoder writes an empty 64-byte field)
    assertSbeCharField(dec.text(), "");
    assertEquals(0, dec.noLegs().count());
  }

  // ===========================================================================
  // QuoteRequestReject (templateId=3)
  // ===========================================================================

  @Test
  void encodeQuoteRequestReject_fromRfqState_roundTrip() {
    final var rfq = acquireSlot();
    final var text = "Pool exhausted".getBytes(StandardCharsets.US_ASCII);

    final int len =
        encoder.encodeQuoteRequestReject(
            dstBuf, 0, rfq, QuoteRejectReasonEnum.Other, text, text.length, NOW);
    assertTrue(len > MessageHeaderEncoder.ENCODED_LENGTH);

    final var dec = new QuoteRequestRejectDecoder();
    headerDecoder.wrap(dstBuf, 0);
    assertEquals(QuoteRequestRejectDecoder.TEMPLATE_ID, headerDecoder.templateId());
    dec.wrap(
        dstBuf,
        MessageHeaderDecoder.ENCODED_LENGTH,
        headerDecoder.blockLength(),
        headerDecoder.version());

    assertSbeCharField(dec.quoteReqId(), QUOTE_REQ_ID);
    assertEquals(QuoteRejectReasonEnum.Other, dec.quoteRejectReason());
    assertSbeCharField(dec.symbol(), SYMBOL);
    assertEquals(SideEnum.Buy, dec.side());
    assertEquals(NOW, dec.transactTime());
    assertSbeCharField(dec.text(), "Pool exhausted");
    assertEquals(ProductTypeEnum.Spot, dec.productType());
  }

  @Test
  void encodeQuoteRequestReject_fromDecoder_roundTrip() {
    final var qrDecoder = wrapQuoteRequest();
    final var text = "Invalid symbol".getBytes(StandardCharsets.US_ASCII);

    final int len =
        encoder.encodeQuoteRequestReject(
            dstBuf, 0, qrDecoder, QuoteRejectReasonEnum.UnknownSymbol, text, text.length, NOW);
    assertTrue(len > MessageHeaderEncoder.ENCODED_LENGTH);

    final var dec = new QuoteRequestRejectDecoder();
    headerDecoder.wrap(dstBuf, 0);
    assertEquals(QuoteRequestRejectDecoder.TEMPLATE_ID, headerDecoder.templateId());
    dec.wrap(
        dstBuf,
        MessageHeaderDecoder.ENCODED_LENGTH,
        headerDecoder.blockLength(),
        headerDecoder.version());

    assertSbeCharField(dec.quoteReqId(), QUOTE_REQ_ID);
    assertEquals(QuoteRejectReasonEnum.UnknownSymbol, dec.quoteRejectReason());
    assertSbeCharField(dec.symbol(), SYMBOL);
    assertEquals(SideEnum.Buy, dec.side());
    assertEquals(NOW, dec.transactTime());
    assertSbeCharField(dec.text(), "Invalid symbol");
    assertEquals(ProductTypeEnum.Spot, dec.productType());
  }

  // ===========================================================================
  // PriceValidationRequest (templateId=52)
  // ===========================================================================

  @Test
  void encodePriceValidationRequest_roundTrip() {
    final var rfq = acquireSlot();
    applyPricing(rfq); // also sets quoteId via onPriceResponseAccepted

    final var nosDecoder = wrapNosDecoder();
    final int len = encoder.encodePriceValidationRequest(dstBuf, 0, rfq, nosDecoder, NOW);
    assertTrue(len > MessageHeaderEncoder.ENCODED_LENGTH);

    final var dec = new PriceValidationRequestDecoder();
    headerDecoder.wrap(dstBuf, 0);
    assertEquals(PriceValidationRequestDecoder.TEMPLATE_ID, headerDecoder.templateId());
    dec.wrap(
        dstBuf,
        MessageHeaderDecoder.ENCODED_LENGTH,
        headerDecoder.blockLength(),
        headerDecoder.version());

    assertSbeCharField(dec.quoteId(), QUOTE_ID);
    assertSbeCharField(dec.quoteReqId(), QUOTE_REQ_ID);
    assertSbeCharField(dec.symbol(), SYMBOL);
    assertEquals(SideEnum.Buy, dec.side());
    assertEquals(BID_PX, dec.price()); // NOS price is BID_PX (see wrapNosDecoder)
    assertEquals(ORDER_QTY, dec.orderQty());
    assertSbeCharField(dec.accountCode(), ACCOUNT_CODE);
    assertEquals(NOW, dec.transactTime());
    assertEquals(ProductTypeEnum.Spot, dec.productType());
    assertSbeCharField(dec.settlDate(), SETTL_DATE);
    assertEquals(SettlTypeEnum.Regular, dec.settlType());
    assertSbeCharField(dec.currency(), CURRENCY);
    assertSbeCharField(dec.settlCurrency(), SETTL_CURRENCY);
    assertEquals(TenorEnum.SN, dec.tenor());
    assertEquals(0, dec.noLegs().count());
  }

  // ===========================================================================
  // Reject ExecutionReport (templateId=5)
  // ===========================================================================

  @Test
  void encodeRejectExecutionReport_roundTrip() {
    final var clOrdId = SbeFieldUtil.zeroPad("ORD-00000000001", 20);
    final var quoteId = SbeFieldUtil.zeroPad(QUOTE_ID, QuoteDecoder.quoteIdLength());
    final var symbol = SbeFieldUtil.zeroPad(SYMBOL, QuoteDecoder.symbolLength());
    final var settlDate =
        SbeFieldUtil.zeroPad(SETTL_DATE, ExecutionReportDecoder.settlDateLength());
    final var currency = SbeFieldUtil.zeroPad(CURRENCY, ExecutionReportDecoder.currencyLength());
    final var settlCurrency =
        SbeFieldUtil.zeroPad(SETTL_CURRENCY, ExecutionReportDecoder.settlCurrencyLength());
    final var text = "Validation failed".getBytes(StandardCharsets.US_ASCII);

    final int len =
        encoder.encodeRejectExecutionReport(
            dstBuf,
            0,
            clOrdId,
            0,
            quoteId,
            0,
            symbol,
            0,
            (byte) SideEnum.Buy.value(),
            text,
            text.length,
            NOW,
            (byte) ProductTypeEnum.Spot.value(),
            settlDate,
            0,
            (byte) SettlTypeEnum.Regular.value(),
            currency,
            0,
            settlCurrency,
            0,
            (byte) TenorEnum.SN.value());
    assertTrue(len > MessageHeaderEncoder.ENCODED_LENGTH);

    final var dec = new ExecutionReportDecoder();
    headerDecoder.wrap(dstBuf, 0);
    assertEquals(ExecutionReportDecoder.TEMPLATE_ID, headerDecoder.templateId());
    dec.wrap(
        dstBuf,
        MessageHeaderDecoder.ENCODED_LENGTH,
        headerDecoder.blockLength(),
        headerDecoder.version());

    assertEquals(ExecTypeEnum.Rejected, dec.execType());
    assertEquals(OrdStatusEnum.Rejected, dec.ordStatus());
    // orderId and execId are sentinel-padded (orchestrator-generated rejects, no cluster IDs yet)
    assertSbeCharField(dec.orderId(), "");
    assertSbeCharField(dec.execId(), "");
    assertSbeCharField(dec.clOrdId(), "ORD-00000000001");
    assertSbeCharField(dec.quoteId(), QUOTE_ID);
    assertSbeCharField(dec.symbol(), SYMBOL);
    assertEquals(SideEnum.Buy, dec.side());
    assertEquals(0L, dec.leavesQty());
    assertEquals(0L, dec.cumQty());
    assertEquals(ExecutionReportDecoder.avgPxNullValue(), dec.avgPx());
    assertEquals(NOW, dec.transactTime());
    assertSbeCharField(dec.text(), "Validation failed");
    assertEquals(ProductTypeEnum.Spot, dec.productType());
    assertSbeCharField(dec.settlDate(), SETTL_DATE);
    assertEquals(SettlTypeEnum.Regular, dec.settlType());
    assertSbeCharField(dec.currency(), CURRENCY);
    assertSbeCharField(dec.settlCurrency(), SETTL_CURRENCY);
    assertEquals(TenorEnum.SN, dec.tenor());
    assertEquals(0, dec.noLegs().count());
  }

  // ===========================================================================
  // Helpers
  // ===========================================================================

  /**
   * Encodes a default QuoteRequest into {@link #srcBuf}, transitions the state machine to {@code
   * PENDING_PRICE}, and returns the acquired RfqState slot.
   */
  private RfqState acquireSlot() {
    SbeTestEncoder.encodeQuoteRequest(
        srcBuf, 0, QUOTE_REQ_ID, SYMBOL, SideEnum.Buy, ORDER_QTY, ACCOUNT_CODE);
    headerDecoder.wrap(srcBuf, 0);
    quoteReqDecoder.wrap(
        srcBuf,
        MessageHeaderDecoder.ENCODED_LENGTH,
        headerDecoder.blockLength(),
        headerDecoder.version());
    return sm.onQuoteRequest(quoteReqDecoder, NOW);
  }

  /**
   * Applies a price-accepted response to the given RFQ, advancing it to {@code QUOTED} with the
   * configured BID/OFFER prices and the canonical {@link #QUOTE_ID}.
   */
  private void applyPricing(final RfqState rfq) {
    final var qrid = SbeFieldUtil.zeroPad(QUOTE_REQ_ID, QuoteRequestDecoder.quoteReqIdLength());
    SbeTestEncoder.encodePriceResponse(
        srcBuf, 0, QUOTE_REQ_ID, SYMBOL, true, BID_PX, OFFER_PX, NOW);
    headerDecoder.wrap(srcBuf, 0);
    priceRespDecoder.wrap(
        srcBuf,
        MessageHeaderDecoder.ENCODED_LENGTH,
        headerDecoder.blockLength(),
        headerDecoder.version());
    final var quoteId = SbeFieldUtil.zeroPad(QUOTE_ID, QuoteDecoder.quoteIdLength());
    sm.onPriceResponseAccepted(
        qrid,
        0,
        QuoteRequestDecoder.quoteReqIdLength(),
        priceRespDecoder,
        quoteId,
        0,
        quoteId.length,
        NOW);
  }

  /** Encodes and wraps a default QuoteRequest decoder for use by encoder overload tests. */
  private QuoteRequestDecoder wrapQuoteRequest() {
    SbeTestEncoder.encodeQuoteRequest(
        srcBuf, 0, QUOTE_REQ_ID, SYMBOL, SideEnum.Buy, ORDER_QTY, ACCOUNT_CODE);
    headerDecoder.wrap(srcBuf, 0);
    quoteReqDecoder.wrap(
        srcBuf,
        MessageHeaderDecoder.ENCODED_LENGTH,
        headerDecoder.blockLength(),
        headerDecoder.version());
    return quoteReqDecoder;
  }

  /** Encodes and wraps a NewOrderSingle decoder priced at {@link #BID_PX} for validation tests. */
  private NewOrderSingleDecoder wrapNosDecoder() {
    SbeTestEncoder.encodeNewOrderSingle(
        srcBuf,
        0,
        "ORD-00000000001",
        SYMBOL,
        SideEnum.Buy,
        OrdTypeEnum.PreviouslyQuoted,
        BID_PX,
        ORDER_QTY,
        ACCOUNT_CODE,
        CURRENCY);
    headerDecoder.wrap(srcBuf, 0);
    final var dec = new NewOrderSingleDecoder();
    dec.wrap(
        srcBuf,
        MessageHeaderDecoder.ENCODED_LENGTH,
        headerDecoder.blockLength(),
        headerDecoder.version());
    return dec;
  }

  /**
   * Asserts that a null-padded SBE char field starts with the expected ASCII string. SBE-generated
   * {@code String}-returning accessors retain trailing null padding when the value is shorter than
   * the field width; {@code startsWith} validates the meaningful prefix while tolerating the
   * decoder-emitted padding.
   */
  private static void assertSbeCharField(final String sbeFieldValue, final String expected) {
    assertTrue(
        sbeFieldValue.startsWith(expected),
        "Expected SBE field to start with '" + expected + "' but was '" + sbeFieldValue + "'");
  }
}
