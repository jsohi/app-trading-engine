package com.trading.engine.orchestrator.codec;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.trading.engine.messages.sbe.ExecTypeEnum;
import com.trading.engine.messages.sbe.ExecutionReportDecoder;
import com.trading.engine.messages.sbe.MessageHeaderDecoder;
import com.trading.engine.messages.sbe.MessageHeaderEncoder;
import com.trading.engine.messages.sbe.OrdStatusEnum;
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
 */
class OrchestratorMessageEncoderTest {

  private static final long NOW = 1_000_000_000L;
  private static final String QUOTE_REQ_ID = "QR-000000000001";
  private static final String QUOTE_ID = "QTE-00000000001";
  private static final String SYMBOL = "EURUSD";
  private static final long BID_PX = 110_000_000L;
  private static final long OFFER_PX = 111_000_000L;

  private final MutableDirectBuffer srcBuf = new ExpandableArrayBuffer(512);
  private final MutableDirectBuffer dstBuf = new ExpandableArrayBuffer(512);
  private final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();
  private final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
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
    assertEquals(100_000_000L, dec.orderQty());
  }

  @Test
  void encodePriceRequest_fromDecoder_roundTrip() {
    final var qrDecoder = wrapQuoteRequest();

    final int len = encoder.encodePriceRequest(dstBuf, 0, qrDecoder);
    assertTrue(len > MessageHeaderEncoder.ENCODED_LENGTH);

    final var dec = new PriceRequestDecoder();
    headerDecoder.wrap(dstBuf, 0);
    dec.wrap(
        dstBuf,
        MessageHeaderDecoder.ENCODED_LENGTH,
        headerDecoder.blockLength(),
        headerDecoder.version());

    assertSbeCharField(dec.quoteReqId(), QUOTE_REQ_ID);
    assertSbeCharField(dec.symbol(), SYMBOL);
    assertEquals(SideEnum.Buy, dec.side());
  }

  // ===========================================================================
  // Quote (templateId=2)
  // ===========================================================================

  @Test
  void encodeQuote_roundTrip() {
    final var rfq = acquireSlot();
    applyPricing(rfq);

    final byte[] quoteIdBytes = padToSbe(QUOTE_ID, QuoteDecoder.quoteIdLength());
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
    assertEquals(QuoteStatusEnum.Accepted, dec.quoteStatus());
    assertEquals(BID_PX, dec.bidPx());
    assertEquals(OFFER_PX, dec.offerPx());
  }

  // ===========================================================================
  // QuoteRequestReject (templateId=3)
  // ===========================================================================

  @Test
  void encodeQuoteRequestReject_fromRfqState_roundTrip() {
    final var rfq = acquireSlot();
    final byte[] text = "Pool exhausted".getBytes(StandardCharsets.US_ASCII);

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
  }

  @Test
  void encodeQuoteRequestReject_fromDecoder_roundTrip() {
    final var qrDecoder = wrapQuoteRequest();
    final byte[] text = "Invalid symbol".getBytes(StandardCharsets.US_ASCII);

    final int len =
        encoder.encodeQuoteRequestReject(
            dstBuf, 0, qrDecoder, QuoteRejectReasonEnum.UnknownSymbol, text, text.length, NOW);
    assertTrue(len > MessageHeaderEncoder.ENCODED_LENGTH);

    final var dec = new QuoteRequestRejectDecoder();
    headerDecoder.wrap(dstBuf, 0);
    dec.wrap(
        dstBuf,
        MessageHeaderDecoder.ENCODED_LENGTH,
        headerDecoder.blockLength(),
        headerDecoder.version());

    assertSbeCharField(dec.quoteReqId(), QUOTE_REQ_ID);
    assertEquals(QuoteRejectReasonEnum.UnknownSymbol, dec.quoteRejectReason());
  }

  // ===========================================================================
  // PriceValidationRequest (templateId=52)
  // ===========================================================================

  @Test
  void encodePriceValidationRequest_roundTrip() {
    final var rfq = acquireSlot();
    applyPricing(rfq);
    setQuoteId(rfq);

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
  }

  // ===========================================================================
  // Reject ExecutionReport (templateId=5)
  // ===========================================================================

  @Test
  void encodeRejectExecutionReport_roundTrip() {
    final byte[] clOrdId = padToSbe("ORD-00000000001", 20);
    final byte[] quoteId = padToSbe(QUOTE_ID, QuoteDecoder.quoteIdLength());
    final byte[] symbol = padToSbe(SYMBOL, QuoteDecoder.symbolLength());
    final byte[] settlDate = padToSbe("20260101", ExecutionReportDecoder.settlDateLength());
    final byte[] currency = padToSbe("USD", ExecutionReportDecoder.currencyLength());
    final byte[] settlCurrency = padToSbe("EUR", ExecutionReportDecoder.settlCurrencyLength());
    final byte[] text = "Validation failed".getBytes(StandardCharsets.US_ASCII);

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
  }

  // ===========================================================================
  // Helpers
  // ===========================================================================

  private RfqState acquireSlot() {
    SbeTestEncoder.encodeQuoteRequest(
        srcBuf, 0, QUOTE_REQ_ID, SYMBOL, SideEnum.Buy, 100_000_000L, "ACCT001");
    headerDecoder.wrap(srcBuf, 0);
    quoteReqDecoder.wrap(
        srcBuf,
        MessageHeaderDecoder.ENCODED_LENGTH,
        headerDecoder.blockLength(),
        headerDecoder.version());
    return sm.onQuoteRequest(quoteReqDecoder, NOW);
  }

  private void applyPricing(final RfqState rfq) {
    final byte[] qrid = padToSbe(QUOTE_REQ_ID, QuoteRequestDecoder.quoteReqIdLength());
    SbeTestEncoder.encodePriceResponse(
        srcBuf, 0, QUOTE_REQ_ID, SYMBOL, true, BID_PX, OFFER_PX, NOW);
    headerDecoder.wrap(srcBuf, 0);
    priceRespDecoder.wrap(
        srcBuf,
        MessageHeaderDecoder.ENCODED_LENGTH,
        headerDecoder.blockLength(),
        headerDecoder.version());
    final byte[] quoteId = padToSbe(QUOTE_ID, QuoteDecoder.quoteIdLength());
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

  private void setQuoteId(final RfqState rfq) {
    // quoteId was set during applyPricing via onPriceResponseAccepted
  }

  private QuoteRequestDecoder wrapQuoteRequest() {
    SbeTestEncoder.encodeQuoteRequest(
        srcBuf, 0, QUOTE_REQ_ID, SYMBOL, SideEnum.Buy, 100_000_000L, "ACCT001");
    headerDecoder.wrap(srcBuf, 0);
    quoteReqDecoder.wrap(
        srcBuf,
        MessageHeaderDecoder.ENCODED_LENGTH,
        headerDecoder.blockLength(),
        headerDecoder.version());
    return quoteReqDecoder;
  }

  private com.trading.engine.messages.sbe.NewOrderSingleDecoder wrapNosDecoder() {
    SbeTestEncoder.encodeNewOrderSingle(
        srcBuf,
        0,
        "ORD-00000000001",
        SYMBOL,
        SideEnum.Buy,
        com.trading.engine.messages.sbe.OrdTypeEnum.PreviouslyQuoted,
        BID_PX,
        100_000_000L,
        "ACCT001",
        "USD");
    headerDecoder.wrap(srcBuf, 0);
    final var dec = new com.trading.engine.messages.sbe.NewOrderSingleDecoder();
    dec.wrap(
        srcBuf,
        MessageHeaderDecoder.ENCODED_LENGTH,
        headerDecoder.blockLength(),
        headerDecoder.version());
    return dec;
  }

  private static byte[] padToSbe(final String value, final int sbeLength) {
    final byte[] padded = new byte[sbeLength];
    final byte[] ascii = value.getBytes(StandardCharsets.US_ASCII);
    System.arraycopy(ascii, 0, padded, 0, Math.min(ascii.length, sbeLength));
    return padded;
  }

  /** Assert that a null-padded SBE char field starts with the expected ASCII string. */
  private static void assertSbeCharField(final String sbeFieldValue, final String expected) {
    assertTrue(
        sbeFieldValue.startsWith(expected),
        "Expected SBE field to start with '" + expected + "' but was '" + sbeFieldValue + "'");
  }
}
