package com.trading.engine.testsupport.sbe;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.trading.engine.messages.sbe.BooleanType;
import com.trading.engine.messages.sbe.MessageHeaderDecoder;
import com.trading.engine.messages.sbe.PriceRequestDecoder;
import com.trading.engine.messages.sbe.PriceResponseDecoder;
import com.trading.engine.messages.sbe.PriceValidationRequestDecoder;
import com.trading.engine.messages.sbe.PriceValidationResponseDecoder;
import com.trading.engine.messages.sbe.ProductTypeEnum;
import com.trading.engine.messages.sbe.QuoteDecoder;
import com.trading.engine.messages.sbe.QuoteRejectReasonEnum;
import com.trading.engine.messages.sbe.QuoteRequestDecoder;
import com.trading.engine.messages.sbe.QuoteRequestRejectDecoder;
import com.trading.engine.messages.sbe.SettlTypeEnum;
import com.trading.engine.messages.sbe.SideEnum;
import com.trading.engine.messages.sbe.TenorEnum;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.junit.jupiter.api.Test;

/**
 * Round-trip tests for the RFQ-related helpers in {@link SbeTestEncoder}. Each test encodes via the
 * helper, decodes with the SBE-generated decoder, and asserts every field — both the
 * caller-supplied parameters and the documented defaults — survives the round trip. Catches
 * helper-internal bugs that would silently corrupt every consuming test in the orchestrator and
 * gateway suites.
 *
 * <p>Existing {@link SbeEncoderDecoderRoundTripTest} covers the cluster/reference-data helpers;
 * this class is paired with it to keep RFQ helpers cleanly separated.
 */
class SbeTestEncoderRfqTest {

  private static final long NOW = 1_000_000_000L;
  private static final String QUOTE_REQ_ID = "QR-000000000001";
  private static final String QUOTE_ID = "QTE-00000000001";
  private static final String SYMBOL = "EURUSD";
  private static final String ACCOUNT = "ACCT001";
  private static final long ORDER_QTY = 100_000_000L;
  private static final long BID_PX = 110_000_000L;
  private static final long OFFER_PX = 111_000_000L;
  // Defaults baked into the helpers
  private static final String DEFAULT_SETTL_DATE = "20260101";
  private static final String DEFAULT_CURRENCY = "USD";
  private static final String DEFAULT_SETTL_CURRENCY = "EUR";

  private final MutableDirectBuffer buf = new ExpandableArrayBuffer(512);
  private final MessageHeaderDecoder hdrDec = new MessageHeaderDecoder();

  @Test
  void encodeQuoteRequest_roundTrip_decodesAllFields() {
    SbeTestEncoder.encodeQuoteRequest(
        buf, 0, QUOTE_REQ_ID, SYMBOL, SideEnum.Buy, ORDER_QTY, ACCOUNT);

    hdrDec.wrap(buf, 0);
    assertEquals(QuoteRequestDecoder.TEMPLATE_ID, hdrDec.templateId());
    final var dec = new QuoteRequestDecoder();
    dec.wrap(buf, MessageHeaderDecoder.ENCODED_LENGTH, hdrDec.blockLength(), hdrDec.version());

    assertSbeCharField(dec.quoteReqId(), QUOTE_REQ_ID);
    assertSbeCharField(dec.symbol(), SYMBOL);
    assertEquals(SideEnum.Buy, dec.side());
    assertEquals(ORDER_QTY, dec.orderQty());
    assertSbeCharField(dec.accountCode(), ACCOUNT);
    assertEquals(0L, dec.transactTime()); // 7-arg helper defaults to 0
    assertEquals(ProductTypeEnum.Spot, dec.productType());
    assertSbeCharField(dec.settlDate(), DEFAULT_SETTL_DATE);
    assertEquals(SettlTypeEnum.Regular, dec.settlType());
    assertSbeCharField(dec.currency(), DEFAULT_CURRENCY);
    assertSbeCharField(dec.settlCurrency(), DEFAULT_SETTL_CURRENCY);
    assertEquals(TenorEnum.SN, dec.tenor());
    assertEquals(0, dec.noLegs().count());
  }

  @Test
  void encodeQuoteRequest_overloadWithTransactTime_passesThroughExplicitly() {
    final long explicitTs = NOW + 12_345L;
    SbeTestEncoder.encodeQuoteRequest(
        buf, 0, QUOTE_REQ_ID, SYMBOL, SideEnum.Sell, ORDER_QTY, ACCOUNT, explicitTs);
    final var dec = wrapQuoteRequest();
    assertEquals(explicitTs, dec.transactTime()); // proves the parameter is honoured
    assertEquals(SideEnum.Sell, dec.side());
  }

  @Test
  void encodeQuote_roundTrip_decodesAllFields() {
    SbeTestEncoder.encodeQuote(
        buf, 0, QUOTE_REQ_ID, QUOTE_ID, SYMBOL, SideEnum.Buy, BID_PX, OFFER_PX, NOW);

    hdrDec.wrap(buf, 0);
    assertEquals(QuoteDecoder.TEMPLATE_ID, hdrDec.templateId());
    final var dec = new QuoteDecoder();
    dec.wrap(buf, MessageHeaderDecoder.ENCODED_LENGTH, hdrDec.blockLength(), hdrDec.version());

    assertSbeCharField(dec.quoteReqId(), QUOTE_REQ_ID);
    assertSbeCharField(dec.quoteId(), QUOTE_ID);
    assertSbeCharField(dec.symbol(), SYMBOL);
    assertEquals(SideEnum.Buy, dec.side());
    assertEquals(BID_PX, dec.bidPx());
    assertEquals(OFFER_PX, dec.offerPx());
    assertEquals(100_000_000L, dec.bidSize()); // helper default
    assertEquals(100_000_000L, dec.offerSize()); // helper default
    assertEquals(NOW, dec.transactTime());
    assertEquals(NOW + 30_000_000_000L, dec.validUntil()); // transactTime + 30s
    assertEquals(ProductTypeEnum.Spot, dec.productType());
    assertSbeCharField(dec.settlDate(), DEFAULT_SETTL_DATE);
    assertEquals(SettlTypeEnum.Regular, dec.settlType());
    assertSbeCharField(dec.currency(), DEFAULT_CURRENCY);
    assertSbeCharField(dec.settlCurrency(), DEFAULT_SETTL_CURRENCY);
    assertEquals(TenorEnum.SN, dec.tenor());
    assertEquals(QuoteDecoder.swapPointsNullValue(), dec.swapPoints());
    assertEquals(0, dec.noLegs().count());
  }

  @Test
  void encodeQuoteRequestReject_roundTrip_decodesAllFields() {
    SbeTestEncoder.encodeQuoteRequestReject(
        buf,
        0,
        QUOTE_REQ_ID,
        QuoteRejectReasonEnum.InvalidPrice,
        SYMBOL,
        SideEnum.Sell,
        "Outside band",
        NOW);

    hdrDec.wrap(buf, 0);
    assertEquals(QuoteRequestRejectDecoder.TEMPLATE_ID, hdrDec.templateId());
    final var dec = new QuoteRequestRejectDecoder();
    dec.wrap(buf, MessageHeaderDecoder.ENCODED_LENGTH, hdrDec.blockLength(), hdrDec.version());

    assertSbeCharField(dec.quoteReqId(), QUOTE_REQ_ID);
    assertEquals(QuoteRejectReasonEnum.InvalidPrice, dec.quoteRejectReason());
    assertSbeCharField(dec.symbol(), SYMBOL);
    assertEquals(SideEnum.Sell, dec.side());
    assertEquals(NOW, dec.transactTime());
    assertSbeCharField(dec.text(), "Outside band");
    assertEquals(ProductTypeEnum.Spot, dec.productType());
  }

  @Test
  void encodePriceRequest_roundTrip_decodesAllFields() {
    SbeTestEncoder.encodePriceRequest(buf, 0, QUOTE_REQ_ID, SYMBOL, SideEnum.Buy, ORDER_QTY);

    hdrDec.wrap(buf, 0);
    assertEquals(PriceRequestDecoder.TEMPLATE_ID, hdrDec.templateId());
    final var dec = new PriceRequestDecoder();
    dec.wrap(buf, MessageHeaderDecoder.ENCODED_LENGTH, hdrDec.blockLength(), hdrDec.version());

    assertSbeCharField(dec.quoteReqId(), QUOTE_REQ_ID);
    assertSbeCharField(dec.symbol(), SYMBOL);
    assertEquals(SideEnum.Buy, dec.side());
    assertEquals(ORDER_QTY, dec.orderQty());
    assertSbeCharField(dec.accountCode(), ACCOUNT); // helper default
    assertEquals(0L, dec.transactTime()); // 6-arg helper defaults to 0
    assertEquals(ProductTypeEnum.Spot, dec.productType());
    assertSbeCharField(dec.settlDate(), DEFAULT_SETTL_DATE);
    assertEquals(SettlTypeEnum.Regular, dec.settlType());
    assertSbeCharField(dec.currency(), DEFAULT_CURRENCY);
    assertSbeCharField(dec.settlCurrency(), DEFAULT_SETTL_CURRENCY);
    assertEquals(TenorEnum.SN, dec.tenor());
    assertEquals(0, dec.noLegs().count());
  }

  @Test
  void encodePriceRequest_overloadWithTransactTime_passesThroughExplicitly() {
    final long explicitTs = NOW + 67_890L;
    SbeTestEncoder.encodePriceRequest(
        buf, 0, QUOTE_REQ_ID, SYMBOL, SideEnum.Buy, ORDER_QTY, explicitTs);
    final var dec = wrapPriceRequest();
    assertEquals(explicitTs, dec.transactTime()); // proves the parameter is honoured
  }

  @Test
  void encodePriceResponse_acceptedRoundTrip_decodesAllFields() {
    SbeTestEncoder.encodePriceResponse(
        buf, 0, QUOTE_REQ_ID, SYMBOL, /* accepted */ true, BID_PX, OFFER_PX, NOW);

    hdrDec.wrap(buf, 0);
    assertEquals(PriceResponseDecoder.TEMPLATE_ID, hdrDec.templateId());
    final var dec = new PriceResponseDecoder();
    dec.wrap(buf, MessageHeaderDecoder.ENCODED_LENGTH, hdrDec.blockLength(), hdrDec.version());

    assertSbeCharField(dec.quoteReqId(), QUOTE_REQ_ID);
    assertSbeCharField(dec.symbol(), SYMBOL);
    assertEquals(BID_PX, dec.bidPx());
    assertEquals(OFFER_PX, dec.offerPx());
    assertEquals(100_000_000L, dec.bidSize()); // accepted default
    assertEquals(100_000_000L, dec.offerSize()); // accepted default
    assertEquals(NOW + 30_000_000_000L, dec.validUntil()); // transactTime + 30s when accepted
    assertEquals(BooleanType.True, dec.accepted());
    assertEquals(QuoteRejectReasonEnum.NULL_VAL, dec.quoteRejectReason());
    assertEquals(NOW, dec.transactTime());
    assertEquals(ProductTypeEnum.Spot, dec.productType());
    assertEquals(PriceResponseDecoder.swapPointsNullValue(), dec.swapPoints());
    assertEquals(0, dec.noLegs().count());
  }

  @Test
  void encodePriceResponse_rejectedRoundTrip_setsNullSizesAndRejectReason() {
    SbeTestEncoder.encodePriceResponse(
        buf,
        0,
        QUOTE_REQ_ID,
        SYMBOL,
        /* accepted */ false,
        PriceResponseDecoder.bidPxNullValue(),
        PriceResponseDecoder.offerPxNullValue(),
        NOW);

    hdrDec.wrap(buf, 0);
    final var dec = new PriceResponseDecoder();
    dec.wrap(buf, MessageHeaderDecoder.ENCODED_LENGTH, hdrDec.blockLength(), hdrDec.version());

    assertEquals(BooleanType.False, dec.accepted());
    assertEquals(PriceResponseDecoder.bidSizeNullValue(), dec.bidSize());
    assertEquals(PriceResponseDecoder.offerSizeNullValue(), dec.offerSize());
    assertEquals(PriceResponseDecoder.validUntilNullValue(), dec.validUntil());
    assertEquals(QuoteRejectReasonEnum.Other, dec.quoteRejectReason());
  }

  @Test
  void encodePriceValidationRequest_roundTrip_decodesAllFields() {
    SbeTestEncoder.encodePriceValidationRequest(
        buf, 0, QUOTE_ID, QUOTE_REQ_ID, SYMBOL, ORDER_QTY, NOW);

    hdrDec.wrap(buf, 0);
    assertEquals(PriceValidationRequestDecoder.TEMPLATE_ID, hdrDec.templateId());
    final var dec = new PriceValidationRequestDecoder();
    dec.wrap(buf, MessageHeaderDecoder.ENCODED_LENGTH, hdrDec.blockLength(), hdrDec.version());

    assertSbeCharField(dec.quoteId(), QUOTE_ID);
    assertSbeCharField(dec.quoteReqId(), QUOTE_REQ_ID);
    assertSbeCharField(dec.symbol(), SYMBOL);
    assertEquals(SideEnum.Buy, dec.side()); // helper default
    assertEquals(PriceValidationRequestDecoder.priceNullValue(), dec.price()); // explicit null
    assertEquals(ORDER_QTY, dec.orderQty());
    assertSbeCharField(dec.accountCode(), ACCOUNT);
    assertEquals(NOW, dec.transactTime());
    assertEquals(ProductTypeEnum.Spot, dec.productType());
    assertSbeCharField(dec.settlDate(), DEFAULT_SETTL_DATE);
    assertEquals(SettlTypeEnum.Regular, dec.settlType());
    assertSbeCharField(dec.currency(), DEFAULT_CURRENCY);
    assertSbeCharField(dec.settlCurrency(), DEFAULT_SETTL_CURRENCY);
    assertEquals(TenorEnum.SN, dec.tenor());
    assertEquals(0, dec.noLegs().count());
  }

  @Test
  void encodePriceValidationResponse_validRoundTrip_setsNullRejectReason() {
    SbeTestEncoder.encodePriceValidationResponse(buf, 0, QUOTE_ID, /* valid */ true, NOW);

    hdrDec.wrap(buf, 0);
    assertEquals(PriceValidationResponseDecoder.TEMPLATE_ID, hdrDec.templateId());
    final var dec = new PriceValidationResponseDecoder();
    dec.wrap(buf, MessageHeaderDecoder.ENCODED_LENGTH, hdrDec.blockLength(), hdrDec.version());

    assertSbeCharField(dec.quoteId(), QUOTE_ID);
    assertEquals(BooleanType.True, dec.valid());
    assertEquals(NOW, dec.transactTime());
    assertSbeCharField(dec.text(), "");
  }

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  private QuoteRequestDecoder wrapQuoteRequest() {
    hdrDec.wrap(buf, 0);
    final var dec = new QuoteRequestDecoder();
    dec.wrap(buf, MessageHeaderDecoder.ENCODED_LENGTH, hdrDec.blockLength(), hdrDec.version());
    return dec;
  }

  private PriceRequestDecoder wrapPriceRequest() {
    hdrDec.wrap(buf, 0);
    final var dec = new PriceRequestDecoder();
    dec.wrap(buf, MessageHeaderDecoder.ENCODED_LENGTH, hdrDec.blockLength(), hdrDec.version());
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
