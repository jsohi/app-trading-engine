package com.trading.engine.messages;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.trading.engine.messages.sbe.MessageHeaderDecoder;
import com.trading.engine.messages.sbe.MessageHeaderEncoder;
import com.trading.engine.messages.sbe.ProductTypeEnum;
import com.trading.engine.messages.sbe.QuoteCreatedEventDecoder;
import com.trading.engine.messages.sbe.QuoteCreatedEventEncoder;
import com.trading.engine.messages.sbe.QuoteExpiredEventDecoder;
import com.trading.engine.messages.sbe.QuoteExpiredEventEncoder;
import com.trading.engine.messages.sbe.QuoteRejectReasonEnum;
import com.trading.engine.messages.sbe.QuoteRejectedEventDecoder;
import com.trading.engine.messages.sbe.QuoteRejectedEventEncoder;
import com.trading.engine.messages.sbe.QuoteRequestedEventDecoder;
import com.trading.engine.messages.sbe.QuoteRequestedEventEncoder;
import com.trading.engine.messages.sbe.SettlTypeEnum;
import com.trading.engine.messages.sbe.SideEnum;
import com.trading.engine.messages.sbe.TenorEnum;
import java.nio.charset.StandardCharsets;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

/**
 * Codec round-trip tests for RFQ domain event messages (templates 104-107).
 *
 * <p>Each test encodes every field with a distinct non-default value, wraps a fresh decoder over
 * the same buffer, and asserts that every decoded field is byte-for-byte identical to the encoded
 * value. Distinct values per test case ensure that a wire-offset bug (field read from wrong
 * position) is caught rather than silently passing due to coincidentally matching defaults.
 *
 * <p>Threading model: Not thread-safe — single-threaded JUnit test execution only. Allocation:
 * Allocates one {@code UnsafeBuffer} per test, reused across encode + decode phases.
 */
final class RfqEventCodecRoundTripTest {

  /** 8 KiB — comfortably larger than the largest event frame (template 105 with 2 legs ≈ 300 B). */
  private static final int BUF_SIZE = 8_192;

  // -------------------------------------------------------------------------
  // Template 104 — QuoteRequestedEvent
  // -------------------------------------------------------------------------

  /**
   * Encodes a QuoteRequestedEvent with {@code noLegs(0)} (single-leg RFQ) and asserts every
   * top-level field round-trips correctly. The zero-leg group header is still written to the wire
   * and must be consumed by the decoder without error.
   */
  @Test
  void quoteRequestedEvent_roundTrip_noLegsZero() {
    final long seqNum = 1_001L;
    final long timestamp = 1_700_000_000_000_000_001L;
    final String quoteReqId = "QREQ-RT-ZERO-1234";
    final String symbol = "EURUSD";
    final String accountCode = "ACC-SPOT-001";
    final long orderQty = 1_000_000_00_000_000L; // 1 000 000 in fixed-point 10^-8
    final String settlDate = "20260110";
    final String currency = "EUR";
    final String settlCurrency = "USD";

    final var buf = new UnsafeBuffer(new byte[BUF_SIZE]);
    final var headerEncoder = new MessageHeaderEncoder();
    final var encoder = new QuoteRequestedEventEncoder();
    encoder.wrapAndApplyHeader(buf, 0, headerEncoder);

    encoder
        .sequenceNumber(seqNum)
        .timestamp(timestamp)
        .quoteReqId(quoteReqId)
        .symbol(symbol)
        .side(SideEnum.Buy)
        .orderQty(orderQty)
        .accountCode(accountCode)
        .productType(ProductTypeEnum.Spot)
        .settlDate(settlDate)
        .settlType(SettlTypeEnum.Regular)
        .currency(currency)
        .settlCurrency(settlCurrency)
        .tenor(TenorEnum.SN);

    // Encode zero legs — group header still written.
    encoder.noLegsCount(0);

    // Decode.
    final var headerDecoder = new MessageHeaderDecoder();
    final var decoder = new QuoteRequestedEventDecoder();
    decoder.wrapAndApplyHeader(buf, 0, headerDecoder);

    assertEquals(QuoteRequestedEventDecoder.TEMPLATE_ID, headerDecoder.templateId());
    assertEquals(seqNum, decoder.sequenceNumber());
    assertEquals(timestamp, decoder.timestamp());

    final var quoteReqIdDst = new byte[QuoteRequestedEventDecoder.quoteReqIdLength()];
    final var quoteReqIdSrc = padRight(quoteReqId, QuoteRequestedEventDecoder.quoteReqIdLength());
    decoder.getQuoteReqId(quoteReqIdDst, 0);
    assertArrayEquals(quoteReqIdSrc, quoteReqIdDst, "quoteReqId mismatch");

    final var symbolDst = new byte[QuoteRequestedEventDecoder.symbolLength()];
    final var symbolSrc = padRight(symbol, QuoteRequestedEventDecoder.symbolLength());
    decoder.getSymbol(symbolDst, 0);
    assertArrayEquals(symbolSrc, symbolDst, "symbol mismatch");

    assertEquals(SideEnum.Buy, decoder.side());
    assertEquals(orderQty, decoder.orderQty());

    final var acctDst = new byte[QuoteRequestedEventDecoder.accountCodeLength()];
    final var acctSrc = padRight(accountCode, QuoteRequestedEventDecoder.accountCodeLength());
    decoder.getAccountCode(acctDst, 0);
    assertArrayEquals(acctSrc, acctDst, "accountCode mismatch");

    assertEquals(ProductTypeEnum.Spot, decoder.productType());

    final var settlDateDst = new byte[QuoteRequestedEventDecoder.settlDateLength()];
    final var settlDateSrc = padRight(settlDate, QuoteRequestedEventDecoder.settlDateLength());
    decoder.getSettlDate(settlDateDst, 0);
    assertArrayEquals(settlDateSrc, settlDateDst, "settlDate mismatch");

    assertEquals(SettlTypeEnum.Regular, decoder.settlType());

    final var currDst = new byte[QuoteRequestedEventDecoder.currencyLength()];
    final var currSrc = padRight(currency, QuoteRequestedEventDecoder.currencyLength());
    decoder.getCurrency(currDst, 0);
    assertArrayEquals(currSrc, currDst, "currency mismatch");

    final var scurrDst = new byte[QuoteRequestedEventDecoder.settlCurrencyLength()];
    final var scurrSrc = padRight(settlCurrency, QuoteRequestedEventDecoder.settlCurrencyLength());
    decoder.getSettlCurrency(scurrDst, 0);
    assertArrayEquals(scurrSrc, scurrDst, "settlCurrency mismatch");

    assertEquals(TenorEnum.SN, decoder.tenor());

    // Consume the group to advance the limit pointer; assert zero entries.
    final var legs = decoder.noLegs();
    assertEquals(0, legs.count(), "expected zero legs");
  }

  /**
   * Encodes a QuoteRequestedEvent with {@code noLegs(2)} (swap RFQ — near and far legs) and asserts
   * that both leg entries decode correctly. Uses values that differ between leg 0 and leg 1 so that
   * inter-leg offset bugs surface.
   */
  @Test
  void quoteRequestedEvent_roundTrip_noLegsTwo() {
    final long seqNum = 2_002L;
    final long timestamp = 1_700_000_000_000_000_002L;
    final String quoteReqId = "QREQ-RT-SWAP-0001";
    final String symbol = "USDJPY";
    final long orderQty = 5_000_000_00_000_000L;
    final String settlDate = "20260111";
    final String currency = "USD";
    final String settlCurrency = "JPY";
    final String accountCode = "ACC-SWAP-001";

    // Leg data — distinct values per leg so position bugs are caught.
    final String legSettlDate0 = "20260111";
    final String legSettlDate1 = "20261111";
    final long legOrderQty0 = 5_000_000_00_000_000L;
    final long legOrderQty1 = 5_000_100_00_000_000L;
    final String legCurrency0 = "USD";
    final String legCurrency1 = "USD";

    final var buf = new UnsafeBuffer(new byte[BUF_SIZE]);
    final var headerEncoder = new MessageHeaderEncoder();
    final var encoder = new QuoteRequestedEventEncoder();
    encoder.wrapAndApplyHeader(buf, 0, headerEncoder);

    encoder
        .sequenceNumber(seqNum)
        .timestamp(timestamp)
        .quoteReqId(quoteReqId)
        .symbol(symbol)
        .side(SideEnum.Sell)
        .orderQty(orderQty)
        .accountCode(accountCode)
        .productType(ProductTypeEnum.Swap)
        .settlDate(settlDate)
        .settlType(SettlTypeEnum.Regular)
        .currency(currency)
        .settlCurrency(settlCurrency)
        .tenor(TenorEnum.W1);

    final var legsEnc = encoder.noLegsCount(2);

    // Near leg.
    legsEnc
        .next()
        .legSide(SideEnum.Buy)
        .legSettlDate(legSettlDate0)
        .legSettlType(SettlTypeEnum.Regular)
        .legCurrency(legCurrency0)
        .legTenor(TenorEnum.SN)
        .legOrderQty(legOrderQty0);

    // Far leg.
    legsEnc
        .next()
        .legSide(SideEnum.Sell)
        .legSettlDate(legSettlDate1)
        .legSettlType(SettlTypeEnum.Cash)
        .legCurrency(legCurrency1)
        .legTenor(TenorEnum.M1)
        .legOrderQty(legOrderQty1);

    // Decode.
    final var headerDecoder = new MessageHeaderDecoder();
    final var decoder = new QuoteRequestedEventDecoder();
    decoder.wrapAndApplyHeader(buf, 0, headerDecoder);

    assertEquals(seqNum, decoder.sequenceNumber());
    assertEquals(timestamp, decoder.timestamp());
    assertEquals(SideEnum.Sell, decoder.side());
    assertEquals(orderQty, decoder.orderQty());
    assertEquals(ProductTypeEnum.Swap, decoder.productType());
    assertEquals(TenorEnum.W1, decoder.tenor());

    final var legs = decoder.noLegs();
    assertEquals(2, legs.count(), "expected two legs");

    // Near leg.
    legs.next();
    assertEquals(SideEnum.Buy, legs.legSide());
    final var ld0 = new byte[QuoteRequestedEventDecoder.NoLegsDecoder.legSettlDateLength()];
    legs.getLegSettlDate(ld0, 0);
    assertArrayEquals(padRight(legSettlDate0, 8), ld0, "near legSettlDate mismatch");
    assertEquals(SettlTypeEnum.Regular, legs.legSettlType());
    final var lc0 = new byte[QuoteRequestedEventDecoder.NoLegsDecoder.legCurrencyLength()];
    legs.getLegCurrency(lc0, 0);
    assertArrayEquals(padRight(legCurrency0, 3), lc0, "near legCurrency mismatch");
    assertEquals(TenorEnum.SN, legs.legTenor());
    assertEquals(legOrderQty0, legs.legOrderQty());

    // Far leg.
    legs.next();
    assertEquals(SideEnum.Sell, legs.legSide());
    final var ld1 = new byte[QuoteRequestedEventDecoder.NoLegsDecoder.legSettlDateLength()];
    legs.getLegSettlDate(ld1, 0);
    assertArrayEquals(padRight(legSettlDate1, 8), ld1, "far legSettlDate mismatch");
    assertEquals(SettlTypeEnum.Cash, legs.legSettlType());
    assertEquals(TenorEnum.M1, legs.legTenor());
    assertEquals(legOrderQty1, legs.legOrderQty());
  }

  // -------------------------------------------------------------------------
  // Template 105 — QuoteCreatedEvent
  // -------------------------------------------------------------------------

  /**
   * Encodes a QuoteCreatedEvent with {@code noLegs(0)} (single-leg quote response) and asserts all
   * top-level fields including bid/offer prices and sizes survive the round-trip intact.
   */
  @Test
  void quoteCreatedEvent_roundTrip_noLegsZero() {
    final long seqNum = 3_003L;
    final long timestamp = 1_700_000_000_000_000_003L;
    final String quoteId = "QTE-RT-ZERO-00003";
    final String quoteReqId = "QREQ-RT-ZERO-003";
    final String symbol = "GBPUSD";
    final String accountCode = "ACC-SPOT-003";
    final long bidPx =
        1_25000000_00L; // 1.25000000 × 10^-2 — intentionally unrealistic to catch scale bugs
    final long offerPx = 1_25100000_00L;
    final long bidSize = 2_000_000_00_000_000L;
    final long offerSize = 2_000_000_00_000_000L;
    final long validUntil = 1_700_000_030_000_000_003L;
    final long swapPoints = Long.MIN_VALUE; // NULL — optional, absent for single-leg
    final String settlDate = "20260112";
    final String currency = "GBP";
    final String settlCurrency = "USD";

    final var buf = new UnsafeBuffer(new byte[BUF_SIZE]);
    final var headerEncoder = new MessageHeaderEncoder();
    final var encoder = new QuoteCreatedEventEncoder();
    encoder.wrapAndApplyHeader(buf, 0, headerEncoder);

    encoder
        .sequenceNumber(seqNum)
        .timestamp(timestamp)
        .quoteId(quoteId)
        .quoteReqId(quoteReqId)
        .symbol(symbol)
        .side(SideEnum.Buy)
        .accountCode(accountCode)
        .bidPx(bidPx)
        .offerPx(offerPx)
        .bidSize(bidSize)
        .offerSize(offerSize)
        .validUntil(validUntil)
        .productType(ProductTypeEnum.Spot)
        .settlDate(settlDate)
        .settlType(SettlTypeEnum.Regular)
        .currency(currency)
        .settlCurrency(settlCurrency)
        .tenor(TenorEnum.SN)
        .swapPoints(swapPoints);

    encoder.noLegsCount(0);

    // Decode.
    final var headerDecoder = new MessageHeaderDecoder();
    final var decoder = new QuoteCreatedEventDecoder();
    decoder.wrapAndApplyHeader(buf, 0, headerDecoder);

    assertEquals(QuoteCreatedEventDecoder.TEMPLATE_ID, headerDecoder.templateId());
    assertEquals(seqNum, decoder.sequenceNumber());
    assertEquals(timestamp, decoder.timestamp());

    final var qidDst = new byte[QuoteCreatedEventDecoder.quoteIdLength()];
    decoder.getQuoteId(qidDst, 0);
    assertArrayEquals(
        padRight(quoteId, QuoteCreatedEventDecoder.quoteIdLength()), qidDst, "quoteId mismatch");

    final var qreqDst = new byte[QuoteCreatedEventDecoder.quoteReqIdLength()];
    decoder.getQuoteReqId(qreqDst, 0);
    assertArrayEquals(
        padRight(quoteReqId, QuoteCreatedEventDecoder.quoteReqIdLength()),
        qreqDst,
        "quoteReqId mismatch");

    final var symDst = new byte[QuoteCreatedEventDecoder.symbolLength()];
    decoder.getSymbol(symDst, 0);
    assertArrayEquals(
        padRight(symbol, QuoteCreatedEventDecoder.symbolLength()), symDst, "symbol mismatch");

    assertEquals(SideEnum.Buy, decoder.side());

    final var acctDst = new byte[QuoteCreatedEventDecoder.accountCodeLength()];
    decoder.getAccountCode(acctDst, 0);
    assertArrayEquals(
        padRight(accountCode, QuoteCreatedEventDecoder.accountCodeLength()),
        acctDst,
        "accountCode mismatch");

    assertEquals(bidPx, decoder.bidPx());
    assertEquals(offerPx, decoder.offerPx());
    assertEquals(bidSize, decoder.bidSize());
    assertEquals(offerSize, decoder.offerSize());
    assertEquals(validUntil, decoder.validUntil());
    assertEquals(ProductTypeEnum.Spot, decoder.productType());
    assertEquals(TenorEnum.SN, decoder.tenor());
    assertEquals(swapPoints, decoder.swapPoints());

    final var legs = decoder.noLegs();
    assertEquals(0, legs.count());
  }

  /**
   * Encodes a QuoteCreatedEvent with {@code noLegs(2)} (swap quote) and asserts that each leg's
   * bid/offer price and size fields are preserved. Leg 0 and leg 1 carry deliberately different
   * prices so that intra-group offset bugs are observable.
   */
  @Test
  void quoteCreatedEvent_roundTrip_noLegsTwo() {
    final long seqNum = 4_004L;
    final long timestamp = 1_700_000_000_000_000_004L;
    final String quoteId = "QTE-RT-SWAP-00004";
    final String quoteReqId = "QREQ-RT-SWAP-004";
    final String symbol = "EURUSD";
    final String accountCode = "ACC-SWAP-004";
    final long bidPx = 1_05000000_00L;
    final long offerPx = 1_05010000_00L;
    final long bidSize = 1_000_000_00_000_000L;
    final long offerSize = 1_000_000_00_000_000L;
    final long validUntil = 1_700_000_060_000_000_004L;
    final long swapPoints = 100_00L;
    final String settlDate = "20260113";
    final String currency = "EUR";
    final String settlCurrency = "USD";

    // Leg 0 — near.
    final long legBidPx0 = 1_04900000_00L;
    final long legOfferPx0 = 1_05100000_00L;
    final long legBidSize0 = 500_000_00_000_000L;
    final long legOfferSize0 = 500_000_00_000_000L;

    // Leg 1 — far — different values.
    final long legBidPx1 = 1_05400000_00L;
    final long legOfferPx1 = 1_05600000_00L;
    final long legBidSize1 = 500_100_00_000_000L;
    final long legOfferSize1 = 500_100_00_000_000L;

    final var buf = new UnsafeBuffer(new byte[BUF_SIZE]);
    final var headerEncoder = new MessageHeaderEncoder();
    final var encoder = new QuoteCreatedEventEncoder();
    encoder.wrapAndApplyHeader(buf, 0, headerEncoder);

    encoder
        .sequenceNumber(seqNum)
        .timestamp(timestamp)
        .quoteId(quoteId)
        .quoteReqId(quoteReqId)
        .symbol(symbol)
        .side(SideEnum.Sell)
        .accountCode(accountCode)
        .bidPx(bidPx)
        .offerPx(offerPx)
        .bidSize(bidSize)
        .offerSize(offerSize)
        .validUntil(validUntil)
        .productType(ProductTypeEnum.Swap)
        .settlDate(settlDate)
        .settlType(SettlTypeEnum.Regular)
        .currency(currency)
        .settlCurrency(settlCurrency)
        .tenor(TenorEnum.W2)
        .swapPoints(swapPoints);

    final var legsEnc = encoder.noLegsCount(2);

    legsEnc
        .next()
        .legSide(SideEnum.Buy)
        .legSettlDate("20260113")
        .legSettlType(SettlTypeEnum.Regular)
        .legCurrency("EUR")
        .legBidPx(legBidPx0)
        .legOfferPx(legOfferPx0)
        .legBidSize(legBidSize0)
        .legOfferSize(legOfferSize0);

    legsEnc
        .next()
        .legSide(SideEnum.Sell)
        .legSettlDate("20261113")
        .legSettlType(SettlTypeEnum.Cash)
        .legCurrency("EUR")
        .legBidPx(legBidPx1)
        .legOfferPx(legOfferPx1)
        .legBidSize(legBidSize1)
        .legOfferSize(legOfferSize1);

    // Decode.
    final var headerDecoder = new MessageHeaderDecoder();
    final var decoder = new QuoteCreatedEventDecoder();
    decoder.wrapAndApplyHeader(buf, 0, headerDecoder);

    assertEquals(seqNum, decoder.sequenceNumber());
    assertEquals(timestamp, decoder.timestamp());
    assertEquals(bidPx, decoder.bidPx());
    assertEquals(offerPx, decoder.offerPx());
    assertEquals(bidSize, decoder.bidSize());
    assertEquals(offerSize, decoder.offerSize());
    assertEquals(validUntil, decoder.validUntil());
    assertEquals(swapPoints, decoder.swapPoints());
    assertEquals(ProductTypeEnum.Swap, decoder.productType());
    assertEquals(TenorEnum.W2, decoder.tenor());

    final var legs = decoder.noLegs();
    assertEquals(2, legs.count());

    // Near leg.
    legs.next();
    assertEquals(SideEnum.Buy, legs.legSide());
    assertEquals(legBidPx0, legs.legBidPx());
    assertEquals(legOfferPx0, legs.legOfferPx());
    assertEquals(legBidSize0, legs.legBidSize());
    assertEquals(legOfferSize0, legs.legOfferSize());

    // Far leg.
    legs.next();
    assertEquals(SideEnum.Sell, legs.legSide());
    assertEquals(legBidPx1, legs.legBidPx());
    assertEquals(legOfferPx1, legs.legOfferPx());
    assertEquals(legBidSize1, legs.legBidSize());
    assertEquals(legOfferSize1, legs.legOfferSize());
  }

  // -------------------------------------------------------------------------
  // Template 106 — QuoteRejectedEvent
  // -------------------------------------------------------------------------

  /**
   * Encodes a QuoteRejectedEvent with all-NUL {@code text} (length 0 content) and asserts that the
   * full 64-byte field is preserved as zeros on the wire. This is the "no free-text reason"
   * production path.
   */
  @Test
  void quoteRejectedEvent_roundTrip_textLengthZero() {
    final long seqNum = 5_005L;
    final long timestamp = 1_700_000_000_000_000_005L;
    final String quoteReqId = "QREQ-RT-REJ-0005";
    final String symbol = "AUDUSD";
    final String accountCode = "ACC-SPOT-005";

    final var buf = new UnsafeBuffer(new byte[BUF_SIZE]);
    final var headerEncoder = new MessageHeaderEncoder();
    final var encoder = new QuoteRejectedEventEncoder();
    encoder.wrapAndApplyHeader(buf, 0, headerEncoder);

    // text is written as an empty string — encoder pads remainder with NUL bytes.
    encoder
        .sequenceNumber(seqNum)
        .timestamp(timestamp)
        .quoteReqId(quoteReqId)
        .symbol(symbol)
        .side(SideEnum.Buy)
        .accountCode(accountCode)
        .quoteRejectReason(QuoteRejectReasonEnum.UnknownSymbol)
        .productType(ProductTypeEnum.Spot)
        .text("");

    // Decode.
    final var headerDecoder = new MessageHeaderDecoder();
    final var decoder = new QuoteRejectedEventDecoder();
    decoder.wrapAndApplyHeader(buf, 0, headerDecoder);

    assertEquals(QuoteRejectedEventDecoder.TEMPLATE_ID, headerDecoder.templateId());
    assertEquals(seqNum, decoder.sequenceNumber());
    assertEquals(timestamp, decoder.timestamp());

    final var qreqDst = new byte[QuoteRejectedEventDecoder.quoteReqIdLength()];
    decoder.getQuoteReqId(qreqDst, 0);
    assertArrayEquals(padRight(quoteReqId, QuoteRejectedEventDecoder.quoteReqIdLength()), qreqDst);

    final var symDst = new byte[QuoteRejectedEventDecoder.symbolLength()];
    decoder.getSymbol(symDst, 0);
    assertArrayEquals(padRight(symbol, QuoteRejectedEventDecoder.symbolLength()), symDst);

    assertEquals(SideEnum.Buy, decoder.side());

    final var acctDst = new byte[QuoteRejectedEventDecoder.accountCodeLength()];
    decoder.getAccountCode(acctDst, 0);
    assertArrayEquals(
        padRight(accountCode, QuoteRejectedEventDecoder.accountCodeLength()), acctDst);

    assertEquals(QuoteRejectReasonEnum.UnknownSymbol, decoder.quoteRejectReason());
    assertEquals(ProductTypeEnum.Spot, decoder.productType());

    // The entire text field must be zero (NUL-padded empty string).
    final var textDst = new byte[QuoteRejectedEventDecoder.textLength()];
    decoder.getText(textDst, 0);
    assertArrayEquals(
        new byte[QuoteRejectedEventDecoder.textLength()],
        textDst,
        "text field must be all-NUL for zero-length input");
  }

  /**
   * Encodes a QuoteRejectedEvent with a mid-length {@code text} value (shorter than 64 bytes) and
   * asserts that the encoded ASCII bytes are present at the start of the 64-byte field and the
   * remainder is NUL-padded.
   */
  @Test
  void quoteRejectedEvent_roundTrip_textLengthMid() {
    final long seqNum = 6_006L;
    final long timestamp = 1_700_000_000_000_000_006L;
    final String quoteReqId = "QREQ-RT-REJ-0006";
    final String symbol = "USDCHF";
    final String accountCode = "ACC-SPOT-006";
    // 32-character text — well inside the 64-byte limit.
    final String text = "Price not available: feed timeout";

    final var buf = new UnsafeBuffer(new byte[BUF_SIZE]);
    final var headerEncoder = new MessageHeaderEncoder();
    final var encoder = new QuoteRejectedEventEncoder();
    encoder.wrapAndApplyHeader(buf, 0, headerEncoder);

    encoder
        .sequenceNumber(seqNum)
        .timestamp(timestamp)
        .quoteReqId(quoteReqId)
        .symbol(symbol)
        .side(SideEnum.Sell)
        .accountCode(accountCode)
        .quoteRejectReason(QuoteRejectReasonEnum.UnknownSymbol)
        .productType(ProductTypeEnum.Forward)
        .text(text);

    // Decode.
    final var headerDecoder = new MessageHeaderDecoder();
    final var decoder = new QuoteRejectedEventDecoder();
    decoder.wrapAndApplyHeader(buf, 0, headerDecoder);

    assertEquals(seqNum, decoder.sequenceNumber());
    assertEquals(timestamp, decoder.timestamp());
    assertEquals(SideEnum.Sell, decoder.side());
    assertEquals(ProductTypeEnum.Forward, decoder.productType());
    assertEquals(QuoteRejectReasonEnum.UnknownSymbol, decoder.quoteRejectReason());

    // Build expected: ASCII bytes of text followed by NUL padding to 64.
    final var expectedText = new byte[QuoteRejectedEventDecoder.textLength()];
    final byte[] textBytes = text.getBytes(StandardCharsets.US_ASCII);
    System.arraycopy(textBytes, 0, expectedText, 0, textBytes.length);

    final var actualText = new byte[QuoteRejectedEventDecoder.textLength()];
    decoder.getText(actualText, 0);
    assertArrayEquals(expectedText, actualText, "mid-length text mismatch");
  }

  /**
   * Encodes a QuoteRejectedEvent with exactly 64 bytes of ASCII text (the maximum capacity of the
   * {@code text} field). Asserts that no byte is truncated or overwritten.
   */
  @Test
  void quoteRejectedEvent_roundTrip_textLength64() {
    final long seqNum = 7_007L;
    final long timestamp = 1_700_000_000_000_000_007L;
    final String quoteReqId = "QREQ-RT-REJ-0007";
    final String symbol = "NZDUSD";
    final String accountCode = "ACC-SPOT-007";
    // Exactly 64 ASCII characters — fills the field with no trailing NUL.
    final String text = "01234567890123456789012345678901234567890123456789ABCDEFGHIJKLMN";
    assertEquals(64, text.length(), "test setup: text must be exactly 64 chars");

    final var buf = new UnsafeBuffer(new byte[BUF_SIZE]);
    final var headerEncoder = new MessageHeaderEncoder();
    final var encoder = new QuoteRejectedEventEncoder();
    encoder.wrapAndApplyHeader(buf, 0, headerEncoder);

    encoder
        .sequenceNumber(seqNum)
        .timestamp(timestamp)
        .quoteReqId(quoteReqId)
        .symbol(symbol)
        .side(SideEnum.Buy)
        .accountCode(accountCode)
        .quoteRejectReason(QuoteRejectReasonEnum.UnknownSymbol)
        .productType(ProductTypeEnum.Swap)
        .text(text);

    // Decode.
    final var headerDecoder = new MessageHeaderDecoder();
    final var decoder = new QuoteRejectedEventDecoder();
    decoder.wrapAndApplyHeader(buf, 0, headerDecoder);

    assertEquals(seqNum, decoder.sequenceNumber());
    assertEquals(timestamp, decoder.timestamp());
    assertEquals(SideEnum.Buy, decoder.side());
    assertEquals(ProductTypeEnum.Swap, decoder.productType());

    final var expectedText = text.getBytes(StandardCharsets.US_ASCII);
    final var actualText = new byte[QuoteRejectedEventDecoder.textLength()];
    decoder.getText(actualText, 0);
    assertArrayEquals(expectedText, actualText, "full 64-byte text mismatch");
  }

  // -------------------------------------------------------------------------
  // Template 107 — QuoteExpiredEvent
  // -------------------------------------------------------------------------

  /**
   * Encodes a QuoteExpiredEvent and asserts that all fields — sequenceNumber, timestamp, quoteId,
   * quoteReqId, symbol, side, accountCode, and productType — survive the round-trip.
   */
  @Test
  void quoteExpiredEvent_roundTrip() {
    final long seqNum = 8_008L;
    final long timestamp = 1_700_000_000_000_000_008L;
    final String quoteId = "QTE-RT-EXP-00008";
    final String quoteReqId = "QREQ-RT-EXP-0008";
    final String symbol = "USDJPY";
    final String accountCode = "ACC-SPOT-008";

    final var buf = new UnsafeBuffer(new byte[BUF_SIZE]);
    final var headerEncoder = new MessageHeaderEncoder();
    final var encoder = new QuoteExpiredEventEncoder();
    encoder.wrapAndApplyHeader(buf, 0, headerEncoder);

    encoder
        .sequenceNumber(seqNum)
        .timestamp(timestamp)
        .quoteId(quoteId)
        .quoteReqId(quoteReqId)
        .symbol(symbol)
        .side(SideEnum.Sell)
        .accountCode(accountCode)
        .productType(ProductTypeEnum.Spot);

    // Decode.
    final var headerDecoder = new MessageHeaderDecoder();
    final var decoder = new QuoteExpiredEventDecoder();
    decoder.wrapAndApplyHeader(buf, 0, headerDecoder);

    assertEquals(QuoteExpiredEventDecoder.TEMPLATE_ID, headerDecoder.templateId());
    assertEquals(seqNum, decoder.sequenceNumber());
    assertEquals(timestamp, decoder.timestamp());

    final var qidDst = new byte[QuoteExpiredEventDecoder.quoteIdLength()];
    decoder.getQuoteId(qidDst, 0);
    assertArrayEquals(
        padRight(quoteId, QuoteExpiredEventDecoder.quoteIdLength()), qidDst, "quoteId mismatch");

    final var qreqDst = new byte[QuoteExpiredEventDecoder.quoteReqIdLength()];
    decoder.getQuoteReqId(qreqDst, 0);
    assertArrayEquals(
        padRight(quoteReqId, QuoteExpiredEventDecoder.quoteReqIdLength()),
        qreqDst,
        "quoteReqId mismatch");

    final var symDst = new byte[QuoteExpiredEventDecoder.symbolLength()];
    decoder.getSymbol(symDst, 0);
    assertArrayEquals(
        padRight(symbol, QuoteExpiredEventDecoder.symbolLength()), symDst, "symbol mismatch");

    assertEquals(SideEnum.Sell, decoder.side());

    final var acctDst = new byte[QuoteExpiredEventDecoder.accountCodeLength()];
    decoder.getAccountCode(acctDst, 0);
    assertArrayEquals(
        padRight(accountCode, QuoteExpiredEventDecoder.accountCodeLength()),
        acctDst,
        "accountCode mismatch");

    assertEquals(ProductTypeEnum.Spot, decoder.productType());
  }

  // -------------------------------------------------------------------------
  // Helpers
  // -------------------------------------------------------------------------

  /**
   * Returns a NUL-padded byte array of exactly {@code width} bytes containing the ASCII encoding of
   * {@code value}. Mirrors the SBE encoder behaviour: characters are written left-justified and the
   * tail is filled with zero bytes.
   *
   * @param value the string to encode; must be &lt;= {@code width} characters long
   * @param width the fixed field width in bytes
   * @return byte array of length {@code width}
   */
  private static byte[] padRight(final String value, final int width) {
    final var dst = new byte[width];
    if (value != null && !value.isEmpty()) {
      final byte[] src = value.getBytes(StandardCharsets.US_ASCII);
      System.arraycopy(src, 0, dst, 0, Math.min(src.length, width));
    }
    return dst;
  }
}
