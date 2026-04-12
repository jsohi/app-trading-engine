package com.trading.engine.gateway;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.trading.engine.fix.builder.ExecutionReportEncoder;
import com.trading.engine.fix.builder.HeaderEncoder;
import com.trading.engine.fix.decoder_flyweight.ExecutionReportDecoder;
import com.trading.engine.messages.sbe.ExecTypeEnum;
import com.trading.engine.messages.sbe.MessageHeaderDecoder;
import com.trading.engine.messages.sbe.MessageHeaderEncoder;
import com.trading.engine.messages.sbe.OrdStatusEnum;
import com.trading.engine.messages.sbe.OrdTypeEnum;
import com.trading.engine.messages.sbe.ProductTypeEnum;
import com.trading.engine.messages.sbe.RejectReasonEnum;
import com.trading.engine.messages.sbe.SettlTypeEnum;
import com.trading.engine.messages.sbe.SideEnum;
import com.trading.engine.messages.sbe.TenorEnum;
import com.trading.engine.messages.sbe.TimeInForceEnum;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.junit.jupiter.api.Test;
import uk.co.real_logic.artio.util.MutableAsciiBuffer;

class SbeToFixTranslatorTest {

  @Test
  void roundTripFilledExecutionReport() {
    // Build an SBE ExecutionReport flyweight (representing a Filled Buy of EUR/USD).
    MutableDirectBuffer sbeBuf = new ExpandableArrayBuffer(512);
    encodeSbeExecReport(
        sbeBuf,
        "ORDER-1",
        "EXEC-1",
        "ORD-1",
        ExecTypeEnum.Fill,
        OrdStatusEnum.Filled,
        "EURUSD",
        SideEnum.Buy,
        0L,
        100_000_000_000L, // 1000.0 cumQty
        110_000_000L, // 1.10 avgPx
        1_712_491_200_000_000_000L, // 2024-04-07T12:00:00 nanos (arbitrary)
        "FILLED",
        "20260409",
        SettlTypeEnum.TPlus2,
        "EUR",
        "USD");

    // Decode the SBE side via flyweight
    com.trading.engine.messages.sbe.ExecutionReportDecoder sbeDec = decodeSbeExecReport(sbeBuf);

    // Translate into a FIX encoder
    ExecutionReportEncoder fix = new ExecutionReportEncoder();
    HeaderEncoder hdr = fix.header();
    hdr.senderCompID("EXCH").targetCompID("CLIENT").msgSeqNum(1);
    hdr.sendingTime("20260407-12:00:00".getBytes());
    new SbeToFixTranslator().translateExecutionReport(sbeDec, fix);

    // Encode FIX to wire and decode it back via Artio's decoder; assert key fields.
    MutableAsciiBuffer wire = new MutableAsciiBuffer(new byte[2048]);
    long encoded = fix.encode(wire, 0);
    int wireOffset = (int) (encoded >>> 32);
    int wireLen = (int) encoded;

    ExecutionReportDecoder fixDec = new ExecutionReportDecoder();
    fixDec.decode(wire, wireOffset, wireLen);

    assertEquals("ORDER-1", fixDec.orderIDAsString());
    assertEquals("EXEC-1", fixDec.execIDAsString());
    assertEquals("ORD-1", fixDec.clOrdIDAsString());
    assertEquals('2', fixDec.execType()); // Fill
    assertEquals('2', fixDec.ordStatus()); // Filled
    assertEquals("EURUSD", fixDec.symbolAsString());
    assertEquals('1', fixDec.side()); // Buy
    assertEquals(0L, fixDec.leavesQty().value());
    // cumQty: 100_000_000_000 → 1000.0; FIX wire normalises to value=1000, scale=0
    assertEquals(1000L, fixDec.cumQty().value());
    assertEquals(0, fixDec.cumQty().scale());
    // avgPx: 110_000_000 → 1.1; FIX wire normalises to value=11, scale=1
    assertEquals(11L, fixDec.avgPx().value());
    assertEquals(1, fixDec.avgPx().scale());
    assertEquals("FILLED", fixDec.textAsString());
    assertEquals("EUR", fixDec.currencyAsString());
    assertEquals("USD", fixDec.settlCurrencyAsString());
    assertEquals('3', fixDec.settlType()); // T+2
    assertEquals("20260409", fixDec.settlDateAsString());
  }

  @Test
  void rejectsNullValSideOnSbeSide() {
    MutableDirectBuffer sbeBuf = new ExpandableArrayBuffer(512);
    encodeSbeExecReport(
        sbeBuf,
        "ORD-X",
        "EXE-X",
        "C-X",
        ExecTypeEnum.New,
        OrdStatusEnum.New,
        "X",
        SideEnum.NULL_VAL, // unsupported on FIX wire
        1L,
        0L,
        0L,
        1L,
        "",
        "",
        SettlTypeEnum.NULL_VAL,
        "",
        "");

    com.trading.engine.messages.sbe.ExecutionReportDecoder sbeDec = decodeSbeExecReport(sbeBuf);
    ExecutionReportEncoder fix = new ExecutionReportEncoder();
    HeaderEncoder hdr = fix.header();
    hdr.senderCompID("EXCH").targetCompID("CLIENT").msgSeqNum(1);
    hdr.sendingTime("20260407-12:00:00".getBytes());

    SbeToFixTranslator translator = new SbeToFixTranslator();
    assertThrows(
        IllegalStateException.class, () -> translator.translateExecutionReport(sbeDec, fix));
  }

  // -------------------------------------------------------------------------
  // Helpers
  // -------------------------------------------------------------------------

  private static void encodeSbeExecReport(
      MutableDirectBuffer buf,
      String orderId,
      String execId,
      String clOrdId,
      ExecTypeEnum execType,
      OrdStatusEnum ordStatus,
      String symbol,
      SideEnum side,
      long leavesQty,
      long cumQty,
      long avgPx,
      long transactTimeNanos,
      String text,
      String settlDate,
      SettlTypeEnum settlType,
      String currency,
      String settlCurrency) {
    com.trading.engine.messages.sbe.ExecutionReportEncoder enc =
        new com.trading.engine.messages.sbe.ExecutionReportEncoder();
    enc.wrapAndApplyHeader(buf, 0, new MessageHeaderEncoder());
    enc.clOrdId(clOrdId);
    enc.orderId(orderId);
    enc.execId(execId);
    enc.quoteId("");
    enc.execType(execType);
    enc.ordStatus(ordStatus);
    enc.symbol(symbol);
    enc.side(side);
    enc.leavesQty(leavesQty);
    enc.cumQty(cumQty);
    enc.avgPx(avgPx);
    enc.transactTime(transactTimeNanos);
    enc.text(text);
    enc.productType(ProductTypeEnum.NULL_VAL);
    enc.settlDate(settlDate);
    enc.settlType(settlType);
    enc.currency(currency);
    enc.settlCurrency(settlCurrency);
    enc.tenor(TenorEnum.NULL_VAL);
    enc.noLegsCount(0);
  }

  private static com.trading.engine.messages.sbe.ExecutionReportDecoder decodeSbeExecReport(
      MutableDirectBuffer buf) {
    MessageHeaderDecoder hdrDec = new MessageHeaderDecoder();
    hdrDec.wrap(buf, 0);
    com.trading.engine.messages.sbe.ExecutionReportDecoder dec =
        new com.trading.engine.messages.sbe.ExecutionReportDecoder();
    dec.wrap(
        buf,
        MessageHeaderDecoder.ENCODED_LENGTH,
        com.trading.engine.messages.sbe.ExecutionReportDecoder.BLOCK_LENGTH,
        com.trading.engine.messages.sbe.ExecutionReportDecoder.SCHEMA_VERSION);
    return dec;
  }

  // ===========================================================================
  // OrderCancelReject (35=9)
  // ===========================================================================

  @Test
  void roundTripOrderCancelReject() {
    MutableDirectBuffer sbeBuf = new ExpandableArrayBuffer(512);
    com.trading.engine.messages.sbe.OrderCancelRejectEncoder enc =
        new com.trading.engine.messages.sbe.OrderCancelRejectEncoder();
    enc.wrapAndApplyHeader(sbeBuf, 0, new MessageHeaderEncoder());
    enc.orderId("ORD-1");
    enc.clOrdId("CXL-1");
    enc.origClOrdId("ORIG-1");
    enc.ordStatus(OrdStatusEnum.Rejected);
    enc.cxlRejResponseTo(com.trading.engine.messages.sbe.CxlRejResponseToEnum.OrderCancelRequest);
    enc.cxlRejReason(com.trading.engine.messages.sbe.CxlRejReasonEnum.UnknownOrder);
    enc.accountCode("ACCT-1");
    enc.transactTime(1_712_491_200_000_000_000L);
    enc.text("Order not found");

    MessageHeaderDecoder hdrDec = new MessageHeaderDecoder();
    hdrDec.wrap(sbeBuf, 0);
    com.trading.engine.messages.sbe.OrderCancelRejectDecoder sbeDec =
        new com.trading.engine.messages.sbe.OrderCancelRejectDecoder();
    sbeDec.wrap(
        sbeBuf,
        MessageHeaderDecoder.ENCODED_LENGTH,
        com.trading.engine.messages.sbe.OrderCancelRejectDecoder.BLOCK_LENGTH,
        com.trading.engine.messages.sbe.OrderCancelRejectDecoder.SCHEMA_VERSION);

    com.trading.engine.fix.builder.OrderCancelRejectEncoder fix =
        new com.trading.engine.fix.builder.OrderCancelRejectEncoder();
    fix.header().senderCompID("EXCH").targetCompID("CLIENT").msgSeqNum(1);
    fix.header().sendingTime("20260407-12:00:00".getBytes());
    new SbeToFixTranslator().translateOrderCancelReject(sbeDec, fix);

    MutableAsciiBuffer wire = new MutableAsciiBuffer(new byte[2048]);
    long encoded = fix.encode(wire, 0);
    int wireOffset = (int) (encoded >>> 32);
    int wireLen = (int) encoded;

    com.trading.engine.fix.decoder_flyweight.OrderCancelRejectDecoder fixDec =
        new com.trading.engine.fix.decoder_flyweight.OrderCancelRejectDecoder();
    fixDec.decode(wire, wireOffset, wireLen);

    assertEquals("ORD-1", fixDec.orderIDAsString());
    assertEquals("CXL-1", fixDec.clOrdIDAsString());
    assertEquals("ORIG-1", fixDec.origClOrdIDAsString());
    assertEquals('8', fixDec.ordStatus()); // Rejected
    assertEquals('1', fixDec.cxlRejResponseTo()); // OrderCancelRequest
    assertEquals(1, fixDec.cxlRejReason()); // UnknownOrder
    assertEquals("ACCT-1", fixDec.accountAsString());
    assertEquals("Order not found", fixDec.textAsString());
  }

  // ===========================================================================
  // Quote (35=S)
  // ===========================================================================

  @Test
  void roundTripQuoteWithoutLegs() {
    MutableDirectBuffer sbeBuf = new ExpandableArrayBuffer(512);
    com.trading.engine.messages.sbe.QuoteEncoder enc =
        new com.trading.engine.messages.sbe.QuoteEncoder();
    enc.wrapAndApplyHeader(sbeBuf, 0, new MessageHeaderEncoder());
    enc.quoteReqId("RFQ-1");
    enc.quoteId("Q-1");
    enc.symbol("EURUSD");
    enc.side(SideEnum.Buy);
    enc.bidPx(110_000_000L); // 1.10
    enc.offerPx(110_050_000L); // 1.1005
    enc.bidSize(100_000_000_000_000L); // 1M
    enc.offerSize(100_000_000_000_000L);
    enc.transactTime(1_712_491_200_000_000_000L);
    enc.quoteStatus(com.trading.engine.messages.sbe.QuoteStatusEnum.Accepted);
    enc.text("");
    enc.productType(ProductTypeEnum.NULL_VAL);
    enc.settlDate("20260409");
    enc.settlType(SettlTypeEnum.TPlus2);
    enc.currency("EUR");
    enc.settlCurrency("USD");
    enc.tenor(TenorEnum.NULL_VAL);
    enc.validUntil(1_712_491_260_000_000_000L); // +60s
    enc.swapPoints(0L);
    enc.noLegsCount(0);

    MessageHeaderDecoder hdrDec = new MessageHeaderDecoder();
    hdrDec.wrap(sbeBuf, 0);
    com.trading.engine.messages.sbe.QuoteDecoder sbeDec =
        new com.trading.engine.messages.sbe.QuoteDecoder();
    sbeDec.wrap(
        sbeBuf,
        MessageHeaderDecoder.ENCODED_LENGTH,
        com.trading.engine.messages.sbe.QuoteDecoder.BLOCK_LENGTH,
        com.trading.engine.messages.sbe.QuoteDecoder.SCHEMA_VERSION);

    com.trading.engine.fix.builder.QuoteEncoder fix =
        new com.trading.engine.fix.builder.QuoteEncoder();
    fix.header().senderCompID("EXCH").targetCompID("CLIENT").msgSeqNum(1);
    fix.header().sendingTime("20260407-12:00:00".getBytes());
    new SbeToFixTranslator().translateQuote(sbeDec, fix);

    MutableAsciiBuffer wire = new MutableAsciiBuffer(new byte[2048]);
    long encoded = fix.encode(wire, 0);
    int wireOffset = (int) (encoded >>> 32);
    int wireLen = (int) encoded;

    com.trading.engine.fix.decoder_flyweight.QuoteDecoder fixDec =
        new com.trading.engine.fix.decoder_flyweight.QuoteDecoder();
    fixDec.decode(wire, wireOffset, wireLen);

    assertEquals("RFQ-1", fixDec.quoteReqIDAsString());
    assertEquals("Q-1", fixDec.quoteIDAsString());
    assertEquals("EURUSD", fixDec.symbolAsString());
    assertEquals('1', fixDec.side()); // Buy
    // bidPx=1.1: Artio normalises to value=11, scale=1
    assertEquals(11L, fixDec.bidPx().value());
    assertEquals(1, fixDec.bidPx().scale());
    // offerPx=1.1005: value=11005, scale=4
    assertEquals(11005L, fixDec.offerPx().value());
    assertEquals(4, fixDec.offerPx().scale());
    assertEquals("EUR", fixDec.currencyAsString());
    assertEquals('3', fixDec.settlType()); // TPlus2
    assertEquals("20260409", fixDec.settlDateAsString());
    // Cover the optional non-null branches that the translator skips on NULL_VAL. Artio
    // normalises trailing zeros differently for different magnitudes, so assert against the
    // unambiguous BigDecimal real value rather than (value, scale) — and use longValueExact
    // (not floating-point) so the assertion is robust to either scale direction.
    assertEquals(
        1_000_000L,
        java.math.BigDecimal.valueOf(fixDec.bidSize().value(), fixDec.bidSize().scale())
            .longValueExact());
    assertEquals(
        1_000_000L,
        java.math.BigDecimal.valueOf(fixDec.offerSize().value(), fixDec.offerSize().scale())
            .longValueExact());
    assertTrue(fixDec.validUntilTimeLength() > 0);
  }

  // ===========================================================================
  // ExecutionReport with noLegs (FX swap fill)
  // ===========================================================================

  @Test
  void roundTripExecutionReportWithLegs() {
    MutableDirectBuffer sbeBuf = new ExpandableArrayBuffer(512);
    com.trading.engine.messages.sbe.ExecutionReportEncoder enc =
        new com.trading.engine.messages.sbe.ExecutionReportEncoder();
    enc.wrapAndApplyHeader(sbeBuf, 0, new MessageHeaderEncoder());
    enc.clOrdId("SWAP-1");
    enc.orderId("ORDER-SWAP-1");
    enc.execId("EXEC-SWAP-1");
    enc.quoteId("");
    enc.execType(ExecTypeEnum.Fill);
    enc.ordStatus(OrdStatusEnum.Filled);
    enc.symbol("EURUSD");
    enc.side(SideEnum.Buy);
    enc.leavesQty(0L);
    enc.cumQty(100_000_000_000L); // 1000.0
    enc.avgPx(110_000_000L);
    enc.transactTime(1_712_491_200_000_000_000L);
    enc.text("");
    enc.productType(ProductTypeEnum.NULL_VAL);
    enc.settlDate("");
    enc.settlType(SettlTypeEnum.NULL_VAL);
    enc.currency("");
    enc.settlCurrency("");
    enc.tenor(TenorEnum.NULL_VAL);

    com.trading.engine.messages.sbe.ExecutionReportEncoder.NoLegsEncoder legs = enc.noLegsCount(2);
    legs.next();
    legs.legSide(SideEnum.Buy);
    legs.legSettlDate("20260409");
    legs.legSettlType(SettlTypeEnum.TPlus2);
    legs.legCurrency("EUR");
    legs.legLastPx(110_000_000L);
    legs.legLastQty(100_000_000_000L);
    legs.legLeavesQty(0L);
    legs.legCumQty(100_000_000_000L);
    legs.next();
    legs.legSide(SideEnum.Sell);
    legs.legSettlDate("20260507");
    legs.legSettlType(SettlTypeEnum.NULL_VAL);
    legs.legCurrency("USD");
    legs.legLastPx(110_500_000L);
    legs.legLastQty(110_000_000_000L);
    legs.legLeavesQty(0L);
    legs.legCumQty(110_000_000_000L);

    MessageHeaderDecoder hdrDec = new MessageHeaderDecoder();
    hdrDec.wrap(sbeBuf, 0);
    com.trading.engine.messages.sbe.ExecutionReportDecoder sbeDec =
        new com.trading.engine.messages.sbe.ExecutionReportDecoder();
    sbeDec.wrap(
        sbeBuf, MessageHeaderDecoder.ENCODED_LENGTH, hdrDec.blockLength(), hdrDec.version());

    ExecutionReportEncoder fix = new ExecutionReportEncoder();
    fix.header().senderCompID("EXCH").targetCompID("CLIENT").msgSeqNum(1);
    fix.header().sendingTime("20260407-12:00:00".getBytes());
    new SbeToFixTranslator().translateExecutionReport(sbeDec, fix);

    MutableAsciiBuffer wire = new MutableAsciiBuffer(new byte[2048]);
    long encoded = fix.encode(wire, 0);
    int wireOffset = (int) (encoded >>> 32);
    int wireLen = (int) encoded;

    com.trading.engine.fix.decoder_flyweight.ExecutionReportDecoder fixDec =
        new com.trading.engine.fix.decoder_flyweight.ExecutionReportDecoder();
    fixDec.decode(wire, wireOffset, wireLen);

    assertEquals("SWAP-1", fixDec.clOrdIDAsString());

    // Walk the legs and assert per-leg field values. This is the critical regression test
    // for the Artio leg.next() linked-list aliasing bug — the pre-fix translator wrote leg 1's
    // values into leg 0's slot, so the count would have been correct (2) but legs[0] would
    // have ended up with leg 1's data (Sell/USD/1.105). Asserting per-leg values catches it.
    com.trading.engine.fix.decoder_flyweight.ExecutionReportDecoder.LegsGroupIterator iter =
        fixDec.legsGroupIterator();
    assertTrue(iter.hasNext());
    com.trading.engine.fix.decoder_flyweight.ExecutionReportDecoder.LegsGroupDecoder leg0 =
        iter.next();
    assertEquals('1', leg0.legSide()); // Buy
    assertEquals("EUR", leg0.legCurrencyAsString());
    assertEquals(11L, leg0.legLastPx().value());
    assertEquals(1, leg0.legLastPx().scale()); // 1.1

    assertTrue(iter.hasNext());
    com.trading.engine.fix.decoder_flyweight.ExecutionReportDecoder.LegsGroupDecoder leg1 =
        iter.next();
    assertEquals('2', leg1.legSide()); // Sell
    assertEquals("USD", leg1.legCurrencyAsString());
    assertEquals(1105L, leg1.legLastPx().value());
    assertEquals(3, leg1.legLastPx().scale()); // 1.105

    assertFalse(iter.hasNext());
  }

  // ===========================================================================
  // Quote with noLegs (FX swap quote)
  // ===========================================================================

  @Test
  void roundTripQuoteWithTwoLegs() {
    MutableDirectBuffer sbeBuf = new ExpandableArrayBuffer(512);
    com.trading.engine.messages.sbe.QuoteEncoder enc =
        new com.trading.engine.messages.sbe.QuoteEncoder();
    enc.wrapAndApplyHeader(sbeBuf, 0, new MessageHeaderEncoder());
    enc.quoteReqId("RFQ-1");
    enc.quoteId("Q-1");
    enc.symbol("EURUSD");
    enc.side(SideEnum.NULL_VAL);
    enc.bidPx(110_000_000L);
    enc.offerPx(110_050_000L);
    enc.bidSize(100_000_000_000_000L);
    enc.offerSize(100_000_000_000_000L);
    enc.transactTime(1_712_491_200_000_000_000L);
    enc.quoteStatus(com.trading.engine.messages.sbe.QuoteStatusEnum.Accepted);
    enc.text("");
    enc.productType(ProductTypeEnum.NULL_VAL);
    enc.settlDate("");
    enc.settlType(SettlTypeEnum.NULL_VAL);
    enc.currency("");
    enc.settlCurrency("");
    enc.tenor(TenorEnum.NULL_VAL);
    enc.validUntil(1_712_491_260_000_000_000L);
    enc.swapPoints(0L);

    com.trading.engine.messages.sbe.QuoteEncoder.NoLegsEncoder qLegs = enc.noLegsCount(2);
    qLegs.next();
    qLegs.legSide(SideEnum.Buy);
    qLegs.legSettlDate("20260409");
    qLegs.legSettlType(SettlTypeEnum.TPlus2);
    qLegs.legCurrency("EUR");
    qLegs.legBidPx(110_000_000L);
    qLegs.legOfferPx(110_050_000L);
    qLegs.legBidSize(0L);
    qLegs.legOfferSize(0L);
    qLegs.next();
    qLegs.legSide(SideEnum.Sell);
    qLegs.legSettlDate("20260507");
    qLegs.legSettlType(SettlTypeEnum.NULL_VAL);
    qLegs.legCurrency("USD");
    qLegs.legBidPx(110_500_000L);
    qLegs.legOfferPx(110_550_000L);
    qLegs.legBidSize(0L);
    qLegs.legOfferSize(0L);

    MessageHeaderDecoder hdrDec = new MessageHeaderDecoder();
    hdrDec.wrap(sbeBuf, 0);
    com.trading.engine.messages.sbe.QuoteDecoder sbeDec =
        new com.trading.engine.messages.sbe.QuoteDecoder();
    sbeDec.wrap(
        sbeBuf, MessageHeaderDecoder.ENCODED_LENGTH, hdrDec.blockLength(), hdrDec.version());

    com.trading.engine.fix.builder.QuoteEncoder fix =
        new com.trading.engine.fix.builder.QuoteEncoder();
    fix.header().senderCompID("EXCH").targetCompID("CLIENT").msgSeqNum(1);
    fix.header().sendingTime("20260407-12:00:00".getBytes());
    new SbeToFixTranslator().translateQuote(sbeDec, fix);

    MutableAsciiBuffer wire = new MutableAsciiBuffer(new byte[2048]);
    long encoded = fix.encode(wire, 0);
    int wireOffset = (int) (encoded >>> 32);
    int wireLen = (int) encoded;

    com.trading.engine.fix.decoder_flyweight.QuoteDecoder fixDec =
        new com.trading.engine.fix.decoder_flyweight.QuoteDecoder();
    fixDec.decode(wire, wireOffset, wireLen);

    com.trading.engine.fix.decoder_flyweight.QuoteDecoder.LegsGroupIterator iter =
        fixDec.legsGroupIterator();
    assertTrue(iter.hasNext());
    com.trading.engine.fix.decoder_flyweight.QuoteDecoder.LegsGroupDecoder leg0 = iter.next();
    assertEquals('1', leg0.legSide());
    assertEquals("EUR", leg0.legCurrencyAsString());
    assertEquals("20260409", leg0.legSettlDateAsString());

    assertTrue(iter.hasNext());
    com.trading.engine.fix.decoder_flyweight.QuoteDecoder.LegsGroupDecoder leg1 = iter.next();
    assertEquals('2', leg1.legSide());
    assertEquals("USD", leg1.legCurrencyAsString());
    assertEquals("20260507", leg1.legSettlDateAsString());

    assertFalse(iter.hasNext());
  }

  // ===========================================================================
  // Edge cases
  // ===========================================================================

  @Test
  void rejectsExecutionReportWithTooManyLegs() {
    MutableDirectBuffer sbeBuf = new ExpandableArrayBuffer(2048);
    com.trading.engine.messages.sbe.ExecutionReportEncoder enc =
        new com.trading.engine.messages.sbe.ExecutionReportEncoder();
    enc.wrapAndApplyHeader(sbeBuf, 0, new MessageHeaderEncoder());
    enc.clOrdId("X");
    enc.orderId("X");
    enc.execId("X");
    enc.quoteId("");
    enc.execType(ExecTypeEnum.New);
    enc.ordStatus(OrdStatusEnum.New);
    enc.symbol("X");
    enc.side(SideEnum.Buy);
    enc.leavesQty(0L);
    enc.cumQty(0L);
    enc.avgPx(0L);
    enc.transactTime(1L);
    enc.text("");
    enc.productType(ProductTypeEnum.NULL_VAL);
    enc.settlDate("");
    enc.settlType(SettlTypeEnum.NULL_VAL);
    enc.currency("");
    enc.settlCurrency("");
    enc.tenor(TenorEnum.NULL_VAL);
    // Encode 9 legs — translator's MAX_LEGS=8 guard should reject.
    com.trading.engine.messages.sbe.ExecutionReportEncoder.NoLegsEncoder legs = enc.noLegsCount(9);
    for (int i = 0; i < 9; i++) {
      legs.next();
      legs.legSide(SideEnum.Buy);
      legs.legSettlDate("");
      legs.legSettlType(SettlTypeEnum.NULL_VAL);
      legs.legCurrency("");
      legs.legLastPx(0L);
      legs.legLastQty(0L);
      legs.legLeavesQty(0L);
      legs.legCumQty(0L);
    }

    MessageHeaderDecoder hdrDec = new MessageHeaderDecoder();
    hdrDec.wrap(sbeBuf, 0);
    com.trading.engine.messages.sbe.ExecutionReportDecoder sbeDec =
        new com.trading.engine.messages.sbe.ExecutionReportDecoder();
    sbeDec.wrap(
        sbeBuf, MessageHeaderDecoder.ENCODED_LENGTH, hdrDec.blockLength(), hdrDec.version());

    ExecutionReportEncoder fix = new ExecutionReportEncoder();
    fix.header().senderCompID("EXCH").targetCompID("CLIENT").msgSeqNum(1);
    fix.header().sendingTime("20260407-12:00:00".getBytes());
    SbeToFixTranslator translator = new SbeToFixTranslator();
    assertThrows(
        IllegalStateException.class, () -> translator.translateExecutionReport(sbeDec, fix));
  }

  // ===========================================================================
  // OrderCreatedEvent → ExecutionReport (ExecType=New)
  // ===========================================================================

  @Test
  void translateOrderCreatedEventMapsAllFields() {
    MutableDirectBuffer sbeBuf = new ExpandableArrayBuffer(512);
    com.trading.engine.messages.sbe.OrderCreatedEventEncoder enc =
        new com.trading.engine.messages.sbe.OrderCreatedEventEncoder();
    enc.wrapAndApplyHeader(sbeBuf, 0, new MessageHeaderEncoder());
    enc.sequenceNumber(1L);
    enc.timestamp(1_712_491_200_000_000_000L);
    enc.orderId("ORDER-1");
    enc.execId("EXEC-1");
    enc.clOrdId("ORD-1");
    enc.symbol("EURUSD");
    enc.side(SideEnum.Buy);
    enc.ordType(OrdTypeEnum.Limit);
    enc.timeInForce(TimeInForceEnum.Day);
    enc.price(110_000_000L); // 1.10
    enc.orderQty(100_000_000_000L); // 1000.0
    enc.quoteId("");
    enc.accountCode("ACCT-1");
    enc.productType(ProductTypeEnum.NULL_VAL);
    enc.settlDate("20260409");
    enc.settlType(SettlTypeEnum.TPlus2);
    enc.currency("EUR");
    enc.settlCurrency("USD");
    enc.tenor(TenorEnum.NULL_VAL);

    MessageHeaderDecoder hdrDec = new MessageHeaderDecoder();
    hdrDec.wrap(sbeBuf, 0);
    com.trading.engine.messages.sbe.OrderCreatedEventDecoder sbeDec =
        new com.trading.engine.messages.sbe.OrderCreatedEventDecoder();
    sbeDec.wrap(
        sbeBuf, MessageHeaderDecoder.ENCODED_LENGTH, hdrDec.blockLength(), hdrDec.version());

    ExecutionReportEncoder fix = new ExecutionReportEncoder();
    fix.header().senderCompID("EXCH").targetCompID("CLIENT").msgSeqNum(1);
    fix.header().sendingTime("20260407-12:00:00".getBytes());
    new SbeToFixTranslator().translateOrderCreatedEvent(sbeDec, fix);

    MutableAsciiBuffer wire = new MutableAsciiBuffer(new byte[2048]);
    long encoded = fix.encode(wire, 0);
    int wireOffset = (int) (encoded >>> 32);
    int wireLen = (int) encoded;

    ExecutionReportDecoder fixDec = new ExecutionReportDecoder();
    fixDec.decode(wire, wireOffset, wireLen);

    assertEquals("ORDER-1", fixDec.orderIDAsString());
    assertEquals("EXEC-1", fixDec.execIDAsString());
    assertEquals("ORD-1", fixDec.clOrdIDAsString());
    assertEquals('0', fixDec.execType()); // New
    assertEquals('0', fixDec.ordStatus()); // New
    assertEquals("EURUSD", fixDec.symbolAsString());
    assertEquals('1', fixDec.side()); // Buy
    // price: 110_000_000 → 1.1; FIX wire normalises to value=11, scale=1
    assertEquals(11L, fixDec.price().value());
    assertEquals(1, fixDec.price().scale());
    // orderQty: 100_000_000_000 → 1000.0
    assertEquals(1000L, fixDec.orderQty().value());
    assertEquals(0, fixDec.orderQty().scale());
    // leavesQty = orderQty for New
    assertEquals(1000L, fixDec.leavesQty().value());
    assertEquals(0, fixDec.leavesQty().scale());
    // cumQty = 0 for New
    assertEquals(0L, fixDec.cumQty().value());
    // avgPx = 0 for New
    assertEquals(0L, fixDec.avgPx().value());
    assertEquals('0', fixDec.timeInForce()); // Day
    assertEquals("EUR", fixDec.currencyAsString());
    assertEquals("USD", fixDec.settlCurrencyAsString());
    assertEquals('3', fixDec.settlType()); // T+2
    assertEquals("20260409", fixDec.settlDateAsString());
    assertEquals("ACCT-1", fixDec.accountAsString());
  }

  // ===========================================================================
  // OrderRejectedEvent → ExecutionReport (ExecType=Rejected)
  // ===========================================================================

  @Test
  void translateOrderRejectedEventMapsAllFields() {
    MutableDirectBuffer sbeBuf = new ExpandableArrayBuffer(512);
    com.trading.engine.messages.sbe.OrderRejectedEventEncoder enc =
        new com.trading.engine.messages.sbe.OrderRejectedEventEncoder();
    enc.wrapAndApplyHeader(sbeBuf, 0, new MessageHeaderEncoder());
    enc.sequenceNumber(1L);
    enc.timestamp(1_712_491_200_000_000_000L);
    enc.clOrdId("ORD-FAIL-1");
    enc.symbol("EURUSD");
    enc.side(SideEnum.Buy);
    enc.rejectReason(RejectReasonEnum.UnknownSymbol);
    enc.accountCode("ACCT-1");
    enc.productType(ProductTypeEnum.NULL_VAL);
    enc.currency("EUR");
    enc.text("Unknown symbol EURUSD");

    MessageHeaderDecoder hdrDec = new MessageHeaderDecoder();
    hdrDec.wrap(sbeBuf, 0);
    com.trading.engine.messages.sbe.OrderRejectedEventDecoder sbeDec =
        new com.trading.engine.messages.sbe.OrderRejectedEventDecoder();
    sbeDec.wrap(
        sbeBuf, MessageHeaderDecoder.ENCODED_LENGTH, hdrDec.blockLength(), hdrDec.version());

    ExecutionReportEncoder fix = new ExecutionReportEncoder();
    fix.header().senderCompID("EXCH").targetCompID("CLIENT").msgSeqNum(1);
    fix.header().sendingTime("20260407-12:00:00".getBytes());
    new SbeToFixTranslator().translateOrderRejectedEvent(sbeDec, fix);

    MutableAsciiBuffer wire = new MutableAsciiBuffer(new byte[2048]);
    long encoded = fix.encode(wire, 0);
    int wireOffset = (int) (encoded >>> 32);
    int wireLen = (int) encoded;

    ExecutionReportDecoder fixDec = new ExecutionReportDecoder();
    fixDec.decode(wire, wireOffset, wireLen);

    assertEquals("NONE", fixDec.orderIDAsString()); // sentinel — no engine order ID
    assertEquals("NONE", fixDec.execIDAsString()); // sentinel — no exec ID
    assertEquals("ORD-FAIL-1", fixDec.clOrdIDAsString());
    assertEquals('8', fixDec.execType()); // Rejected
    assertEquals('8', fixDec.ordStatus()); // Rejected
    assertEquals("EURUSD", fixDec.symbolAsString());
    assertEquals('1', fixDec.side()); // Buy
    // leavesQty = 0 for rejection
    assertEquals(0L, fixDec.leavesQty().value());
    // cumQty = 0 for rejection
    assertEquals(0L, fixDec.cumQty().value());
    // avgPx = 0 for rejection
    assertEquals(0L, fixDec.avgPx().value());
    // ordRejReason: UnknownSymbol → FIX 1
    assertEquals(1, fixDec.ordRejReason());
    assertEquals("Unknown symbol EURUSD", fixDec.textAsString());
    assertEquals("EUR", fixDec.currencyAsString());
    assertEquals("ACCT-1", fixDec.accountAsString());
  }
}
