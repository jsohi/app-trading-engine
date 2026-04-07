package com.trading.engine.gateway;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.trading.engine.fix.builder.HeaderEncoder;
import com.trading.engine.fix.builder.NewOrderSingleEncoder;
import com.trading.engine.fix.decoder_flyweight.NewOrderSingleDecoder;
import com.trading.engine.messages.sbe.MessageHeaderDecoder;
import com.trading.engine.messages.sbe.OrdTypeEnum;
import com.trading.engine.messages.sbe.SettlTypeEnum;
import com.trading.engine.messages.sbe.SideEnum;
import com.trading.engine.messages.sbe.TimeInForceEnum;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.junit.jupiter.api.Test;
import uk.co.real_logic.artio.fields.DecimalFloat;
import uk.co.real_logic.artio.util.MutableAsciiBuffer;

class FixToSbeTranslatorTest {

  /**
   * Build a FIX 35=D NewOrderSingle as ASCII bytes via Artio's encoder, then decode it back into
   * the flyweight decoder so the translator can consume it. Tests are not zero-allocation; they use
   * the convenient {@code String} setters.
   */
  private static NewOrderSingleDecoder encodeAndDecodeNos(
      String clOrdId,
      String symbol,
      char side,
      char ordType,
      DecimalFloat price,
      DecimalFloat orderQty,
      char timeInForce,
      String account,
      String currency,
      String settlCurrency,
      char settlType,
      String settlDate) {
    NewOrderSingleEncoder enc = new NewOrderSingleEncoder();
    HeaderEncoder hdr = enc.header();
    hdr.senderCompID("CLIENT").targetCompID("EXCH").msgSeqNum(1);
    hdr.sendingTime("20260407-12:00:00".getBytes());
    enc.clOrdID(clOrdId);
    enc.instrument().symbol(symbol);
    enc.side(side);
    enc.transactTime("20260407-12:00:00".getBytes());
    enc.ordType(ordType);
    if (price != null) {
      enc.price(price);
    }
    enc.orderQtyData().orderQty(orderQty);
    if (timeInForce != 0) {
      enc.timeInForce(timeInForce);
    }
    if (account != null) {
      enc.account(account);
    }
    if (currency != null) {
      enc.currency(currency);
    }
    if (settlCurrency != null) {
      enc.settlCurrency(settlCurrency);
    }
    if (settlType != 0) {
      enc.settlType(settlType);
    }
    if (settlDate != null) {
      enc.settlDate(settlDate.getBytes());
    }

    MutableAsciiBuffer wire = new MutableAsciiBuffer(new byte[2048]);
    long encodedResult = enc.encode(wire, 0);
    int wireOffset = (int) (encodedResult >>> 32);
    int wireLen = (int) encodedResult;

    NewOrderSingleDecoder dec = new NewOrderSingleDecoder();
    dec.decode(wire, wireOffset, wireLen);
    return dec;
  }

  private static com.trading.engine.messages.sbe.NewOrderSingleDecoder decodeSbeNos(
      MutableDirectBuffer buf) {
    MessageHeaderDecoder hdrDec = new MessageHeaderDecoder();
    hdrDec.wrap(buf, 0);
    assertEquals(
        com.trading.engine.messages.sbe.NewOrderSingleEncoder.TEMPLATE_ID, hdrDec.templateId());
    com.trading.engine.messages.sbe.NewOrderSingleDecoder sbeDec =
        new com.trading.engine.messages.sbe.NewOrderSingleDecoder();
    sbeDec.wrap(
        buf,
        MessageHeaderDecoder.ENCODED_LENGTH,
        com.trading.engine.messages.sbe.NewOrderSingleDecoder.BLOCK_LENGTH,
        com.trading.engine.messages.sbe.NewOrderSingleDecoder.SCHEMA_VERSION);
    return sbeDec;
  }

  @Test
  void roundTripNewOrderSingleMinimalLimit() {
    DecimalFloat price = new DecimalFloat(15_025L, 2); // 150.25
    DecimalFloat qty = new DecimalFloat(100L, 0); // 100
    NewOrderSingleDecoder fix =
        encodeAndDecodeNos(
            "ORD-1", "EURUSD", '1', '2', price, qty, '0', null, null, null, (char) 0, null);

    MutableDirectBuffer sbeBuf = new ExpandableArrayBuffer(512);
    int len = new FixToSbeTranslator().translateNewOrderSingle(fix, sbeBuf, 0);
    assertTrue(len > 0);

    com.trading.engine.messages.sbe.NewOrderSingleDecoder sbeDec = decodeSbeNos(sbeBuf);
    assertEquals("ORD-1", trimSbeString(sbeDec.clOrdId()));
    assertEquals("EURUSD", trimSbeString(sbeDec.symbol()));
    assertEquals(SideEnum.Buy, sbeDec.side());
    assertEquals(OrdTypeEnum.Limit, sbeDec.ordType());
    assertEquals(15_025_000_000L, sbeDec.price());
    assertEquals(10_000_000_000L, sbeDec.orderQty());
    assertEquals(TimeInForceEnum.Day, sbeDec.timeInForce());
  }

  @Test
  void roundTripNewOrderSingleMarketOmitsPrice() {
    DecimalFloat qty = new DecimalFloat(50L, 0);
    NewOrderSingleDecoder fix =
        encodeAndDecodeNos(
            "MKT-1", "GBPUSD", '2', '1', null, qty, '3', null, null, null, (char) 0, null);

    MutableDirectBuffer sbeBuf = new ExpandableArrayBuffer(512);
    new FixToSbeTranslator().translateNewOrderSingle(fix, sbeBuf, 0);
    com.trading.engine.messages.sbe.NewOrderSingleDecoder sbeDec = decodeSbeNos(sbeBuf);

    assertEquals(SideEnum.Sell, sbeDec.side());
    assertEquals(OrdTypeEnum.Market, sbeDec.ordType());
    assertEquals(
        com.trading.engine.messages.sbe.NewOrderSingleDecoder.priceNullValue(), sbeDec.price());
    assertEquals(TimeInForceEnum.IOC, sbeDec.timeInForce());
  }

  @Test
  void roundTripNewOrderSingleWithCurrencyAndSettlement() {
    DecimalFloat price = new DecimalFloat(11_000L, 4); // 1.1000
    DecimalFloat qty = new DecimalFloat(1_000_000L, 0); // 1M
    NewOrderSingleDecoder fix =
        encodeAndDecodeNos(
            "FX-1",
            "EURUSD",
            '1',
            '2',
            price,
            qty,
            '0',
            "ACCT-42",
            "EUR",
            "USD",
            '3', // T+2
            "20260409");

    MutableDirectBuffer sbeBuf = new ExpandableArrayBuffer(512);
    new FixToSbeTranslator().translateNewOrderSingle(fix, sbeBuf, 0);
    com.trading.engine.messages.sbe.NewOrderSingleDecoder sbeDec = decodeSbeNos(sbeBuf);

    assertEquals(110_000_000L, sbeDec.price()); // 1.1 * 10^8
    assertEquals(100_000_000_000_000L, sbeDec.orderQty()); // 1M * 10^8
    assertEquals("ACCT-42", trimSbeString(sbeDec.accountCode()));
    assertEquals("EUR", trimSbeString(sbeDec.currency()));
    assertEquals("USD", trimSbeString(sbeDec.settlCurrency()));
    assertEquals(SettlTypeEnum.TPlus2, sbeDec.settlType());
    assertEquals("20260409", trimSbeString(sbeDec.settlDate()));
  }

  @Test
  void rejectsUnsupportedSide() {
    DecimalFloat qty = new DecimalFloat(1L, 0);
    NewOrderSingleDecoder fix =
        encodeAndDecodeNos(
            "BAD-1", "X", 'G', '1', null, qty, '0', null, null, null, (char) 0, null);

    MutableDirectBuffer sbeBuf = new ExpandableArrayBuffer(512);
    assertThrows(
        IllegalStateException.class,
        () -> new FixToSbeTranslator().translateNewOrderSingle(fix, sbeBuf, 0));
  }

  // ===========================================================================
  // OrderCancelRequest (35=F) — happy path round trip
  // ===========================================================================

  @Test
  void roundTripOrderCancelRequest() {
    com.trading.engine.fix.builder.OrderCancelRequestEncoder enc =
        new com.trading.engine.fix.builder.OrderCancelRequestEncoder();
    enc.header().senderCompID("CLIENT").targetCompID("EXCH").msgSeqNum(1);
    enc.header().sendingTime("20260407-12:00:00".getBytes());
    enc.origClOrdID("ORD-1");
    enc.clOrdID("CXL-1");
    enc.instrument().symbol("EURUSD");
    enc.side('1');
    enc.transactTime("20260407-12:00:00".getBytes());

    MutableAsciiBuffer wire = new MutableAsciiBuffer(new byte[2048]);
    long encoded = enc.encode(wire, 0);
    int wireOffset = (int) (encoded >>> 32);
    int wireLen = (int) encoded;

    com.trading.engine.fix.decoder_flyweight.OrderCancelRequestDecoder dec =
        new com.trading.engine.fix.decoder_flyweight.OrderCancelRequestDecoder();
    dec.decode(wire, wireOffset, wireLen);

    MutableDirectBuffer sbeBuf = new ExpandableArrayBuffer(512);
    int len = new FixToSbeTranslator().translateOrderCancelRequest(dec, sbeBuf, 0);
    assertTrue(len > 0);

    MessageHeaderDecoder hdr = new MessageHeaderDecoder();
    hdr.wrap(sbeBuf, 0);
    assertEquals(
        com.trading.engine.messages.sbe.CancelOrderRequestEncoder.TEMPLATE_ID, hdr.templateId());

    com.trading.engine.messages.sbe.CancelOrderRequestDecoder sbe =
        new com.trading.engine.messages.sbe.CancelOrderRequestDecoder();
    sbe.wrap(
        sbeBuf,
        MessageHeaderDecoder.ENCODED_LENGTH,
        com.trading.engine.messages.sbe.CancelOrderRequestDecoder.BLOCK_LENGTH,
        com.trading.engine.messages.sbe.CancelOrderRequestDecoder.SCHEMA_VERSION);
    assertEquals("ORD-1", trimSbeString(sbe.origClOrdId()));
    assertEquals("CXL-1", trimSbeString(sbe.clOrdId()));
    assertEquals("EURUSD", trimSbeString(sbe.symbol()));
    assertEquals(SideEnum.Buy, sbe.side());
  }

  // ===========================================================================
  // NewOrderMultileg (35=AB) — round trip with two legs
  // ===========================================================================

  @Test
  void roundTripNewOrderMultilegTwoLegs() {
    com.trading.engine.fix.builder.NewOrderMultilegEncoder enc =
        new com.trading.engine.fix.builder.NewOrderMultilegEncoder();
    enc.header().senderCompID("CLIENT").targetCompID("EXCH").msgSeqNum(1);
    enc.header().sendingTime("20260407-12:00:00".getBytes());
    enc.clOrdID("SWAP-1");
    enc.instrument().symbol("EURUSD");
    enc.side('1');
    enc.transactTime("20260407-12:00:00".getBytes());
    enc.ordType('2');
    enc.price(new DecimalFloat(11_000L, 4)); // 1.1000
    enc.orderQtyData().orderQty(new DecimalFloat(1_000_000L, 0));
    enc.timeInForce('0');
    enc.currency("EUR");
    enc.settlCurrency("USD");

    // Artio's LegsGroupEncoder.next() returns a *new* LegsGroupEncoder (linked-list pattern).
    // Each leg gets its own encoder instance; the local variable must be re-bound on each
    // advance — same fix applied to SbeToFixTranslator's leg loops.
    com.trading.engine.fix.builder.NewOrderMultilegEncoder.LegsGroupEncoder leg1 = enc.legsGroup(2);
    leg1.instrumentLeg().legSymbol("EURUSD").legSide('1').legCurrency("EUR");
    leg1.instrumentLeg().legRatioQty(new DecimalFloat(1L, 0));
    leg1.legPrice(new DecimalFloat(11_000L, 4));
    com.trading.engine.fix.builder.NewOrderMultilegEncoder.LegsGroupEncoder leg2 = leg1.next();
    leg2.instrumentLeg().legSymbol("EURUSD").legSide('2').legCurrency("USD");
    leg2.instrumentLeg().legRatioQty(new DecimalFloat(1L, 0));
    leg2.legPrice(new DecimalFloat(11_005L, 4));

    MutableAsciiBuffer wire = new MutableAsciiBuffer(new byte[2048]);
    long encoded = enc.encode(wire, 0);
    int wireOffset = (int) (encoded >>> 32);
    int wireLen = (int) encoded;

    com.trading.engine.fix.decoder_flyweight.NewOrderMultilegDecoder dec =
        new com.trading.engine.fix.decoder_flyweight.NewOrderMultilegDecoder();
    dec.decode(wire, wireOffset, wireLen);

    MutableDirectBuffer sbeBuf = new ExpandableArrayBuffer(512);
    int len = new FixToSbeTranslator().translateNewOrderMultileg(dec, sbeBuf, 0);
    assertTrue(len > 0);

    MessageHeaderDecoder hdr = new MessageHeaderDecoder();
    hdr.wrap(sbeBuf, 0);
    assertEquals(
        com.trading.engine.messages.sbe.NewOrderMultilegEncoder.TEMPLATE_ID, hdr.templateId());

    com.trading.engine.messages.sbe.NewOrderMultilegDecoder sbe =
        new com.trading.engine.messages.sbe.NewOrderMultilegDecoder();
    sbe.wrap(
        sbeBuf,
        MessageHeaderDecoder.ENCODED_LENGTH,
        com.trading.engine.messages.sbe.NewOrderMultilegDecoder.BLOCK_LENGTH,
        com.trading.engine.messages.sbe.NewOrderMultilegDecoder.SCHEMA_VERSION);

    assertEquals("SWAP-1", trimSbeString(sbe.clOrdId()));
    assertEquals("EURUSD", trimSbeString(sbe.symbol()));
    assertEquals(SideEnum.Buy, sbe.side());
    assertEquals(110_000_000L, sbe.price());
    assertEquals("EUR", trimSbeString(sbe.currency()));
    assertEquals("USD", trimSbeString(sbe.settlCurrency()));

    com.trading.engine.messages.sbe.NewOrderMultilegDecoder.NoLegsDecoder legs = sbe.noLegs();
    assertEquals(2, legs.count());
    legs.next();
    assertEquals(SideEnum.Buy, legs.legSide());
    assertEquals("EUR", trimSbeString(legs.legCurrency()));
    assertEquals(110_000_000L, legs.legPrice());
    legs.next();
    assertEquals(SideEnum.Sell, legs.legSide());
    assertEquals("USD", trimSbeString(legs.legCurrency()));
    assertEquals(110_050_000L, legs.legPrice());
  }

  // ===========================================================================
  // QuoteRequest (35=R) — round trip with single related-sym
  // ===========================================================================

  @Test
  void roundTripQuoteRequest() {
    com.trading.engine.fix.builder.QuoteRequestEncoder enc =
        new com.trading.engine.fix.builder.QuoteRequestEncoder();
    enc.header().senderCompID("CLIENT").targetCompID("EXCH").msgSeqNum(1);
    enc.header().sendingTime("20260407-12:00:00".getBytes());
    enc.quoteReqID("RFQ-1");

    com.trading.engine.fix.builder.QuoteRequestEncoder.RelatedSymGroupEncoder relSym =
        enc.relatedSymGroup(1);
    relSym.instrument().symbol("EURUSD");
    relSym.side('1');
    relSym.orderQtyData().orderQty(new DecimalFloat(1_000_000L, 0));
    relSym.currency("EUR");

    MutableAsciiBuffer wire = new MutableAsciiBuffer(new byte[2048]);
    long encoded = enc.encode(wire, 0);
    int wireOffset = (int) (encoded >>> 32);
    int wireLen = (int) encoded;

    com.trading.engine.fix.decoder_flyweight.QuoteRequestDecoder dec =
        new com.trading.engine.fix.decoder_flyweight.QuoteRequestDecoder();
    dec.decode(wire, wireOffset, wireLen);

    MutableDirectBuffer sbeBuf = new ExpandableArrayBuffer(512);
    int len = new FixToSbeTranslator().translateQuoteRequest(dec, sbeBuf, 0);
    assertTrue(len > 0);

    MessageHeaderDecoder hdr = new MessageHeaderDecoder();
    hdr.wrap(sbeBuf, 0);
    assertEquals(com.trading.engine.messages.sbe.QuoteRequestEncoder.TEMPLATE_ID, hdr.templateId());

    com.trading.engine.messages.sbe.QuoteRequestDecoder sbe =
        new com.trading.engine.messages.sbe.QuoteRequestDecoder();
    sbe.wrap(
        sbeBuf,
        MessageHeaderDecoder.ENCODED_LENGTH,
        com.trading.engine.messages.sbe.QuoteRequestDecoder.BLOCK_LENGTH,
        com.trading.engine.messages.sbe.QuoteRequestDecoder.SCHEMA_VERSION);
    assertEquals("RFQ-1", trimSbeString(sbe.quoteReqId()));
    assertEquals("EURUSD", trimSbeString(sbe.symbol()));
    assertEquals(SideEnum.Buy, sbe.side());
    assertEquals(100_000_000_000_000L, sbe.orderQty());
    assertEquals("EUR", trimSbeString(sbe.currency()));
  }

  // ===========================================================================
  // MultilegOrderCancelReplaceRequest (35=AC) — happy path round trip
  // ===========================================================================

  @Test
  void roundTripMultilegOrderCancelReplace() {
    com.trading.engine.fix.builder.MultilegOrderCancelReplaceRequestEncoder enc =
        new com.trading.engine.fix.builder.MultilegOrderCancelReplaceRequestEncoder();
    enc.header().senderCompID("CLIENT").targetCompID("EXCH").msgSeqNum(1);
    enc.header().sendingTime("20260407-12:00:00".getBytes());
    enc.origClOrdID("SWAP-1");
    enc.clOrdID("SWAP-1A");
    enc.instrument().symbol("EURUSD");
    enc.side('1');
    enc.ordType('2');
    enc.price(new DecimalFloat(11_010L, 4));
    enc.orderQtyData().orderQty(new DecimalFloat(1_000_000L, 0));
    enc.timeInForce('0');
    enc.currency("EUR");
    enc.settlCurrency("USD");
    enc.transactTime("20260407-12:00:00".getBytes());

    com.trading.engine.fix.builder.MultilegOrderCancelReplaceRequestEncoder.LegsGroupEncoder leg =
        enc.legsGroup(1);
    leg.instrumentLeg().legSymbol("EURUSD").legSide('1').legCurrency("EUR");
    leg.instrumentLeg().legRatioQty(new DecimalFloat(1L, 0));

    MutableAsciiBuffer wire = new MutableAsciiBuffer(new byte[2048]);
    long encoded = enc.encode(wire, 0);
    int wireOffset = (int) (encoded >>> 32);
    int wireLen = (int) encoded;

    com.trading.engine.fix.decoder_flyweight.MultilegOrderCancelReplaceRequestDecoder dec =
        new com.trading.engine.fix.decoder_flyweight.MultilegOrderCancelReplaceRequestDecoder();
    dec.decode(wire, wireOffset, wireLen);

    MutableDirectBuffer sbeBuf = new ExpandableArrayBuffer(512);
    int len = new FixToSbeTranslator().translateMultilegOrderCancelReplace(dec, sbeBuf, 0);
    assertTrue(len > 0);

    MessageHeaderDecoder hdr = new MessageHeaderDecoder();
    hdr.wrap(sbeBuf, 0);
    assertEquals(
        com.trading.engine.messages.sbe.MultilegOrderCancelReplaceEncoder.TEMPLATE_ID,
        hdr.templateId());

    com.trading.engine.messages.sbe.MultilegOrderCancelReplaceDecoder sbe =
        new com.trading.engine.messages.sbe.MultilegOrderCancelReplaceDecoder();
    sbe.wrap(
        sbeBuf,
        MessageHeaderDecoder.ENCODED_LENGTH,
        com.trading.engine.messages.sbe.MultilegOrderCancelReplaceDecoder.BLOCK_LENGTH,
        com.trading.engine.messages.sbe.MultilegOrderCancelReplaceDecoder.SCHEMA_VERSION);
    assertEquals("SWAP-1", trimSbeString(sbe.origClOrdId()));
    assertEquals("SWAP-1A", trimSbeString(sbe.clOrdId()));
    assertEquals("EUR", trimSbeString(sbe.currency()));
    assertEquals("USD", trimSbeString(sbe.settlCurrency()));
    assertEquals(110_100_000L, sbe.price());
  }

  // ===========================================================================
  // MassQuote (35=i) — FIX nested groups → SBE flat group
  // ===========================================================================

  @Test
  void roundTripMassQuoteWithFlattening() {
    com.trading.engine.fix.builder.MassQuoteEncoder enc =
        new com.trading.engine.fix.builder.MassQuoteEncoder();
    enc.header().senderCompID("MM").targetCompID("EXCH").msgSeqNum(1);
    enc.header().sendingTime("20260407-12:00:00".getBytes());
    enc.quoteID("MQ-1");
    enc.account("ACCT-MM");

    // 2 sets × 2 entries = 4 entries flattened on the SBE side.
    com.trading.engine.fix.builder.MassQuoteEncoder.QuoteSetsGroupEncoder set1 =
        enc.quoteSetsGroup(2);
    set1.quoteSetID("S1");
    set1.totNoQuoteEntries(2);
    set1.underlyingInstrument().underlyingSymbol("EUR"); // FIX requires it; SBE drops it
    com.trading.engine.fix.builder.MassQuoteEncoder.QuoteSetsGroupEncoder.QuoteEntriesGroupEncoder
        entry11 = set1.quoteEntriesGroup(2);
    entry11.quoteEntryID("E11").instrument().symbol("EURUSD");
    entry11.bidPx(new DecimalFloat(11_000L, 4));
    entry11.offerPx(new DecimalFloat(11_005L, 4));
    com.trading.engine.fix.builder.MassQuoteEncoder.QuoteSetsGroupEncoder.QuoteEntriesGroupEncoder
        entry12 = entry11.next();
    entry12.quoteEntryID("E12").instrument().symbol("GBPUSD");
    entry12.bidPx(new DecimalFloat(12_500L, 4));
    entry12.offerPx(new DecimalFloat(12_510L, 4));

    com.trading.engine.fix.builder.MassQuoteEncoder.QuoteSetsGroupEncoder set2 = set1.next();
    set2.quoteSetID("S2");
    set2.totNoQuoteEntries(2);
    set2.underlyingInstrument().underlyingSymbol("USD");
    com.trading.engine.fix.builder.MassQuoteEncoder.QuoteSetsGroupEncoder.QuoteEntriesGroupEncoder
        entry21 = set2.quoteEntriesGroup(2);
    entry21.quoteEntryID("E21").instrument().symbol("USDJPY");
    entry21.bidPx(new DecimalFloat(150_00L, 2));
    entry21.offerPx(new DecimalFloat(150_05L, 2));
    com.trading.engine.fix.builder.MassQuoteEncoder.QuoteSetsGroupEncoder.QuoteEntriesGroupEncoder
        entry22 = entry21.next();
    entry22.quoteEntryID("E22").instrument().symbol("AUDUSD");
    entry22.bidPx(new DecimalFloat(6_500L, 4));
    entry22.offerPx(new DecimalFloat(6_510L, 4));

    MutableAsciiBuffer wire = new MutableAsciiBuffer(new byte[2048]);
    long encoded = enc.encode(wire, 0);
    int wireOffset = (int) (encoded >>> 32);
    int wireLen = (int) encoded;

    com.trading.engine.fix.decoder_flyweight.MassQuoteDecoder dec =
        new com.trading.engine.fix.decoder_flyweight.MassQuoteDecoder();
    dec.decode(wire, wireOffset, wireLen);

    MutableDirectBuffer sbeBuf = new ExpandableArrayBuffer(1024);
    int len = new FixToSbeTranslator().translateMassQuote(dec, sbeBuf, 0);
    assertTrue(len > 0);

    MessageHeaderDecoder hdr = new MessageHeaderDecoder();
    hdr.wrap(sbeBuf, 0);
    assertEquals(com.trading.engine.messages.sbe.MassQuoteEncoder.TEMPLATE_ID, hdr.templateId());

    com.trading.engine.messages.sbe.MassQuoteDecoder sbe =
        new com.trading.engine.messages.sbe.MassQuoteDecoder();
    sbe.wrap(sbeBuf, MessageHeaderDecoder.ENCODED_LENGTH, hdr.blockLength(), hdr.version());
    assertEquals("MQ-1", trimSbeString(sbe.quoteId()));
    assertEquals("ACCT-MM", trimSbeString(sbe.accountCode()));

    com.trading.engine.messages.sbe.MassQuoteDecoder.NoQuoteEntriesDecoder entries =
        sbe.noQuoteEntries();
    assertEquals(4, entries.count(), "FIX 2 sets × 2 entries should flatten to 4 SBE entries");

    entries.next();
    assertEquals("E11", trimSbeString(entries.quoteEntryId()));
    assertEquals("EURUSD", trimSbeString(entries.symbol()));
    assertEquals(110_000_000L, entries.bidPx());
    assertEquals(110_050_000L, entries.offerPx());

    entries.next();
    assertEquals("E12", trimSbeString(entries.quoteEntryId()));
    assertEquals("GBPUSD", trimSbeString(entries.symbol()));
    assertEquals(125_000_000L, entries.bidPx());
    assertEquals(125_100_000L, entries.offerPx());

    entries.next();
    assertEquals("E21", trimSbeString(entries.quoteEntryId()));
    assertEquals("USDJPY", trimSbeString(entries.symbol()));
    assertEquals(15000_000_000L, entries.bidPx()); // 150.00 → 15_000_000_000
    assertEquals(15005_000_000L, entries.offerPx());

    entries.next();
    assertEquals("E22", trimSbeString(entries.quoteEntryId()));
    assertEquals("AUDUSD", trimSbeString(entries.symbol()));
    assertEquals(65_000_000L, entries.bidPx()); // 0.6500 → 65_000_000
    assertEquals(65_100_000L, entries.offerPx());
  }

  /** SBE asString() returns null-padded fixed-length strings; trim trailing nulls. */
  private static String trimSbeString(String s) {
    int end = s.length();
    while (end > 0 && s.charAt(end - 1) == '\0') end--;
    return s.substring(0, end);
  }
}
