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
    int len = FixToSbeTranslator.translateNewOrderSingle(fix, sbeBuf, 0);
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
    FixToSbeTranslator.translateNewOrderSingle(fix, sbeBuf, 0);
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
    FixToSbeTranslator.translateNewOrderSingle(fix, sbeBuf, 0);
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
        () -> FixToSbeTranslator.translateNewOrderSingle(fix, sbeBuf, 0));
  }

  /** SBE asString() returns null-padded fixed-length strings; trim trailing nulls. */
  private static String trimSbeString(String s) {
    int end = s.length();
    while (end > 0 && s.charAt(end - 1) == '\0') end--;
    return s.substring(0, end);
  }
}
