package com.trading.engine.gateway;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.trading.engine.fix.builder.ExecutionReportEncoder;
import com.trading.engine.fix.builder.HeaderEncoder;
import com.trading.engine.fix.decoder_flyweight.ExecutionReportDecoder;
import com.trading.engine.messages.sbe.ExecTypeEnum;
import com.trading.engine.messages.sbe.MessageHeaderDecoder;
import com.trading.engine.messages.sbe.MessageHeaderEncoder;
import com.trading.engine.messages.sbe.OrdStatusEnum;
import com.trading.engine.messages.sbe.ProductTypeEnum;
import com.trading.engine.messages.sbe.SettlTypeEnum;
import com.trading.engine.messages.sbe.SideEnum;
import com.trading.engine.messages.sbe.TenorEnum;
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
}
