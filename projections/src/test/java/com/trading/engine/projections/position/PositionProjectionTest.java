package com.trading.engine.projections.position;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.trading.engine.messages.sbe.MessageHeaderEncoder;
import com.trading.engine.messages.sbe.OrderFilledEventDecoder;
import com.trading.engine.messages.sbe.OrderFilledEventEncoder;
import com.trading.engine.messages.sbe.ProductTypeEnum;
import com.trading.engine.messages.sbe.SettlTypeEnum;
import com.trading.engine.messages.sbe.SideEnum;
import com.trading.engine.messages.sbe.TenorEnum;
import com.trading.engine.testsupport.sbe.SbeTestEncoder;
import java.util.List;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class PositionProjectionTest {

  private static final long PRICE_SCALE = 100_000_000L;
  private static final int HDR_LEN = MessageHeaderEncoder.ENCODED_LENGTH;

  private PositionProjection projection;
  private MutableDirectBuffer buf;
  private long seqNo;

  @BeforeEach
  void setUp() {
    projection = new PositionProjection();
    buf = new ExpandableArrayBuffer(512);
    seqNo = 0;
  }

  // ---------------------------------------------------------------------------
  // Encoding helpers — single-leg fills delegate to shared SbeTestEncoder
  // ---------------------------------------------------------------------------

  private int encodeFill(
      final String symbol,
      final SideEnum side,
      final long lastPx,
      final long lastQty,
      final String accountCode,
      final String settlDate,
      final long timestamp) {
    ++seqNo;
    return SbeTestEncoder.encodeOrderFilledEvent(
        buf,
        0,
        seqNo,
        timestamp,
        "EXE-" + seqNo,
        "ORD-" + seqNo,
        "CLO-" + seqNo,
        symbol,
        side,
        lastPx,
        lastQty,
        0,
        lastQty,
        accountCode,
        ProductTypeEnum.Spot,
        settlDate,
        SettlTypeEnum.Regular,
        "USD",
        "USD",
        TenorEnum.SN);
  }

  /**
   * Swap fills have 2 legs — kept inline because the shared encoder only supports noLegsCount(0).
   */
  private int encodeSwapFill(
      final String symbol,
      final String accountCode,
      final SideEnum nearSide,
      final long nearPx,
      final long nearQty,
      final String nearSettlDate,
      final SideEnum farSide,
      final long farPx,
      final long farQty,
      final String farSettlDate,
      final long timestamp) {
    final MessageHeaderEncoder hdr = new MessageHeaderEncoder();
    final OrderFilledEventEncoder enc = new OrderFilledEventEncoder();
    enc.wrapAndApplyHeader(buf, 0, hdr);
    enc.sequenceNumber(++seqNo);
    enc.timestamp(timestamp);
    enc.execId("EXE-" + seqNo);
    enc.orderId("ORD-" + seqNo);
    enc.clOrdId("CLO-" + seqNo);
    enc.symbol(symbol);
    enc.side(nearSide);
    enc.lastPx(nearPx);
    enc.lastQty(nearQty);
    enc.leavesQty(0);
    enc.cumQty(nearQty);
    enc.productType(ProductTypeEnum.Swap);
    enc.settlDate(nearSettlDate);
    enc.settlType(SettlTypeEnum.Regular);
    enc.currency("EUR");
    enc.settlCurrency("USD");
    enc.tenor(TenorEnum.SN);
    enc.accountCode(accountCode);

    final OrderFilledEventEncoder.NoLegsEncoder legs = enc.noLegsCount(2);
    legs.next()
        .legSide(nearSide)
        .legSettlDate(nearSettlDate)
        .legSettlType(SettlTypeEnum.Regular)
        .legCurrency("EUR")
        .legLastPx(nearPx)
        .legLastQty(nearQty)
        .legLeavesQty(0)
        .legCumQty(nearQty);
    legs.next()
        .legSide(farSide)
        .legSettlDate(farSettlDate)
        .legSettlType(SettlTypeEnum.Regular)
        .legCurrency("EUR")
        .legLastPx(farPx)
        .legLastQty(farQty)
        .legLeavesQty(0)
        .legCumQty(farQty);

    return HDR_LEN + enc.encodedLength();
  }

  private void dispatch(final int totalLen) {
    projection.onEvent(
        seqNo, OrderFilledEventDecoder.TEMPLATE_ID, buf, HDR_LEN, totalLen - HDR_LEN);
  }

  // ---------------------------------------------------------------------------
  // Tests
  // ---------------------------------------------------------------------------

  @Test
  void singleBuyFillCreatesLongPosition() {
    final int len =
        encodeFill(
            "EURUSD",
            SideEnum.Buy,
            108_500_000L,
            1_000_000L * PRICE_SCALE,
            "ACME",
            "20260412",
            100L);
    dispatch(len);

    final PositionSnapshot s = projection.getPosition("EURUSD", "ACME", "20260412");
    assertNotNull(s);
    assertEquals(1_000_000L * PRICE_SCALE, s.netQty());
    assertEquals(1_000_000L * PRICE_SCALE, s.buyQty());
    assertEquals(0, s.sellQty());
    assertTrue(s.avgBuyPx() > 0);
    assertEquals(0, s.avgSellPx());
  }

  @Test
  void singleSellFillCreatesShortPosition() {
    final int len =
        encodeFill(
            "EURUSD",
            SideEnum.Sell,
            108_500_000L,
            500_000L * PRICE_SCALE,
            "ACME",
            "20260412",
            100L);
    dispatch(len);

    final PositionSnapshot s = projection.getPosition("EURUSD", "ACME", "20260412");
    assertEquals(-500_000L * PRICE_SCALE, s.netQty());
    assertEquals(0, s.buyQty());
    assertEquals(500_000L * PRICE_SCALE, s.sellQty());
  }

  @Test
  void buyThenSellReducesNetQty() {
    int len =
        encodeFill(
            "EURUSD", SideEnum.Buy, 108_500_000L, 10L * PRICE_SCALE, "ACME", "20260412", 100L);
    dispatch(len);

    len =
        encodeFill(
            "EURUSD", SideEnum.Sell, 108_600_000L, 3L * PRICE_SCALE, "ACME", "20260412", 200L);
    dispatch(len);

    final PositionSnapshot s = projection.getPosition("EURUSD", "ACME", "20260412");
    assertEquals(7L * PRICE_SCALE, s.netQty());
    assertEquals(10L * PRICE_SCALE, s.buyQty());
    assertEquals(3L * PRICE_SCALE, s.sellQty());
  }

  @Test
  void buyThenSellToFlatZerosNetQty() {
    int len =
        encodeFill(
            "EURUSD", SideEnum.Buy, 108_500_000L, 5L * PRICE_SCALE, "ACME", "20260412", 100L);
    dispatch(len);

    len =
        encodeFill(
            "EURUSD", SideEnum.Sell, 108_600_000L, 5L * PRICE_SCALE, "ACME", "20260412", 200L);
    dispatch(len);

    final PositionSnapshot s = projection.getPosition("EURUSD", "ACME", "20260412");
    assertEquals(0, s.netQty());
    assertEquals(1, projection.size()); // Flat position stays in map
  }

  @Test
  void multipleBuysAccumulateWithCorrectVwap() {
    // Buy 3 @ 1.0800
    int len =
        encodeFill(
            "EURUSD", SideEnum.Buy, 108_000_000L, 3L * PRICE_SCALE, "ACME", "20260412", 100L);
    dispatch(len);

    // Buy 7 @ 1.0900
    len =
        encodeFill(
            "EURUSD", SideEnum.Buy, 109_000_000L, 7L * PRICE_SCALE, "ACME", "20260412", 200L);
    dispatch(len);

    final PositionSnapshot s = projection.getPosition("EURUSD", "ACME", "20260412");
    assertEquals(10L * PRICE_SCALE, s.netQty());
    // VWAP = (3*1.0800 + 7*1.0900) / 10 = (3.24 + 7.63) / 10 = 1.087
    assertEquals(108_700_000L, s.avgBuyPx());
  }

  @Test
  void mixedBuysAndSellsTrackGrossVolumes() {
    int len =
        encodeFill(
            "EURUSD", SideEnum.Buy, 108_500_000L, 10L * PRICE_SCALE, "ACME", "20260412", 100L);
    dispatch(len);

    len =
        encodeFill(
            "EURUSD", SideEnum.Sell, 108_600_000L, 3L * PRICE_SCALE, "ACME", "20260412", 200L);
    dispatch(len);

    final PositionSnapshot s = projection.getPosition("EURUSD", "ACME", "20260412");
    assertEquals(10L * PRICE_SCALE, s.buyQty());
    assertEquals(3L * PRICE_SCALE, s.sellQty());
    assertEquals(108_500_000L, s.avgBuyPx());
    assertEquals(108_600_000L, s.avgSellPx());
  }

  @Test
  void differentSymbolsSameAccountSeparatePositions() {
    int len =
        encodeFill(
            "EURUSD", SideEnum.Buy, 108_500_000L, 5L * PRICE_SCALE, "ACME", "20260412", 100L);
    dispatch(len);

    len =
        encodeFill(
            "USDJPY", SideEnum.Buy, 15_000_000_000L, 3L * PRICE_SCALE, "ACME", "20260412", 200L);
    dispatch(len);

    assertEquals(2, projection.size());
    assertNotNull(projection.getPosition("EURUSD", "ACME", "20260412"));
    assertNotNull(projection.getPosition("USDJPY", "ACME", "20260412"));
  }

  @Test
  void sameSymbolDifferentAccountsSeparatePositions() {
    int len =
        encodeFill(
            "EURUSD", SideEnum.Buy, 108_500_000L, 5L * PRICE_SCALE, "ACME", "20260412", 100L);
    dispatch(len);

    len =
        encodeFill(
            "EURUSD", SideEnum.Buy, 108_500_000L, 3L * PRICE_SCALE, "BETA", "20260412", 200L);
    dispatch(len);

    assertEquals(2, projection.size());
    assertEquals(5L * PRICE_SCALE, projection.getPosition("EURUSD", "ACME", "20260412").netQty());
    assertEquals(3L * PRICE_SCALE, projection.getPosition("EURUSD", "BETA", "20260412").netQty());
  }

  @Test
  void differentSettlDatesSeparatePositions() {
    // Spot
    int len =
        encodeFill(
            "EURUSD", SideEnum.Buy, 108_500_000L, 5L * PRICE_SCALE, "ACME", "20260412", 100L);
    dispatch(len);

    // 1M forward
    len =
        encodeFill(
            "EURUSD", SideEnum.Buy, 108_800_000L, 3L * PRICE_SCALE, "ACME", "20260512", 200L);
    dispatch(len);

    assertEquals(2, projection.size());
    assertNotNull(projection.getPosition("EURUSD", "ACME", "20260412"));
    assertNotNull(projection.getPosition("EURUSD", "ACME", "20260512"));
  }

  @Test
  void swapFillProcessesBothLegs() {
    final int len =
        encodeSwapFill(
            "EURUSD",
            "ACME",
            SideEnum.Buy,
            108_500_000L,
            5L * PRICE_SCALE,
            "20260412",
            SideEnum.Sell,
            108_800_000L,
            5L * PRICE_SCALE,
            "20260512",
            100L);
    dispatch(len);

    assertEquals(2, projection.size());
    // Near leg: Buy position at spot date
    final PositionSnapshot near = projection.getPosition("EURUSD", "ACME", "20260412");
    assertNotNull(near);
    assertEquals(5L * PRICE_SCALE, near.netQty());

    // Far leg: Sell position at forward date
    final PositionSnapshot far = projection.getPosition("EURUSD", "ACME", "20260512");
    assertNotNull(far);
    assertEquals(-5L * PRICE_SCALE, far.netQty());
  }

  @Test
  void swapLegUsesParentAccountCode() {
    final int len =
        encodeSwapFill(
            "EURUSD",
            "ACME",
            SideEnum.Buy,
            108_500_000L,
            5L * PRICE_SCALE,
            "20260412",
            SideEnum.Sell,
            108_800_000L,
            5L * PRICE_SCALE,
            "20260512",
            100L);
    dispatch(len);

    final PositionSnapshot near = projection.getPosition("EURUSD", "ACME", "20260412");
    assertEquals("ACME", near.accountCode());
    final PositionSnapshot far = projection.getPosition("EURUSD", "ACME", "20260512");
    assertEquals("ACME", far.accountCode());
  }

  @Test
  void getPositionsByAccountReturnsCorrectSet() {
    int len =
        encodeFill(
            "EURUSD", SideEnum.Buy, 108_500_000L, 5L * PRICE_SCALE, "ACME", "20260412", 100L);
    dispatch(len);

    len =
        encodeFill(
            "USDJPY", SideEnum.Sell, 15_000_000_000L, 3L * PRICE_SCALE, "ACME", "20260412", 200L);
    dispatch(len);

    len =
        encodeFill(
            "EURUSD", SideEnum.Buy, 108_500_000L, 1L * PRICE_SCALE, "BETA", "20260412", 300L);
    dispatch(len);

    final List<PositionSnapshot> acme = projection.getPositionsByAccount("ACME");
    assertEquals(2, acme.size());
    final List<PositionSnapshot> beta = projection.getPositionsByAccount("BETA");
    assertEquals(1, beta.size());
  }

  @Test
  void getPositionsBySymbolReturnsCorrectSet() {
    int len =
        encodeFill(
            "EURUSD", SideEnum.Buy, 108_500_000L, 5L * PRICE_SCALE, "ACME", "20260412", 100L);
    dispatch(len);

    len =
        encodeFill(
            "EURUSD", SideEnum.Buy, 108_500_000L, 3L * PRICE_SCALE, "BETA", "20260412", 200L);
    dispatch(len);

    len =
        encodeFill(
            "USDJPY", SideEnum.Buy, 15_000_000_000L, 1L * PRICE_SCALE, "ACME", "20260412", 300L);
    dispatch(len);

    assertEquals(2, projection.getPositionsBySymbol("EURUSD").size());
    assertEquals(1, projection.getPositionsBySymbol("USDJPY").size());
  }

  @Test
  void largeNotionalNoOverflow() {
    // 500M EUR — exercises 128-bit mulDiv
    final long largeQty = 500_000_000L * PRICE_SCALE;
    final long price = 108_500_000L;

    final int len = encodeFill("EURUSD", SideEnum.Buy, price, largeQty, "ACME", "20260412", 100L);
    dispatch(len);

    final PositionSnapshot s = projection.getPosition("EURUSD", "ACME", "20260412");
    assertEquals(largeQty, s.netQty());
    assertEquals(price, s.avgBuyPx());
    assertEquals(0, projection.errorCount());
  }

  @Test
  void resetClearsAllState() {
    final int len =
        encodeFill(
            "EURUSD", SideEnum.Buy, 108_500_000L, 5L * PRICE_SCALE, "ACME", "20260412", 100L);
    dispatch(len);

    projection.reset();
    assertEquals(0, projection.size());
    assertEquals(0, projection.lastProcessedSequence());
    assertEquals(0, projection.eventsProcessed());
    assertNull(projection.getPosition("EURUSD", "ACME", "20260412"));
  }

  @Test
  void fillWithEmptyAccountCodeSkipped() {
    final MessageHeaderEncoder hdr = new MessageHeaderEncoder();
    final OrderFilledEventEncoder enc = new OrderFilledEventEncoder();
    enc.wrapAndApplyHeader(buf, 0, hdr);
    enc.sequenceNumber(++seqNo);
    enc.timestamp(100L);
    enc.execId("EXE-X");
    enc.orderId("ORD-X");
    enc.clOrdId("CLO-X");
    enc.symbol("EURUSD");
    enc.side(SideEnum.Buy);
    enc.lastPx(108_500_000L);
    enc.lastQty(1L * PRICE_SCALE);
    enc.leavesQty(0);
    enc.cumQty(1L * PRICE_SCALE);
    enc.productType(ProductTypeEnum.Spot);
    enc.settlDate("20260412");
    enc.settlType(SettlTypeEnum.Regular);
    enc.currency("USD");
    enc.settlCurrency("USD");
    enc.tenor(TenorEnum.SN);
    // accountCode left as empty (all NUL bytes)
    enc.noLegsCount(0);
    final int totalLen = HDR_LEN + enc.encodedLength();
    dispatch(totalLen);

    assertEquals(0, projection.size());
    assertEquals(1, projection.eventsProcessed());
  }

  @Test
  void replayAfterResetProducesSameState() {
    // Build state
    int len =
        encodeFill(
            "EURUSD", SideEnum.Buy, 108_500_000L, 5L * PRICE_SCALE, "ACME", "20260412", 100L);
    dispatch(len);

    len =
        encodeFill(
            "EURUSD", SideEnum.Sell, 108_600_000L, 2L * PRICE_SCALE, "ACME", "20260412", 200L);
    dispatch(len);

    final PositionSnapshot before = projection.getPosition("EURUSD", "ACME", "20260412");

    // Reset and replay
    projection.reset();
    seqNo = 0;

    len =
        encodeFill(
            "EURUSD", SideEnum.Buy, 108_500_000L, 5L * PRICE_SCALE, "ACME", "20260412", 100L);
    dispatch(len);

    len =
        encodeFill(
            "EURUSD", SideEnum.Sell, 108_600_000L, 2L * PRICE_SCALE, "ACME", "20260412", 200L);
    dispatch(len);

    final PositionSnapshot after = projection.getPosition("EURUSD", "ACME", "20260412");
    assertEquals(before.netQty(), after.netQty());
    assertEquals(before.buyQty(), after.buyQty());
    assertEquals(before.sellQty(), after.sellQty());
    assertEquals(before.avgBuyPx(), after.avgBuyPx());
    assertEquals(before.avgSellPx(), after.avgSellPx());
  }

  @Test
  void currencyAndSettlCurrencyTrackedOnPosition() {
    final int len =
        encodeFill(
            "EURUSD", SideEnum.Buy, 108_500_000L, 5L * PRICE_SCALE, "ACME", "20260412", 100L);
    dispatch(len);

    final PositionSnapshot s = projection.getPosition("EURUSD", "ACME", "20260412");
    assertEquals("USD", s.currency());
    assertEquals("USD", s.settlCurrency());
  }

  @Test
  void decodeErrorIncrementsErrorCount() {
    // Dispatch with a null buffer — should cause a NullPointerException in the decoder
    seqNo = 99;
    projection.onEvent(99, OrderFilledEventDecoder.TEMPLATE_ID, null, 0, 100);
    assertEquals(99, projection.lastProcessedSequence());
    assertEquals(1, projection.errorCount());
    assertEquals(0, projection.size());
  }
}
