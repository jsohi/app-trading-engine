package com.trading.engine.projections.order;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.trading.engine.messages.sbe.ExecTypeEnum;
import com.trading.engine.messages.sbe.MessageHeaderEncoder;
import com.trading.engine.messages.sbe.OrdStatusEnum;
import com.trading.engine.messages.sbe.OrdTypeEnum;
import com.trading.engine.messages.sbe.OrderCanceledEventDecoder;
import com.trading.engine.messages.sbe.OrderCreatedEventDecoder;
import com.trading.engine.messages.sbe.OrderFilledEventDecoder;
import com.trading.engine.messages.sbe.OrderRejectedEventDecoder;
import com.trading.engine.messages.sbe.ProductTypeEnum;
import com.trading.engine.messages.sbe.RejectReasonEnum;
import com.trading.engine.messages.sbe.SettlTypeEnum;
import com.trading.engine.messages.sbe.SideEnum;
import com.trading.engine.messages.sbe.TenorEnum;
import com.trading.engine.messages.sbe.TimeInForceEnum;
import com.trading.engine.testsupport.sbe.SbeTestEncoder;
import java.util.List;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class OrderProjectionTest {

  private static final long PRICE_SCALE = 100_000_000L;
  private static final int HDR_LEN = MessageHeaderEncoder.ENCODED_LENGTH;

  private OrderProjection projection;
  private MutableDirectBuffer buf;
  private long seqNo;

  @BeforeEach
  void setUp() {
    projection = new OrderProjection(64);
    buf = new ExpandableArrayBuffer(512);
    seqNo = 0;
  }

  // ---------------------------------------------------------------------------
  // Encoding helpers — delegate to shared SbeTestEncoder
  // ---------------------------------------------------------------------------

  private int encodeOrderCreated(
      final String orderId,
      final String clOrdId,
      final String symbol,
      final SideEnum side,
      final OrdTypeEnum ordType,
      final long price,
      final long orderQty,
      final String accountCode,
      final long timestamp) {
    return SbeTestEncoder.encodeOrderCreatedEvent(
        buf,
        0,
        ++seqNo,
        timestamp,
        orderId,
        "",
        clOrdId,
        symbol,
        side,
        ordType,
        TimeInForceEnum.Day,
        price,
        orderQty,
        "",
        accountCode,
        ProductTypeEnum.Spot,
        "20260412",
        SettlTypeEnum.Regular,
        "USD",
        "USD",
        TenorEnum.SN);
  }

  private int encodeOrderRejected(
      final String clOrdId,
      final String symbol,
      final SideEnum side,
      final RejectReasonEnum reason,
      final String accountCode,
      final long timestamp) {
    return SbeTestEncoder.encodeOrderRejectedEvent(
        buf,
        0,
        ++seqNo,
        timestamp,
        clOrdId,
        reason,
        "rejected",
        symbol,
        side,
        accountCode,
        ProductTypeEnum.Spot,
        "USD");
  }

  private int encodeOrderFilled(
      final String execId,
      final String orderId,
      final String clOrdId,
      final String symbol,
      final SideEnum side,
      final long lastPx,
      final long lastQty,
      final long leavesQty,
      final long cumQty,
      final String accountCode,
      final long timestamp) {
    return SbeTestEncoder.encodeOrderFilledEvent(
        buf,
        0,
        ++seqNo,
        timestamp,
        execId,
        orderId,
        clOrdId,
        symbol,
        side,
        lastPx,
        lastQty,
        leavesQty,
        cumQty,
        accountCode,
        ProductTypeEnum.Spot,
        "20260412",
        SettlTypeEnum.Regular,
        "USD",
        "USD",
        TenorEnum.SN);
  }

  private int encodeOrderCanceled(
      final String orderId,
      final String clOrdId,
      final String symbol,
      final SideEnum side,
      final long timestamp) {
    return SbeTestEncoder.encodeOrderCanceledEvent(
        buf, 0, ++seqNo, timestamp, orderId, clOrdId, clOrdId, symbol, side, ProductTypeEnum.Spot);
  }

  private void dispatch(final int templateId, final int totalLen) {
    // EventConsumer strips the header — pass payload only
    projection.onEvent(seqNo, templateId, buf, HDR_LEN, totalLen - HDR_LEN);
  }

  // ---------------------------------------------------------------------------
  // Tests
  // ---------------------------------------------------------------------------

  @Test
  void orderCreatedPopulatesAllFields() {
    final int len =
        encodeOrderCreated(
            "ORD-001",
            "CLO-001",
            "EURUSD",
            SideEnum.Buy,
            OrdTypeEnum.Limit,
            108_500_000L,
            5L * PRICE_SCALE,
            "ACME",
            1_000_000L);
    dispatch(OrderCreatedEventDecoder.TEMPLATE_ID, len);

    final OrderSnapshot s = projection.getOrder("ORD-001");
    assertNotNull(s);
    assertEquals("ORD-001", s.orderId());
    assertEquals("CLO-001", s.clOrdId());
    assertEquals("EURUSD", s.symbol());
    assertEquals("ACME", s.accountCode());
    assertEquals(SideEnum.Buy, s.side());
    assertEquals(OrdTypeEnum.Limit, s.ordType());
    assertEquals(OrdStatusEnum.New, s.ordStatus());
    assertEquals(ExecTypeEnum.New, s.execType());
    assertEquals(ProductTypeEnum.Spot, s.productType());
    assertEquals(108_500_000L, s.price());
    assertEquals(5L * PRICE_SCALE, s.orderQty());
    assertEquals(5L * PRICE_SCALE, s.leavesQty());
    assertEquals(0, s.cumQty());
    assertEquals(0, s.avgPx());
    assertEquals(1, projection.size());
  }

  @Test
  void orderRejectedPopulatesRejectReason() {
    final int len =
        encodeOrderRejected(
            "CLO-002",
            "EURUSD",
            SideEnum.Sell,
            RejectReasonEnum.AccountNotFound,
            "BAD",
            2_000_000L);
    dispatch(OrderRejectedEventDecoder.TEMPLATE_ID, len);

    final OrderSnapshot s = projection.getOrderByClOrdId("CLO-002");
    assertNotNull(s);
    assertEquals(OrdStatusEnum.Rejected, s.ordStatus());
    assertEquals(ExecTypeEnum.Rejected, s.execType());
    assertEquals(RejectReasonEnum.AccountNotFound, s.rejectReason());
    assertEquals("", s.orderId()); // No orderId on rejection
  }

  @Test
  void orderRejectedQueryableByClOrdId() {
    final int len =
        encodeOrderRejected(
            "CLO-003", "USDJPY", SideEnum.Buy, RejectReasonEnum.InvalidPrice, "ACME", 3_000_000L);
    dispatch(OrderRejectedEventDecoder.TEMPLATE_ID, len);

    assertNotNull(projection.getOrderByClOrdId("CLO-003"));
    assertNull(projection.getOrder("CLO-003")); // Not in orderId index
  }

  @Test
  void partialFillUpdatesQuantitiesAndStatus() {
    final int createLen =
        encodeOrderCreated(
            "ORD-010",
            "CLO-010",
            "EURUSD",
            SideEnum.Buy,
            OrdTypeEnum.Limit,
            108_500_000L,
            10L * PRICE_SCALE,
            "ACME",
            100L);
    dispatch(OrderCreatedEventDecoder.TEMPLATE_ID, createLen);

    final int fillLen =
        encodeOrderFilled(
            "EXE-001",
            "ORD-010",
            "CLO-010",
            "EURUSD",
            SideEnum.Buy,
            108_500_000L,
            3L * PRICE_SCALE,
            7L * PRICE_SCALE,
            3L * PRICE_SCALE,
            "ACME",
            200L);
    dispatch(OrderFilledEventDecoder.TEMPLATE_ID, fillLen);

    final OrderSnapshot s = projection.getOrder("ORD-010");
    assertNotNull(s);
    assertEquals(OrdStatusEnum.PartiallyFilled, s.ordStatus());
    assertEquals(ExecTypeEnum.PartialFill, s.execType());
    assertEquals(7L * PRICE_SCALE, s.leavesQty());
    assertEquals(3L * PRICE_SCALE, s.cumQty());
    assertEquals(108_500_000L, s.avgPx()); // Single fill at 1.0850 → VWAP = 1.0850
  }

  @Test
  void fullFillSetsFilledStatus() {
    final int createLen =
        encodeOrderCreated(
            "ORD-020",
            "CLO-020",
            "EURUSD",
            SideEnum.Buy,
            OrdTypeEnum.Limit,
            108_500_000L,
            5L * PRICE_SCALE,
            "ACME",
            100L);
    dispatch(OrderCreatedEventDecoder.TEMPLATE_ID, createLen);

    final int fillLen =
        encodeOrderFilled(
            "EXE-002",
            "ORD-020",
            "CLO-020",
            "EURUSD",
            SideEnum.Buy,
            108_500_000L,
            5L * PRICE_SCALE,
            0,
            5L * PRICE_SCALE,
            "ACME",
            200L);
    dispatch(OrderFilledEventDecoder.TEMPLATE_ID, fillLen);

    final OrderSnapshot s = projection.getOrder("ORD-020");
    assertEquals(OrdStatusEnum.Filled, s.ordStatus());
    assertEquals(ExecTypeEnum.Fill, s.execType());
    assertEquals(0, s.leavesQty());
  }

  @Test
  void multipleFillsCalculateCorrectVwap() {
    final int createLen =
        encodeOrderCreated(
            "ORD-030",
            "CLO-030",
            "EURUSD",
            SideEnum.Buy,
            OrdTypeEnum.Limit,
            110_000_000L,
            10L * PRICE_SCALE,
            "ACME",
            100L);
    dispatch(OrderCreatedEventDecoder.TEMPLATE_ID, createLen);

    // Fill 1: 3 @ 1.0800
    int fillLen =
        encodeOrderFilled(
            "EXE-010",
            "ORD-030",
            "CLO-030",
            "EURUSD",
            SideEnum.Buy,
            108_000_000L,
            3L * PRICE_SCALE,
            7L * PRICE_SCALE,
            3L * PRICE_SCALE,
            "ACME",
            200L);
    dispatch(OrderFilledEventDecoder.TEMPLATE_ID, fillLen);

    // Fill 2: 4 @ 1.0850
    fillLen =
        encodeOrderFilled(
            "EXE-011",
            "ORD-030",
            "CLO-030",
            "EURUSD",
            SideEnum.Buy,
            108_500_000L,
            4L * PRICE_SCALE,
            3L * PRICE_SCALE,
            7L * PRICE_SCALE,
            "ACME",
            300L);
    dispatch(OrderFilledEventDecoder.TEMPLATE_ID, fillLen);

    // Fill 3: 3 @ 1.0900
    fillLen =
        encodeOrderFilled(
            "EXE-012",
            "ORD-030",
            "CLO-030",
            "EURUSD",
            SideEnum.Buy,
            109_000_000L,
            3L * PRICE_SCALE,
            0,
            10L * PRICE_SCALE,
            "ACME",
            400L);
    dispatch(OrderFilledEventDecoder.TEMPLATE_ID, fillLen);

    final OrderSnapshot s = projection.getOrder("ORD-030");
    assertEquals(OrdStatusEnum.Filled, s.ordStatus());
    assertEquals(10L * PRICE_SCALE, s.cumQty());

    // VWAP = (3*1.0800 + 4*1.0850 + 3*1.0900) / 10 = (3.24 + 4.34 + 3.27) / 10 = 1.085
    // In fixed-point: (3*108000000 + 4*108500000 + 3*109000000) / 10 = 108500000
    assertEquals(108_500_000L, s.avgPx());
  }

  @Test
  void vwapHandlesLargeNotionals() {
    // 500M EUR — exercises the 128-bit mulDiv path
    final long largeQty = 500_000_000L * PRICE_SCALE; // 50_000_000_000_000_000
    final long price = 108_500_000L; // 1.0850

    final int createLen =
        encodeOrderCreated(
            "ORD-040",
            "CLO-040",
            "EURUSD",
            SideEnum.Buy,
            OrdTypeEnum.Limit,
            price,
            largeQty,
            "ACME",
            100L);
    dispatch(OrderCreatedEventDecoder.TEMPLATE_ID, createLen);

    final int fillLen =
        encodeOrderFilled(
            "EXE-020",
            "ORD-040",
            "CLO-040",
            "EURUSD",
            SideEnum.Buy,
            price,
            largeQty,
            0,
            largeQty,
            "ACME",
            200L);
    dispatch(OrderFilledEventDecoder.TEMPLATE_ID, fillLen);

    final OrderSnapshot s = projection.getOrder("ORD-040");
    assertEquals(price, s.avgPx());
    assertEquals(0, projection.errorCount());
  }

  @Test
  void vwapSafeWhenCumQtyZero() {
    // Edge case: fill with cumQty=0 (should not divide by zero)
    final int createLen =
        encodeOrderCreated(
            "ORD-045",
            "CLO-045",
            "EURUSD",
            SideEnum.Buy,
            OrdTypeEnum.Limit,
            108_500_000L,
            5L * PRICE_SCALE,
            "ACME",
            100L);
    dispatch(OrderCreatedEventDecoder.TEMPLATE_ID, createLen);

    final int fillLen =
        encodeOrderFilled(
            "EXE-025",
            "ORD-045",
            "CLO-045",
            "EURUSD",
            SideEnum.Buy,
            108_500_000L,
            0,
            5L * PRICE_SCALE,
            0,
            "ACME",
            200L);
    dispatch(OrderFilledEventDecoder.TEMPLATE_ID, fillLen);

    final OrderSnapshot s = projection.getOrder("ORD-045");
    assertEquals(0, s.avgPx()); // No division by zero
    assertEquals(0, projection.errorCount());
  }

  @Test
  void orderCanceledSetsStatus() {
    final int createLen =
        encodeOrderCreated(
            "ORD-050",
            "CLO-050",
            "EURUSD",
            SideEnum.Buy,
            OrdTypeEnum.Limit,
            108_500_000L,
            5L * PRICE_SCALE,
            "ACME",
            100L);
    dispatch(OrderCreatedEventDecoder.TEMPLATE_ID, createLen);

    final int cancelLen = encodeOrderCanceled("ORD-050", "CLO-050", "EURUSD", SideEnum.Buy, 200L);
    dispatch(OrderCanceledEventDecoder.TEMPLATE_ID, cancelLen);

    final OrderSnapshot s = projection.getOrder("ORD-050");
    assertEquals(OrdStatusEnum.Canceled, s.ordStatus());
    assertEquals(ExecTypeEnum.Canceled, s.execType());
    assertEquals(0, s.leavesQty()); // FIX 4.4: leavesQty must be 0 when canceled
  }

  @Test
  void fillForUnknownOrderIdDroppedSilently() {
    final int fillLen =
        encodeOrderFilled(
            "EXE-099",
            "ORD-UNKNOWN",
            "CLO-099",
            "EURUSD",
            SideEnum.Buy,
            108_500_000L,
            1L * PRICE_SCALE,
            0,
            1L * PRICE_SCALE,
            "ACME",
            100L);
    dispatch(OrderFilledEventDecoder.TEMPLATE_ID, fillLen);

    assertEquals(0, projection.errorCount());
    assertEquals(1, projection.eventsProcessed());
  }

  @Test
  void cancelForUnknownOrderIdDroppedSilently() {
    final int cancelLen =
        encodeOrderCanceled("ORD-UNKNOWN", "CLO-099", "EURUSD", SideEnum.Buy, 100L);
    dispatch(OrderCanceledEventDecoder.TEMPLATE_ID, cancelLen);

    assertEquals(0, projection.errorCount());
  }

  @Test
  void duplicateOrderIdCleansUpSecondaryIndexes() {
    // First creation
    final int len1 =
        encodeOrderCreated(
            "ORD-060",
            "CLO-060A",
            "EURUSD",
            SideEnum.Buy,
            OrdTypeEnum.Limit,
            108_500_000L,
            5L * PRICE_SCALE,
            "ACME",
            100L);
    dispatch(OrderCreatedEventDecoder.TEMPLATE_ID, len1);

    // Second creation with same orderId but different clOrdId and account
    final int len2 =
        encodeOrderCreated(
            "ORD-060",
            "CLO-060B",
            "USDJPY",
            SideEnum.Sell,
            OrdTypeEnum.Market,
            0,
            3L * PRICE_SCALE,
            "BETA",
            200L);
    dispatch(OrderCreatedEventDecoder.TEMPLATE_ID, len2);

    // Old clOrdId should be gone
    assertNull(projection.getOrderByClOrdId("CLO-060A"));
    // New clOrdId should work
    assertNotNull(projection.getOrderByClOrdId("CLO-060B"));
    // Old account should not have the order
    assertTrue(projection.getOrdersByAccount("ACME").isEmpty());
    // New account should have the order
    assertEquals(1, projection.getOrdersByAccount("BETA").size());
    // Old symbol index should be empty
    assertTrue(projection.getOrdersBySymbol("EURUSD").isEmpty());
    // New symbol index should have the order
    assertEquals(1, projection.getOrdersBySymbol("USDJPY").size());
  }

  @Test
  void secondaryIndexByClOrdId() {
    final int len =
        encodeOrderCreated(
            "ORD-070",
            "CLO-070",
            "EURUSD",
            SideEnum.Buy,
            OrdTypeEnum.Limit,
            108_500_000L,
            5L * PRICE_SCALE,
            "ACME",
            100L);
    dispatch(OrderCreatedEventDecoder.TEMPLATE_ID, len);

    final OrderSnapshot byOrderId = projection.getOrder("ORD-070");
    final OrderSnapshot byClOrdId = projection.getOrderByClOrdId("CLO-070");
    assertNotNull(byOrderId);
    assertNotNull(byClOrdId);
    assertEquals(byOrderId.orderId(), byClOrdId.orderId());
    assertEquals(byOrderId.clOrdId(), byClOrdId.clOrdId());
  }

  @Test
  void accountIndexReturnsCorrectOrders() {
    encodeAndDispatchCreate("ORD-080", "CLO-080", "EURUSD", "ACME");
    encodeAndDispatchCreate("ORD-081", "CLO-081", "USDJPY", "ACME");
    encodeAndDispatchCreate("ORD-082", "CLO-082", "GBPUSD", "BETA");

    final List<OrderSnapshot> acmeOrders = projection.getOrdersByAccount("ACME");
    assertEquals(2, acmeOrders.size());
    final List<OrderSnapshot> betaOrders = projection.getOrdersByAccount("BETA");
    assertEquals(1, betaOrders.size());
  }

  @Test
  void activeOrdersFiltersCorrectly() {
    encodeAndDispatchCreate("ORD-090", "CLO-090", "EURUSD", "ACME");
    encodeAndDispatchCreate("ORD-091", "CLO-091", "USDJPY", "ACME");

    // Cancel one
    final int cancelLen = encodeOrderCanceled("ORD-090", "CLO-090", "EURUSD", SideEnum.Buy, 200L);
    dispatch(OrderCanceledEventDecoder.TEMPLATE_ID, cancelLen);

    final List<OrderSnapshot> active = projection.getActiveOrders();
    assertEquals(1, active.size());
    assertEquals("ORD-091", active.get(0).orderId());
  }

  @Test
  void ordersBySymbolFiltersCorrectly() {
    encodeAndDispatchCreate("ORD-100", "CLO-100", "EURUSD", "ACME");
    encodeAndDispatchCreate("ORD-101", "CLO-101", "EURUSD", "BETA");
    encodeAndDispatchCreate("ORD-102", "CLO-102", "USDJPY", "ACME");

    assertEquals(2, projection.getOrdersBySymbol("EURUSD").size());
    assertEquals(1, projection.getOrdersBySymbol("USDJPY").size());
    assertEquals(0, projection.getOrdersBySymbol("GBPUSD").size());
  }

  @Test
  void resetClearsAllState() {
    encodeAndDispatchCreate("ORD-110", "CLO-110", "EURUSD", "ACME");
    projection.reset();

    assertEquals(0, projection.size());
    assertEquals(0, projection.lastProcessedSequence());
    assertEquals(0, projection.eventsProcessed());
    assertNull(projection.getOrder("ORD-110"));
  }

  @Test
  void lastProcessedSequenceUpdatedOnEveryEvent() {
    final int len1 =
        encodeOrderCreated(
            "ORD-120",
            "CLO-120",
            "EURUSD",
            SideEnum.Buy,
            OrdTypeEnum.Limit,
            108_500_000L,
            5L * PRICE_SCALE,
            "ACME",
            100L);
    dispatch(OrderCreatedEventDecoder.TEMPLATE_ID, len1);
    assertEquals(1, projection.lastProcessedSequence());

    final int len2 =
        encodeOrderCreated(
            "ORD-121",
            "CLO-121",
            "USDJPY",
            SideEnum.Sell,
            OrdTypeEnum.Market,
            0,
            3L * PRICE_SCALE,
            "ACME",
            200L);
    dispatch(OrderCreatedEventDecoder.TEMPLATE_ID, len2);
    assertEquals(2, projection.lastProcessedSequence());
  }

  @Test
  void lastProcessedSequenceUpdatedEvenOnError() {
    // Dispatch a truncated buffer — should cause a decode error
    seqNo = 99;
    projection.onEvent(99, OrderCreatedEventDecoder.TEMPLATE_ID, buf, 0, 2);
    assertEquals(99, projection.lastProcessedSequence());
    assertEquals(1, projection.errorCount());
  }

  @Test
  void replayAfterResetProducesSameState() {
    // Build initial state
    final int len1 =
        encodeOrderCreated(
            "ORD-130",
            "CLO-130",
            "EURUSD",
            SideEnum.Buy,
            OrdTypeEnum.Limit,
            108_500_000L,
            10L * PRICE_SCALE,
            "ACME",
            100L);
    dispatch(OrderCreatedEventDecoder.TEMPLATE_ID, len1);

    final int fillLen =
        encodeOrderFilled(
            "EXE-030",
            "ORD-130",
            "CLO-130",
            "EURUSD",
            SideEnum.Buy,
            108_500_000L,
            5L * PRICE_SCALE,
            5L * PRICE_SCALE,
            5L * PRICE_SCALE,
            "ACME",
            200L);
    dispatch(OrderFilledEventDecoder.TEMPLATE_ID, fillLen);

    final OrderSnapshot before = projection.getOrder("ORD-130");

    // Reset and replay
    projection.reset();
    seqNo = 0;

    final int replayLen1 =
        encodeOrderCreated(
            "ORD-130",
            "CLO-130",
            "EURUSD",
            SideEnum.Buy,
            OrdTypeEnum.Limit,
            108_500_000L,
            10L * PRICE_SCALE,
            "ACME",
            100L);
    dispatch(OrderCreatedEventDecoder.TEMPLATE_ID, replayLen1);

    final int replayFillLen =
        encodeOrderFilled(
            "EXE-030",
            "ORD-130",
            "CLO-130",
            "EURUSD",
            SideEnum.Buy,
            108_500_000L,
            5L * PRICE_SCALE,
            5L * PRICE_SCALE,
            5L * PRICE_SCALE,
            "ACME",
            200L);
    dispatch(OrderFilledEventDecoder.TEMPLATE_ID, replayFillLen);

    final OrderSnapshot after = projection.getOrder("ORD-130");
    assertEquals(before.ordStatus(), after.ordStatus());
    assertEquals(before.cumQty(), after.cumQty());
    assertEquals(before.avgPx(), after.avgPx());
    assertEquals(before.leavesQty(), after.leavesQty());
  }

  @Test
  void execIdTrackedFromFill() {
    final int createLen =
        encodeOrderCreated(
            "ORD-140",
            "CLO-140",
            "EURUSD",
            SideEnum.Buy,
            OrdTypeEnum.Limit,
            108_500_000L,
            5L * PRICE_SCALE,
            "ACME",
            100L);
    dispatch(OrderCreatedEventDecoder.TEMPLATE_ID, createLen);

    assertEquals("", projection.getOrder("ORD-140").lastExecId());

    final int fillLen =
        encodeOrderFilled(
            "EXE-040",
            "ORD-140",
            "CLO-140",
            "EURUSD",
            SideEnum.Buy,
            108_500_000L,
            5L * PRICE_SCALE,
            0,
            5L * PRICE_SCALE,
            "ACME",
            200L);
    dispatch(OrderFilledEventDecoder.TEMPLATE_ID, fillLen);

    assertEquals("EXE-040", projection.getOrder("ORD-140").lastExecId());
  }

  @Test
  void fxFieldsPopulatedOnCreated() {
    final int len =
        encodeOrderCreated(
            "ORD-150",
            "CLO-150",
            "EURUSD",
            SideEnum.Buy,
            OrdTypeEnum.Limit,
            108_500_000L,
            5L * PRICE_SCALE,
            "ACME",
            100L);
    dispatch(OrderCreatedEventDecoder.TEMPLATE_ID, len);

    final OrderSnapshot s = projection.getOrder("ORD-150");
    assertEquals("20260412", s.settlDate());
    assertEquals(SettlTypeEnum.Regular, s.settlType());
    assertEquals("USD", s.currency());
    assertEquals("USD", s.settlCurrency());
    assertEquals(TenorEnum.SN, s.tenor());
  }

  // ---------------------------------------------------------------------------
  // Convenience helper
  // ---------------------------------------------------------------------------

  private void encodeAndDispatchCreate(
      final String orderId, final String clOrdId, final String symbol, final String account) {
    final int len =
        encodeOrderCreated(
            orderId,
            clOrdId,
            symbol,
            SideEnum.Buy,
            OrdTypeEnum.Limit,
            108_500_000L,
            5L * PRICE_SCALE,
            account,
            100L);
    dispatch(OrderCreatedEventDecoder.TEMPLATE_ID, len);
  }
}
