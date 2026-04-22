package com.trading.engine.projections.quote;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.trading.engine.messages.sbe.MessageHeaderEncoder;
import com.trading.engine.messages.sbe.OrdTypeEnum;
import com.trading.engine.messages.sbe.OrderCreatedEventDecoder;
import com.trading.engine.messages.sbe.ProductTypeEnum;
import com.trading.engine.messages.sbe.QuoteCreatedEventDecoder;
import com.trading.engine.messages.sbe.QuoteCreatedEventEncoder;
import com.trading.engine.messages.sbe.QuoteExpiredEventDecoder;
import com.trading.engine.messages.sbe.QuoteRejectReasonEnum;
import com.trading.engine.messages.sbe.QuoteRejectedEventDecoder;
import com.trading.engine.messages.sbe.QuoteRequestedEventDecoder;
import com.trading.engine.messages.sbe.SettlTypeEnum;
import com.trading.engine.messages.sbe.SideEnum;
import com.trading.engine.messages.sbe.TenorEnum;
import com.trading.engine.messages.sbe.TimeInForceEnum;
import com.trading.engine.testsupport.sbe.SbeTestEncoder;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link QuoteProjection} — verifies quote lifecycle tracking across 5 event types
 * (QuoteRequestedEvent 104, QuoteCreatedEvent 105, QuoteRejectedEvent 106, QuoteExpiredEvent 107,
 * OrderCreatedEvent 100) with terminal state guards, FX field capture, eviction, and concurrency.
 */
class QuoteProjectionTest {

  private static final long PRICE_SCALE = 100_000_000L;
  private static final int HDR_LEN = MessageHeaderEncoder.ENCODED_LENGTH;

  private QuoteProjection projection;
  private MutableDirectBuffer buf;
  private long seqNo;

  @BeforeEach
  void setUp() {
    projection = new QuoteProjection(64);
    buf = new ExpandableArrayBuffer(512);
    seqNo = 0;
  }

  // ---------------------------------------------------------------------------
  // Encoding helpers — delegate to shared SbeTestEncoder
  // ---------------------------------------------------------------------------

  private int encodeQuoteRequested(
      final String quoteReqId,
      final String symbol,
      final SideEnum side,
      final long orderQty,
      final String accountCode,
      final long timestamp) {
    return SbeTestEncoder.encodeQuoteRequestedEvent(
        buf, 0, ++seqNo, timestamp, quoteReqId, symbol, side, orderQty, accountCode);
  }

  private int encodeQuoteCreated(
      final String quoteId,
      final String quoteReqId,
      final String symbol,
      final SideEnum side,
      final String accountCode,
      final long bidPx,
      final long offerPx,
      final long validUntil,
      final long timestamp) {
    return SbeTestEncoder.encodeQuoteCreatedEvent(
        buf,
        0,
        ++seqNo,
        timestamp,
        quoteId,
        quoteReqId,
        symbol,
        side,
        accountCode,
        bidPx,
        offerPx,
        validUntil);
  }

  private int encodeQuoteCreatedFull(
      final String quoteId,
      final String quoteReqId,
      final String symbol,
      final SideEnum side,
      final String accountCode,
      final long bidPx,
      final long offerPx,
      final long bidSize,
      final long offerSize,
      final long validUntil,
      final long timestamp,
      final ProductTypeEnum productType,
      final String settlDate,
      final SettlTypeEnum settlType,
      final String currency,
      final String settlCurrency,
      final TenorEnum tenor,
      final long swapPoints) {
    return SbeTestEncoder.encodeQuoteCreatedEvent(
        buf,
        0,
        ++seqNo,
        timestamp,
        quoteId,
        quoteReqId,
        symbol,
        side,
        accountCode,
        bidPx,
        offerPx,
        bidSize,
        offerSize,
        validUntil,
        productType,
        settlDate,
        settlType,
        currency,
        settlCurrency,
        tenor,
        swapPoints);
  }

  private int encodeQuoteRejected(
      final String quoteReqId,
      final String symbol,
      final SideEnum side,
      final QuoteRejectReasonEnum reason,
      final String text,
      final long timestamp) {
    return SbeTestEncoder.encodeQuoteRejectedEvent(
        buf, 0, ++seqNo, timestamp, quoteReqId, symbol, side, reason, text);
  }

  private int encodeQuoteExpired(
      final String quoteId,
      final String quoteReqId,
      final String symbol,
      final SideEnum side,
      final long timestamp) {
    return SbeTestEncoder.encodeQuoteExpiredEvent(
        buf, 0, ++seqNo, timestamp, quoteId, quoteReqId, symbol, side);
  }

  private int encodeOrderCreated(
      final String orderId,
      final String clOrdId,
      final String symbol,
      final SideEnum side,
      final OrdTypeEnum ordType,
      final long price,
      final long orderQty,
      final String quoteId,
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
        quoteId,
        accountCode,
        ProductTypeEnum.Spot,
        "20260101",
        SettlTypeEnum.Regular,
        "USD",
        "EUR",
        TenorEnum.SN);
  }

  private void dispatch(final int templateId, final int totalLen) {
    projection.onEvent(seqNo, templateId, buf, HDR_LEN, totalLen - HDR_LEN);
  }

  // ---------------------------------------------------------------------------
  // Core lifecycle tests
  // ---------------------------------------------------------------------------

  @Test
  void onEvent_quoteRequested_createsViewInRequestedStatus() {
    final int len =
        encodeQuoteRequested("RFQ-001", "EURUSD", SideEnum.Buy, PRICE_SCALE, "ACCT01", 1_000_000L);
    dispatch(QuoteRequestedEventDecoder.TEMPLATE_ID, len);

    final QuoteSnapshot s = projection.getQuoteByReqId("RFQ-001");
    assertNotNull(s);
    assertEquals("RFQ-001", s.quoteReqId());
    assertEquals("EURUSD", s.symbol());
    assertEquals(SideEnum.Buy, s.side());
    assertEquals(PRICE_SCALE, s.orderQty());
    assertEquals("ACCT01", s.accountCode());
    assertEquals(QuoteStatus.Requested, s.status());
    assertEquals("", s.quoteId()); // No quoteId yet
    assertEquals(1_000_000L, s.createdAt());
    assertEquals(1, projection.size());
  }

  @Test
  void onEvent_quoteCreated_populatesAllFieldsAndSetsActive() {
    final int len =
        encodeQuoteCreated(
            "QTE-001",
            "RFQ-001",
            "EURUSD",
            SideEnum.Buy,
            "ACCT01",
            108_500_000L,
            108_700_000L,
            2_000_000L,
            1_000_000L);
    dispatch(QuoteCreatedEventDecoder.TEMPLATE_ID, len);

    final QuoteSnapshot s = projection.getQuote("QTE-001");
    assertNotNull(s);
    assertEquals("QTE-001", s.quoteId());
    assertEquals("RFQ-001", s.quoteReqId());
    assertEquals(QuoteStatus.Active, s.status());
    assertEquals(108_500_000L, s.bidPx());
    assertEquals(108_700_000L, s.offerPx());
    assertEquals(2_000_000L, s.validUntil());
    assertEquals(-1L, s.responseLatencyNanos()); // No prior 104 — sentinel
    assertEquals(0, s.orderQty()); // No prior 104 — zero
  }

  @Test
  void onEvent_quoteCreatedAfterRequested_updatesExistingView() {
    final int len1 =
        encodeQuoteRequested(
            "RFQ-002", "GBPUSD", SideEnum.Sell, 2 * PRICE_SCALE, "ACCT02", 1_000_000L);
    dispatch(QuoteRequestedEventDecoder.TEMPLATE_ID, len1);

    final int len2 =
        encodeQuoteCreated(
            "QTE-002",
            "RFQ-002",
            "GBPUSD",
            SideEnum.Sell,
            "ACCT02",
            150_000_000L,
            150_200_000L,
            5_000_000L,
            2_000_000L);
    dispatch(QuoteCreatedEventDecoder.TEMPLATE_ID, len2);

    final QuoteSnapshot s = projection.getQuote("QTE-002");
    assertNotNull(s);
    assertEquals(QuoteStatus.Active, s.status());
    assertEquals(2 * PRICE_SCALE, s.orderQty()); // Preserved from 104
    assertEquals(2_000_000L - 1_000_000L, s.responseLatencyNanos()); // Computed
    assertEquals(1_000_000L, s.createdAt()); // Set once from 104, not overwritten
  }

  @Test
  void onEvent_quoteRejected_setsStatusAndRejectReason() {
    final int len1 =
        encodeQuoteRequested("RFQ-003", "USDJPY", SideEnum.Buy, PRICE_SCALE, "ACCT01", 1_000_000L);
    dispatch(QuoteRequestedEventDecoder.TEMPLATE_ID, len1);

    final int len2 =
        encodeQuoteRejected(
            "RFQ-003",
            "USDJPY",
            SideEnum.Buy,
            QuoteRejectReasonEnum.UnknownSymbol,
            "symbol not tradeable",
            2_000_000L);
    dispatch(QuoteRejectedEventDecoder.TEMPLATE_ID, len2);

    final QuoteSnapshot s = projection.getQuoteByReqId("RFQ-003");
    assertNotNull(s);
    assertEquals(QuoteStatus.Rejected, s.status());
    assertEquals(QuoteRejectReasonEnum.UnknownSymbol, s.rejectReason());
    assertEquals("symbol not tradeable", s.text());
    assertEquals(PRICE_SCALE, s.orderQty()); // Preserved from 104
    assertEquals("USDJPY", s.symbol()); // Preserved from 104
  }

  @Test
  void onEvent_quoteRejectedWithoutPriorRequest_createsRejectedView() {
    final int len =
        encodeQuoteRejected(
            "RFQ-004",
            "AUDUSD",
            SideEnum.Sell,
            QuoteRejectReasonEnum.ExchangeClosed,
            "market closed",
            1_000_000L);
    dispatch(QuoteRejectedEventDecoder.TEMPLATE_ID, len);

    final QuoteSnapshot s = projection.getQuoteByReqId("RFQ-004");
    assertNotNull(s);
    assertEquals(QuoteStatus.Rejected, s.status());
    assertEquals("AUDUSD", s.symbol());
    assertEquals(1, projection.size());
  }

  @Test
  void onEvent_quoteExpired_setsStatusExpired() {
    final int len1 =
        encodeQuoteCreated(
            "QTE-003",
            "RFQ-005",
            "EURUSD",
            SideEnum.Buy,
            "ACCT01",
            108_500_000L,
            108_700_000L,
            3_000_000L,
            1_000_000L);
    dispatch(QuoteCreatedEventDecoder.TEMPLATE_ID, len1);

    final int len2 = encodeQuoteExpired("QTE-003", "RFQ-005", "EURUSD", SideEnum.Buy, 4_000_000L);
    dispatch(QuoteExpiredEventDecoder.TEMPLATE_ID, len2);

    final QuoteSnapshot s = projection.getQuote("QTE-003");
    assertNotNull(s);
    assertEquals(QuoteStatus.Expired, s.status());
    assertEquals(4_000_000L, s.lastUpdatedAt());
  }

  @Test
  void onEvent_quoteExpired_doesNotOverrideUsedStatus() {
    final int len1 =
        encodeQuoteCreated(
            "QTE-004",
            "RFQ-006",
            "EURUSD",
            SideEnum.Buy,
            "ACCT01",
            108_500_000L,
            108_700_000L,
            5_000_000L,
            1_000_000L);
    dispatch(QuoteCreatedEventDecoder.TEMPLATE_ID, len1);

    final int len2 =
        encodeOrderCreated(
            "ORD-001",
            "CLO-001",
            "EURUSD",
            SideEnum.Buy,
            OrdTypeEnum.PreviouslyQuoted,
            108_500_000L,
            PRICE_SCALE,
            "QTE-004",
            "ACCT01",
            2_000_000L);
    dispatch(OrderCreatedEventDecoder.TEMPLATE_ID, len2);

    final int len3 = encodeQuoteExpired("QTE-004", "RFQ-006", "EURUSD", SideEnum.Buy, 3_000_000L);
    dispatch(QuoteExpiredEventDecoder.TEMPLATE_ID, len3);

    final QuoteSnapshot s = projection.getQuote("QTE-004");
    assertNotNull(s);
    assertEquals(QuoteStatus.Used, s.status()); // Terminal — not overridden by Expired
    assertEquals(
        2_000_000L, s.lastUpdatedAt()); // Timestamp preserved — not pushed forward by late 107
  }

  @Test
  void onEvent_quoteExpired_doesNotOverrideRejectedStatus() {
    final int len1 =
        encodeQuoteRequested("RFQ-007", "EURUSD", SideEnum.Buy, PRICE_SCALE, "ACCT01", 1_000_000L);
    dispatch(QuoteRequestedEventDecoder.TEMPLATE_ID, len1);

    final int len2 =
        encodeQuoteRejected(
            "RFQ-007", "EURUSD", SideEnum.Buy, QuoteRejectReasonEnum.Other, "rejected", 2_000_000L);
    dispatch(QuoteRejectedEventDecoder.TEMPLATE_ID, len2);

    final int len3 = encodeQuoteExpired("", "RFQ-007", "EURUSD", SideEnum.Buy, 3_000_000L);
    dispatch(QuoteExpiredEventDecoder.TEMPLATE_ID, len3);

    final QuoteSnapshot s = projection.getQuoteByReqId("RFQ-007");
    assertNotNull(s);
    assertEquals(QuoteStatus.Rejected, s.status()); // Terminal — not overridden
    assertEquals(
        2_000_000L, s.lastUpdatedAt()); // Timestamp preserved — not pushed forward by late 107
  }

  @Test
  void onEvent_quoteRejected_doesNotOverrideUsedStatus() {
    final int len1 =
        encodeQuoteCreated(
            "QTE-TG1",
            "RFQ-TG1",
            "EURUSD",
            SideEnum.Buy,
            "ACCT01",
            108_500_000L,
            108_700_000L,
            5_000_000L,
            1_000_000L);
    dispatch(QuoteCreatedEventDecoder.TEMPLATE_ID, len1);

    final int len2 =
        encodeOrderCreated(
            "ORD-TG1",
            "CLO-TG1",
            "EURUSD",
            SideEnum.Buy,
            OrdTypeEnum.PreviouslyQuoted,
            108_500_000L,
            PRICE_SCALE,
            "QTE-TG1",
            "ACCT01",
            2_000_000L);
    dispatch(OrderCreatedEventDecoder.TEMPLATE_ID, len2);

    // Rejected arrives after Used — should NOT override terminal state
    final int len3 =
        encodeQuoteRejected(
            "RFQ-TG1",
            "EURUSD",
            SideEnum.Buy,
            QuoteRejectReasonEnum.Other,
            "late reject",
            3_000_000L);
    dispatch(QuoteRejectedEventDecoder.TEMPLATE_ID, len3);

    final QuoteSnapshot s = projection.getQuoteByReqId("RFQ-TG1");
    assertNotNull(s);
    assertEquals(QuoteStatus.Used, s.status()); // Terminal — not overridden by Rejected
  }

  @Test
  void onEvent_quoteRejected_doesNotOverrideExpiredStatus() {
    final int len1 =
        encodeQuoteCreated(
            "QTE-TG2",
            "RFQ-TG2",
            "EURUSD",
            SideEnum.Buy,
            "ACCT01",
            108_500_000L,
            108_700_000L,
            5_000_000L,
            1_000_000L);
    dispatch(QuoteCreatedEventDecoder.TEMPLATE_ID, len1);

    final int len2 = encodeQuoteExpired("QTE-TG2", "RFQ-TG2", "EURUSD", SideEnum.Buy, 2_000_000L);
    dispatch(QuoteExpiredEventDecoder.TEMPLATE_ID, len2);

    // Rejected arrives after Expired — should NOT override terminal state
    final int len3 =
        encodeQuoteRejected(
            "RFQ-TG2",
            "EURUSD",
            SideEnum.Buy,
            QuoteRejectReasonEnum.Other,
            "late reject",
            3_000_000L);
    dispatch(QuoteRejectedEventDecoder.TEMPLATE_ID, len3);

    final QuoteSnapshot s = projection.getQuoteByReqId("RFQ-TG2");
    assertNotNull(s);
    assertEquals(QuoteStatus.Expired, s.status()); // Terminal — not overridden by Rejected
  }

  @Test
  void onEvent_quoteCreated_doesNotOverrideUsedStatus() {
    // Quote goes Requested → Active → Used, then a duplicate 105 replays
    final int len1 =
        encodeQuoteCreated(
            "QTE-TG3",
            "RFQ-TG3",
            "EURUSD",
            SideEnum.Buy,
            "ACCT01",
            108_500_000L,
            108_700_000L,
            5_000_000L,
            1_000_000L);
    dispatch(QuoteCreatedEventDecoder.TEMPLATE_ID, len1);

    final int len2 =
        encodeOrderCreated(
            "ORD-TG3",
            "CLO-TG3",
            "EURUSD",
            SideEnum.Buy,
            OrdTypeEnum.PreviouslyQuoted,
            108_500_000L,
            PRICE_SCALE,
            "QTE-TG3",
            "ACCT01",
            2_000_000L);
    dispatch(OrderCreatedEventDecoder.TEMPLATE_ID, len2);

    // Duplicate QuoteCreated replays — must NOT revert Used back to Active
    final int len3 =
        encodeQuoteCreated(
            "QTE-TG3",
            "RFQ-TG3",
            "EURUSD",
            SideEnum.Buy,
            "ACCT01",
            109_000_000L,
            109_200_000L,
            6_000_000L,
            3_000_000L);
    dispatch(QuoteCreatedEventDecoder.TEMPLATE_ID, len3);

    final QuoteSnapshot s = projection.getQuote("QTE-TG3");
    assertNotNull(s);
    assertEquals(QuoteStatus.Used, s.status()); // Terminal — not overridden by late 105
    assertEquals(2_000_000L, s.lastUpdatedAt()); // Timestamp preserved
  }

  @Test
  void onEvent_quoteCreated_differentQuoteIdForSameReqId_removesStaleEntry() {
    // First 105 with quoteId=QTE-OLD
    final int len1 =
        encodeQuoteCreated(
            "QTE-OLD",
            "RFQ-STALE",
            "EURUSD",
            SideEnum.Buy,
            "ACCT01",
            108_500_000L,
            108_700_000L,
            5_000_000L,
            1_000_000L);
    dispatch(QuoteCreatedEventDecoder.TEMPLATE_ID, len1);

    assertNotNull(projection.getQuote("QTE-OLD"));

    // Second 105 with same quoteReqId but different quoteId=QTE-NEW (replay with corrected ID)
    final int len2 =
        encodeQuoteCreated(
            "QTE-NEW",
            "RFQ-STALE",
            "EURUSD",
            SideEnum.Buy,
            "ACCT01",
            109_000_000L,
            109_200_000L,
            6_000_000L,
            2_000_000L);
    dispatch(QuoteCreatedEventDecoder.TEMPLATE_ID, len2);

    // Old quoteId entry must be removed — should not return the view with mismatched quoteId
    assertNull(projection.getQuote("QTE-OLD"));
    // New quoteId should work
    final QuoteSnapshot s = projection.getQuote("QTE-NEW");
    assertNotNull(s);
    assertEquals("QTE-NEW", s.quoteId());
    assertEquals("RFQ-STALE", s.quoteReqId());
  }

  @Test
  void onEvent_orderCreatedWithQuoteId_marksQuoteUsed() {
    final int len1 =
        encodeQuoteCreated(
            "QTE-005",
            "RFQ-008",
            "EURUSD",
            SideEnum.Buy,
            "ACCT01",
            108_500_000L,
            108_700_000L,
            5_000_000L,
            1_000_000L);
    dispatch(QuoteCreatedEventDecoder.TEMPLATE_ID, len1);

    final int len2 =
        encodeOrderCreated(
            "ORD-002",
            "CLO-002",
            "EURUSD",
            SideEnum.Buy,
            OrdTypeEnum.PreviouslyQuoted,
            108_500_000L,
            PRICE_SCALE,
            "QTE-005",
            "ACCT01",
            2_000_000L);
    dispatch(OrderCreatedEventDecoder.TEMPLATE_ID, len2);

    final QuoteSnapshot s = projection.getQuote("QTE-005");
    assertNotNull(s);
    assertEquals(QuoteStatus.Used, s.status());
    assertEquals(2_000_000L, s.lastUpdatedAt());
  }

  @Test
  void onEvent_orderCreatedWithQuoteIdButWrongOrdType_noEffect() {
    final int len1 =
        encodeQuoteCreated(
            "QTE-006",
            "RFQ-009",
            "EURUSD",
            SideEnum.Buy,
            "ACCT01",
            108_500_000L,
            108_700_000L,
            5_000_000L,
            1_000_000L);
    dispatch(QuoteCreatedEventDecoder.TEMPLATE_ID, len1);

    final int len2 =
        encodeOrderCreated(
            "ORD-003",
            "CLO-003",
            "EURUSD",
            SideEnum.Buy,
            OrdTypeEnum.Limit,
            108_500_000L,
            PRICE_SCALE,
            "QTE-006",
            "ACCT01",
            2_000_000L);
    dispatch(OrderCreatedEventDecoder.TEMPLATE_ID, len2);

    assertEquals(QuoteStatus.Active, projection.getQuote("QTE-006").status());
  }

  @Test
  void onEvent_orderCreatedEmptyQuoteId_noEffect() {
    final int len1 =
        encodeQuoteCreated(
            "QTE-007",
            "RFQ-010",
            "EURUSD",
            SideEnum.Buy,
            "ACCT01",
            108_500_000L,
            108_700_000L,
            5_000_000L,
            1_000_000L);
    dispatch(QuoteCreatedEventDecoder.TEMPLATE_ID, len1);

    final int len2 =
        encodeOrderCreated(
            "ORD-004",
            "CLO-004",
            "EURUSD",
            SideEnum.Buy,
            OrdTypeEnum.PreviouslyQuoted,
            108_500_000L,
            PRICE_SCALE,
            "",
            "ACCT01",
            2_000_000L);
    dispatch(OrderCreatedEventDecoder.TEMPLATE_ID, len2);

    assertEquals(QuoteStatus.Active, projection.getQuote("QTE-007").status());
  }

  // ---------------------------------------------------------------------------
  // Query method tests
  // ---------------------------------------------------------------------------

  @Test
  void getActiveQuotes_filtersCorrectly() {
    dispatchQuoteInState("QTE-A", "RFQ-A", "EURUSD", QuoteStatus.Active, 1_000_000L);
    dispatchQuoteInState("QTE-B", "RFQ-B", "EURUSD", QuoteStatus.Expired, 2_000_000L);
    dispatchQuoteInState("QTE-C", "RFQ-C", "GBPUSD", QuoteStatus.Active, 3_000_000L);

    final List<QuoteSnapshot> active = projection.getActiveQuotes();
    assertEquals(2, active.size());
    assertTrue(active.stream().allMatch(s -> s.status() == QuoteStatus.Active));
  }

  @Test
  void getInFlightQuotes_returnsRequestedAndActive() {
    // Requested-only (no 105 yet)
    final int len1 =
        encodeQuoteRequested("RFQ-INF1", "EURUSD", SideEnum.Buy, PRICE_SCALE, "ACCT01", 1_000_000L);
    dispatch(QuoteRequestedEventDecoder.TEMPLATE_ID, len1);

    // Active
    dispatchQuoteInState("QTE-INF2", "RFQ-INF2", "EURUSD", QuoteStatus.Active, 2_000_000L);

    // Expired
    dispatchQuoteInState("QTE-INF3", "RFQ-INF3", "EURUSD", QuoteStatus.Expired, 3_000_000L);

    final List<QuoteSnapshot> inFlight = projection.getInFlightQuotes();
    assertEquals(2, inFlight.size());
    assertTrue(
        inFlight.stream()
            .allMatch(
                s -> s.status() == QuoteStatus.Requested || s.status() == QuoteStatus.Active));
  }

  @Test
  void getQuotesBySymbol_returnsCorrectQuotes() {
    dispatchQuoteInState("QTE-S1", "RFQ-S1", "EURUSD", QuoteStatus.Active, 1_000_000L);
    dispatchQuoteInState("QTE-S2", "RFQ-S2", "GBPUSD", QuoteStatus.Active, 2_000_000L);
    dispatchQuoteInState("QTE-S3", "RFQ-S3", "EURUSD", QuoteStatus.Expired, 3_000_000L);

    final List<QuoteSnapshot> eurusd = projection.getQuotesBySymbol("EURUSD");
    assertEquals(2, eurusd.size());
    assertTrue(eurusd.stream().allMatch(s -> "EURUSD".equals(s.symbol())));
  }

  @Test
  void getQuotesByAccount_returnsCorrectQuotes() {
    final int len1 =
        encodeQuoteCreated(
            "QTE-AC1",
            "RFQ-AC1",
            "EURUSD",
            SideEnum.Buy,
            "ALPHA",
            108_500_000L,
            108_700_000L,
            5_000_000L,
            1_000_000L);
    dispatch(QuoteCreatedEventDecoder.TEMPLATE_ID, len1);

    final int len2 =
        encodeQuoteCreated(
            "QTE-AC2",
            "RFQ-AC2",
            "EURUSD",
            SideEnum.Buy,
            "BETA",
            108_500_000L,
            108_700_000L,
            5_000_000L,
            2_000_000L);
    dispatch(QuoteCreatedEventDecoder.TEMPLATE_ID, len2);

    assertEquals(1, projection.getQuotesByAccount("ALPHA").size());
    assertEquals(1, projection.getQuotesByAccount("BETA").size());
    assertEquals(0, projection.getQuotesByAccount("GAMMA").size());
  }

  @Test
  void getQuotesByStatus_filtersCorrectly() {
    dispatchQuoteInState("QTE-ST1", "RFQ-ST1", "EURUSD", QuoteStatus.Active, 1_000_000L);
    dispatchQuoteInState("QTE-ST2", "RFQ-ST2", "GBPUSD", QuoteStatus.Expired, 2_000_000L);
    dispatchQuoteInState("QTE-ST3", "RFQ-ST3", "USDJPY", QuoteStatus.Active, 3_000_000L);

    assertEquals(2, projection.getQuotesByStatus(QuoteStatus.Active).size());
    assertEquals(1, projection.getQuotesByStatus(QuoteStatus.Expired).size());
    assertEquals(0, projection.getQuotesByStatus(QuoteStatus.Used).size());
  }

  // ---------------------------------------------------------------------------
  // Edge case tests
  // ---------------------------------------------------------------------------

  @Test
  void reset_clearsAllStateAndCounters() {
    dispatchQuoteInState("QTE-R1", "RFQ-R1", "EURUSD", QuoteStatus.Active, 1_000_000L);
    assertTrue(projection.size() > 0);

    projection.reset();

    assertEquals(0, projection.size());
    assertEquals(0, projection.lastProcessedSequence());
    assertEquals(0, projection.eventsProcessed());
    assertEquals(0, projection.errorCount());
    assertNull(projection.getQuote("QTE-R1"));
  }

  @Test
  void lastProcessedSequence_updatedOnEveryEvent() {
    final int len1 =
        encodeQuoteRequested("RFQ-SEQ1", "EURUSD", SideEnum.Buy, PRICE_SCALE, "ACCT01", 1_000_000L);
    dispatch(QuoteRequestedEventDecoder.TEMPLATE_ID, len1);
    assertEquals(1, projection.lastProcessedSequence());

    final int len2 =
        encodeQuoteCreated(
            "QTE-SEQ1",
            "RFQ-SEQ1",
            "EURUSD",
            SideEnum.Buy,
            "ACCT01",
            108_500_000L,
            108_700_000L,
            5_000_000L,
            2_000_000L);
    dispatch(QuoteCreatedEventDecoder.TEMPLATE_ID, len2);
    assertEquals(2, projection.lastProcessedSequence());
  }

  @Test
  void lastProcessedSequence_updatedEvenOnDecodeError() {
    // Dispatch a truncated buffer — should cause decode error but seqNo still advances
    seqNo = 42;
    projection.onEvent(42, QuoteCreatedEventDecoder.TEMPLATE_ID, buf, 0, 2);
    assertEquals(42, projection.lastProcessedSequence());
    assertEquals(1, projection.errorCount());
  }

  @Test
  void duplicateQuoteId_cleansUpSecondaryIndexes() {
    final int len1 =
        encodeQuoteCreated(
            "QTE-DUP",
            "RFQ-DUP1",
            "EURUSD",
            SideEnum.Buy,
            "ACCT01",
            108_500_000L,
            108_700_000L,
            5_000_000L,
            1_000_000L);
    dispatch(QuoteCreatedEventDecoder.TEMPLATE_ID, len1);

    // Same quoteId, different quoteReqId (replay scenario)
    final int len2 =
        encodeQuoteCreated(
            "QTE-DUP",
            "RFQ-DUP2",
            "GBPUSD",
            SideEnum.Sell,
            "ACCT02",
            150_000_000L,
            150_200_000L,
            5_000_000L,
            2_000_000L);
    dispatch(QuoteCreatedEventDecoder.TEMPLATE_ID, len2);

    final QuoteSnapshot s = projection.getQuote("QTE-DUP");
    assertNotNull(s);
    assertEquals("RFQ-DUP2", s.quoteReqId());
    assertEquals("GBPUSD", s.symbol());

    // Old symbol index should not contain phantom entry
    assertEquals(
        0,
        projection.getQuotesBySymbol("EURUSD").stream()
            .filter(q -> "QTE-DUP".equals(q.quoteId()))
            .count());
  }

  @Test
  void expiredEvent_unknownQuoteId_silentlyDropped() {
    final int len =
        encodeQuoteExpired("QTE-UNKNOWN", "RFQ-UNKNOWN", "EURUSD", SideEnum.Buy, 1_000_000L);
    dispatch(QuoteExpiredEventDecoder.TEMPLATE_ID, len);

    assertEquals(0, projection.size());
    assertEquals(0, projection.errorCount());
  }

  @Test
  void quoteRequested_afterQuoteCreated_doesNotOverwrite() {
    // 105 arrives first (out of order)
    final int len1 =
        encodeQuoteCreated(
            "QTE-OOO",
            "RFQ-OOO",
            "EURUSD",
            SideEnum.Buy,
            "ACCT01",
            108_500_000L,
            108_700_000L,
            5_000_000L,
            1_000_000L);
    dispatch(QuoteCreatedEventDecoder.TEMPLATE_ID, len1);

    // Then 104 arrives for the same quoteReqId — should be no-op
    final int len2 =
        encodeQuoteRequested("RFQ-OOO", "EURUSD", SideEnum.Buy, PRICE_SCALE, "ACCT01", 500_000L);
    dispatch(QuoteRequestedEventDecoder.TEMPLATE_ID, len2);

    final QuoteSnapshot s = projection.getQuoteByReqId("RFQ-OOO");
    assertNotNull(s);
    assertEquals(QuoteStatus.Active, s.status()); // Preserved — 104 was no-op
  }

  @Test
  void multiLegQuoteCreated_decodesCorrectlyWithZeroLegs() {
    // Encode with noLegsCount(0) — verifies group skipping works
    final int len =
        encodeQuoteCreated(
            "QTE-ML",
            "RFQ-ML",
            "EURUSD",
            SideEnum.Buy,
            "ACCT01",
            108_500_000L,
            108_700_000L,
            5_000_000L,
            1_000_000L);
    dispatch(QuoteCreatedEventDecoder.TEMPLATE_ID, len);

    final QuoteSnapshot s = projection.getQuote("QTE-ML");
    assertNotNull(s);
    assertEquals(108_500_000L, s.bidPx());
    assertEquals(108_700_000L, s.offerPx());
  }

  // ---------------------------------------------------------------------------
  // FX field coverage tests
  // ---------------------------------------------------------------------------

  @Test
  void quoteCreated_capturesSettlDateCurrencyTenor() {
    final int len =
        encodeQuoteCreatedFull(
            "QTE-FX1",
            "RFQ-FX1",
            "EURUSD",
            SideEnum.Buy,
            "ACCT01",
            108_500_000L,
            108_700_000L,
            200_000_000L,
            200_000_000L,
            5_000_000L,
            1_000_000L,
            ProductTypeEnum.Forward,
            "20260401",
            SettlTypeEnum.Future,
            "EUR",
            "USD",
            TenorEnum.M1,
            5_000L);
    dispatch(QuoteCreatedEventDecoder.TEMPLATE_ID, len);

    final QuoteSnapshot s = projection.getQuote("QTE-FX1");
    assertNotNull(s);
    assertEquals("20260401", s.settlDate());
    assertEquals(SettlTypeEnum.Future, s.settlType());
    assertEquals("EUR", s.currency());
    assertEquals("USD", s.settlCurrency());
    assertEquals(TenorEnum.M1, s.tenor());
    assertEquals(5_000L, s.swapPoints());
    assertEquals(ProductTypeEnum.Forward, s.productType());
  }

  @Test
  void quoteRequested_capturesFxFields() {
    final int len =
        SbeTestEncoder.encodeQuoteRequestedEvent(
            buf,
            0,
            ++seqNo,
            1_000_000L,
            "RFQ-FX2",
            "GBPUSD",
            SideEnum.Sell,
            PRICE_SCALE,
            "ACCT02",
            ProductTypeEnum.Spot,
            "20260315",
            SettlTypeEnum.Regular,
            "GBP",
            "USD",
            TenorEnum.SN);
    dispatch(QuoteRequestedEventDecoder.TEMPLATE_ID, len);

    final QuoteSnapshot s = projection.getQuoteByReqId("RFQ-FX2");
    assertNotNull(s);
    assertEquals("20260315", s.settlDate());
    assertEquals("GBP", s.currency());
    assertEquals("USD", s.settlCurrency());
    assertEquals(TenorEnum.SN, s.tenor());
  }

  @Test
  void quoteCreated_swapPointsNullSentinel_propagated() {
    final int len =
        encodeQuoteCreatedFull(
            "QTE-SPN",
            "RFQ-SPN",
            "EURUSD",
            SideEnum.Buy,
            "ACCT01",
            108_500_000L,
            108_700_000L,
            100_000_000L,
            100_000_000L,
            5_000_000L,
            1_000_000L,
            ProductTypeEnum.Spot,
            "20260101",
            SettlTypeEnum.Regular,
            "USD",
            "EUR",
            TenorEnum.SN,
            QuoteCreatedEventEncoder.swapPointsNullValue());
    dispatch(QuoteCreatedEventDecoder.TEMPLATE_ID, len);

    final QuoteSnapshot s = projection.getQuote("QTE-SPN");
    assertNotNull(s);
    assertEquals(QuoteCreatedEventEncoder.swapPointsNullValue(), s.swapPoints());
  }

  // ---------------------------------------------------------------------------
  // Additional edge cases
  // ---------------------------------------------------------------------------

  @Test
  void onEvent_quoteRequestedThenExpired_expiresViaQuoteReqIdFallback() {
    // 104 creates a Requested view (no quoteId in byQuoteId)
    final int len1 =
        encodeQuoteRequested("RFQ-FALL", "EURUSD", SideEnum.Buy, PRICE_SCALE, "ACCT01", 1_000_000L);
    dispatch(QuoteRequestedEventDecoder.TEMPLATE_ID, len1);

    // 107 with empty quoteId — falls back to quoteReqId lookup
    final int len2 = encodeQuoteExpired("", "RFQ-FALL", "EURUSD", SideEnum.Buy, 2_000_000L);
    dispatch(QuoteExpiredEventDecoder.TEMPLATE_ID, len2);

    final QuoteSnapshot s = projection.getQuoteByReqId("RFQ-FALL");
    assertNotNull(s);
    assertEquals(QuoteStatus.Expired, s.status());
    assertEquals(2_000_000L, s.lastUpdatedAt());
  }

  @Test
  void getActiveQuotes_crossThreadQuery_returnsConsistentSnapshot() throws Exception {
    // Populate quotes on the test thread, then query from a different thread to verify
    // StampedLock provides cross-thread visibility of the read model
    for (int i = 0; i < 100; i++) {
      final int len =
          encodeQuoteCreated(
              "QTE-C" + i,
              "RFQ-C" + i,
              "EURUSD",
              SideEnum.Buy,
              "ACCT01",
              108_500_000L,
              108_700_000L,
              5_000_000L,
              1_000_000L);
      dispatch(QuoteCreatedEventDecoder.TEMPLATE_ID, len);
    }

    // Query from a different thread — verifies StampedLock read visibility
    final var future = CompletableFuture.supplyAsync(() -> projection.getActiveQuotes());

    final var result = future.get();
    assertEquals(100, result.size());
    assertTrue(result.stream().allMatch(s -> s.status() == QuoteStatus.Active));
  }

  @Test
  void onEvent_duplicateQuoteRequested_cleansSecondaryIndexesBeforeReplace() {
    final int len1 =
        encodeQuoteRequested("RFQ-DDUP", "EURUSD", SideEnum.Buy, PRICE_SCALE, "ACCT01", 1_000_000L);
    dispatch(QuoteRequestedEventDecoder.TEMPLATE_ID, len1);

    // Duplicate 104 replay with same quoteReqId
    final int len2 =
        encodeQuoteRequested("RFQ-DDUP", "EURUSD", SideEnum.Buy, PRICE_SCALE, "ACCT01", 1_500_000L);
    dispatch(QuoteRequestedEventDecoder.TEMPLATE_ID, len2);

    // Should have exactly 1 entry in symbol index, not 2 (phantom leak)
    assertEquals(1, projection.getQuotesBySymbol("EURUSD").size());
    assertEquals(1, projection.getQuotesByAccount("ACCT01").size());
    assertEquals(1, projection.size());
  }

  @Test
  void size_returnsQuoteReqIdMapSize() {
    // Requested-only (in byQuoteReqId, not byQuoteId)
    final int len1 =
        encodeQuoteRequested("RFQ-SZ1", "EURUSD", SideEnum.Buy, PRICE_SCALE, "ACCT01", 1_000_000L);
    dispatch(QuoteRequestedEventDecoder.TEMPLATE_ID, len1);

    // Rejected-only (in byQuoteReqId, not byQuoteId)
    final int len2 =
        encodeQuoteRejected(
            "RFQ-SZ2",
            "GBPUSD",
            SideEnum.Sell,
            QuoteRejectReasonEnum.Other,
            "rejected",
            2_000_000L);
    dispatch(QuoteRejectedEventDecoder.TEMPLATE_ID, len2);

    // Active (in both byQuoteReqId and byQuoteId)
    final int len3 =
        encodeQuoteCreated(
            "QTE-SZ3",
            "RFQ-SZ3",
            "USDJPY",
            SideEnum.Buy,
            "ACCT01",
            108_500_000L,
            108_700_000L,
            5_000_000L,
            3_000_000L);
    dispatch(QuoteCreatedEventDecoder.TEMPLATE_ID, len3);

    assertEquals(3, projection.size()); // All 3 via byQuoteReqId
  }

  // ---------------------------------------------------------------------------
  // Terminal eviction tests
  // ---------------------------------------------------------------------------

  @Test
  void purgeTerminal_evictsExpiredAndUsedQuotes() {
    dispatchQuoteInState("QTE-P1", "RFQ-P1", "EURUSD", QuoteStatus.Active, 1_000_000L);
    dispatchQuoteInState("QTE-P2", "RFQ-P2", "EURUSD", QuoteStatus.Expired, 2_000_000L);
    dispatchQuoteInState("QTE-P3", "RFQ-P3", "GBPUSD", QuoteStatus.Used, 3_000_000L);

    final int evicted = projection.purgeTerminal(4_000_000L);
    assertEquals(2, evicted);
    assertEquals(1, projection.size()); // Only Active remains
    assertNotNull(projection.getQuote("QTE-P1")); // Active preserved
    assertNull(projection.getQuote("QTE-P2")); // Expired evicted
    assertNull(projection.getQuote("QTE-P3")); // Used evicted
  }

  @Test
  void purgeTerminal_preservesActiveAndRequestedQuotes() {
    final int len1 =
        encodeQuoteRequested("RFQ-PP1", "EURUSD", SideEnum.Buy, PRICE_SCALE, "ACCT01", 1_000_000L);
    dispatch(QuoteRequestedEventDecoder.TEMPLATE_ID, len1);

    dispatchQuoteInState("QTE-PP2", "RFQ-PP2", "GBPUSD", QuoteStatus.Active, 2_000_000L);

    final int evicted = projection.purgeTerminal(5_000_000L);
    assertEquals(0, evicted);
    assertEquals(2, projection.size());
  }

  @Test
  void purgeTerminal_returnsEvictedCount() {
    for (int i = 0; i < 5; i++) {
      dispatchQuoteInState(
          "QTE-CT" + i, "RFQ-CT" + i, "EURUSD", QuoteStatus.Expired, 1_000_000L + i);
    }
    assertEquals(5, projection.purgeTerminal(10_000_000L));
  }

  @Test
  void purgeTerminal_removesFromAllFourIndexes() {
    dispatchQuoteInState("QTE-IDX", "RFQ-IDX", "EURUSD", QuoteStatus.Expired, 1_000_000L);

    assertEquals(1, projection.getQuotesBySymbol("EURUSD").size());
    assertEquals(1, projection.getQuotesByAccount("ACCT01").size());

    projection.purgeTerminal(2_000_000L);

    assertNull(projection.getQuote("QTE-IDX"));
    assertNull(projection.getQuoteByReqId("RFQ-IDX"));
    assertEquals(0, projection.getQuotesBySymbol("EURUSD").size());
    assertEquals(0, projection.getQuotesByAccount("ACCT01").size());
  }

  @Test
  void purgeTerminal_noEvictionWhenAllRecent() {
    dispatchQuoteInState("QTE-REC", "RFQ-REC", "EURUSD", QuoteStatus.Expired, 5_000_000L);

    // Cutoff is before the quote's lastUpdatedAt — should not evict
    final int evicted = projection.purgeTerminal(4_000_000L);
    assertEquals(0, evicted);
    assertEquals(1, projection.size());
  }

  // ---------------------------------------------------------------------------
  // Test helpers
  // ---------------------------------------------------------------------------

  /**
   * Creates a quote in the specified terminal state. For Active: dispatches QuoteCreated only. For
   * Expired: QuoteCreated then QuoteExpired. For Used: QuoteCreated then OrderCreated with
   * PreviouslyQuoted.
   */
  private void dispatchQuoteInState(
      final String quoteId,
      final String quoteReqId,
      final String symbol,
      final QuoteStatus targetStatus,
      final long timestamp) {

    final int len1 =
        encodeQuoteCreated(
            quoteId,
            quoteReqId,
            symbol,
            SideEnum.Buy,
            "ACCT01",
            108_500_000L,
            108_700_000L,
            timestamp + 30_000_000_000L,
            timestamp);
    dispatch(QuoteCreatedEventDecoder.TEMPLATE_ID, len1);

    switch (targetStatus) {
      case Active -> {
        /* already Active */
      }
      case Expired -> {
        final int len2 =
            encodeQuoteExpired(quoteId, quoteReqId, symbol, SideEnum.Buy, timestamp + 1_000L);
        dispatch(QuoteExpiredEventDecoder.TEMPLATE_ID, len2);
      }
      case Used -> {
        final int len2 =
            encodeOrderCreated(
                "ORD-" + quoteId,
                "CLO-" + quoteId,
                symbol,
                SideEnum.Buy,
                OrdTypeEnum.PreviouslyQuoted,
                108_500_000L,
                PRICE_SCALE,
                quoteId,
                "ACCT01",
                timestamp + 1_000L);
        dispatch(OrderCreatedEventDecoder.TEMPLATE_ID, len2);
      }
      default -> throw new IllegalArgumentException("Unsupported target status: " + targetStatus);
    }
  }
}
