package com.trading.engine.queryservice;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.trading.engine.messages.sbe.MessageHeaderEncoder;
import com.trading.engine.messages.sbe.OrdTypeEnum;
import com.trading.engine.messages.sbe.OrderCreatedEventDecoder;
import com.trading.engine.messages.sbe.OrderFilledEventDecoder;
import com.trading.engine.messages.sbe.QuoteCreatedEventDecoder;
import com.trading.engine.messages.sbe.QuoteRequestedEventDecoder;
import com.trading.engine.messages.sbe.SideEnum;
import com.trading.engine.projections.EventConsumer;
import com.trading.engine.projections.ProjectionRegistry;
import com.trading.engine.projections.account.AccountProjection;
import com.trading.engine.projections.order.OrderProjection;
import com.trading.engine.projections.position.PositionProjection;
import com.trading.engine.projections.quote.QuoteProjection;
import com.trading.engine.projections.quote.QuoteStatus;
import com.trading.engine.testsupport.sbe.SbeTestEncoder;
import java.util.Map;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Verifies {@link QueryService} delegates correctly to all 4 projections (Order, Position, Account,
 * Quote) and to {@link ProjectionRegistry} for health/lag monitoring. Uses real projection
 * instances (not mocks) with events encoded via {@link SbeTestEncoder}.
 */
class QueryServiceTest {

  private static final long PRICE_SCALE = 100_000_000L;
  private static final int HDR_LEN = MessageHeaderEncoder.ENCODED_LENGTH;

  private OrderProjection orderProjection;
  private PositionProjection positionProjection;
  private AccountProjection accountProjection;
  private QuoteProjection quoteProjection;
  private ProjectionRegistry registry;
  private EventConsumer consumer;
  private QueryService queryService;
  private MutableDirectBuffer buf;
  private long seqNo;

  @BeforeEach
  void setUp() {
    orderProjection = new OrderProjection(64);
    positionProjection = new PositionProjection();
    accountProjection = new AccountProjection(64);
    quoteProjection = new QuoteProjection(64);

    consumer = new EventConsumer();
    consumer.registerProjection(
        orderProjection, OrderCreatedEventDecoder.TEMPLATE_ID, OrderFilledEventDecoder.TEMPLATE_ID);
    consumer.registerProjection(
        quoteProjection,
        QuoteRequestedEventDecoder.TEMPLATE_ID,
        QuoteCreatedEventDecoder.TEMPLATE_ID);

    registry = new ProjectionRegistry(consumer, 100);
    registry.register("order", orderProjection);
    registry.register("position", positionProjection);
    registry.register("account", accountProjection);
    registry.register("quote", quoteProjection);

    queryService =
        new QueryService(
            orderProjection, positionProjection, accountProjection, quoteProjection, registry);

    buf = new ExpandableArrayBuffer(512);
    seqNo = 0;
  }

  // ---------------------------------------------------------------------------
  // Constructor tests
  // ---------------------------------------------------------------------------

  @Test
  void constructor_rejectsNullArguments() {
    assertThrows(
        NullPointerException.class,
        () ->
            new QueryService(
                null, positionProjection, accountProjection, quoteProjection, registry));
    assertThrows(
        NullPointerException.class,
        () ->
            new QueryService(orderProjection, null, accountProjection, quoteProjection, registry));
    assertThrows(
        NullPointerException.class,
        () ->
            new QueryService(orderProjection, positionProjection, null, quoteProjection, registry));
    assertThrows(
        NullPointerException.class,
        () ->
            new QueryService(
                orderProjection, positionProjection, accountProjection, null, registry));
    assertThrows(
        NullPointerException.class,
        () ->
            new QueryService(
                orderProjection, positionProjection, accountProjection, quoteProjection, null));
  }

  // ---------------------------------------------------------------------------
  // Order delegation tests
  // ---------------------------------------------------------------------------

  @Test
  void getOrder_delegatesToOrderProjection() {
    dispatchOrderCreated("ORD-001", "CLO-001", "EURUSD");
    assertNotNull(queryService.getOrder("ORD-001"));
    assertNull(queryService.getOrder("ORD-NONE"));
  }

  @Test
  void getOrderByClOrdId_delegatesToOrderProjection() {
    dispatchOrderCreated("ORD-002", "CLO-002", "EURUSD");
    assertNotNull(queryService.getOrderByClOrdId("CLO-002"));
  }

  @Test
  void getActiveOrders_delegatesToOrderProjection() {
    dispatchOrderCreated("ORD-003", "CLO-003", "EURUSD");
    assertEquals(1, queryService.getActiveOrders().size());
  }

  // ---------------------------------------------------------------------------
  // Position delegation tests
  // ---------------------------------------------------------------------------

  @Test
  void getPosition_delegatesToPositionProjection() {
    // PositionProjection is populated by OrderFilledEvent, which requires an existing order
    // in OrderProjection. For pure delegation testing, we verify null return for missing position.
    assertNull(queryService.getPosition("EURUSD", "ACCT01", "20260101"));
  }

  @Test
  void getAllPositions_delegatesToPositionProjection() {
    assertEquals(0, queryService.getAllPositions().size());
  }

  // ---------------------------------------------------------------------------
  // Account delegation tests
  // ---------------------------------------------------------------------------

  @Test
  void getAccountById_delegatesToAccountProjection() {
    dispatchAccountLoaded(1L, "ACCT-A01");
    assertNotNull(queryService.getAccountById(1L));
    assertNull(queryService.getAccountById(999L));
  }

  @Test
  void getActiveAccounts_delegatesToAccountProjection() {
    dispatchAccountLoaded(2L, "ACCT-A02");
    assertTrue(queryService.getActiveAccounts().size() >= 1);
  }

  // ---------------------------------------------------------------------------
  // Quote delegation tests
  // ---------------------------------------------------------------------------

  @Test
  void getQuote_delegatesToQuoteProjection() {
    dispatchQuoteCreated("QTE-001", "RFQ-001", "EURUSD");
    assertNotNull(queryService.getQuote("QTE-001"));
    assertNull(queryService.getQuote("QTE-NONE"));
  }

  @Test
  void getActiveQuotes_delegatesToQuoteProjection() {
    dispatchQuoteCreated("QTE-002", "RFQ-002", "EURUSD");
    assertEquals(1, queryService.getActiveQuotes().size());
  }

  @Test
  void getQuotesBySymbol_delegatesToQuoteProjection() {
    dispatchQuoteCreated("QTE-003", "RFQ-003", "GBPUSD");
    assertEquals(1, queryService.getQuotesBySymbol("GBPUSD").size());
    assertEquals(0, queryService.getQuotesBySymbol("USDJPY").size());
  }

  @Test
  void getQuotesByAccount_delegatesToQuoteProjection() {
    dispatchQuoteCreated("QTE-004", "RFQ-004", "EURUSD");
    assertEquals(1, queryService.getQuotesByAccount("ACCT01").size());
  }

  @Test
  void getQuotesByStatus_delegatesToQuoteProjection() {
    dispatchQuoteCreated("QTE-005", "RFQ-005", "EURUSD");
    assertEquals(1, queryService.getQuotesByStatus(QuoteStatus.Active).size());
    assertEquals(0, queryService.getQuotesByStatus(QuoteStatus.Expired).size());
  }

  @Test
  void getQuoteByReqId_delegatesToQuoteProjection() {
    final int len =
        SbeTestEncoder.encodeQuoteRequestedEvent(
            buf, 0, ++seqNo, 1_000_000L, "RFQ-REQ1", "EURUSD", SideEnum.Buy, PRICE_SCALE, "ACCT01");
    quoteProjection.onEvent(
        seqNo, QuoteRequestedEventDecoder.TEMPLATE_ID, buf, HDR_LEN, len - HDR_LEN);

    assertNotNull(queryService.getQuoteByReqId("RFQ-REQ1"));
    assertNull(queryService.getQuoteByReqId("RFQ-NONE"));
  }

  @Test
  void getInFlightQuotes_delegatesToQuoteProjection() {
    // Requested (in-flight)
    final int len1 =
        SbeTestEncoder.encodeQuoteRequestedEvent(
            buf, 0, ++seqNo, 1_000_000L, "RFQ-IF1", "EURUSD", SideEnum.Buy, PRICE_SCALE, "ACCT01");
    quoteProjection.onEvent(
        seqNo, QuoteRequestedEventDecoder.TEMPLATE_ID, buf, HDR_LEN, len1 - HDR_LEN);

    // Active (in-flight)
    dispatchQuoteCreated("QTE-IF2", "RFQ-IF2", "GBPUSD");

    assertEquals(2, queryService.getInFlightQuotes().size());
  }

  // ---------------------------------------------------------------------------
  // Health / diagnostics delegation tests
  // ---------------------------------------------------------------------------

  @Test
  void isHealthy_delegatesToProjectionRegistry() {
    // All projections at lag 0, consumer not started but projections registered
    assertTrue(queryService.isHealthy());
  }

  @Test
  void getLagSnapshot_delegatesToProjectionRegistry() {
    final Map<String, Long> lag = queryService.getLagSnapshot();
    assertEquals(4, lag.size());
    assertTrue(lag.containsKey("order"));
    assertTrue(lag.containsKey("position"));
    assertTrue(lag.containsKey("account"));
    assertTrue(lag.containsKey("quote"));
  }

  // ---------------------------------------------------------------------------
  // Event dispatch helpers
  // ---------------------------------------------------------------------------

  private void dispatchOrderCreated(
      final String orderId, final String clOrdId, final String symbol) {
    final int len =
        SbeTestEncoder.encodeOrderCreatedEvent(
            buf,
            0,
            ++seqNo,
            1_000_000L,
            orderId,
            "",
            clOrdId,
            symbol,
            SideEnum.Buy,
            OrdTypeEnum.Limit,
            108_500_000L,
            PRICE_SCALE,
            "ACCT01");
    orderProjection.onEvent(
        seqNo, OrderCreatedEventDecoder.TEMPLATE_ID, buf, HDR_LEN, len - HDR_LEN);
  }

  private void dispatchQuoteCreated(
      final String quoteId, final String quoteReqId, final String symbol) {
    final int len =
        SbeTestEncoder.encodeQuoteCreatedEvent(
            buf,
            0,
            ++seqNo,
            1_000_000L,
            quoteId,
            quoteReqId,
            symbol,
            SideEnum.Buy,
            "ACCT01",
            108_500_000L,
            108_700_000L,
            5_000_000L);
    quoteProjection.onEvent(
        seqNo, QuoteCreatedEventDecoder.TEMPLATE_ID, buf, HDR_LEN, len - HDR_LEN);
  }

  private void dispatchAccountLoaded(final long accountId, final String accountCode) {
    final int len =
        SbeTestEncoder.encodeAccountLoadedEvent(
            buf, 0, ++seqNo, 1_000_000L, accountId, accountCode, "Test Account", "USD");
    accountProjection.onEvent(
        seqNo,
        com.trading.engine.messages.sbe.AccountLoadedEventDecoder.TEMPLATE_ID,
        buf,
        HDR_LEN,
        len - HDR_LEN);
  }
}
