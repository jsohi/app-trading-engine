package com.trading.engine.cluster;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.trading.engine.messages.sbe.OrdTypeEnum;
import com.trading.engine.messages.sbe.SideEnum;
import com.trading.engine.messages.sbe.TimeInForceEnum;
import org.junit.jupiter.api.Test;

class OrderBookTest {

  private static final long PRICE_SCALE = 100_000_000L;

  private static OrderState newOrder(String clOrdId, String symbol, long pxWhole, long qtyWhole) {
    return new OrderState(
        clOrdId,
        symbol,
        SideEnum.Buy,
        OrdTypeEnum.Limit,
        pxWhole * PRICE_SCALE,
        qtyWhole * PRICE_SCALE,
        TimeInForceEnum.Day,
        "ACME-001");
  }

  @Test
  void addAndRetrieveOrder() {
    OrderBook book = new OrderBook();
    OrderState order = newOrder("CL-1", "EURUSD", 1L, 1_000_000L);
    book.addOrder("ORD-000000001", order);

    assertEquals(1, book.size());
    assertEquals(order, book.getOrder("ORD-000000001"));
  }

  @Test
  void getReturnsNullForUnknownId() {
    OrderBook book = new OrderBook();
    assertNull(book.getOrder("ORD-000000999"));
  }

  @Test
  void multipleOrdersTrackedIndependently() {
    OrderBook book = new OrderBook();
    OrderState a = newOrder("CL-1", "EURUSD", 1L, 1_000_000L);
    OrderState b = newOrder("CL-2", "GBPUSD", 1L, 2_000_000L);
    OrderState c = newOrder("CL-3", "USDJPY", 150L, 500_000L);

    book.addOrder("ORD-000000001", a);
    book.addOrder("ORD-000000002", b);
    book.addOrder("ORD-000000003", c);

    assertEquals(3, book.size());
    assertEquals(a, book.getOrder("ORD-000000001"));
    assertEquals(b, book.getOrder("ORD-000000002"));
    assertEquals(c, book.getOrder("ORD-000000003"));
  }

  @Test
  void addReplacesExistingOrder() {
    OrderBook book = new OrderBook();
    OrderState first = newOrder("CL-1", "EURUSD", 1L, 1_000_000L);
    OrderState replacement = newOrder("CL-1", "EURUSD", 2L, 500_000L);
    book.addOrder("ORD-000000001", first);
    book.addOrder("ORD-000000001", replacement);

    assertEquals(1, book.size());
    assertEquals(replacement, book.getOrder("ORD-000000001"));
  }
}
