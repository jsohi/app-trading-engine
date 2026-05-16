package com.trading.engine.websocket;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.trading.engine.messages.sbe.OrdTypeEnum;
import com.trading.engine.messages.sbe.SideEnum;
import com.trading.engine.projections.SymbolPacker;
import com.trading.engine.testsupport.sbe.SbeTestEncoder;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link SubscriptionFilter} -- verifies symbol+eventType matching, the copy-on-write
 * volatile snapshot pattern, no-symbol template routing via globalEventBitMask, subscription
 * lifecycle (add/remove/clear), and capacity enforcement.
 */
final class SubscriptionFilterTest {

  private static final int MAX_SUBSCRIPTIONS = 100;

  private final MutableDirectBuffer buffer = new ExpandableArrayBuffer(512);

  // --- Basic matching ---

  @Test
  void matches_subscribedSymbolAndEventType_returnsTrue() {
    final var filter = new SubscriptionFilter(MAX_SUBSCRIPTIONS);
    filter.addSubscription(SymbolPacker.pack("EURUSD"), 0x01); // orders only

    final int len = encodeOrderCreated("EURUSD");
    final byte[] bytes = toByteArray(buffer, len);

    assertTrue(filter.matches(100, bytes, 0, len));
  }

  @Test
  void matches_subscribedSymbolWrongEventType_returnsFalse() {
    final var filter = new SubscriptionFilter(MAX_SUBSCRIPTIONS);
    filter.addSubscription(SymbolPacker.pack("EURUSD"), 0x04); // prices only

    final int len = encodeOrderCreated("EURUSD");
    final byte[] bytes = toByteArray(buffer, len);

    assertFalse(filter.matches(100, bytes, 0, len)); // order event, but subscribed to prices
  }

  @Test
  void matches_unsubscribedSymbol_returnsFalse() {
    final var filter = new SubscriptionFilter(MAX_SUBSCRIPTIONS);
    filter.addSubscription(SymbolPacker.pack("GBPUSD"), 0x01);

    final int len = encodeOrderCreated("EURUSD");
    final byte[] bytes = toByteArray(buffer, len);

    assertFalse(filter.matches(100, bytes, 0, len)); // subscribed to GBPUSD, not EURUSD
  }

  @Test
  void matches_emptyFilter_returnsFalse() {
    final var filter = new SubscriptionFilter(MAX_SUBSCRIPTIONS);

    final int len = encodeOrderCreated("EURUSD");
    final byte[] bytes = toByteArray(buffer, len);

    assertFalse(filter.matches(100, bytes, 0, len));
  }

  // --- All event types bitmask ---

  @Test
  void matches_allEventTypesBitmask_matchesOrderEvent() {
    final var filter = new SubscriptionFilter(MAX_SUBSCRIPTIONS);
    // 0xFFFFFFFF masked to 0x1F by addSubscription
    filter.addSubscription(SymbolPacker.pack("EURUSD"), 0xFFFFFFFF);

    final int len = encodeOrderCreated("EURUSD");
    final byte[] bytes = toByteArray(buffer, len);

    assertTrue(filter.matches(100, bytes, 0, len));
  }

  @Test
  void matches_allEventTypesBitmask_dropsOrchestratorBoundPriceResponse51() {
    // Phase 3 Commit 5: template 51 (PriceResponse) is orchestrator-bound and is now mapped to
    // -1 by templateIdToEventBit. Even an all-bits-set subscription MUST NOT match it; the
    // egress filter actively rejects misrouted PriceResponses.
    final var filter = new SubscriptionFilter(MAX_SUBSCRIPTIONS);
    filter.addSubscription(SymbolPacker.pack("USDJPY"), 0x1F); // all valid bits

    final int len =
        SbeTestEncoder.encodePriceResponse(
            buffer, 0, "QR001", "USDJPY", true, 149_500_000_000L, 149_600_000_000L, 1000L);
    final byte[] bytes = toByteArray(buffer, len);

    assertFalse(filter.matches(51, bytes, 0, len));
  }

  // --- No-symbol templates (globalEventBitMask) ---

  @Test
  void matches_accountLoadedEvent_noSymbol_matchesViaGlobalBitMask() {
    final var filter = new SubscriptionFilter(MAX_SUBSCRIPTIONS);
    filter.addSubscription(SymbolPacker.pack("EURUSD"), 0x10); // account events

    // Template 110 has no symbol — matched via globalEventBitMask
    assertTrue(filter.matches(110, new byte[256], 0, 256));
  }

  @Test
  void matches_accountLoadedEvent_noAccountBitSubscribed_returnsFalse() {
    final var filter = new SubscriptionFilter(MAX_SUBSCRIPTIONS);
    filter.addSubscription(SymbolPacker.pack("EURUSD"), 0x01); // orders only, not accounts

    assertFalse(filter.matches(110, new byte[256], 0, 256));
  }

  @Test
  void matches_orderCancelRejected_noSymbol_matchesViaGlobalBitMask() {
    final var filter = new SubscriptionFilter(MAX_SUBSCRIPTIONS);
    filter.addSubscription(SymbolPacker.pack("EURUSD"), 0x01); // orders — includes template 112

    // Template 112 has no symbol but maps to BIT_ORDERS
    assertTrue(filter.matches(112, new byte[256], 0, 256));
  }

  @Test
  void matches_positionSnapshot_noSymbol_matchesViaGlobalBitMask() {
    final var filter = new SubscriptionFilter(MAX_SUBSCRIPTIONS);
    filter.addSubscription(SymbolPacker.pack("EURUSD"), 0x02); // positions

    assertTrue(filter.matches(204, new byte[256], 0, 256));
  }

  // --- Internal events ---

  @Test
  void matches_internalEvent108_returnsFalse() {
    final var filter = new SubscriptionFilter(MAX_SUBSCRIPTIONS);
    filter.addSubscription(SymbolPacker.pack("EURUSD"), 0x1F); // all types

    assertFalse(filter.matches(108, new byte[256], 0, 256));
  }

  @Test
  void matches_unknownTemplateId_returnsFalse() {
    final var filter = new SubscriptionFilter(MAX_SUBSCRIPTIONS);
    filter.addSubscription(SymbolPacker.pack("EURUSD"), 0x1F);

    assertFalse(filter.matches(999, new byte[256], 0, 256));
  }

  // --- Add / remove lifecycle ---

  @Test
  void addSubscription_thenRemove_noLongerMatches() {
    final var filter = new SubscriptionFilter(MAX_SUBSCRIPTIONS);
    filter.addSubscription(SymbolPacker.pack("EURUSD"), 0x01);

    final int len = encodeOrderCreated("EURUSD");
    final byte[] bytes = toByteArray(buffer, len);

    assertTrue(filter.matches(100, bytes, 0, len));

    filter.removeSubscription(SymbolPacker.pack("EURUSD"));
    assertFalse(filter.matches(100, bytes, 0, len));
  }

  @Test
  void addSubscription_updateExistingSymbol_overwritesBitmask() {
    // Phase 3 Commit 5: PriceResponse (template 51) is orchestrator-bound and now maps to -1;
    // verifying the overwrite semantics here uses an order-lifecycle template (100) instead so
    // the assertion still exercises the "bitmask grew → match flips false→true" behaviour
    // without touching the misrouted-RFQ regression-guard template.
    final var filter = new SubscriptionFilter(MAX_SUBSCRIPTIONS);
    filter.addSubscription(SymbolPacker.pack("EURUSD"), 0x04); // prices only

    final int len =
        SbeTestEncoder.encodeOrderCreatedEvent(
            buffer,
            0,
            1L,
            1_000_000_000L,
            "OID-001",
            "EX-001",
            "CL-001",
            "EURUSD",
            SideEnum.Buy,
            OrdTypeEnum.Limit,
            110_000_000L,
            1_000_000L,
            "ACC-001");
    final byte[] bytes = toByteArray(buffer, len);

    assertFalse(filter.matches(100, bytes, 0, len)); // no orders bit

    filter.addSubscription(SymbolPacker.pack("EURUSD"), 0x05); // prices + orders
    assertTrue(filter.matches(100, bytes, 0, len)); // now orders bit is set
  }

  @Test
  void clear_removesAllSubscriptions() {
    final var filter = new SubscriptionFilter(MAX_SUBSCRIPTIONS);
    filter.addSubscription(SymbolPacker.pack("EURUSD"), 0x1F);
    filter.addSubscription(SymbolPacker.pack("GBPUSD"), 0x1F);

    assertEquals(2, filter.subscriptionCount());
    filter.clear();
    assertEquals(0, filter.subscriptionCount());
    assertTrue(filter.isEmpty());
  }

  // --- Capacity enforcement ---

  @Test
  void addSubscription_atCapacity_returnsFalse() {
    final var filter = new SubscriptionFilter(2); // max 2 subscriptions
    assertTrue(filter.addSubscription(SymbolPacker.pack("EURUSD"), 0x01));
    assertTrue(filter.addSubscription(SymbolPacker.pack("GBPUSD"), 0x01));
    assertFalse(filter.addSubscription(SymbolPacker.pack("USDJPY"), 0x01)); // at capacity

    assertEquals(2, filter.subscriptionCount());
  }

  @Test
  void addSubscription_updateExistingAtCapacity_succeeds() {
    final var filter = new SubscriptionFilter(2);
    assertTrue(filter.addSubscription(SymbolPacker.pack("EURUSD"), 0x01));
    assertTrue(filter.addSubscription(SymbolPacker.pack("GBPUSD"), 0x01));

    // Update existing symbol — should succeed even at capacity
    assertTrue(filter.addSubscription(SymbolPacker.pack("EURUSD"), 0x05));
    assertEquals(2, filter.subscriptionCount());
  }

  // --- subscriptionCount / isEmpty ---

  @Test
  void subscriptionCount_afterAddAndRemove_reflectsState() {
    final var filter = new SubscriptionFilter(MAX_SUBSCRIPTIONS);
    assertEquals(0, filter.subscriptionCount());
    assertTrue(filter.isEmpty());

    filter.addSubscription(SymbolPacker.pack("EURUSD"), 0x01);
    assertEquals(1, filter.subscriptionCount());
    assertFalse(filter.isEmpty());

    filter.addSubscription(SymbolPacker.pack("GBPUSD"), 0x01);
    assertEquals(2, filter.subscriptionCount());

    filter.removeSubscription(SymbolPacker.pack("EURUSD"));
    assertEquals(1, filter.subscriptionCount());
  }

  // --- templateIdToEventBit ---

  @Test
  void templateIdToEventBit_orderTemplates_returnBitOrders() {
    final int[] orderTemplates = {100, 101, 102, 103, 112};
    for (final int tid : orderTemplates) {
      assertEquals(
          0x01,
          SubscriptionFilter.templateIdToEventBit(tid),
          "Expected BIT_ORDERS for templateId " + tid);
    }
  }

  @Test
  void templateIdToEventBit_positionTemplate_returnBitPositions() {
    assertEquals(0x02, SubscriptionFilter.templateIdToEventBit(204));
  }

  @Test
  void templateIdToEventBit_marketDataTemplates_returnBitPrices() {
    // Phase 3 Commit 5: BIT_PRICES (0x04) now covers the market-data broadcast templates
    // 54 (MarketDataTick), 55 (MarketDataHeartbeat), 57 (MarketDataFeedStateChange).
    // Template 51 (PriceResponse) used to map here but is orchestrator-bound and is now -1.
    assertEquals(0x04, SubscriptionFilter.templateIdToEventBit(54));
    assertEquals(0x04, SubscriptionFilter.templateIdToEventBit(55));
    assertEquals(0x04, SubscriptionFilter.templateIdToEventBit(57));
  }

  @Test
  void templateIdToEventBit_priceResponse51_returnsMinusOne_orchestratorBound() {
    // PriceResponse (template 51) is orchestrator-bound; the cluster routes it to the
    // orchestrator's session, never to the browser. An arrival at the egress filter is a
    // routing regression — mapping to -1 means the filter drops it.
    assertEquals(-1, SubscriptionFilter.templateIdToEventBit(51));
  }

  @Test
  void templateIdToEventBit_quoteTemplates_returnBitQuotes() {
    final int[] quoteTemplates = {104, 105, 106, 107};
    for (final int tid : quoteTemplates) {
      assertEquals(
          0x08,
          SubscriptionFilter.templateIdToEventBit(tid),
          "Expected BIT_QUOTES for templateId " + tid);
    }
  }

  @Test
  void templateIdToEventBit_accountTemplates_returnBitAccounts() {
    assertEquals(0x10, SubscriptionFilter.templateIdToEventBit(110));
    assertEquals(0x10, SubscriptionFilter.templateIdToEventBit(111));
  }

  @Test
  void templateIdToEventBit_internalTemplates_returnNegativeOne() {
    final int[] internal = {108, 109, 113, 114, 115, 116};
    for (final int tid : internal) {
      assertEquals(
          -1,
          SubscriptionFilter.templateIdToEventBit(tid),
          "Expected -1 for internal templateId " + tid);
    }
  }

  // --- Multiple symbols ---

  @Test
  void matches_multipleSymbols_onlyMatchesSubscribed() {
    final var filter = new SubscriptionFilter(MAX_SUBSCRIPTIONS);
    filter.addSubscription(SymbolPacker.pack("EURUSD"), 0x01);
    filter.addSubscription(SymbolPacker.pack("GBPUSD"), 0x04);
    filter.addSubscription(SymbolPacker.pack("USDJPY"), 0x1F);

    final int lenEur = encodeOrderCreated("EURUSD");
    final byte[] bytesEur = toByteArray(buffer, lenEur);
    assertTrue(filter.matches(100, bytesEur, 0, lenEur)); // EURUSD orders — YES

    final int lenGbp = encodeOrderCreated("GBPUSD");
    final byte[] bytesGbp = toByteArray(buffer, lenGbp);
    assertFalse(filter.matches(100, bytesGbp, 0, lenGbp)); // GBPUSD orders — NO (only prices)

    final int lenJpy = encodeOrderCreated("USDJPY");
    final byte[] bytesJpy = toByteArray(buffer, lenJpy);
    assertTrue(filter.matches(100, bytesJpy, 0, lenJpy)); // USDJPY all — YES
  }

  // --- Bitmask masking ---

  @Test
  void addSubscription_undefinedBitsStripped() {
    final var filter = new SubscriptionFilter(MAX_SUBSCRIPTIONS);
    // Pass 0xFF — only bits 0-4 should be retained (0x1F)
    filter.addSubscription(SymbolPacker.pack("EURUSD"), 0xFF);

    final int len = encodeOrderCreated("EURUSD");
    final byte[] bytes = toByteArray(buffer, len);

    assertTrue(filter.matches(100, bytes, 0, len)); // bit 0 (orders) should be set
  }

  // --- Zero bitmask ---

  @Test
  void addSubscription_zeroBitmask_returnsFalse() {
    final var filter = new SubscriptionFilter(MAX_SUBSCRIPTIONS);
    assertFalse(filter.addSubscription(SymbolPacker.pack("EURUSD"), 0));
    assertEquals(0, filter.subscriptionCount());
  }

  @Test
  void addSubscription_zeroBitmaskAfterMasking_returnsFalse() {
    final var filter = new SubscriptionFilter(MAX_SUBSCRIPTIONS);
    // 0xE0 has no valid bits (bits 5-7 only) — after masking with 0x1F → 0
    assertFalse(filter.addSubscription(SymbolPacker.pack("EURUSD"), 0xE0));
    assertEquals(0, filter.subscriptionCount());
  }

  // --- Remove non-existent symbol ---

  @Test
  void removeSubscription_nonExistentSymbol_noOp() {
    final var filter = new SubscriptionFilter(MAX_SUBSCRIPTIONS);
    filter.addSubscription(SymbolPacker.pack("EURUSD"), 0x01);

    filter.removeSubscription(SymbolPacker.pack("GBPUSD")); // never added
    assertEquals(1, filter.subscriptionCount()); // unchanged
  }

  // --- globalEventBitMask with mixed subscriptions ---

  @Test
  void matches_noSymbolTemplate_mixedSubscriptions_matchesOnlyIfAnyHasEventBit() {
    final var filter = new SubscriptionFilter(MAX_SUBSCRIPTIONS);
    filter.addSubscription(SymbolPacker.pack("EURUSD"), 0x01); // orders only
    filter.addSubscription(SymbolPacker.pack("GBPUSD"), 0x04); // prices only
    // globalEventBitMask = 0x01 | 0x04 = 0x05

    // Template 110 (AccountLoaded) maps to BIT_ACCOUNTS (0x10) — not in globalMask
    assertFalse(filter.matches(110, new byte[256], 0, 256));

    // Template 112 (OrderCancelRejected) maps to BIT_ORDERS (0x01) — in globalMask
    assertTrue(filter.matches(112, new byte[256], 0, 256));

    // Template 51 (PriceResponse) with unknown symbol would use globalMask
    // But PriceResponse HAS a symbol field — so it uses symbol lookup, not globalMask
    // This is correct: only no-symbol templates use globalMask
  }

  // --- Single subscription boundary ---

  @Test
  void matches_singleSubscription_binarySearchWorks() {
    final var filter = new SubscriptionFilter(MAX_SUBSCRIPTIONS);
    filter.addSubscription(SymbolPacker.pack("AUDUSD"), 0x1F);

    final int len = encodeOrderCreated("AUDUSD");
    final byte[] bytes = toByteArray(buffer, len);

    assertTrue(filter.matches(100, bytes, 0, len));
  }

  // --- Batch subscribe ---

  @Test
  void addSubscriptionsBatch_multipleSymbols_rebuildsSnapshotOnce() {
    final var filter = new SubscriptionFilter(MAX_SUBSCRIPTIONS);
    final long[] symbols = {
      SymbolPacker.pack("EURUSD"), SymbolPacker.pack("GBPUSD"), SymbolPacker.pack("USDJPY")
    };
    final int[] types = {0x01, 0x04, 0x1F};

    final int added = filter.addSubscriptionsBatch(symbols, types, 3);

    assertEquals(3, added);
    assertEquals(3, filter.subscriptionCount());
  }

  @Test
  void addSubscriptionsBatch_atCapacity_returnsPartialCount() {
    final var filter = new SubscriptionFilter(2);
    final long[] symbols = {
      SymbolPacker.pack("EURUSD"), SymbolPacker.pack("GBPUSD"), SymbolPacker.pack("USDJPY")
    };
    final int[] types = {0x01, 0x04, 0x1F};

    final int added = filter.addSubscriptionsBatch(symbols, types, 3);

    assertEquals(2, added); // capacity is 2
    assertEquals(2, filter.subscriptionCount());
  }

  @Test
  void addSubscriptionsBatch_zeroBitmaskEntries_skipped() {
    final var filter = new SubscriptionFilter(MAX_SUBSCRIPTIONS);
    final long[] symbols = {SymbolPacker.pack("EURUSD"), SymbolPacker.pack("GBPUSD")};
    final int[] types = {0x01, 0x00}; // second has zero bitmask

    final int added = filter.addSubscriptionsBatch(symbols, types, 2);

    assertEquals(1, added); // only first added
    assertEquals(1, filter.subscriptionCount());
  }

  // --- Truncated payload vs no-symbol template ---

  @Test
  void matches_truncatedPayloadOnSymbolTemplate_returnsFalse() {
    final var filter = new SubscriptionFilter(MAX_SUBSCRIPTIONS);
    filter.addSubscription(SymbolPacker.pack("EURUSD"), 0x01);

    // Template 100 (OrderCreated) has a symbol field, but provide truncated payload (20 bytes)
    assertFalse(
        filter.matches(100, new byte[20], 0, 20),
        "Truncated payload with symbol template should be dropped, not delivered");
  }

  // --- Helper methods ---

  private int encodeOrderCreated(final String symbol) {
    return SbeTestEncoder.encodeOrderCreatedEvent(
        buffer,
        0,
        1L,
        1000L,
        "ORD001",
        "EXEC001",
        "CLORD001",
        symbol,
        SideEnum.Buy,
        OrdTypeEnum.Limit,
        110_000_000L,
        100_000_000L,
        "ACME-001");
  }

  private static byte[] toByteArray(final MutableDirectBuffer buffer, final int length) {
    final byte[] bytes = new byte[length];
    buffer.getBytes(0, bytes);
    return bytes;
  }
}
