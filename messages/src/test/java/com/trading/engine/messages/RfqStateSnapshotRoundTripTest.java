package com.trading.engine.messages;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import com.trading.engine.messages.sbe.MessageHeaderDecoder;
import com.trading.engine.messages.sbe.MessageHeaderEncoder;
import com.trading.engine.messages.sbe.ProductTypeEnum;
import com.trading.engine.messages.sbe.RfqStateEnum;
import com.trading.engine.messages.sbe.RfqStateSnapshotDecoder;
import com.trading.engine.messages.sbe.RfqStateSnapshotDecoder.NoRfqsDecoder;
import com.trading.engine.messages.sbe.RfqStateSnapshotEncoder;
import com.trading.engine.messages.sbe.SettlTypeEnum;
import com.trading.engine.messages.sbe.SideEnum;
import com.trading.engine.messages.sbe.TenorEnum;
import java.nio.charset.StandardCharsets;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

/**
 * Codec round-trip tests for the RfqStateSnapshot message (template 203).
 *
 * <p>Template 203 carries a top-level repeating group {@code noRfqs} where each slot has its own
 * inner repeating group {@code noLegs} for swap legs. Tests cover the empty-pool case, single-slot
 * states (Requested / Quoted with legs), mixed states, and a bulk encode of 100 slots.
 *
 * <p>Each test allocates a 1 MiB {@link UnsafeBuffer} — large enough for 100 fully-populated swap
 * slots — and asserts every encoded field byte-for-byte after decoding to catch wire-offset bugs.
 *
 * <p>Threading model: Not thread-safe — single-threaded JUnit test execution only. Allocation:
 * Allocates one {@code UnsafeBuffer} per test (heap-backed byte array).
 */
final class RfqStateSnapshotRoundTripTest {

  /**
   * 1 MiB — comfortably fits 100 fully-populated swap slots (≈ 147 + 2×66 ≈ 283 B each = ~29 KB).
   */
  private static final int BUF_SIZE = 1_024 * 1_024;

  // Fixed-point price scale: 10^-8.
  private static final long PRICE_SCALE = 100_000_000L;

  // -------------------------------------------------------------------------
  // Test 1: empty pool
  // -------------------------------------------------------------------------

  /**
   * Encodes an RfqStateSnapshot with {@code noRfqsCount(0)} and asserts that the decoder sees an
   * empty group without error. This exercises the snapshot's wire format when there are no active
   * RFQs at snapshot time.
   */
  @Test
  void emptyPool_roundTrips() {
    final var buf = new UnsafeBuffer(new byte[BUF_SIZE]);
    final var headerEncoder = new MessageHeaderEncoder();
    final var encoder = new RfqStateSnapshotEncoder();
    encoder.wrapAndApplyHeader(buf, 0, headerEncoder);

    // Write zero slots.
    encoder.noRfqsCount(0);

    // Decode.
    final var headerDecoder = new MessageHeaderDecoder();
    final var decoder = new RfqStateSnapshotDecoder();
    decoder.wrapAndApplyHeader(buf, 0, headerDecoder);

    assertEquals(RfqStateSnapshotDecoder.TEMPLATE_ID, headerDecoder.templateId());

    final var rfqs = decoder.noRfqs();
    assertEquals(0, rfqs.count(), "expected zero RFQ slots");
    assertFalse(rfqs.hasNext(), "iterator must report no elements");
  }

  // -------------------------------------------------------------------------
  // Test 2: single slot — REQUESTED state, no legs
  // -------------------------------------------------------------------------

  /**
   * Encodes a single RFQ slot in the {@code Requested} state with all top-level fields populated
   * and {@code noLegs(0)} (single-leg RFQ). Asserts every field value after decode.
   *
   * <p>The {@code bidPx}, {@code offerPx}, {@code bidSize}, {@code offerSize}, {@code lastPx}, and
   * {@code swapPoints} optional fields are written as their null sentinels because Requested-state
   * RFQs have not yet received pricing from the pricing service.
   */
  @Test
  void singleRequestedSlot_roundTrips_allFieldsPreserved() {
    final var quoteReqId = "QREQ-RT-1";
    final long accountId = 101L;
    final var quoteId = ""; // not yet assigned
    final var symbol = "EURUSD";
    final long orderQty = 1_000_000L * PRICE_SCALE;
    final long validUntil = 1_750_000_030_000_000_000L;
    final long transactTime = 1_750_000_000_000_000_000L;
    final var settlDate = "20260115";
    final var currency = "EUR";
    final var settlCurrency = "USD";

    // Optional fields absent in Requested state — write null sentinel.
    final long nullLong = Long.MIN_VALUE; // SBE int64 null sentinel

    final var buf = new UnsafeBuffer(new byte[BUF_SIZE]);
    final var headerEncoder = new MessageHeaderEncoder();
    final var encoder = new RfqStateSnapshotEncoder();
    encoder.wrapAndApplyHeader(buf, 0, headerEncoder);

    final var rfqsEnc = encoder.noRfqsCount(1);
    rfqsEnc
        .next()
        .quoteReqId(quoteReqId)
        .accountId(accountId)
        .state(RfqStateEnum.Requested)
        .quoteId(quoteId)
        .symbol(symbol)
        .side(SideEnum.Buy)
        .orderQty(orderQty)
        .bidPx(nullLong)
        .offerPx(nullLong)
        .bidSize(nullLong)
        .offerSize(nullLong)
        .lastPx(nullLong)
        .swapPoints(nullLong)
        .validUntil(validUntil)
        .transactTime(transactTime)
        .productType(ProductTypeEnum.Spot)
        .settlDate(settlDate)
        .settlType(SettlTypeEnum.Regular)
        .currency(currency)
        .settlCurrency(settlCurrency)
        .tenor(TenorEnum.SN)
        .noLegsCount(0);

    // Decode.
    final var headerDecoder = new MessageHeaderDecoder();
    final var decoder = new RfqStateSnapshotDecoder();
    decoder.wrapAndApplyHeader(buf, 0, headerDecoder);

    final var rfqs = decoder.noRfqs();
    assertEquals(1, rfqs.count());

    rfqs.next();

    // quoteReqId
    final var qreqDst = new byte[NoRfqsDecoder.quoteReqIdLength()];
    rfqs.getQuoteReqId(qreqDst, 0);
    assertArrayEquals(
        padRight(quoteReqId, NoRfqsDecoder.quoteReqIdLength()), qreqDst, "quoteReqId");

    assertEquals(accountId, rfqs.accountId(), "accountId");
    assertEquals(RfqStateEnum.Requested, rfqs.state(), "state");

    // quoteId — empty string becomes all-NUL on the wire.
    final var qidDst = new byte[NoRfqsDecoder.quoteIdLength()];
    rfqs.getQuoteId(qidDst, 0);
    assertArrayEquals(new byte[NoRfqsDecoder.quoteIdLength()], qidDst, "quoteId should be all-NUL");

    // symbol
    final var symDst = new byte[NoRfqsDecoder.symbolLength()];
    rfqs.getSymbol(symDst, 0);
    assertArrayEquals(padRight(symbol, NoRfqsDecoder.symbolLength()), symDst, "symbol");

    assertEquals(SideEnum.Buy, rfqs.side(), "side");
    assertEquals(orderQty, rfqs.orderQty(), "orderQty");

    // Optional fields — must decode back to null sentinel.
    assertEquals(nullLong, rfqs.bidPx(), "bidPx must be null");
    assertEquals(nullLong, rfqs.offerPx(), "offerPx must be null");
    assertEquals(nullLong, rfqs.bidSize(), "bidSize must be null");
    assertEquals(nullLong, rfqs.offerSize(), "offerSize must be null");
    assertEquals(nullLong, rfqs.lastPx(), "lastPx must be null");
    assertEquals(nullLong, rfqs.swapPoints(), "swapPoints must be null");

    assertEquals(validUntil, rfqs.validUntil(), "validUntil");
    assertEquals(transactTime, rfqs.transactTime(), "transactTime");
    assertEquals(ProductTypeEnum.Spot, rfqs.productType(), "productType");
    assertEquals(TenorEnum.SN, rfqs.tenor(), "tenor");

    // settlDate
    final var sdDst = new byte[NoRfqsDecoder.settlDateLength()];
    rfqs.getSettlDate(sdDst, 0);
    assertArrayEquals(padRight(settlDate, NoRfqsDecoder.settlDateLength()), sdDst, "settlDate");

    assertEquals(SettlTypeEnum.Regular, rfqs.settlType(), "settlType");

    // currency
    final var cDst = new byte[NoRfqsDecoder.currencyLength()];
    rfqs.getCurrency(cDst, 0);
    assertArrayEquals(padRight(currency, NoRfqsDecoder.currencyLength()), cDst, "currency");

    // settlCurrency
    final var scDst = new byte[NoRfqsDecoder.settlCurrencyLength()];
    rfqs.getSettlCurrency(scDst, 0);
    assertArrayEquals(
        padRight(settlCurrency, NoRfqsDecoder.settlCurrencyLength()), scDst, "settlCurrency");

    // Inner legs group — expect zero.
    final var legs = rfqs.noLegs();
    assertEquals(0, legs.count(), "expected zero legs");
  }

  // -------------------------------------------------------------------------
  // Test 3: single slot — QUOTED state with 2 legs
  // -------------------------------------------------------------------------

  /**
   * Encodes a single RFQ slot in the {@code Quoted} state with {@code noLegs(2)} (swap RFQ).
   * Asserts all top-level price/size fields plus per-leg bid/offer fields after decode.
   *
   * <p>Leg 0 (near) and leg 1 (far) carry different prices and quantities so that intra-group
   * offset bugs produce observable failures.
   */
  @Test
  void singleQuotedSlot_roundTrips_withNoLegsTwo() {
    final var quoteReqId = "QREQ-RT-2";
    final long accountId = 202L;
    final var quoteId = "QTE-RT-2";
    final var symbol = "USDJPY";
    final long orderQty = 2_000_000L * PRICE_SCALE;
    final long bidPx = 149_50000000L;
    final long offerPx = 149_60000000L;
    final long bidSize = 1_000_000L * PRICE_SCALE;
    final long offerSize = 1_000_000L * PRICE_SCALE;
    final long swapPoints = 15000L; // swap mid-points in fixed-point
    final long validUntil = 1_750_000_060_000_000_000L;
    final long transactTime = 1_750_000_000_000_000_000L;
    final var settlDate = "20260116";
    final var currency = "USD";
    final var settlCurrency = "JPY";

    // Near leg (leg 0).
    final long legOrderQty0 = 1_000_000L * PRICE_SCALE;
    final long legBidPx0 = 149_48000000L;
    final long legOfferPx0 = 149_58000000L;
    final long legBidSize0 = 1_000_000L * PRICE_SCALE;
    final long legOfferSize0 = 1_000_000L * PRICE_SCALE;

    // Far leg (leg 1) — distinct values.
    final long legOrderQty1 = 1_000_100L * PRICE_SCALE;
    final long legBidPx1 = 149_98000000L;
    final long legOfferPx1 = 150_08000000L;
    final long legBidSize1 = 1_000_100L * PRICE_SCALE;
    final long legOfferSize1 = 1_000_100L * PRICE_SCALE;

    final var buf = new UnsafeBuffer(new byte[BUF_SIZE]);
    final var headerEncoder = new MessageHeaderEncoder();
    final var encoder = new RfqStateSnapshotEncoder();
    encoder.wrapAndApplyHeader(buf, 0, headerEncoder);

    final var rfqsEnc = encoder.noRfqsCount(1);
    final var slot = rfqsEnc.next();
    slot.quoteReqId(quoteReqId)
        .accountId(accountId)
        .state(RfqStateEnum.Quoted)
        .quoteId(quoteId)
        .symbol(symbol)
        .side(SideEnum.Buy)
        .orderQty(orderQty)
        .bidPx(bidPx)
        .offerPx(offerPx)
        .bidSize(bidSize)
        .offerSize(offerSize)
        .lastPx(Long.MIN_VALUE) // not yet accepted
        .swapPoints(swapPoints)
        .validUntil(validUntil)
        .transactTime(transactTime)
        .productType(ProductTypeEnum.Swap)
        .settlDate(settlDate)
        .settlType(SettlTypeEnum.Regular)
        .currency(currency)
        .settlCurrency(settlCurrency)
        .tenor(TenorEnum.W1);

    final var legsEnc = slot.noLegsCount(2);

    // Near leg.
    legsEnc
        .next()
        .legSide(SideEnum.Buy)
        .legSettlDate("20260116")
        .legSettlType(SettlTypeEnum.Regular)
        .legCurrency("USD")
        .legTenor(TenorEnum.SN)
        .legOrderQty(legOrderQty0)
        .legPrice(Long.MIN_VALUE) // not yet locked-in
        .legBidPx(legBidPx0)
        .legOfferPx(legOfferPx0)
        .legBidSize(legBidSize0)
        .legOfferSize(legOfferSize0);

    // Far leg.
    legsEnc
        .next()
        .legSide(SideEnum.Sell)
        .legSettlDate("20261116")
        .legSettlType(SettlTypeEnum.Cash)
        .legCurrency("USD")
        .legTenor(TenorEnum.M3)
        .legOrderQty(legOrderQty1)
        .legPrice(Long.MIN_VALUE)
        .legBidPx(legBidPx1)
        .legOfferPx(legOfferPx1)
        .legBidSize(legBidSize1)
        .legOfferSize(legOfferSize1);

    // Decode.
    final var headerDecoder = new MessageHeaderDecoder();
    final var decoder = new RfqStateSnapshotDecoder();
    decoder.wrapAndApplyHeader(buf, 0, headerDecoder);

    final var rfqs = decoder.noRfqs();
    assertEquals(1, rfqs.count());
    rfqs.next();

    final var qreqDst = new byte[NoRfqsDecoder.quoteReqIdLength()];
    rfqs.getQuoteReqId(qreqDst, 0);
    assertArrayEquals(
        padRight(quoteReqId, NoRfqsDecoder.quoteReqIdLength()), qreqDst, "quoteReqId");

    assertEquals(accountId, rfqs.accountId(), "accountId");
    assertEquals(RfqStateEnum.Quoted, rfqs.state(), "state");

    final var qidDst = new byte[NoRfqsDecoder.quoteIdLength()];
    rfqs.getQuoteId(qidDst, 0);
    assertArrayEquals(padRight(quoteId, NoRfqsDecoder.quoteIdLength()), qidDst, "quoteId");

    assertEquals(SideEnum.Buy, rfqs.side(), "side");
    assertEquals(orderQty, rfqs.orderQty(), "orderQty");
    assertEquals(bidPx, rfqs.bidPx(), "bidPx");
    assertEquals(offerPx, rfqs.offerPx(), "offerPx");
    assertEquals(bidSize, rfqs.bidSize(), "bidSize");
    assertEquals(offerSize, rfqs.offerSize(), "offerSize");
    assertEquals(Long.MIN_VALUE, rfqs.lastPx(), "lastPx must be null (not yet accepted)");
    assertEquals(swapPoints, rfqs.swapPoints(), "swapPoints");
    assertEquals(validUntil, rfqs.validUntil(), "validUntil");
    assertEquals(transactTime, rfqs.transactTime(), "transactTime");
    assertEquals(ProductTypeEnum.Swap, rfqs.productType(), "productType");
    assertEquals(TenorEnum.W1, rfqs.tenor(), "tenor");

    final var legs = rfqs.noLegs();
    assertEquals(2, legs.count(), "expected 2 legs");

    // Near leg.
    legs.next();
    assertEquals(SideEnum.Buy, legs.legSide(), "near legSide");
    assertEquals(TenorEnum.SN, legs.legTenor(), "near legTenor");
    assertEquals(legOrderQty0, legs.legOrderQty(), "near legOrderQty");
    assertEquals(Long.MIN_VALUE, legs.legPrice(), "near legPrice must be null");
    assertEquals(legBidPx0, legs.legBidPx(), "near legBidPx");
    assertEquals(legOfferPx0, legs.legOfferPx(), "near legOfferPx");
    assertEquals(legBidSize0, legs.legBidSize(), "near legBidSize");
    assertEquals(legOfferSize0, legs.legOfferSize(), "near legOfferSize");

    // Far leg.
    legs.next();
    assertEquals(SideEnum.Sell, legs.legSide(), "far legSide");
    assertEquals(TenorEnum.M3, legs.legTenor(), "far legTenor");
    assertEquals(legOrderQty1, legs.legOrderQty(), "far legOrderQty");
    assertEquals(Long.MIN_VALUE, legs.legPrice(), "far legPrice must be null");
    assertEquals(legBidPx1, legs.legBidPx(), "far legBidPx");
    assertEquals(legOfferPx1, legs.legOfferPx(), "far legOfferPx");
    assertEquals(legBidSize1, legs.legBidSize(), "far legBidSize");
    assertEquals(legOfferSize1, legs.legOfferSize(), "far legOfferSize");
  }

  // -------------------------------------------------------------------------
  // Test 4: mixed states — 3 slots
  // -------------------------------------------------------------------------

  /**
   * Encodes three RFQ slots with distinct states (Requested → Quoted → Accepted) in a single {@code
   * noRfqs} group and asserts that after decode each slot preserves its own state and
   * distinguishing field values. Using different quoteReqIds and orderQtys for each slot ensures
   * that a decoder that misadvances between slots is observable.
   */
  @Test
  void mixedStates_roundTripPreservesEachSlotsFieldsAndState() {
    // Slot 0 — Requested.
    final var qreqId0 = "QREQ-MIX-0";
    final long accountId0 = 301L;
    final long orderQty0 = 1_000_000L * PRICE_SCALE;

    // Slot 1 — Quoted.
    final var qreqId1 = "QREQ-MIX-1";
    final long accountId1 = 302L;
    final long orderQty1 = 2_000_000L * PRICE_SCALE;
    final var quoteId1 = "QTE-MIX-1";
    final long bidPx1 = 1_05100000_00L;
    final long offerPx1 = 1_05200000_00L;
    final long validUntil1 = 1_750_000_060_000_000_000L;

    // Slot 2 — Accepted.
    final var qreqId2 = "QREQ-MIX-2";
    final long accountId2 = 303L;
    final long orderQty2 = 3_000_000L * PRICE_SCALE;
    final var quoteId2 = "QTE-MIX-2";
    final long lastPx2 = 1_05150000_00L;

    final long ts = 1_750_000_000_000_000_000L;
    final long vu = 1_750_000_030_000_000_000L;

    final var buf = new UnsafeBuffer(new byte[BUF_SIZE]);
    final var headerEncoder = new MessageHeaderEncoder();
    final var encoder = new RfqStateSnapshotEncoder();
    encoder.wrapAndApplyHeader(buf, 0, headerEncoder);

    final var rfqsEnc = encoder.noRfqsCount(3);

    // Slot 0 — Requested, no pricing, no legs.
    rfqsEnc
        .next()
        .quoteReqId(qreqId0)
        .accountId(accountId0)
        .state(RfqStateEnum.Requested)
        .quoteId("")
        .symbol("EURUSD")
        .side(SideEnum.Buy)
        .orderQty(orderQty0)
        .bidPx(Long.MIN_VALUE)
        .offerPx(Long.MIN_VALUE)
        .bidSize(Long.MIN_VALUE)
        .offerSize(Long.MIN_VALUE)
        .lastPx(Long.MIN_VALUE)
        .swapPoints(Long.MIN_VALUE)
        .validUntil(vu)
        .transactTime(ts)
        .productType(ProductTypeEnum.Spot)
        .settlDate("20260117")
        .settlType(SettlTypeEnum.Regular)
        .currency("EUR")
        .settlCurrency("USD")
        .tenor(TenorEnum.SN)
        .noLegsCount(0);

    // Slot 1 — Quoted, prices set, no legs.
    rfqsEnc
        .next()
        .quoteReqId(qreqId1)
        .accountId(accountId1)
        .state(RfqStateEnum.Quoted)
        .quoteId(quoteId1)
        .symbol("GBPUSD")
        .side(SideEnum.Sell)
        .orderQty(orderQty1)
        .bidPx(bidPx1)
        .offerPx(offerPx1)
        .bidSize(orderQty1)
        .offerSize(orderQty1)
        .lastPx(Long.MIN_VALUE)
        .swapPoints(Long.MIN_VALUE)
        .validUntil(validUntil1)
        .transactTime(ts)
        .productType(ProductTypeEnum.Spot)
        .settlDate("20260118")
        .settlType(SettlTypeEnum.Regular)
        .currency("GBP")
        .settlCurrency("USD")
        .tenor(TenorEnum.SN)
        .noLegsCount(0);

    // Slot 2 — Accepted, lastPx set, no legs.
    rfqsEnc
        .next()
        .quoteReqId(qreqId2)
        .accountId(accountId2)
        .state(RfqStateEnum.Accepted)
        .quoteId(quoteId2)
        .symbol("USDCHF")
        .side(SideEnum.Buy)
        .orderQty(orderQty2)
        .bidPx(Long.MIN_VALUE)
        .offerPx(Long.MIN_VALUE)
        .bidSize(Long.MIN_VALUE)
        .offerSize(Long.MIN_VALUE)
        .lastPx(lastPx2)
        .swapPoints(Long.MIN_VALUE)
        .validUntil(vu)
        .transactTime(ts)
        .productType(ProductTypeEnum.Spot)
        .settlDate("20260119")
        .settlType(SettlTypeEnum.Regular)
        .currency("USD")
        .settlCurrency("CHF")
        .tenor(TenorEnum.SN)
        .noLegsCount(0);

    // Decode.
    final var headerDecoder = new MessageHeaderDecoder();
    final var decoder = new RfqStateSnapshotDecoder();
    decoder.wrapAndApplyHeader(buf, 0, headerDecoder);

    final var rfqs = decoder.noRfqs();
    assertEquals(3, rfqs.count(), "expected 3 RFQ slots");

    // Slot 0.
    rfqs.next();
    final var q0 = new byte[NoRfqsDecoder.quoteReqIdLength()];
    rfqs.getQuoteReqId(q0, 0);
    assertArrayEquals(padRight(qreqId0, NoRfqsDecoder.quoteReqIdLength()), q0, "slot0 quoteReqId");
    assertEquals(accountId0, rfqs.accountId(), "slot0 accountId");
    assertEquals(RfqStateEnum.Requested, rfqs.state(), "slot0 state");
    assertEquals(orderQty0, rfqs.orderQty(), "slot0 orderQty");
    assertEquals(Long.MIN_VALUE, rfqs.bidPx(), "slot0 bidPx must be null");
    assertEquals(Long.MIN_VALUE, rfqs.lastPx(), "slot0 lastPx must be null");
    rfqs.noLegs(); // consume inner group to advance limit pointer

    // Slot 1.
    rfqs.next();
    final var q1 = new byte[NoRfqsDecoder.quoteReqIdLength()];
    rfqs.getQuoteReqId(q1, 0);
    assertArrayEquals(padRight(qreqId1, NoRfqsDecoder.quoteReqIdLength()), q1, "slot1 quoteReqId");
    assertEquals(accountId1, rfqs.accountId(), "slot1 accountId");
    assertEquals(RfqStateEnum.Quoted, rfqs.state(), "slot1 state");
    assertEquals(orderQty1, rfqs.orderQty(), "slot1 orderQty");
    assertEquals(bidPx1, rfqs.bidPx(), "slot1 bidPx");
    assertEquals(offerPx1, rfqs.offerPx(), "slot1 offerPx");
    assertEquals(validUntil1, rfqs.validUntil(), "slot1 validUntil");
    rfqs.noLegs();

    // Slot 2.
    rfqs.next();
    final var q2 = new byte[NoRfqsDecoder.quoteReqIdLength()];
    rfqs.getQuoteReqId(q2, 0);
    assertArrayEquals(padRight(qreqId2, NoRfqsDecoder.quoteReqIdLength()), q2, "slot2 quoteReqId");
    assertEquals(accountId2, rfqs.accountId(), "slot2 accountId");
    assertEquals(RfqStateEnum.Accepted, rfqs.state(), "slot2 state");
    assertEquals(orderQty2, rfqs.orderQty(), "slot2 orderQty");
    assertEquals(lastPx2, rfqs.lastPx(), "slot2 lastPx");
    rfqs.noLegs();
  }

  // -------------------------------------------------------------------------
  // Test 5: 100-slot capacity fill
  // -------------------------------------------------------------------------

  /**
   * Encodes 100 RFQ slots in a single snapshot and asserts that the decoded count is exactly 100.
   * Also spot-checks the first, last, and a middle slot to confirm that the encoder and decoder
   * advance through the repeating group correctly without corruption.
   *
   * <p>Each slot uses a unique orderQty derived from its index so that an incorrect group-advance
   * implementation produces observable assertion failures.
   */
  @Test
  void capacityFull_roundTrip() {
    final int count = 100;
    final long baseQty = 500_000L * PRICE_SCALE;
    final long ts = 1_750_000_000_000_000_000L;
    final long vu = 1_750_000_030_000_000_000L;

    final var buf = new UnsafeBuffer(new byte[BUF_SIZE]);
    final var headerEncoder = new MessageHeaderEncoder();
    final var encoder = new RfqStateSnapshotEncoder();
    encoder.wrapAndApplyHeader(buf, 0, headerEncoder);

    final var rfqsEnc = encoder.noRfqsCount(count);
    for (int i = 0; i < count; i++) {
      // quoteReqId: "QREQ-BULK-NNN" padded to 20 characters.
      final var qreqId = String.format("QREQ-BULK-%03d", i);
      // Vary orderQty per slot so misalignment is observable.
      final long orderQty = baseQty + (long) i * PRICE_SCALE;

      rfqsEnc
          .next()
          .quoteReqId(qreqId)
          .accountId((long) (1000 + i))
          .state(RfqStateEnum.Requested)
          .quoteId("")
          .symbol("EURUSD")
          .side(SideEnum.Buy)
          .orderQty(orderQty)
          .bidPx(Long.MIN_VALUE)
          .offerPx(Long.MIN_VALUE)
          .bidSize(Long.MIN_VALUE)
          .offerSize(Long.MIN_VALUE)
          .lastPx(Long.MIN_VALUE)
          .swapPoints(Long.MIN_VALUE)
          .validUntil(vu)
          .transactTime(ts)
          .productType(ProductTypeEnum.Spot)
          .settlDate("20260120")
          .settlType(SettlTypeEnum.Regular)
          .currency("EUR")
          .settlCurrency("USD")
          .tenor(TenorEnum.SN)
          .noLegsCount(0);
    }

    // Decode.
    final var headerDecoder = new MessageHeaderDecoder();
    final var decoder = new RfqStateSnapshotDecoder();
    decoder.wrapAndApplyHeader(buf, 0, headerDecoder);

    final var rfqs = decoder.noRfqs();
    assertEquals(count, rfqs.count(), "expected 100 RFQ slots");

    // Verify slot 0 (first).
    rfqs.next();
    final var qreqDst0 = new byte[NoRfqsDecoder.quoteReqIdLength()];
    rfqs.getQuoteReqId(qreqDst0, 0);
    assertArrayEquals(
        padRight("QREQ-BULK-000", NoRfqsDecoder.quoteReqIdLength()), qreqDst0, "slot0 quoteReqId");
    assertEquals(1000L, rfqs.accountId(), "slot0 accountId");
    assertEquals(baseQty, rfqs.orderQty(), "slot0 orderQty");
    rfqs.noLegs();

    // Advance through slots 1-48, consuming inner legs to keep the limit pointer aligned.
    for (int i = 1; i < 49; i++) {
      rfqs.next();
      rfqs.noLegs();
    }

    // Verify slot 49 (middle spot-check).
    rfqs.next();
    assertEquals(1049L, rfqs.accountId(), "slot49 accountId");
    assertEquals(baseQty + 49L * PRICE_SCALE, rfqs.orderQty(), "slot49 orderQty");
    rfqs.noLegs();

    // Advance through slots 50-98.
    for (int i = 50; i < 99; i++) {
      rfqs.next();
      rfqs.noLegs();
    }

    // Verify slot 99 (last).
    rfqs.next();
    final var qreqDst99 = new byte[NoRfqsDecoder.quoteReqIdLength()];
    rfqs.getQuoteReqId(qreqDst99, 0);
    assertArrayEquals(
        padRight("QREQ-BULK-099", NoRfqsDecoder.quoteReqIdLength()),
        qreqDst99,
        "slot99 quoteReqId");
    assertEquals(1099L, rfqs.accountId(), "slot99 accountId");
    assertEquals(baseQty + 99L * PRICE_SCALE, rfqs.orderQty(), "slot99 orderQty");
    rfqs.noLegs();

    assertFalse(rfqs.hasNext(), "no more slots after slot 99");
  }

  // -------------------------------------------------------------------------
  // Helpers
  // -------------------------------------------------------------------------

  /**
   * Returns a NUL-padded byte array of exactly {@code width} bytes containing the ASCII encoding of
   * {@code value}. Mirrors the SBE encoder behaviour: characters are written left-justified and the
   * tail is filled with zero bytes.
   *
   * @param value the string to encode; must be &lt;= {@code width} characters long
   * @param width the fixed field width in bytes
   * @return byte array of length {@code width}
   */
  private static byte[] padRight(final String value, final int width) {
    final var dst = new byte[width];
    if (value != null && !value.isEmpty()) {
      final byte[] src = value.getBytes(StandardCharsets.US_ASCII);
      System.arraycopy(src, 0, dst, 0, Math.min(src.length, width));
    }
    return dst;
  }
}
