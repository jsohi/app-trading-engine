package com.trading.engine.testsupport.sbe;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.trading.engine.messages.sbe.AccountLoadRejectedEventDecoder;
import com.trading.engine.messages.sbe.AccountLoadedEventDecoder;
import com.trading.engine.messages.sbe.AccountStatusEnum;
import com.trading.engine.messages.sbe.AccountTypeEnum;
import com.trading.engine.messages.sbe.AcctIDSourceEnum;
import com.trading.engine.messages.sbe.CancelOrderRequestDecoder;
import com.trading.engine.messages.sbe.ComplianceStatusEnum;
import com.trading.engine.messages.sbe.CurrencyClassEnum;
import com.trading.engine.messages.sbe.CurrencyLoadedEventDecoder;
import com.trading.engine.messages.sbe.ExecTypeEnum;
import com.trading.engine.messages.sbe.ExecutionReportEncoder;
import com.trading.engine.messages.sbe.LoadAccountBatchDecoder;
import com.trading.engine.messages.sbe.LoadAccountDecoder;
import com.trading.engine.messages.sbe.LoadCurrencyDecoder;
import com.trading.engine.messages.sbe.LoadRiskLimitDecoder;
import com.trading.engine.messages.sbe.MessageHeaderDecoder;
import com.trading.engine.messages.sbe.NewOrderSingleDecoder;
import com.trading.engine.messages.sbe.NewOrderSingleEncoder;
import com.trading.engine.messages.sbe.OrdStatusEnum;
import com.trading.engine.messages.sbe.OrdTypeEnum;
import com.trading.engine.messages.sbe.OrderCanceledEventDecoder;
import com.trading.engine.messages.sbe.OrderCreatedEventDecoder;
import com.trading.engine.messages.sbe.OrderFilledEventDecoder;
import com.trading.engine.messages.sbe.OrderRejectedEventDecoder;
import com.trading.engine.messages.sbe.ProductTypeEnum;
import com.trading.engine.messages.sbe.RejectReasonEnum;
import com.trading.engine.messages.sbe.RiskLimitLoadRejectedEventDecoder;
import com.trading.engine.messages.sbe.RiskLimitLoadedEventDecoder;
import com.trading.engine.messages.sbe.SettlTypeEnum;
import com.trading.engine.messages.sbe.SideEnum;
import com.trading.engine.messages.sbe.TenorEnum;
import com.trading.engine.messages.sbe.TimeInForceEnum;
import com.trading.engine.testsupport.buffer.TestBuffers;
import org.agrona.MutableDirectBuffer;
import org.junit.jupiter.api.Test;

/**
 * Round-trip tests for {@link SbeTestEncoder} and {@link SbeTestDecoder}.
 *
 * <p>Each test encodes a single SBE message type into a buffer using {@link SbeTestEncoder}, then
 * decodes it using either {@link SbeTestDecoder} (for events) or the raw SBE decoder (for
 * commands), and verifies that every field survives the round trip.
 *
 * <p>Also exercises {@link SbeMessageAssertions#assertTemplateId} for both passing and failing
 * cases.
 *
 * <p>Not thread-safe -- test instance per method (JUnit default).
 */
class SbeEncoderDecoderRoundTripTest {

  private static final long PRICE_SCALE =
      com.trading.engine.testsupport.FixedPointTestUtil.PRICE_SCALE;

  // -----------------------------------------------------------------------
  // Commands — template ID + raw decode verification
  // -----------------------------------------------------------------------

  @Test
  void encodeNewOrderSingle_allFields_correctTemplateIdAndFields() {
    final MutableDirectBuffer buf = TestBuffers.command();
    final String clOrdId = "CLO-001";
    final String symbol = "EURUSD";
    final SideEnum side = SideEnum.Buy;
    final OrdTypeEnum ordType = OrdTypeEnum.Limit;
    final long price = 110_500_000L; // 1.105 in fixed-point
    final long orderQty = 1_000_000 * PRICE_SCALE;
    final String accountCode = "ACCT-A";
    final String currency = "EUR";
    final ProductTypeEnum productType = ProductTypeEnum.Spot;
    final String settlDate = "20260115";
    final SettlTypeEnum settlType = SettlTypeEnum.Regular;
    final String settlCurrency = "USD";
    final TenorEnum tenor = TenorEnum.SN;
    final String quoteId = "Q-42";
    final TimeInForceEnum timeInForce = TimeInForceEnum.GTC;
    final long transactTime = 1_000_000_000L;

    final int encodedLen =
        SbeTestEncoder.encodeNewOrderSingle(
            buf,
            0,
            clOrdId,
            symbol,
            side,
            ordType,
            price,
            orderQty,
            accountCode,
            currency,
            productType,
            settlDate,
            settlType,
            settlCurrency,
            tenor,
            quoteId,
            timeInForce,
            transactTime);

    // Verify template ID
    SbeMessageAssertions.assertTemplateId(NewOrderSingleEncoder.TEMPLATE_ID, buf, 0);

    // Decode with raw decoder and verify all fields
    final NewOrderSingleDecoder dec = new NewOrderSingleDecoder();
    final MessageHeaderDecoder hdr = new MessageHeaderDecoder();
    dec.wrapAndApplyHeader(buf, 0, hdr);

    assertEquals(clOrdId, dec.clOrdId());
    assertEquals(symbol, dec.symbol());
    assertEquals(side, dec.side());
    assertEquals(ordType, dec.ordType());
    assertEquals(price, dec.price());
    assertEquals(orderQty, dec.orderQty());
    assertEquals(accountCode, dec.accountCode());
    assertEquals(currency, dec.currency());
    assertEquals(productType, dec.productType());
    assertEquals(settlDate, dec.settlDate());
    assertEquals(settlType, dec.settlType());
    assertEquals(settlCurrency, dec.settlCurrency());
    assertEquals(tenor, dec.tenor());
    assertEquals(quoteId, dec.quoteId());
    assertEquals(timeInForce, dec.timeInForce());
    assertEquals(transactTime, dec.transactTime());
  }

  @Test
  void encodeOrderCancelRequest_roundTrip_correctFields() {
    final MutableDirectBuffer buf = TestBuffers.command();
    final String origClOrdId = "CLO-001";
    final String clOrdId = "CXL-001";
    final String symbol = "USDJPY";
    final SideEnum side = SideEnum.Sell;
    final long transactTime = 2_000_000_000L;
    final String accountCode = "ACCT-B";
    final ProductTypeEnum productType = ProductTypeEnum.Spot;

    SbeTestEncoder.encodeOrderCancelRequest(
        buf, 0, origClOrdId, clOrdId, symbol, side, transactTime, accountCode, productType);

    SbeMessageAssertions.assertTemplateId(CancelOrderRequestDecoder.TEMPLATE_ID, buf, 0);

    final CancelOrderRequestDecoder dec = new CancelOrderRequestDecoder();
    final MessageHeaderDecoder hdr = new MessageHeaderDecoder();
    dec.wrapAndApplyHeader(buf, 0, hdr);

    assertEquals(origClOrdId, dec.origClOrdId());
    assertEquals(clOrdId, dec.clOrdId());
    assertEquals(symbol, dec.symbol());
    assertEquals(side, dec.side());
    assertEquals(transactTime, dec.transactTime());
    assertEquals(accountCode, dec.accountCode());
    assertEquals(productType, dec.productType());
  }

  @Test
  void encodeLoadAccount_roundTrip_correctFields() {
    final MutableDirectBuffer buf = TestBuffers.command();
    final long accountId = 42L;
    final String accountCode = "HEDGE-FUND-1";
    final String accountName = "Hedge Fund Alpha";
    final String baseCurrency = "GBP";

    SbeTestEncoder.encodeLoadAccount(buf, 0, accountId, accountCode, accountName, baseCurrency);

    SbeMessageAssertions.assertTemplateId(LoadAccountDecoder.TEMPLATE_ID, buf, 0);

    final LoadAccountDecoder dec = new LoadAccountDecoder();
    final MessageHeaderDecoder hdr = new MessageHeaderDecoder();
    dec.wrapAndApplyHeader(buf, 0, hdr);

    assertEquals(accountId, dec.accountId());
    assertEquals(0L, dec.parentAccountId());
    assertEquals(accountCode, dec.accountCode());
    assertEquals(AcctIDSourceEnum.Internal, dec.acctIdSource());
    assertEquals(accountName, dec.accountName());
    assertEquals(AccountTypeEnum.Client, dec.accountType());
    assertEquals(baseCurrency, dec.baseCurrency());
    assertEquals(AccountStatusEnum.Active, dec.status());
    assertEquals(ComplianceStatusEnum.OK, dec.complianceStatus());
    assertEquals(AccountRecord.CAN_TRADE | AccountRecord.CAN_RFQ, dec.capabilities());
    assertEquals(0L, dec.transactTime());
  }

  @Test
  void encodeLoadCurrency_roundTrip_correctFields() {
    final MutableDirectBuffer buf = TestBuffers.command();
    final String ccyCode = "JPY";
    final int isoNumeric = 392;
    final String name = "Japanese Yen";
    final int decimals = 0;
    final CurrencyClassEnum currencyClass = CurrencyClassEnum.Fiat;
    final AccountStatusEnum status = AccountStatusEnum.Active;

    SbeTestEncoder.encodeLoadCurrency(
        buf, 0, ccyCode, isoNumeric, name, decimals, currencyClass, status);

    SbeMessageAssertions.assertTemplateId(LoadCurrencyDecoder.TEMPLATE_ID, buf, 0);

    final LoadCurrencyDecoder dec = new LoadCurrencyDecoder();
    final MessageHeaderDecoder hdr = new MessageHeaderDecoder();
    dec.wrapAndApplyHeader(buf, 0, hdr);

    assertEquals(ccyCode, dec.ccyCode());
    assertEquals(isoNumeric, dec.isoNumeric());
    assertEquals(name, dec.name());
    assertEquals((short) decimals, dec.decimals());
    assertEquals(currencyClass, dec.currencyClass());
    assertEquals(status, dec.status());
    assertEquals(0L, dec.transactTime());
  }

  @Test
  void encodeLoadRiskLimit_roundTrip_correctFields() {
    final MutableDirectBuffer buf = TestBuffers.command();
    final long accountId = 99L;
    final long maxOrderSize = 500_000 * PRICE_SCALE;
    final long maxOrderNotional = 10_000_000 * PRICE_SCALE;
    final long maxDailyVolume = 50_000_000 * PRICE_SCALE;
    final long maxDailyLossBps = 500L;
    final AccountStatusEnum status = AccountStatusEnum.Active;
    final long transactTime = 3_000_000_000L;

    SbeTestEncoder.encodeLoadRiskLimit(
        buf,
        0,
        accountId,
        maxOrderSize,
        maxOrderNotional,
        maxDailyVolume,
        maxDailyLossBps,
        status,
        transactTime);

    SbeMessageAssertions.assertTemplateId(LoadRiskLimitDecoder.TEMPLATE_ID, buf, 0);

    final LoadRiskLimitDecoder dec = new LoadRiskLimitDecoder();
    final MessageHeaderDecoder hdr = new MessageHeaderDecoder();
    dec.wrapAndApplyHeader(buf, 0, hdr);

    assertEquals(accountId, dec.accountId());
    assertEquals(maxOrderSize, dec.maxOrderSize());
    assertEquals(maxOrderNotional, dec.maxOrderNotional());
    assertEquals(maxDailyVolume, dec.maxDailyVolume());
    assertEquals(maxDailyLossBps, dec.maxDailyLossBps());
    assertEquals(status, dec.status());
    assertEquals(transactTime, dec.transactTime());
  }

  // -----------------------------------------------------------------------
  // Events — full round-trip via SbeTestEncoder → SbeTestDecoder
  // -----------------------------------------------------------------------

  @Test
  void encodeOrderCreatedEvent_roundTrip_allFieldsMatch() {
    final MutableDirectBuffer buf = TestBuffers.event();
    final long seqNum = 1L;
    final long timestamp = 1_713_000_000_000_000_000L;
    final String orderId = "ORD-100";
    final String execId = "EXEC-100";
    final String clOrdId = "CLO-100";
    final String symbol = "GBPUSD";
    final SideEnum side = SideEnum.Buy;
    final OrdTypeEnum ordType = OrdTypeEnum.Limit;
    final TimeInForceEnum timeInForce = TimeInForceEnum.Day;
    final long price = 126_750_000L; // 1.2675
    final long orderQty = 2_000_000 * PRICE_SCALE;
    final String quoteId = "QT-77";
    final String accountCode = "ACCT-C";
    final ProductTypeEnum productType = ProductTypeEnum.Spot;
    final String settlDate = "20260201";
    final SettlTypeEnum settlType = SettlTypeEnum.Regular;
    final String currency = "GBP";
    final String settlCurrency = "USD";
    final TenorEnum tenor = TenorEnum.SN;

    SbeTestEncoder.encodeOrderCreatedEvent(
        buf,
        0,
        seqNum,
        timestamp,
        orderId,
        execId,
        clOrdId,
        symbol,
        side,
        ordType,
        timeInForce,
        price,
        orderQty,
        quoteId,
        accountCode,
        productType,
        settlDate,
        settlType,
        currency,
        settlCurrency,
        tenor);

    final OrderCreatedEventDecoder dec = SbeTestDecoder.decodeOrderCreated(buf, 0);

    assertEquals(seqNum, dec.sequenceNumber());
    assertEquals(timestamp, dec.timestamp());
    assertEquals(orderId, dec.orderId());
    assertEquals(execId, dec.execId());
    assertEquals(clOrdId, dec.clOrdId());
    assertEquals(symbol, dec.symbol());
    assertEquals(side, dec.side());
    assertEquals(ordType, dec.ordType());
    assertEquals(timeInForce, dec.timeInForce());
    assertEquals(price, dec.price());
    assertEquals(orderQty, dec.orderQty());
    assertEquals(quoteId, dec.quoteId());
    assertEquals(accountCode, dec.accountCode());
    assertEquals(productType, dec.productType());
    assertEquals(settlDate, dec.settlDate());
    assertEquals(settlType, dec.settlType());
    assertEquals(currency, dec.currency());
    assertEquals(settlCurrency, dec.settlCurrency());
    assertEquals(tenor, dec.tenor());
  }

  @Test
  void encodeOrderCreatedEvent_simpleOverload_roundTripsWithDefaults() {
    final MutableDirectBuffer buf = TestBuffers.event();
    final long seqNum = 2L;
    final long timestamp = 1_713_100_000_000_000_000L;
    final String orderId = "ORD-200";
    final String execId = "EXEC-200";
    final String clOrdId = "CLO-200";
    final String symbol = "EURUSD";
    final SideEnum side = SideEnum.Sell;
    final OrdTypeEnum ordType = OrdTypeEnum.Market;
    final long price = 0L;
    final long orderQty = 500_000 * PRICE_SCALE;
    final String accountCode = "ACCT-D";

    SbeTestEncoder.encodeOrderCreatedEvent(
        buf,
        0,
        seqNum,
        timestamp,
        orderId,
        execId,
        clOrdId,
        symbol,
        side,
        ordType,
        price,
        orderQty,
        accountCode);

    final OrderCreatedEventDecoder dec = SbeTestDecoder.decodeOrderCreated(buf, 0);

    assertEquals(seqNum, dec.sequenceNumber());
    assertEquals(timestamp, dec.timestamp());
    assertEquals(orderId, dec.orderId());
    assertEquals(execId, dec.execId());
    assertEquals(clOrdId, dec.clOrdId());
    assertEquals(symbol, dec.symbol());
    assertEquals(side, dec.side());
    assertEquals(ordType, dec.ordType());
    // Verify convenience defaults
    assertEquals(TimeInForceEnum.Day, dec.timeInForce());
    assertEquals(price, dec.price());
    assertEquals(orderQty, dec.orderQty());
    assertEquals("", dec.quoteId());
    assertEquals(accountCode, dec.accountCode());
    assertEquals(ProductTypeEnum.Spot, dec.productType());
    assertEquals("20260101", dec.settlDate());
    assertEquals(SettlTypeEnum.Regular, dec.settlType());
    assertEquals("USD", dec.currency());
    assertEquals("USD", dec.settlCurrency());
    assertEquals(TenorEnum.SN, dec.tenor());
  }

  @Test
  void encodeOrderRejectedEvent_roundTrip_allFieldsMatch() {
    final MutableDirectBuffer buf = TestBuffers.event();
    final long seqNum = 3L;
    final long timestamp = 1_713_200_000_000_000_000L;
    final String clOrdId = "CLO-300";
    final RejectReasonEnum rejectReason = RejectReasonEnum.UnknownSymbol;
    final String text = "Symbol XYZABC not found";
    final String symbol = "XYZABC";
    final SideEnum side = SideEnum.Buy;
    final String accountCode = "ACCT-E";
    final ProductTypeEnum productType = ProductTypeEnum.Spot;
    final String currency = "USD";

    SbeTestEncoder.encodeOrderRejectedEvent(
        buf,
        0,
        seqNum,
        timestamp,
        clOrdId,
        rejectReason,
        text,
        symbol,
        side,
        accountCode,
        productType,
        currency);

    final OrderRejectedEventDecoder dec = SbeTestDecoder.decodeOrderRejected(buf, 0);

    assertEquals(seqNum, dec.sequenceNumber());
    assertEquals(timestamp, dec.timestamp());
    assertEquals(clOrdId, dec.clOrdId());
    assertEquals(symbol, dec.symbol());
    assertEquals(side, dec.side());
    assertEquals(rejectReason, dec.rejectReason());
    assertEquals(accountCode, dec.accountCode());
    assertEquals(productType, dec.productType());
    assertEquals(currency, dec.currency());
    assertEquals(text, dec.text());
  }

  @Test
  void encodeOrderRejectedEvent_minimalOverload_roundTripsWithDefaults() {
    final MutableDirectBuffer buf = TestBuffers.event();
    final long seqNum = 4L;
    final long timestamp = 1_713_300_000_000_000_000L;
    final String clOrdId = "CLO-400";
    final RejectReasonEnum rejectReason = RejectReasonEnum.DuplicateClOrdId;
    final String text = "Duplicate order ID";

    SbeTestEncoder.encodeOrderRejectedEvent(buf, 0, seqNum, timestamp, clOrdId, rejectReason, text);

    final OrderRejectedEventDecoder dec = SbeTestDecoder.decodeOrderRejected(buf, 0);

    assertEquals(seqNum, dec.sequenceNumber());
    assertEquals(timestamp, dec.timestamp());
    assertEquals(clOrdId, dec.clOrdId());
    assertEquals(rejectReason, dec.rejectReason());
    assertEquals(text, dec.text());
    // Verify convenience defaults
    assertEquals("", dec.symbol());
    assertEquals(SideEnum.Buy, dec.side());
    assertEquals("", dec.accountCode());
    assertEquals(ProductTypeEnum.Spot, dec.productType());
    assertEquals("", dec.currency());
  }

  @Test
  void encodeOrderFilledEvent_roundTrip_allFieldsMatch() {
    final MutableDirectBuffer buf = TestBuffers.event();
    final long seqNum = 5L;
    final long timestamp = 1_713_400_000_000_000_000L;
    final String execId = "EXEC-500";
    final String orderId = "ORD-500";
    final String clOrdId = "CLO-500";
    final String symbol = "AUDUSD";
    final SideEnum side = SideEnum.Buy;
    final long lastPx = 65_250_000L; // 0.6525
    final long lastQty = 1_000_000 * PRICE_SCALE;
    final long leavesQty = 0L;
    final long cumQty = 1_000_000 * PRICE_SCALE;
    final String accountCode = "ACCT-F";
    final ProductTypeEnum productType = ProductTypeEnum.Spot;
    final String settlDate = "20260301";
    final SettlTypeEnum settlType = SettlTypeEnum.Regular;
    final String currency = "AUD";
    final String settlCurrency = "USD";
    final TenorEnum tenor = TenorEnum.SN;

    SbeTestEncoder.encodeOrderFilledEvent(
        buf,
        0,
        seqNum,
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
        productType,
        settlDate,
        settlType,
        currency,
        settlCurrency,
        tenor);

    final OrderFilledEventDecoder dec = SbeTestDecoder.decodeOrderFilled(buf, 0);

    assertEquals(seqNum, dec.sequenceNumber());
    assertEquals(timestamp, dec.timestamp());
    assertEquals(execId, dec.execId());
    assertEquals(orderId, dec.orderId());
    assertEquals(clOrdId, dec.clOrdId());
    assertEquals(symbol, dec.symbol());
    assertEquals(side, dec.side());
    assertEquals(lastPx, dec.lastPx());
    assertEquals(lastQty, dec.lastQty());
    assertEquals(leavesQty, dec.leavesQty());
    assertEquals(cumQty, dec.cumQty());
    assertEquals(accountCode, dec.accountCode());
    assertEquals(productType, dec.productType());
    assertEquals(settlDate, dec.settlDate());
    assertEquals(settlType, dec.settlType());
    assertEquals(currency, dec.currency());
    assertEquals(settlCurrency, dec.settlCurrency());
    assertEquals(tenor, dec.tenor());
    // Single-leg fill: noLegs group must have count 0
    assertEquals(0, dec.noLegs().count(), "Single-leg fill must have noLegsCount=0");
  }

  @Test
  void encodeOrderFilledEvent_simpleOverload_roundTripsWithDefaults() {
    final MutableDirectBuffer buf = TestBuffers.event();
    final long seqNum = 6L;
    final long timestamp = 1_713_500_000_000_000_000L;
    final String execId = "EXEC-600";
    final String orderId = "ORD-600";
    final String clOrdId = "CLO-600";
    final String symbol = "EURUSD";
    final SideEnum side = SideEnum.Sell;
    final long lastPx = 108_000_000L;
    final long lastQty = 250_000 * PRICE_SCALE;
    final long leavesQty = 750_000 * PRICE_SCALE;
    final long cumQty = 250_000 * PRICE_SCALE;
    final String accountCode = "ACCT-G";

    SbeTestEncoder.encodeOrderFilledEvent(
        buf,
        0,
        seqNum,
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
        accountCode);

    final OrderFilledEventDecoder dec = SbeTestDecoder.decodeOrderFilled(buf, 0);

    assertEquals(seqNum, dec.sequenceNumber());
    assertEquals(orderId, dec.orderId());
    assertEquals(lastPx, dec.lastPx());
    assertEquals(lastQty, dec.lastQty());
    assertEquals(leavesQty, dec.leavesQty());
    assertEquals(cumQty, dec.cumQty());
    // Verify convenience defaults
    assertEquals(ProductTypeEnum.Spot, dec.productType());
    assertEquals("20260101", dec.settlDate());
    assertEquals(SettlTypeEnum.Regular, dec.settlType());
    assertEquals("USD", dec.currency());
    assertEquals("USD", dec.settlCurrency());
    assertEquals(TenorEnum.SN, dec.tenor());
    assertEquals(0, dec.noLegs().count());
  }

  @Test
  void encodeOrderCanceledEvent_roundTrip_allFieldsMatch() {
    final MutableDirectBuffer buf = TestBuffers.event();
    final long seqNum = 7L;
    final long timestamp = 1_713_600_000_000_000_000L;
    final String orderId = "ORD-700";
    final String clOrdId = "CXL-700";
    final String origClOrdId = "CLO-700";
    final String symbol = "NZDUSD";
    final SideEnum side = SideEnum.Buy;
    final ProductTypeEnum productType = ProductTypeEnum.Spot;

    SbeTestEncoder.encodeOrderCanceledEvent(
        buf, 0, seqNum, timestamp, orderId, clOrdId, origClOrdId, symbol, side, productType);

    final OrderCanceledEventDecoder dec = SbeTestDecoder.decodeOrderCanceled(buf, 0);

    assertEquals(seqNum, dec.sequenceNumber());
    assertEquals(timestamp, dec.timestamp());
    assertEquals(orderId, dec.orderId());
    assertEquals(clOrdId, dec.clOrdId());
    assertEquals(origClOrdId, dec.origClOrdId());
    assertEquals(symbol, dec.symbol());
    assertEquals(side, dec.side());
    assertEquals(productType, dec.productType());
  }

  // -----------------------------------------------------------------------
  // Events — Reference data round-trips
  // -----------------------------------------------------------------------

  @Test
  void encodeAccountLoadedEvent_roundTrip_allFieldsMatch() {
    final MutableDirectBuffer buf = TestBuffers.event();
    final long seqNum = 10L;
    final long timestamp = 1_713_700_000_000_000_000L;
    final long accountId = 101L;
    final String accountCode = "PROP-DESK-1";
    final String accountName = "Proprietary Desk Alpha";
    final String baseCurrency = "EUR";

    SbeTestEncoder.encodeAccountLoadedEvent(
        buf, 0, seqNum, timestamp, accountId, accountCode, accountName, baseCurrency);

    final AccountLoadedEventDecoder dec = SbeTestDecoder.decodeAccountLoaded(buf, 0);

    assertEquals(seqNum, dec.sequenceNumber());
    assertEquals(timestamp, dec.timestamp());
    assertEquals(accountId, dec.accountId());
    assertEquals(0L, dec.parentAccountId());
    assertEquals(accountCode, dec.accountCode());
    assertEquals(AcctIDSourceEnum.Internal, dec.acctIdSource());
    assertEquals(accountName, dec.accountName());
    assertEquals(AccountTypeEnum.Client, dec.accountType());
    assertEquals(baseCurrency, dec.baseCurrency());
    assertEquals(AccountStatusEnum.Active, dec.status());
    assertEquals(ComplianceStatusEnum.OK, dec.complianceStatus());
    assertEquals(AccountRecord.CAN_TRADE | AccountRecord.CAN_RFQ, dec.capabilities());
    assertEquals(0L, dec.transactTime());
  }

  @Test
  void encodeAccountLoadRejectedEvent_roundTrip_allFieldsMatch() {
    final MutableDirectBuffer buf = TestBuffers.event();
    final long seqNum = 11L;
    final long timestamp = 1_713_800_000_000_000_000L;
    final String accountCode = "BAD-ACCT";
    final RejectReasonEnum rejectReason = RejectReasonEnum.DuplicateAccountCode;
    final String text = "Account BAD-ACCT already exists";

    SbeTestEncoder.encodeAccountLoadRejectedEvent(
        buf, 0, seqNum, timestamp, accountCode, rejectReason, text);

    final AccountLoadRejectedEventDecoder dec = SbeTestDecoder.decodeAccountLoadRejected(buf, 0);

    assertEquals(seqNum, dec.sequenceNumber());
    assertEquals(timestamp, dec.timestamp());
    assertEquals(accountCode, dec.accountCode());
    assertEquals(rejectReason, dec.rejectReason());
    assertEquals(text, dec.text());
  }

  @Test
  void encodeCurrencyLoadedEvent_roundTrip_allFieldsMatch() {
    final MutableDirectBuffer buf = TestBuffers.event();
    final long seqNum = 12L;
    final long timestamp = 1_713_900_000_000_000_000L;
    final String ccyCode = "CHF";
    final int isoNumeric = 756;
    final String name = "Swiss Franc";
    final int decimals = 2;
    final CurrencyClassEnum currencyClass = CurrencyClassEnum.Fiat;

    SbeTestEncoder.encodeCurrencyLoadedEvent(
        buf, 0, seqNum, timestamp, ccyCode, isoNumeric, name, decimals, currencyClass);

    final CurrencyLoadedEventDecoder dec = SbeTestDecoder.decodeCurrencyLoaded(buf, 0);

    assertEquals(seqNum, dec.sequenceNumber());
    assertEquals(timestamp, dec.timestamp());
    assertEquals(ccyCode, dec.ccyCode());
    assertEquals(isoNumeric, dec.isoNumeric());
    assertEquals(name, dec.name());
    assertEquals((short) decimals, dec.decimals());
    assertEquals(currencyClass, dec.currencyClass());
    assertEquals(AccountStatusEnum.Active, dec.status());
    assertEquals(0L, dec.transactTime());
  }

  @Test
  void encodeRiskLimitLoadedEvent_roundTrip_allFieldsMatch() {
    final MutableDirectBuffer buf = TestBuffers.event();
    final long seqNum = 13L;
    final long timestamp = 1_714_000_000_000_000_000L;
    final long accountId = 55L;
    final long maxOrderSize = 1_000_000 * PRICE_SCALE;
    final long maxOrderNotional = 50_000_000 * PRICE_SCALE;
    final long maxDailyVolume = 200_000_000 * PRICE_SCALE;
    final long maxDailyLossBps = 250L;

    SbeTestEncoder.encodeRiskLimitLoadedEvent(
        buf,
        0,
        seqNum,
        timestamp,
        accountId,
        maxOrderSize,
        maxOrderNotional,
        maxDailyVolume,
        maxDailyLossBps);

    final RiskLimitLoadedEventDecoder dec = SbeTestDecoder.decodeRiskLimitLoaded(buf, 0);

    assertEquals(seqNum, dec.sequenceNumber());
    assertEquals(timestamp, dec.timestamp());
    assertEquals(accountId, dec.accountId());
    assertEquals(maxOrderSize, dec.maxOrderSize());
    assertEquals(maxOrderNotional, dec.maxOrderNotional());
    assertEquals(maxDailyVolume, dec.maxDailyVolume());
    assertEquals(maxDailyLossBps, dec.maxDailyLossBps());
    assertEquals(AccountStatusEnum.Active, dec.status());
    assertEquals(0L, dec.transactTime());
  }

  @Test
  void encodeRiskLimitLoadRejectedEvent_roundTrip_allFieldsMatch() {
    final MutableDirectBuffer buf = TestBuffers.event();
    final long seqNum = 14L;
    final long timestamp = 1_714_100_000_000_000_000L;
    final long accountId = 77L;
    final RejectReasonEnum rejectReason = RejectReasonEnum.AccountNotFound;
    final String text = "Account 77 does not exist";

    SbeTestEncoder.encodeRiskLimitLoadRejectedEvent(
        buf, 0, seqNum, timestamp, accountId, rejectReason, text);

    final RiskLimitLoadRejectedEventDecoder dec =
        SbeTestDecoder.decodeRiskLimitLoadRejected(buf, 0);

    assertEquals(seqNum, dec.sequenceNumber());
    assertEquals(timestamp, dec.timestamp());
    assertEquals(accountId, dec.accountId());
    assertEquals(rejectReason, dec.rejectReason());
    assertEquals(text, dec.text());
  }

  // -----------------------------------------------------------------------
  // Batch commands
  // -----------------------------------------------------------------------

  @Test
  void encodeLoadAccountBatch_threeRecords_correctGroupCount() {
    final MutableDirectBuffer buf = TestBuffers.batch();
    final long transactTime = 5_000_000_000L;
    final AccountRecord r1 = new AccountRecord(1L, "ACCT-1", "USD");
    final AccountRecord r2 =
        new AccountRecord(
            2L, "ACCT-2", "Account ACCT-2", "EUR", AccountRecord.CAN_TRADE | AccountRecord.CAN_RFQ);
    final AccountRecord r3 = new AccountRecord(3L, "ACCT-3", "GBP");

    SbeTestEncoder.encodeLoadAccountBatch(buf, 0, transactTime, r1, r2, r3);

    SbeMessageAssertions.assertTemplateId(LoadAccountBatchDecoder.TEMPLATE_ID, buf, 0);

    final LoadAccountBatchDecoder dec = new LoadAccountBatchDecoder();
    final MessageHeaderDecoder hdr = new MessageHeaderDecoder();
    dec.wrapAndApplyHeader(buf, 0, hdr);

    assertEquals(transactTime, dec.transactTime());

    final LoadAccountBatchDecoder.NoAccountsDecoder group = dec.noAccounts();
    assertEquals(3, group.count(), "Expected 3 account records in batch");

    // Record 1
    group.next();
    assertEquals(1L, group.accountId());
    assertEquals("ACCT-1", group.accountCode());
    assertEquals("USD", group.baseCurrency());
    assertEquals(AccountRecord.CAN_TRADE, group.capabilities());
    assertEquals(AcctIDSourceEnum.Internal, group.acctIdSource());
    assertEquals(AccountTypeEnum.Client, group.accountType());
    assertEquals(AccountStatusEnum.Active, group.status());
    assertEquals(ComplianceStatusEnum.OK, group.complianceStatus());

    // Record 2
    group.next();
    assertEquals(2L, group.accountId());
    assertEquals("ACCT-2", group.accountCode());
    assertEquals("EUR", group.baseCurrency());
    assertEquals(AccountRecord.CAN_TRADE | AccountRecord.CAN_RFQ, group.capabilities());

    // Record 3
    group.next();
    assertEquals(3L, group.accountId());
    assertEquals("ACCT-3", group.accountCode());
    assertEquals("GBP", group.baseCurrency());
    assertEquals(AccountRecord.CAN_TRADE, group.capabilities());
  }

  // -----------------------------------------------------------------------
  // SbeMessageAssertions — assertTemplateId
  // -----------------------------------------------------------------------

  @Test
  void assertTemplateId_correctId_passes() {
    final MutableDirectBuffer buf = TestBuffers.event();
    SbeTestEncoder.encodeOrderCreatedEvent(
        buf,
        0,
        1L,
        0L,
        "O1",
        "E1",
        "C1",
        "EURUSD",
        SideEnum.Buy,
        OrdTypeEnum.Limit,
        100L,
        100L,
        "ACCT-X");

    // Must not throw
    SbeMessageAssertions.assertTemplateId(OrderCreatedEventDecoder.TEMPLATE_ID, buf, 0);
  }

  // -----------------------------------------------------------------------
  // R2 — previously missing round-trip tests
  // -----------------------------------------------------------------------

  @Test
  void encodeCurrencyLoadRejectedEvent_roundTrip_allFieldsMatch() {
    final MutableDirectBuffer buf = TestBuffers.event();
    SbeTestEncoder.encodeCurrencyLoadRejectedEvent(
        buf, 0, 5L, 99L, "GBP", RejectReasonEnum.DuplicateAccountCode, "dup currency");

    final var dec = SbeTestDecoder.decodeCurrencyLoadRejected(buf, 0);
    assertEquals(5L, dec.sequenceNumber());
    assertEquals(99L, dec.timestamp());
    assertEquals("GBP", dec.ccyCode());
    assertEquals(RejectReasonEnum.DuplicateAccountCode, dec.rejectReason());
    assertEquals("dup currency", dec.text().trim());
  }

  @Test
  void encodeLoadCurrencyBatch_twoRecords_correctGroupCount() {
    final MutableDirectBuffer buf = TestBuffers.batch();
    SbeTestEncoder.encodeLoadCurrencyBatch(
        buf,
        0,
        0L,
        new CurrencyRecord(
            "USD", 840, "US Dollar", 2, CurrencyClassEnum.Fiat, AccountStatusEnum.Active),
        new CurrencyRecord(
            "EUR", 978, "Euro", 2, CurrencyClassEnum.Fiat, AccountStatusEnum.Active));

    final MessageHeaderDecoder hdr = new MessageHeaderDecoder();
    hdr.wrap(buf, 0);
    final var dec = new com.trading.engine.messages.sbe.LoadCurrencyBatchDecoder();
    dec.wrap(buf, MessageHeaderDecoder.ENCODED_LENGTH, hdr.blockLength(), hdr.version());
    final var group = dec.noCurrencies();
    assertEquals(2, group.count());
    group.next();
    assertEquals("USD", group.ccyCode());
  }

  @Test
  void encodeLoadRiskLimitBatch_twoRecords_correctGroupCount() {
    final MutableDirectBuffer buf = TestBuffers.batch();
    SbeTestEncoder.encodeLoadRiskLimitBatch(
        buf,
        0,
        0L,
        new RiskLimitRecord(1L, 10L * PRICE_SCALE, 0L, 0L, 0L),
        new RiskLimitRecord(2L, 20L * PRICE_SCALE, 0L, 0L, 0L));

    final MessageHeaderDecoder hdr = new MessageHeaderDecoder();
    hdr.wrap(buf, 0);
    final var dec = new com.trading.engine.messages.sbe.LoadRiskLimitBatchDecoder();
    dec.wrap(buf, MessageHeaderDecoder.ENCODED_LENGTH, hdr.blockLength(), hdr.version());
    final var group = dec.noRiskLimits();
    assertEquals(2, group.count());
    group.next();
    assertEquals(1L, group.accountId());
    assertEquals(10L * PRICE_SCALE, group.maxOrderSize());
    group.next();
    assertEquals(2L, group.accountId());
    assertEquals(20L * PRICE_SCALE, group.maxOrderSize());
  }

  @Test
  void encodeExecutionReport_roundTrip_allFieldsMatch() {
    final MutableDirectBuffer buf = TestBuffers.event();
    SbeTestEncoder.encodeExecutionReport(
        buf,
        0,
        "ORD-999",
        "EXE-888",
        "CLO-777",
        "",
        ExecTypeEnum.New,
        OrdStatusEnum.New,
        "GBPUSD",
        SideEnum.Sell,
        500L * PRICE_SCALE,
        0L,
        ExecutionReportEncoder.avgPxNullValue(),
        12345L,
        "",
        ProductTypeEnum.Spot,
        "20260415",
        SettlTypeEnum.Regular,
        "GBP",
        "USD",
        TenorEnum.SN);

    final var dec = SbeTestDecoder.decodeExecutionReport(buf, 0);
    assertEquals("ORD-999", dec.orderId().trim());
    assertEquals("EXE-888", dec.execId().trim());
    assertEquals("CLO-777", dec.clOrdId().trim());
    assertEquals("", dec.quoteId().trim());
    assertEquals(ExecTypeEnum.New, dec.execType());
    assertEquals(OrdStatusEnum.New, dec.ordStatus());
    assertEquals("GBPUSD", dec.symbol().trim());
    assertEquals(SideEnum.Sell, dec.side());
    assertEquals(500L * PRICE_SCALE, dec.leavesQty());
    assertEquals(0L, dec.cumQty());
    assertEquals(ExecutionReportEncoder.avgPxNullValue(), dec.avgPx());
    assertEquals(12345L, dec.transactTime());
    assertEquals("", dec.text().trim());
    assertEquals(ProductTypeEnum.Spot, dec.productType());
    assertEquals("20260415", dec.settlDate().trim());
    assertEquals(SettlTypeEnum.Regular, dec.settlType());
    assertEquals("GBP", dec.currency());
    assertEquals("USD", dec.settlCurrency());
    assertEquals(TenorEnum.SN, dec.tenor());
    assertEquals(0, dec.noLegs().count());
  }

  @Test
  void assertTemplateId_wrongId_throwsAssertionError() {
    final MutableDirectBuffer buf = TestBuffers.event();
    SbeTestEncoder.encodeOrderCreatedEvent(
        buf,
        0,
        1L,
        0L,
        "O1",
        "E1",
        "C1",
        "EURUSD",
        SideEnum.Buy,
        OrdTypeEnum.Limit,
        100L,
        100L,
        "ACCT-X");

    // Template ID 100 (OrderCreated) vs. expected 101 (OrderRejected) — must fail
    assertThrows(
        AssertionError.class,
        () -> SbeMessageAssertions.assertTemplateId(OrderRejectedEventDecoder.TEMPLATE_ID, buf, 0));
  }
}
