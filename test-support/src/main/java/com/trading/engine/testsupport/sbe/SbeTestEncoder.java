package com.trading.engine.testsupport.sbe;

import com.trading.engine.messages.sbe.AccountLoadRejectedEventEncoder;
import com.trading.engine.messages.sbe.AccountLoadedEventEncoder;
import com.trading.engine.messages.sbe.AccountStatusEnum;
import com.trading.engine.messages.sbe.AccountTypeEnum;
import com.trading.engine.messages.sbe.AcctIDSourceEnum;
import com.trading.engine.messages.sbe.BooleanType;
import com.trading.engine.messages.sbe.CancelOrderRequestEncoder;
import com.trading.engine.messages.sbe.ClientAckEncoder;
import com.trading.engine.messages.sbe.ClientHeartbeatEncoder;
import com.trading.engine.messages.sbe.CommandAckEncoder;
import com.trading.engine.messages.sbe.CommandAckStatus;
import com.trading.engine.messages.sbe.ComplianceStatusEnum;
import com.trading.engine.messages.sbe.CurrencyClassEnum;
import com.trading.engine.messages.sbe.CurrencyLoadRejectedEventEncoder;
import com.trading.engine.messages.sbe.CurrencyLoadedEventEncoder;
import com.trading.engine.messages.sbe.ExecTypeEnum;
import com.trading.engine.messages.sbe.ExecutionReportEncoder;
import com.trading.engine.messages.sbe.LoadAccountBatchEncoder;
import com.trading.engine.messages.sbe.LoadAccountEncoder;
import com.trading.engine.messages.sbe.LoadCurrencyBatchEncoder;
import com.trading.engine.messages.sbe.LoadCurrencyEncoder;
import com.trading.engine.messages.sbe.LoadRiskLimitBatchEncoder;
import com.trading.engine.messages.sbe.LoadRiskLimitEncoder;
import com.trading.engine.messages.sbe.MessageHeaderEncoder;
import com.trading.engine.messages.sbe.NewOrderSingleEncoder;
import com.trading.engine.messages.sbe.OrdStatusEnum;
import com.trading.engine.messages.sbe.OrdTypeEnum;
import com.trading.engine.messages.sbe.OrderCanceledEventEncoder;
import com.trading.engine.messages.sbe.OrderCreatedEventEncoder;
import com.trading.engine.messages.sbe.OrderFilledEventEncoder;
import com.trading.engine.messages.sbe.OrderRejectedEventEncoder;
import com.trading.engine.messages.sbe.PriceRequestEncoder;
import com.trading.engine.messages.sbe.PriceResponseEncoder;
import com.trading.engine.messages.sbe.PriceValidationRequestEncoder;
import com.trading.engine.messages.sbe.PriceValidationResponseEncoder;
import com.trading.engine.messages.sbe.ProductTypeEnum;
import com.trading.engine.messages.sbe.QuoteCreatedEventEncoder;
import com.trading.engine.messages.sbe.QuoteEncoder;
import com.trading.engine.messages.sbe.QuoteExpiredEventEncoder;
import com.trading.engine.messages.sbe.QuoteRejectReasonEnum;
import com.trading.engine.messages.sbe.QuoteRejectedEventEncoder;
import com.trading.engine.messages.sbe.QuoteRequestEncoder;
import com.trading.engine.messages.sbe.QuoteRequestRejectEncoder;
import com.trading.engine.messages.sbe.QuoteRequestedEventEncoder;
import com.trading.engine.messages.sbe.QuoteStatusEnum;
import com.trading.engine.messages.sbe.RejectReasonEnum;
import com.trading.engine.messages.sbe.ReplayCompleteEncoder;
import com.trading.engine.messages.sbe.RiskLimitLoadRejectedEventEncoder;
import com.trading.engine.messages.sbe.RiskLimitLoadedEventEncoder;
import com.trading.engine.messages.sbe.SessionResumeEncoder;
import com.trading.engine.messages.sbe.SettlTypeEnum;
import com.trading.engine.messages.sbe.SideEnum;
import com.trading.engine.messages.sbe.TenorEnum;
import com.trading.engine.messages.sbe.TimeInForceEnum;
import com.trading.engine.messages.sbe.WebSocketAuthAckEncoder;
import com.trading.engine.messages.sbe.WebSocketAuthEncoder;
import com.trading.engine.messages.sbe.WebSocketErrorCode;
import com.trading.engine.messages.sbe.WebSocketErrorEncoder;
import com.trading.engine.messages.sbe.WebSocketGapRequestEncoder;
import com.trading.engine.messages.sbe.WebSocketHeartbeatEncoder;
import com.trading.engine.messages.sbe.WebSocketSubscribeEncoder;
import com.trading.engine.messages.sbe.WebSocketUnsubscribeEncoder;
import org.agrona.MutableDirectBuffer;

/**
 * Static utility for encoding SBE messages into buffers during tests.
 *
 * <p>Consolidates 48+ instances of duplicated encoding boilerplate spread across 30+ test files
 * into a single, well-documented entry point. Every method creates a local {@link
 * MessageHeaderEncoder} per call to avoid shared mutable state.
 *
 * <p>All methods follow the contract {@code (MutableDirectBuffer dst, int offset, ...)} and return
 * the total encoded length (header + body), suitable for sending to a cluster service or writing to
 * an Aeron publication.
 *
 * <p>Thread-safe -- all methods are stateless static functions. No fields, no shared encoder
 * instances, no static mutable state.
 *
 * <p>This class is test infrastructure only; it allocates encoders on every call.
 */
public final class SbeTestEncoder {

  /** Default capabilities for convenience overloads: CAN_TRADE | CAN_RFQ. */
  private static final long DEFAULT_CAPABILITIES = AccountRecord.CAN_TRADE | AccountRecord.CAN_RFQ;

  /**
   * Default settle date (LocalMktDate, FIX tag 64) used by RFQ/order helper convenience overloads.
   */
  private static final String DEFAULT_SETTL_DATE = "20260101";

  private SbeTestEncoder() {}

  // -----------------------------------------------------------------------
  // Commands
  // -----------------------------------------------------------------------

  /**
   * Encodes a {@link NewOrderSingleEncoder} (template 4, FIX MsgType=D) with sensible FX spot
   * defaults.
   *
   * <p>Defaults applied:
   *
   * <ul>
   *   <li>ProductType (tag 10013) = {@code Spot}
   *   <li>SettlDate (tag 64) = {@code "20260101"}
   *   <li>SettlType (tag 63) = {@code Regular}
   *   <li>Tenor (tag 10001) = {@code SN} (spot-next)
   *   <li>SettlCurrency (tag 120) = same as {@code currency}
   *   <li>QuoteID (tag 117) = {@code ""} (no quote)
   *   <li>TimeInForce (tag 59) = {@code Day}
   *   <li>TransactTime (tag 60) = {@code 0}
   * </ul>
   *
   * @param dst destination buffer
   * @param offset byte offset within {@code dst}
   * @param clOrdId client order ID (ClOrdID, tag 11); max 20 ASCII chars
   * @param symbol instrument symbol (Symbol, tag 55); max 8 ASCII chars
   * @param side order side (Side, tag 54)
   * @param ordType order type (OrdType, tag 40)
   * @param price limit price in fixed-point 10^8 (Price, tag 44); ignored for Market orders
   * @param orderQty order quantity in fixed-point 10^8 (OrderQty, tag 38)
   * @param accountCode account code (Account, tag 1); max 16 ASCII chars
   * @param currency dealt currency ISO code (Currency, tag 15); 3 ASCII chars
   * @return total encoded length including SBE header
   */
  public static int encodeNewOrderSingle(
      final MutableDirectBuffer dst,
      final int offset,
      final String clOrdId,
      final String symbol,
      final SideEnum side,
      final OrdTypeEnum ordType,
      final long price,
      final long orderQty,
      final String accountCode,
      final String currency) {

    return encodeNewOrderSingle(
        dst,
        offset,
        clOrdId,
        symbol,
        side,
        ordType,
        price,
        orderQty,
        accountCode,
        currency,
        ProductTypeEnum.Spot,
        "20260101",
        SettlTypeEnum.Regular,
        currency,
        TenorEnum.SN,
        "",
        TimeInForceEnum.Day,
        0L);
  }

  /**
   * Encodes a {@link NewOrderSingleEncoder} (template 4, FIX MsgType=D) with all FX-specific fields
   * explicitly supplied.
   *
   * @param dst destination buffer
   * @param offset byte offset within {@code dst}
   * @param clOrdId client order ID (ClOrdID, tag 11); max 20 ASCII chars
   * @param symbol instrument symbol (Symbol, tag 55); max 8 ASCII chars
   * @param side order side (Side, tag 54)
   * @param ordType order type (OrdType, tag 40)
   * @param price limit price in fixed-point 10^8 (Price, tag 44)
   * @param orderQty order quantity in fixed-point 10^8 (OrderQty, tag 38)
   * @param accountCode account code (Account, tag 1); max 16 ASCII chars
   * @param currency dealt currency ISO code (Currency, tag 15); 3 ASCII chars
   * @param productType product type (tag 10013)
   * @param settlDate settlement date YYYYMMDD (SettlDate, tag 64); max 8 ASCII chars
   * @param settlType settlement type (SettlType, tag 63)
   * @param settlCurrency settlement currency ISO code (SettlCurrency, tag 120); 3 ASCII chars
   * @param tenor tenor (tag 10001)
   * @param quoteId quote ID if accepting a quote (QuoteID, tag 117); max 20 ASCII chars
   * @param timeInForce time-in-force (TimeInForce, tag 59)
   * @param transactTime transaction time epoch nanos (TransactTime, tag 60)
   * @return total encoded length including SBE header
   */
  public static int encodeNewOrderSingle(
      final MutableDirectBuffer dst,
      final int offset,
      final String clOrdId,
      final String symbol,
      final SideEnum side,
      final OrdTypeEnum ordType,
      final long price,
      final long orderQty,
      final String accountCode,
      final String currency,
      final ProductTypeEnum productType,
      final String settlDate,
      final SettlTypeEnum settlType,
      final String settlCurrency,
      final TenorEnum tenor,
      final String quoteId,
      final TimeInForceEnum timeInForce,
      final long transactTime) {

    final MessageHeaderEncoder header = new MessageHeaderEncoder();
    final NewOrderSingleEncoder enc = new NewOrderSingleEncoder();
    enc.wrapAndApplyHeader(dst, offset, header);

    enc.clOrdId(clOrdId)
        .quoteId(quoteId)
        .symbol(symbol)
        .side(side)
        .ordType(ordType)
        .price(price)
        .orderQty(orderQty)
        .timeInForce(timeInForce)
        .transactTime(transactTime)
        .accountCode(accountCode)
        .productType(productType)
        .settlDate(settlDate)
        .settlType(settlType)
        .currency(currency)
        .settlCurrency(settlCurrency)
        .tenor(tenor);

    return MessageHeaderEncoder.ENCODED_LENGTH + enc.encodedLength();
  }

  /**
   * Encodes a {@link CancelOrderRequestEncoder} (template 6, FIX MsgType=F).
   *
   * @param dst destination buffer
   * @param offset byte offset within {@code dst}
   * @param origClOrdId original client order ID to cancel (OrigClOrdID, tag 41); max 20 ASCII chars
   * @param clOrdId new client order ID for the cancel request (ClOrdID, tag 11); max 20 ASCII chars
   * @param symbol instrument symbol (Symbol, tag 55); max 8 ASCII chars
   * @param side order side (Side, tag 54)
   * @param transactTime transaction time epoch nanos (TransactTime, tag 60)
   * @param accountCode account code (Account, tag 1); max 16 ASCII chars
   * @param productType product type (tag 10013)
   * @return total encoded length including SBE header
   */
  public static int encodeOrderCancelRequest(
      final MutableDirectBuffer dst,
      final int offset,
      final String origClOrdId,
      final String clOrdId,
      final String symbol,
      final SideEnum side,
      final long transactTime,
      final String accountCode,
      final ProductTypeEnum productType) {

    final MessageHeaderEncoder header = new MessageHeaderEncoder();
    final CancelOrderRequestEncoder enc = new CancelOrderRequestEncoder();
    enc.wrapAndApplyHeader(dst, offset, header);

    enc.origClOrdId(origClOrdId)
        .clOrdId(clOrdId)
        .symbol(symbol)
        .side(side)
        .transactTime(transactTime)
        .accountCode(accountCode)
        .productType(productType);

    return MessageHeaderEncoder.ENCODED_LENGTH + enc.encodedLength();
  }

  // -----------------------------------------------------------------------
  // Reference Data — Accounts
  // -----------------------------------------------------------------------

  /**
   * Encodes a {@link LoadAccountEncoder} (template 11) with sensible defaults.
   *
   * <p>Defaults applied:
   *
   * <ul>
   *   <li>parentAccountId = 0 (no parent)
   *   <li>acctIdSource (tag 660) = {@code Internal}
   *   <li>accountType (tag 10029) = {@code Client}
   *   <li>status (tag 10027) = {@code Active}
   *   <li>complianceStatus (tag 10041) = {@code OK}
   *   <li>capabilities (tag 10042) = {@code CAN_TRADE | CAN_RFQ}
   *   <li>transactTime (tag 60) = 0
   * </ul>
   *
   * @param dst destination buffer
   * @param offset byte offset within {@code dst}
   * @param accountId unique account identifier
   * @param accountCode account code (Account, tag 1); max 16 ASCII chars
   * @param accountName display name (tag 10026); max 64 ASCII chars
   * @param baseCurrency base currency ISO code (Currency, tag 15); 3 ASCII chars
   * @return total encoded length including SBE header
   */
  public static int encodeLoadAccount(
      final MutableDirectBuffer dst,
      final int offset,
      final long accountId,
      final String accountCode,
      final String accountName,
      final String baseCurrency) {

    final MessageHeaderEncoder header = new MessageHeaderEncoder();
    final LoadAccountEncoder enc = new LoadAccountEncoder();
    enc.wrapAndApplyHeader(dst, offset, header);

    enc.accountId(accountId)
        .parentAccountId(0L)
        .accountCode(accountCode)
        .acctIdSource(AcctIDSourceEnum.Internal)
        .accountName(accountName)
        .accountType(AccountTypeEnum.Client)
        .baseCurrency(baseCurrency)
        .status(AccountStatusEnum.Active)
        .complianceStatus(ComplianceStatusEnum.OK)
        .capabilities(DEFAULT_CAPABILITIES)
        .transactTime(0L);

    return MessageHeaderEncoder.ENCODED_LENGTH + enc.encodedLength();
  }

  /**
   * Encodes a {@link LoadAccountBatchEncoder} (template 12) from an array of {@link AccountRecord}
   * entries.
   *
   * <p>Per-entry defaults mirroring the single-account convenience overload: parentAccountId=0,
   * acctIdSource=Internal, accountName=code (same as code), accountType=Client, status=Active,
   * complianceStatus=OK. Capabilities are taken from each record's {@link
   * AccountRecord#capabilities()} field.
   *
   * @param dst destination buffer
   * @param offset byte offset within {@code dst}
   * @param transactTime transaction time epoch nanos (TransactTime, tag 60)
   * @param records one or more account records to encode
   * @return total encoded length including SBE header
   */
  public static int encodeLoadAccountBatch(
      final MutableDirectBuffer dst,
      final int offset,
      final long transactTime,
      final AccountRecord... records) {

    final MessageHeaderEncoder header = new MessageHeaderEncoder();
    final LoadAccountBatchEncoder enc = new LoadAccountBatchEncoder();
    enc.wrapAndApplyHeader(dst, offset, header);

    enc.transactTime(transactTime);

    final LoadAccountBatchEncoder.NoAccountsEncoder group = enc.noAccountsCount(records.length);
    for (final AccountRecord r : records) {
      group
          .next()
          .accountId(r.id())
          .parentAccountId(0L)
          .accountCode(r.code())
          .acctIdSource(AcctIDSourceEnum.Internal)
          .accountName(r.name())
          .accountType(AccountTypeEnum.Client)
          .baseCurrency(r.baseCcy())
          .status(AccountStatusEnum.Active)
          .complianceStatus(ComplianceStatusEnum.OK)
          .capabilities(r.capabilities());
    }

    return MessageHeaderEncoder.ENCODED_LENGTH + enc.encodedLength();
  }

  // -----------------------------------------------------------------------
  // Reference Data — Currencies
  // -----------------------------------------------------------------------

  /**
   * Encodes a {@link LoadCurrencyEncoder} (template 13) with transactTime defaulted to 0.
   *
   * @param dst destination buffer
   * @param offset byte offset within {@code dst}
   * @param ccyCode currency ISO code (Currency, tag 15); 3 ASCII chars
   * @param isoNumeric ISO 4217 numeric code (e.g. 840 for USD)
   * @param name currency display name; max 64 ASCII chars
   * @param decimals decimal precision (e.g. 2 for USD)
   * @param currencyClass currency classification
   * @param status currency status
   * @return total encoded length including SBE header
   */
  public static int encodeLoadCurrency(
      final MutableDirectBuffer dst,
      final int offset,
      final String ccyCode,
      final int isoNumeric,
      final String name,
      final int decimals,
      final CurrencyClassEnum currencyClass,
      final AccountStatusEnum status) {

    return encodeLoadCurrency(
        dst, offset, ccyCode, isoNumeric, name, decimals, currencyClass, status, 0L);
  }

  /**
   * Encodes a {@link LoadCurrencyEncoder} (template 13) with explicit transactTime.
   *
   * @param dst destination buffer
   * @param offset byte offset within {@code dst}
   * @param ccyCode currency ISO code (Currency, tag 15); 3 ASCII chars
   * @param isoNumeric ISO 4217 numeric code (e.g. 840 for USD)
   * @param name currency display name; max 64 ASCII chars
   * @param decimals decimal precision (e.g. 2 for USD)
   * @param currencyClass currency classification
   * @param status currency status
   * @param transactTime transaction time epoch nanos (TransactTime, tag 60)
   * @return total encoded length including SBE header
   */
  public static int encodeLoadCurrency(
      final MutableDirectBuffer dst,
      final int offset,
      final String ccyCode,
      final int isoNumeric,
      final String name,
      final int decimals,
      final CurrencyClassEnum currencyClass,
      final AccountStatusEnum status,
      final long transactTime) {

    final MessageHeaderEncoder header = new MessageHeaderEncoder();
    final LoadCurrencyEncoder enc = new LoadCurrencyEncoder();
    enc.wrapAndApplyHeader(dst, offset, header);

    enc.ccyCode(ccyCode)
        .isoNumeric(isoNumeric)
        .name(name)
        .decimals((short) decimals)
        .currencyClass(currencyClass)
        .status(status)
        .transactTime(transactTime);

    return MessageHeaderEncoder.ENCODED_LENGTH + enc.encodedLength();
  }

  /**
   * Encodes a {@link LoadCurrencyBatchEncoder} (template 14) from an array of {@link
   * CurrencyRecord} entries.
   *
   * @param dst destination buffer
   * @param offset byte offset within {@code dst}
   * @param transactTime transaction time epoch nanos (TransactTime, tag 60)
   * @param records one or more currency records to encode
   * @return total encoded length including SBE header
   */
  public static int encodeLoadCurrencyBatch(
      final MutableDirectBuffer dst,
      final int offset,
      final long transactTime,
      final CurrencyRecord... records) {

    final MessageHeaderEncoder header = new MessageHeaderEncoder();
    final LoadCurrencyBatchEncoder enc = new LoadCurrencyBatchEncoder();
    enc.wrapAndApplyHeader(dst, offset, header);

    enc.transactTime(transactTime);

    final LoadCurrencyBatchEncoder.NoCurrenciesEncoder group =
        enc.noCurrenciesCount(records.length);
    for (final CurrencyRecord r : records) {
      group
          .next()
          .ccyCode(r.code())
          .isoNumeric(r.isoNumeric())
          .name(r.name())
          .decimals((short) r.decimals())
          .currencyClass(r.cls())
          .status(r.status());
    }

    return MessageHeaderEncoder.ENCODED_LENGTH + enc.encodedLength();
  }

  // -----------------------------------------------------------------------
  // Reference Data — Risk Limits
  // -----------------------------------------------------------------------

  /**
   * Encodes a {@link LoadRiskLimitEncoder} (template 15).
   *
   * @param dst destination buffer
   * @param offset byte offset within {@code dst}
   * @param accountId target account identifier
   * @param maxOrderSize maximum single-order size in fixed-point 10^8
   * @param maxOrderNotional maximum single-order notional in fixed-point 10^8
   * @param maxDailyVolume maximum daily volume in fixed-point 10^8
   * @param maxDailyLossBps maximum daily loss in basis points
   * @param status risk limit status
   * @param transactTime transaction time epoch nanos (TransactTime, tag 60)
   * @return total encoded length including SBE header
   */
  public static int encodeLoadRiskLimit(
      final MutableDirectBuffer dst,
      final int offset,
      final long accountId,
      final long maxOrderSize,
      final long maxOrderNotional,
      final long maxDailyVolume,
      final long maxDailyLossBps,
      final AccountStatusEnum status,
      final long transactTime) {

    final MessageHeaderEncoder header = new MessageHeaderEncoder();
    final LoadRiskLimitEncoder enc = new LoadRiskLimitEncoder();
    enc.wrapAndApplyHeader(dst, offset, header);

    enc.accountId(accountId)
        .maxOrderSize(maxOrderSize)
        .maxOrderNotional(maxOrderNotional)
        .maxDailyVolume(maxDailyVolume)
        .maxDailyLossBps(maxDailyLossBps)
        .status(status)
        .transactTime(transactTime);

    return MessageHeaderEncoder.ENCODED_LENGTH + enc.encodedLength();
  }

  /**
   * Encodes a {@link LoadRiskLimitBatchEncoder} (template 16) from an array of {@link
   * RiskLimitRecord} entries.
   *
   * <p>Per-entry status defaults to {@link AccountStatusEnum#Active}.
   *
   * @param dst destination buffer
   * @param offset byte offset within {@code dst}
   * @param transactTime transaction time epoch nanos (TransactTime, tag 60)
   * @param records one or more risk limit records to encode
   * @return total encoded length including SBE header
   */
  public static int encodeLoadRiskLimitBatch(
      final MutableDirectBuffer dst,
      final int offset,
      final long transactTime,
      final RiskLimitRecord... records) {

    final MessageHeaderEncoder header = new MessageHeaderEncoder();
    final LoadRiskLimitBatchEncoder enc = new LoadRiskLimitBatchEncoder();
    enc.wrapAndApplyHeader(dst, offset, header);

    enc.transactTime(transactTime);

    final LoadRiskLimitBatchEncoder.NoRiskLimitsEncoder group =
        enc.noRiskLimitsCount(records.length);
    for (final RiskLimitRecord r : records) {
      group
          .next()
          .accountId(r.accountId())
          .maxOrderSize(r.maxOrderSize())
          .maxOrderNotional(r.maxOrderNotional())
          .maxDailyVolume(r.maxDailyVolume())
          .maxDailyLossBps(r.maxDailyLossBps())
          .status(AccountStatusEnum.Active);
    }

    return MessageHeaderEncoder.ENCODED_LENGTH + enc.encodedLength();
  }

  // -----------------------------------------------------------------------
  // Events — Order Lifecycle
  // -----------------------------------------------------------------------

  /**
   * Encodes an {@link OrderCreatedEventEncoder} (template 100) with all FX fields explicitly
   * supplied.
   *
   * @param dst destination buffer
   * @param offset byte offset within {@code dst}
   * @param seqNum event sequence number
   * @param timestamp cluster timestamp epoch nanos
   * @param orderId system-assigned order ID (OrderID, tag 37); max 20 ASCII chars
   * @param execId execution ID (ExecID, tag 17); max 20 ASCII chars
   * @param clOrdId client order ID (ClOrdID, tag 11); max 20 ASCII chars
   * @param symbol instrument symbol (Symbol, tag 55); max 8 ASCII chars
   * @param side order side (Side, tag 54)
   * @param ordType order type (OrdType, tag 40)
   * @param timeInForce time-in-force (TimeInForce, tag 59)
   * @param price limit price in fixed-point 10^8 (Price, tag 44)
   * @param orderQty order quantity in fixed-point 10^8 (OrderQty, tag 38)
   * @param quoteId associated quote ID (QuoteID, tag 117); max 20 ASCII chars
   * @param accountCode account code (Account, tag 1); max 16 ASCII chars
   * @param productType product type (tag 10013)
   * @param settlDate settlement date YYYYMMDD (SettlDate, tag 64); max 8 ASCII chars
   * @param settlType settlement type (SettlType, tag 63)
   * @param currency dealt currency ISO code (Currency, tag 15); 3 ASCII chars
   * @param settlCurrency settlement currency ISO code (SettlCurrency, tag 120); 3 ASCII chars
   * @param tenor tenor (tag 10001)
   * @return total encoded length including SBE header
   */
  public static int encodeOrderCreatedEvent(
      final MutableDirectBuffer dst,
      final int offset,
      final long seqNum,
      final long timestamp,
      final String orderId,
      final String execId,
      final String clOrdId,
      final String symbol,
      final SideEnum side,
      final OrdTypeEnum ordType,
      final TimeInForceEnum timeInForce,
      final long price,
      final long orderQty,
      final String quoteId,
      final String accountCode,
      final ProductTypeEnum productType,
      final String settlDate,
      final SettlTypeEnum settlType,
      final String currency,
      final String settlCurrency,
      final TenorEnum tenor) {

    final MessageHeaderEncoder header = new MessageHeaderEncoder();
    final OrderCreatedEventEncoder enc = new OrderCreatedEventEncoder();
    enc.wrapAndApplyHeader(dst, offset, header);

    enc.sequenceNumber(seqNum)
        .timestamp(timestamp)
        .orderId(orderId)
        .execId(execId)
        .clOrdId(clOrdId)
        .symbol(symbol)
        .side(side)
        .ordType(ordType)
        .timeInForce(timeInForce)
        .price(price)
        .orderQty(orderQty)
        .quoteId(quoteId)
        .accountCode(accountCode)
        .productType(productType)
        .settlDate(settlDate)
        .settlType(settlType)
        .currency(currency)
        .settlCurrency(settlCurrency)
        .tenor(tenor);

    return MessageHeaderEncoder.ENCODED_LENGTH + enc.encodedLength();
  }

  /**
   * Encodes an {@link OrderCreatedEventEncoder} (template 100) with FX spot defaults.
   *
   * <p>Defaults: productType=Spot, settlDate="20260101", settlType=Regular, currency="USD",
   * settlCurrency="USD", tenor=SN, quoteId="", timeInForce=Day.
   *
   * @param dst destination buffer
   * @param offset byte offset within {@code dst}
   * @param seqNum event sequence number
   * @param timestamp cluster timestamp epoch nanos
   * @param orderId system-assigned order ID (OrderID, tag 37); max 20 ASCII chars
   * @param execId execution ID (ExecID, tag 17); max 20 ASCII chars
   * @param clOrdId client order ID (ClOrdID, tag 11); max 20 ASCII chars
   * @param symbol instrument symbol (Symbol, tag 55); max 8 ASCII chars
   * @param side order side (Side, tag 54)
   * @param ordType order type (OrdType, tag 40)
   * @param price limit price in fixed-point 10^8 (Price, tag 44)
   * @param orderQty order quantity in fixed-point 10^8 (OrderQty, tag 38)
   * @param accountCode account code (Account, tag 1); max 16 ASCII chars
   * @return total encoded length including SBE header
   */
  public static int encodeOrderCreatedEvent(
      final MutableDirectBuffer dst,
      final int offset,
      final long seqNum,
      final long timestamp,
      final String orderId,
      final String execId,
      final String clOrdId,
      final String symbol,
      final SideEnum side,
      final OrdTypeEnum ordType,
      final long price,
      final long orderQty,
      final String accountCode) {

    return encodeOrderCreatedEvent(
        dst,
        offset,
        seqNum,
        timestamp,
        orderId,
        execId,
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
        "20260101",
        SettlTypeEnum.Regular,
        "USD",
        "USD",
        TenorEnum.SN);
  }

  /**
   * Encodes an {@link OrderRejectedEventEncoder} (template 101) with all fields explicitly
   * supplied.
   *
   * @param dst destination buffer
   * @param offset byte offset within {@code dst}
   * @param seqNum event sequence number
   * @param timestamp cluster timestamp epoch nanos
   * @param clOrdId client order ID (ClOrdID, tag 11); max 20 ASCII chars
   * @param rejectReason rejection reason (tag 380)
   * @param text free-text rejection description (Text, tag 58); max 64 ASCII chars
   * @param symbol instrument symbol (Symbol, tag 55); max 8 ASCII chars
   * @param side order side (Side, tag 54)
   * @param accountCode account code (Account, tag 1); max 16 ASCII chars
   * @param productType product type (tag 10013)
   * @param currency dealt currency ISO code (Currency, tag 15); 3 ASCII chars
   * @return total encoded length including SBE header
   */
  public static int encodeOrderRejectedEvent(
      final MutableDirectBuffer dst,
      final int offset,
      final long seqNum,
      final long timestamp,
      final String clOrdId,
      final RejectReasonEnum rejectReason,
      final String text,
      final String symbol,
      final SideEnum side,
      final String accountCode,
      final ProductTypeEnum productType,
      final String currency) {

    final MessageHeaderEncoder header = new MessageHeaderEncoder();
    final OrderRejectedEventEncoder enc = new OrderRejectedEventEncoder();
    enc.wrapAndApplyHeader(dst, offset, header);

    enc.sequenceNumber(seqNum)
        .timestamp(timestamp)
        .clOrdId(clOrdId)
        .symbol(symbol)
        .side(side)
        .rejectReason(rejectReason)
        .accountCode(accountCode)
        .productType(productType)
        .currency(currency)
        .text(text);

    return MessageHeaderEncoder.ENCODED_LENGTH + enc.encodedLength();
  }

  /**
   * Encodes an {@link OrderRejectedEventEncoder} (template 101) with minimal parameters and
   * placeholder defaults.
   *
   * <p>Defaults: symbol="", side=Buy, accountCode="", productType=Spot, currency="".
   *
   * @param dst destination buffer
   * @param offset byte offset within {@code dst}
   * @param seqNum event sequence number
   * @param timestamp cluster timestamp epoch nanos
   * @param clOrdId client order ID (ClOrdID, tag 11); max 20 ASCII chars
   * @param rejectReason rejection reason (tag 380)
   * @param text free-text rejection description (Text, tag 58); max 64 ASCII chars
   * @return total encoded length including SBE header
   */
  public static int encodeOrderRejectedEvent(
      final MutableDirectBuffer dst,
      final int offset,
      final long seqNum,
      final long timestamp,
      final String clOrdId,
      final RejectReasonEnum rejectReason,
      final String text) {

    return encodeOrderRejectedEvent(
        dst,
        offset,
        seqNum,
        timestamp,
        clOrdId,
        rejectReason,
        text,
        "",
        SideEnum.Buy,
        "",
        ProductTypeEnum.Spot,
        "");
  }

  /**
   * Encodes an {@link OrderFilledEventEncoder} (template 102) with all FX fields explicitly
   * supplied. Calls {@code enc.noLegsCount(0)} for single-leg fills.
   *
   * @param dst destination buffer
   * @param offset byte offset within {@code dst}
   * @param seqNum event sequence number
   * @param timestamp cluster timestamp epoch nanos
   * @param execId execution ID (ExecID, tag 17); max 20 ASCII chars
   * @param orderId system-assigned order ID (OrderID, tag 37); max 20 ASCII chars
   * @param clOrdId client order ID (ClOrdID, tag 11); max 20 ASCII chars
   * @param symbol instrument symbol (Symbol, tag 55); max 8 ASCII chars
   * @param side order side (Side, tag 54)
   * @param lastPx fill price in fixed-point 10^8 (LastPx, tag 31)
   * @param lastQty fill quantity in fixed-point 10^8 (LastQty, tag 32)
   * @param leavesQty remaining quantity in fixed-point 10^8 (LeavesQty, tag 151)
   * @param cumQty cumulative filled quantity in fixed-point 10^8 (CumQty, tag 14)
   * @param accountCode account code (Account, tag 1); max 16 ASCII chars
   * @param productType product type (tag 10013)
   * @param settlDate settlement date YYYYMMDD (SettlDate, tag 64); max 8 ASCII chars
   * @param settlType settlement type (SettlType, tag 63)
   * @param currency dealt currency ISO code (Currency, tag 15); 3 ASCII chars
   * @param settlCurrency settlement currency ISO code (SettlCurrency, tag 120); 3 ASCII chars
   * @param tenor tenor (tag 10001)
   * @return total encoded length including SBE header
   */
  public static int encodeOrderFilledEvent(
      final MutableDirectBuffer dst,
      final int offset,
      final long seqNum,
      final long timestamp,
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
      final ProductTypeEnum productType,
      final String settlDate,
      final SettlTypeEnum settlType,
      final String currency,
      final String settlCurrency,
      final TenorEnum tenor) {

    final MessageHeaderEncoder header = new MessageHeaderEncoder();
    final OrderFilledEventEncoder enc = new OrderFilledEventEncoder();
    enc.wrapAndApplyHeader(dst, offset, header);

    enc.sequenceNumber(seqNum)
        .timestamp(timestamp)
        .execId(execId)
        .orderId(orderId)
        .clOrdId(clOrdId)
        .symbol(symbol)
        .side(side)
        .lastPx(lastPx)
        .lastQty(lastQty)
        .leavesQty(leavesQty)
        .cumQty(cumQty)
        .productType(productType)
        .settlDate(settlDate)
        .settlType(settlType)
        .currency(currency)
        .settlCurrency(settlCurrency)
        .tenor(tenor)
        .accountCode(accountCode);

    // Single-leg fill — must still write the repeating-group header with count=0
    enc.noLegsCount(0);

    return MessageHeaderEncoder.ENCODED_LENGTH + enc.encodedLength();
  }

  /**
   * Encodes an {@link OrderFilledEventEncoder} (template 102) with FX spot defaults. Calls {@code
   * enc.noLegsCount(0)} for single-leg fills.
   *
   * <p>Defaults: productType=Spot, settlDate="20260101", settlType=Regular, currency="USD",
   * settlCurrency="USD", tenor=SN.
   *
   * @param dst destination buffer
   * @param offset byte offset within {@code dst}
   * @param seqNum event sequence number
   * @param timestamp cluster timestamp epoch nanos
   * @param execId execution ID (ExecID, tag 17); max 20 ASCII chars
   * @param orderId system-assigned order ID (OrderID, tag 37); max 20 ASCII chars
   * @param clOrdId client order ID (ClOrdID, tag 11); max 20 ASCII chars
   * @param symbol instrument symbol (Symbol, tag 55); max 8 ASCII chars
   * @param side order side (Side, tag 54)
   * @param lastPx fill price in fixed-point 10^8 (LastPx, tag 31)
   * @param lastQty fill quantity in fixed-point 10^8 (LastQty, tag 32)
   * @param leavesQty remaining quantity in fixed-point 10^8 (LeavesQty, tag 151)
   * @param cumQty cumulative filled quantity in fixed-point 10^8 (CumQty, tag 14)
   * @param accountCode account code (Account, tag 1); max 16 ASCII chars
   * @return total encoded length including SBE header
   */
  public static int encodeOrderFilledEvent(
      final MutableDirectBuffer dst,
      final int offset,
      final long seqNum,
      final long timestamp,
      final String execId,
      final String orderId,
      final String clOrdId,
      final String symbol,
      final SideEnum side,
      final long lastPx,
      final long lastQty,
      final long leavesQty,
      final long cumQty,
      final String accountCode) {

    return encodeOrderFilledEvent(
        dst,
        offset,
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
        ProductTypeEnum.Spot,
        "20260101",
        SettlTypeEnum.Regular,
        "USD",
        "USD",
        TenorEnum.SN);
  }

  /**
   * Encodes an {@link OrderCanceledEventEncoder} (template 103).
   *
   * <p>Note: this event does not carry leavesQty or cumQty fields.
   *
   * @param dst destination buffer
   * @param offset byte offset within {@code dst}
   * @param seqNum event sequence number
   * @param timestamp cluster timestamp epoch nanos
   * @param orderId system-assigned order ID (OrderID, tag 37); max 20 ASCII chars
   * @param clOrdId client order ID (ClOrdID, tag 11); max 20 ASCII chars
   * @param origClOrdId original client order ID (OrigClOrdID, tag 41); max 20 ASCII chars
   * @param symbol instrument symbol (Symbol, tag 55); max 8 ASCII chars
   * @param side order side (Side, tag 54)
   * @param productType product type (tag 10013)
   * @return total encoded length including SBE header
   */
  public static int encodeOrderCanceledEvent(
      final MutableDirectBuffer dst,
      final int offset,
      final long seqNum,
      final long timestamp,
      final String orderId,
      final String clOrdId,
      final String origClOrdId,
      final String symbol,
      final SideEnum side,
      final ProductTypeEnum productType) {

    final MessageHeaderEncoder header = new MessageHeaderEncoder();
    final OrderCanceledEventEncoder enc = new OrderCanceledEventEncoder();
    enc.wrapAndApplyHeader(dst, offset, header);

    enc.sequenceNumber(seqNum)
        .timestamp(timestamp)
        .orderId(orderId)
        .clOrdId(clOrdId)
        .origClOrdId(origClOrdId)
        .symbol(symbol)
        .side(side)
        .productType(productType);

    return MessageHeaderEncoder.ENCODED_LENGTH + enc.encodedLength();
  }

  // -----------------------------------------------------------------------
  // Events — Account
  // -----------------------------------------------------------------------

  /**
   * Encodes an {@link AccountLoadedEventEncoder} (template 110) with convenience defaults.
   *
   * <p>Defaults: parentAccountId=0, acctIdSource=Internal, accountType=Client, status=Active,
   * complianceStatus=OK, capabilities=CAN_TRADE|CAN_RFQ, transactTime=0.
   *
   * @param dst destination buffer
   * @param offset byte offset within {@code dst}
   * @param seqNum event sequence number
   * @param timestamp cluster timestamp epoch nanos
   * @param accountId unique account identifier
   * @param accountCode account code (Account, tag 1); max 16 ASCII chars
   * @param accountName display name (tag 10026); max 64 ASCII chars
   * @param baseCurrency base currency ISO code (Currency, tag 15); 3 ASCII chars
   * @return total encoded length including SBE header
   */
  public static int encodeAccountLoadedEvent(
      final MutableDirectBuffer dst,
      final int offset,
      final long seqNum,
      final long timestamp,
      final long accountId,
      final String accountCode,
      final String accountName,
      final String baseCurrency) {

    final MessageHeaderEncoder header = new MessageHeaderEncoder();
    final AccountLoadedEventEncoder enc = new AccountLoadedEventEncoder();
    enc.wrapAndApplyHeader(dst, offset, header);

    enc.sequenceNumber(seqNum)
        .timestamp(timestamp)
        .accountId(accountId)
        .parentAccountId(0L)
        .accountCode(accountCode)
        .acctIdSource(AcctIDSourceEnum.Internal)
        .accountName(accountName)
        .accountType(AccountTypeEnum.Client)
        .baseCurrency(baseCurrency)
        .status(AccountStatusEnum.Active)
        .complianceStatus(ComplianceStatusEnum.OK)
        .capabilities(DEFAULT_CAPABILITIES)
        .transactTime(0L);

    return MessageHeaderEncoder.ENCODED_LENGTH + enc.encodedLength();
  }

  /**
   * Encodes an {@link AccountLoadRejectedEventEncoder} (template 111).
   *
   * @param dst destination buffer
   * @param offset byte offset within {@code dst}
   * @param seqNum event sequence number
   * @param timestamp cluster timestamp epoch nanos
   * @param accountCode account code string (Account, tag 1); max 16 ASCII chars
   * @param rejectReason rejection reason (tag 380)
   * @param text free-text description (Text, tag 58); max 64 ASCII chars
   * @return total encoded length including SBE header
   */
  public static int encodeAccountLoadRejectedEvent(
      final MutableDirectBuffer dst,
      final int offset,
      final long seqNum,
      final long timestamp,
      final String accountCode,
      final RejectReasonEnum rejectReason,
      final String text) {

    final MessageHeaderEncoder header = new MessageHeaderEncoder();
    final AccountLoadRejectedEventEncoder enc = new AccountLoadRejectedEventEncoder();
    enc.wrapAndApplyHeader(dst, offset, header);

    enc.sequenceNumber(seqNum)
        .timestamp(timestamp)
        .accountCode(accountCode)
        .rejectReason(rejectReason)
        .text(text);

    return MessageHeaderEncoder.ENCODED_LENGTH + enc.encodedLength();
  }

  // -----------------------------------------------------------------------
  // Events — Currency
  // -----------------------------------------------------------------------

  /**
   * Encodes a {@link CurrencyLoadedEventEncoder} (template 113) with convenience defaults.
   *
   * <p>Defaults: status=Active, transactTime=0.
   *
   * @param dst destination buffer
   * @param offset byte offset within {@code dst}
   * @param seqNum event sequence number
   * @param timestamp cluster timestamp epoch nanos
   * @param ccyCode currency ISO code (Currency, tag 15); 3 ASCII chars
   * @param isoNumeric ISO 4217 numeric code (e.g. 840 for USD)
   * @param name currency display name; max 64 ASCII chars
   * @param decimals decimal precision (e.g. 2 for USD)
   * @param currencyClass currency classification
   * @return total encoded length including SBE header
   */
  public static int encodeCurrencyLoadedEvent(
      final MutableDirectBuffer dst,
      final int offset,
      final long seqNum,
      final long timestamp,
      final String ccyCode,
      final int isoNumeric,
      final String name,
      final int decimals,
      final CurrencyClassEnum currencyClass) {

    final MessageHeaderEncoder header = new MessageHeaderEncoder();
    final CurrencyLoadedEventEncoder enc = new CurrencyLoadedEventEncoder();
    enc.wrapAndApplyHeader(dst, offset, header);

    enc.sequenceNumber(seqNum)
        .timestamp(timestamp)
        .ccyCode(ccyCode)
        .isoNumeric(isoNumeric)
        .name(name)
        .decimals((short) decimals)
        .currencyClass(currencyClass)
        .status(AccountStatusEnum.Active)
        .transactTime(0L);

    return MessageHeaderEncoder.ENCODED_LENGTH + enc.encodedLength();
  }

  /**
   * Encodes a {@link CurrencyLoadRejectedEventEncoder} (template 114).
   *
   * @param dst destination buffer
   * @param offset byte offset within {@code dst}
   * @param seqNum event sequence number
   * @param timestamp cluster timestamp epoch nanos
   * @param ccyCode currency ISO code (Currency, tag 15); 3 ASCII chars
   * @param rejectReason rejection reason (tag 380)
   * @param text free-text description (Text, tag 58); max 64 ASCII chars
   * @return total encoded length including SBE header
   */
  public static int encodeCurrencyLoadRejectedEvent(
      final MutableDirectBuffer dst,
      final int offset,
      final long seqNum,
      final long timestamp,
      final String ccyCode,
      final RejectReasonEnum rejectReason,
      final String text) {

    final MessageHeaderEncoder header = new MessageHeaderEncoder();
    final CurrencyLoadRejectedEventEncoder enc = new CurrencyLoadRejectedEventEncoder();
    enc.wrapAndApplyHeader(dst, offset, header);

    enc.sequenceNumber(seqNum)
        .timestamp(timestamp)
        .ccyCode(ccyCode)
        .rejectReason(rejectReason)
        .text(text);

    return MessageHeaderEncoder.ENCODED_LENGTH + enc.encodedLength();
  }

  // -----------------------------------------------------------------------
  // Events — Risk Limits
  // -----------------------------------------------------------------------

  /**
   * Encodes a {@link RiskLimitLoadedEventEncoder} (template 115) with convenience defaults.
   *
   * <p>Defaults: status=Active, transactTime=0.
   *
   * @param dst destination buffer
   * @param offset byte offset within {@code dst}
   * @param seqNum event sequence number
   * @param timestamp cluster timestamp epoch nanos
   * @param accountId target account identifier
   * @param maxOrderSize maximum single-order size in fixed-point 10^8
   * @param maxOrderNotional maximum single-order notional in fixed-point 10^8
   * @param maxDailyVolume maximum daily volume in fixed-point 10^8
   * @param maxDailyLossBps maximum daily loss in basis points
   * @return total encoded length including SBE header
   */
  public static int encodeRiskLimitLoadedEvent(
      final MutableDirectBuffer dst,
      final int offset,
      final long seqNum,
      final long timestamp,
      final long accountId,
      final long maxOrderSize,
      final long maxOrderNotional,
      final long maxDailyVolume,
      final long maxDailyLossBps) {

    final MessageHeaderEncoder header = new MessageHeaderEncoder();
    final RiskLimitLoadedEventEncoder enc = new RiskLimitLoadedEventEncoder();
    enc.wrapAndApplyHeader(dst, offset, header);

    enc.sequenceNumber(seqNum)
        .timestamp(timestamp)
        .accountId(accountId)
        .maxOrderSize(maxOrderSize)
        .maxOrderNotional(maxOrderNotional)
        .maxDailyVolume(maxDailyVolume)
        .maxDailyLossBps(maxDailyLossBps)
        .status(AccountStatusEnum.Active)
        .transactTime(0L);

    return MessageHeaderEncoder.ENCODED_LENGTH + enc.encodedLength();
  }

  /**
   * Encodes a {@link RiskLimitLoadRejectedEventEncoder} (template 116).
   *
   * @param dst destination buffer
   * @param offset byte offset within {@code dst}
   * @param seqNum event sequence number
   * @param timestamp cluster timestamp epoch nanos
   * @param accountId target account identifier
   * @param rejectReason rejection reason (tag 380)
   * @param text free-text description (Text, tag 58); max 64 ASCII chars
   * @return total encoded length including SBE header
   */
  public static int encodeRiskLimitLoadRejectedEvent(
      final MutableDirectBuffer dst,
      final int offset,
      final long seqNum,
      final long timestamp,
      final long accountId,
      final RejectReasonEnum rejectReason,
      final String text) {

    final MessageHeaderEncoder header = new MessageHeaderEncoder();
    final RiskLimitLoadRejectedEventEncoder enc = new RiskLimitLoadRejectedEventEncoder();
    enc.wrapAndApplyHeader(dst, offset, header);

    enc.sequenceNumber(seqNum)
        .timestamp(timestamp)
        .accountId(accountId)
        .rejectReason(rejectReason)
        .text(text);

    return MessageHeaderEncoder.ENCODED_LENGTH + enc.encodedLength();
  }

  // -----------------------------------------------------------------------
  // Gateway — Execution Report
  // -----------------------------------------------------------------------

  /**
   * Encodes an {@link ExecutionReportEncoder} (template 5, FIX MsgType=8) with all fields. Calls
   * {@code enc.noLegsCount(0)} for single-leg reports.
   *
   * @param dst destination buffer
   * @param offset byte offset within {@code dst}
   * @param orderId system-assigned order ID (OrderID, tag 37); max 20 ASCII chars
   * @param execId execution ID (ExecID, tag 17); max 20 ASCII chars
   * @param clOrdId client order ID (ClOrdID, tag 11); max 20 ASCII chars
   * @param quoteId associated quote ID (QuoteID, tag 117); max 20 ASCII chars
   * @param execType execution type (ExecType, tag 150)
   * @param ordStatus order status (OrdStatus, tag 39)
   * @param symbol instrument symbol (Symbol, tag 55); max 8 ASCII chars
   * @param side order side (Side, tag 54)
   * @param leavesQty remaining quantity in fixed-point 10^8 (LeavesQty, tag 151)
   * @param cumQty cumulative filled quantity in fixed-point 10^8 (CumQty, tag 14)
   * @param avgPx average fill price in fixed-point 10^8 (AvgPx, tag 6); optional
   * @param transactTime transaction time epoch nanos (TransactTime, tag 60)
   * @param text free-text field (Text, tag 58); max 64 ASCII chars
   * @param productType product type (tag 10013)
   * @param settlDate settlement date YYYYMMDD (SettlDate, tag 64); max 8 ASCII chars
   * @param settlType settlement type (SettlType, tag 63)
   * @param currency dealt currency ISO code (Currency, tag 15); 3 ASCII chars
   * @param settlCurrency settlement currency ISO code (SettlCurrency, tag 120); 3 ASCII chars
   * @param tenor tenor (tag 10001)
   * @return total encoded length including SBE header
   */
  public static int encodeExecutionReport(
      final MutableDirectBuffer dst,
      final int offset,
      final String orderId,
      final String execId,
      final String clOrdId,
      final String quoteId,
      final ExecTypeEnum execType,
      final OrdStatusEnum ordStatus,
      final String symbol,
      final SideEnum side,
      final long leavesQty,
      final long cumQty,
      final long avgPx,
      final long transactTime,
      final String text,
      final ProductTypeEnum productType,
      final String settlDate,
      final SettlTypeEnum settlType,
      final String currency,
      final String settlCurrency,
      final TenorEnum tenor) {

    final MessageHeaderEncoder header = new MessageHeaderEncoder();
    final ExecutionReportEncoder enc = new ExecutionReportEncoder();
    enc.wrapAndApplyHeader(dst, offset, header);

    enc.orderId(orderId)
        .execId(execId)
        .clOrdId(clOrdId)
        .quoteId(quoteId)
        .execType(execType)
        .ordStatus(ordStatus)
        .symbol(symbol)
        .side(side)
        .leavesQty(leavesQty)
        .cumQty(cumQty)
        .avgPx(avgPx)
        .transactTime(transactTime)
        .text(text)
        .productType(productType)
        .settlDate(settlDate)
        .settlType(settlType)
        .currency(currency)
        .settlCurrency(settlCurrency)
        .tenor(tenor);

    // Single-leg report — must still write the repeating-group header with count=0
    enc.noLegsCount(0);

    return MessageHeaderEncoder.ENCODED_LENGTH + enc.encodedLength();
  }

  // -----------------------------------------------------------------------
  // RFQ / Orchestrator messages
  // -----------------------------------------------------------------------

  /**
   * Encodes a {@link QuoteRequestEncoder} (template 1, FIX MsgType=R) with sensible defaults.
   *
   * <p><b>Defaults baked in:</b> {@code productType=Spot}, {@code settlDate="20260101"}, {@code
   * settlType=Regular}, {@code tenor=SN}, {@code currency="USD"}, {@code settlCurrency="EUR"},
   * {@code transactTime=0}, {@code noLegsCount=0}. Tests that need different values should encode
   * via the SBE encoder directly rather than this helper.
   *
   * @param dst destination buffer
   * @param offset byte offset within {@code dst}
   * @param quoteReqId QuoteReqID (tag 131); max 20 ASCII chars
   * @param symbol instrument symbol (tag 55); max 8 ASCII chars
   * @param side order side (tag 54)
   * @param orderQty quantity in fixed-point 10^8 (tag 38)
   * @param accountCode account code (tag 1); max 16 ASCII chars
   * @return total encoded length including SBE header
   */
  public static int encodeQuoteRequest(
      final MutableDirectBuffer dst,
      final int offset,
      final String quoteReqId,
      final String symbol,
      final SideEnum side,
      final long orderQty,
      final String accountCode) {
    return encodeQuoteRequest(dst, offset, quoteReqId, symbol, side, orderQty, accountCode, 0L);
  }

  /**
   * Encodes a {@link QuoteRequestEncoder} (template 1, FIX MsgType=R) with sensible defaults and an
   * explicit transactTime. Use this overload for tests that exercise time-sensitive logic or audit
   * trail assertions.
   *
   * <p><b>Defaults baked in:</b> {@code productType=Spot}, {@code settlDate=DEFAULT_SETTL_DATE},
   * {@code settlType=Regular}, {@code tenor=SN}, {@code currency="USD"}, {@code
   * settlCurrency="EUR"}, {@code noLegsCount=0}. Tests that need different values should encode via
   * the SBE encoder directly rather than this helper.
   *
   * @param dst destination buffer
   * @param offset byte offset within {@code dst}
   * @param quoteReqId QuoteReqID (tag 131); max 20 ASCII chars
   * @param symbol instrument symbol (tag 55); max 8 ASCII chars
   * @param side order side (tag 54)
   * @param orderQty quantity in fixed-point 10^8 (tag 38)
   * @param accountCode account code (tag 1); max 16 ASCII chars
   * @param transactTime epoch nanos (tag 60); pass {@code 0L} if not asserted
   * @return total encoded length including SBE header
   */
  public static int encodeQuoteRequest(
      final MutableDirectBuffer dst,
      final int offset,
      final String quoteReqId,
      final String symbol,
      final SideEnum side,
      final long orderQty,
      final String accountCode,
      final long transactTime) {

    final var header = new MessageHeaderEncoder();
    final var enc = new QuoteRequestEncoder();
    enc.wrapAndApplyHeader(dst, offset, header);

    enc.quoteReqId(quoteReqId);
    enc.symbol(symbol);
    enc.side(side);
    enc.orderQty(orderQty);
    enc.accountCode(accountCode);
    enc.productType(ProductTypeEnum.Spot);
    enc.settlDate(DEFAULT_SETTL_DATE);
    enc.settlType(SettlTypeEnum.Regular);
    enc.tenor(TenorEnum.SN);
    enc.currency("USD");
    enc.settlCurrency("EUR");
    enc.transactTime(transactTime);
    enc.noLegsCount(0);

    return MessageHeaderEncoder.ENCODED_LENGTH + enc.encodedLength();
  }

  /**
   * Encodes a {@link QuoteEncoder} (template 2, FIX MsgType=S) with sensible defaults.
   *
   * @param dst destination buffer
   * @param offset byte offset within {@code dst}
   * @param quoteReqId QuoteReqID (tag 131)
   * @param quoteId QuoteID (tag 117)
   * @param symbol instrument symbol (tag 55)
   * @param side order side (tag 54)
   * @param bidPx bid price in fixed-point 10^8 (tag 132)
   * @param offerPx offer price in fixed-point 10^8 (tag 133)
   * @param transactTime epoch nanos (tag 60)
   * @return total encoded length including SBE header
   */
  public static int encodeQuote(
      final MutableDirectBuffer dst,
      final int offset,
      final String quoteReqId,
      final String quoteId,
      final String symbol,
      final SideEnum side,
      final long bidPx,
      final long offerPx,
      final long transactTime) {

    final var header = new MessageHeaderEncoder();
    final var enc = new QuoteEncoder();
    enc.wrapAndApplyHeader(dst, offset, header);

    enc.quoteReqId(quoteReqId);
    enc.quoteId(quoteId);
    enc.symbol(symbol);
    enc.side(side);
    enc.bidPx(bidPx);
    enc.offerPx(offerPx);
    enc.bidSize(100_000_000L);
    enc.offerSize(100_000_000L);
    enc.quoteStatus(QuoteStatusEnum.Accepted);
    enc.text("");
    enc.transactTime(transactTime);
    enc.validUntil(transactTime + 30_000_000_000L);
    enc.productType(ProductTypeEnum.Spot);
    enc.settlDate(DEFAULT_SETTL_DATE);
    enc.settlType(SettlTypeEnum.Regular);
    enc.tenor(TenorEnum.SN);
    enc.currency("USD");
    enc.settlCurrency("EUR");
    enc.swapPoints(QuoteEncoder.swapPointsNullValue());
    enc.noLegsCount(0);

    return MessageHeaderEncoder.ENCODED_LENGTH + enc.encodedLength();
  }

  /**
   * Encodes a {@link QuoteRequestRejectEncoder} (template 3, FIX MsgType=AG).
   *
   * @param dst destination buffer
   * @param offset byte offset within {@code dst}
   * @param quoteReqId QuoteReqID (tag 131)
   * @param reason reject reason (tag 658)
   * @param symbol instrument symbol (tag 55)
   * @param side order side (tag 54)
   * @param text free-text reject reason (tag 58)
   * @param transactTime epoch nanos (tag 60)
   * @return total encoded length including SBE header
   */
  public static int encodeQuoteRequestReject(
      final MutableDirectBuffer dst,
      final int offset,
      final String quoteReqId,
      final QuoteRejectReasonEnum reason,
      final String symbol,
      final SideEnum side,
      final String text,
      final long transactTime) {

    final var header = new MessageHeaderEncoder();
    final var enc = new QuoteRequestRejectEncoder();
    enc.wrapAndApplyHeader(dst, offset, header);

    enc.quoteReqId(quoteReqId);
    enc.quoteRejectReason(reason);
    enc.symbol(symbol);
    enc.side(side);
    enc.transactTime(transactTime);
    enc.text(text);
    enc.productType(ProductTypeEnum.Spot);

    return MessageHeaderEncoder.ENCODED_LENGTH + enc.encodedLength();
  }

  /**
   * Encodes a {@link PriceRequestEncoder} (template 50) — orchestrator → pricing service.
   *
   * <p><b>Defaults baked in:</b> {@code accountCode="ACCT001"}, {@code transactTime=0}, {@code
   * productType=Spot}, {@code settlDate=DEFAULT_SETTL_DATE}, {@code settlType=Regular}, {@code
   * tenor=SN}, {@code currency="USD"}, {@code settlCurrency="EUR"}, {@code noLegsCount=0}. Use the
   * 7-arg overload to specify a non-zero transactTime.
   *
   * @param dst destination buffer
   * @param offset byte offset within {@code dst}
   * @param quoteReqId QuoteReqID (tag 131)
   * @param symbol instrument symbol (tag 55)
   * @param side order side (tag 54)
   * @param orderQty quantity in fixed-point 10^8 (tag 38)
   * @return total encoded length including SBE header
   */
  public static int encodePriceRequest(
      final MutableDirectBuffer dst,
      final int offset,
      final String quoteReqId,
      final String symbol,
      final SideEnum side,
      final long orderQty) {
    return encodePriceRequest(dst, offset, quoteReqId, symbol, side, orderQty, 0L);
  }

  /**
   * Encodes a {@link PriceRequestEncoder} (template 50) with an explicit transactTime. Use this
   * overload for tests asserting time-sensitive logic.
   *
   * @param dst destination buffer
   * @param offset byte offset within {@code dst}
   * @param quoteReqId QuoteReqID (tag 131)
   * @param symbol instrument symbol (tag 55)
   * @param side order side (tag 54)
   * @param orderQty quantity in fixed-point 10^8 (tag 38)
   * @param transactTime epoch nanos (tag 60); pass {@code 0L} if not asserted
   * @return total encoded length including SBE header
   */
  public static int encodePriceRequest(
      final MutableDirectBuffer dst,
      final int offset,
      final String quoteReqId,
      final String symbol,
      final SideEnum side,
      final long orderQty,
      final long transactTime) {

    final var header = new MessageHeaderEncoder();
    final var enc = new PriceRequestEncoder();
    enc.wrapAndApplyHeader(dst, offset, header);

    enc.quoteReqId(quoteReqId);
    enc.symbol(symbol);
    enc.side(side);
    enc.orderQty(orderQty);
    enc.productType(ProductTypeEnum.Spot);
    enc.settlDate(DEFAULT_SETTL_DATE);
    enc.settlType(SettlTypeEnum.Regular);
    enc.tenor(TenorEnum.SN);
    enc.currency("USD");
    enc.settlCurrency("EUR");
    enc.accountCode("ACCT001");
    enc.transactTime(transactTime);
    enc.noLegsCount(0);

    return MessageHeaderEncoder.ENCODED_LENGTH + enc.encodedLength();
  }

  /**
   * Encodes a {@link PriceResponseEncoder} (template 51) — pricing service → orchestrator.
   *
   * <p><b>Defaults baked in:</b> {@code productType=Spot}, {@code bidSize/offerSize=10^8} when
   * accepted (null otherwise), {@code validUntil = transactTime + 30s} when accepted, {@code
   * swapPoints = NULL_VAL}, {@code quoteRejectReason = Other} when declined. Tests that need
   * different values should encode via the SBE encoder directly rather than this helper.
   *
   * @param dst destination buffer
   * @param offset byte offset within {@code dst}
   * @param quoteReqId QuoteReqID (tag 131)
   * @param symbol instrument symbol (tag 55)
   * @param accepted true if pricing service produced a quote; false to decline
   * @param bidPx bid price in fixed-point 10^8 (use null value when declined)
   * @param offerPx offer price in fixed-point 10^8 (use null value when declined)
   * @param transactTime epoch nanos (tag 60)
   * @return total encoded length including SBE header
   */
  public static int encodePriceResponse(
      final MutableDirectBuffer dst,
      final int offset,
      final String quoteReqId,
      final String symbol,
      final boolean accepted,
      final long bidPx,
      final long offerPx,
      final long transactTime) {

    final var header = new MessageHeaderEncoder();
    final var enc = new PriceResponseEncoder();
    enc.wrapAndApplyHeader(dst, offset, header);

    enc.quoteReqId(quoteReqId);
    enc.symbol(symbol);
    enc.bidPx(bidPx);
    enc.offerPx(offerPx);
    enc.bidSize(accepted ? 100_000_000L : PriceResponseEncoder.bidSizeNullValue());
    enc.offerSize(accepted ? 100_000_000L : PriceResponseEncoder.offerSizeNullValue());
    enc.validUntil(
        accepted ? transactTime + 30_000_000_000L : PriceResponseEncoder.validUntilNullValue());
    enc.accepted(accepted ? BooleanType.True : BooleanType.False);
    enc.quoteRejectReason(accepted ? QuoteRejectReasonEnum.NULL_VAL : QuoteRejectReasonEnum.Other);
    enc.transactTime(transactTime);
    enc.text("");
    enc.productType(ProductTypeEnum.Spot);
    enc.swapPoints(PriceResponseEncoder.swapPointsNullValue());
    enc.noLegsCount(0);

    return MessageHeaderEncoder.ENCODED_LENGTH + enc.encodedLength();
  }

  /**
   * Encodes a {@link PriceValidationRequestEncoder} (template 52) — orchestrator → pricing.
   *
   * @param dst destination buffer
   * @param offset byte offset within {@code dst}
   * @param quoteId QuoteID (tag 117)
   * @param quoteReqId QuoteReqID (tag 131)
   * @param symbol instrument symbol (tag 55)
   * @param orderQty quantity in fixed-point 10^8 (tag 38)
   * @param transactTime epoch nanos (tag 60)
   * @return total encoded length including SBE header
   */
  public static int encodePriceValidationRequest(
      final MutableDirectBuffer dst,
      final int offset,
      final String quoteId,
      final String quoteReqId,
      final String symbol,
      final long orderQty,
      final long transactTime) {

    final var header = new MessageHeaderEncoder();
    final var enc = new PriceValidationRequestEncoder();
    enc.wrapAndApplyHeader(dst, offset, header);

    enc.quoteId(quoteId);
    enc.quoteReqId(quoteReqId);
    enc.symbol(symbol);
    enc.side(SideEnum.Buy);
    enc.price(PriceValidationRequestEncoder.priceNullValue()); // explicitly null for market orders
    enc.orderQty(orderQty);
    enc.accountCode("ACCT001");
    enc.transactTime(transactTime);
    enc.productType(ProductTypeEnum.Spot);
    enc.settlDate(DEFAULT_SETTL_DATE);
    enc.settlType(SettlTypeEnum.Regular);
    enc.currency("USD");
    enc.settlCurrency("EUR");
    enc.tenor(TenorEnum.SN);
    enc.noLegsCount(0);

    return MessageHeaderEncoder.ENCODED_LENGTH + enc.encodedLength();
  }

  /**
   * Encodes a {@link PriceValidationResponseEncoder} (template 53) — pricing → orchestrator.
   *
   * @param dst destination buffer
   * @param offset byte offset within {@code dst}
   * @param quoteId QuoteID (tag 117)
   * @param valid true if validation passed; false if failed
   * @param transactTime epoch nanos (tag 60)
   * @return total encoded length including SBE header
   */
  public static int encodePriceValidationResponse(
      final MutableDirectBuffer dst,
      final int offset,
      final String quoteId,
      final boolean valid,
      final long transactTime) {

    final var header = new MessageHeaderEncoder();
    final var enc = new PriceValidationResponseEncoder();
    enc.wrapAndApplyHeader(dst, offset, header);

    enc.quoteId(quoteId);
    enc.valid(valid ? BooleanType.True : BooleanType.False);
    enc.rejectReason(valid ? RejectReasonEnum.NULL_VAL : RejectReasonEnum.InvalidPrice);
    enc.text("");
    enc.transactTime(transactTime);

    return MessageHeaderEncoder.ENCODED_LENGTH + enc.encodedLength();
  }

  // -----------------------------------------------------------------------
  // Quote domain events (templates 104-107)
  // -----------------------------------------------------------------------

  /**
   * Encodes a {@link QuoteRequestedEventEncoder} (template 104) with all fields explicitly
   * supplied.
   *
   * @param dst destination buffer
   * @param offset byte offset within {@code dst}
   * @param seqNum event sequence number (id 10020)
   * @param timestamp cluster timestamp epoch nanos (id 10021)
   * @param quoteReqId QuoteReqID (tag 131); max 20 ASCII chars
   * @param symbol instrument symbol (tag 55); max 8 ASCII chars
   * @param side order side (tag 54)
   * @param orderQty requested quantity in fixed-point 10^8 (tag 38)
   * @param accountCode account code (tag 1); max 16 ASCII chars
   * @param productType product type classification
   * @param settlDate settlement date YYYYMMDD (tag 64); max 8 ASCII chars
   * @param settlType settlement type (tag 63)
   * @param currency dealt currency ISO 4217 (tag 15); 3 ASCII chars
   * @param settlCurrency settlement currency ISO 4217 (tag 120); 3 ASCII chars
   * @param tenor tenor classification
   * @return total encoded length including SBE header
   */
  public static int encodeQuoteRequestedEvent(
      final MutableDirectBuffer dst,
      final int offset,
      final long seqNum,
      final long timestamp,
      final String quoteReqId,
      final String symbol,
      final SideEnum side,
      final long orderQty,
      final String accountCode,
      final ProductTypeEnum productType,
      final String settlDate,
      final SettlTypeEnum settlType,
      final String currency,
      final String settlCurrency,
      final TenorEnum tenor) {

    final MessageHeaderEncoder header = new MessageHeaderEncoder();
    final QuoteRequestedEventEncoder enc = new QuoteRequestedEventEncoder();
    enc.wrapAndApplyHeader(dst, offset, header);

    enc.sequenceNumber(seqNum)
        .timestamp(timestamp)
        .quoteReqId(quoteReqId)
        .symbol(symbol)
        .side(side)
        .orderQty(orderQty)
        .accountCode(accountCode)
        .productType(productType)
        .settlDate(settlDate)
        .settlType(settlType)
        .currency(currency)
        .settlCurrency(settlCurrency)
        .tenor(tenor);
    enc.noLegsCount(0);

    return MessageHeaderEncoder.ENCODED_LENGTH + enc.encodedLength();
  }

  /**
   * Encodes a {@link QuoteRequestedEventEncoder} (template 104) with FX spot defaults.
   *
   * <p>Defaults: productType=Spot, settlDate="20260101", settlType=Regular, currency="USD",
   * settlCurrency="EUR", tenor=SN.
   *
   * @param dst destination buffer
   * @param offset byte offset within {@code dst}
   * @param seqNum event sequence number
   * @param timestamp cluster timestamp epoch nanos
   * @param quoteReqId QuoteReqID (tag 131); max 20 ASCII chars
   * @param symbol instrument symbol (tag 55); max 8 ASCII chars
   * @param side order side (tag 54)
   * @param orderQty requested quantity in fixed-point 10^8 (tag 38)
   * @param accountCode account code (tag 1); max 16 ASCII chars
   * @return total encoded length including SBE header
   */
  public static int encodeQuoteRequestedEvent(
      final MutableDirectBuffer dst,
      final int offset,
      final long seqNum,
      final long timestamp,
      final String quoteReqId,
      final String symbol,
      final SideEnum side,
      final long orderQty,
      final String accountCode) {

    return encodeQuoteRequestedEvent(
        dst,
        offset,
        seqNum,
        timestamp,
        quoteReqId,
        symbol,
        side,
        orderQty,
        accountCode,
        ProductTypeEnum.Spot,
        DEFAULT_SETTL_DATE,
        SettlTypeEnum.Regular,
        "USD",
        "EUR",
        TenorEnum.SN);
  }

  /**
   * Encodes a {@link QuoteCreatedEventEncoder} (template 105) with all fields explicitly supplied.
   *
   * @param dst destination buffer
   * @param offset byte offset within {@code dst}
   * @param seqNum event sequence number (id 10020)
   * @param timestamp cluster timestamp epoch nanos (id 10021)
   * @param quoteId QuoteID (tag 117); max 20 ASCII chars
   * @param quoteReqId QuoteReqID (tag 131); max 20 ASCII chars
   * @param symbol instrument symbol (tag 55); max 8 ASCII chars
   * @param side order side (tag 54)
   * @param accountCode account code (tag 1); max 16 ASCII chars
   * @param bidPx bid price in fixed-point 10^8 (tag 132)
   * @param offerPx offer price in fixed-point 10^8 (tag 133)
   * @param bidSize bid size in fixed-point 10^8 (tag 134)
   * @param offerSize offer size in fixed-point 10^8 (tag 135)
   * @param validUntil quote expiry timestamp epoch nanos (tag 62)
   * @param productType product type classification
   * @param settlDate settlement date YYYYMMDD (tag 64); max 8 ASCII chars
   * @param settlType settlement type (tag 63)
   * @param currency dealt currency ISO 4217 (tag 15); 3 ASCII chars
   * @param settlCurrency settlement currency ISO 4217 (tag 120); 3 ASCII chars
   * @param tenor tenor classification
   * @param swapPoints swap points (optional; pass {@code
   *     QuoteCreatedEventEncoder.swapPointsNullValue()} for non-swap)
   * @return total encoded length including SBE header
   */
  public static int encodeQuoteCreatedEvent(
      final MutableDirectBuffer dst,
      final int offset,
      final long seqNum,
      final long timestamp,
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
      final ProductTypeEnum productType,
      final String settlDate,
      final SettlTypeEnum settlType,
      final String currency,
      final String settlCurrency,
      final TenorEnum tenor,
      final long swapPoints) {

    final MessageHeaderEncoder header = new MessageHeaderEncoder();
    final QuoteCreatedEventEncoder enc = new QuoteCreatedEventEncoder();
    enc.wrapAndApplyHeader(dst, offset, header);

    enc.sequenceNumber(seqNum)
        .timestamp(timestamp)
        .quoteId(quoteId)
        .quoteReqId(quoteReqId)
        .symbol(symbol)
        .side(side)
        .accountCode(accountCode)
        .bidPx(bidPx)
        .offerPx(offerPx)
        .bidSize(bidSize)
        .offerSize(offerSize)
        .validUntil(validUntil)
        .productType(productType)
        .settlDate(settlDate)
        .settlType(settlType)
        .currency(currency)
        .settlCurrency(settlCurrency)
        .tenor(tenor)
        .swapPoints(swapPoints);
    enc.noLegsCount(0);

    return MessageHeaderEncoder.ENCODED_LENGTH + enc.encodedLength();
  }

  /**
   * Encodes a {@link QuoteCreatedEventEncoder} (template 105) with FX spot defaults.
   *
   * <p>Defaults: productType=Spot, settlDate="20260101", settlType=Regular, currency="USD",
   * settlCurrency="EUR", tenor=SN, swapPoints=NULL_VAL, bidSize/offerSize=10^8.
   *
   * @param dst destination buffer
   * @param offset byte offset within {@code dst}
   * @param seqNum event sequence number
   * @param timestamp cluster timestamp epoch nanos
   * @param quoteId QuoteID (tag 117); max 20 ASCII chars
   * @param quoteReqId QuoteReqID (tag 131); max 20 ASCII chars
   * @param symbol instrument symbol (tag 55); max 8 ASCII chars
   * @param side order side (tag 54)
   * @param accountCode account code (tag 1); max 16 ASCII chars
   * @param bidPx bid price in fixed-point 10^8 (tag 132)
   * @param offerPx offer price in fixed-point 10^8 (tag 133)
   * @param validUntil quote expiry timestamp epoch nanos (tag 62)
   * @return total encoded length including SBE header
   */
  public static int encodeQuoteCreatedEvent(
      final MutableDirectBuffer dst,
      final int offset,
      final long seqNum,
      final long timestamp,
      final String quoteId,
      final String quoteReqId,
      final String symbol,
      final SideEnum side,
      final String accountCode,
      final long bidPx,
      final long offerPx,
      final long validUntil) {

    return encodeQuoteCreatedEvent(
        dst,
        offset,
        seqNum,
        timestamp,
        quoteId,
        quoteReqId,
        symbol,
        side,
        accountCode,
        bidPx,
        offerPx,
        100_000_000L,
        100_000_000L,
        validUntil,
        ProductTypeEnum.Spot,
        DEFAULT_SETTL_DATE,
        SettlTypeEnum.Regular,
        "USD",
        "EUR",
        TenorEnum.SN,
        QuoteCreatedEventEncoder.swapPointsNullValue());
  }

  /**
   * Encodes a {@link QuoteRejectedEventEncoder} (template 106) with all fields explicitly supplied.
   *
   * @param dst destination buffer
   * @param offset byte offset within {@code dst}
   * @param seqNum event sequence number (id 10020)
   * @param timestamp cluster timestamp epoch nanos (id 10021)
   * @param quoteReqId QuoteReqID (tag 131); max 20 ASCII chars
   * @param symbol instrument symbol (tag 55); max 8 ASCII chars
   * @param side order side (tag 54)
   * @param accountCode account code (tag 1); max 16 ASCII chars
   * @param quoteRejectReason reject reason (tag 658)
   * @param productType product type classification
   * @param text free-text reason (tag 58); max 64 ASCII chars
   * @return total encoded length including SBE header
   */
  public static int encodeQuoteRejectedEvent(
      final MutableDirectBuffer dst,
      final int offset,
      final long seqNum,
      final long timestamp,
      final String quoteReqId,
      final String symbol,
      final SideEnum side,
      final String accountCode,
      final QuoteRejectReasonEnum quoteRejectReason,
      final ProductTypeEnum productType,
      final String text) {

    final MessageHeaderEncoder header = new MessageHeaderEncoder();
    final QuoteRejectedEventEncoder enc = new QuoteRejectedEventEncoder();
    enc.wrapAndApplyHeader(dst, offset, header);

    enc.sequenceNumber(seqNum)
        .timestamp(timestamp)
        .quoteReqId(quoteReqId)
        .symbol(symbol)
        .side(side)
        .accountCode(accountCode)
        .quoteRejectReason(quoteRejectReason)
        .productType(productType)
        .text(text);

    return MessageHeaderEncoder.ENCODED_LENGTH + enc.encodedLength();
  }

  /**
   * Encodes a {@link QuoteRejectedEventEncoder} (template 106) with FX spot defaults.
   *
   * <p>Defaults: productType=Spot, accountCode="ACCT001".
   *
   * @param dst destination buffer
   * @param offset byte offset within {@code dst}
   * @param seqNum event sequence number
   * @param timestamp cluster timestamp epoch nanos
   * @param quoteReqId QuoteReqID (tag 131); max 20 ASCII chars
   * @param symbol instrument symbol (tag 55); max 8 ASCII chars
   * @param side order side (tag 54)
   * @param quoteRejectReason reject reason (tag 658)
   * @param text free-text reason (tag 58); max 64 ASCII chars
   * @return total encoded length including SBE header
   */
  public static int encodeQuoteRejectedEvent(
      final MutableDirectBuffer dst,
      final int offset,
      final long seqNum,
      final long timestamp,
      final String quoteReqId,
      final String symbol,
      final SideEnum side,
      final QuoteRejectReasonEnum quoteRejectReason,
      final String text) {

    return encodeQuoteRejectedEvent(
        dst,
        offset,
        seqNum,
        timestamp,
        quoteReqId,
        symbol,
        side,
        "ACCT001",
        quoteRejectReason,
        ProductTypeEnum.Spot,
        text);
  }

  /**
   * Encodes a {@link QuoteExpiredEventEncoder} (template 107) with all fields explicitly supplied.
   *
   * @param dst destination buffer
   * @param offset byte offset within {@code dst}
   * @param seqNum event sequence number (id 10020)
   * @param timestamp cluster timestamp epoch nanos (id 10021)
   * @param quoteId QuoteID (tag 117); max 20 ASCII chars
   * @param quoteReqId QuoteReqID (tag 131); max 20 ASCII chars
   * @param symbol instrument symbol (tag 55); max 8 ASCII chars
   * @param side order side (tag 54)
   * @param accountCode account code (tag 1); max 16 ASCII chars
   * @param productType product type classification
   * @return total encoded length including SBE header
   */
  public static int encodeQuoteExpiredEvent(
      final MutableDirectBuffer dst,
      final int offset,
      final long seqNum,
      final long timestamp,
      final String quoteId,
      final String quoteReqId,
      final String symbol,
      final SideEnum side,
      final String accountCode,
      final ProductTypeEnum productType) {

    final MessageHeaderEncoder header = new MessageHeaderEncoder();
    final QuoteExpiredEventEncoder enc = new QuoteExpiredEventEncoder();
    enc.wrapAndApplyHeader(dst, offset, header);

    enc.sequenceNumber(seqNum)
        .timestamp(timestamp)
        .quoteId(quoteId)
        .quoteReqId(quoteReqId)
        .symbol(symbol)
        .side(side)
        .accountCode(accountCode)
        .productType(productType);

    return MessageHeaderEncoder.ENCODED_LENGTH + enc.encodedLength();
  }

  /**
   * Encodes a {@link QuoteExpiredEventEncoder} (template 107) with FX spot defaults.
   *
   * <p>Defaults: productType=Spot, accountCode="ACCT001".
   *
   * @param dst destination buffer
   * @param offset byte offset within {@code dst}
   * @param seqNum event sequence number
   * @param timestamp cluster timestamp epoch nanos
   * @param quoteId QuoteID (tag 117); max 20 ASCII chars
   * @param quoteReqId QuoteReqID (tag 131); max 20 ASCII chars
   * @param symbol instrument symbol (tag 55); max 8 ASCII chars
   * @param side order side (tag 54)
   * @return total encoded length including SBE header
   */
  public static int encodeQuoteExpiredEvent(
      final MutableDirectBuffer dst,
      final int offset,
      final long seqNum,
      final long timestamp,
      final String quoteId,
      final String quoteReqId,
      final String symbol,
      final SideEnum side) {

    return encodeQuoteExpiredEvent(
        dst,
        offset,
        seqNum,
        timestamp,
        quoteId,
        quoteReqId,
        symbol,
        side,
        "ACCT001",
        ProductTypeEnum.Spot);
  }

  // -----------------------------------------------------------------------
  // WebSocket control messages (templates 60-72)
  // -----------------------------------------------------------------------

  /**
   * Encodes a {@link WebSocketAuthEncoder} (template 60) — browser-to-server JWT authentication
   * after WebSocket upgrade.
   *
   * <p>The token is written as SBE varData (4-byte length header + raw bytes). Callers should pass
   * the UTF-8 bytes of the JWT string (e.g., {@code "myJwt".getBytes(StandardCharsets.UTF_8)}).
   *
   * @param dst destination buffer
   * @param offset byte offset within {@code dst}
   * @param protocolVersion protocol version negotiated by the client (field id 1)
   * @param token JWT token bytes (varData, field id 2); max 1 MiB
   * @return total encoded length including SBE header
   */
  public static int encodeWebSocketAuth(
      final MutableDirectBuffer dst,
      final int offset,
      final int protocolVersion,
      final byte[] token) {

    final var header = new MessageHeaderEncoder();
    final var enc = new WebSocketAuthEncoder();
    enc.wrapAndApplyHeader(dst, offset, header);

    enc.protocolVersion(protocolVersion);
    enc.putToken(token, 0, token.length);

    return MessageHeaderEncoder.ENCODED_LENGTH + enc.encodedLength();
  }

  /**
   * Encodes a {@link WebSocketAuthAckEncoder} (template 61) — server-to-browser authentication
   * accepted with assigned session UUID.
   *
   * @param dst destination buffer
   * @param offset byte offset within {@code dst}
   * @param sessionIdMsb most significant bits of the assigned session UUID (field id 1)
   * @param sessionIdLsb least significant bits of the assigned session UUID (field id 1)
   * @param protocolVersion negotiated protocol version echoed back (field id 2)
   * @param maxSubscriptions maximum number of concurrent subscriptions allowed (field id 3)
   * @return total encoded length including SBE header
   */
  public static int encodeWebSocketAuthAck(
      final MutableDirectBuffer dst,
      final int offset,
      final long sessionIdMsb,
      final long sessionIdLsb,
      final int protocolVersion,
      final int maxSubscriptions) {

    final var header = new MessageHeaderEncoder();
    final var enc = new WebSocketAuthAckEncoder();
    enc.wrapAndApplyHeader(dst, offset, header);

    enc.sessionId().mostSignificantBits(sessionIdMsb).leastSignificantBits(sessionIdLsb);
    enc.protocolVersion(protocolVersion);
    enc.maxSubscriptions(maxSubscriptions);

    return MessageHeaderEncoder.ENCODED_LENGTH + enc.encodedLength();
  }

  /**
   * Encodes a {@link WebSocketHeartbeatEncoder} (template 64) — server-to-browser heartbeat with
   * wall-clock epoch nanos.
   *
   * @param dst destination buffer
   * @param offset byte offset within {@code dst}
   * @param serverNanos server wall-clock timestamp in epoch nanos (field id 1)
   * @return total encoded length including SBE header
   */
  public static int encodeWebSocketHeartbeat(
      final MutableDirectBuffer dst, final int offset, final long serverNanos) {

    final var header = new MessageHeaderEncoder();
    final var enc = new WebSocketHeartbeatEncoder();
    enc.wrapAndApplyHeader(dst, offset, header);

    enc.serverNanos(serverNanos);

    return MessageHeaderEncoder.ENCODED_LENGTH + enc.encodedLength();
  }

  /**
   * Encodes a {@link ClientHeartbeatEncoder} (template 65) — browser-to-server heartbeat with
   * client-side epoch nanos.
   *
   * @param dst destination buffer
   * @param offset byte offset within {@code dst}
   * @param clientNanos client wall-clock timestamp in epoch nanos (field id 1)
   * @return total encoded length including SBE header
   */
  public static int encodeClientHeartbeat(
      final MutableDirectBuffer dst, final int offset, final long clientNanos) {

    final var header = new MessageHeaderEncoder();
    final var enc = new ClientHeartbeatEncoder();
    enc.wrapAndApplyHeader(dst, offset, header);

    enc.clientNanos(clientNanos);

    return MessageHeaderEncoder.ENCODED_LENGTH + enc.encodedLength();
  }

  /**
   * Encodes a {@link WebSocketErrorEncoder} (template 67) — server-to-browser error notification.
   *
   * <p>The errorText is written as SBE varData (4-byte length header + raw bytes). Callers should
   * pass the UTF-8 bytes of the error description (e.g., {@code "auth
   * failed".getBytes(StandardCharsets.UTF_8)}).
   *
   * @param dst destination buffer
   * @param offset byte offset within {@code dst}
   * @param errorCode structured error code enum (field id 1)
   * @param errorText error description bytes (varData, field id 2); max 1 MiB
   * @return total encoded length including SBE header
   */
  public static int encodeWebSocketError(
      final MutableDirectBuffer dst,
      final int offset,
      final WebSocketErrorCode errorCode,
      final byte[] errorText) {

    final var header = new MessageHeaderEncoder();
    final var enc = new WebSocketErrorEncoder();
    enc.wrapAndApplyHeader(dst, offset, header);

    enc.errorCode(errorCode);
    enc.putErrorText(errorText, 0, errorText.length);

    return MessageHeaderEncoder.ENCODED_LENGTH + enc.encodedLength();
  }

  /**
   * Encodes a {@link WebSocketGapRequestEncoder} (template 68) — browser-to-server request to
   * replay missed reliable messages in the range {@code [fromSeqNo, toSeqNo]}.
   *
   * @param dst destination buffer
   * @param offset byte offset within {@code dst}
   * @param fromSeqNo first missed sequence number (inclusive, field id 1)
   * @param toSeqNo last missed sequence number (inclusive, field id 2)
   * @return total encoded length including SBE header
   */
  public static int encodeWebSocketGapRequest(
      final MutableDirectBuffer dst, final int offset, final long fromSeqNo, final long toSeqNo) {

    final var header = new MessageHeaderEncoder();
    final var enc = new WebSocketGapRequestEncoder();
    enc.wrapAndApplyHeader(dst, offset, header);

    enc.fromSeqNo(fromSeqNo);
    enc.toSeqNo(toSeqNo);

    return MessageHeaderEncoder.ENCODED_LENGTH + enc.encodedLength();
  }

  /**
   * Encodes a {@link SessionResumeEncoder} (template 69) — browser-to-server reconnection to an
   * existing session within the 30-second grace period.
   *
   * @param dst destination buffer
   * @param offset byte offset within {@code dst}
   * @param sessionIdMsb most significant bits of the previous session UUID (field id 1)
   * @param sessionIdLsb least significant bits of the previous session UUID (field id 1)
   * @param lastSeqNo last reliable sequence number received by the client (field id 2)
   * @return total encoded length including SBE header
   */
  public static int encodeSessionResume(
      final MutableDirectBuffer dst,
      final int offset,
      final long sessionIdMsb,
      final long sessionIdLsb,
      final long lastSeqNo) {

    final var header = new MessageHeaderEncoder();
    final var enc = new SessionResumeEncoder();
    enc.wrapAndApplyHeader(dst, offset, header);

    enc.sessionId().mostSignificantBits(sessionIdMsb).leastSignificantBits(sessionIdLsb);
    enc.lastSeqNo(lastSeqNo);

    return MessageHeaderEncoder.ENCODED_LENGTH + enc.encodedLength();
  }

  /**
   * Encodes a {@link CommandAckEncoder} (template 70) — server-to-browser acknowledgement of a
   * browser command (NOS, Cancel, QuoteReq).
   *
   * @param dst destination buffer
   * @param offset byte offset within {@code dst}
   * @param clientCmdSeqNo client-assigned command sequence number (field id 1)
   * @param status acknowledgement status (field id 2)
   * @return total encoded length including SBE header
   */
  public static int encodeCommandAck(
      final MutableDirectBuffer dst,
      final int offset,
      final long clientCmdSeqNo,
      final CommandAckStatus status) {

    final var header = new MessageHeaderEncoder();
    final var enc = new CommandAckEncoder();
    enc.wrapAndApplyHeader(dst, offset, header);

    enc.clientCmdSeqNo(clientCmdSeqNo);
    enc.status(status);

    return MessageHeaderEncoder.ENCODED_LENGTH + enc.encodedLength();
  }

  /**
   * Encodes a {@link ClientAckEncoder} (template 71) — browser-to-server acknowledgement of the
   * highest received reliable sequence number. Used for slow-consumer detection.
   *
   * @param dst destination buffer
   * @param offset byte offset within {@code dst}
   * @param lastReceivedSeqNo highest reliable seqNo successfully processed by the client (field id
   *     1)
   * @return total encoded length including SBE header
   */
  public static int encodeClientAck(
      final MutableDirectBuffer dst, final int offset, final long lastReceivedSeqNo) {

    final var header = new MessageHeaderEncoder();
    final var enc = new ClientAckEncoder();
    enc.wrapAndApplyHeader(dst, offset, header);

    enc.lastReceivedSeqNo(lastReceivedSeqNo);

    return MessageHeaderEncoder.ENCODED_LENGTH + enc.encodedLength();
  }

  /**
   * Encodes a {@link ReplayCompleteEncoder} (template 72) — server-to-browser marker indicating gap
   * replay is finished and live delivery resumes. Empty body (header only).
   *
   * @param dst destination buffer
   * @param offset byte offset within {@code dst}
   * @return total encoded length including SBE header
   */
  public static int encodeReplayComplete(final MutableDirectBuffer dst, final int offset) {

    final var header = new MessageHeaderEncoder();
    final var enc = new ReplayCompleteEncoder();
    enc.wrapAndApplyHeader(dst, offset, header);

    return MessageHeaderEncoder.ENCODED_LENGTH + enc.encodedLength();
  }

  /**
   * Encodes a {@link WebSocketSubscribeEncoder} (template 62) — browser-to-server subscribe request
   * with a repeating group of symbol + eventTypes entries.
   *
   * <p>Each group entry is 12 bytes: 8-byte symbol (ASCII, NUL-padded) + 4-byte eventTypes bitmask.
   *
   * @param dst destination buffer
   * @param offset byte offset within {@code dst}
   * @param symbols array of symbol strings (max 8 chars each)
   * @param eventTypes parallel array of event type bitmasks as unsigned 32-bit values in {@code
   *     long} to match SBE {@code uint32} convention (same length as symbols)
   * @return total encoded length including SBE header
   * @throws IllegalArgumentException if symbols and eventTypes have different lengths
   */
  public static int encodeWebSocketSubscribe(
      final MutableDirectBuffer dst,
      final int offset,
      final String[] symbols,
      final long[] eventTypes) {

    if (symbols.length != eventTypes.length) {
      throw new IllegalArgumentException(
          "symbols and eventTypes must have the same length: "
              + symbols.length
              + " vs "
              + eventTypes.length);
    }

    final var header = new MessageHeaderEncoder();
    final var enc = new WebSocketSubscribeEncoder();
    enc.wrapAndApplyHeader(dst, offset, header);

    final var group = enc.symbolsCount(symbols.length);
    for (int i = 0; i < symbols.length; i++) {
      group.next().symbol(symbols[i]).eventTypes(eventTypes[i]);
    }

    return MessageHeaderEncoder.ENCODED_LENGTH + enc.encodedLength();
  }

  /**
   * Encodes a {@link WebSocketUnsubscribeEncoder} (template 63) — browser-to-server unsubscribe
   * request with a repeating group of symbol entries.
   *
   * <p>Each group entry is 8 bytes: 8-byte symbol (ASCII, NUL-padded). An empty array encodes an
   * "unsubscribe all" request (group count = 0).
   *
   * @param dst destination buffer
   * @param offset byte offset within {@code dst}
   * @param symbols array of symbol strings to unsubscribe; empty array means unsubscribe all
   * @return total encoded length including SBE header
   */
  public static int encodeWebSocketUnsubscribe(
      final MutableDirectBuffer dst, final int offset, final String[] symbols) {

    final var header = new MessageHeaderEncoder();
    final var enc = new WebSocketUnsubscribeEncoder();
    enc.wrapAndApplyHeader(dst, offset, header);

    final var group = enc.symbolsCount(symbols.length);
    for (final var symbol : symbols) {
      group.next().symbol(symbol);
    }

    return MessageHeaderEncoder.ENCODED_LENGTH + enc.encodedLength();
  }
}
