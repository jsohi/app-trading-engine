package com.trading.engine.projections.order;

import com.trading.engine.messages.sbe.ExecTypeEnum;
import com.trading.engine.messages.sbe.OrdStatusEnum;
import com.trading.engine.messages.sbe.OrdTypeEnum;
import com.trading.engine.messages.sbe.ProductTypeEnum;
import com.trading.engine.messages.sbe.RejectReasonEnum;
import com.trading.engine.messages.sbe.SettlTypeEnum;
import com.trading.engine.messages.sbe.SideEnum;
import com.trading.engine.messages.sbe.TenorEnum;
import java.nio.charset.StandardCharsets;

/**
 * Immutable snapshot of an order's state at a point in time. Returned by {@link OrderProjection}
 * query methods to provide a thread-safe, detached view of order state.
 *
 * <p><b>Threading:</b> immutable — safe to share across threads without synchronization.
 *
 * <p><b>Allocation:</b> one instance per query result. Created by copying fields from the internal
 * mutable {@link OrderView} under the projection's read lock.
 *
 * @param orderId FIX tag 37: exchange order identifier (empty for rejected orders)
 * @param clOrdId FIX tag 11: client order identifier
 * @param symbol FIX tag 55: instrument symbol
 * @param accountCode FIX tag 1: account code
 * @param side FIX tag 54: order side
 * @param ordType FIX tag 40: order type
 * @param ordStatus FIX tag 39: current order status
 * @param productType product type classification (Spot, Forward, Swap)
 * @param execType FIX tag 150: execution type of the most recent execution report
 * @param rejectReason rejection reason (NULL_VAL if not rejected)
 * @param lastExecId FIX tag 17: last execution identifier (empty if no fills)
 * @param price FIX tag 44: order price, fixed-point 10^-8
 * @param orderQty FIX tag 38: order quantity, fixed-point 10^-8
 * @param leavesQty FIX tag 151: remaining quantity, fixed-point 10^-8
 * @param cumQty FIX tag 14: cumulative filled quantity, fixed-point 10^-8
 * @param avgPx weighted average fill price, fixed-point 10^-8
 * @param settlDate FIX tag 64: settlement date YYYYMMDD (empty for rejected orders)
 * @param settlType FIX tag 63: settlement type (NULL_VAL for rejected orders)
 * @param currency FIX tag 15: order currency (empty for rejected orders)
 * @param settlCurrency FIX tag 120: settlement currency (empty for rejected orders)
 * @param tenor tenor classification (NULL_VAL for rejected orders)
 * @param sequenceNumber event sequence number of the most recently applied event
 * @param createdAt cluster timestamp (epoch nanos) of order creation
 * @param lastUpdatedAt cluster timestamp (epoch nanos) of the most recent event
 */
public record OrderSnapshot(
    String orderId,
    String clOrdId,
    String symbol,
    String accountCode,
    SideEnum side,
    OrdTypeEnum ordType,
    OrdStatusEnum ordStatus,
    ProductTypeEnum productType,
    ExecTypeEnum execType,
    RejectReasonEnum rejectReason,
    String lastExecId,
    long price,
    long orderQty,
    long leavesQty,
    long cumQty,
    long avgPx,
    String settlDate,
    SettlTypeEnum settlType,
    String currency,
    String settlCurrency,
    TenorEnum tenor,
    long sequenceNumber,
    long createdAt,
    long lastUpdatedAt) {

  /**
   * Creates an immutable snapshot by copying all fields from a mutable {@link OrderView}. String
   * fields are decoded from SBE byte arrays using US-ASCII.
   *
   * <p>Must be called under the projection's read lock (or write lock during snapshot creation
   * inside event dispatch).
   *
   * @param v the mutable order view to copy from
   * @return a new immutable snapshot
   */
  static OrderSnapshot from(final OrderView v) {
    return new OrderSnapshot(
        asciiString(v.orderId(), v.orderIdLen()),
        asciiString(v.clOrdId(), v.clOrdIdLen()),
        asciiString(v.symbol(), v.symbolLen()),
        asciiString(v.accountCode(), v.accountCodeLen()),
        v.side(),
        v.ordType(),
        v.ordStatus(),
        v.productType(),
        v.execType(),
        v.rejectReason(),
        asciiString(v.lastExecId(), v.lastExecIdLen()),
        v.price(),
        v.orderQty(),
        v.leavesQty(),
        v.cumQty(),
        v.avgPx(),
        asciiString(v.settlDate(), v.settlDateLen()),
        v.settlType(),
        asciiString(v.currency(), v.currencyLen()),
        asciiString(v.settlCurrency(), v.settlCurrencyLen()),
        v.tenor(),
        v.sequenceNumber(),
        v.createdAt(),
        v.lastUpdatedAt());
  }

  private static String asciiString(final byte[] data, final int length) {
    if (length <= 0) {
      return "";
    }
    // Trim trailing NUL bytes (SBE pads fixed-length char fields with 0x00)
    int end = length;
    while (end > 0 && data[end - 1] == 0) {
      end--;
    }
    return end == 0 ? "" : new String(data, 0, end, StandardCharsets.US_ASCII);
  }
}
