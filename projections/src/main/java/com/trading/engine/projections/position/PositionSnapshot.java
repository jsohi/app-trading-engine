package com.trading.engine.projections.position;

import java.nio.charset.StandardCharsets;

/**
 * Immutable snapshot of a position's state at a point in time. Returned by {@link
 * PositionProjection} query methods to provide a thread-safe, detached view.
 *
 * <p><b>Threading:</b> immutable — safe to share across threads without synchronization.
 *
 * <p><b>Derived fields:</b> {@code avgBuyPx} and {@code avgSellPx} are computed at snapshot
 * creation time from cumulative notional and quantity values.
 *
 * @param symbol FIX tag 55: instrument symbol
 * @param accountCode FIX tag 1: account code
 * @param settlDate FIX tag 64: settlement date YYYYMMDD
 * @param currency FIX tag 15: currency
 * @param settlCurrency FIX tag 120: settlement currency
 * @param netQty net position quantity, fixed-point 10^-8 (positive=long, negative=short)
 * @param buyQty gross buy quantity, fixed-point 10^-8
 * @param sellQty gross sell quantity, fixed-point 10^-8
 * @param avgBuyPx weighted average buy price, fixed-point 10^-8 (0 if no buys)
 * @param avgSellPx weighted average sell price, fixed-point 10^-8 (0 if no sells)
 * @param lastUpdatedAt cluster timestamp (epoch nanos) of the most recent fill
 * @param lastSequenceNumber event sequence number of the most recent fill
 */
public record PositionSnapshot(
    String symbol,
    String accountCode,
    String settlDate,
    String currency,
    String settlCurrency,
    long netQty,
    long buyQty,
    long sellQty,
    long avgBuyPx,
    long avgSellPx,
    long lastUpdatedAt,
    long lastSequenceNumber) {

  /**
   * Creates an immutable snapshot from a mutable {@link PositionView}, computing derived VWAP
   * fields. Must be called under the projection's read lock.
   *
   * @param v the mutable position view
   * @param symbolStr the pre-decoded symbol string (avoids re-decoding from packed long)
   * @param avgBuyPx pre-computed average buy price (via mulDiv)
   * @param avgSellPx pre-computed average sell price (via mulDiv)
   * @return a new immutable snapshot
   */
  static PositionSnapshot from(
      final PositionView v, final String symbolStr, final long avgBuyPx, final long avgSellPx) {
    return new PositionSnapshot(
        symbolStr,
        asciiString(v.accountCode(), v.accountCodeLen()),
        asciiString(v.settlDate(), v.settlDateLen()),
        asciiString(v.currency(), v.currencyLen()),
        asciiString(v.settlCurrency(), v.settlCurrencyLen()),
        v.netQty(),
        v.buyQty(),
        v.sellQty(),
        avgBuyPx,
        avgSellPx,
        v.lastUpdatedAt(),
        v.lastSequenceNumber());
  }

  private static String asciiString(final byte[] data, final int length) {
    if (length <= 0) {
      return "";
    }
    int end = length;
    while (end > 0 && data[end - 1] == 0) {
      end--;
    }
    return end == 0 ? "" : new String(data, 0, end, StandardCharsets.US_ASCII);
  }
}
