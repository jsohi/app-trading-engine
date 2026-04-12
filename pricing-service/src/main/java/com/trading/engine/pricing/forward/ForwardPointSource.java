package com.trading.engine.pricing.forward;

import org.agrona.DirectBuffer;

/**
 * Service-provider interface (SPI) for querying the FX forward point term structure.
 *
 * <p><b>FX forward pricing convention.</b> In the interbank market, forward exchange rates are
 * quoted as <em>spot + forward points</em>. The forward points (also called swap points) represent
 * the interest rate differential between the two currencies for the given settlement tenor,
 * expressed in the same pip scale as the spot quote. For example, if EUR/USD spot is 1.08500000 and
 * the 1-month forward points are +0.00125000, the 1-month outright forward rate is 1.08625000. This
 * convention avoids transmitting the full forward price on every update — only the differential
 * changes when the term structure shifts.
 *
 * <p><b>Fixed-point representation.</b> All forward point values returned by this interface are in
 * the engine's standard fixed-point format: {@code long} with implicit scale {@code 10^-8} (i.e.,
 * multiply the real-unit forward points by {@link
 * com.trading.engine.messages.FixedPointScale#PRICE_SCALE PRICE_SCALE} to obtain the stored value).
 * Forward points may be positive or negative depending on the interest rate differential between
 * the base and quote currencies.
 *
 * <p><b>Swap points.</b> For FX swap pricing, the relevant quantity is the <em>difference</em>
 * between forward points at two tenors (near leg and far leg). The {@link #swapPoints} method
 * provides this as a convenience: {@code swapPoints(sym, near, far) = forwardPoints(far) -
 * forwardPoints(near)}.
 *
 * <p><b>Threading model.</b> Implementations are <b>single-threaded</b>. All methods are invoked
 * exclusively from the pricing-service agent's duty cycle. No synchronisation is required or
 * provided.
 *
 * <p><b>Allocation behaviour.</b> Implementations must be <b>zero-allocation after
 * construction</b>. All internal buffers, caches, and probe keys are pre-allocated at startup;
 * lookup methods must not allocate on the heap.
 *
 * @see ConfigurableForwardPointSource
 * @see com.trading.engine.messages.FixedPointScale#PRICE_SCALE
 * @see com.trading.engine.pricing.PricingMath#mulDiv(long, long, long)
 */
public interface ForwardPointSource {

  /**
   * Returns the forward points for the given symbol and settlement tenor in fixed-point {@code
   * 10^-8} representation.
   *
   * <p>Forward points represent the interest rate differential component of an FX forward price.
   * The outright forward rate is computed as {@code spot + forwardPoints(symbol,
   * daysToSettlement)}.
   *
   * <p>The return value may be positive or negative:
   *
   * <ul>
   *   <li><b>Positive</b> — the base currency has a lower interest rate than the quote currency
   *       (forward premium)
   *   <li><b>Negative</b> — the base currency has a higher interest rate than the quote currency
   *       (forward discount)
   * </ul>
   *
   * <p><b>Special cases:</b>
   *
   * <ul>
   *   <li>{@code daysToSettlement <= 0} — returns {@code 0} (spot settlement, no forward
   *       adjustment)
   *   <li>Unknown symbol — returns {@code 0} (no forward adjustment applied)
   * </ul>
   *
   * <p><b>Allocation:</b> zero-allocation — implementations use pre-allocated probe keys.
   *
   * @param symbol buffer containing the symbol bytes (8-byte fixed-width SBE {@code Symbol} type)
   * @param offset start offset of the symbol within the buffer
   * @param length number of bytes to read (always 8 for the SBE Symbol type)
   * @param daysToSettlement number of calendar days from spot to forward settlement; must be {@code
   *     >= 0} for meaningful results
   * @return forward points in fixed-point {@code 10^-8}, or {@code 0} if the symbol is unknown or
   *     {@code daysToSettlement <= 0}
   */
  long forwardPoints(DirectBuffer symbol, int offset, int length, int daysToSettlement);

  /**
   * Returns the swap points (forward point differential) between a near-leg and far-leg settlement
   * tenor for the given symbol.
   *
   * <p>Swap points are defined as {@code forwardPoints(far) - forwardPoints(near)}, which
   * represents the cost of rolling a position from the near settlement date to the far settlement
   * date. This is the standard market convention for quoting FX swap prices.
   *
   * <p><b>Allocation:</b> zero-allocation — delegates to {@link #forwardPoints} twice.
   *
   * @param symbol buffer containing the symbol bytes (8-byte fixed-width SBE {@code Symbol} type)
   * @param offset start offset of the symbol within the buffer
   * @param length number of bytes to read (always 8 for the SBE Symbol type)
   * @param nearDays days to settlement for the near (first) leg
   * @param farDays days to settlement for the far (second) leg; should be {@code >= nearDays}
   * @return swap points in fixed-point {@code 10^-8}, computed as {@code forwardPoints(farDays) -
   *     forwardPoints(nearDays)}
   */
  default long swapPoints(
      final DirectBuffer symbol,
      final int offset,
      final int length,
      final int nearDays,
      final int farDays) {

    return forwardPoints(symbol, offset, length, farDays)
        - forwardPoints(symbol, offset, length, nearDays);
  }
}
