package com.trading.engine.pricing.skew;

import org.agrona.DirectBuffer;

/**
 * Strategy interface for computing an inventory-driven mid-rate adjustment.
 *
 * <p><b>Mid-shift convention.</b> The adjustment returned by {@link #skewAdjustment} is a signed
 * fixed-point value that is <em>added</em> to the current mid-rate to produce a skewed mid:
 *
 * <pre>
 *   skewedMid = midRate + skewAdjustment(symbol, offset, length, midRate)
 * </pre>
 *
 * The sign convention follows the standard dealer-risk model:
 *
 * <ul>
 *   <li><b>Positive adjustment</b> — shifts the mid <em>up</em>. This occurs when the dealer is
 *       <em>short</em>, making the bid more aggressive to attract buying flow and reduce the short
 *       position.
 *   <li><b>Negative adjustment</b> — shifts the mid <em>down</em>. This occurs when the dealer is
 *       <em>long</em>, making the offer more aggressive to attract selling flow and reduce the long
 *       position.
 *   <li><b>Zero</b> — no inventory; the mid-rate is unperturbed.
 * </ul>
 *
 * <p>After the mid-shift, the spread model applies its symmetric (or asymmetric) bid/offer spread
 * around the skewed mid. The combination of inventory skew and spread gives the dealer a natural
 * mechanism to manage risk: the skew shifts the center of the market, while the spread controls the
 * cost of transacting.
 *
 * <p><b>Threading model.</b> Implementations are <b>single-threaded</b>. All methods are invoked
 * exclusively from the pricing-service agent's duty cycle — no synchronisation is required or
 * provided.
 *
 * <p><b>Allocation behaviour.</b> Implementations must be <b>zero-allocation after
 * construction</b>. All internal buffers, caches, and probe keys are pre-allocated at startup; the
 * {@link #skewAdjustment} method must not allocate on the heap.
 *
 * @see ConvexInventorySkew
 * @see LinearInventorySkew
 * @see PositionSource
 * @see com.trading.engine.pricing.PricingMath#mulDiv(long, long, long)
 */
public interface InventorySkewModel {

  /**
   * Computes the signed fixed-point mid-rate adjustment driven by the dealer's current inventory in
   * the given symbol.
   *
   * <p>The returned value is in the same fixed-point {@code 10^-8} scale as the mid-rate. It should
   * be added to the mid-rate before applying the bid/offer spread:
   *
   * <pre>
   *   skewedMid = midRate + skewAdjustment(symbol, offset, length, midRate)
   * </pre>
   *
   * <p><b>Allocation:</b> zero-allocation — implementations use pre-allocated probe keys for
   * position lookups.
   *
   * @param symbol buffer containing the symbol bytes (8-byte fixed-width SBE {@code Symbol} type)
   * @param offset start offset of the symbol within the buffer
   * @param length number of bytes to read (always 8 for the SBE Symbol type)
   * @param midRate current mid-rate in fixed-point {@code 10^-8}; must be {@code > 0}
   * @return signed mid-rate adjustment in fixed-point {@code 10^-8}. Positive shifts mid up (dealer
   *     is short), negative shifts mid down (dealer is long), zero if no position.
   */
  long skewAdjustment(DirectBuffer symbol, int offset, int length, long midRate);
}
