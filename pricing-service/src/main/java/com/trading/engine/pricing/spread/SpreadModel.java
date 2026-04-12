package com.trading.engine.pricing.spread;

import com.trading.engine.messages.sbe.AccountTypeEnum;
import com.trading.engine.messages.sbe.ProductTypeEnum;
import org.agrona.DirectBuffer;

/**
 * Strategy interface for computing bid/offer spread around a mid-rate.
 *
 * <p>Implementations apply account-tier, quantity, volatility, and inventory-skew adjustments to
 * produce a two-sided price from a single mid-rate input. The production implementation is {@link
 * TieredSpreadModel}, which follows the mid-shift skew convention used on EBS and Currenex
 * single-dealer platforms.
 *
 * <h3>Output-parameter pattern</h3>
 *
 * <p>The computed bid and offer prices are written into a caller-supplied {@link SpreadResult}
 * flyweight rather than returned via a new object. This eliminates per-call heap allocation on the
 * pricing hot path. The caller pre-allocates a single {@code SpreadResult} instance at construction
 * time and passes it into every {@code compute()} invocation. The result fields are valid only
 * until the next call to {@code compute()} on the same result instance.
 *
 * <h3>Threading model</h3>
 *
 * <p>Implementations are <b>not required to be thread-safe</b>. The pricing service invokes {@code
 * compute()} exclusively from its single-threaded agent duty cycle.
 *
 * <h3>Allocation contract</h3>
 *
 * <p><b>Zero allocation after construction.</b> Implementations must not allocate on the hot path.
 * All internal lookup structures (probe keys, config maps) must be pre-allocated at construction
 * time.
 *
 * @see SpreadResult
 * @see TieredSpreadModel
 */
public interface SpreadModel {

  /**
   * Computes the bid and offer prices for a given symbol, mid-rate, and order context, writing the
   * result into the supplied {@link SpreadResult} flyweight.
   *
   * <p>The implementation applies spread widening factors (account tier, quantity, volatility) and
   * inventory skew to shift and widen the bid/offer around the mid-rate. The resulting prices are
   * rounded to pip precision.
   *
   * <p><b>Allocation:</b> zero allocation — all lookups use pre-allocated probe keys.
   *
   * @param symbol buffer containing the symbol bytes (e.g., "EURUSD " right-padded per SBE
   *     convention)
   * @param symOff start offset of the symbol within the buffer
   * @param symLen number of bytes in the symbol (must be {@code <= 8} per SBE Symbol type)
   * @param midRate the current mid-rate in fixed-point {@code 10^-8} representation; must be
   *     positive
   * @param orderQty the order quantity in fixed-point {@code 10^-8} representation; must be
   *     positive. Used to determine quantity-dependent spread widening.
   * @param accountType the account type of the requesting counterparty — determines the tier
   *     multiplier (House, Client, or MarketMaker)
   * @param productType the product type (Spot, Forward, Swap) — reserved for future
   *     product-specific spread logic
   * @param result the mutable flyweight into which bid and offer prices are written. The caller
   *     must not read previous values after this call returns — they are overwritten.
   */
  void compute(
      DirectBuffer symbol,
      int symOff,
      int symLen,
      long midRate,
      long orderQty,
      AccountTypeEnum accountType,
      ProductTypeEnum productType,
      SpreadResult result);
}
