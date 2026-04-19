package com.trading.engine.pricing.spread;

import static com.trading.engine.pricing.PricingMath.mulDiv;

import com.trading.engine.messages.sbe.AccountTypeEnum;
import com.trading.engine.messages.sbe.ProductTypeEnum;
import com.trading.engine.messages.util.ByteArrayKey;
import com.trading.engine.pricing.skew.InventorySkewModel;
import org.agrona.DirectBuffer;
import org.agrona.collections.Object2ObjectHashMap;

/**
 * Production {@link SpreadModel} implementation that computes two-sided bid/offer prices using a
 * mid-shift skew model with tiered, quantity-dependent, and volatility-responsive spread widening.
 *
 * <h3>Mid-shift skew model (EBS / Currenex convention)</h3>
 *
 * <p>Rather than adjusting bid and offer independently, this model first shifts the mid-rate toward
 * inventory risk reduction (via the {@link InventorySkewModel}), then applies a symmetric
 * half-spread around the shifted mid. This is the convention used on single-dealer platforms and
 * ECNs such as EBS Direct and Currenex, where the dealer's inventory position biases the mid-point
 * while keeping the spread symmetric.
 *
 * <h3>Spread computation</h3>
 *
 * <p>The half-spread is computed from four multiplicative factors:
 *
 * <ol>
 *   <li><b>Base spread</b> — per-symbol configuration in basis points ({@link
 *       SpreadConfig#baseSpreadBps})
 *   <li><b>Tier multiplier</b> — per-{@link AccountTypeEnum} factor from {@link ClientTierConfig}
 *       (e.g., House=0.50x, Client=1.00x, MarketMaker=0.80x)
 *   <li><b>Quantity multiplier</b> — linear widening above a per-symbol quantity threshold ({@link
 *       SpreadConfig#quantityThreshold})
 *   <li><b>Volatility multiplier</b> — range-based widening from {@link VolatilityMonitor}
 * </ol>
 *
 * <p>All multipliers are integers scaled by 100 to avoid floating-point arithmetic. The
 * intermediate multiplication is split across two {@link
 * com.trading.engine.pricing.PricingMath#mulDiv} calls to stay within signed {@code long} overflow
 * bounds:
 *
 * <pre>
 *   spreadNumerator = baseSpreadBps * tierMult * qtyMult        // max ~10^8, fits int
 *   halfSpread = mulDiv(midRate, spreadNumerator, 2 * 10_000 * 100 * 100)
 *   halfSpread = mulDiv(halfSpread, volMult, 100)               // apply volatility separately
 * </pre>
 *
 * <h3>Pip rounding</h3>
 *
 * <p>After computing the raw bid and offer prices, both are rounded to the symbol's pip precision
 * ({@link SpreadConfig#pipSize}). The bid is rounded <b>down</b> (toward negative infinity) and the
 * offer is rounded <b>up</b> (toward positive infinity), ensuring the quoted spread is never
 * narrower than the raw computed spread. This is the standard rounding convention for dealer quotes
 * in FX markets.
 *
 * <h3>Threading model</h3>
 *
 * <p><b>Not thread-safe.</b> The pre-allocated probe key ({@link #probeKey}) is mutated in place on
 * each call. All usage is from the pricing-service agent's single-threaded duty cycle.
 *
 * <h3>Allocation behaviour</h3>
 *
 * <p><b>Zero allocation after construction.</b> Symbol config lookups use the reusable {@link
 * #probeKey}. All arithmetic is performed on primitive {@code long} and {@code int} values. The
 * {@link SpreadResult} output parameter is caller-owned and pre-allocated.
 *
 * @see SpreadModel
 * @see SpreadConfig
 * @see ClientTierConfig
 * @see VolatilityMonitor
 * @see InventorySkewModel
 */
public final class TieredSpreadModel implements SpreadModel {

  /** Maximum byte length of an SBE Symbol field. */
  private static final int SYMBOL_LENGTH = 8;

  /**
   * Combined divisor for the first mulDiv step: {@code 2 (half-spread) * 10_000 (bps→decimal) * 100
   * (tier scale) * 100 (qty scale)}.
   */
  private static final long SPREAD_DIVISOR = 2L * 10_000L * 100L * 100L;

  /** Divisor for the second mulDiv step that applies the volatility multiplier. */
  private static final long VOL_DIVISOR = 100L;

  /** Per-symbol spread configurations keyed by owned ByteArrayKey. */
  private final Object2ObjectHashMap<ByteArrayKey, SpreadConfig> symbolConfigs;

  /** Fallback configuration for symbols not present in {@link #symbolConfigs}. */
  private final SpreadConfig defaultConfig;

  /** Per-account-type spread multipliers. */
  private final ClientTierConfig tierConfig;

  /** Inventory-based mid-rate skew calculator. */
  private final InventorySkewModel skewModel;

  /** Rolling-window volatility tracker for spread widening during volatile conditions. */
  private final VolatilityMonitor volatilityMonitor;

  /**
   * Reusable probe key for zero-allocation symbol lookups in {@link #symbolConfigs}. Mutated in
   * place on each {@link #compute} call — never inserted into any map.
   */
  private final ByteArrayKey probeKey = ByteArrayKey.emptyForLookup(SYMBOL_LENGTH);

  /**
   * Constructs a tiered spread model with the given configuration and collaborators.
   *
   * @param symbolConfigs per-symbol spread configurations; keys are owned ByteArrayKeys. The map is
   *     retained by reference — the caller must not modify it after construction.
   * @param defaultConfig fallback spread configuration for symbols not in {@code symbolConfigs};
   *     must not be null
   * @param tierConfig per-account-type spread multipliers; must not be null
   * @param skewModel inventory skew calculator for mid-rate shifting; must not be null
   * @param volatilityMonitor rolling-window volatility tracker; must not be null
   */
  public TieredSpreadModel(
      final Object2ObjectHashMap<ByteArrayKey, SpreadConfig> symbolConfigs,
      final SpreadConfig defaultConfig,
      final ClientTierConfig tierConfig,
      final InventorySkewModel skewModel,
      final VolatilityMonitor volatilityMonitor) {
    this.symbolConfigs = symbolConfigs;
    this.defaultConfig = defaultConfig;
    this.tierConfig = tierConfig;
    this.skewModel = skewModel;
    this.volatilityMonitor = volatilityMonitor;
  }

  /**
   * {@inheritDoc}
   *
   * <p>Computes bid/offer prices using the mid-shift skew convention:
   *
   * <ol>
   *   <li>Look up per-symbol config (falling back to {@link #defaultConfig})
   *   <li>Compute the half-spread from base bps, tier, quantity, and volatility multipliers
   *   <li>Query the inventory skew model for a mid-rate adjustment
   *   <li>Shift the mid-rate by the skew adjustment (toward risk reduction)
   *   <li>Apply the symmetric half-spread around the shifted mid
   *   <li>Round bid down and offer up to pip precision
   * </ol>
   *
   * <p><b>Allocation:</b> zero allocation.
   */
  @Override
  public void compute(
      final DirectBuffer symbol,
      final int symOff,
      final int symLen,
      final long midRate,
      final long orderQty,
      final AccountTypeEnum accountType,
      final ProductTypeEnum productType,
      final SpreadResult result) {

    // 1. Symbol config lookup (zero-alloc via probe key).
    probeKey.set(symbol, symOff, symLen);
    SpreadConfig cfg = symbolConfigs.get(probeKey);
    if (cfg == null) {
      cfg = defaultConfig;
    }

    // 2. Gather multipliers (all ×100 integer values).
    final int tierMult = tierConfig.multiplier(accountType);
    final int qtyMult = quantityMultiplier(orderQty, cfg);
    final int volMult = volatilityMonitor.volatilityMultiplier(symbol, symOff, symLen);

    // 3. Compute half-spread in two overflow-safe mulDiv steps.
    //    spreadNumerator = baseSpreadBps * tierMult * qtyMult  (max ~10^8, fits long easily)
    //    halfSpread = mulDiv(midRate, spreadNumerator, 2 * 10_000 * 100 * 100)
    //    halfSpread = mulDiv(halfSpread, volMult, 100)
    final long spreadNumerator = (long) cfg.baseSpreadBps * (long) tierMult * (long) qtyMult;
    long halfSpread = mulDiv(midRate, spreadNumerator, SPREAD_DIVISOR);
    halfSpread = mulDiv(halfSpread, volMult, VOL_DIVISOR);

    // 4. Apply inventory skew: shift mid toward risk reduction.
    //    InventorySkewModel contract: positive adjustment = dealer is short → shift mid UP;
    //    negative adjustment = dealer is long → shift mid DOWN. Addition matches the interface.
    final long skewAdj = skewModel.skewAdjustment(symbol, symOff, symLen, midRate);
    final long skewedMid = midRate + skewAdj;

    // 5. Compute raw bid and offer around the skewed mid.
    final long rawBid = skewedMid - halfSpread;
    final long rawOffer = skewedMid + halfSpread;

    // 6. Round to pip precision: bid DOWN (toward negative infinity), offer UP.
    // Math.floorDiv is correct for negative rawBid (truncation toward zero would round up).
    final long pip = cfg.pipSize;
    final long bidPx = Math.floorDiv(rawBid, pip) * pip;
    final long offerPx = Math.ceilDiv(rawOffer, pip) * pip;

    result.set(bidPx, offerPx);
  }

  /**
   * Computes the quantity-dependent spread multiplier as an integer scaled by 100.
   *
   * <p>For order quantities at or below the symbol's quantity threshold, returns 100 (1.00x — no
   * widening). Above the threshold, the multiplier increases linearly:
   *
   * <pre>
   *   multiplier = 100 + (orderQty - threshold) * 100 / threshold
   * </pre>
   *
   * capped at the symbol's configured maximum ({@link SpreadConfig#quantityMaxMultiplier}).
   *
   * <p>All arithmetic is integer-only. The linear formula ensures that an order at 2x the threshold
   * receives a multiplier of 200 (2.00x), at 3x threshold 300 (3.00x), and so on, up to the cap.
   *
   * <p><b>Allocation:</b> zero allocation.
   *
   * @param orderQty the order quantity in fixed-point {@code 10^-8}; must be positive
   * @param cfg the per-symbol spread configuration providing threshold and max multiplier
   * @return multiplier scaled by 100; at least 100, at most {@link
   *     SpreadConfig#quantityMaxMultiplier}
   */
  private static int quantityMultiplier(final long orderQty, final SpreadConfig cfg) {
    // Clamp orderQty to maxQuoteSize to prevent overflow in the widening calculation.
    // Orders exceeding maxQuoteSize should be rejected upstream, but defensive clamping
    // ensures the spread math never produces inverted bid/offer from overflow.
    final long clampedQty = Math.min(orderQty, cfg.maxQuoteSize);
    if (clampedQty <= cfg.quantityThreshold) {
      return 100;
    }
    final long excess = clampedQty - cfg.quantityThreshold;
    final long widening = mulDiv(excess, 100, cfg.quantityThreshold);
    return (int) Math.min(100L + widening, cfg.quantityMaxMultiplier);
  }
}
