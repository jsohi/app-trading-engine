package com.trading.engine.pricing.skew;

import com.epam.deltix.gflog.api.Log;
import com.epam.deltix.gflog.api.LogFactory;
import com.trading.engine.pricing.PricingMath;
import org.agrona.DirectBuffer;

/**
 * Convex (quadratic) inventory skew model for production FX market-making.
 *
 * <p><b>Why convex over linear?</b> In FX market-making, a linear skew function produces a constant
 * marginal penalty per unit of inventory, which means the dealer is equally reluctant to accumulate
 * the first lot and the hundredth lot. In practice, risk grows non-linearly: concentration risk,
 * margin requirements, and potential gap-move losses all accelerate as inventory builds. A convex
 * (quadratic) skew function captures this by making the mid-shift <em>gentle</em> at small
 * inventory and <em>aggressive</em> near the position threshold. This is the standard approach used
 * by Citadel Securities, XTX Markets, and most institutional FX market-makers — see Gueant, Lehalle
 * &amp; Fernandez-Tapia (2013), "Dealing with the Inventory Risk".
 *
 * <p><b>Skew formula.</b> For a dealer with net position {@code P}, position threshold {@code T},
 * and maximum skew in basis points {@code S}:
 *
 * <pre>
 *   clampedPos = clamp(P, -T, +T)
 *   skewBps    = sign(clampedPos) * S * (|clampedPos| / T)^alpha
 *   adjustment = midRate * skewBps / 10,000
 * </pre>
 *
 * <p>The sign convention follows the dealer-risk model: when the dealer is <em>long</em> (positive
 * position), the adjustment is <em>negative</em> (shift mid down to attract sellers). When the
 * dealer is <em>short</em> (negative position), the adjustment is <em>positive</em> (shift mid up
 * to attract buyers). Note that the formula naturally produces this: a positive {@code clampedPos}
 * yields a positive {@code skewBps}, but because a long position should push the mid <em>down</em>,
 * the final adjustment is negated. The negation is folded into the sign of the {@code skewBps *
 * sign} product passed to the final {@code mulDiv} — see the implementation for details.
 *
 * <p><b>v1 limitation — quadratic only.</b> The {@code alphaX100} constructor parameter is stored
 * for forward compatibility but the current implementation hard-codes the quadratic formula ({@code
 * alpha = 2}). Generalising to arbitrary power exponents requires either a lookup table for {@code
 * pow(x, alpha)} or a fixed-point exponentiation routine, both of which add complexity without
 * clear product need today. The parameter is validated and stored so that extending to arbitrary
 * alpha later is a backward-compatible change.
 *
 * <p><b>Overflow safety.</b> The intermediate computation uses two chained {@link
 * PricingMath#mulDiv(long, long, long)} calls to avoid {@code long} overflow. The first normalises
 * the squared position by the threshold; the second scales by {@code maxSkewBps}. Each intermediate
 * result stays well within {@code long} range for realistic FX positions (see {@link PricingMath}
 * overflow proof for details).
 *
 * <p><b>Threading model.</b> <b>Not thread-safe.</b> All methods are invoked exclusively from the
 * pricing-service agent's single-threaded duty cycle.
 *
 * <p><b>Allocation behaviour.</b> <b>Zero-allocation after construction.</b> The {@link
 * #skewAdjustment} method performs only integer arithmetic via {@link PricingMath#mulDiv} and
 * delegates the position lookup to the injected {@link PositionSource}.
 *
 * @see LinearInventorySkew
 * @see InventorySkewModel
 * @see PositionSource
 * @see PricingMath#mulDiv(long, long, long)
 */
public final class ConvexInventorySkew implements InventorySkewModel {

  private static final Log LOG = LogFactory.getLog(ConvexInventorySkew.class);

  /**
   * Basis-point divisor for converting bps to a unit fraction. {@code 1 bp = 1/10,000 = 0.0001}.
   */
  private static final long BPS_DIVISOR = 10_000L;

  /** Source of per-symbol net position data. Queried on every skew calculation. */
  private final PositionSource positionSource;

  /**
   * Maximum skew in basis points, applied when the position is at (or beyond) the threshold. For
   * example, {@code 50} means the mid-rate can shift by up to 50 bps at full inventory.
   */
  private final int maxSkewBps;

  /**
   * Position threshold in fixed-point {@code 10^-8}. Positions beyond this magnitude are clamped,
   * so the skew never exceeds {@link #maxSkewBps}. Represents the maximum position the desk is
   * willing to hold before applying full skew.
   */
  private final long positionThreshold;

  /**
   * Convexity exponent scaled by 100. {@code 200} means {@code alpha = 2.0} (quadratic). Stored for
   * future generalisation; the current implementation only supports {@code alpha = 2}.
   */
  private final int alphaX100;

  /**
   * Constructs a convex inventory skew model with the given risk parameters.
   *
   * <p><b>Parameter guidelines (typical FX market-making):</b>
   *
   * <ul>
   *   <li>{@code maxSkewBps = 30..100} — aggressive desks use lower values; conservative desks use
   *       higher values
   *   <li>{@code positionThreshold} — in fixed-point {@code 10^-8}; e.g., 10,000,000 EUR notional =
   *       {@code 10_000_000L * 100_000_000L}
   *   <li>{@code alphaX100 = 200} — quadratic (only supported value in v1)
   * </ul>
   *
   * @param positionSource source of per-symbol net position data; must not be {@code null}
   * @param maxSkewBps maximum skew in basis points at full inventory; must be {@code > 0}
   * @param positionThreshold position magnitude at which full skew is applied, in fixed-point
   *     {@code 10^-8}; must be {@code > 0}
   * @param alphaX100 convexity exponent multiplied by 100 (e.g., 200 for quadratic). Stored for
   *     future use; current implementation uses quadratic regardless of this value. Must be {@code
   *     > 0}.
   * @throws IllegalArgumentException if any parameter violates its constraints
   * @throws NullPointerException if {@code positionSource} is {@code null}
   */
  public ConvexInventorySkew(
      final PositionSource positionSource,
      final int maxSkewBps,
      final long positionThreshold,
      final int alphaX100) {

    if (positionSource == null) {
      throw new NullPointerException("positionSource must not be null");
    }
    if (maxSkewBps <= 0) {
      throw new IllegalArgumentException("maxSkewBps must be > 0, got: " + maxSkewBps);
    }
    if (positionThreshold <= 0) {
      throw new IllegalArgumentException(
          "positionThreshold must be > 0, got: " + positionThreshold);
    }
    if (alphaX100 <= 0) {
      throw new IllegalArgumentException("alphaX100 must be > 0, got: " + alphaX100);
    }
    if (alphaX100 != 200) {
      LOG.warn()
          .append("ConvexInventorySkew: alphaX100=")
          .append(alphaX100)
          .append(" but only quadratic (alpha=2.0, alphaX100=200) is implemented; ")
          .append("the supplied value will be stored but ignored in v1")
          .commit();
    }

    this.positionSource = positionSource;
    this.maxSkewBps = maxSkewBps;
    this.positionThreshold = positionThreshold;
    this.alphaX100 = alphaX100;
  }

  /**
   * {@inheritDoc}
   *
   * <p>Computes a quadratic (convex) skew adjustment based on the dealer's net position in the
   * given symbol. The adjustment magnitude grows quadratically with position size, clamped at
   * {@code maxSkewBps} when the position reaches or exceeds {@code positionThreshold}.
   *
   * <p><b>Computation steps:</b>
   *
   * <ol>
   *   <li>Query the {@link PositionSource} for the net position
   *   <li>Short-circuit to {@code 0} if flat (no position)
   *   <li>Clamp the position to {@code [-threshold, +threshold]}
   *   <li>Compute normalised squared ratio: {@code absPos^2 / threshold^2} using two-step {@code
   *       mulDiv} to avoid overflow
   *   <li>Scale by {@code maxSkewBps} and convert from bps to price-space
   *   <li>Apply sign: long position yields negative adjustment (shift mid down)
   * </ol>
   *
   * <p><b>Allocation:</b> zero-allocation. All arithmetic is performed via {@link
   * PricingMath#mulDiv(long, long, long)}.
   */
  @Override
  public long skewAdjustment(
      final DirectBuffer symbol, final int offset, final int length, final long midRate) {

    final long netPos = positionSource.netPosition(symbol, offset, length);
    if (netPos == 0) {
      return 0;
    }

    // Clamp the position to the configured threshold range.
    final long clampedPos = Math.max(-positionThreshold, Math.min(netPos, positionThreshold));

    // Factor out the sign. Long position (positive) produces negative skew (shift mid down).
    // The negation is applied here: sign = -1 for long, +1 for short.
    final long sign = clampedPos >= 0 ? -1 : 1;
    final long absPos = Math.abs(clampedPos);

    // Quadratic (alpha=2): skewBps = maxSkewBps * absPos^2 / threshold^2
    //
    // To avoid overflow of absPos * absPos for large positions, we split into two mulDiv
    // steps. Each intermediate result stays bounded:
    //   step 1: normalizedSquared = absPos * absPos / positionThreshold  (units: position)
    //   step 2: skewBps = normalizedSquared * maxSkewBps / positionThreshold  (units: bps)
    final long normalizedSquared = PricingMath.mulDiv(absPos, absPos, positionThreshold);
    final long skewBps = PricingMath.mulDiv(normalizedSquared, maxSkewBps, positionThreshold);

    // Convert bps adjustment to fixed-point price delta:
    //   adjustment = midRate * (skewBps * sign) / 10,000
    return PricingMath.mulDiv(midRate, skewBps * sign, BPS_DIVISOR);
  }
}
