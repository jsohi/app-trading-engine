package com.trading.engine.pricing.skew;

import com.trading.engine.pricing.PricingMath;
import org.agrona.DirectBuffer;

/**
 * Simple linear inventory skew model, primarily intended for testing and development.
 *
 * <p>Applies a mid-rate adjustment that scales <em>linearly</em> with the dealer's net position.
 * The maximum adjustment (in basis points) is reached when the position hits the configured
 * threshold. Positions beyond the threshold are clamped, so the skew never exceeds {@code
 * maxSkewBps}.
 *
 * <p><b>Skew formula.</b> For a dealer with net position {@code P}, position threshold {@code T},
 * and maximum skew in basis points {@code S}:
 *
 * <pre>
 *   clampedPos = clamp(P, -T, +T)
 *   skewBps    = S * clampedPos / T
 *   adjustment = midRate * skewBps / 10,000
 * </pre>
 *
 * <p>The sign convention follows the dealer-risk model: when the dealer is <em>long</em> (positive
 * position), the adjustment is <em>negative</em> (shift mid down to attract sellers). When the
 * dealer is <em>short</em> (negative position), the adjustment is <em>positive</em> (shift mid up
 * to attract buyers). The negation is applied after computing the raw skew bps.
 *
 * <p><b>Linear vs. convex.</b> A linear skew function applies the same marginal penalty per unit of
 * inventory, which may not adequately penalise large concentrations. For production FX
 * market-making, prefer {@link ConvexInventorySkew} which applies increasing pressure as inventory
 * grows. The linear model is useful for:
 *
 * <ul>
 *   <li>Unit testing the skew pipeline with predictable, easy-to-verify arithmetic
 *   <li>Back-testing and comparison against the convex model
 *   <li>Instruments where the risk profile is genuinely linear (e.g., low-volatility government
 *       bonds)
 * </ul>
 *
 * <p><b>Threading model.</b> <b>Not thread-safe.</b> All methods are invoked exclusively from the
 * pricing-service agent's single-threaded duty cycle.
 *
 * <p><b>Allocation behaviour.</b> <b>Zero-allocation after construction.</b> The {@link
 * #skewAdjustment} method performs only integer arithmetic via {@link PricingMath#mulDiv} and
 * delegates the position lookup to the injected {@link PositionSource}.
 *
 * @see ConvexInventorySkew
 * @see InventorySkewModel
 * @see PositionSource
 * @see PricingMath#mulDiv(long, long, long)
 */
public final class LinearInventorySkew implements InventorySkewModel {

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
   * Constructs a linear inventory skew model with the given risk parameters.
   *
   * <p><b>Parameter guidelines:</b>
   *
   * <ul>
   *   <li>{@code maxSkewBps = 10..50} — lower than convex because linear skew already applies full
   *       marginal penalty from the first unit of inventory
   *   <li>{@code positionThreshold} — in fixed-point {@code 10^-8}; e.g., 5,000,000 EUR notional =
   *       {@code 5_000_000L * 100_000_000L}
   * </ul>
   *
   * @param positionSource source of per-symbol net position data; must not be {@code null}
   * @param maxSkewBps maximum skew in basis points at full inventory; must be {@code > 0}
   * @param positionThreshold position magnitude at which full skew is applied, in fixed-point
   *     {@code 10^-8}; must be {@code > 0}
   * @throws IllegalArgumentException if {@code maxSkewBps} or {@code positionThreshold} is not
   *     positive
   * @throws NullPointerException if {@code positionSource} is {@code null}
   */
  public LinearInventorySkew(
      final PositionSource positionSource, final int maxSkewBps, final long positionThreshold) {

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

    this.positionSource = positionSource;
    this.maxSkewBps = maxSkewBps;
    this.positionThreshold = positionThreshold;
  }

  /**
   * {@inheritDoc}
   *
   * <p>Computes a linear skew adjustment based on the dealer's net position in the given symbol.
   * The adjustment magnitude scales linearly from zero (flat) to {@code maxSkewBps} (at threshold).
   *
   * <p><b>Computation steps:</b>
   *
   * <ol>
   *   <li>Query the {@link PositionSource} for the net position
   *   <li>Short-circuit to {@code 0} if flat (no position)
   *   <li>Clamp the position to {@code [-threshold, +threshold]}
   *   <li>Compute linear skew bps: {@code maxSkewBps * clampedPos / threshold}
   *   <li>Negate: long position yields negative adjustment (shift mid down)
   *   <li>Convert from bps to price-space: {@code midRate * skewBps / 10,000}
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

    // Linear: skewBps = maxSkewBps * clampedPos / positionThreshold
    // The sign of clampedPos propagates through mulDiv, giving positive skewBps for long
    // and negative for short.
    final long skewBps = PricingMath.mulDiv(clampedPos, maxSkewBps, positionThreshold);

    // Negate: long position (positive skewBps) produces negative adjustment (shift mid down).
    // Convert bps to fixed-point price delta:
    //   adjustment = midRate * (-skewBps) / 10,000
    return PricingMath.mulDiv(midRate, -skewBps, BPS_DIVISOR);
  }
}
