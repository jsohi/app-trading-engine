package com.trading.engine.fixbridge.transport;

import com.trading.engine.fixbridge.json.BrowserEvent;

/**
 * Adapter for converting cluster-side per-account risk-limit primitives into the bridge-facing
 * {@link BrowserEvent.AccountLimits} record (APP-62 §A wiring point).
 *
 * <p><b>Why a primitive-fields adapter.</b> The bridge module deliberately does NOT depend on
 * {@code :query-service} or {@code :projections} so its module graph stays small and the bridge can
 * be deployed independently of the read-side projection lifecycle. The cluster-side bridge binding
 * (in the overarching launcher) reads the projection record, extracts primitives, and passes them
 * through {@link #toBrowserLimits} to produce the wire-compatible {@link
 * BrowserEvent.AccountLimits}. This adapter is the single source of truth for the field-by-field
 * narrowing required by the {@code int} vs {@code long} mismatch on {@code priceDeviationBps} and
 * {@code maxOrdersPerSecond}.
 *
 * <p><b>Narrowing semantics.</b> The schema stores both {@code priceDeviationBps} (uint32) and
 * {@code maxOrdersPerSecond} (uint32) as 64-bit longs in the SBE codec for safety; the
 * browser-facing record carries them as Java {@code int}. Values that exceed {@link
 * Integer#MAX_VALUE} are clamped down to {@code Integer.MAX_VALUE} rather than overflowing —
 * matching CME's "sentinel-at-max" pattern for cap fields.
 *
 * <p><b>Threading.</b> Stateless — safe for unrestricted concurrent use.
 *
 * <p><b>Allocation.</b> One {@link BrowserEvent.AccountLimits} per call. Acceptable on the cold
 * auth path (see {@link ClusterAccountLimitsProvider} caching contract).
 *
 * <p><b>Wiring status (APP-62 R11 MEDIUM Agent B #1).</b> This adapter and its companion {@link
 * ClusterAccountLimitsProvider} are NOT yet invoked from any production code path. The natural call
 * site is a top-level launcher binding that:
 *
 * <ol>
 *   <li>constructs the {@code :projections} {@code RiskLimitProjection} (consumes template 115
 *       {@code RiskLimitChangedEvent}),
 *   <li>wraps it behind a {@code :query-service} {@code QueryService} ({@code getAccountLimits}
 *       binding target), and
 *   <li>injects {@code QueryService::getAccountLimits} into {@link ClusterAccountLimitsProvider} so
 *       the bridge's JWT-cold-path auth handler can fetch live per-account limits without enlarging
 *       the bridge module's compile-time graph to depend on {@code :query-service} or {@code
 *       :projections} directly.
 * </ol>
 *
 * <p>This indirection is intentional: it keeps the bridge module independently deployable. The
 * launcher binding is tracked under TODO(APP-62) below; until it lands, all bridge auth flows fall
 * through to the in-memory default limits provider.
 */
// TODO(APP-62): wire RiskLimitToBrowserAdapter into the production auth path by binding
//   QueryService::getAccountLimits in the top-level launcher to ClusterAccountLimitsProvider.
//   Tracked alongside the RiskLimitProjection consumer wiring (see fix-client-bridge module
//   dependency boundary note above).
public final class RiskLimitToBrowserAdapter {

  private RiskLimitToBrowserAdapter() {}

  /**
   * Convert per-account primitives to the bridge-facing {@link BrowserEvent.AccountLimits} record.
   *
   * @param accountCode the FIX-style account code (tag 1); must not be null
   * @param maxOrderSize max single-order quantity in fixed-point 10⁻⁸
   * @param maxOrderNotional max single-order notional in fixed-point 10⁻⁸
   * @param priceDeviationBps max allowed price deviation from last, in basis points (uint32 →
   *     narrowed to int, clamped at {@link Integer#MAX_VALUE})
   * @param maxOrdersPerSecond per-account command rate cap (uint32 → narrowed to int, clamped at
   *     {@link Integer#MAX_VALUE})
   * @return a fresh {@link BrowserEvent.AccountLimits} carrying these values
   */
  public static BrowserEvent.AccountLimits toBrowserLimits(
      final String accountCode,
      final long maxOrderSize,
      final long maxOrderNotional,
      final long priceDeviationBps,
      final long maxOrdersPerSecond) {
    return new BrowserEvent.AccountLimits(
        accountCode,
        maxOrderSize,
        maxOrderNotional,
        clampToInt(priceDeviationBps),
        clampToInt(maxOrdersPerSecond));
  }

  /**
   * Clamp a non-negative long to fit in {@code int}, capping at {@link Integer#MAX_VALUE} when the
   * source exceeds the int range. Mirrors the CME-style "max-sentinel" pattern: a cap value that
   * overflows int is still semantically "very large" rather than wrapping negative.
   *
   * @param value the long input (non-negative caller invariant)
   * @return the clamped int
   */
  private static int clampToInt(final long value) {
    if (value < 0L) {
      return 0;
    }
    if (value > Integer.MAX_VALUE) {
      return Integer.MAX_VALUE;
    }
    return (int) value;
  }
}
