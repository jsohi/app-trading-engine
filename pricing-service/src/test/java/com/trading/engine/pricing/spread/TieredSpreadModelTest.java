package com.trading.engine.pricing.spread;

import static com.trading.engine.testsupport.buffer.SbeFieldUtil.spacePad;
import static com.trading.engine.testsupport.buffer.SbeFieldUtil.wrapSymbol;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.trading.engine.messages.sbe.AccountTypeEnum;
import com.trading.engine.messages.sbe.ProductTypeEnum;
import com.trading.engine.messages.util.ByteArrayKey;
import com.trading.engine.pricing.skew.InventorySkewModel;
import com.trading.engine.testsupport.buffer.SbeFieldUtil;
import org.agrona.collections.Object2ObjectHashMap;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link TieredSpreadModel}.
 *
 * <p>Validates the tiered spread computation pipeline: base bps, client-tier multiplier,
 * quantity-dependent widening, volatility pass-through, and pip rounding. All tests use a no-op
 * {@link InventorySkewModel} (returns 0 for all symbols) to isolate spread logic from skew
 * behaviour. A calm {@link VolatilityMonitor} (no registered symbols, so multiplier = 100) ensures
 * volatility does not interfere with base-spread assertions.
 */
class TieredSpreadModelTest {

  /** SBE Symbol field width. */
  private static final int SYMBOL_LENGTH = SbeFieldUtil.SYMBOL_LENGTH;

  /**
   * EUR/USD mid-rate: 1.085 in fixed-point 10^-8. Chosen because it exercises typical G10 FX
   * magnitudes and produces non-trivial spread values at 3 bps.
   */
  private static final long MID_RATE = 108_500_000L;

  /**
   * EUR/USD pip size: 0.00001 in fixed-point 10^-8 = 1_000L. Standard G10 FX pip precision used by
   * the spread model's floor/ceil rounding.
   */
  private static final long PIP_SIZE = 1_000L;

  /** Base spread: 3 basis points — typical tight G10 spot spread. */
  private static final int BASE_SPREAD_BPS = 3;

  /** Quantity threshold: 1M units in fixed-point 10^-8. */
  private static final long QTY_THRESHOLD = 1_000_000L * 100_000_000L;

  /** Max quantity multiplier: 3.00x (scaled by 100 = 300). */
  private static final int QTY_MAX_MULT = 300;

  /** Small order quantity below the threshold — should not widen the spread. */
  private static final long SMALL_QTY = 500_000L * 100_000_000L;

  /** Large order quantity above the threshold — should widen the spread. */
  private static final long LARGE_QTY = 2_000_000L * 100_000_000L;

  /** Reusable symbol buffer for EURUSD, right-padded to 8 bytes. */
  private UnsafeBuffer eurusdBuf;

  /** Pre-allocated spread result flyweight. */
  private SpreadResult result;

  /** Per-symbol config map with EURUSD entry. */
  private Object2ObjectHashMap<ByteArrayKey, SpreadConfig> symbolConfigs;

  /** Default (fallback) config. */
  private SpreadConfig defaultConfig;

  /** Default tier config: House=50, Client=100, MarketMaker=80. */
  private ClientTierConfig tierConfig;

  /** No-op skew model returning 0 adjustment for all symbols. */
  private InventorySkewModel zeroSkew;

  /**
   * Calm volatility monitor — no symbols registered, so {@code volatilityMultiplier} always returns
   * 100 (1.00x, no widening).
   */
  private VolatilityMonitor volatilityMonitor;

  @BeforeEach
  void setUp() {
    eurusdBuf = wrapSymbol("EURUSD");
    result = new SpreadResult();

    // Per-symbol config for EURUSD.
    final SpreadConfig eurusdCfg =
        new SpreadConfig(
            BASE_SPREAD_BPS,
            PIP_SIZE,
            100_000L * 100_000_000L, // minQuoteSize: 100K
            10_000_000L * 100_000_000L, // maxQuoteSize: 10M
            QTY_THRESHOLD,
            QTY_MAX_MULT);

    symbolConfigs = new Object2ObjectHashMap<>();
    final byte[] eurusdKey = spacePad("EURUSD", SbeFieldUtil.SYMBOL_LENGTH);
    symbolConfigs.put(ByteArrayKey.owned(eurusdKey, 0, eurusdKey.length), eurusdCfg);

    // Conservative defaults for unknown symbols.
    defaultConfig = SpreadConfig.defaultConfig();

    // Standard tier multipliers.
    tierConfig = ClientTierConfig.defaultConfig();

    // Zero-skew stub: no inventory position, mid-rate unshifted.
    zeroSkew = (symbol, offset, length, midRate) -> 0L;

    // Calm volatility: windowSize=2, threshold=100 bps, max=300 — but no symbols registered,
    // so volatilityMultiplier returns 100 for all queries.
    volatilityMonitor = new VolatilityMonitor(2, 100, 300);
  }

  /**
   * Spot EUR/USD with Client tier (multiplier=100) and quantity below threshold. The spread should
   * be approximately 3 bps of the mid-rate. Bid must be strictly less than mid, offer strictly
   * greater, and the total spread (offer - bid) should approximate 2 * halfSpread.
   *
   * <p>Expected half-spread calculation:
   *
   * <pre>
   *   spreadNumerator = baseSpreadBps * tierMult * qtyMult = 3 * 100 * 100 = 30_000
   *   halfSpread = mulDiv(108_500_000, 30_000, 2 * 10_000 * 100 * 100)
   *              = 108_500_000 * 30_000 / 200_000_000 = 16_275
   *   volMult step: mulDiv(16_275, 100, 100) = 16_275
   *   rawBid  = 108_500_000 - 16_275 = 108_483_725
   *   rawOffer = 108_500_000 + 16_275 = 108_516_275
   *   bid (floor to pip 1000) = 108_483_000
   *   offer (ceil to pip 1000) = 108_517_000
   * </pre>
   */
  @Test
  void compute_spotEurusd_clientTier_correctBidOffer() {
    final TieredSpreadModel model =
        new TieredSpreadModel(
            symbolConfigs, defaultConfig, tierConfig, zeroSkew, volatilityMonitor);

    model.compute(
        eurusdBuf,
        0,
        SYMBOL_LENGTH,
        MID_RATE,
        SMALL_QTY,
        AccountTypeEnum.Client,
        ProductTypeEnum.Spot,
        result);

    // Verify structural invariant: bid < mid < offer.
    assertTrue(result.bidPx < MID_RATE, "bid must be less than mid");
    assertTrue(result.offerPx > MID_RATE, "offer must be greater than mid");

    // Verify pip alignment.
    assertEquals(0L, result.bidPx % PIP_SIZE, "bid must be pip-aligned");
    assertEquals(0L, result.offerPx % PIP_SIZE, "offer must be pip-aligned");

    // Expected values from the half-spread calculation documented above.
    assertEquals(108_483_000L, result.bidPx);
    assertEquals(108_517_000L, result.offerPx);
  }

  /**
   * House tier (multiplier=50) should produce a tighter spread than Client tier (multiplier=100)
   * for the same symbol, quantity, and volatility conditions.
   */
  @Test
  void compute_houseTier_tighterSpread() {
    final TieredSpreadModel model =
        new TieredSpreadModel(
            symbolConfigs, defaultConfig, tierConfig, zeroSkew, volatilityMonitor);

    // Compute Client spread.
    model.compute(
        eurusdBuf,
        0,
        SYMBOL_LENGTH,
        MID_RATE,
        SMALL_QTY,
        AccountTypeEnum.Client,
        ProductTypeEnum.Spot,
        result);
    final long clientSpread = result.offerPx - result.bidPx;

    // Compute House spread.
    model.compute(
        eurusdBuf,
        0,
        SYMBOL_LENGTH,
        MID_RATE,
        SMALL_QTY,
        AccountTypeEnum.House,
        ProductTypeEnum.Spot,
        result);
    final long houseSpread = result.offerPx - result.bidPx;

    assertTrue(
        houseSpread < clientSpread,
        "House spread ("
            + houseSpread
            + ") must be tighter than Client spread ("
            + clientSpread
            + ")");

    // Verify House bid/offer are still valid (bid < mid < offer).
    assertTrue(result.bidPx < MID_RATE, "House bid must be less than mid");
    assertTrue(result.offerPx > MID_RATE, "House offer must be greater than mid");
  }

  /**
   * An order quantity above the symbol's quantity threshold should produce a wider spread than a
   * sub-threshold order. The quantity multiplier is:
   *
   * <pre>
   *   qtyMult = 100 + (orderQty - threshold) * 100 / threshold
   *           = 100 + (2M - 1M) * 100 / 1M = 200
   * </pre>
   *
   * which doubles the spread compared to the sub-threshold case (qtyMult=100).
   */
  @Test
  void compute_aboveQuantityThreshold_widerSpread() {
    final TieredSpreadModel model =
        new TieredSpreadModel(
            symbolConfigs, defaultConfig, tierConfig, zeroSkew, volatilityMonitor);

    // Sub-threshold.
    model.compute(
        eurusdBuf,
        0,
        SYMBOL_LENGTH,
        MID_RATE,
        SMALL_QTY,
        AccountTypeEnum.Client,
        ProductTypeEnum.Spot,
        result);
    final long narrowSpread = result.offerPx - result.bidPx;

    // Above threshold (2x threshold).
    model.compute(
        eurusdBuf,
        0,
        SYMBOL_LENGTH,
        MID_RATE,
        LARGE_QTY,
        AccountTypeEnum.Client,
        ProductTypeEnum.Spot,
        result);
    final long wideSpread = result.offerPx - result.bidPx;

    assertTrue(
        wideSpread > narrowSpread,
        "Above-threshold spread ("
            + wideSpread
            + ") must be wider than sub-threshold spread ("
            + narrowSpread
            + ")");
  }

  /**
   * Pip rounding: the bid must be rounded <b>down</b> (floor) to the nearest pip, and the offer
   * must be rounded <b>up</b> (ceil) to the nearest pip. This ensures the quoted spread is never
   * narrower than the raw computed spread — the standard FX dealer convention.
   */
  @Test
  void compute_pipRounding_bidRoundsDown_offerRoundsUp() {
    final TieredSpreadModel model =
        new TieredSpreadModel(
            symbolConfigs, defaultConfig, tierConfig, zeroSkew, volatilityMonitor);

    model.compute(
        eurusdBuf,
        0,
        SYMBOL_LENGTH,
        MID_RATE,
        SMALL_QTY,
        AccountTypeEnum.Client,
        ProductTypeEnum.Spot,
        result);

    // Both prices must be exact multiples of PIP_SIZE.
    assertEquals(0L, result.bidPx % PIP_SIZE, "bid must be rounded to pip precision");
    assertEquals(0L, result.offerPx % PIP_SIZE, "offer must be rounded to pip precision");

    // The raw half-spread = 16_275, which is not a multiple of 1_000 (pip).
    // rawBid  = 108_483_725 → floor → 108_483_000 (rounded down)
    // rawOffer = 108_516_275 → ceil  → 108_517_000 (rounded up)
    // So the rounded spread (34_000) must be >= the raw spread (2 * 16_275 = 32_550).
    final long roundedSpread = result.offerPx - result.bidPx;
    final long rawSpread = 2L * 16_275L;
    assertTrue(
        roundedSpread >= rawSpread,
        "Rounded spread (" + roundedSpread + ") must be >= raw spread (" + rawSpread + ")");
  }
}
