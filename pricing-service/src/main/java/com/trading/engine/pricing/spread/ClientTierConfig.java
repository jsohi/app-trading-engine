package com.trading.engine.pricing.spread;

import com.trading.engine.messages.sbe.AccountTypeEnum;

/**
 * Per-{@link AccountTypeEnum} spread multiplier configuration for a single-dealer platform.
 *
 * <p>In a single-dealer model the proprietary desk (House) trades at tighter spreads than external
 * clients, reflecting the absence of counterparty credit risk and the desire to encourage internal
 * hedging flow. Market makers receive an intermediate spread to incentivize liquidity provision.
 * This mirrors the tiered pricing structures used on platforms such as EBS Direct, Currenex, and
 * proprietary bank single-dealer portals.
 *
 * <h3>Multiplier convention</h3>
 *
 * <p>Multipliers are stored as integers scaled by 100 (i.e., 100 = 1.00x, 50 = 0.50x, 150 = 1.50x).
 * This avoids floating-point arithmetic on the pricing hot path. The multiplier is applied to the
 * base spread in {@link TieredSpreadModel#compute}: a House multiplier of 50 halves the base
 * spread, while a Client multiplier of 100 applies it at face value.
 *
 * <h3>Implementation</h3>
 *
 * <p>The multipliers are stored in a flat {@code int[]} indexed by {@link AccountTypeEnum#value()},
 * providing O(1) array lookup with no hashing or branching. The array is sized to accommodate all
 * valid enum ordinals (0-2); the SBE {@code NULL_VAL} (255) is not stored and will cause an {@link
 * ArrayIndexOutOfBoundsException} if passed, which is the correct fail-fast behavior for an invalid
 * account type on the pricing path.
 *
 * <h3>Threading model</h3>
 *
 * <p>Effectively immutable after construction — all fields are set in the constructor and never
 * modified. Safe for concurrent reads from any thread, though in practice only accessed from the
 * single-threaded pricing-service duty cycle.
 *
 * <h3>Allocation behaviour</h3>
 *
 * <p>Zero allocation after construction. {@link #multiplier(AccountTypeEnum)} performs a single
 * array read.
 *
 * @see TieredSpreadModel
 * @see AccountTypeEnum
 */
public final class ClientTierConfig {

  /**
   * Number of valid (non-NULL_VAL) account type ordinals. Matches the SBE enum: House(0),
   * Client(1), MarketMaker(2).
   */
  private static final int ACCOUNT_TYPE_COUNT = 3;

  /**
   * Multipliers indexed by {@link AccountTypeEnum#value()}. Each entry is an integer scaled by 100
   * (e.g., 50 = 0.50x, 100 = 1.00x).
   */
  private final int[] multipliers;

  /**
   * Constructs a tier configuration with explicit multipliers for each account type.
   *
   * @param houseMultiplier spread multiplier for House accounts (x100); e.g., 50 = 0.50x
   * @param clientMultiplier spread multiplier for Client accounts (x100); e.g., 100 = 1.00x
   * @param marketMakerMultiplier spread multiplier for MarketMaker accounts (x100); e.g., 80 =
   *     0.80x
   */
  public ClientTierConfig(
      final int houseMultiplier, final int clientMultiplier, final int marketMakerMultiplier) {
    this.multipliers = new int[ACCOUNT_TYPE_COUNT];
    this.multipliers[AccountTypeEnum.House.value()] = houseMultiplier;
    this.multipliers[AccountTypeEnum.Client.value()] = clientMultiplier;
    this.multipliers[AccountTypeEnum.MarketMaker.value()] = marketMakerMultiplier;
  }

  /**
   * Returns the spread multiplier for the given account type as an integer scaled by 100.
   *
   * <p><b>Allocation:</b> zero allocation — single array read.
   *
   * @param type the account type to look up; must not be {@link AccountTypeEnum#NULL_VAL}
   * @return the multiplier scaled by 100 (e.g., 50 for House, 100 for Client, 80 for MarketMaker)
   * @throws ArrayIndexOutOfBoundsException if {@code type} is {@link AccountTypeEnum#NULL_VAL} or
   *     otherwise out of range — fail-fast for invalid input
   */
  public int multiplier(final AccountTypeEnum type) {
    return multipliers[type.value()];
  }

  /**
   * Returns a default tier configuration with standard single-dealer-platform multipliers:
   *
   * <ul>
   *   <li>House: 50 (0.50x) — proprietary desk, tightest spreads
   *   <li>Client: 100 (1.00x) — full base spread
   *   <li>MarketMaker: 80 (0.80x) — intermediate, incentivizing liquidity provision
   * </ul>
   *
   * @return a new default ClientTierConfig instance
   */
  public static ClientTierConfig defaultConfig() {
    return new ClientTierConfig(50, 100, 80);
  }
}
