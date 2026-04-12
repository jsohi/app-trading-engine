package com.trading.engine.pricing.skew;

import org.agrona.DirectBuffer;

/**
 * Service-provider interface (SPI) for querying the dealer's aggregate net position per symbol.
 *
 * <p>The inventory skew layer needs to know the dealer's current net exposure in each symbol to
 * compute a mid-rate adjustment that incentivises risk-reducing flow. This interface decouples the
 * skew calculation from the source of position data, allowing different implementations for
 * testing, static configuration, and live position feeds.
 *
 * <p><b>Position sign convention.</b> Net position follows the standard FX market-making
 * convention:
 *
 * <ul>
 *   <li><b>Positive</b> — the dealer is <em>long</em> the base currency (has bought more than sold)
 *   <li><b>Negative</b> — the dealer is <em>short</em> the base currency (has sold more than
 *       bought)
 *   <li><b>Zero</b> — flat; no open position and no skew should be applied
 * </ul>
 *
 * <p><b>Fixed-point representation.</b> All position values are in the engine's standard
 * fixed-point format: {@code long} with implicit scale {@code 10^-8} (i.e., multiply the real-unit
 * quantity by {@link com.trading.engine.messages.FixedPointScale#PRICE_SCALE PRICE_SCALE} to obtain
 * the stored value).
 *
 * <p><b>Planned integration (APP-30).</b> In the initial implementation the position source is
 * seeded from static configuration or test fixtures via {@link ConfigurablePositionSource}. Once
 * the orchestrator (APP-30) is wired to publish fill events, a live implementation will subscribe
 * to the Aeron IPC fill stream and maintain real-time aggregate positions.
 *
 * <p><b>Threading model.</b> Implementations are <b>single-threaded</b>. All methods are invoked
 * exclusively from the pricing-service agent's duty cycle — no synchronisation is required or
 * provided.
 *
 * <p><b>Allocation behaviour.</b> Implementations must be <b>zero-allocation after
 * construction</b>. All internal buffers, caches, and probe keys are pre-allocated at startup; the
 * {@link #netPosition} method must not allocate on the heap.
 *
 * @see ConfigurablePositionSource
 * @see InventorySkewModel
 * @see com.trading.engine.messages.FixedPointScale#PRICE_SCALE
 */
public interface PositionSource {

  /**
   * Returns the dealer's aggregate net position for the given symbol in fixed-point {@code 10^-8}
   * representation.
   *
   * <p>The returned value represents the net base-currency quantity across all accounts and venues.
   * A positive value means the dealer is long; negative means short; zero means flat (or symbol is
   * unknown).
   *
   * <p><b>Unknown symbols:</b> implementations must return {@code 0} for symbols that have no
   * position data. This ensures the skew layer applies no adjustment for unconfigured instruments,
   * which is the safe default.
   *
   * <p><b>Allocation:</b> zero-allocation — implementations use pre-allocated probe keys for
   * internal map lookups.
   *
   * @param symbol buffer containing the symbol bytes (8-byte fixed-width SBE {@code Symbol} type)
   * @param offset start offset of the symbol within the buffer
   * @param length number of bytes to read (always 8 for the SBE Symbol type)
   * @return aggregate net position in fixed-point {@code 10^-8}. Positive = long, negative = short,
   *     zero = flat or unknown symbol.
   */
  long netPosition(DirectBuffer symbol, int offset, int length);
}
