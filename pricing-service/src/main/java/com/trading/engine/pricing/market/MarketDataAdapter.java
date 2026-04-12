package com.trading.engine.pricing.market;

import org.agrona.DirectBuffer;
import org.agrona.concurrent.Agent;

/**
 * Service-provider interface (SPI) for sourcing spot mid-rates into the pricing service.
 *
 * <p>Each implementation wraps a different market data source — deterministic fixtures for
 * integration tests, synthetic Brownian-motion prices for dev/staging, and (eventually) a real
 * market data feed for production. The pricing service selects the adapter at startup based on
 * configuration and plugs it into the {@link io.aeron.driver.MediaDriver}-side {@link
 * org.agrona.concurrent.AgentRunner} composite duty cycle.
 *
 * <h3>Threading model</h3>
 *
 * <p>Implementations are <b>single-threaded</b>. The adapter is invoked exclusively from the Aeron
 * {@link Agent} duty cycle ({@link #doWork()} on the agent thread). Both {@link #midRate} and
 * {@link #lastUpdateNanos} are called from the same thread (the pricing-service agent) between
 * duty-cycle ticks, so no synchronisation is required.
 *
 * <p><b>Single-writer principle:</b> only the adapter writes mid-rates into its internal {@link
 * MidRateCache}. The pricing service reads from the cache on the same thread — there is exactly one
 * writer and one reader, and they never execute concurrently.
 *
 * <h3>Allocation behaviour</h3>
 *
 * <p>Implementations must be <b>zero-allocation after construction</b>. All buffers, caches, and
 * scratch state are pre-allocated in the constructor; {@link #doWork()} must not allocate on the
 * heap.
 *
 * @see MidRateCache
 * @see DeterministicMarketDataAdapter
 * @see SyntheticMarketDataAdapter
 */
public interface MarketDataAdapter extends Agent {

  /**
   * Returns the current spot mid-rate for the given symbol in fixed-point {@code 10^-8}
   * representation.
   *
   * <p>If the symbol is unknown or the adapter has not yet received a price for it, returns {@link
   * com.trading.engine.messages.FixedPointScale#PRICE_NOT_AVAILABLE}.
   *
   * <p>Zero-allocation — the lookup uses a reusable probe key internally.
   *
   * @param symbol buffer containing the symbol bytes (8-byte fixed-width SBE {@code Symbol} type)
   * @param offset start offset of the symbol within the buffer
   * @param length number of bytes to read (always 8 for the SBE Symbol type)
   * @return the mid-rate in fixed-point {@code 10^-8}, or {@link
   *     com.trading.engine.messages.FixedPointScale#PRICE_NOT_AVAILABLE} if unavailable
   */
  long midRate(DirectBuffer symbol, int offset, int length);

  /**
   * Returns the monotonic nanosecond timestamp (from {@link org.agrona.concurrent.NanoClock}) of
   * the last price update for the given symbol.
   *
   * <p>Returns {@code 0} if the symbol has never been updated.
   *
   * <p>Zero-allocation — the lookup uses a reusable probe key internally.
   *
   * @param symbol buffer containing the symbol bytes (8-byte fixed-width SBE {@code Symbol} type)
   * @param offset start offset of the symbol within the buffer
   * @param length number of bytes to read (always 8 for the SBE Symbol type)
   * @return monotonic nanoseconds of the last update, or {@code 0} if the symbol is unknown
   */
  long lastUpdateNanos(DirectBuffer symbol, int offset, int length);
}
