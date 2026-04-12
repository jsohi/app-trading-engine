package com.trading.engine.pricing.market;

import com.trading.engine.messages.FixedPointScale;

/**
 * Mutable flyweight holding a single symbol's mid-rate and its last-update timestamp.
 *
 * <p>Instances are pre-allocated at {@link MidRateCache} construction time (one per configured
 * symbol) and mutated in place on every price tick. This avoids per-tick heap allocation in the hot
 * path — the same pattern used by Aeron's internal flyweight decoders and exchange-core's
 * order-book entries.
 *
 * <h3>Threading model</h3>
 *
 * <p><b>Not thread-safe.</b> Entries are written exclusively by the owning {@link
 * MarketDataAdapter} on the agent duty-cycle thread and read from the same thread by the pricing
 * service. No concurrent access occurs.
 *
 * <h3>Allocation behaviour</h3>
 *
 * <p>Zero allocation after construction. {@link #update(long, long)} mutates fields in place.
 *
 * @see MidRateCache
 */
final class MidRateEntry {

  /**
   * Current mid-rate in fixed-point {@code 10^-8} representation, or {@link
   * FixedPointScale#PRICE_NOT_AVAILABLE} if the symbol has never been priced.
   */
  private long midRate;

  /**
   * Monotonic nanosecond timestamp (from {@link org.agrona.concurrent.NanoClock}) of the last price
   * update. Zero if the entry has never been updated.
   */
  private long lastUpdateNanos;

  /** Constructs an entry with no price available and a zero timestamp. */
  MidRateEntry() {
    this.midRate = FixedPointScale.PRICE_NOT_AVAILABLE;
    this.lastUpdateNanos = 0L;
  }

  /**
   * Mutates this entry in place with a new mid-rate and timestamp. Zero allocation.
   *
   * @param midRate the new mid-rate in fixed-point {@code 10^-8}
   * @param lastUpdateNanos monotonic nanoseconds of this update
   */
  void update(final long midRate, final long lastUpdateNanos) {
    this.midRate = midRate;
    this.lastUpdateNanos = lastUpdateNanos;
  }

  /**
   * Returns the current mid-rate.
   *
   * @return mid-rate in fixed-point {@code 10^-8}, or {@link FixedPointScale#PRICE_NOT_AVAILABLE}
   *     if never set
   */
  long midRate() {
    return midRate;
  }

  /**
   * Returns the monotonic nanosecond timestamp of the last update.
   *
   * @return nanoseconds, or {@code 0} if never updated
   */
  long lastUpdateNanos() {
    return lastUpdateNanos;
  }
}
