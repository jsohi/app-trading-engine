package com.trading.engine.pricing.spread;

/**
 * Mutable flyweight holding the output of a {@link SpreadModel#compute} invocation.
 *
 * <p>A single instance is pre-allocated by the pricing service at construction time and passed into
 * every spread computation as an output parameter. This eliminates per-call heap allocation on the
 * hot path — the same pattern used by SBE decoders and Aeron's internal flyweights.
 *
 * <p>Fields are public for direct access from the same thread (flyweight convention). There is no
 * need for accessor methods when the caller and the spread model execute on the same
 * single-threaded duty cycle.
 *
 * <h3>Lifecycle</h3>
 *
 * <p>The result is valid from the moment {@link #set(long, long)} is called until the next call to
 * {@code set()} or {@link #reset()}. The pricing service calls {@code reset()} at the start of each
 * duty-cycle iteration as a defensive measure, but this is not strictly required — the spread model
 * always writes both fields before returning.
 *
 * <h3>Threading model</h3>
 *
 * <p><b>Not thread-safe.</b> A single instance is reused on the pricing-service agent's duty-cycle
 * thread. No concurrent access occurs.
 *
 * <h3>Allocation behaviour</h3>
 *
 * <p>Zero allocation after construction. {@link #set(long, long)} and {@link #reset()} mutate
 * fields in place.
 *
 * @see SpreadModel
 */
public final class SpreadResult {

  /**
   * Computed bid price in fixed-point {@code 10^-8} representation, rounded down to pip precision.
   * Zero if {@link #reset()} has been called and no subsequent computation has been performed.
   */
  public long bidPx;

  /**
   * Computed offer price in fixed-point {@code 10^-8} representation, rounded up to pip precision.
   * Zero if {@link #reset()} has been called and no subsequent computation has been performed.
   */
  public long offerPx;

  /** Constructs a result flyweight with both prices initialized to zero. */
  public SpreadResult() {
    this.bidPx = 0L;
    this.offerPx = 0L;
  }

  /**
   * Sets both bid and offer prices in a single call. Intended to be called by {@link
   * SpreadModel#compute} implementations at the end of their calculation.
   *
   * <p><b>Allocation:</b> zero allocation.
   *
   * @param bidPx the computed bid price in fixed-point {@code 10^-8}, rounded down to pip
   * @param offerPx the computed offer price in fixed-point {@code 10^-8}, rounded up to pip
   */
  public void set(final long bidPx, final long offerPx) {
    this.bidPx = bidPx;
    this.offerPx = offerPx;
  }

  /**
   * Resets both prices to zero. Called defensively at the start of each duty-cycle iteration to
   * ensure stale results are never propagated if a computation is skipped.
   *
   * <p><b>Allocation:</b> zero allocation.
   */
  public void reset() {
    this.bidPx = 0L;
    this.offerPx = 0L;
  }
}
