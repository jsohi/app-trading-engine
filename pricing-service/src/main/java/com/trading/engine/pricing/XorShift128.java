package com.trading.engine.pricing;

/**
 * Zero-allocation pseudo-random number generator implementing the xorshift128+ algorithm.
 *
 * <p>Used exclusively by the {@code SyntheticMarketDataAdapter} to generate randomized mid-rate
 * perturbations for synthetic (dummy) price feeds. This PRNG is <b>not suitable for
 * security-sensitive or cryptographic purposes</b> — it is fast and has acceptable statistical
 * properties for market-data simulation, but is trivially predictable.
 *
 * <p><b>Algorithm.</b> xorshift128+ is a member of the xorshift family described by Marsaglia
 * (2003) with an additive output combiner. It has a period of 2^128 - 1, passes BigCrush, and
 * requires only three XOR-shift operations plus one addition per output — no multiplication, no
 * division, no memory access beyond two {@code long} state words.
 *
 * <p><b>Threading:</b> not thread-safe. Intended for use in a single-threaded Aeron {@link
 * io.aeron.agent.EventLogAgent Agent} duty cycle only. The two mutable state fields ({@code s0},
 * {@code s1}) are updated on every call to {@link #nextLong()}.
 *
 * <p><b>Allocation:</b> zero allocation after construction. All methods operate on primitive state
 * only.
 */
public final class XorShift128 {

  /**
   * Golden ratio constant (floor(2^64 / phi)) used to mix the seed into two distinct state words.
   * Ensures that even simple seeds (e.g., 1, 2, 3) produce well-separated initial states.
   */
  private static final long PHI_MIX = 0x9E3779B97F4A7C15L;

  /** First 64-bit state word. */
  private long s0;

  /** Second 64-bit state word. */
  private long s1;

  /**
   * Constructs a new xorshift128+ generator with the given seed.
   *
   * <p>The seed is split into two state words using a phi-based mixing constant to ensure adequate
   * dispersion even for sequential seed values. The xorshift128+ algorithm requires that the state
   * is not all-zero; since {@code seed ^ PHI_MIX} is non-zero for any {@code seed != PHI_MIX}, and
   * we also have the unmodified seed in {@code s0}, the all-zero state is avoided as long as the
   * seed is non-zero.
   *
   * @param seed initial seed value; must not be zero
   * @throws IllegalArgumentException if {@code seed} is zero, which would produce an all-zero state
   *     and a degenerate output sequence
   */
  public XorShift128(final long seed) {
    if (seed == 0) {
      throw new IllegalArgumentException("seed must not be zero");
    }
    this.s0 = seed;
    this.s1 = seed ^ PHI_MIX;
  }

  /**
   * Returns the next pseudo-random {@code long} value from the full 64-bit output range.
   *
   * <p><b>Allocation:</b> zero allocation.
   *
   * @return a pseudo-random {@code long} (may be negative)
   */
  public long nextLong() {
    long x = s0;
    final long y = s1;

    // Advance state: xorshift128+ core
    s0 = y;
    x ^= (x << 23);
    x ^= (x >>> 17);
    x ^= y;
    x ^= (y >>> 26);
    s1 = x;

    // Additive output combiner
    return x + y;
  }

  /**
   * Returns the next pseudo-random non-negative {@code long} value.
   *
   * <p>Equivalent to {@code nextLong() >>> 1}, which clears the sign bit. The result is uniformly
   * distributed over {@code [0, Long.MAX_VALUE]}.
   *
   * <p><b>Allocation:</b> zero allocation.
   *
   * @return a non-negative pseudo-random {@code long} in the range {@code [0, Long.MAX_VALUE]}
   */
  public long nextPositiveLong() {
    return nextLong() >>> 1;
  }

  /**
   * Returns a pseudo-random {@code int} uniformly distributed in {@code [0, bound)}.
   *
   * <p>Uses rejection sampling to eliminate modulo bias. The method computes the largest multiple
   * of {@code bound} that fits in a non-negative long ({@code Long.MAX_VALUE - Long.MAX_VALUE %
   * bound}), and rejects values at or above that threshold. For any reasonable bound (up to ~10^9
   * in pricing), the expected number of rejections is negligible (< 10^-9 per call).
   *
   * <p><b>Allocation:</b> zero allocation.
   *
   * @param bound exclusive upper bound; must be positive
   * @return a pseudo-random {@code int} in {@code [0, bound)}
   * @throws IllegalArgumentException if {@code bound} is not positive
   */
  public int nextBoundedInt(final int bound) {
    if (bound <= 0) {
      throw new IllegalArgumentException("bound must be positive, was: " + bound);
    }
    if (bound == 1) {
      return 0;
    }

    // Rejection sampling: discard values in the partial final bucket to avoid modulo bias.
    // threshold = largest multiple of bound that is <= Long.MAX_VALUE
    final long threshold = Long.MAX_VALUE - (Long.MAX_VALUE % bound);

    long bits;
    do {
      bits = nextPositiveLong();
    } while (bits >= threshold);

    return (int) (bits % bound);
  }
}
