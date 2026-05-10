package com.trading.engine.fixbridge.rawfix;

/**
 * Per-session token-bucket rate limiter for {@code RawFix} debug-event emission (§3.5).
 *
 * <p><b>Purpose.</b> Caps {@code RawFix} emission at {@link #DEFAULT_RATE_PER_SEC} frames per
 * second per session so a debug-toggled session cannot DoS the bridge's outbound queue. Drops
 * past the cap are surfaced via the {@code fixbridge_rawfix_dropped_total{session,reason}}
 * counter (the dispatcher increments on each rejected frame; this class only answers the
 * accept/reject question).
 *
 * <p><b>Threading.</b> NOT thread-safe. Owned by the per-session Netty handler on the channel's
 * single-threaded event loop.
 *
 * <p><b>Allocation.</b> Constructor allocates the bucket; {@link #tryConsume(long)} is zero-alloc
 * (only primitive bucket state mutated).
 *
 * <p><b>Lifecycle.</b> Per-session — created on {@code bridgeDebug=true} flip, discarded when the
 * channel closes or the flag flips off.
 *
 * <p><b>Dependencies.</b> JDK only.
 */
public final class RawFixRateLimiter {

  /**
   * Default sustained rate — 1000 frames per second per session. Locked from §3.5 wire-protocol
   * concerns: at 1000/s/session × default 256 max sessions, the bridge's outbound queue (4096
   * per session, §3.1) absorbs the worst-case fan-in without exceeding terminal-overflow.
   */
  public static final double DEFAULT_RATE_PER_SEC = 1000.0;

  /**
   * Default burst — equal to the sustained rate (1000 frames). Allows a brief catch-up after a
   * blocked write without granting unbounded amplification.
   */
  public static final long DEFAULT_BURST = 1000L;

  private final double capacity;
  private final double refillTokensPerNanos;

  // Mutable token-bucket state. Owned by the channel's event loop.
  private double tokens;
  private long lastRefillNs;

  /**
   * Construct a limiter with the {@link #DEFAULT_RATE_PER_SEC} / {@link #DEFAULT_BURST} defaults.
   *
   * @param initialNanos {@code System.nanoTime()} captured at limiter construction
   */
  public RawFixRateLimiter(final long initialNanos) {
    this(DEFAULT_BURST, DEFAULT_RATE_PER_SEC, initialNanos);
  }

  /**
   * Construct a limiter with a custom burst cap and sustained refill rate.
   *
   * @param burstCapacity maximum tokens held by the bucket; first {@code burstCapacity} consumers
   *     after construction succeed without waiting
   * @param refillPerSec sustained refill rate, tokens per second
   * @param initialNanos {@code System.nanoTime()} captured at limiter construction
   * @throws IllegalArgumentException if {@code burstCapacity <= 0} or {@code refillPerSec <= 0}
   */
  public RawFixRateLimiter(
      final long burstCapacity, final double refillPerSec, final long initialNanos) {
    if (burstCapacity <= 0L) {
      throw new IllegalArgumentException("burstCapacity must be > 0, was " + burstCapacity);
    }
    if (refillPerSec <= 0.0) {
      throw new IllegalArgumentException("refillPerSec must be > 0, was " + refillPerSec);
    }
    this.capacity = burstCapacity;
    this.refillTokensPerNanos = refillPerSec / 1_000_000_000.0;
    this.tokens = capacity; // start full so the burst is available immediately
    this.lastRefillNs = initialNanos;
  }

  /**
   * Attempt to consume one token. Returns {@code true} iff a token was available (frame may
   * proceed downstream), {@code false} if the rate limit was hit (frame must be dropped and the
   * dispatcher MUST increment {@code fixbridge_rawfix_dropped_total}).
   *
   * @param nowNs current {@code System.nanoTime()}
   * @return {@code true} if a token was consumed; {@code false} if rate-limited
   */
  public boolean tryConsume(final long nowNs) {
    refill(nowNs);
    if (tokens >= 1.0) {
      tokens -= 1.0;
      return true;
    }
    return false;
  }

  /** Visible for testing — read the current token count without mutating state. */
  public double tokensVisibleForTesting() {
    return tokens;
  }

  private void refill(final long nowNs) {
    final long elapsed = nowNs - lastRefillNs;
    if (elapsed <= 0L) {
      // Clock did not advance — leave lastRefillNs alone so the next call accumulates time.
      return;
    }
    final double added = (double) elapsed * refillTokensPerNanos;
    final double newTokens = tokens + added;
    tokens = newTokens > capacity ? capacity : newTokens;
    lastRefillNs = nowNs;
  }
}
