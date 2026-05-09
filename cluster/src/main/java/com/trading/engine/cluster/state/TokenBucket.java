package com.trading.engine.cluster.state;

/**
 * Per-session token-bucket rate limiter flyweight. One instance is allocated per pool entry at
 * {@link RfqStateMachine} construction time; instances are returned to a free list when a session
 * closes and reused for subsequent sessions.
 *
 * <p>The algorithm is deterministic and replay-safe: all arithmetic uses {@code long} integer math
 * with no wall-clock dependency. Cluster timestamps supplied by {@code onSessionMessage} drive
 * refill timing.
 *
 * <p><b>Algorithm (token-bucket with continuous refill):</b>
 *
 * <pre>
 * elapsed  = clusterTs - lastRefillTs
 * refill   = min(capacity - tokens, elapsed / refillNanosPerToken)
 * if refill &gt; 0: tokens += refill; lastRefillTs += refill * refillNanosPerToken
 * if tokens == 0: return false  (rate limit exceeded)
 * tokens--; return true
 * </pre>
 *
 * <p>All values are integer; no floating-point. Deterministic across log replays.
 *
 * <p><b>Threading:</b> not thread-safe — single-threaded cluster duty cycle only.
 *
 * <p><b>Allocation:</b> zero allocation after construction.
 *
 * @see RfqStateMachine
 */
public final class TokenBucket {

  /** Token capacity (maximum tokens per refill window). Set at activation from LauncherConfig. */
  private long capacity;

  /**
   * Nanos per token refill. Computed as {@code rfqRateLimitWindowNanos / capacity}. Set at
   * activation.
   */
  private long refillNanosPerToken;

  /** Current token count. */
  private long tokens;

  /** Cluster timestamp of the last refill (epoch nanos). */
  private long lastRefillTs;

  // Pool index within the free-list array. Immutable after construction.
  final int poolIndex;

  /**
   * Constructs a TokenBucket at the given pool index. All mutable fields are zero-initialized;
   * {@link #activate(long, long, long)} must be called before use.
   *
   * @param poolIndex immutable pool index; must be &gt;= 0
   */
  public TokenBucket(final int poolIndex) {
    this.poolIndex = poolIndex;
  }

  /**
   * Activates this bucket for a new session. Sets capacity and refill rate; fills to capacity.
   * Called when a rate-limit bucket is assigned to a session from the free list.
   *
   * @param capacity the maximum number of tokens (= {@code rfqRateLimitPerSession}); must be
   *     &gt; 0
   * @param refillNanosPerToken nanos per token refill
   *     ({@code rfqRateLimitWindowNanos / capacity}); must be &gt; 0
   * @param clusterTs the current cluster timestamp in epoch nanos (used as the initial
   *     {@code lastRefillTs})
   */
  public void activate(
      final long capacity, final long refillNanosPerToken, final long clusterTs) {
    this.capacity = capacity;
    this.refillNanosPerToken = refillNanosPerToken;
    this.tokens = capacity; // Full at activation — first window free.
    this.lastRefillTs = clusterTs;
  }

  /**
   * Attempts to consume one token. Returns {@code true} if a token was available and consumed;
   * {@code false} if the rate limit is exceeded.
   *
   * <p>Zero allocation. All arithmetic is integer-only and deterministic across log replays.
   *
   * @param clusterTs the current cluster timestamp in epoch nanos
   * @return {@code true} if the request is allowed; {@code false} if rate-limited
   */
  public boolean tryConsume(final long clusterTs) {
    final long elapsed = clusterTs - lastRefillTs;
    if (elapsed > 0L && refillNanosPerToken > 0L) {
      final long refill = Math.min(capacity - tokens, elapsed / refillNanosPerToken);
      if (refill > 0L) {
        tokens += refill;
        lastRefillTs += refill * refillNanosPerToken;
      }
    }
    if (tokens <= 0L) {
      return false;
    }
    tokens--;
    return true;
  }

  /**
   * Returns the current token count. For testing and diagnostics only.
   *
   * @return current tokens
   */
  public long tokens() {
    return tokens;
  }

  /**
   * Returns the pool index of this bucket.
   *
   * @return pool index
   */
  public int poolIndex() {
    return poolIndex;
  }
}
