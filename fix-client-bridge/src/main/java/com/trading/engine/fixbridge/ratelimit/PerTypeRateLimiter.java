package com.trading.engine.fixbridge.ratelimit;

/**
 * Server-side per-command-type token-bucket rate limiter for one bridge session.
 *
 * <p><b>Purpose (locked plan §3.13).</b> Enforce the per-type burst + sustained limits for every
 * inbound command type, plus a tighter "first-60s after Auth" anti-flood window for the
 * stolen-token threat model. Numbers are mirrored exactly between this server-side enforcer and
 * the UI-side client RateLimiter (UX guard) per the §3.13 table:
 *
 * <pre>
 *   Command         | Sustained | Burst | First-60s-after-Auth
 *   ----------------+-----------+-------+----------------------
 *   QuoteRequest    | 5/s       | 10    | 5/s, 2 burst
 *   AcceptQuote     | 5/s       | 10    | 2/s, 2 burst       (anti-flood)
 *   RejectQuote     | 10/s      | 20    | 10/s
 *   NewOrderSingle  | 5/s       | 10    | 2/s, 2 burst       (anti-flood)
 *   CancelOrder     | 20/s      | 50    | 20/s
 *   OrderStatusRequest | -      | -     | -      (excluded — recovery path)
 * </pre>
 *
 * <p>Excess: emit {@code OrderReject{reason:"rate-limit-exceeded"}} (or {@code
 * "rate-limit-initial-window"} during the first-60s gate).
 *
 * <p><b>Clock source.</b> {@code System.nanoTime()} server-monotonic — captured as {@code
 * authNanos} at AUTHENTICATED transition; the first-60s window expires when {@code
 * System.nanoTime() - authNanos >= 60s}. Tamper-resistant against server wall-clock adjustments
 * per §3.13 / §B-r2-15.
 *
 * <p><b>Threading.</b> NOT thread-safe. Owned by the per-session Netty handler on the channel's
 * single-threaded event loop. One {@code PerTypeRateLimiter} instance per session.
 *
 * <p><b>Allocation.</b> Zero on the hot path. Five {@link TokenBucket} fields are pre-allocated
 * at construction; each {@code tryConsume} mutates only primitive bucket state.
 *
 * <p><b>Lifecycle.</b> Per-session — created in {@code JwtAuthHandler} on AUTHENTICATED
 * transition and discarded on channel close.
 *
 * <p><b>Dependencies.</b> JDK only.
 */
public final class PerTypeRateLimiter {

  /** First-60s window length in nanoseconds. */
  static final long FIRST_60S_WINDOW_NANOS = 60_000_000_000L;

  // Sustained refill rates (tokens per second) — normal mode.
  static final double QR_SUSTAINED_PER_SEC = 5.0;
  static final double AQ_SUSTAINED_PER_SEC = 5.0;
  static final double RJ_SUSTAINED_PER_SEC = 10.0;
  static final double NOS_SUSTAINED_PER_SEC = 5.0;
  static final double CXL_SUSTAINED_PER_SEC = 20.0;

  // Burst caps — normal mode.
  static final long QR_BURST = 10L;
  static final long AQ_BURST = 10L;
  static final long RJ_BURST = 20L;
  static final long NOS_BURST = 10L;
  static final long CXL_BURST = 50L;

  // First-60s mode — tightened sustained rates and burst caps for anti-flood.
  static final double QR_INITIAL_SUSTAINED_PER_SEC = 5.0;
  static final long QR_INITIAL_BURST = 2L;

  static final double AQ_INITIAL_SUSTAINED_PER_SEC = 2.0;
  static final long AQ_INITIAL_BURST = 2L;

  static final double RJ_INITIAL_SUSTAINED_PER_SEC = 10.0;
  static final long RJ_INITIAL_BURST = 20L;

  static final double NOS_INITIAL_SUSTAINED_PER_SEC = 2.0;
  static final long NOS_INITIAL_BURST = 2L;

  static final double CXL_INITIAL_SUSTAINED_PER_SEC = 20.0;
  static final long CXL_INITIAL_BURST = 50L;

  /** Rate-limit attempt outcome. */
  public enum Outcome {
    /** Token consumed; command MAY proceed downstream. */
    ALLOWED,
    /**
     * Steady-state rate limit exhausted. Dispatcher MUST emit {@code OrderReject{reason:"rate-
     * limit-exceeded"}} (the corresponding {@link
     * com.trading.engine.fixbridge.json.OrderRejectReason#RATE_LIMIT_EXCEEDED}).
     */
    REJECTED_RATE_LIMIT,
    /**
     * First-60s anti-flood gate hit. Dispatcher MUST emit {@code OrderReject{reason:"rate-limit-
     * initial-window"}} ({@link
     * com.trading.engine.fixbridge.json.OrderRejectReason#RATE_LIMIT_INITIAL_WINDOW}).
     */
    REJECTED_INITIAL_WINDOW
  }

  /** Command-type tag for {@link #tryConsume(CommandType, long)}. */
  public enum CommandType {
    QUOTE_REQUEST,
    ACCEPT_QUOTE,
    REJECT_QUOTE,
    NEW_ORDER_SINGLE,
    CANCEL_ORDER
  }

  // ---------------------------------------------------------------------------
  // Per-session state.
  // ---------------------------------------------------------------------------

  /**
   * {@code System.nanoTime()} captured at AUTHENTICATED transition. The first-60s gate ends when
   * {@code now - authNanos >= FIRST_60S_WINDOW_NANOS}.
   */
  private final long authNanos;

  // Two buckets per command type — one for normal mode, one for first-60s mode. Pre-allocated;
  // tryConsume picks the right one based on the elapsed-since-auth check.
  private final TokenBucket qrNormal;
  private final TokenBucket aqNormal;
  private final TokenBucket rjNormal;
  private final TokenBucket nosNormal;
  private final TokenBucket cxlNormal;

  private final TokenBucket qrInitial;
  private final TokenBucket aqInitial;
  private final TokenBucket rjInitial;
  private final TokenBucket nosInitial;
  private final TokenBucket cxlInitial;

  /**
   * Constructs a per-session rate limiter pinned to {@code authNanos}.
   *
   * @param authNanos {@code System.nanoTime()} captured at AUTHENTICATED transition
   */
  public PerTypeRateLimiter(final long authNanos) {
    this.authNanos = authNanos;
    this.qrNormal = new TokenBucket(QR_BURST, QR_SUSTAINED_PER_SEC, authNanos);
    this.aqNormal = new TokenBucket(AQ_BURST, AQ_SUSTAINED_PER_SEC, authNanos);
    this.rjNormal = new TokenBucket(RJ_BURST, RJ_SUSTAINED_PER_SEC, authNanos);
    this.nosNormal = new TokenBucket(NOS_BURST, NOS_SUSTAINED_PER_SEC, authNanos);
    this.cxlNormal = new TokenBucket(CXL_BURST, CXL_SUSTAINED_PER_SEC, authNanos);
    this.qrInitial = new TokenBucket(QR_INITIAL_BURST, QR_INITIAL_SUSTAINED_PER_SEC, authNanos);
    this.aqInitial = new TokenBucket(AQ_INITIAL_BURST, AQ_INITIAL_SUSTAINED_PER_SEC, authNanos);
    this.rjInitial = new TokenBucket(RJ_INITIAL_BURST, RJ_INITIAL_SUSTAINED_PER_SEC, authNanos);
    this.nosInitial = new TokenBucket(NOS_INITIAL_BURST, NOS_INITIAL_SUSTAINED_PER_SEC, authNanos);
    this.cxlInitial = new TokenBucket(CXL_INITIAL_BURST, CXL_INITIAL_SUSTAINED_PER_SEC, authNanos);
  }

  /**
   * Attempt to consume a token for the given command type at {@code nowNs}.
   *
   * <p>During the first-60s window the tighter "initial" bucket is consulted first; if it
   * rejects, the result is {@link Outcome#REJECTED_INITIAL_WINDOW}. Outside the window the
   * normal bucket is consulted; if it rejects the result is {@link Outcome#REJECTED_RATE_LIMIT}.
   *
   * <p>Note: only the relevant bucket (initial OR normal) is consumed — the other bucket is left
   * untouched so it accumulates tokens during the off-window. This keeps the model simple and
   * prevents the user being silently double-charged when crossing the 60s boundary.
   *
   * @param type command type
   * @param nowNs current {@code System.nanoTime()}
   * @return outcome — ALLOWED, REJECTED_RATE_LIMIT, or REJECTED_INITIAL_WINDOW
   */
  public Outcome tryConsume(final CommandType type, final long nowNs) {
    final boolean initialWindow = (nowNs - authNanos) < FIRST_60S_WINDOW_NANOS;
    final TokenBucket bucket = bucketFor(type, initialWindow);
    if (bucket.tryConsume(nowNs)) {
      return Outcome.ALLOWED;
    }
    return initialWindow ? Outcome.REJECTED_INITIAL_WINDOW : Outcome.REJECTED_RATE_LIMIT;
  }

  /** Visible for testing — exposes the auth-anchor nanosecond. */
  public long authNanos() {
    return authNanos;
  }

  private TokenBucket bucketFor(final CommandType type, final boolean initialWindow) {
    switch (type) {
      case QUOTE_REQUEST:
        return initialWindow ? qrInitial : qrNormal;
      case ACCEPT_QUOTE:
        return initialWindow ? aqInitial : aqNormal;
      case REJECT_QUOTE:
        return initialWindow ? rjInitial : rjNormal;
      case NEW_ORDER_SINGLE:
        return initialWindow ? nosInitial : nosNormal;
      case CANCEL_ORDER:
        return initialWindow ? cxlInitial : cxlNormal;
      default:
        throw new IllegalArgumentException("unknown command type: " + type);
    }
  }

  // ---------------------------------------------------------------------------
  // Token bucket — internal.
  // ---------------------------------------------------------------------------

  /**
   * Standard token bucket: capacity tokens at burst, refilled at {@code refillTokensPerNanos}.
   * Mutable; not thread-safe; caller-enforced ownership by one event loop.
   */
  static final class TokenBucket {

    private final double capacity;
    private final double refillTokensPerNanos;

    private double tokens;
    private long lastRefillNs;

    TokenBucket(final long capacity, final double refillPerSec, final long initialNanos) {
      this.capacity = capacity;
      this.refillTokensPerNanos = refillPerSec / 1_000_000_000.0;
      // Buckets start full (allow the burst on the first request after auth).
      this.tokens = capacity;
      this.lastRefillNs = initialNanos;
    }

    boolean tryConsume(final long nowNs) {
      refill(nowNs);
      if (tokens >= 1.0) {
        tokens -= 1.0;
        return true;
      }
      return false;
    }

    private void refill(final long nowNs) {
      final long elapsed = nowNs - lastRefillNs;
      if (elapsed <= 0L) {
        // Clock did not advance (or, vanishingly, ran backwards on this thread): no refill,
        // but do not corrupt lastRefillNs — leave it alone so the next call refills the
        // accumulated time.
        return;
      }
      final double added = (double) elapsed * refillTokensPerNanos;
      final double newTokens = tokens + added;
      tokens = newTokens > capacity ? capacity : newTokens;
      lastRefillNs = nowNs;
    }

    double tokensVisibleForTesting() {
      return tokens;
    }
  }
}
