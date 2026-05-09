package com.trading.engine.cluster.handler;

import java.nio.charset.StandardCharsets;

/**
 * Pre-allocated {@code byte[64]} constants for every possible {@code text} field value in a {@code
 * QuoteRejectedEvent} (template 106, FIX tag 58 {@code Text}). The SBE schema defines {@code text}
 * as {@code char[64]} (fixed-length, NUL-padded US-ASCII) — see {@code trading-schema.xml
 * QuoteRejectedEvent.text}.
 *
 * <p>All constants are exactly 64 bytes. Short message bytes are NUL-padded to 64 bytes at
 * construction. A static initializer asserts that every constant is at most 64 bytes of ASCII; this
 * check fires at class-load time and surfaces as an {@link ExceptionInInitializerError} if any
 * constant exceeds the bound, making it a build-time failure rather than a runtime surprise.
 *
 * <p>Usage: callers write the constant directly into the SBE encoder via {@code
 * QuoteRejectedEventEncoder.putText(RfqRejectMessages.MALFORMED, 0)}.
 *
 * <p><b>Threading:</b> all constants are effectively immutable (NUL-padded once, never mutated) —
 * safe to access from any thread. The cluster uses them on the single-threaded duty cycle.
 *
 * <p><b>Allocation:</b> all arrays allocated once at class load. Zero allocation on the hot path.
 */
public final class RfqRejectMessages {

  /** Maximum byte length of the {@code text} field — matches SBE {@code char[64]} definition. */
  public static final int MAX_TEXT_LEN = 64;

  // --- Reject text constants (ASCII, NUL-padded to exactly MAX_TEXT_LEN bytes) ---

  /** Inbound QuoteRequest SBE message is shorter than the declared block length. */
  public static final byte[] MALFORMED = pad("malformed");

  /** Symbol field is empty (zero bytes before trailing NUL). */
  public static final byte[] SYMBOL_EMPTY = pad("symbol empty");

  /** Account is inactive, suspended, or not found in AccountStore. */
  public static final byte[] ACCOUNT_INACTIVE = pad("account inactive");

  /** Account does not have the {@code CAN_RFQ} capability bit set. */
  public static final byte[] RFQ_NOT_PERMITTED = pad("rfq not permitted");

  /** Currency or settlCurrency is not a known 3-letter code in CurrencyStore. */
  public static final byte[] CURRENCY_UNKNOWN = pad("currency unknown");

  /**
   * Per-session token-bucket rate limit exceeded ({@code rfqRateLimitPerSession} tokens per {@code
   * rfqRateLimitWindowNanos}).
   */
  public static final byte[] RATE_LIMIT = pad("rate limit");

  /** A different QuoteRequest body is active under the same quoteReqId. */
  public static final byte[] DUPLICATE = pad("duplicate");

  /** No free slots remain in the RfqStateMachine slot pool. */
  public static final byte[] POOL_EXHAUSTED = pad("pool exhausted");

  /** Aeron timer service pool was exhausted; cannot schedule TTL or request-timeout timer. */
  public static final byte[] TIMER_POOL_EXHAUSTED = pad("timer pool exhausted");

  /**
   * Pricing service declined the quote (accepted=false in the PriceResponse). FIX tag 658 reason:
   * InvalidPrice(5).
   */
  public static final byte[] PRICING_REJECTED = pad("pricing rejected");

  /**
   * The pricing service never responded within {@code rfqRequestTimeoutNanos}. FIX tag 658 reason:
   * Other(99).
   */
  public static final byte[] REQUEST_TIMEOUT = pad("request timeout");

  /**
   * Request-timeout fired during snapshot recovery (the cluster was restarted while a QuoteRequest
   * was in REQUESTED state and the deadline had already elapsed).
   */
  public static final byte[] REQUEST_TIMEOUT_ON_RECOVERY = pad("request timeout on recovery");

  /** Recovery timer re-arm failed — Aeron timer pool was exhausted at recovery time. */
  public static final byte[] RECOVERY_TIMER_REARM_FAILED = pad("recovery timer rearm failed");

  /**
   * Account was deleted between the snapshot and cluster recovery. The REQUESTED slot is rejected
   * because accountCode cannot be rehydrated.
   */
  public static final byte[] ACCOUNT_MISSING_ON_RECOVERY = pad("account missing on recovery");

  /**
   * The originating client session was closed while the RFQ was in-flight. The slot is fast- failed
   * on the next timer tick.
   */
  public static final byte[] SESSION_CLOSED = pad("session closed");

  /**
   * A NewOrderSingle referenced a quoteId that is not in QUOTED state (unknown or already
   * consumed/expired).
   */
  public static final byte[] UNKNOWN_QUOTE = pad("unknown quote");

  /**
   * A NewOrderSingle referenced a quoteId that has already expired (TTL elapsed before the accept
   * arrived).
   */
  public static final byte[] QUOTE_EXPIRED = pad("quote expired");

  /**
   * NOS side does not match the quoted side (hard reject — no tolerance). Counter: {@code
   * rfq.reject.quoteSideMismatch}.
   */
  public static final byte[] SIDE_MISMATCH = pad("side mismatch");

  // price-mismatch and qty-mismatch are generated dynamically (include bps value),
  // so they are not pre-allocated here. See QuoteRequestHandler / NewOrderSingleHandler.

  private RfqRejectMessages() {}

  /**
   * Returns a new {@code byte[MAX_TEXT_LEN]} containing the ASCII bytes of {@code msg} followed by
   * NUL-padding to 64 bytes. Used by all field initializers above.
   *
   * <p><b>Precondition (enforced by static initializer):</b> {@code msg.length() <= MAX_TEXT_LEN}
   * and all characters are 7-bit ASCII.
   *
   * @param msg the reject text; must be ASCII and at most {@link #MAX_TEXT_LEN} chars
   * @return a 64-byte NUL-padded array
   * @throws IllegalArgumentException if {@code msg} exceeds {@link #MAX_TEXT_LEN} bytes
   */
  public static byte[] pad(final String msg) {
    final byte[] src = msg.getBytes(StandardCharsets.US_ASCII);
    if (src.length > MAX_TEXT_LEN) {
      throw new IllegalArgumentException(
          "reject text \""
              + msg
              + "\" exceeds MAX_TEXT_LEN="
              + MAX_TEXT_LEN
              + " (was "
              + src.length
              + " bytes)");
    }
    final byte[] result = new byte[MAX_TEXT_LEN];
    System.arraycopy(src, 0, result, 0, src.length);
    // Remaining bytes are already NUL (Java zero-initializes arrays).
    return result;
  }
}
