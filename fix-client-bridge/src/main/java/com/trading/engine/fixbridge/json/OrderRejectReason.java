package com.trading.engine.fixbridge.json;

/**
 * Closed taxonomy of {@code OrderReject.reason} wire-format strings.
 *
 * <p><b>Purpose.</b> Locks the set of reason strings the bridge may emit on an outbound {@link
 * BrowserEvent.OrderReject}. The Browser-side {@code zod} schema mirrors this enum; the contract
 * test (§7) round-trips every variant. The wire field stays a JSON string for forward-compat with
 * the existing {@link BrowserEventWriter#writeOrderReject(BrowserEvent.OrderReject,
 * io.netty.buffer.ByteBuf) writeOrderReject} byte-exact assertions, but every reason a production
 * dispatcher emits MUST come from this enum's {@link #wireValue()}.
 *
 * <p><b>SLO classification.</b> The {@link #classification} of each reason determines whether it
 * counts toward the §9 error-budget burn:
 *
 * <ul>
 *   <li>{@link Classification#FAILURE} — counts as a failure (consumes error budget).
 *   <li>{@link Classification#TRANSIENT} — expected/retryable; counted in {@code
 *       fixbridge_transient_reject_total{reason}} but does not consume error budget.
 * </ul>
 *
 * <p><b>Threading.</b> Enum constants are immutable; safe to share across threads.
 *
 * <p><b>Allocation.</b> {@link #lookup(String)} performs a small switch over interned strings —
 * zero allocation. Constants reference pre-interned String literals, so a writer wishing to assert
 * taxonomy membership at byte-write time can do so via {@code lookup(reason) != null} without
 * allocating.
 *
 * <p><b>Lifecycle.</b> Class-init only.
 *
 * <p><b>Dependencies.</b> JDK only.
 *
 * @see BrowserEvent.OrderReject
 */
public enum OrderRejectReason {

  // ---------------------------------------------------------------------------
  // FAILURE reasons — consume error budget per §9.
  // ---------------------------------------------------------------------------

  /** Inbound JSON failed parser strictness checks (locked taxonomy). */
  MALFORMED("malformed", Classification.FAILURE),

  /** Server-side internal error (NPE, IO, programming bug). */
  INTERNAL("internal", Classification.FAILURE),

  /** Bridge cannot reach the FIX gateway / Artio session. */
  BRIDGE_DOWN("bridge-down", Classification.FAILURE),

  /** Cluster-side pre-trade limit (account credit, max-qty, max-notional, deviation) breached. */
  PRE_TRADE_LIMIT_EXCEEDED("pre-trade-limit-exceeded", Classification.FAILURE),

  /** AcceptQuote/RejectQuote referenced a quoteId issued to a different session (§3.2). */
  QUOTE_NOT_OWNED("quote-not-owned", Classification.FAILURE),

  /**
   * {@code (sub, clOrdId)} reused after a terminal outcome within the 10-min idempotency window
   * (cluster-side §B-r2-14). The UI's design precludes legitimate emission; appearance indicates
   * client bug.
   */
  DUPLICATE_CLORD("duplicate-clord", Classification.FAILURE),

  /** {@code reqId} reused from same session within 60s (§3.2). */
  DUPLICATE_REQID("duplicate-reqId", Classification.FAILURE),

  // ---------------------------------------------------------------------------
  // TRANSIENT reasons — excluded from failure budget per §9 / §B-r2-14.
  // ---------------------------------------------------------------------------

  /** Quote TTL elapsed before AcceptQuote landed. */
  QUOTE_EXPIRED("quote-expired", Classification.TRANSIENT),

  /** Quote unknown to bridge (already evicted or never issued). */
  QUOTE_NOT_FOUND("quote-not-found", Classification.TRANSIENT),

  /** Quote already accepted by a prior AcceptQuote. */
  QUOTE_ALREADY_ACCEPTED("quote-already-accepted", Classification.TRANSIENT),

  /** Quote already rejected by a prior RejectQuote. */
  QUOTE_ALREADY_REJECTED("quote-already-rejected", Classification.TRANSIENT),

  /**
   * Outbound queue overflowed; per-command soft throttle. UI re-enables submit if quote TTL has not
   * expired. See §3.1 + §4.5.
   */
  BACKPRESSURE("backpressure", Classification.TRANSIENT),

  /** Per-type rate-limit token bucket exhausted (§3.13). */
  RATE_LIMIT_EXCEEDED("rate-limit-exceeded", Classification.TRANSIENT),

  /**
   * First-60s-after-Auth gate — tightened anti-flood window per §3.13 / §B-r2-11. Distinct from
   * {@link #RATE_LIMIT_EXCEEDED} so the UI can render a more specific message.
   */
  RATE_LIMIT_INITIAL_WINDOW("rate-limit-initial-window", Classification.TRANSIENT),

  /**
   * DPoP proof signed with a key that has been rotated (§B-r2-7 / Round-5 finding 2). Worker
   * re-Auths under the new key and replays via {@code InFlightRegistry}; no user action required.
   */
  STALE_DPOP("stale-dpop", Classification.TRANSIENT);

  /** SLO classification — see class-level Javadoc. */
  public enum Classification {
    /** Counts toward §9 error-budget burn. */
    FAILURE,
    /**
     * Expected / retryable; counted in {@code fixbridge_transient_reject_total{reason}} but does
     * not consume error budget.
     */
    TRANSIENT
  }

  private final String wireValue;
  private final Classification classification;

  OrderRejectReason(final String wireValue, final Classification classification) {
    this.wireValue = wireValue;
    this.classification = classification;
  }

  /**
   * Wire-format JSON-string value emitted by {@link BrowserEventWriter} for this reason.
   *
   * @return the canonical wire string (e.g. {@code "quote-expired"})
   */
  public String wireValue() {
    return wireValue;
  }

  /**
   * SLO classification (FAILURE counts toward error budget; TRANSIENT does not).
   *
   * @return the classification
   */
  public Classification classification() {
    return classification;
  }

  /**
   * Reverse-lookup an {@link OrderRejectReason} by its wire string. Used by the writer to assert
   * taxonomy membership and by tests to round-trip the contract.
   *
   * @param wire the wire-format string (e.g. {@code "quote-expired"}); may be {@code null}
   * @return the matching enum constant or {@code null} if the string is unknown / null
   */
  public static OrderRejectReason lookup(final String wire) {
    if (wire == null) {
      return null;
    }
    for (final OrderRejectReason r : VALUES) {
      if (r.wireValue.equals(wire)) {
        return r;
      }
    }
    return null;
  }

  // Cached values() to avoid the JVM's defensive array-clone on each lookup() call. Safe — the
  // enum array's contents are immutable enum identities.
  private static final OrderRejectReason[] VALUES = values();
}
