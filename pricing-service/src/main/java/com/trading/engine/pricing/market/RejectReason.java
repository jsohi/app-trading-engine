package com.trading.engine.pricing.market;

/**
 * Categorical reasons the {@link MarketDataPublisher} drops or rejects a tick before it leaves the
 * publisher's agent thread. Each ordinal is a slot in a primitive {@code long[]} rate-limit log
 * array on the publisher, indexed in O(1) by the enum ordinal — never a {@code Map<String, Long>}
 * (which would box {@code Long} on every put + grow on insertion-rate spikes).
 *
 * <p><b>Threading model.</b> Immutable enum singletons; safe for unrestricted concurrent access.
 *
 * <p><b>Allocation.</b> Zero — enum instances are loaded once at class init and are referenced by
 * ordinal. The hot-path call site is {@code rateLimitedLog(RejectReason.X)}; no allocation in
 * either the enum lookup or the rate-limit decision.
 *
 * <p><b>Design rationale.</b> The choice of which Aeron offer return codes appear here vs are
 * handled out-of-band is deliberate:
 *
 * <ul>
 *   <li>{@link #CROSSED}, {@link #NON_POSITIVE}, {@link #UNCONFIGURED} — sanity rejects from the
 *       publisher's input-validation layer. The caller's adapter produced an unusable tick; the
 *       publisher drops it before any Aeron offer.
 *   <li>{@link #BACK_PRESSURED}, {@link #NOT_CONNECTED}, {@link #ADMIN_ACTION}, {@link
 *       #MAX_POSITION_EXCEEDED} — Aeron offer return codes. Each is transient or recoverable; the
 *       publisher rate-limits the WARN/INFO log and increments the per-reason counter, but the next
 *       drain cycle re-attempts publication of the latest conflated top-of-book.
 * </ul>
 *
 * <p><b>{@code CLOSED} is deliberately excluded.</b> {@code Publication.CLOSED} (-4) is fatal and
 * terminal — the publisher's agent shuts down via {@code agent.onClose()} and emits a single Log4j2
 * FATAL on the launcher's shutdown thread (out-of-hot-path). Rate-limiting a
 * once-per-process-lifetime event makes no sense; including it in this enum would imply it shares
 * the periodic-rate-limit semantics of the other reasons, which it does not. The publisher handles
 * {@code CLOSED} via a dedicated branch in its offer-return-code switch.
 *
 * <p><b>Dependencies.</b> None.
 */
public enum RejectReason {
  /** Inbound tick had {@code bid >= ask}. Crossed market — sanity reject before Aeron offer. */
  CROSSED,

  /** Inbound tick had {@code bid <= 0 || ask <= 0}. Non-positive price — sanity reject. */
  NON_POSITIVE,

  /**
   * Inbound tick was for a symbol not registered in the publisher's symbol-config registry.
   * Indicates a producer/consumer mismatch (adapter publishing a symbol the publisher hasn't been
   * told about); sanity reject before Aeron offer.
   */
  UNCONFIGURED,

  /**
   * {@code Publication.BACK_PRESSURED} (-2). Aeron's publish queue is full; retried once on the
   * same drain cycle. If retry fails, the tick remains in the conflation slot and the NEXT drain (5
   * ms later) publishes the latest top-of-book for that symbol regardless.
   */
  BACK_PRESSURED,

  /**
   * {@code Publication.NOT_CONNECTED} (-1). No subscriber on this stream yet (or subscriber
   * detached). Expected during cluster startup; rate-limited INFO.
   */
  NOT_CONNECTED,

  /**
   * {@code Publication.ADMIN_ACTION} (-3). Aeron archive rolling or other transient admin
   * disruption; rate-limited INFO.
   */
  ADMIN_ACTION,

  /**
   * {@code Publication.MAX_POSITION_EXCEEDED} (-5). Very rare with the 16 MiB term length;
   * indicates pathological backlog. Rate-limited WARN with publication position + term-length for
   * forensics.
   */
  MAX_POSITION_EXCEEDED;

  /**
   * Number of distinct reject reasons. Used to size the primitive {@code long[]} rate-limit-log
   * array on {@link MarketDataPublisher}. Pre-computed once at class load (avoids the
   * defensive-clone cost of {@code values().length} on every access).
   */
  public static final int COUNT = values().length;
}
