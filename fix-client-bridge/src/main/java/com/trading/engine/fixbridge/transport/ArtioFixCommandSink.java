package com.trading.engine.fixbridge.transport;

import com.trading.engine.fix.builder.NewOrderSingleEncoder;
import com.trading.engine.fix.builder.OrderCancelRequestEncoder;
import com.trading.engine.fix.builder.QuoteRequestEncoder;
import com.trading.engine.fixbridge.json.BrowserEvent;
import com.trading.engine.fixbridge.json.MutableParsedMessage;
import com.trading.engine.fixbridge.json.OrderRejectReason;
import com.trading.engine.fixbridge.translator.JsonToFixTranslator;

/**
 * Production {@link FixCommandSink} impl — translates parsed browser JSON into Artio FIX 4.4
 * encoders and dispatches them to the gateway via the injected {@link FixSessionAdapter}.
 *
 * <p><b>Purpose.</b> Replaces {@link FixCommandSink#NOOP} for production use. One instance per
 * authenticated browser session; constructed by the launcher's {@link FixCommandSink.Factory} when
 * the browser completes JWT auth and the FIX session is live.
 *
 * <p><b>ClOrdID minting (locked §4).</b> Each send method that produces a FIX order increments a
 * per-session monotonic counter and delegates to {@link JsonToFixTranslator#mintClOrdId} using the
 * bridge process {@code instanceTag} (clock-derived at boot) and a {@code sessionIdLong} derived
 * from the launcher-supplied 28-bit {@code sessionToken} (process-wide {@code AtomicLong}
 * sequence). This produces the {@code <6-hex>-<7-hex>-<5-digit>} format required by the locked §4
 * spec while guaranteeing uniqueness across concurrent sessions (PR #70 R2 Gemini fix — earlier
 * {@link System#identityHashCode}-based scheme was birthday-paradox prone above ~19,000 concurrent
 * sessions).
 *
 * <p><b>AcceptQuote two-phase commit (locked §2).</b> {@link #sendAcceptQuote} first looks up the
 * {@link QuoteSnapshotCache}. On a cache miss it immediately enqueues an {@link
 * BrowserEvent.OrderReject} with reason {@link OrderRejectReason#QUOTE_EXPIRED} onto the
 * per-session outbound queue and returns {@link FixCommandSink#NO_SEND} — no FIX wire activity
 * occurs. On a cache hit it translates, sends, and only evicts the cache slot after {@code trySend}
 * succeeds (return value {@code >= 0}).
 *
 * <p><b>RejectQuote (locked §11).</b> {@link #handleRejectQuote} evicts the cache slot and returns
 * {@link FixCommandSink#NO_SEND} — no FIX wire activity.
 *
 * <p><b>OrderStatusRequest (§3.15).</b> Query is projection-side; no FIX wire activity. Returns
 * {@link FixCommandSink#NO_SEND}.
 *
 * <p><b>Threading.</b> Not thread-safe. Owned exclusively by the channel's Netty event loop.
 * Methods must never be called concurrently for the same session.
 *
 * <p><b>Allocation.</b> Zero on the hot path after construction. All encoders are pre-allocated as
 * final fields and reused across calls. The {@link JsonToFixTranslator}'s per-instance scratch
 * buffers are also reused. The only allocation on the miss path is the {@link
 * BrowserEvent.OrderReject} record — this is the miss/error path, not the hot send path.
 *
 * <p><b>Lifecycle.</b> Per-session. Constructed at auth-success by the launcher factory; released
 * to GC when the channel closes.
 *
 * <p><b>Dependencies.</b> {@link JsonToFixTranslator}, {@link QuoteSnapshotCache}, {@link
 * FixSessionAdapter}, {@link BridgeSession}.
 *
 * @see FixCommandSink
 * @see FixSessionAdapter
 * @see QuoteSnapshotCache
 * @see JsonToFixTranslator
 */
public final class ArtioFixCommandSink implements FixCommandSink {

  /** Artio-agnostic wrapper around the real Artio {@code Session#trySend} call. */
  private final FixSessionAdapter fixSession;

  /** Per-session translator; single-threaded by event-loop ownership. */
  private final JsonToFixTranslator translator;

  /** Per-session quote-snapshot cache (locked §2 two-phase commit). */
  private final QuoteSnapshotCache quoteCache;

  /**
   * Bridge-process tag used in ClOrdID minting (locked §4). Derived from the bridge process startup
   * clock; injected at construction so it is stable across the process lifetime.
   */
  private final long instanceTag;

  /**
   * 28-bit session identifier derived from the {@code SessionId} string's identity hash code, used
   * in ClOrdID minting (locked §4). Computed once at construction.
   */
  private final long sessionIdLong;

  /** Per-session outbound queue; used to enqueue OrderReject on cache-miss path. */
  private final BridgeSession session;

  /**
   * Per-session monotonic ClOrdID counter. Starts at 0; incremented before each FIX message that
   * requires a fresh ClOrdID (NewOrderSingle, OrderCancelRequest). Never decremented or reset.
   */
  private long clOrdIdCounter;

  // ---------------------------------------------------------------------------
  // Pre-allocated per-session encoders. Never reallocated after construction.
  // ---------------------------------------------------------------------------

  /** Reusable {@code NewOrderSingle (35=D)} encoder. */
  private final NewOrderSingleEncoder nosEncoder = new NewOrderSingleEncoder();

  /** Reusable {@code OrderCancelRequest (35=F)} encoder. */
  private final OrderCancelRequestEncoder ocrEncoder = new OrderCancelRequestEncoder();

  /** Reusable {@code QuoteRequest (35=R)} encoder. */
  private final QuoteRequestEncoder qrEncoder = new QuoteRequestEncoder();

  /**
   * Constructs a production command sink for one browser session.
   *
   * <p>The {@code sessionIdLong} is derived from the {@code sessionToken} parameter — a 28-bit
   * masked view of the launcher's process-wide {@code AtomicLong} sequence. This guarantees
   * uniqueness across concurrent sessions without relying on {@link System#identityHashCode} (which
   * is birthday-paradox prone above ~19,000 concurrent sessions; see PR #70 R2 Gemini high-priority
   * finding). The launcher binds the sessionToken at session creation; this constructor just
   * consumes it.
   *
   * @param session the freshly-authenticated bridge session; owns the outbound queue
   * @param fixSession Artio-session adapter; method-reference to {@code Session::trySend} in prod
   * @param translator per-session JSON-to-FIX translator (pre-warmed by caller)
   * @param quoteCache per-session quote snapshot cache
   * @param instanceTag 24-bit bridge process tag (locked §4); injected from boot clock
   * @param sessionToken 28-bit per-session token assigned by the launcher's process-wide {@code
   *     AtomicLong} sequence — guarantees ClOrdID uniqueness across concurrent sessions without
   *     relying on {@link System#identityHashCode} which is birthday-paradox prone above ~19,000
   *     concurrent sessions (Gemini high-priority finding on PR #70 R2).
   * @throws NullPointerException if any reference parameter is {@code null}
   * @throws IllegalArgumentException if {@code instanceTag} or {@code sessionToken} is negative
   */
  public ArtioFixCommandSink(
      final BridgeSession session,
      final FixSessionAdapter fixSession,
      final JsonToFixTranslator translator,
      final QuoteSnapshotCache quoteCache,
      final long instanceTag,
      final long sessionToken) {
    if (session == null) {
      throw new NullPointerException("session must not be null");
    }
    if (fixSession == null) {
      throw new NullPointerException("fixSession must not be null");
    }
    if (translator == null) {
      throw new NullPointerException("translator must not be null");
    }
    if (quoteCache == null) {
      throw new NullPointerException("quoteCache must not be null");
    }
    if (instanceTag < 0L) {
      throw new IllegalArgumentException("instanceTag must be non-negative: " + instanceTag);
    }
    if (sessionToken < 0L) {
      throw new IllegalArgumentException("sessionToken must be non-negative: " + sessionToken);
    }
    this.session = session;
    this.fixSession = fixSession;
    this.translator = translator;
    this.quoteCache = quoteCache;
    this.instanceTag = instanceTag;
    // Mask to 28 bits to fit the locked §4 seven-hex-digit ClOrdID field. The launcher's
    // AtomicLong sequence is monotonic across process lifetime; modulo-2^28 wrapping is safe
    // because no two LIVE sessions can hold the same modulo within the rate-limited bridge
    // session population (~256 max per FixClientBridgeConfig).
    this.sessionIdLong = sessionToken & 0xFFFFFFFL;
    this.clOrdIdCounter = 0L;
  }

  /**
   * {@inheritDoc}
   *
   * <p>Translates the parsed {@code QuoteRequest} into a FIX {@code QuoteRequest (35=R)} using the
   * pre-allocated {@link QuoteRequestEncoder} and forwards it to the gateway via {@link
   * FixSessionAdapter#trySend}.
   *
   * @return Artio send-position ({@code >= 0}) on success, {@link #NO_SEND} on backpressure or
   *     session-down
   */
  @Override
  public long sendQuoteRequest(final MutableParsedMessage parsed, final long nowNs) {
    translator.translateQuoteRequest(parsed, qrEncoder, sessionIdLong, instanceTag, clOrdIdCounter);
    return fixSession.trySend(qrEncoder);
  }

  /**
   * {@inheritDoc}
   *
   * <p>Implements the locked §2 two-phase commit:
   *
   * <ol>
   *   <li>Look up the per-session quote cache for {@code parsed.quoteId}.
   *   <li>On miss: enqueue {@link BrowserEvent.OrderReject} with reason {@link
   *       OrderRejectReason#QUOTE_EXPIRED} and return {@link #NO_SEND}.
   *   <li>On hit: translate into {@link NewOrderSingleEncoder}, call {@link
   *       FixSessionAdapter#trySend}, and evict the cache slot only on success ({@code >= 0}).
   * </ol>
   *
   * @return Artio send-position ({@code >= 0}) on success, {@link #NO_SEND} on cache miss or
   *     backpressure
   */
  @Override
  public long sendAcceptQuote(final MutableParsedMessage parsed, final long nowNs) {
    final var snapshot = quoteCache.lookup(parsed.scratch, parsed.quoteIdOff, parsed.quoteIdLen);
    if (snapshot == null) {
      // Cache miss — quote expired or unknown. Build a ClOrdID string for the reject event.
      // The browser may have supplied a clOrdId; prefer it, otherwise mint a fresh one.
      final String clOrdIdStr = clOrdIdString(parsed);
      session.enqueue(new BrowserEvent.OrderReject(clOrdIdStr, OrderRejectReason.QUOTE_EXPIRED));
      return NO_SEND;
    }

    // Cache hit: increment counter, translate, send, evict on success.
    final long counter = ++clOrdIdCounter;
    // quoteCacheToken is unused by the two-phase commit here because ArtioFixCommandSink owns
    // both the trySend call and the eviction — the token round-trip in translateAcceptQuote is
    // the translator's contract, but we evaluate the result inline.
    translator.translateAcceptQuote(
        parsed, nosEncoder, snapshot, sessionIdLong, instanceTag, counter, 0L);
    final long position = fixSession.trySend(nosEncoder);
    if (position >= 0L) {
      quoteCache.evict(parsed.scratch, parsed.quoteIdOff, parsed.quoteIdLen);
    }
    return position;
  }

  /**
   * {@inheritDoc}
   *
   * <p>Implements locked §11: evicts the per-session cache slot for {@code parsed.quoteId} and
   * returns {@link #NO_SEND} — no FIX wire activity.
   */
  @Override
  public long handleRejectQuote(final MutableParsedMessage parsed, final long nowNs) {
    translator.handleRejectQuote(parsed);
    quoteCache.evict(parsed.scratch, parsed.quoteIdOff, parsed.quoteIdLen);
    return NO_SEND;
  }

  /**
   * {@inheritDoc}
   *
   * <p>Translates the parsed {@code NewOrderSingle} into a FIX {@code NewOrderSingle (35=D)} and
   * forwards it to the gateway.
   *
   * @return Artio send-position ({@code >= 0}) on success, {@link #NO_SEND} on backpressure
   */
  @Override
  public long sendNewOrderSingle(final MutableParsedMessage parsed, final long nowNs) {
    final long counter = ++clOrdIdCounter;
    translator.translateNewOrderSingle(parsed, nosEncoder, sessionIdLong, instanceTag, counter);
    return fixSession.trySend(nosEncoder);
  }

  /**
   * {@inheritDoc}
   *
   * <p>Translates the parsed {@code CancelOrder} into a FIX {@code OrderCancelRequest (35=F)} and
   * forwards it to the gateway.
   *
   * @return Artio send-position ({@code >= 0}) on success, {@link #NO_SEND} on backpressure
   */
  @Override
  public long sendCancelOrder(final MutableParsedMessage parsed, final long nowNs) {
    final long counter = ++clOrdIdCounter;
    translator.translateCancelOrder(parsed, ocrEncoder, sessionIdLong, instanceTag, counter);
    return fixSession.trySend(ocrEncoder);
  }

  /**
   * {@inheritDoc}
   *
   * <p>Per §3.15, {@code OrderStatusRequest} is handled on the projection side — no FIX wire
   * activity is required. Always returns {@link #NO_SEND}.
   */
  @Override
  public long sendOrderStatusRequest(final MutableParsedMessage parsed, final long nowNs) {
    // §3.15: projection-side query — no FIX gateway involvement. The caller (dispatcher) is
    // responsible for routing the query to the cluster projection and enqueuing the reply.
    return NO_SEND;
  }

  // ---------------------------------------------------------------------------
  // Internal helpers.
  // ---------------------------------------------------------------------------

  /**
   * Derive a ClOrdID string for an error-path {@link BrowserEvent.OrderReject}. Returns the
   * browser-supplied {@code clOrdId} if present, otherwise returns a freshly-minted string from the
   * locked §4 format. This method allocates a {@code String} — it is only called on the cache-miss
   * / error path, never on the hot send path.
   *
   * @param parsed the parsed inbound message whose {@code clOrdIdOff}/{@code clOrdIdLen} are
   *     inspected
   * @return non-null ClOrdID string for the reject event
   */
  private String clOrdIdString(final MutableParsedMessage parsed) {
    if (parsed.clOrdIdOff >= 0 && parsed.clOrdIdLen > 0) {
      return new String(
          parsed.scratch,
          parsed.clOrdIdOff,
          parsed.clOrdIdLen,
          java.nio.charset.StandardCharsets.US_ASCII);
    }
    // Mint a ClOrdID for the reject event — the counter is NOT incremented here because no
    // FIX order was sent; we use the current counter value as a stable correlation tag.
    final byte[] scratch = new byte[JsonToFixTranslator.CLORDID_LENGTH];
    JsonToFixTranslator.mintClOrdId(instanceTag, sessionIdLong, clOrdIdCounter, scratch, 0);
    return new String(scratch, java.nio.charset.StandardCharsets.US_ASCII);
  }
}
