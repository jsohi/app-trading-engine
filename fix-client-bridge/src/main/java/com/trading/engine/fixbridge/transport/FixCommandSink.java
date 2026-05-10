package com.trading.engine.fixbridge.transport;

import com.trading.engine.fixbridge.json.MutableParsedMessage;

/**
 * Per-session FIX command sink — abstracts the Artio {@code Session#trySend} binding (§3.1).
 *
 * <p><b>Why a SAM seam?</b> Day 5 of APP-40a needs the dispatcher to actually translate parsed
 * inbound JSON into FIX 4.4 and ship it through Artio, but the Artio session lifecycle (logon,
 * disconnect, sequence-reset, reconnect) is launcher-level wiring (APP-40b). Putting the wire-send
 * behind a 5-method interface lets the bridge module stay Artio-free at its public boundary while
 * the launcher provides the concrete impl that owns the Artio Session + per-type encoders + {@link
 * com.trading.engine.fixbridge.translator.JsonToFixTranslator}.
 *
 * <p><b>Implementation contract.</b> Each method takes the parsed flyweight as a borrow-only
 * reference (the dispatcher resets the flyweight on the next inbound frame), translates into the
 * impl-owned encoder, and calls {@code session.trySend(encoder)}. Returns Artio's send-position
 * ({@code &gt;= 0}) on success or {@code -1} on backpressure / session-down — the dispatcher
 * decides what to surface to the browser.
 *
 * <p><b>Threading.</b> Per-session impls are owned by the channel's Netty event loop. Methods are
 * never called concurrently for the same session. Cross-session impls (one shared sink with a
 * session-id parameter) are NOT supported by this interface — write a per-channel impl instead.
 *
 * <p><b>Allocation.</b> Implementations MUST be zero-allocation on the hot path. The Artio
 * encoders, {@link com.trading.engine.fixbridge.translator.JsonToFixTranslator}, and any
 * per-session ID counters all belong inside the impl as final fields.
 *
 * <p><b>AcceptQuote two-phase commit.</b> {@link #sendAcceptQuote} returns Artio's send-position;
 * the caller MUST evict the snapshot from the per-session quote cache only when the return value is
 * {@code &gt;= 0}. The translator's {@code quoteCacheToken} round-trip (locked §2) is folded into
 * the impl: the dispatcher hands the snapshot lookup result + token, and the impl does the eviction
 * itself when {@code trySend} succeeds.
 *
 * @see com.trading.engine.fixbridge.translator.JsonToFixTranslator
 */
public interface FixCommandSink {

  /** Sentinel returned by every {@code send*} method when the wire send is suppressed. */
  long NO_SEND = -1L;

  /**
   * Translate + send a {@code QuoteRequest (35=R)}.
   *
   * @param parsed populated parsed flyweight (type {@code TYPE_QUOTE_REQUEST})
   * @param nowNs monotonic dispatch timestamp
   * @return Artio send position ({@code &gt;= 0}) on success, {@link #NO_SEND} otherwise
   */
  long sendQuoteRequest(MutableParsedMessage parsed, long nowNs);

  /**
   * Translate + send a {@code NewOrderSingle (35=D)} for an {@code AcceptQuote} inbound (locked §2
   * two-phase commit). The impl reads the per-session quote cache to source symbol/side/qty/ price;
   * if no snapshot exists for {@code parsed.quoteId}, the impl emits an {@link
   * com.trading.engine.fixbridge.json.BrowserEvent.OrderReject} with reason {@code QUOTE_EXPIRED}
   * (or {@code QUOTE_UNKNOWN}) directly to the per-session outbound queue and returns {@link
   * #NO_SEND}.
   *
   * @param parsed populated parsed flyweight (type {@code TYPE_ACCEPT_QUOTE})
   * @param nowNs monotonic dispatch timestamp
   * @return Artio send position ({@code &gt;= 0}) on success, {@link #NO_SEND} otherwise
   */
  long sendAcceptQuote(MutableParsedMessage parsed, long nowNs);

  /**
   * Handle a {@code RejectQuote} inbound (locked §11 — emits no FIX). The impl evicts the
   * per-session quote cache slot for {@code parsed.quoteId}.
   *
   * @param parsed populated parsed flyweight (type {@code TYPE_REJECT_QUOTE})
   * @param nowNs monotonic dispatch timestamp
   * @return always {@link #NO_SEND} (no FIX wire activity)
   */
  long handleRejectQuote(MutableParsedMessage parsed, long nowNs);

  /**
   * Translate + send a direct-entry {@code NewOrderSingle (35=D)}.
   *
   * @param parsed populated parsed flyweight (type {@code TYPE_NEW_ORDER_SINGLE})
   * @param nowNs monotonic dispatch timestamp
   * @return Artio send position ({@code &gt;= 0}) on success, {@link #NO_SEND} otherwise
   */
  long sendNewOrderSingle(MutableParsedMessage parsed, long nowNs);

  /**
   * Translate + send an {@code OrderCancelRequest (35=F)}.
   *
   * @param parsed populated parsed flyweight (type {@code TYPE_CANCEL_ORDER})
   * @param nowNs monotonic dispatch timestamp
   * @return Artio send position ({@code &gt;= 0}) on success, {@link #NO_SEND} otherwise
   */
  long sendCancelOrder(MutableParsedMessage parsed, long nowNs);

  /**
   * Forward an {@code OrderStatusRequest} to the cluster's {@code OrderQueryByClOrdId} projection
   * (§3.15). The impl uses the projection result to enqueue an {@link
   * com.trading.engine.fixbridge.json.BrowserEvent.OrderStatusReply} on the per-session outbound
   * queue; {@link #NO_SEND} indicates no FIX wire activity (queries are projection-side, not
   * Artio-side).
   *
   * @param parsed populated parsed flyweight (type {@code TYPE_ORDER_STATUS_REQUEST})
   * @param nowNs monotonic dispatch timestamp
   * @return always {@link #NO_SEND}
   */
  long sendOrderStatusRequest(MutableParsedMessage parsed, long nowNs);

  /**
   * No-op sink used by tests and by the bootstrap until the launcher's real impl lands. Every
   * {@code send*} method returns {@link #NO_SEND}.
   */
  FixCommandSink NOOP =
      new FixCommandSink() {
        @Override
        public long sendQuoteRequest(final MutableParsedMessage parsed, final long nowNs) {
          return NO_SEND;
        }

        @Override
        public long sendAcceptQuote(final MutableParsedMessage parsed, final long nowNs) {
          return NO_SEND;
        }

        @Override
        public long handleRejectQuote(final MutableParsedMessage parsed, final long nowNs) {
          return NO_SEND;
        }

        @Override
        public long sendNewOrderSingle(final MutableParsedMessage parsed, final long nowNs) {
          return NO_SEND;
        }

        @Override
        public long sendCancelOrder(final MutableParsedMessage parsed, final long nowNs) {
          return NO_SEND;
        }

        @Override
        public long sendOrderStatusRequest(final MutableParsedMessage parsed, final long nowNs) {
          return NO_SEND;
        }
      };

  /**
   * Per-session factory invoked by {@link BridgeNettyBootstrap} to mint a fresh sink for each
   * authenticated channel. Lets the launcher capture per-session state (Artio session reference,
   * encoders, ID counter, quote cache, projection client) at session-start.
   */
  @FunctionalInterface
  interface Factory {

    /**
     * Mint a sink for the given session.
     *
     * @param session the freshly-minted bridge session (from {@code JwtAuthHandler})
     * @return a per-session sink
     */
    FixCommandSink create(BridgeSession session);

    /** Factory that always returns {@link FixCommandSink#NOOP}. */
    Factory NOOP = session -> FixCommandSink.NOOP;
  }
}
