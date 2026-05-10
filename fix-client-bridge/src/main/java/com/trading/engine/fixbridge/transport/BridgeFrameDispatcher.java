package com.trading.engine.fixbridge.transport;

import com.trading.engine.fixbridge.json.MutableParsedMessage;
import java.util.function.Supplier;

/**
 * SAM dispatch hook invoked by {@link WsListener} once an inbound text frame has been parsed.
 *
 * <p><b>Why an interface?</b> Day 4-c only delivers the Netty front door (handshake, auth, frame
 * intake, backpressure). The downstream wiring — translating {@link MutableParsedMessage} into FIX
 * via {@code JsonToFixTranslator}, interacting with the Artio session, and shipping events through
 * the orchestrator — lands in subsequent days. Defining a single-method interface here lets the
 * front door land first with a no-op stub, then upgrade in place when the dispatch implementation
 * arrives.
 *
 * <p><b>Threading.</b> Implementations are invoked exclusively on the channel's Netty event loop.
 * Implementations MUST treat the supplied {@code parsed} flyweight as <i>borrow-only</i>: its
 * scratch buffer is reset on every call.
 *
 * <p><b>Allocation.</b> Implementations MUST be zero-allocation on the hot path (per CLAUDE.md
 * §Cluster Service / §SBE Schema). Unit tests verify this via dedicated {@code *AllocTest}
 * regression suites.
 */
@FunctionalInterface
public interface BridgeFrameDispatcher {

  /**
   * Dispatch one parsed inbound frame. Called after {@link
   * com.trading.engine.fixbridge.json.BrowserMessageReader} has populated {@code parsed} and after
   * the per-type rate limiter has admitted the message type.
   *
   * @param session the per-channel session state (caller-owned, never null)
   * @param parsed the populated message flyweight (borrow only — caller resets on next frame)
   * @param messageType the {@code MutableParsedMessage.TYPE_*} sentinel (also accessible as {@code
   *     parsed.type}, hoisted here so implementations don't need to re-read the field)
   * @param nowNs the monotonic dispatch timestamp (from the listener's nano clock)
   */
  void dispatch(BridgeSession session, MutableParsedMessage parsed, int messageType, long nowNs);

  /**
   * No-op dispatcher used by Day 4-c integration tests and by the bootstrap until the real dispatch
   * path lands. Drops every parsed frame on the floor.
   */
  BridgeFrameDispatcher NOOP = (session, parsed, messageType, nowNs) -> {};

  /**
   * Per-session dispatcher factory. Required by {@link BridgeNettyBootstrap} so each authenticated
   * channel gets its own dispatcher instance — necessary because production dispatchers (e.g.
   * {@code RoutingBridgeFrameDispatcher} backed by per-session {@code ArtioFixCommandSink} state)
   * are NOT thread-safe and hold per-session state (per-session ClOrdID counter, per-session Artio
   * session adapter, per-channel remote IP supplier). Sharing a single instance across channels
   * would corrupt per-session state and break ClOrdID uniqueness (Gemini critical finding on PR #70
   * R3).
   *
   * <p>The factory is invoked on the channel event loop during the auth-success flow (after JWT
   * validation succeeds and the {@link BridgeSession} is constructed); it MUST NOT block.
   */
  @FunctionalInterface
  interface Factory {

    /**
     * Construct a per-session dispatcher.
     *
     * @param session the freshly-authenticated bridge session
     * @param remoteIpSupplier supplies the current remote IP for audit-row population (the
     *     dispatcher invokes this once per audited dispatch); typically a stable per-channel
     *     reference resolved at handshake
     * @return a freshly-allocated per-session dispatcher
     */
    BridgeFrameDispatcher create(BridgeSession session, Supplier<String> remoteIpSupplier);

    /**
     * Factory variant that always returns the {@link #NOOP} dispatcher. Used by Day 4-c bring-up
     * tests and by the bootstrap until the launcher binds the real per-session factory (slated for
     * the launcher follow-up PR).
     */
    Factory NOOP_FACTORY = (session, remoteIpSupplier) -> NOOP;
  }
}
