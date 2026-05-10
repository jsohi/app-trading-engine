package com.trading.engine.fixbridge.transport;

import com.trading.engine.fixbridge.json.BrowserEvent;
import com.trading.engine.websocket.JwtValidator.ValidatedClaims;

/**
 * Pull source for {@link BrowserEvent.AccountLimits} push frames (§3.14).
 *
 * <p><b>Why a SAM seam?</b> AccountLimits are sourced from the cluster's {@code AccountStore}
 * projection, which the bridge cannot reach directly — APP-40b's launcher wires a Aeron-cluster
 * client and provides the impl. Day 5 needs the {@code JwtAuthHandler} to push an initial
 * AccountLimits frame on AUTH_SUCCESS for every entitled account; the SAM lets the auth handler
 * stay launcher-agnostic.
 *
 * <p><b>Threading.</b> Implementations MAY be invoked from any Netty event loop. Impls MUST be
 * thread-safe (typically by holding only immutable references and shipping work through a lock-free
 * queue to the cluster client thread). Read-mostly, never on the hot per-message path.
 *
 * <p><b>Allocation.</b> Cold path (once per AUTH_SUCCESS); the impl MAY allocate on call. The
 * {@link BrowserEvent.AccountLimits} record allocations are unavoidable since the queue holds boxed
 * events.
 */
@FunctionalInterface
public interface AccountLimitsSource {

  /**
   * Look up the current pre-trade limits for every account named in the validated claims and push
   * one {@link BrowserEvent.AccountLimits} per account onto the supplied {@link Sink}.
   * Implementations that cannot resolve a particular account (cluster unavailable, account not
   * provisioned) MUST emit a {@link BrowserEvent.AccountLimits} with the safest pessimistic
   * defaults (zero qty / zero notional / 0 deviation / 0 OPS rate) rather than skipping — the UI
   * relies on at least one frame per claimed account so submit buttons remain disabled-by-default
   * if the source has no data.
   *
   * @param claims validated JWT claims (the {@code accounts} list drives which accounts to push)
   * @param session the bridge session whose outbound queue is the destination (informational — the
   *     impl uses {@code sink} for emission, not the session directly)
   * @param sink one-call-per-account sink for emitting AccountLimits
   */
  void pushFor(ValidatedClaims claims, BridgeSession session, Sink sink);

  /**
   * Single-method emitter used by {@link AccountLimitsSource#pushFor} so the source doesn't couple
   * to the per-session {@link OutboundQueue}'s drop-policy contract directly. The dispatcher (auth
   * handler) wires the sink to its session's queue.
   */
  @FunctionalInterface
  interface Sink {

    /**
     * Emit one AccountLimits frame for one account.
     *
     * @param event the limits frame to enqueue
     * @return queue offer outcome — caller may inspect for backpressure escalation
     */
    OutboundQueue.OfferResult emit(BrowserEvent.AccountLimits event);
  }

  /**
   * Pessimistic-default source used by tests and by the bootstrap until the launcher's real impl
   * lands. Emits one {@link BrowserEvent.AccountLimits} per claimed account with all-zero limits
   * (zero qty, zero notional, zero deviation, zero OPS rate) — honouring the {@link #pushFor}
   * contract that says "the UI relies on at least one frame per claimed account so submit buttons
   * remain disabled-by-default if the source has no data".
   *
   * <p>Renamed from a true no-op (which violated the contract by emitting nothing — flagged by
   * CodeRabbit on PR #70) to a fail-secure default that keeps UI submit buttons disabled until the
   * real launcher binding lands.
   */
  AccountLimitsSource NOOP =
      (claims, session, sink) -> {
        // Index-based loop over the claims.accounts() list — no Iterator allocation. The cold
        // path (once per AUTH_SUCCESS) tolerates the per-account record allocation since the
        // OutboundQueue inherently boxes events.
        final var accounts = claims.accounts();
        for (int i = 0, n = accounts.size(); i < n; i++) {
          final var account = accounts.get(i);
          sink.emit(new BrowserEvent.AccountLimits(account, 0L, 0L, 0, 0));
        }
      };
}
