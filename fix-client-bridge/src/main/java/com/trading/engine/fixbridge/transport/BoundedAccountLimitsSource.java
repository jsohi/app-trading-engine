package com.trading.engine.fixbridge.transport;

import com.trading.engine.fixbridge.json.BrowserEvent;
import com.trading.engine.websocket.JwtValidator.ValidatedClaims;

/**
 * Production {@link AccountLimitsSource} impl. Iterates the validated JWT claims' account list and
 * pushes one {@link BrowserEvent.AccountLimits} frame per account onto the caller-supplied {@link
 * AccountLimitsSource.Sink}.
 *
 * <p><b>Purpose.</b> Replaces {@link AccountLimitsSource#NOOP} for production. Per-process
 * singleton; the launcher constructs one instance at boot and wires it into every session's {@code
 * JwtAuthHandler}.
 *
 * <p><b>Pessimistic defaults.</b> When {@link AccountLimitsProvider#lookup} returns {@code null}
 * for an account (cluster unavailable, account not provisioned), this impl emits a {@link
 * BrowserEvent.AccountLimits} with all numeric limits set to {@code 0}. The UI's disabled-by-
 * default gating relies on receiving at least one frame per claimed account; a zero-limits frame
 * keeps submit buttons disabled until the cluster provides real data.
 *
 * <p><b>Threading.</b> Thread-safe by design — the only mutable state is the injected {@link
 * AccountLimitsProvider}, which must be thread-safe itself. {@link #pushFor} iterates the immutable
 * JWT claims and delegates each lookup to the provider; no per-call mutable state is held.
 *
 * <p><b>Allocation.</b> Cold path (once per {@code AUTH_SUCCESS}). Allocates one {@link
 * BrowserEvent.AccountLimits} record per account per call — unavoidable because the record is
 * enqueued by reference.
 *
 * <p><b>Lifecycle.</b> Per-process singleton. Constructed at boot by the launcher; never closed.
 *
 * <p><b>Dependencies.</b> {@link AccountLimitsProvider} (cluster-backed in production).
 *
 * @see AccountLimitsSource
 * @see AccountLimitsProvider
 * @see BrowserEvent.AccountLimits
 */
public final class BoundedAccountLimitsSource implements AccountLimitsSource {

  /**
   * Pessimistic default quantity when the provider has no data — {@code 0} disables order entry
   * until real limits arrive. Fixed-point int64 (scale {@code 10^-8}).
   */
  private static final long PESSIMISTIC_QTY = 0L;

  /**
   * Pessimistic default notional when the provider has no data. Fixed-point int64 (scale {@code
   * 10^-8}).
   */
  private static final long PESSIMISTIC_NOTIONAL = 0L;

  /** Pessimistic price-deviation limit in basis points; {@code 0} means no deviation allowed. */
  private static final int PESSIMISTIC_DEVIATION_BPS = 0;

  /** Pessimistic order rate limit in operations per second; {@code 0} disables new orders. */
  private static final int PESSIMISTIC_MAX_OPS = 0;

  /**
   * Cluster-backed provider for per-account limits. Must be thread-safe — this class is a per-
   * process singleton called from multiple Netty event loops concurrently.
   */
  private final AccountLimitsProvider provider;

  /**
   * Constructs the production account-limits source.
   *
   * @param provider cluster-backed limits provider; must be thread-safe and never {@code null}
   * @throws NullPointerException if {@code provider} is {@code null}
   */
  public BoundedAccountLimitsSource(final AccountLimitsProvider provider) {
    if (provider == null) {
      throw new NullPointerException("provider must not be null");
    }
    this.provider = provider;
  }

  /**
   * {@inheritDoc}
   *
   * <p>For each account in {@code claims.accounts()}:
   *
   * <ol>
   *   <li>Calls {@link AccountLimitsProvider#lookup(String)}.
   *   <li>If non-null, emits the returned frame via {@code sink}.
   *   <li>If {@code null} (account not provisioned), emits a pessimistic-defaults frame so the UI
   *       still receives a frame and keeps submit buttons disabled.
   * </ol>
   *
   * <p>Exceptions thrown by the provider are propagated to the caller without being swallowed.
   */
  @Override
  public void pushFor(final ValidatedClaims claims, final BridgeSession session, final Sink sink) {
    final var accounts = claims.accounts();
    for (int i = 0, n = accounts.size(); i < n; i++) {
      final var account = accounts.get(i);
      var limits = provider.lookup(account);
      if (limits == null) {
        // Account not provisioned: emit pessimistic defaults so the UI receives a frame and
        // gates order entry off until real limits land via a subsequent push.
        limits =
            new BrowserEvent.AccountLimits(
                account,
                PESSIMISTIC_QTY,
                PESSIMISTIC_NOTIONAL,
                PESSIMISTIC_DEVIATION_BPS,
                PESSIMISTIC_MAX_OPS);
      }
      final var result = sink.emit(limits);
      if (result == OutboundQueue.OfferResult.TERMINAL) {
        // The per-session outbound queue overflowed before we could push every account's limits.
        // Caller (typically JwtAuthHandler.completeAuthOnEventLoop) is responsible for the §3.1
        // step-5 escalation (fatal BridgeStatus + channel close); we just stop pushing because
        // every subsequent emit would also return TERMINAL. Honouring the OutboundQueue contract
        // per Gemini medium finding on PR #70 R3 — prior code silently swallowed the TERMINAL
        // result and continued iterating, hiding the overflow from the operator.
        return;
      }
    }
  }
}
