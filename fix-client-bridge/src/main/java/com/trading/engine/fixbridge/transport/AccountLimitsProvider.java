package com.trading.engine.fixbridge.transport;

import com.trading.engine.fixbridge.json.BrowserEvent;

/**
 * SAM seam for looking up pre-trade account limits by account name. The real implementation is
 * backed by the cluster's {@code AccountStore} projection; the launcher wires it via an Aeron
 * cluster client. This SAM keeps {@link BoundedAccountLimitsSource} decoupled from the cluster
 * transport layer.
 *
 * <p><b>Threading.</b> Implementations MAY be called from any Netty event loop because {@link
 * AccountLimitsSource#pushFor} is triggered from the auth handler, which runs on a per- channel
 * Netty worker thread. Implementations MUST be thread-safe (typically by serving reads from an
 * immutable snapshot or a concurrent projection store updated by a separate cluster-client thread).
 *
 * <p><b>Allocation.</b> Cold path — called once per {@code AUTH_SUCCESS}. Implementations MAY
 * allocate a {@link BrowserEvent.AccountLimits} record per call; the record ends up in the per-
 * session outbound queue so allocation here is unavoidable.
 *
 * @see BoundedAccountLimitsSource
 * @see BrowserEvent.AccountLimits
 */
@FunctionalInterface
public interface AccountLimitsProvider {

  /**
   * Look up the current pre-trade limits for the named account.
   *
   * @param account FIX {@code Account (1)} identifier; never {@code null}
   * @return a fully-populated {@link BrowserEvent.AccountLimits} on success, or {@code null} when
   *     the account is not provisioned (unknown to the cluster projection)
   */
  BrowserEvent.AccountLimits lookup(String account);

  /**
   * No-op provider that always returns {@code null} (account not provisioned). Used at bootstrap
   * and in unit tests that do not exercise the projection path.
   */
  AccountLimitsProvider NOOP = account -> null;
}
