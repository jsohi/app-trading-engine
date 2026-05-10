package com.trading.engine.fixbridge.transport;

/**
 * Application-defined WebSocket close codes used by the FIX client bridge (§3.3 close-code
 * registry).
 *
 * <p>Codes are drawn from the IANA "private use" range {@code 4000-4999}. Distinct codes are
 * assigned per failure class so the browser can branch on exit reason without parsing a body.
 *
 * <p><b>Threading.</b> Stateless constants — safe across all threads.
 *
 * <p><b>Allocation.</b> None.
 */
public final class BridgeCloseCodes {

  /**
   * {@code 4001} — JWT lifetime exhausted ({@code AuthExpired} event sent immediately before close
   * per locked §13). Distinct from {@link #POLICY_VIOLATION} so the browser can opt to re-prompt
   * for credentials silently rather than surface a hard failure.
   */
  public static final int AUTH_EXPIRED = 4001;

  /**
   * {@code 4002} — Sign-out from another tab. Sent to all-other sessions of the same {@code sub}
   * after a successful {@code signOut()} on one session (§3.3 / §3.7 / §4.9). Browser surfaces a
   * passive "signed out in another tab" notice.
   */
  public static final int SESSION_TERMINATED = 4002;

  /**
   * {@code 4008} — Policy violation. Catch-all for: missing or non-allowlisted Origin header,
   * remote-IP changed mid-session under {@code ip_pinned=true}, repeated auth failures triggering
   * tarpit, malformed first frame, oversized frame, and any other contract breach the bridge
   * detects after handshake.
   */
  public static final int POLICY_VIOLATION = 4008;

  private BridgeCloseCodes() {}
}
