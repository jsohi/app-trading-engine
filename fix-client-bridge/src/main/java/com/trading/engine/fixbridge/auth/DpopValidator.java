package com.trading.engine.fixbridge.auth;

import com.trading.engine.websocket.JwtValidator.ValidatedClaims;

/**
 * DPoP (Demonstration of Proof of Possession, RFC 9449) validator hook (§3.3 / §B-r2-7 / Round-5
 * finding 2).
 *
 * <p><b>Why a SAM seam?</b> The bridge module's role is the wire-format + Netty front door. Real
 * DPoP validation requires nimbus-jose's {@code DPoPProofVerifier} (or equivalent) which is a
 * cryptographic component the launcher binds at startup with a JWK source / nonce store / replay
 * cache. Putting the validation behind a single-method interface lets the bridge invoke the
 * DPoP-bind check on every authenticated WebSocket frame's bearer JWT (initially per-Auth, future
 * per-frame if the JWT cnf.jkt rotates) without dragging the crypto primitives into this module's
 * dependency surface. The {@link OrderRejectReason#STALE_DPOP STALE_DPOP} taxonomy entry has
 * existed since Day 1 specifically for the runtime check this seam enables.
 *
 * <p><b>Three valid outcomes</b>:
 *
 * <ul>
 *   <li>{@link Result#VALID} — the JWT's {@code cnf.jkt} (JWK thumbprint) matches the DPoP proof's
 *       public key, the proof JWT signature verifies, the {@code htm}/{@code htu}/{@code jti}/
 *       {@code iat} claims are all valid, and the {@code jti} has not been replayed inside the
 *       configured window.
 *   <li>{@link Result#STALE_DPOP} — the {@code cnf.jkt} has rotated relative to the DPoP key (token
 *       was issued against an old key the user has since rotated). The bridge MUST close with
 *       {@link com.trading.engine.fixbridge.transport.BridgeCloseCodes#AUTH_EXPIRED} and emit
 *       {@link com.trading.engine.fixbridge.json.OrderRejectReason#STALE_DPOP STALE_DPOP} so the
 *       worker knows to silently re-mint a fresh token-and-DPoP pair (per §B-r2-7) instead of
 *       prompting for credentials.
 *   <li>{@link Result#INVALID} — the proof signature failed, claims are out of band, or the {@code
 *       jti} is replayed. The bridge MUST close with {@link
 *       com.trading.engine.fixbridge.transport.BridgeCloseCodes#POLICY_VIOLATION} (auth-failed
 *       single error code, no oracle leak per §3.3).
 * </ul>
 *
 * <p><b>Default binding.</b> {@link #NOOP} returns {@link Result#VALID} for every call, matching
 * the pre-DPoP behaviour where bearer-JWT validation alone is sufficient. Production deployments
 * MUST replace this with a real impl when the worker is required to present DPoP proofs (this
 * decision is per-deployment — some institutional deployments treat the per-IP pinning + JWT JTI
 * revocation as sufficient binding without DPoP, others require DPoP for regulatory reasons).
 *
 * <p><b>Threading.</b> Implementations MAY be invoked from any Netty event loop. Impls MUST be
 * thread-safe (typically by holding only immutable JWK references + a thread-safe nonce/replay
 * cache). The validation is on the auth cold path (per-connection, not per-message).
 *
 * <p><b>Allocation.</b> Cold path; impls MAY allocate.
 */
@FunctionalInterface
public interface DpopValidator {

  /** Outcome of a single {@link #validate} call. */
  enum Result {
    /** DPoP proof + JWT cnf.jkt binding all check out. Auth proceeds. */
    VALID,
    /**
     * JWT was issued against a DPoP key the worker has since rotated. The bridge closes with {@code
     * AUTH_EXPIRED} (4001) and emits {@link
     * com.trading.engine.fixbridge.json.OrderRejectReason#STALE_DPOP} so the worker silently
     * re-mints rather than prompting the user.
     */
    STALE_DPOP,
    /**
     * DPoP proof failed any of: signature verification, {@code htm}/{@code htu}/{@code iat} claims,
     * replay-cache check on {@code jti}. The bridge closes with {@code POLICY_VIOLATION} (4008)
     * using the single auth-failed error code (no oracle leak per §3.3).
     */
    INVALID
  }

  /**
   * Validate the DPoP proof presented alongside a bearer JWT.
   *
   * @param claims the validated bearer-JWT claims (already passed the JWT's own signature +
   *     algorithm + iss/aud/exp/nbf/iat/sub/jti/accounts/ip_pinned/roles checks via {@link
   *     com.trading.engine.websocket.JwtValidator}); the {@code cnf.jkt} claim — if present — is
   *     the JWK thumbprint the DPoP key MUST match
   * @param dpopProofHeader the raw {@code DPoP} HTTP header value from the WebSocket upgrade
   *     request (typically a single compact-serialised JWS); may be {@code null} or empty when the
   *     worker did not present a DPoP proof — in which case impls SHOULD return {@link
   *     Result#VALID} when the bearer JWT lacks {@code cnf.jkt} (DPoP not required) or {@link
   *     Result#INVALID} when the bearer carries {@code cnf.jkt} but no proof was sent
   * @return one of {@link Result#VALID}, {@link Result#STALE_DPOP}, {@link Result#INVALID}
   */
  Result validate(ValidatedClaims claims, String dpopProofHeader);

  /**
   * No-op validator that returns {@link Result#VALID} for every call. Bridge default — matches
   * pre-DPoP semantics where bearer-JWT validation alone is sufficient. Production deployments that
   * REQUIRE DPoP MUST replace this with a real cryptographic impl.
   */
  DpopValidator NOOP = (claims, dpopProofHeader) -> Result.VALID;
}
