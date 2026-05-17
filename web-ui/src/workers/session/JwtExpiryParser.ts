/**
 * Defensive client-side JWT timing validator.
 *
 * Used by {@link AuthClient.handleAuthExpiringSoon} to refuse a freshly
 * minted token whose `nbf` (not-before) is still in the future or whose
 * `exp` (expiration) is already in the past. The server is the real
 * signature authority — this is a fast pre-flight that catches a clearly
 * unusable token before we burn a reauth round-trip and surface the
 * resulting `AuthenticationFailed` error to the UI.
 *
 * **Leeway.** A {@code DEFAULT_LEEWAY_SECONDS = 60} clock-skew tolerance
 * is applied in BOTH directions (nbf allowed up to leeway in the future;
 * exp allowed up to leeway in the past) to match the server's `nbf` /
 * `exp` validation window in {@code JwtValidator.java}.
 *
 * **Allocation.** One {@code atob} decode + one {@code JSON.parse} per
 * call. Hot enough to live on the AuthClient cold path (one per reauth)
 * but not the per-frame fast path.
 */

export const DEFAULT_LEEWAY_SECONDS = 60;

export type JwtTimingValidationResult =
  | { readonly ok: true; readonly expSec: number; readonly nbfSec: number | null }
  | { readonly ok: false; readonly reason: "MALFORMED" | "NOT_YET_VALID" | "EXPIRED" };

interface JwtTimingClaims {
  readonly exp?: unknown;
  readonly nbf?: unknown;
}

/**
 * Validate the timing claims of an unverified JWT compact-serialised
 * token. Caller MUST treat the result as advisory only — the server is
 * still the signature authority. Returns a discriminated result so the
 * caller can branch on the specific failure (and surface a precise UI
 * message instead of a single generic "auth failed").
 *
 * @param token compact-serialised JWT (header.payload.signature)
 * @param nowSeconds current wall-clock time in seconds since epoch
 *     (caller supplies — keeps the function pure for tests)
 * @param leewaySeconds clock-skew tolerance in seconds; default
 *     {@link DEFAULT_LEEWAY_SECONDS}
 */
export function validateJwtTiming(
  token: string,
  nowSeconds: number,
  leewaySeconds: number = DEFAULT_LEEWAY_SECONDS,
): JwtTimingValidationResult {
  const parts = token.split(".");
  if (parts.length !== 3) {
    return { ok: false, reason: "MALFORMED" };
  }
  const payloadB64 = parts[1];
  if (payloadB64 === undefined || payloadB64.length === 0) {
    return { ok: false, reason: "MALFORMED" };
  }
  let claims: JwtTimingClaims;
  try {
    // JWT payloads use base64url; atob accepts base64. Convert URL-safe
    // chars (`-` → `+`, `_` → `/`) and re-pad to a multiple of 4.
    const normalised = payloadB64.replace(/-/g, "+").replace(/_/g, "/");
    const padded = normalised + "===".slice((normalised.length + 3) % 4);
    const json = atob(padded);
    claims = JSON.parse(json) as JwtTimingClaims;
  } catch {
    return { ok: false, reason: "MALFORMED" };
  }
  if (typeof claims.exp !== "number" || !Number.isFinite(claims.exp)) {
    return { ok: false, reason: "MALFORMED" };
  }
  const expSec = claims.exp;
  const nbfSec = typeof claims.nbf === "number" && Number.isFinite(claims.nbf) ? claims.nbf : null;
  // nbf gate: token cannot become valid more than `leewaySeconds` from now.
  if (nbfSec !== null && nbfSec > nowSeconds + leewaySeconds) {
    return { ok: false, reason: "NOT_YET_VALID" };
  }
  // exp gate: token must still be valid (with leeway looking backwards).
  if (expSec + leewaySeconds <= nowSeconds) {
    return { ok: false, reason: "EXPIRED" };
  }
  return { ok: true, expSec, nbfSec };
}
