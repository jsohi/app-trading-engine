/**
 * JwtExpiryParser.test.ts — unit tests for validateJwtTiming per Phase 3
 * Commit B (B.7 AuthExpiringSoon path).
 *
 * JWT fixtures are hand-constructed 3-part base64url tokens; only the
 * payload's exp/nbf claims are inspected by the parser. Header and
 * signature slots carry minimal placeholders (`e30` = `{}` base64url,
 * `sig` respectively).
 *
 * Test naming follows `<unit>_<scenario>_<expectedBehavior>` per plan §5.8.
 *
 * Threading: single-threaded (Vitest jsdom).
 */

import { describe, expect, it } from "vitest";
import { validateJwtTiming, DEFAULT_LEEWAY_SECONDS } from "@/workers/session/JwtExpiryParser";

// ─── Fixture helpers ─────────────────────────────────────────────────────────

/**
 * Build a compact-serialised JWT from a plain-object payload. The header is
 * always `{}` and the signature is always `"sig"` — sufficient because
 * validateJwtTiming only reads the payload.
 */
function makeJwt(payload: Record<string, unknown>): string {
  const encode = (obj: Record<string, unknown>): string => {
    const json = JSON.stringify(obj);
    // btoa requires binary string; payload is ASCII-safe for all numeric claims
    const b64 = btoa(json);
    // base64url: strip `=` padding, swap `+`→`-`, `/`→`_`
    return b64.replace(/=+$/, "").replace(/\+/g, "-").replace(/\//g, "_");
  };
  const header = encode({});
  const payloadB64 = encode(payload);
  return `${header}.${payloadB64}.sig`;
}

const NOW = 1_700_000_000; // arbitrary epoch-seconds fixture

// ─── Tests ───────────────────────────────────────────────────────────────────

describe("validateJwtTiming", () => {
  it("validateJwtTiming_validToken_returnsOkWithExpAndNbf", () => {
    const expSec = NOW + 3_600;
    const nbfSec = NOW - 60;
    const token = makeJwt({ exp: expSec, nbf: nbfSec });

    const result = validateJwtTiming(token, NOW);

    expect(result.ok).toBe(true);
    if (!result.ok) return;
    expect(result.expSec).toBe(expSec);
    expect(result.nbfSec).toBe(nbfSec);
  });

  it("validateJwtTiming_validTokenWithoutNbf_returnsOkWithNbfNull", () => {
    const expSec = NOW + 3_600;
    const token = makeJwt({ exp: expSec });

    const result = validateJwtTiming(token, NOW);

    expect(result.ok).toBe(true);
    if (!result.ok) return;
    expect(result.expSec).toBe(expSec);
    expect(result.nbfSec).toBeNull();
  });

  it("validateJwtTiming_missingExpClaim_returnsMalformed", () => {
    const token = makeJwt({ sub: "user-123" });

    const result = validateJwtTiming(token, NOW);

    expect(result.ok).toBe(false);
    if (result.ok) return;
    expect(result.reason).toBe("MALFORMED");
  });

  it("validateJwtTiming_expiredToken_noLeeway_returnsExpired", () => {
    // exp is exactly `NOW - DEFAULT_LEEWAY_SECONDS - 1` so the leeway cannot save it
    const expSec = NOW - DEFAULT_LEEWAY_SECONDS - 1;
    const token = makeJwt({ exp: expSec });

    const result = validateJwtTiming(token, NOW, DEFAULT_LEEWAY_SECONDS);

    expect(result.ok).toBe(false);
    if (result.ok) return;
    expect(result.reason).toBe("EXPIRED");
  });

  it("validateJwtTiming_expiredTokenWithinLeeway_returnsOk", () => {
    // exp is 30 s in the past — within the 60 s default leeway
    const expSec = NOW - 30;
    const token = makeJwt({ exp: expSec });

    const result = validateJwtTiming(token, NOW, DEFAULT_LEEWAY_SECONDS);

    // expSec + leeway (60) > NOW (0 + 30 = 30 > 0), so ok
    expect(result.ok).toBe(true);
  });

  it("validateJwtTiming_futureNbfBeyondLeeway_returnsNotYetValid", () => {
    // nbf is 90 s in the future — beyond the 60 s default leeway
    const expSec = NOW + 3_600;
    const nbfSec = NOW + DEFAULT_LEEWAY_SECONDS + 30; // +90 s
    const token = makeJwt({ exp: expSec, nbf: nbfSec });

    const result = validateJwtTiming(token, NOW, DEFAULT_LEEWAY_SECONDS);

    expect(result.ok).toBe(false);
    if (result.ok) return;
    expect(result.reason).toBe("NOT_YET_VALID");
  });

  it("validateJwtTiming_futureNbfWithinLeeway_returnsOk", () => {
    // nbf is 30 s in the future — within the 60 s leeway
    const expSec = NOW + 3_600;
    const nbfSec = NOW + 30;
    const token = makeJwt({ exp: expSec, nbf: nbfSec });

    const result = validateJwtTiming(token, NOW, DEFAULT_LEEWAY_SECONDS);

    expect(result.ok).toBe(true);
  });

  it("validateJwtTiming_onePart_returnsMalformed", () => {
    const result = validateJwtTiming("onlyonepart", NOW);

    expect(result.ok).toBe(false);
    if (result.ok) return;
    expect(result.reason).toBe("MALFORMED");
  });

  it("validateJwtTiming_twoParts_returnsMalformed", () => {
    const result = validateJwtTiming("header.payload", NOW);

    expect(result.ok).toBe(false);
    if (result.ok) return;
    expect(result.reason).toBe("MALFORMED");
  });

  it("validateJwtTiming_nonBase64Payload_returnsMalformed", () => {
    // Payload slot is not valid base64url — atob will throw
    const result = validateJwtTiming("header.!!!not-base64!!!.sig", NOW);

    expect(result.ok).toBe(false);
    if (result.ok) return;
    expect(result.reason).toBe("MALFORMED");
  });

  it("validateJwtTiming_payloadNotJsonObject_returnsMalformed", () => {
    // base64url-encode a plain string (not a JSON object)
    const payloadB64 = btoa('"just-a-string"').replace(/=+$/, "");
    const result = validateJwtTiming(`e30.${payloadB64}.sig`, NOW);

    // exp claim will be undefined → typeof !== "number" → MALFORMED
    expect(result.ok).toBe(false);
    if (result.ok) return;
    expect(result.reason).toBe("MALFORMED");
  });

  it("validateJwtTiming_expNaN_returnsMalformed", () => {
    // exp is NaN — Number.isFinite(NaN) is false → MALFORMED
    const payloadObj = { exp: NaN };
    // JSON.stringify(NaN) becomes "null" — which is correct: `null` is not a number
    const payloadJson = '{"exp":null}';
    const payloadB64 = btoa(payloadJson).replace(/=+$/, "");
    const result = validateJwtTiming(`e30.${payloadB64}.sig`, NOW);

    expect(result.ok).toBe(false);
    if (result.ok) return;
    expect(result.reason).toBe("MALFORMED");
    void payloadObj; // suppress unused-var lint
  });

  it("validateJwtTiming_zeroLeeway_expiredByOneSecond_returnsExpired", () => {
    const expSec = NOW - 1; // expired 1 s ago
    const token = makeJwt({ exp: expSec });

    const result = validateJwtTiming(token, NOW, 0);

    expect(result.ok).toBe(false);
    if (result.ok) return;
    expect(result.reason).toBe("EXPIRED");
  });

  it("validateJwtTiming_emptyPayloadSlot_returnsMalformed", () => {
    const result = validateJwtTiming("header..sig", NOW);

    expect(result.ok).toBe(false);
    if (result.ok) return;
    expect(result.reason).toBe("MALFORMED");
  });
});
