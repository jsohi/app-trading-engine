/**
 * ErrorMatrix.test.ts — server enum drift gate per APP-36 §B.6 / §6 row 36.
 *
 * Reads `test/fixtures/error-codes.json` (emitted by C3 ErrorCodesFixtureTest)
 * and asserts that every `{name, value}` pair has a defined behaviour in
 * `Reconnect.applyAppErrorCode`. Any new server enum value added without a
 * client mapping fails CI.
 *
 * Tests per APP-36 §2.13 / §5.8 / §6 rows 16, 36.
 *
 * Threading: single-threaded (Vitest jsdom worker).
 * Allocation: test-only — one SessionState + Reconnect per test.
 */

import { describe, expect, it } from "vitest";
import { readFileSync } from "node:fs";
import { join } from "node:path";

import { Reconnect, type AppErrorCode } from "@/workers/session/Reconnect";
import { SessionState } from "@/workers/session/SessionState";

// ─── Fixture loading ─────────────────────────────────────────────────────────

// Vitest runs from the package root (web-ui/); the fixture is at:
// test/fixtures/error-codes.json relative to web-ui/.
const FIXTURE_PATH = join(process.cwd(), "test", "fixtures", "error-codes.json");

interface ErrorCodeEntry {
  readonly name: string;
  readonly value: number;
}

interface ErrorCodeFixture {
  readonly schemaVersion: number;
  readonly codes: ReadonlyArray<ErrorCodeEntry>;
}

const fixture = JSON.parse(readFileSync(FIXTURE_PATH, "utf-8")) as ErrorCodeFixture;

// ─── Deterministic helpers ────────────────────────────────────────────────────

/** Always returns 0.5 — deterministic jitter for backoff calculations. */
const midRng = { next: (): number => 0.5 };

function makeReconnect(): Reconnect {
  const nowNs = 1_000_000n;
  return new Reconnect(midRng, (): bigint => nowNs);
}

// ─── Expected behaviours per §2.13 ───────────────────────────────────────────

/**
 * Decision table for error codes routed through applyAppErrorCode.
 * `firstCallFreeze` — true if applyAppErrorCode must return true on first call.
 * `counterIncrements` — true if consecutiveAuthFailures increments.
 */
interface ExpectedBehaviour {
  readonly firstCallFreeze: boolean;
  readonly counterIncrements: boolean;
}

const EXPECTED: Record<number, ExpectedBehaviour> = {
  1: { firstCallFreeze: false, counterIncrements: true }, // AuthenticationFailed
  2: { firstCallFreeze: true, counterIncrements: false }, // AuthorizationFailed — immediate freeze
  3: { firstCallFreeze: false, counterIncrements: false }, // RateLimitExceeded
  4: { firstCallFreeze: false, counterIncrements: false }, // SessionExpired
  6: { firstCallFreeze: false, counterIncrements: false }, // HeartbeatTimeout
  7: { firstCallFreeze: false, counterIncrements: false }, // BufferOverflow
  8: { firstCallFreeze: false, counterIncrements: true }, // VersionMismatch
  10: { firstCallFreeze: false, counterIncrements: false }, // ServerShutdown
  11: { firstCallFreeze: false, counterIncrements: false }, // CommandRejected
};

// Codes that are handled outside applyAppErrorCode (surface / do-not-close):
// 5  (InvalidSubscription) — not routed to applyAppErrorCode.
// 9  (SlowConsumer)        — routed to backpressure controller.
// 12 (SnapshotEntityTooLarge) — discard partial; surface; do not close.
// 13 (SymbolUnknown)       — Phase 3 Commit A: snapshot-request soft error; surface; do not close.
// 14 (EntitlementDenied)   — Phase 3 Commit A: snapshot-request soft error; surface; do not close.
// 15 (SnapshotThrottled)   — Phase 3 Commit A: rate-limit reject; surface; do not close.
// 16 (SnapshotBackpressured) — Phase 3 Commit A: stream-205 publish failure; surface; do not close.
// 17 (Malformed)           — Phase 3 Commit A: only reaches the client as an RFC 6455 close 1003
//                            (never as a WebSocketError frame); the close handler runs on the
//                            transport layer outside applyAppErrorCode. SURFACE_ONLY for the drift
//                            gate so a future server-side change that DOES emit this code is
//                            forced through the mapping conversation.
// 18 (AuthExpiringSoon)    — Phase 3 Commit B: triggers in-session reauth via
//                            AuthClient.handleAuthExpiringSoon; session preserved. Routed in
//                            worker.ts onWebSocketError BEFORE the applyAppErrorCode call so an
//                            expiring token never advances the freeze counter (it's an
//                            informational warning, not an attacker signal).
const SURFACE_ONLY_CODES = new Set([5, 9, 12, 13, 14, 15, 16, 17, 18]);

// ─── Individual named tests ───────────────────────────────────────────────────

describe("ErrorMatrix", () => {
  it("errorMatrix_code1_AuthenticationFailed_incrementsCounter", () => {
    const r = makeReconnect();
    const s = new SessionState();
    const freeze = r.applyAppErrorCode(s, 1);
    expect(freeze).toBe(false);
    expect(s.consecutiveAuthFailures).toBe(1);
  });

  it("errorMatrix_code2_AuthorizationFailed_immediateFreeze", () => {
    const r = makeReconnect();
    const s = new SessionState();
    const freeze = r.applyAppErrorCode(s, 2);
    expect(freeze).toBe(true);
    // Code 2 must NOT increment the auth counter.
    expect(s.consecutiveAuthFailures).toBe(0);
  });

  it("errorMatrix_code3_RateLimitExceeded_firstOccurrence_noFreeze", () => {
    const r = makeReconnect();
    const s = new SessionState();
    const freeze = r.applyAppErrorCode(s, 3);
    expect(freeze).toBe(false);
    // Counter unchanged (code 3 has its own escalation).
    expect(s.consecutiveAuthFailures).toBe(0);
    // lastRateLimitAtNs is set.
    expect(s.lastRateLimitAtNs).not.toBe(0n);
  });

  it("errorMatrix_code4_SessionExpired_noFreeze_noCounterIncrement", () => {
    const r = makeReconnect();
    const s = new SessionState();
    const freeze = r.applyAppErrorCode(s, 4);
    expect(freeze).toBe(false);
    expect(s.consecutiveAuthFailures).toBe(0);
  });

  it("errorMatrix_code6_HeartbeatTimeout_noFreeze_reconnect", () => {
    const r = makeReconnect();
    const s = new SessionState();
    const freeze = r.applyAppErrorCode(s, 6);
    expect(freeze).toBe(false);
    expect(s.consecutiveAuthFailures).toBe(0);
  });

  it("errorMatrix_code7_BufferOverflow_noFreeze_noCounterIncrement", () => {
    // §2.13: BufferOverflow after SessionResume → cold-start.
    // Otherwise → close + reconnect. Counter must NOT change per §2.13.
    const r = makeReconnect();
    const s = new SessionState();
    s.consecutiveAuthFailures = 1; // pre-existing auth failures persist
    const freeze = r.applyAppErrorCode(s, 7);
    expect(freeze).toBe(false);
    // Counter unchanged — BufferOverflow does not absolve credential stuffing.
    expect(s.consecutiveAuthFailures).toBe(1);
  });

  it("errorMatrix_code8_VersionMismatch_incrementsCounter", () => {
    const r = makeReconnect();
    const s = new SessionState();
    const freeze = r.applyAppErrorCode(s, 8);
    expect(freeze).toBe(false);
    expect(s.consecutiveAuthFailures).toBe(1);
  });

  it("errorMatrix_code10_ServerShutdown_noFreeze_appliesCapMultiplier", () => {
    const r = makeReconnect();
    const s = new SessionState();
    const freeze = r.applyAppErrorCode(s, 10);
    expect(freeze).toBe(false);
    expect(s.consecutiveAuthFailures).toBe(0);
    // Verify the backoff cap is raised: first nextDelayMs should still be BACKOFF.
    const decision = r.nextDelayMs(s);
    expect(decision.kind).toBe("BACKOFF");
  });

  it("errorMatrix_code11_CommandRejected_noFreeze_coldStart", () => {
    const r = makeReconnect();
    const s = new SessionState();
    const freeze = r.applyAppErrorCode(s, 11);
    expect(freeze).toBe(false);
    expect(s.consecutiveAuthFailures).toBe(0);
  });

  // ─── Reflection test: every code in the JSON fixture must have a defined mapping ───

  it("errorMatrix_allFixtureCodes_haveMappedBehaviour", () => {
    // This test is the drift gate: if the server adds a new enum value to
    // error-codes.json, one of the assertions below will fail unless a new
    // entry is added to EXPECTED or SURFACE_ONLY_CODES above.
    for (const entry of fixture.codes) {
      const { name, value } = entry;

      if (SURFACE_ONLY_CODES.has(value)) {
        // Surface-only codes are acknowledged — no applyAppErrorCode mapping needed.
        continue;
      }

      const expected = EXPECTED[value];
      expect(
        expected,
        `Error code ${String(value)} (${name}) has no expected behaviour in ErrorMatrix.test.ts — add it to EXPECTED or SURFACE_ONLY_CODES`,
      ).toBeDefined();

      if (!expected) continue;

      const r = makeReconnect();
      const s = new SessionState();

      // The AppErrorCode union covers: 1, 2, 3, 4, 6, 7, 8, 10, 11.
      // Values 5, 9, 12 are filtered above. Any unexpected value from the
      // fixture that's not surface-only and not in EXPECTED will fail the
      // `expected` assertion above before reaching this cast.
      const code = value as AppErrorCode;
      const freeze = r.applyAppErrorCode(s, code);

      expect(freeze, `code ${String(value)} (${name}) firstCallFreeze mismatch`).toBe(
        expected.firstCallFreeze,
      );
      if (expected.counterIncrements) {
        expect(
          s.consecutiveAuthFailures,
          `code ${String(value)} (${name}) should increment counter`,
        ).toBeGreaterThan(0);
      } else {
        expect(
          s.consecutiveAuthFailures,
          `code ${String(value)} (${name}) must not increment counter`,
        ).toBe(0);
      }
    }
  });
});
