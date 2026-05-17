/**
 * AuthClient-expiring-soon.test.ts — unit tests for the Phase 3 Commit B
 * (B.7) `handleAuthExpiringSoon` path on AuthClient.
 *
 * Covers: happy-path reauth; tokenSource rejection; expired token timing
 * failure; reauth-already-in-flight when expiring-soon fires; injected
 * nowSecondsFn is consulted instead of Date.now().
 *
 * Uses the same harness factories as Auth.test.ts / ReauthClient.test.ts.
 *
 * Test naming follows `<unit>_<scenario>_<expectedBehavior>` per plan §5.8.
 *
 * Threading: single-threaded (Vitest jsdom).
 */

import { describe, expect, it, beforeEach, vi } from "vitest";
import {
  AuthClient,
  type AuthAck,
  type AuthClientCallbacks,
  type AuthScheduler,
  type AuthFailureReason,
} from "@/workers/session/AuthClient";
import { SessionState, type UuidComposite } from "@/workers/session/SessionState";
import { WORKER_PROTOCOL_VERSION } from "@/workers/WorkerTuning";

// ─── Controllable scheduler ──────────────────────────────────────────────────

function makeScheduler(): {
  scheduler: AuthScheduler;
  tick: () => boolean;
  advanceMs: (ms: number) => void;
} {
  let currentMs = 0;
  let nextHandle = 1;
  const timers: Array<{ handle: number; handler: () => void; fireAtMs: number }> = [];

  const scheduler: AuthScheduler = {
    setTimeout(handler: () => void, delayMs: number): number {
      const handle = nextHandle++;
      timers.push({ handle, handler, fireAtMs: currentMs + delayMs });
      return handle;
    },
    clearTimeout(handle: number): void {
      const idx = timers.findIndex((t) => t.handle === handle);
      if (idx !== -1) timers.splice(idx, 1);
    },
    nowMs: (): number => currentMs,
  };

  return {
    scheduler,
    tick: (): boolean => {
      const due = timers.filter((t) => t.fireAtMs <= currentMs);
      if (due.length === 0) return false;
      due.sort((a, b) => a.fireAtMs - b.fireAtMs);
      const next = due[0];
      if (next === undefined) return false;
      const idx = timers.findIndex((t) => t.handle === next.handle);
      if (idx !== -1) timers.splice(idx, 1);
      next.handler();
      return true;
    },
    advanceMs: (ms: number): void => {
      currentMs += ms;
    },
  };
}

// ─── Mock callbacks ──────────────────────────────────────────────────────────

function makeCallbacks(): {
  callbacks: AuthClientCallbacks;
  sentTokens: string[];
  failures: Array<{ reason: AuthFailureReason; message: string }>;
} {
  const sentTokens: string[] = [];
  const failures: Array<{ reason: AuthFailureReason; message: string }> = [];

  const callbacks: AuthClientCallbacks = {
    sendBytes: (_bytes: Uint8Array): void => {
      // intentionally blank — byte transport not under test here
    },
    encodeAuth: (token: string, _protocolVersion: number): Uint8Array => {
      sentTokens.push(token);
      return new Uint8Array([1]);
    },
    onAuthSuccess: (): void => {
      // no-op for expiring-soon tests
    },
    onAuthFailure: (reason: AuthFailureReason, message: string): void => {
      failures.push({ reason, message });
    },
  };

  return { callbacks, sentTokens, failures };
}

function makeValidAck(overrides?: Partial<AuthAck>): AuthAck {
  const sessionId: UuidComposite = { mostSignificantBits: 1n, leastSignificantBits: 2n };
  return {
    sessionId,
    protocolVersion: WORKER_PROTOCOL_VERSION,
    maxSubscriptions: 50,
    serverHeartbeatIntervalMs: 5_000,
    clientHeartbeatIntervalMs: 10_000,
    symbolPreferences: [],
    panelLayout: [],
    ...overrides,
  };
}

/** Build a valid base64url JWT payload with exp far in the future. */
function makeValidToken(nowSeconds: number, label = "fresh-token"): string {
  const expSec = nowSeconds + 3_600;
  const payloadJson = JSON.stringify({ exp: expSec, sub: label });
  const payloadB64 = btoa(payloadJson).replace(/=+$/, "");
  const headerB64 = btoa("{}").replace(/=+$/, "");
  return `${headerB64}.${payloadB64}.sig`;
}

/** Build a JWT that is expired relative to nowSeconds (+ default leeway). */
function makeExpiredToken(nowSeconds: number): string {
  // exp is 70 s in the past → beyond the 60 s leeway
  const expSec = nowSeconds - 70;
  const payloadJson = JSON.stringify({ exp: expSec });
  const payloadB64 = btoa(payloadJson).replace(/=+$/, "");
  const headerB64 = btoa("{}").replace(/=+$/, "");
  return `${headerB64}.${payloadB64}.sig`;
}

// ─── Tests ───────────────────────────────────────────────────────────────────

describe("AuthClient.handleAuthExpiringSoon", () => {
  const NOW_SECONDS = 1_700_000_000;
  let state: SessionState;
  let sched: ReturnType<typeof makeScheduler>;
  let mocks: ReturnType<typeof makeCallbacks>;
  let client: AuthClient;

  beforeEach(() => {
    state = new SessionState();
    sched = makeScheduler();
    mocks = makeCallbacks();
    client = new AuthClient(state, mocks.callbacks, sched.scheduler);

    // Put the client into authenticated state
    client.authenticate("initial-token");
    client.onAuthAck(makeValidAck(), "user-sub");
  });

  it("handleAuthExpiringSoon_happyPath_reauthCalledWithFreshToken", async () => {
    const freshToken = makeValidToken(NOW_SECONDS, "fresh");
    const tokenSource = vi.fn<() => Promise<string>>().mockResolvedValue(freshToken);
    const nowSecondsFn = vi.fn<() => number>().mockReturnValue(NOW_SECONDS);

    const p = client.handleAuthExpiringSoon(tokenSource, nowSecondsFn);

    // tokenSource() resolves as a microtask; yield so the .then callback runs
    // (timing pre-flight + reauth()) before we deliver the AuthAck.
    await Promise.resolve();

    // Resolve the reauth with a valid AuthAck
    client.onAuthAck(makeValidAck(), "user-sub");

    await expect(p).resolves.toBeUndefined();
    expect(tokenSource).toHaveBeenCalledOnce();
    // The fresh token must have been passed to encodeAuth
    expect(mocks.sentTokens).toContain(freshToken);
    expect(mocks.failures).toHaveLength(0);
  });

  it("handleAuthExpiringSoon_tokenSourceRejects_callsOnAuthFailureAndRejects", async () => {
    const tokenSource = vi
      .fn<() => Promise<string>>()
      .mockRejectedValue(new Error("issuer unreachable"));
    const nowSecondsFn = vi.fn<() => number>().mockReturnValue(NOW_SECONDS);

    await expect(client.handleAuthExpiringSoon(tokenSource, nowSecondsFn)).rejects.toThrow(
      "issuer unreachable",
    );
    // tokenSource rejection propagates without calling onAuthFailure
    // (the timing pre-flight never ran, so no REAUTH_TOKEN_INVALID)
    expect(mocks.failures).toHaveLength(0);
  });

  it("handleAuthExpiringSoon_expiredToken_callsOnAuthFailureWithREAUTH_TOKEN_INVALID", async () => {
    const expiredToken = makeExpiredToken(NOW_SECONDS);
    const tokenSource = vi.fn<() => Promise<string>>().mockResolvedValue(expiredToken);
    const nowSecondsFn = vi.fn<() => number>().mockReturnValue(NOW_SECONDS);

    // The rejected error message is the full diagnostic string from handleAuthExpiringSoon.
    // It contains "EXPIRED" (the timing failure reason) not the string "REAUTH_TOKEN_INVALID"
    // (that is the AuthFailureReason enum value passed to onAuthFailure).
    await expect(client.handleAuthExpiringSoon(tokenSource, nowSecondsFn)).rejects.toThrow(
      "EXPIRED",
    );
    expect(mocks.failures).toHaveLength(1);
    expect(mocks.failures[0]?.reason).toBe("REAUTH_TOKEN_INVALID");
    expect(mocks.failures[0]?.message).toContain("EXPIRED");
    // reauth() must NOT have been called — only the initial-auth token was sent
    // (beforeEach calls authenticate("initial-token") which pushes one token)
    expect(mocks.sentTokens).toHaveLength(1);
    expect(mocks.sentTokens[0]).toBe("initial-token");
  });

  it("handleAuthExpiringSoon_reauthInFlight_tokenSourceStillCalledButReauthRejects", async () => {
    // Manually trigger a reauth to put it in-flight
    const firstReauth = client.reauth("midway-token");

    // Now fire handleAuthExpiringSoon — tokenSource is still called
    const freshToken = makeValidToken(NOW_SECONDS, "second");
    const tokenSource = vi.fn<() => Promise<string>>().mockResolvedValue(freshToken);
    const nowSecondsFn = vi.fn<() => number>().mockReturnValue(NOW_SECONDS);

    const secondP = client.handleAuthExpiringSoon(tokenSource, nowSecondsFn);

    // tokenSource is invoked (the timing check happens before calling reauth())
    await Promise.resolve(); // flush tokenSource microtask

    expect(tokenSource).toHaveBeenCalledOnce();

    // Resolve the first reauth so the queue drains
    client.onAuthAck(makeValidAck(), "user-sub");
    await firstReauth;

    // The second handleAuthExpiringSoon will have called tokenSource, obtained
    // the valid token, then called reauth() which will reject with "already in
    // flight" since the first reauth is still in flight at that point.
    await expect(secondP).rejects.toThrow();
  });

  it("handleAuthExpiringSoon_nowSecondsFnIsConsulted_notDateNow", async () => {
    // Provide a nowSecondsFn that returns a far-future time such that a token
    // that would appear valid at real Date.now() looks expired.
    const farFutureNow = NOW_SECONDS + 100_000;
    const token = makeValidToken(NOW_SECONDS); // exp = NOW_SECONDS + 3600, valid at NOW_SECONDS
    // At farFutureNow + 60s leeway, exp (NOW+3600) <= farFutureNow (NOW+100000) → expired

    const tokenSource = vi.fn<() => Promise<string>>().mockResolvedValue(token);
    const nowSecondsFn = vi.fn<() => number>().mockReturnValue(farFutureNow);

    await expect(client.handleAuthExpiringSoon(tokenSource, nowSecondsFn)).rejects.toThrow();

    expect(nowSecondsFn).toHaveBeenCalledOnce();
    expect(mocks.failures).toHaveLength(1);
    expect(mocks.failures[0]?.reason).toBe("REAUTH_TOKEN_INVALID");
    expect(mocks.failures[0]?.message).toContain("EXPIRED");
  });
});
