/**
 * Auth.test.ts — unit tests for the initial WebSocket auth handshake
 * via AuthClient per APP-36 §2.5 / §A1.
 *
 * Mocks `AuthClientCallbacks` (sendBytes captures bytes, onAuthSuccess /
 * onAuthFailure spies) and `AuthScheduler` with controllable timers.
 *
 * Test naming follows `<unit>_<scenario>_<expected>` per plan §5.8.
 *
 * Threading: single-threaded (Vitest jsdom worker).
 */

import { describe, expect, it, beforeEach } from "vitest";
import {
  AuthClient,
  type AuthAck,
  type AuthClientCallbacks,
  type AuthScheduler,
  type AuthFailureReason,
  SENT_PROTOCOL_VERSION,
} from "@/workers/session/AuthClient";
import { SessionState, type UuidComposite } from "@/workers/session/SessionState";
import {
  AUTH_TIMEOUT_MS,
  WORKER_PROTOCOL_VERSION,
  SERVER_HEARTBEAT_INTERVAL_DEFAULT_MS,
  CLIENT_HEARTBEAT_INTERVAL_DEFAULT_MS,
} from "@/workers/WorkerTuning";

// ─── Controllable scheduler ──────────────────────────────────────────────────

function makeScheduler(): {
  scheduler: AuthScheduler;
  tick: () => boolean;
  pendingCount: () => number;
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
    pendingCount: (): number => timers.length,
    advanceMs: (ms: number): void => {
      currentMs += ms;
    },
  };
}

// ─── Mock callbacks ──────────────────────────────────────────────────────────

function makeCallbacks(): {
  callbacks: AuthClientCallbacks;
  sentBytes: Uint8Array[];
  successes: number[];
  failures: Array<{ reason: AuthFailureReason; message: string }>;
} {
  const sentBytes: Uint8Array[] = [];
  const successes: number[] = [];
  const failures: Array<{ reason: AuthFailureReason; message: string }> = [];

  const callbacks: AuthClientCallbacks = {
    sendBytes: (bytes: Uint8Array): void => {
      sentBytes.push(bytes.slice());
    },
    encodeAuth: (_token: string, protocolVersion: number): Uint8Array => {
      // Minimal mock encoder — just return a 1-byte array with the version
      return new Uint8Array([protocolVersion]);
    },
    onAuthSuccess: (): void => {
      successes.push(1);
    },
    onAuthFailure: (reason: AuthFailureReason, message: string): void => {
      failures.push({ reason, message });
    },
  };

  return { callbacks, sentBytes, successes, failures };
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

// ─── Tests ───────────────────────────────────────────────────────────────────

describe("AuthClient (initial handshake)", () => {
  let state: SessionState;
  let sched: ReturnType<typeof makeScheduler>;
  let mocks: ReturnType<typeof makeCallbacks>;
  let client: AuthClient;

  beforeEach(() => {
    state = new SessionState();
    sched = makeScheduler();
    mocks = makeCallbacks();
    client = new AuthClient(state, mocks.callbacks, sched.scheduler);
  });

  it("authenticate_validAck_callsOnAuthSuccess_andAppliesAuthAck", () => {
    client.authenticate("test-jwt");

    // Should send encoded bytes
    expect(mocks.sentBytes.length).toBe(1);

    // AuthAck arrives with valid protocolVersion
    client.onAuthAck(makeValidAck(), "user-sub");

    expect(mocks.successes.length).toBe(1);
    expect(mocks.failures.length).toBe(0);
    // State should be populated
    expect(state.currentSessionId).not.toBeNull();
    expect(state.protocolVersion).toBe(WORKER_PROTOCOL_VERSION);
    expect(state.maxSubscriptions).toBe(50);
  });

  it("authenticate_protocolVersionMismatch_callsOnAuthFailure_PROTOCOL_VERSION_MISMATCH", () => {
    client.authenticate("test-jwt");

    // AuthAck with wrong protocol version
    client.onAuthAck(makeValidAck({ protocolVersion: WORKER_PROTOCOL_VERSION + 1 }), "user-sub");

    expect(mocks.failures.length).toBe(1);
    expect(mocks.failures[0]?.reason).toBe("PROTOCOL_VERSION_MISMATCH");
    expect(mocks.successes.length).toBe(0);
  });

  it("authenticate_5sTimeout_visibility_hiddenSafe_firesTimeout", () => {
    client.authenticate("test-jwt");

    // Advance past AUTH_TIMEOUT_MS (5000 ms)
    sched.advanceMs(AUTH_TIMEOUT_MS + 1);
    sched.tick();

    expect(mocks.failures.length).toBe(1);
    expect(mocks.failures[0]?.reason).toBe("TIMEOUT");
  });

  it("authenticate_serverErrorBeforeAck_callsOnAuthFailure_SERVER_ERROR", () => {
    client.authenticate("test-jwt");

    // Server sends an error before AuthAck arrives
    client.onAuthError("session rejected by server");

    expect(mocks.failures.length).toBe(1);
    expect(mocks.failures[0]?.reason).toBe("SERVER_ERROR");
    expect(mocks.successes.length).toBe(0);
  });

  it("authAck_zeroIntervalsFallback_appliesDefaults5000_10000_inSessionState", () => {
    client.authenticate("test-jwt");

    // AuthAck with zero intervals → should apply defaults per §A1
    client.onAuthAck(
      makeValidAck({ serverHeartbeatIntervalMs: 0, clientHeartbeatIntervalMs: 0 }),
      "user-sub",
    );

    expect(mocks.successes.length).toBe(1);
    expect(state.serverHeartbeatIntervalMs).toBe(SERVER_HEARTBEAT_INTERVAL_DEFAULT_MS);
    expect(state.clientHeartbeatIntervalMs).toBe(CLIENT_HEARTBEAT_INTERVAL_DEFAULT_MS);
  });

  it("authAck_storesSessionIdMaxSubsProtocolVersionAndIntervals_inSessionState", () => {
    const sessionId: UuidComposite = {
      mostSignificantBits: 0xdeadbeefn,
      leastSignificantBits: 0xcafebaben,
    };
    client.authenticate("test-jwt");
    client.onAuthAck(
      {
        sessionId,
        protocolVersion: WORKER_PROTOCOL_VERSION,
        maxSubscriptions: 100,
        serverHeartbeatIntervalMs: 3_000,
        clientHeartbeatIntervalMs: 7_500,
        symbolPreferences: [],
        panelLayout: [],
      },
      "my-sub",
    );

    expect(state.currentSessionId).toEqual(sessionId);
    expect(state.maxSubscriptions).toBe(100);
    expect(state.protocolVersion).toBe(WORKER_PROTOCOL_VERSION);
    expect(state.serverHeartbeatIntervalMs).toBe(3_000);
    expect(state.clientHeartbeatIntervalMs).toBe(7_500);
    expect(state.subClaim).toBe("my-sub");
  });

  it("authenticate_protocolVersion_serverAsserted_clientValidatesAgainstConstant", () => {
    // Verify that SENT_PROTOCOL_VERSION === WORKER_PROTOCOL_VERSION (the constant
    // the client validates against). This is the anti-echo defense from §2.5.
    expect(SENT_PROTOCOL_VERSION).toBe(WORKER_PROTOCOL_VERSION);

    client.authenticate("test-jwt");

    // Only matching version passes
    client.onAuthAck(makeValidAck({ protocolVersion: WORKER_PROTOCOL_VERSION }), "sub");
    expect(mocks.successes.length).toBe(1);
    expect(mocks.failures.length).toBe(0);
  });
});
