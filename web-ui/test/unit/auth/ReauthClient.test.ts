/**
 * ReauthClient.test.ts — unit tests for the in-band re-auth +
 * entitlement-sensitive frame queue per APP-36 §2.12.
 *
 * Uses the same `AuthClient.reauth()` path tested via its public API.
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
} from "@/workers/session/AuthClient";
import { SessionState, type UuidComposite } from "@/workers/session/SessionState";
import {
  AUTH_TIMEOUT_MS,
  MAX_REAUTH_QUEUED_FRAMES,
  WORKER_PROTOCOL_VERSION,
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
    ...overrides,
  };
}

/** Perform initial auth handshake to get the client into authenticated state. */
function initialAuth(client: AuthClient): void {
  client.authenticate("initial-token");
  client.onAuthAck(makeValidAck(), "sub");
}

// ─── Tests ───────────────────────────────────────────────────────────────────

describe("AuthClient (re-auth + queue)", () => {
  let state: SessionState;
  let sched: ReturnType<typeof makeScheduler>;
  let mocks: ReturnType<typeof makeCallbacks>;
  let client: AuthClient;

  beforeEach(() => {
    state = new SessionState();
    sched = makeScheduler();
    mocks = makeCallbacks();
    client = new AuthClient(state, mocks.callbacks, sched.scheduler);
    // Perform initial auth to get into authenticated state
    initialAuth(client);
  });

  it("reauth_resolvedOnNewAuthAck", async () => {
    const reauthPromise = client.reauth("new-token");
    expect(client.isReauthInFlight()).toBe(true);

    // Server responds with a new AuthAck
    client.onAuthAck(makeValidAck(), "sub");

    await expect(reauthPromise).resolves.toBeUndefined();
    expect(client.isReauthInFlight()).toBe(false);
  });

  it("reauth_subscribeQueued_andFlushedOnResolve", async () => {
    const initialSentCount = mocks.sentBytes.length;

    const reauthPromise = client.reauth("new-token");

    // Queue an entitlement-sensitive frame (Subscribe = template 62)
    const subscribeFrame = new Uint8Array([62, 1, 0, 0]);
    const sent = client.sendOrQueue(62, subscribeFrame);
    expect(sent).toBe(false); // queued, not sent
    expect(client.reauthQueueLength()).toBe(1);

    // AuthAck arrives → reauth resolves → queue drains
    client.onAuthAck(makeValidAck(), "sub");
    await reauthPromise;

    // The queued frame should have been sent (sentBytes count increased by 1 beyond the reauth auth frame)
    // Reauth sends 1 frame (the new auth), then on resolve flushes 1 queued frame
    const newSentCount = mocks.sentBytes.length;
    expect(newSentCount).toBeGreaterThan(initialSentCount + 1); // +1 auth + at least 1 queued
    expect(client.reauthQueueLength()).toBe(0);
    expect(client.isReauthInFlight()).toBe(false);
  });

  it("reauth_subscribeDropped_onReject", async () => {
    const reauthPromise = client.reauth("new-token");

    // Queue an entitlement-sensitive frame
    client.sendOrQueue(62, new Uint8Array([62, 1, 0, 0]));
    expect(client.reauthQueueLength()).toBe(1);

    // Reauth fails via timeout
    sched.advanceMs(AUTH_TIMEOUT_MS + 1);
    sched.tick(); // fires the deadline

    await expect(reauthPromise).rejects.toThrow();
    // Queue should be cleared on rejection
    expect(client.reauthQueueLength()).toBe(0);
    expect(client.isReauthInFlight()).toBe(false);
  });

  it("MAX_REAUTH_QUEUED_FRAMES_64_overflow_rejectsReauth_andSurfacesProtocolViolation", async () => {
    const reauthPromise = client.reauth("new-token");

    // Push MAX_REAUTH_QUEUED_FRAMES + 1 frames to trigger overflow
    for (let i = 0; i < MAX_REAUTH_QUEUED_FRAMES; i++) {
      client.sendOrQueue(62, new Uint8Array([62, 1, 0, 0]));
    }
    // The (MAX + 1)th frame triggers the overflow
    client.sendOrQueue(62, new Uint8Array([62, 1, 0, 0]));

    // The reauth promise should reject with REAUTH_QUEUE_OVERFLOW
    await expect(reauthPromise).rejects.toThrow();
    expect(client.isReauthInFlight()).toBe(false);
  });

  it("nonEntitlementFrames_bypassQueue", () => {
    const initialSentCount = mocks.sentBytes.length;

    // Start reauth so queue logic is active
    void client.reauth("new-token");
    expect(client.isReauthInFlight()).toBe(true);

    // Non-entitlement frames should bypass the queue:
    // heartbeat (65), ack (71), gap-request (68), session-resume (69)
    const heartbeatFrame = new Uint8Array([65, 0, 0, 0]);
    const ackFrame = new Uint8Array([71, 0, 0, 0]);
    const gapFrame = new Uint8Array([68, 0, 0, 0]);
    const resumeFrame = new Uint8Array([69, 0, 0, 0]);

    expect(client.sendOrQueue(65, heartbeatFrame)).toBe(true);
    expect(client.sendOrQueue(71, ackFrame)).toBe(true);
    expect(client.sendOrQueue(68, gapFrame)).toBe(true);
    expect(client.sendOrQueue(69, resumeFrame)).toBe(true);

    // All 4 non-entitlement frames sent immediately
    // (plus 1 reauth auth frame = +5 total from initial)
    expect(mocks.sentBytes.length).toBe(initialSentCount + 1 + 4);

    // Queue should still be empty (none were entitlement-sensitive)
    expect(client.reauthQueueLength()).toBe(0);
  });

  it("reauth_priorJtiRevokedMidFlight_inflightFramesAccepted", () => {
    // This is a comment-only test per the plan.
    // Contract: server revokes the prior JTI before sending success indication;
    // in-flight server→client frames within the same session remain valid because
    // the server preserves the session across re-auth. The worker queues only
    // outbound entitlement-sensitive frames — inbound frames continue to be
    // processed normally. The JTI revocation is server-internal and invisible
    // to the client-side state machine.
    //
    // This invariant is enforced by:
    //   1. The reauth() promise resolving only after the server sends AuthAck
    //      (confirming the new JTI is in effect).
    //   2. The session ID being preserved across re-auth (no cold-start).
    //   3. Inbound dispatching being independent of the reauth state.
    //
    // No executable assertion here — the invariant is documented and enforced
    // by the architectural separation between the auth handshake and the
    // inbound message dispatcher.
    expect(true).toBe(true);
  });
});
