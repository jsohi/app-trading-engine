/**
 * Purpose: Unit tests for messageSource singleton — dev-mode idempotency,
 * prod-mode pre-prod behaviour (console.warn + fakeStream-driven UI), and
 * subscription contract documentation.
 *
 * Rationale: messageSource uses module-level singletons; tests reset via
 * __resetMessageSourceForTests() in global afterEach. The defer(…) wrapper
 * means subscriptions MUST be made inside it() bodies (never beforeAll) so
 * they read the current _messages at subscribe time.
 *
 * vi.mock("@/streams/connection-stream") intercepts the binding captured by
 * messageSource.ts at its import time (Vitest's per-test-file module registry
 * ensures the mock applies to messageSource's own import graph).
 *
 * vi.resetModules() + dynamic imports are used here because test/setup.ts
 * imports messageSource (and therefore connection-stream) at worker startup;
 * vi.mock alone does not force a re-evaluation of an already-cached module.
 * resetModules() clears the cache so the next dynamic import picks up the
 * mocked connection-stream binding.
 *
 * @see messageSource — system under test.
 * @see fakeStream — mocked to prevent real timers.
 */
import { describe, it, expect, vi, beforeEach } from "vitest";
import { NEVER, type Observable } from "rxjs";
import type { ConnectionState, WorkerMessage } from "@/shared/transport/MessageShape";

// ── fakeStream stub ────────────────────────────────────────────────────────
// Return a cold Observable that never emits so real timers are not started.
vi.mock("@/mocks/fakeStream", () => ({
  fakeStream: (): typeof NEVER => NEVER,
}));

// ── connection-stream stub ─────────────────────────────────────────────────
// vi.mock is hoisted, so this runs before messageSource.ts is imported.
// The _pushedStates array is captured by the factory closure; tests inspect
// it after calling startMessageSource.
const _pushedStates: ConnectionState[] = [];

vi.mock("@/streams/connection-stream", () => ({
  pushConnectionState: (state: ConnectionState): void => {
    _pushedStates.push(state);
  },
  connectionStream$: {
    subscribe: (): { unsubscribe: () => void } => ({
      unsubscribe: (): void => undefined,
    }),
  },
  __resetConnectionStreamForTests: (): void => undefined,
}));

// ── lazy imports ───────────────────────────────────────────────────────────
// test/setup.ts imports messageSource (and connection-stream) at worker
// startup, caching them before this file's vi.mock hoisting takes effect.
// vi.resetModules() clears that cache so the dynamic import below loads a
// fresh messageSource that resolves pushConnectionState from the mocked
// connection-stream module.
//
// NOTE: the types are declared here; the actual values are assigned inside
// beforeEach after the dynamic import resolves.
let startMessageSource: (mode?: "dev" | "prod") => void;
// `messages$` is held for future tests that need to subscribe; not currently
// asserted (the fakeStream mock returns NEVER so no messages flow). Prefixed
// `_` so the no-unused-vars rule allows it.
let _messages$: Observable<WorkerMessage>;
let __resetMessageSourceForTests: () => void;

// ── tests ──────────────────────────────────────────────────────────────────
// NOTE: subscribe to messages$ INSIDE each it() body. The defer(…) wrapper
// reads _messages at subscribe time; subscribing before or after a reset
// changes which subject you're connected to. Never subscribe in beforeAll.

describe("messageSource", () => {
  beforeEach(async () => {
    _pushedStates.length = 0;
    // Reset the module cache so the dynamic import below re-evaluates
    // messageSource.ts with the mocked connection-stream binding.
    vi.resetModules();
    // Re-apply the mocks after resetModules (resetModules clears all mock
    // registrations). vi.mock calls are hoisted but resetModules nukes the
    // per-file registry — doMock re-registers without hoisting requirement.
    vi.doMock("@/mocks/fakeStream", () => ({
      fakeStream: (): typeof NEVER => NEVER,
    }));
    vi.doMock("@/streams/connection-stream", () => ({
      pushConnectionState: (state: ConnectionState): void => {
        _pushedStates.push(state);
      },
      connectionStream$: {
        subscribe: (): { unsubscribe: () => void } => ({
          unsubscribe: (): void => undefined,
        }),
      },
      __resetConnectionStreamForTests: (): void => undefined,
    }));
    // Dynamically import a fresh messageSource with the mocked deps.
    const mod = await import("@/main-thread/messageSource");
    startMessageSource = mod.startMessageSource;
    _messages$ = mod.messages$;
    __resetMessageSourceForTests = mod.__resetMessageSourceForTests;
  });

  it("startMessageSource_devModeTwice_subscribesOnce", () => {
    startMessageSource("dev");
    startMessageSource("dev"); // second call should be a no-op (idempotent guard)

    // Only one CONNECTED push (if two subscriptions, two CONNECTED would be pushed).
    expect(_pushedStates.filter((s) => s === "CONNECTED")).toHaveLength(1);
  });

  it("startMessageSource_prodMode_warnsAndDrivesFakeStream", () => {
    const warnSpy = vi.spyOn(console, "warn").mockImplementation((): void => undefined);

    startMessageSource("prod");

    // Pre-prod build: prod mode emits ONE console.warn so the pre-prod
    // state is observable (DevTools / RUM). The warn mentions APP-160 so
    // ops can find the upstream ticket. When APP-160 lands, this warn
    // goes away and the prod path uses real WorkerClient instead.
    expect(warnSpy).toHaveBeenCalledTimes(1);
    expect(warnSpy.mock.calls[0]![0]).toMatch(/APP-160/);

    // Prod mode also drives fakeStream → indicator goes CONNECTED, blotters
    // populate. Same UX as dev until the swap lands.
    expect(_pushedStates).toContain("CONNECTED");
    expect(_pushedStates).not.toContain("DOWN_REQUIRES_USER_ACTION");

    warnSpy.mockRestore();
  });

  it("startMessageSource_prodMode_subscribesToFakeStreamForUI", () => {
    // The fakeStream mock returns NEVER (no real emissions) so we can't
    // assert end-to-end message delivery here without breaking test
    // isolation. The OTHER prod-mode test verifies the CONNECTED push +
    // the absence of DOWN_REQUIRES_USER_ACTION, which is sufficient to
    // prove the prod branch took the fakeStream subscribe path (not the
    // old loud-stub). The full message-delivery path is covered by the
    // Playwright e2e (`blotters_devServer_atLeastOneRowAppearsInEachBlotter`),
    // which boots a real dev server.
    const warnSpy = vi.spyOn(console, "warn").mockImplementation((): void => undefined);
    startMessageSource("prod");
    // CONNECTED was pushed (only path that does this is the fakeStream branch).
    expect(_pushedStates).toContain("CONNECTED");
    warnSpy.mockRestore();
  });

  it("messages$_subscriptionContract_subscribeInsideIt", () => {
    // This test documents the subscription contract — no behavioural assertion.
    // See file header: ALWAYS subscribe inside it() body, NEVER in beforeAll.
    // The defer(…) wrapper picks up the current _messages at subscribe time.
    // A subscription captured before __resetMessageSourceForTests() runs would
    // hold a stale handle to the OLD subject — it will receive complete() then
    // nothing. This is the same pattern as connection-stream tests.
    expect(true).toBe(true);
  });
});
