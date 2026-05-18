/**
 * e2eHooks — single home for every test-mode `window.*` global referenced by
 * the full-stack Playwright suite (`web-ui/e2e/full-stack/*.spec.ts`).
 *
 * <p><b>Production posture:</b> the module's effects are a NO-OP unless
 * {@code import.meta.env.VITE_E2E_REAL_BACKEND === "true"}. Vite inlines that
 * comparison at build time, esbuild dead-code-eliminates the body, and the
 * bundle-guard test (`web-ui/test/integration/build-bundle.test.ts`) greps
 * every emitted .js for the literal symbol names below to prove zero leakage:
 *
 * <ul>
 *   <li>{@code __forceWsClose}            (plan §14, test 7 reconnect)
 *   <li>{@code __cellFlashes}             (plan §8 test 5 flash recorder)
 *   <li>{@code __ordersGridApi}           (plan §8 test 5; OrderBlotter exposes its AG Grid api)
 *   <li>{@code __E2E_JWT_OVERRIDE__}      (plan §8 test 8 multi-issuer per-context token)
 *   <li>{@code __connStates}              (plan §8 test 7 connectionState$ recorder)
 *   <li>{@code __connStatesUnsub}         (plan §8 test 7 recorder teardown)
 *   <li>{@code __e2eHooks}                (this module's own ready-marker)
 * </ul>
 *
 * <p>{@code feedState$} (plan §Commit 9, spec 09 feed-stale) is exposed via
 * {@link E2EHooks#feedState$} — it is bundle-guarded alongside the hooks above.
 *
 * <p>Other UI components register into the hooks via {@link installEarlyHooks}
 * (called from `main.tsx`) and {@link registerOrdersGridApi}. Tests then
 * manipulate / read these fields through Playwright's {@code page.evaluate}.
 *
 * <p><b>Threading:</b> main thread.
 *
 * <p><b>Allocation:</b> A single module-load assignment of `window.__e2eHooks`
 * + array allocations on demand by the Playwright spec. Not on the hot path.
 */

import { connectionStream$ } from "@/streams/connection-stream";
import { feedState$ } from "@/streams/feed-state-stream";

/** Ambient type for the global hooks namespace (typed access from inside the project). */
declare global {
  var __e2eHooks: E2EHooks | undefined;

  var __forceWsClose: (() => void) | undefined;

  var __cellFlashes: Array<{ field: string; rowId: string }> | undefined;

  var __ordersGridApi: unknown;

  var __E2E_JWT_OVERRIDE__: string | undefined;

  var __connStates: Array<{ s: string; t: number }> | undefined;

  var __connStatesUnsub: (() => void) | undefined;
}

/** The hooks namespace exposed to Playwright. */
export interface E2EHooks {
  /** RxJS connection-state Observable, exposed for the test 7 recorder. */
  readonly connectionState$: typeof connectionStream$;
  /**
   * RxJS market-data feed-state Observable ({@code "LIVE" | "QUIET" | "STALE"}), exposed for
   * spec 09 (feed-stale). Separate from {@link connectionState$} — a STALE feed MUST NOT trip
   * the WS reconnect breaker; the transport remains healthy only the pricing feed is down.
   *
   * <p><b>Bundle-guard:</b> the literal {@code feedState$} is grepped by the bundle-guard test
   * to assert zero leakage into the production build.
   */
  readonly feedState$: typeof feedState$;
  /** Marker the spec waits on to confirm hooks are wired. */
  readonly ready: true;
}

/** True iff the test-mode escape hatch is enabled at build time. */
function e2eEnabled(): boolean {
  return import.meta.env.VITE_E2E_REAL_BACKEND === "true";
}

/**
 * Wire the always-on hooks at app boot. Call once from `main.tsx` BEFORE
 * `startMessageSource()` so the recorder hook is available when the worker
 * begins emitting connection-state events.
 *
 * Production no-op: the function returns early when
 * `VITE_E2E_REAL_BACKEND !== "true"`. The dead branch is DCE'd by esbuild.
 */
export function installEarlyHooks(): void {
  if (!e2eEnabled()) return;
  globalThis.__e2eHooks = { connectionState$: connectionStream$, feedState$, ready: true };
  // __forceWsClose is registered by the WorkerClient setup site (in messageSource.ts) once the
  // WorkerClient instance exists. Pre-register a stub so a stale page.evaluate during the
  // bootstrap window doesn't blow up — replaced by the real implementation immediately after.
  globalThis.__forceWsClose = () => {
    /* WorkerClient not yet initialised — best-effort no-op. */
  };
}

/**
 * Replace the {@code __forceWsClose} stub with a real implementation backed by
 * the WorkerClient under test. Called by {@code messageSource.ts} after the
 * WorkerClient is constructed in test-mode. No-op outside test mode.
 */
export function registerForceWsClose(impl: () => void): void {
  if (!e2eEnabled()) return;
  globalThis.__forceWsClose = impl;
}

/**
 * Expose the OrderBlotter's AG Grid api on the hooks namespace so plan §8
 * test 5 can attach the cellFlash recorder. No-op outside test mode.
 */
export function registerOrdersGridApi(api: unknown): void {
  if (!e2eEnabled()) return;
  globalThis.__ordersGridApi = api;
}
