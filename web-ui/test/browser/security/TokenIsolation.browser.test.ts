/**
 * TokenIsolation.browser.test.ts — verify that after the INIT message is
 * delivered the JWT is not reachable on the main-thread heap, and that the
 * production worker bundle does not contain the `devTokenProvider` symbol
 * or the literal string `VITE_DEV_JWT`.
 *
 * Tier: @vitest/browser (Chromium). Skipped until the Chromium runner is wired
 * in CI per the C9 follow-up.
 *
 * C9 follow-up: needs @vitest/browser harness — Chromium runner not yet wired.
 *   Uses `measureUserAgentSpecificMemory()` API (Chromium ≥ 89) and bundle
 *   artifact string scan.
 *
 * Threading: browser main thread.
 * Allocation: test-only.
 *
 * Plan reference: APP-36 §4.2 / §6 rows 2, 3.
 */

import { describe, it } from "vitest";

describe("TokenIsolation (browser)", () => {
  it.skip("tokenIsolation_heapSnapshot_noTokenReachableOnMainThread", () => {
    // C9 follow-up: needs @vitest/browser harness — Chromium runner not yet wired.
    // Steps when enabled:
    //   1. Spawn a WorkerClient with a devTokenProvider that uses a known sentinel token.
    //   2. Await WorkerClient.start().
    //   3. GC + await performance.measureUserAgentSpecificMemory().
    //   4. Iterate the breakdown — confirm no object in the main-thread breakdown
    //      contains the sentinel token string.
  });

  it.skip("tokenIsolation_prodBundleDoesNotContainDevProviderSymbol", () => {
    // C9 follow-up: needs @vitest/browser harness — Chromium runner not yet wired.
    // Steps when enabled:
    //   1. Read the built worker bundle artifact (via fetch from the test server).
    //   2. Assert the string "devTokenProvider" does not appear.
    //   3. Assert the string "VITE_DEV_JWT" does not appear.
    //   This corresponds to the CI size-limit regex check in APP-36 §6 row 3.
  });
});
