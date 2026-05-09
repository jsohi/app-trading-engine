/**
 * CspHeaders.browser.test.ts — assert Content-Security-Policy headers in
 * the build artifact (`_headers` file or `index.html` meta tag) match the
 * APP-36 §7.1 baseline:
 *
 *   default-src 'self'; script-src 'self'; connect-src 'self' wss://<host>;
 *   worker-src 'self'; frame-ancestors 'none'; COOP same-origin; COEP require-corp.
 *
 * Tier: @vitest/browser (Chromium). Skipped until the Chromium runner is wired
 * in CI per the C9 follow-up.
 *
 * C9 follow-up: needs @vitest/browser harness — Chromium runner not yet wired.
 *
 * Threading: browser main thread.
 * Allocation: test-only.
 *
 * Plan reference: APP-36 §7.1 / §6 row 1.
 */

import { describe, it } from "vitest";

describe("CspHeaders (browser)", () => {
  it.skip("cspHeaders_defaultSrc_self_matchesBaseline", () => {
    // C9 follow-up: needs @vitest/browser harness — Chromium runner not yet wired.
    // Steps when enabled:
    //   1. Perform a fetch() to the dev server root and inspect headers.
    //   2. Assert `Content-Security-Policy` response header includes:
    //      "default-src 'self'"
    //      "script-src 'self'"
    //      "connect-src 'self' wss://"
    //      "worker-src 'self'"
    //      "frame-ancestors 'none'"
    //   3. Assert `Cross-Origin-Opener-Policy: same-origin`.
    //   4. Assert `Cross-Origin-Embedder-Policy: require-corp`.
    //   5. Assert `Permissions-Policy` denies camera/microphone/geolocation.
  });

  it.skip("cspHeaders_connectSrc_includesWssHost", () => {
    // C9 follow-up: needs @vitest/browser harness — Chromium runner not yet wired.
  });

  it.skip("cspHeaders_frameAncestors_none_preventClickjacking", () => {
    // C9 follow-up: needs @vitest/browser harness — Chromium runner not yet wired.
  });
});
