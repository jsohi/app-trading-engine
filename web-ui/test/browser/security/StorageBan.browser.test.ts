/**
 * StorageBan.browser.test.ts — verify that every banned storage API throws
 * when invoked in the browser context per APP-36 §4.3.
 *
 * Banned globals: localStorage, sessionStorage, indexedDB, cookies,
 * caches (CacheStorage), WebRTC, Bluetooth, USB, Serial, HID.
 *
 * Tier: @vitest/browser (Chromium). Skipped until the Chromium runner is wired
 * in CI per the C9 follow-up.
 *
 * C9 follow-up: needs @vitest/browser harness — Chromium runner not yet wired.
 *
 * Threading: browser main thread.
 * Allocation: test-only.
 *
 * Plan reference: APP-36 §4.3 / §6 row 1.
 */

import { describe, it } from "vitest";

describe("StorageBan (browser)", () => {
  it.skip("storageBan_localStorage_throwsOnSet", () => {
    // C9 follow-up: needs @vitest/browser harness — Chromium runner not yet wired.
    // Verify: window.localStorage.setItem('x','y') throws SecurityError or is
    // overridden to throw in the production module initializer.
  });

  it.skip("storageBan_sessionStorage_throwsOnSet", () => {
    // C9 follow-up: needs @vitest/browser harness — Chromium runner not yet wired.
  });

  it.skip("storageBan_indexedDB_throwsOnOpen", () => {
    // C9 follow-up: needs @vitest/browser harness — Chromium runner not yet wired.
  });

  it.skip("storageBan_documentCookie_throwsOnWrite", () => {
    // C9 follow-up: needs @vitest/browser harness — Chromium runner not yet wired.
  });

  it.skip("storageBan_caches_throwsOnOpen", () => {
    // C9 follow-up: needs @vitest/browser harness — Chromium runner not yet wired.
  });

  it.skip("storageBan_webrtc_throwsOnCreate", () => {
    // C9 follow-up: needs @vitest/browser harness — Chromium runner not yet wired.
  });
});
