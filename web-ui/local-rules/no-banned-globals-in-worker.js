/**
 * ESLint rule: local/no-banned-globals-in-worker
 *
 * Bans access to a curated set of browser globals inside
 * `src/workers/**` and `src/main-thread/**`. The list covers storage
 * (localStorage/sessionStorage/IndexedDB/Cache), exfil channels
 * (sendBeacon/Reporting/Notification/clipboard/WebTransport/RTC),
 * device APIs (Bluetooth/USB/Serial/HID), and shared-state primitives
 * (SharedArrayBuffer/BroadcastChannel/navigator.locks).
 *
 * Plan reference: §4.3 / §6 row 1.
 *
 * Implementation note: this is a **stub** in C1 (per §B.6) — the
 * lexical ban is enforced by the `no-restricted-globals` /
 * `no-restricted-properties` rules in `eslint.config.js` overrides.
 * The custom rule slot is reserved here so the rule registration
 * is stable and later commits can add taint-style checks (e.g.
 * forbidding aliasing the banned global through `globalThis['x']`).
 */

/** @type {import('eslint').Rule.RuleModule} */
export default {
  meta: {
    type: "problem",
    docs: {
      description:
        "Bans browser storage / exfil / device-API globals inside workers + main-thread modules.",
    },
    schema: [],
    messages: {
      banned: "Banned global '{{name}}' inside {{scope}} per APP-36 §4.3.",
    },
  },
  create() {
    // Stub — no-restricted-globals override in eslint.config.js handles
    // the lexical surface in C1. Promoted in C7 when main-thread code lands.
    return {};
  },
};
