/**
 * WsUrlValidator.test.ts — unit tests for the WebSocket URL safety validator.
 *
 * Covers:
 *  - Production mode: rejects cleartext ws://, loopback hosts, *.local, userinfo,
 *    query strings, and fragments.
 *  - Production mode: accepts wss:// with a public hostname.
 *  - Dev mode: accepts wss://localhost and ws://localhost.
 *  - Default port resolution: wss → 443, ws → 80 when no port is explicit.
 *
 * Threading: single-threaded (Vitest jsdom worker).
 * Allocation: cold-path — URL parse per call; test-only.
 */

import { describe, expect, it } from "vitest";

import { validateWsUrl } from "@/workers/frame/WsUrlValidator";

// ─── Production mode — rejections ───────────────────────────────────────────

describe("validateWsUrl — prod mode rejects unsafe URLs", () => {
  it("validateWsUrl_ws_prod_rejectsCleartext", () => {
    expect(() => validateWsUrl("ws://prod.example.com:8443", "prod")).toThrow(
      /production scheme must be wss:/,
    );
  });

  it("validateWsUrl_wssLocalhost_prod_rejectsLoopback", () => {
    expect(() => validateWsUrl("wss://localhost:8443", "prod")).toThrow(/loopback/);
  });

  it("validateWsUrl_wss127001_prod_rejectsLoopback", () => {
    expect(() => validateWsUrl("wss://127.0.0.1:8443", "prod")).toThrow(/loopback/);
  });

  it("validateWsUrl_wssDotLocal_prod_rejectsLinkLocal", () => {
    expect(() => validateWsUrl("wss://trading.local:8443", "prod")).toThrow(/loopback/);
  });

  it("validateWsUrl_withUserinfo_prod_rejectsTokenLeakage", () => {
    expect(() => validateWsUrl("wss://user:pass@prod.example.com:8443", "prod")).toThrow(
      /userinfo/,
    );
  });

  it("validateWsUrl_withQueryString_prod_rejectsTokenLeakage", () => {
    expect(() => validateWsUrl("wss://prod.example.com:8443?token=abc", "prod")).toThrow(
      /query string/,
    );
  });

  it("validateWsUrl_withFragment_prod_rejectsTokenLeakage", () => {
    expect(() => validateWsUrl("wss://prod.example.com:8443#section", "prod")).toThrow(
      /query string or fragment/,
    );
  });

  it("validateWsUrl_malformed_prod_throwsMalformed", () => {
    expect(() => validateWsUrl("not-a-url", "prod")).toThrow(/malformed URL/);
  });
});

// ─── Production mode — acceptance ───────────────────────────────────────────

describe("validateWsUrl — prod mode accepts valid URLs", () => {
  it("validateWsUrl_wssPublicHost_prod_accepted", () => {
    const result = validateWsUrl("wss://prod.example.com:8443", "prod");
    expect(result.url).toBe("wss://prod.example.com:8443");
    // URL.host includes the port when non-default, matching the source's `parsed.host`.
    expect(result.host).toBe("prod.example.com:8443");
    expect(result.port).toBe(8443);
  });
});

// ─── Dev mode — acceptance of loopback ──────────────────────────────────────

describe("validateWsUrl — dev mode accepts loopback", () => {
  it("validateWsUrl_wssLocalhost_dev_accepted", () => {
    const result = validateWsUrl("wss://localhost:8443", "dev");
    // URL.host includes the port when non-default.
    expect(result.host).toBe("localhost:8443");
    expect(result.port).toBe(8443);
  });

  it("validateWsUrl_wsLocalhost_dev_accepted", () => {
    const result = validateWsUrl("ws://localhost:8444", "dev");
    // URL.host includes the port when non-default.
    expect(result.host).toBe("localhost:8444");
    expect(result.port).toBe(8444);
  });
});

// ─── Default port resolution ─────────────────────────────────────────────────

describe("validateWsUrl — default port resolution", () => {
  it("validateWsUrl_wssNoPort_prod_returnsPort443", () => {
    const result = validateWsUrl("wss://prod.example.com", "prod");
    expect(result.port).toBe(443);
  });

  it("validateWsUrl_wsNoPort_dev_returnsPort80", () => {
    const result = validateWsUrl("ws://localhost", "dev");
    expect(result.port).toBe(80);
  });
});
