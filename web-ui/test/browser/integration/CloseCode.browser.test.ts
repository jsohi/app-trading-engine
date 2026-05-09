/**
 * CloseCode.browser.test.ts — full close-code matrix per APP-36 §2.13 second
 * table. Covers all RFC 6455 standard codes plus the 4xxx custom range.
 *
 * Tier: @vitest/browser (Chromium). Skipped until the Chromium runner is wired
 * in CI per the C9 follow-up.
 *
 * C9 follow-up: needs @vitest/browser harness — Chromium runner not yet wired.
 *   Requires a mock-socket server that can be instructed to close with a specific
 *   code and then observe the WorkerClient's connectionStream$ state transitions.
 *
 * Threading: browser main thread + worker thread.
 * Allocation: test-only.
 *
 * Plan reference: APP-36 §2.13 / §6 row 16.
 */

import { describe, it } from "vitest";

describe("CloseCode (browser)", () => {
  it.skip("closeCode_1000_normalClosure_reconnectsWithBackoff", () => {
    // C9 follow-up: needs @vitest/browser harness — Chromium runner not yet wired.
  });

  it.skip("closeCode_1001_goingAway_reconnectsWithBackoff", () => {
    // C9 follow-up: needs @vitest/browser harness — Chromium runner not yet wired.
  });

  it.skip("closeCode_1002_protocolError_protocolViolationNoReconnect", () => {
    // C9 follow-up: needs @vitest/browser harness — Chromium runner not yet wired.
  });

  it.skip("closeCode_1003_unsupportedData_protocolViolation", () => {
    // C9 follow-up: needs @vitest/browser harness — Chromium runner not yet wired.
  });

  it.skip("closeCode_1006_abnormal_reconnectsWithBackoff", () => {
    // C9 follow-up: needs @vitest/browser harness — Chromium runner not yet wired.
  });

  it.skip("closeCode_1007_invalidPayload_protocolViolation", () => {
    // C9 follow-up: needs @vitest/browser harness — Chromium runner not yet wired.
  });

  it.skip("closeCode_1008_policyViolation_protocolViolation", () => {
    // C9 follow-up: needs @vitest/browser harness — Chromium runner not yet wired.
  });

  it.skip("closeCode_1009_messageTooLarge_protocolViolation", () => {
    // C9 follow-up: needs @vitest/browser harness — Chromium runner not yet wired.
  });

  it.skip("closeCode_1010_mandatoryExtension_protocolViolation", () => {
    // C9 follow-up: needs @vitest/browser harness — Chromium runner not yet wired.
  });

  it.skip("closeCode_1011_serverInternalError_reconnectsCapx4", () => {
    // C9 follow-up: needs @vitest/browser harness — Chromium runner not yet wired.
  });

  it.skip("closeCode_1012_serviceRestart_reconnectsCapx8", () => {
    // C9 follow-up: needs @vitest/browser harness — Chromium runner not yet wired.
  });

  it.skip("closeCode_1013_tryAgainLater_reconnectsCapx8", () => {
    // C9 follow-up: needs @vitest/browser harness — Chromium runner not yet wired.
  });

  it.skip("closeCode_1014_badGateway_reconnectsCapx4", () => {
    // C9 follow-up: needs @vitest/browser harness — Chromium runner not yet wired.
  });

  it.skip("closeCode_1015_tlsHandshakeFailure_schemaMismatchNoReconnect", () => {
    // C9 follow-up: needs @vitest/browser harness — Chromium runner not yet wired.
  });

  it.skip("closeCode_4xxx_withPrecedingWebSocketError_followsErrorMatrix", () => {
    // C9 follow-up: needs @vitest/browser harness — Chromium runner not yet wired.
    // Scenario: worker receives WebSocketError template (code X), then
    // WebSocket closes with 4xxx — worker follows §2.13 matrix for code X.
  });

  it.skip("closeCode_4xxx_bare_protocolViolationNoReconnect", () => {
    // C9 follow-up: needs @vitest/browser harness — Chromium runner not yet wired.
    // Scenario: bare 4xxx close without preceding WebSocketError template
    // → PROTOCOL_VIOLATION, no auto-reconnect.
  });
});
