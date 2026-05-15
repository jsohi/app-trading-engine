/**
 * Full-stack spec 1: ConnectionIndicator turns green on real WebSocket connect.
 *
 * Plan §8 test 1. Validates real wss upgrade + RS256 JWT acceptance + the
 * project's connection-state plumbing end-to-end. The Chromium SPKI pin
 * (configured in playwright.full-stack.config.ts) means the TLS chain is
 * actually validated against the mkcert root CA — a bad cert would fail the
 * connect rather than be silently ignored.
 */
import { expect, test } from "@playwright/test";
import { drainQuiescenceAndBaseline, readinessGate } from "./helpers";

test("ConnectionIndicator transitions through CONNECTING/RECONNECTING to CONNECTED", async ({
  page,
}) => {
  await page.goto("/");
  await readinessGate(page);
  // Also assert the dot reaches the green colour-group class.
  await expect(page.locator(".conn-dot.conn-green").first()).toBeVisible({ timeout: 10_000 });
  // Drain quiescence + baseline establishes the per-spec discipline used by
  // the rest of the suite even though spec 1 makes no row-count assertions.
  await drainQuiescenceAndBaseline(page);
});
