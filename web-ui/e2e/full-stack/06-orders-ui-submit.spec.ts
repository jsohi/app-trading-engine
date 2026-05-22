/**
 * Full-stack spec 6: OrderBlotter receives a UI-submitted order via the real
 * APP-160 path: OrderEntryForm → useOrderSubmission → CommandClient →
 * NewOrderSingleEncoder → WorkerClient.submitCommand → worker commandPort →
 * wss → cluster → CommandAck (templateId=70) → worker → commandClient →
 * useOrderSubmission resolve → form clears → row appears in OrderBlotter.
 *
 * Plan §8 test 6 — five subtests pinned to one expected outcome each:
 * happy-path / Throttle / Backpressure / Duplicate / Validation.
 *
 * Note on Entitlement: the JWT's `accounts` claim shape is owned by the
 * cluster's CommandDispatcher entitlement check; until a dedicated test
 * fixture mints a JWT scoped to a specific symbol set, the negative subtest
 * is asserted via the symbol-validation client-side guard (a non-allowed
 * symbol cannot be submitted at all).
 */
import { expect, test } from "@playwright/test";
import { clOrdId, drainQuiescenceAndBaseline, readinessGate } from "./helpers";

test("UI submit — OrderEntryForm round-trips through cluster, row appears with status", async ({
  page,
}) => {
  await page.goto("/");
  await readinessGate(page);
  await drainQuiescenceAndBaseline(page);

  const id = clOrdId("06");
  await page.locator('[data-testid="order-entry-clord-id"]').fill(id);
  await page.locator('[data-testid="order-entry-symbol"]').fill("EUR/USD");
  await page.locator('[data-testid="order-entry-qty"]').fill("1.0");
  await page.locator('[data-testid="order-entry-price"]').fill("1.05");
  await page.locator('[data-testid="order-entry-submit"]').click();

  await expect(
    page.locator('[data-testid="order-entry-submit"][data-state="loading"]'),
  ).toBeVisible({ timeout: 5_000 });

  await expect
    .poll(
      async () => {
        const cells = await page.locator(".ag-cell").allTextContents();
        return cells.some((c) => c.includes(id));
      },
      { timeout: 15_000 },
    )
    .toBe(true);
});

// Drives commandClient.submitOrder directly via the __submitCommandRaw escape hatch
// (APP-225 §D) — 300 concurrent submits fired in a tight Promise.allSettled loop.
// The form's useOrderSubmission state machine serializes (at most one in-flight at
// a time through the button), so it cannot reach the server-side token-bucket limiter
// (burst=256, sustained=100/sec). The hatch bypasses the form and talks straight to
// the CommandClient instance, enabling a tight burst that crosses the burst ceiling
// (~256) and confirms Throttled acks flow back to the browser.
test("UI submit — Throttle: 300 raw submits trip server-side rate limiter", async ({ page }) => {
  await page.goto("/");
  await readinessGate(page);
  await drainQuiescenceAndBaseline(page);

  // The OrderEntryFormPanel is mounted in slot right-top by the panel registry
  // (panels/order-entry/register.ts). Confirm the submit button is visible, which
  // proves useOrderSubmission has constructed the CommandClient and registered the
  // __submitCommandRaw hatch (registerSubmitCommandRaw in e2eHooks.ts).
  await expect(page.locator('[data-testid="order-entry-submit"]')).toBeVisible({ timeout: 5_000 });

  // Burst 300 raw submits in a single page.evaluate. Each call goes via
  // window.__submitCommandRaw → CommandClient.submitOrder → SBE encode → wss →
  // cluster → CommandAck (Accepted / Throttled / Duplicate / Rejected).
  //
  // Non-Accepted acks reject with CommandRejectedError (which carries .status).
  // Promise.allSettled ensures a single Throttled rejection does not abort the batch.
  //
  // ClOrdID format: "RAW-{i}-{last10}" — longest at i=299: "RAW-299-" (8) + 10 = 18
  // bytes, safely within the FIX 4.4 / SBE schema 20-byte ClOrdID cap.
  //
  // NewOrderSinglePayload: symbol is the canonical 6-char form (no slash); qty and
  // price are bigint × 10^8 (PRICE_SCALE). accountCode must be the same dev-fixture
  // code that the mounted OrderEntryFormPanel uses ("ACME") — sending a blank string
  // causes an InvalidAccountCodeError synchronous rejection and never reaches the
  // limiter at all.
  const statuses = await page.evaluate(async () => {
    const hatch = window.__submitCommandRaw;
    if (typeof hatch !== "function") {
      throw new Error("__submitCommandRaw not bound — OrderEntryForm not mounted");
    }
    const SCALE = 100_000_000n;
    const ts = String(Date.now()).slice(-10);
    const promises = Array.from({ length: 300 }, (_, i) =>
      hatch({
        clOrdId: `RAW-${String(i)}-${ts}`,
        symbol: "EURUSD",
        side: "buy",
        qty: 1n * SCALE,
        price: BigInt(Math.round(1.05 * 1e8)),
        accountCode: "ACME",
      }),
    );
    const results = await Promise.allSettled(promises);
    return results.map((r) => {
      if (r.status === "fulfilled") {
        // CommandAckResult — only Accepted resolves; shape: { status, correlationId }
        return (r.value as { status: string }).status;
      }
      // CommandRejectedError carries .status (Throttled / Duplicate / Rejected)
      const reason = r.reason as { status?: string; name?: string };
      return reason.status ?? reason.name ?? "Unknown";
    });
  });

  // At least one Throttled proves the server-side limiter fired. If every submit
  // comes back Accepted or Duplicate the burst ceiling was never crossed — that is
  // a real regression (limiter disabled or burst raised far above 300).
  expect(statuses.some((s) => s === "Throttled")).toBe(true);

  // No silent drops — every submit must account for itself.
  const knownStatuses = ["Accepted", "Throttled", "Duplicate", "Rejected"];
  const total = statuses.filter((s) => knownStatuses.includes(s)).length;
  expect(total).toBe(300);
});

// Backpressure (client-side slot-table cap) is verified by the proper unit test at
// `web-ui/test/unit/main-thread/commandClient.test.ts` — exercising the bounded slot table
// belongs in a unit test (deterministic, no live worker required), not in a Playwright
// spec talking to a real cluster. The Throttle subtest above asserts the orthogonal
// server-side path that this Playwright spec is the right place for.

test("UI submit — Duplicate: same ClOrdID twice → second resolves Duplicate", async ({ page }) => {
  await page.goto("/");
  await readinessGate(page);
  // Drain quiescence — wait for the worker's command-channel handshake and the
  // first WebSocketSubscribe ACK round-trip to complete. Without this, the
  // first submit can fire BEFORE the worker has fully wired the command port,
  // and the CommandAck never arrives within the 5 s commandClient slot
  // timeout — the form transitions to error state ("timeout waiting for
  // CommandAck for seq=1") instead of success. Mirrors the round-trip
  // subtest which already calls drainQuiescenceAndBaseline.
  await drainQuiescenceAndBaseline(page);
  const dupId = clOrdId("06-dup");
  await page.locator('[data-testid="order-entry-symbol"]').fill("EUR/USD");
  await page.locator('[data-testid="order-entry-qty"]').fill("1.0");
  await page.locator('[data-testid="order-entry-price"]').fill("1.05");

  // First submit — accepted.
  await page.locator('[data-testid="order-entry-clord-id"]').fill(dupId);
  await page.locator('[data-testid="order-entry-submit"]').click();
  await expect(page.locator('[data-testid="order-entry-success"]')).toBeVisible({
    timeout: 15_000,
  });
  await page.locator('[data-testid="order-entry-reset"]').click();

  // Second submit with the SAME ClOrdID — must surface Duplicate.
  await page.locator('[data-testid="order-entry-clord-id"]').fill(dupId);
  await page.locator('[data-testid="order-entry-submit"]').click();
  await expect(page.locator('[data-testid="order-entry-error"]')).toContainText(/Duplicate/i, {
    timeout: 10_000,
  });
});

test("UI submit — Validation: malformed symbol blocked client-side, no worker traffic", async ({
  page,
}) => {
  await page.goto("/");
  await readinessGate(page);
  await page.locator('[data-testid="order-entry-symbol"]').fill("abc");
  await page.locator('[data-testid="order-entry-qty"]').fill("1.0");
  await page.locator('[data-testid="order-entry-price"]').fill("1.05");
  // The submit button must be disabled (validated.error !== null disables it)
  // — the form prevents the click rather than relying on server rejection.
  await expect(page.locator('[data-testid="order-entry-submit"]')).toBeDisabled();
});
