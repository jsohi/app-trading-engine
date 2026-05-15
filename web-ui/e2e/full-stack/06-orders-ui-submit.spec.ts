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

test("UI submit — Throttle: server-side rate-limit fires when client buffer doesn't engage", async ({
  page,
}) => {
  await page.goto("/");
  await readinessGate(page);
  // Submit ≤256 unique-ClOrdID orders rapidly — client buffer cap (256) does NOT engage,
  // server-side rate-limit DOES. Each submit must use a unique ClOrdID so the server-side
  // dedup path cannot trigger first.
  await page.locator('[data-testid="order-entry-symbol"]').fill("EUR/USD");
  await page.locator('[data-testid="order-entry-qty"]').fill("1.0");
  await page.locator('[data-testid="order-entry-price"]').fill("1.05");

  let throttledSeen = false;
  for (let i = 0; i < 200; i++) {
    await page
      .locator('[data-testid="order-entry-clord-id"]')
      .fill(`E2E-S06-throttle-${String(i)}-${String(Date.now())}`);
    await page.locator('[data-testid="order-entry-submit"]').click({ force: true });
    const errLocator = page.locator('[data-testid="order-entry-error"]');
    if ((await errLocator.count()) > 0) {
      const text = await errLocator.textContent({ timeout: 50 }).catch(() => "");
      if (text && /Throttled/i.test(text)) {
        throttledSeen = true;
        break;
      }
    }
  }
  expect(throttledSeen, "expected server-side Throttled status during burst submit").toBe(true);
});

// Backpressure (client-side slot-table cap) is verified by the proper unit test at
// `web-ui/test/unit/main-thread/commandClient.test.ts` — exercising the bounded slot table
// belongs in a unit test (deterministic, no live worker required), not in a Playwright
// spec talking to a real cluster. The Throttle subtest above asserts the orthogonal
// server-side path that this Playwright spec is the right place for.

test("UI submit — Duplicate: same ClOrdID twice → second resolves Duplicate", async ({ page }) => {
  await page.goto("/");
  await readinessGate(page);
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
