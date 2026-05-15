/**
 * Full-stack spec 3: OrderBlotter receives a FIX-injected order.
 * Plan §8 test 3.
 */
import { expect, test } from "@playwright/test";
import { clOrdId, drainQuiescenceAndBaseline, readinessGate, spawnFixCli } from "./helpers";

test("OrderBlotter receives a FIX-injected NewOrderSingle within 15s", async ({ page }) => {
  await page.goto("/");
  await readinessGate(page);
  await drainQuiescenceAndBaseline(page);

  const id = clOrdId("03");
  const status = spawnFixCli(["--scenario", "single", "--clord-id", id]);
  expect(status, "FIX initiator exit code").toBe(0);

  // Per-spec ClOrdID prefix lets us grep deterministically — a stray row from a
  // prior spec cannot satisfy this assertion.
  await expect
    .poll(
      async () => {
        const cells = await page.locator(".ag-cell").allTextContents();
        return cells.some((c) => c.includes(id));
      },
      { timeout: 15_000, message: `expected ClOrdID ${id} to appear in OrderBlotter` },
    )
    .toBe(true);
});
