/**
 * Full-stack spec 2: PriceBlotter populates from the live PricingService.
 * Plan §8 test 2.
 */
import { expect, test } from "@playwright/test";
import { drainQuiescenceAndBaseline, readinessGate } from "./helpers";

test("PriceBlotter shows at least one major-FX symbol within 15s", async ({ page }) => {
  await page.goto("/");
  await readinessGate(page);
  await drainQuiescenceAndBaseline(page);

  // Pricing emits at a deterministic cadence; 15 s comfortably covers the worst-case startup
  // jitter on a cold CI runner.
  await expect
    .poll(
      async () => {
        const cells = await page.locator(".ag-cell").allTextContents();
        return cells.some((c) => /EUR\/?USD|GBP\/?USD|USD\/?JPY/.test(c));
      },
      { timeout: 15_000, message: "expected a major-FX symbol in PriceBlotter" },
    )
    .toBe(true);
});
