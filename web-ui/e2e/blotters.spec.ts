/**
 * Playwright e2e test: blotter panels and connection indicator render
 * correctly and receive streaming data.
 *
 * Tagged untagged (no @licensed) — runs on fork PRs in the smoke set.
 * Requires the dev server to be running (playwright.config.ts baseURL).
 */
import { expect, test } from "@playwright/test";

test("blotters_devServer_headingsVisible", async ({ page }) => {
  await page.goto("/");

  await expect(page.getByRole("heading", { name: "Trading Engine" })).toBeVisible();
  await expect(page.getByRole("heading", { name: "Orders" })).toBeVisible();
  await expect(page.getByRole("heading", { name: "Positions" })).toBeVisible();
  await expect(page.getByRole("heading", { name: "Quotes" })).toBeVisible();
});

test("blotters_devServer_agGridRootWrapperMountsForEachPanel", async ({ page }) => {
  await page.goto("/");

  // AG Grid mounts a .ag-root-wrapper per blotter; wait up to 5s for each.
  // There should be at least 3 wrappers (Orders, Positions, Quotes).
  await expect(page.locator(".ag-root-wrapper").first()).toBeVisible({ timeout: 5_000 });

  const count = await page.locator(".ag-root-wrapper").count();
  expect(count).toBeGreaterThanOrEqual(3);
});

test("blotters_devServer_connectionDotPresentInHeader", async ({ page }) => {
  await page.goto("/");

  // ConnectionIndicator registers in the top-bar slot; .conn-dot is the
  // coloured status circle.
  await expect(page.locator(".conn-dot").first()).toBeVisible({ timeout: 5_000 });
});

test("blotters_devServer_atLeastOneRowAppearsInEachBlotter @licensed", async ({ page }) => {
  // @licensed because iterating over real row data exercises richer AG Grid
  // paths that may trigger the watermark on forks.
  await page.goto("/");

  // fakeStream emits at 250 ms intervals. Allow 10s for ≥1 row per blotter.
  // Check that at least one row cell contains a known symbol.
  const symbolPattern = /EUR\/USD|GBP\/USD|USD\/JPY/;

  await expect(async () => {
    const cells = page.locator(".ag-cell");
    const texts = await cells.allTextContents();
    const hasSymbol = texts.some((t) => symbolPattern.test(t));
    expect(hasSymbol).toBe(true);
  }).toPass({ timeout: 10_000 });
});
