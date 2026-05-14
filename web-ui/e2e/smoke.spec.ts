/**
 * Playwright smoke test: dev server boots, the app shell renders,
 * and the streaming blotters receive at least one synthetic message.
 *
 * Untagged — runs on fork PRs without an AG Grid license.
 */
import { expect, test } from "@playwright/test";

test("dev server boots and renders panel shell", async ({ page }) => {
  await page.goto("/");
  await expect(page.getByRole("heading", { name: "Trading Engine" })).toBeVisible();
  // Three blotter headings registered by APP-37.
  await expect(page.getByRole("heading", { name: "Orders" })).toBeVisible();
  await expect(page.getByRole("heading", { name: "Positions" })).toBeVisible();
  await expect(page.getByRole("heading", { name: "Quotes" })).toBeVisible();
});

test("blotters receive fakeStream data and conn-dot is visible @licensed", async ({ page }) => {
  // Tagged @licensed — exercises a richer rendering path.
  await page.goto("/");

  // Wait for at least one .ag-root-wrapper (AG Grid mounted).
  await expect(page.locator(".ag-root-wrapper").first()).toBeVisible({ timeout: 5_000 });

  // Connection indicator dot (top-bar ConnectionIndicator).
  await expect(page.locator(".conn-dot").first()).toBeVisible({ timeout: 5_000 });

  // At least one row cell containing a symbol from fakeStream within 10s.
  const symbolPattern = /EUR\/USD|GBP\/USD|USD\/JPY/;
  await expect(async () => {
    const texts = await page.locator(".ag-cell").allTextContents();
    expect(texts.some((t) => symbolPattern.test(t))).toBe(true);
  }).toPass({ timeout: 10_000 });
});
