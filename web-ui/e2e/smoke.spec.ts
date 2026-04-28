/**
 * Playwright smoke test: dev server boots, the app shell renders,
 * and the sample panel emits at least one synthetic message.
 *
 * Tagged untagged → runs on fork PRs without an AG Grid license.
 */
import { expect, test } from "@playwright/test";

test("dev server boots and renders panel shell", async ({ page }) => {
  await page.goto("/");
  await expect(page.getByRole("heading", { name: "Trading Engine" })).toBeVisible();
  // Sample panel is registered into left-top.
  await expect(page.getByRole("heading", { name: "Sample (1A)" })).toBeVisible();
});

test("sample panel receives a fakeStream tick @licensed", async ({ page }) => {
  // Tagged @licensed only because this test exercises a richer
  // path that is more flaky on watermark-only forks; main repo runs
  // it as part of the full e2e suite.
  await page.goto("/");
  await expect(page.getByText(/"type":\s*"(price|order|fill|event)"/)).toBeVisible({
    timeout: 5_000,
  });
});
