/**
 * Full-stack spec 8: multi-issuer JWT — two browser contexts authenticate against
 * two different issuers and both receive price ticks.
 *
 * Plan §8 test 8 + §15. The test is invoked AFTER the launcher has been
 * restarted with the multi-issuer overlay (orchestrated by
 * scripts/full-stack-e2e.sh §14). Each Playwright context overrides the JWT
 * via `window.__E2E_JWT_OVERRIDE__` (set by `addInitScript` BEFORE the page
 * loads — must precede `devTokenProvider` evaluation).
 *
 * `__E2E_JWT_OVERRIDE__` is honoured by `devTokenProvider.ts` (defence-in-depth:
 * dev-only guard + bundle-guard absence assertion). The two contexts each
 * receive a different JWT bound to a different issuer; the launcher MUST be
 * running with the multi-issuer overlay (orchestrated by
 * scripts/full-stack-e2e.sh — single-issuer overlay would reject issuer B's tokens).
 */
import { test, expect } from "@playwright/test";

test("multi-issuer: two contexts auth against two issuers in parallel", async ({ browser }) => {
  const tokenA = process.env.VITE_DEV_JWT_A;
  const tokenB = process.env.VITE_DEV_JWT_B;
  expect(tokenA, "VITE_DEV_JWT_A must be set by full-stack-e2e.sh").toBeTruthy();
  expect(tokenB, "VITE_DEV_JWT_B must be set by full-stack-e2e.sh").toBeTruthy();

  const ctxA = await browser.newContext();
  await ctxA.addInitScript((t: string) => {
    (globalThis as unknown as { __E2E_JWT_OVERRIDE__?: string }).__E2E_JWT_OVERRIDE__ = t;
  }, tokenA);
  const ctxB = await browser.newContext();
  await ctxB.addInitScript((t: string) => {
    (globalThis as unknown as { __E2E_JWT_OVERRIDE__?: string }).__E2E_JWT_OVERRIDE__ = t;
  }, tokenB);

  const pageA = await ctxA.newPage();
  const pageB = await ctxB.newPage();
  await Promise.all([pageA.goto("/"), pageB.goto("/")]);

  await Promise.all([
    expect(pageA.locator('.conn-indicator[data-state="CONNECTED"]')).toBeVisible({
      timeout: 45_000,
    }),
    expect(pageB.locator('.conn-indicator[data-state="CONNECTED"]')).toBeVisible({
      timeout: 45_000,
    }),
  ]);

  // Both should also show ≥1 price tick within 15s of CONNECTED.
  for (const p of [pageA, pageB]) {
    await expect
      .poll(
        async () => {
          const cells = await p.locator(".ag-cell").allTextContents();
          return cells.some((c) => /EUR\/USD|GBP\/USD|USD\/JPY/.test(c));
        },
        { timeout: 15_000 },
      )
      .toBe(true);
  }

  await ctxA.close();
  await ctxB.close();
});
