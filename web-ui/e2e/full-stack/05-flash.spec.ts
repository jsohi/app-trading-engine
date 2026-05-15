/**
 * Full-stack spec 5: OrderBlotter emits a `cellFlash` event on status transition.
 *
 * Plan §8 test 5. Uses the in-page recorder via `window.__ordersGridApi`
 * (registered by OrderBlotter.tsx when `VITE_E2E_REAL_BACKEND === "true"`),
 * NOT a class poll — AG Grid removes `.ag-cell-data-changed` on a ~500 ms
 * timer and the assertion would race the removal.
 */
import { expect, test } from "@playwright/test";
import { clOrdId, drainQuiescenceAndBaseline, readinessGate, spawnFixCli } from "./helpers";

test("OrderBlotter cellFlash fires on status transition", async ({ page }) => {
  await page.goto("/");
  await readinessGate(page);
  await drainQuiescenceAndBaseline(page);

  // Install the cellFlash recorder BEFORE injecting the FIX order. The recorder
  // captures every cellFlash in real time (no Playwright-poll race).
  await page.evaluate(() => {
    (globalThis as unknown as { __cellFlashes?: unknown[] }).__cellFlashes = [];
    const api = (
      globalThis as unknown as {
        __ordersGridApi?: { addEventListener?: (event: string, cb: (e: unknown) => void) => void };
      }
    ).__ordersGridApi;
    if (api && typeof api.addEventListener === "function") {
      api.addEventListener(
        "cellFlash",
        (e: { colDef?: { field?: string }; node?: { id?: string } }) => {
          const flashes = (globalThis as unknown as { __cellFlashes?: unknown[] }).__cellFlashes;
          if (Array.isArray(flashes)) {
            flashes.push({ field: e.colDef?.field ?? "", rowId: e.node?.id ?? "" });
          }
        },
      );
    }
  });

  const id = clOrdId("05");
  const status = spawnFixCli(["--scenario", "single", "--clord-id", id]);
  expect(status).toBe(0);

  await expect
    .poll(
      async () =>
        page.evaluate(() => {
          const flashes = (
            globalThis as unknown as {
              __cellFlashes?: { field: string }[];
            }
          ).__cellFlashes;
          return Array.isArray(flashes) && flashes.some((f) => f.field === "status");
        }),
      { timeout: 10_000, message: "expected a cellFlash on the status field" },
    )
    .toBe(true);
});
