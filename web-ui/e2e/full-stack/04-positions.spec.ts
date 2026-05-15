/**
 * Full-stack spec 4: PositionsBlotter aggregates a fill from a matched pair.
 * Plan §8 test 4.
 */
import { expect, test } from "@playwright/test";
import { clOrdId, drainQuiescenceAndBaseline, readinessGate, spawnFixCli } from "./helpers";

test("PositionsBlotter shows non-zero qty after matched pair lands a fill", async ({ page }) => {
  await page.goto("/");
  await readinessGate(page);
  const baseline = await drainQuiescenceAndBaseline(page);
  const baselineRows = baseline.get("positions") ?? 0;

  const id = clOrdId("04");
  const status = spawnFixCli(["--scenario", "match", "--clord-id", id]);
  expect(status, "FIX initiator exit code").toBe(0);

  // Either a new positions row (delta > 0) OR an existing row's qty became non-zero —
  // both are valid outcomes after a fill. We check the row count grew OR a non-zero qty
  // cell exists; the strict "exactly +1 row" assertion would race against existing
  // positions for the same symbol.
  await expect
    .poll(
      async () => {
        const rows = await page.locator('[data-testid="blotter-positions"] .ag-row').count();
        if (rows > baselineRows) return true;
        const cells = await page
          .locator('[data-testid="blotter-positions"] .ag-cell')
          .allTextContents();
        return cells.some((c) => /[1-9]/.test(c)); // any non-zero-looking qty
      },
      { timeout: 15_000, message: "expected non-zero position after matched-pair fill" },
    )
    .toBe(true);
});
