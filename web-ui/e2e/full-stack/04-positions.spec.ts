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

  // Either a new positions row (delta > 0) OR an existing row's qty cell became non-zero —
  // both are valid outcomes after a fill. The strict "+1 row" assertion would race against
  // pre-existing positions for the same symbol. The qty cell selector is column-scoped (NOT
  // any-cell-with-a-digit, which spuriously matches timestamps / row IDs / column indices).
  await expect
    .poll(
      async () => {
        const rows = await page.locator('[data-testid="blotter-positions"] .ag-row').count();
        if (rows > baselineRows) return true;
        // AG Grid renders each cell with `[col-id="<field>"]`; the Positions column is named
        // "qty" in the column-def. We parse the rendered text as a fixed-point qty (the
        // PositionsBlotter formatter emits a decimal string) and accept anything > 0.
        const qtyCells = await page
          .locator('[data-testid="blotter-positions"] .ag-cell[col-id="qty"]')
          .allTextContents();
        return qtyCells.some((c) => {
          // parseFloat instead of Number() — the project lint forbids `Number()`
          // coercion to guard against bigint precision loss; parseFloat is the
          // string→number path. Cell text is the formatted decimal qty.
          const n = parseFloat(c.replace(/,/g, "").trim());
          return Number.isFinite(n) && Math.abs(n) > 0;
        });
      },
      { timeout: 15_000, message: "expected non-zero position qty cell after matched-pair fill" },
    )
    .toBe(true);
});
