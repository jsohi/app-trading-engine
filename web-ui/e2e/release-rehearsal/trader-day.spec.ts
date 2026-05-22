/**
 * Release-rehearsal: trader-day narrative (APP-225).
 *
 * One ordered Playwright spec, one Page, one auth context, one continuous
 * WebSocket session. The six steps mirror how a human walks through pre-release
 * sign-off:
 *
 *   1. Sign in — JWT-bound auth lands, ConnectionIndicator turns green.
 *   2. Market data — price blotter has at least one row (ticks are flowing).
 *   3. Baseline orders — inject 3 FIX orders so the trader has context.
 *   4. UI order entry — submit a NewOrderSingle via OrderEntryForm; capture id.
 *   5. WS disconnect — force-close the WebSocket, observe RECONNECTING.
 *   6. Replay recovery — after reconnect, ALL orders (baseline + UI-submitted)
 *      are still present in the blotter (proves the replay path delivers no
 *      message loss within RECONNECT_GATE_MS).
 *
 * Threading: Playwright runs this spec serially (single worker per
 * release-rehearsal config). Each step is a `test.step(...)` inside ONE
 * `test()` so the Page, auth context, and in-page recorder state flow
 * uninterrupted across steps — the structural difference from the
 * `e2e/full-stack/*` specs, which each get a fresh Page.
 *
 * Selectors reused from the production UI:
 *   `[data-testid="order-entry-{clord-id|symbol|side|qty|price|submit}"]`
 *   `[data-testid="blotter-{orders|positions|prices}"] .ag-row`
 *   `.conn-indicator[data-state="CONNECTED"]`
 *   `.conn-dot.conn-green`
 *   `window.__forceWsClose()` (e2eHooks escape hatch, only present when
 *   `VITE_E2E_REAL_BACKEND === "true"`).
 *
 * Why no fill assertion: APP-180 (price-time matcher) is still in the backlog,
 * so submitted orders land as ack'd (NEW), not FILLED. Asserting on FILLED
 * today would either be flaky or require harness changes that go beyond the
 * scope of a release rehearsal. The narrative therefore verifies acknowledgement
 * + replay — the two paths that ARE prod-live today.
 */
import { expect, test } from "@playwright/test";
import {
  clOrdId,
  drainQuiescenceAndBaseline,
  readinessGate,
  spawnFixCli,
} from "../full-stack/helpers";
import { RECONNECT_GATE_MS } from "@/workers/WorkerTuning";
import perfBaselines from "../../perf-baselines.json" with { type: "json" };

const REPLAY_BUDGET_MS = Math.max(
  perfBaselines.replayReconnect.budgetFloorMs,
  perfBaselines.replayReconnect.p99WarmupMs * 1.5,
);

test("trader-day: sign in → see ticks → place order → disconnect → replay recovers state", async ({
  page,
}) => {
  // ---------------------------------------------------------------------
  // STEP 1 — sign in (JWT-bound) and reach CONNECTED.
  // ---------------------------------------------------------------------
  await test.step("1. sign in with JWT and see ConnectionIndicator green", async () => {
    await page.goto("/");
    await readinessGate(page);
    await expect(page.locator(".conn-dot.conn-green").first()).toBeVisible({ timeout: 10_000 });
  });

  // ---------------------------------------------------------------------
  // STEP 2 — market-data ticks flow.
  //
  // The price blotter ag-grid renders one row per subscribed symbol; the
  // pricing-service publishes a MarketDataTick (template 54) on every 30 Hz
  // drain cycle. Within drainQuiescenceAndBaseline()'s 5 s budget, at least
  // one row must be present.
  // ---------------------------------------------------------------------
  const baseline = await test.step("2. watch market-data ticks accumulate", async () => {
    const b = await drainQuiescenceAndBaseline(page);
    expect(
      b.get("prices") ?? 0,
      "price blotter must have at least one row (subscribed symbols + MarketDataTick flow)",
    ).toBeGreaterThan(0);
    return b;
  });
  const baselineOrders = baseline.get("orders") ?? 0;

  // ---------------------------------------------------------------------
  // STEP 3 — inject 3 baseline FIX orders so the trader has context.
  //
  // Drives the real Artio FIX initiator (`integration-tests/bin/integration-tests`)
  // against the real cluster; each order round-trips through the cluster and
  // is broadcast back to the WS session via EventSink.
  // ---------------------------------------------------------------------
  const baselinePrefix = clOrdId("RR");
  await test.step("3. inject 3 baseline FIX orders", async () => {
    for (let i = 1; i <= 3; i++) {
      expect(
        spawnFixCli(["--scenario", "single", "--clord-id", `${baselinePrefix}-${String(i)}`]),
        `FIX inject ${baselinePrefix}-${String(i)} must exit 0`,
      ).toBe(0);
    }
    await expect
      .poll(async () => page.locator('[data-testid="blotter-orders"] .ag-row').count(), {
        timeout: 15_000,
      })
      .toBeGreaterThanOrEqual(baselineOrders + 3);
  });

  // ---------------------------------------------------------------------
  // STEP 4 — UI order entry: submit one NewOrderSingle through OrderEntryForm.
  //
  // Same path as full-stack spec 6 happy-path: form → useOrderSubmission →
  // CommandClient → NewOrderSingleEncoder → worker → wss → cluster →
  // CommandAck → form clears → row appears in OrderBlotter.
  // ---------------------------------------------------------------------
  const uiClOrdId = clOrdId("RU");
  await test.step("4. submit one order via OrderEntryForm", async () => {
    await page.locator('[data-testid="order-entry-clord-id"]').fill(uiClOrdId);
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
          return cells.some((c) => c.includes(uiClOrdId));
        },
        { timeout: 15_000 },
      )
      .toBe(true);
  });

  // ---------------------------------------------------------------------
  // STEP 5 — force WebSocket close; observe RECONNECTING.
  //
  // Uses the production e2eHooks escape hatch (`__forceWsClose`, only present
  // when VITE_E2E_REAL_BACKEND="true") plus an in-page transition recorder
  // subscribing to `__e2eHooks.connectionState$`. A sub-poll-interval flash
  // through RECONNECTING is captured by the recorder rather than missed by
  // Playwright's poll cadence.
  // ---------------------------------------------------------------------
  await test.step("5. force WS close and observe RECONNECTING", async () => {
    await page.evaluate(() => {
      const g = globalThis as unknown as {
        __connStates?: Array<{ s: string; t: number }>;
        __connStatesUnsub?: () => void;
        __e2eHooks?: {
          connectionState$: {
            subscribe: (o: { next: (s: string) => void }) => { unsubscribe: () => void };
          };
        };
      };
      g.__connStates = [];
      const hooks = g.__e2eHooks;
      if (hooks?.connectionState$) {
        const sub = hooks.connectionState$.subscribe({
          next: (s) => {
            if (Array.isArray(g.__connStates)) {
              g.__connStates.push({ s, t: performance.now() });
            }
          },
        });
        g.__connStatesUnsub = () => {
          sub.unsubscribe();
        };
      }
    });

    await page.evaluate(() => {
      const fn = (globalThis as unknown as { __forceWsClose?: () => void }).__forceWsClose;
      if (typeof fn === "function") fn();
    });
  });

  // ---------------------------------------------------------------------
  // STEP 6 — replay recovery: every order (baseline + UI-submitted) survives
  // the disconnect and is present in the blotter after reconnect, under
  // RECONNECT_GATE_MS (derived from WorkerTuning) + REPLAY_BUDGET_MS
  // (perf-baselines).
  // ---------------------------------------------------------------------
  await test.step("6. replay recovers all orders within budget", async () => {
    await expect(
      page.locator('.conn-indicator[data-state="CONNECTED"]').first(),
      "must reconnect within RECONNECT_GATE_MS",
    ).toBeVisible({ timeout: RECONNECT_GATE_MS });

    const transitions = await page.evaluate(
      () => (globalThis as unknown as { __connStates?: Array<{ s: string }> }).__connStates ?? [],
    );
    const states = transitions.map((t) => t.s);
    expect(
      states.includes("RECONNECTING"),
      `expected RECONNECTING in transitions; saw ${states.join(",")}`,
    ).toBe(true);

    // All 3 baseline FIX orders + 1 UI-submitted order must be present after
    // replay. Asserting on the per-clOrdId presence (not the row count alone)
    // proves the worker replay actually re-decoded the same orders rather
    // than counting a freshly-emitted batch.
    await expect
      .poll(
        async () => {
          const cells = await page.locator(".ag-cell").allTextContents();
          const baselinePresent = Array.from({ length: 3 }, (_, i) =>
            cells.some((c) => c.includes(`${baselinePrefix}-${String(i + 1)}`)),
          );
          const uiPresent = cells.some((c) => c.includes(uiClOrdId));
          return baselinePresent.every(Boolean) && uiPresent;
        },
        { timeout: REPLAY_BUDGET_MS },
      )
      .toBe(true);

    await page.evaluate(() => {
      const fn = (globalThis as unknown as { __connStatesUnsub?: () => void }).__connStatesUnsub;
      if (typeof fn === "function") fn();
    });
  });
});
