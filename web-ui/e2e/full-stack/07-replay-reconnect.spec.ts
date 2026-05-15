/**
 * Full-stack spec 7: Replay reconnect — no event loss within window.
 *
 * Plan §8 test 7. Uses the in-page connectionState$ recorder via
 * `window.__e2eHooks` (registered by `installEarlyHooks()` in main.tsx when
 * `VITE_E2E_REAL_BACKEND === "true"`) so a sub-poll-interval RECONNECTING
 * flash cannot be missed.
 */
import { expect, test } from "@playwright/test";
import { clOrdId, drainQuiescenceAndBaseline, readinessGate, spawnFixCli } from "./helpers";
import { RECONNECT_GATE_MS } from "@/workers/WorkerTuning";
import perfBaselines from "../../perf-baselines.json" with { type: "json" };

const REPLAY_BUDGET_FLOOR_MS = perfBaselines.replayReconnect.budgetFloorMs;
const REPLAY_BUDGET_MS = Math.max(
  REPLAY_BUDGET_FLOOR_MS,
  perfBaselines.replayReconnect.p99WarmupMs * 1.5,
);

test("WorkerClient reconnects after force-close and replays missed FIX-injected orders", async ({
  page,
}) => {
  await page.goto("/");
  await readinessGate(page);
  const baseline = await drainQuiescenceAndBaseline(page);
  const baselineOrders = baseline.get("orders") ?? 0;

  // Install in-page transition recorder BEFORE __forceWsClose. Captures every state
  // transition in real time — Playwright's poll cadence cannot miss a sub-100ms amber flash.
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

  // Inject seq 1..5 BEFORE the disconnect so the OrderBlotter has a known starting point.
  const idPrefix = clOrdId("07");
  for (let i = 1; i <= 5; i++) {
    expect(spawnFixCli(["--scenario", "single", "--clord-id", `${idPrefix}-${String(i)}`])).toBe(0);
  }

  // Wait for the 5 baseline orders to land. The 15s poll comfortably covers the
  // pricing+order-emit cadence on a cold CI runner; tightening it would race the
  // worker's first-MESSAGE_BATCH flush.
  await expect
    .poll(async () => page.locator('[data-testid="blotter-orders"] .ag-row').count(), {
      timeout: 15_000,
    })
    .toBeGreaterThanOrEqual(baselineOrders + 5);

  // Capture wall-clock at force-close so we can assert the post-reconnect replay
  // budget against the WorkerTuning-derived RECONNECT_GATE_MS.
  const tForceCloseMs = Date.now();
  await page.evaluate(() => {
    const fn = (globalThis as unknown as { __forceWsClose?: () => void }).__forceWsClose;
    if (typeof fn === "function") fn();
  });

  // Inject seq 6..10 during the disconnect window.
  for (let i = 6; i <= 10; i++) {
    expect(spawnFixCli(["--scenario", "single", "--clord-id", `${idPrefix}-${String(i)}`])).toBe(0);
  }

  // Reconnect ceiling: derived from WorkerTuning constants
  // (RECONNECT_BASE_MS + WS_HANDSHAKE_BUDGET_MS + RECONNECT_CAP_MS/2).
  // Tuning the backoff stays in sync with the gate; no hardcoded magic number.
  await expect(page.locator('.conn-indicator[data-state="CONNECTED"]').first()).toBeVisible({
    timeout: RECONNECT_GATE_MS,
  });

  // Assert RECONNECTING was actually observed (proves the breaker noticed the close).
  const transitions = await page.evaluate(
    () => (globalThis as unknown as { __connStates?: Array<{ s: string }> }).__connStates ?? [],
  );
  const states = transitions.map((t) => t.s);
  const hasReconnecting = states.includes("RECONNECTING");
  // If the reconnect happened sub-poll-interval, the recorder MUST have caught it. A missing
  // RECONNECTING transition means the worker masked the close (the test would be invalid).
  expect(hasReconnecting, `expected RECONNECTING in ${states.join(",")}`).toBe(true);

  // Wait for all 10 orders to appear under the perf-baselines REPLAY_BUDGET_MS.
  // The budget is `max(1500ms floor, p99_warmup * 1.5)` per perf-baselines.json —
  // tuning the floor stays in sync with the spec; cold-CI variance absorbed by 1.5×.
  const tCapturedReplayBudget = REPLAY_BUDGET_MS;
  await expect
    .poll(async () => page.locator('[data-testid="blotter-orders"] .ag-row').count(), {
      timeout: tCapturedReplayBudget,
    })
    .toBeGreaterThanOrEqual(baselineOrders + 10);
  // Sanity-log: tForceCloseMs bracket vs final budget. Aids CI triage of slow runs.
  console.log(
    `replay-reconnect: tForceClose→allRowsVisible elapsed=${String(Date.now() - tForceCloseMs)}ms; ` +
      `budget=${String(tCapturedReplayBudget)}ms (floor=${String(REPLAY_BUDGET_FLOOR_MS)}ms, p99×1.5=${String(perfBaselines.replayReconnect.p99WarmupMs * 1.5)}ms)`,
  );

  // Tear down recorder.
  await page.evaluate(() => {
    const fn = (globalThis as unknown as { __connStatesUnsub?: () => void }).__connStatesUnsub;
    if (typeof fn === "function") fn();
  });
});
