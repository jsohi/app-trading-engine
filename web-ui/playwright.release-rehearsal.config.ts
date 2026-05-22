/**
 * Playwright config for the RELEASE-REHEARSAL narrative (APP-225).
 *
 * Distinct from `playwright.full-stack.config.ts` because the rehearsal is
 * a single ordered narrative spec with shared state across `test.step(...)`,
 * not a parallel-safe regression suite. Properties tightened versus full-stack:
 *
 *   - testDir = `./e2e/release-rehearsal`.
 *   - testMatch = `trader-day.spec.ts` only.
 *   - retries = 0 — a flake in the rehearsal means investigate; we do not
 *     mask flakiness on the release-gate path.
 *   - trace = `on` — every rehearsal run produces a trace, so the failure
 *     bundle assembled by `scripts/full-stack-e2e.sh` always has one.
 *   - video = `retain-on-failure` — captured per spec on red.
 *
 * Same as full-stack:
 *   - `ignoreHTTPSErrors=false` + Chromium SPKI pin.
 *   - `webServer.command="npm run dev"` with VITE_DEV_JWT* inlined.
 *   - timeout = 60 s, expect timeout = 10 s (cold CI runner budget).
 *
 * Threading: serial, workers=1; the spec relies on a single Page sharing
 * auth + WebSocket session across all six `test.step(...)` blocks.
 */
import { defineConfig, devices } from "@playwright/test";

const PORT = "5173" as const;
const BASE_URL = `https://localhost:${PORT}`;
const SPKI = process.env.MKCERT_SPKI ?? "";

export default defineConfig({
  testDir: "./e2e/release-rehearsal",
  testMatch: ["trader-day.spec.ts"],
  // Narrative spec runs 6 ordered steps within ONE Playwright test, so the
  // top-level `timeout` must cover the SUM of step budgets, not any single one.
  // Allocation budget (used by the per-step expects):
  //   step 1 sign-in / readinessGate           : 45 s (helpers.ts:53)
  //   step 2 drain + ticks-visible             : ~5 s
  //   step 3 inject 3 FIX orders + poll 15 s   : ~18 s
  //   step 4 UI submit + poll 15 s             : ~17 s
  //   step 5 force WS close                    : ~1 s
  //   step 6 RECONNECT_GATE_MS + REPLAY_BUDGET : ~25 s (WorkerTuning + baselines)
  //   total budget                             : ~111 s; pad to 180 s for cold CI.
  timeout: 180_000,
  expect: { timeout: 10_000 },
  fullyParallel: false,
  workers: 1,
  forbidOnly: !!process.env.CI,
  retries: 0,
  reporter: [
    ["list"],
    ["html", { open: "never", outputFolder: "playwright-report-release-rehearsal" }],
    ["json", { outputFile: "test-results/release-rehearsal-results.json" }],
  ],
  use: {
    baseURL: BASE_URL,
    ignoreHTTPSErrors: false,
    trace: "on",
    video: "retain-on-failure",
    screenshot: "only-on-failure",
    launchOptions: {
      args: SPKI ? [`--ignore-certificate-errors-spki-list=${SPKI}`] : [],
    },
    actionTimeout: 30_000,
  },
  projects: [
    {
      name: "release-rehearsal",
      use: { ...devices["Desktop Chrome"] },
    },
  ],
  webServer: {
    command: "npm run dev",
    url: BASE_URL,
    timeout: 120_000,
    reuseExistingServer: false,
    ignoreHTTPSErrors: true,
    env: {
      VITE_E2E_REAL_BACKEND: "true",
      ...(process.env.VITE_DEV_JWT ? { VITE_DEV_JWT: process.env.VITE_DEV_JWT } : {}),
      ...(process.env.VITE_DEV_JWT_A ? { VITE_DEV_JWT_A: process.env.VITE_DEV_JWT_A } : {}),
      ...(process.env.VITE_DEV_JWT_B ? { VITE_DEV_JWT_B: process.env.VITE_DEV_JWT_B } : {}),
    },
  },
});
