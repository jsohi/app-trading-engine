/**
 * Playwright config for the FULL-STACK E2E suite.
 *
 * Triggered by `scripts/full-stack-e2e.sh` (which boots the real cluster +
 * websocket-server + dev JWKS instances). NEVER run concurrently with the
 * default `playwright.config.ts` — both bind port 5173. The Gradle
 * `:web-ui:fullStackE2eRun` task is serialized against `:web-ui:webUiE2e`
 * via the project-wide `PortLockService` build-service.
 *
 * Plan §7. Distinct from `playwright.config.ts` because:
 *   - testDir is `./e2e/full-stack` (not `./e2e`).
 *   - fullyParallel=false + workers=1 — specs share live cluster state.
 *   - retries=1 — one retry budget; flake debugging via the JSON reporter.
 *   - `webServer.reuseExistingServer=false` — Vite must rebuild so the
 *     freshly-baked `VITE_E2E_REAL_BACKEND` + `VITE_DEV_JWT*` env vars are
 *     inlined into the bundle.
 *   - `ignoreHTTPSErrors=false` — real chain validation via Chromium
 *     `--ignore-certificate-errors-spki-list=$MKCERT_SPKI`.
 */
import { defineConfig, devices } from "@playwright/test";

const PORT = "5173" as const;
const BASE_URL = `https://localhost:${PORT}`;
const SPKI = process.env.MKCERT_SPKI ?? "";

export default defineConfig({
  testDir: "./e2e/full-stack",
  timeout: 60_000,
  expect: { timeout: 10_000 },
  fullyParallel: false,
  workers: 1,
  forbidOnly: !!process.env.CI,
  retries: 1,
  reporter: [
    ["list"],
    ["html", { open: "never", outputFolder: "playwright-report-full-stack" }],
    ["json", { outputFile: "test-results/full-stack-results.json" }],
  ],
  use: {
    baseURL: BASE_URL,
    ignoreHTTPSErrors: false,
    trace: "on-first-retry",
    screenshot: "only-on-failure",
    launchOptions: {
      args: SPKI ? [`--ignore-certificate-errors-spki-list=${SPKI}`] : [],
    },
    actionTimeout: 30_000,
  },
  projects: [
    {
      name: "chromium-full-stack",
      use: { ...devices["Desktop Chrome"] },
    },
  ],
  webServer: {
    command: "npm run dev",
    url: BASE_URL,
    timeout: 120_000,
    reuseExistingServer: false,
    ignoreHTTPSErrors: true, // Playwright's URL probe; the per-spec assertions use the strict launchOptions.
    env: {
      VITE_E2E_REAL_BACKEND: "true",
      ...(process.env.VITE_DEV_JWT ? { VITE_DEV_JWT: process.env.VITE_DEV_JWT } : {}),
      ...(process.env.VITE_DEV_JWT_A ? { VITE_DEV_JWT_A: process.env.VITE_DEV_JWT_A } : {}),
      ...(process.env.VITE_DEV_JWT_B ? { VITE_DEV_JWT_B: process.env.VITE_DEV_JWT_B } : {}),
    },
  },
});
