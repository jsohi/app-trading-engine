/**
 * Playwright config for web-ui e2e smoke tests.
 *
 * Suites:
 *   - default: fork-safe smoke (boots dev server, checks panel renders).
 *   - @licensed: tagged tests that require VITE_AG_GRID_LICENSE
 *     (skipped on fork PRs that can't read the secret).
 *
 * Threading: Playwright manages its own browser context per test;
 * tests must be parallel-safe.
 */
import { defineConfig, devices } from "@playwright/test";

const PORT = "5173" as const;
const BASE_URL = `https://localhost:${PORT}`;

export default defineConfig({
  testDir: "./e2e",
  timeout: 30_000,
  expect: {
    timeout: 5_000,
  },
  fullyParallel: true,
  forbidOnly: !!process.env.CI,
  retries: process.env.CI ? 2 : 0,
  ...(process.env.CI ? { workers: 2 } : {}),
  reporter: process.env.CI ? [["github"], ["html", { open: "never" }]] : "list",
  use: {
    baseURL: BASE_URL,
    ignoreHTTPSErrors: true,
    trace: "on-first-retry",
    screenshot: "only-on-failure",
  },
  projects: [
    {
      name: "chromium",
      use: { ...devices["Desktop Chrome"] },
    },
  ],
  webServer: {
    command: "npm run dev",
    url: BASE_URL,
    timeout: 60_000,
    reuseExistingServer: !process.env.CI,
    ignoreHTTPSErrors: true,
  },
});
