/*
 * Vitest 4 configuration for the @vitest/browser project (Playwright + Chromium).
 *
 * Used for tests that require a real browser DOM — most notably the AG Grid
 * cell-flash assertion (jsdom lacks ResizeObserver + zero-measurement DOM
 * causes the real grid to silently no-op there).
 *
 * Discovery: `test/browser` subtree only — files matching the
 * `*.browser.test.ts` suffix. The unit project
 * (vitest.config.ts) explicitly excludes this path so files don't double-run.
 *
 * The existing 5 placeholder *.browser.test.ts files under test/browser/
 * {integration,security}/ are all `it.skip` today and move into this runner
 * intentionally.
 *
 * Threading model: Vitest workers; tests must be parallel-safe (each spec
 * gets its own Chromium browser context).
 *
 * Plan reference: APP-37 §Files to modify (vitest.config.ts) — implemented
 * as a separate file rather than a `projects: [...]` migration to keep the
 * Storybook addon-vitest auto-injection working unchanged.
 */
import { fileURLToPath } from "node:url";
import { resolve } from "node:path";
import { defineConfig } from "vitest/config";
import react from "@vitejs/plugin-react";
import { playwright } from "@vitest/browser-playwright";

const __dirname = fileURLToPath(new URL(".", import.meta.url));

export default defineConfig({
  plugins: [react()],
  resolve: {
    alias: {
      "@": resolve(__dirname, "src"),
    },
  },
  test: {
    globals: false,
    include: ["test/browser/**/*.browser.test.ts", "test/browser/**/*.browser.test.tsx"],
    exclude: ["node_modules", "dist", "e2e/**"],
    setupFiles: ["./test/setup.ts"],
    browser: {
      enabled: true,
      provider: playwright(),
      headless: true,
      instances: [{ browser: "chromium" }],
    },
  },
});
