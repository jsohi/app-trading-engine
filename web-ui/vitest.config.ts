/*
 * Vitest 4 configuration for the Trading Engine web-ui.
 *
 * Three explicit projects:
 *   - `unit`     — jsdom (component + module tests).
 *   - `browser`  — @vitest/browser-playwright (real Chromium; cell-flash
 *                  AC for the streaming blotters lives here).
 *   - `storybook` — @storybook/addon-vitest's storybookTest() plugin runs
 *                  every `*.stories.tsx`'s `play` function as a test
 *                  inside a real browser. Explicit per @storybook/addon-vitest@10
 *                  README — auto-injection is no longer used when consumers
 *                  opt into `projects: [...]`.
 *
 * `npm run test` invokes only the `unit` project to keep the default-dev
 * pre-commit gate fast and Playwright-free. `test:browser` and
 * `test:storybook` invoke the heavier projects explicitly.
 *
 * Threading model: Vitest workers; tests must be parallel-safe.
 *
 * Plan reference: APP-37 §vitest.config.ts.
 */
import { fileURLToPath } from "node:url";
import { resolve } from "node:path";
import { defineConfig } from "vitest/config";
import react from "@vitejs/plugin-react";
import { playwright } from "@vitest/browser-playwright";
import { storybookTest } from "@storybook/addon-vitest/vitest-plugin";

const __dirname = fileURLToPath(new URL(".", import.meta.url));

const alias = {
  "@": resolve(__dirname, "src"),
};

// Shared exclude list — every project carries the same exclusions.
// (Vitest 4 evaluates `exclude` per-project; omitting it on any project
// would silently pick up Playwright `e2e/*.spec.ts` files.)
const sharedExclude = ["node_modules", "dist", "e2e/**"];

export default defineConfig({
  // Root resolve so dynamic-import paths in test files resolve identically
  // across projects.
  resolve: { alias },
  test: {
    coverage: {
      provider: "v8",
      reporter: ["text", "html"],
    },
    projects: [
      {
        extends: true,
        plugins: [react()],
        resolve: { alias },
        test: {
          name: "unit",
          environment: "jsdom",
          globals: false,
          include: [
            "src/**/*.{test,spec}.{ts,tsx}",
            "test/unit/**/*.{test,spec}.{ts,tsx}",
            "test/lint-fixtures/**/*.{test,spec}.{ts,tsx}",
          ],
          exclude: [...sharedExclude, "test/browser/**"],
          setupFiles: ["./test/setup.ts"],
        },
      },
      {
        extends: true,
        plugins: [react()],
        resolve: { alias },
        test: {
          name: "browser",
          globals: false,
          include: ["test/browser/**/*.browser.test.ts", "test/browser/**/*.browser.test.tsx"],
          exclude: sharedExclude,
          setupFiles: ["./test/setup.ts"],
          browser: {
            enabled: true,
            provider: playwright(),
            headless: true,
            instances: [{ browser: "chromium" }],
          },
        },
      },
      {
        extends: true,
        plugins: [storybookTest({ configDir: "./.storybook" })],
        resolve: { alias },
        test: {
          name: "storybook",
          // `storybookTest` discovers stories via `.storybook/main.ts`'s
          // `stories` glob. Setup file wires `setProjectAnnotations(...)`
          // so preview-time decorators (theme CSS, ModuleRegistry import)
          // propagate to play-as-test runs.
          setupFiles: ["./.storybook/vitest.setup.ts"],
          browser: {
            enabled: true,
            provider: playwright(),
            headless: true,
            instances: [{ browser: "chromium" }],
          },
        },
      },
    ],
  },
});
