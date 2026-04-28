/*
 * Vitest 4 configuration for the Trading Engine web-ui.
 *
 * - jsdom environment for React component tests + createStore tests.
 * - Storybook addon-vitest discovers *.stories.tsx separately via its
 *   own project entry; here we just cover unit + integration tests.
 *
 * Threading model: Vitest workers; tests must be parallel-safe.
 */
import { fileURLToPath } from "node:url";
import { resolve } from "node:path";
import { defineConfig } from "vitest/config";
import react from "@vitejs/plugin-react";

const __dirname = fileURLToPath(new URL(".", import.meta.url));

export default defineConfig({
  plugins: [react()],
  resolve: {
    alias: {
      "@": resolve(__dirname, "src"),
    },
  },
  test: {
    environment: "jsdom",
    globals: false,
    include: ["src/**/*.{test,spec}.{ts,tsx}", "test/**/*.{test,spec}.{ts,tsx}"],
    // The fixture *.ts files (no .test suffix) are not collected as
    // tests; they're inputs to the lint-fixtures.test.ts spawner.
    exclude: ["node_modules", "dist", "e2e/**"],
    setupFiles: ["./test/setup.ts"],
    typecheck: {
      enabled: true,
      tsconfig: "./tsconfig.json",
    },
    coverage: {
      provider: "v8",
      reporter: ["text", "html"],
    },
  },
});
