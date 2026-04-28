/**
 * Storybook 10.3 main config for the Trading Engine web-ui.
 *
 * Framework: @storybook/react-vite (Vite 8 builder).
 * Addons:
 *   - @storybook/addon-vitest: runs play functions as Vitest tests
 *     in headless browser mode (Chromium via Playwright).
 *
 * Stories live next to their components: `*.stories.tsx`.
 */
import type { StorybookConfig } from "@storybook/react-vite";

const config: StorybookConfig = {
  framework: {
    name: "@storybook/react-vite",
    options: {},
  },
  stories: ["../src/**/*.stories.@(ts|tsx)"],
  addons: ["@storybook/addon-vitest"],
  typescript: {
    check: false,
    reactDocgen: "react-docgen-typescript",
  },
};

export default config;
