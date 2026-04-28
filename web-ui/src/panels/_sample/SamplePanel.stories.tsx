/**
 * Storybook story for the SamplePanel reference panel. Becomes the
 * template Phase 2 panels follow:
 *   - One story per panel
 *   - Story drives the same fixture data as Vitest fixture tests
 *   - addon-vitest runs the play function in headless browser mode
 *   - Every story includes a play function asserting at least one
 *     observable behaviour — bare render-only stories set a low bar
 *     that Phase 2 panels would inherit.
 */
import type { Meta, StoryObj } from "@storybook/react-vite";
// Storybook 9+ moved the test helpers (expect/within from `@testing-library`-compatible
// shims, instrumented via `@storybook/instrumenter`) into the `storybook/test` subpath.
// The legacy `@storybook/test` package no longer exists in v10.
import { expect, within } from "storybook/test";

import { SamplePanel } from "./SamplePanel";

const meta: Meta<typeof SamplePanel> = {
  title: "Panels/Sample",
  component: SamplePanel,
  parameters: {
    layout: "fullscreen",
  },
};

export default meta;

type Story = StoryObj<typeof SamplePanel>;

export const Default: Story = {
  play: async ({ canvasElement }) => {
    const canvas = within(canvasElement);
    // Initial render shows the bootstrap message — verify the contract
    // that SamplePanel renders SOMETHING immediately (no null returns,
    // no Suspense boundary, no spinner-only state). Phase 2 panels
    // copy this story; setting this baseline forces every panel to be
    // instantly mountable.
    await expect(canvas.findByText(/Latest message type:/)).resolves.toBeTruthy();
    // The bigint-aware JSON serializer is exercised — the rendered
    // <pre> must contain an "n"-suffixed bigint stringification of
    // serverNanos. This guards the JSON.stringify replacer regression.
    const pre = canvasElement.querySelector("pre");
    await expect(pre?.textContent ?? "").toMatch(/"serverNanos":\s*"\d+n"/);
  },
};
