/**
 * Storybook story for the SamplePanel reference panel. Becomes the
 * template Phase 2 panels follow:
 *   - One story per panel
 *   - Story drives the same fixture data as Vitest fixture tests
 *   - addon-vitest runs the play function in headless browser mode
 */
import type { Meta, StoryObj } from "@storybook/react-vite";

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

export const Default: Story = {};
