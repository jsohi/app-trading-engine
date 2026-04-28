/**
 * Storybook preview config — global decorators and parameters
 * applied to every story.
 */
import type { Preview } from "@storybook/react-vite";

import "@/shared/layout/PanelGrid.css";

const preview: Preview = {
  parameters: {
    layout: "fullscreen",
    backgrounds: {
      default: "panel",
      values: [{ name: "panel", value: "#0c1117" }],
    },
  },
};

export default preview;
