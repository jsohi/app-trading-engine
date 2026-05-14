/**
 * Storybook preview config — global decorators and parameters
 * applied to every story.
 */
import type { Preview } from "@storybook/react-vite";

// Side-effect: register AG Grid v33+ modules so any story mounting a
// blotter doesn't trip the missing-module console.error / silent no-op.
import "@/shared/grid/registerAgGridModules";

import "@/shared/layout/PanelGrid.css";
import "@/shared/grid/agGridTheme.css";

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
