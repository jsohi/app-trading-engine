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

// Boot the singleton broadcast point so blotter stories receive
// fakeStream data. `startMessageSource` is idempotency-guarded so
// repeated story remounts (including HMR) are safe. Without this,
// stories that mount a blotter directly subscribe to `messages$` but
// no producer ever pushes — `play` functions waiting for `.ag-row`
// would time out.
import { startMessageSource } from "@/main-thread/messageSource";
startMessageSource();

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
