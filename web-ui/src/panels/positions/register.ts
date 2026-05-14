/**
 * Positions panel registration. Picked up by `App.tsx`'s eager
 * `import.meta.glob` over the panels directory.
 */
import { registerPanel } from "@/app/panelRegistry";

import { PositionsBlotter } from "./PositionsBlotter";

registerPanel({
  id: "positions",
  title: "Positions",
  slot: "left-bottom",
  component: PositionsBlotter,
});
