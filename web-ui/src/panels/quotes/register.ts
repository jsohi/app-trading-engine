/**
 * Quotes panel registration. Picked up by `App.tsx`'s eager
 * `import.meta.glob` over the panels directory.
 */
import { registerPanel } from "@/app/panelRegistry";

import { PriceBlotter } from "./PriceBlotter";

registerPanel({
  id: "quotes",
  title: "Quotes",
  slot: "right-middle",
  component: PriceBlotter,
});
