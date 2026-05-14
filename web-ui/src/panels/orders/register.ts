/**
 * Orders panel registration. Picked up by `App.tsx`'s eager
 * `import.meta.glob` over the panels directory.
 */
import { registerPanel } from "@/app/panelRegistry";

import { OrderBlotter } from "./OrderBlotter";

registerPanel({
  id: "orders",
  title: "Orders",
  slot: "left-top",
  component: OrderBlotter,
});
