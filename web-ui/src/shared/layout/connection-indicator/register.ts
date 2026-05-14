/**
 * ConnectionIndicator registration. Picked up by App.tsx's eager
 * `import.meta.glob` over the shared/layout directory.
 * Registers into the top-bar slot rendered by PanelGrid inside the
 * app-header.
 */
import { registerPanel } from "@/app/panelRegistry";

import { ConnectionIndicator } from "./ConnectionIndicator";

registerPanel({
  id: "connection",
  title: "Connection",
  slot: "top-bar",
  component: ConnectionIndicator,
});
