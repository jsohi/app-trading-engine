/**
 * Top-level app shell. Renders the panel layout from
 * `docs/web-ui.md` lines 97–132 entirely from the panel registry —
 * Phase 2 fan-out tickets (APP-37 blotters, APP-40 RFQ, APP-42 events)
 * add panels by dropping `src/panels/<name>/register.ts` files; this
 * component does NOT need editing.
 *
 * Threading model: main thread, React 19 concurrent mode.
 */
import { useMemo, type JSX } from "react";

import { PanelGrid } from "@/shared/layout/PanelGrid";
import { collectPanels } from "@/app/panelRegistry";

// Eager glob: every ./panels/<name>/register.ts must call
// `registerPanel(...)` at module scope so the registry is populated
// by the time React mounts. The glob is the single edge that
// "discovers" the panel surface.
const PANEL_MODULES = import.meta.glob("../panels/*/register.ts", { eager: true });

export function App(): JSX.Element {
  const panels = useMemo(() => {
    // Reading the glob value is enough to trigger module-level
    // side effects (registerPanel calls); collectPanels then returns
    // the populated registration list.
    void PANEL_MODULES;
    return collectPanels();
  }, []);

  return <PanelGrid panels={panels} />;
}
