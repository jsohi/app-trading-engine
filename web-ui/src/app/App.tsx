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
// "discovers" the panel surface — its return value is unused, but
// IMPORTING the module list is the load-bearing side effect.
//
// CRITICAL: do NOT remove this constant or its reference in `App()`.
// Vite's tree-shaker would otherwise drop the glob, panels would never
// register, and the app would render an empty grid. The
// `panelRegistryGlob` reference inside `useMemo` is the load-bearing
// expression that keeps the glob alive through bundling.
const panelRegistryGlob = import.meta.glob("../panels/*/register.ts", { eager: true });

export function App(): JSX.Element {
  const panels = useMemo(() => {
    // Touch the glob to keep it alive through tree-shaking; the value
    // (a record of resolved modules) is unused — the side effect is
    // the registerPanel calls that already ran at module evaluation.
    // Use Object.keys so a future maintainer can't "simplify" this
    // away as obviously dead — Object.keys on an unused glob has no
    // optimisation that drops the glob itself.
    Object.keys(panelRegistryGlob);
    return collectPanels();
  }, []);

  return <PanelGrid panels={panels} />;
}
