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

// Eager glob: every `./panels/<name>/register.ts` must call
// `registerPanel(...)` at module scope so the registry is populated
// by the time React mounts. Vite's `import.meta.glob({ eager: true })`
// is statically transformed into actual `import` statements at build
// time; the resulting modules' top-level side effects (registerPanel
// calls) are NOT eliminated by tree-shaking under Rollup defaults.
//
// We still keep an `Object.keys(...)` reference in `App()` below as a
// readability anchor — it documents at the call site that the glob's
// VALUE is intentionally unused (the registerPanel side effect is the
// whole point) and discourages a "simplify" refactor that drops the
// declaration.
const panelRegistryGlob = import.meta.glob("../panels/*/register.ts", { eager: true });

export function App(): JSX.Element {
  const panels = useMemo(() => {
    // Read but discard the glob's keys — documents the side-effect-only
    // contract of the glob declaration. The expression is cheap and
    // improves the chance a future maintainer reading this code grasps
    // why the glob exists.
    Object.keys(panelRegistryGlob);
    return collectPanels();
  }, []);

  return <PanelGrid panels={panels} />;
}
