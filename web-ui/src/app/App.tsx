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

// Eager glob: every `./panels/<name>/register.ts` and every
// `./shared/layout/<widget>/register.ts` must call `registerPanel(...)`
// at module scope so the registry is populated by the time React mounts.
// `import.meta.glob({ eager: true })` is statically transformed by Vite
// into top-level `import` statements; the imported modules' side effects
// (the `registerPanel` calls) run at module-load and are NOT eliminated
// by Rollup tree-shaking.
//
// We DON'T need to reference the glob value at runtime — the import
// declaration alone is the load-bearing instruction. The `void` cast
// only documents that the value is intentionally unused.
//
// `shared/layout/<widget>/register.ts` covers shell-level registered
// widgets like the top-bar ConnectionIndicator (APP-37) — semantically
// shell, not content panels.
void import.meta.glob("../panels/*/register.ts", { eager: true });
void import.meta.glob("../shared/layout/*/register.ts", { eager: true });

export function App(): JSX.Element {
  const panels = useMemo(() => collectPanels(), []);
  return <PanelGrid panels={panels} />;
}
