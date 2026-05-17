/**
 * Top-level app shell. Renders the panel layout from
 * `docs/web-ui.md` lines 97–132 entirely from the panel registry —
 * Phase 2 fan-out tickets (APP-37 blotters, APP-40 RFQ, APP-42 events)
 * add panels by dropping `src/panels/<name>/register.ts` files; this
 * component does NOT need editing.
 *
 * Phase 3 Commit B: at AuthAck the server sends a per-account
 * {@code panelLayout} (template 61 group) telling the shell which slot
 * to mount each panel into. App.tsx subscribes to {@link
 * WorkerClient.panelLayout$} and merges those overrides onto the
 * registry's static slots — empty layout → use the registry defaults.
 *
 * Threading model: main thread, React 19 concurrent mode.
 */
import { useMemo, useSyncExternalStore, type JSX } from "react";

import { PanelGrid } from "@/shared/layout/PanelGrid";
import { collectPanels, type PanelRegistration, type PanelSlot } from "@/app/panelRegistry";
import { peekWorkerClient } from "@/main-thread/workerClientSingleton";

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

const VALID_SLOTS: ReadonlySet<PanelSlot> = new Set<PanelSlot>([
  "top-bar",
  "left-top",
  "left-bottom",
  "right-top",
  "right-middle",
  "right-bottom",
]);

const EMPTY_LAYOUT: ReadonlyArray<{ readonly panelId: string; readonly slot: string }> = [];

function usePanelLayoutOverrides(): ReadonlyMap<string, PanelSlot> {
  // useSyncExternalStore subscription bridges the WorkerClient BehaviorSubject
  // to React's render cycle. The snapshot identity is the BehaviorSubject's
  // current value — stable across re-renders until the worker posts a new
  // PANEL_LAYOUT, which is once per session at AuthAck. When no worker
  // singleton has been started (fake-stream demo mode), the subscribe/snapshot
  // pair both resolve to the empty array — the panel registry's static slots
  // apply unchanged.
  const client = peekWorkerClient();
  const layout = useSyncExternalStore(
    (notify) => {
      // No worker singleton (fake-stream demo mode): nothing to subscribe to —
      // return a no-op teardown so useSyncExternalStore stays happy.
      if (client === null) {
        return () => {
          // intentional no-op
        };
      }
      const sub = client.panelLayout$.subscribe(notify);
      return () => {
        sub.unsubscribe();
      };
    },
    () => (client === null ? EMPTY_LAYOUT : client.panelLayout$.getValue()),
    () => EMPTY_LAYOUT,
  );
  return useMemo(() => {
    const map = new Map<string, PanelSlot>();
    for (const entry of layout) {
      // Drop entries whose slot string is not one of the 6 registered slots.
      // The server-side YAML schema validates `^[A-Z]{6,8}$` only for symbol
      // strings; panel slot names are free-form on the wire. A fail-closed
      // narrowing here keeps a misconfigured account from silently mounting
      // panels into invalid slots.
      if (VALID_SLOTS.has(entry.slot as PanelSlot)) {
        map.set(entry.panelId, entry.slot as PanelSlot);
      }
    }
    return map;
  }, [layout]);
}

function applyOverrides(
  panels: readonly PanelRegistration[],
  overrides: ReadonlyMap<string, PanelSlot>,
): readonly PanelRegistration[] {
  if (overrides.size === 0) return panels;
  return panels.map((p) => {
    const override = overrides.get(p.id);
    return override === undefined ? p : { ...p, slot: override };
  });
}

export function App(): JSX.Element {
  const registered = useMemo(() => collectPanels(), []);
  const overrides = usePanelLayoutOverrides();
  const panels = useMemo(() => applyOverrides(registered, overrides), [registered, overrides]);
  return <PanelGrid panels={panels} />;
}
