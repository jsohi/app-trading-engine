/**
 * 2-column responsive layout shell that renders panels in their
 * registered slot. Layout matches docs/web-ui.md lines 97–132.
 *
 * As of APP-37 the shell also renders a `top-bar` slot inside the
 * existing `<header className="app-header">` so status widgets (e.g.
 * `ConnectionIndicator`) register normally via the panel registry —
 * no per-widget shell edit needed for future status panels.
 *
 * Threading model: main thread.
 * Allocation: trivial — re-renders only when the panel list changes
 * (which is once per app boot since the registry is module-scoped).
 *
 * Plan reference: APP-37 §Files to modify (PanelGrid.tsx).
 */
import { type JSX } from "react";

import { type PanelRegistration, type PanelSlot } from "@/app/panelRegistry";

interface PanelGridProps {
  readonly panels: readonly PanelRegistration[];
}

const SLOT_LABELS: Record<PanelSlot, string> = {
  "top-bar": "Status",
  "left-top": "Orders",
  "left-bottom": "Positions",
  "right-top": "RFQ",
  "right-middle": "Quotes",
  "right-bottom": "Events",
};

function findPanel(
  panels: readonly PanelRegistration[],
  slot: PanelSlot,
): PanelRegistration | undefined {
  return panels.find((p) => p.slot === slot);
}

function PanelChrome({
  slot,
  panel,
}: {
  readonly slot: PanelSlot;
  readonly panel: PanelRegistration | undefined;
}): JSX.Element {
  if (!panel) {
    return (
      <section
        className="panel panel-empty"
        data-slot={slot}
        aria-label={`${SLOT_LABELS[slot]} (empty)`}
      >
        <header className="panel-header">
          <h2>{SLOT_LABELS[slot]}</h2>
          <span className="panel-status">unregistered</span>
        </header>
        <div className="panel-body">
          <p className="panel-empty-msg">
            No panel registered for slot <code>{slot}</code>.
          </p>
        </div>
      </section>
    );
  }
  const Body = panel.component;
  return (
    <section className="panel" data-slot={slot} aria-label={panel.title}>
      <header className="panel-header">
        <h2>{panel.title}</h2>
      </header>
      <div className="panel-body">
        <Body />
      </div>
    </section>
  );
}

/**
 * Top-bar slot rendered inside the app-header. Unlike grid slots, an
 * empty top-bar slot renders NOTHING (no chrome, no "unregistered"
 * placeholder) — status widgets are optional and an empty header
 * shouldn't show negative space.
 */
function TopBarSlot({ panel }: { readonly panel: PanelRegistration | undefined }): JSX.Element {
  if (!panel) return <></>;
  const Body = panel.component;
  return (
    <div className="app-header__right" data-slot="top-bar" aria-label={panel.title}>
      <Body />
    </div>
  );
}

export function PanelGrid({ panels }: PanelGridProps): JSX.Element {
  return (
    <main className="app-shell">
      <header className="app-header">
        <h1>Trading Engine</h1>
        <span className="app-mode">dev</span>
        <TopBarSlot panel={findPanel(panels, "top-bar")} />
      </header>
      <div className="panel-grid">
        <div className="panel-column panel-column-left">
          <PanelChrome slot="left-top" panel={findPanel(panels, "left-top")} />
          <PanelChrome slot="left-bottom" panel={findPanel(panels, "left-bottom")} />
        </div>
        <div className="panel-column panel-column-right">
          <PanelChrome slot="right-top" panel={findPanel(panels, "right-top")} />
          <PanelChrome slot="right-middle" panel={findPanel(panels, "right-middle")} />
          <PanelChrome slot="right-bottom" panel={findPanel(panels, "right-bottom")} />
        </div>
      </div>
    </main>
  );
}
