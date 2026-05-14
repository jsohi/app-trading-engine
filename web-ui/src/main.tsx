/**
 * Entry point for the Trading Engine browser UI.
 *
 * Lifecycle (ORDER MATTERS — APP-37 invariants):
 *   1. Initialise OpenTelemetry SDK with NoopSpanProcessor (production).
 *   2. Register AG Grid Community modules (side-effect import). MUST
 *      precede any AG Grid component mount; otherwise `enableCellChangeFlash`
 *      and most v33+ features silently no-op.
 *   3. Boot the message source (`startMessageSource()`). Single call site —
 *      the function is idempotency-guarded but only `main.tsx` should call it.
 *   4. Render <App /> — which discovers panels via panelRegistry's
 *      `import.meta.glob`.
 *
 * APP-37 deliberately ships AG Grid Community (NOT Enterprise) — the
 * feature surface we need is Community-tier and dropping Enterprise removes
 * the License-Not-Found console.error / watermark concern entirely.
 *
 * Threading model: main thread (browser). All hot-path message
 * handling lives in the Web Worker (APP-36).
 *
 * Plan reference: APP-37 §Files to modify (main.tsx).
 */
import { StrictMode } from "react";
import { createRoot } from "react-dom/client";

import { App } from "@/app/App";
import { startMessageSource } from "@/main-thread/messageSource";
import { initialiseTelemetry } from "@/shared/telemetry/otel";

// Side-effect import: AG Grid v33+ ModuleRegistry. MUST precede <App/> mount.
import "@/shared/grid/registerAgGridModules";

import "@/shared/layout/PanelGrid.css";
import "@/shared/grid/agGridTheme.css";

initialiseTelemetry();

// Boot the singleton broadcast point. Idempotent. Sole call site.
startMessageSource();

const root = document.getElementById("root");
if (!root) {
  throw new Error("Root element #root not found in index.html");
}

createRoot(root).render(
  <StrictMode>
    <App />
  </StrictMode>,
);
