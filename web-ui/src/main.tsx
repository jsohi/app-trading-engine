/**
 * Entry point for the Trading Engine browser UI.
 *
 * Lifecycle:
 *   1. Initialise OpenTelemetry SDK with NoopSpanProcessor (production).
 *   2. Set the AG Grid Enterprise license from `import.meta.env.VITE_AG_GRID_LICENSE`.
 *      Missing key is acceptable — the grid renders with a watermark.
 *   3. Render <App /> — which discovers panels via panelRegistry's
 *      `import.meta.glob` to avoid serialised edits to App.tsx across
 *      Phase 2 fan-out branches.
 *
 * Threading model: main thread (browser). All hot-path message
 * handling lives in the Web Worker (APP-36).
 */
import { StrictMode } from "react";
import { createRoot } from "react-dom/client";

import { LicenseManager } from "ag-grid-enterprise";

import { App } from "@/app/App";
import { initialiseTelemetry } from "@/shared/telemetry/otel";

import "@/shared/layout/PanelGrid.css";

initialiseTelemetry();

// AG Grid Enterprise license. Watermark is acceptable in dev / fork
// PRs (which cannot read repo secrets). Production CI builds inject
// this from the AG_GRID_LICENSE secret.
const agGridLicense: unknown = import.meta.env.VITE_AG_GRID_LICENSE;
if (typeof agGridLicense === "string" && agGridLicense.length > 0) {
  LicenseManager.setLicenseKey(agGridLicense);
}

const root = document.getElementById("root");
if (!root) {
  throw new Error("Root element #root not found in index.html");
}

createRoot(root).render(
  <StrictMode>
    <App />
  </StrictMode>,
);
