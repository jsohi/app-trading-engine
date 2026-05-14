/**
 * Entry point for the Trading Engine browser UI.
 *
 * Lifecycle (ORDER MATTERS — APP-37 invariants):
 *   1. Initialise OpenTelemetry SDK with NoopSpanProcessor (production).
 *   2. Register AG Grid Enterprise modules (side-effect import). MUST
 *      precede any AG Grid component mount; otherwise `enableCellChangeFlash`
 *      and most v33+ features silently no-op.
 *   3. Set the AG Grid Enterprise license. Watermark fallback is acceptable
 *      in dev / fork PRs.
 *   4. Boot the message source (`startMessageSource()`). Single call site —
 *      the function is idempotency-guarded but only `main.tsx` should call it.
 *   5. Render <App /> — which discovers panels via panelRegistry's
 *      `import.meta.glob`.
 *
 * Threading model: main thread (browser). All hot-path message
 * handling lives in the Web Worker (APP-36).
 *
 * Plan reference: APP-37 §Files to modify (main.tsx).
 */
import { StrictMode } from "react";
import { createRoot } from "react-dom/client";

import { LicenseManager } from "ag-grid-enterprise";

import { App } from "@/app/App";
import { startMessageSource } from "@/main-thread/messageSource";
import { initialiseTelemetry } from "@/shared/telemetry/otel";

// Side-effect import: AG Grid v33+ ModuleRegistry. MUST precede <App/> mount.
import "@/shared/grid/registerAgGridModules";

import "@/shared/layout/PanelGrid.css";
import "@/shared/grid/agGridTheme.css";

initialiseTelemetry();

// AG Grid Enterprise license. Watermark is acceptable in dev / fork
// PRs (which cannot read repo secrets). Production CI builds inject
// this from the AG_GRID_LICENSE secret.
const agGridLicense: unknown = import.meta.env.VITE_AG_GRID_LICENSE;
if (typeof agGridLicense === "string" && agGridLicense.length > 0) {
  LicenseManager.setLicenseKey(agGridLicense);
}

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
