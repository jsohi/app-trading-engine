/**
 * AG Grid v33+ module registration — side-effect import.
 *
 * AG Grid v33 split the feature surface into modules that MUST be
 * registered before any `<AgGridReact>` mounts; without registration,
 * `enableCellChangeFlash` and most other features silently no-op and
 * the grid may render empty.
 *
 * This file is imported as a side effect from:
 *   - `main.tsx` (BEFORE `<App/>` renders).
 *   - `.storybook/preview.ts` (so stories that mount AG Grid work).
 *   - `test/setup.ts` (so jsdom test mounts don't trip the missing-module
 *     console.error and break `App.test.tsx`'s errorSpy invariant).
 *
 * Threading: any (module-load time only).
 * Allocation: one-shot registration call; no per-render cost.
 *
 * @see main.tsx — primary call site.
 * @see preview.ts — Storybook call site.
 * @see test/setup.ts — Vitest call site.
 *
 * Plan reference: APP-37 §AG Grid module registration.
 */

import { ModuleRegistry } from "ag-grid-community";
import { AllEnterpriseModule } from "ag-grid-enterprise";

ModuleRegistry.registerModules([AllEnterpriseModule]);
