/**
 * AG Grid v33+ module registration — side-effect import.
 *
 * APP-37 ships with `ag-grid-community` only (NOT Enterprise). The feature
 * surface we need — `applyTransactionAsync`, `getRowId`, `enableCellChangeFlash`,
 * `cellClassRules`, `themeQuartz` Theming API — is all Community-tier. By
 * dropping Enterprise we eliminate the License-Not-Found `console.error`
 * boundary entirely (no watermark, no fork-PR license headache, no test
 * stub needed). Future Enterprise-only features (server-side row model,
 * grouping, pivoting, advanced filters) would require revisiting this.
 *
 * AG Grid v33 split the feature surface into modules that MUST be
 * registered before any `<AgGridReact>` mounts; without registration,
 * `enableCellChangeFlash` and most features silently no-op.
 *
 * Imported as a side effect from:
 *   - `main.tsx` (BEFORE `<App/>` renders).
 *   - `.storybook/preview.ts` (so stories that mount AG Grid work).
 *   - `test/setup.ts` (so jsdom test mounts don't trip a missing-module
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

import { AllCommunityModule, ModuleRegistry } from "ag-grid-community";

ModuleRegistry.registerModules([AllCommunityModule]);
