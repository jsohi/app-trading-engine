/**
 * Sample panel registration. 1A only — Phase 2 fan-out tickets
 * follow the same shape with `src/panels/<name>/register.ts`.
 *
 * The module-scoped `registerPanel` call is the side effect that
 * `App.tsx`'s `import.meta.glob('../panels/*\/register.ts')` triggers.
 */
import { registerPanel } from "@/app/panelRegistry";

import { SamplePanel } from "./SamplePanel";

registerPanel({
  id: "sample",
  title: "Sample (1A)",
  slot: "left-top",
  component: SamplePanel,
});
