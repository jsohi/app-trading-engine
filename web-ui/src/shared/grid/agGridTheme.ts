/**
 * AG Grid v35 dark theme via the Theming API.
 *
 * Returns a `themeQuartz`-derived theme configured with the same colour
 * tokens as `PanelGrid.css` (`--panel-bg`, `--panel-fg`, `--panel-border`,
 * `--panel-header-bg`) so the grid blends seamlessly into the surrounding
 * shell. The Theming API self-injects its own CSS at runtime — no legacy
 * `ag-theme-quartz.css` import needed.
 *
 * Cell-level colour overrides (side/status/position) live in
 * `agGridTheme.css` (separate file imported alongside this module).
 *
 * Threading: any (module-load time only).
 * Allocation: one-shot at module load.
 *
 * @see registerAgGridModules — must run BEFORE any blotter mounts.
 * @see agGridTheme.css — companion CSS for cellClassRules tokens.
 *
 * Plan reference: APP-37 §AG Grid theme.
 */

import { type Theme, themeQuartz } from "ag-grid-community";

/** Quartz dark theme tuned to the shell's CSS custom properties. */
export const themeQuartzDark: Theme = themeQuartz.withParams({
  backgroundColor: "#0c1117",
  foregroundColor: "#d6dde6",
  headerBackgroundColor: "#11171f",
  headerTextColor: "#d6dde6",
  borderColor: "#1f2a37",
  rowHoverColor: "#161d27",
  selectedRowBackgroundColor: "#1b2532",
  oddRowBackgroundColor: "#0e141b",
  fontFamily:
    'ui-sans-serif, system-ui, -apple-system, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif',
  fontSize: 13,
});
