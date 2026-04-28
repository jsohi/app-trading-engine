/**
 * Panel registry — discovery contract for Phase 2 fan-out tickets.
 *
 * Why this exists:
 *   The naïve approach is for every Phase 2 ticket to edit
 *   `App.tsx` to import + render its panel. That serialises four
 *   parallel branches (APP-37, APP-40, APP-42, plus 1B's first
 *   real consumer) on a single shared file.
 *
 * The registry replaces that:
 *   - Phase 2 tickets create `src/panels/<name>/register.ts` and
 *     call `registerPanel(...)` at module top-level.
 *   - `App.tsx` consumes `import.meta.glob('../panels/*\/register.ts',
 *     { eager: true })` so module loading triggers registration.
 *   - `collectPanels()` returns a stable, slot-sorted list.
 *
 * Threading model: main thread, single-instance singleton — module
 * scope. Registration happens during initial module evaluation;
 * `collectPanels()` is read-only after that.
 */
import { type ComponentType } from "react";

/**
 * Panel slots in the 2-column responsive layout. Three slots on the
 * right (top/middle/bottom) and two on the left (top/bottom) match
 * the layout sketch in docs/web-ui.md lines 97–132.
 */
export type PanelSlot = "left-top" | "left-bottom" | "right-top" | "right-middle" | "right-bottom";

export interface PanelRegistration {
  /** Stable id used for React keys + analytics. Must be globally unique. */
  readonly id: string;
  /** Human-readable title rendered in the panel chrome. */
  readonly title: string;
  /** Layout slot. At most one panel per slot in 1A; collisions throw. */
  readonly slot: PanelSlot;
  /** The React component that renders the panel body. */
  readonly component: ComponentType;
}

const REGISTRY = new Map<string, PanelRegistration>();

/**
 * Register a panel. Called from `src/panels/<name>/register.ts`
 * at module top-level. Throws on duplicate `id` to surface ordering
 * bugs early.
 *
 * @param registration the panel registration record.
 * @throws Error if a panel with the same id is already registered.
 */
export function registerPanel(registration: PanelRegistration): void {
  const existing = REGISTRY.get(registration.id);
  if (existing && existing !== registration) {
    throw new Error(
      `Duplicate panel registration: ${registration.id} (slot ${registration.slot}). ` +
        `Existing slot: ${existing.slot}.`,
    );
  }
  REGISTRY.set(registration.id, registration);
}

/**
 * Test-only utility: clear all registrations. NOT exported as part
 * of the public surface; intended for use in tests that re-import
 * register modules under a fresh registry.
 */
export function __clearPanelRegistryForTests(): void {
  REGISTRY.clear();
}

/**
 * Snapshot the current registry, sorted by slot order, then by id.
 * Stable across calls until a `registerPanel` happens.
 *
 * @return immutable array of registrations.
 */
export function collectPanels(): readonly PanelRegistration[] {
  const slotOrder: readonly PanelSlot[] = [
    "left-top",
    "left-bottom",
    "right-top",
    "right-middle",
    "right-bottom",
  ];
  const all = Array.from(REGISTRY.values());
  all.sort((a, b) => {
    const slotDelta = slotOrder.indexOf(a.slot) - slotOrder.indexOf(b.slot);
    if (slotDelta !== 0) return slotDelta;
    return a.id.localeCompare(b.id);
  });
  return all;
}
