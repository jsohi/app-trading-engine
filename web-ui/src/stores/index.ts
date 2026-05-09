/**
 * stores — barrel for selectorized React stores.
 *
 * AG Grid does NOT consume stores — it subscribes directly to streams
 * via `applyTransactionAsync` to avoid `useSyncExternalStore` tearing
 * on whole-Map snapshots. Stores here are for non-grid widgets
 * (status banner, summary panel).
 *
 * Plan reference: §5.6 / §6 row 34.
 */

export { connectionStore } from "@/stores/connection-store";
