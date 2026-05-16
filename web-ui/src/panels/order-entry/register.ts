/**
 * Panel registration for OrderEntryForm. Plan §12 (APP-160) — slot `right-top`
 * (verified vacant: orders=left-top, positions=left-bottom, quotes=right-middle).
 *
 * Mounts {@link OrderEntryFormPanel} (the dev-fixture wrapper) — NOT
 * {@link OrderEntryForm} directly — so the strict `accountCode` prop on the
 * form is honoured and the dev-fixture default is isolated to one named site.
 * Production replaces the wrapper with one that resolves the authenticated
 * account from the resolved AccountReadModel at AuthAck time (per APP-244 —
 * Web UI Production Hardening umbrella, which owns the per-account symbol-
 * preference + panel-layout editor and the cluster-egress-event-driven live
 * AccountProjection that replaces the dev YAML lookup).
 */
import { registerPanel } from "@/app/panelRegistry";
import { OrderEntryFormPanel } from "@/panels/order-entry/OrderEntryForm";

registerPanel({
  id: "order-entry",
  title: "Order Entry",
  slot: "right-top",
  component: OrderEntryFormPanel,
});
