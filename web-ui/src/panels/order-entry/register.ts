/**
 * Panel registration for OrderEntryForm. Plan §12 (APP-160) — slot `right-top`
 * (verified vacant: orders=left-top, positions=left-bottom, quotes=right-middle).
 */
import { registerPanel } from "@/app/panelRegistry";
import { OrderEntryForm } from "@/panels/order-entry/OrderEntryForm";

registerPanel({
  id: "order-entry",
  title: "Order Entry",
  slot: "right-top",
  component: OrderEntryForm,
});
