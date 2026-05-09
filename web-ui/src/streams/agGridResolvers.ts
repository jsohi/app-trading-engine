/**
 * agGridResolvers — `getRowId` resolvers for AG Grid `applyTransactionAsync`.
 *
 * Without `getRowId`, AG Grid treats every update as an insert (the
 * grid cannot match incoming updates to existing rows). Plan §5.5
 * exports these as part of the transport contract so APP-37 / APP-40
 * / APP-42 wire them identically.
 *
 * Threading: main thread (called from grid options).
 *
 * Allocation: pure functions; no allocation.
 *
 * Plan reference: §5.5 / §6 rows 32, 33.
 */

import {
  type FillUpdate,
  type OrderUpdate,
  type PriceUpdate,
} from "@/shared/transport/MessageShape";

export const getOrderRowId = (data: OrderUpdate): string => data.clOrdId;
export const getPriceRowId = (data: PriceUpdate): string => data.symbol;
export const getFillRowId = (data: FillUpdate): string => data.execId;
