/**
 * streams — public barrel for APP-37 / APP-40 / APP-42 consumers.
 *
 * Only `api.ts` types + the per-stream factory functions are
 * re-exported. Internal subjects (e.g. the `pushConnectionState`
 * helper used by `workerClient`) are NOT part of this barrel and
 * must be deep-imported via `@/streams/connection-stream` from the
 * orchestrator only.
 *
 * Plan reference: §5.5 / §6 row 21.
 */

export {
  type ConnectionStream,
  type EventLogSnapshot,
  type EventLogStream,
  type FillStream,
  type OrderStream,
  type PriceStream,
  STREAM_API_VERSION,
} from "@/streams/api";

export { connectionStream$ } from "@/streams/connection-stream";
export { eventLogStream } from "@/streams/event-log-stream";
export { orderStream } from "@/streams/order-stream";
export { positionStream } from "@/streams/position-stream";
export { priceStream } from "@/streams/price-stream";
export {
  getFillRowId,
  getOrderRowId,
  getPositionRowId,
  getPriceRowId,
} from "@/streams/agGridResolvers";
