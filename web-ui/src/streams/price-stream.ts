/**
 * price-stream — symbol-keyed price aggregation with paint-aligned throttle.
 *
 * `throttleTime(100, animationFrameScheduler, { leading: true, trailing: true })`
 * per symbol; aggregated `Map<string, PriceUpdate>` via `scan` with
 * **in-place mutation of the same Map across emissions** (zero-alloc).
 * Downstream consumers MUST treat the value as immutable for the
 * duration of one tick.
 *
 * BACKPRESSURE state widens the throttle from 100 ms → 250 ms per §2.9.
 *
 * Threading: main thread.
 *
 * Allocation: zero per emission after the initial Map allocation.
 *
 * Plan reference: §5.5 / §6 rows 21, 48.
 */

import {
  animationFrameScheduler,
  type Observable,
  scan,
  throttleTime,
  type OperatorFunction,
} from "rxjs";

import { type PriceUpdate } from "@/shared/transport/MessageShape";

const THROTTLE_NOMINAL_MS = 100;
const THROTTLE_BACKPRESSURE_MS = 250;

/**
 * RxJS operator: aggregate per-symbol PriceUpdate emissions into a
 * shared `Map<string, PriceUpdate>` and emit the (mutated-in-place)
 * map per throttled tick. The Map identity is stable across
 * emissions; consumers MUST diff via referential equality of the
 * mutated entries OR by tracking a per-symbol version counter
 * upstream. Snapshot semantics: read-only for one tick.
 *
 * @param backpressure widens the throttle to 250 ms per §2.9
 */
export function priceStream(
  backpressure = false,
): OperatorFunction<PriceUpdate, ReadonlyMap<string, PriceUpdate>> {
  const interval = backpressure ? THROTTLE_BACKPRESSURE_MS : THROTTLE_NOMINAL_MS;
  return (source: Observable<PriceUpdate>) =>
    source.pipe(
      throttleTime(interval, animationFrameScheduler, {
        leading: true,
        trailing: true,
      }),
      scan<PriceUpdate, Map<string, PriceUpdate>>((acc, update) => {
        // In-place mutation — the same Map identity is emitted every tick.
        acc.set(update.symbol, update);
        return acc;
      }, new Map<string, PriceUpdate>()),
    );
}
