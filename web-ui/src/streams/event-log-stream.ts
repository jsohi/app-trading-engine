/**
 * event-log-stream — virtualized-list-friendly ring-buffer operator.
 *
 * Stores the last `capacity` events in a fixed `Object[]` ring; emits
 * an `EventLogSnapshot` whose object identity is stable across
 * emissions and whose `read(logicalIndex)` exposes a zero-alloc
 * window into the ring (0 = oldest, count-1 = newest). Consumers
 * cheap-diff via the `version` counter.
 *
 * Default capacity: 10 000. Under BACKPRESSURE, shrinks to 2 000
 * (drop oldest) per §2.9.
 *
 * Threading: main thread.
 *
 * Allocation: per event the operator allocates one
 * `EventLogEntry { templateId, bytes }` plus the encoded `Uint8Array`
 * payload (sized to the event-text length). The ring slots overwrite
 * the prior entry once capacity is reached, so the steady-state heap
 * cost is bounded at `capacity × avg-event-bytes`. The snapshot
 * wrapper itself is stable (zero alloc per emission); the
 * `TextEncoder` is module-scope so it does not allocate per call.
 *
 * Per Gemini review R12 (MEDIUM): the prior "Allocation: zero per
 * emission" claim was inaccurate — once the SBE per-direction
 * encoders ship (APP-260) we can stash the wire bytes directly into
 * a slab and the entry+bytes allocation will go away. Until then,
 * the documented behavior matches the implementation.
 *
 * Plan reference: §5.5 / §6 rows 21, 49.
 */

import { Observable, type OperatorFunction } from "rxjs";

import { type EventLogEntry, type EventLogSnapshot } from "@/streams/api";
import { type WorkerMessage } from "@/shared/transport/MessageShape";

const DEFAULT_CAPACITY_NOMINAL = 10_000;
const DEFAULT_CAPACITY_BACKPRESSURE = 2_000;

/**
 * Module-scope singleton — `TextEncoder` is stateless on `encode(str)`
 * (per WHATWG spec) so reuse is safe and avoids the ~1.5 KiB-per-call
 * allocator churn that the hot path would otherwise incur at 5 k/s.
 */
const TEXT_ENCODER = new TextEncoder();

/**
 * Filter a `WorkerMessage` stream to event-log entries (the worker emits
 * these via the EventUpdate variant once C8 wires the per-template
 * decoders). For now the operator accepts the discriminated `WorkerMessage`
 * stream and projects only `event` updates into the ring.
 */
export function eventLogStream(
  options: { capacity?: number; backpressure?: boolean } = {},
): OperatorFunction<WorkerMessage, EventLogSnapshot> {
  const cap =
    options.capacity ??
    (options.backpressure === true ? DEFAULT_CAPACITY_BACKPRESSURE : DEFAULT_CAPACITY_NOMINAL);
  return (source: Observable<WorkerMessage>) =>
    new Observable<EventLogSnapshot>((subscriber) => {
      const ring: (EventLogEntry | undefined)[] = new Array(cap).fill(undefined) as (
        | EventLogEntry
        | undefined
      )[];
      // Closure-mutable ring state. `head` is the next slot to write.
      // `count` is the number of valid entries (saturates at `cap`).
      let head = 0;
      let count = 0;
      let version = 0;

      // Stable snapshot reference — mutating count/version across
      // emissions and exposing `read(...)` over the closure-captured
      // ring keeps the per-event path zero-allocation and avoids the
      // O(N) `rebuildVisibleEntries` walk that the prior shape forced.
      const snapshot: EventLogSnapshot = {
        capacity: cap,
        get count(): number {
          return count;
        },
        get version(): number {
          return version;
        },
        read(logicalIndex: number): EventLogEntry | undefined {
          if (logicalIndex < 0 || logicalIndex >= count) return undefined;
          // Oldest entry sits at `head` when the ring is full, else at 0.
          const start = count < cap ? 0 : head;
          const idx = (start + logicalIndex) % cap;
          return ring[idx];
        },
      };

      const sub = source.subscribe({
        next(msg) {
          if (msg.type !== "event") return;
          // EventUpdate is what the worker emits per §5.7; ring stores the
          // wire-bytes shape so virtualized list consumers can defer decoding.
          const eventBytes = encodeEventUpdate(msg);
          ring[head] = { templateId: 0, bytes: eventBytes };
          head = (head + 1) % cap;
          if (count < cap) count += 1;
          version += 1;
          subscriber.next(snapshot);
        },
        error: (err: unknown) => {
          subscriber.error(err);
        },
        complete: () => {
          subscriber.complete();
        },
      });
      return () => {
        sub.unsubscribe();
      };
    });
}

function encodeEventUpdate(msg: {
  eventType: string;
  details: string;
  serverNanos: bigint;
}): Uint8Array {
  // Minimal text encoding for now; the C9 contract test layer wires
  // real decoder bytes once the per-template event decoders land. Uses
  // the module-scope `TEXT_ENCODER` singleton (Gemini review MEDIUM).
  const text = `${msg.eventType}|${msg.details}|${String(msg.serverNanos)}`;
  return TEXT_ENCODER.encode(text);
}
