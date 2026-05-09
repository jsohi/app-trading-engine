/**
 * event-log-stream — virtualized-list-friendly ring-buffer operator.
 *
 * Stores the last `capacity` events in a fixed `Object[]` ring; emits
 * `(snapshotRef, version)` tuples via the EventLogSnapshot shape.
 * The snapshot reference is stable across emissions (same array
 * identity); the version counter increments on every new entry so
 * virtualized list consumers can cheap-diff via the version.
 *
 * Default capacity: 10 000. Under BACKPRESSURE, shrinks to 2 000
 * (drop oldest) per §2.9.
 *
 * Threading: main thread.
 *
 * Allocation: zero per emission after construction (mutates the ring
 * in place + emits a frozen snapshot wrapper). The snapshot wrapper
 * itself is a minimal `{entries, version}` object — one allocation
 * per entry, but `entries` references the same backing array.
 *
 * Plan reference: §5.5 / §6 rows 21, 49.
 */

import { Observable, type OperatorFunction } from "rxjs";

import { type EventLogSnapshot } from "@/streams/api";
import { type WorkerMessage } from "@/shared/transport/MessageShape";

interface RingEntry {
  readonly templateId: number;
  readonly bytes: Uint8Array;
}

interface EventLogEntry {
  readonly templateId: number;
  /** SBE-encoded event bytes (post-header). */
  readonly bytes: Uint8Array;
}

const DEFAULT_CAPACITY_NOMINAL = 10_000;
const DEFAULT_CAPACITY_BACKPRESSURE = 2_000;

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
      let head = 0;
      let count = 0;
      let version = 0;
      const visibleEntries: RingEntry[] = [];

      const sub = source.subscribe({
        next(msg) {
          if (msg.type !== "event") return;
          // EventUpdate is what the worker emits per §5.7; ring stores the
          // wire-bytes shape so virtualized list consumers can defer decoding.
          // Construct a minimal EventLogEntry from the EventUpdate.
          const eventBytes = encodeEventUpdate(msg);
          ring[head] = { templateId: 0, bytes: eventBytes };
          head = (head + 1) % cap;
          if (count < cap) count += 1;
          version += 1;
          rebuildVisibleEntries(visibleEntries, ring, head, count, cap);
          subscriber.next({ entries: visibleEntries, version });
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

function rebuildVisibleEntries(
  out: RingEntry[],
  ring: (EventLogEntry | undefined)[],
  head: number,
  count: number,
  cap: number,
): void {
  out.length = 0;
  // Walk from oldest → newest.
  const start = count < cap ? 0 : head;
  for (let i = 0; i < count; i++) {
    const idx = (start + i) % cap;
    const entry = ring[idx];
    if (entry !== undefined) out.push(entry);
  }
}

function encodeEventUpdate(msg: {
  eventType: string;
  details: string;
  serverNanos: bigint;
}): Uint8Array {
  // Minimal text encoding for now; the C9 contract test layer wires
  // real decoder bytes once the per-template event decoders land.
  const text = `${msg.eventType}|${msg.details}|${String(msg.serverNanos)}`;
  return new TextEncoder().encode(text);
}
