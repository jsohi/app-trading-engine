/**
 * Reference implementation of a panel — used by 1A solely to
 * exercise the registry contract end-to-end (Vitest + the live dev
 * shell). Phase 2 fan-out tickets will REPLACE this with real
 * panels in `src/panels/{orders,positions,quotes,rfq,events}/`.
 *
 * Threading model: main thread.
 */
import { type JSX, useMemo } from "react";

import { createStore, useStore } from "@/shared/store/createStore";
import { fakeStream } from "@/mocks/fakeStream";
import { type WorkerMessage } from "@/shared/transport/MessageShape";

const INITIAL: WorkerMessage = {
  type: "event",
  seq: 0n,
  eventType: "Bootstrap",
  details: "awaiting first tick…",
  serverNanos: 0n,
};

export function SamplePanel(): JSX.Element {
  const store = useMemo(
    () =>
      createStore<WorkerMessage>(fakeStream({ intervalMs: 1000 }), {
        name: "sample",
        initial: INITIAL,
      }),
    [],
  );
  const message = useStore(store);

  return (
    <div className="sample-panel">
      <p>
        Latest message type: <strong>{message.type}</strong>
      </p>
      <pre style={{ margin: 0, fontSize: "0.8rem" }}>
        {JSON.stringify(
          message,
          (_key: string, value: unknown) => {
            if (typeof value === "bigint") {
              return value.toString() + "n";
            }
            return value;
          },
          2,
        )}
      </pre>
    </div>
  );
}
