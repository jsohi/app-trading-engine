/**
 * In-memory mock of the fix-client-bridge :8444 JSON protocol used
 * by the RFQ panel (APP-40). Built on `mock-socket` so the panel
 * code can talk to a `WebSocket` global identically in dev and
 * tests.
 *
 * Threading model: main thread.
 * Allocation: per-message JSON construction is acceptable in the
 * mock path; APP-40 is JSON-on-the-wire by design.
 */
import { Server, WebSocket as MockWebSocket } from "mock-socket";

/**
 * Subset of the FIX bridge JSON protocol we need to mock for 1A.
 * Real protocol is defined by APP-40; this is a stable interim
 * shape that matches `docs/web-ui.md` §RFQ panel.
 */
// TODO(APP-40): The RFQ JSON path uses JS `number` for `qty`/`bid`/`ask`
// — a deliberate exception to the project-wide bigint discipline (CLAUDE.md
// "Pricing: fixed-point only"). The exception is justified ONLY because:
//   (a) this is the JSON-WebSocket path to fix-client-bridge :8444, which
//       per `docs/web-ui.md` §RFQ panel is a separate transport from the
//       binary SBE stream that ALL non-RFQ pricing flows through;
//   (b) APP-40 will define the final RFQ wire schema and may revisit this
//       decision (e.g., switch to a decimal-string representation that
//       preserves precision). Until APP-40 lands, downstream consumers
//       must NOT propagate these `number` values into any state where
//       precision matters — convert at the boundary.
// If APP-40 chooses bigint or decimal-string, this mock and its consumers
// (RFQ panel) flip together.
interface QuoteRequestMessage {
  readonly type: "QuoteRequest";
  readonly reqId: string;
  readonly symbol: string;
  readonly side: "BUY" | "SELL";
  readonly qty: number;
}

interface QuoteResponseMessage {
  readonly type: "Quote";
  readonly reqId: string;
  readonly bid: number;
  readonly ask: number;
}

/**
 * Start the mock fix-client-bridge server on `ws://localhost:8444`
 * (matching the proxy entry in vite.config.ts). Returns a cleanup
 * handle.
 */
export function startFakeFixBridge(): { stop: () => void } {
  const url = "ws://localhost:8444";
  const server = new Server(url);

  server.on("connection", (socket) => {
    socket.on("message", (raw) => {
      const text = typeof raw === "string" ? raw : new TextDecoder().decode(raw as ArrayBuffer);
      let parsed: unknown;
      try {
        parsed = JSON.parse(text);
      } catch {
        socket.send(JSON.stringify({ type: "Error", reason: "invalid-json" }));
        return;
      }
      if (
        typeof parsed === "object" &&
        parsed !== null &&
        (parsed as { type?: unknown }).type === "QuoteRequest"
      ) {
        const message = parsed as QuoteRequestMessage;
        const response: QuoteResponseMessage = {
          type: "Quote",
          reqId: message.reqId,
          bid: 1.085,
          ask: 1.0852,
        };
        socket.send(JSON.stringify(response));
      }
    });
  });

  return {
    stop(): void {
      server.stop();
    },
  };
}

/**
 * Re-exported so consumers in tests can construct mock-socket
 * clients without re-importing the package.
 */
export { MockWebSocket };
