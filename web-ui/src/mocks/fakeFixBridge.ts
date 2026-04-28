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
interface QuoteRequestMessage {
  readonly type: "QuoteRequest";
  readonly reqId: string;
  readonly symbol: string;
  readonly side: "BUY" | "SELL";
  readonly qty: number; // RFQ JSON path is intentionally JS-number per APP-40 spec
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
