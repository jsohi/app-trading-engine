/**
 * Binary round-trip test through `mock-socket`. Validates that the
 * mock library correctly preserves ArrayBuffer bytes — a quirk in
 * past versions silently coerced to string. APP-36 depends on
 * binary fidelity, so this is the contract pin.
 */
import { describe, expect, it } from "vitest";
import { Server, WebSocket as MockWebSocket } from "mock-socket";

describe("mock-socket binary round-trip", () => {
  it("send_arrayBufferPayload_serverReceivesIdenticalBytes", async () => {
    const url = "ws://localhost:9999";
    const server = new Server(url);
    try {
      const payload = new Uint8Array([0x01, 0x02, 0xff, 0x7f, 0x80]);
      const received = new Promise<Uint8Array>((resolve, reject) => {
        const timer = setTimeout(() => {
          reject(new Error("server did not receive message"));
        }, 1000);
        server.on("connection", (socket) => {
          socket.on("message", (data) => {
            clearTimeout(timer);
            if (data instanceof ArrayBuffer) {
              resolve(new Uint8Array(data));
              return;
            }
            // mock-socket may give Uint8Array depending on version.
            // Copy bytes into a fresh Uint8Array backed by ArrayBuffer
            // (NOT SharedArrayBuffer) so the assertion path is
            // unambiguous.
            if (data instanceof Uint8Array) {
              const copy = new Uint8Array(data.byteLength);
              copy.set(data);
              resolve(copy);
              return;
            }
            reject(new Error(`unexpected payload type: ${typeof data}`));
          });
        });
      });

      const client = new MockWebSocket(url);
      client.binaryType = "arraybuffer";
      await new Promise<void>((resolve) => {
        client.onopen = () => {
          resolve();
        };
      });
      client.send(payload.buffer);

      const view = await received;
      expect(Array.from(view)).toEqual(Array.from(payload));
      client.close();
    } finally {
      server.stop();
    }
  });
});
