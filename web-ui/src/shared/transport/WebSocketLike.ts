/**
 * Minimal WebSocket-shaped interface used by APP-36's Web Worker
 * transport. Both the real `WebSocket` global and `mock-socket`'s
 * `WebSocket` polyfill are structurally assignable to it, which
 * lets tests swap a fake without consumer code change.
 *
 * Production constraint: `binaryType` MUST be `"arraybuffer"`.
 * The Netty server (APP-35) emits raw `ArrayBuffer` SBE frames;
 * mock-socket defaults to `Blob` and must be overridden in tests.
 *
 * Threading model: single-instance per Worker, owned by the worker
 * bootstrap. Not shared across threads.
 */
export interface WebSocketLike {
  /**
   * Send a payload to the server. ArrayBuffer for binary command
   * frames; string for the rare control text messages (subscribe
   * ACK). The parameter type is intentionally widened to `string |
   * Blob | BufferSource` to remain assignable from the global
   * `WebSocket` (browser) and `mock-socket`'s implementation.
   * Production code should always send ArrayBuffer/string.
   */
  send(data: string | Blob | BufferSource): void;

  /** Initiate a clean close. */
  close(code?: number, reason?: string): void;

  onmessage: ((ev: MessageEvent) => unknown) | null;
  onclose: ((ev: CloseEvent) => unknown) | null;
  onerror: ((ev: Event) => unknown) | null;
  onopen: ((ev: Event) => unknown) | null;

  binaryType: BinaryType;
  readyState: number;

  addEventListener(type: string, listener: (ev: Event) => unknown): void;
  removeEventListener(type: string, listener: (ev: Event) => unknown): void;
}
