/**
 * Compile-time assertion that both the global `WebSocket` and
 * `mock-socket`'s `WebSocket` are structurally assignable to
 * `WebSocketLike`. Catches drift before APP-36 depends on the
 * polymorphism.
 *
 * We use direct `satisfies`-style assignability checks rather than
 * expectTypeOf because the `toExtend` API in expect-type v1.x
 * compares the full structural shape including readonly literals
 * (CONNECTING / OPEN / CLOSING / CLOSED) which we deliberately do
 * NOT include in WebSocketLike (irrelevant to our use case).
 *
 * Vitest discovers `*.test-d.ts` via its `typecheck` config and
 * compiles them — the `function takes` arguments establish the
 * subtype check. Any breakage causes the typecheck task to fail.
 */
import { test } from "vitest";
import { type WebSocket as MockWebSocket } from "mock-socket";

import { type WebSocketLike } from "./WebSocketLike";

// The compile-time assignability checks: the function signatures
// require the argument to be a `WebSocketLike`. If `WebSocket` or
// `MockWebSocket` ever diverge, these assignments fail to compile.
function _acceptsNativeWebSocket(socket: WebSocket): WebSocketLike {
  return socket;
}

function _acceptsMockWebSocket(socket: InstanceType<typeof MockWebSocket>): WebSocketLike {
  return socket;
}

test("WebSocketLike: structural assignability is enforced at compile time", () => {
  // Runtime body is intentionally empty — the contract is enforced
  // by the types of `_acceptsNativeWebSocket` and `_acceptsMockWebSocket`.
});
