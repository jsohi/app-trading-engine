/**
 * Compile-time assertion that both the global `WebSocket` and
 * `mock-socket`'s `WebSocket` are structurally assignable to
 * `WebSocketLike`. Catches drift before APP-36 depends on the
 * polymorphism.
 *
 * We use direct `satisfies`-style assignability checks rather than
 * `expectTypeOf` because the `toExtend` API in expect-type v1.x
 * compares the full structural shape including readonly literals
 * (CONNECTING / OPEN / CLOSING / CLOSED) which we deliberately do
 * NOT include in WebSocketLike (irrelevant to our use case).
 *
 * Verification path:
 *   - `tsc --noEmit -p tsconfig.json` (run via `npm run typecheck` /
 *     `:web-ui:webUiTypecheck`) compiles every file under `src/`,
 *     including this `*.test-d.ts`. The `function _acceptsX`
 *     signatures require their parameter to be assignable to
 *     `WebSocketLike`; if either `WebSocket` or `MockWebSocket`
 *     diverges, the typecheck task fails.
 *   - This file is intentionally not a Vitest test — there is no
 *     runtime body to execute. The `_acceptsX` function declarations
 *     ARE the assertions.
 */
import { type WebSocket as MockWebSocket } from "mock-socket";

import { type WebSocketLike } from "./WebSocketLike";

// The compile-time assignability checks: the function signatures
// require the argument to be a `WebSocketLike`. If `WebSocket` or
// `MockWebSocket` ever diverge, these assignments fail to compile.
//
// `void` references suppress unused-warning chatter without altering
// any runtime behaviour (the file has none).
function _acceptsNativeWebSocket(socket: WebSocket): WebSocketLike {
  return socket;
}

function _acceptsMockWebSocket(socket: InstanceType<typeof MockWebSocket>): WebSocketLike {
  return socket;
}

void _acceptsNativeWebSocket;
void _acceptsMockWebSocket;
