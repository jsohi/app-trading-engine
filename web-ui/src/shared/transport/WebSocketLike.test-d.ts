/**
 * Compile-time assertion that both the global `WebSocket` and
 * `mock-socket`'s `WebSocket` are structurally assignable to
 * `WebSocketLike`. Catches drift before APP-36 depends on the
 * polymorphism.
 *
 * Belt-and-suspenders coverage — both approaches must hold:
 *
 *   1. `expectTypeOf<...>().toExtend<WebSocketLike>()` — Vitest's
 *      type-level assertion, narrower than `toEqualTypeOf` (tolerant of
 *      extra properties on the source). This is the plan-mandated
 *      assertion form (see APP-254 acceptance criteria).
 *
 *   2. `function _acceptsX(socket: X): WebSocketLike { return socket; }`
 *      — direct assignability via function signature. Catches the same
 *      drift if `expectTypeOf` ever changes semantics across Vitest
 *      majors.
 *
 * Verification path:
 *   - `tsc --noEmit -p tsconfig.json` (run via `npm run typecheck` /
 *     `:web-ui:webUiTypecheck`) compiles this `*.test-d.ts`. Either
 *     check failing is a hard typecheck error.
 *   - This file is intentionally not a Vitest test (no runtime body).
 *     The declarations + `expectTypeOf` calls ARE the assertions.
 */
import { expectTypeOf } from "vitest";
import { type WebSocket as MockWebSocket } from "mock-socket";

import { type WebSocketLike } from "./WebSocketLike";

// (1) Plan-mandated assertion form via Vitest's expectTypeOf.
expectTypeOf<WebSocket>().toExtend<WebSocketLike>();
expectTypeOf<InstanceType<typeof MockWebSocket>>().toExtend<WebSocketLike>();

// (2) Belt-and-suspenders: function-signature assignability. The
// signatures require their argument to be a `WebSocketLike`; if
// `WebSocket` or `MockWebSocket` ever diverge, these assignments
// fail to compile. `void` references suppress unused-warning chatter.
function _acceptsNativeWebSocket(socket: WebSocket): WebSocketLike {
  return socket;
}

function _acceptsMockWebSocket(socket: InstanceType<typeof MockWebSocket>): WebSocketLike {
  return socket;
}

void _acceptsNativeWebSocket;
void _acceptsMockWebSocket;
