/**
 * tokenProvider — production interface contract for the main-thread
 * token issuer (APP-160).
 *
 * The worker requires the JWT to never touch the trading-app main-
 * thread heap. The issuer (e.g. an auth iframe owned by APP-160)
 * writes the token directly to a `MessagePort`; the trading-app
 * main thread holds only the port handle and forwards it via
 * `Worker.postMessage(initMsg, [tokenPort])`. The worker reads a
 * single `{type: 'TOKEN', value: string}` from the port, captures
 * in a closure, then `port.close()`s on its side.
 *
 * Threading: main thread.
 *
 * Allocation: one `MessageChannel` per call.
 *
 * Plan reference: §4.2 / §6 row 2.
 */

/**
 * Returns a `MessagePort` whose first message is the JWT token.
 * Caller transfers the returned port to the worker via
 * `Worker.postMessage(init, [port])`.
 */
export type TokenProvider = () => Promise<MessagePort>;
