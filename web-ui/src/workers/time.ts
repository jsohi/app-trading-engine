/**
 * time — high-precision nanosecond clock helpers shared by worker +
 * main-thread code.
 *
 * Per Gemini review R12 (MEDIUM): the obvious one-liner
 *   `BigInt(Math.floor((performance.timeOrigin + performance.now()) * 1e6))`
 * loses sub-millisecond precision. `performance.timeOrigin` is a Unix-
 * epoch milliseconds value (≈ 1.7e12 today), and once it is summed
 * with `performance.now()` and scaled to nanoseconds it exceeds
 * `Number.MAX_SAFE_INTEGER` (2^53 ≈ 9e15) — IEEE-754 doubles cannot
 * represent the full integer, so the lower bits get zeroed.
 *
 * The fix: convert `timeOrigin` and `performance.now()` to BigInt
 * nanoseconds separately before summing. `timeOrigin` is converted at
 * ms precision (it is itself a millisecond-scale wall-clock value) and
 * `performance.now()` keeps full microsecond precision before scaling
 * to nanoseconds.
 *
 * Threading: any (pure function over `performance.timeOrigin` +
 * `performance.now()`).
 *
 * Allocation: one bigint per call (cold paths only — heartbeat, ack,
 * gap-request, watchdog ping; never per inbound frame).
 *
 * Plan reference: §2.8 (heartbeat), §6 row 17 (bigint discipline).
 */

/**
 * Returns the current epoch time in nanoseconds as a `bigint`.
 *
 * Composed as:
 *   `BigInt(floor(timeOrigin)) * 1_000_000n + BigInt(floor(now() * 1_000_000))`
 *
 * which preserves microsecond precision throughout (the worker / main
 * thread never sees better than that on Firefox's privacy-RF mode or
 * Safari's 1 ms floor anyway, but the math is correct everywhere).
 */
export function nowEpochNs(): bigint {
  return (
    BigInt(Math.floor(performance.timeOrigin)) * 1_000_000n +
    BigInt(Math.floor(performance.now() * 1_000_000))
  );
}

/**
 * Returns the current epoch time in milliseconds as a number, computed
 * the same way as `nowEpochNs` but truncated to millis. Use when a
 * caller needs Date-like math and the lossy `timeOrigin + now()` sum
 * is acceptable (e.g. timer scheduling, where ≤ 1 ms drift is fine).
 */
export function nowEpochMs(): number {
  return performance.timeOrigin + performance.now();
}
