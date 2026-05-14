/**
 * AG Grid sort comparators for non-default value types.
 *
 * Why this exists: AG Grid v33+ Community defaults to a `_default_compare`
 * that string-coerces unknown values, so a column whose `valueGetter`
 * returns `bigint` will sort lexicographically (`"10" < "2"`). Setting
 * `cellDataType: "number"` is NOT a fix — that path calls `Number(value)`
 * internally, which throws `TypeError: Cannot convert a BigInt value to
 * a number`. Custom comparator is the right seam.
 *
 * Threading: any (sort runs on the main thread inside AG Grid).
 * Allocation: zero per call — just primitive comparison.
 *
 * @see OrderBlotter / PositionsBlotter / PriceBlotter — apply on every
 *      bigint column.
 *
 * Plan reference: APP-37 Gemini R2 fix (cellDataType bigint sort).
 */

/**
 * Strict bigint comparator with null-tolerant ordering. Nulls sort first
 * (matches AG Grid's default null-handling). Mixed-type calls (one bigint,
 * one not) are treated as unordered (return 0) — defensive vs. a future
 * column that mistakenly returns mixed types.
 *
 * @param a — value from row A (bigint, null, or undefined).
 * @param b — value from row B (bigint, null, or undefined).
 * @returns -1 if a < b, 1 if a > b, 0 otherwise.
 */
export function bigintComparator(a: unknown, b: unknown): number {
  if (a === b) return 0;
  if (a == null && b == null) return 0;
  if (a == null) return -1;
  if (b == null) return 1;
  if (typeof a === "bigint" && typeof b === "bigint") {
    return a < b ? -1 : a > b ? 1 : 0;
  }
  // Mixed types — defensive no-op (logging would spam under sort).
  return 0;
}
