/**
 * toFixed8 — sole sanctioned bigint → string formatter for fixed-point
 * pricing on the display boundary.
 *
 * ESLint `no-restricted-syntax` bans `Number(<bigint>)` everywhere
 * EXCEPT this file (and its tests). Render code MUST format prices /
 * quantities through this helper; the worker / streams / stores layers
 * keep all int64/uint64 values as `bigint`.
 *
 * Convention: SBE schema fixed-point scale is 1e8 (PRICE_SCALE in
 * `@trading/sbe-codecs`). A wire value `123_456_789n` represents
 * `1.23456789` in display units.
 *
 * Threading: callable from main thread render code only.
 *
 * Allocation: one string per call (display-only; never on the
 * per-frame hot path — banned in `src/workers/**` and `src/streams/**`
 * by ESLint `local/no-bigint-to-number-coerce`).
 *
 * Plan reference: §5.7 / §6 row 16.
 */

const SCALE = 100_000_000n;
const SCALE_DIGITS = 8;

/**
 * Format a fixed-point bigint (1e8 scale) as a decimal string with
 * exactly 8 fractional digits.
 *
 * Negative values are formatted with a leading minus. The integer
 * part is rendered verbatim (no thousands separators); locale
 * formatting is a downstream concern.
 *
 * @param scaled fixed-point value (price or quantity)
 * @returns decimal string with `.` separator, 8 fractional digits
 */
export function toFixed8(scaled: bigint): string {
  const negative = scaled < 0n;
  const abs = negative ? -scaled : scaled;
  const whole = abs / SCALE;
  const frac = abs % SCALE;
  // Pad the fractional part to exactly SCALE_DIGITS.
  const fracStr = frac.toString().padStart(SCALE_DIGITS, "0");
  return `${negative ? "-" : ""}${whole.toString()}.${fracStr}`;
}

/**
 * Convert epoch nanoseconds (bigint) to a JS `Date` for render-time
 * use. Loses sub-millisecond precision; that is acceptable on the
 * display boundary.
 *
 * @param epochNanos epoch nanoseconds as bigint
 * @returns Date constructed from epoch milliseconds
 */
export function nanosToDate(epochNanos: bigint): Date {
  // Integer-divide ns → ms; fits in Number safely until year 275760.
  // eslint-disable-next-line no-restricted-syntax -- sanctioned bigint→Number boundary, see §5.7
  const ms = Number(epochNanos / 1_000_000n);
  return new Date(ms);
}
