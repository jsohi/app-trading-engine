/**
 * Purpose: Unit tests for `bigintComparator` — verifies AG Grid-compatible
 * sort behaviour over bigint values with null/undefined edge cases.
 *
 * Rationale: AG Grid v33+ Community sorts unknown types lexicographically
 * by default (`"10" < "2"`). `cellDataType: "number"` calls `Number(bigint)`
 * which throws. The custom comparator is the right seam — these tests pin
 * the ordering contract so future AG Grid upgrades or refactors can't drift.
 *
 * @see ../comparators — system under test.
 */
import { describe, it, expect } from "vitest";

import { bigintComparator } from "./comparators";

describe("bigintComparator", () => {
  it("bigintComparator_aLessThanB_returnsNegative", () => {
    expect(bigintComparator(2n, 10n)).toBeLessThan(0);
  });

  it("bigintComparator_aGreaterThanB_returnsPositive", () => {
    expect(bigintComparator(10n, 2n)).toBeGreaterThan(0);
  });

  it("bigintComparator_aEqualsB_returnsZero", () => {
    expect(bigintComparator(42n, 42n)).toBe(0);
  });

  it("bigintComparator_negativeBigints_sortBeforePositive", () => {
    expect(bigintComparator(-10n, 5n)).toBeLessThan(0);
    expect(bigintComparator(5n, -10n)).toBeGreaterThan(0);
  });

  it("bigintComparator_veryLargeBigints_doesNotOverflow", () => {
    const huge = 10n ** 100n;
    expect(bigintComparator(huge, huge + 1n)).toBeLessThan(0);
    expect(bigintComparator(huge + 1n, huge)).toBeGreaterThan(0);
  });

  it("bigintComparator_bothNull_returnsZero", () => {
    expect(bigintComparator(null, null)).toBe(0);
    expect(bigintComparator(undefined, undefined)).toBe(0);
    expect(bigintComparator(null, undefined)).toBe(0);
  });

  it("bigintComparator_aNullbValue_returnsNegative", () => {
    // null sorts first (matches AG Grid default null handling).
    expect(bigintComparator(null, 10n)).toBeLessThan(0);
    expect(bigintComparator(undefined, 10n)).toBeLessThan(0);
  });

  it("bigintComparator_aValuebNull_returnsPositive", () => {
    expect(bigintComparator(10n, null)).toBeGreaterThan(0);
    expect(bigintComparator(10n, undefined)).toBeGreaterThan(0);
  });

  it("bigintComparator_mixedTypes_returnsZero", () => {
    // Defensive no-op vs. a column that mistakenly returns mixed types.
    expect(bigintComparator(10n, "10")).toBe(0);
    expect(bigintComparator(10n, 10)).toBe(0);
  });

  it("bigintComparator_arraySort_orderingIsAscending", () => {
    // End-to-end: the comparator drives Array.prototype.sort correctly.
    const values: ReadonlyArray<bigint> = [10n, 2n, 30n, 1n, 100n, 50n];
    const sorted = [...values].sort(bigintComparator);
    expect(sorted).toEqual([1n, 2n, 10n, 30n, 50n, 100n]);
  });
});
