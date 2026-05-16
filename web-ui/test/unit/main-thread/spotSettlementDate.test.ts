/**
 * Unit tests for `spotSettlementDate` — the T+2 spot-FX settlement-date helper
 * exported by `commandClient.ts`. Covers every day-of-week start point + the
 * boundary cases the Gemini iter-2 review called out (Friday→Tue, Sat→Tue,
 * Sun→Tue, end-of-month, end-of-year, leap-year Feb crossing).
 *
 * All `Date` literals use `Date.UTC(...)` so the test is timezone-independent.
 */
import { describe, it, expect } from "vitest";
import { spotSettlementDate } from "@/main-thread/commandClient";

function utc(y: number, m: number, d: number): Date {
  return new Date(Date.UTC(y, m - 1, d));
}

describe("spotSettlementDate (T+2 with weekend skip)", () => {
  it.each([
    // Mon → Wed
    { from: utc(2026, 5, 18), expected: "20260520" },
    // Tue → Thu
    { from: utc(2026, 5, 19), expected: "20260521" },
    // Wed → Fri
    { from: utc(2026, 5, 20), expected: "20260522" },
    // Thu → Mon (skip Sat + Sun)
    { from: utc(2026, 5, 21), expected: "20260525" },
    // Fri → Tue (skip Sat + Sun)
    { from: utc(2026, 5, 22), expected: "20260526" },
    // Sat → Tue (skip Sun)
    { from: utc(2026, 5, 23), expected: "20260526" },
    // Sun → Tue
    { from: utc(2026, 5, 24), expected: "20260526" },
  ])("$expected from $from.toISOString", ({ from, expected }) => {
    expect(spotSettlementDate(from)).toBe(expected);
  });

  it("crosses month boundary (Mon 2026-06-29 → Wed 2026-07-01)", () => {
    expect(spotSettlementDate(utc(2026, 6, 29))).toBe("20260701");
  });

  it("crosses year boundary (Thu 2026-12-31 → Mon 2027-01-04)", () => {
    expect(spotSettlementDate(utc(2026, 12, 31))).toBe("20270104");
  });

  it("crosses leap-year Feb (Wed 2024-02-28 → Fri 2024-03-01)", () => {
    // 2024-02-28 is Wednesday; +2 business days → Friday 2024-03-01.
    expect(spotSettlementDate(utc(2024, 2, 28))).toBe("20240301");
  });

  it("zero-pads single-digit month and day", () => {
    // Tue 2026-01-06 → Thu 2026-01-08
    expect(spotSettlementDate(utc(2026, 1, 6))).toBe("20260108");
  });

  it("returns YYYYMMDD shape (8 ASCII digits)", () => {
    const out = spotSettlementDate(utc(2026, 5, 18));
    expect(out).toMatch(/^\d{8}$/);
  });

  it("UTC-normalises the input — local timezone of the host does not affect output", () => {
    // `new Date("2026-05-22T23:30:00-05:00")` has UTC components for 2026-05-23.
    // The helper takes the UTC year/month/day, so the answer is "Sat → Tue".
    const lateFridayCdt = new Date("2026-05-22T23:30:00-05:00");
    expect(spotSettlementDate(lateFridayCdt)).toBe("20260526");
  });
});
