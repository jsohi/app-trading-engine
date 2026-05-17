/**
 * SymbolPacking.test.ts — unit tests for pack / packBytes / unpack per
 * Phase 3 Commit B.
 *
 * Covers: round-trip for 6, 7, and 8-char symbols; rejection of 5-char
 * and 9-char inputs; non-ASCII char rejection; packed value fits in
 * Number.MAX_SAFE_INTEGER; stability across multiple distinct symbols.
 *
 * Test naming follows `<unit>_<scenario>_<expectedBehavior>` per plan §5.8.
 *
 * Threading: single-threaded (Vitest jsdom).
 */

import { describe, expect, it } from "vitest";
import { pack, packBytes, unpack, SYMBOL_MAX_CHARS } from "@/shared/transport/SymbolPacking";

// ─── Tests ───────────────────────────────────────────────────────────────────

describe("SymbolPacking.pack + unpack", () => {
  it("pack_unpack_sixCharSymbol_roundTrips", () => {
    const symbol = "EURUSD";
    const packed = pack(symbol);
    const unpacked = unpack(packed);

    expect(unpacked).toBe(symbol);
  });

  it("pack_unpack_sevenCharSymbol_roundTrips", () => {
    const symbol = "GBPUSDT";
    const packed = pack(symbol);
    const unpacked = unpack(packed);

    expect(unpacked).toBe(symbol);
  });

  it("pack_unpack_eightCharSymbol_roundTrips", () => {
    const symbol = "USDJPYAB";
    const packed = pack(symbol);
    const unpacked = unpack(packed);

    expect(unpacked).toBe(symbol);
  });

  it("pack_fiveCharSymbol_throwsRangeError", () => {
    expect(() => pack("GBPUS")).toThrow(RangeError);
  });

  it("pack_nineCharSymbol_throwsRangeError", () => {
    expect(() => pack("EURUSDABC")).toThrow(RangeError);
  });

  it("pack_lowercaseChar_throwsRangeError", () => {
    // 'e' is not in A-Z range → must throw
    expect(() => pack("eurusd")).toThrow(RangeError);
  });

  it("pack_digitChar_throwsRangeError", () => {
    // '1' is not an upper-case ASCII letter
    expect(() => pack("EUR1SD")).toThrow(RangeError);
  });

  it("pack_nonAsciiChar_throwsRangeError", () => {
    // 'Ä' is outside A-Z
    expect(() => pack("ÄURUSD")).toThrow(RangeError);
  });

  it("pack_multipleSymbols_producesDistinctPackedValues", () => {
    const symbols = ["EURUSD", "GBPUSD", "USDJPY", "AUDUSD", "USDCHF", "EURGBP"];
    const packed = symbols.map(pack);
    const unique = new Set(packed);

    expect(unique.size).toBe(symbols.length);
  });

  it("pack_packedValue_fitsInNumberMaxSafeInteger", () => {
    // Worst case is 8 × 'Z' (charValue=26, 6 bits each)
    const packed = pack("ZZZZZZZZ");

    expect(packed).toBeLessThanOrEqual(Number.MAX_SAFE_INTEGER);
    expect(Number.isInteger(packed)).toBe(true);
  });

  it("pack_unpack_stability_sameSymbolPacksIdentically", () => {
    const symbol = "USDJPY";
    const first = pack(symbol);
    const second = pack(symbol);

    expect(first).toBe(second);
    expect(unpack(first)).toBe(symbol);
    expect(unpack(second)).toBe(symbol);
  });
});

describe("SymbolPacking.packBytes", () => {
  /** Encode a symbol into a NUL-padded 8-byte Uint8Array. */
  function symbolToBytes(symbol: string): Uint8Array {
    const bytes = new Uint8Array(SYMBOL_MAX_CHARS);
    for (let i = 0; i < symbol.length; i++) {
      bytes[i] = symbol.charCodeAt(i);
    }
    return bytes;
  }

  it("packBytes_sixCharSymbol_matchesPackOutput", () => {
    const symbol = "EURUSD";
    const bytes = symbolToBytes(symbol);

    expect(packBytes(bytes)).toBe(pack(symbol));
  });

  it("packBytes_sevenCharSymbol_matchesPackOutput", () => {
    const symbol = "GBPUSDT";
    const bytes = symbolToBytes(symbol);

    expect(packBytes(bytes)).toBe(pack(symbol));
  });

  it("packBytes_eightCharSymbol_matchesPackOutput", () => {
    const symbol = "USDJPYAB";
    const bytes = symbolToBytes(symbol);

    expect(packBytes(bytes)).toBe(pack(symbol));
  });

  it("packBytes_embeddedNul_throwsRangeError", () => {
    // NUL at position 3 followed by non-NUL at position 4 is malformed
    const bytes = new Uint8Array([0x45, 0x55, 0x52, 0x00, 0x53, 0x44, 0x00, 0x00]); // EUR\0SD\0\0
    expect(() => packBytes(bytes)).toThrow(RangeError);
  });

  it("packBytes_shortBuffer_throwsRangeError", () => {
    const bytes = new Uint8Array(4); // shorter than SYMBOL_MAX_CHARS=8
    expect(() => packBytes(bytes)).toThrow(RangeError);
  });

  it("packBytes_firstByteNul_throwsRangeError", () => {
    const bytes = new Uint8Array(SYMBOL_MAX_CHARS); // all NUL
    expect(() => packBytes(bytes)).toThrow(RangeError);
  });
});

describe("SymbolPacking.unpack", () => {
  it("unpack_negativeInput_throwsRangeError", () => {
    expect(() => unpack(-1)).toThrow(RangeError);
  });

  it("unpack_nonIntegerInput_throwsRangeError", () => {
    expect(() => unpack(1.5)).toThrow(RangeError);
  });
});
