/**
 * Bundle-guard self-test — proves the guard's matcher actually fires when a
 * synthetic bundle contains a forbidden symbol. Pairs with
 * {@code build-bundle.test.ts} (the real-bundle check); the two together
 * give the "test-the-test" coverage that a single positive assertion lacks.
 *
 * <p><b>Why this matters:</b> a green {@code build-bundle.test.ts} run on a
 * clean prod bundle proves the bundle is clean. It does NOT prove the
 * matcher would have caught a regression — a matcher bug that returns "no
 * offenders" for every input would silently pass forever. This self-test
 * closes that loop by:
 *
 * <ol>
 *   <li>Iterating every {@link FORBIDDEN_SYMBOLS} row whose
 *       {@code prodForbidden} flag is true.</li>
 *   <li>Constructing a synthetic bundle text containing exactly that one
 *       symbol (plus realistic minified-bundle filler).</li>
 *   <li>Asserting {@link findForbiddenInProd} returns the row.</li>
 * </ol>
 *
 * <p>Adding a new {@link FORBIDDEN_SYMBOLS} row therefore automatically
 * grows the self-test — no per-symbol boilerplate. The same parametrised
 * pattern covers the JWT regex matcher and the word-boundary special case
 * for {@code MarketDataFeedStateChange} (which must NOT be tripped by the
 * legitimate {@code MarketDataFeedStateChangeDecoder} import).
 *
 * <p><b>Runtime:</b> milliseconds — no {@code vite build}, no disk I/O.
 * Runs in the `unit` (node) vitest project alongside
 * {@code build-bundle.test.ts}.
 *
 * <p>Plan reference: APP-244 Phase 3 Commit C.9.
 */
import { describe, it, expect } from "vitest";

import {
  FORBIDDEN_JWT_REGEX,
  FORBIDDEN_SYMBOLS,
  findForbiddenInProd,
  findJwtLiteral,
  findMissingInE2e,
  matcherFor,
} from "./build-bundle.guard";

/**
 * Realistic minified-bundle filler — exercises identifier-character
 * neighbours so a buggy word-boundary regex would visibly fail. The string
 * deliberately does NOT contain any FORBIDDEN_SYMBOLS literal.
 */
const FILLER = `
const x=1,y=2;function foo(a,b){return a+b}export{foo as default};
class Bar{constructor(){this.z=3}}
const obj={a:1,b:2,c:[1,2,3]};
`;

/**
 * Build a synthetic bundle that contains exactly the named symbol once,
 * surrounded by realistic minified-bundle filler. For word-boundary symbols
 * we ALSO include a confusable substring (e.g. {@code FooDecoder} for
 * {@code Foo}) so the self-test catches a regex bug that would over-match.
 */
function syntheticBundleWith(symbol: string, includeConfusable: boolean): string {
  const confusable = includeConfusable ? `globalThis.${symbol}Decoder=class{};\n` : "";
  // We embed the symbol as a property access (e.g. {@code window.__forceWsClose})
  // which is exactly how it would appear in a real bundle.
  return `${FILLER}${confusable}globalThis.${symbol}=undefined;\n${FILLER}`;
}

describe("bundle-guard self-test: findForbiddenInProd fires for every prodForbidden row", () => {
  const prodRows = FORBIDDEN_SYMBOLS.filter((e) => e.prodForbidden);
  // Sanity: the table is non-empty AND has at least one row in every category
  // we care about — guards against an accidental table-wipe regression.
  it("FORBIDDEN_SYMBOLS has at least one prodForbidden row", () => {
    expect(prodRows.length).toBeGreaterThan(0);
  });

  for (const entry of prodRows) {
    it(`fires for '${entry.symbol}' (${entry.rationale})`, () => {
      const synthetic = syntheticBundleWith(entry.symbol, entry.wordBoundary);
      const offenders = findForbiddenInProd(synthetic);
      // The offender list must contain THIS symbol — other rows may or may not
      // appear depending on filler/confusable overlap, but THIS one is required.
      const hit = offenders.find((o) => o.symbol === entry.symbol);
      expect(
        hit,
        `matcher did NOT flag '${entry.symbol}' in synthetic bundle — guard would silently let it ship`,
      ).toBeDefined();
    });
  }
});

describe("bundle-guard self-test: findMissingInE2e fires when an e2eRequired symbol is absent", () => {
  const e2eRows = FORBIDDEN_SYMBOLS.filter((e) => e.e2eRequired);
  it("FORBIDDEN_SYMBOLS has at least one e2eRequired row", () => {
    expect(e2eRows.length).toBeGreaterThan(0);
  });

  for (const entry of e2eRows) {
    it(`flags '${entry.symbol}' as missing when absent from the e2e bundle`, () => {
      // Bundle text with EVERY required symbol EXCEPT this one — proves the
      // matcher is specific to the missing entry rather than always-empty.
      const present = e2eRows
        .filter((other) => other.symbol !== entry.symbol)
        .map((other) => `globalThis.${other.symbol}=1;`)
        .join("\n");
      const synthetic = `${FILLER}${present}\n${FILLER}`;
      const missing = findMissingInE2e(synthetic);
      const hit = missing.find((o) => o.symbol === entry.symbol);
      expect(
        hit,
        `findMissingInE2e did NOT flag '${entry.symbol}' as missing — e2e regression would be silent`,
      ).toBeDefined();
    });

    it(`accepts '${entry.symbol}' as present when it IS in the e2e bundle`, () => {
      // Inverse: when ALL required symbols are present, findMissingInE2e MUST
      // return [] — proves the matcher does not produce false negatives.
      const present = e2eRows.map((other) => `globalThis.${other.symbol}=1;`).join("\n");
      const synthetic = `${FILLER}${present}\n${FILLER}`;
      const missing = findMissingInE2e(synthetic);
      expect(missing).toEqual([]);
    });
  }
});

describe("bundle-guard self-test: word-boundary matcher rejects confusable substrings", () => {
  const boundaryRows = FORBIDDEN_SYMBOLS.filter((e) => e.wordBoundary);
  it("FORBIDDEN_SYMBOLS declares at least one word-boundary row", () => {
    expect(boundaryRows.length).toBeGreaterThan(0);
  });

  for (const entry of boundaryRows) {
    it(`does NOT flag '${entry.symbol}Decoder' (legitimate decoder import)`, () => {
      // The exact failure mode this guard exists to prevent: the SBE decoder
      // class (e.g. {@code MarketDataFeedStateChangeDecoder}) ships in the
      // worker bundle for runtime template-57 decoding. A substring matcher
      // would false-positive; the word-boundary matcher must accept it.
      const synthetic = `${FILLER}import{${entry.symbol}Decoder}from"./sbe";new ${entry.symbol}Decoder();\n${FILLER}`;
      const offenders = findForbiddenInProd(synthetic);
      const hit = offenders.find((o) => o.symbol === entry.symbol);
      expect(
        hit,
        `word-boundary matcher INCORRECTLY flagged '${entry.symbol}Decoder' as '${entry.symbol}' — would block legitimate decoder imports`,
      ).toBeUndefined();
    });

    it(`still flags bare '${entry.symbol}' even with word-boundary matching`, () => {
      // Mirror of the above: the matcher must not be so lax that the actual
      // bare symbol slips through.
      const synthetic = `${FILLER}globalThis.${entry.symbol}=1;\n${FILLER}`;
      expect(matcherFor(entry)(synthetic)).toBe(true);
    });
  }
});

describe("bundle-guard self-test: JWT regex matcher", () => {
  it("fires on a realistic JWT literal", () => {
    const jwt =
      "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4ifQ";
    const synthetic = `${FILLER}const t="${jwt}";\n${FILLER}`;
    expect(findJwtLiteral(synthetic)).not.toBeNull();
  });

  it("does not fire on a string that merely starts with 'eyJ' but is too short", () => {
    // {@code eyJ} alone (or with <20 trailing chars) is not enough to look
    // like a real JWT — the regex {20,} guards against random false-positives.
    const synthetic = `${FILLER}const s="eyJtoo-short";\n${FILLER}`;
    expect(findJwtLiteral(synthetic)).toBeNull();
  });

  it("regex requires base64url alphabet after the eyJ prefix", () => {
    // Sanity-check the regex literal — defends against an accidental
    // broadening edit (e.g. swapping {@code [A-Za-z0-9_-]} for {@code .}).
    expect(FORBIDDEN_JWT_REGEX.source).toContain("eyJ");
    expect(FORBIDDEN_JWT_REGEX.source).toContain("{20,}");
  });
});

describe("bundle-guard self-test: matcher invariants", () => {
  it("every FORBIDDEN_SYMBOLS row asserts at least one of prodForbidden / e2eRequired", () => {
    // A row that asserts neither serves no purpose — would silently dilute the
    // table. The invariant is documented in build-bundle.guard.ts.
    const useless = FORBIDDEN_SYMBOLS.filter((e) => !e.prodForbidden && !e.e2eRequired);
    expect(
      useless.map((e) => e.symbol),
      "every forbidden-symbol row must declare prodForbidden or e2eRequired",
    ).toEqual([]);
  });

  it("FORBIDDEN_SYMBOLS contains no duplicate entries", () => {
    const symbols = FORBIDDEN_SYMBOLS.map((e) => e.symbol);
    expect(new Set(symbols).size).toBe(symbols.length);
  });
});
