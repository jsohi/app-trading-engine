/**
 * Bundle-guard helper module — shared between {@code build-bundle.test.ts}
 * (the real prod-vs-e2e bundle check) and {@code build-bundle.self-test.test.ts}
 * (proves the guard's matcher actually fires on synthetic offending input).
 *
 * <p>Owns three concerns:
 *
 * <ol>
 *   <li>The single canonical {@link FORBIDDEN_SYMBOLS} table — one row per
 *       symbol with explicit {@code prodForbidden} / {@code e2eRequired} /
 *       {@code wordBoundary} flags. Adding a new symbol here automatically
 *       extends both the production assertion AND the self-test loop.</li>
 *   <li>{@link findForbiddenInProd} — given a bundle's text contents, returns
 *       every {@link FORBIDDEN_SYMBOLS} entry whose {@code prodForbidden} flag
 *       is true and whose literal appears in the text. Used by the real test
 *       to assert {@code result.length === 0} on the prod bundle, and by the
 *       self-test to assert it correctly fires on a synthetic bundle.</li>
 *   <li>{@link findMissingInE2e} — inverse: returns every {@code e2eRequired}
 *       entry whose literal is ABSENT from the e2e-mode bundle. Empty result
 *       means the conditional-export mechanism is working (the test-mode
 *       symbols actually ship when {@code VITE_E2E_REAL_BACKEND=true}).</li>
 * </ol>
 *
 * <p><b>Word-boundary matching:</b> some symbols (notably
 * {@code MarketDataFeedStateChange}) are substrings of legitimately-bundled
 * identifiers ({@code MarketDataFeedStateChangeDecoder} is imported by the
 * worker for runtime template-57 decoding). For those rows {@code wordBoundary}
 * is true; the matcher requires a non-identifier character (or string
 * boundary) on each side so the decoder import does not produce a
 * false-positive.
 *
 * <p><b>Threading / allocation:</b> not on any hot path — invoked once per
 * vitest run. No allocation-sensitive code lives here.
 *
 * <p>Plan reference: APP-244 Phase 3 Commit C.9.
 */

/**
 * One forbidden-symbol entry.
 *
 * <p>Invariants:
 *
 * <ul>
 *   <li>{@code prodForbidden} must be true OR {@code e2eRequired} must be true
 *       (a row that asserts neither serves no purpose).</li>
 *   <li>{@code wordBoundary} is reserved for symbols that legitimately appear
 *       as a substring of bundled identifiers (e.g. SBE decoder class names).
 *       Use sparingly — the default substring scan is stricter.</li>
 * </ul>
 */
export interface ForbiddenSymbol {
  /** The literal symbol name as it would appear in source / bundle text. */
  readonly symbol: string;
  /** Asserted absent from the production bundle ({@code VITE_E2E_REAL_BACKEND} unset). */
  readonly prodForbidden: boolean;
  /** Asserted present in the e2e bundle ({@code VITE_E2E_REAL_BACKEND=true}). */
  readonly e2eRequired: boolean;
  /**
   * When true, the matcher requires non-identifier-character boundaries on both
   * sides of the literal. Default false (plain substring scan).
   */
  readonly wordBoundary: boolean;
  /** Human-readable rationale shown in failure messages. */
  readonly rationale: string;
}

/**
 * Canonical forbidden-symbol table. Adding a new row here automatically
 * extends BOTH the real bundle-guard test AND the self-test loop. Keep
 * sorted by category to make the table self-documenting.
 */
export const FORBIDDEN_SYMBOLS: readonly ForbiddenSymbol[] = [
  // ---- Build-time env vars: never embed in any bundle. -----------------
  {
    symbol: "VITE_DEV_JWT",
    prodForbidden: true,
    e2eRequired: false,
    wordBoundary: false,
    rationale: "VITE_DEV_JWT is the dev token literal — must never ship to any browser.",
  },
  // VITE_E2E_REAL_BACKEND is the env-var name Vite uses to gate the e2e
  // branch — but Vite REPLACES the {@code import.meta.env.VITE_E2E_REAL_BACKEND}
  // expression with the inlined value ({@code "true"} or {@code undefined})
  // at build time, so the literal env-var name itself does NOT survive into
  // either bundle in the general case. We assert prodForbidden (catches a
  // catastrophic regression where the var name DID leak — e.g. through a
  // {@code JSON.stringify(import.meta.env)} blunder) but not e2eRequired.
  {
    symbol: "VITE_E2E_REAL_BACKEND",
    prodForbidden: true,
    e2eRequired: false,
    wordBoundary: false,
    rationale:
      "VITE_E2E_REAL_BACKEND env-var name — Vite inlines the value; the literal name leaking signals a JSON.stringify(import.meta.env) blunder.",
  },

  // ---- E2EHooks recorder symbols (see web-ui/src/main-thread/e2eHooks.ts). -
  // Symbols that are written as STRING LITERALS in source code that ships in
  // the e2e branch are e2eRequired. Symbols that are only declared as
  // {@code declare global var X} (and assigned by Playwright via
  // {@code page.evaluate}) never appear as bundle literals, so e2eRequired
  // is false — but they remain prodForbidden because if they DID leak into
  // prod that would prove a test-mode code path slipped through DCE.
  {
    symbol: "__e2eHooks",
    prodForbidden: true,
    e2eRequired: true,
    wordBoundary: false,
    rationale: "E2EHooks ready-marker — registered on globalThis in installEarlyHooks.",
  },
  {
    symbol: "__forceWsClose",
    prodForbidden: true,
    e2eRequired: true,
    wordBoundary: false,
    rationale: "Spec 07 reconnect hook — registered on globalThis in installEarlyHooks.",
  },
  {
    symbol: "__ordersGridApi",
    prodForbidden: true,
    e2eRequired: true,
    wordBoundary: false,
    rationale: "Spec 05 AG Grid api hook — registered by OrderBlotter in e2e mode.",
  },
  {
    symbol: "__cellFlashes",
    prodForbidden: true,
    e2eRequired: false,
    wordBoundary: false,
    rationale:
      "Spec 05 flash recorder — written by Playwright page.evaluate, never as source literal.",
  },
  {
    symbol: "__E2E_JWT_OVERRIDE__",
    prodForbidden: true,
    e2eRequired: false,
    wordBoundary: false,
    rationale:
      "Spec 08 per-context JWT override — read via devTokenProvider (lazy-loaded; may DCE out).",
  },
  {
    symbol: "__connStates",
    prodForbidden: true,
    e2eRequired: false,
    wordBoundary: false,
    rationale:
      "Spec 07 connectionState recorder — written by Playwright page.evaluate, never as source literal.",
  },
  {
    symbol: "__connStatesUnsub",
    prodForbidden: true,
    e2eRequired: false,
    wordBoundary: false,
    rationale:
      "Spec 07 recorder teardown — written by Playwright page.evaluate, never as source literal.",
  },

  // ---- Phase 3 Commit 9 / spec 09 feed-stale (E2EHooks#feedState$). ------
  {
    symbol: "feedState$",
    prodForbidden: true,
    e2eRequired: true,
    wordBoundary: false,
    rationale: "Spec 09 feed-state RxJS Observable — exposed via E2EHooks in e2e mode.",
  },

  // ---- SBE decoder constants — must only appear inside generated codec. ---
  // wordBoundary=true is critical: the worker imports
  // {@code MarketDataFeedStateChangeDecoder} for runtime template-57 decoding,
  // which is a legitimate substring overlap. The boundary check ensures only
  // the EXACT constant (e.g. as a re-exported top-level symbol) trips the
  // guard, not the decoder import.
  {
    symbol: "MarketDataFeedStateChange",
    prodForbidden: true,
    e2eRequired: false,
    wordBoundary: true,
    rationale:
      "Top-level re-export of MarketDataFeedStateChange would leak the SBE constant — only the *Decoder import is allowed.",
  },
];

/** Regex that catches any JWT-shaped literal embedded in the bundle. */
export const FORBIDDEN_JWT_REGEX = /eyJ[A-Za-z0-9_-]{20,}/;

/**
 * Escape a literal string for safe injection into a {@link RegExp}.
 *
 * @param s the literal to escape
 * @returns the escaped pattern, suitable for {@code new RegExp(escaped)}
 */
function escapeRegExp(s: string): string {
  return s.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

/**
 * Build the per-symbol matcher: substring-includes by default, word-boundary
 * regex when {@code wordBoundary} is set. The boundary pattern treats any
 * non-identifier character (and string start/end) as a boundary so the
 * matcher works on minified bundles where identifier neighbours vary.
 *
 * @param entry the {@link ForbiddenSymbol} row to compile
 * @returns a predicate that returns true when {@code text} contains the symbol
 */
export function matcherFor(entry: ForbiddenSymbol): (text: string) => boolean {
  if (!entry.wordBoundary) {
    return (text: string) => text.includes(entry.symbol);
  }
  // (?:^|[^A-Za-z0-9_$]) — left boundary or start
  // <escaped symbol>
  // (?:$|[^A-Za-z0-9_$]) — right boundary or end
  const pattern = new RegExp(
    `(?:^|[^A-Za-z0-9_$])${escapeRegExp(entry.symbol)}(?:$|[^A-Za-z0-9_$])`,
  );
  return (text: string) => pattern.test(text);
}

/**
 * One offender finding (symbol that appeared / was missing where it should
 * not have been).
 */
export interface Offender {
  readonly symbol: string;
  readonly rationale: string;
}

/**
 * Scan a single bundle file's text for every {@code prodForbidden} entry that
 * appears. Used by the prod-bundle assertion (expect 0 offenders) and by the
 * self-test (expect 1 offender for a synthetic bundle containing the symbol).
 *
 * @param contents the bundle file text
 * @returns array of offenders (empty when clean)
 */
export function findForbiddenInProd(contents: string): Offender[] {
  const out: Offender[] = [];
  for (const entry of FORBIDDEN_SYMBOLS) {
    if (!entry.prodForbidden) continue;
    if (matcherFor(entry)(contents)) {
      out.push({ symbol: entry.symbol, rationale: entry.rationale });
    }
  }
  return out;
}

/**
 * Scan the concatenation of every e2e-mode bundle file for symbols that MUST
 * appear (their presence proves the conditional-export mechanism works).
 * Empty result means every {@code e2eRequired} symbol shipped — the mirror of
 * the prod assertion.
 *
 * @param concatenatedContents text of every emitted .js file in the e2e bundle, joined
 * @returns array of missing entries (empty when all required symbols are present)
 */
export function findMissingInE2e(concatenatedContents: string): Offender[] {
  const out: Offender[] = [];
  for (const entry of FORBIDDEN_SYMBOLS) {
    if (!entry.e2eRequired) continue;
    if (!matcherFor(entry)(concatenatedContents)) {
      out.push({ symbol: entry.symbol, rationale: entry.rationale });
    }
  }
  return out;
}

/**
 * JWT-literal scan — separate from {@link findForbiddenInProd} because the
 * matcher is a regex rather than a {@link FORBIDDEN_SYMBOLS} row. Returns the
 * first match or null.
 *
 * @param contents the bundle file text
 * @returns the matched JWT-shaped substring, or null when clean
 */
export function findJwtLiteral(contents: string): string | null {
  const m = FORBIDDEN_JWT_REGEX.exec(contents);
  return m === null ? null : m[0];
}
