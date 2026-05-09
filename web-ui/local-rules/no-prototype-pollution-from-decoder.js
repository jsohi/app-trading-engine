/**
 * ESLint rule: local/no-prototype-pollution-from-decoder
 *
 * Bans patterns that could allow a hostile/buggy server to inject
 * `__proto__` / `constructor` / `prototype` into JS objects:
 *
 *   - `{ ...decoded }` spread of decoder output.
 *   - `Object.assign({}, decoded)`.
 *   - `Reflect.set(...)` with decoder string keys.
 *   - `JSON.parse` anywhere in `src/workers/**`.
 *   - Computed-key writes `obj[k] = v` where `k` originates from a
 *     decoder return value.
 *
 * Maps keyed by server-string data MUST be `Object.create(null)`.
 *
 * Plan reference: §4.8 / §6 row 27.
 *
 * Implementation note: stub in C1. Lexical patterns wired in C6
 * (`MessageRouter`) when decoder output sites exist.
 */

/** @type {import('eslint').Rule.RuleModule} */
export default {
  meta: {
    type: "problem",
    docs: {
      description: "Bans prototype-pollution patterns from SBE decoder output.",
    },
    schema: [],
    messages: {
      spread: "Do not spread decoder output ({...decoded}); use explicit field copies.",
      objectAssign:
        "Do not Object.assign() from decoder output; use explicit field copies.",
      reflectSet: "Reflect.set with untrusted key — banned in src/workers/**.",
      jsonParse: "JSON.parse forbidden in src/workers/** — wire is SBE binary.",
      computedKey:
        "Computed-key write from decoder string — banned. Use Object.create(null) maps.",
    },
  },
  create() {
    return {};
  },
};
