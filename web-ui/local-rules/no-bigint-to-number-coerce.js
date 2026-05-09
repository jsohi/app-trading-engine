/**
 * ESLint rule: local/no-bigint-to-number-coerce
 *
 * Bans `Number(<expr>)` where `<expr>` is typed `bigint` inside
 * `src/workers/**` and `src/streams/**`. Forces all int64/uint64 wire
 * arithmetic (seqNo, timestamps, fixed-point prices) through bigint
 * operations; permits the sanctioned `toFixed8` / `nanosToDate`
 * helpers under `src/shared/transport/format/**` only.
 *
 * Plan reference: §2.7 / §4.8 / §6 row 16 / §6 row 53.
 *
 * Implementation note: this is a **stub** in C1 (per §B.6) — the
 * strict bigint-coercion check is currently delegated to the existing
 * `no-restricted-syntax` rule in `eslint.config.js` which catches
 * `Number(<Identifier>)`. The custom rule will be promoted to do
 * type-aware checks (typescript-eslint utils) in a later commit when
 * the worker code lands.
 */

/** @type {import('eslint').Rule.RuleModule} */
export default {
  meta: {
    type: "problem",
    docs: {
      description:
        "Bans Number(<bigint>) in src/workers/** and src/streams/**.",
    },
    schema: [],
    messages: {
      noBigintCoerce:
        "Do not coerce bigint to Number in workers/streams. Use toFixed8() / nanosToDate() in src/shared/transport/format/**.",
    },
  },
  create() {
    // Stub: existing `no-restricted-syntax` in eslint.config.js covers
    // the lexical case. Type-aware enforcement lands when worker code does.
    return {};
  },
};
