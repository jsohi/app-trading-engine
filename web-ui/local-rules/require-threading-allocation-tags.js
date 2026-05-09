/**
 * ESLint rule: local/require-threading-allocation-tags
 *
 * Requires every exported class / function / interface in the
 * documented globs (`src/workers/**`, `src/streams/**`,
 * `src/main-thread/**`, `src/shared/**`) to carry a TSDoc block with
 * `Threading:` and `Allocation:` annotations — the TS analog of
 * CLAUDE.md's class-level Javadoc requirement for production trading
 * code.
 *
 * Plan reference: §4.8 / §6 row 52.
 *
 * Implementation note: stub in C1. Active scan deferred until the
 * majority of exports are written (C5/C6/C7); promoting to error on
 * a half-implemented file set would create churn. Code authored under
 * APP-36 already includes the tags by hand convention; later commits
 * verify via this rule.
 */

/** @type {import('eslint').Rule.RuleModule} */
export default {
  meta: {
    type: "suggestion",
    docs: {
      description:
        "Requires TSDoc Threading: / Allocation: tags on exported declarations in selected globs.",
    },
    schema: [],
    messages: {
      missingThreading:
        "Exported {{kind}} '{{name}}' missing 'Threading:' tag in TSDoc.",
      missingAllocation:
        "Exported {{kind}} '{{name}}' missing 'Allocation:' tag in TSDoc.",
    },
  },
  create() {
    return {};
  },
};
