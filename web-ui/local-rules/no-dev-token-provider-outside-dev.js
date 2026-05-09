/**
 * ESLint rule: local/no-dev-token-provider-outside-dev
 *
 * Bans `import` of `devTokenProvider` from any file other than:
 *   - the main-thread bootstrap in dev-only branch
 *     (`src/main-thread/devTokenProvider.ts` itself, or `*.dev.ts`)
 *   - tests under `test/**`
 *
 * Defends against the dev-fallback token path being dragged into a
 * production bundle by an accidental import. Belt-and-braces with
 * `vite.config.ts` `define` stripping `VITE_DEV_JWT` in PROD and the
 * `size-limit` regex check on the prod worker bundle.
 *
 * Plan reference: §4.2 / §6 row 3.
 *
 * Implementation note: stub in C1; activated in C7 when
 * `devTokenProvider.ts` lands.
 */

/** @type {import('eslint').Rule.RuleModule} */
export default {
  meta: {
    type: "problem",
    docs: {
      description:
        "Bans imports of devTokenProvider outside *.dev.ts and test/**.",
    },
    schema: [],
    messages: {
      forbidden:
        "devTokenProvider must not be imported outside *.dev.ts / test/** — would risk inclusion in prod bundle.",
    },
  },
  create() {
    return {};
  },
};
