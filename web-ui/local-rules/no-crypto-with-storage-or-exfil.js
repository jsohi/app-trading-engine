/**
 * ESLint rule: local/no-crypto-with-storage-or-exfil
 *
 * Composition rule: any file that imports / uses `crypto.subtle.*`
 * is forbidden from also importing or using ANY API in §4.3 (storage,
 * concurrency-shared-state, exfil channels, device APIs). Defends
 * against the "encrypt token, then stash" bypass pattern.
 *
 * For APP-36 the `crypto.subtle` allow-list is **empty** — no file in
 * this PR imports `crypto.subtle`. The rule is registered for future
 * authenticated-binary-protocol work; per-file allow-list extensions
 * must be deliberate.
 *
 * Plan reference: §4.3.1 / §6 row 1a.
 *
 * Implementation note: stub in C1; cross-import scan added when the
 * first crypto.subtle usage lands (currently zero usages).
 */

/** @type {import('eslint').Rule.RuleModule} */
export default {
  meta: {
    type: "problem",
    docs: {
      description:
        "Bans co-import of crypto.subtle with banned storage/exfil/device APIs in the same file.",
    },
    schema: [],
    messages: {
      composition:
        "File imports crypto.subtle and a banned storage/exfil API together — defends 'encrypt-then-stash' bypass per APP-36 §4.3.1.",
    },
  },
  create() {
    return {};
  },
};
