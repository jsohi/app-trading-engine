/**
 * ESLint rule: local/no-otel-attribute-outside-allowlist
 *
 * Forbids OTel span-attribute keys not present in
 * `src/shared/telemetry/attributeAllowlist.ts:OTEL_ATTRIBUTE_ALLOWLIST`.
 * Catches both `tracer.startSpan(name, { attributes: { ... } })` and
 * `span.setAttribute('key', ...)` calls.
 *
 * Plan reference: §3 / §6 row 28.
 *
 * Implementation note: stub in C1. The lexical scan landings when
 * actual OTel emission sites are added (C5 / C6).
 */

/** @type {import('eslint').Rule.RuleModule} */
export default {
  meta: {
    type: "problem",
    docs: {
      description:
        "Forbids OTel span-attribute keys not in OTEL_ATTRIBUTE_ALLOWLIST.",
    },
    schema: [],
    messages: {
      unlistedKey:
        "OTel attribute key '{{key}}' not in OTEL_ATTRIBUTE_ALLOWLIST. Add to attributeAllowlist.ts deliberately.",
    },
  },
  create() {
    return {};
  },
};
