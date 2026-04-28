/**
 * INTENTIONAL ESLint violation fixture for the bigint-coercion rule.
 * `npm run lint:fixtures` is expected to FAIL on this file; the
 * Vitest test in test/lint-fixtures/lint-fixtures.test.ts asserts
 * the failure happens.
 *
 * DO NOT FIX. DO NOT INCLUDE IN `npm run lint`.
 */
const bid: bigint = 108_500_000n;

// Each of these MUST trigger no-restricted-syntax (Number coercion).
// eslint-disable-next-line @typescript-eslint/no-unused-vars
const coerced = Number(bid);
// eslint-disable-next-line @typescript-eslint/no-unused-vars
const literal = Number(123n);

export { coerced, literal };
