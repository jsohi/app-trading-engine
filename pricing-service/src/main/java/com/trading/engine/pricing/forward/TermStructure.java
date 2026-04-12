package com.trading.engine.pricing.forward;

/**
 * Immutable holder for a single symbol's forward point term structure: a parallel pair of sorted
 * tenor day counts and their corresponding forward point values.
 *
 * <p>Instances are created during cold-path symbol registration in {@link
 * ConfigurableForwardPointSource#registerSymbol(byte[], int[], long[])} and are never mutated after
 * construction. The arrays are owned exclusively by this record — callers pass defensive copies at
 * construction time.
 *
 * <p><b>Threading:</b> effectively immutable after construction. Safe for read access from any
 * single thread without synchronisation (the pricing-service agent's duty cycle).
 *
 * <p><b>Allocation:</b> construction allocates the record object only (the arrays are passed in,
 * not copied here). No allocation on the query path.
 *
 * @param tenorDays sorted ascending array of standard tenor day counts (e.g., {@code {30, 60, 90,
 *     180, 360}}). Must contain at least one element.
 * @param forwardPointsFixed forward point values in fixed-point {@code 10^-8}, parallel to {@code
 *     tenorDays}. May contain positive or negative values depending on the interest rate
 *     differential between base and quote currencies.
 * @see ConfigurableForwardPointSource
 */
record TermStructure(int[] tenorDays, long[] forwardPointsFixed) {}
