package com.trading.engine.cluster.refdata;

/**
 * Utility helpers for comparing fixed-length account-identifier byte buffers (SBE {@code Account}
 * char[16] type). Used by APP-62 §H 4-eyes ingress validation in {@link LoadRiskLimitHandler} /
 * {@link LoadRiskLimitBatchHandler}, and by {@link RiskLimitState} for its own emptiness checks.
 *
 * <p>Threading: stateless, thread-safe.
 *
 * <p>Allocation: zero-allocation — no objects created, no streams, no varargs.
 *
 * <p>Security model: APP-62 §H runs at admin-load frequency on a trusted ingress path; constant-
 * time comparison is NOT required and not implemented. Short-circuit on first mismatch is
 * acceptable because the inputs are operator-supplied identifiers, not cryptographic secrets.
 */
final class AccountIdentifierBytes {

  /**
   * Wire length of the SBE {@code Account} char[16] type — every proposerId / approverId byte
   * buffer in APP-62 §H validation is exactly this many bytes. Hoisted here to remove duplicate
   * {@code ACCOUNT_ID_BYTE_LEN = 16} constants previously declared in {@link RiskLimitState},
   * {@link RiskLimitStore}, {@link LoadRiskLimitHandler}, and {@link LoadRiskLimitBatchHandler}.
   */
  static final int LENGTH = 16;

  private AccountIdentifierBytes() {}

  /** Returns {@code true} when every byte of {@code buf} is zero (empty / unpopulated buffer). */
  static boolean isAllZero(final byte[] buf) {
    for (int i = 0; i < buf.length; i++) {
      if (buf[i] != 0) {
        return false;
      }
    }
    return true;
  }

  /**
   * Returns {@code true} when {@code a} and {@code b} have identical length AND every byte is
   * equal. The length precondition guards against a future caller passing buffers of different
   * sizes (today both APP-62 §H call sites are the same fixed 16-byte scratch).
   */
  static boolean byteEquals(final byte[] a, final byte[] b) {
    if (a.length != b.length) {
      return false;
    }
    for (int i = 0; i < a.length; i++) {
      if (a[i] != b[i]) {
        return false;
      }
    }
    return true;
  }
}
