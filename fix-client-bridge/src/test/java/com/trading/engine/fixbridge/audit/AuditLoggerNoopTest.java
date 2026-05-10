package com.trading.engine.fixbridge.audit;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link AuditLogger.Noop}. Covers the singleton contract, {@link
 * AuditLogger.Noop#isWritable()} semantics, and {@link AuditLogger.Noop#record(long, String,
 * String, String, AuditAction, String, String, String, String, String, String, String, String,
 * String, String, String, String, String)} no-op contract — including null-tolerance for every
 * nullable argument and passing a real {@link AuditAction} value.
 */
class AuditLoggerNoopTest {

  // ===========================================================================
  // Singleton
  // ===========================================================================

  @Test
  void instance_isNotNull() {
    assertNotNull(AuditLogger.Noop.INSTANCE, "AuditLogger.Noop.INSTANCE must not be null");
  }

  @Test
  void instance_isSingleton_sameReferenceEveryAccess() {
    final var first = AuditLogger.Noop.INSTANCE;
    final var second = AuditLogger.Noop.INSTANCE;
    assertSame(first, second, "INSTANCE must return the same object reference on every access");
  }

  @Test
  void instance_implementsAuditLogger() {
    assertTrue(AuditLogger.Noop.INSTANCE instanceof AuditLogger, "Noop must implement AuditLogger");
  }

  // ===========================================================================
  // isWritable
  // ===========================================================================

  @Test
  void isWritable_returnsTrue() {
    assertTrue(
        AuditLogger.Noop.INSTANCE.isWritable(),
        "Noop.isWritable() must return true — always-healthy stub for APP-40a");
  }

  // ===========================================================================
  // record() — no-op contract, all 18 args
  // ===========================================================================

  @Test
  void record_allNullStrings_doesNotThrow() {
    // Verifies null-tolerance for every nullable string parameter.
    AuditLogger.Noop.INSTANCE.record(
        0L, // tsNs
        null, // userId (null for pre-auth)
        null, // jti
        null, // sourceIp
        AuditAction.AUTH_FAIL, // action (non-null per contract)
        null, // symbol
        null, // side
        null, // qtyStr
        null, // priceStr
        null, // ordType
        null, // tif
        null, // account
        null, // clOrdId
        null, // origClOrdId
        null, // quoteId
        null, // result
        null, // failureReason
        null); // traceparent
    // If we get here without exception the no-op contract is satisfied.
  }

  @Test
  void record_allFieldsPopulated_doesNotThrow() {
    AuditLogger.Noop.INSTANCE.record(
        1_700_000_000_000_000_000L,
        "user-123",
        "jti-abc",
        "192.168.1.100",
        AuditAction.NEW_ORDER_RECEIVED,
        "EURUSD",
        "Buy",
        "1.00000000",
        "1.08000000",
        "Limit",
        "GTC",
        "ACCT-1",
        "C-1",
        "C-0",
        "Q-42",
        "ok",
        null,
        "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01");
  }

  @Test
  void record_withAuditActionValue_doesNotThrow() {
    // Verifies AuditAction enum constant passes through the no-op without any exception.
    for (final var action : AuditAction.values()) {
      AuditLogger.Noop.INSTANCE.record(
          System.nanoTime(),
          "user",
          "jti",
          "127.0.0.1",
          action,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          "ok",
          null,
          null);
    }
  }

  @Test
  void record_withFrameOversizedDrop_nullUserId_doesNotThrow() {
    // frame_oversized_drop is a pre-auth action — userId and jti are null per contract.
    AuditLogger.Noop.INSTANCE.record(
        999_999_999L,
        null,
        null,
        "10.0.0.1",
        AuditAction.FRAME_OVERSIZED_DROP,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        "dropped",
        "oversized",
        null);
  }

  @Test
  void record_negativeTimestamp_doesNotThrow() {
    // Negative nanosecond values should not cause any exception in the no-op.
    AuditLogger.Noop.INSTANCE.record(
        Long.MIN_VALUE,
        "user",
        "jti",
        "127.0.0.1",
        AuditAction.AUTH_SUCCESS,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        "ok",
        null,
        null);
  }
}
