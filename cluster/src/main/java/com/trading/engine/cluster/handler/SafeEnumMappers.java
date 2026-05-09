package com.trading.engine.cluster.handler;

import com.trading.engine.messages.sbe.ProductTypeEnum;
import com.trading.engine.messages.sbe.SettlTypeEnum;
import com.trading.engine.messages.sbe.SideEnum;
import com.trading.engine.messages.sbe.TenorEnum;

/**
 * Stateless, allocation-free helpers for safely encoding raw enum bytes back to typed SBE enums
 * during egress. The slot pool stores raw enum bytes (including {@code NULL_VAL.value()}, which
 * sign-extends to {@code byte -1}) so that malformed inbound enum values cannot throw out of the
 * cluster duty cycle. The SBE-generated {@code XEnum.get(short)} methods, however, only accept the
 * documented enum cases plus the literal NULL_VAL value (255) — they do NOT accept the
 * sign-extended {@code short -1}.
 *
 * <p>Each helper compares the byte against {@code (byte) NULL_VAL.value()} to detect the sentinel
 * and otherwise widens the byte unsigned (mask {@code & 0xFF}) before calling the SBE getter. Any
 * wire value not in the enum's case list still returns NULL_VAL via the underlying enum (which
 * throws IllegalArgumentException only if the case list is incomplete — that is the contract
 * failure mode we explicitly defend against).
 *
 * <p><b>Threading.</b> Pure static helpers — no shared state.
 *
 * <p><b>Allocation.</b> Zero allocation: returns enum singletons.
 */
public final class SafeEnumMappers {

  private SafeEnumMappers() {}

  /** Maps a raw side byte to {@link SideEnum}, returning NULL_VAL for unrecognized values. */
  public static SideEnum safeSide(final byte raw) {
    return raw == (byte) SideEnum.NULL_VAL.value()
        ? SideEnum.NULL_VAL
        : SideEnum.get((short) (raw & 0xFF));
  }

  /** Maps a raw tenor byte to {@link TenorEnum}, returning NULL_VAL for unrecognized values. */
  public static TenorEnum safeTenor(final byte raw) {
    return raw == (byte) TenorEnum.NULL_VAL.value()
        ? TenorEnum.NULL_VAL
        : TenorEnum.get((short) (raw & 0xFF));
  }

  /**
   * Maps a raw settlType byte to {@link SettlTypeEnum}, returning NULL_VAL for unrecognized values.
   */
  public static SettlTypeEnum safeSettlType(final byte raw) {
    return raw == (byte) SettlTypeEnum.NULL_VAL.value()
        ? SettlTypeEnum.NULL_VAL
        : SettlTypeEnum.get((short) (raw & 0xFF));
  }

  /**
   * Maps a raw productType byte to {@link ProductTypeEnum}, returning NULL_VAL for unrecognized
   * values.
   */
  public static ProductTypeEnum safeProductType(final byte raw) {
    return raw == (byte) ProductTypeEnum.NULL_VAL.value()
        ? ProductTypeEnum.NULL_VAL
        : ProductTypeEnum.get((short) (raw & 0xFF));
  }
}
