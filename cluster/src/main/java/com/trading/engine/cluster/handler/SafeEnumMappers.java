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
 * sign-extended {@code short -1}, and they DO throw {@link IllegalArgumentException} on any value
 * outside the documented case list (e.g. a 99 byte from a malformed or version-skewed input).
 *
 * <p>Each helper:
 *
 * <ol>
 *   <li>Returns {@code NULL_VAL} immediately if the byte equals the sentinel (handles signed
 *       sign-extension to {@code byte -1}).
 *   <li>Validates the unsigned byte against the enum's known wire-value range. Bytes outside the
 *       range return {@code NULL_VAL} instead of letting {@code XEnum.get} throw.
 *   <li>Otherwise masks {@code & 0xFF} and calls the SBE getter — guaranteed not to throw because
 *       the range check above covers every case.
 * </ol>
 *
 * <p><b>Threading.</b> Pure static helpers — no shared state.
 *
 * <p><b>Allocation.</b> Zero allocation: returns enum singletons.
 */
public final class SafeEnumMappers {

  private SafeEnumMappers() {}

  /**
   * Maps a raw side byte to {@link SideEnum}. SideEnum wire values: 1=Buy, 2=Sell, 255=NULL_VAL.
   * Any other value (including the sign-extended {@code byte -1}) returns NULL_VAL.
   */
  public static SideEnum safeSide(final byte raw) {
    if (raw == (byte) SideEnum.NULL_VAL.value()) {
      return SideEnum.NULL_VAL;
    }
    final int u = raw & 0xFF;
    if (u != 1 && u != 2) {
      return SideEnum.NULL_VAL;
    }
    return SideEnum.get((short) u);
  }

  /**
   * Maps a raw tenor byte to {@link TenorEnum}. TenorEnum wire values: 1..14 (ON/TN/SN/W1/...),
   * 255=NULL_VAL. Any other value returns NULL_VAL.
   */
  public static TenorEnum safeTenor(final byte raw) {
    if (raw == (byte) TenorEnum.NULL_VAL.value()) {
      return TenorEnum.NULL_VAL;
    }
    final int u = raw & 0xFF;
    if (u < 1 || u > 14) {
      return TenorEnum.NULL_VAL;
    }
    return TenorEnum.get((short) u);
  }

  /**
   * Maps a raw settlType byte to {@link SettlTypeEnum}. SettlTypeEnum wire values: 0..11
   * (Regular/Cash/NextDay/...), 255=NULL_VAL. Any other value returns NULL_VAL.
   */
  public static SettlTypeEnum safeSettlType(final byte raw) {
    if (raw == (byte) SettlTypeEnum.NULL_VAL.value()) {
      return SettlTypeEnum.NULL_VAL;
    }
    final int u = raw & 0xFF;
    if (u > 11) {
      return SettlTypeEnum.NULL_VAL;
    }
    return SettlTypeEnum.get((short) u);
  }

  /**
   * Maps a raw productType byte to {@link ProductTypeEnum}. ProductTypeEnum wire values: 1=Spot,
   * 2=Forward, 3=Swap, 255=NULL_VAL. Any other value returns NULL_VAL.
   */
  public static ProductTypeEnum safeProductType(final byte raw) {
    if (raw == (byte) ProductTypeEnum.NULL_VAL.value()) {
      return ProductTypeEnum.NULL_VAL;
    }
    final int u = raw & 0xFF;
    if (u < 1 || u > 3) {
      return ProductTypeEnum.NULL_VAL;
    }
    return ProductTypeEnum.get((short) u);
  }
}
