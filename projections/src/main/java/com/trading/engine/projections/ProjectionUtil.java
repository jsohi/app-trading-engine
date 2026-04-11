package com.trading.engine.projections;

import java.nio.charset.StandardCharsets;

/**
 * Shared utility methods for projection implementations. Centralizes non-trivial arithmetic
 * (fixed-point multiply-divide) and SBE string handling to prevent code duplication across {@link
 * com.trading.engine.projections.order.OrderProjection} and {@link
 * com.trading.engine.projections.position.PositionProjection}.
 *
 * <p><b>Threading:</b> all methods are stateless and thread-safe.
 *
 * <p><b>Allocation:</b> {@link #mulDiv(long, long, long)} allocates a {@link java.math.BigInteger}
 * only when the product overflows a signed long (rare, large FX notionals). {@link
 * #asciiString(byte[], int)} allocates a {@link String} (query path only). {@link #sbeStrLen(int,
 * byte[])} is zero-allocation.
 */
public final class ProjectionUtil {

  private ProjectionUtil() {}

  /**
   * Computes {@code (a * b) / divisor} using 128-bit intermediate to avoid overflow. All values
   * must be non-negative. Divisor must be positive.
   *
   * <p>Fast path: if the product {@code a * b} fits in a signed long, uses direct division. Slow
   * path: uses {@link java.math.BigInteger} for exact 128-bit arithmetic. The slow path allocates,
   * but is only hit for large notionals (> ~92 billion units) and is acceptable on the projection
   * event path since it occurs at most once per fill.
   *
   * @param a non-negative multiplicand
   * @param b non-negative multiplier
   * @param divisor positive divisor
   * @return the quotient, or 0 if any input is non-positive
   */
  public static long mulDiv(final long a, final long b, final long divisor) {
    if (a <= 0 || b <= 0 || divisor <= 0) {
      return 0;
    }
    final long hi = Math.multiplyHigh(a, b);
    final long lo = a * b; // lower 64 bits (unsigned wraparound)

    // Fast path: product fits in signed long — no overflow
    if (hi == 0 && lo >= 0) {
      return lo / divisor;
    }

    // Slow path: 128-bit arithmetic via BigInteger.
    // Allocates but only reached for large FX notionals (500M+ units at typical prices).
    final java.math.BigInteger product =
        java.math.BigInteger.valueOf(a).multiply(java.math.BigInteger.valueOf(b));
    return product.divide(java.math.BigInteger.valueOf(divisor)).longValueExact();
  }

  /**
   * Computes the actual string length in an SBE fixed-length char field by scanning backwards for
   * the first non-NUL byte. SBE pads shorter strings with 0x00.
   *
   * <p>Zero allocation.
   *
   * @param fieldLength the declared SBE field length (e.g., 20 for OrderID, 8 for Symbol)
   * @param data the byte array containing the field data
   * @return the number of meaningful (non-NUL) bytes, or 0 if all bytes are NUL
   */
  public static int sbeStrLen(final int fieldLength, final byte[] data) {
    int end = fieldLength;
    while (end > 0 && data[end - 1] == 0) {
      end--;
    }
    return end;
  }

  /**
   * Converts a NUL-padded SBE byte array to a trimmed ASCII string. Trims trailing NUL bytes before
   * conversion.
   *
   * <p>Allocates a {@link String} — suitable for query paths only, not event dispatch.
   *
   * @param data the byte array containing SBE char field data
   * @param length the occupied length (from {@link #sbeStrLen} or decoder)
   * @return the trimmed ASCII string, or empty string if length is 0
   */
  public static String asciiString(final byte[] data, final int length) {
    if (length <= 0) {
      return "";
    }
    int end = length;
    while (end > 0 && data[end - 1] == 0) {
      end--;
    }
    return end == 0 ? "" : new String(data, 0, end, StandardCharsets.US_ASCII);
  }
}
