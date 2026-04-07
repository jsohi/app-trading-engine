package com.trading.engine.cluster.refdata;

/**
 * Shared helpers used across the refdata stores and loaders. Package-private by design —
 * these are internal utilities, not a public API.
 */
final class RefDataUtils {

  private RefDataUtils() {}

  /**
   * Trim trailing zero-padding from the first {@code upToLength} bytes of a fixed-length char
   * field (SBE {@code char[N]} types are zero-padded on the wire). Returns the live length.
   *
   * @param bytes the buffer holding the padded field
   * @param upToLength the fixed field width to scan
   * @return the length of the live prefix (bytes before the trailing zeros)
   */
  static int trimTrailingZeros(final byte[] bytes, final int upToLength) {
    int len = upToLength;
    while (len > 0 && bytes[len - 1] == 0) {
      len--;
    }
    return len;
  }
}
