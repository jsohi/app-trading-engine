package com.trading.engine.projections;

import java.util.Arrays;
import org.agrona.DirectBuffer;

/**
 * Lightweight byte-array key for use with Agrona's {@link
 * org.agrona.collections.Object2ObjectHashMap}. Provides value-based {@link #equals(Object)} and
 * {@link #hashCode()} over a contiguous byte range.
 *
 * <p>Follows the same pattern as the cluster module's {@code ByteArrayKey} but lives in the
 * projections module to avoid a compile dependency on {@code cluster}.
 *
 * <p><b>Two usage modes:</b>
 *
 * <ol>
 *   <li><b>Immutable map key</b> — created via {@link #copyOf(byte[], int, int)}. The constructor
 *       takes a defensive copy; the key is safe to store in a map and will not be corrupted by
 *       subsequent buffer reuse.
 *   <li><b>Mutable probe key</b> — created via {@link #emptyForLookup(int)}. Pre-allocated once at
 *       construction time; the backing array is mutated in-place via {@link #set(DirectBuffer, int,
 *       int)} for zero-allocation map lookups. <b>MUST NOT</b> be stored as a map key — only used
 *       for transient {@code map.get(probe)} calls.
 * </ol>
 *
 * <p><b>Threading:</b> not thread-safe. Probe keys are mutated on the event-dispatch thread only.
 * Immutable keys (from {@code copyOf}) may be shared across threads once published.
 *
 * <p><b>Allocation:</b> {@code copyOf} allocates (once per map entry). {@code set} and {@code
 * setComposite} on a probe key are zero-allocation.
 */
public final class ByteArrayKey {

  private final byte[] data;
  private int length;
  private int hashCode;

  private ByteArrayKey(final byte[] data, final int length) {
    this.data = data;
    this.length = length;
    this.hashCode = computeHashCode(data, length);
  }

  /**
   * Creates an immutable key by copying {@code length} bytes from {@code src} starting at {@code
   * offset}. Safe to use as a map key.
   *
   * @param src source byte array
   * @param offset start offset in {@code src}
   * @param length number of bytes to copy
   * @return a new ByteArrayKey with a defensive copy of the specified range
   */
  public static ByteArrayKey copyOf(final byte[] src, final int offset, final int length) {
    final byte[] copy = new byte[length];
    System.arraycopy(src, offset, copy, 0, length);
    return new ByteArrayKey(copy, length);
  }

  /**
   * Creates a mutable probe key pre-allocated with the given maximum length. Use {@link
   * #set(DirectBuffer, int, int)} or {@link #set(byte[], int, int)} to populate before lookup.
   *
   * <p><b>Do NOT store this as a map key.</b> The backing array is reused across lookups.
   *
   * @param maxLength the maximum byte length this probe will hold
   * @return a new mutable ByteArrayKey for zero-allocation lookups
   */
  public static ByteArrayKey emptyForLookup(final int maxLength) {
    return new ByteArrayKey(new byte[maxLength], 0);
  }

  /**
   * Mutates this probe key in-place by copying bytes from a {@link DirectBuffer}. Zero allocation.
   *
   * @param buffer source buffer
   * @param offset start offset in the buffer
   * @param length number of bytes to copy (must not exceed backing array length)
   */
  public void set(final DirectBuffer buffer, final int offset, final int length) {
    buffer.getBytes(offset, data, 0, length);
    this.length = length;
    this.hashCode = computeHashCode(data, length);
  }

  /**
   * Mutates this probe key in-place by copying bytes from a byte array. Zero allocation.
   *
   * @param src source byte array
   * @param offset start offset in {@code src}
   * @param length number of bytes to copy (must not exceed backing array length)
   */
  public void set(final byte[] src, final int offset, final int length) {
    System.arraycopy(src, offset, data, 0, length);
    this.length = length;
    this.hashCode = computeHashCode(data, length);
  }

  /**
   * Mutates this probe key in-place by concatenating two byte ranges into a composite key. Used for
   * position keys: {@code accountCode[16] + settlDate[8] = 24 bytes}.
   *
   * @param a first byte range
   * @param aOff offset into {@code a}
   * @param aLen length from {@code a}
   * @param b second byte range
   * @param bOff offset into {@code b}
   * @param bLen length from {@code b}
   */
  public void setComposite(
      final byte[] a,
      final int aOff,
      final int aLen,
      final byte[] b,
      final int bOff,
      final int bLen) {
    System.arraycopy(a, aOff, data, 0, aLen);
    System.arraycopy(b, bOff, data, aLen, bLen);
    this.length = aLen + bLen;
    this.hashCode = computeHashCode(data, this.length);
  }

  /**
   * Checks whether this key's content starts with the given prefix bytes.
   *
   * @param prefix the prefix bytes to check
   * @param offset start offset in {@code prefix}
   * @param prefixLength number of bytes to compare
   * @return {@code true} if this key starts with the specified prefix
   */
  public boolean prefixEquals(final byte[] prefix, final int offset, final int prefixLength) {
    if (prefixLength > length) {
      return false;
    }
    for (int i = 0; i < prefixLength; i++) {
      if (data[i] != prefix[offset + i]) {
        return false;
      }
    }
    return true;
  }

  /**
   * Copies bytes from this key's content into a destination array.
   *
   * @param dst destination byte array
   * @param dstOffset start offset in {@code dst}
   * @param srcOffset start offset within this key's content
   * @param len number of bytes to copy
   */
  public void getBytes(final byte[] dst, final int dstOffset, final int srcOffset, final int len) {
    System.arraycopy(data, srcOffset, dst, dstOffset, len);
  }

  /**
   * Returns the actual occupied length of this key.
   *
   * @return the number of meaningful bytes in this key
   */
  public int length() {
    return length;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof ByteArrayKey other)) {
      return false;
    }
    if (length != other.length || hashCode != other.hashCode) {
      return false;
    }
    return Arrays.equals(data, 0, length, other.data, 0, other.length);
  }

  @Override
  public int hashCode() {
    return hashCode;
  }

  @Override
  public String toString() {
    return new String(data, 0, length, java.nio.charset.StandardCharsets.US_ASCII);
  }

  /**
   * Creates a defensive copy of this key. The returned key is independent — safe for map insertion.
   *
   * @return a new ByteArrayKey with copied content
   */
  public ByteArrayKey copyOf() {
    return copyOf(data, 0, length);
  }

  private static int computeHashCode(final byte[] data, final int length) {
    // FNV-1a hash for good distribution across Agrona's open-addressing tables
    int hash = 0x811c9dc5;
    for (int i = 0; i < length; i++) {
      hash ^= (data[i] & 0xFF);
      hash *= 0x01000193;
    }
    return hash;
  }
}
