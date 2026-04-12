package com.trading.engine.pricing;

import java.util.Arrays;
import org.agrona.DirectBuffer;

// TODO(APP-207): extract ByteArrayKey to shared module — duplicated in cluster, projections,
//                pricing-service

/**
 * Lightweight byte-array wrapper for use as a key in Agrona's {@link
 * org.agrona.collections.Object2ObjectHashMap}. Provides content-based {@link #equals(Object)} and
 * {@link #hashCode()} over a contiguous byte range, enabling zero-allocation map lookups via the
 * reusable probe pattern.
 *
 * <p><b>Two usage modes:</b>
 *
 * <ol>
 *   <li><b>Owned (immutable map key)</b> — created via {@link #owned(byte[], int, int)}. Takes a
 *       defensive copy of the source bytes so the key is independent of any reused buffer. Safe to
 *       insert into a map; must not be mutated after insertion.
 *   <li><b>Probe (mutable lookup key)</b> — created via {@link #probe(byte[], int, int)} or
 *       pre-allocated and mutated via {@link #wrapForProbe(byte[], int, int)} / {@link
 *       #wrapForProbe(DirectBuffer, int, int)}. Wraps an external byte range <em>without</em>
 *       copying, enabling zero-allocation {@code map.get(probe)} lookups. The caller must not
 *       mutate the wrapped bytes between the {@code wrapForProbe} call and the map lookup. <b>Never
 *       insert a probe key into a map</b> — its content may change, corrupting the map.
 * </ol>
 *
 * <p><b>Hash function.</b> 32-bit FNV-1a — fast, zero-allocation, and well-distributed for the
 * short ASCII keys (symbols, account codes) used in the pricing service's lookup tables. The hash
 * is computed eagerly on construction or wrap, so repeated probes against the same content are
 * O(1).
 *
 * <p><b>Threading:</b> not thread-safe. Probe keys are mutated on the pricing-service agent's
 * single-threaded duty cycle. Owned keys are effectively immutable after construction and may be
 * safely shared once published, but this class provides no synchronization guarantees.
 *
 * <p><b>Allocation:</b> {@link #owned(byte[], int, int)} allocates once (defensive copy for map
 * storage). {@link #probe(byte[], int, int)} allocates the wrapper object only (no byte copy).
 * {@link #wrapForProbe(byte[], int, int)} and {@link #wrapForProbe(DirectBuffer, int, int)} are
 * zero-allocation when reusing an existing probe instance.
 *
 * @see com.trading.engine.cluster.refdata.ByteArrayKey
 * @see com.trading.engine.projections.ByteArrayKey
 */
public final class ByteArrayKey {

  /** FNV-1a 32-bit offset basis. */
  private static final int FNV_OFFSET_BASIS = 0x811C9DC5;

  /** FNV-1a 32-bit prime. */
  private static final int FNV_PRIME = 0x01000193;

  /** Backing byte data — either a defensive copy (owned) or an external reference (probe). */
  private byte[] data;

  /** Offset into {@link #data} where the key content starts. Always 0 for owned keys. */
  private int offset;

  /** Number of meaningful bytes in the key, starting from {@link #offset}. */
  private int length;

  /** Cached FNV-1a hash, computed eagerly on construction or wrap. */
  private int hashCode;

  private ByteArrayKey(final byte[] data, final int offset, final int length) {
    this.data = data;
    this.offset = offset;
    this.length = length;
    this.hashCode = computeHash(data, offset, length);
  }

  /**
   * Creates an owned (immutable) key by defensively copying {@code length} bytes from {@code src}
   * starting at {@code offset}. The returned key is independent of the source array and safe for
   * insertion into an Agrona hash map.
   *
   * <p><b>Allocation:</b> allocates a new {@code byte[length]} and a new {@code ByteArrayKey}
   * wrapper.
   *
   * @param src source byte array
   * @param offset start offset in {@code src}
   * @param length number of bytes to copy
   * @return a new owned ByteArrayKey with a defensive copy of the specified range
   */
  public static ByteArrayKey owned(final byte[] src, final int offset, final int length) {
    final byte[] copy = new byte[length];
    System.arraycopy(src, offset, copy, 0, length);
    return new ByteArrayKey(copy, 0, length);
  }

  /**
   * Creates an owned (immutable) key by defensively copying {@code length} bytes from a {@link
   * DirectBuffer}. The returned key is independent of the buffer and safe for insertion into an
   * Agrona hash map.
   *
   * <p>Alias matching the cluster/projections {@code copyOf} convention.
   *
   * <p><b>Allocation:</b> allocates a new {@code byte[length]} and a new {@code ByteArrayKey}
   * wrapper.
   *
   * @param src source DirectBuffer
   * @param offset start offset in the buffer
   * @param length number of bytes to copy
   * @return a new owned ByteArrayKey with a defensive copy of the specified range
   */
  public static ByteArrayKey copyOf(final DirectBuffer src, final int offset, final int length) {
    final byte[] copy = new byte[length];
    if (length > 0) {
      src.getBytes(offset, copy, 0, length);
    }
    return new ByteArrayKey(copy, 0, length);
  }

  /**
   * Creates an owned (immutable) key by defensively copying {@code length} bytes from a byte array.
   * Alias for {@link #owned(byte[], int, int)}, matching the cluster/projections {@code copyOf}
   * convention.
   *
   * <p><b>Allocation:</b> allocates a new {@code byte[length]} and a new {@code ByteArrayKey}
   * wrapper.
   *
   * @param src source byte array
   * @param offset start offset in {@code src}
   * @param length number of bytes to copy
   * @return a new owned ByteArrayKey with a defensive copy of the specified range
   */
  public static ByteArrayKey copyOf(final byte[] src, final int offset, final int length) {
    return owned(src, offset, length);
  }

  /**
   * Overwrites the content of this owned key with new bytes from a {@link DirectBuffer}. The
   * backing array must be large enough to hold {@code length} bytes; if not, this method allocates
   * a new backing array (which should only happen if the key was initially created with a shorter
   * length).
   *
   * <p><b>IMPORTANT:</b> this key must NOT be in any map at the time of this call. Remove it from
   * the map first, then overwrite, then re-insert. Calling this while the key is in a map corrupts
   * the map's hash invariant.
   *
   * <p><b>Allocation:</b> zero allocation when the new length fits in the existing backing array
   * (the common case for fixed-width SBE fields like QuoteReqID). Allocates a new {@code byte[]}
   * only if the new length exceeds the backing array capacity.
   *
   * @param src source buffer
   * @param srcOffset start offset in the source buffer
   * @param newLength number of bytes to copy
   */
  public void overwrite(final DirectBuffer src, final int srcOffset, final int newLength) {
    if (newLength > data.length) {
      data = new byte[newLength];
    }
    src.getBytes(srcOffset, data, 0, newLength);
    this.offset = 0;
    this.length = newLength;
    this.hashCode = computeHash(data, 0, newLength);
  }

  /**
   * Creates a mutable probe key pre-allocated with a backing array of {@code maxLength} bytes.
   * Mutate via {@link #set(DirectBuffer, int, int)} or {@link #wrapForProbe(byte[], int, int)}
   * before each map lookup. The initial length is zero and the key is not valid for lookups until
   * populated.
   *
   * <p><b>Do NOT insert this key into a map.</b> The backing array is reused across lookups.
   *
   * <p>Matches the cluster/projections {@code emptyForLookup} factory convention.
   *
   * <p><b>Allocation:</b> allocates the backing {@code byte[maxLength]} and the wrapper once.
   *
   * @param maxLength maximum byte length this probe will hold
   * @return a new mutable ByteArrayKey for zero-allocation lookups
   */
  public static ByteArrayKey emptyForLookup(final int maxLength) {
    return new ByteArrayKey(new byte[maxLength], 0, 0);
  }

  /**
   * Creates a probe key that wraps an external byte range without copying. The caller must ensure
   * that the bytes in {@code src[offset..offset+length)} are not mutated between this call and any
   * subsequent map lookup.
   *
   * <p><b>Do NOT insert this key into a map.</b> The backing array is external and may be reused.
   *
   * <p><b>Allocation:</b> allocates only the {@code ByteArrayKey} wrapper object (no byte copy).
   * For fully zero-allocation lookups, pre-allocate a probe via {@code probe(new byte[maxLen], 0,
   * 0)} and reuse it with {@link #wrapForProbe(byte[], int, int)}.
   *
   * @param src source byte array (not copied — caller retains ownership)
   * @param offset start offset in {@code src}
   * @param length number of bytes in the key range
   * @return a new probe ByteArrayKey wrapping the specified range
   */
  public static ByteArrayKey probe(final byte[] src, final int offset, final int length) {
    return new ByteArrayKey(src, offset, length);
  }

  /**
   * Mutates this probe key in-place by copying bytes from a {@link DirectBuffer} into the
   * pre-allocated backing array. Zero allocation. Matches the cluster/projections {@code set}
   * convention.
   *
   * <p>Equivalent to {@link #wrapForProbe(DirectBuffer, int, int)} — provided for API compatibility
   * with the cluster and projections modules' {@code ByteArrayKey.set()} idiom.
   *
   * @param src source DirectBuffer
   * @param offset start offset in the buffer
   * @param length number of bytes to copy (must not exceed backing array length)
   * @throws IndexOutOfBoundsException if {@code length} exceeds the backing array capacity
   */
  public void set(final DirectBuffer src, final int offset, final int length) {
    wrapForProbe(src, offset, length);
  }

  /**
   * Mutates this probe key in-place to wrap a new byte-array range. Zero allocation.
   *
   * <p>This method is intended for reusing a single pre-allocated probe across multiple map lookups
   * in the agent duty cycle. The caller must not mutate the wrapped bytes between this call and the
   * subsequent {@code map.get(this)} call.
   *
   * @param src source byte array (not copied — caller retains ownership)
   * @param offset start offset in {@code src}
   * @param length number of bytes in the key range
   */
  public void wrapForProbe(final byte[] src, final int offset, final int length) {
    this.data = src;
    this.offset = offset;
    this.length = length;
    this.hashCode = computeHash(src, offset, length);
  }

  /**
   * Mutates this probe key in-place to wrap a range from a {@link DirectBuffer}. Copies the bytes
   * into the probe's existing backing array to avoid holding a reference to the DirectBuffer's
   * underlying storage (which may be off-heap or recycled).
   *
   * <p><b>Allocation:</b> zero allocation — bytes are copied into the pre-existing backing array.
   * The backing array must have been allocated with sufficient capacity (e.g., via {@code probe(new
   * byte[maxLen], 0, 0)}).
   *
   * @param src source DirectBuffer
   * @param offset start offset in the buffer
   * @param length number of bytes to copy (must not exceed backing array length)
   * @throws IndexOutOfBoundsException if {@code length} exceeds the backing array capacity
   */
  public void wrapForProbe(final DirectBuffer src, final int offset, final int length) {
    if (length > this.data.length) {
      throw new IndexOutOfBoundsException(
          "length " + length + " exceeds probe backing capacity " + this.data.length);
    }
    src.getBytes(offset, this.data, 0, length);
    this.offset = 0;
    this.length = length;
    this.hashCode = computeHash(this.data, 0, length);
  }

  /**
   * Returns the number of meaningful bytes in this key.
   *
   * @return the key length in bytes
   */
  public int length() {
    return length;
  }

  /**
   * Returns the cached FNV-1a hash code computed over the key's byte range.
   *
   * @return the 32-bit hash code
   */
  @Override
  public int hashCode() {
    return hashCode;
  }

  /**
   * Content-based equality: two keys are equal if they have the same length and identical bytes
   * over their respective ranges.
   *
   * @param o the object to compare
   * @return {@code true} if {@code o} is a {@code ByteArrayKey} with identical content
   */
  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof ByteArrayKey other)) {
      return false;
    }
    if (this.length != other.length || this.hashCode != other.hashCode) {
      return false;
    }
    return Arrays.equals(
        this.data,
        this.offset,
        this.offset + this.length,
        other.data,
        other.offset,
        other.offset + other.length);
  }

  /**
   * Returns the key bytes as an ASCII string. <b>Allocates a new {@link String}</b> — cold-path /
   * debug use only. Never call on the hot path.
   */
  @Override
  public String toString() {
    return new String(data, offset, length, java.nio.charset.StandardCharsets.US_ASCII);
  }

  /**
   * Computes the FNV-1a 32-bit hash over the byte range {@code data[offset..offset+length)}.
   *
   * <p>FNV-1a provides good avalanche properties for short ASCII keys and is widely used in
   * Agrona-based open-addressing hash maps for its speed and distribution quality.
   *
   * @param data byte array
   * @param offset start offset
   * @param length number of bytes to hash
   * @return 32-bit FNV-1a hash
   */
  private static int computeHash(final byte[] data, final int offset, final int length) {
    int hash = FNV_OFFSET_BASIS;
    final int end = offset + length;
    for (int i = offset; i < end; i++) {
      hash ^= (data[i] & 0xFF);
      hash *= FNV_PRIME;
    }
    return hash;
  }
}
