package com.trading.engine.messages.util;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import org.agrona.DirectBuffer;

/**
 * Lightweight byte-array wrapper for use as a key in Agrona's {@link
 * org.agrona.collections.Object2ObjectHashMap}. Provides content-based {@link #equals(Object)} and
 * {@link #hashCode()} over a contiguous byte range, enabling zero-allocation map lookups via the
 * reusable probe pattern.
 *
 * <p><b>Two usage modes:</b>
 *
 * <ol>
 *   <li><b>Owned (immutable map key)</b> — created via {@link #owned(byte[], int, int)} or {@link
 *       #copyOf(DirectBuffer, int, int)}. Takes a defensive copy of the source bytes so the key is
 *       independent of any reused buffer. Safe to insert into a map; must not be mutated after
 *       insertion (unless first removed — see {@link #overwrite}).
 *   <li><b>Probe (mutable lookup key)</b> — created via {@link #emptyForLookup(int)} or {@link
 *       #probe(byte[], int, int)}. Wraps or copies into a pre-allocated buffer for zero-allocation
 *       {@code map.get(probe)} lookups. <b>Never insert a probe key into a map</b> — its content
 *       may change, corrupting the map.
 * </ol>
 *
 * <p><b>Hash function.</b> 32-bit FNV-1a — fast, zero-allocation, and well-distributed for the
 * short ASCII keys (ClOrdID, symbols, account codes) used across the trading engine. The hash is
 * computed eagerly on construction or mutation, so repeated probes against the same content are
 * O(1).
 *
 * <p><b>Threading:</b> not thread-safe. Probe keys are mutated on a single agent's duty-cycle
 * thread. Owned keys are effectively immutable after construction and may be safely shared once
 * published, but this class provides no synchronization guarantees.
 *
 * <p><b>Allocation:</b>
 *
 * <ul>
 *   <li>{@link #owned} / {@link #copyOf} — allocates once (defensive copy for map storage)
 *   <li>{@link #probe} — allocates the wrapper object only (no byte copy)
 *   <li>{@link #emptyForLookup} — allocates backing array + wrapper once
 *   <li>{@link #set} / {@link #wrapForProbe} / {@link #overwrite} — zero-allocation when reusing an
 *       existing instance (common case for fixed-width SBE fields)
 * </ul>
 *
 * @see org.agrona.collections.Object2ObjectHashMap
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

  /** Cached FNV-1a hash, computed eagerly on construction or mutation. */
  private int hashCode;

  private ByteArrayKey(final byte[] data, final int offset, final int length) {
    this.data = data;
    this.offset = offset;
    this.length = length;
    this.hashCode = computeHash(data, offset, length);
  }

  // ---------------------------------------------------------------------------
  // Factory methods — owned (immutable map keys)
  // ---------------------------------------------------------------------------

  /**
   * Creates an owned (immutable) key by defensively copying {@code length} bytes from {@code src}
   * starting at {@code offset}. The returned key is independent of the source array and safe for
   * insertion into an Agrona hash map.
   *
   * <p><b>Allocation:</b> allocates a new {@code byte[length]} and a new {@code ByteArrayKey}
   * wrapper.
   *
   * @param src source byte array
   * @param srcOffset start offset in {@code src}
   * @param length number of bytes to copy
   * @return a new owned ByteArrayKey with a defensive copy of the specified range
   */
  public static ByteArrayKey owned(final byte[] src, final int srcOffset, final int length) {
    final byte[] copy = new byte[length];
    if (length > 0) {
      System.arraycopy(src, srcOffset, copy, 0, length);
    }
    return new ByteArrayKey(copy, 0, length);
  }

  /**
   * Creates an owned (immutable) key by defensively copying {@code length} bytes from a byte array.
   * Alias for {@link #owned(byte[], int, int)}.
   *
   * @param src source byte array
   * @param srcOffset start offset in {@code src}
   * @param length number of bytes to copy
   * @return a new owned ByteArrayKey with a defensive copy of the specified range
   */
  public static ByteArrayKey copyOf(final byte[] src, final int srcOffset, final int length) {
    return owned(src, srcOffset, length);
  }

  /**
   * Creates an owned (immutable) key by defensively copying {@code length} bytes from a {@link
   * DirectBuffer}. The returned key is independent of the buffer and safe for insertion into an
   * Agrona hash map.
   *
   * <p><b>Allocation:</b> allocates a new {@code byte[length]} and a new {@code ByteArrayKey}
   * wrapper.
   *
   * @param src source DirectBuffer
   * @param srcOffset start offset in the buffer
   * @param length number of bytes to copy
   * @return a new owned ByteArrayKey with a defensive copy of the specified range
   */
  public static ByteArrayKey copyOf(final DirectBuffer src, final int srcOffset, final int length) {
    final byte[] copy = new byte[length];
    if (length > 0) {
      src.getBytes(srcOffset, copy, 0, length);
    }
    return new ByteArrayKey(copy, 0, length);
  }

  /**
   * Creates an owned key with a backing array of {@code capacity} bytes, copying {@code length}
   * bytes from {@code src}. The extra capacity allows in-place growth via {@link #overwrite}
   * without re-allocation — useful for stores that keep a per-record key alive across upserts.
   *
   * <p><b>Allocation:</b> allocates a new {@code byte[capacity]} and a new {@code ByteArrayKey}
   * wrapper.
   *
   * @param src source byte array
   * @param srcOffset start offset in {@code src}
   * @param length number of bytes to copy (live key length)
   * @param capacity backing array size; must be {@code >= length}
   * @return a new owned ByteArrayKey with extra backing capacity
   * @throws IllegalArgumentException if {@code capacity < length}
   */
  public static ByteArrayKey copyOfWithCapacity(
      final byte[] src, final int srcOffset, final int length, final int capacity) {
    if (capacity < length) {
      throw new IllegalArgumentException("capacity " + capacity + " must be >= length " + length);
    }
    final byte[] backing = new byte[capacity];
    if (length > 0) {
      System.arraycopy(src, srcOffset, backing, 0, length);
    }
    return new ByteArrayKey(backing, 0, length);
  }

  // ---------------------------------------------------------------------------
  // Factory methods — probe (mutable lookup keys)
  // ---------------------------------------------------------------------------

  /**
   * Creates a mutable probe key pre-allocated with a backing array of {@code maxLength} bytes.
   * Mutate via {@link #set(DirectBuffer, int, int)}, {@link #set(byte[], int, int)}, or {@link
   * #wrapForProbe(byte[], int, int)} before each map lookup. The initial length is zero and the key
   * is not valid for lookups until populated.
   *
   * <p><b>Do NOT insert this key into a map.</b> The backing array is reused across lookups.
   *
   * <p><b>Allocation:</b> allocates the backing {@code byte[maxLength]} and the wrapper once.
   *
   * @param maxLength maximum byte length this probe will hold; must be {@code >= 0}
   * @return a new mutable ByteArrayKey for zero-allocation lookups
   * @throws IllegalArgumentException if {@code maxLength} is negative
   */
  public static ByteArrayKey emptyForLookup(final int maxLength) {
    if (maxLength < 0) {
      throw new IllegalArgumentException("maxLength must be >= 0, was " + maxLength);
    }
    return new ByteArrayKey(new byte[maxLength], 0, 0);
  }

  /**
   * Creates a probe key that wraps an external byte range without copying. The caller must ensure
   * that the bytes in {@code src[srcOffset..srcOffset+length)} are not mutated between this call
   * and any subsequent map lookup.
   *
   * <p><b>Do NOT insert this key into a map.</b> The backing array is external and may be reused.
   *
   * <p><b>Allocation:</b> allocates only the {@code ByteArrayKey} wrapper object (no byte copy).
   * For fully zero-allocation lookups, pre-allocate a probe via {@link #emptyForLookup(int)} and
   * reuse it with {@link #wrapForProbe(byte[], int, int)}.
   *
   * @param src source byte array (not copied — caller retains ownership)
   * @param srcOffset start offset in {@code src}
   * @param length number of bytes in the key range
   * @return a new probe ByteArrayKey wrapping the specified range
   */
  public static ByteArrayKey probe(final byte[] src, final int srcOffset, final int length) {
    return new ByteArrayKey(src, srcOffset, length);
  }

  // ---------------------------------------------------------------------------
  // Mutation methods — probe keys (zero-allocation lookups)
  // ---------------------------------------------------------------------------

  /**
   * Mutates this probe key in-place by copying bytes from a {@link DirectBuffer} into the
   * pre-allocated backing array. Zero allocation.
   *
   * <p>The probe key must have been created via {@link #emptyForLookup(int)} with sufficient
   * capacity. After this call, the key is ready for {@code map.get(this)} or {@code
   * map.remove(this)}.
   *
   * @param src source DirectBuffer
   * @param srcOffset start offset in the buffer
   * @param length number of bytes to copy (must not exceed backing array length)
   * @throws IndexOutOfBoundsException if {@code length} exceeds the backing array capacity
   */
  public void set(final DirectBuffer src, final int srcOffset, final int length) {
    if (length > this.data.length) {
      throw new IndexOutOfBoundsException(
          "length " + length + " exceeds probe backing capacity " + this.data.length);
    }
    if (length > 0) {
      src.getBytes(srcOffset, this.data, 0, length);
    }
    this.offset = 0;
    this.length = length;
    this.hashCode = computeHash(this.data, 0, length);
  }

  /**
   * Mutates this probe key in-place by copying bytes from a byte array into the pre-allocated
   * backing array. Zero allocation.
   *
   * @param src source byte array
   * @param srcOffset start offset in {@code src}
   * @param length number of bytes to copy (must not exceed backing array length)
   * @throws IndexOutOfBoundsException if {@code length} exceeds the backing array capacity
   */
  public void set(final byte[] src, final int srcOffset, final int length) {
    if (length > this.data.length) {
      throw new IndexOutOfBoundsException(
          "length " + length + " exceeds probe backing capacity " + this.data.length);
    }
    if (length > 0) {
      System.arraycopy(src, srcOffset, this.data, 0, length);
    }
    this.offset = 0;
    this.length = length;
    this.hashCode = computeHash(this.data, 0, length);
  }

  /**
   * Mutates this probe key in-place to wrap a new byte-array range <em>without copying</em>. Zero
   * allocation.
   *
   * <p>The caller must not mutate the wrapped bytes between this call and the subsequent {@code
   * map.get(this)} or {@code map.remove(this)} call. This method is intended for reusing a single
   * pre-allocated probe across multiple map lookups in the agent duty cycle.
   *
   * <p><b>Difference from {@link #set(byte[], int, int)}:</b> {@code set} copies bytes into the
   * probe's own backing array (safe but slower). {@code wrapForProbe} wraps by reference (faster
   * but caller must guarantee byte stability during lookup).
   *
   * @param src source byte array (not copied — caller retains ownership)
   * @param srcOffset start offset in {@code src}
   * @param length number of bytes in the key range
   */
  public void wrapForProbe(final byte[] src, final int srcOffset, final int length) {
    this.data = src;
    this.offset = srcOffset;
    this.length = length;
    this.hashCode = computeHash(src, srcOffset, length);
  }

  /**
   * Mutates this probe key in-place by copying bytes from a {@link DirectBuffer} into the
   * pre-allocated backing array. Zero allocation.
   *
   * <p>Equivalent to {@link #set(DirectBuffer, int, int)}.
   *
   * @param src source DirectBuffer
   * @param srcOffset start offset in the buffer
   * @param length number of bytes to copy (must not exceed backing array length)
   * @throws IndexOutOfBoundsException if {@code length} exceeds the backing array capacity
   */
  public void wrapForProbe(final DirectBuffer src, final int srcOffset, final int length) {
    set(src, srcOffset, length);
  }

  /**
   * Mutates this probe key in-place by concatenating two byte ranges into a composite key. Used for
   * position keys: {@code accountCode[16] + settlDate[8] = 24 bytes}. Zero allocation.
   *
   * @param a first byte range
   * @param aOff offset into {@code a}
   * @param aLen length from {@code a}
   * @param b second byte range
   * @param bOff offset into {@code b}
   * @param bLen length from {@code b}
   * @throws IndexOutOfBoundsException if {@code aLen + bLen} exceeds the backing array capacity
   */
  public void setComposite(
      final byte[] a,
      final int aOff,
      final int aLen,
      final byte[] b,
      final int bOff,
      final int bLen) {
    // No overflow concern: trading engine keys are short ASCII (max ~24 bytes for composite).
    final int totalLength = aLen + bLen;
    if (totalLength > data.length) {
      throw new IndexOutOfBoundsException(
          "composite length " + totalLength + " exceeds probe backing capacity " + data.length);
    }
    System.arraycopy(a, aOff, data, 0, aLen);
    System.arraycopy(b, bOff, data, aLen, bLen);
    this.offset = 0;
    this.length = aLen + bLen;
    this.hashCode = computeHash(data, 0, this.length);
  }

  // ---------------------------------------------------------------------------
  // Mutation methods — owned keys (pool reuse)
  // ---------------------------------------------------------------------------

  /**
   * Overwrites the content of this owned key with new bytes from a {@link DirectBuffer}. The
   * backing array must be large enough to hold {@code newLength} bytes; if not, this method
   * allocates a new backing array (which should only happen if the key was initially created with a
   * shorter length).
   *
   * <p><b>IMPORTANT:</b> this key must NOT be in any map at the time of this call. Remove it from
   * the map first, then overwrite, then re-insert. Calling this while the key is in a map corrupts
   * the map's hash invariant.
   *
   * <p><b>Allocation:</b> zero allocation when the new length fits in the existing backing array
   * (the common case for fixed-width SBE fields like ClOrdID). Allocates a new {@code byte[]} only
   * if the new length exceeds the backing array capacity.
   *
   * @param src source buffer
   * @param srcOffset start offset in the source buffer
   * @param newLength number of bytes to copy
   */
  public void overwrite(final DirectBuffer src, final int srcOffset, final int newLength) {
    if (newLength > data.length) {
      data = new byte[newLength];
    }
    if (newLength > 0) {
      src.getBytes(srcOffset, data, 0, newLength);
    }
    this.offset = 0;
    this.length = newLength;
    this.hashCode = computeHash(data, 0, newLength);
  }

  /**
   * Overwrites the content of this owned key with new bytes from a byte array. Same semantics as
   * {@link #overwrite(DirectBuffer, int, int)} but for byte-array sources.
   *
   * <p><b>IMPORTANT:</b> this key must NOT be in any map at the time of this call.
   *
   * <p><b>Allocation:</b> zero allocation when {@code newLength <= data.length}.
   *
   * @param src source byte array
   * @param srcOffset start offset in {@code src}
   * @param newLength number of bytes to copy
   */
  public void overwrite(final byte[] src, final int srcOffset, final int newLength) {
    if (newLength > data.length) {
      data = new byte[newLength];
    }
    if (newLength > 0) {
      System.arraycopy(src, srcOffset, data, 0, newLength);
    }
    this.offset = 0;
    this.length = newLength;
    this.hashCode = computeHash(data, 0, newLength);
  }

  // ---------------------------------------------------------------------------
  // Accessors
  // ---------------------------------------------------------------------------

  /**
   * Returns the number of meaningful bytes in this key.
   *
   * @return the key length in bytes
   */
  public int length() {
    return length;
  }

  /**
   * Returns the internal backing array. The caller <b>must not mutate</b> the returned array — this
   * would corrupt the key and any map it is stored in.
   *
   * <p>This accessor is provided for zero-allocation read access to the key content (e.g., passing
   * bytes to a logging callback). For owned keys, {@link #offset()} is always 0.
   *
   * @return the backing byte array (not a copy)
   * @apiNote Callers must not mutate the returned array.
   */
  public byte[] backingArray() {
    return data;
  }

  /**
   * Returns the start offset into the {@link #backingArray()} where key content begins.
   *
   * <p>For owned keys (created via {@link #owned}, {@link #copyOf}, {@link #copyOfWithCapacity}),
   * this is always 0. For probe keys created via {@link #wrapForProbe(byte[], int, int)}, this
   * reflects the wrapped range's offset.
   *
   * @return the offset into the backing array
   */
  public int offset() {
    return offset;
  }

  /**
   * Copies bytes from this key's content into a destination array.
   *
   * @param dst destination byte array
   * @param dstOffset start offset in {@code dst}
   * @param srcOffset start offset within this key's content (relative to {@link #offset()})
   * @param len number of bytes to copy
   * @throws ArrayIndexOutOfBoundsException if the specified ranges exceed the source or destination
   *     array bounds
   */
  public void getBytes(final byte[] dst, final int dstOffset, final int srcOffset, final int len) {
    System.arraycopy(data, this.offset + srcOffset, dst, dstOffset, len);
  }

  /**
   * Checks whether this key's content starts with the given prefix bytes.
   *
   * @param prefix the prefix bytes to check
   * @param prefixOffset start offset in {@code prefix}
   * @param prefixLength number of bytes to compare
   * @return {@code true} if this key starts with the specified prefix
   */
  public boolean prefixEquals(final byte[] prefix, final int prefixOffset, final int prefixLength) {
    if (prefixLength > length) {
      return false;
    }
    return Arrays.mismatch(
            data, offset, offset + prefixLength, prefix, prefixOffset, prefixOffset + prefixLength)
        < 0;
  }

  /**
   * Creates a defensive copy of this key. The returned key is independent — safe for map insertion.
   *
   * @return a new ByteArrayKey with copied content
   */
  public ByteArrayKey copyOf() {
    return copyOf(data, offset, length);
  }

  // ---------------------------------------------------------------------------
  // Object methods
  // ---------------------------------------------------------------------------

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
   * over their respective ranges. Uses {@link Arrays#mismatch} (a JVM intrinsic on modern HotSpot)
   * for fast comparison.
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
    return Arrays.mismatch(
            this.data,
            this.offset,
            this.offset + this.length,
            other.data,
            other.offset,
            other.offset + other.length)
        < 0;
  }

  /**
   * Returns the key bytes as an ASCII string. <b>Allocates a new {@link String}</b> — cold-path /
   * debug use only. Never call on the hot path.
   *
   * @return ASCII string representation of the key bytes
   */
  @Override
  public String toString() {
    return new String(data, offset, length, StandardCharsets.US_ASCII);
  }

  // ---------------------------------------------------------------------------
  // Internal
  // ---------------------------------------------------------------------------

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
