package com.trading.engine.cluster.refdata;

import org.agrona.DirectBuffer;

/**
 * Heap-resident wrapper around a {@code byte[]} with content-based {@link #hashCode()} and {@link
 * #equals(Object)}. Designed to be used as a hash-map key in stores like {@link AccountStore}'s
 * code secondary index, where the natural key is a slice of an SBE message field (a fixed-length
 * char array such as {@code Account char[16]}).
 *
 * <p><b>Why this exists.</b> Agrona's {@link org.agrona.concurrent.UnsafeBuffer UnsafeBuffer}
 * <i>does</i> implement content-based hashing, but using it directly as a map key is unsafe in the
 * cluster duty cycle because UnsafeBuffer holds a reference to the source byte[] — it doesn't own
 * its bytes. When the cluster recycles its per-command scratch buffers, the inserted "key" silently
 * changes content and corrupts the map. {@code ByteArrayKey} solves this by taking a <b>defensive
 * copy</b> of the bytes on insert, so the map key is independent of any source buffer's lifecycle.
 * The cached {@link #hashCode()} (FNV-1a) is a secondary benefit — repeated lookups don't re-hash.
 *
 * <p><b>Two usage modes.</b>
 *
 * <ol>
 *   <li><b>Inserted key.</b> Construct via {@link #ByteArrayKey(byte[])} or {@link #copyOf(byte[],
 *       int, int)} or {@link #copyOf(DirectBuffer, int, int)}. The constructor takes a defensive
 *       copy so the key is independent of any reused source buffer. Insert this instance into a
 *       map; never mutate it after insertion.
 *   <li><b>Reusable lookup probe.</b> Construct one zero-length instance via {@link
 *       #emptyForLookup(int)} (with the maximum byte length the store will ever look up). Mutate it
 *       in place via {@link #set(DirectBuffer, int, int)} and use it as the argument to {@code
 *       map.get(...)} / {@code map.containsKey(...)}. <b>Never insert this instance into the
 *       map</b> — its bytes change between lookups, which would corrupt the map.
 * </ol>
 *
 * <p><b>Hash function.</b> 32-bit FNV-1a — fast, allocation-free, well-distributed for short ASCII
 * keys, used by similar data structures in the JVM ecosystem. Cached after the first call so
 * repeated probes are O(1).
 */
public final class ByteArrayKey {

  private static final int FNV_OFFSET_BASIS = 0x811C9DC5;
  private static final int FNV_PRIME = 0x01000193;

  private final byte[] data;
  private int length;
  private int hash;
  private boolean hashValid;

  /**
   * Construct a stable, defensively-copied key from the given byte array. The full {@code
   * data.length} is treated as the key. Suitable for insertion into a hash map.
   */
  public ByteArrayKey(final byte[] data) {
    if (data == null) {
      throw new NullPointerException("data must not be null");
    }
    this.data = new byte[data.length];
    System.arraycopy(data, 0, this.data, 0, data.length);
    this.length = data.length;
    this.hashValid = false;
  }

  private ByteArrayKey(final byte[] data, final int length) {
    this.data = data;
    this.length = length;
    this.hashValid = false;
  }

  /** Defensive copy from a byte[] slice. The returned key is independent of the source. */
  public static ByteArrayKey copyOf(final byte[] src, final int offset, final int length) {
    if (src == null) {
      throw new NullPointerException("src must not be null");
    }
    if (offset < 0 || length < 0 || offset + length > src.length) {
      throw new IndexOutOfBoundsException(
          "offset=" + offset + " length=" + length + " src.length=" + src.length);
    }
    final byte[] copy = new byte[length];
    System.arraycopy(src, offset, copy, 0, length);
    return new ByteArrayKey(copy, length);
  }

  /**
   * Defensive copy with a backing capacity larger than the live length. Useful for stores that keep
   * a per-record key alive across upserts and may need to grow the live bytes (e.g., {@link
   * AccountStore}'s sidecar map allocates each key with the max account-code capacity so a re-load
   * with a longer code can mutate the key in place via {@link #set(byte[], int, int)} without
   * re-allocating).
   *
   * @param capacity backing array size; must be {@code >= length}
   */
  public static ByteArrayKey copyOfWithCapacity(
      final byte[] src, final int offset, final int length, final int capacity) {
    if (src == null) {
      throw new NullPointerException("src must not be null");
    }
    if (offset < 0 || length < 0 || offset + length > src.length) {
      throw new IndexOutOfBoundsException(
          "offset=" + offset + " length=" + length + " src.length=" + src.length);
    }
    if (capacity < length) {
      throw new IllegalArgumentException("capacity " + capacity + " must be >= length " + length);
    }
    final byte[] backing = new byte[capacity];
    if (length > 0) {
      System.arraycopy(src, offset, backing, 0, length);
    }
    return new ByteArrayKey(backing, length);
  }

  /** Defensive copy from a {@link DirectBuffer} slice. */
  public static ByteArrayKey copyOf(final DirectBuffer src, final int offset, final int length) {
    if (src == null) {
      throw new NullPointerException("src must not be null");
    }
    if (length < 0) {
      throw new IndexOutOfBoundsException("length=" + length);
    }
    final byte[] copy = new byte[length];
    if (length > 0) {
      src.getBytes(offset, copy, 0, length);
    }
    return new ByteArrayKey(copy, length);
  }

  /**
   * Allocate a reusable lookup-probe key with a fixed backing buffer of {@code maxLength} bytes.
   * Mutate via {@link #set(DirectBuffer, int, int)} before each {@code map.get()}. Never insert
   * this instance into a map.
   */
  public static ByteArrayKey emptyForLookup(final int maxLength) {
    if (maxLength < 0) {
      throw new IllegalArgumentException("maxLength must be >= 0, was " + maxLength);
    }
    return new ByteArrayKey(new byte[maxLength], 0);
  }

  /**
   * Reset this key (a lookup probe) to wrap the given source slice. Copies bytes into the
   * pre-allocated backing array; invalidates the cached hash. Call only on a probe key, never on a
   * key that has been inserted into a map.
   *
   * @throws IndexOutOfBoundsException if {@code length} exceeds this key's backing capacity
   */
  public void set(final DirectBuffer src, final int offset, final int length) {
    if (length > data.length) {
      throw new IndexOutOfBoundsException(
          "length " + length + " exceeds probe capacity " + data.length);
    }
    if (length > 0) {
      src.getBytes(offset, data, 0, length);
    }
    this.length = length;
    this.hashValid = false;
  }

  /** Same as {@link #set(DirectBuffer, int, int)} but for a {@code byte[]} source. */
  public void set(final byte[] src, final int offset, final int length) {
    if (length > data.length) {
      throw new IndexOutOfBoundsException(
          "length " + length + " exceeds probe capacity " + data.length);
    }
    if (length > 0) {
      System.arraycopy(src, offset, data, 0, length);
    }
    this.length = length;
    this.hashValid = false;
  }

  /** Length in bytes of the live key (≤ backing capacity for a probe). */
  public int length() {
    return length;
  }

  @Override
  public int hashCode() {
    if (!hashValid) {
      int h = FNV_OFFSET_BASIS;
      for (int i = 0; i < length; i++) {
        h ^= (data[i] & 0xFF);
        h *= FNV_PRIME;
      }
      this.hash = h;
      this.hashValid = true;
    }
    return hash;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof ByteArrayKey other)) {
      return false;
    }
    if (this.length != other.length) {
      return false;
    }
    // Arrays.mismatch is a JVM intrinsic on modern HotSpot — significantly faster than a
    // byte-by-byte loop, especially for the 16-byte account-code keys we use here.
    return java.util.Arrays.mismatch(this.data, 0, this.length, other.data, 0, other.length) < 0;
  }
}
