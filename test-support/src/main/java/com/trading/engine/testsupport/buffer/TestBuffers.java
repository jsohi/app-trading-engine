package com.trading.engine.testsupport.buffer;

import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;

/**
 * Pre-sized buffer factory methods for common test scenarios.
 *
 * <p>All methods return {@link ExpandableArrayBuffer} instances (heap-backed, auto-growing,
 * debugger-friendly). Buffer sizes are initial capacities chosen to match typical SBE message
 * requirements without reallocation:
 *
 * <ul>
 *   <li>{@link #command()} — 256 bytes, sufficient for any single SBE command
 *   <li>{@link #event()} — 512 bytes, sufficient for any single SBE event
 *   <li>{@link #batch()} — 4096 bytes, sufficient for batch commands/events
 *   <li>{@link #snapshot()} — 65536 bytes, sufficient for snapshot concatenation
 * </ul>
 *
 * <p>Thread-safe — each call allocates a new buffer instance.
 *
 * <p>For tests requiring fixed-size (non-growing) buffers, use {@code new UnsafeBuffer(new
 * byte[size])} directly.
 */
public final class TestBuffers {

  private TestBuffers() {}

  /**
   * Creates a 256-byte buffer suitable for any single SBE command.
   *
   * @return new {@link ExpandableArrayBuffer} with 256-byte initial capacity
   */
  public static MutableDirectBuffer command() {
    return new ExpandableArrayBuffer(256);
  }

  /**
   * Creates a 512-byte buffer suitable for any single SBE event.
   *
   * @return new {@link ExpandableArrayBuffer} with 512-byte initial capacity
   */
  public static MutableDirectBuffer event() {
    return new ExpandableArrayBuffer(512);
  }

  /**
   * Creates a 4096-byte buffer suitable for batch commands and events.
   *
   * @return new {@link ExpandableArrayBuffer} with 4096-byte initial capacity
   */
  public static MutableDirectBuffer batch() {
    return new ExpandableArrayBuffer(4096);
  }

  /**
   * Creates a 65536-byte buffer suitable for snapshot concatenation.
   *
   * @return new {@link ExpandableArrayBuffer} with 65536-byte initial capacity
   */
  public static MutableDirectBuffer snapshot() {
    return new ExpandableArrayBuffer(65_536);
  }

  /**
   * Creates a buffer with the specified initial capacity.
   *
   * @param size initial capacity in bytes; must be &gt; 0
   * @return new {@link ExpandableArrayBuffer} with the given initial capacity
   * @throws IllegalArgumentException if {@code size} is &lt;= 0
   */
  public static MutableDirectBuffer of(final int size) {
    if (size <= 0) {
      throw new IllegalArgumentException("size must be > 0, was: " + size);
    }
    return new ExpandableArrayBuffer(size);
  }
}
