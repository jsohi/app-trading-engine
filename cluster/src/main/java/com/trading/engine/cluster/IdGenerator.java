package com.trading.engine.cluster;

import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

/**
 * Deterministic, single-threaded sequential id generator used by the cluster service.
 *
 * <p>Produces ids of the form {@code "PREFIX-NNNNNNNNNNN"} (11 zero-padded decimal digits) where
 * {@code PREFIX} is supplied at construction (e.g., {@code "ORD"}, {@code "EXE"}, {@code "QTE"}).
 * 11 digits gives ~100 billion ids per generator instance — sufficient headroom for a multi-year
 * cluster lifetime even at sustained million-orders-per-second peak load.
 *
 * <p>Determinism guarantees required for Aeron Cluster log replay:
 *
 * <ul>
 *   <li>No randomness, no UUID, no wall-clock time
 *   <li>Not thread-safe — called only from the single-threaded cluster duty cycle
 *   <li>Counter state is part of cluster state and survives failover via snapshot
 * </ul>
 *
 * <p><b>Hot-path API:</b> {@link #nextInto(MutableDirectBuffer, int)}. Zero allocation — increments
 * the counter and writes the rendered id ASCII bytes directly into a caller-provided buffer. This
 * is the only entry point cluster command handlers should call.
 *
 * <p><b>Diagnostic API:</b> {@code next()} returns a {@code String} for tests in the same package.
 * Visibility is package-private so subpackage handlers and external modules cannot reach it; the
 * cluster main path has only one option, which is the zero-allocation flyweight above.
 */
public final class IdGenerator {

  /**
   * Maximum counter value renderable as 11 zero-padded digits. ~100 billion ids gives a multi-year
   * lifetime per generator instance even at sustained million-orders-per-second peak load — ample
   * headroom across cluster restarts since the counter survives via snapshot.
   */
  public static final long MAX_COUNTER = 99_999_999_999L;

  /** Number of bytes written by {@link #saveTo(MutableDirectBuffer, int)}. */
  public static final int SNAPSHOT_LENGTH = Long.BYTES;

  private static final int DIGITS = 11;

  /**
   * Maximum prefix length. Bound by the {@code IdPrefix} SBE type ({@code char[8]} in {@code
   * trading-schema.xml}), which is the key field of {@code IdGeneratorSnapshot} (205). A longer
   * prefix would silently truncate on snapshot save and break replay determinism on recovery. At
   * length 8 the rendered id is {@code "XXXXXXXX-NNNNNNNNNNN"} (20 chars) — exactly fills the
   * 20-char {@code OrderID} / {@code ExecID} / {@code ClOrdID} SBE field limit.
   */
  public static final int MAX_PREFIX_LENGTH = 8;

  private final String prefix;

  /**
   * Pre-allocated render buffer holding the prefix bytes + '-' + 11 digit positions. Reused across
   * every {@link #nextInto} / {@link #next} call — no per-call allocation.
   */
  private final byte[] bytes;

  private final int digitsStart;
  private long counter;

  /**
   * @param prefix non-empty ASCII id prefix, e.g., {@code "ORD"}. Stored verbatim — case is
   *     preserved.
   */
  public IdGenerator(String prefix) {
    if (prefix == null || prefix.isEmpty()) {
      throw new IllegalArgumentException("prefix must be non-empty");
    }
    if (prefix.length() > MAX_PREFIX_LENGTH) {
      throw new IllegalArgumentException(
          "prefix length must be <= "
              + MAX_PREFIX_LENGTH
              + " to fit IdPrefix char[8] in IdGeneratorSnapshot (205), was "
              + prefix.length());
    }
    // SBE char[8] is 8 *bytes*, not 8 UTF-16 code units. Reject any non-ASCII char so the
    // String length check above is also a byte-length check, and the rendered id is safe to
    // copy into SBE OrderID/ExecID/ClOrdID fields (also char[N] = N bytes).
    for (int i = 0; i < prefix.length(); i++) {
      char c = prefix.charAt(i);
      if (c >= 0x80) {
        throw new IllegalArgumentException(
            "prefix must be ASCII (chars < 0x80) so its byte length matches String length; "
                + "non-ASCII char 0x"
                + Integer.toHexString(c)
                + " at index "
                + i);
      }
    }
    this.prefix = prefix;
    this.bytes = new byte[prefix.length() + 1 + DIGITS];
    for (int i = 0; i < prefix.length(); i++) {
      this.bytes[i] = (byte) prefix.charAt(i); // safe — ASCII validated above
    }
    this.bytes[prefix.length()] = (byte) '-';
    this.digitsStart = prefix.length() + 1;
    this.counter = 0L;
  }

  /** Returns the prefix supplied at construction. */
  public String prefix() {
    return prefix;
  }

  /**
   * Total byte length of an id rendered by this generator: {@code prefix.length() + 1 + 11}. Stable
   * for the lifetime of this instance — useful for SBE field sizing and bounds checks.
   */
  public int idByteLength() {
    return bytes.length;
  }

  /**
   * <b>Hot-path API.</b> Increment the counter and write the rendered id ASCII bytes into {@code
   * dst} starting at {@code offset}. Zero allocation.
   *
   * @return number of bytes written ({@link #idByteLength()})
   * @throws IllegalStateException if the counter would exceed {@link #MAX_COUNTER}
   */
  public int nextInto(MutableDirectBuffer dst, int offset) {
    renderNextId();
    dst.putBytes(offset, bytes);
    return bytes.length;
  }

  /**
   * <b>Diagnostic API — package-private to keep it off the cluster main path.</b> Increment the
   * counter and return the next id as a {@code String}. Allocates exactly one {@code String}. Use
   * {@link #nextInto(MutableDirectBuffer, int)} from cluster command handlers.
   *
   * <p>Visibility is intentionally package-private so only tests and other classes in {@code
   * com.trading.engine.cluster} can call it. Subpackage command handlers and external modules will
   * get a compile error if they reach for it instead of {@code nextInto}.
   *
   * @throws IllegalStateException if the counter would exceed {@link #MAX_COUNTER}
   */
  String next() {
    renderNextId();
    return new String(bytes, StandardCharsets.US_ASCII);
  }

  /**
   * Increment the counter and render the digits into the pre-allocated {@link #bytes} buffer.
   * Shared by both {@link #nextInto} and {@link #next}; mutates only the digit positions, the
   * prefix and hyphen are written once at construction.
   */
  private void renderNextId() {
    if (counter >= MAX_COUNTER) {
      throw new IllegalStateException(
          "IdGenerator counter exhausted for prefix '" + prefix + "' at " + counter);
    }
    // 100B exceeds Integer.MAX_VALUE, so the render loop runs in long arithmetic. Long div/mod
    // on a register-resident value is still ~10ns; the lifetime headroom is worth it.
    long n = ++counter;
    for (int i = DIGITS - 1; i >= 0; i--) {
      bytes[digitsStart + i] = (byte) ('0' + (int) (n % 10));
      n /= 10;
    }
  }

  /**
   * Current counter value; the next id returned by {@link #next()} or {@link #nextInto} will encode
   * {@code counter + 1}. Returns 0 before any call to next, or after {@link #loadFrom} restores a
   * snapshot taken before any id was assigned.
   */
  public long currentCounter() {
    return counter;
  }

  /**
   * Serialize the counter into {@code buffer} at {@code offset}. Writes exactly {@link
   * #SNAPSHOT_LENGTH} bytes in little-endian order to match {@code trading-schema.xml}'s {@code
   * byteOrder="littleEndian"} declaration so snapshot bytes survive cross-architecture transfer
   * (e.g., disaster-recovery copies between hosts).
   */
  public void saveTo(MutableDirectBuffer buffer, int offset) {
    buffer.putLong(offset, counter, ByteOrder.LITTLE_ENDIAN);
  }

  /**
   * Restore the counter from {@code buffer} at {@code offset}. After restore, the next call to
   * {@link #next()} or {@link #nextInto} returns id {@code counter + 1}. Reads in little-endian
   * order to match {@link #saveTo}.
   */
  public void loadFrom(DirectBuffer buffer, int offset) {
    long restored = buffer.getLong(offset, ByteOrder.LITTLE_ENDIAN);
    setCounter(restored);
  }

  /**
   * Restore the counter from a primitive {@code long}. Used by the cluster service's snapshot
   * restore path where the counter is decoded from the {@code IdGeneratorSnapshot} repeating-group
   * field (already a primitive), so there is no buffer to wrap. After restore, the next call to
   * {@link #next()} or {@link #nextInto} returns id {@code counter + 1}.
   *
   * @throws IllegalStateException if {@code restored} is negative or greater than {@link
   *     #MAX_COUNTER}
   */
  public void setCounter(final long restored) {
    if (restored < 0L || restored > MAX_COUNTER) {
      throw new IllegalStateException(
          "IdGenerator snapshot counter out of range for prefix '" + prefix + "': " + restored);
    }
    this.counter = restored;
  }
}
