package com.trading.engine.cluster;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

/**
 * Deterministic, single-threaded sequential id generator used by the cluster service.
 *
 * <p>Produces ids of the form {@code "PREFIX-NNNNNNNNN"} (9 zero-padded decimal digits) where
 * {@code PREFIX} is supplied at construction (e.g., {@code "ORD"}, {@code "EXE"}, {@code "QTE"}).
 *
 * <p>Determinism guarantees required for Aeron Cluster log replay:
 *
 * <ul>
 *   <li>No randomness, no UUID, no wall-clock time
 *   <li>Not thread-safe — called only from the single-threaded cluster duty cycle
 *   <li>Counter state is part of cluster state and survives failover via snapshot
 * </ul>
 *
 * <p>Allocation: each call to {@link #next()} performs exactly one {@code String} allocation. The
 * id text is rendered into a pre-allocated {@code char[]} so no intermediate {@code StringBuilder}
 * or {@code Long.toString} garbage is produced.
 *
 * <p>The counter is a {@code long} but the rendered id width is fixed at 9 digits, supporting up to
 * 999,999,999 ids per generator instance. Exceeding this throws {@link IllegalStateException}; a
 * wider id format would silently break replay determinism for any consumer that parses the id.
 */
public final class IdGenerator {

  /** Maximum counter value renderable as 9 zero-padded digits. */
  public static final long MAX_COUNTER = 999_999_999L;

  /** Number of bytes written by {@link #saveTo(MutableDirectBuffer, int)}. */
  public static final int SNAPSHOT_LENGTH = Long.BYTES;

  private static final int DIGITS = 9;

  /**
   * Maximum prefix length. Bound by the {@code IdPrefix} SBE type ({@code char[8]} in {@code
   * trading-schema.xml}), which is the key field of {@code IdGeneratorSnapshot} (205). A longer
   * prefix would silently truncate on snapshot save and break replay determinism on recovery. At
   * length 8 the rendered id is {@code "XXXXXXXX-NNNNNNNNN"} (18 chars), well under the 20-char
   * {@code OrderID} / {@code ExecID} / {@code ClOrdID} SBE field limit.
   */
  public static final int MAX_PREFIX_LENGTH = 8;

  private final String prefix;
  private final char[] buf;
  private final int digitsStart;
  private long counter;

  /**
   * @param prefix non-empty id prefix, e.g., {@code "ORD"}. Stored verbatim — case is preserved.
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
    this.buf = new char[prefix.length() + 1 + DIGITS];
    prefix.getChars(0, prefix.length(), buf, 0);
    buf[prefix.length()] = '-';
    this.digitsStart = prefix.length() + 1;
    this.counter = 0L;
  }

  /** Returns the prefix supplied at construction. */
  public String prefix() {
    return prefix;
  }

  /**
   * Increment the counter and return the next id as {@code "PREFIX-NNNNNNNNN"}. Allocates exactly
   * one {@code String}.
   *
   * @throws IllegalStateException if the counter would exceed {@link #MAX_COUNTER}
   */
  public String next() {
    if (counter >= MAX_COUNTER) {
      throw new IllegalStateException(
          "IdGenerator counter exhausted for prefix '" + prefix + "' at " + counter);
    }
    // Counter is bounded by MAX_COUNTER (999_999_999), which fits in an int — use int
    // arithmetic in the render loop to avoid the cost of long division/modulo.
    int n = (int) ++counter;
    for (int i = DIGITS - 1; i >= 0; i--) {
      buf[digitsStart + i] = (char) ('0' + (n % 10));
      n /= 10;
    }
    return new String(buf);
  }

  /**
   * Current counter value; the next id returned by {@link #next()} will encode {@code counter + 1}.
   * Returns 0 before any call to {@link #next()}, or after {@link #loadFrom} restores a snapshot
   * taken before any id was assigned.
   */
  public long currentCounter() {
    return counter;
  }

  /**
   * Serialize the counter into {@code buffer} at {@code offset}. Writes exactly {@link
   * #SNAPSHOT_LENGTH} bytes.
   */
  public void saveTo(MutableDirectBuffer buffer, int offset) {
    buffer.putLong(offset, counter);
  }

  /**
   * Restore the counter from {@code buffer} at {@code offset}. After restore, the next call to
   * {@link #next()} returns id {@code counter + 1}.
   */
  public void loadFrom(DirectBuffer buffer, int offset) {
    long restored = buffer.getLong(offset);
    if (restored < 0L || restored > MAX_COUNTER) {
      throw new IllegalStateException(
          "IdGenerator snapshot counter out of range for prefix '" + prefix + "': " + restored);
    }
    this.counter = restored;
  }
}
