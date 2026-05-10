package com.trading.engine.orchestrator;

import java.nio.charset.StandardCharsets;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.EpochNanoClock;

/**
 * Sequential ID generator for the orchestrator module. Produces IDs of the form {@code
 * "PREFIX-NNNNNNNNNNN"} (11 zero-padded decimal digits) where {@code PREFIX} is supplied at
 * construction (e.g., {@code "QTE"} for quote IDs).
 *
 * <p>This is a simplified, snapshot-free version of the cluster's {@link
 * com.trading.engine.cluster.IdGenerator}. The orchestrator does not participate in Aeron Cluster
 * log replay, so snapshot save/load is unnecessary.
 *
 * <p><b>Restart safety (APP-40a §3.2 fix).</b> The counter is seeded at construction from {@code
 * clock.nanoTime() &gt;&gt;&gt; 20} so post-restart sequences are mathematically disjoint from any
 * pre-restart sequence still cached anywhere in the system (e.g. the bridge's {@code
 * quoteIdToSessionId} map, which has a 120s TTL but does not cache-bust on orchestrator restart).
 * The shift retains the high-order ~44 bits of the nanosecond clock — providing a unique seed per
 * orchestrator boot at &lt;~1ms granularity — while leaving 20 low bits of headroom before the next
 * boot's {@link #MAX_COUNTER} would collide with the previous boot's counter. {@code
 * EpochNanoClock} is monotonic across restarts, guaranteeing the seed strictly increases boot to
 * boot.
 *
 * <p>11 digits gives ~100 billion IDs per generator instance — sufficient headroom for a multi-year
 * orchestrator lifetime even at sustained million-RFQs-per-second peak load.
 *
 * <p><b>Hot-path API:</b> {@link #nextInto(MutableDirectBuffer, int)}. Zero allocation — increments
 * the counter and writes the rendered ID ASCII bytes directly into a caller-provided buffer.
 *
 * <p><b>Threading:</b> not thread-safe — single-threaded orchestrator duty cycle only.
 *
 * <p><b>Allocation:</b> zero allocation after construction. The render buffer is pre-allocated at
 * construction; {@link #nextInto} only writes into the pre-allocated byte array and the caller's
 * buffer.
 *
 * @see com.trading.engine.cluster.IdGenerator
 */
public final class OrchestratorIdGenerator {

  /**
   * Maximum counter value renderable as 11 zero-padded digits. ~100 billion IDs gives a multi-year
   * lifetime per generator instance.
   */
  public static final long MAX_COUNTER = 99_999_999_999L;

  private static final int DIGITS = 11;

  /**
   * Maximum prefix length. The generated ID must fit in the 20-byte SBE QuoteID field: {@code
   * prefix.length() + 1 ('-') + 11 (digits) <= 20}, so max prefix length is 8. This matches the
   * cluster's {@code IdGenerator.MAX_PREFIX_LENGTH}.
   */
  public static final int MAX_PREFIX_LENGTH = 8;

  /**
   * Maximum SBE QuoteID field length. Used to validate that the generated ID fits in the SBE field
   * at construction time.
   */
  private static final int QUOTE_ID_SBE_LENGTH = 20;

  private final String prefix;

  /**
   * Pre-allocated render buffer holding the prefix bytes + '-' + 11 digit positions. Reused across
   * every {@link #nextInto} call — no per-call allocation.
   */
  private final byte[] bytes;

  private final int digitsStart;
  private long counter;

  /**
   * Creates a deterministic ID generator with counter starting at zero. Test-friendly form — used
   * exclusively by unit tests that assert specific ID sequences (e.g. {@code "QTE-00000000001"}).
   * Production code MUST use the {@link #OrchestratorIdGenerator(String, EpochNanoClock)} clock-
   * injected form so post-restart sequences are disjoint from any pre-restart cached state.
   *
   * @param prefix non-empty ASCII ID prefix, e.g., {@code "QTE"}. Stored verbatim — case is
   *     preserved.
   * @throws NullPointerException if {@code prefix} is null
   * @throws IllegalArgumentException if prefix is empty, exceeds {@link #MAX_PREFIX_LENGTH},
   *     contains non-ASCII characters, or would produce IDs that do not fit in the 20-byte SBE
   *     QuoteID field
   */
  public OrchestratorIdGenerator(final String prefix) {
    this(prefix, 0L);
  }

  /**
   * Creates a new ID generator with the given prefix and a clock-derived initial seed.
   *
   * <p>Per CLAUDE.md §Clock Usage, out-of-cluster modules (orchestrator included) MUST take their
   * clock from {@link com.trading.engine.messages.clock.TradingClocks#epochNanoClock()} via
   * dependency injection rather than calling {@code System.currentTimeMillis()} directly. The
   * counter is seeded from {@code clock.nanoTime() &gt;&gt;&gt; 20} so successive restarts of the
   * orchestrator generate disjoint sequences (see class-level Javadoc for the restart-safety
   * argument). The seed is clamped to {@code [0, MAX_COUNTER)} so {@link #renderNextId()}'s
   * exhaustion check still applies even on a clock that returns a near-{@code Long.MAX_VALUE}
   * value.
   *
   * @param prefix non-empty ASCII ID prefix, e.g., {@code "QTE"}
   * @param clock injected epoch-nanosecond clock; used once at construction to seed the counter
   * @throws NullPointerException if {@code prefix} or {@code clock} is null
   * @throws IllegalArgumentException if prefix is invalid (see {@link #OrchestratorIdGenerator(String)})
   */
  public OrchestratorIdGenerator(final String prefix, final EpochNanoClock clock) {
    this(prefix, computeRestartSafeSeed(clock));
  }

  /**
   * Internal canonical constructor — both public ctors funnel through here.
   *
   * @param prefix non-empty ASCII ID prefix
   * @param initialCounter starting counter value; rendered ID is for {@code initialCounter + 1}
   * @throws NullPointerException if {@code prefix} is null
   * @throws IllegalArgumentException if prefix is invalid or {@code initialCounter} negative
   */
  private OrchestratorIdGenerator(final String prefix, final long initialCounter) {
    if (prefix == null) {
      throw new NullPointerException("prefix must not be null");
    }
    if (prefix.isEmpty()) {
      throw new IllegalArgumentException("prefix must be non-empty");
    }
    if (prefix.length() > MAX_PREFIX_LENGTH) {
      throw new IllegalArgumentException(
          "prefix length must be <= "
              + MAX_PREFIX_LENGTH
              + " to fit in 20-byte SBE QuoteID field, was "
              + prefix.length());
    }
    for (int i = 0; i < prefix.length(); i++) {
      final char c = prefix.charAt(i);
      if (c >= 0x80) {
        throw new IllegalArgumentException(
            "prefix must be ASCII (chars < 0x80) so its byte length matches String length; "
                + "non-ASCII char 0x"
                + Integer.toHexString(c)
                + " at index "
                + i);
      }
    }
    if (initialCounter < 0L) {
      throw new IllegalArgumentException("initialCounter must be >= 0, was " + initialCounter);
    }
    final int totalLength = prefix.length() + 1 + DIGITS;
    if (totalLength > QUOTE_ID_SBE_LENGTH) {
      throw new IllegalArgumentException(
          "generated ID length ("
              + totalLength
              + " = prefix "
              + prefix.length()
              + " + 1 + "
              + DIGITS
              + " digits) exceeds SBE QuoteID field length ("
              + QUOTE_ID_SBE_LENGTH
              + ")");
    }
    this.prefix = prefix;
    this.bytes = new byte[totalLength];
    for (int i = 0; i < prefix.length(); i++) {
      this.bytes[i] = (byte) prefix.charAt(i);
    }
    this.bytes[prefix.length()] = (byte) '-';
    this.digitsStart = prefix.length() + 1;
    this.counter = initialCounter;
  }

  /**
   * Compute the restart-safe initial-counter seed from the supplied clock.
   *
   * <p>{@code clock.nanoTime() >>> 20} retains the high-order ~44 bits — providing a unique seed
   * per orchestrator boot at &lt;~1ms granularity — and the result is clamped to {@code [0,
   * MAX_COUNTER)} via {@link Math#floorMod} so the {@link #renderNextId()} exhaustion check still
   * applies.
   *
   * @param clock injected epoch-nanosecond clock
   * @return a counter seed in {@code [0, MAX_COUNTER)}
   * @throws NullPointerException if {@code clock} is null
   */
  private static long computeRestartSafeSeed(final EpochNanoClock clock) {
    if (clock == null) {
      throw new NullPointerException("clock must not be null");
    }
    final long rawSeed = clock.nanoTime() >>> 20;
    return Math.floorMod(rawSeed, MAX_COUNTER);
  }

  /**
   * Returns the prefix supplied at construction.
   *
   * @return the ID prefix string (e.g., "QTE")
   */
  public String prefix() {
    return prefix;
  }

  /**
   * Total byte length of an ID rendered by this generator: {@code prefix.length() + 1 + 11}. Stable
   * for the lifetime of this instance — useful for SBE field sizing and bounds checks.
   *
   * @return the ID byte length
   */
  public int idByteLength() {
    return bytes.length;
  }

  /**
   * <b>Hot-path API.</b> Increment the counter and write the rendered ID ASCII bytes into {@code
   * dst} starting at {@code offset}. Zero allocation.
   *
   * @param dst the destination buffer to write the ID bytes into
   * @param offset the byte offset in {@code dst} at which to start writing
   * @return number of bytes written ({@link #idByteLength()})
   * @throws IllegalStateException if the counter would exceed {@link #MAX_COUNTER}
   */
  public int nextInto(final MutableDirectBuffer dst, final int offset) {
    renderNextId();
    dst.putBytes(offset, bytes);
    return bytes.length;
  }

  /**
   * Current counter value; the next ID returned by {@link #nextInto} will encode {@code counter +
   * 1}. Returns 0 before any call to nextInto.
   *
   * @return the current counter value
   */
  public long currentCounter() {
    return counter;
  }

  /**
   * Increment the counter and render the digits into the pre-allocated {@link #bytes} buffer.
   * Mutates only the digit positions; the prefix and hyphen are written once at construction.
   */
  private void renderNextId() {
    if (counter >= MAX_COUNTER) {
      throw new IllegalStateException(
          "OrchestratorIdGenerator counter exhausted for prefix '" + prefix + "' at " + counter);
    }
    long n = ++counter;
    for (int i = DIGITS - 1; i >= 0; i--) {
      bytes[digitsStart + i] = (byte) ('0' + (int) (n % 10));
      n /= 10;
    }
  }

  /**
   * <b>Diagnostic API.</b> Increment the counter and return the next ID as a {@code String}.
   * Allocates exactly one {@code String} — use {@link #nextInto(MutableDirectBuffer, int)} from the
   * orchestrator duty cycle.
   *
   * @return the next ID as a String
   * @throws IllegalStateException if the counter would exceed {@link #MAX_COUNTER}
   */
  String next() {
    renderNextId();
    return new String(bytes, StandardCharsets.US_ASCII);
  }
}
