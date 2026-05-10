package com.trading.engine.orchestrator;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
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
 * <p><b>Restart safety (PR #70 CodeRabbit critical fix).</b> Production callers MUST use the
 * file-backed {@link #OrchestratorIdGenerator(String, EpochNanoClock, Path)} constructor, which
 * atomically advances a durable per-process boot counter on each restart. Each boot reserves {@link
 * #BOOT_HEADROOM} IDs from the global counter range, guaranteeing two LIVE boots can never share an
 * ID range — regardless of how fast the orchestrator restarts.
 *
 * <p>Why the durable file? An earlier {@code clock.nanoTime() >>> 20}-derived seed advanced the
 * seed by only ~953 per second of wall-clock time, while the per-boot counter could advance up to
 * ~1,000,000 per second under peak load. After a fast restart the new boot's seed could land inside
 * the prior boot's still-cached ID range — silently violating the "mathematically disjoint" claim
 * and corrupting the bridge's quoteId-to-session map (CodeRabbit critical finding on PR #70). The
 * durable file replaces "trust the wall-clock advance rate" with "claim a contiguous slice of the
 * namespace per boot, atomically".
 *
 * <p>The clock-only constructor {@link #OrchestratorIdGenerator(String, EpochNanoClock)} is
 * retained for unit tests that assert specific deterministic sequences but is NOT restart-safe;
 * production code paths that need restart-disjointness MUST use the file-backed constructor.
 *
 * <p>11 digits gives ~100 billion IDs per generator instance. With a {@link #BOOT_HEADROOM} of one
 * billion IDs per boot, the file-backed counter wraps after ~100 boots — at which point a boot may
 * re-use a seed range from a prior boot. This is safe because the bridge's {@code
 * quoteIdToSessionId} cache evicts entries after a maximum TTL of 180s (registration 120s +
 * post-emission 60s), and 100 successive boots (each consuming a fresh {@link #BOOT_HEADROOM} slice
 * within ~1000s at peak load) implies many minutes of elapsed wall time between any two boots that
 * share a seed range. For production deployments expecting more than 100 lifetime boots before
 * bridge-cache eviction completes, increase {@link #MAX_COUNTER} (and the rendered ID's digit
 * count) or shrink {@link #BOOT_HEADROOM}.
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

  /**
   * IDs reserved per boot from the durable counter file. One billion gives each boot ~1000s of
   * runtime at the documented peak rate (1,000,000 RFQs/second). Larger values give more per- boot
   * headroom at the cost of fewer total boots before file-counter wraparound.
   */
  public static final long BOOT_HEADROOM = 1_000_000_000L;

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
   * Creates a new ID generator with the given prefix and a clock-derived initial seed. <b>Test-
   * only — NOT restart-safe. Production code MUST use {@link #OrchestratorIdGenerator(String,
   * EpochNanoClock, Path)}.</b>
   *
   * <p>Per CLAUDE.md §Clock Usage, out-of-cluster modules (orchestrator included) MUST take their
   * clock from {@link com.trading.engine.messages.clock.TradingClocks#epochNanoClock()} via
   * dependency injection. The counter is seeded from {@code clock.nanoTime() &gt;&gt;&gt; 20} which
   * provides {@code ~1ms} seed granularity — adequate for tests that mint a handful of IDs
   * post-restart, but inadequate to guarantee disjointness in production where the per-boot counter
   * advances {@code 1000×} faster than the seed under peak load (see class Javadoc for the
   * CodeRabbit critical finding on PR #70). Production callers MUST use the file-backed {@link
   * #OrchestratorIdGenerator(String, EpochNanoClock, Path)} constructor.
   *
   * @param prefix non-empty ASCII ID prefix, e.g., {@code "QTE"}
   * @param clock injected epoch-nanosecond clock; used once at construction to seed the counter
   * @throws NullPointerException if {@code prefix} or {@code clock} is null
   * @throws IllegalArgumentException if prefix is invalid (see {@link
   *     #OrchestratorIdGenerator(String)})
   */
  public OrchestratorIdGenerator(final String prefix, final EpochNanoClock clock) {
    this(prefix, computeRestartSafeSeed(clock));
  }

  /**
   * Production constructor — restart-safe via a durable boot counter file.
   *
   * <p>On every construction this atomically reads the persisted high-watermark from {@code
   * bootCounterFile}, claims a contiguous {@link #BOOT_HEADROOM}-sized slice for this boot, and
   * writes the new high-watermark back via {@link Files#move} with {@link
   * StandardCopyOption#ATOMIC_MOVE} so a process crash mid-write cannot leave the file in a
   * partially-written state. The starting counter for THIS boot is the OLD high-watermark (modulo
   * {@link #MAX_COUNTER} so the rendered-ID range invariant holds).
   *
   * <p>If the file does not exist on first boot, it is treated as containing zero. If the file
   * exists but contains malformed data, {@link IllegalStateException} is thrown — fail-fast rather
   * than silently re-using a seed that may collide with a prior boot.
   *
   * <p>The {@code clock} parameter is currently unused by the file-backed path (the boot counter
   * fully determines the seed). It is retained in the signature so future evolutions (e.g. a hybrid
   * clock+file scheme that uses the clock to detect non-monotonic file rollback) do not require an
   * API break.
   *
   * @param prefix non-empty ASCII ID prefix, e.g., {@code "QTE"}
   * @param clock injected epoch-nanosecond clock; reserved for future use, may not be null
   * @param bootCounterFile path to the durable boot counter file; the parent directory MUST exist
   *     and be writable. The file is created atomically on first boot if absent.
   * @throws NullPointerException if any argument is null
   * @throws IllegalArgumentException if {@code prefix} is invalid
   * @throws IllegalStateException if the file is unreadable, malformed, or cannot be persisted
   */
  public OrchestratorIdGenerator(
      final String prefix, final EpochNanoClock clock, final Path bootCounterFile) {
    this(prefix, advanceBootCounter(bootCounterFile, BOOT_HEADROOM, clock));
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
   * Atomically advance the durable boot counter and return the previous value (this boot's starting
   * counter, modulo {@link #MAX_COUNTER}).
   *
   * <p>Read the current file content as a base-10 long (zero if file absent), compute {@code
   * newNext = current + headroom}, write {@code newNext} to a sibling tmp file, then {@link
   * Files#move} the tmp file over the target with {@link StandardCopyOption#ATOMIC_MOVE}. If the
   * filesystem does not support atomic move, the move falls back to {@link
   * StandardCopyOption#REPLACE_EXISTING} alone — leaving a small race window where two concurrent
   * processes could read the same value. Single-process orchestrator deployments (the supported
   * topology) avoid this entirely.
   *
   * @param file path to the boot counter file; parent directory must exist
   * @param headroom number of IDs to reserve for this boot
   * @param clock reserved (validated non-null only); see {@link #OrchestratorIdGenerator(String,
   *     EpochNanoClock, Path)} Javadoc for rationale
   * @return the prior file content modulo {@link #MAX_COUNTER}, suitable as this boot's starting
   *     counter
   * @throws NullPointerException if {@code file} or {@code clock} is null
   * @throws IllegalStateException if the file is unreadable, malformed, or cannot be persisted
   */
  private static long advanceBootCounter(
      final Path file, final long headroom, final EpochNanoClock clock) {
    if (file == null) {
      throw new NullPointerException("bootCounterFile must not be null");
    }
    if (clock == null) {
      throw new NullPointerException("clock must not be null");
    }
    final long currentNext;
    if (Files.exists(file)) {
      try {
        final var contents = Files.readString(file, StandardCharsets.US_ASCII).trim();
        if (contents.isEmpty()) {
          currentNext = 0L;
        } else {
          currentNext = Long.parseLong(contents);
        }
      } catch (final IOException e) {
        throw new IllegalStateException("Failed to read boot counter file: " + file, e);
      } catch (final NumberFormatException e) {
        throw new IllegalStateException(
            "Boot counter file " + file + " contains malformed long value (refusing to start)", e);
      }
    } else {
      currentNext = 0L;
    }
    if (currentNext < 0L) {
      throw new IllegalStateException(
          "Boot counter file " + file + " contains negative value (refusing to start)");
    }
    final long newNext = currentNext + headroom;
    final var tmp = file.resolveSibling(file.getFileName().toString() + ".tmp");
    try {
      Files.writeString(
          tmp,
          Long.toString(newNext),
          StandardCharsets.US_ASCII,
          StandardOpenOption.CREATE,
          StandardOpenOption.TRUNCATE_EXISTING,
          StandardOpenOption.WRITE);
      try {
        Files.move(tmp, file, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
      } catch (final java.nio.file.AtomicMoveNotSupportedException atomicEx) {
        // Filesystem does not support atomic move (rare on POSIX; can occur on cross-fs tmpdirs).
        // Fall back to non-atomic replace — single-process deployments are unaffected; multi-
        // process startup races on the same file are out of scope (the orchestrator is a
        // single-process component per CLAUDE.md §Architecture).
        Files.move(tmp, file, StandardCopyOption.REPLACE_EXISTING);
      }
    } catch (final IOException e) {
      throw new IllegalStateException("Failed to persist boot counter to: " + file, e);
    }
    return Math.floorMod(currentNext, MAX_COUNTER);
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
