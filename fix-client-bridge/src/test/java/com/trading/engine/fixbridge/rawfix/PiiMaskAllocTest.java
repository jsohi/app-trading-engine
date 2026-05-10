package com.trading.engine.fixbridge.rawfix;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

/**
 * Allocation regression tripwire for {@link PiiMask#mask(byte[], int, int, byte[], int)}.
 *
 * <p>Mirrors the {@code GarbageCollectorMXBean.getCollectionCount()} delta pattern used by other
 * {@code *AllocTest}s in this module: warm the JIT, sample GC count, run the hot path in a tight
 * loop, sample again, assert no GC collection occurred.
 *
 * <p>Gated by {@code -DrunAllocTests=true} — opt-in only because GC counts can be advanced by
 * unrelated background processes on a shared CI host (locked §21, §23).
 *
 * <p>Threading: single-threaded. {@link PiiMask} is thread-safe per its contract but the test owns
 * the instance exclusively.
 */
@EnabledIfSystemProperty(named = "runAllocTests", matches = "true")
final class PiiMaskAllocTest {

  private static final int WARMUP_ITERATIONS = 5_000;
  private static final int MEASURED_ITERATIONS = 100_000;

  /**
   * A realistic 250-byte FIX NewOrderSingle message with Account (tag 1) and multiple non-masked
   * fields. The Account value is 8 characters so the star replacement exercises the masking branch
   * rather than the trivial empty-value path.
   */
  private static final byte[] SAMPLE_MESSAGE =
      ("8=FIX.4.4|9=187|35=D|49=BRIDGE01|56=EXCHANGE|34=42|52=20240410-12:00:00.000|"
              + "11=C-1234567890|55=EURUSD|54=1|38=1000000|44=110000000|40=2|59=1|"
              + "1=ACCT-001|10=199|")
          .getBytes(StandardCharsets.US_ASCII);

  /**
   * Pre-allocate src and dst outside the loop so the loop itself is zero-alloc. The src is a copy
   * of the sample so repeated calls don't corrupt it (mask writes stars into dst, not src — but the
   * in-place variant could; we use separate buffers to keep the loop clean).
   */
  private static final byte[] SRC = SAMPLE_MESSAGE.clone();

  private static final byte[] DST = new byte[SRC.length];

  /**
   * Run {@link PiiMask#mask} 100k times on a realistic message and assert no GC collection fires
   * after JIT warmup. Confirms the zero-alloc guarantee documented on the class.
   */
  @Test
  void mask_repeatedFullMessage_zeroAlloc() {
    final var mask = PiiMask.withDefaultMask();

    // Warmup — let the JIT compile the hot masking path before measuring.
    for (int i = 0; i < WARMUP_ITERATIONS; i++) {
      final int written = mask.mask(SRC, 0, SRC.length, DST, 0);
      // Suppress dead-code elimination.
      if (written != SRC.length) {
        throw new AssertionError("unexpected written length: " + written);
      }
    }

    final long beforeGc = totalGcCount();
    for (int i = 0; i < MEASURED_ITERATIONS; i++) {
      final int written = mask.mask(SRC, 0, SRC.length, DST, 0);
      // Suppress dead-code elimination.
      if (written != SRC.length) {
        throw new AssertionError("unexpected written length: " + written);
      }
    }
    final long afterGc = totalGcCount();

    assertEquals(
        beforeGc, afterGc, "PiiMask.mask advanced GC count from " + beforeGc + " to " + afterGc);
  }

  // ---------------------------------------------------------------------------
  // GC count helper — shared pattern across *AllocTest classes in this module.
  // ---------------------------------------------------------------------------

  private static long totalGcCount() {
    long total = 0L;
    final var beans = ManagementFactory.getGarbageCollectorMXBeans();
    for (final var bean : beans) {
      final long c = bean.getCollectionCount();
      if (c >= 0L) {
        total += c;
      }
    }
    return total;
  }
}
