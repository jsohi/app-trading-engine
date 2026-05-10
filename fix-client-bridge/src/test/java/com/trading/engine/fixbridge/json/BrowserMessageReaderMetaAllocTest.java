package com.trading.engine.fixbridge.json;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.netty.buffer.Unpooled;
import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

/**
 * Allocation regression tripwire for {@link BrowserMessageReader} when parsing the {@code _meta}
 * envelope (§3.6). Mirrors the {@code GarbageCollectorMXBean.getCollectionCount()} delta pattern
 * from other {@code *AllocTest}s in this module: warm the JIT, sample GC count, run the parser in a
 * tight loop, sample again, assert no GC advanced.
 *
 * <p>Gated by {@code -DrunAllocTests=true} (locked §21, §23) — opt-in only because GC counts can be
 * advanced by unrelated background processes on a shared CI host.
 *
 * <p>Threading: single-threaded. The {@link MutableParsedMessage} flyweight and {@link
 * io.netty.buffer.ByteBuf} are not shared across threads.
 */
@EnabledIfSystemProperty(named = "runAllocTests", matches = "true")
final class BrowserMessageReaderMetaAllocTest {

  private static final int WARMUP_ITERATIONS = 5_000;
  private static final int MEASURED_ITERATIONS = 100_000;

  // ---------------------------------------------------------------------------
  // Test 1: QuoteRequest + _meta + traceparent (hot path through meta object).
  // ---------------------------------------------------------------------------

  @Test
  void parse_quoteRequestWithMetaTraceparent_zeroAlloc() {
    final byte[] frame =
        ("{\"type\":\"QuoteRequest\","
                + "\"reqId\":\"R-1\","
                + "\"symbol\":\"EURUSD\","
                + "\"side\":\"Buy\","
                + "\"qty\":\"1000000\","
                + "\"_meta\":{\"traceparent\":"
                + "\"00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01\"}}")
            .getBytes(StandardCharsets.UTF_8);
    runLoop(frame);
  }

  // ---------------------------------------------------------------------------
  // Test 2: QuoteRequest + _meta with a worst-case skip-balanced path.
  //
  // The skip-balanced path is exercised by an unknown inner key whose value is a
  // raw multi-byte UTF-8 string (no backslash — scanStringEnd rejects backslash,
  // so the test uses literal multi-byte UTF-8 code points that are > 0x7F and
  // are not control chars or backslash). The point is to exercise the full
  // string-scanning branch of skipBalancedValue on every iteration.
  //
  // Note: bytes 0xC3 0xA9 = U+00E9 (é), 0xC3 0xBC = U+00FC (ü) — valid UTF-8
  // continuation sequences, all >= 0x20 and != '\\', so scanStringEnd will scan
  // past them without throwing.
  // ---------------------------------------------------------------------------

  @Test
  void parse_quoteRequestWithMetaMultibyteUtf8InSkipPath_zeroAlloc() {
    // Build the JSON frame as a byte array so we can embed raw multi-byte UTF-8.
    final String jsonStr =
        "{\"type\":\"QuoteRequest\","
            + "\"reqId\":\"R-1\","
            + "\"symbol\":\"EURUSD\","
            + "\"side\":\"Buy\","
            + "\"qty\":\"1000000\","
            + "\"_meta\":{\"unknown\":\"éüéü\","
            + "\"traceparent\":"
            + "\"00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01\"}}";
    final byte[] frame = jsonStr.getBytes(StandardCharsets.UTF_8);
    runLoop(frame);
  }

  // ---------------------------------------------------------------------------
  // Test 3: QuoteRequest + empty _meta (empty-meta fast path — no traceparent).
  // ---------------------------------------------------------------------------

  @Test
  void parse_quoteRequestWithEmptyMeta_zeroAlloc() {
    final byte[] frame =
        ("{\"type\":\"QuoteRequest\","
                + "\"reqId\":\"R-1\","
                + "\"symbol\":\"EURUSD\","
                + "\"side\":\"Buy\","
                + "\"qty\":\"1000000\","
                + "\"_meta\":{}}")
            .getBytes(StandardCharsets.UTF_8);
    runLoop(frame);
  }

  // ---------------------------------------------------------------------------
  // Shared loop helper.
  // ---------------------------------------------------------------------------

  private static void runLoop(final byte[] frame) {
    final var out = new MutableParsedMessage();
    final var src = Unpooled.wrappedBuffer(frame);

    // Warmup — let JIT profile + compile the hot parse path before we measure.
    for (int i = 0; i < WARMUP_ITERATIONS; i++) {
      src.readerIndex(0);
      BrowserMessageReader.parse(src, out);
    }

    final long beforeGc = totalGcCount();
    for (int i = 0; i < MEASURED_ITERATIONS; i++) {
      src.readerIndex(0);
      final int t = BrowserMessageReader.parse(src, out);
      assertTrue(t > 0); // sanity: returned a valid type sentinel
    }
    final long afterGc = totalGcCount();

    assertEquals(
        beforeGc,
        afterGc,
        "BrowserMessageReader.parse(_meta) advanced GC count from " + beforeGc + " to " + afterGc);
  }

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
