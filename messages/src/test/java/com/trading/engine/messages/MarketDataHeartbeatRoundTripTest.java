package com.trading.engine.messages;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.trading.engine.messages.sbe.MarketDataHeartbeatDecoder;
import com.trading.engine.messages.sbe.MarketDataHeartbeatEncoder;
import com.trading.engine.messages.sbe.MessageHeaderDecoder;
import com.trading.engine.messages.sbe.MessageHeaderEncoder;
import java.nio.charset.StandardCharsets;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

/**
 * Codec round-trip tests for MarketDataHeartbeat (template 55).
 *
 * <p>Covers two structurally distinct cases:
 *
 * <ol>
 *   <li><strong>Empty group</strong> — {@code lastPublishedSeqCount(0)}: the group header is
 *       written (4 bytes) but no entries follow. This is the first heartbeat fired before any tick
 *       has been published.
 *   <li><strong>Four-entry group</strong> (EURUSD, GBPUSD, USDJPY, AUDUSD): verifies that iteration
 *       order is preserved and that per-entry {@code symbol} and {@code seq} values decode
 *       correctly. Entry 0 and entry 3 carry deliberately different {@code seq} values so
 *       intra-group offset bugs are observable.
 * </ol>
 *
 * <p>Threading model: Not thread-safe — single-threaded JUnit test execution only. Allocation:
 * Allocates one {@code UnsafeBuffer} per test, reused across encode and decode phases.
 */
final class MarketDataHeartbeatRoundTripTest {

  /** 8 KiB — comfortably larger than the largest heartbeat frame (4 symbols × 16 B ≈ 100 B). */
  private static final int BUF_SIZE = 8_192;

  // -------------------------------------------------------------------------
  // Template 55 — empty lastPublishedSeq group
  // -------------------------------------------------------------------------

  /**
   * Encodes a MarketDataHeartbeat with {@code lastPublishedSeqCount(0)} and asserts that the {@code
   * serverNanos} field round-trips and the group count decodes as zero. This is the production path
   * for the very first heartbeat before any symbol has been published.
   */
  @Test
  void marketDataHeartbeat_roundTrip_emptyGroup() {
    final long serverNanos = 1_700_000_000_000_000_001L;

    final var buf = new UnsafeBuffer(new byte[BUF_SIZE]);
    final var headerEncoder = new MessageHeaderEncoder();
    final var encoder = new MarketDataHeartbeatEncoder();
    encoder.wrapAndApplyHeader(buf, 0, headerEncoder);
    encoder.serverNanos(serverNanos);
    encoder.lastPublishedSeqCount(0);

    final var headerDecoder = new MessageHeaderDecoder();
    final var decoder = new MarketDataHeartbeatDecoder();
    decoder.wrapAndApplyHeader(buf, 0, headerDecoder);

    assertEquals(MarketDataHeartbeatDecoder.TEMPLATE_ID, headerDecoder.templateId(), "templateId");
    assertEquals(MarketDataHeartbeatDecoder.SCHEMA_ID, headerDecoder.schemaId(), "schemaId");
    assertEquals(MarketDataHeartbeatDecoder.SCHEMA_VERSION, headerDecoder.version(), "version");
    assertEquals(serverNanos, decoder.serverNanos(), "serverNanos");

    final var group = decoder.lastPublishedSeq();
    assertEquals(0, group.count(), "expected zero entries in lastPublishedSeq group");
  }

  // -------------------------------------------------------------------------
  // Template 55 — four-entry lastPublishedSeq group
  // -------------------------------------------------------------------------

  /**
   * Encodes a MarketDataHeartbeat with four {@code lastPublishedSeq} entries (EURUSD, GBPUSD,
   * USDJPY, AUDUSD) and asserts that the decoded iteration order matches the encode order. Each
   * entry carries a distinct {@code seq} value so that an intra-group byte-offset bug (e.g., entry
   * 1 reading entry 0's seq) is immediately visible.
   */
  @Test
  void marketDataHeartbeat_roundTrip_fourEntries() {
    final long serverNanos = 1_700_000_000_999_888_777L;

    final var symbols = new String[] {"EURUSD", "GBPUSD", "USDJPY", "AUDUSD"};
    final long[] seqs = {1_001L, 2_002L, 3_003L, 4_004L};

    final var buf = new UnsafeBuffer(new byte[BUF_SIZE]);
    final var headerEncoder = new MessageHeaderEncoder();
    final var encoder = new MarketDataHeartbeatEncoder();
    encoder.wrapAndApplyHeader(buf, 0, headerEncoder);
    encoder.serverNanos(serverNanos);

    final var groupEnc = encoder.lastPublishedSeqCount(4);
    for (int i = 0; i < 4; i++) {
      groupEnc.next().symbol(symbols[i]).seq(seqs[i]);
    }

    final var headerDecoder = new MessageHeaderDecoder();
    final var decoder = new MarketDataHeartbeatDecoder();
    decoder.wrapAndApplyHeader(buf, 0, headerDecoder);

    assertEquals(serverNanos, decoder.serverNanos(), "serverNanos");

    final var group = decoder.lastPublishedSeq();
    assertEquals(4, group.count(), "group count");

    for (int i = 0; i < 4; i++) {
      group.next();
      final var symDst =
          new byte[MarketDataHeartbeatDecoder.LastPublishedSeqDecoder.symbolLength()];
      group.getSymbol(symDst, 0);
      assertArrayEquals(
          padRight(symbols[i], MarketDataHeartbeatDecoder.LastPublishedSeqDecoder.symbolLength()),
          symDst,
          "symbol at index " + i);
      assertEquals(seqs[i], group.seq(), "seq at index " + i);
    }
  }

  // -------------------------------------------------------------------------
  // Helpers
  // -------------------------------------------------------------------------

  /**
   * Returns a NUL-padded byte array of exactly {@code width} bytes containing the ASCII encoding of
   * {@code value}. Mirrors the SBE encoder behaviour.
   *
   * @param value the string to encode; must be &lt;= {@code width} characters long
   * @param width the fixed field width in bytes
   * @return byte array of length {@code width}
   */
  private static byte[] padRight(final String value, final int width) {
    final var dst = new byte[width];
    if (value != null && !value.isEmpty()) {
      final byte[] src = value.getBytes(StandardCharsets.US_ASCII);
      System.arraycopy(src, 0, dst, 0, Math.min(src.length, width));
    }
    return dst;
  }
}
