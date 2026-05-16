package com.trading.engine.messages;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.trading.engine.messages.sbe.MarketDataTickDecoder;
import com.trading.engine.messages.sbe.MarketDataTickEncoder;
import com.trading.engine.messages.sbe.MessageHeaderDecoder;
import com.trading.engine.messages.sbe.MessageHeaderEncoder;
import java.nio.charset.StandardCharsets;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

/**
 * Codec round-trip tests for MarketDataTick (template 54).
 *
 * <p>Each test encodes every field with a distinct non-default value, wraps a fresh decoder over
 * the same buffer, and asserts that every decoded field is byte-for-byte identical to the encoded
 * value. Distinct values per field ensure that a wire-offset bug (field read from the wrong
 * position) is caught rather than silently passing due to coincidentally matching defaults.
 *
 * <p>Boundary cases (Long.MIN_VALUE, Long.MAX_VALUE, 0, PRICE_SCALE, PRICE_SCALE - 1) are exercised
 * in dedicated test methods to verify correct little-endian long serialisation at the extremes of
 * the signed 64-bit range.
 *
 * <p>Threading model: Not thread-safe — single-threaded JUnit test execution only. Allocation:
 * Allocates one {@code UnsafeBuffer} per test, reused across encode and decode phases.
 */
final class MarketDataTickRoundTripTest {

  /** Fixed-point scale factor: 1 unit = 10^-8. */
  private static final long PRICE_SCALE = 100_000_000L;

  /** 8 KiB — comfortably larger than the MarketDataTick frame (header + 64-byte block = 72 B). */
  private static final int BUF_SIZE = 8_192;

  // -------------------------------------------------------------------------
  // Template 54 — canonical round-trip with all distinct non-default values
  // -------------------------------------------------------------------------

  /**
   * Encodes a MarketDataTick with all eight fields set to distinct non-default values and asserts
   * every decoded field matches. Verifies templateId (54), schemaId (1), version (1) from the
   * message header, and that all field offsets in the 64-byte block are correct.
   */
  @Test
  void marketDataTick_roundTrip_allFieldsDistinct() {
    final var symbol = "EURUSD";
    final long bidPrice = 1_05000000_00L; // 1.05 × PRICE_SCALE^2 (intentionally non-canonical)
    final long askPrice = 1_05010000_00L;
    final long bidSize = 1_000_000_00_000_000L;
    final long askSize = 2_000_000_00_000_000L;
    final long symbolSeq = 4_242L;
    final long ingressNanos = 1_700_000_000_111_222_333L;
    final long serverNanos = 1_700_000_000_222_333_444L;

    final var buf = new UnsafeBuffer(new byte[BUF_SIZE]);
    final var headerEncoder = new MessageHeaderEncoder();
    final var encoder = new MarketDataTickEncoder();
    encoder.wrapAndApplyHeader(buf, 0, headerEncoder);

    encoder
        .symbol(symbol)
        .bidPrice(bidPrice)
        .askPrice(askPrice)
        .bidSize(bidSize)
        .askSize(askSize)
        .symbolSeq(symbolSeq)
        .ingressNanos(ingressNanos)
        .serverNanos(serverNanos);

    final var headerDecoder = new MessageHeaderDecoder();
    final var decoder = new MarketDataTickDecoder();
    decoder.wrapAndApplyHeader(buf, 0, headerDecoder);

    assertEquals(MarketDataTickDecoder.TEMPLATE_ID, headerDecoder.templateId(), "templateId");
    assertEquals(MarketDataTickDecoder.SCHEMA_ID, headerDecoder.schemaId(), "schemaId");
    assertEquals(MarketDataTickDecoder.SCHEMA_VERSION, headerDecoder.version(), "version");

    final var symDst = new byte[MarketDataTickDecoder.symbolLength()];
    decoder.getSymbol(symDst, 0);
    assertArrayEquals(padRight(symbol, MarketDataTickDecoder.symbolLength()), symDst, "symbol");

    assertEquals(bidPrice, decoder.bidPrice(), "bidPrice");
    assertEquals(askPrice, decoder.askPrice(), "askPrice");
    assertEquals(bidSize, decoder.bidSize(), "bidSize");
    assertEquals(askSize, decoder.askSize(), "askSize");
    assertEquals(symbolSeq, decoder.symbolSeq(), "symbolSeq");
    assertEquals(ingressNanos, decoder.ingressNanos(), "ingressNanos");
    assertEquals(serverNanos, decoder.serverNanos(), "serverNanos");
  }

  // -------------------------------------------------------------------------
  // Boundary: Long.MIN_VALUE on bidPrice, Long.MAX_VALUE on askPrice
  // -------------------------------------------------------------------------

  /**
   * Exercises the largest possible negative long ({@code Long.MIN_VALUE = -9223372036854775808}) on
   * {@code bidPrice} and the largest positive ({@code Long.MAX_VALUE = 9223372036854775807}) on
   * {@code askPrice}. Verifies that the little-endian serialisation does not corrupt the sign bit
   * or the msb.
   */
  @Test
  void marketDataTick_roundTrip_longBoundaries_minBidMaxAsk() {
    final long bidPrice = Long.MIN_VALUE;
    final long askPrice = Long.MAX_VALUE;
    final long bidSize = 0L;
    final long askSize = 0L;
    final long symbolSeq = 0L;
    final long ingressNanos = 0L;
    final long serverNanos = 0L;

    final var buf = new UnsafeBuffer(new byte[BUF_SIZE]);
    final var headerEncoder = new MessageHeaderEncoder();
    final var encoder = new MarketDataTickEncoder();
    encoder.wrapAndApplyHeader(buf, 0, headerEncoder);

    encoder
        .symbol("USDJPY")
        .bidPrice(bidPrice)
        .askPrice(askPrice)
        .bidSize(bidSize)
        .askSize(askSize)
        .symbolSeq(symbolSeq)
        .ingressNanos(ingressNanos)
        .serverNanos(serverNanos);

    final var headerDecoder = new MessageHeaderDecoder();
    final var decoder = new MarketDataTickDecoder();
    decoder.wrapAndApplyHeader(buf, 0, headerDecoder);

    assertEquals(bidPrice, decoder.bidPrice(), "bidPrice MIN_VALUE");
    assertEquals(askPrice, decoder.askPrice(), "askPrice MAX_VALUE");
    assertEquals(bidSize, decoder.bidSize(), "bidSize zero");
    assertEquals(askSize, decoder.askSize(), "askSize zero");
    assertEquals(symbolSeq, decoder.symbolSeq(), "symbolSeq zero");
    assertEquals(ingressNanos, decoder.ingressNanos(), "ingressNanos zero");
    assertEquals(serverNanos, decoder.serverNanos(), "serverNanos zero");
  }

  // -------------------------------------------------------------------------
  // Boundary: PRICE_SCALE and PRICE_SCALE - 1
  // -------------------------------------------------------------------------

  /**
   * Exercises {@code PRICE_SCALE = 100_000_000L} (1 whole unit in fixed-point 10^-8) on {@code
   * bidPrice} and {@code PRICE_SCALE - 1} (maximum fractional value below 1.0) on {@code askPrice}.
   * Distinct non-zero values on the remaining fields confirm no aliasing between adjacent long
   * slots in the block.
   */
  @Test
  void marketDataTick_roundTrip_priceScaleBoundaries() {
    final long bidPrice = PRICE_SCALE;
    final long askPrice = PRICE_SCALE - 1L;
    final long bidSize = 500_000_00_000_000L;
    final long askSize = 600_000_00_000_000L;
    final long symbolSeq = 99L;
    final long ingressNanos = 1_700_000_001_000_000_000L;
    final long serverNanos = 1_700_000_002_000_000_000L;

    final var buf = new UnsafeBuffer(new byte[BUF_SIZE]);
    final var headerEncoder = new MessageHeaderEncoder();
    final var encoder = new MarketDataTickEncoder();
    encoder.wrapAndApplyHeader(buf, 0, headerEncoder);

    encoder
        .symbol("GBPUSD")
        .bidPrice(bidPrice)
        .askPrice(askPrice)
        .bidSize(bidSize)
        .askSize(askSize)
        .symbolSeq(symbolSeq)
        .ingressNanos(ingressNanos)
        .serverNanos(serverNanos);

    final var headerDecoder = new MessageHeaderDecoder();
    final var decoder = new MarketDataTickDecoder();
    decoder.wrapAndApplyHeader(buf, 0, headerDecoder);

    assertEquals(bidPrice, decoder.bidPrice(), "bidPrice PRICE_SCALE");
    assertEquals(askPrice, decoder.askPrice(), "askPrice PRICE_SCALE-1");
    assertEquals(bidSize, decoder.bidSize(), "bidSize");
    assertEquals(askSize, decoder.askSize(), "askSize");
    assertEquals(symbolSeq, decoder.symbolSeq(), "symbolSeq");
    assertEquals(ingressNanos, decoder.ingressNanos(), "ingressNanos");
    assertEquals(serverNanos, decoder.serverNanos(), "serverNanos");
  }

  // -------------------------------------------------------------------------
  // Boundary: symbolSeq = Long.MIN_VALUE / Long.MAX_VALUE
  // -------------------------------------------------------------------------

  /**
   * Exercises {@code Long.MIN_VALUE} and {@code Long.MAX_VALUE} on the {@code symbolSeq} field (FIX
   * RptSeq tag 83) — the per-symbol monotonic gap-detection counter. The snapshot sentinel value
   * (0) is verified in {@link #marketDataTick_roundTrip_longBoundaries_minBidMaxAsk()}.
   */
  @Test
  void marketDataTick_roundTrip_symbolSeqBoundaries() {
    final long seqMin = Long.MIN_VALUE;
    final long seqMax = Long.MAX_VALUE;

    // MIN_VALUE test.
    {
      final var buf = new UnsafeBuffer(new byte[BUF_SIZE]);
      final var headerEncoder = new MessageHeaderEncoder();
      final var encoder = new MarketDataTickEncoder();
      encoder.wrapAndApplyHeader(buf, 0, headerEncoder);
      encoder
          .symbol("AUDUSD")
          .bidPrice(1L)
          .askPrice(2L)
          .bidSize(3L)
          .askSize(4L)
          .symbolSeq(seqMin)
          .ingressNanos(5L)
          .serverNanos(6L);

      final var headerDecoder = new MessageHeaderDecoder();
      final var decoder = new MarketDataTickDecoder();
      decoder.wrapAndApplyHeader(buf, 0, headerDecoder);
      assertEquals(seqMin, decoder.symbolSeq(), "symbolSeq MIN_VALUE");
    }

    // MAX_VALUE test.
    {
      final var buf = new UnsafeBuffer(new byte[BUF_SIZE]);
      final var headerEncoder = new MessageHeaderEncoder();
      final var encoder = new MarketDataTickEncoder();
      encoder.wrapAndApplyHeader(buf, 0, headerEncoder);
      encoder
          .symbol("NZDUSD")
          .bidPrice(10L)
          .askPrice(20L)
          .bidSize(30L)
          .askSize(40L)
          .symbolSeq(seqMax)
          .ingressNanos(50L)
          .serverNanos(60L);

      final var headerDecoder = new MessageHeaderDecoder();
      final var decoder = new MarketDataTickDecoder();
      decoder.wrapAndApplyHeader(buf, 0, headerDecoder);
      assertEquals(seqMax, decoder.symbolSeq(), "symbolSeq MAX_VALUE");
    }
  }

  // -------------------------------------------------------------------------
  // Symbol field: 8-char exact-length (no padding required)
  // -------------------------------------------------------------------------

  /**
   * Encodes an 8-character ASCII symbol (the maximum field capacity) and asserts that the decoded
   * raw 8 bytes are byte-identical to the encoded value with no trailing NUL padding. This case
   * exercises the path where the symbol fills the entire field without zero-fill.
   */
  @Test
  void marketDataTick_roundTrip_symbolExact8Chars() {
    final var symbol = "USDCADXX"; // exactly 8 ASCII characters
    assertEquals(8, symbol.length(), "test setup: symbol must be exactly 8 chars");

    final var buf = new UnsafeBuffer(new byte[BUF_SIZE]);
    final var headerEncoder = new MessageHeaderEncoder();
    final var encoder = new MarketDataTickEncoder();
    encoder.wrapAndApplyHeader(buf, 0, headerEncoder);
    encoder
        .symbol(symbol)
        .bidPrice(1_000_000L)
        .askPrice(2_000_000L)
        .bidSize(100L)
        .askSize(200L)
        .symbolSeq(7L)
        .ingressNanos(8L)
        .serverNanos(9L);

    final var headerDecoder = new MessageHeaderDecoder();
    final var decoder = new MarketDataTickDecoder();
    decoder.wrapAndApplyHeader(buf, 0, headerDecoder);

    final var symDst = new byte[MarketDataTickDecoder.symbolLength()];
    decoder.getSymbol(symDst, 0);
    assertArrayEquals(symbol.getBytes(StandardCharsets.US_ASCII), symDst, "symbol exact 8 chars");
  }

  // -------------------------------------------------------------------------
  // Helpers
  // -------------------------------------------------------------------------

  /**
   * Returns a NUL-padded byte array of exactly {@code width} bytes containing the ASCII encoding of
   * {@code value}. Mirrors the SBE encoder behaviour: characters are written left-justified and the
   * tail is zero-filled.
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
