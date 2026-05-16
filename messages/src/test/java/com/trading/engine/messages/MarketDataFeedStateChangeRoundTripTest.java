package com.trading.engine.messages;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.trading.engine.messages.sbe.FeedStateEnum;
import com.trading.engine.messages.sbe.MarketDataFeedStateChangeDecoder;
import com.trading.engine.messages.sbe.MarketDataFeedStateChangeEncoder;
import com.trading.engine.messages.sbe.MessageHeaderDecoder;
import com.trading.engine.messages.sbe.MessageHeaderEncoder;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

/**
 * Codec round-trip tests for MarketDataFeedStateChange (template 57).
 *
 * <p>Template 57 carries a one-byte {@link FeedStateEnum} field ({@code state}, id=1, offset=0)
 * followed by an eight-byte {@code serverNanos} (FIX SendingTime tag 52, offset=1). Tests verify
 * that each enum variant ({@code Live=0}, {@code Quiet=1}, {@code Stale=2}) is serialised to the
 * correct wire byte and round-trips back to the matching enum constant, and that {@code
 * serverNanos} is not corrupted by the adjacent single-byte field.
 *
 * <p>Threading model: Not thread-safe — single-threaded JUnit test execution only. Allocation:
 * Allocates one {@code UnsafeBuffer} per test, reused across encode and decode phases.
 */
final class MarketDataFeedStateChangeRoundTripTest {

  /** 8 KiB — more than sufficient for the 9-byte block + 8-byte header. */
  private static final int BUF_SIZE = 8_192;

  // -------------------------------------------------------------------------
  // Template 57 — FeedStateEnum.Live (wire byte = 0)
  // -------------------------------------------------------------------------

  /**
   * Encodes {@link FeedStateEnum#Live} with a distinct {@code serverNanos} value and asserts that
   * the decoder returns {@code Live} and the correct timestamp. The raw wire byte at block offset 0
   * must be {@code 0x00}.
   */
  @Test
  void marketDataFeedStateChange_roundTrip_live() {
    final long serverNanos = 1_700_000_001_111_222_333L;

    final var buf = new UnsafeBuffer(new byte[BUF_SIZE]);
    final var headerEncoder = new MessageHeaderEncoder();
    final var encoder = new MarketDataFeedStateChangeEncoder();
    encoder.wrapAndApplyHeader(buf, 0, headerEncoder);
    encoder.state(FeedStateEnum.Live).serverNanos(serverNanos);

    final var headerDecoder = new MessageHeaderDecoder();
    final var decoder = new MarketDataFeedStateChangeDecoder();
    decoder.wrapAndApplyHeader(buf, 0, headerDecoder);

    assertEquals(
        MarketDataFeedStateChangeDecoder.TEMPLATE_ID, headerDecoder.templateId(), "templateId");
    assertEquals(MarketDataFeedStateChangeDecoder.SCHEMA_ID, headerDecoder.schemaId(), "schemaId");
    assertEquals(
        MarketDataFeedStateChangeDecoder.SCHEMA_VERSION, headerDecoder.version(), "version");

    assertEquals(FeedStateEnum.Live, decoder.state(), "state enum");
    // Verify the raw wire byte to confirm the on-wire encoding is 0x00 for Live.
    assertEquals((short) 0, decoder.stateRaw(), "Live wire byte must be 0");
    assertEquals(serverNanos, decoder.serverNanos(), "serverNanos");
  }

  // -------------------------------------------------------------------------
  // Template 57 — FeedStateEnum.Quiet (wire byte = 1)
  // -------------------------------------------------------------------------

  /**
   * Encodes {@link FeedStateEnum#Quiet} and asserts that the decoder returns {@code Quiet}. The raw
   * wire byte at offset 0 must be {@code 0x01}.
   */
  @Test
  void marketDataFeedStateChange_roundTrip_quiet() {
    final long serverNanos = 1_700_000_002_222_333_444L;

    final var buf = new UnsafeBuffer(new byte[BUF_SIZE]);
    final var headerEncoder = new MessageHeaderEncoder();
    final var encoder = new MarketDataFeedStateChangeEncoder();
    encoder.wrapAndApplyHeader(buf, 0, headerEncoder);
    encoder.state(FeedStateEnum.Quiet).serverNanos(serverNanos);

    final var headerDecoder = new MessageHeaderDecoder();
    final var decoder = new MarketDataFeedStateChangeDecoder();
    decoder.wrapAndApplyHeader(buf, 0, headerDecoder);

    assertEquals(FeedStateEnum.Quiet, decoder.state(), "state enum");
    assertEquals((short) 1, decoder.stateRaw(), "Quiet wire byte must be 1");
    assertEquals(serverNanos, decoder.serverNanos(), "serverNanos");
  }

  // -------------------------------------------------------------------------
  // Template 57 — FeedStateEnum.Stale (wire byte = 2)
  // -------------------------------------------------------------------------

  /**
   * Encodes {@link FeedStateEnum#Stale} and asserts that the decoder returns {@code Stale}. The raw
   * wire byte at offset 0 must be {@code 0x02}. Uses a distinct {@code serverNanos} value to rule
   * out aliasing with the state byte.
   */
  @Test
  void marketDataFeedStateChange_roundTrip_stale() {
    final long serverNanos = 1_700_000_003_333_444_555L;

    final var buf = new UnsafeBuffer(new byte[BUF_SIZE]);
    final var headerEncoder = new MessageHeaderEncoder();
    final var encoder = new MarketDataFeedStateChangeEncoder();
    encoder.wrapAndApplyHeader(buf, 0, headerEncoder);
    encoder.state(FeedStateEnum.Stale).serverNanos(serverNanos);

    final var headerDecoder = new MessageHeaderDecoder();
    final var decoder = new MarketDataFeedStateChangeDecoder();
    decoder.wrapAndApplyHeader(buf, 0, headerDecoder);

    assertEquals(FeedStateEnum.Stale, decoder.state(), "state enum");
    assertEquals((short) 2, decoder.stateRaw(), "Stale wire byte must be 2");
    assertEquals(serverNanos, decoder.serverNanos(), "serverNanos");
  }

  // -------------------------------------------------------------------------
  // Template 57 — serverNanos boundary: Long.MIN_VALUE and Long.MAX_VALUE
  // -------------------------------------------------------------------------

  /**
   * Verifies that {@code serverNanos} round-trips correctly at {@link Long#MIN_VALUE} and {@link
   * Long#MAX_VALUE}. Uses {@code FeedStateEnum.Live} (wire byte 0) in both cases so the focus is on
   * the 8-byte field adjacent to the single-byte enum.
   */
  @Test
  void marketDataFeedStateChange_roundTrip_serverNanosBoundaries() {
    // MIN_VALUE.
    {
      final var buf = new UnsafeBuffer(new byte[BUF_SIZE]);
      final var headerEncoder = new MessageHeaderEncoder();
      final var encoder = new MarketDataFeedStateChangeEncoder();
      encoder.wrapAndApplyHeader(buf, 0, headerEncoder);
      encoder.state(FeedStateEnum.Live).serverNanos(Long.MIN_VALUE);

      final var headerDecoder = new MessageHeaderDecoder();
      final var decoder = new MarketDataFeedStateChangeDecoder();
      decoder.wrapAndApplyHeader(buf, 0, headerDecoder);

      assertEquals(FeedStateEnum.Live, decoder.state(), "state at MIN_VALUE serverNanos");
      assertEquals(Long.MIN_VALUE, decoder.serverNanos(), "serverNanos MIN_VALUE");
    }

    // MAX_VALUE.
    {
      final var buf = new UnsafeBuffer(new byte[BUF_SIZE]);
      final var headerEncoder = new MessageHeaderEncoder();
      final var encoder = new MarketDataFeedStateChangeEncoder();
      encoder.wrapAndApplyHeader(buf, 0, headerEncoder);
      encoder.state(FeedStateEnum.Stale).serverNanos(Long.MAX_VALUE);

      final var headerDecoder = new MessageHeaderDecoder();
      final var decoder = new MarketDataFeedStateChangeDecoder();
      decoder.wrapAndApplyHeader(buf, 0, headerDecoder);

      assertEquals(FeedStateEnum.Stale, decoder.state(), "state at MAX_VALUE serverNanos");
      assertEquals(Long.MAX_VALUE, decoder.serverNanos(), "serverNanos MAX_VALUE");
    }
  }
}
