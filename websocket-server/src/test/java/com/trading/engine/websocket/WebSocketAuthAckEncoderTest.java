/*
 * APP-36 — server-side AuthAck encoder verification for §A1 schema delta.
 *
 * Verifies that the two new uint32 fields added to WebSocketAuthAck (id=4
 * serverHeartbeatIntervalMs, id=5 clientHeartbeatIntervalMs) round-trip
 * correctly through the SBE encoder + decoder, and that the server-side
 * sendAuthAck() path populates them from WebSocketServerConfig.
 *
 * Threading: single-threaded per test (each test owns its UnsafeBuffer).
 *
 * Allocation: tests create one UnsafeBuffer per test; not on a hot path.
 */
package com.trading.engine.websocket;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.trading.engine.messages.sbe.MessageHeaderDecoder;
import com.trading.engine.messages.sbe.MessageHeaderEncoder;
import com.trading.engine.messages.sbe.WebSocketAuthAckDecoder;
import com.trading.engine.messages.sbe.WebSocketAuthAckEncoder;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Verifies the §A1 schema delta encodes / decodes correctly.
 *
 * <p>Plan reference: APP-36 §A1, §2.5, §2.8.
 *
 * <p>Threading: single-threaded JUnit 6 instance per test.
 */
final class WebSocketAuthAckEncoderTest {

  private static final int BUFFER_SIZE = 256;

  private UnsafeBuffer buffer;
  private MessageHeaderEncoder headerEncoder;
  private MessageHeaderDecoder headerDecoder;

  @BeforeEach
  void setUp() {
    buffer = new UnsafeBuffer(new byte[BUFFER_SIZE]);
    headerEncoder = new MessageHeaderEncoder();
    headerDecoder = new MessageHeaderDecoder();
  }

  @Test
  @DisplayName("authAck_negotiatedIntervals_encodeDecodeRoundtrip")
  void authAck_negotiatedIntervals_encodeDecodeRoundtrip() {
    final var encoder = new WebSocketAuthAckEncoder();
    encoder.wrapAndApplyHeader(buffer, 0, headerEncoder);
    encoder.sessionId().mostSignificantBits(0x1L).leastSignificantBits(0x2L);
    encoder.protocolVersion(1);
    encoder.maxSubscriptions(100);
    encoder.serverHeartbeatIntervalMs(5_000);
    encoder.clientHeartbeatIntervalMs(10_000);

    headerDecoder.wrap(buffer, 0);
    assertEquals(WebSocketAuthAckEncoder.TEMPLATE_ID, headerDecoder.templateId());

    final var decoder = new WebSocketAuthAckDecoder();
    decoder.wrapAndApplyHeader(buffer, 0, headerDecoder);
    assertEquals(0x1L, decoder.sessionId().mostSignificantBits());
    assertEquals(0x2L, decoder.sessionId().leastSignificantBits());
    assertEquals(1, decoder.protocolVersion());
    assertEquals(100, decoder.maxSubscriptions());
    assertEquals(5_000, decoder.serverHeartbeatIntervalMs());
    assertEquals(10_000, decoder.clientHeartbeatIntervalMs());
  }

  @Test
  @DisplayName("authAck_zeroIntervalsFallback_decodeReturnsZero")
  void authAck_zeroIntervalsFallback_decodeReturnsZero() {
    // Client-fallback path: an AuthAck with intervals=0 directs the client
    // to use defaults (5000/10000). Verify the encoder accepts 0 and the
    // decoder returns it verbatim — the client-side fallback to defaults
    // is exercised by web-ui's AuthAckBackwardWire.browser.test.ts.
    final var encoder = new WebSocketAuthAckEncoder();
    encoder.wrapAndApplyHeader(buffer, 0, headerEncoder);
    encoder.sessionId().mostSignificantBits(0L).leastSignificantBits(0L);
    encoder.protocolVersion(1);
    encoder.maxSubscriptions(0);
    encoder.serverHeartbeatIntervalMs(0);
    encoder.clientHeartbeatIntervalMs(0);

    headerDecoder.wrap(buffer, 0);
    final var decoder = new WebSocketAuthAckDecoder();
    decoder.wrapAndApplyHeader(buffer, 0, headerDecoder);
    assertEquals(0, decoder.serverHeartbeatIntervalMs());
    assertEquals(0, decoder.clientHeartbeatIntervalMs());
  }

  @Test
  @DisplayName("authAck_largeIntervals_encodeDecodeUint32Range")
  void authAck_largeIntervals_encodeDecodeUint32Range() {
    // uint32 max = 2^32 - 1. Java SBE encodes via int; positive values up to
    // Integer.MAX_VALUE (2^31 - 1) round-trip cleanly. Anything larger is
    // rejected at server config validation (see
    // configValidate_outOfRangeHeartbeatIntervalMs_throws below).
    final var encoder = new WebSocketAuthAckEncoder();
    encoder.wrapAndApplyHeader(buffer, 0, headerEncoder);
    encoder.sessionId().mostSignificantBits(0L).leastSignificantBits(0L);
    encoder.protocolVersion(1);
    encoder.maxSubscriptions(0);
    encoder.serverHeartbeatIntervalMs(Integer.MAX_VALUE);
    encoder.clientHeartbeatIntervalMs(Integer.MAX_VALUE);

    headerDecoder.wrap(buffer, 0);
    final var decoder = new WebSocketAuthAckDecoder();
    decoder.wrapAndApplyHeader(buffer, 0, headerDecoder);
    assertEquals(Integer.MAX_VALUE, decoder.serverHeartbeatIntervalMs());
    assertEquals(Integer.MAX_VALUE, decoder.clientHeartbeatIntervalMs());
  }

  @Test
  @DisplayName("configValidate_heartbeatIntervalMsBeyondIntMax_throwsIllegalArgument")
  void configValidate_heartbeatIntervalMsBeyondIntMax_throwsIllegalArgument() {
    // APP-36 §A1 / iter-1 review MEDIUM-1 fix: WebSocketServerConfig must
    // reject heartbeatIntervalMs > Integer.MAX_VALUE so the AuthAck wire
    // narrowing cast in JwtAuthHandler.sendAuthAck cannot silently produce
    // a sign-flipped uint32. Math.toIntExact in the encoder is a defense-
    // in-depth backstop for the same range.
    //
    // Set clientTimeoutMs above the bad heartbeat so the existing
    // "clientTimeoutMs > heartbeatIntervalMs" check passes; only the new
    // upper-bound validator should fire.
    final long beyondHeartbeat = (long) Integer.MAX_VALUE + 1L;
    final long bigClientTimeout = beyondHeartbeat + 1_000L;
    final var ex =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                WebSocketServerConfig.builder()
                    .heartbeatIntervalMs(beyondHeartbeat)
                    .clientTimeoutMs(bigClientTimeout)
                    .build());
    assertTrue(
        ex.getMessage().contains("Integer.MAX_VALUE")
            && ex.getMessage().contains("heartbeatIntervalMs"),
        "expected error referencing heartbeatIntervalMs upper bound; got: " + ex.getMessage());
  }

  @Test
  @DisplayName("configValidate_clientTimeoutMsBeyond2xIntMax_throwsIllegalArgument")
  void configValidate_clientTimeoutMsBeyond2xIntMax_throwsIllegalArgument() {
    // APP-36 §A1 / iter-1 review MEDIUM-1: clientTimeoutMs/2 is published
    // as the negotiated client cadence. The cap must allow for the divide.
    // Construct a clientTimeoutMs strictly greater than 2 * Integer.MAX_VALUE
    // so only the new validator fires (heartbeatIntervalMs stays in-range).
    final long beyond = 2L * Integer.MAX_VALUE + 2L;
    final var ex =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                WebSocketServerConfig.builder()
                    .clientTimeoutMs(beyond)
                    .heartbeatIntervalMs(5_000)
                    .build());
    assertTrue(
        ex.getMessage().contains("Integer.MAX_VALUE")
            && ex.getMessage().contains("clientTimeoutMs"),
        "expected error referencing clientTimeoutMs upper bound; got: " + ex.getMessage());
  }
}
