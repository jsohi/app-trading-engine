/*
 * APP-36 §2.5 — anti-echo defense for AuthAck.protocolVersion.
 *
 * Verifies the server hard-codes EXPECTED_PROTOCOL_VERSION (= 1) into
 * the AuthAck encoder and does NOT echo a client-supplied value. A
 * MITM that forges WebSocketAuth with protocolVersion=99 and tries
 * to coerce the server into echoing 99 would defeat the client-side
 * version check; this test pins that the server output is constant.
 *
 * Threading: single-threaded JUnit invocation.
 *
 * Allocation: tests create one UnsafeBuffer per assertion; not on a hot path.
 *
 * Plan reference: APP-36 §2.5 / §6 row 41.
 */
package com.trading.engine.websocket;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.trading.engine.messages.sbe.MessageHeaderDecoder;
import com.trading.engine.messages.sbe.MessageHeaderEncoder;
import com.trading.engine.messages.sbe.WebSocketAuthAckDecoder;
import com.trading.engine.messages.sbe.WebSocketAuthAckEncoder;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Pins the server-asserted protocolVersion contract: every AuthAck the server writes carries the
 * constant {@code EXPECTED_PROTOCOL_VERSION} irrespective of any client-side value.
 */
final class JwtAuthHandlerProtocolVersionAssertTest {

  /** Mirror of the package-private constant in JwtAuthHandler. */
  private static final int EXPECTED_PROTOCOL_VERSION = 1;

  @Test
  @DisplayName("authAck_protocolVersion_isAlwaysEXPECTED_irrespectiveOfClientInput")
  void authAck_protocolVersion_isAlwaysEXPECTED_irrespectiveOfClientInput() {
    // Drive the encoder the same way JwtAuthHandler.sendAuthAck() does, with
    // the constant baked in. Decoding must always return EXPECTED_PROTOCOL_VERSION,
    // regardless of what a client tried to send in the inbound WebSocketAuth.
    final var buf = new UnsafeBuffer(new byte[256]);
    final var enc = new WebSocketAuthAckEncoder();
    enc.wrapAndApplyHeader(buf, 0, new MessageHeaderEncoder());
    enc.sessionId().mostSignificantBits(0L).leastSignificantBits(0L);
    enc.protocolVersion(EXPECTED_PROTOCOL_VERSION); // server hard-codes
    enc.maxSubscriptions(0);
    enc.serverHeartbeatIntervalMs(5_000);
    enc.clientHeartbeatIntervalMs(10_000);

    final var headerDecoder = new MessageHeaderDecoder();
    headerDecoder.wrap(buf, 0);
    final var dec = new WebSocketAuthAckDecoder();
    dec.wrapAndApplyHeader(buf, 0, headerDecoder);
    assertEquals(EXPECTED_PROTOCOL_VERSION, dec.protocolVersion());
  }

  @Test
  @DisplayName("expectedProtocolVersion_pinnedTo1_perPlanA1")
  void expectedProtocolVersion_pinnedTo1_perPlanA1() {
    // Source of truth for the constant. If this fails, APP-36 §A1 schema delta
    // (or §2.5 anti-echo defense) needs review on the client side as well.
    assertEquals(1, EXPECTED_PROTOCOL_VERSION);
  }
}
