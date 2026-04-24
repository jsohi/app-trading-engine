package com.trading.engine.websocket;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.trading.engine.messages.sbe.ClientAckDecoder;
import com.trading.engine.messages.sbe.ClientAckEncoder;
import com.trading.engine.messages.sbe.ClientHeartbeatDecoder;
import com.trading.engine.messages.sbe.ClientHeartbeatEncoder;
import com.trading.engine.messages.sbe.CommandAckDecoder;
import com.trading.engine.messages.sbe.CommandAckEncoder;
import com.trading.engine.messages.sbe.CommandAckStatus;
import com.trading.engine.messages.sbe.MessageHeaderDecoder;
import com.trading.engine.messages.sbe.MessageHeaderEncoder;
import com.trading.engine.messages.sbe.ReplayCompleteDecoder;
import com.trading.engine.messages.sbe.ReplayCompleteEncoder;
import com.trading.engine.messages.sbe.SessionResumeDecoder;
import com.trading.engine.messages.sbe.SessionResumeEncoder;
import com.trading.engine.messages.sbe.WebSocketAuthAckDecoder;
import com.trading.engine.messages.sbe.WebSocketAuthAckEncoder;
import com.trading.engine.messages.sbe.WebSocketAuthDecoder;
import com.trading.engine.messages.sbe.WebSocketAuthEncoder;
import com.trading.engine.messages.sbe.WebSocketErrorCode;
import com.trading.engine.messages.sbe.WebSocketErrorDecoder;
import com.trading.engine.messages.sbe.WebSocketErrorEncoder;
import com.trading.engine.messages.sbe.WebSocketGapRequestDecoder;
import com.trading.engine.messages.sbe.WebSocketGapRequestEncoder;
import com.trading.engine.messages.sbe.WebSocketHeartbeatDecoder;
import com.trading.engine.messages.sbe.WebSocketHeartbeatEncoder;
import com.trading.engine.messages.sbe.WebSocketSnapshotDecoder;
import com.trading.engine.messages.sbe.WebSocketSnapshotEncoder;
import com.trading.engine.messages.sbe.WebSocketSubscribeDecoder;
import com.trading.engine.messages.sbe.WebSocketSubscribeEncoder;
import com.trading.engine.messages.sbe.WebSocketUnsubscribeDecoder;
import com.trading.engine.messages.sbe.WebSocketUnsubscribeEncoder;
import java.nio.charset.StandardCharsets;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.junit.jupiter.api.Test;

/**
 * SBE encode/decode roundtrip smoke tests for all WebSocket templates (60-72).
 *
 * <p>Verifies that SBE codegen produced correct encoders/decoders for the new schema additions:
 * varDataEncoding composite, uuid composite, WebSocketErrorCode enum, CommandAckStatus enum, and
 * all 13 message templates.
 */
final class WebSocketSbeRoundtripTest {

  private final MutableDirectBuffer buffer = new ExpandableArrayBuffer(512);
  private final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
  private final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();

  @Test
  void webSocketAuth_roundtrip() {
    final byte[] token = "eyJhbGciOiJSUzI1NiJ9.test-payload".getBytes(StandardCharsets.UTF_8);
    final var encoder = new WebSocketAuthEncoder();
    encoder.wrapAndApplyHeader(buffer, 0, headerEncoder);
    encoder.protocolVersion(1);
    encoder.putToken(token, 0, token.length);

    headerDecoder.wrap(buffer, 0);
    assertEquals(WebSocketAuthEncoder.TEMPLATE_ID, headerDecoder.templateId());

    final var decoder = new WebSocketAuthDecoder();
    decoder.wrapAndApplyHeader(buffer, 0, headerDecoder);
    assertEquals(1, decoder.protocolVersion());
    assertEquals(token.length, decoder.tokenLength());
    final byte[] decoded = new byte[decoder.tokenLength()];
    decoder.getToken(decoded, 0, decoded.length);
    assertEquals(
        new String(token, StandardCharsets.UTF_8), new String(decoded, StandardCharsets.UTF_8));
  }

  @Test
  void webSocketAuthAck_roundtrip() {
    final var encoder = new WebSocketAuthAckEncoder();
    encoder.wrapAndApplyHeader(buffer, 0, headerEncoder);
    encoder.sessionId().mostSignificantBits(0x1234_5678_9ABC_DEF0L);
    encoder.sessionId().leastSignificantBits(0xFEDC_BA98_7654_3210L);
    encoder.protocolVersion(1);
    encoder.maxSubscriptions(100);

    headerDecoder.wrap(buffer, 0);
    assertEquals(WebSocketAuthAckEncoder.TEMPLATE_ID, headerDecoder.templateId());

    final var decoder = new WebSocketAuthAckDecoder();
    decoder.wrapAndApplyHeader(buffer, 0, headerDecoder);
    assertEquals(0x1234_5678_9ABC_DEF0L, decoder.sessionId().mostSignificantBits());
    assertEquals(0xFEDC_BA98_7654_3210L, decoder.sessionId().leastSignificantBits());
    assertEquals(1, decoder.protocolVersion());
    assertEquals(100, decoder.maxSubscriptions());
  }

  @Test
  void webSocketSubscribe_roundtrip_withGroup() {
    final var encoder = new WebSocketSubscribeEncoder();
    encoder.wrapAndApplyHeader(buffer, 0, headerEncoder);
    final var group = encoder.symbolsCount(2);
    group.next().symbol("EURUSD  ").eventTypes(0x0000_00FF);
    group.next().symbol("GBPUSD  ").eventTypes(0x0000_000F);

    headerDecoder.wrap(buffer, 0);
    assertEquals(WebSocketSubscribeEncoder.TEMPLATE_ID, headerDecoder.templateId());

    final var decoder = new WebSocketSubscribeDecoder();
    decoder.wrapAndApplyHeader(buffer, 0, headerDecoder);
    final var symbols = decoder.symbols();
    assertEquals(2, symbols.count());
    symbols.next();
    assertEquals("EURUSD  ", symbols.symbol());
    assertEquals(0x0000_00FF, symbols.eventTypes());
    symbols.next();
    assertEquals("GBPUSD  ", symbols.symbol());
    assertEquals(0x0000_000F, symbols.eventTypes());
  }

  @Test
  void webSocketUnsubscribe_roundtrip_withGroup() {
    final var encoder = new WebSocketUnsubscribeEncoder();
    encoder.wrapAndApplyHeader(buffer, 0, headerEncoder);
    encoder.symbolsCount(1).next().symbol("USDJPY  ");

    headerDecoder.wrap(buffer, 0);
    final var decoder = new WebSocketUnsubscribeDecoder();
    decoder.wrapAndApplyHeader(buffer, 0, headerDecoder);
    final var symbols = decoder.symbols();
    assertEquals(1, symbols.count());
    symbols.next();
    assertEquals("USDJPY  ", symbols.symbol());
  }

  @Test
  void webSocketHeartbeat_roundtrip() {
    final var encoder = new WebSocketHeartbeatEncoder();
    encoder.wrapAndApplyHeader(buffer, 0, headerEncoder);
    encoder.serverNanos(1_700_000_000_000_000_000L);

    headerDecoder.wrap(buffer, 0);
    assertEquals(WebSocketHeartbeatEncoder.TEMPLATE_ID, headerDecoder.templateId());

    final var decoder = new WebSocketHeartbeatDecoder();
    decoder.wrapAndApplyHeader(buffer, 0, headerDecoder);
    assertEquals(1_700_000_000_000_000_000L, decoder.serverNanos());
  }

  @Test
  void clientHeartbeat_roundtrip() {
    final var encoder = new ClientHeartbeatEncoder();
    encoder.wrapAndApplyHeader(buffer, 0, headerEncoder);
    encoder.clientNanos(42_000_000L);

    headerDecoder.wrap(buffer, 0);
    final var decoder = new ClientHeartbeatDecoder();
    decoder.wrapAndApplyHeader(buffer, 0, headerDecoder);
    assertEquals(42_000_000L, decoder.clientNanos());
  }

  @Test
  void webSocketSnapshot_roundtrip_withVarData() {
    final byte[] payload = new byte[] {0x01, 0x02, 0x03, 0x04, 0x05};
    final var encoder = new WebSocketSnapshotEncoder();
    encoder.wrapAndApplyHeader(buffer, 0, headerEncoder);
    encoder.snapshotId().mostSignificantBits(0xAAAA_BBBB_CCCC_DDDDL);
    encoder.snapshotId().leastSignificantBits(0x1111_2222_3333_4444L);
    encoder.fragmentIndex(0);
    encoder.totalFragments(3);
    encoder.putPayload(payload, 0, payload.length);

    headerDecoder.wrap(buffer, 0);
    assertEquals(WebSocketSnapshotEncoder.TEMPLATE_ID, headerDecoder.templateId());

    final var decoder = new WebSocketSnapshotDecoder();
    decoder.wrapAndApplyHeader(buffer, 0, headerDecoder);
    assertEquals(0xAAAA_BBBB_CCCC_DDDDL, decoder.snapshotId().mostSignificantBits());
    assertEquals(0, decoder.fragmentIndex());
    assertEquals(3, decoder.totalFragments());
    assertEquals(5, decoder.payloadLength());
  }

  @Test
  void webSocketError_roundtrip_withEnum() {
    final byte[] text = "Authentication failed".getBytes(StandardCharsets.UTF_8);
    final var encoder = new WebSocketErrorEncoder();
    encoder.wrapAndApplyHeader(buffer, 0, headerEncoder);
    encoder.errorCode(WebSocketErrorCode.AuthenticationFailed);
    encoder.putErrorText(text, 0, text.length);

    headerDecoder.wrap(buffer, 0);
    final var decoder = new WebSocketErrorDecoder();
    decoder.wrapAndApplyHeader(buffer, 0, headerDecoder);
    assertEquals(WebSocketErrorCode.AuthenticationFailed, decoder.errorCode());
    assertEquals(text.length, decoder.errorTextLength());
  }

  @Test
  void webSocketGapRequest_roundtrip() {
    final var encoder = new WebSocketGapRequestEncoder();
    encoder.wrapAndApplyHeader(buffer, 0, headerEncoder);
    encoder.fromSeqNo(100);
    encoder.toSeqNo(150);

    headerDecoder.wrap(buffer, 0);
    final var decoder = new WebSocketGapRequestDecoder();
    decoder.wrapAndApplyHeader(buffer, 0, headerDecoder);
    assertEquals(100, decoder.fromSeqNo());
    assertEquals(150, decoder.toSeqNo());
  }

  @Test
  void sessionResume_roundtrip_withUuid() {
    final var encoder = new SessionResumeEncoder();
    encoder.wrapAndApplyHeader(buffer, 0, headerEncoder);
    encoder.sessionId().mostSignificantBits(Long.MAX_VALUE);
    encoder.sessionId().leastSignificantBits(Long.MIN_VALUE);
    encoder.lastSeqNo(999);

    headerDecoder.wrap(buffer, 0);
    final var decoder = new SessionResumeDecoder();
    decoder.wrapAndApplyHeader(buffer, 0, headerDecoder);
    assertEquals(Long.MAX_VALUE, decoder.sessionId().mostSignificantBits());
    assertEquals(Long.MIN_VALUE, decoder.sessionId().leastSignificantBits());
    assertEquals(999, decoder.lastSeqNo());
  }

  @Test
  void commandAck_roundtrip_withEnum() {
    final var encoder = new CommandAckEncoder();
    encoder.wrapAndApplyHeader(buffer, 0, headerEncoder);
    encoder.clientCmdSeqNo(42);
    encoder.status(CommandAckStatus.Throttled);

    headerDecoder.wrap(buffer, 0);
    final var decoder = new CommandAckDecoder();
    decoder.wrapAndApplyHeader(buffer, 0, headerDecoder);
    assertEquals(42, decoder.clientCmdSeqNo());
    assertEquals(CommandAckStatus.Throttled, decoder.status());
  }

  @Test
  void clientAck_roundtrip() {
    final var encoder = new ClientAckEncoder();
    encoder.wrapAndApplyHeader(buffer, 0, headerEncoder);
    encoder.lastReceivedSeqNo(12345);

    headerDecoder.wrap(buffer, 0);
    final var decoder = new ClientAckDecoder();
    decoder.wrapAndApplyHeader(buffer, 0, headerDecoder);
    assertEquals(12345, decoder.lastReceivedSeqNo());
  }

  @Test
  void replayComplete_roundtrip_emptyBody() {
    final var encoder = new ReplayCompleteEncoder();
    encoder.wrapAndApplyHeader(buffer, 0, headerEncoder);

    headerDecoder.wrap(buffer, 0);
    assertEquals(ReplayCompleteEncoder.TEMPLATE_ID, headerDecoder.templateId());

    final var decoder = new ReplayCompleteDecoder();
    decoder.wrapAndApplyHeader(buffer, 0, headerDecoder);
    // Empty body — just verifying the header is correct
    assertEquals(72, ReplayCompleteEncoder.TEMPLATE_ID);
  }

  @Test
  void templateIds_matchArchitectureDocReservedRange() {
    assertEquals(60, WebSocketAuthEncoder.TEMPLATE_ID);
    assertEquals(61, WebSocketAuthAckEncoder.TEMPLATE_ID);
    assertEquals(62, WebSocketSubscribeEncoder.TEMPLATE_ID);
    assertEquals(63, WebSocketUnsubscribeEncoder.TEMPLATE_ID);
    assertEquals(64, WebSocketHeartbeatEncoder.TEMPLATE_ID);
    assertEquals(65, ClientHeartbeatEncoder.TEMPLATE_ID);
    assertEquals(66, WebSocketSnapshotEncoder.TEMPLATE_ID);
    assertEquals(67, WebSocketErrorEncoder.TEMPLATE_ID);
    assertEquals(68, WebSocketGapRequestEncoder.TEMPLATE_ID);
    assertEquals(69, SessionResumeEncoder.TEMPLATE_ID);
    assertEquals(70, CommandAckEncoder.TEMPLATE_ID);
    assertEquals(71, ClientAckEncoder.TEMPLATE_ID);
    assertEquals(72, ReplayCompleteEncoder.TEMPLATE_ID);
  }
}
