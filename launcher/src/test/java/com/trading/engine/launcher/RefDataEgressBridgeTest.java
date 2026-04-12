package com.trading.engine.launcher;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.trading.engine.messages.sbe.AccountLoadRejectedEventEncoder;
import com.trading.engine.messages.sbe.AccountLoadedEventEncoder;
import com.trading.engine.messages.sbe.CurrencyLoadRejectedEventEncoder;
import com.trading.engine.messages.sbe.CurrencyLoadedEventEncoder;
import com.trading.engine.messages.sbe.MessageHeaderDecoder;
import com.trading.engine.messages.sbe.MessageHeaderEncoder;
import com.trading.engine.messages.sbe.RiskLimitLoadRejectedEventEncoder;
import com.trading.engine.messages.sbe.RiskLimitLoadedEventEncoder;
import com.trading.refdata.ResponseCollector;
import io.aeron.logbuffer.ControlledFragmentHandler.Action;
import org.agrona.ExpandableDirectByteBuffer;
import org.agrona.MutableDirectBuffer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link RefDataEgressBridge}. Uses actual SBE encoders to build realistic test buffers,
 * verifying that concatenated messages are correctly iterated and routed to the {@link
 * ResponseCollector}.
 */
class RefDataEgressBridgeTest {

  private static final int HEADER_LENGTH = MessageHeaderDecoder.ENCODED_LENGTH;

  private final MutableDirectBuffer buffer = new ExpandableDirectByteBuffer(4096);
  private final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
  private final ResponseCollector collector = new ResponseCollector();
  private final RefDataEgressBridge bridge = new RefDataEgressBridge(collector);

  @BeforeEach
  void setUp() {
    collector.expectResponses(100); // large enough for any test
  }

  @Test
  void constructor_nullCollector_throwsNpe() {
    assertThrows(NullPointerException.class, () -> new RefDataEgressBridge(null));
  }

  // ===== Single event routing =====

  @Test
  void onMessage_singleAccountLoadedEvent_routesToOnLoaded() {
    final int length = encodeAccountLoaded(buffer, 0);

    final var result = bridge.onMessage(1L, 0L, buffer, 0, length, null);

    assertEquals(Action.CONTINUE, result);
    assertEquals(1, collector.loadedCount());
    assertEquals(0, collector.rejectedCount());
  }

  @Test
  void onMessage_singleAccountLoadRejectedEvent_routesToOnRejected() {
    final int length = encodeAccountLoadRejected(buffer, 0, "duplicate account");

    final var result = bridge.onMessage(1L, 0L, buffer, 0, length, null);

    assertEquals(Action.CONTINUE, result);
    assertEquals(0, collector.loadedCount());
    assertEquals(1, collector.rejectedCount());
    assertEquals("duplicate account", collector.rejectionReasons().getFirst());
  }

  @Test
  void onMessage_singleCurrencyLoadedEvent_routesToOnLoaded() {
    final int length = encodeCurrencyLoaded(buffer, 0);

    bridge.onMessage(1L, 0L, buffer, 0, length, null);

    assertEquals(1, collector.loadedCount());
  }

  @Test
  void onMessage_singleCurrencyLoadRejectedEvent_routesToOnRejected() {
    final int length = encodeCurrencyLoadRejected(buffer, 0, "invalid ISO code");

    bridge.onMessage(1L, 0L, buffer, 0, length, null);

    assertEquals(1, collector.rejectedCount());
    assertEquals("invalid ISO code", collector.rejectionReasons().getFirst());
  }

  @Test
  void onMessage_singleRiskLimitLoadedEvent_routesToOnLoaded() {
    final int length = encodeRiskLimitLoaded(buffer, 0);

    bridge.onMessage(1L, 0L, buffer, 0, length, null);

    assertEquals(1, collector.loadedCount());
  }

  @Test
  void onMessage_singleRiskLimitLoadRejectedEvent_routesToOnRejected() {
    final int length = encodeRiskLimitLoadRejected(buffer, 0, "limit too high");

    bridge.onMessage(1L, 0L, buffer, 0, length, null);

    assertEquals(1, collector.rejectedCount());
    assertEquals("limit too high", collector.rejectionReasons().getFirst());
  }

  // ===== Batch processing =====

  @Test
  void onMessage_batchOfThreeLoadedEvents_onLoadedCalledThreeTimes() {
    int offset = 0;
    offset += encodeAccountLoaded(buffer, offset);
    offset += encodeAccountLoaded(buffer, offset);
    offset += encodeAccountLoaded(buffer, offset);

    bridge.onMessage(1L, 0L, buffer, 0, offset, null);

    assertEquals(3, collector.loadedCount());
    assertEquals(0, collector.rejectedCount());
  }

  @Test
  void onMessage_mixedBatch_twoLoadedOneRejected_correctRouting() {
    int offset = 0;
    offset += encodeAccountLoaded(buffer, offset);
    offset += encodeAccountLoaded(buffer, offset);
    offset += encodeAccountLoadRejected(buffer, offset, "bad account");

    bridge.onMessage(1L, 0L, buffer, 0, offset, null);

    assertEquals(2, collector.loadedCount());
    assertEquals(1, collector.rejectedCount());
  }

  @Test
  void onMessage_mixedBatch_allSixEventTypes_correctRouting() {
    int offset = 0;
    offset += encodeAccountLoaded(buffer, offset);
    offset += encodeAccountLoadRejected(buffer, offset, "acct reject");
    offset += encodeCurrencyLoaded(buffer, offset);
    offset += encodeCurrencyLoadRejected(buffer, offset, "ccy reject");
    offset += encodeRiskLimitLoaded(buffer, offset);
    offset += encodeRiskLimitLoadRejected(buffer, offset, "risk reject");

    bridge.onMessage(1L, 0L, buffer, 0, offset, null);

    assertEquals(3, collector.loadedCount());
    assertEquals(3, collector.rejectedCount());
  }

  // ===== Edge cases =====

  @Test
  void onMessage_unknownTemplateId_ignoredNoCollectorCalls() {
    // Write a header with an unknown template ID (999)
    headerEncoder.wrap(buffer, 0);
    headerEncoder.blockLength(50).templateId(999).schemaId(1).version(1);
    // Fill block with zeros
    final int length = HEADER_LENGTH + 50;
    for (int i = HEADER_LENGTH; i < length; i++) {
      buffer.putByte(i, (byte) 0);
    }

    bridge.onMessage(1L, 0L, buffer, 0, length, null);

    assertEquals(0, collector.loadedCount());
    assertEquals(0, collector.rejectedCount());
  }

  @Test
  void onMessage_truncatedFragment_processesCompleteMessagesOnly() {
    int offset = 0;
    offset += encodeAccountLoaded(buffer, offset);
    // Add partial header (less than HEADER_LENGTH bytes of a second message)
    final int totalLength = offset + (HEADER_LENGTH - 1);

    bridge.onMessage(1L, 0L, buffer, 0, totalLength, null);

    assertEquals(1, collector.loadedCount(), "Should process the complete first message");
  }

  @Test
  void onMessage_truncatedBody_processesCompleteMessagesOnly() {
    int offset = 0;
    offset += encodeAccountLoaded(buffer, offset);
    // Write a valid header for a second message but truncate the body
    headerEncoder.wrap(buffer, offset);
    headerEncoder
        .blockLength(AccountLoadedEventEncoder.BLOCK_LENGTH)
        .templateId(AccountLoadedEventEncoder.TEMPLATE_ID)
        .schemaId(1)
        .version(1);
    // Only include the header, not the full block
    final int totalLength = offset + HEADER_LENGTH + 10; // 10 bytes of a 135-byte block

    bridge.onMessage(1L, 0L, buffer, 0, totalLength, null);

    assertEquals(1, collector.loadedCount(), "Should process only the complete first message");
  }

  @Test
  void onMessage_nonZeroOffset_messagesDecodedCorrectly() {
    final int startOffset = 64; // simulate fragment starting mid-buffer
    final int length = encodeAccountLoaded(buffer, startOffset);

    bridge.onMessage(1L, 0L, buffer, startOffset, length, null);

    assertEquals(1, collector.loadedCount());
  }

  @Test
  void onMessage_emptyFragment_noCollectorCalls() {
    bridge.onMessage(1L, 0L, buffer, 0, 0, null);

    assertEquals(0, collector.loadedCount());
    assertEquals(0, collector.rejectedCount());
  }

  // ===== SBE encoder helpers =====

  /**
   * Encodes an AccountLoadedEvent at the given offset. Returns total encoded length (header +
   * block).
   */
  private int encodeAccountLoaded(final MutableDirectBuffer buf, final int offset) {
    final var encoder = new AccountLoadedEventEncoder();
    encoder.wrapAndApplyHeader(buf, offset, headerEncoder);
    encoder.accountId(1);
    return HEADER_LENGTH + AccountLoadedEventEncoder.BLOCK_LENGTH;
  }

  private int encodeAccountLoadRejected(
      final MutableDirectBuffer buf, final int offset, final String reason) {
    final var encoder = new AccountLoadRejectedEventEncoder();
    encoder.wrapAndApplyHeader(buf, offset, headerEncoder);
    encoder.text(reason);
    return HEADER_LENGTH + AccountLoadRejectedEventEncoder.BLOCK_LENGTH;
  }

  private int encodeCurrencyLoaded(final MutableDirectBuffer buf, final int offset) {
    final var encoder = new CurrencyLoadedEventEncoder();
    encoder.wrapAndApplyHeader(buf, offset, headerEncoder);
    return HEADER_LENGTH + CurrencyLoadedEventEncoder.BLOCK_LENGTH;
  }

  private int encodeCurrencyLoadRejected(
      final MutableDirectBuffer buf, final int offset, final String reason) {
    final var encoder = new CurrencyLoadRejectedEventEncoder();
    encoder.wrapAndApplyHeader(buf, offset, headerEncoder);
    encoder.text(reason);
    return HEADER_LENGTH + CurrencyLoadRejectedEventEncoder.BLOCK_LENGTH;
  }

  private int encodeRiskLimitLoaded(final MutableDirectBuffer buf, final int offset) {
    final var encoder = new RiskLimitLoadedEventEncoder();
    encoder.wrapAndApplyHeader(buf, offset, headerEncoder);
    return HEADER_LENGTH + RiskLimitLoadedEventEncoder.BLOCK_LENGTH;
  }

  private int encodeRiskLimitLoadRejected(
      final MutableDirectBuffer buf, final int offset, final String reason) {
    final var encoder = new RiskLimitLoadRejectedEventEncoder();
    encoder.wrapAndApplyHeader(buf, offset, headerEncoder);
    encoder.text(reason);
    return HEADER_LENGTH + RiskLimitLoadRejectedEventEncoder.BLOCK_LENGTH;
  }
}
