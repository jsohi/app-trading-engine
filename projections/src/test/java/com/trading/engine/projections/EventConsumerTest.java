package com.trading.engine.projections;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.trading.engine.messages.sbe.MessageHeaderEncoder;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

class EventConsumerTest {

  private static final int ORDER_CREATED = 100;
  private static final int ORDER_FILLED = 102;

  /** In-test projection that records every dispatched onEvent's arguments. */
  private static final class RecordingProjection implements Projection {
    final long[] seqNos = new long[64];
    final int[] eventTypes = new int[64];
    final int[] offsets = new int[64];
    final int[] lengths = new int[64];
    final byte[][] payloads = new byte[64][];
    int count;
    int resets;

    @Override
    public void onEvent(
        final long seqNo,
        final int eventType,
        final DirectBuffer buffer,
        final int offset,
        final int length) {
      seqNos[count] = seqNo;
      eventTypes[count] = eventType;
      offsets[count] = offset;
      lengths[count] = length;
      final byte[] copy = new byte[length];
      if (length > 0) {
        buffer.getBytes(offset, copy);
      }
      payloads[count] = copy;
      count++;
    }

    @Override
    public long lastProcessedSequence() {
      return count == 0 ? 0L : seqNos[count - 1];
    }

    @Override
    public void reset() {
      count = 0;
      resets++;
    }
  }

  /** Wraps an 8-byte SBE header with the given templateId at the start of a fresh buffer. */
  private static UnsafeBuffer bufferWithHeader(final int templateId) {
    final UnsafeBuffer buf = new UnsafeBuffer(new byte[32]);
    final MessageHeaderEncoder header = new MessageHeaderEncoder();
    header.wrap(buf, 0).blockLength(0).templateId(templateId).schemaId(1).version(1);
    return buf;
  }

  // ---------------------------------------------------------------------------
  // Registration validation
  // ---------------------------------------------------------------------------

  @Test
  void registerRejectsNullProjection() {
    final EventConsumer c = new EventConsumer();
    assertThrows(NullPointerException.class, () -> c.registerProjection(null, ORDER_CREATED));
  }

  @Test
  void registerRejectsEmptyEventTypes() {
    final EventConsumer c = new EventConsumer();
    final RecordingProjection p = new RecordingProjection();
    assertThrows(IllegalArgumentException.class, () -> c.registerProjection(p));
  }

  @Test
  void registerRejectsDuplicateForSameEventType() {
    final EventConsumer c = new EventConsumer();
    final RecordingProjection p = new RecordingProjection();
    c.registerProjection(p, ORDER_CREATED);
    assertThrows(IllegalArgumentException.class, () -> c.registerProjection(p, ORDER_CREATED));
  }

  @Test
  void registerRejectsAfterStart() {
    final EventConsumer c = new EventConsumer();
    final RecordingProjection p = new RecordingProjection();
    c.registerProjection(p, ORDER_CREATED);
    c.markStartedForTest();
    assertThrows(
        IllegalStateException.class,
        () -> c.registerProjection(new RecordingProjection(), ORDER_FILLED));
  }

  @Test
  void registerRejectsDuplicateEventTypeWithinSingleCall() {
    final EventConsumer c = new EventConsumer();
    final RecordingProjection p = new RecordingProjection();
    assertThrows(
        IllegalArgumentException.class,
        () -> c.registerProjection(p, ORDER_CREATED, ORDER_CREATED));
    // After rejection the dispatch table must be unchanged — a subsequent valid call succeeds
    // and dispatches normally.
    c.registerProjection(p, ORDER_CREATED);
    c.onFragment(bufferWithHeader(ORDER_CREATED), 0, 32, null);
    assertEquals(1, p.count);
  }

  @Test
  void registerRejectsAfterClose() {
    final EventConsumer c = new EventConsumer();
    c.close();
    assertThrows(
        IllegalStateException.class,
        () -> c.registerProjection(new RecordingProjection(), ORDER_CREATED));
  }

  // ---------------------------------------------------------------------------
  // Dispatch
  // ---------------------------------------------------------------------------

  @Test
  void dispatchByTemplateIdSingleProjection() {
    final EventConsumer c = new EventConsumer();
    final RecordingProjection p = new RecordingProjection();
    c.registerProjection(p, ORDER_CREATED);

    c.onFragment(bufferWithHeader(ORDER_CREATED), 0, 32, null);
    c.onFragment(bufferWithHeader(ORDER_CREATED), 0, 32, null);

    assertEquals(2, p.count);
    assertEquals(1L, p.seqNos[0]);
    assertEquals(2L, p.seqNos[1]);
    assertEquals(ORDER_CREATED, p.eventTypes[0]);
    assertEquals(2L, c.lastProcessedSequence());
  }

  @Test
  void dispatchIgnoresUnknownTemplateId() {
    final EventConsumer c = new EventConsumer();
    final RecordingProjection p = new RecordingProjection();
    c.registerProjection(p, ORDER_CREATED);

    // Event with no registered projection — silently dropped, counter NOT bumped.
    c.onFragment(bufferWithHeader(ORDER_FILLED), 0, 32, null);
    assertEquals(0, p.count);
    assertEquals(0L, c.lastProcessedSequence());

    // A subsequent registered event still gets seqNo 1 — unknowns don't consume the counter.
    c.onFragment(bufferWithHeader(ORDER_CREATED), 0, 32, null);
    assertEquals(1, p.count);
    assertEquals(1L, p.seqNos[0]);
    assertEquals(1L, c.lastProcessedSequence());
  }

  @Test
  void dispatchToMultipleProjectionsForSameEventType() {
    final EventConsumer c = new EventConsumer();
    final RecordingProjection a = new RecordingProjection();
    final RecordingProjection b = new RecordingProjection();
    c.registerProjection(a, ORDER_CREATED);
    c.registerProjection(b, ORDER_CREATED);

    c.onFragment(bufferWithHeader(ORDER_CREATED), 0, 32, null);

    assertEquals(1, a.count);
    assertEquals(1, b.count);
    assertEquals(1L, a.seqNos[0]);
    assertEquals(1L, b.seqNos[0]);
  }

  @Test
  void dispatchSameProjectionRegisteredForMultipleEventTypes() {
    final EventConsumer c = new EventConsumer();
    final RecordingProjection p = new RecordingProjection();
    c.registerProjection(p, ORDER_CREATED, ORDER_FILLED);

    c.onFragment(bufferWithHeader(ORDER_CREATED), 0, 32, null);
    c.onFragment(bufferWithHeader(ORDER_FILLED), 0, 32, null);

    assertEquals(2, p.count);
    assertEquals(ORDER_CREATED, p.eventTypes[0]);
    assertEquals(ORDER_FILLED, p.eventTypes[1]);
    assertEquals(1L, p.seqNos[0]);
    assertEquals(2L, p.seqNos[1]);
  }

  @Test
  void lastProcessedSequenceIsMonotonicAcrossMixedStream() {
    final EventConsumer c = new EventConsumer();
    final RecordingProjection p = new RecordingProjection();
    c.registerProjection(p, ORDER_CREATED, ORDER_FILLED);

    c.onFragment(bufferWithHeader(ORDER_CREATED), 0, 32, null);
    c.onFragment(bufferWithHeader(ORDER_FILLED), 0, 32, null);
    c.onFragment(bufferWithHeader(ORDER_CREATED), 0, 32, null);

    assertEquals(3L, c.lastProcessedSequence());
    assertEquals(3L, p.seqNos[2]);
  }

  // ---------------------------------------------------------------------------
  // Reset
  // ---------------------------------------------------------------------------

  @Test
  void resetZerosIngressCounterAndResetsEachProjectionOnce() {
    final EventConsumer c = new EventConsumer();
    final RecordingProjection p = new RecordingProjection();
    // Register for two eventTypes — dedup must call reset() only once.
    c.registerProjection(p, ORDER_CREATED, ORDER_FILLED);

    c.onFragment(bufferWithHeader(ORDER_CREATED), 0, 32, null);
    c.onFragment(bufferWithHeader(ORDER_FILLED), 0, 32, null);
    assertEquals(2L, c.lastProcessedSequence());

    c.reset();
    assertEquals(0L, c.lastProcessedSequence());
    assertEquals(1, p.resets);
    assertEquals(0, p.count);
  }

  @Test
  void resetDedupsAcrossSharedEventTypes() {
    final EventConsumer c = new EventConsumer();
    final RecordingProjection a = new RecordingProjection();
    final RecordingProjection b = new RecordingProjection();
    c.registerProjection(a, ORDER_CREATED, ORDER_FILLED);
    c.registerProjection(b, ORDER_FILLED);

    c.reset();
    assertEquals(1, a.resets);
    assertEquals(1, b.resets);
  }

  // ---------------------------------------------------------------------------
  // Lifecycle
  // ---------------------------------------------------------------------------

  @Test
  void pollBeforeStartThrows() {
    final EventConsumer c = new EventConsumer();
    assertThrows(IllegalStateException.class, () -> c.poll(10));
  }

  @Test
  void closeWithoutStartIsNoop() {
    final EventConsumer c = new EventConsumer();
    c.close(); // Should not throw.
    assertTrue(c.isClosed());
  }

  @Test
  void closeIsTerminalAndZerosIngressCounter() {
    final EventConsumer c = new EventConsumer();
    final RecordingProjection p = new RecordingProjection();
    c.registerProjection(p, ORDER_CREATED);
    c.onFragment(bufferWithHeader(ORDER_CREATED), 0, 32, null);
    assertEquals(1L, c.lastProcessedSequence());

    c.close();
    assertTrue(c.isClosed());
    assertEquals(0L, c.lastProcessedSequence());
  }

  // ---------------------------------------------------------------------------
  // Payload slice correctness
  // ---------------------------------------------------------------------------

  @Test
  void onFragmentPassesPayloadSliceAfterHeader() {
    final EventConsumer c = new EventConsumer();
    final RecordingProjection p = new RecordingProjection();
    c.registerProjection(p, ORDER_CREATED);

    // Build a fragment: 8-byte SBE header + 4 payload bytes.
    final byte[] bytes = new byte[12];
    final UnsafeBuffer buf = new UnsafeBuffer(bytes);
    final MessageHeaderEncoder header = new MessageHeaderEncoder();
    header.wrap(buf, 0).blockLength(0).templateId(ORDER_CREATED).schemaId(1).version(1);
    bytes[8] = (byte) 0xDE;
    bytes[9] = (byte) 0xAD;
    bytes[10] = (byte) 0xBE;
    bytes[11] = (byte) 0xEF;

    c.onFragment(buf, 0, 12, null);

    assertEquals(1, p.count);
    assertEquals(8, p.offsets[0]); // payload starts after the 8-byte header
    assertEquals(4, p.lengths[0]); // payload is 4 bytes
    final byte[] expected = {(byte) 0xDE, (byte) 0xAD, (byte) 0xBE, (byte) 0xEF};
    assertArrayEquals(expected, p.payloads[0]);
  }

  @Test
  void onFragmentPassesPayloadSliceAtNonZeroOffset() {
    final EventConsumer c = new EventConsumer();
    final RecordingProjection p = new RecordingProjection();
    c.registerProjection(p, ORDER_CREATED);

    // Buffer with 16 bytes of lead-in garbage, then header + 2 payload bytes.
    final byte[] bytes = new byte[26];
    for (int i = 0; i < 16; i++) {
      bytes[i] = (byte) 0xFF;
    }
    final UnsafeBuffer buf = new UnsafeBuffer(bytes);
    final MessageHeaderEncoder header = new MessageHeaderEncoder();
    header.wrap(buf, 16).blockLength(0).templateId(ORDER_CREATED).schemaId(1).version(1);
    bytes[24] = (byte) 0x42;
    bytes[25] = (byte) 0x43;

    c.onFragment(buf, 16, 10, null);

    assertEquals(1, p.count);
    assertEquals(24, p.offsets[0]); // 16 (base) + 8 (header)
    assertEquals(2, p.lengths[0]);
    assertArrayEquals(new byte[] {(byte) 0x42, (byte) 0x43}, p.payloads[0]);
  }

  @Test
  void onFragmentDropsTruncatedFragmentShorterThanHeader() {
    final EventConsumer c = new EventConsumer();
    final RecordingProjection p = new RecordingProjection();
    c.registerProjection(p, ORDER_CREATED);

    // Only 4 bytes — can't decode an 8-byte SBE header.
    final UnsafeBuffer buf = new UnsafeBuffer(new byte[8]);
    c.onFragment(buf, 0, 4, null);

    assertEquals(0, p.count);
    assertEquals(0L, c.lastProcessedSequence());
    assertEquals(1L, c.truncatedFragmentDropCount());
  }

  @Test
  void onFragmentDropsFragmentShorterThanDeclaredBlockLength() {
    final EventConsumer c = new EventConsumer();
    final RecordingProjection p = new RecordingProjection();
    c.registerProjection(p, ORDER_CREATED);

    // Header declares blockLength=16 but the fragment only carries 4 payload bytes after the
    // 8-byte header. Total length 12 < 8 + 16 = 24, so the fragment must be dropped — letting
    // it through would let a projection decoder read 12 bytes past the end of the buffer.
    final UnsafeBuffer buf = new UnsafeBuffer(new byte[64]);
    final MessageHeaderEncoder header = new MessageHeaderEncoder();
    header.wrap(buf, 0).blockLength(16).templateId(ORDER_CREATED).schemaId(1).version(1);

    c.onFragment(buf, 0, 12, null);

    assertEquals(0, p.count);
    assertEquals(0L, c.lastProcessedSequence());
    assertEquals(1L, c.truncatedFragmentDropCount());
  }

  @Test
  void onFragmentAcceptsFragmentMatchingDeclaredBlockLength() {
    final EventConsumer c = new EventConsumer();
    final RecordingProjection p = new RecordingProjection();
    c.registerProjection(p, ORDER_CREATED);

    // Header declares blockLength=16, fragment carries header + 16 bytes = 24 total. Just barely
    // valid — should dispatch normally.
    final UnsafeBuffer buf = new UnsafeBuffer(new byte[64]);
    final MessageHeaderEncoder header = new MessageHeaderEncoder();
    header.wrap(buf, 0).blockLength(16).templateId(ORDER_CREATED).schemaId(1).version(1);

    c.onFragment(buf, 0, 24, null);

    assertEquals(1, p.count);
    assertEquals(16, p.lengths[0]);
    assertEquals(0L, c.truncatedFragmentDropCount());
  }

  @Test
  void onFragmentAcceptsHeaderOnlyFragmentWithZeroLengthPayload() {
    final EventConsumer c = new EventConsumer();
    final RecordingProjection p = new RecordingProjection();
    c.registerProjection(p, ORDER_CREATED);

    // Exactly 8 bytes — header with no payload. Valid, dispatched with length=0.
    c.onFragment(bufferWithHeader(ORDER_CREATED), 0, 8, null);

    assertEquals(1, p.count);
    assertEquals(0, p.lengths[0]);
  }

  @Test
  void isStartedInitiallyFalse() {
    final EventConsumer c = new EventConsumer();
    assertFalse(c.isStarted());
  }
}
