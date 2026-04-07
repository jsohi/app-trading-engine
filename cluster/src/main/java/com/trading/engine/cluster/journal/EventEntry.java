package com.trading.engine.cluster.journal;

import java.nio.ByteOrder;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;

/**
 * One slot in the {@link EventJournal} ring. Holds the sequence number, event type, and the
 * serialized event payload bytes. Every slot is pre-allocated at journal construction and mutated
 * in place on every {@link EventJournal#append} — no per-event allocation.
 *
 * <p>The payload is stored in a per-slot {@link ExpandableArrayBuffer} with a small initial
 * capacity (64 bytes). The first time an event larger than 64 bytes lands in this slot the buffer
 * grows once to fit; subsequent events reuse the grown capacity without further allocation. Under
 * steady-state load the buffer settles at the size of the largest event the slot has ever seen.
 *
 * <p>Package-private — only {@link EventJournal} constructs and mutates these. Consumers reach
 * entries via {@link EventJournal#get(long)} or the {@link EventReplayHandler} callback and may
 * only read the accessor methods.
 */
final class EventEntry {

  /** Initial per-slot payload buffer size. Grows on demand to fit the largest event seen. */
  private static final int INITIAL_PAYLOAD_CAPACITY = 64;

  /** Fixed per-entry snapshot header length: seqNo(long) + eventType(int) + payloadLength(int). */
  static final int SNAPSHOT_HEADER_LENGTH = Long.BYTES + Integer.BYTES + Integer.BYTES;

  /**
   * Sanity upper bound on a single event's payload length. Any realistic domain event (order, fill,
   * quote, mass quote) is well under 64 KB. A corrupted snapshot carrying something absurd (say,
   * Integer.MAX_VALUE) would otherwise try to {@code putBytes} gigabytes and crash with a confusing
   * native error; rejecting it early gives a crisp diagnostic.
   */
  static final int MAX_PAYLOAD_LENGTH = 64 * 1024;

  /** 0 means the slot is unused (journal is not yet full and this slot has never held an event). */
  private long sequenceNumber;

  private int eventType;
  private final ExpandableArrayBuffer payload;
  private int payloadLength;

  EventEntry() {
    this.sequenceNumber = 0L;
    this.eventType = 0;
    this.payload = new ExpandableArrayBuffer(INITIAL_PAYLOAD_CAPACITY);
    this.payloadLength = 0;
  }

  /**
   * Overwrite the slot with a new event. Copies {@code srcLength} bytes from {@code src} starting
   * at {@code srcOffset} into the per-slot payload buffer at offset 0; subsequent reads via {@link
   * #payload()} return that copy. Caller is responsible for honouring {@link #payloadLength()} on
   * the way out — the per-slot buffer's underlying capacity may exceed {@code srcLength}.
   */
  void set(long seqNo, int eventType, DirectBuffer src, int srcOffset, int srcLength) {
    this.sequenceNumber = seqNo;
    this.eventType = eventType;
    this.payloadLength = srcLength;
    if (srcLength > 0) {
      this.payload.putBytes(0, src, srcOffset, srcLength);
    }
  }

  /**
   * Reset the slot's metadata to the unused state ({@code sequenceNumber = 0}, {@code payloadLength
   * = 0}). The underlying payload buffer is NOT zeroed — bytes beyond {@code payloadLength()} are
   * undefined and consumers must honour the length.
   */
  void clear() {
    this.sequenceNumber = 0L;
    this.eventType = 0;
    this.payloadLength = 0;
  }

  long sequenceNumber() {
    return sequenceNumber;
  }

  int eventType() {
    return eventType;
  }

  /**
   * The slot's internal payload buffer. Read-only for consumers — do NOT mutate or retain a
   * reference past the current call, the slot may be overwritten on the next journal append.
   */
  DirectBuffer payload() {
    return payload;
  }

  int payloadLength() {
    return payloadLength;
  }

  /**
   * Serialize this entry to {@code dst} at {@code offset}. Writes exactly {@code
   * SNAPSHOT_HEADER_LENGTH + payloadLength} bytes, little-endian. Returns the number of bytes
   * written so the caller can advance its own offset.
   */
  int writeTo(MutableDirectBuffer dst, int offset) {
    dst.putLong(offset, sequenceNumber, ByteOrder.LITTLE_ENDIAN);
    dst.putInt(offset + Long.BYTES, eventType, ByteOrder.LITTLE_ENDIAN);
    dst.putInt(offset + Long.BYTES + Integer.BYTES, payloadLength, ByteOrder.LITTLE_ENDIAN);
    if (payloadLength > 0) {
      dst.putBytes(offset + SNAPSHOT_HEADER_LENGTH, payload, 0, payloadLength);
    }
    return SNAPSHOT_HEADER_LENGTH + payloadLength;
  }

  /**
   * Deserialize this entry from {@code src} at {@code offset}. Reads exactly {@code
   * SNAPSHOT_HEADER_LENGTH + payloadLength} bytes and copies the payload into the per-slot buffer,
   * growing it if necessary. Returns the number of bytes consumed.
   */
  int readFrom(DirectBuffer src, int offset) {
    this.sequenceNumber = src.getLong(offset, ByteOrder.LITTLE_ENDIAN);
    this.eventType = src.getInt(offset + Long.BYTES, ByteOrder.LITTLE_ENDIAN);
    this.payloadLength = src.getInt(offset + Long.BYTES + Integer.BYTES, ByteOrder.LITTLE_ENDIAN);
    if (payloadLength < 0 || payloadLength > MAX_PAYLOAD_LENGTH) {
      throw new IllegalStateException(
          "EventEntry snapshot payloadLength out of range [0, "
              + MAX_PAYLOAD_LENGTH
              + "], was "
              + payloadLength);
    }
    if (payloadLength > 0) {
      this.payload.putBytes(0, src, offset + SNAPSHOT_HEADER_LENGTH, payloadLength);
    }
    return SNAPSHOT_HEADER_LENGTH + payloadLength;
  }
}
