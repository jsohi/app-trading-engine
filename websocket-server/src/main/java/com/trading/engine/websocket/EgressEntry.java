package com.trading.engine.websocket;

import com.trading.engine.messages.sbe.MessageHeaderDecoder;
import java.util.Objects;

/**
 * Pre-allocated pool entry for buffering SBE messages between the Aeron egress thread and the Netty
 * drain handler via {@link org.agrona.concurrent.ManyToOneConcurrentArrayQueue}.
 *
 * <p>Each entry owns a fixed-size backing {@code byte[]} that is reused across pool cycles. The
 * {@link #length} field tracks the actual message size for each use.
 *
 * <p><b>Pool pattern.</b> Entries are managed by a LIFO free-list stack in {@link
 * WebSocketEgressListener}, mirroring the {@code InFlightTracker} pattern from the gateway module.
 * Pop on {@code onMessage()}, push back after the drain handler processes the entry.
 *
 * <p><b>Thread safety.</b> An entry is owned by exactly one thread at a time: the Aeron egress
 * thread writes into it, then it is passed via the lock-free queue to the Netty event loop thread
 * which reads from it. No concurrent access.
 *
 * <p><b>Allocation.</b> Zero allocation after pool construction. The backing array is allocated
 * once and reused indefinitely.
 */
public final class EgressEntry {

  private final byte[] bytes;
  private int length;
  private int templateId;

  /**
   * Create a new entry with the given backing array size.
   *
   * @param maxMessageSize maximum SBE message size in bytes (typically {@code
   *     WebSocketServerConfig.replayBufferFrameSize()})
   */
  public EgressEntry(final int maxMessageSize) {
    this.bytes = new byte[maxMessageSize];
  }

  /**
   * Copy message bytes into this entry from an Aeron egress buffer.
   *
   * @param srcBytes source byte array (from Aeron DirectBuffer)
   * @param srcOffset offset within the source
   * @param srcLength number of bytes to copy
   * @param templateId the SBE templateId extracted from the message header
   */
  public void fill(
      final byte[] srcBytes, final int srcOffset, final int srcLength, final int templateId) {
    Objects.requireNonNull(srcBytes, "srcBytes");
    System.arraycopy(srcBytes, srcOffset, bytes, 0, srcLength);
    this.length = srcLength;
    this.templateId = templateId;
  }

  /**
   * Set only the metadata fields (length and templateId) without copying bytes. Used when the
   * backing array has already been written into directly (e.g., via {@code
   * DirectBuffer.getBytes()}) and only the metadata needs updating.
   *
   * @param length the actual message length within the backing array
   * @param templateId the SBE templateId extracted from the message header
   */
  public void setMetadata(final int length, final int templateId) {
    this.length = length;
    this.templateId = templateId;
  }

  /**
   * @return the backing byte array (shared — do not retain references beyond processing)
   */
  public byte[] bytes() {
    return bytes;
  }

  /**
   * @return the actual message length within the backing array
   */
  public int length() {
    return length;
  }

  /**
   * @return the SBE templateId of the buffered message
   */
  public int templateId() {
    return templateId;
  }

  /**
   * @return true if this message is a reliable-stream message (orders, fills, positions, errors,
   *     CommandAck) as opposed to best-effort (prices, quotes, heartbeat)
   */
  public boolean isReliable() {
    // Reliable: domain events (100+), ExecutionReport (5), CommandAck (70), errors (67)
    // Best-effort: PriceResponse (51), Quote (2), WebSocketHeartbeat (64)
    return templateId >= 100
        || templateId == 5
        || templateId == 70
        || templateId == 67
        || templateId == 10; // OrderCancelReject
  }

  /**
   * Determine the SBE templateId from a raw message buffer at the given offset. Uses the standard
   * SBE header layout (blockLength[2] + templateId[2]).
   *
   * @param buffer the raw SBE message bytes
   * @param offset the start of the SBE message header
   * @return the templateId (unsigned 16-bit)
   */
  public static int extractTemplateId(final byte[] buffer, final int offset) {
    // SBE header: [blockLength:uint16][templateId:uint16][schemaId:uint16][version:uint16]
    // templateId is at offset + 2 (little-endian per schema byteOrder="littleEndian")
    final int tidOffset = offset + MessageHeaderDecoder.templateIdEncodingOffset();
    return (buffer[tidOffset] & 0xFF) | ((buffer[tidOffset + 1] & 0xFF) << 8);
  }
}
