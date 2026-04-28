package com.trading.engine.websocket;

import com.trading.engine.messages.sbe.CommandAckEncoder;
import com.trading.engine.messages.sbe.MessageHeaderDecoder;
import java.util.Objects;
import java.util.UUID;

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
 * <p><b>Direction.</b> The same entry type carries traffic in both flow directions to avoid
 * maintaining two separate pools. {@link Direction#CLUSTER_TO_BROWSER} entries are populated by
 * {@link WebSocketEgressListener} and drained by {@link WebSocketDrainHandler}. {@link
 * Direction#BROWSER_TO_CLUSTER} entries are populated by {@link CommandDispatcher} on the Netty
 * event loop and drained by {@link AeronEgressThread} for cluster offer; failure ack entries (e.g.,
 * THROTTLED) flip back to {@code CLUSTER_TO_BROWSER} via the ack back-channel.
 *
 * <p><b>Thread safety.</b> An entry is owned by exactly one thread at a time: the producer thread
 * writes into it, then it is passed via the lock-free queue to the consumer thread which reads from
 * it. No concurrent access.
 *
 * <p><b>Allocation.</b> Zero allocation after pool construction. The backing array is allocated
 * once and reused indefinitely.
 */
public final class EgressEntry {

  /** Carrier direction for this entry — controls how the consumer should dispatch the bytes. */
  public enum Direction {
    /** Cluster → browser: standard egress fan-out path. */
    CLUSTER_TO_BROWSER,
    /** Browser → cluster: command forwarding path. */
    BROWSER_TO_CLUSTER
  }

  private final byte[] bytes;
  private int length;
  private int templateId;
  private Direction direction = Direction.CLUSTER_TO_BROWSER;
  // Browser→cluster routing state (populated only for command entries; otherwise meaningless).
  private long sessionIdMsb;
  private long sessionIdLsb;
  private long clientCmdSeqNo;

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
    this.direction = Direction.CLUSTER_TO_BROWSER;
  }

  /**
   * Set only the metadata fields (length and templateId) without copying bytes. Used when the
   * backing array has already been written into directly (e.g., via {@code
   * DirectBuffer.getBytes()}) and only the metadata needs updating. Resets direction to {@link
   * Direction#CLUSTER_TO_BROWSER}.
   *
   * @param length the actual message length within the backing array
   * @param templateId the SBE templateId extracted from the message header
   */
  public void setMetadata(final int length, final int templateId) {
    this.length = length;
    this.templateId = templateId;
    this.direction = Direction.CLUSTER_TO_BROWSER;
  }

  /**
   * Populate the entry as a browser-to-cluster command. Direction is set to {@link
   * Direction#BROWSER_TO_CLUSTER}.
   *
   * @param srcBytes the SBE-encoded command bytes
   * @param srcOffset start offset in {@code srcBytes}
   * @param srcLength number of bytes to copy
   * @param templateId the command's SBE templateId (1, 4, or 6)
   * @param sessionId the originating WebSocket session UUID (for routing CommandAck back)
   * @param clientCmdSeqNo the client-supplied command sequence number for the CommandAck
   */
  public void fillCommand(
      final byte[] srcBytes,
      final int srcOffset,
      final int srcLength,
      final int templateId,
      final UUID sessionId,
      final long clientCmdSeqNo) {
    Objects.requireNonNull(srcBytes, "srcBytes");
    Objects.requireNonNull(sessionId, "sessionId");
    System.arraycopy(srcBytes, srcOffset, bytes, 0, srcLength);
    this.length = srcLength;
    this.templateId = templateId;
    this.direction = Direction.BROWSER_TO_CLUSTER;
    this.sessionIdMsb = sessionId.getMostSignificantBits();
    this.sessionIdLsb = sessionId.getLeastSignificantBits();
    this.clientCmdSeqNo = clientCmdSeqNo;
  }

  /**
   * Populate command metadata WITHOUT copying bytes — assumes the caller has already written into
   * {@link #bytes()} directly. Used by the dispatcher fast path to avoid a redundant byte copy when
   * the source is a {@code ByteBuf} already drained via {@code getBytes(...)} into the entry's
   * backing array.
   *
   * @param length the message length
   * @param templateId the SBE templateId
   * @param sessionId the originating WebSocket session UUID
   * @param clientCmdSeqNo the client-supplied command seqNo
   */
  public void fillCommandMetadata(
      final int length, final int templateId, final UUID sessionId, final long clientCmdSeqNo) {
    Objects.requireNonNull(sessionId, "sessionId");
    this.length = length;
    this.templateId = templateId;
    this.direction = Direction.BROWSER_TO_CLUSTER;
    this.sessionIdMsb = sessionId.getMostSignificantBits();
    this.sessionIdLsb = sessionId.getLeastSignificantBits();
    this.clientCmdSeqNo = clientCmdSeqNo;
  }

  /**
   * Populate the entry with a pre-encoded CommandAck for the back-channel (AeronEgressThread →
   * drain handler). Direction is {@link Direction#CLUSTER_TO_BROWSER}.
   *
   * @param srcBytes the SBE-encoded CommandAck bytes
   * @param srcOffset start offset in {@code srcBytes}
   * @param srcLength number of bytes to copy
   * @param sessionId the destination session UUID (for findById lookup in the drain handler)
   */
  public void fillAckBackChannel(
      final byte[] srcBytes, final int srcOffset, final int srcLength, final UUID sessionId) {
    Objects.requireNonNull(srcBytes, "srcBytes");
    Objects.requireNonNull(sessionId, "sessionId");
    System.arraycopy(srcBytes, srcOffset, bytes, 0, srcLength);
    this.length = srcLength;
    this.templateId = CommandAckEncoder.TEMPLATE_ID;
    this.direction = Direction.CLUSTER_TO_BROWSER;
    this.sessionIdMsb = sessionId.getMostSignificantBits();
    this.sessionIdLsb = sessionId.getLeastSignificantBits();
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
   * @return the direction this entry is being routed in
   */
  public Direction direction() {
    return direction;
  }

  /**
   * @return the most-significant bits of the routing session UUID (command/ack-channel only)
   */
  public long sessionIdMsb() {
    return sessionIdMsb;
  }

  /**
   * @return the least-significant bits of the routing session UUID (command/ack-channel only)
   */
  public long sessionIdLsb() {
    return sessionIdLsb;
  }

  /**
   * @return the client command sequence number this entry corresponds to (commands only)
   */
  public long clientCmdSeqNo() {
    return clientCmdSeqNo;
  }

  /**
   * Reset the entry's transient routing fields. Called by the pool's release path so that a
   * subsequent {@code fill}/{@code fillCommand}/{@code fillAckBackChannel} starts from a clean
   * slate.
   */
  public void resetForPool() {
    this.length = 0;
    this.templateId = 0;
    this.direction = Direction.CLUSTER_TO_BROWSER;
    this.sessionIdMsb = 0L;
    this.sessionIdLsb = 0L;
    this.clientCmdSeqNo = 0L;
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
