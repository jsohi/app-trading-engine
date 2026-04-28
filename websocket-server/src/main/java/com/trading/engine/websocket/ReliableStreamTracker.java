package com.trading.engine.websocket;

import java.util.Objects;

/**
 * Per-session ring buffer of the most recent reliable-stream frames, used to satisfy {@code
 * WebSocketGapRequest} (template 68) and {@code SessionResume} (template 69) replays without
 * re-reading from the cluster log.
 *
 * <p><b>Storage layout.</b> A single pre-allocated {@code byte[capacity * frameSize]} backing
 * array. Each slot holds a fixed-size record:
 *
 * <pre>
 * Offset  Size  Field
 * 0       8     seqNo (long; {@code 0} when the slot is empty)
 * 8       4     length (int; the SBE message length stored in {@code payload})
 * 12      4     templateId (int)
 * 16      N     SBE payload (N == frameSize - 16)
 * </pre>
 *
 * <p>Slots are written round-robin keyed by {@code seqNo % capacity} (capacity is required to be a
 * power of two, validated by {@link WebSocketServerConfig}). When a new seqNo overwrites an older
 * entry, the older entry is implicitly evicted; the only state the tracker keeps externally is
 * {@code oldestSeqNo()} for cheap upper-bound checks during gap-request validation.
 *
 * <p><b>Capture failure semantics.</b> Capture is invoked AFTER the seqNo has been allocated by the
 * session ({@link WebSocketSession#nextReliableSeqNo()}) and BEFORE {@code ch.write}. If the caller
 * decides not to forward the frame to the wire (write throws, alloc throws, etc.) the caller must
 * call {@link #evict(long)} to clear the captured slot — otherwise replay would deliver a frame the
 * client never received in the live stream, producing a phantom-gap on next {@code ClientAck}.
 *
 * <p><b>Threading.</b> Thread-safe via per-instance intrinsic monitor (synchronized methods). Two
 * writers exist: the drain thread (cluster→browser live fan-out) and the channel thread (CommandAck
 * emission from {@code CommandDispatcher}); these are different worker event loops so capture
 * cannot be assumed single-threaded (Gemini PR #62 round 2). The monitor is uncontended in
 * steady-state because each session has its own tracker; only the rare overlap of
 * CommandAck-while-egress-draining contends.
 *
 * <p><b>Allocation.</b> Zero allocation after construction. The backing array is allocated once.
 * Replay re-uses the existing per-session ByteBuf allocator path (same as live writes).
 */
public final class ReliableStreamTracker {

  /** Slot header size in bytes (seqNo:8 + length:4 + templateId:4). */
  static final int SLOT_HEADER_SIZE = 16;

  private final int capacity;
  private final int frameSize;
  private final int payloadCapacity;
  private final byte[] buffer;
  private final int mask;
  private final WebSocketMetrics metrics;

  /** Highest seqNo successfully captured (0 if no capture has occurred). */
  private long highestSeqNo;

  /**
   * Create a tracker with the given ring buffer geometry.
   *
   * @param capacity number of frame slots; must be a positive power of two
   * @param frameSize size of each slot in bytes; must be {@code > {@link #SLOT_HEADER_SIZE}}
   * @param metrics metrics instance for replay and eviction counters
   * @throws IllegalArgumentException if capacity is not a positive power of two or if frameSize
   *     does not leave room for the slot header
   */
  public ReliableStreamTracker(
      final int capacity, final int frameSize, final WebSocketMetrics metrics) {
    if (capacity <= 0 || Integer.bitCount(capacity) != 1) {
      throw new IllegalArgumentException(
          "capacity must be a positive power of two, got: " + capacity);
    }
    if (frameSize <= SLOT_HEADER_SIZE) {
      throw new IllegalArgumentException(
          "frameSize must be > " + SLOT_HEADER_SIZE + ", got: " + frameSize);
    }
    this.capacity = capacity;
    this.frameSize = frameSize;
    this.payloadCapacity = frameSize - SLOT_HEADER_SIZE;
    this.buffer = new byte[capacity * frameSize];
    this.mask = capacity - 1;
    this.metrics = Objects.requireNonNull(metrics, "metrics");
  }

  /**
   * @return the number of frame slots in the ring
   */
  public int capacity() {
    return capacity;
  }

  /**
   * @return the maximum SBE payload size that can be captured per slot (frameSize - 16)
   */
  public int payloadCapacity() {
    return payloadCapacity;
  }

  /**
   * @return the highest seqNo successfully captured (0 if none yet)
   */
  public synchronized long highestSeqNo() {
    return highestSeqNo;
  }

  /**
   * @return the seqNo of the oldest still-present slot in the ring (1 if nothing has rolled over,
   *     {@code highestSeqNo - capacity + 1} otherwise; never less than 1)
   */
  public synchronized long oldestSeqNo() {
    if (highestSeqNo <= capacity) {
      return 1L;
    }
    return highestSeqNo - capacity + 1L;
  }

  /**
   * Capture a reliable frame's SBE payload at the given seqNo. The frame is stored in the ring slot
   * indexed by {@code seqNo % capacity}, overwriting any older entry there.
   *
   * <p>If the payload exceeds {@link #payloadCapacity()}, the slot is left empty (seqNo=0) and the
   * {@code replayEvictions} metric is incremented — replay will not be able to satisfy a gap
   * request for this seqNo. The live frame still goes out unmodified; this is documented as
   * best-effort replay coverage for oversized frames.
   *
   * @param seqNo the reliable sequence number assigned to this frame
   * @param templateId the SBE templateId of the frame
   * @param payload the raw SBE payload bytes (header + body)
   * @param offset the start offset within {@code payload}
   * @param length the number of payload bytes
   * @throws IllegalArgumentException if {@code seqNo <= 0}
   */
  public synchronized void capture(
      final long seqNo,
      final int templateId,
      final byte[] payload,
      final int offset,
      final int length) {
    if (seqNo <= 0) {
      throw new IllegalArgumentException("seqNo must be positive, got: " + seqNo);
    }
    if (length > payloadCapacity) {
      // Oversized frame — increment eviction metric and do not capture. Replay will return
      // BufferOverflow if the client requests this seqNo.
      metrics.replayEviction();
      if (seqNo > highestSeqNo) {
        highestSeqNo = seqNo;
      }
      return;
    }
    final int slot = (int) (seqNo & mask);
    final int slotOffset = slot * frameSize;
    writeLongLE(buffer, slotOffset, seqNo);
    writeIntLE(buffer, slotOffset + 8, length);
    writeIntLE(buffer, slotOffset + 12, templateId);
    if (length > 0) {
      System.arraycopy(payload, offset, buffer, slotOffset + SLOT_HEADER_SIZE, length);
    }
    if (seqNo > highestSeqNo) {
      highestSeqNo = seqNo;
    }
  }

  /**
   * Mark the slot for the given seqNo as empty. Called when a capture has been written but the
   * subsequent {@code ch.write} failed — the client never received the frame, so replay must not
   * deliver it later (which would produce a phantom-gap).
   *
   * <p>Idempotent: a no-op if the slot does not currently hold this seqNo.
   *
   * @param seqNo the reliable sequence number to evict
   */
  public synchronized void evict(final long seqNo) {
    if (seqNo <= 0) {
      return;
    }
    final int slot = (int) (seqNo & mask);
    final int slotOffset = slot * frameSize;
    if (readLongLE(buffer, slotOffset) == seqNo) {
      writeLongLE(buffer, slotOffset, 0L);
    }
  }

  /**
   * Look up a captured frame by seqNo and return its payload length, or -1 if the slot does not
   * hold that seqNo (either evicted, never captured, or rolled over).
   *
   * @param seqNo the reliable sequence number to look up
   * @return the payload length if found, or -1 otherwise
   */
  public synchronized int lookupLength(final long seqNo) {
    if (seqNo <= 0) {
      return -1;
    }
    final int slot = (int) (seqNo & mask);
    final int slotOffset = slot * frameSize;
    if (readLongLE(buffer, slotOffset) != seqNo) {
      return -1;
    }
    return readIntLE(buffer, slotOffset + 8);
  }

  /**
   * Returns the templateId for a captured frame, or -1 if not present.
   *
   * @param seqNo the reliable sequence number
   * @return the templateId, or -1 if the slot does not hold this seqNo
   */
  public synchronized int lookupTemplateId(final long seqNo) {
    if (seqNo <= 0) {
      return -1;
    }
    final int slot = (int) (seqNo & mask);
    final int slotOffset = slot * frameSize;
    if (readLongLE(buffer, slotOffset) != seqNo) {
      return -1;
    }
    return readIntLE(buffer, slotOffset + 12);
  }

  /**
   * Copy the captured payload for the given seqNo into the destination array.
   *
   * @param seqNo the reliable sequence number
   * @param dst destination byte array; must have at least {@code lookupLength(seqNo)} bytes free
   *     starting at {@code dstOffset}
   * @param dstOffset start offset in {@code dst}
   * @return the number of bytes copied, or -1 if the slot does not hold this seqNo
   */
  public synchronized int copyPayload(final long seqNo, final byte[] dst, final int dstOffset) {
    if (seqNo <= 0) {
      return -1;
    }
    final int slot = (int) (seqNo & mask);
    final int slotOffset = slot * frameSize;
    // Atomic re-check inside the lock — between an earlier lookupLength() call and this copy,
    // a concurrent capture could have rewritten the slot. Re-read length under the lock so
    // {@code copyPayload} is self-consistent regardless of caller pattern.
    if (readLongLE(buffer, slotOffset) != seqNo) {
      return -1;
    }
    final int len = readIntLE(buffer, slotOffset + 8);
    if (len > 0) {
      System.arraycopy(buffer, slotOffset + SLOT_HEADER_SIZE, dst, dstOffset, len);
    }
    return len;
  }

  // ---------------------------------------------------------------------------
  // Internal little-endian helpers (avoid VarHandle / ByteBuffer allocation)
  // ---------------------------------------------------------------------------

  private static void writeLongLE(final byte[] dst, final int offset, final long value) {
    dst[offset] = (byte) value;
    dst[offset + 1] = (byte) (value >>> 8);
    dst[offset + 2] = (byte) (value >>> 16);
    dst[offset + 3] = (byte) (value >>> 24);
    dst[offset + 4] = (byte) (value >>> 32);
    dst[offset + 5] = (byte) (value >>> 40);
    dst[offset + 6] = (byte) (value >>> 48);
    dst[offset + 7] = (byte) (value >>> 56);
  }

  private static long readLongLE(final byte[] src, final int offset) {
    return (src[offset] & 0xFFL)
        | ((src[offset + 1] & 0xFFL) << 8)
        | ((src[offset + 2] & 0xFFL) << 16)
        | ((src[offset + 3] & 0xFFL) << 24)
        | ((src[offset + 4] & 0xFFL) << 32)
        | ((src[offset + 5] & 0xFFL) << 40)
        | ((src[offset + 6] & 0xFFL) << 48)
        | ((src[offset + 7] & 0xFFL) << 56);
  }

  private static void writeIntLE(final byte[] dst, final int offset, final int value) {
    dst[offset] = (byte) value;
    dst[offset + 1] = (byte) (value >>> 8);
    dst[offset + 2] = (byte) (value >>> 16);
    dst[offset + 3] = (byte) (value >>> 24);
  }

  private static int readIntLE(final byte[] src, final int offset) {
    return (src[offset] & 0xFF)
        | ((src[offset + 1] & 0xFF) << 8)
        | ((src[offset + 2] & 0xFF) << 16)
        | ((src[offset + 3] & 0xFF) << 24);
  }
}
