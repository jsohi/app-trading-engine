package com.trading.engine.websocket;

import io.netty.buffer.ByteBuf;
import java.util.zip.CRC32C;

/**
 * Encodes and validates the custom wire envelope wrapping SBE messages for WebSocket transport.
 *
 * <p><b>Reliable stream (17-byte header):</b>
 *
 * <pre>
 * Offset  Size  Field
 * 0       4     totalLength (uint32, inclusive of header + payload)
 * 4       8     seqNo (int64, monotonically increasing per session)
 * 12      1     flags (bit 0=reliable, bit 1=replay, bit 2=snapshot, bit 3=snapshot-final)
 * 13      4     CRC32C (hardware-accelerated, over bytes 0..12 + payload)
 * 17      N     SBE payload
 * </pre>
 *
 * <p><b>Best-effort stream (13-byte header):</b>
 *
 * <pre>
 * Offset  Size  Field
 * 0       4     totalLength (uint32, inclusive)
 * 4       8     seqNo (int64, always 0)
 * 12      1     flags (reliable=0)
 * 13      N     SBE payload (no CRC32C)
 * </pre>
 *
 * <p><b>Flag bits:</b> bit 0 = reliable, bit 1 = replay, bit 2 = snapshot, bit 3 = snapshot-final.
 * Bits 4-7 are reserved and must be 0.
 *
 * <p><b>Valid flag combinations:</b>
 *
 * <ul>
 *   <li>{@code 0b0000} — best-effort live
 *   <li>{@code 0b0001} — reliable live
 *   <li>{@code 0b0011} — reliable replay
 *   <li>{@code 0b0100} — snapshot (more fragments)
 *   <li>{@code 0b1100} — snapshot (final fragment)
 * </ul>
 *
 * <p><b>Invalid:</b> replay without reliable ({@code 0b0010}), reliable with snapshot ({@code
 * 0b0101}). Reserved bits 4-7 non-zero.
 *
 * <p><b>Thread safety.</b> The encode methods allocate a thread-local {@link CRC32C} via reuse.
 * Safe for single-threaded use per Netty event loop.
 *
 * <p><b>Allocation.</b> Zero allocation per frame. CRC32C is reused via thread-local.
 *
 * @see <a href="docs/websocket-architecture.md">WebSocket Architecture — Section 2</a>
 */
public final class FrameParser {

  /** Reliable stream header size in bytes. */
  public static final int RELIABLE_HEADER_SIZE = 17;

  /** Best-effort stream header size in bytes. */
  public static final int BEST_EFFORT_HEADER_SIZE = 13;

  // Flag bit positions
  public static final byte FLAG_RELIABLE = 0x01;
  public static final byte FLAG_REPLAY = 0x02;
  public static final byte FLAG_SNAPSHOT = 0x04;
  public static final byte FLAG_SNAPSHOT_FINAL = 0x08;
  private static final byte RESERVED_MASK = (byte) 0xF0;

  // Valid flag combinations (5 valid, per architecture doc Section 2)
  private static final byte[] VALID_FLAGS = {
    0x00, // best-effort live
    FLAG_RELIABLE, // reliable live
    (byte) (FLAG_RELIABLE | FLAG_REPLAY), // reliable replay
    FLAG_SNAPSHOT, // snapshot (more)
    (byte) (FLAG_SNAPSHOT | FLAG_SNAPSHOT_FINAL), // snapshot (final)
  };

  // Thread-local CRC32C for zero-alloc per-frame computation
  private static final ThreadLocal<CRC32C> CRC = ThreadLocal.withInitial(CRC32C::new);

  private FrameParser() {}

  /**
   * Encode a reliable-stream frame (17B header + SBE payload + CRC32C).
   *
   * @param out the output ByteBuf (must have enough writable space)
   * @param seqNo the reliable sequence number for this frame
   * @param sbePayload the raw SBE message bytes
   * @param sbeOffset offset within the SBE payload array
   * @param sbeLength length of the SBE payload
   */
  public static void encodeReliable(
      final ByteBuf out,
      final long seqNo,
      final byte[] sbePayload,
      final int sbeOffset,
      final int sbeLength) {
    final int totalLength = RELIABLE_HEADER_SIZE + sbeLength;

    // Write header (without CRC placeholder)
    final int startIndex = out.writerIndex();
    out.writeIntLE(totalLength); // totalLength (inclusive)
    out.writeLongLE(seqNo); // seqNo
    out.writeByte(FLAG_RELIABLE); // flags

    // Compute CRC32C over header bytes (0..12) + payload
    final var crc = CRC.get();
    crc.reset();
    // Header bytes 0..12 (totalLength + seqNo + flags = 13 bytes)
    for (int i = startIndex; i < startIndex + 13; i++) {
      crc.update(out.getByte(i));
    }
    crc.update(sbePayload, sbeOffset, sbeLength);

    out.writeIntLE((int) crc.getValue()); // CRC32C
    out.writeBytes(sbePayload, sbeOffset, sbeLength); // SBE payload
  }

  /**
   * Encode a reliable-stream replay frame (17B header with replay flag + CRC32C).
   *
   * @param out the output ByteBuf
   * @param seqNo the original sequence number being replayed
   * @param sbePayload the raw SBE message bytes
   * @param sbeOffset offset within the SBE payload array
   * @param sbeLength length of the SBE payload
   */
  public static void encodeReliableReplay(
      final ByteBuf out,
      final long seqNo,
      final byte[] sbePayload,
      final int sbeOffset,
      final int sbeLength) {
    final int totalLength = RELIABLE_HEADER_SIZE + sbeLength;
    final byte flags = (byte) (FLAG_RELIABLE | FLAG_REPLAY);

    final int startIndex = out.writerIndex();
    out.writeIntLE(totalLength);
    out.writeLongLE(seqNo);
    out.writeByte(flags);

    final var crc = CRC.get();
    crc.reset();
    for (int i = startIndex; i < startIndex + 13; i++) {
      crc.update(out.getByte(i));
    }
    crc.update(sbePayload, sbeOffset, sbeLength);

    out.writeIntLE((int) crc.getValue());
    out.writeBytes(sbePayload, sbeOffset, sbeLength);
  }

  /**
   * Encode a best-effort stream frame (13B header, no CRC32C).
   *
   * @param out the output ByteBuf
   * @param sbePayload the raw SBE message bytes
   * @param sbeOffset offset within the SBE payload array
   * @param sbeLength length of the SBE payload
   */
  public static void encodeBestEffort(
      final ByteBuf out, final byte[] sbePayload, final int sbeOffset, final int sbeLength) {
    final int totalLength = BEST_EFFORT_HEADER_SIZE + sbeLength;

    out.writeIntLE(totalLength); // totalLength (inclusive)
    out.writeLongLE(0L); // seqNo = 0 (always, still occupies 8 bytes)
    out.writeByte(0); // flags = 0 (best-effort)
    out.writeBytes(sbePayload, sbeOffset, sbeLength); // SBE payload
  }

  /**
   * Validate a flag byte. Returns {@code true} if the combination is valid per the architecture
   * doc.
   *
   * @param flags the flag byte to validate
   * @return true if the flag combination is valid
   */
  public static boolean isValidFlagCombination(final byte flags) {
    // Reject reserved bits (4-7)
    if ((flags & RESERVED_MASK) != 0) {
      return false;
    }
    for (final byte valid : VALID_FLAGS) {
      if (flags == valid) {
        return true;
      }
    }
    return false;
  }

  /**
   * Check if a flag byte indicates a reliable-stream frame.
   *
   * @param flags the flag byte
   * @return true if the reliable bit is set
   */
  public static boolean isReliable(final byte flags) {
    return (flags & FLAG_RELIABLE) != 0;
  }

  /**
   * Check if a flag byte indicates a replay frame.
   *
   * @param flags the flag byte
   * @return true if the replay bit is set
   */
  public static boolean isReplay(final byte flags) {
    return (flags & FLAG_REPLAY) != 0;
  }

  /**
   * Check if a flag byte indicates a snapshot frame.
   *
   * @param flags the flag byte
   * @return true if the snapshot bit is set
   */
  public static boolean isSnapshot(final byte flags) {
    return (flags & FLAG_SNAPSHOT) != 0;
  }

  /**
   * Check if a flag byte indicates the final snapshot fragment.
   *
   * @param flags the flag byte
   * @return true if both snapshot and snapshot-final bits are set
   */
  public static boolean isSnapshotFinal(final byte flags) {
    return (flags & (FLAG_SNAPSHOT | FLAG_SNAPSHOT_FINAL)) == (FLAG_SNAPSHOT | FLAG_SNAPSHOT_FINAL);
  }
}
