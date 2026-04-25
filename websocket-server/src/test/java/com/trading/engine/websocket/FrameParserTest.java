package com.trading.engine.websocket;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.ResourceLeakDetector;
import java.util.zip.CRC32C;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link FrameParser} — verifies reliable/best-effort encoding, CRC32C integrity, flag
 * validation, and flag inspection helpers.
 *
 * <p>Uses {@link Unpooled#buffer()} for ByteBuf allocation. Buffers are released in {@link
 * #tearDown()} to prevent leaks under PARANOID detection.
 */
final class FrameParserTest {

  private ByteBuf buf;

  @BeforeAll
  static void enableLeakDetection() {
    ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID);
  }

  @AfterEach
  void tearDown() {
    if (buf != null && buf.refCnt() > 0) {
      buf.release();
    }
  }

  @Test
  void encodeReliable_validPayload_produces17ByteHeaderPlusSbe() {
    buf = Unpooled.buffer(64);
    final var payload = new byte[] {0x01, 0x02, 0x03, 0x04, 0x05};
    final long seqNo = 42L;

    FrameParser.encodeReliable(buf, seqNo, payload, 0, payload.length);

    final int expectedTotal = FrameParser.RELIABLE_HEADER_SIZE + payload.length;
    assertEquals(expectedTotal, buf.readableBytes());

    // totalLength (LE uint32)
    final int totalLength = buf.readIntLE();
    assertEquals(expectedTotal, totalLength);

    // seqNo (LE int64)
    final long readSeqNo = buf.readLongLE();
    assertEquals(seqNo, readSeqNo);

    // flags
    final byte flags = buf.readByte();
    assertEquals(FrameParser.FLAG_RELIABLE, flags);

    // CRC32C (4 bytes, skip for now)
    buf.skipBytes(4);

    // SBE payload
    final var readPayload = new byte[payload.length];
    buf.readBytes(readPayload);
    assertArrayEquals(payload, readPayload);
  }

  @Test
  void encodeBestEffort_validPayload_produces13ByteHeaderPlusSbe() {
    buf = Unpooled.buffer(64);
    final var payload = new byte[] {(byte) 0xAA, (byte) 0xBB, (byte) 0xCC};

    FrameParser.encodeBestEffort(buf, payload, 0, payload.length);

    final int expectedTotal = FrameParser.BEST_EFFORT_HEADER_SIZE + payload.length;
    assertEquals(expectedTotal, buf.readableBytes());

    // totalLength (LE uint32)
    final int totalLength = buf.readIntLE();
    assertEquals(expectedTotal, totalLength);

    // seqNo = 0 (LE int64)
    final long readSeqNo = buf.readLongLE();
    assertEquals(0L, readSeqNo);

    // flags = 0
    final byte flags = buf.readByte();
    assertEquals(0, flags);

    // SBE payload (no CRC)
    final var readPayload = new byte[payload.length];
    buf.readBytes(readPayload);
    assertArrayEquals(payload, readPayload);
  }

  @Test
  void encodeReliable_crc32c_matchesJdkComputation() {
    buf = Unpooled.buffer(64);
    final var payload = new byte[] {0x10, 0x20, 0x30, 0x40};
    final long seqNo = 7L;

    FrameParser.encodeReliable(buf, seqNo, payload, 0, payload.length);

    // Independently compute CRC32C over the first 13 header bytes + payload
    final var crc = new CRC32C();
    // Read the 13 header bytes from the buffer (totalLength + seqNo + flags)
    final var headerBytes = new byte[13];
    buf.getBytes(0, headerBytes);
    crc.update(headerBytes, 0, headerBytes.length);
    crc.update(payload, 0, payload.length);
    final int expectedCrc = (int) crc.getValue();

    // Read CRC from frame at offset 13 (after header, before payload)
    final int frameCrc = buf.getIntLE(13);
    assertEquals(expectedCrc, frameCrc);
  }

  @Test
  void encodeReliableReplay_validPayload_setsReplayFlag() {
    buf = Unpooled.buffer(64);
    final var payload = new byte[] {0x01};
    final long seqNo = 99L;

    FrameParser.encodeReliableReplay(buf, seqNo, payload, 0, payload.length);

    // Skip totalLength (4) + seqNo (8) = offset 12
    final byte flags = buf.getByte(12);
    final byte expectedFlags = (byte) (FrameParser.FLAG_RELIABLE | FrameParser.FLAG_REPLAY);
    assertEquals(expectedFlags, flags);

    // Verify total length includes reliable header
    final int totalLength = buf.getIntLE(0);
    assertEquals(FrameParser.RELIABLE_HEADER_SIZE + payload.length, totalLength);
  }

  @Test
  void isValidFlagCombination_allValidCombos_returnsTrue() {
    // 0b0000 — best-effort live
    assertTrue(FrameParser.isValidFlagCombination((byte) 0x00));
    // 0b0001 — reliable live
    assertTrue(FrameParser.isValidFlagCombination(FrameParser.FLAG_RELIABLE));
    // 0b0011 — reliable replay
    assertTrue(
        FrameParser.isValidFlagCombination(
            (byte) (FrameParser.FLAG_RELIABLE | FrameParser.FLAG_REPLAY)));
    // 0b0100 — snapshot (more fragments)
    assertTrue(FrameParser.isValidFlagCombination(FrameParser.FLAG_SNAPSHOT));
    // 0b1100 — snapshot (final fragment)
    assertTrue(
        FrameParser.isValidFlagCombination(
            (byte) (FrameParser.FLAG_SNAPSHOT | FrameParser.FLAG_SNAPSHOT_FINAL)));
  }

  @Test
  void isValidFlagCombination_invalidCombos_returnsFalse() {
    // 0b0010 — replay without reliable
    assertFalse(FrameParser.isValidFlagCombination(FrameParser.FLAG_REPLAY));
    // 0b0101 — reliable with snapshot
    assertFalse(
        FrameParser.isValidFlagCombination(
            (byte) (FrameParser.FLAG_RELIABLE | FrameParser.FLAG_SNAPSHOT)));
  }

  @Test
  void isValidFlagCombination_reservedBitsSet_returnsFalse() {
    // Bit 4 set
    assertFalse(FrameParser.isValidFlagCombination((byte) 0x10));
    // Bit 5 set
    assertFalse(FrameParser.isValidFlagCombination((byte) 0x20));
    // Bit 7 set
    assertFalse(FrameParser.isValidFlagCombination((byte) 0x80));
    // All reserved bits set + valid reliable
    assertFalse(FrameParser.isValidFlagCombination((byte) 0xF1));
  }

  @Test
  void isReliable_reliableFlag_returnsTrue() {
    assertTrue(FrameParser.isReliable(FrameParser.FLAG_RELIABLE));
    assertTrue(
        FrameParser.isReliable((byte) (FrameParser.FLAG_RELIABLE | FrameParser.FLAG_REPLAY)));
    assertFalse(FrameParser.isReliable((byte) 0x00));
  }

  @Test
  void isSnapshot_snapshotFlag_returnsTrue() {
    assertTrue(FrameParser.isSnapshot(FrameParser.FLAG_SNAPSHOT));
    assertTrue(
        FrameParser.isSnapshot(
            (byte) (FrameParser.FLAG_SNAPSHOT | FrameParser.FLAG_SNAPSHOT_FINAL)));
    assertFalse(FrameParser.isSnapshot((byte) 0x00));
    assertFalse(FrameParser.isSnapshot(FrameParser.FLAG_RELIABLE));
  }

  @Test
  void isSnapshotFinal_bothBits_returnsTrue() {
    final byte finalSnapshot = (byte) (FrameParser.FLAG_SNAPSHOT | FrameParser.FLAG_SNAPSHOT_FINAL);
    assertTrue(FrameParser.isSnapshotFinal(finalSnapshot));
    // Snapshot without final bit is not final
    assertFalse(FrameParser.isSnapshotFinal(FrameParser.FLAG_SNAPSHOT));
    // Snapshot-final bit alone without snapshot bit is not final
    assertFalse(FrameParser.isSnapshotFinal(FrameParser.FLAG_SNAPSHOT_FINAL));
    // No bits
    assertFalse(FrameParser.isSnapshotFinal((byte) 0x00));
  }
}
