/*
 * APP-36 §5.8.1 — cross-stack binary fixture emitter.
 *
 * Writes a small deterministic protocol conversation to
 * web-ui/test/fixtures/protocol-conversation.bin during
 * :websocket-server:test. The web-ui browser-tier
 * CrossStack.browser.test.ts (C9) replays this file through
 * FrameParser + the generated SBE TS decoders to assert byte-for-byte
 * cross-stack agreement.
 *
 * Wire format mirrors the server (FrameParser.java) and the plan §2.1:
 *   13B header (best-effort): totalLength u32 LE, seqNo i64 LE, flags u8
 *   17B header (reliable):    same + crc32c u32 LE at offset 13
 *   payload:                  SBE-encoded message (header + body)
 *
 * CRC32C: Castagnoli polynomial (java.util.zip.CRC32C), seed 0,
 *   no post-XOR. Region = header[0..12] ‖ payload.
 *
 * Threading: build-time only.
 *
 * Allocation: trivial.
 *
 * Plan reference: APP-36 §5.8 / §5.8.1 / §5.8.3.
 */
package com.trading.engine.websocket;

import static org.junit.jupiter.api.Assertions.assertTrue;

import com.trading.engine.messages.sbe.MessageHeaderEncoder;
import com.trading.engine.messages.sbe.ReplayCompleteEncoder;
import com.trading.engine.messages.sbe.WebSocketAuthAckEncoder;
import com.trading.engine.messages.sbe.WebSocketErrorCode;
import com.trading.engine.messages.sbe.WebSocketErrorEncoder;
import com.trading.engine.messages.sbe.WebSocketHeartbeatEncoder;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.zip.CRC32C;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** Emits the binary cross-stack fixture consumed by the web-ui browser tier. */
final class WebSocketProtocolFixturesTest {

  private static final int FLAG_RELIABLE = 0x01;
  private static final int FLAG_REPLAY = 0x02;
  private static final int FLAG_SNAPSHOT_FINAL = 0x0C;
  private static final int BEST_EFFORT_HEADER = 13;
  private static final int RELIABLE_HEADER = 17;

  @Test
  @DisplayName("emit_protocolConversation_writesDeterministicFixtureForCrossStack")
  void emit_protocolConversation_writesDeterministicFixtureForCrossStack() throws IOException {
    final Path fixturesDir = findRepoRoot().resolve("web-ui/test/fixtures");
    Files.createDirectories(fixturesDir);
    final Path target = fixturesDir.resolve("protocol-conversation.bin");

    try (final OutputStream out = Files.newOutputStream(target)) {
      // Frame 1: best-effort WebSocketHeartbeat — server-side cadence pulse.
      final var heartbeatPayload = encodeHeartbeat(0xDEADBEEFCAFE0001L);
      writeBestEffort(out, heartbeatPayload);

      // Frame 2: reliable WebSocketAuthAck — sessionId + intervals (§A1).
      final var ackPayload = encodeAuthAck(0x1L, 0x2L, 1, 100, 5_000, 10_000);
      writeReliable(out, 1L, FLAG_RELIABLE, ackPayload);

      // Frame 3: reliable replay-tagged ReplayComplete (template 72) —
      // exercises the FLAG_REPLAY flag combo and the no-gap path. The
      // SBE body is empty per schema; the seqNo lives in the frame envelope
      // (we pass 2L to writeReliable below).
      final var replayCompletePayload = encodeReplayComplete();
      writeReliable(out, 2L, FLAG_RELIABLE | FLAG_REPLAY, replayCompletePayload);

      // Frame 4: best-effort WebSocketError — server-side notification of
      // BACKPRESSURE (code 9 SlowConsumer per §2.13).
      final var errorPayload = encodeError(WebSocketErrorCode.SlowConsumer);
      writeBestEffort(out, errorPayload);

      // Frame 5: snapshot-final flag combo — terminator of a 1-fragment
      // logical snapshot. Payload here is a placeholder zero-byte buffer;
      // the C9 cross-stack test extends with realistic snapshot bodies as
      // the snapshot-template encoders are wired in.
      writeReliable(out, 3L, FLAG_SNAPSHOT_FINAL | FLAG_RELIABLE, new byte[0]);
    }

    assertTrue(Files.exists(target), "fixture not written: " + target);
    assertTrue(Files.size(target) > 0, "fixture is empty: " + target);
  }

  // ─── Frame envelope encoders (mirror FrameParser.java) ─────────────

  private static void writeBestEffort(final OutputStream out, final byte[] payload)
      throws IOException {
    final int totalLen = BEST_EFFORT_HEADER + payload.length;
    final var header = ByteBuffer.allocate(BEST_EFFORT_HEADER).order(ByteOrder.LITTLE_ENDIAN);
    header.putInt(totalLen);
    header.putLong(0L); // seqNo = 0 on best-effort
    header.put((byte) 0x00); // flags = 0
    out.write(header.array());
    out.write(payload);
  }

  private static void writeReliable(
      final OutputStream out, final long seqNo, final int flags, final byte[] payload)
      throws IOException {
    final int totalLen = RELIABLE_HEADER + payload.length;
    // Header bytes 0..12 (13B) participate in CRC; CRC itself sits at 13..16.
    final var headerNoCrc = ByteBuffer.allocate(BEST_EFFORT_HEADER).order(ByteOrder.LITTLE_ENDIAN);
    headerNoCrc.putInt(totalLen);
    headerNoCrc.putLong(seqNo);
    headerNoCrc.put((byte) (flags | FLAG_RELIABLE));
    final byte[] headerBytes = headerNoCrc.array();

    final var crc = new CRC32C();
    crc.reset();
    crc.update(headerBytes, 0, BEST_EFFORT_HEADER);
    if (payload.length > 0) {
      crc.update(payload, 0, payload.length);
    }
    final int crcValue = (int) crc.getValue();

    final var crcBytes = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);
    crcBytes.putInt(crcValue);

    out.write(headerBytes);
    out.write(crcBytes.array());
    out.write(payload);
  }

  // ─── SBE message encoders ──────────────────────────────────────────

  private static byte[] encodeHeartbeat(final long serverNanos) {
    final var buf = new UnsafeBuffer(new byte[64]);
    final var enc = new WebSocketHeartbeatEncoder();
    enc.wrapAndApplyHeader(buf, 0, new MessageHeaderEncoder());
    enc.serverNanos(serverNanos);
    final int len = MessageHeaderEncoder.ENCODED_LENGTH + enc.encodedLength();
    final byte[] copy = new byte[len];
    buf.getBytes(0, copy, 0, len);
    return copy;
  }

  private static byte[] encodeAuthAck(
      final long sidMsb,
      final long sidLsb,
      final int protocolVersion,
      final int maxSubs,
      final int serverIntervalMs,
      final int clientIntervalMs) {
    final var buf = new UnsafeBuffer(new byte[128]);
    final var enc = new WebSocketAuthAckEncoder();
    enc.wrapAndApplyHeader(buf, 0, new MessageHeaderEncoder());
    enc.sessionId().mostSignificantBits(sidMsb).leastSignificantBits(sidLsb);
    enc.protocolVersion(protocolVersion);
    enc.maxSubscriptions(maxSubs);
    enc.serverHeartbeatIntervalMs(serverIntervalMs);
    enc.clientHeartbeatIntervalMs(clientIntervalMs);
    final int len = MessageHeaderEncoder.ENCODED_LENGTH + enc.encodedLength();
    final byte[] copy = new byte[len];
    buf.getBytes(0, copy, 0, len);
    return copy;
  }

  /**
   * Encodes a ReplayComplete (template 72) — header-only per schema.
   *
   * <p>The reliable-stream sequence number lives in the frame envelope, not in the SBE body, so
   * this encoder takes no parameters. Callers pass {@code seqNo} to {@link
   * #writeReliable(OutputStream, long, int, byte[])} instead.
   */
  private static byte[] encodeReplayComplete() {
    final var buf = new UnsafeBuffer(new byte[16]);
    final var enc = new ReplayCompleteEncoder();
    enc.wrapAndApplyHeader(buf, 0, new MessageHeaderEncoder());
    final int len = MessageHeaderEncoder.ENCODED_LENGTH + enc.encodedLength();
    final byte[] copy = new byte[len];
    buf.getBytes(0, copy, 0, len);
    return copy;
  }

  private static byte[] encodeError(final WebSocketErrorCode code) {
    final var buf = new UnsafeBuffer(new byte[128]);
    final var enc = new WebSocketErrorEncoder();
    enc.wrapAndApplyHeader(buf, 0, new MessageHeaderEncoder());
    enc.errorCode(code);
    // varData errorText — empty for this fixture; the C9 cross-stack test
    // extends with realistic payloads keyed off ErrorTextRegistry entries.
    final byte[] empty = new byte[0];
    enc.putErrorText(empty, 0, 0);
    final int len = MessageHeaderEncoder.ENCODED_LENGTH + enc.encodedLength();
    final byte[] copy = new byte[len];
    buf.getBytes(0, copy, 0, len);
    return copy;
  }

  // ─── Repo-root resolver (worktree-friendly) ────────────────────────

  private static Path findRepoRoot() throws IOException {
    Path cur = Paths.get("").toAbsolutePath();
    for (int i = 0; i < 10; i++) {
      if (Files.isDirectory(cur.resolve("web-ui"))
          && Files.isRegularFile(cur.resolve("settings.gradle.kts"))) {
        return cur;
      }
      final Path parent = cur.getParent();
      if (parent == null) break;
      cur = parent;
    }
    throw new IOException("could not locate repo root from " + Paths.get("").toAbsolutePath());
  }
}
