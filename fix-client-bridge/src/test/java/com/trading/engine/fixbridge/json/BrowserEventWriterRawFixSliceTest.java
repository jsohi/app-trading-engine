package com.trading.engine.fixbridge.json;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.trading.engine.fixbridge.translator.DecimalStringEmitter;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

/**
 * Byte-exact JSON output tests for {@link BrowserEventWriter#writeRawFixSlice}.
 *
 * <p>Verifies the zero-String flyweight path introduced in APP-40a Day 5: SOH (0x01) bytes in the
 * masked FIX slice are substituted with {@code |} at write time; direction is encoded from the
 * {@code boolean inbound} field rather than a pre-allocated String. All other writer contract
 * properties (writerIndex rollback on error, return value equals bytes written) are also verified.
 *
 * <p><b>Threading.</b> Single-threaded — test isolation.
 *
 * <p><b>Allocation.</b> Test-only — allocation is acceptable in tests.
 */
final class BrowserEventWriterRawFixSliceTest {

  // SOH byte (0x01) — FIX field delimiter used in the raw wire protocol.
  private static final byte SOH = 0x01;

  private final BrowserEventWriter writer = new BrowserEventWriter(new DecimalStringEmitter());

  private static ByteBuf fresh() {
    return Unpooled.buffer(512);
  }

  private static String drain(final ByteBuf buf, final int written) {
    final byte[] arr = new byte[written];
    buf.readBytes(arr);
    return new String(arr, StandardCharsets.UTF_8);
  }

  /**
   * Build a SOH-delimited FIX byte array from an array of tag=value strings. Each field is
   * terminated with a SOH byte (0x01) to mirror the real FIX wire format before SOH substitution.
   */
  private static byte[] sohFix(final String... fields) {
    final var sb = new StringBuilder();
    for (final var f : fields) {
      sb.append(f).append((char) 0x01);
    }
    return sb.toString().getBytes(StandardCharsets.US_ASCII);
  }

  // ---------------------------------------------------------------------------
  // SOH → | substitution — inbound direction.
  // ---------------------------------------------------------------------------

  @Test
  void writeRawFixSlice_inbound_sohSubstituted_emitsByteExactJson() {
    // Build a SOH-delimited FIX message (0x01 field terminators, as on the real wire).
    final byte[] scratch = sohFix("8=FIX.4.4", "35=D", "49=BRIDGE", "56=EXCH");
    final var e = new BrowserEvent.RawFixSlice(true, scratch, 0, scratch.length);
    final var buf = fresh();
    final int n = writer.writeRawFixSlice(e, buf);
    final var json = drain(buf, n);
    assertEquals(
        "{\"type\":\"RawFix\",\"direction\":\"in\",\"fix\":\"8=FIX.4.4|35=D|49=BRIDGE|56=EXCH|\"}",
        json,
        "SOH bytes (0x01) must be replaced by '|' in the emitted JSON fix value");
  }

  // ---------------------------------------------------------------------------
  // Outbound direction.
  // ---------------------------------------------------------------------------

  @Test
  void writeRawFixSlice_outbound_directionIsOut() {
    final byte[] scratch = sohFix("8=FIX.4.4", "35=8");
    final var e = new BrowserEvent.RawFixSlice(false, scratch, 0, scratch.length);
    final var buf = fresh();
    final int n = writer.writeRawFixSlice(e, buf);
    final var json = drain(buf, n);
    assertTrue(json.contains("\"direction\":\"out\""), "inbound=false must emit direction=\"out\"");
    assertTrue(json.startsWith("{\"type\":\"RawFix\""), "must start with RawFix type header");
  }

  // ---------------------------------------------------------------------------
  // Slice offset and length sub-range.
  // ---------------------------------------------------------------------------

  @Test
  void writeRawFixSlice_nonZeroOffset_emitsOnlySubSlice() {
    // Prepend 5 garbage bytes before the actual FIX content to exercise off > 0.
    final byte[] garbage = "XXXXX".getBytes(StandardCharsets.US_ASCII);
    final byte[] fix = sohFix("8=FIX.4.4", "35=D");
    final byte[] scratch = new byte[garbage.length + fix.length];
    System.arraycopy(garbage, 0, scratch, 0, garbage.length);
    System.arraycopy(fix, 0, scratch, garbage.length, fix.length);
    final var e = new BrowserEvent.RawFixSlice(true, scratch, garbage.length, fix.length);
    final var buf = fresh();
    final int n = writer.writeRawFixSlice(e, buf);
    final var json = drain(buf, n);
    assertEquals(
        "{\"type\":\"RawFix\",\"direction\":\"in\",\"fix\":\"8=FIX.4.4|35=D|\"}",
        json,
        "Only the bytes within [off, off+len) must be emitted — garbage prefix must be excluded");
  }

  // ---------------------------------------------------------------------------
  // Return value equals actual bytes written.
  // ---------------------------------------------------------------------------

  @Test
  void writeRawFixSlice_returnValueEqualsActualBytesWritten() {
    final byte[] scratch = sohFix("8=FIX.4.4", "35=D");
    final var e = new BrowserEvent.RawFixSlice(true, scratch, 0, scratch.length);
    final var buf = fresh();
    final int n = writer.writeRawFixSlice(e, buf);
    assertTrue(n > 0, "must return positive byte count");
    assertEquals(n, buf.readableBytes(), "return value must equal readable bytes in buffer");
  }

  // ---------------------------------------------------------------------------
  // writeAny dispatches to writeRawFixSlice — byte-identical output.
  // ---------------------------------------------------------------------------

  @Test
  void writeAny_rawFixSlice_dispatchesToWriteRawFixSlice() {
    final byte[] scratch = sohFix("8=FIX.4.4", "35=D");
    final var e = new BrowserEvent.RawFixSlice(true, scratch, 0, scratch.length);
    final var direct = fresh();
    final var via = fresh();
    final int nDirect = writer.writeRawFixSlice(e, direct);
    final int nVia = writer.writeAny(e, via);
    assertEquals(nDirect, nVia, "writeAny must produce same byte count as writeRawFixSlice");
    assertEquals(
        drain(direct, nDirect),
        drain(via, nVia),
        "writeAny must produce byte-identical output to writeRawFixSlice");
  }

  // ---------------------------------------------------------------------------
  // Compact constructor validation — zero-len slice rejected.
  // ---------------------------------------------------------------------------

  @Test
  void rawFixSlice_zeroLen_compactCtorRejects() {
    final byte[] scratch = "abc".getBytes(StandardCharsets.US_ASCII);
    assertThrows(
        IllegalArgumentException.class,
        () -> new BrowserEvent.RawFixSlice(true, scratch, 0, 0),
        "RawFixSlice compact ctor must reject len=0");
  }

  // ---------------------------------------------------------------------------
  // Compact constructor validation — slice exceeds scratch length.
  // ---------------------------------------------------------------------------

  @Test
  void rawFixSlice_sliceExceedsScratch_compactCtorRejects() {
    final byte[] scratch = "abc".getBytes(StandardCharsets.US_ASCII);
    assertThrows(
        IllegalArgumentException.class,
        () -> new BrowserEvent.RawFixSlice(true, scratch, 0, scratch.length + 1),
        "RawFixSlice compact ctor must reject off+len > scratch.length");
  }

  // ---------------------------------------------------------------------------
  // Compact constructor validation — null scratch rejected.
  // ---------------------------------------------------------------------------

  @Test
  void rawFixSlice_nullScratch_compactCtorRejects() {
    assertThrows(
        NullPointerException.class,
        () -> new BrowserEvent.RawFixSlice(true, null, 0, 1),
        "RawFixSlice compact ctor must reject null scratch");
  }

  // ---------------------------------------------------------------------------
  // Compact constructor validation — negative offset rejected.
  // ---------------------------------------------------------------------------

  @Test
  void rawFixSlice_negativeOffset_compactCtorRejects() {
    final byte[] scratch = "abc".getBytes(StandardCharsets.US_ASCII);
    assertThrows(
        IllegalArgumentException.class,
        () -> new BrowserEvent.RawFixSlice(true, scratch, -1, 1),
        "RawFixSlice compact ctor must reject off < 0");
  }

  // ---------------------------------------------------------------------------
  // JSON-escaping in slice (Gemini PR #70 R4 medium fix).
  // The writer used to throw on '"' and '\\'; that dropped the entire RawFix event for any
  // legitimate FIX message containing these chars (e.g. Text 58 / EncodedText 96). The new
  // behaviour escapes inline so the debug tap survives.
  // ---------------------------------------------------------------------------

  @Test
  void writeRawFixSlice_embeddedQuote_escapedAsBackslashQuote() {
    final byte[] scratch = "8=FIX.4.4\001\"oops".getBytes(StandardCharsets.US_ASCII);
    final var e = new BrowserEvent.RawFixSlice(true, scratch, 0, scratch.length);
    final var buf = fresh();
    writer.writeRawFixSlice(e, buf);
    final var json = buf.toString(StandardCharsets.UTF_8);
    // The escaped quote appears as \" inside the JSON string field.
    assertTrue(
        json.contains("\\\""),
        "writeRawFixSlice must JSON-escape embedded double-quote, got: " + json);
    assertTrue(json.contains("|"), "SOH must still substitute to | as before");
  }

  @Test
  void writeRawFixSlice_embeddedBackslash_escapedAsDoubleBackslash() {
    final byte[] scratch = "8=FIX.4.4\001\\oops".getBytes(StandardCharsets.US_ASCII);
    final var e = new BrowserEvent.RawFixSlice(true, scratch, 0, scratch.length);
    final var buf = fresh();
    writer.writeRawFixSlice(e, buf);
    final var json = buf.toString(StandardCharsets.UTF_8);
    assertTrue(
        json.contains("\\\\"),
        "writeRawFixSlice must JSON-escape embedded backslash, got: " + json);
    assertTrue(json.contains("|"), "SOH must still substitute to | as before");
  }
}
