package com.trading.engine.fixbridge.json;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.trading.engine.fixbridge.translator.DecimalStringEmitter;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

/**
 * Verifies {@link BrowserEventWriter} renders byte-exact JSON for every outbound event kind.
 *
 * <p>Pairs with {@link BrowserMessageReaderTest}: the emitted bytes for {@code Quote}, {@code
 * ExecutionReport}, etc. are the same wire format we will round-trip through the parser in Phase 4
 * (no full round-trip yet — outbound-only here).
 */
final class BrowserEventWriterTest {

  // ---------------------------------------------------------------------------
  // Helpers.
  // ---------------------------------------------------------------------------

  private final BrowserEventWriter writer = new BrowserEventWriter(new DecimalStringEmitter());

  private static String drain(final ByteBuf buf, final int written) {
    final var arr = new byte[written];
    buf.readBytes(arr);
    return new String(arr, StandardCharsets.UTF_8);
  }

  // ---------------------------------------------------------------------------
  // Quote.
  // ---------------------------------------------------------------------------

  @Test
  void writeQuote_typicalPayload_emitsByteExactJson() {
    final var e =
        new BrowserEvent.Quote(
            "R-1",
            "Q-7",
            "EURUSD",
            "Buy",
            100_000_050_000_000L,
            110_000_000L,
            1_700_000_000_000_000_000L);
    final var buf = Unpooled.buffer(256);
    final int n = writer.writeQuote(e, buf);
    final var json = drain(buf, n);
    assertEquals(
        "{\"type\":\"Quote\",\"reqId\":\"R-1\",\"quoteId\":\"Q-7\",\"symbol\":\"EURUSD\","
            + "\"side\":\"Buy\",\"qty\":\"1000000.50000000\",\"price\":\"1.10000000\","
            + "\"expiryNs\":1700000000000000000}",
        json);
  }

  @Test
  void writeQuote_negativePrice_emitsSignPrefix() {
    final var e = new BrowserEvent.Quote("R", "Q", "X", "Sell", 100_000_000L, -100_000_000L, 0L);
    final var buf = Unpooled.buffer(256);
    final int n = writer.writeQuote(e, buf);
    assertTrue(drain(buf, n).contains("\"price\":\"-1.00000000\""));
  }

  // ---------------------------------------------------------------------------
  // ExecutionReport.
  // ---------------------------------------------------------------------------

  @Test
  void writeExecutionReport_filled_emitsByteExactJson() {
    final var e =
        new BrowserEvent.ExecutionReport(
            "C-1", "EX-1", 'F', '2', "EURUSD", "Buy", 100_000_000_000_000L, 0L, 110_000_000L);
    final var buf = Unpooled.buffer(256);
    final int n = writer.writeExecutionReport(e, buf);
    assertEquals(
        "{\"type\":\"ExecutionReport\",\"clOrdId\":\"C-1\",\"execId\":\"EX-1\","
            + "\"execType\":\"F\",\"ordStatus\":\"2\",\"symbol\":\"EURUSD\",\"side\":\"Buy\","
            + "\"cumQty\":\"1000000.00000000\",\"leavesQty\":\"0.00000000\","
            + "\"avgPx\":\"1.10000000\"}",
        drain(buf, n));
  }

  @Test
  void writeExecutionReport_nonPrintableExecType_throwsIllegalArgument() {
    final var e = new BrowserEvent.ExecutionReport("C", "X", '\n', '0', "S", "Buy", 0L, 0L, 0L);
    final var buf = Unpooled.buffer(256);
    assertThrows(IllegalArgumentException.class, () -> writer.writeExecutionReport(e, buf));
  }

  // ---------------------------------------------------------------------------
  // OrderReject.
  // ---------------------------------------------------------------------------

  @Test
  void writeOrderReject_typicalPayload_emitsByteExactJson() {
    final var e = new BrowserEvent.OrderReject("C-1", "bridge-down");
    final var buf = Unpooled.buffer(128);
    final int n = writer.writeOrderReject(e, buf);
    assertEquals(
        "{\"type\":\"OrderReject\",\"clOrdId\":\"C-1\",\"reason\":\"bridge-down\"}", drain(buf, n));
  }

  @Test
  void writeOrderReject_reasonContainsForbiddenQuote_throwsIllegalArgument() {
    final var e = new BrowserEvent.OrderReject("C-1", "has\"quote");
    final var buf = Unpooled.buffer(128);
    assertThrows(IllegalArgumentException.class, () -> writer.writeOrderReject(e, buf));
  }

  @Test
  void writeOrderReject_reasonContainsBackslash_throwsIllegalArgument() {
    final var e = new BrowserEvent.OrderReject("C-1", "has\\back");
    final var buf = Unpooled.buffer(128);
    assertThrows(IllegalArgumentException.class, () -> writer.writeOrderReject(e, buf));
  }

  @Test
  void writeOrderReject_reasonContainsControlChar_throwsIllegalArgument() {
    final var e = new BrowserEvent.OrderReject("C-1", "x\nbad");
    final var buf = Unpooled.buffer(128);
    assertThrows(IllegalArgumentException.class, () -> writer.writeOrderReject(e, buf));
  }

  // ---------------------------------------------------------------------------
  // BridgeStatus.
  // ---------------------------------------------------------------------------

  @Test
  void writeBridgeStatus_fixUpAndNonFatal_emitsByteExactJson() {
    final var e = new BrowserEvent.BridgeStatus(true, false, "ready");
    final var buf = Unpooled.buffer(128);
    final int n = writer.writeBridgeStatus(e, buf);
    assertEquals(
        "{\"type\":\"BridgeStatus\",\"fixSessionUp\":true,\"fatal\":false,"
            + "\"reason\":\"ready\"}",
        drain(buf, n));
  }

  @Test
  void writeBridgeStatus_fatalShutdown_emitsByteExactJson() {
    final var e = new BrowserEvent.BridgeStatus(false, true, "shutdown");
    final var buf = Unpooled.buffer(128);
    final int n = writer.writeBridgeStatus(e, buf);
    assertEquals(
        "{\"type\":\"BridgeStatus\",\"fixSessionUp\":false,\"fatal\":true,"
            + "\"reason\":\"shutdown\"}",
        drain(buf, n));
  }

  // ---------------------------------------------------------------------------
  // RawFix.
  // ---------------------------------------------------------------------------

  @Test
  void writeRawFix_inboundDirection_emitsByteExactJson() {
    final var e = new BrowserEvent.RawFix("in", "8=FIX.4.4|9=10|35=8|10=000|");
    final var buf = Unpooled.buffer(256);
    final int n = writer.writeRawFix(e, buf);
    assertEquals(
        "{\"type\":\"RawFix\",\"direction\":\"in\"," + "\"fix\":\"8=FIX.4.4|9=10|35=8|10=000|\"}",
        drain(buf, n));
  }

  // ---------------------------------------------------------------------------
  // AuthExpired.
  // ---------------------------------------------------------------------------

  @Test
  void writeAuthExpired_emitsByteExactJson() {
    final var buf = Unpooled.buffer(64);
    final int n = writer.writeAuthExpired(buf);
    assertEquals("{\"type\":\"AuthExpired\"}", drain(buf, n));
  }

  @Test
  void browserEventAuthExpiredSingleton_isStableInstance() {
    assertSame(BrowserEvent.AuthExpired.INSTANCE, BrowserEvent.AuthExpired.INSTANCE);
  }

  // ---------------------------------------------------------------------------
  // Error.
  // ---------------------------------------------------------------------------

  @Test
  void writeError_typicalPayload_emitsByteExactJson() {
    final var e = new BrowserEvent.Error("malformed");
    final var buf = Unpooled.buffer(64);
    final int n = writer.writeError(e, buf);
    assertEquals("{\"type\":\"Error\",\"reason\":\"malformed\"}", drain(buf, n));
  }

  // ---------------------------------------------------------------------------
  // Sanity: writer works against an UnpooledByteBufAllocator instance too.
  // ---------------------------------------------------------------------------

  @Test
  void writeQuote_unpooledAllocatedBuffer_emitsExpectedLength() {
    final var buf = UnpooledByteBufAllocator.DEFAULT.buffer(256);
    try {
      final var e = new BrowserEvent.Quote("R", "Q", "S", "Buy", 100_000_000L, 100_000_000L, 0L);
      final int n = writer.writeQuote(e, buf);
      assertTrue(n > 0);
      assertEquals(buf.readableBytes(), n);
    } finally {
      buf.release();
    }
  }

  @Test
  void writer_nullDecimalEmitter_throwsIllegalArgument() {
    assertThrows(IllegalArgumentException.class, () -> new BrowserEventWriter(null));
  }
}
