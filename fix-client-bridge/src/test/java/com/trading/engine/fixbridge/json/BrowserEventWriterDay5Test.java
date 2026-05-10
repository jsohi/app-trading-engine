package com.trading.engine.fixbridge.json;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.trading.engine.fixbridge.translator.DecimalStringEmitter;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for the Day 5 extensions to {@link BrowserEventWriter}: {@code writeAccountLimits},
 * {@code writeSessionTerminated}, {@code writeOrderReconciled}, {@code writeOrderStatusReply}, the
 * extended 7-field {@code writeBridgeStatus}, the backwards-compat 3-arg {@link
 * BrowserEvent.BridgeStatus} ctor, and the polymorphic {@code writeAny} dispatch.
 *
 * <p>Verifies byte-exact JSON output for each new method. All numerics are emitted as quoted
 * decimal strings at 8 fractional digits (10^-8 scale) by {@link DecimalStringEmitter}.
 *
 * <p><b>Threading.</b> Single-threaded — test-only.
 *
 * <p><b>Allocation.</b> Test-only — allocation is acceptable.
 */
final class BrowserEventWriterDay5Test {

  // ---------------------------------------------------------------------------
  // Helpers.
  // ---------------------------------------------------------------------------

  private final BrowserEventWriter writer = new BrowserEventWriter(new DecimalStringEmitter());

  private static String drain(final ByteBuf buf, final int written) {
    final byte[] arr = new byte[written];
    buf.readBytes(arr);
    return new String(arr, StandardCharsets.UTF_8);
  }

  private static ByteBuf fresh() {
    return Unpooled.buffer(512);
  }

  // ---------------------------------------------------------------------------
  // writeAccountLimits.
  // ---------------------------------------------------------------------------

  @Test
  void writeAccountLimits_typicalPayload_emitsByteExactJson() {
    // 100.00000000 qty, 1000000.00000000 notional, 50 bps, 10 OPS.
    final long maxQty = 100L * 100_000_000L; // 100.00000000
    final long maxNotional = 1_000_000L * 100_000_000L; // 1000000.00000000
    final var e = new BrowserEvent.AccountLimits("ACME-001", maxQty, maxNotional, 50, 10);
    final var buf = fresh();
    final int n = writer.writeAccountLimits(e, buf);
    final var json = drain(buf, n);
    assertEquals(
        "{\"type\":\"AccountLimits\",\"account\":\"ACME-001\","
            + "\"maxQty\":\"100.00000000\",\"maxNotional\":\"1000000.00000000\","
            + "\"priceDeviationBps\":50,\"maxOrdersPerSecond\":10}",
        json);
  }

  @Test
  void writeAccountLimits_zeroValues_emitsZeroDecimalStrings() {
    final var e = new BrowserEvent.AccountLimits("ZERO-ACCT", 0L, 0L, 0, 0);
    final var buf = fresh();
    final int n = writer.writeAccountLimits(e, buf);
    final var json = drain(buf, n);
    assertEquals(
        "{\"type\":\"AccountLimits\",\"account\":\"ZERO-ACCT\","
            + "\"maxQty\":\"0.00000000\",\"maxNotional\":\"0.00000000\","
            + "\"priceDeviationBps\":0,\"maxOrdersPerSecond\":0}",
        json);
  }

  // ---------------------------------------------------------------------------
  // writeSessionTerminated.
  // ---------------------------------------------------------------------------

  @Test
  void writeSessionTerminated_emitsByteExactJson() {
    final var buf = fresh();
    final int n = writer.writeSessionTerminated(buf);
    final var json = drain(buf, n);
    assertEquals("{\"type\":\"SessionTerminated\"}", json);
  }

  @Test
  void writeSessionTerminated_singletonInstance_sameJson() {
    final var buf1 = fresh();
    final var buf2 = fresh();
    final int n1 = writer.writeSessionTerminated(buf1);
    final int n2 = writer.writeSessionTerminated(buf2);
    assertEquals(drain(buf1, n1), drain(buf2, n2));
  }

  // ---------------------------------------------------------------------------
  // writeOrderReconciled.
  // ---------------------------------------------------------------------------

  @Test
  void writeOrderReconciled_typicalPayload_emitsByteExactJson() {
    // cumQty=50.50000000, leavesQty=49.50000000, avgPx=1.10000000
    final long cumQty = 50_50_000_000L; // 5050000000 / 10^8 = 50.50000000
    final long leavesQty = 49_50_000_000L; // 4950000000 / 10^8 = 49.50000000
    final long avgPx = 110_000_000L; // 1.10000000
    final var e = new BrowserEvent.OrderReconciled("C-001", "Filled", cumQty, leavesQty, avgPx);
    final var buf = fresh();
    final int n = writer.writeOrderReconciled(e, buf);
    final var json = drain(buf, n);
    assertEquals(
        "{\"type\":\"OrderReconciled\",\"clOrdId\":\"C-001\",\"status\":\"Filled\","
            + "\"cumQty\":\"50.50000000\",\"leavesQty\":\"49.50000000\","
            + "\"avgPx\":\"1.10000000\"}",
        json);
  }

  // ---------------------------------------------------------------------------
  // writeOrderStatusReply.
  // ---------------------------------------------------------------------------

  @Test
  void writeOrderStatusReply_nullLastExecId_omitsLastExecIdKey() {
    final long cumQty = 100_000_000L; // 1.00000000
    final long leavesQty = 0L;
    final long avgPx = 110_000_000L; // 1.10000000
    final var e =
        new BrowserEvent.OrderStatusReply("C-002", "Filled", cumQty, leavesQty, avgPx, null);
    final var buf = fresh();
    final int n = writer.writeOrderStatusReply(e, buf);
    final var json = drain(buf, n);
    assertTrue(
        !json.contains("lastExecId"), "lastExecId key must be omitted when lastExecId is null");
    assertEquals(
        "{\"type\":\"OrderStatusReply\",\"clOrdId\":\"C-002\",\"status\":\"Filled\","
            + "\"cumQty\":\"1.00000000\",\"leavesQty\":\"0.00000000\","
            + "\"avgPx\":\"1.10000000\"}",
        json);
  }

  @Test
  void writeOrderStatusReply_nonNullLastExecId_includesLastExecIdAfterAvgPx() {
    final long cumQty = 100_000_000L; // 1.00000000
    final long leavesQty = 0L;
    final long avgPx = 110_000_000L; // 1.10000000
    final var e =
        new BrowserEvent.OrderStatusReply(
            "C-003", "PartiallyFilled", cumQty, leavesQty, avgPx, "E-123");
    final var buf = fresh();
    final int n = writer.writeOrderStatusReply(e, buf);
    final var json = drain(buf, n);
    assertEquals(
        "{\"type\":\"OrderStatusReply\",\"clOrdId\":\"C-003\",\"status\":\"PartiallyFilled\","
            + "\"cumQty\":\"1.00000000\",\"leavesQty\":\"0.00000000\","
            + "\"avgPx\":\"1.10000000\",\"lastExecId\":\"E-123\"}",
        json);
  }

  // ---------------------------------------------------------------------------
  // writeBridgeStatus — 7-field form.
  // ---------------------------------------------------------------------------

  @Test
  void writeBridgeStatus_sevenArgForm_emitsAllSevenFieldsInOrder() {
    // fixSessionUp=true, fatal=false, reason="ready", newOrders=true, newQuotes=true,
    // protocolVersion=1, serverOrderTimeoutMs=30000
    final var e = new BrowserEvent.BridgeStatus(true, false, "ready", true, true, 1, 30_000L);
    final var buf = fresh();
    final int n = writer.writeBridgeStatus(e, buf);
    final var json = drain(buf, n);
    assertEquals(
        "{\"type\":\"BridgeStatus\",\"fixSessionUp\":true,\"fatal\":false,"
            + "\"reason\":\"ready\",\"newOrders\":true,\"newQuotes\":true,"
            + "\"protocolVersion\":1,\"serverOrderTimeoutMs\":30000}",
        json);
  }

  @Test
  void writeBridgeStatus_sevenArgForm_killSwitchGated() {
    // newOrders=false, newQuotes=false — runtime kill-switch
    final var e = new BrowserEvent.BridgeStatus(true, false, "kill-switch", false, false, 2, 0L);
    final var buf = fresh();
    final int n = writer.writeBridgeStatus(e, buf);
    final var json = drain(buf, n);
    assertTrue(json.contains("\"newOrders\":false"), "newOrders must be false");
    assertTrue(json.contains("\"newQuotes\":false"), "newQuotes must be false");
    assertTrue(json.contains("\"protocolVersion\":2"), "protocolVersion must be 2");
    assertTrue(json.contains("\"serverOrderTimeoutMs\":0"), "serverOrderTimeoutMs must be 0");
  }

  @Test
  void writeBridgeStatus_fatalTrue_reason_outboundStall_emitsCorrectJson() {
    final var e = new BrowserEvent.BridgeStatus(true, true, "outbound-stall", false, false, 1, 0L);
    final var buf = fresh();
    final int n = writer.writeBridgeStatus(e, buf);
    final var json = drain(buf, n);
    assertTrue(json.contains("\"fatal\":true"), "fatal must be true");
    assertTrue(json.contains("\"reason\":\"outbound-stall\""), "reason must be outbound-stall");
  }

  // ---------------------------------------------------------------------------
  // writeBridgeStatus — 3-arg backwards-compat ctor → emits all 7 fields with defaults.
  // ---------------------------------------------------------------------------

  @Test
  void writeBridgeStatus_threeArgCompat_emitsAllSevenFieldsWithDefaults() {
    // 3-arg ctor defaults: newOrders=true, newQuotes=true, protocolVersion=1,
    // serverOrderTimeoutMs=0
    final var e = new BrowserEvent.BridgeStatus(true, false, "startup");
    final var buf = fresh();
    final int n = writer.writeBridgeStatus(e, buf);
    final var json = drain(buf, n);
    assertEquals(
        "{\"type\":\"BridgeStatus\",\"fixSessionUp\":true,\"fatal\":false,"
            + "\"reason\":\"startup\",\"newOrders\":true,\"newQuotes\":true,"
            + "\"protocolVersion\":1,\"serverOrderTimeoutMs\":0}",
        json);
  }

  // ---------------------------------------------------------------------------
  // writeAny — polymorphic dispatch.
  // ---------------------------------------------------------------------------

  @Test
  void writeAny_quote_dispatchesToWriteQuote() {
    final var e =
        new BrowserEvent.Quote("R-1", "Q-1", "EUR/USD", "Buy", 100_000_000L, 110_000_000L, 1_000L);
    final var direct = fresh();
    final var via = fresh();
    final int nDirect = writer.writeQuote(e, direct);
    final int nVia = writer.writeAny(e, via);
    assertEquals(nDirect, nVia);
    assertEquals(drain(direct, nDirect), drain(via, nVia));
  }

  @Test
  void writeAny_executionReport_dispatchesToWriteExecutionReport() {
    final var e =
        new BrowserEvent.ExecutionReport(
            "C1", "EX1", 'F', '2', "AAPL", "Buy", 100L, 0L, 110_000_000L);
    final var direct = fresh();
    final var via = fresh();
    final int nDirect = writer.writeExecutionReport(e, direct);
    final int nVia = writer.writeAny(e, via);
    assertEquals(nDirect, nVia);
    assertEquals(drain(direct, nDirect), drain(via, nVia));
  }

  @Test
  void writeAny_orderReject_dispatchesToWriteOrderReject() {
    final var e = new BrowserEvent.OrderReject("C1", "bridge-down");
    final var direct = fresh();
    final var via = fresh();
    final int nDirect = writer.writeOrderReject(e, direct);
    final int nVia = writer.writeAny(e, via);
    assertEquals(nDirect, nVia);
    assertEquals(drain(direct, nDirect), drain(via, nVia));
  }

  @Test
  void writeAny_bridgeStatus_dispatchesToWriteBridgeStatus() {
    final var e = new BrowserEvent.BridgeStatus(false, true, "shutdown", false, false, 1, 0L);
    final var direct = fresh();
    final var via = fresh();
    final int nDirect = writer.writeBridgeStatus(e, direct);
    final int nVia = writer.writeAny(e, via);
    assertEquals(nDirect, nVia);
    assertEquals(drain(direct, nDirect), drain(via, nVia));
  }

  @Test
  void writeAny_rawFix_dispatchesToWriteRawFix() {
    final var e = new BrowserEvent.RawFix("in", "8=FIX.4.4|...");
    final var direct = fresh();
    final var via = fresh();
    final int nDirect = writer.writeRawFix(e, direct);
    final int nVia = writer.writeAny(e, via);
    assertEquals(nDirect, nVia);
    assertEquals(drain(direct, nDirect), drain(via, nVia));
  }

  @Test
  void writeAny_authExpired_dispatchesToWriteAuthExpired() {
    final var e = BrowserEvent.AuthExpired.INSTANCE;
    final var direct = fresh();
    final var via = fresh();
    final int nDirect = writer.writeAuthExpired(direct);
    final int nVia = writer.writeAny(e, via);
    assertEquals(nDirect, nVia);
    assertEquals(drain(direct, nDirect), drain(via, nVia));
  }

  @Test
  void writeAny_error_dispatchesToWriteError() {
    final var e = new BrowserEvent.Error("malformed", "QuoteRequest:R-7");
    final var direct = fresh();
    final var via = fresh();
    final int nDirect = writer.writeError(e, direct);
    final int nVia = writer.writeAny(e, via);
    assertEquals(nDirect, nVia);
    assertEquals(drain(direct, nDirect), drain(via, nVia));
  }

  @Test
  void writeAny_accountLimits_dispatchesToWriteAccountLimits() {
    final var e =
        new BrowserEvent.AccountLimits("ACME-001", 100_000_000L, 1_000_000_000_000L, 25, 5);
    final var direct = fresh();
    final var via = fresh();
    final int nDirect = writer.writeAccountLimits(e, direct);
    final int nVia = writer.writeAny(e, via);
    assertEquals(nDirect, nVia);
    assertEquals(drain(direct, nDirect), drain(via, nVia));
  }

  @Test
  void writeAny_sessionTerminated_dispatchesToWriteSessionTerminated() {
    final var e = BrowserEvent.SessionTerminated.INSTANCE;
    final var direct = fresh();
    final var via = fresh();
    final int nDirect = writer.writeSessionTerminated(direct);
    final int nVia = writer.writeAny(e, via);
    assertEquals(nDirect, nVia);
    assertEquals(drain(direct, nDirect), drain(via, nVia));
  }

  @Test
  void writeAny_orderReconciled_dispatchesToWriteOrderReconciled() {
    final var e = new BrowserEvent.OrderReconciled("C-001", "Cancelled", 0L, 100_000_000L, 0L);
    final var direct = fresh();
    final var via = fresh();
    final int nDirect = writer.writeOrderReconciled(e, direct);
    final int nVia = writer.writeAny(e, via);
    assertEquals(nDirect, nVia);
    assertEquals(drain(direct, nDirect), drain(via, nVia));
  }

  @Test
  void writeAny_orderStatusReply_dispatchesToWriteOrderStatusReply() {
    final var e =
        new BrowserEvent.OrderStatusReply("C-004", "Working", 0L, 100_000_000L, 0L, "E-42");
    final var direct = fresh();
    final var via = fresh();
    final int nDirect = writer.writeOrderStatusReply(e, direct);
    final int nVia = writer.writeAny(e, via);
    assertEquals(nDirect, nVia);
    assertEquals(drain(direct, nDirect), drain(via, nVia));
  }

  // ---------------------------------------------------------------------------
  // writeBridgeStatus — return value consistency.
  // ---------------------------------------------------------------------------

  @Test
  void writeBridgeStatus_returnsActualBytesWritten() {
    final var e = new BrowserEvent.BridgeStatus(true, false, "ok", true, true, 1, 5_000L);
    final var buf = fresh();
    final int n = writer.writeBridgeStatus(e, buf);
    assertTrue(n > 0, "writeBridgeStatus must return positive byte count");
    assertEquals(n, buf.readableBytes(), "return value must equal actual bytes in buffer");
  }

  // ---------------------------------------------------------------------------
  // writeOrderStatusReply — return value.
  // ---------------------------------------------------------------------------

  @Test
  void writeOrderStatusReply_returnsActualBytesWritten() {
    final var e = new BrowserEvent.OrderStatusReply("C-REV", "Unknown", 0L, 0L, 0L, null);
    final var buf = fresh();
    final int n = writer.writeOrderStatusReply(e, buf);
    assertTrue(n > 0);
    assertEquals(n, buf.readableBytes());
  }
}
