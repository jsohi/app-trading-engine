package com.trading.engine.websocket;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.trading.engine.messages.sbe.OrdTypeEnum;
import com.trading.engine.messages.sbe.ProductTypeEnum;
import com.trading.engine.messages.sbe.QuoteRejectReasonEnum;
import com.trading.engine.messages.sbe.RejectReasonEnum;
import com.trading.engine.messages.sbe.SideEnum;
import com.trading.engine.projections.SymbolPacker;
import com.trading.engine.testsupport.sbe.SbeTestEncoder;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link SymbolExtractor} -- verifies packed symbol extraction from all supported egress
 * SBE templates and correct handling of no-symbol and truncated payloads.
 */
final class SymbolExtractorTest {

  private final MutableDirectBuffer buffer = new ExpandableArrayBuffer(512);

  // --- Template 100: OrderCreated ---

  @Test
  void extractPackedSymbol_orderCreatedEvent_extractsCorrectSymbol() {
    final int len =
        SbeTestEncoder.encodeOrderCreatedEvent(
            buffer,
            0,
            1L,
            1000L,
            "ORD001",
            "EXEC001",
            "CLORD001",
            "EURUSD",
            SideEnum.Buy,
            OrdTypeEnum.Limit,
            110_000_000L,
            100_000_000L,
            "ACME-001");
    final byte[] bytes = toByteArray(buffer, len);

    assertEquals(
        SymbolPacker.pack("EURUSD"), SymbolExtractor.extractPackedSymbol(100, bytes, 0, len));
  }

  // --- Template 101: OrderRejected ---

  @Test
  void extractPackedSymbol_orderRejectedEvent_extractsCorrectSymbol() {
    final int len =
        SbeTestEncoder.encodeOrderRejectedEvent(
            buffer,
            0,
            1L,
            1000L,
            "CLORD001",
            RejectReasonEnum.InsufficientQuantity,
            "Insufficient margin",
            "EURUSD",
            SideEnum.Buy,
            "ACME-001",
            ProductTypeEnum.Spot,
            "USD");
    final byte[] bytes = toByteArray(buffer, len);

    assertEquals(
        SymbolPacker.pack("EURUSD"), SymbolExtractor.extractPackedSymbol(101, bytes, 0, len));
  }

  // --- Template 102: OrderFilled ---

  @Test
  void extractPackedSymbol_orderFilledEvent_extractsCorrectSymbol() {
    final int len =
        SbeTestEncoder.encodeOrderFilledEvent(
            buffer,
            0,
            1L,
            1000L,
            "EXEC001",
            "ORD001",
            "CLORD001",
            "GBPUSD",
            SideEnum.Sell,
            130_000_000L,
            50_000_000L,
            0L,
            50_000_000L,
            "ACME-001");
    final byte[] bytes = toByteArray(buffer, len);

    assertEquals(
        SymbolPacker.pack("GBPUSD"), SymbolExtractor.extractPackedSymbol(102, bytes, 0, len));
  }

  // --- Template 103: OrderCanceled ---

  @Test
  void extractPackedSymbol_orderCanceledEvent_extractsCorrectSymbol() {
    final int len =
        SbeTestEncoder.encodeOrderCanceledEvent(
            buffer,
            0,
            1L,
            1000L,
            "ORD001",
            "CLORD001",
            "ORIGCL01",
            "USDCHF",
            SideEnum.Sell,
            ProductTypeEnum.Spot);
    final byte[] bytes = toByteArray(buffer, len);

    assertEquals(
        SymbolPacker.pack("USDCHF"), SymbolExtractor.extractPackedSymbol(103, bytes, 0, len));
  }

  // --- Template 51: PriceResponse ---

  @Test
  void extractPackedSymbol_priceResponse_extractsCorrectSymbol() {
    final int len =
        SbeTestEncoder.encodePriceResponse(
            buffer, 0, "QR001", "USDJPY", true, 149_500_000_000L, 149_600_000_000L, 1000L);
    final byte[] bytes = toByteArray(buffer, len);

    assertEquals(
        SymbolPacker.pack("USDJPY"), SymbolExtractor.extractPackedSymbol(51, bytes, 0, len));
  }

  // --- Template 104: QuoteRequested ---

  @Test
  void extractPackedSymbol_quoteRequestedEvent_extractsCorrectSymbol() {
    final int len =
        SbeTestEncoder.encodeQuoteRequestedEvent(
            buffer, 0, 1L, 1000L, "QR001", "AUDUSD", SideEnum.Buy, 100_000_000L, "ACME-001");
    final byte[] bytes = toByteArray(buffer, len);

    assertEquals(
        SymbolPacker.pack("AUDUSD"), SymbolExtractor.extractPackedSymbol(104, bytes, 0, len));
  }

  // --- Template 105: QuoteCreated ---

  @Test
  void extractPackedSymbol_quoteCreatedEvent_extractsCorrectSymbol() {
    final int len =
        SbeTestEncoder.encodeQuoteCreatedEvent(
            buffer,
            0,
            1L,
            1000L,
            "Q001",
            "QR001",
            "NZDUSD",
            SideEnum.Buy,
            "ACME-001",
            65_000_000L,
            66_000_000L,
            1_000_000_000_000L);
    final byte[] bytes = toByteArray(buffer, len);

    assertEquals(
        SymbolPacker.pack("NZDUSD"), SymbolExtractor.extractPackedSymbol(105, bytes, 0, len));
  }

  // --- Template 106: QuoteRejected ---

  @Test
  void extractPackedSymbol_quoteRejectedEvent_extractsCorrectSymbol() {
    final int len =
        SbeTestEncoder.encodeQuoteRejectedEvent(
            buffer,
            0,
            1L,
            1000L,
            "QR001",
            "USDCAD",
            SideEnum.Buy,
            QuoteRejectReasonEnum.UnknownSymbol,
            "Symbol not found");
    final byte[] bytes = toByteArray(buffer, len);

    assertEquals(
        SymbolPacker.pack("USDCAD"), SymbolExtractor.extractPackedSymbol(106, bytes, 0, len));
  }

  // --- Template 107: QuoteExpired ---

  @Test
  void extractPackedSymbol_quoteExpiredEvent_extractsCorrectSymbol() {
    final int len =
        SbeTestEncoder.encodeQuoteExpiredEvent(
            buffer, 0, 1L, 1000L, "Q001", "QR001", "EURGBP", SideEnum.Buy);
    final byte[] bytes = toByteArray(buffer, len);

    assertEquals(
        SymbolPacker.pack("EURGBP"), SymbolExtractor.extractPackedSymbol(107, bytes, 0, len));
  }

  // --- No-symbol templates ---

  @Test
  void extractPackedSymbol_accountLoadedEvent_returnsUnknownSymbol() {
    assertEquals(
        SymbolExtractor.UNKNOWN_SYMBOL,
        SymbolExtractor.extractPackedSymbol(110, new byte[256], 0, 256));
  }

  @Test
  void extractPackedSymbol_accountLoadRejectedEvent_returnsUnknownSymbol() {
    assertEquals(
        SymbolExtractor.UNKNOWN_SYMBOL,
        SymbolExtractor.extractPackedSymbol(111, new byte[256], 0, 256));
  }

  @Test
  void extractPackedSymbol_orderCancelRejectedEvent_returnsUnknownSymbol() {
    assertEquals(
        SymbolExtractor.UNKNOWN_SYMBOL,
        SymbolExtractor.extractPackedSymbol(112, new byte[256], 0, 256));
  }

  @Test
  void extractPackedSymbol_positionSnapshot_returnsUnknownSymbol() {
    assertEquals(
        SymbolExtractor.UNKNOWN_SYMBOL,
        SymbolExtractor.extractPackedSymbol(204, new byte[256], 0, 256));
  }

  // --- Unknown and internal templates ---

  @Test
  void extractPackedSymbol_unknownTemplateId_returnsUnknownSymbol() {
    assertEquals(
        SymbolExtractor.UNKNOWN_SYMBOL,
        SymbolExtractor.extractPackedSymbol(999, new byte[256], 0, 256));
  }

  @Test
  void extractPackedSymbol_internalEvent108_returnsUnknownSymbol() {
    assertEquals(
        SymbolExtractor.UNKNOWN_SYMBOL,
        SymbolExtractor.extractPackedSymbol(108, new byte[256], 0, 256));
  }

  // --- Boundary and truncation ---

  @Test
  void extractPackedSymbol_truncatedPayload_returnsUnknownSymbol() {
    // OrderCreated needs symbol at absolute offset 84, plus 8 bytes = 92 minimum
    assertEquals(
        SymbolExtractor.UNKNOWN_SYMBOL,
        SymbolExtractor.extractPackedSymbol(100, new byte[20], 0, 20));
  }

  @Test
  void extractPackedSymbol_exactBoundaryOneByteShort_returnsUnknownSymbol() {
    // OrderCreated symbol: offset 84 + 8 bytes = 92. Provide 91 (one byte short).
    assertEquals(
        SymbolExtractor.UNKNOWN_SYMBOL,
        SymbolExtractor.extractPackedSymbol(100, new byte[91], 0, 91));
  }

  @Test
  void extractPackedSymbol_zeroLengthPayload_returnsUnknownSymbol() {
    assertEquals(
        SymbolExtractor.UNKNOWN_SYMBOL,
        SymbolExtractor.extractPackedSymbol(100, new byte[0], 0, 0));
  }

  // --- Cross-symbol differentiation ---

  @Test
  void extractPackedSymbol_differentSymbols_produceDifferentPackedValues() {
    final int len1 =
        SbeTestEncoder.encodeOrderCreatedEvent(
            buffer,
            0,
            1L,
            1000L,
            "ORD001",
            "EXEC001",
            "CLORD001",
            "EURUSD",
            SideEnum.Buy,
            OrdTypeEnum.Limit,
            110_000_000L,
            100_000_000L,
            "ACME-001");
    final byte[] bytes1 = toByteArray(buffer, len1);
    final long packed1 = SymbolExtractor.extractPackedSymbol(100, bytes1, 0, len1);

    final int len2 =
        SbeTestEncoder.encodeOrderCreatedEvent(
            buffer,
            0,
            1L,
            1000L,
            "ORD001",
            "EXEC001",
            "CLORD001",
            "GBPUSD",
            SideEnum.Buy,
            OrdTypeEnum.Limit,
            110_000_000L,
            100_000_000L,
            "ACME-001");
    final byte[] bytes2 = toByteArray(buffer, len2);
    final long packed2 = SymbolExtractor.extractPackedSymbol(100, bytes2, 0, len2);

    assertNotEquals(packed1, packed2);
    assertEquals(SymbolPacker.pack("EURUSD"), packed1);
    assertEquals(SymbolPacker.pack("GBPUSD"), packed2);
  }

  // --- Offset mapping validation ---

  @Test
  void absoluteSymbolOffset_allMappedTemplates_returnsPositiveOffset() {
    final int[] mapped = {100, 101, 102, 103, 104, 105, 106, 107, 51};
    for (final int tid : mapped) {
      final int offset = SymbolExtractor.absoluteSymbolOffset(tid);
      assertTrue(offset > 0, "Expected positive offset for templateId " + tid + ", got " + offset);
    }
  }

  @Test
  void absoluteSymbolOffset_unmappedTemplates_returnsNegativeOne() {
    final int[] unmapped = {110, 111, 112, 204, 108, 109, 113, 114, 115, 116, 999};
    for (final int tid : unmapped) {
      assertEquals(-1, SymbolExtractor.absoluteSymbolOffset(tid));
    }
  }

  // --- Non-zero offset ---

  @Test
  void extractPackedSymbol_nonZeroOffset_extractsCorrectSymbol() {
    final int prefix = 16;
    final int len =
        SbeTestEncoder.encodeOrderCreatedEvent(
            buffer,
            prefix,
            1L,
            1000L,
            "ORD001",
            "EXEC001",
            "CLORD001",
            "EURJPY",
            SideEnum.Buy,
            OrdTypeEnum.Limit,
            110_000_000L,
            100_000_000L,
            "ACME-001");
    final byte[] bytes = new byte[prefix + len];
    buffer.getBytes(0, bytes, 0, prefix + len);

    assertEquals(
        SymbolPacker.pack("EURJPY"), SymbolExtractor.extractPackedSymbol(100, bytes, prefix, len));
  }

  // --- Negative offset guard ---

  @Test
  void extractPackedSymbol_negativeOffset_returnsUnknownSymbol() {
    assertEquals(
        SymbolExtractor.UNKNOWN_SYMBOL,
        SymbolExtractor.extractPackedSymbol(100, new byte[256], -1, 256));
  }

  @Test
  void extractPackedSymbol_negativeLength_returnsUnknownSymbol() {
    assertEquals(
        SymbolExtractor.UNKNOWN_SYMBOL,
        SymbolExtractor.extractPackedSymbol(100, new byte[256], 0, -1));
  }

  private static byte[] toByteArray(final MutableDirectBuffer buffer, final int length) {
    final byte[] bytes = new byte[length];
    buffer.getBytes(0, bytes);
    return bytes;
  }
}
