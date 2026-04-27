package com.trading.engine.websocket;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.trading.engine.messages.sbe.AccountLoadRejectedEventEncoder;
import com.trading.engine.messages.sbe.AccountLoadedEventEncoder;
import com.trading.engine.messages.sbe.AccountStatusEnum;
import com.trading.engine.messages.sbe.AcctIDSourceEnum;
import com.trading.engine.messages.sbe.ComplianceStatusEnum;
import com.trading.engine.messages.sbe.CxlRejReasonEnum;
import com.trading.engine.messages.sbe.CxlRejResponseToEnum;
import com.trading.engine.messages.sbe.MessageHeaderEncoder;
import com.trading.engine.messages.sbe.OrdStatusEnum;
import com.trading.engine.messages.sbe.OrdTypeEnum;
import com.trading.engine.messages.sbe.OrderCancelRejectedEventEncoder;
import com.trading.engine.messages.sbe.ProductTypeEnum;
import com.trading.engine.messages.sbe.QuoteRejectReasonEnum;
import com.trading.engine.messages.sbe.RejectReasonEnum;
import com.trading.engine.messages.sbe.SideEnum;
import com.trading.engine.testsupport.sbe.SbeTestEncoder;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link AccountExtractor} -- verifies account code extraction from all supported egress
 * SBE templates and correct handling of no-account and truncated payloads.
 */
final class AccountExtractorTest {

  private final MutableDirectBuffer buffer = new ExpandableArrayBuffer(512);
  private final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();

  // --- Template 100: OrderCreated ---

  @Test
  void extractAccountCode_orderCreatedEvent_extractsCorrectAccount() {
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

    assertEquals("ACME-001", AccountExtractor.extractAccountCode(100, bytes, 0, len));
  }

  // --- Template 101: OrderRejected ---

  @Test
  void extractAccountCode_orderRejectedEvent_extractsCorrectAccount() {
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
            "HEDGE-002",
            ProductTypeEnum.Spot,
            "USD");
    final byte[] bytes = toByteArray(buffer, len);

    assertEquals("HEDGE-002", AccountExtractor.extractAccountCode(101, bytes, 0, len));
  }

  // --- Template 102: OrderFilled ---

  @Test
  void extractAccountCode_orderFilledEvent_extractsCorrectAccount() {
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
            "PROP-003");
    final byte[] bytes = toByteArray(buffer, len);

    assertEquals("PROP-003", AccountExtractor.extractAccountCode(102, bytes, 0, len));
  }

  // --- Template 103: OrderCanceled (no account field) ---

  @Test
  void extractAccountCode_orderCanceledEvent_returnsNull() {
    assertNull(AccountExtractor.extractAccountCode(103, new byte[256], 0, 256));
  }

  // --- Template 104: QuoteRequested ---

  @Test
  void extractAccountCode_quoteRequestedEvent_extractsCorrectAccount() {
    final int len =
        SbeTestEncoder.encodeQuoteRequestedEvent(
            buffer, 0, 1L, 1000L, "QR001", "AUDUSD", SideEnum.Buy, 100_000_000L, "RFQ-ACCT");
    final byte[] bytes = toByteArray(buffer, len);

    assertEquals("RFQ-ACCT", AccountExtractor.extractAccountCode(104, bytes, 0, len));
  }

  // --- Template 105: QuoteCreated ---

  @Test
  void extractAccountCode_quoteCreatedEvent_extractsCorrectAccount() {
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
            "DESK-A",
            65_000_000L,
            66_000_000L,
            1_000_000_000_000L);
    final byte[] bytes = toByteArray(buffer, len);

    assertEquals("DESK-A", AccountExtractor.extractAccountCode(105, bytes, 0, len));
  }

  // --- Template 106: QuoteRejected ---

  @Test
  void extractAccountCode_quoteRejectedEvent_extractsCorrectAccount() {
    final int len =
        SbeTestEncoder.encodeQuoteRejectedEvent(
            buffer,
            0,
            1L,
            1000L,
            "QR001",
            "USDCAD",
            SideEnum.Buy,
            "REJECT-AC",
            QuoteRejectReasonEnum.UnknownSymbol,
            ProductTypeEnum.Spot,
            "Symbol not found");
    final byte[] bytes = toByteArray(buffer, len);

    assertEquals("REJECT-AC", AccountExtractor.extractAccountCode(106, bytes, 0, len));
  }

  // --- Template 107: QuoteExpired ---

  @Test
  void extractAccountCode_quoteExpiredEvent_extractsCorrectAccount() {
    final int len =
        SbeTestEncoder.encodeQuoteExpiredEvent(
            buffer,
            0,
            1L,
            1000L,
            "Q001",
            "QR001",
            "EURGBP",
            SideEnum.Buy,
            "EXPIRED-A",
            ProductTypeEnum.Spot);
    final byte[] bytes = toByteArray(buffer, len);

    assertEquals("EXPIRED-A", AccountExtractor.extractAccountCode(107, bytes, 0, len));
  }

  // --- Template 110: AccountLoaded ---

  @Test
  void extractAccountCode_accountLoadedEvent_extractsCorrectAccount() {
    final var enc = new AccountLoadedEventEncoder();
    enc.wrapAndApplyHeader(buffer, 0, headerEncoder);
    enc.sequenceNumber(1L)
        .timestamp(1000L)
        .accountId(42L)
        .parentAccountId(0L)
        .accountCode("LOADED-01")
        .status(AccountStatusEnum.Active)
        .complianceStatus(ComplianceStatusEnum.OK)
        .acctIdSource(AcctIDSourceEnum.Internal)
        .capabilities(3L);

    final int len = headerEncoder.encodedLength() + enc.encodedLength();
    final byte[] bytes = toByteArray(buffer, len);

    assertEquals("LOADED-01", AccountExtractor.extractAccountCode(110, bytes, 0, len));
  }

  // --- Template 111: AccountLoadRejected ---

  @Test
  void extractAccountCode_accountLoadRejectedEvent_extractsCorrectAccount() {
    final var enc = new AccountLoadRejectedEventEncoder();
    enc.wrapAndApplyHeader(buffer, 0, headerEncoder);
    enc.sequenceNumber(1L)
        .timestamp(1000L)
        .accountCode("BAD-ACCT")
        .rejectReason(RejectReasonEnum.DuplicateAccountCode);

    final int len = headerEncoder.encodedLength() + enc.encodedLength();
    final byte[] bytes = toByteArray(buffer, len);

    assertEquals("BAD-ACCT", AccountExtractor.extractAccountCode(111, bytes, 0, len));
  }

  // --- Template 112: OrderCancelRejected ---

  @Test
  void extractAccountCode_orderCancelRejectedEvent_extractsCorrectAccount() {
    final var enc = new OrderCancelRejectedEventEncoder();
    enc.wrapAndApplyHeader(buffer, 0, headerEncoder);
    enc.sequenceNumber(1L)
        .timestamp(1000L)
        .orderId("ORD001")
        .clOrdId("CLORD002")
        .origClOrdId("CLORD001")
        .ordStatus(OrdStatusEnum.New)
        .cxlRejResponseTo(CxlRejResponseToEnum.OrderCancelRequest)
        .cxlRejReason(CxlRejReasonEnum.UnknownOrder)
        .accountCode("CANCEL-AC")
        .text("Order not found");

    final int len = headerEncoder.encodedLength() + enc.encodedLength();
    final byte[] bytes = toByteArray(buffer, len);

    assertEquals("CANCEL-AC", AccountExtractor.extractAccountCode(112, bytes, 0, len));
  }

  // --- No-account templates ---

  @Test
  void extractAccountCode_priceResponse_returnsNull() {
    assertNull(AccountExtractor.extractAccountCode(51, new byte[256], 0, 256));
  }

  @Test
  void extractAccountCode_positionSnapshot_returnsNull() {
    assertNull(AccountExtractor.extractAccountCode(204, new byte[256], 0, 256));
  }

  @Test
  void extractAccountCode_unknownTemplateId_returnsNull() {
    assertNull(AccountExtractor.extractAccountCode(999, new byte[256], 0, 256));
  }

  // --- Boundary and truncation ---

  @Test
  void extractAccountCode_truncatedPayload_returnsNull() {
    // OrderCreated accountCode at offset 131 + 16 bytes = 147 minimum. Provide 50.
    assertNull(AccountExtractor.extractAccountCode(100, new byte[50], 0, 50));
  }

  @Test
  void extractAccountCode_zeroLengthPayload_returnsNull() {
    assertNull(AccountExtractor.extractAccountCode(100, new byte[0], 0, 0));
  }

  // --- Offset mapping validation ---

  @Test
  void absoluteAccountOffset_allMappedTemplates_returnsPositiveOffset() {
    final int[] mapped = {100, 101, 102, 104, 105, 106, 107, 110, 111, 112};
    for (final int tid : mapped) {
      final int offset = AccountExtractor.absoluteAccountOffset(tid);
      assertTrue(offset > 0, "Expected positive offset for templateId " + tid + ", got " + offset);
    }
  }

  @Test
  void absoluteAccountOffset_unmappedTemplates_returnsNegativeOne() {
    final int[] unmapped = {51, 103, 204, 108, 109, 113, 114, 115, 116, 999};
    for (final int tid : unmapped) {
      assertEquals(-1, AccountExtractor.absoluteAccountOffset(tid));
    }
  }

  // --- Non-zero offset ---

  @Test
  void extractAccountCode_nonZeroOffset_extractsCorrectAccount() {
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
            "EURUSD",
            SideEnum.Buy,
            OrdTypeEnum.Limit,
            110_000_000L,
            100_000_000L,
            "OFFSET-AC");
    final byte[] bytes = new byte[prefix + len];
    buffer.getBytes(0, bytes, 0, prefix + len);

    assertEquals("OFFSET-AC", AccountExtractor.extractAccountCode(100, bytes, prefix, len));
  }

  // --- Negative offset guard ---

  @Test
  void extractAccountCode_negativeOffset_returnsNull() {
    assertNull(AccountExtractor.extractAccountCode(100, new byte[256], -1, 256));
  }

  @Test
  void extractAccountCode_negativeLength_returnsNull() {
    assertNull(AccountExtractor.extractAccountCode(100, new byte[256], 0, -1));
  }

  private static byte[] toByteArray(final MutableDirectBuffer buffer, final int length) {
    final byte[] bytes = new byte[length];
    buffer.getBytes(0, bytes);
    return bytes;
  }
}
