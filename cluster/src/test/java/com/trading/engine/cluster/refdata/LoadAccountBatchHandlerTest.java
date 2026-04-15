package com.trading.engine.cluster.refdata;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.trading.engine.messages.sbe.AccountLoadRejectedEventDecoder;
import com.trading.engine.messages.sbe.AccountLoadedEventDecoder;
import com.trading.engine.messages.sbe.LoadAccountBatchEncoder;
import com.trading.engine.messages.sbe.MessageHeaderDecoder;
import com.trading.engine.messages.sbe.RejectReasonEnum;
import com.trading.engine.testsupport.sbe.AccountRecord;
import com.trading.engine.testsupport.sbe.SbeTestEncoder;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.junit.jupiter.api.Test;

class LoadAccountBatchHandlerTest {

  private static final long FIRST_SEQ = 200L;
  private static final long TS = 1_700_000_000_000_000_000L;

  /** Encode a LoadAccountBatch with the given account records. */
  private static int encodeBatch(final MutableDirectBuffer dst, final AccountRecord... records) {
    return SbeTestEncoder.encodeLoadAccountBatch(dst, 0, 0L, records);
  }

  private static int dispatch(
      final LoadAccountBatchHandler handler,
      final MutableDirectBuffer src,
      final int srcLength,
      final MutableDirectBuffer eventDst) {
    final MessageHeaderDecoder header = new MessageHeaderDecoder();
    header.wrap(src, 0);
    return handler.onBatchCommand(header, src, 0, srcLength, eventDst, 0, FIRST_SEQ, TS);
  }

  @Test
  void allValidEmitsLoadedEventPerRecord() {
    final AccountStore accountStore = new AccountStore();
    final LoadAccountBatchHandler handler = new LoadAccountBatchHandler(accountStore);

    final MutableDirectBuffer src = new ExpandableArrayBuffer(2048);
    final int srcLength =
        encodeBatch(
            src,
            new AccountRecord(1L, "ACME", "USD"),
            new AccountRecord(2L, "BIGCO", "EUR"),
            new AccountRecord(3L, "JPN", "JPY"));
    final MutableDirectBuffer eventDst = new ExpandableArrayBuffer(4096);
    final int totalEventBytes = dispatch(handler, src, srcLength, eventDst);
    assertTrue(totalEventBytes > 0);
    assertEquals(3, accountStore.size());

    // Walk events; all should be loaded with monotonic sequenceNumber.
    int offset = 0;
    long expectedSeq = FIRST_SEQ;
    int count = 0;
    final MessageHeaderDecoder header = new MessageHeaderDecoder();
    final AccountLoadedEventDecoder loaded = new AccountLoadedEventDecoder();
    while (offset < totalEventBytes) {
      header.wrap(eventDst, offset);
      assertEquals(AccountLoadedEventDecoder.TEMPLATE_ID, header.templateId());
      loaded.wrap(
          eventDst,
          offset + MessageHeaderDecoder.ENCODED_LENGTH,
          header.blockLength(),
          header.version());
      assertEquals(expectedSeq++, loaded.sequenceNumber());
      offset += MessageHeaderDecoder.ENCODED_LENGTH + loaded.encodedLength();
      count++;
    }
    assertEquals(3, count);
  }

  @Test
  void mixedValidAndInvalidProducesPerRecordEvents() {
    final AccountStore accountStore = new AccountStore();
    final LoadAccountBatchHandler handler = new LoadAccountBatchHandler(accountStore);

    final MutableDirectBuffer src = new ExpandableArrayBuffer(2048);
    final int srcLength =
        encodeBatch(
            src,
            new AccountRecord(1L, "ACME", "USD"), // valid
            new AccountRecord(0L, "BAD", "USD"), // invalid accountId
            new AccountRecord(2L, "BIGCO", "EUR")); // valid
    final MutableDirectBuffer eventDst = new ExpandableArrayBuffer(4096);
    final int totalEventBytes = dispatch(handler, src, srcLength, eventDst);
    assertEquals(2, accountStore.size()); // ACME, BIGCO

    // Walk events bounded by the actual bytes written, not the buffer capacity.
    int offset = 0;
    int loadedCount = 0;
    int rejectedCount = 0;
    final MessageHeaderDecoder header = new MessageHeaderDecoder();
    while (offset < totalEventBytes) {
      header.wrap(eventDst, offset);
      if (header.templateId() == AccountLoadedEventDecoder.TEMPLATE_ID) {
        final AccountLoadedEventDecoder loaded = new AccountLoadedEventDecoder();
        loaded.wrap(
            eventDst,
            offset + MessageHeaderDecoder.ENCODED_LENGTH,
            header.blockLength(),
            header.version());
        loadedCount++;
        offset += MessageHeaderDecoder.ENCODED_LENGTH + loaded.encodedLength();
      } else if (header.templateId() == AccountLoadRejectedEventDecoder.TEMPLATE_ID) {
        final AccountLoadRejectedEventDecoder rejected = new AccountLoadRejectedEventDecoder();
        rejected.wrap(
            eventDst,
            offset + MessageHeaderDecoder.ENCODED_LENGTH,
            header.blockLength(),
            header.version());
        assertEquals(RejectReasonEnum.InvalidAccountId, rejected.rejectReason());
        rejectedCount++;
        offset += MessageHeaderDecoder.ENCODED_LENGTH + rejected.encodedLength();
      }
    }
    assertEquals(2, loadedCount);
    assertEquals(1, rejectedCount);
  }

  @Test
  void emptyBatchEmitsNoEvents() {
    final AccountStore accountStore = new AccountStore();
    final LoadAccountBatchHandler handler = new LoadAccountBatchHandler(accountStore);

    final MutableDirectBuffer src = new ExpandableArrayBuffer(64);
    final int srcLength = encodeBatch(src);
    final MutableDirectBuffer eventDst = new ExpandableArrayBuffer(64);
    final int totalEventBytes = dispatch(handler, src, srcLength, eventDst);

    assertEquals(0, totalEventBytes);
    assertEquals(0, accountStore.size());
  }

  @Test
  void duplicateCodeAcrossBatchRejected() {
    final AccountStore accountStore = new AccountStore();
    final LoadAccountBatchHandler handler = new LoadAccountBatchHandler(accountStore);

    final MutableDirectBuffer src = new ExpandableArrayBuffer(2048);
    final int srcLength =
        encodeBatch(
            src,
            new AccountRecord(1L, "ACME", "USD"), // first one wins
            new AccountRecord(2L, "ACME", "USD"), // duplicate code, different id — rejected
            new AccountRecord(3L, "BIGCO", "EUR")); // valid
    final MutableDirectBuffer eventDst = new ExpandableArrayBuffer(4096);
    dispatch(handler, src, srcLength, eventDst);

    assertEquals(2, accountStore.size());
    assertNotNull(accountStore.get(1L));
    assertNotNull(accountStore.get(3L));
  }

  @Test
  void batchTemplateIdMatches() {
    final LoadAccountBatchHandler h = new LoadAccountBatchHandler(new AccountStore());
    assertEquals(LoadAccountBatchEncoder.TEMPLATE_ID, h.batchCommandTemplateId());
  }
}
