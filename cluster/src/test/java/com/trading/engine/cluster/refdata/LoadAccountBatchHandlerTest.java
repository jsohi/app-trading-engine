package com.trading.engine.cluster.refdata;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.trading.engine.messages.sbe.AccountLoadRejectedEventDecoder;
import com.trading.engine.messages.sbe.AccountLoadedEventDecoder;
import com.trading.engine.messages.sbe.AccountStatusEnum;
import com.trading.engine.messages.sbe.AccountTypeEnum;
import com.trading.engine.messages.sbe.AcctIDSourceEnum;
import com.trading.engine.messages.sbe.ComplianceStatusEnum;
import com.trading.engine.messages.sbe.LoadAccountBatchEncoder;
import com.trading.engine.messages.sbe.LoadAccountBatchEncoder.NoAccountsEncoder;
import com.trading.engine.messages.sbe.MessageHeaderDecoder;
import com.trading.engine.messages.sbe.MessageHeaderEncoder;
import com.trading.engine.messages.sbe.RejectReasonEnum;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.junit.jupiter.api.Test;

class LoadAccountBatchHandlerTest {

  private static final long FIRST_SEQ = 200L;
  private static final long TS = 1_700_000_000_000_000_000L;

  /** Encode a LoadAccountBatch with the given (id, code, baseCcy) records. */
  private static int encodeBatch(final MutableDirectBuffer dst, final Object[][] records) {
    final MessageHeaderEncoder header = new MessageHeaderEncoder();
    final LoadAccountBatchEncoder enc = new LoadAccountBatchEncoder();
    enc.wrapAndApplyHeader(dst, 0, header);
    enc.transactTime(0L);
    final NoAccountsEncoder group = enc.noAccountsCount(records.length);
    for (final Object[] r : records) {
      group.next();
      group.accountId((long) r[0]);
      group.parentAccountId(0L);
      group.accountCode((String) r[1]);
      group.acctIdSource(AcctIDSourceEnum.Internal);
      group.accountName("Account " + r[1]);
      group.accountType(AccountTypeEnum.Client);
      group.baseCurrency((String) r[2]);
      group.status(AccountStatusEnum.Active);
      group.complianceStatus(ComplianceStatusEnum.OK);
      group.capabilities(AccountState.Capabilities.CAN_TRADE);
    }
    return MessageHeaderEncoder.ENCODED_LENGTH + enc.encodedLength();
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
            new Object[][] {
              {1L, "ACME", "USD"},
              {2L, "BIGCO", "EUR"},
              {3L, "JPN", "JPY"},
            });
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
            new Object[][] {
              {1L, "ACME", "USD"}, // valid
              {0L, "BAD", "USD"}, // invalid accountId
              {2L, "BIGCO", "EUR"}, // valid
            });
    final MutableDirectBuffer eventDst = new ExpandableArrayBuffer(4096);
    dispatch(handler, src, srcLength, eventDst);
    assertEquals(2, accountStore.size()); // ACME, BIGCO

    // Walk events.
    int offset = 0;
    int loadedCount = 0;
    int rejectedCount = 0;
    final MessageHeaderDecoder header = new MessageHeaderDecoder();
    while (offset < eventDst.capacity()) {
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
      } else {
        break; // walked off the end of valid events
      }
      if (loadedCount + rejectedCount == 3) {
        break;
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
    final int srcLength = encodeBatch(src, new Object[0][]);
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
            new Object[][] {
              {1L, "ACME", "USD"}, // first one wins
              {2L, "ACME", "USD"}, // duplicate code, different id — rejected
              {3L, "BIGCO", "EUR"}, // valid
            });
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
