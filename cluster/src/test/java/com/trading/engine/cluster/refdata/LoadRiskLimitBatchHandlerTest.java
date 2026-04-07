package com.trading.engine.cluster.refdata;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.trading.engine.messages.sbe.AccountStatusEnum;
import com.trading.engine.messages.sbe.LoadRiskLimitBatchEncoder;
import com.trading.engine.messages.sbe.LoadRiskLimitBatchEncoder.NoRiskLimitsEncoder;
import com.trading.engine.messages.sbe.MessageHeaderDecoder;
import com.trading.engine.messages.sbe.MessageHeaderEncoder;
import com.trading.engine.messages.sbe.RiskLimitLoadRejectedEventDecoder;
import com.trading.engine.messages.sbe.RiskLimitLoadedEventDecoder;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.junit.jupiter.api.Test;

class LoadRiskLimitBatchHandlerTest {

  private static final long FIRST_SEQ = 300L;
  private static final long TS = 1_700_000_000_000_000_000L;

  /**
   * Encode a LoadRiskLimitBatch with the given (accountId, maxOrderSize, maxOrderNotional,
   * maxDailyVolume, maxDailyLossBps) records.
   */
  private static int encodeBatch(final MutableDirectBuffer dst, final long[][] records) {
    final MessageHeaderEncoder header = new MessageHeaderEncoder();
    final LoadRiskLimitBatchEncoder enc = new LoadRiskLimitBatchEncoder();
    enc.wrapAndApplyHeader(dst, 0, header);
    enc.transactTime(0L);
    final NoRiskLimitsEncoder group = enc.noRiskLimitsCount(records.length);
    for (final long[] r : records) {
      group.next();
      group.accountId(r[0]);
      group.maxOrderSize(r[1]);
      group.maxOrderNotional(r[2]);
      group.maxDailyVolume(r[3]);
      group.maxDailyLossBps(r[4]);
      group.status(AccountStatusEnum.Active);
    }
    return MessageHeaderEncoder.ENCODED_LENGTH + enc.encodedLength();
  }

  private static int dispatch(
      final LoadRiskLimitBatchHandler handler,
      final MutableDirectBuffer src,
      final int srcLength,
      final MutableDirectBuffer eventDst) {
    final MessageHeaderDecoder header = new MessageHeaderDecoder();
    header.wrap(src, 0);
    return handler.onBatchCommand(header, src, 0, srcLength, eventDst, 0, FIRST_SEQ, TS);
  }

  private static AccountStore accountStoreWith(final long... ids) {
    final AccountStore store = new AccountStore();
    for (final long id : ids) {
      store.put(AccountStoreTest.makeState(id, "ACC" + id, "Account", "USD"));
    }
    return store;
  }

  @Test
  void allValidEmitsLoadedEventPerRecord() {
    final AccountStore accountStore = accountStoreWith(1L, 2L, 3L);
    final RiskLimitStore riskStore = new RiskLimitStore();
    final LoadRiskLimitBatchHandler handler =
        new LoadRiskLimitBatchHandler(riskStore, accountStore);

    final MutableDirectBuffer src = new ExpandableArrayBuffer(2048);
    final int srcLength =
        encodeBatch(
            src,
            new long[][] {
              {1L, 100_00000000L, 0L, 1000_00000000L, 50L},
              {2L, 200_00000000L, 0L, 2000_00000000L, 100L},
              {3L, 0L, 0L, 0L, 0L}, // unlimited
            });
    final MutableDirectBuffer eventDst = new ExpandableArrayBuffer(4096);
    final int totalEventBytes = dispatch(handler, src, srcLength, eventDst);
    assertTrue(totalEventBytes > 0);
    assertEquals(3, riskStore.size());

    // Walk events.
    int offset = 0;
    long expectedSeq = FIRST_SEQ;
    int count = 0;
    final MessageHeaderDecoder header = new MessageHeaderDecoder();
    final RiskLimitLoadedEventDecoder loaded = new RiskLimitLoadedEventDecoder();
    while (offset < totalEventBytes) {
      header.wrap(eventDst, offset);
      assertEquals(RiskLimitLoadedEventDecoder.TEMPLATE_ID, header.templateId());
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
    final AccountStore accountStore = accountStoreWith(1L, 3L); // missing id 2
    final RiskLimitStore riskStore = new RiskLimitStore();
    final LoadRiskLimitBatchHandler handler =
        new LoadRiskLimitBatchHandler(riskStore, accountStore);

    final MutableDirectBuffer src = new ExpandableArrayBuffer(2048);
    final int srcLength =
        encodeBatch(
            src,
            new long[][] {
              {1L, 100L, 0L, 1000L, 0L}, // valid
              {2L, 200L, 0L, 2000L, 0L}, // accountId not in AccountStore
              {3L, -1L, 0L, 1000L, 0L}, // negative limit
            });
    final MutableDirectBuffer eventDst = new ExpandableArrayBuffer(4096);
    final int totalEventBytes = dispatch(handler, src, srcLength, eventDst);
    assertEquals(1, riskStore.size()); // only id 1
    assertNotNull(riskStore.get(1L));

    // Walk events: expect Loaded(seq=300), Rejected(seq=301, AccountNotFound),
    // Rejected(seq=302, InvalidLimitValue).
    int offset = 0;
    int loadedCount = 0;
    int rejectedCount = 0;
    int totalEvents = 0;
    final MessageHeaderDecoder header = new MessageHeaderDecoder();
    while (totalEvents < 3 && offset < totalEventBytes) {
      header.wrap(eventDst, offset);
      if (header.templateId() == RiskLimitLoadedEventDecoder.TEMPLATE_ID) {
        final RiskLimitLoadedEventDecoder loaded = new RiskLimitLoadedEventDecoder();
        loaded.wrap(
            eventDst,
            offset + MessageHeaderDecoder.ENCODED_LENGTH,
            header.blockLength(),
            header.version());
        loadedCount++;
        offset += MessageHeaderDecoder.ENCODED_LENGTH + loaded.encodedLength();
      } else {
        final RiskLimitLoadRejectedEventDecoder rejected = new RiskLimitLoadRejectedEventDecoder();
        rejected.wrap(
            eventDst,
            offset + MessageHeaderDecoder.ENCODED_LENGTH,
            header.blockLength(),
            header.version());
        rejectedCount++;
        offset += MessageHeaderDecoder.ENCODED_LENGTH + rejected.encodedLength();
      }
      totalEvents++;
    }
    assertEquals(1, loadedCount);
    assertEquals(2, rejectedCount);
  }

  @Test
  void emptyBatchEmitsNoEvents() {
    final AccountStore accountStore = accountStoreWith();
    final RiskLimitStore riskStore = new RiskLimitStore();
    final LoadRiskLimitBatchHandler handler =
        new LoadRiskLimitBatchHandler(riskStore, accountStore);

    final MutableDirectBuffer src = new ExpandableArrayBuffer(64);
    final int srcLength = encodeBatch(src, new long[0][]);
    final MutableDirectBuffer eventDst = new ExpandableArrayBuffer(64);
    final int totalEventBytes = dispatch(handler, src, srcLength, eventDst);

    assertEquals(0, totalEventBytes);
    assertEquals(0, riskStore.size());
  }

  @Test
  void batchTemplateIdMatches() {
    final LoadRiskLimitBatchHandler h =
        new LoadRiskLimitBatchHandler(new RiskLimitStore(), new AccountStore());
    assertEquals(LoadRiskLimitBatchEncoder.TEMPLATE_ID, h.batchCommandTemplateId());
  }
}
