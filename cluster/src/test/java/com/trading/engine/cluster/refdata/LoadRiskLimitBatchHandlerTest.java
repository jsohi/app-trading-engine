package com.trading.engine.cluster.refdata;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.trading.engine.messages.sbe.LoadRiskLimitBatchEncoder;
import com.trading.engine.messages.sbe.MessageHeaderDecoder;
import com.trading.engine.messages.sbe.RiskLimitLoadRejectedEventDecoder;
import com.trading.engine.messages.sbe.RiskLimitLoadedEventDecoder;
import com.trading.engine.testsupport.sbe.RiskLimitRecord;
import com.trading.engine.testsupport.sbe.SbeTestEncoder;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.junit.jupiter.api.Test;

class LoadRiskLimitBatchHandlerTest {

  private static long FIRST_SEQ = 300L;
  private static long TS = 1_700_000_000_000_000_000L;

  /** Encode a LoadRiskLimitBatch with the given risk limit records. */
  private static int encodeBatch(final MutableDirectBuffer dst, final RiskLimitRecord... records) {
    return SbeTestEncoder.encodeLoadRiskLimitBatch(dst, 0, 0L, records);
  }

  private static int dispatch(
      final LoadRiskLimitBatchHandler handler,
      final MutableDirectBuffer src,
      int srcLength,
      final MutableDirectBuffer eventDst) {
    final MessageHeaderDecoder header = new MessageHeaderDecoder();
    header.wrap(src, 0);
    return handler.onBatchCommand(header, src, 0, srcLength, eventDst, 0, FIRST_SEQ, TS);
  }

  private static AccountStore accountStoreWith(long... ids) {
    final AccountStore store = new AccountStore();
    for (long id : ids) {
      store.put(AccountFixtures.account(id, "ACC" + id, "Account", "USD"));
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
    int srcLength =
        encodeBatch(
            src,
            new RiskLimitRecord(1L, 100_00000000L, 0L, 1000_00000000L),
            new RiskLimitRecord(2L, 200_00000000L, 0L, 2000_00000000L),
            new RiskLimitRecord(3L, 0L, 0L, 0L)); // unlimited
    final MutableDirectBuffer eventDst = new ExpandableArrayBuffer(4096);
    int totalEventBytes = dispatch(handler, src, srcLength, eventDst);
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
    int srcLength =
        encodeBatch(
            src,
            new RiskLimitRecord(1L, 100L, 0L, 1000L), // valid
            new RiskLimitRecord(2L, 200L, 0L, 2000L), // accountId not in AccountStore
            new RiskLimitRecord(3L, -1L, 0L, 1000L)); // negative limit
    final MutableDirectBuffer eventDst = new ExpandableArrayBuffer(4096);
    int totalEventBytes = dispatch(handler, src, srcLength, eventDst);
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
    int srcLength = encodeBatch(src);
    final MutableDirectBuffer eventDst = new ExpandableArrayBuffer(64);
    int totalEventBytes = dispatch(handler, src, srcLength, eventDst);

    assertEquals(0, totalEventBytes);
    assertEquals(0, riskStore.size());
  }

  @Test
  void batchTemplateIdMatches() {
    final LoadRiskLimitBatchHandler h =
        new LoadRiskLimitBatchHandler(new RiskLimitStore(), new AccountStore());
    assertEquals(LoadRiskLimitBatchEncoder.TEMPLATE_ID, h.batchCommandTemplateId());
  }

  // TODO(APP-62): cover §H FourEyesViolation on the batch path. Mirrors the four §H tests on
  // LoadRiskLimitHandlerTest (emptyProposerId, emptyApproverId, proposerEqualsApprover,
  // distinctAccepted) but requires direct SBE batch-group encoding (the auto-fill helpers in
  // SbeTestEncoder always populate DEFAULT_PROPOSER_ID / DEFAULT_APPROVER_ID at the per-record
  // level inside the group). Deferred to plan §17 — Unit + integration + alloc tests — which
  // adds a `SbeTestEncoder.encodeLoadRiskLimitBatchWith4EyesBytes` helper.
}
