package com.trading.engine.cluster.refdata;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.trading.engine.messages.sbe.AccountStatusEnum;
import com.trading.engine.messages.sbe.LoadRiskLimitEncoder;
import com.trading.engine.messages.sbe.MessageHeaderDecoder;
import com.trading.engine.messages.sbe.MessageHeaderEncoder;
import com.trading.engine.messages.sbe.RejectReasonEnum;
import com.trading.engine.messages.sbe.RiskLimitLoadRejectedEventDecoder;
import com.trading.engine.messages.sbe.RiskLimitLoadedEventDecoder;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.junit.jupiter.api.Test;

class LoadRiskLimitHandlerTest {

  private static final long SEQ_NO = 99L;
  private static final long TS = 1_700_000_000_000_000_000L;

  private static int encodeLoadRiskLimit(
      final MutableDirectBuffer dst,
      final long accountId,
      final long maxOrderSize,
      final long maxOrderNotional,
      final long maxDailyVolume,
      final long maxDailyLossBps) {
    final MessageHeaderEncoder header = new MessageHeaderEncoder();
    final LoadRiskLimitEncoder enc = new LoadRiskLimitEncoder();
    enc.wrapAndApplyHeader(dst, 0, header);
    enc.accountId(accountId);
    enc.maxOrderSize(maxOrderSize);
    enc.maxOrderNotional(maxOrderNotional);
    enc.maxDailyVolume(maxDailyVolume);
    enc.maxDailyLossBps(maxDailyLossBps);
    enc.status(AccountStatusEnum.Active);
    enc.transactTime(0L);
    return MessageHeaderEncoder.ENCODED_LENGTH + enc.encodedLength();
  }

  private static int dispatch(
      final LoadRiskLimitHandler handler,
      final MutableDirectBuffer src,
      final int srcLength,
      final MutableDirectBuffer eventDst) {
    final MessageHeaderDecoder header = new MessageHeaderDecoder();
    header.wrap(src, 0);
    return handler.onCommand(header, src, 0, srcLength, eventDst, 0, SEQ_NO, TS);
  }

  private static AccountStore accountStoreWith(final long... ids) {
    final AccountStore store = new AccountStore();
    for (final long id : ids) {
      store.put(AccountStoreTest.makeState(id, "ACC" + id, "Account " + id, "USD"));
    }
    return store;
  }

  @Test
  void validLoadEmitsLoadedEventAndUpserts() {
    final AccountStore accountStore = accountStoreWith(1L);
    final RiskLimitStore riskStore = new RiskLimitStore();
    final LoadRiskLimitHandler handler = new LoadRiskLimitHandler(riskStore, accountStore);

    final MutableDirectBuffer src = new ExpandableArrayBuffer(256);
    final int srcLength = encodeLoadRiskLimit(src, 1L, 100_00000000L, 0L, 1000_00000000L, 0L);
    final MutableDirectBuffer eventDst = new ExpandableArrayBuffer(256);
    dispatch(handler, src, srcLength, eventDst);

    final MessageHeaderDecoder header = new MessageHeaderDecoder();
    header.wrap(eventDst, 0);
    assertEquals(RiskLimitLoadedEventDecoder.TEMPLATE_ID, header.templateId());

    final RiskLimitLoadedEventDecoder dec = new RiskLimitLoadedEventDecoder();
    dec.wrap(eventDst, MessageHeaderDecoder.ENCODED_LENGTH, header.blockLength(), header.version());
    assertEquals(1L, dec.accountId());
    assertEquals(100_00000000L, dec.maxOrderSize());

    assertEquals(1, riskStore.size());
    assertNotNull(riskStore.get(1L));
  }

  @Test
  void zeroAccountIdRejected() {
    final AccountStore accountStore = accountStoreWith(1L);
    final RiskLimitStore riskStore = new RiskLimitStore();
    final LoadRiskLimitHandler handler = new LoadRiskLimitHandler(riskStore, accountStore);

    final MutableDirectBuffer src = new ExpandableArrayBuffer(256);
    final int srcLength = encodeLoadRiskLimit(src, 0L, 100L, 0L, 1000L, 0L);
    final MutableDirectBuffer eventDst = new ExpandableArrayBuffer(256);
    dispatch(handler, src, srcLength, eventDst);

    final MessageHeaderDecoder header = new MessageHeaderDecoder();
    header.wrap(eventDst, 0);
    assertEquals(RiskLimitLoadRejectedEventDecoder.TEMPLATE_ID, header.templateId());

    final RiskLimitLoadRejectedEventDecoder dec = new RiskLimitLoadRejectedEventDecoder();
    dec.wrap(eventDst, MessageHeaderDecoder.ENCODED_LENGTH, header.blockLength(), header.version());
    assertEquals(RejectReasonEnum.InvalidAccountId, dec.rejectReason());
    assertEquals(0, riskStore.size());
  }

  @Test
  void unknownAccountIdRejected() {
    final AccountStore accountStore = accountStoreWith(); // empty
    final RiskLimitStore riskStore = new RiskLimitStore();
    final LoadRiskLimitHandler handler = new LoadRiskLimitHandler(riskStore, accountStore);

    final MutableDirectBuffer src = new ExpandableArrayBuffer(256);
    final int srcLength = encodeLoadRiskLimit(src, 999L, 100L, 0L, 1000L, 0L);
    final MutableDirectBuffer eventDst = new ExpandableArrayBuffer(256);
    dispatch(handler, src, srcLength, eventDst);

    final MessageHeaderDecoder header = new MessageHeaderDecoder();
    header.wrap(eventDst, 0);
    assertEquals(RiskLimitLoadRejectedEventDecoder.TEMPLATE_ID, header.templateId());

    final RiskLimitLoadRejectedEventDecoder dec = new RiskLimitLoadRejectedEventDecoder();
    dec.wrap(eventDst, MessageHeaderDecoder.ENCODED_LENGTH, header.blockLength(), header.version());
    assertEquals(RejectReasonEnum.AccountNotFound, dec.rejectReason());
    assertEquals(0, riskStore.size());
  }

  @Test
  void negativeLimitRejected() {
    final AccountStore accountStore = accountStoreWith(1L);
    final RiskLimitStore riskStore = new RiskLimitStore();
    final LoadRiskLimitHandler handler = new LoadRiskLimitHandler(riskStore, accountStore);

    final MutableDirectBuffer src = new ExpandableArrayBuffer(256);
    final int srcLength = encodeLoadRiskLimit(src, 1L, -1L, 0L, 1000L, 0L);
    final MutableDirectBuffer eventDst = new ExpandableArrayBuffer(256);
    dispatch(handler, src, srcLength, eventDst);

    final MessageHeaderDecoder header = new MessageHeaderDecoder();
    header.wrap(eventDst, 0);
    assertEquals(RiskLimitLoadRejectedEventDecoder.TEMPLATE_ID, header.templateId());

    final RiskLimitLoadRejectedEventDecoder dec = new RiskLimitLoadRejectedEventDecoder();
    dec.wrap(eventDst, MessageHeaderDecoder.ENCODED_LENGTH, header.blockLength(), header.version());
    assertEquals(RejectReasonEnum.InvalidLimitValue, dec.rejectReason());
    assertEquals(0, riskStore.size());
  }

  @Test
  void upsertOverwrites() {
    final AccountStore accountStore = accountStoreWith(1L);
    final RiskLimitStore riskStore = new RiskLimitStore();
    final LoadRiskLimitHandler handler = new LoadRiskLimitHandler(riskStore, accountStore);

    final MutableDirectBuffer src = new ExpandableArrayBuffer(256);
    final MutableDirectBuffer eventDst = new ExpandableArrayBuffer(256);

    int srcLength = encodeLoadRiskLimit(src, 1L, 100L, 0L, 1000L, 0L);
    dispatch(handler, src, srcLength, eventDst);
    srcLength = encodeLoadRiskLimit(src, 1L, 500L, 0L, 5000L, 0L);
    dispatch(handler, src, srcLength, eventDst);

    assertEquals(1, riskStore.size());
    assertEquals(500L, riskStore.get(1L).maxOrderSize());
  }

  @Test
  void commandTemplateIdMatches() {
    final LoadRiskLimitHandler h =
        new LoadRiskLimitHandler(new RiskLimitStore(), new AccountStore());
    assertEquals(LoadRiskLimitEncoder.TEMPLATE_ID, h.commandTemplateId());
  }
}
