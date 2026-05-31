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
import com.trading.engine.testsupport.sbe.SbeTestEncoder;
import java.nio.charset.StandardCharsets;
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
      final long maxDailyVolume) {
    return SbeTestEncoder.encodeLoadRiskLimit(
        dst,
        0,
        accountId,
        maxOrderSize,
        maxOrderNotional,
        maxDailyVolume,
        AccountStatusEnum.Active,
        0L);
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
      store.put(AccountFixtures.account(id, "ACC" + id, "Account " + id, "USD"));
    }
    return store;
  }

  @Test
  void validLoadEmitsLoadedEventAndUpserts() {
    final AccountStore accountStore = accountStoreWith(1L);
    final RiskLimitStore riskStore = new RiskLimitStore();
    final LoadRiskLimitHandler handler = new LoadRiskLimitHandler(riskStore, accountStore);

    final MutableDirectBuffer src = new ExpandableArrayBuffer(256);
    final int srcLength = encodeLoadRiskLimit(src, 1L, 100_00000000L, 0L, 1000_00000000L);
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
    final int srcLength = encodeLoadRiskLimit(src, 0L, 100L, 0L, 1000L);
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
    final int srcLength = encodeLoadRiskLimit(src, 999L, 100L, 0L, 1000L);
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
    final int srcLength = encodeLoadRiskLimit(src, 1L, -1L, 0L, 1000L);
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

    int srcLength = encodeLoadRiskLimit(src, 1L, 100L, 0L, 1000L);
    dispatch(handler, src, srcLength, eventDst);
    srcLength = encodeLoadRiskLimit(src, 1L, 500L, 0L, 5000L);
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

  // ---------------------------------------------------------------------------
  // APP-62 §H — 4-eyes (MiFID II RTS 6 §1(2)) — proposerId / approverId must both
  // be non-empty and not byte-equal. Tests encode the SBE command directly so they
  // can override the defaults that SbeTestEncoder helpers fill in.
  // ---------------------------------------------------------------------------

  private static int encodeLoadRiskLimitWith4EyesBytes(
      final MutableDirectBuffer dst, final byte[] proposerId, final byte[] approverId) {
    final var headerEnc = new MessageHeaderEncoder();
    final var enc = new LoadRiskLimitEncoder();
    enc.wrapAndApplyHeader(dst, 0, headerEnc);
    enc.accountId(1L)
        .maxOrderSize(100L)
        .maxOrderNotional(0L)
        .maxDailyVolume(1000L)
        .putProposerId(proposerId, 0)
        .putApproverId(approverId, 0)
        .status(AccountStatusEnum.Active)
        .transactTime(0L);
    return MessageHeaderEncoder.ENCODED_LENGTH + enc.encodedLength();
  }

  private static byte[] padded(final String s) {
    final byte[] out = new byte[16];
    final byte[] src = s.getBytes(StandardCharsets.US_ASCII);
    System.arraycopy(src, 0, out, 0, Math.min(src.length, out.length));
    return out;
  }

  @Test
  void emptyProposerIdRejectedAsFourEyesViolation() {
    final AccountStore accountStore = accountStoreWith(1L);
    final RiskLimitStore riskStore = new RiskLimitStore();
    final LoadRiskLimitHandler handler = new LoadRiskLimitHandler(riskStore, accountStore);

    final MutableDirectBuffer src = new ExpandableArrayBuffer(256);
    final int srcLength =
        encodeLoadRiskLimitWith4EyesBytes(src, new byte[16] /* empty */, padded("APPROVER"));
    final MutableDirectBuffer eventDst = new ExpandableArrayBuffer(256);
    dispatch(handler, src, srcLength, eventDst);

    final MessageHeaderDecoder header = new MessageHeaderDecoder();
    header.wrap(eventDst, 0);
    assertEquals(RiskLimitLoadRejectedEventDecoder.TEMPLATE_ID, header.templateId());

    final RiskLimitLoadRejectedEventDecoder dec = new RiskLimitLoadRejectedEventDecoder();
    dec.wrap(eventDst, MessageHeaderDecoder.ENCODED_LENGTH, header.blockLength(), header.version());
    assertEquals(RejectReasonEnum.FourEyesViolation, dec.rejectReason());
    assertEquals(0, riskStore.size());
  }

  @Test
  void emptyApproverIdRejectedAsFourEyesViolation() {
    final AccountStore accountStore = accountStoreWith(1L);
    final RiskLimitStore riskStore = new RiskLimitStore();
    final LoadRiskLimitHandler handler = new LoadRiskLimitHandler(riskStore, accountStore);

    final MutableDirectBuffer src = new ExpandableArrayBuffer(256);
    final int srcLength =
        encodeLoadRiskLimitWith4EyesBytes(src, padded("PROPOSER"), new byte[16] /* empty */);
    final MutableDirectBuffer eventDst = new ExpandableArrayBuffer(256);
    dispatch(handler, src, srcLength, eventDst);

    final MessageHeaderDecoder header = new MessageHeaderDecoder();
    header.wrap(eventDst, 0);
    assertEquals(RiskLimitLoadRejectedEventDecoder.TEMPLATE_ID, header.templateId());

    final RiskLimitLoadRejectedEventDecoder dec = new RiskLimitLoadRejectedEventDecoder();
    dec.wrap(eventDst, MessageHeaderDecoder.ENCODED_LENGTH, header.blockLength(), header.version());
    assertEquals(RejectReasonEnum.FourEyesViolation, dec.rejectReason());
    assertEquals(0, riskStore.size());
  }

  @Test
  void proposerEqualsApproverRejectedAsFourEyesViolation() {
    final AccountStore accountStore = accountStoreWith(1L);
    final RiskLimitStore riskStore = new RiskLimitStore();
    final LoadRiskLimitHandler handler = new LoadRiskLimitHandler(riskStore, accountStore);

    final MutableDirectBuffer src = new ExpandableArrayBuffer(256);
    final byte[] same = padded("ALICE");
    final int srcLength = encodeLoadRiskLimitWith4EyesBytes(src, same, same);
    final MutableDirectBuffer eventDst = new ExpandableArrayBuffer(256);
    dispatch(handler, src, srcLength, eventDst);

    final MessageHeaderDecoder header = new MessageHeaderDecoder();
    header.wrap(eventDst, 0);
    assertEquals(RiskLimitLoadRejectedEventDecoder.TEMPLATE_ID, header.templateId());

    final RiskLimitLoadRejectedEventDecoder dec = new RiskLimitLoadRejectedEventDecoder();
    dec.wrap(eventDst, MessageHeaderDecoder.ENCODED_LENGTH, header.blockLength(), header.version());
    assertEquals(RejectReasonEnum.FourEyesViolation, dec.rejectReason());
    assertEquals(0, riskStore.size());
  }

  @Test
  void distinctNonEmptyProposerAndApproverAccepted() {
    final AccountStore accountStore = accountStoreWith(1L);
    final RiskLimitStore riskStore = new RiskLimitStore();
    final LoadRiskLimitHandler handler = new LoadRiskLimitHandler(riskStore, accountStore);

    final MutableDirectBuffer src = new ExpandableArrayBuffer(256);
    final int srcLength = encodeLoadRiskLimitWith4EyesBytes(src, padded("ALICE"), padded("BOB"));
    final MutableDirectBuffer eventDst = new ExpandableArrayBuffer(256);
    dispatch(handler, src, srcLength, eventDst);

    final MessageHeaderDecoder header = new MessageHeaderDecoder();
    header.wrap(eventDst, 0);
    assertEquals(RiskLimitLoadedEventDecoder.TEMPLATE_ID, header.templateId());
    assertEquals(1, riskStore.size());
  }
}
