package com.trading.engine.cluster.refdata;

import com.trading.engine.messages.sbe.LoadRiskLimitBatchDecoder;
import com.trading.engine.messages.sbe.LoadRiskLimitBatchEncoder;
import com.trading.engine.messages.sbe.MessageHeaderDecoder;
import com.trading.engine.messages.sbe.MessageHeaderEncoder;
import com.trading.engine.messages.sbe.RejectReasonEnum;
import com.trading.engine.messages.sbe.RiskLimitLoadRejectedEventEncoder;
import com.trading.engine.messages.sbe.RiskLimitLoadedEventEncoder;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

/**
 * {@link ReferenceDataBatchLoader} for {@link LoadRiskLimitBatchDecoder LoadRiskLimitBatch}
 * (templateId 16). Iterates the {@code noRiskLimits} repeating group, validates each record with
 * the same rules as {@link LoadRiskLimitHandler} (including APP-62 §H 4-eyes), and emits one event
 * per record.
 */
public final class LoadRiskLimitBatchHandler implements ReferenceDataBatchLoader {

  private static final int ACCOUNT_ID_BYTE_LEN = 16;

  private final RiskLimitStore riskLimitStore;
  private final AccountStore accountStore;

  private final LoadRiskLimitBatchDecoder decoder = new LoadRiskLimitBatchDecoder();
  private final RiskLimitLoadedEventEncoder loadedEncoder = new RiskLimitLoadedEventEncoder();
  private final RiskLimitLoadRejectedEventEncoder rejectedEncoder =
      new RiskLimitLoadRejectedEventEncoder();
  private final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();

  private final byte[] proposerIdScratch = new byte[ACCOUNT_ID_BYTE_LEN];
  private final byte[] approverIdScratch = new byte[ACCOUNT_ID_BYTE_LEN];

  public LoadRiskLimitBatchHandler(RiskLimitStore riskLimitStore, AccountStore accountStore) {
    if (riskLimitStore == null) {
      throw new NullPointerException("riskLimitStore must not be null");
    }
    if (accountStore == null) {
      throw new NullPointerException("accountStore must not be null");
    }
    this.riskLimitStore = riskLimitStore;
    this.accountStore = accountStore;
  }

  @Override
  public int batchCommandTemplateId() {
    return LoadRiskLimitBatchEncoder.TEMPLATE_ID;
  }

  @Override
  public int onBatchCommand(
      MessageHeaderDecoder header,
      DirectBuffer src,
      int srcOffset,
      int srcLength,
      MutableDirectBuffer eventDst,
      int eventDstOffset,
      long firstSequenceNumber,
      long clusterTimestampNanos) {
    decoder.wrap(
        src,
        srcOffset + MessageHeaderDecoder.ENCODED_LENGTH,
        header.blockLength(),
        header.version());
    long batchTransactTime = decoder.transactTime();

    int written = 0;
    long seqNo = firstSequenceNumber;

    final var group = decoder.noRiskLimits();
    while (group.hasNext()) {
      group.next();

      long accountId = group.accountId();
      if (accountId <= 0L) {
        written +=
            emitRejected(
                eventDst,
                eventDstOffset + written,
                seqNo++,
                clusterTimestampNanos,
                accountId,
                RejectReasonEnum.InvalidAccountId,
                "accountId must be > 0");
        continue;
      }
      if (!accountStore.contains(accountId)) {
        written +=
            emitRejected(
                eventDst,
                eventDstOffset + written,
                seqNo++,
                clusterTimestampNanos,
                accountId,
                RejectReasonEnum.AccountNotFound,
                "accountId not in AccountStore");
        continue;
      }
      long maxOrderSize = group.maxOrderSize();
      long maxOrderNotional = group.maxOrderNotional();
      long maxDailyVolume = group.maxDailyVolume();
      long maxOrdersPerSecond = group.maxOrdersPerSecond();
      long maxLongPosition = group.maxLongPosition();
      long maxShortPosition = group.maxShortPosition();
      long priceDeviationBps = group.priceDeviationBps();
      long idleSessionTimeoutNanos = group.idleSessionTimeoutNanos();
      if (maxOrderSize < 0L
          || maxOrderNotional < 0L
          || maxDailyVolume < 0L
          || maxOrdersPerSecond < 0L
          || maxLongPosition < 0L
          || maxShortPosition < 0L
          || priceDeviationBps < 0L
          || idleSessionTimeoutNanos < 0L) {
        written +=
            emitRejected(
                eventDst,
                eventDstOffset + written,
                seqNo++,
                clusterTimestampNanos,
                accountId,
                RejectReasonEnum.InvalidLimitValue,
                "all risk limit values must be >= 0");
        continue;
      }

      // §H 4-eyes
      group.getProposerId(proposerIdScratch, 0);
      group.getApproverId(approverIdScratch, 0);
      if (isAllZero(proposerIdScratch) || isAllZero(approverIdScratch)) {
        written +=
            emitRejected(
                eventDst,
                eventDstOffset + written,
                seqNo++,
                clusterTimestampNanos,
                accountId,
                RejectReasonEnum.FourEyesViolation,
                "proposerId and approverId must be non-empty");
        continue;
      }
      if (byteEquals(proposerIdScratch, approverIdScratch)) {
        written +=
            emitRejected(
                eventDst,
                eventDstOffset + written,
                seqNo++,
                clusterTimestampNanos,
                accountId,
                RejectReasonEnum.FourEyesViolation,
                "proposerId must not equal approverId");
        continue;
      }

      boolean positionLimitEnabled = group.positionLimitEnabled() != 0;
      boolean fatFingerEnabled = group.fatFingerEnabled() != 0;
      boolean fatFingerFailClosed = group.fatFingerFailClosed() != 0;

      var state = riskLimitStore.get(accountId);
      if (state == null) {
        state = new RiskLimitState();
      }
      state.setAccountId(accountId);
      state.setMaxOrderSize(maxOrderSize);
      state.setMaxOrderNotional(maxOrderNotional);
      state.setMaxDailyVolume(maxDailyVolume);
      state.setMaxOrdersPerSecond(maxOrdersPerSecond);
      state.setMaxLongPosition(maxLongPosition);
      state.setMaxShortPosition(maxShortPosition);
      state.setPositionLimitEnabled(positionLimitEnabled);
      state.setPriceDeviationBps(priceDeviationBps);
      state.setFatFingerEnabled(fatFingerEnabled);
      state.setFatFingerFailClosed(fatFingerFailClosed);
      state.setIdleSessionTimeoutNanos(idleSessionTimeoutNanos);
      state.setProposerId(proposerIdScratch, 0, ACCOUNT_ID_BYTE_LEN);
      state.setApproverId(approverIdScratch, 0, ACCOUNT_ID_BYTE_LEN);
      state.setStatus(group.status());
      state.setTransactTime(batchTransactTime);
      riskLimitStore.put(state);

      // Emit loaded event.
      loadedEncoder.wrapAndApplyHeader(eventDst, eventDstOffset + written, headerEncoder);
      loadedEncoder.sequenceNumber(seqNo++);
      loadedEncoder.timestamp(clusterTimestampNanos);
      loadedEncoder.accountId(accountId);
      loadedEncoder.maxOrderSize(maxOrderSize);
      loadedEncoder.maxOrderNotional(maxOrderNotional);
      loadedEncoder.maxDailyVolume(maxDailyVolume);
      loadedEncoder.maxOrdersPerSecond(maxOrdersPerSecond);
      loadedEncoder.maxLongPosition(maxLongPosition);
      loadedEncoder.maxShortPosition(maxShortPosition);
      loadedEncoder.positionLimitEnabled((short) (positionLimitEnabled ? 1 : 0));
      loadedEncoder.priceDeviationBps(priceDeviationBps);
      loadedEncoder.fatFingerEnabled((short) (fatFingerEnabled ? 1 : 0));
      loadedEncoder.fatFingerFailClosed((short) (fatFingerFailClosed ? 1 : 0));
      loadedEncoder.idleSessionTimeoutNanos(idleSessionTimeoutNanos);
      loadedEncoder.putProposerId(proposerIdScratch, 0);
      loadedEncoder.putApproverId(approverIdScratch, 0);
      loadedEncoder.status(state.status());
      loadedEncoder.transactTime(batchTransactTime);
      written += MessageHeaderEncoder.ENCODED_LENGTH + loadedEncoder.encodedLength();
    }
    return written;
  }

  private int emitRejected(
      MutableDirectBuffer eventDst,
      int eventDstOffset,
      long sequenceNumber,
      long clusterTimestampNanos,
      long accountId,
      RejectReasonEnum reason,
      String text) {
    rejectedEncoder.wrapAndApplyHeader(eventDst, eventDstOffset, headerEncoder);
    rejectedEncoder.sequenceNumber(sequenceNumber);
    rejectedEncoder.timestamp(clusterTimestampNanos);
    rejectedEncoder.accountId(accountId);
    rejectedEncoder.rejectReason(reason);
    rejectedEncoder.text(text);
    return MessageHeaderEncoder.ENCODED_LENGTH + rejectedEncoder.encodedLength();
  }

  private static boolean isAllZero(byte[] buf) {
    for (int i = 0; i < buf.length; i++) {
      if (buf[i] != 0) {
        return false;
      }
    }
    return true;
  }

  private static boolean byteEquals(byte[] a, byte[] b) {
    for (int i = 0; i < a.length; i++) {
      if (a[i] != b[i]) {
        return false;
      }
    }
    return true;
  }
}
