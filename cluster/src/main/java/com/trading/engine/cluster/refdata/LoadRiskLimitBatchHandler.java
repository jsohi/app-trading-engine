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
 * the same rules as {@link LoadRiskLimitHandler}, and emits one event per record.
 */
public final class LoadRiskLimitBatchHandler implements ReferenceDataBatchLoader {

  private final RiskLimitStore riskLimitStore;
  private final AccountStore accountStore;

  private final LoadRiskLimitBatchDecoder decoder = new LoadRiskLimitBatchDecoder();
  private final RiskLimitLoadedEventEncoder loadedEncoder = new RiskLimitLoadedEventEncoder();
  private final RiskLimitLoadRejectedEventEncoder rejectedEncoder =
      new RiskLimitLoadRejectedEventEncoder();
  private final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();

  public LoadRiskLimitBatchHandler(
      final RiskLimitStore riskLimitStore, final AccountStore accountStore) {
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
      final MessageHeaderDecoder header,
      final DirectBuffer src,
      final int srcOffset,
      final int srcLength,
      final MutableDirectBuffer eventDst,
      final int eventDstOffset,
      final long firstSequenceNumber,
      final long clusterTimestampNanos) {
    decoder.wrap(
        src,
        srcOffset + MessageHeaderDecoder.ENCODED_LENGTH,
        header.blockLength(),
        header.version());
    final long batchTransactTime = decoder.transactTime();

    int written = 0;
    long seqNo = firstSequenceNumber;

    final LoadRiskLimitBatchDecoder.NoRiskLimitsDecoder group = decoder.noRiskLimits();
    while (group.hasNext()) {
      group.next();

      final long accountId = group.accountId();
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
      final long maxOrderSize = group.maxOrderSize();
      final long maxOrderNotional = group.maxOrderNotional();
      final long maxDailyVolume = group.maxDailyVolume();
      final long maxDailyLossBps = group.maxDailyLossBps();
      if (maxOrderSize < 0L
          || maxOrderNotional < 0L
          || maxDailyVolume < 0L
          || maxDailyLossBps < 0L) {
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

      // Upsert.
      RiskLimitState state = riskLimitStore.get(accountId);
      if (state == null) {
        state = new RiskLimitState();
      }
      state.setAccountId(accountId);
      state.setMaxOrderSize(maxOrderSize);
      state.setMaxOrderNotional(maxOrderNotional);
      state.setMaxDailyVolume(maxDailyVolume);
      state.setMaxDailyLossBps(maxDailyLossBps);
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
      loadedEncoder.maxDailyLossBps(maxDailyLossBps);
      loadedEncoder.status(state.status());
      loadedEncoder.transactTime(batchTransactTime);
      written += MessageHeaderEncoder.ENCODED_LENGTH + loadedEncoder.encodedLength();
    }
    return written;
  }

  private int emitRejected(
      final MutableDirectBuffer eventDst,
      final int eventDstOffset,
      final long sequenceNumber,
      final long clusterTimestampNanos,
      final long accountId,
      final RejectReasonEnum reason,
      final String text) {
    rejectedEncoder.wrapAndApplyHeader(eventDst, eventDstOffset, headerEncoder);
    rejectedEncoder.sequenceNumber(sequenceNumber);
    rejectedEncoder.timestamp(clusterTimestampNanos);
    rejectedEncoder.accountId(accountId);
    rejectedEncoder.rejectReason(reason);
    rejectedEncoder.text(text);
    return MessageHeaderEncoder.ENCODED_LENGTH + rejectedEncoder.encodedLength();
  }
}
