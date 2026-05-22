package com.trading.engine.cluster.refdata;

import com.trading.engine.messages.sbe.LoadRiskLimitDecoder;
import com.trading.engine.messages.sbe.LoadRiskLimitEncoder;
import com.trading.engine.messages.sbe.MessageHeaderDecoder;
import com.trading.engine.messages.sbe.MessageHeaderEncoder;
import com.trading.engine.messages.sbe.RejectReasonEnum;
import com.trading.engine.messages.sbe.RiskLimitLoadRejectedEventEncoder;
import com.trading.engine.messages.sbe.RiskLimitLoadedEventEncoder;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

/**
 * {@link ReferenceDataLoader} for {@link LoadRiskLimitDecoder LoadRiskLimit} (templateId 15).
 *
 * <p>Validation rules:
 *
 * <ul>
 *   <li><b>InvalidAccountId</b> — accountId &lt;= 0
 *   <li><b>AccountNotFound</b> — accountId not in {@link AccountStore} (FK validation)
 *   <li><b>InvalidLimitValue</b> — any of the limit values is negative
 * </ul>
 *
 * <p>Successful upserts emit {@code RiskLimitLoadedEvent}; re-loading the same accountId is
 * idempotent. Zero allocation on the validate-and-emit path.
 */
public final class LoadRiskLimitHandler implements ReferenceDataLoader {

  private final RiskLimitStore riskLimitStore;
  private final AccountStore accountStore; // FK target

  private final LoadRiskLimitDecoder decoder = new LoadRiskLimitDecoder();
  private final RiskLimitLoadedEventEncoder loadedEncoder = new RiskLimitLoadedEventEncoder();
  private final RiskLimitLoadRejectedEventEncoder rejectedEncoder =
      new RiskLimitLoadRejectedEventEncoder();
  private final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();

  public LoadRiskLimitHandler(
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
  public int commandTemplateId() {
    return LoadRiskLimitEncoder.TEMPLATE_ID;
  }

  @Override
  public int onCommand(
      final MessageHeaderDecoder header,
      final DirectBuffer src,
      final int srcOffset,
      final int srcLength,
      final MutableDirectBuffer eventDst,
      final int eventDstOffset,
      final long sequenceNumber,
      final long clusterTimestampNanos) {
    decoder.wrap(
        src,
        srcOffset + MessageHeaderDecoder.ENCODED_LENGTH,
        header.blockLength(),
        header.version());

    final long accountId = decoder.accountId();
    if (accountId <= 0L) {
      return emitRejected(
          eventDst,
          eventDstOffset,
          sequenceNumber,
          clusterTimestampNanos,
          accountId,
          RejectReasonEnum.InvalidAccountId,
          "accountId must be > 0");
    }
    if (!accountStore.contains(accountId)) {
      return emitRejected(
          eventDst,
          eventDstOffset,
          sequenceNumber,
          clusterTimestampNanos,
          accountId,
          RejectReasonEnum.AccountNotFound,
          "accountId not in AccountStore");
    }
    final long maxOrderSize = decoder.maxOrderSize();
    final long maxOrderNotional = decoder.maxOrderNotional();
    final long maxDailyVolume = decoder.maxDailyVolume();
    final long maxDailyLossBps = decoder.maxDailyLossBps();
    final long maxOrdersPerSecond = decoder.maxOrdersPerSecond();
    if (maxOrderSize < 0L
        || maxOrderNotional < 0L
        || maxDailyVolume < 0L
        || maxDailyLossBps < 0L
        || maxOrdersPerSecond < 0L) {
      return emitRejected(
          eventDst,
          eventDstOffset,
          sequenceNumber,
          clusterTimestampNanos,
          accountId,
          RejectReasonEnum.InvalidLimitValue,
          "all risk limit values must be >= 0");
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
    state.setMaxOrdersPerSecond(maxOrdersPerSecond);
    state.setStatus(decoder.status());
    state.setTransactTime(decoder.transactTime());
    riskLimitStore.put(state);

    // Emit loaded event.
    loadedEncoder.wrapAndApplyHeader(eventDst, eventDstOffset, headerEncoder);
    loadedEncoder.sequenceNumber(sequenceNumber);
    loadedEncoder.timestamp(clusterTimestampNanos);
    loadedEncoder.accountId(accountId);
    loadedEncoder.maxOrderSize(maxOrderSize);
    loadedEncoder.maxOrderNotional(maxOrderNotional);
    loadedEncoder.maxDailyVolume(maxDailyVolume);
    loadedEncoder.maxDailyLossBps(maxDailyLossBps);
    loadedEncoder.maxOrdersPerSecond(maxOrdersPerSecond);
    loadedEncoder.status(state.status());
    loadedEncoder.transactTime(state.transactTime());

    return MessageHeaderEncoder.ENCODED_LENGTH + loadedEncoder.encodedLength();
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
