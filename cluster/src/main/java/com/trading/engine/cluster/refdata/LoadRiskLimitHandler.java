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
 *   <li><b>InvalidLimitValue</b> — any of the numeric limit values is negative
 *   <li><b>FourEyesViolation</b> — APP-62 §H: proposerId or approverId empty, or proposerId
 *       byte-equals approverId (MiFID II RTS 6 §1(2) dual-control)
 * </ul>
 *
 * <p>Successful upserts emit {@code RiskLimitLoadedEvent}; re-loading the same accountId is
 * idempotent.
 *
 * <p><b>Threading.</b> Not thread-safe — single-threaded cluster duty cycle only. The {@code
 * proposerIdScratch} and {@code approverIdScratch} byte arrays are mutable per-instance buffers
 * that are reused on every {@link #onCommand} call.
 *
 * <p><b>Allocation.</b> Zero-allocation on the validate-and-emit hot path. The single exception is
 * the first-load {@code new RiskLimitState()} when the account has no prior record; this is a
 * reference-data ingress path (not the order-matching hot path) and is acceptable per the CLAUDE.md
 * reference-data carve-out.
 *
 * <p>APP-62: dropped {@code maxDailyLossBps} (re-added by APP-180); added 9 new fields (position
 * L/S caps, fat-finger knobs, per-account idle timeout, proposerId/approverId 4-eyes identifiers).
 */
public final class LoadRiskLimitHandler implements ReferenceDataLoader {

  private final RiskLimitStore riskLimitStore;
  private final AccountStore accountStore; // FK target

  private final LoadRiskLimitDecoder decoder = new LoadRiskLimitDecoder();
  private final RiskLimitLoadedEventEncoder loadedEncoder = new RiskLimitLoadedEventEncoder();
  private final RiskLimitLoadRejectedEventEncoder rejectedEncoder =
      new RiskLimitLoadRejectedEventEncoder();
  private final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();

  private final byte[] proposerIdScratch = new byte[AccountIdentifierBytes.LENGTH];
  private final byte[] approverIdScratch = new byte[AccountIdentifierBytes.LENGTH];

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
      int srcOffset,
      int srcLength,
      final MutableDirectBuffer eventDst,
      int eventDstOffset,
      long sequenceNumber,
      long clusterTimestampNanos) {
    decoder.wrap(
        src,
        srcOffset + MessageHeaderDecoder.ENCODED_LENGTH,
        header.blockLength(),
        header.version());

    long accountId = decoder.accountId();
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
    long maxOrderSize = decoder.maxOrderSize();
    long maxOrderNotional = decoder.maxOrderNotional();
    long maxDailyVolume = decoder.maxDailyVolume();
    long maxOrdersPerSecond = decoder.maxOrdersPerSecond();
    long maxLongPosition = decoder.maxLongPosition();
    long maxShortPosition = decoder.maxShortPosition();
    long priceDeviationBps = decoder.priceDeviationBps();
    long idleSessionTimeoutNanos = decoder.idleSessionTimeoutNanos();
    if (maxOrderSize < 0L
        || maxOrderNotional < 0L
        || maxDailyVolume < 0L
        || maxOrdersPerSecond < 0L
        || maxLongPosition < 0L
        || maxShortPosition < 0L
        || priceDeviationBps < 0L
        || idleSessionTimeoutNanos < 0L) {
      return emitRejected(
          eventDst,
          eventDstOffset,
          sequenceNumber,
          clusterTimestampNanos,
          accountId,
          RejectReasonEnum.InvalidLimitValue,
          "all risk limit values must be >= 0");
    }

    // §H 4-eyes — proposerId and approverId both non-empty, must not match each other.
    decoder.getProposerId(proposerIdScratch, 0);
    decoder.getApproverId(approverIdScratch, 0);
    if (AccountIdentifierBytes.isAllZero(proposerIdScratch)
        || AccountIdentifierBytes.isAllZero(approverIdScratch)) {
      return emitRejected(
          eventDst,
          eventDstOffset,
          sequenceNumber,
          clusterTimestampNanos,
          accountId,
          RejectReasonEnum.FourEyesViolation,
          "proposerId and approverId must be non-empty");
    }
    if (AccountIdentifierBytes.byteEquals(proposerIdScratch, approverIdScratch)) {
      return emitRejected(
          eventDst,
          eventDstOffset,
          sequenceNumber,
          clusterTimestampNanos,
          accountId,
          RejectReasonEnum.FourEyesViolation,
          "proposerId must not equal approverId (MiFID II RTS 6 §1(2))");
    }

    boolean positionLimitEnabled = decoder.positionLimitEnabled() != 0;
    boolean fatFingerEnabled = decoder.fatFingerEnabled() != 0;
    boolean fatFingerFailClosed = decoder.fatFingerFailClosed() != 0;

    // Upsert.
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
    state.setProposerId(proposerIdScratch, 0, AccountIdentifierBytes.LENGTH);
    state.setApproverId(approverIdScratch, 0, AccountIdentifierBytes.LENGTH);
    state.setStatus(decoder.status());
    state.setTransactTime(decoder.transactTime());
    riskLimitStore.put(state);

    // Emit loaded event mirroring the input.
    loadedEncoder.wrapAndApplyHeader(eventDst, eventDstOffset, headerEncoder);
    loadedEncoder.sequenceNumber(sequenceNumber);
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
    loadedEncoder.transactTime(state.transactTime());

    return MessageHeaderEncoder.ENCODED_LENGTH + loadedEncoder.encodedLength();
  }

  private int emitRejected(
      final MutableDirectBuffer eventDst,
      int eventDstOffset,
      long sequenceNumber,
      long clusterTimestampNanos,
      long accountId,
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
