package com.trading.engine.cluster.refdata;

import com.trading.engine.messages.sbe.LoadRiskLimitDecoder;
import com.trading.engine.messages.sbe.LoadRiskLimitEncoder;
import com.trading.engine.messages.sbe.MessageHeaderDecoder;
import com.trading.engine.messages.sbe.MessageHeaderEncoder;
import com.trading.engine.messages.sbe.RejectReasonEnum;
import com.trading.engine.messages.sbe.RiskLimitChangedEventEncoder;
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

  /**
   * APP-62 §D audit event — emitted after every successful LoadRiskLimit upsert. Carries the
   * proposerId / approverId from {@link #setProposerId}/{@link #setApproverId} plus a repeating
   * group of (fieldName, oldValue, newValue) for each scalar field whose value changed. First-load
   * (no prior {@code RiskLimitRecord}) emits an empty group per plan §3.1; the operator
   * distinguishes first-load from update by checking the group count.
   */
  private final RiskLimitChangedEventEncoder changedEncoder = new RiskLimitChangedEventEncoder();

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
    boolean firstLoad = state == null;
    // APP-62 §D — capture prior scalar field values BEFORE mutating the state, so the
    // RiskLimitChangedEvent (template 119) carries accurate old/new pairs for the audit trail.
    // On first-load (state == null) the prior values are all zero and the oldRecord group will
    // be emitted empty per the schema convention.
    long oldMaxOrderSize = firstLoad ? 0L : state.maxOrderSize();
    long oldMaxOrderNotional = firstLoad ? 0L : state.maxOrderNotional();
    long oldMaxDailyVolume = firstLoad ? 0L : state.maxDailyVolume();
    long oldMaxOrdersPerSecond = firstLoad ? 0L : state.maxOrdersPerSecond();
    long oldMaxLongPosition = firstLoad ? 0L : state.maxLongPosition();
    long oldMaxShortPosition = firstLoad ? 0L : state.maxShortPosition();
    boolean oldPositionLimitEnabled = !firstLoad && state.positionLimitEnabled();
    long oldPriceDeviationBps = firstLoad ? 0L : state.priceDeviationBps();
    boolean oldFatFingerEnabled = !firstLoad && state.fatFingerEnabled();
    boolean oldFatFingerFailClosed = !firstLoad && state.fatFingerFailClosed();
    long oldIdleSessionTimeoutNanos = firstLoad ? 0L : state.idleSessionTimeoutNanos();

    if (firstLoad) {
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

    int loadedLen = MessageHeaderEncoder.ENCODED_LENGTH + loadedEncoder.encodedLength();

    // APP-62 §D — append RiskLimitChangedEvent (template 119) right after the loaded event so
    // both land in the same EventSink emit call. The cluster's event sequencer will assign
    // sequenceNumber + 1 to this event when it stamps the egress.
    int changedLen =
        encodeChangedEvent(
            eventDst,
            eventDstOffset + loadedLen,
            sequenceNumber + 1L,
            clusterTimestampNanos,
            accountId,
            firstLoad,
            oldMaxOrderSize,
            maxOrderSize,
            oldMaxOrderNotional,
            maxOrderNotional,
            oldMaxDailyVolume,
            maxDailyVolume,
            oldMaxOrdersPerSecond,
            maxOrdersPerSecond,
            oldMaxLongPosition,
            maxLongPosition,
            oldMaxShortPosition,
            maxShortPosition,
            oldPositionLimitEnabled,
            positionLimitEnabled,
            oldPriceDeviationBps,
            priceDeviationBps,
            oldFatFingerEnabled,
            fatFingerEnabled,
            oldFatFingerFailClosed,
            fatFingerFailClosed,
            oldIdleSessionTimeoutNanos,
            idleSessionTimeoutNanos);

    return loadedLen + changedLen;
  }

  /**
   * APP-62 §D — encodes one {@code RiskLimitChangedEvent} (template 119) into the egress buffer.
   * The repeating {@code oldRecord} group carries one entry per scalar field whose value changed
   * (oldValue != newValue); on first-load the group is emitted empty per the schema convention.
   * Boolean fields are sign-extended to int64 as 0/1; uint32 fields are zero-extended.
   *
   * @return total encoded length including the SBE message header
   */
  private int encodeChangedEvent(
      final MutableDirectBuffer eventDst,
      int eventDstOffset,
      long sequenceNumber,
      long clusterTimestampNanos,
      long accountId,
      boolean firstLoad,
      long oldMaxOrderSize,
      long newMaxOrderSize,
      long oldMaxOrderNotional,
      long newMaxOrderNotional,
      long oldMaxDailyVolume,
      long newMaxDailyVolume,
      long oldMaxOrdersPerSecond,
      long newMaxOrdersPerSecond,
      long oldMaxLongPosition,
      long newMaxLongPosition,
      long oldMaxShortPosition,
      long newMaxShortPosition,
      boolean oldPositionLimitEnabled,
      boolean newPositionLimitEnabled,
      long oldPriceDeviationBps,
      long newPriceDeviationBps,
      boolean oldFatFingerEnabled,
      boolean newFatFingerEnabled,
      boolean oldFatFingerFailClosed,
      boolean newFatFingerFailClosed,
      long oldIdleSessionTimeoutNanos,
      long newIdleSessionTimeoutNanos) {
    changedEncoder.wrapAndApplyHeader(eventDst, eventDstOffset, headerEncoder);
    changedEncoder.sequenceNumber(sequenceNumber);
    changedEncoder.timestamp(clusterTimestampNanos);
    changedEncoder.accountId(accountId);
    changedEncoder.putProposerId(proposerIdScratch, 0);
    changedEncoder.putApproverId(approverIdScratch, 0);

    // Group population — first-load → empty group (count=0). Update → one entry per CHANGED field.
    int diffCount =
        firstLoad
            ? 0
            : countDiffs(
                oldMaxOrderSize, newMaxOrderSize,
                oldMaxOrderNotional, newMaxOrderNotional,
                oldMaxDailyVolume, newMaxDailyVolume,
                oldMaxOrdersPerSecond, newMaxOrdersPerSecond,
                oldMaxLongPosition, newMaxLongPosition,
                oldMaxShortPosition, newMaxShortPosition,
                oldPositionLimitEnabled, newPositionLimitEnabled,
                oldPriceDeviationBps, newPriceDeviationBps,
                oldFatFingerEnabled, newFatFingerEnabled,
                oldFatFingerFailClosed, newFatFingerFailClosed,
                oldIdleSessionTimeoutNanos, newIdleSessionTimeoutNanos);

    final var group = changedEncoder.oldRecordCount(diffCount);
    if (diffCount > 0) {
      writeDiffIfChanged(group, "maxOrderSize", oldMaxOrderSize, newMaxOrderSize);
      writeDiffIfChanged(group, "maxOrderNotional", oldMaxOrderNotional, newMaxOrderNotional);
      writeDiffIfChanged(group, "maxDailyVolume", oldMaxDailyVolume, newMaxDailyVolume);
      writeDiffIfChanged(group, "maxOrdersPerSecond", oldMaxOrdersPerSecond, newMaxOrdersPerSecond);
      writeDiffIfChanged(group, "maxLongPosition", oldMaxLongPosition, newMaxLongPosition);
      writeDiffIfChanged(group, "maxShortPosition", oldMaxShortPosition, newMaxShortPosition);
      writeDiffIfChanged(
          group,
          "positionLimitEnabled",
          oldPositionLimitEnabled ? 1L : 0L,
          newPositionLimitEnabled ? 1L : 0L);
      writeDiffIfChanged(group, "priceDeviationBps", oldPriceDeviationBps, newPriceDeviationBps);
      writeDiffIfChanged(
          group, "fatFingerEnabled", oldFatFingerEnabled ? 1L : 0L, newFatFingerEnabled ? 1L : 0L);
      writeDiffIfChanged(
          group,
          "fatFingerFailClosed",
          oldFatFingerFailClosed ? 1L : 0L,
          newFatFingerFailClosed ? 1L : 0L);
      writeDiffIfChanged(
          group, "idleSessionTimeoutNanos", oldIdleSessionTimeoutNanos, newIdleSessionTimeoutNanos);
    }

    return MessageHeaderEncoder.ENCODED_LENGTH + changedEncoder.encodedLength();
  }

  private static void writeDiffIfChanged(
      final RiskLimitChangedEventEncoder.OldRecordEncoder group,
      final String fieldName,
      long oldValue,
      long newValue) {
    if (oldValue == newValue) {
      return;
    }
    group.next();
    group.fieldName(fieldName);
    group.oldValue(oldValue);
    group.newValue(newValue);
  }

  private static int countDiffs(
      long oldA,
      long newA,
      long oldB,
      long newB,
      long oldC,
      long newC,
      long oldD,
      long newD,
      long oldE,
      long newE,
      long oldF,
      long newF,
      boolean oldG,
      boolean newG,
      long oldH,
      long newH,
      boolean oldI,
      boolean newI,
      boolean oldJ,
      boolean newJ,
      long oldK,
      long newK) {
    int count = 0;
    if (oldA != newA) count++;
    if (oldB != newB) count++;
    if (oldC != newC) count++;
    if (oldD != newD) count++;
    if (oldE != newE) count++;
    if (oldF != newF) count++;
    if (oldG != newG) count++;
    if (oldH != newH) count++;
    if (oldI != newI) count++;
    if (oldJ != newJ) count++;
    if (oldK != newK) count++;
    return count;
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
