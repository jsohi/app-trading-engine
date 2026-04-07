package com.trading.engine.cluster.refdata;

import com.trading.engine.messages.sbe.CurrencyClassEnum;
import com.trading.engine.messages.sbe.CurrencyLoadRejectedEventEncoder;
import com.trading.engine.messages.sbe.CurrencyLoadedEventEncoder;
import com.trading.engine.messages.sbe.LoadCurrencyBatchDecoder;
import com.trading.engine.messages.sbe.LoadCurrencyBatchEncoder;
import com.trading.engine.messages.sbe.MessageHeaderDecoder;
import com.trading.engine.messages.sbe.MessageHeaderEncoder;
import com.trading.engine.messages.sbe.RejectReasonEnum;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

/**
 * {@link ReferenceDataBatchLoader} for {@link LoadCurrencyBatchDecoder LoadCurrencyBatch}
 * (templateId 14).
 *
 * <p>Iterates the {@code noCurrencies} repeating group, validates each record independently with
 * the same rules as {@link LoadCurrencyHandler}, upserts into the {@link CurrencyStore}, and emits
 * one {@code CurrencyLoadedEvent} or {@code CurrencyLoadRejectedEvent} per record. The batch is NOT
 * all-or-nothing — a bad record produces a rejection event and the batch continues, matching
 * exchange-core's start-of-day load semantics.
 *
 * <p>Each emitted event is assigned a sequence number starting at {@code firstSequenceNumber} and
 * incrementing by 1 per emitted event (so projection consumers see contiguous sequence numbers
 * across the batch).
 */
public final class LoadCurrencyBatchHandler implements ReferenceDataBatchLoader {

  private static final int NAME_LENGTH = 64;

  private final CurrencyStore store;

  private final LoadCurrencyBatchDecoder decoder = new LoadCurrencyBatchDecoder();
  private final CurrencyLoadedEventEncoder loadedEncoder = new CurrencyLoadedEventEncoder();
  private final CurrencyLoadRejectedEventEncoder rejectedEncoder =
      new CurrencyLoadRejectedEventEncoder();
  private final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();

  private final byte[] scratchName = new byte[NAME_LENGTH];

  public LoadCurrencyBatchHandler(final CurrencyStore store) {
    if (store == null) {
      throw new NullPointerException("store must not be null");
    }
    this.store = store;
  }

  @Override
  public int batchCommandTemplateId() {
    return LoadCurrencyBatchEncoder.TEMPLATE_ID;
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

    final LoadCurrencyBatchDecoder.NoCurrenciesDecoder group = decoder.noCurrencies();
    while (group.hasNext()) {
      group.next();

      final byte b0 = group.ccyCode(0);
      final byte b1 = group.ccyCode(1);
      final byte b2 = group.ccyCode(2);
      final int isoNumeric = group.isoNumeric();
      final short decimals = group.decimals();

      if (!isUpperAlpha(b0) || !isUpperAlpha(b1) || !isUpperAlpha(b2)) {
        written +=
            emitRejected(
                eventDst,
                eventDstOffset + written,
                seqNo++,
                clusterTimestampNanos,
                b0,
                b1,
                b2,
                RejectReasonEnum.InvalidCurrencyCode,
                "ccyCode must be 3 uppercase ASCII letters");
        continue;
      }
      if (isoNumeric < 1 || isoNumeric > 999) {
        written +=
            emitRejected(
                eventDst,
                eventDstOffset + written,
                seqNo++,
                clusterTimestampNanos,
                b0,
                b1,
                b2,
                RejectReasonEnum.InvalidLimitValue,
                "isoNumeric out of range 1..999");
        continue;
      }
      if (decimals < 0 || decimals > 18) {
        written +=
            emitRejected(
                eventDst,
                eventDstOffset + written,
                seqNo++,
                clusterTimestampNanos,
                b0,
                b1,
                b2,
                RejectReasonEnum.InvalidLimitValue,
                "decimals out of range 0..18");
        continue;
      }

      final CurrencyClassEnum currencyClass = group.currencyClass();
      final int nameRead = group.getName(scratchName, 0);
      final int nameLength = trimTrailingZeros(scratchName, nameRead);

      // Upsert.
      final int packedKey = CurrencyStore.packCode(b0, b1, b2);
      CurrencyState state = store.get(packedKey);
      if (state == null) {
        state = new CurrencyState();
      }
      state.setCcyCode(b0, b1, b2);
      state.setIsoNumeric(isoNumeric);
      state.setName(scratchName, 0, nameLength);
      state.setDecimals(decimals);
      state.setCurrencyClass(currencyClass);
      state.setStatus(group.status());
      state.setTransactTime(batchTransactTime);
      store.put(packedKey, state);

      // Emit loaded event.
      loadedEncoder.wrapAndApplyHeader(eventDst, eventDstOffset + written, headerEncoder);
      loadedEncoder.sequenceNumber(seqNo++);
      loadedEncoder.timestamp(clusterTimestampNanos);
      loadedEncoder.putCcyCode(b0, b1, b2);
      loadedEncoder.isoNumeric(isoNumeric);
      // scratchName is still the 64-byte fixed-length name from group.getName above — no
      // re-read needed.
      loadedEncoder.putName(scratchName, 0);
      loadedEncoder.decimals(decimals);
      loadedEncoder.currencyClass(currencyClass);
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
      final byte b0,
      final byte b1,
      final byte b2,
      final RejectReasonEnum reason,
      final String text) {
    rejectedEncoder.wrapAndApplyHeader(eventDst, eventDstOffset, headerEncoder);
    rejectedEncoder.sequenceNumber(sequenceNumber);
    rejectedEncoder.timestamp(clusterTimestampNanos);
    rejectedEncoder.putCcyCode(b0, b1, b2);
    rejectedEncoder.rejectReason(reason);
    rejectedEncoder.text(text);
    return MessageHeaderEncoder.ENCODED_LENGTH + rejectedEncoder.encodedLength();
  }

  private static boolean isUpperAlpha(final byte b) {
    return b >= 'A' && b <= 'Z';
  }

  private static int trimTrailingZeros(final byte[] bytes, final int upToLength) {
    int len = upToLength;
    while (len > 0 && bytes[len - 1] == 0) {
      len--;
    }
    return len;
  }
}
