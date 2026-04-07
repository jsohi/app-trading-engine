package com.trading.engine.cluster.refdata;

import com.trading.engine.messages.sbe.CurrencyClassEnum;
import com.trading.engine.messages.sbe.CurrencyLoadRejectedEventEncoder;
import com.trading.engine.messages.sbe.CurrencyLoadedEventEncoder;
import com.trading.engine.messages.sbe.LoadCurrencyDecoder;
import com.trading.engine.messages.sbe.LoadCurrencyEncoder;
import com.trading.engine.messages.sbe.MessageHeaderDecoder;
import com.trading.engine.messages.sbe.MessageHeaderEncoder;
import com.trading.engine.messages.sbe.RejectReasonEnum;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

/**
 * {@link ReferenceDataLoader} for {@link LoadCurrencyDecoder LoadCurrency} (templateId 13).
 *
 * <p>Validation rules (each failure produces a {@code CurrencyLoadRejectedEvent}):
 *
 * <ul>
 *   <li><b>InvalidCurrencyCode</b> — code is not 3 uppercase ASCII letters
 *   <li><b>InvalidLimitValue</b> — isoNumeric &lt; 1 or &gt; 999, or decimals &gt; 18
 * </ul>
 *
 * <p>Successful upserts emit {@code CurrencyLoadedEvent}. Re-loading the same code is idempotent
 * (overwrite). Zero allocation on the validate-and-emit path; the {@link CurrencyState} instance is
 * reused for upserts of an existing code (a fresh one is allocated on first insert — startup-only
 * path, allocation acceptable).
 */
public final class LoadCurrencyHandler implements ReferenceDataLoader {

  private static final int NAME_LENGTH = 64;

  private final CurrencyStore store;

  // Pre-allocated SBE flyweights — reused across every dispatch.
  private final LoadCurrencyDecoder decoder = new LoadCurrencyDecoder();
  private final CurrencyLoadedEventEncoder loadedEncoder = new CurrencyLoadedEventEncoder();
  private final CurrencyLoadRejectedEventEncoder rejectedEncoder =
      new CurrencyLoadRejectedEventEncoder();
  private final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();

  // Scratch buffers for copying char[] fields out of the decoder without allocation.
  private final byte[] scratchName = new byte[NAME_LENGTH];

  public LoadCurrencyHandler(final CurrencyStore store) {
    if (store == null) {
      throw new NullPointerException("store must not be null");
    }
    this.store = store;
  }

  @Override
  public int commandTemplateId() {
    return LoadCurrencyEncoder.TEMPLATE_ID;
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

    // Read the 3 code bytes via primitive accessors (no String allocation).
    final byte b0 = decoder.ccyCode(0);
    final byte b1 = decoder.ccyCode(1);
    final byte b2 = decoder.ccyCode(2);
    if (!isUpperAlpha(b0) || !isUpperAlpha(b1) || !isUpperAlpha(b2)) {
      return emitRejected(
          eventDst,
          eventDstOffset,
          sequenceNumber,
          clusterTimestampNanos,
          b0,
          b1,
          b2,
          RejectReasonEnum.InvalidCurrencyCode,
          "ccyCode must be 3 uppercase ASCII letters");
    }
    final int isoNumeric = decoder.isoNumeric();
    if (isoNumeric < 1 || isoNumeric > 999) {
      return emitRejected(
          eventDst,
          eventDstOffset,
          sequenceNumber,
          clusterTimestampNanos,
          b0,
          b1,
          b2,
          RejectReasonEnum.InvalidLimitValue,
          "isoNumeric out of range 1..999");
    }
    final short decimals = decoder.decimals();
    if (decimals < 0 || decimals > 18) {
      return emitRejected(
          eventDst,
          eventDstOffset,
          sequenceNumber,
          clusterTimestampNanos,
          b0,
          b1,
          b2,
          RejectReasonEnum.InvalidLimitValue,
          "decimals out of range 0..18");
    }
    final CurrencyClassEnum currencyClass = decoder.currencyClass();

    // Read the name into the scratch buffer once (no String allocation). scratchName now holds
    // the 64-byte fixed-length name field; we keep it pristine for the rest of this method.
    decoder.getName(scratchName, 0);
    final int nameLength = trimTrailingZeros(scratchName, NAME_LENGTH);

    // Upsert. If the code already exists, mutate the existing CurrencyState in place (no
    // allocation). Otherwise allocate one (startup-only path, allowed).
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
    state.setStatus(decoder.status());
    state.setTransactTime(decoder.transactTime());
    store.put(packedKey, state);

    // Emit CurrencyLoadedEvent. scratchName is still the pristine 64-byte name we read above.
    loadedEncoder.wrapAndApplyHeader(eventDst, eventDstOffset, headerEncoder);
    loadedEncoder.sequenceNumber(sequenceNumber);
    loadedEncoder.timestamp(clusterTimestampNanos);
    loadedEncoder.putCcyCode(b0, b1, b2);
    loadedEncoder.isoNumeric(isoNumeric);
    loadedEncoder.putName(scratchName, 0);
    loadedEncoder.decimals(decimals);
    loadedEncoder.currencyClass(currencyClass);
    loadedEncoder.status(state.status());
    loadedEncoder.transactTime(state.transactTime());

    return MessageHeaderEncoder.ENCODED_LENGTH + loadedEncoder.encodedLength();
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
