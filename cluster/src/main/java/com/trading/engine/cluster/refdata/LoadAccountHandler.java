package com.trading.engine.cluster.refdata;

import com.trading.engine.messages.sbe.AccountLoadRejectedEventEncoder;
import com.trading.engine.messages.sbe.AccountLoadedEventEncoder;
import com.trading.engine.messages.sbe.LoadAccountDecoder;
import com.trading.engine.messages.sbe.LoadAccountEncoder;
import com.trading.engine.messages.sbe.MessageHeaderDecoder;
import com.trading.engine.messages.sbe.MessageHeaderEncoder;
import com.trading.engine.messages.sbe.RejectReasonEnum;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

/**
 * {@link ReferenceDataLoader} for {@link LoadAccountDecoder LoadAccount} (templateId 11).
 *
 * <p>Validation rules (each failure produces an {@code AccountLoadRejectedEvent}):
 *
 * <ul>
 *   <li><b>InvalidAccountId</b> — accountId &lt;= 0
 *   <li><b>UnknownCurrency</b> — base currency not present in the configured {@link CurrencyStore}
 *       (FK validation; only enforced if a CurrencyStore reference was passed to the constructor)
 *   <li><b>DuplicateAccountCode</b> — another accountId already owns this accountCode
 * </ul>
 *
 * <p>Successful upserts emit {@code AccountLoadedEvent}. Re-loading the same accountId is
 * idempotent (overwrite). Zero allocation on the validate-and-emit path; existing AccountState
 * instances are reused on overwrite.
 */
public final class LoadAccountHandler implements ReferenceDataLoader {

  private static final int CODE_LENGTH = AccountStore.MAX_ACCOUNT_CODE_LENGTH;
  private static final int NAME_LENGTH = 64;

  private final AccountStore accountStore;
  private final CurrencyStore
      currencyStore; // Nullable — if null, base-currency FK check is skipped.

  // Pre-allocated SBE flyweights.
  private final LoadAccountDecoder decoder = new LoadAccountDecoder();
  private final AccountLoadedEventEncoder loadedEncoder = new AccountLoadedEventEncoder();
  private final AccountLoadRejectedEventEncoder rejectedEncoder =
      new AccountLoadRejectedEventEncoder();
  private final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();

  // Scratch buffers for reading/writing fixed-length char fields.
  private final byte[] codeScratch = new byte[CODE_LENGTH];
  private final byte[] nameScratch = new byte[NAME_LENGTH];

  public LoadAccountHandler(final AccountStore accountStore) {
    this(accountStore, null);
  }

  public LoadAccountHandler(final AccountStore accountStore, final CurrencyStore currencyStore) {
    if (accountStore == null) {
      throw new NullPointerException("accountStore must not be null");
    }
    this.accountStore = accountStore;
    this.currencyStore = currencyStore;
  }

  @Override
  public int commandTemplateId() {
    return LoadAccountEncoder.TEMPLATE_ID;
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
    decoder.getAccountCode(codeScratch, 0);
    final int codeLength = RefDataUtils.trimTrailingZeros(codeScratch, CODE_LENGTH);

    if (accountId <= 0L) {
      return emitRejected(
          eventDst,
          eventDstOffset,
          sequenceNumber,
          clusterTimestampNanos,
          codeScratch,
          RejectReasonEnum.InvalidAccountId,
          "accountId must be > 0");
    }
    if (codeLength == 0) {
      return emitRejected(
          eventDst,
          eventDstOffset,
          sequenceNumber,
          clusterTimestampNanos,
          codeScratch,
          RejectReasonEnum.InvalidAccountId,
          "accountCode must be non-empty");
    }

    // Base currency FK validation (only if a CurrencyStore is wired). Use the non-throwing
    // packCodeOrInvalid so a malformed wire byte is a branch, not an exception allocation.
    final byte ccy0 = decoder.baseCurrency(0);
    final byte ccy1 = decoder.baseCurrency(1);
    final byte ccy2 = decoder.baseCurrency(2);
    if (currencyStore != null) {
      final int packed = CurrencyStore.packCodeOrInvalid(ccy0, ccy1, ccy2);
      if (packed == CurrencyStore.INVALID_PACKED_CODE) {
        return emitRejected(
            eventDst,
            eventDstOffset,
            sequenceNumber,
            clusterTimestampNanos,
            codeScratch,
            RejectReasonEnum.InvalidCurrencyCode,
            "baseCurrency must be 3 uppercase ASCII letters");
      }
      if (!currencyStore.contains(packed)) {
        return emitRejected(
            eventDst,
            eventDstOffset,
            sequenceNumber,
            clusterTimestampNanos,
            codeScratch,
            RejectReasonEnum.UnknownCurrency,
            "baseCurrency not in CurrencyStore");
      }
    }

    // Duplicate-code check: if some OTHER accountId already owns this code, reject.
    final AccountState existingByCode = accountStore.getByCodeBytes(codeScratch, 0, codeLength);
    if (existingByCode != null && existingByCode.accountId() != accountId) {
      return emitRejected(
          eventDst,
          eventDstOffset,
          sequenceNumber,
          clusterTimestampNanos,
          codeScratch,
          RejectReasonEnum.DuplicateAccountCode,
          "accountCode already owned by a different accountId");
    }

    // Upsert. Reuse existing AccountState if accountId already known.
    AccountState state = accountStore.get(accountId);
    if (state == null) {
      state = new AccountState();
    }
    state.setAccountId(accountId);
    state.setParentAccountId(decoder.parentAccountId());
    state.setAccountCode(codeScratch, 0, codeLength);
    state.setAcctIdSource(decoder.acctIdSource());
    decoder.getAccountName(nameScratch, 0);
    state.setAccountName(nameScratch, 0, RefDataUtils.trimTrailingZeros(nameScratch, NAME_LENGTH));
    state.setAccountType(decoder.accountType());
    state.setBaseCurrency(ccy0, ccy1, ccy2);
    state.setStatus(decoder.status());
    state.setComplianceStatus(decoder.complianceStatus());
    state.setCapabilities(decoder.capabilities());
    state.setTransactTime(decoder.transactTime());
    accountStore.put(state);

    // Emit AccountLoadedEvent.
    loadedEncoder.wrapAndApplyHeader(eventDst, eventDstOffset, headerEncoder);
    loadedEncoder.sequenceNumber(sequenceNumber);
    loadedEncoder.timestamp(clusterTimestampNanos);
    loadedEncoder.accountId(accountId);
    loadedEncoder.parentAccountId(state.parentAccountId());
    // Re-pad the code to fixed 16 bytes for the wire (System.arraycopy + Arrays.fill).
    state.copyAccountCodeTo(codeScratch, 0);
    if (codeLength < CODE_LENGTH) {
      java.util.Arrays.fill(codeScratch, codeLength, CODE_LENGTH, (byte) 0);
    }
    loadedEncoder.putAccountCode(codeScratch, 0);
    loadedEncoder.acctIdSource(state.acctIdSource());
    final int storedNameLen = state.copyAccountNameTo(nameScratch, 0);
    if (storedNameLen < NAME_LENGTH) {
      java.util.Arrays.fill(nameScratch, storedNameLen, NAME_LENGTH, (byte) 0);
    }
    loadedEncoder.putAccountName(nameScratch, 0);
    loadedEncoder.accountType(state.accountType());
    loadedEncoder.putBaseCurrency(ccy0, ccy1, ccy2);
    loadedEncoder.status(state.status());
    loadedEncoder.complianceStatus(state.complianceStatus());
    loadedEncoder.capabilities(state.capabilities());
    loadedEncoder.transactTime(state.transactTime());

    return MessageHeaderEncoder.ENCODED_LENGTH + loadedEncoder.encodedLength();
  }

  private int emitRejected(
      final MutableDirectBuffer eventDst,
      final int eventDstOffset,
      final long sequenceNumber,
      final long clusterTimestampNanos,
      final byte[] codeBytes,
      final RejectReasonEnum reason,
      final String text) {
    rejectedEncoder.wrapAndApplyHeader(eventDst, eventDstOffset, headerEncoder);
    rejectedEncoder.sequenceNumber(sequenceNumber);
    rejectedEncoder.timestamp(clusterTimestampNanos);
    // codeBytes is always the 16-byte codeScratch (zero-padded after the live length).
    rejectedEncoder.putAccountCode(codeBytes, 0);
    rejectedEncoder.rejectReason(reason);
    rejectedEncoder.text(text);
    return MessageHeaderEncoder.ENCODED_LENGTH + rejectedEncoder.encodedLength();
  }
}
