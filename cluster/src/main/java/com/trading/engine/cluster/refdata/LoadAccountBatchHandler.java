package com.trading.engine.cluster.refdata;

import com.trading.engine.messages.sbe.AccountLoadRejectedEventEncoder;
import com.trading.engine.messages.sbe.AccountLoadedEventEncoder;
import com.trading.engine.messages.sbe.LoadAccountBatchDecoder;
import com.trading.engine.messages.sbe.LoadAccountBatchEncoder;
import com.trading.engine.messages.sbe.MessageHeaderDecoder;
import com.trading.engine.messages.sbe.MessageHeaderEncoder;
import com.trading.engine.messages.sbe.RejectReasonEnum;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

/**
 * {@link ReferenceDataBatchLoader} for {@link LoadAccountBatchDecoder LoadAccountBatch} (templateId
 * 12). Iterates the {@code noAccounts} repeating group, validates each record with the same rules
 * as {@link LoadAccountHandler}, and emits one event per record.
 *
 * <p>Each emitted event gets a sequence number starting at {@code firstSequenceNumber} and
 * incrementing by 1.
 */
public final class LoadAccountBatchHandler implements ReferenceDataBatchLoader {

  private static final int CODE_LENGTH = AccountStore.MAX_ACCOUNT_CODE_LENGTH;
  private static final int NAME_LENGTH = 64;

  private final AccountStore accountStore;
  private final CurrencyStore currencyStore;

  private final LoadAccountBatchDecoder decoder = new LoadAccountBatchDecoder();
  private final AccountLoadedEventEncoder loadedEncoder = new AccountLoadedEventEncoder();
  private final AccountLoadRejectedEventEncoder rejectedEncoder =
      new AccountLoadRejectedEventEncoder();
  private final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();

  private final byte[] codeScratch = new byte[CODE_LENGTH];
  private final byte[] nameScratch = new byte[NAME_LENGTH];

  public LoadAccountBatchHandler(final AccountStore accountStore) {
    this(accountStore, null);
  }

  public LoadAccountBatchHandler(
      final AccountStore accountStore, final CurrencyStore currencyStore) {
    if (accountStore == null) {
      throw new NullPointerException("accountStore must not be null");
    }
    this.accountStore = accountStore;
    this.currencyStore = currencyStore;
  }

  @Override
  public int batchCommandTemplateId() {
    return LoadAccountBatchEncoder.TEMPLATE_ID;
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

    final LoadAccountBatchDecoder.NoAccountsDecoder group = decoder.noAccounts();
    while (group.hasNext()) {
      group.next();

      final long accountId = group.accountId();
      group.getAccountCode(codeScratch, 0);
      final int codeLength = trimTrailingZeros(codeScratch, CODE_LENGTH);

      if (accountId <= 0L) {
        written +=
            emitRejected(
                eventDst,
                eventDstOffset + written,
                seqNo++,
                clusterTimestampNanos,
                RejectReasonEnum.InvalidAccountId,
                "accountId must be > 0");
        continue;
      }
      if (codeLength == 0) {
        written +=
            emitRejected(
                eventDst,
                eventDstOffset + written,
                seqNo++,
                clusterTimestampNanos,
                RejectReasonEnum.InvalidAccountId,
                "accountCode must be non-empty");
        continue;
      }

      final byte ccy0 = group.baseCurrency(0);
      final byte ccy1 = group.baseCurrency(1);
      final byte ccy2 = group.baseCurrency(2);
      if (currencyStore != null) {
        final int packed = CurrencyStore.packCodeOrInvalid(ccy0, ccy1, ccy2);
        if (packed == CurrencyStore.INVALID_PACKED_CODE) {
          written +=
              emitRejected(
                  eventDst,
                  eventDstOffset + written,
                  seqNo++,
                  clusterTimestampNanos,
                  RejectReasonEnum.InvalidCurrencyCode,
                  "baseCurrency must be 3 uppercase ASCII letters");
          continue;
        }
        if (!currencyStore.contains(packed)) {
          written +=
              emitRejected(
                  eventDst,
                  eventDstOffset + written,
                  seqNo++,
                  clusterTimestampNanos,
                  RejectReasonEnum.UnknownCurrency,
                  "baseCurrency not in CurrencyStore");
          continue;
        }
      }

      // Duplicate-code check.
      final AccountState existing = accountStore.getByCodeBytes(codeScratch, 0, codeLength);
      if (existing != null && existing.accountId() != accountId) {
        written +=
            emitRejected(
                eventDst,
                eventDstOffset + written,
                seqNo++,
                clusterTimestampNanos,
                RejectReasonEnum.DuplicateAccountCode,
                "accountCode already owned by a different accountId");
        continue;
      }

      // Upsert.
      AccountState state = accountStore.get(accountId);
      if (state == null) {
        state = new AccountState();
      }
      state.setAccountId(accountId);
      state.setParentAccountId(group.parentAccountId());
      state.setAccountCode(codeScratch, 0, codeLength);
      state.setAcctIdSource(group.acctIdSource());
      group.getAccountName(nameScratch, 0);
      state.setAccountName(nameScratch, 0, trimTrailingZeros(nameScratch, NAME_LENGTH));
      state.setAccountType(group.accountType());
      state.setBaseCurrency(ccy0, ccy1, ccy2);
      state.setStatus(group.status());
      state.setComplianceStatus(group.complianceStatus());
      state.setCapabilities(group.capabilities());
      state.setTransactTime(batchTransactTime);
      accountStore.put(state);

      // Emit loaded event.
      loadedEncoder.wrapAndApplyHeader(eventDst, eventDstOffset + written, headerEncoder);
      loadedEncoder.sequenceNumber(seqNo++);
      loadedEncoder.timestamp(clusterTimestampNanos);
      loadedEncoder.accountId(accountId);
      loadedEncoder.parentAccountId(state.parentAccountId());
      // Re-pad the code to fixed 16 bytes.
      for (int i = 0; i < CODE_LENGTH; i++) {
        codeScratch[i] = i < codeLength ? state.accountCodeByte(i) : (byte) 0;
      }
      loadedEncoder.putAccountCode(codeScratch, 0);
      loadedEncoder.acctIdSource(state.acctIdSource());
      for (int i = 0; i < NAME_LENGTH; i++) {
        nameScratch[i] = i < state.accountNameLength() ? state.accountNameByte(i) : (byte) 0;
      }
      loadedEncoder.putAccountName(nameScratch, 0);
      loadedEncoder.accountType(state.accountType());
      loadedEncoder.putBaseCurrency(ccy0, ccy1, ccy2);
      loadedEncoder.status(state.status());
      loadedEncoder.complianceStatus(state.complianceStatus());
      loadedEncoder.capabilities(state.capabilities());
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
      final RejectReasonEnum reason,
      final String text) {
    rejectedEncoder.wrapAndApplyHeader(eventDst, eventDstOffset, headerEncoder);
    rejectedEncoder.sequenceNumber(sequenceNumber);
    rejectedEncoder.timestamp(clusterTimestampNanos);
    rejectedEncoder.putAccountCode(codeScratch, 0);
    rejectedEncoder.rejectReason(reason);
    rejectedEncoder.text(text);
    return MessageHeaderEncoder.ENCODED_LENGTH + rejectedEncoder.encodedLength();
  }

  private static int trimTrailingZeros(final byte[] bytes, final int upToLength) {
    int len = upToLength;
    while (len > 0 && bytes[len - 1] == 0) {
      len--;
    }
    return len;
  }
}
