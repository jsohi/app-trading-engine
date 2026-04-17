package com.trading.refdata.account;

import com.trading.engine.messages.sbe.AccountStatusEnum;
import com.trading.engine.messages.sbe.AccountTypeEnum;
import com.trading.engine.messages.sbe.AcctIDSourceEnum;
import com.trading.engine.messages.sbe.ComplianceStatusEnum;
import com.trading.engine.messages.sbe.LoadAccountBatchEncoder;
import com.trading.engine.messages.sbe.LoadAccountBatchEncoder.NoAccountsEncoder;
import com.trading.engine.messages.sbe.MessageHeaderEncoder;
import com.trading.refdata.ReferenceDataLoadException;
import com.trading.refdata.spi.ReferenceDataEncoder;
import java.util.List;
import org.agrona.MutableDirectBuffer;

/**
 * Encodes {@link AccountRecord} instances into a {@code LoadAccountBatch} SBE message
 * (templateId&nbsp;12).
 *
 * <p>Not thread-safe — reuses mutable SBE flyweight fields. Single-threaded use only.
 */
public final class AccountCommandEncoder implements ReferenceDataEncoder<AccountRecord> {

  private static final int MAX_BATCH_SIZE = 200;
  private static final String ENTITY_TYPE = "Account";

  private final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
  private final LoadAccountBatchEncoder batchEncoder = new LoadAccountBatchEncoder();

  /** {@inheritDoc} */
  @Override
  public int encodeBatch(
      final List<AccountRecord> records,
      final int fromIndex,
      final int toIndex,
      final MutableDirectBuffer buffer,
      final int offset)
      throws ReferenceDataLoadException {

    final int count = toIndex - fromIndex;
    batchEncoder.wrapAndApplyHeader(buffer, offset, headerEncoder);
    // Cluster overwrites this field with its own deterministic timestamp
    batchEncoder.transactTime(0L);

    final NoAccountsEncoder group = batchEncoder.noAccountsCount(count);

    for (int i = fromIndex; i < toIndex; i++) {
      final var record = records.get(i);
      group
          .next()
          .accountId(record.accountId())
          .parentAccountId(record.parentAccountId())
          .accountCode(record.accountCode())
          .acctIdSource(toAcctIdSource(record.acctIdSource()))
          .accountName(record.accountName())
          .accountType(toAccountType(record.accountType()))
          .baseCurrency(record.baseCurrency())
          .status(toStatus(record.status()))
          .complianceStatus(toComplianceStatus(record.complianceStatus()))
          .capabilities(record.capabilities());
    }

    return MessageHeaderEncoder.ENCODED_LENGTH + batchEncoder.encodedLength();
  }

  /** {@inheritDoc} */
  @Override
  public int templateId() {
    return LoadAccountBatchEncoder.TEMPLATE_ID;
  }

  /** {@inheritDoc} */
  @Override
  public int maxBatchSize() {
    return MAX_BATCH_SIZE;
  }

  /** {@inheritDoc} */
  @Override
  public String entityType() {
    return ENTITY_TYPE;
  }

  private static AcctIDSourceEnum toAcctIdSource(final String value)
      throws ReferenceDataLoadException {
    return switch (value) {
      case "Internal" -> AcctIDSourceEnum.Internal;
      case "BIC" -> AcctIDSourceEnum.BIC;
      case "SID" -> AcctIDSourceEnum.SID;
      case "TFM" -> AcctIDSourceEnum.TFM;
      case "OMGEO" -> AcctIDSourceEnum.OMGEO;
      default ->
          throw new ReferenceDataLoadException(
              ENTITY_TYPE, "unknown acctIdSource: '" + value + "'");
    };
  }

  private static AccountTypeEnum toAccountType(final String value)
      throws ReferenceDataLoadException {
    return switch (value) {
      case "House" -> AccountTypeEnum.House;
      case "Client" -> AccountTypeEnum.Client;
      case "MarketMaker" -> AccountTypeEnum.MarketMaker;
      default ->
          throw new ReferenceDataLoadException(ENTITY_TYPE, "unknown accountType: '" + value + "'");
    };
  }

  private static AccountStatusEnum toStatus(final String value) throws ReferenceDataLoadException {
    return switch (value) {
      case "Active" -> AccountStatusEnum.Active;
      case "Suspended" -> AccountStatusEnum.Suspended;
      case "Closed" -> AccountStatusEnum.Closed;
      default ->
          throw new ReferenceDataLoadException(ENTITY_TYPE, "unknown status: '" + value + "'");
    };
  }

  private static ComplianceStatusEnum toComplianceStatus(final String value)
      throws ReferenceDataLoadException {
    return switch (value) {
      case "OK" -> ComplianceStatusEnum.OK;
      case "PendingReview" -> ComplianceStatusEnum.PendingReview;
      case "Suspended" -> ComplianceStatusEnum.Suspended;
      case "Blocked" -> ComplianceStatusEnum.Blocked;
      default ->
          throw new ReferenceDataLoadException(
              ENTITY_TYPE, "unknown complianceStatus: '" + value + "'");
    };
  }
}
