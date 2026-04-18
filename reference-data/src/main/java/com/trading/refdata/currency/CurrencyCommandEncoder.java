package com.trading.refdata.currency;

import com.trading.engine.messages.sbe.AccountStatusEnum;
import com.trading.engine.messages.sbe.CurrencyClassEnum;
import com.trading.engine.messages.sbe.LoadCurrencyBatchEncoder;
import com.trading.engine.messages.sbe.LoadCurrencyBatchEncoder.NoCurrenciesEncoder;
import com.trading.engine.messages.sbe.MessageHeaderEncoder;
import com.trading.refdata.ReferenceDataLoadException;
import com.trading.refdata.spi.ReferenceDataEncoder;
import java.util.List;
import org.agrona.MutableDirectBuffer;

/**
 * Encodes {@link CurrencyRecord} instances into a {@code LoadCurrencyBatch} SBE message
 * (templateId&nbsp;14).
 *
 * <p>Not thread-safe — reuses mutable SBE flyweight fields. Single-threaded use only.
 */
public final class CurrencyCommandEncoder implements ReferenceDataEncoder<CurrencyRecord> {

  private static final int MAX_BATCH_SIZE = 200;
  private static final String ENTITY_TYPE = "Currency";

  private final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
  private final LoadCurrencyBatchEncoder batchEncoder = new LoadCurrencyBatchEncoder();

  /** {@inheritDoc} */
  @Override
  public int encodeBatch(
      final List<CurrencyRecord> records,
      final int fromIndex,
      final int toIndex,
      final MutableDirectBuffer buffer,
      final int offset)
      throws ReferenceDataLoadException {

    final int count = toIndex - fromIndex;
    batchEncoder.wrapAndApplyHeader(buffer, offset, headerEncoder);
    // Cluster overwrites this field with its own deterministic timestamp
    batchEncoder.transactTime(0L);

    final NoCurrenciesEncoder group = batchEncoder.noCurrenciesCount(count);

    for (int i = fromIndex; i < toIndex; i++) {
      final var record = records.get(i);
      group
          .next()
          .ccyCode(record.ccyCode())
          .isoNumeric(record.isoNumeric())
          .name(record.name())
          .decimals((short) record.decimals())
          .currencyClass(toCurrencyClass(record.currencyClass()))
          .status(toStatus(record.status()));
    }

    return MessageHeaderEncoder.ENCODED_LENGTH + batchEncoder.encodedLength();
  }

  /** {@inheritDoc} */
  @Override
  public int templateId() {
    return LoadCurrencyBatchEncoder.TEMPLATE_ID;
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

  private static CurrencyClassEnum toCurrencyClass(final String value)
      throws ReferenceDataLoadException {
    return switch (value) {
      case "Fiat" -> CurrencyClassEnum.Fiat;
      case "Metal" -> CurrencyClassEnum.Metal;
      case "Crypto" -> CurrencyClassEnum.Crypto;
      case "Fund" -> CurrencyClassEnum.Fund;
      default ->
          throw new ReferenceDataLoadException(
              ENTITY_TYPE, "unknown currencyClass: '" + value + "'");
    };
  }

  // SBE schema reuses AccountStatusEnum for currency/risk-limit status lifecycle
  // (Active/Suspended/Closed)
  private static AccountStatusEnum toStatus(final String value) throws ReferenceDataLoadException {
    return switch (value) {
      case "Active" -> AccountStatusEnum.Active;
      case "Suspended" -> AccountStatusEnum.Suspended;
      case "Closed" -> AccountStatusEnum.Closed;
      default ->
          throw new ReferenceDataLoadException(ENTITY_TYPE, "unknown status: '" + value + "'");
    };
  }
}
