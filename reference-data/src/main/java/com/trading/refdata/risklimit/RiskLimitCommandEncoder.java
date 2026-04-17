package com.trading.refdata.risklimit;

import com.trading.engine.messages.sbe.AccountStatusEnum;
import com.trading.engine.messages.sbe.LoadRiskLimitBatchEncoder;
import com.trading.engine.messages.sbe.LoadRiskLimitBatchEncoder.NoRiskLimitsEncoder;
import com.trading.engine.messages.sbe.MessageHeaderEncoder;
import com.trading.refdata.ReferenceDataLoadException;
import com.trading.refdata.spi.ReferenceDataEncoder;
import java.util.List;
import org.agrona.MutableDirectBuffer;

/**
 * Encodes {@link RiskLimitRecord} instances into a {@code LoadRiskLimitBatch} SBE message
 * (templateId&nbsp;16).
 *
 * <p>Not thread-safe — reuses mutable SBE flyweight fields. Single-threaded use only.
 */
public final class RiskLimitCommandEncoder implements ReferenceDataEncoder<RiskLimitRecord> {

  private static final int MAX_BATCH_SIZE = 200;
  private static final String ENTITY_TYPE = "RiskLimit";

  private final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
  private final LoadRiskLimitBatchEncoder batchEncoder = new LoadRiskLimitBatchEncoder();

  /** {@inheritDoc} */
  @Override
  public int encodeBatch(
      final List<RiskLimitRecord> records,
      final int fromIndex,
      final int toIndex,
      final MutableDirectBuffer buffer,
      final int offset)
      throws ReferenceDataLoadException {

    final int count = toIndex - fromIndex;
    batchEncoder.wrapAndApplyHeader(buffer, offset, headerEncoder);
    // Cluster overwrites this field with its own deterministic timestamp
    batchEncoder.transactTime(0L);

    final NoRiskLimitsEncoder group = batchEncoder.noRiskLimitsCount(count);

    for (int i = fromIndex; i < toIndex; i++) {
      final var record = records.get(i);
      group
          .next()
          .accountId(record.accountId())
          .maxOrderSize(record.maxOrderSize())
          .maxOrderNotional(record.maxOrderNotional())
          .maxDailyVolume(record.maxDailyVolume())
          .maxDailyLossBps(record.maxDailyLossBps())
          .status(toStatus(record.status()));
    }

    return MessageHeaderEncoder.ENCODED_LENGTH + batchEncoder.encodedLength();
  }

  /** {@inheritDoc} */
  @Override
  public int templateId() {
    return LoadRiskLimitBatchEncoder.TEMPLATE_ID;
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
