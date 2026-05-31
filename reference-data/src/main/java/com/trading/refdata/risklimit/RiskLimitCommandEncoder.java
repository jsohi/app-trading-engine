package com.trading.refdata.risklimit;

import com.trading.engine.messages.sbe.AccountStatusEnum;
import com.trading.engine.messages.sbe.LoadRiskLimitBatchEncoder;
import com.trading.engine.messages.sbe.LoadRiskLimitBatchEncoder.NoRiskLimitsEncoder;
import com.trading.engine.messages.sbe.MessageHeaderEncoder;
import com.trading.refdata.ReferenceDataLoadException;
import com.trading.refdata.spi.ReferenceDataEncoder;
import java.nio.charset.StandardCharsets;
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

  /**
   * APP-62 §H — default proposerId / approverId populated for ops-loaded YAML fixtures so the
   * cluster's 4-eyes validation accepts the YAML-bootstrap load path. The byte buffers must be 16
   * bytes (SBE {@code Account} type) and not byte-equal.
   *
   * <p><b>Audit posture (KNOWN GAP):</b> these sentinels short-circuit the MiFID II RTS 6 §1(2)
   * intent of identifying the actual proposing and approving operators. Every YAML-loaded risk
   * limit will record {@code proposerId="OPS-LOADER"} / {@code approverId="OPS-APPROVER"} in {@link
   * com.trading.engine.messages.sbe.RiskLimitChangedEventEncoder RiskLimitChangedEvent} and in
   * {@code RiskLimitSnapshot}. This is acceptable ONLY for the pre-production / bootstrap path. A
   * real operator-console surface that supplies per-record proposerId/approverId from a session
   * principal is required before production deployment — tracked under APP-58 (AccountStore +
   * operator identity work).
   */
  private static final byte[] DEFAULT_PROPOSER_ID = paddedAscii("OPS-LOADER");

  private static final byte[] DEFAULT_APPROVER_ID = paddedAscii("OPS-APPROVER");

  private final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
  private final LoadRiskLimitBatchEncoder batchEncoder = new LoadRiskLimitBatchEncoder();

  // TODO(APP-58): surface real operator identity from session principal; remove these sentinels.
  private static byte[] paddedAscii(final String s) {
    final byte[] out = new byte[16];
    final byte[] src = s.getBytes(StandardCharsets.US_ASCII);
    System.arraycopy(src, 0, out, 0, Math.min(src.length, out.length));
    return out;
  }

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
          .putProposerId(DEFAULT_PROPOSER_ID, 0)
          .putApproverId(DEFAULT_APPROVER_ID, 0)
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
