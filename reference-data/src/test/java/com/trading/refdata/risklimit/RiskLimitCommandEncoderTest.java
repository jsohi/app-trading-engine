package com.trading.refdata.risklimit;

import static org.junit.jupiter.api.Assertions.*;

import com.trading.engine.messages.sbe.AccountStatusEnum;
import com.trading.engine.messages.sbe.LoadRiskLimitBatchDecoder;
import com.trading.engine.messages.sbe.LoadRiskLimitBatchDecoder.NoRiskLimitsDecoder;
import com.trading.engine.messages.sbe.LoadRiskLimitBatchEncoder;
import com.trading.engine.messages.sbe.MessageHeaderDecoder;
import com.trading.engine.messages.sbe.MessageHeaderEncoder;
import com.trading.refdata.ReferenceDataLoadException;
import java.util.List;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.junit.jupiter.api.Test;

/** Tests SBE encode/decode round-trip and error handling for {@link RiskLimitCommandEncoder}. */
final class RiskLimitCommandEncoderTest {

  private final RiskLimitCommandEncoder encoder = new RiskLimitCommandEncoder();
  private final MutableDirectBuffer buffer = new ExpandableArrayBuffer(4096);
  private final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();
  private final LoadRiskLimitBatchDecoder batchDecoder = new LoadRiskLimitBatchDecoder();

  @Test
  void encodeBatchRoundTrip() throws Exception {
    final var records =
        List.of(
            new RiskLimitRecord(1L, 1_000_000_000L, 500_000_000L, 10_000_000_000L, 50L, "Active"),
            new RiskLimitRecord(2L, 0L, 0L, 0L, 100L, "Suspended"));

    final int length = encoder.encodeBatch(records, 0, 2, buffer, 0);
    assertTrue(length > 0);

    headerDecoder.wrap(buffer, 0);
    assertEquals(LoadRiskLimitBatchEncoder.TEMPLATE_ID, headerDecoder.templateId());

    batchDecoder.wrapAndApplyHeader(buffer, 0, headerDecoder);
    assertEquals(0L, batchDecoder.transactTime());

    final NoRiskLimitsDecoder group = batchDecoder.noRiskLimits();
    assertEquals(2, group.count());

    // First record
    group.next();
    assertEquals(1L, group.accountId());
    assertEquals(1_000_000_000L, group.maxOrderSize());
    assertEquals(500_000_000L, group.maxOrderNotional());
    assertEquals(10_000_000_000L, group.maxDailyVolume());
    assertEquals(50L, group.maxDailyLossBps());
    assertEquals(AccountStatusEnum.Active, group.status());

    // Second record
    group.next();
    assertEquals(2L, group.accountId());
    assertEquals(0L, group.maxOrderSize());
    assertEquals(0L, group.maxOrderNotional());
    assertEquals(0L, group.maxDailyVolume());
    assertEquals(100L, group.maxDailyLossBps());
    assertEquals(AccountStatusEnum.Suspended, group.status());
  }

  @Test
  void encodeBatchSubRange() throws Exception {
    final var records =
        List.of(
            new RiskLimitRecord(1L, 100L, 0L, 0L, 10L, "Active"),
            new RiskLimitRecord(2L, 200L, 0L, 0L, 20L, "Active"),
            new RiskLimitRecord(3L, 300L, 0L, 0L, 30L, "Closed"));

    final int length = encoder.encodeBatch(records, 1, 3, buffer, 0);
    assertTrue(length > 0);

    headerDecoder.wrap(buffer, 0);
    batchDecoder.wrapAndApplyHeader(buffer, 0, headerDecoder);

    final var group = batchDecoder.noRiskLimits();
    assertEquals(2, group.count());

    group.next();
    assertEquals(2L, group.accountId());
    assertEquals(200L, group.maxOrderSize());

    group.next();
    assertEquals(3L, group.accountId());
    assertEquals(AccountStatusEnum.Closed, group.status());
  }

  @Test
  void encodeInvalidStatusThrows() {
    final var records = List.of(new RiskLimitRecord(1L, 0L, 0L, 0L, 0L, "BadStatus"));

    final var ex =
        assertThrows(
            ReferenceDataLoadException.class, () -> encoder.encodeBatch(records, 0, 1, buffer, 0));
    assertTrue(ex.getMessage().contains("status"));
  }

  @Test
  void templateIdMatchesSbeConstant() {
    assertEquals(LoadRiskLimitBatchEncoder.TEMPLATE_ID, encoder.templateId());
  }

  @Test
  void maxBatchSizeIsPositive() {
    assertTrue(encoder.maxBatchSize() > 0);
  }

  @Test
  void encodedLengthMatchesReturnedValue() throws Exception {
    final var records = List.of(new RiskLimitRecord(1L, 100L, 0L, 0L, 50L, "Active"));

    final int length = encoder.encodeBatch(records, 0, 1, buffer, 0);

    // Verify: header + block + group header + (1 * group block)
    final int expected =
        MessageHeaderEncoder.ENCODED_LENGTH
            + LoadRiskLimitBatchEncoder.BLOCK_LENGTH
            + 4 // group header (blockLength u16 + numInGroup u16)
            + LoadRiskLimitBatchEncoder.NoRiskLimitsEncoder.sbeBlockLength();
    assertEquals(expected, length);
  }
}
