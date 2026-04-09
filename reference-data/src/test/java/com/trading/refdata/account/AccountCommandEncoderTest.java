package com.trading.refdata.account;

import static org.junit.jupiter.api.Assertions.*;

import com.trading.engine.messages.sbe.AccountStatusEnum;
import com.trading.engine.messages.sbe.AccountTypeEnum;
import com.trading.engine.messages.sbe.AcctIDSourceEnum;
import com.trading.engine.messages.sbe.ComplianceStatusEnum;
import com.trading.engine.messages.sbe.LoadAccountBatchDecoder;
import com.trading.engine.messages.sbe.LoadAccountBatchDecoder.NoAccountsDecoder;
import com.trading.engine.messages.sbe.LoadAccountBatchEncoder;
import com.trading.engine.messages.sbe.MessageHeaderDecoder;
import com.trading.engine.messages.sbe.MessageHeaderEncoder;
import com.trading.refdata.ReferenceDataLoadException;
import java.util.List;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.junit.jupiter.api.Test;

final class AccountCommandEncoderTest {

  private final AccountCommandEncoder encoder = new AccountCommandEncoder();
  private final MutableDirectBuffer buffer = new ExpandableArrayBuffer(4096);
  private final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();
  private final LoadAccountBatchDecoder batchDecoder = new LoadAccountBatchDecoder();

  @Test
  void encodeBatchRoundTrip() throws Exception {
    final var records =
        List.of(
            new AccountRecord(
                1L,
                0L,
                "ACME-001",
                "Internal",
                "ACME Capital",
                "Client",
                "USD",
                "Active",
                "OK",
                3L),
            new AccountRecord(
                2L,
                1L,
                "HOUSE-001",
                "BIC",
                "House Trading",
                "House",
                "EUR",
                "Suspended",
                "PendingReview",
                1L));

    final int length = encoder.encodeBatch(records, 0, 2, buffer, 0);
    assertTrue(length > 0);

    headerDecoder.wrap(buffer, 0);
    assertEquals(LoadAccountBatchEncoder.TEMPLATE_ID, headerDecoder.templateId());

    batchDecoder.wrapAndApplyHeader(buffer, 0, headerDecoder);
    assertTrue(batchDecoder.transactTime() > 0);

    final NoAccountsDecoder group = batchDecoder.noAccounts();
    assertEquals(2, group.count());

    // First record
    group.next();
    assertEquals(1L, group.accountId());
    assertEquals(0L, group.parentAccountId());
    assertEquals("ACME-001", group.accountCode().trim());
    assertEquals(AcctIDSourceEnum.Internal, group.acctIdSource());
    assertEquals("ACME Capital", group.accountName().trim());
    assertEquals(AccountTypeEnum.Client, group.accountType());
    assertEquals("USD", group.baseCurrency().trim());
    assertEquals(AccountStatusEnum.Active, group.status());
    assertEquals(ComplianceStatusEnum.OK, group.complianceStatus());
    assertEquals(3L, group.capabilities());

    // Second record
    group.next();
    assertEquals(2L, group.accountId());
    assertEquals(1L, group.parentAccountId());
    assertEquals("HOUSE-001", group.accountCode().trim());
    assertEquals(AcctIDSourceEnum.BIC, group.acctIdSource());
    assertEquals("House Trading", group.accountName().trim());
    assertEquals(AccountTypeEnum.House, group.accountType());
    assertEquals("EUR", group.baseCurrency().trim());
    assertEquals(AccountStatusEnum.Suspended, group.status());
    assertEquals(ComplianceStatusEnum.PendingReview, group.complianceStatus());
    assertEquals(1L, group.capabilities());
  }

  @Test
  void encodeBatchSubRange() throws Exception {
    final var records =
        List.of(
            new AccountRecord(
                1L, 0L, "A1", "Internal", "First", "Client", "USD", "Active", "OK", 0L),
            new AccountRecord(
                2L, 0L, "A2", "Internal", "Second", "House", "GBP", "Active", "OK", 0L),
            new AccountRecord(
                3L, 0L, "A3", "Internal", "Third", "MarketMaker", "JPY", "Closed", "Blocked", 0L));

    final int length = encoder.encodeBatch(records, 1, 3, buffer, 0);
    assertTrue(length > 0);

    headerDecoder.wrap(buffer, 0);
    batchDecoder.wrapAndApplyHeader(buffer, 0, headerDecoder);

    final var group = batchDecoder.noAccounts();
    assertEquals(2, group.count());

    group.next();
    assertEquals(2L, group.accountId());
    assertEquals("A2", group.accountCode().trim());

    group.next();
    assertEquals(3L, group.accountId());
    assertEquals("A3", group.accountCode().trim());
    assertEquals(AccountTypeEnum.MarketMaker, group.accountType());
    assertEquals(AccountStatusEnum.Closed, group.status());
    assertEquals(ComplianceStatusEnum.Blocked, group.complianceStatus());
  }

  @Test
  void encodeInvalidAccountTypeThrows() {
    final var records =
        List.of(
            new AccountRecord(
                1L, 0L, "BAD", "Internal", "Bad Type", "InvalidType", "USD", "Active", "OK", 0L));

    final var ex =
        assertThrows(
            ReferenceDataLoadException.class, () -> encoder.encodeBatch(records, 0, 1, buffer, 0));
    assertTrue(ex.getMessage().contains("accountType"));
  }

  @Test
  void encodeInvalidStatusThrows() {
    final var records =
        List.of(
            new AccountRecord(
                1L, 0L, "BAD", "Internal", "Bad Status", "Client", "USD", "BadStatus", "OK", 0L));

    final var ex =
        assertThrows(
            ReferenceDataLoadException.class, () -> encoder.encodeBatch(records, 0, 1, buffer, 0));
    assertTrue(ex.getMessage().contains("status"));
  }

  @Test
  void encodeInvalidAcctIdSourceThrows() {
    final var records =
        List.of(
            new AccountRecord(
                1L,
                0L,
                "BAD",
                "UNKNOWN_SOURCE",
                "Bad Source",
                "Client",
                "USD",
                "Active",
                "OK",
                0L));

    final var ex =
        assertThrows(
            ReferenceDataLoadException.class, () -> encoder.encodeBatch(records, 0, 1, buffer, 0));
    assertTrue(ex.getMessage().contains("acctIdSource"));
  }

  @Test
  void templateIdMatchesSbeConstant() {
    assertEquals(LoadAccountBatchEncoder.TEMPLATE_ID, encoder.templateId());
  }

  @Test
  void maxBatchSizeIsPositive() {
    assertTrue(encoder.maxBatchSize() > 0);
  }

  @Test
  void encodedLengthMatchesReturnedValue() throws Exception {
    final var records =
        List.of(
            new AccountRecord(
                1L,
                0L,
                "LEN-TEST",
                "Internal",
                "Length Test",
                "Client",
                "USD",
                "Active",
                "OK",
                0L));

    final int length = encoder.encodeBatch(records, 0, 1, buffer, 0);

    // Verify: header + block + group header + (1 * group block)
    final int expected =
        MessageHeaderEncoder.ENCODED_LENGTH
            + LoadAccountBatchEncoder.BLOCK_LENGTH
            + 4 // group header (blockLength u16 + numInGroup u16)
            + LoadAccountBatchEncoder.NoAccountsEncoder.sbeBlockLength();
    assertEquals(expected, length);
  }
}
