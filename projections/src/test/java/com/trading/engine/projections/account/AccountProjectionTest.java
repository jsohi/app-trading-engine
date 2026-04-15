package com.trading.engine.projections.account;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.trading.engine.messages.sbe.AccountLoadRejectedEventDecoder;
import com.trading.engine.messages.sbe.AccountLoadedEventDecoder;
import com.trading.engine.messages.sbe.AccountLoadedEventEncoder;
import com.trading.engine.messages.sbe.AccountStatusEnum;
import com.trading.engine.messages.sbe.AccountTypeEnum;
import com.trading.engine.messages.sbe.AcctIDSourceEnum;
import com.trading.engine.messages.sbe.ComplianceStatusEnum;
import com.trading.engine.messages.sbe.MessageHeaderEncoder;
import com.trading.engine.messages.sbe.RejectReasonEnum;
import com.trading.engine.testsupport.sbe.SbeTestEncoder;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link AccountProjection} covering account load, upsert (same code, changed code,
 * shorter/longer code, hijack-and-reclaim), rejection handling, collection queries, reset/replay
 * determinism, capability bitfield decoding, enum round-tripping, boundary conditions (max-length
 * code, empty code, accountId zero, null/overlength query input), concurrency under StampedLock,
 * and batch load stress.
 */
class AccountProjectionTest {

  private static final int HDR_LEN = MessageHeaderEncoder.ENCODED_LENGTH;

  private AccountProjection projection;
  private MutableDirectBuffer buf;
  private long seqNo;
  private long timestamp;

  @BeforeEach
  void setUp() {
    projection = new AccountProjection(64);
    buf = new ExpandableArrayBuffer(512);
    seqNo = 0;
    timestamp = 1_000_000_000L;
  }

  // ---------------------------------------------------------------------------
  // Encoding helpers
  // ---------------------------------------------------------------------------

  private int encodeAccountLoaded(
      final long accountId,
      final long parentAccountId,
      final String accountCode,
      final AcctIDSourceEnum acctIdSource,
      final String accountName,
      final AccountTypeEnum accountType,
      final String baseCurrency,
      final AccountStatusEnum status,
      final ComplianceStatusEnum complianceStatus,
      final long capabilities,
      final long transactTime) {
    final var hdr = new MessageHeaderEncoder();
    final var enc = new AccountLoadedEventEncoder();
    enc.wrapAndApplyHeader(buf, 0, hdr);
    enc.sequenceNumber(++seqNo);
    enc.timestamp(timestamp++);
    enc.accountId(accountId);
    enc.parentAccountId(parentAccountId);
    enc.accountCode(accountCode);
    enc.acctIdSource(acctIdSource);
    enc.accountName(accountName);
    enc.accountType(accountType);
    enc.baseCurrency(baseCurrency);
    enc.status(status);
    enc.complianceStatus(complianceStatus);
    enc.capabilities(capabilities);
    enc.transactTime(transactTime);
    return HDR_LEN + enc.encodedLength();
  }

  private int encodeAccountLoadRejected(
      final String accountCode, final RejectReasonEnum rejectReason, final String text) {
    return SbeTestEncoder.encodeAccountLoadRejectedEvent(
        buf, 0, ++seqNo, timestamp++, accountCode, rejectReason, text);
  }

  private void dispatch(final int templateId, final int totalLen) {
    // EventConsumer strips the header — pass payload only
    projection.onEvent(seqNo, templateId, buf, HDR_LEN, totalLen - HDR_LEN);
  }

  /** Convenience: encode + dispatch an AccountLoadedEvent. */
  private void loadAccount(
      final long accountId,
      final String accountCode,
      final AccountStatusEnum status,
      final long capabilities) {
    final int len =
        encodeAccountLoaded(
            accountId,
            0L,
            accountCode,
            AcctIDSourceEnum.Internal,
            "Account " + accountCode,
            AccountTypeEnum.Client,
            "USD",
            status,
            ComplianceStatusEnum.OK,
            capabilities,
            900_000_000L);
    dispatch(AccountLoadedEventDecoder.TEMPLATE_ID, len);
  }

  // ---------------------------------------------------------------------------
  // Core load + query path
  // ---------------------------------------------------------------------------

  @Test
  void accountLoadedPopulatesAllFields() {
    final long txTime = 500_000_000L;
    final int len =
        encodeAccountLoaded(
            42L,
            7L,
            "ACME-001",
            AcctIDSourceEnum.BIC,
            "ACME Corp Trading",
            AccountTypeEnum.MarketMaker,
            "EUR",
            AccountStatusEnum.Active,
            ComplianceStatusEnum.PendingReview,
            3L, // CAN_TRADE | CAN_RFQ
            txTime);
    dispatch(AccountLoadedEventDecoder.TEMPLATE_ID, len);

    final AccountReadModel snap = projection.getByAccountId(42L);
    assertNotNull(snap);
    assertEquals(42L, snap.accountId());
    assertEquals(7L, snap.parentAccountId());
    assertEquals("ACME-001", snap.accountCode());
    assertEquals(AcctIDSourceEnum.BIC, snap.acctIdSource());
    assertEquals("ACME Corp Trading", snap.accountName());
    assertEquals(AccountTypeEnum.MarketMaker, snap.accountType());
    assertEquals("EUR", snap.baseCurrency());
    assertEquals(AccountStatusEnum.Active, snap.status());
    assertEquals(ComplianceStatusEnum.PendingReview, snap.complianceStatus());
    assertEquals(3L, snap.capabilities());
    assertTrue(snap.canTrade());
    assertTrue(snap.canRequestQuotes());
    assertEquals(txTime, snap.transactTime());
    assertEquals(1L, snap.sequenceNumber());
    assertTrue(snap.lastUpdatedAt() > 0);
  }

  @Test
  void accountLoadedQueryByCode() {
    loadAccount(1L, "ACME", AccountStatusEnum.Active, 1L);

    final AccountReadModel snap = projection.getByAccountCode("ACME");
    assertNotNull(snap);
    assertEquals(1L, snap.accountId());
    assertEquals("ACME", snap.accountCode());
  }

  @Test
  void getByAccountIdReturnsNullForUnknownId() {
    assertNull(projection.getByAccountId(99999L));
  }

  @Test
  void getByAccountCodeReturnsNullForUnknownCode() {
    assertNull(projection.getByAccountCode("NONEXISTENT"));
  }

  @Test
  void getByAccountCodeReturnsNullForNullInput() {
    assertNull(projection.getByAccountCode(null));
  }

  @Test
  void getByAccountCodeReturnsNullForOverlengthCode() {
    assertNull(projection.getByAccountCode("12345678901234567")); // 17 chars > max 16
  }

  // ---------------------------------------------------------------------------
  // Upsert paths
  // ---------------------------------------------------------------------------

  @Test
  void upsertSameCodeUpdatesFields() {
    loadAccount(1L, "ACME", AccountStatusEnum.Active, 1L);
    assertEquals(AccountStatusEnum.Active, projection.getByAccountId(1L).status());

    loadAccount(1L, "ACME", AccountStatusEnum.Suspended, 0L);
    assertEquals(1, projection.size());
    final AccountReadModel snap = projection.getByAccountId(1L);
    assertEquals(AccountStatusEnum.Suspended, snap.status());
    assertFalse(snap.canTrade());

    // Secondary index still works with same code
    assertNotNull(projection.getByAccountCode("ACME"));
    assertEquals(1L, projection.getByAccountCode("ACME").accountId());
  }

  @Test
  void upsertChangedCodeUpdatesSecondaryIndex() {
    loadAccount(1L, "ACME-001", AccountStatusEnum.Active, 1L);
    assertNotNull(projection.getByAccountCode("ACME-001"));

    loadAccount(1L, "ACME-002", AccountStatusEnum.Active, 1L);
    assertEquals(1, projection.size());
    assertNull(projection.getByAccountCode("ACME-001"));
    assertNotNull(projection.getByAccountCode("ACME-002"));
    assertEquals(1L, projection.getByAccountCode("ACME-002").accountId());
  }

  @Test
  void upsertShorterCodeZerosTrailingBytes() {
    loadAccount(1L, "LONGCODE123456", AccountStatusEnum.Active, 1L);
    assertNotNull(projection.getByAccountCode("LONGCODE123456"));

    loadAccount(1L, "ACM", AccountStatusEnum.Active, 1L);
    assertEquals(1, projection.size());
    assertNull(projection.getByAccountCode("LONGCODE123456"));
    assertNotNull(projection.getByAccountCode("ACM"));
    assertEquals("ACM", projection.getByAccountCode("ACM").accountCode());
  }

  @Test
  void upsertLongerCodeHandledCorrectly() {
    loadAccount(1L, "ACM", AccountStatusEnum.Active, 1L);
    assertNotNull(projection.getByAccountCode("ACM"));

    loadAccount(1L, "ACME-LONGCODE1", AccountStatusEnum.Active, 1L);
    assertEquals(1, projection.size());
    assertNull(projection.getByAccountCode("ACM"));
    assertNotNull(projection.getByAccountCode("ACME-LONGCODE1"));
  }

  @Test
  void upsertReclaimsSecondaryIndexAfterCodeHijackedAndReleased() {
    // A owns "CODE-X"
    loadAccount(1L, "CODE-X", AccountStatusEnum.Active, 1L);
    assertEquals(1L, projection.getByAccountCode("CODE-X").accountId());

    // B hijacks "CODE-X" (last-write-wins)
    loadAccount(2L, "CODE-X", AccountStatusEnum.Active, 1L);
    assertEquals(2L, projection.getByAccountCode("CODE-X").accountId());

    // B changes to "CODE-Y", releasing "CODE-X" from secondary index
    loadAccount(2L, "CODE-Y", AccountStatusEnum.Active, 1L);
    assertNull(projection.getByAccountCode("CODE-X"));
    assertNotNull(projection.getByAccountCode("CODE-Y"));

    // A upserts with same code "CODE-X" — should reclaim secondary index
    loadAccount(1L, "CODE-X", AccountStatusEnum.Active, 1L);
    assertNotNull(projection.getByAccountCode("CODE-X"));
    assertEquals(1L, projection.getByAccountCode("CODE-X").accountId());
  }

  // ---------------------------------------------------------------------------
  // Rejection path
  // ---------------------------------------------------------------------------

  @Test
  void accountLoadRejectedIncrementsRejectCount() {
    final int len =
        encodeAccountLoadRejected("ACME", RejectReasonEnum.DuplicateAccountCode, "dup code");
    dispatch(AccountLoadRejectedEventDecoder.TEMPLATE_ID, len);

    assertEquals(1, projection.rejectCount());
    assertEquals(0, projection.size());
  }

  @Test
  void accountLoadRejectedDoesNotCreateView() {
    final int len = encodeAccountLoadRejected("ACME", RejectReasonEnum.InvalidAccountId, "bad id");
    dispatch(AccountLoadRejectedEventDecoder.TEMPLATE_ID, len);

    assertNull(projection.getByAccountCode("ACME"));
    assertNull(projection.getByAccountId(0L));
  }

  @Test
  void accountLoadRejectedIncrementsEventsProcessed() {
    final int len = encodeAccountLoadRejected("ACME", RejectReasonEnum.DuplicateAccountCode, "dup");
    dispatch(AccountLoadRejectedEventDecoder.TEMPLATE_ID, len);

    assertEquals(1, projection.eventsProcessed());
  }

  @Test
  void multipleRejectionsAccumulateCount() {
    for (int i = 0; i < 3; i++) {
      final int len =
          encodeAccountLoadRejected("ACME-" + i, RejectReasonEnum.InvalidAccountId, "bad");
      dispatch(AccountLoadRejectedEventDecoder.TEMPLATE_ID, len);
    }
    assertEquals(3, projection.rejectCount());
  }

  // ---------------------------------------------------------------------------
  // Collection queries
  // ---------------------------------------------------------------------------

  @Test
  void getAllReturnsAllLoadedAccounts() {
    loadAccount(1L, "ACME-1", AccountStatusEnum.Active, 1L);
    loadAccount(2L, "ACME-2", AccountStatusEnum.Active, 1L);
    loadAccount(3L, "ACME-3", AccountStatusEnum.Suspended, 0L);

    final List<AccountReadModel> all = projection.getAll();
    assertEquals(3, all.size());
  }

  @Test
  void getActiveAccountsFiltersNonActive() {
    loadAccount(1L, "ACTIVE", AccountStatusEnum.Active, 1L);
    loadAccount(2L, "SUSPENDED", AccountStatusEnum.Suspended, 0L);
    loadAccount(3L, "CLOSED", AccountStatusEnum.Closed, 0L);

    final List<AccountReadModel> active = projection.getActiveAccounts();
    assertEquals(1, active.size());
    assertEquals("ACTIVE", active.getFirst().accountCode());
  }

  // ---------------------------------------------------------------------------
  // Reset + replay
  // ---------------------------------------------------------------------------

  @Test
  void resetClearsAllStateAndCounters() {
    loadAccount(1L, "ACME", AccountStatusEnum.Active, 1L);
    final int rejLen = encodeAccountLoadRejected("BAD", RejectReasonEnum.InvalidAccountId, "bad");
    dispatch(AccountLoadRejectedEventDecoder.TEMPLATE_ID, rejLen);

    projection.reset();

    assertEquals(0, projection.size());
    assertEquals(0, projection.rejectCount());
    assertEquals(0, projection.lastProcessedSequence());
    assertEquals(0, projection.eventsProcessed());
    assertEquals(0, projection.errorCount());
    assertNull(projection.getByAccountCode("ACME"));
  }

  @Test
  void replayAfterResetProducesSameState() {
    // First pass: load 5 accounts with varied properties
    final int[][] params = {{1, 0}, {2, 1}, {3, 2}, {4, 0}, {5, 1}};
    final AccountStatusEnum[] statuses = {
      AccountStatusEnum.Active,
      AccountStatusEnum.Suspended,
      AccountStatusEnum.Active,
      AccountStatusEnum.Closed,
      AccountStatusEnum.Active
    };
    final long[] caps = {3L, 0L, 1L, 0L, 2L};

    for (int i = 0; i < 5; i++) {
      loadAccount(params[i][0], "ACC-" + i, statuses[i], caps[i]);
    }

    // Record snapshots
    final List<AccountReadModel> firstPass = projection.getAll();
    assertEquals(5, firstPass.size());

    // Reset
    projection.reset();
    assertEquals(0, projection.size());

    // Replay same events (reset seqNo and timestamp to replay from scratch)
    seqNo = 0;
    timestamp = 1_000_000_000L;
    for (int i = 0; i < 5; i++) {
      loadAccount(params[i][0], "ACC-" + i, statuses[i], caps[i]);
    }

    // Verify identical state
    final List<AccountReadModel> secondPass = projection.getAll();
    assertEquals(firstPass.size(), secondPass.size());

    for (final AccountReadModel expected : firstPass) {
      final AccountReadModel actual = projection.getByAccountId(expected.accountId());
      assertNotNull(actual);
      assertEquals(expected.accountCode(), actual.accountCode());
      assertEquals(expected.status(), actual.status());
      assertEquals(expected.capabilities(), actual.capabilities());
      assertEquals(expected.canTrade(), actual.canTrade());
      assertEquals(expected.canRequestQuotes(), actual.canRequestQuotes());
    }
  }

  // ---------------------------------------------------------------------------
  // Capabilities and enums
  // ---------------------------------------------------------------------------

  @Test
  void canTradeAndCanRequestQuotesReflectCapabilities() {
    // Both capabilities
    loadAccount(1L, "BOTH", AccountStatusEnum.Active, 3L);
    final AccountReadModel both = projection.getByAccountId(1L);
    assertTrue(both.canTrade());
    assertTrue(both.canRequestQuotes());

    // No capabilities
    loadAccount(2L, "NONE", AccountStatusEnum.Active, 0L);
    final AccountReadModel none = projection.getByAccountId(2L);
    assertFalse(none.canTrade());
    assertFalse(none.canRequestQuotes());

    // Trade only
    loadAccount(3L, "TRADE", AccountStatusEnum.Active, 1L);
    final AccountReadModel tradeOnly = projection.getByAccountId(3L);
    assertTrue(tradeOnly.canTrade());
    assertFalse(tradeOnly.canRequestQuotes());

    // RFQ only
    loadAccount(4L, "RFQ", AccountStatusEnum.Active, 2L);
    final AccountReadModel rfqOnly = projection.getByAccountId(4L);
    assertFalse(rfqOnly.canTrade());
    assertTrue(rfqOnly.canRequestQuotes());
  }

  @Test
  void complianceStatusAllValuesPreserved() {
    long id = 1L;
    for (final ComplianceStatusEnum cs : ComplianceStatusEnum.values()) {
      if (cs == ComplianceStatusEnum.NULL_VAL) {
        continue;
      }
      final int len =
          encodeAccountLoaded(
              id,
              0L,
              "CS-" + id,
              AcctIDSourceEnum.Internal,
              "Test",
              AccountTypeEnum.Client,
              "USD",
              AccountStatusEnum.Active,
              cs,
              1L,
              100L);
      dispatch(AccountLoadedEventDecoder.TEMPLATE_ID, len);
      assertEquals(cs, projection.getByAccountId(id).complianceStatus());
      id++;
    }
  }

  @Test
  void accountTypeAllValuesPreserved() {
    long id = 1L;
    for (final AccountTypeEnum at : AccountTypeEnum.values()) {
      if (at == AccountTypeEnum.NULL_VAL) {
        continue;
      }
      final int len =
          encodeAccountLoaded(
              id,
              0L,
              "AT-" + id,
              AcctIDSourceEnum.Internal,
              "Test",
              at,
              "USD",
              AccountStatusEnum.Active,
              ComplianceStatusEnum.OK,
              1L,
              100L);
      dispatch(AccountLoadedEventDecoder.TEMPLATE_ID, len);
      assertEquals(at, projection.getByAccountId(id).accountType());
      id++;
    }
  }

  @Test
  void acctIdSourceAllValuesPreserved() {
    long id = 1L;
    for (final AcctIDSourceEnum ais : AcctIDSourceEnum.values()) {
      if (ais == AcctIDSourceEnum.NULL_VAL) {
        continue;
      }
      final int len =
          encodeAccountLoaded(
              id,
              0L,
              "AIS-" + id,
              ais,
              "Test",
              AccountTypeEnum.Client,
              "USD",
              AccountStatusEnum.Active,
              ComplianceStatusEnum.OK,
              1L,
              100L);
      dispatch(AccountLoadedEventDecoder.TEMPLATE_ID, len);
      assertEquals(ais, projection.getByAccountId(id).acctIdSource());
      id++;
    }
  }

  @Test
  void parentAccountIdPreserved() {
    final int len =
        encodeAccountLoaded(
            1L,
            42L,
            "CHILD",
            AcctIDSourceEnum.Internal,
            "Child Account",
            AccountTypeEnum.Client,
            "USD",
            AccountStatusEnum.Active,
            ComplianceStatusEnum.OK,
            1L,
            100L);
    dispatch(AccountLoadedEventDecoder.TEMPLATE_ID, len);

    assertEquals(42L, projection.getByAccountId(1L).parentAccountId());
  }

  // ---------------------------------------------------------------------------
  // Sequence tracking and error paths
  // ---------------------------------------------------------------------------

  @Test
  void unknownEventTypeIgnored() {
    // Dispatch with a bogus template ID
    loadAccount(1L, "ACME", AccountStatusEnum.Active, 1L);
    final long processedBefore = projection.eventsProcessed();
    final long seqBefore = projection.lastProcessedSequence();

    // Fabricate a dispatch with unknown template
    projection.onEvent(seqNo + 100, 999, buf, HDR_LEN, 10);

    assertEquals(processedBefore, projection.eventsProcessed()); // NOT incremented
    assertEquals(0, projection.errorCount());
    assertEquals(seqNo + 100, projection.lastProcessedSequence()); // Advances
  }

  @Test
  void lastProcessedSequenceUpdatedOnEveryEvent() {
    loadAccount(1L, "A", AccountStatusEnum.Active, 1L);
    loadAccount(2L, "B", AccountStatusEnum.Active, 1L);
    loadAccount(3L, "C", AccountStatusEnum.Active, 1L);

    assertEquals(seqNo, projection.lastProcessedSequence());
    assertEquals(3, seqNo);
  }

  @Test
  void lastProcessedSequenceUpdatedEvenOnError() {
    // Encode a valid header but truncated payload (4 bytes < BLOCK_LENGTH=135)
    final var hdr = new MessageHeaderEncoder();
    final var enc = new AccountLoadedEventEncoder();
    enc.wrapAndApplyHeader(buf, 0, hdr);
    // Dispatch with truncated length
    projection.onEvent(42L, AccountLoadedEventDecoder.TEMPLATE_ID, buf, HDR_LEN, 4);

    assertEquals(1, projection.errorCount());
    assertEquals(42L, projection.lastProcessedSequence());
  }

  @Test
  void eventsProcessedIncrementedCorrectly() {
    // 3 loaded events
    loadAccount(1L, "A1", AccountStatusEnum.Active, 1L);
    loadAccount(2L, "A2", AccountStatusEnum.Active, 1L);
    loadAccount(3L, "A3", AccountStatusEnum.Active, 1L);
    // 2 rejection events
    int len = encodeAccountLoadRejected("BAD1", RejectReasonEnum.InvalidAccountId, "bad");
    dispatch(AccountLoadRejectedEventDecoder.TEMPLATE_ID, len);
    len = encodeAccountLoadRejected("BAD2", RejectReasonEnum.DuplicateAccountCode, "dup");
    dispatch(AccountLoadRejectedEventDecoder.TEMPLATE_ID, len);

    assertEquals(5, projection.eventsProcessed());
  }

  // ---------------------------------------------------------------------------
  // Boundary conditions
  // ---------------------------------------------------------------------------

  @Test
  void maxLengthAccountCodeHandledCorrectly() {
    final String maxCode = "1234567890ABCDEF"; // exactly 16 chars
    loadAccount(1L, maxCode, AccountStatusEnum.Active, 1L);

    final AccountReadModel snap = projection.getByAccountCode(maxCode);
    assertNotNull(snap);
    assertEquals(maxCode, snap.accountCode());
    assertEquals(1L, snap.accountId());
  }

  @Test
  void emptyAccountCodeSkippedAsError() {
    // Encode with all-NUL account code by using empty string
    final int len =
        encodeAccountLoaded(
            1L,
            0L,
            "",
            AcctIDSourceEnum.Internal,
            "No Code",
            AccountTypeEnum.Client,
            "USD",
            AccountStatusEnum.Active,
            ComplianceStatusEnum.OK,
            1L,
            100L);
    dispatch(AccountLoadedEventDecoder.TEMPLATE_ID, len);

    assertEquals(1, projection.errorCount());
    assertEquals(0, projection.eventsProcessed()); // NOT counted as processed — guard throws
    assertEquals(0, projection.size());
    assertNull(projection.getByAccountId(1L));
  }

  @Test
  void accountIdZeroIsValidKey() {
    loadAccount(0L, "ZERO-ID", AccountStatusEnum.Active, 1L);

    final AccountReadModel snap = projection.getByAccountId(0L);
    assertNotNull(snap);
    assertEquals("ZERO-ID", snap.accountCode());
  }

  // ---------------------------------------------------------------------------
  // Concurrency and stress
  // ---------------------------------------------------------------------------

  @Test
  void concurrentReadsAndWritesNoRace() throws Exception {
    final int numAccounts = 1000;
    final CountDownLatch startLatch = new CountDownLatch(1);
    final AtomicBoolean writerDone = new AtomicBoolean(false);
    final AtomicBoolean writerFailed = new AtomicBoolean(false);
    final AtomicBoolean readerFailed = new AtomicBoolean(false);

    // Writer thread
    final Thread writer =
        new Thread(
            () -> {
              try {
                startLatch.await();
                for (int i = 0; i < numAccounts; i++) {
                  loadAccount(i, "CODE-" + i, AccountStatusEnum.Active, 1L);
                }
              } catch (final Exception e) {
                writerFailed.set(true);
              } finally {
                writerDone.set(true);
              }
            });

    // Reader thread
    final Thread reader =
        new Thread(
            () -> {
              try {
                startLatch.await();
                while (!writerDone.get()) {
                  for (int i = 0; i < 100; i++) {
                    final int idx = i % numAccounts;
                    final AccountReadModel snap = projection.getByAccountCode("CODE-" + idx);
                    if (snap != null) {
                      // Every returned snapshot must be self-consistent
                      if (!snap.accountCode().equals("CODE-" + idx)) {
                        readerFailed.set(true);
                        return;
                      }
                      if (snap.accountId() < 0 || snap.accountId() >= numAccounts) {
                        readerFailed.set(true);
                        return;
                      }
                    }
                  }
                }
              } catch (final Exception e) {
                readerFailed.set(true);
              }
            });

    writer.start();
    reader.start();
    startLatch.countDown();
    writer.join(10_000);
    reader.join(10_000);

    assertFalse(writerFailed.get(), "Writer thread threw exception");
    assertFalse(readerFailed.get(), "Reader observed inconsistent data or threw exception");
    assertEquals(numAccounts, projection.size());
  }

  @Test
  void batchLoadReplayMaintainsIndexConsistency() {
    final int batchSize = 500;
    for (int i = 0; i < batchSize; i++) {
      loadAccount(i, "BATCH-" + i, AccountStatusEnum.Active, 1L);
    }

    assertEquals(batchSize, projection.size());
    assertEquals(batchSize, projection.getAll().size());

    // Deterministic spot-checks at specific indices
    final int[] checkpoints = {0, 49, 99, 249, 499};
    for (final int idx : checkpoints) {
      final AccountReadModel byId = projection.getByAccountId(idx);
      assertNotNull(byId, "Missing account by id at index " + idx);
      assertEquals("BATCH-" + idx, byId.accountCode());

      final AccountReadModel byCode = projection.getByAccountCode("BATCH-" + idx);
      assertNotNull(byCode, "Missing account by code at index " + idx);
      assertEquals(idx, byCode.accountId());
    }
  }

  @Test
  void duplicateAccountCodeAcrossDifferentIdsLastWriteWins() {
    // Two different accountIds loaded with the same accountCode.
    // The secondary index (byAccountCode) maps to the last-loaded account.
    // Both accounts are retained in the primary index (byAccountId).
    loadAccount(1L, "SHARED", AccountStatusEnum.Active, 1L);
    loadAccount(2L, "SHARED", AccountStatusEnum.Active, 3L);

    assertEquals(2, projection.size());
    assertNotNull(projection.getByAccountId(1L));
    assertNotNull(projection.getByAccountId(2L));

    // Code index points to last-loaded (accountId=2)
    final AccountReadModel byCode = projection.getByAccountCode("SHARED");
    assertNotNull(byCode);
    assertEquals(2L, byCode.accountId());
  }
}
