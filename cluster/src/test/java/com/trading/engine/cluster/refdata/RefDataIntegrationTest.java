package com.trading.engine.cluster.refdata;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.trading.engine.messages.sbe.AccountLoadedEventDecoder;
import com.trading.engine.messages.sbe.AccountStatusEnum;
import com.trading.engine.messages.sbe.AccountTypeEnum;
import com.trading.engine.messages.sbe.AcctIDSourceEnum;
import com.trading.engine.messages.sbe.ComplianceStatusEnum;
import com.trading.engine.messages.sbe.CurrencyClassEnum;
import com.trading.engine.messages.sbe.CurrencyLoadedEventDecoder;
import com.trading.engine.messages.sbe.LoadAccountEncoder;
import com.trading.engine.messages.sbe.LoadCurrencyEncoder;
import com.trading.engine.messages.sbe.LoadRiskLimitEncoder;
import com.trading.engine.messages.sbe.MessageHeaderDecoder;
import com.trading.engine.messages.sbe.MessageHeaderEncoder;
import com.trading.engine.messages.sbe.RiskLimitLoadedEventDecoder;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

/**
 * End-to-end test exercising the full ref-data framework: a {@link ReferenceDataRegistry} with all
 * three stores (currency, account, risk-limit) wired through their loaders. Issues a realistic
 * sequence of commands (LoadCurrency → LoadAccount → LoadRiskLimit), verifies the resulting state,
 * snapshots, restores into a fresh registry, and verifies the restored state matches.
 */
class RefDataIntegrationTest {

  @Test
  void fullSequenceLoadAndSnapshotRoundTrip() {
    // ----- Build the source registry -----
    final CurrencyStore currencyStore = new CurrencyStore();
    final AccountStore accountStore = new AccountStore();
    final RiskLimitStore riskLimitStore = new RiskLimitStore();
    final ReferenceDataRegistry registry = new ReferenceDataRegistry();
    registry.registerStore(currencyStore);
    registry.registerStore(accountStore);
    registry.registerStore(riskLimitStore);
    registry.registerLoader(new LoadCurrencyHandler(currencyStore));
    registry.registerLoader(new LoadAccountHandler(accountStore, currencyStore));
    registry.registerLoader(new LoadRiskLimitHandler(riskLimitStore, accountStore));

    // ----- Step 1: load USD currency -----
    final MutableDirectBuffer src = new ExpandableArrayBuffer(512);
    final MutableDirectBuffer eventDst = new ExpandableArrayBuffer(512);
    int srcLength = encodeLoadCurrency(src, "USD", 840, "United States Dollar", 2);
    int eventLength = dispatch(registry, src, srcLength, eventDst, 1L, 1_000_000_000L);

    // Verify CurrencyLoadedEvent.
    final MessageHeaderDecoder header = new MessageHeaderDecoder();
    header.wrap(eventDst, 0);
    assertEquals(CurrencyLoadedEventDecoder.TEMPLATE_ID, header.templateId());
    assertEquals(1, currencyStore.size());

    // ----- Step 2: load ACME account with baseCurrency=USD (FK satisfied) -----
    srcLength = encodeLoadAccount(src, 100L, "ACME", "Acme Inc", "USD");
    eventLength = dispatch(registry, src, srcLength, eventDst, 2L, 2_000_000_000L);

    header.wrap(eventDst, 0);
    assertEquals(AccountLoadedEventDecoder.TEMPLATE_ID, header.templateId());
    assertEquals(1, accountStore.size());
    assertNotNull(accountStore.get(100L));

    // ----- Step 3: load risk limit for accountId 100 -----
    srcLength = encodeLoadRiskLimit(src, 100L, 1_000_00000000L, 0L, 10_000_00000000L, 50L);
    eventLength = dispatch(registry, src, srcLength, eventDst, 3L, 3_000_000_000L);

    header.wrap(eventDst, 0);
    assertEquals(RiskLimitLoadedEventDecoder.TEMPLATE_ID, header.templateId());
    assertEquals(1, riskLimitStore.size());
    assertNotNull(riskLimitStore.get(100L));

    // ----- Snapshot all stores -----
    final MutableDirectBuffer snapshotBuf = new ExpandableArrayBuffer(8192);
    final int snapshotBytes = registry.snapshotAll(snapshotBuf, 0);
    assertTrue(snapshotBytes > 0);

    // ----- Restore into a fresh registry -----
    final CurrencyStore restoredCurrency = new CurrencyStore();
    final AccountStore restoredAccount = new AccountStore();
    final RiskLimitStore restoredRiskLimit = new RiskLimitStore();
    final ReferenceDataRegistry restoredRegistry = new ReferenceDataRegistry();
    restoredRegistry.registerStore(restoredCurrency);
    restoredRegistry.registerStore(restoredAccount);
    restoredRegistry.registerStore(restoredRiskLimit);

    int offset = 0;
    while (offset < snapshotBytes) {
      header.wrap(snapshotBuf, offset);
      final int consumed = restoredRegistry.restoreFragment(header, snapshotBuf, offset);
      assertNotEquals(ReferenceDataRegistry.NOT_HANDLED, consumed);
      offset += consumed;
    }
    assertEquals(snapshotBytes, offset);

    // ----- Verify restored state matches -----
    assertEquals(1, restoredCurrency.size());
    assertEquals(1, restoredAccount.size());
    assertEquals(1, restoredRiskLimit.size());

    final CurrencyState usd =
        restoredCurrency.get(CurrencyStore.packCode((byte) 'U', (byte) 'S', (byte) 'D'));
    assertNotNull(usd);
    assertEquals(840, usd.isoNumeric());

    final AccountState acme = restoredAccount.get(100L);
    assertNotNull(acme);
    assertEquals(100L, acme.accountId());

    final UnsafeBuffer codeBuf = new UnsafeBuffer(new byte[] {'A', 'C', 'M', 'E'});
    final AccountState byCode = restoredAccount.getByCode(codeBuf, 0, 4);
    assertNotNull(byCode);
    assertEquals(100L, byCode.accountId());

    final RiskLimitState rl = restoredRiskLimit.get(100L);
    assertNotNull(rl);
    assertEquals(1_000_00000000L, rl.maxOrderSize());
    assertEquals(50L, rl.maxDailyLossBps());
  }

  @Test
  void resetAllAndReloadFromCommands() {
    final CurrencyStore currencyStore = new CurrencyStore();
    final AccountStore accountStore = new AccountStore();
    final ReferenceDataRegistry registry = new ReferenceDataRegistry();
    registry.registerStore(currencyStore);
    registry.registerStore(accountStore);
    registry.registerLoader(new LoadCurrencyHandler(currencyStore));
    registry.registerLoader(new LoadAccountHandler(accountStore, currencyStore));

    final MutableDirectBuffer src = new ExpandableArrayBuffer(512);
    final MutableDirectBuffer eventDst = new ExpandableArrayBuffer(512);

    dispatch(registry, src, encodeLoadCurrency(src, "USD", 840, "USD", 2), eventDst, 1L, 0L);
    dispatch(registry, src, encodeLoadAccount(src, 1L, "X", "Y", "USD"), eventDst, 2L, 0L);
    assertEquals(1, currencyStore.size());
    assertEquals(1, accountStore.size());

    registry.resetAll();
    assertEquals(0, currencyStore.size());
    assertEquals(0, accountStore.size());

    // Re-load.
    dispatch(registry, src, encodeLoadCurrency(src, "EUR", 978, "EUR", 2), eventDst, 3L, 0L);
    dispatch(registry, src, encodeLoadAccount(src, 2L, "Z", "Z", "EUR"), eventDst, 4L, 0L);
    assertEquals(1, currencyStore.size());
    assertEquals(1, accountStore.size());
    assertNotNull(accountStore.get(2L));
  }

  // ---------------------------------------------------------------------------
  // Encoders
  // ---------------------------------------------------------------------------

  private static int encodeLoadCurrency(
      final MutableDirectBuffer dst,
      final String code,
      final int isoNumeric,
      final String name,
      final int decimals) {
    final MessageHeaderEncoder header = new MessageHeaderEncoder();
    final LoadCurrencyEncoder enc = new LoadCurrencyEncoder();
    enc.wrapAndApplyHeader(dst, 0, header);
    enc.ccyCode(code);
    enc.isoNumeric(isoNumeric);
    enc.name(name);
    enc.decimals((short) decimals);
    enc.currencyClass(CurrencyClassEnum.Fiat);
    enc.status(AccountStatusEnum.Active);
    enc.transactTime(0L);
    return MessageHeaderEncoder.ENCODED_LENGTH + enc.encodedLength();
  }

  private static int encodeLoadAccount(
      final MutableDirectBuffer dst,
      final long accountId,
      final String code,
      final String name,
      final String baseCcy) {
    final MessageHeaderEncoder header = new MessageHeaderEncoder();
    final LoadAccountEncoder enc = new LoadAccountEncoder();
    enc.wrapAndApplyHeader(dst, 0, header);
    enc.accountId(accountId);
    enc.parentAccountId(0L);
    enc.accountCode(code);
    enc.acctIdSource(AcctIDSourceEnum.Internal);
    enc.accountName(name);
    enc.accountType(AccountTypeEnum.Client);
    enc.baseCurrency(baseCcy);
    enc.status(AccountStatusEnum.Active);
    enc.complianceStatus(ComplianceStatusEnum.OK);
    enc.capabilities(AccountState.Capabilities.CAN_TRADE | AccountState.Capabilities.CAN_RFQ);
    enc.transactTime(0L);
    return MessageHeaderEncoder.ENCODED_LENGTH + enc.encodedLength();
  }

  private static int encodeLoadRiskLimit(
      final MutableDirectBuffer dst,
      final long accountId,
      final long maxOrderSize,
      final long maxOrderNotional,
      final long maxDailyVolume,
      final long maxDailyLossBps) {
    final MessageHeaderEncoder header = new MessageHeaderEncoder();
    final LoadRiskLimitEncoder enc = new LoadRiskLimitEncoder();
    enc.wrapAndApplyHeader(dst, 0, header);
    enc.accountId(accountId);
    enc.maxOrderSize(maxOrderSize);
    enc.maxOrderNotional(maxOrderNotional);
    enc.maxDailyVolume(maxDailyVolume);
    enc.maxDailyLossBps(maxDailyLossBps);
    enc.status(AccountStatusEnum.Active);
    enc.transactTime(0L);
    return MessageHeaderEncoder.ENCODED_LENGTH + enc.encodedLength();
  }

  private static int dispatch(
      final ReferenceDataRegistry registry,
      final MutableDirectBuffer src,
      final int srcLength,
      final MutableDirectBuffer eventDst,
      final long sequenceNumber,
      final long clusterTimestampNanos) {
    final MessageHeaderDecoder header = new MessageHeaderDecoder();
    header.wrap(src, 0);
    return registry.dispatchCommand(
        header, src, 0, srcLength, eventDst, 0, sequenceNumber, clusterTimestampNanos);
  }
}
