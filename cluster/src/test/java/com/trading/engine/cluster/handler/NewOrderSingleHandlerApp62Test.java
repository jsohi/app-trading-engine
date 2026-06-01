package com.trading.engine.cluster.handler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.trading.engine.cluster.IdGenerator;
import com.trading.engine.cluster.OrderBook;
import com.trading.engine.cluster.TradingClusteredServiceFactory;
import com.trading.engine.cluster.journal.EventJournal;
import com.trading.engine.cluster.metrics.RfqMetrics;
import com.trading.engine.cluster.refdata.AccountFixtures;
import com.trading.engine.cluster.refdata.AccountState;
import com.trading.engine.cluster.refdata.AccountStore;
import com.trading.engine.cluster.refdata.CurrencyFixtures;
import com.trading.engine.cluster.refdata.CurrencyStore;
import com.trading.engine.cluster.refdata.ReferenceDataSeeder;
import com.trading.engine.cluster.refdata.RiskLimitFixtures;
import com.trading.engine.cluster.refdata.RiskLimitState;
import com.trading.engine.cluster.refdata.RiskLimitStore;
import com.trading.engine.cluster.refdata.SymbolEligibilityState;
import com.trading.engine.cluster.refdata.SymbolEligibilityStore;
import com.trading.engine.cluster.sequencer.EventSequencer;
import com.trading.engine.cluster.state.RfqStateMachine;
import com.trading.engine.cluster.state.TradingState;
import com.trading.engine.messages.FixedPointScale;
import com.trading.engine.messages.sbe.AccountStatusEnum;
import com.trading.engine.messages.sbe.MessageHeaderDecoder;
import com.trading.engine.messages.sbe.NewOrderSingleDecoder;
import com.trading.engine.messages.sbe.OrdTypeEnum;
import com.trading.engine.messages.sbe.OrderRejectedEventDecoder;
import com.trading.engine.messages.sbe.RejectReasonEnum;
import com.trading.engine.messages.sbe.RiskCheckEnum;
import com.trading.engine.messages.sbe.SideEnum;
import com.trading.engine.testsupport.aeron.FakeClientSession;
import com.trading.engine.testsupport.aeron.FakeCluster;
import com.trading.engine.testsupport.sbe.SbeTestEncoder;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * APP-62 §I (per-symbol fat-finger override) + §B (per-account idle-session timeout) unit tests for
 * {@link NewOrderSingleHandler}.
 *
 * <p><b>§I tests.</b> Verify that {@code
 * symbolEligibilityStore.get(symbol).priceDeviationBpsOverride()} overrides the account-wide {@code
 * riskLimit.priceDeviationBps()} when {@code > 0}, and falls back to the account knob when the
 * override is exactly {@code 0L} (sentinel).
 *
 * <p><b>§B tests.</b> Verify that {@code riskLimitStore.get(accountId).idleSessionTimeoutNanos()}
 * overrides the system-wide default supplied to {@link NewOrderSingleHandler#onIdleScan} when
 * {@code > 0L}, and falls back to the system default when {@code 0L}.
 *
 * <p><b>Threading.</b> single-threaded JUnit worker (matches cluster duty-cycle invariant).
 */
class NewOrderSingleHandlerApp62Test {

  private static final long TS = 1_700_000_000_000_000_000L;
  private static final long PRICE = 100_000_000L; // 1.0 in fixed-point 10⁻⁸
  private static final long QTY = 100_000_000L; // 1 whole unit
  private static final long SCALE = FixedPointScale.PRICE_SCALE;

  private AccountStore accountStore;
  private CurrencyStore currencyStore;
  private RiskLimitStore riskLimitStore;
  private SymbolEligibilityStore symbolEligibilityStore;
  private TradingState tradingState;
  private OrderBook orderBook;
  private EventSink eventSink;
  private NewOrderSingleHandler handler;
  private FakeClientSession session;
  private FakeCluster fakeCluster;
  private MutableDirectBuffer msgBuf;
  private final UnsafeBuffer decodeBuf = new UnsafeBuffer(0, 0);

  @BeforeEach
  void setUp() {
    accountStore = new AccountStore();
    currencyStore = new CurrencyStore();
    riskLimitStore = new RiskLimitStore();
    symbolEligibilityStore = ReferenceDataSeeder.permissiveSymbolEligibilityStore();

    // Two accounts so §B can test per-account override divergence.
    accountStore.put(
        AccountFixtures.account(
            1L, "ACCT-A", AccountStatusEnum.Active, AccountState.Capabilities.CAN_TRADE));
    accountStore.put(
        AccountFixtures.account(
            2L, "ACCT-B", AccountStatusEnum.Active, AccountState.Capabilities.CAN_TRADE));
    currencyStore.put(
        CurrencyStore.packCode((byte) 'U', (byte) 'S', (byte) 'D'), CurrencyFixtures.usd());

    riskLimitStore.put(RiskLimitFixtures.permissive(1L));
    riskLimitStore.put(RiskLimitFixtures.permissive(2L));

    fakeCluster = new FakeCluster(0L);
    orderBook = new OrderBook(128);
    final var orderIdGen = new IdGenerator("ORD");
    final var execIdGen = new IdGenerator("EXE");
    final var quoteIdGen = new IdGenerator("QTE");
    tradingState = new TradingState(orderBook, orderIdGen, execIdGen, quoteIdGen);
    final var sequencer = new EventSequencer();
    final var journal = new EventJournal(64);
    eventSink = new EventSink(sequencer, journal);
    eventSink.setCluster(fakeCluster);
    final var rfqMetrics = new RfqMetrics();
    final var rfqStateMachine =
        new RfqStateMachine(
            256,
            TradingClusteredServiceFactory.DEFAULT_RFQ_TTL_NANOS,
            TradingClusteredServiceFactory.DEFAULT_RFQ_TTL_NANOS,
            TradingClusteredServiceFactory.DEFAULT_RFQ_TTL_NANOS,
            TradingClusteredServiceFactory.DEFAULT_RFQ_TTL_NANOS,
            TradingClusteredServiceFactory.DEFAULT_RFQ_REQUEST_TIMEOUT_NANOS,
            TradingClusteredServiceFactory.DEFAULT_RFQ_RATE_LIMIT_PER_SESSION,
            TradingClusteredServiceFactory.DEFAULT_RFQ_RATE_LIMIT_WINDOW_NANOS,
            0,
            0,
            accountStore,
            rfqMetrics);
    handler =
        new NewOrderSingleHandler(
            tradingState, accountStore, currencyStore, riskLimitStore, symbolEligibilityStore);
    handler.wireRfqStateMachine(rfqStateMachine, rfqMetrics);

    session = new FakeClientSession(1L);
    fakeCluster.addClientSession(session);
    msgBuf = new ExpandableArrayBuffer(512);
  }

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  /** Configure the fat-finger gates for the given account with the specified deviation bps. */
  private void enableFatFinger(
      final long accountId, final long deviationBps, final boolean failClosed) {
    final var limit = riskLimitStore.get(accountId);
    limit.setFatFingerEnabled(true);
    limit.setFatFingerFailClosed(failClosed);
    limit.setPriceDeviationBps(deviationBps);
  }

  /**
   * Pack a 6-byte symbol like "EURUSD" using the same packing the handler uses (FNV-1a 64-bit over
   * the trimmed bytes). For tests we re-use the public {@code SymbolPacker}-equivalent via the
   * permissive store seed of "EURUSD"; for tests requiring a NEW eligibility record we add it
   * directly here.
   */
  private void seedSymbolEligibilityOverride(final String symbol, final long overrideBps) {
    final byte[] sym = new byte[8];
    final byte[] src = symbol.getBytes(java.nio.charset.StandardCharsets.US_ASCII);
    System.arraycopy(src, 0, sym, 0, Math.min(src.length, 8));
    final long hash = SymbolEligibilityState.packSymbolKey(sym, 0);
    var existing = symbolEligibilityStore.get(hash);
    if (existing == null) {
      existing = new SymbolEligibilityState();
      existing.setSymbolBytes(sym, 0, sym.length);
      existing.setSymbolHash(hash);
      existing.setTradingAllowed(true);
      existing.setShortSaleAllowed(true);
    }
    existing.setPriceDeviationBpsOverride(overrideBps);
    symbolEligibilityStore.put(existing);
  }

  private int dispatchLimitNos(
      final String clOrdId, final String accountCode, final long price, final SideEnum side) {
    final int len =
        SbeTestEncoder.encodeNewOrderSingle(
            msgBuf, 0, clOrdId, "EURUSD", side, OrdTypeEnum.Limit, price, QTY, accountCode, "USD");
    handler.onCommand(
        session,
        TS,
        msgBuf,
        0,
        len,
        NewOrderSingleDecoder.BLOCK_LENGTH,
        NewOrderSingleDecoder.SCHEMA_VERSION,
        eventSink);
    return len;
  }

  /**
   * Decode the most recently emitted OrderRejectedEvent from the captured session messages and
   * return its breach context (the priceDeviationBps cap cited in tag 58).
   */
  private OrderRejectedEventDecoder lastRejected() {
    decodeBuf.wrap(session.messages.get(session.messages.size() - 1));
    final var hdr = new MessageHeaderDecoder();
    hdr.wrap(decodeBuf, 0);
    final var dec = new OrderRejectedEventDecoder();
    dec.wrap(decodeBuf, MessageHeaderDecoder.ENCODED_LENGTH, hdr.blockLength(), hdr.version());
    return dec;
  }

  // ---------------------------------------------------------------------------
  // §I — per-symbol fat-finger override
  // ---------------------------------------------------------------------------

  @Test
  void fatFinger_perSymbolOverride_appliedOverAccountKnob() {
    enableFatFinger(1L, 1_000L, true);
    seedSymbolEligibilityOverride("EURUSD", 50L);
    // Seed a fresh mid for the symbol so the reference is not stale → 1.0 in fixed-point.
    // Bid + ask average to PRICE = 1.0 → use PRICE - 1 and PRICE + 1 (truly negligible spread).
    handler.updateLastQuotedMid(
        SymbolEligibilityState.packSymbolKey(eurUsdBytes(), 0), PRICE - 1L, PRICE + 1L, TS);

    // Order priced 75 bps above mid (1.0 → 1.0075). |delta| = 75 bps. Account knob (1000) would
    // PASS; per-symbol override (50) must REJECT.
    dispatchLimitNos("ORD-S-OVR-1", "ACCT-A", PRICE + (PRICE * 75L) / 10_000L, SideEnum.Buy);
    assertEquals(1, session.messages.size(), "exactly one event must be emitted");
    final var rej = lastRejected();
    assertEquals(RejectReasonEnum.PriceTooFarFromMarket, rej.rejectReason());
    assertEquals(RiskCheckEnum.FatFinger, rej.checkId());
    assertEquals(
        50L,
        rej.limitValue(),
        "rejected with per-symbol override threshold (50), not account knob (1000)");
  }

  @Test
  void fatFinger_perSymbolOverride_zeroFallsBackToAccount() {
    enableFatFinger(1L, 100L, true);
    // Override left at the default 0L on the permissive seed (treated as "no override").
    // Bid + ask average to PRICE = 1.0 → use PRICE - 1 and PRICE + 1 (truly negligible spread).
    handler.updateLastQuotedMid(
        SymbolEligibilityState.packSymbolKey(eurUsdBytes(), 0), PRICE - 1L, PRICE + 1L, TS);

    // Order priced 150 bps above mid — exceeds the 100 bps account knob.
    dispatchLimitNos("ORD-S-FBK-1", "ACCT-A", PRICE + (PRICE * 150L) / 10_000L, SideEnum.Buy);
    assertEquals(1, session.messages.size(), "exactly one reject must be emitted");
    final var rej = lastRejected();
    assertEquals(RiskCheckEnum.FatFinger, rej.checkId());
    assertEquals(
        100L,
        rej.limitValue(),
        "no override → falls back to account knob (100), not zero sentinel");
  }

  // ---------------------------------------------------------------------------
  // §B — per-account idle-session timeout
  // ---------------------------------------------------------------------------

  @Test
  void idleScan_perAccountOverride_respected() {
    // Account 1 → 60s timeout, account 2 → 5s timeout.
    final RiskLimitState lim1 = riskLimitStore.get(1L);
    lim1.setIdleSessionTimeoutNanos(60L * 1_000_000_000L);
    final RiskLimitState lim2 = riskLimitStore.get(2L);
    lim2.setIdleSessionTimeoutNanos(5L * 1_000_000_000L);

    // Two sessions: A (account 1), B (account 2). Both open at t=0; both admit at t=0.
    final long sessionA = 101L;
    final long sessionB = 102L;
    final var fa = new FakeClientSession(sessionA);
    final var fb = new FakeClientSession(sessionB);
    fakeCluster.addClientSession(fa);
    fakeCluster.addClientSession(fb);
    handler.onSessionOpen(sessionA, TS);
    handler.onSessionOpen(sessionB, TS);

    // Seed admit-time bindings via the production wiring: dispatch a NOS through each session,
    // which populates sessionAccountMapping.
    session = fa;
    dispatchLimitNos("OA-1", "ACCT-A", PRICE, SideEnum.Buy);
    session = fb;
    dispatchLimitNos("OB-1", "ACCT-B", PRICE, SideEnum.Buy);

    // Scan at t=10s with system default = 30s. Only account 2 (5s override) should expire.
    final long scanTs = TS + 10L * 1_000_000_000L;
    handler.onIdleScan(scanTs, 30L * 1_000_000_000L, eventSink);

    // After scan: A still bound; B's binding removed.
    assertTrue(
        handler.sessionAccountMapping.containsKey(sessionA),
        "session A binding must remain — 10s < 60s per-account override");
    assertEquals(
        NewOrderSingleHandler.ACCOUNT_MAPPING_MISSING,
        handler.sessionAccountMapping.get(sessionB),
        "session B binding removed after per-account override eviction");
    assertNotNull(
        tradingState.orderBook().get(1L), "session A order preserved (within per-account window)");
    assertNull(
        tradingState.orderBook().get(2L), "session B order expired (per-account override = 5s)");
  }

  @Test
  void idleScan_perAccountTimeoutZero_fallsBackToSystemDefault() {
    final RiskLimitState lim1 = riskLimitStore.get(1L);
    lim1.setIdleSessionTimeoutNanos(0L);

    session = new FakeClientSession(1L);
    fakeCluster.addClientSession(session);
    handler.onSessionOpen(1L, TS);
    dispatchLimitNos("OA-FB-1", "ACCT-A", PRICE, SideEnum.Buy);

    // System default = 30s. Scan at t=20s — still active (under default).
    final long scanTs20 = TS + 20L * 1_000_000_000L;
    handler.onIdleScan(scanTs20, 30L * 1_000_000_000L, eventSink);
    assertNotNull(
        tradingState.orderBook().get(1L),
        "order must NOT expire at t=20s (system default 30s, override = 0 fall-back)");

    // Scan at t=35s — exceeds system default; should expire.
    final long scanTs35 = TS + 35L * 1_000_000_000L;
    handler.onIdleScan(scanTs35, 30L * 1_000_000_000L, eventSink);
    assertNull(
        tradingState.orderBook().get(1L),
        "order must expire at t=35s (system default fired, override stays 0)");
  }

  /** Pack the 6-byte "EURUSD" symbol into the same 8-byte fixed-length buffer the handler uses. */
  private static byte[] eurUsdBytes() {
    final byte[] sym = new byte[8];
    final byte[] src = "EURUSD".getBytes(java.nio.charset.StandardCharsets.US_ASCII);
    System.arraycopy(src, 0, sym, 0, src.length);
    return sym;
  }
}
