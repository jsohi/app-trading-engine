package com.trading.engine.cluster;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.trading.engine.cluster.journal.EventJournal;
import com.trading.engine.cluster.refdata.AccountState;
import com.trading.engine.cluster.refdata.AccountStore;
import com.trading.engine.cluster.refdata.CurrencyState;
import com.trading.engine.cluster.refdata.CurrencyStore;
import com.trading.engine.cluster.refdata.LoadAccountHandler;
import com.trading.engine.cluster.refdata.LoadCurrencyHandler;
import com.trading.engine.cluster.refdata.LoadRiskLimitHandler;
import com.trading.engine.cluster.refdata.ReferenceDataRegistry;
import com.trading.engine.cluster.refdata.RiskLimitState;
import com.trading.engine.cluster.refdata.RiskLimitStore;
import com.trading.engine.cluster.sequencer.EventSequencer;
import com.trading.engine.messages.sbe.AccountStatusEnum;
import com.trading.engine.messages.sbe.AccountTypeEnum;
import com.trading.engine.messages.sbe.AcctIDSourceEnum;
import com.trading.engine.messages.sbe.ComplianceStatusEnum;
import com.trading.engine.messages.sbe.CurrencyClassEnum;
import com.trading.engine.messages.sbe.CurrencyLoadedEventDecoder;
import com.trading.engine.messages.sbe.ExecTypeEnum;
import com.trading.engine.messages.sbe.ExecutionReportDecoder;
import com.trading.engine.messages.sbe.LoadCurrencyEncoder;
import com.trading.engine.messages.sbe.MessageHeaderDecoder;
import com.trading.engine.messages.sbe.MessageHeaderEncoder;
import com.trading.engine.messages.sbe.NewOrderSingleEncoder;
import com.trading.engine.messages.sbe.OrdStatusEnum;
import com.trading.engine.messages.sbe.OrdTypeEnum;
import com.trading.engine.messages.sbe.OrderCreatedEventDecoder;
import com.trading.engine.messages.sbe.OrderRejectedEventDecoder;
import com.trading.engine.messages.sbe.RejectReasonEnum;
import com.trading.engine.messages.sbe.SideEnum;
import com.trading.engine.messages.sbe.TimeInForceEnum;
import io.aeron.DirectBufferVector;
import io.aeron.Publication;
import io.aeron.cluster.codecs.CloseReason;
import io.aeron.cluster.service.ClientSession;
import io.aeron.cluster.service.Cluster;
import io.aeron.cluster.service.ClusteredServiceContainer;
import io.aeron.logbuffer.BufferClaim;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TradingClusteredServiceTest {

  private static final long TIMESTAMP = 1_700_000_000_000_000_000L;
  private static final long PRICE_SCALE = 100_000_000L;

  private IdGenerator orderIdGen;
  private IdGenerator execIdGen;
  private OrderBook orderBook;
  private EventSequencer eventSequencer;
  private EventJournal eventJournal;
  private AccountStore accountStore;
  private CurrencyStore currencyStore;
  private RiskLimitStore riskLimitStore;
  private ReferenceDataRegistry registry;
  private TradingClusteredService service;
  private FakeCluster cluster;
  private FakeClientSession session;

  @BeforeEach
  void setUp() {
    orderIdGen = new IdGenerator("ORD");
    execIdGen = new IdGenerator("EXE");
    orderBook = new OrderBook(128);
    eventSequencer = new EventSequencer();
    eventJournal = new EventJournal(64);
    accountStore = new AccountStore();
    currencyStore = new CurrencyStore();
    riskLimitStore = new RiskLimitStore();

    seedReferenceData(accountStore, currencyStore, riskLimitStore);

    registry = new ReferenceDataRegistry();
    registry.registerStore(accountStore);
    registry.registerStore(currencyStore);
    registry.registerStore(riskLimitStore);
    registry.registerLoader(new LoadAccountHandler(accountStore, currencyStore));
    registry.registerLoader(new LoadCurrencyHandler(currencyStore));
    registry.registerLoader(new LoadRiskLimitHandler(riskLimitStore, accountStore));

    service =
        new TradingClusteredService(
            orderIdGen,
            execIdGen,
            orderBook,
            eventSequencer,
            eventJournal,
            accountStore,
            currencyStore,
            riskLimitStore,
            registry);

    cluster = new FakeCluster(TIMESTAMP);
    session = new FakeClientSession();
    // Prime the service with a cluster ref via onStart(cluster, null).
    service.onStart(cluster, null);
  }

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  private static AccountState makeAccount(
      final long id, final String code, final AccountStatusEnum status, final long capabilities) {
    final AccountState s = new AccountState();
    s.setAccountId(id);
    final byte[] codeBytes = code.getBytes();
    s.setAccountCode(codeBytes, 0, codeBytes.length);
    s.setAcctIdSource(AcctIDSourceEnum.Internal);
    final byte[] name = ("Account " + code).getBytes();
    s.setAccountName(name, 0, name.length);
    s.setAccountType(AccountTypeEnum.Client);
    s.setBaseCurrency((byte) 'U', (byte) 'S', (byte) 'D');
    s.setStatus(status);
    s.setComplianceStatus(ComplianceStatusEnum.OK);
    s.setCapabilities(capabilities);
    s.setTransactTime(0L);
    return s;
  }

  private static CurrencyState makeCurrency(final String code, final int isoNumeric) {
    final CurrencyState c = new CurrencyState();
    final byte[] codeBytes = code.getBytes();
    c.setCcyCode(codeBytes, 0);
    c.setIsoNumeric(isoNumeric);
    final byte[] name = ("Currency " + code).getBytes();
    c.setName(name, 0, name.length);
    c.setDecimals(2);
    c.setCurrencyClass(CurrencyClassEnum.Fiat);
    c.setStatus(AccountStatusEnum.Active);
    c.setTransactTime(0L);
    return c;
  }

  /** Seed the 3 accounts, 2 currencies, and 1 risk limit used by all order-processing tests. */
  private static void seedReferenceData(
      final AccountStore accounts, final CurrencyStore currencies, final RiskLimitStore limits) {
    accounts.put(
        makeAccount(1L, "ACME", AccountStatusEnum.Active, AccountState.Capabilities.CAN_TRADE));
    accounts.put(
        makeAccount(
            2L, "LOCKED", AccountStatusEnum.Suspended, AccountState.Capabilities.CAN_TRADE));
    accounts.put(makeAccount(3L, "QUOTEONLY", AccountStatusEnum.Active, 0L));
    currencies.put(
        CurrencyStore.packCode((byte) 'U', (byte) 'S', (byte) 'D'), makeCurrency("USD", 840));
    currencies.put(
        CurrencyStore.packCode((byte) 'E', (byte) 'U', (byte) 'R'), makeCurrency("EUR", 978));
    final RiskLimitState limit = new RiskLimitState();
    limit.setAccountId(1L);
    limit.setMaxOrderSize(10L * PRICE_SCALE);
    limit.setStatus(AccountStatusEnum.Active);
    limits.put(limit);
  }

  private static int encodeNewOrderSingle(
      final MutableDirectBuffer dst,
      final String clOrdId,
      final String symbol,
      final SideEnum side,
      final OrdTypeEnum ordType,
      final long price,
      final long orderQty,
      final String accountCode,
      final String currency) {
    final MessageHeaderEncoder header = new MessageHeaderEncoder();
    final NewOrderSingleEncoder enc = new NewOrderSingleEncoder();
    enc.wrapAndApplyHeader(dst, 0, header);
    enc.clOrdId(clOrdId);
    enc.symbol(symbol);
    enc.side(side);
    enc.ordType(ordType);
    enc.price(price);
    enc.orderQty(orderQty);
    enc.timeInForce(TimeInForceEnum.Day);
    enc.transactTime(0L);
    enc.accountCode(accountCode);
    enc.currency(currency);
    return MessageHeaderEncoder.ENCODED_LENGTH + enc.encodedLength();
  }

  private void dispatch(final MutableDirectBuffer buffer, final int length) {
    service.onSessionMessage(session, TIMESTAMP, buffer, 0, length, null);
  }

  // ---------------------------------------------------------------------------
  // NewOrderSingle — happy path
  // ---------------------------------------------------------------------------

  @Test
  void validNewOrderSingleEmitsOrderCreatedAndExecutionReportNew() {
    final MutableDirectBuffer buf = new ExpandableArrayBuffer(256);
    final int len =
        encodeNewOrderSingle(
            buf,
            "CL-1",
            "EURUSD",
            SideEnum.Buy,
            OrdTypeEnum.Limit,
            1L * PRICE_SCALE,
            5L * PRICE_SCALE,
            "ACME",
            "USD");
    dispatch(buf, len);

    // Expect two messages on the session: OrderCreatedEvent + ExecutionReport(New).
    assertEquals(2, session.messages.size());
    assertTemplateId(OrderCreatedEventDecoder.TEMPLATE_ID, session.messages.get(0));
    assertTemplateId(ExecutionReportDecoder.TEMPLATE_ID, session.messages.get(1));

    // Decode the OrderCreatedEvent.
    final OrderCreatedEventDecoder created = decodeOrderCreated(session.messages.get(0));
    assertEquals(1L, created.sequenceNumber());
    assertEquals(TIMESTAMP, created.timestamp());
    assertEquals(SideEnum.Buy, created.side());
    assertEquals(OrdTypeEnum.Limit, created.ordType());
    assertEquals(5L * PRICE_SCALE, created.orderQty());

    // Decode the ExecutionReport ACK.
    final ExecutionReportDecoder er = decodeExecutionReport(session.messages.get(1));
    assertEquals(ExecTypeEnum.New, er.execType());
    assertEquals(OrdStatusEnum.New, er.ordStatus());
    assertEquals(5L * PRICE_SCALE, er.leavesQty());
    assertEquals(0L, er.cumQty());
    assertEquals(TIMESTAMP, er.transactTime());

    // OrderBook now has one active order keyed by the ORD counter (=1).
    assertEquals(1, orderBook.size());
    assertNotNull(orderBook.get(1L));
    assertEquals(1L, orderBook.get(1L).accountId());

    // EventJournal has exactly one appended event (the OrderCreatedEvent).
    assertEquals(1, eventJournal.size());
    assertEquals(1L, eventJournal.lowestSequence());
    assertEquals(1L, eventJournal.highestSequence());
  }

  @Test
  void rejectedExecutionReportCarriesCurrentOrdersCurrency() {
    // Regression for the egressBuffer currency-leak: emit a happy-path order with USD, then
    // submit a reject-triggering order with EUR — the rejected ExecutionReport must carry EUR,
    // not the stale USD bytes left in egressBuffer.
    final MutableDirectBuffer buf = new ExpandableArrayBuffer(256);
    int len =
        encodeNewOrderSingle(
            buf,
            "CL-OK",
            "EURUSD",
            SideEnum.Buy,
            OrdTypeEnum.Limit,
            1L * PRICE_SCALE,
            1L * PRICE_SCALE,
            "ACME",
            "USD");
    dispatch(buf, len);
    session.messages.clear();

    // Now a reject with a different currency (EUR, which is in the store, but zero qty forces
    // the reject path).
    len =
        encodeNewOrderSingle(
            buf,
            "CL-BAD",
            "EURUSD",
            SideEnum.Buy,
            OrdTypeEnum.Limit,
            1L * PRICE_SCALE,
            0L,
            "ACME",
            "EUR");
    dispatch(buf, len);

    assertEquals(2, session.messages.size());
    final ExecutionReportDecoder er = decodeExecutionReport(session.messages.get(1));
    assertEquals(ExecTypeEnum.Rejected, er.execType());
    assertEquals((byte) 'E', er.currency(0));
    assertEquals((byte) 'U', er.currency(1));
    assertEquals((byte) 'R', er.currency(2));
  }

  @Test
  void monotonicOrderAndExecIdsAcrossMultipleOrders() {
    final MutableDirectBuffer buf = new ExpandableArrayBuffer(256);
    for (int i = 1; i <= 3; i++) {
      final int len =
          encodeNewOrderSingle(
              buf,
              "CL-" + i,
              "EURUSD",
              SideEnum.Buy,
              OrdTypeEnum.Limit,
              1L * PRICE_SCALE,
              1L * PRICE_SCALE,
              "ACME",
              "USD");
      dispatch(buf, len);
    }
    assertEquals(3, orderBook.size());
    assertNotNull(orderBook.get(1L));
    assertNotNull(orderBook.get(2L));
    assertNotNull(orderBook.get(3L));
    // Three orders → three OrderCreatedEvent + three ExecutionReport → 6 messages.
    assertEquals(6, session.messages.size());
    assertEquals(3L, orderIdGen.currentCounter());
    assertEquals(3L, execIdGen.currentCounter());
  }

  // ---------------------------------------------------------------------------
  // NewOrderSingle — validation rejects
  // ---------------------------------------------------------------------------

  @Test
  void zeroQtyRejected() {
    assertRejected(
        encodeOrder("CL-1", "EURUSD", OrdTypeEnum.Limit, 1L, 0L, "ACME", "USD"),
        RejectReasonEnum.InvalidQuantity);
  }

  @Test
  void limitWithZeroPriceRejected() {
    assertRejected(
        encodeOrder("CL-1", "EURUSD", OrdTypeEnum.Limit, 0L, 5L, "ACME", "USD"),
        RejectReasonEnum.InvalidPrice);
  }

  @Test
  void unknownAccountRejected() {
    assertRejected(
        encodeOrder("CL-1", "EURUSD", OrdTypeEnum.Limit, 1L, 5L, "UNKNOWN", "USD"),
        RejectReasonEnum.AccountNotFound);
  }

  @Test
  void suspendedAccountRejected() {
    assertRejected(
        encodeOrder("CL-1", "EURUSD", OrdTypeEnum.Limit, 1L, 5L, "LOCKED", "USD"),
        RejectReasonEnum.AccountSuspended);
  }

  @Test
  void noTradePermissionRejected() {
    assertRejected(
        encodeOrder("CL-1", "EURUSD", OrdTypeEnum.Limit, 1L, 5L, "QUOTEONLY", "USD"),
        RejectReasonEnum.AccountNoTradePermission);
  }

  @Test
  void unknownCurrencyRejected() {
    assertRejected(
        encodeOrder("CL-1", "EURUSD", OrdTypeEnum.Limit, 1L, 5L, "ACME", "XXX"),
        RejectReasonEnum.UnknownCurrency);
  }

  @Test
  void invalidCurrencyCodeRejected() {
    // Lowercase bytes fail packCodeOrInvalid.
    assertRejected(
        encodeOrder("CL-1", "EURUSD", OrdTypeEnum.Limit, 1L, 5L, "ACME", "usd"),
        RejectReasonEnum.InvalidCurrencyCode);
  }

  @Test
  void orderExceedsAccountMaxSizeRejected() {
    // Account 1 (ACME) has a risk limit of maxOrderSize = 10 (scaled). Order qty 100 > limit.
    assertRejected(
        encodeOrder("CL-1", "EURUSD", OrdTypeEnum.Limit, 1L, 100L, "ACME", "USD"),
        RejectReasonEnum.OrderExceedsMaxSize);
  }

  @Test
  void emptySymbolRejected() {
    // Build an order with an all-zero symbol — SBE allows empty by padding.
    final MutableDirectBuffer buf = new ExpandableArrayBuffer(256);
    final MessageHeaderEncoder header = new MessageHeaderEncoder();
    final NewOrderSingleEncoder enc = new NewOrderSingleEncoder();
    enc.wrapAndApplyHeader(buf, 0, header);
    enc.clOrdId("CL-1");
    enc.symbol("");
    enc.side(SideEnum.Buy);
    enc.ordType(OrdTypeEnum.Limit);
    enc.price(1L * PRICE_SCALE);
    enc.orderQty(5L * PRICE_SCALE);
    enc.timeInForce(TimeInForceEnum.Day);
    enc.transactTime(0L);
    enc.accountCode("ACME");
    enc.currency("USD");
    final int len = MessageHeaderEncoder.ENCODED_LENGTH + enc.encodedLength();

    dispatch(buf, len);
    final OrderRejectedEventDecoder dec = decodeOrderRejected(session.messages.get(0));
    assertEquals(RejectReasonEnum.UnknownSymbol, dec.rejectReason());
  }

  private byte[] encodeOrder(
      final String clOrdId,
      final String symbol,
      final OrdTypeEnum ordType,
      final long pxWhole,
      final long qtyWhole,
      final String accountCode,
      final String currency) {
    final MutableDirectBuffer buf = new ExpandableArrayBuffer(256);
    final int len =
        encodeNewOrderSingle(
            buf,
            clOrdId,
            symbol,
            SideEnum.Buy,
            ordType,
            pxWhole * PRICE_SCALE,
            qtyWhole * PRICE_SCALE,
            accountCode,
            currency);
    final byte[] bytes = new byte[len];
    buf.getBytes(0, bytes);
    return bytes;
  }

  private void assertRejected(final byte[] commandBytes, final RejectReasonEnum expected) {
    final UnsafeBuffer wrapper = new UnsafeBuffer(commandBytes);
    dispatch(wrapper, commandBytes.length);

    // Two replies: OrderRejectedEvent + ExecutionReport(Rejected).
    assertEquals(2, session.messages.size());
    assertTemplateId(OrderRejectedEventDecoder.TEMPLATE_ID, session.messages.get(0));
    assertTemplateId(ExecutionReportDecoder.TEMPLATE_ID, session.messages.get(1));

    final OrderRejectedEventDecoder rejected = decodeOrderRejected(session.messages.get(0));
    assertEquals(expected, rejected.rejectReason());

    final ExecutionReportDecoder er = decodeExecutionReport(session.messages.get(1));
    assertEquals(ExecTypeEnum.Rejected, er.execType());
    assertEquals(OrdStatusEnum.Rejected, er.ordStatus());

    // No order should have been admitted to the book.
    assertEquals(0, orderBook.size());
    // Exactly one event (the reject) was journaled.
    assertEquals(1, eventJournal.size());
  }

  // ---------------------------------------------------------------------------
  // Ref-data dispatch
  // ---------------------------------------------------------------------------

  @Test
  void loadCurrencyCommandDispatchedToRegistry() {
    final MutableDirectBuffer buf = new ExpandableArrayBuffer(256);
    final MessageHeaderEncoder header = new MessageHeaderEncoder();
    final LoadCurrencyEncoder enc = new LoadCurrencyEncoder();
    enc.wrapAndApplyHeader(buf, 0, header);
    enc.putCcyCode((byte) 'J', (byte) 'P', (byte) 'Y');
    enc.isoNumeric(392);
    enc.name("Japanese Yen");
    enc.decimals((short) 0);
    enc.currencyClass(CurrencyClassEnum.Fiat);
    enc.status(AccountStatusEnum.Active);
    enc.transactTime(0L);
    final int len = MessageHeaderEncoder.ENCODED_LENGTH + enc.encodedLength();

    dispatch(buf, len);

    // The registry's loader emitted a CurrencyLoadedEvent — expect it on the session + in the
    // journal.
    assertEquals(1, session.messages.size());
    assertTemplateId(CurrencyLoadedEventDecoder.TEMPLATE_ID, session.messages.get(0));
    assertEquals(1, eventJournal.size());

    // The currency is now in the store.
    final int packed = CurrencyStore.packCode((byte) 'J', (byte) 'P', (byte) 'Y');
    assertTrue(currencyStore.contains(packed));
  }

  @Test
  void unknownTemplateIdIsSilentlyDropped() {
    // Build a message with an arbitrary (unused) templateId so both the registry and the order
    // dispatch ignore it.
    final MutableDirectBuffer buf = new ExpandableArrayBuffer(16);
    final MessageHeaderEncoder header = new MessageHeaderEncoder();
    header.wrap(buf, 0).blockLength(0).templateId(9999).schemaId(1).version(1);
    dispatch(buf, MessageHeaderEncoder.ENCODED_LENGTH);
    assertEquals(0, session.messages.size());
    assertEquals(0, orderBook.size());
  }

  // ---------------------------------------------------------------------------
  // Snapshot round-trip
  // ---------------------------------------------------------------------------

  @Test
  void snapshotRoundTripRestoresFullState() {
    // Populate some order book state by dispatching valid NewOrderSingles.
    final MutableDirectBuffer buf = new ExpandableArrayBuffer(256);
    for (int i = 1; i <= 2; i++) {
      final int len =
          encodeNewOrderSingle(
              buf,
              "CL-" + i,
              "EURUSD",
              SideEnum.Buy,
              OrdTypeEnum.Limit,
              1L * PRICE_SCALE,
              1L * PRICE_SCALE,
              "ACME",
              "USD");
      dispatch(buf, len);
    }
    assertEquals(2, orderBook.size());
    assertEquals(2L, eventSequencer.currentSequence());
    assertEquals(2L, orderIdGen.currentCounter());

    // Encode the snapshot into a single contiguous buffer (header + 6 body fragments).
    final MutableDirectBuffer concatenated = new ExpandableArrayBuffer(65_536);
    final int totalLength = encodeAndConcatenateSnapshot(service, concatenated);

    // Now rebuild a fresh service with empty stores and load the snapshot.
    final IdGenerator freshOrderGen = new IdGenerator("ORD");
    final IdGenerator freshExecGen = new IdGenerator("EXE");
    final OrderBook freshBook = new OrderBook(128);
    final EventSequencer freshSeq = new EventSequencer();
    final EventJournal freshJournal = new EventJournal(64);
    final AccountStore freshAccounts = new AccountStore();
    final CurrencyStore freshCurrencies = new CurrencyStore();
    final RiskLimitStore freshLimits = new RiskLimitStore();
    final ReferenceDataRegistry freshRegistry = new ReferenceDataRegistry();
    freshRegistry.registerStore(freshAccounts);
    freshRegistry.registerStore(freshCurrencies);
    freshRegistry.registerStore(freshLimits);

    final TradingClusteredService restored =
        new TradingClusteredService(
            freshOrderGen,
            freshExecGen,
            freshBook,
            freshSeq,
            freshJournal,
            freshAccounts,
            freshCurrencies,
            freshLimits,
            freshRegistry);
    restored.onStart(cluster, null); // wire the cluster ref
    restored.loadSnapshot(concatenated, 0, totalLength);

    // Counters restored.
    assertEquals(2L, freshSeq.currentSequence());
    assertEquals(2L, freshOrderGen.currentCounter());
    assertEquals(2L, freshExecGen.currentCounter());

    // Stores restored.
    assertEquals(3, freshAccounts.size());
    assertNotNull(freshAccounts.get(1L));
    assertEquals(2, freshCurrencies.size());
    assertTrue(
        freshCurrencies.contains(CurrencyStore.packCode((byte) 'U', (byte) 'S', (byte) 'D')));
    assertEquals(1, freshLimits.size());
    assertEquals(10L * PRICE_SCALE, freshLimits.get(1L).maxOrderSize());

    // OrderBook restored.
    assertEquals(2, freshBook.size());
    assertNotNull(freshBook.get(1L));
    assertNotNull(freshBook.get(2L));
    assertEquals(1L, freshBook.get(1L).accountId());
  }

  @Test
  void snapshotPlusReplayMatchesFullReplay() {
    // -- Path B: snapshot after 3 commands, then replay 2 more on restored service -----------

    final MutableDirectBuffer buf = new ExpandableArrayBuffer(256);
    for (int i = 1; i <= 3; i++) {
      final int len =
          encodeNewOrderSingle(
              buf,
              "CL-" + i,
              "EURUSD",
              SideEnum.Buy,
              OrdTypeEnum.Limit,
              1L * PRICE_SCALE,
              1L * PRICE_SCALE,
              "ACME",
              "USD");
      dispatch(buf, len);
    }
    assertEquals(3, orderBook.size());

    // Take snapshot at position 3.
    final MutableDirectBuffer concatenated = new ExpandableArrayBuffer(65_536);
    final int totalLength = encodeAndConcatenateSnapshot(service, concatenated);

    // Build restored service, load snapshot, replay commands 4-5.
    final IdGenerator resOrderGen = new IdGenerator("ORD");
    final IdGenerator resExecGen = new IdGenerator("EXE");
    final OrderBook resBook = new OrderBook(128);
    final EventSequencer resSeq = new EventSequencer();
    final EventJournal resJournal = new EventJournal(64);
    final AccountStore resAccounts = new AccountStore();
    final CurrencyStore resCurrencies = new CurrencyStore();
    final RiskLimitStore resLimits = new RiskLimitStore();
    final ReferenceDataRegistry resRegistry = new ReferenceDataRegistry();
    resRegistry.registerStore(resAccounts);
    resRegistry.registerStore(resCurrencies);
    resRegistry.registerStore(resLimits);
    final TradingClusteredService restored =
        new TradingClusteredService(
            resOrderGen,
            resExecGen,
            resBook,
            resSeq,
            resJournal,
            resAccounts,
            resCurrencies,
            resLimits,
            resRegistry);
    restored.onStart(cluster, null);
    restored.loadSnapshot(concatenated, 0, totalLength);

    final FakeClientSession resSession = new FakeClientSession();
    for (int i = 4; i <= 5; i++) {
      final int len =
          encodeNewOrderSingle(
              buf,
              "CL-" + i,
              "EURUSD",
              SideEnum.Buy,
              OrdTypeEnum.Limit,
              1L * PRICE_SCALE,
              1L * PRICE_SCALE,
              "ACME",
              "USD");
      restored.onSessionMessage(resSession, TIMESTAMP, buf, 0, len, null);
    }

    // -- Path A: full replay of all 5 commands from genesis --------------------------------

    final IdGenerator fullOrderGen = new IdGenerator("ORD");
    final IdGenerator fullExecGen = new IdGenerator("EXE");
    final OrderBook fullBook = new OrderBook(128);
    final EventSequencer fullSeq = new EventSequencer();
    final EventJournal fullJournal = new EventJournal(64);
    final AccountStore fullAccounts = new AccountStore();
    final CurrencyStore fullCurrencies = new CurrencyStore();
    final RiskLimitStore fullLimits = new RiskLimitStore();
    final ReferenceDataRegistry fullRegistry = new ReferenceDataRegistry();
    seedReferenceData(fullAccounts, fullCurrencies, fullLimits);
    fullRegistry.registerStore(fullAccounts);
    fullRegistry.registerStore(fullCurrencies);
    fullRegistry.registerStore(fullLimits);
    final TradingClusteredService fullService =
        new TradingClusteredService(
            fullOrderGen,
            fullExecGen,
            fullBook,
            fullSeq,
            fullJournal,
            fullAccounts,
            fullCurrencies,
            fullLimits,
            fullRegistry);
    fullService.onStart(cluster, null);

    final FakeClientSession fullSession = new FakeClientSession();
    for (int i = 1; i <= 5; i++) {
      final int len =
          encodeNewOrderSingle(
              buf,
              "CL-" + i,
              "EURUSD",
              SideEnum.Buy,
              OrdTypeEnum.Limit,
              1L * PRICE_SCALE,
              1L * PRICE_SCALE,
              "ACME",
              "USD");
      fullService.onSessionMessage(fullSession, TIMESTAMP, buf, 0, len, null);
    }

    // -- Assert Path A == Path B ----------------------------------------------------------

    // Absolute sanity checks first — catch cases where both paths fail identically.
    assertEquals(5L, fullSeq.currentSequence());
    assertEquals(5L, fullOrderGen.currentCounter());
    assertEquals(5L, fullExecGen.currentCounter());
    assertEquals(5, fullBook.size());

    // Relative equality between the two paths.
    assertEquals(fullSeq.currentSequence(), resSeq.currentSequence());
    assertEquals(fullOrderGen.currentCounter(), resOrderGen.currentCounter());
    assertEquals(fullExecGen.currentCounter(), resExecGen.currentCounter());
    assertEquals(fullBook.size(), resBook.size());
    assertEquals(fullAccounts.size(), resAccounts.size());
    assertEquals(fullCurrencies.size(), resCurrencies.size());
    assertEquals(fullLimits.size(), resLimits.size());

    // Ref-data field-level fidelity — sizes alone could mask corrupted contents.
    final AccountState fullAcct = fullAccounts.get(1L);
    final AccountState resAcct = resAccounts.get(1L);
    assertNotNull(resAcct, "ACME account missing after snapshot+replay");
    assertEquals(fullAcct.accountId(), resAcct.accountId());
    assertEquals(fullAcct.status(), resAcct.status());
    assertEquals(fullAcct.capabilities(), resAcct.capabilities());

    final int usdKey = CurrencyStore.packCode((byte) 'U', (byte) 'S', (byte) 'D');
    assertTrue(fullCurrencies.contains(usdKey));
    assertTrue(resCurrencies.contains(usdKey));

    final RiskLimitState fullRl = fullLimits.get(1L);
    final RiskLimitState resRl = resLimits.get(1L);
    assertNotNull(resRl, "risk limit for account 1 missing after snapshot+replay");
    assertEquals(fullRl.maxOrderSize(), resRl.maxOrderSize());
    assertEquals(fullRl.status(), resRl.status());

    // EventJournal is intentionally NOT compared: it is not snapshotted (projections replay
    // from Aeron Archive position 0), so resJournal only contains post-snapshot events (2)
    // while fullJournal contains all 5. This divergence is by design.

    for (long ordKey = 1; ordKey <= 5; ordKey++) {
      final OrderState fullOrd = fullBook.get(ordKey);
      final OrderState resOrd = resBook.get(ordKey);
      assertNotNull(fullOrd, "full-replay order " + ordKey);
      assertNotNull(resOrd, "snapshot+replay order " + ordKey);
      assertEquals(fullOrd.accountId(), resOrd.accountId());
      assertEquals(fullOrd.side(), resOrd.side());
      assertEquals(fullOrd.ordType(), resOrd.ordType());
      assertEquals(fullOrd.price(), resOrd.price());
      assertEquals(fullOrd.orderQty(), resOrd.orderQty());
      assertEquals(fullOrd.leavesQty(), resOrd.leavesQty());
      assertEquals(fullOrd.cumQty(), resOrd.cumQty());
    }
  }

  @Test
  void accountStoreSecondaryIndexRebuiltAfterSnapshot() {
    // Snapshot the service from setUp (3 accounts already seeded).
    final MutableDirectBuffer concatenated = new ExpandableArrayBuffer(65_536);
    final int totalLength = encodeAndConcatenateSnapshot(service, concatenated);

    // Restore into fresh service with explicit AccountStore reference.
    final AccountStore freshAccounts = new AccountStore();
    final CurrencyStore freshCurrencies = new CurrencyStore();
    final RiskLimitStore freshLimits = new RiskLimitStore();
    final ReferenceDataRegistry freshRegistry = new ReferenceDataRegistry();
    freshRegistry.registerStore(freshAccounts);
    freshRegistry.registerStore(freshCurrencies);
    freshRegistry.registerStore(freshLimits);
    final TradingClusteredService restored =
        new TradingClusteredService(
            new IdGenerator("ORD"),
            new IdGenerator("EXE"),
            new OrderBook(128),
            new EventSequencer(),
            new EventJournal(64),
            freshAccounts,
            freshCurrencies,
            freshLimits,
            freshRegistry);
    restored.onStart(cluster, null);
    restored.loadSnapshot(concatenated, 0, totalLength);

    // Primary index works (sanity).
    assertNotNull(freshAccounts.get(1L));
    assertNotNull(freshAccounts.get(2L));
    assertNotNull(freshAccounts.get(3L));

    // Secondary index (getByCode) rebuilt correctly for all 3 accounts.
    final UnsafeBuffer acmeBuf = new UnsafeBuffer("ACME".getBytes(StandardCharsets.US_ASCII));
    assertEquals(1L, freshAccounts.getByCode(acmeBuf, 0, 4).accountId());

    final UnsafeBuffer lockedBuf = new UnsafeBuffer("LOCKED".getBytes(StandardCharsets.US_ASCII));
    assertEquals(2L, freshAccounts.getByCode(lockedBuf, 0, 6).accountId());

    final UnsafeBuffer quoteOnlyBuf =
        new UnsafeBuffer("QUOTEONLY".getBytes(StandardCharsets.US_ASCII));
    assertEquals(3L, freshAccounts.getByCode(quoteOnlyBuf, 0, 9).accountId());

    // Unknown code returns null.
    final UnsafeBuffer unknownBuf = new UnsafeBuffer("NOPE".getBytes(StandardCharsets.US_ASCII));
    assertNull(freshAccounts.getByCode(unknownBuf, 0, 4));
  }

  @Test
  void snapshotRoundTripEmptyState() {
    // Take a snapshot of an initial (empty) service and restore into a fresh one.
    final MutableDirectBuffer concatenated = new ExpandableArrayBuffer(65_536);
    final int totalLength = encodeAndConcatenateSnapshot(service, concatenated);

    // Use explicit fresh collaborators so we can assert state on them directly (a plain
    // freshService() hides them behind the TradingClusteredService).
    final IdGenerator freshOrderGen = new IdGenerator("ORD");
    final IdGenerator freshExecGen = new IdGenerator("EXE");
    final OrderBook freshBook = new OrderBook(32);
    final EventSequencer freshSeq = new EventSequencer();
    final EventJournal freshJournal = new EventJournal(16);
    final AccountStore freshAccounts = new AccountStore();
    final CurrencyStore freshCurrencies = new CurrencyStore();
    final RiskLimitStore freshLimits = new RiskLimitStore();
    final ReferenceDataRegistry freshRegistry = new ReferenceDataRegistry();
    freshRegistry.registerStore(freshAccounts);
    freshRegistry.registerStore(freshCurrencies);
    freshRegistry.registerStore(freshLimits);
    final TradingClusteredService restored =
        new TradingClusteredService(
            freshOrderGen,
            freshExecGen,
            freshBook,
            freshSeq,
            freshJournal,
            freshAccounts,
            freshCurrencies,
            freshLimits,
            freshRegistry);
    restored.onStart(cluster, null);
    restored.loadSnapshot(concatenated, 0, totalLength);

    // The source service (setUp) has 3 accounts, 2 currencies, 1 risk limit, and no orders;
    // sequencer counter is 0. All of this should now be reflected in the fresh one.
    assertEquals(0L, freshSeq.currentSequence());
    assertEquals(0L, freshOrderGen.currentCounter());
    assertEquals(0L, freshExecGen.currentCounter());
    assertEquals(3, freshAccounts.size());
    assertEquals(2, freshCurrencies.size());
    assertEquals(1, freshLimits.size());
    assertEquals(0, freshBook.size());
  }

  @Test
  void snapshotCorruptedChecksumDetected() {
    final MutableDirectBuffer concatenated = new ExpandableArrayBuffer(65_536);
    final int totalLength = encodeAndConcatenateSnapshot(service, concatenated);

    // Flip a byte in the body (somewhere in the eventSeq fragment which is just after the
    // SnapshotTaken header).
    final int flipOffset =
        MessageHeaderDecoder.ENCODED_LENGTH
            + com.trading.engine.messages.sbe.SnapshotTakenDecoder.BLOCK_LENGTH
            + MessageHeaderDecoder.ENCODED_LENGTH;
    final byte original = concatenated.getByte(flipOffset);
    concatenated.putByte(flipOffset, (byte) (original ^ 0xFF));

    final TradingClusteredService restored = freshService();
    final IllegalStateException ise =
        assertThrows(
            IllegalStateException.class, () -> restored.loadSnapshot(concatenated, 0, totalLength));
    assertTrue(ise.getMessage().toLowerCase().contains("crc"));
  }

  private TradingClusteredService freshService() {
    // The TradingClusteredService constructor asserts that the ref-data registry is backed by
    // the same store instances passed in directly, so we must register and pass the same
    // objects.
    final AccountStore freshAccountStore = new AccountStore();
    final CurrencyStore freshCurrencyStore = new CurrencyStore();
    final RiskLimitStore freshRiskLimitStore = new RiskLimitStore();
    final ReferenceDataRegistry freshRegistry = new ReferenceDataRegistry();
    freshRegistry.registerStore(freshAccountStore);
    freshRegistry.registerStore(freshCurrencyStore);
    freshRegistry.registerStore(freshRiskLimitStore);
    final TradingClusteredService s =
        new TradingClusteredService(
            new IdGenerator("ORD"),
            new IdGenerator("EXE"),
            new OrderBook(128),
            new EventSequencer(),
            new EventJournal(64),
            freshAccountStore,
            freshCurrencyStore,
            freshRiskLimitStore,
            freshRegistry);
    s.onStart(cluster, null);
    return s;
  }

  private static int appendFragment(
      final MutableDirectBuffer dst,
      final int offset,
      final MutableDirectBuffer src,
      final int length) {
    dst.putBytes(offset, src, 0, length);
    return offset + length;
  }

  /**
   * Encode all snapshot fragments from the given service into a single contiguous buffer. Returns
   * the total encoded length.
   */
  private static int encodeAndConcatenateSnapshot(
      final TradingClusteredService svc, final MutableDirectBuffer dst) {
    svc.encodeSnapshotFragments(TIMESTAMP);
    int cursor = 0;
    cursor = appendFragment(dst, cursor, svc.snapshotHeaderBuffer(), svc.snapshotHeaderLength());
    cursor = appendFragment(dst, cursor, svc.eventSeqSnapBuffer(), svc.eventSeqSnapLength());
    cursor = appendFragment(dst, cursor, svc.idGenSnapBuffer(), svc.idGenSnapLength());
    cursor = appendFragment(dst, cursor, svc.accountSnapBuffer(), svc.accountSnapLength());
    cursor = appendFragment(dst, cursor, svc.currencySnapBuffer(), svc.currencySnapLength());
    cursor = appendFragment(dst, cursor, svc.riskLimitSnapBuffer(), svc.riskLimitSnapLength());
    cursor = appendFragment(dst, cursor, svc.orderBookSnapBuffer(), svc.orderBookSnapLength());
    return cursor;
  }

  // ---------------------------------------------------------------------------
  // Backpressure + lifecycle
  // ---------------------------------------------------------------------------

  @Test
  void sessionOfferBackpressureRetriedUntilSuccess() {
    final FakeClientSession bpSession = new FakeClientSession();
    bpSession.pendingBackpressures = 2;

    final MutableDirectBuffer buf = new ExpandableArrayBuffer(256);
    final int len =
        encodeNewOrderSingle(
            buf,
            "CL-1",
            "EURUSD",
            SideEnum.Buy,
            OrdTypeEnum.Limit,
            1L * PRICE_SCALE,
            1L * PRICE_SCALE,
            "ACME",
            "USD");
    service.onSessionMessage(bpSession, TIMESTAMP, buf, 0, len, null);
    // The two messages eventually landed despite the back-pressure response on the first two
    // attempts.
    assertEquals(2, bpSession.messages.size());
    // Idler was tickled at least twice (once per retry — two back-pressured attempts).
    assertTrue(cluster.idleCount >= 2);
  }

  @Test
  void lifecycleCallbacksNoThrow() {
    service.onSessionOpen(session, TIMESTAMP);
    service.onSessionClose(session, TIMESTAMP, CloseReason.CLIENT_ACTION);
    service.onTimerEvent(1L, TIMESTAMP);
    service.onRoleChange(Cluster.Role.LEADER);
    service.onTerminate(cluster);
  }

  @Test
  void constructorRejectsRegistryWithDifferentStoreInstances() {
    // Pass a different AccountStore instance than the one registered in the registry. The
    // constructor must fail fast rather than let the service validate orders against one graph
    // while ref-data commands mutate another.
    final AccountStore differentAccountStore = new AccountStore();
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new TradingClusteredService(
                orderIdGen,
                execIdGen,
                orderBook,
                eventSequencer,
                eventJournal,
                differentAccountStore, // different instance than what is registered
                currencyStore,
                riskLimitStore,
                registry));
  }

  @Test
  void sessionClosedWhenOfferRetryExhausts() {
    // Persistent BACK_PRESSURED → after MAX_BACKPRESSURE_RETRY attempts the session must be
    // closed so the cluster framework tears it down. Silent drop would leave the client
    // without ACK / NACK and free it to replay the command.
    final FakeClientSession stuckSession = new FakeClientSession();
    stuckSession.alwaysBackpressured = true;

    final MutableDirectBuffer buf = new ExpandableArrayBuffer(256);
    final int len =
        encodeNewOrderSingle(
            buf,
            "CL-STUCK",
            "EURUSD",
            SideEnum.Buy,
            OrdTypeEnum.Limit,
            1L * PRICE_SCALE,
            1L * PRICE_SCALE,
            "ACME",
            "USD");
    service.onSessionMessage(stuckSession, TIMESTAMP, buf, 0, len, null);
    assertTrue(stuckSession.closed, "session should be closed after retry exhaustion");
  }

  @Test
  void nullCollaboratorsRejected() {
    assertThrows(
        NullPointerException.class,
        () ->
            new TradingClusteredService(
                null,
                execIdGen,
                orderBook,
                eventSequencer,
                eventJournal,
                accountStore,
                currencyStore,
                riskLimitStore,
                registry));
  }

  // ---------------------------------------------------------------------------
  // Decoding helpers
  // ---------------------------------------------------------------------------

  private static void assertTemplateId(final int expected, final byte[] bytes) {
    final MessageHeaderDecoder hd = new MessageHeaderDecoder();
    hd.wrap(new UnsafeBuffer(bytes), 0);
    assertEquals(expected, hd.templateId());
  }

  private static OrderCreatedEventDecoder decodeOrderCreated(final byte[] bytes) {
    final UnsafeBuffer buf = new UnsafeBuffer(bytes);
    final MessageHeaderDecoder hd = new MessageHeaderDecoder();
    hd.wrap(buf, 0);
    final OrderCreatedEventDecoder d = new OrderCreatedEventDecoder();
    d.wrap(buf, MessageHeaderDecoder.ENCODED_LENGTH, hd.blockLength(), hd.version());
    return d;
  }

  private static OrderRejectedEventDecoder decodeOrderRejected(final byte[] bytes) {
    final UnsafeBuffer buf = new UnsafeBuffer(bytes);
    final MessageHeaderDecoder hd = new MessageHeaderDecoder();
    hd.wrap(buf, 0);
    final OrderRejectedEventDecoder d = new OrderRejectedEventDecoder();
    d.wrap(buf, MessageHeaderDecoder.ENCODED_LENGTH, hd.blockLength(), hd.version());
    return d;
  }

  private static ExecutionReportDecoder decodeExecutionReport(final byte[] bytes) {
    final UnsafeBuffer buf = new UnsafeBuffer(bytes);
    final MessageHeaderDecoder hd = new MessageHeaderDecoder();
    hd.wrap(buf, 0);
    final ExecutionReportDecoder d = new ExecutionReportDecoder();
    d.wrap(buf, MessageHeaderDecoder.ENCODED_LENGTH, hd.blockLength(), hd.version());
    return d;
  }

  // ---------------------------------------------------------------------------
  // Test doubles
  // ---------------------------------------------------------------------------

  /** Records every {@code offer(...)} call as a defensive byte[] copy. */
  static final class FakeClientSession implements ClientSession {
    final List<byte[]> messages = new ArrayList<>();
    int pendingBackpressures;
    boolean alwaysBackpressured;
    boolean closed;

    @Override
    public long id() {
      return 42L;
    }

    @Override
    public int responseStreamId() {
      return 0;
    }

    @Override
    public String responseChannel() {
      return "aeron:ipc";
    }

    @Override
    public byte[] encodedPrincipal() {
      return new byte[0];
    }

    @Override
    public void close() {
      closed = true;
    }

    @Override
    public boolean isClosing() {
      return closed;
    }

    @Override
    public long offer(final DirectBuffer buffer, final int offset, final int length) {
      if (alwaysBackpressured) {
        return Publication.BACK_PRESSURED;
      }
      if (pendingBackpressures > 0) {
        pendingBackpressures--;
        return Publication.BACK_PRESSURED;
      }
      final byte[] copy = new byte[length];
      buffer.getBytes(offset, copy);
      messages.add(copy);
      return 1L;
    }

    @Override
    public long offer(final DirectBufferVector[] vectors) {
      return offer(vectors[0].buffer(), vectors[0].offset(), vectors[0].length());
    }

    @Override
    public long tryClaim(final int length, final BufferClaim bufferClaim) {
      throw new UnsupportedOperationException();
    }
  }

  /** Minimal {@link Cluster} double returning a fixed timestamp. */
  static final class FakeCluster implements Cluster {
    private final long time;
    private final IdleStrategy idleStrategy;
    int idleCount;

    FakeCluster(final long time) {
      this.time = time;
      this.idleStrategy =
          new IdleStrategy() {
            @Override
            public void idle(final int workCount) {
              idleCount++;
            }

            @Override
            public void idle() {
              idleCount++;
            }

            @Override
            public void reset() {}
          };
    }

    @Override
    public int memberId() {
      return 0;
    }

    @Override
    public Role role() {
      return Role.LEADER;
    }

    @Override
    public long logPosition() {
      return 0L;
    }

    @Override
    public io.aeron.Aeron aeron() {
      return null;
    }

    @Override
    public ClusteredServiceContainer.Context context() {
      return null;
    }

    @Override
    public ClientSession getClientSession(final long clusterSessionId) {
      return null;
    }

    @Override
    public java.util.Collection<ClientSession> clientSessions() {
      return java.util.List.of();
    }

    @Override
    public void forEachClientSession(final Consumer<? super ClientSession> action) {}

    @Override
    public boolean closeClientSession(final long clusterSessionId) {
      return false;
    }

    @Override
    public long time() {
      return time;
    }

    @Override
    public TimeUnit timeUnit() {
      return TimeUnit.NANOSECONDS;
    }

    @Override
    public boolean scheduleTimer(final long correlationId, final long deadline) {
      return false;
    }

    @Override
    public boolean cancelTimer(final long correlationId) {
      return false;
    }

    @Override
    public long offer(final DirectBuffer buffer, final int offset, final int length) {
      return 0L;
    }

    @Override
    public long offer(final DirectBufferVector[] vectors) {
      return 0L;
    }

    @Override
    public long tryClaim(final int length, final BufferClaim bufferClaim) {
      return 0L;
    }

    @Override
    public IdleStrategy idleStrategy() {
      return idleStrategy;
    }
  }
}
