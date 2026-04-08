package com.trading.engine.cluster;

import static io.aeron.Publication.ADMIN_ACTION;
import static io.aeron.Publication.BACK_PRESSURED;
import static io.aeron.cluster.service.ClientSession.MOCKED_OFFER;

import com.trading.engine.cluster.journal.EventJournal;
import com.trading.engine.cluster.refdata.AccountState;
import com.trading.engine.cluster.refdata.AccountStore;
import com.trading.engine.cluster.refdata.CurrencyStore;
import com.trading.engine.cluster.refdata.ReferenceDataRegistry;
import com.trading.engine.cluster.refdata.RiskLimitStore;
import com.trading.engine.cluster.sequencer.EventSequencer;
import com.trading.engine.messages.sbe.AccountSnapshotDecoder;
import com.trading.engine.messages.sbe.AccountStatusEnum;
import com.trading.engine.messages.sbe.CurrencySnapshotDecoder;
import com.trading.engine.messages.sbe.EventSequencerSnapshotDecoder;
import com.trading.engine.messages.sbe.EventSequencerSnapshotEncoder;
import com.trading.engine.messages.sbe.ExecTypeEnum;
import com.trading.engine.messages.sbe.ExecutionReportEncoder;
import com.trading.engine.messages.sbe.IdGeneratorSnapshotDecoder;
import com.trading.engine.messages.sbe.IdGeneratorSnapshotEncoder;
import com.trading.engine.messages.sbe.MessageHeaderDecoder;
import com.trading.engine.messages.sbe.MessageHeaderEncoder;
import com.trading.engine.messages.sbe.NewOrderSingleDecoder;
import com.trading.engine.messages.sbe.OrdStatusEnum;
import com.trading.engine.messages.sbe.OrdTypeEnum;
import com.trading.engine.messages.sbe.OrderBookSnapshotDecoder;
import com.trading.engine.messages.sbe.OrderCreatedEventEncoder;
import com.trading.engine.messages.sbe.OrderRejectedEventEncoder;
import com.trading.engine.messages.sbe.RejectReasonEnum;
import com.trading.engine.messages.sbe.RiskLimitSnapshotDecoder;
import com.trading.engine.messages.sbe.SideEnum;
import com.trading.engine.messages.sbe.SnapshotTakenDecoder;
import com.trading.engine.messages.sbe.SnapshotTakenEncoder;
import io.aeron.ExclusivePublication;
import io.aeron.Image;
import io.aeron.cluster.codecs.CloseReason;
import io.aeron.cluster.service.ClientSession;
import io.aeron.cluster.service.Cluster;
import io.aeron.cluster.service.ClusteredService;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import java.util.zip.CRC32C;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

/**
 * The keystone cluster service for Wave 4. Wires every deterministic state-machine component from
 * Wave 3 into a single {@link ClusteredService}:
 *
 * <ul>
 *   <li>Ref-data commands (templateIds 11-16) are dispatched to the {@link ReferenceDataRegistry}
 *       and the resulting events are journaled and replied to the client session.
 *   <li>{@code NewOrderSingle} commands are validated against the account master, currency master,
 *       and per-account risk limits; on success a pooled {@link OrderState} is acquired from the
 *       {@link OrderBook}, an {@code OrderCreatedEvent} is journaled, and an {@code
 *       ExecutionReport(New)} is replied to the client session. On failure an {@code
 *       OrderRejectedEvent} is journaled and an {@code ExecutionReport(Rejected)} is replied.
 *   <li>{@link #onTakeSnapshot(ExclusivePublication)} emits a six-fragment envelope: {@code
 *       SnapshotTaken} (header, templateId 200) + {@code EventSequencerSnapshot} (206) + {@code
 *       IdGeneratorSnapshot} (205) + {@code AccountSnapshot} (201) + {@code CurrencySnapshot} (208)
 *       + {@code RiskLimitSnapshot} (209) + {@code OrderBookSnapshot} (202). The header carries a
 *       CRC32C checksum covering the concatenated body bytes in publish order; {@link #onStart}
 *       verifies the checksum before handing control back to the cluster framework.
 * </ul>
 *
 * <p>Phase 1 scope: {@code NewOrderSingle} is only ACKed (no matching, no fills). Follow-up issues
 * add matching, cancel/replace, fills, and the RFQ/position stores.
 *
 * <p><b>Determinism.</b> Every mutation is driven by either (a) a cluster-supplied message and
 * timestamp or (b) a cluster-supplied snapshot fragment. No wall-clock, no randomness, no heap
 * allocation on the hot path.
 *
 * <p><b>Threading.</b> Not thread-safe — single-threaded cluster duty cycle only.
 */
public final class TradingClusteredService implements ClusteredService {

  static final int NOT_HANDLED = -1;

  /**
   * Maximum number of times {@link #offerToSession} / {@link #offerFragment} will retry a
   * back-pressured offer before giving up (and closing the session / throwing on the snapshot
   * path). Bounded tightly so even Aeron's default {@code BackoffIdleStrategy} — whose max park is
   * ~1 ms — keeps the total retry wall-time (≈ 128 ms worst case) well under a 500 ms cluster
   * heartbeat budget, so a single slow session can never trigger spurious leader elections. A
   * follow-up issue will move session egress off the duty cycle entirely and remove this retry
   * loop.
   */
  private static final int MAX_BACKPRESSURE_RETRY = 128;

  /**
   * Snapshot envelope version understood by this service. Bumped whenever the envelope layout
   * (fragment set, header fields, checksum algorithm) changes in a non-forward-compatible way.
   */
  private static final long SUPPORTED_SNAPSHOT_VERSION = 1L;

  /** Number of body fragments in a well-formed snapshot envelope. */
  private static final int SNAPSHOT_STORE_COUNT = 6;

  /**
   * Maximum consecutive empty polls tolerated during snapshot reassembly in {@link #onStart} before
   * we give up and throw. Protects cluster startup from hanging indefinitely on a corrupted image
   * that never signals end-of-stream. Tuned generously (1M polls with the cluster idle strategy is
   * several seconds of wall time at worst) so we never trip on a normal snapshot.
   */
  private static final int MAX_SNAPSHOT_EMPTY_POLLS = 1_000_000;

  // ===== Collaborators =====
  private final IdGenerator orderIdGen;
  private final IdGenerator execIdGen;
  private final OrderBook orderBook;
  private final EventSequencer eventSequencer;
  private final EventJournal eventJournal;
  private final AccountStore accountStore;
  private final CurrencyStore currencyStore;
  private final RiskLimitStore riskLimitStore;
  private final ReferenceDataRegistry referenceDataRegistry;

  // ===== Pre-allocated SBE flyweights (zero allocation on the hot path) =====
  private final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();
  // Dedicated decoder used exclusively by {@link #appendToJournal} so the inbound command's
  // header-wrap state on {@link #headerDecoder} is never clobbered mid-flow.
  private final MessageHeaderDecoder journalHeaderDecoder = new MessageHeaderDecoder();
  private final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
  private final NewOrderSingleDecoder nosDecoder = new NewOrderSingleDecoder();
  private final ExecutionReportEncoder erEncoder = new ExecutionReportEncoder();
  private final OrderCreatedEventEncoder orderCreatedEncoder = new OrderCreatedEventEncoder();
  private final OrderRejectedEventEncoder orderRejectedEncoder = new OrderRejectedEventEncoder();

  // Snapshot encoders / decoders.
  private final SnapshotTakenEncoder snapshotTakenEncoder = new SnapshotTakenEncoder();
  private final SnapshotTakenDecoder snapshotTakenDecoder = new SnapshotTakenDecoder();
  private final EventSequencerSnapshotEncoder eventSeqSnapEncoder =
      new EventSequencerSnapshotEncoder();
  private final EventSequencerSnapshotDecoder eventSeqSnapDecoder =
      new EventSequencerSnapshotDecoder();
  private final IdGeneratorSnapshotEncoder idGenSnapEncoder = new IdGeneratorSnapshotEncoder();
  private final IdGeneratorSnapshotDecoder idGenSnapDecoder = new IdGeneratorSnapshotDecoder();

  // ===== Pre-allocated scratch buffers =====
  // Sized at 8 KiB so the variable-length text/reason fields on OrderRejectedEvent and
  // ExecutionReport can never exceed the fixed-size encoder window on any realistic path.
  // Allocated once at construction — no hot-path allocation.
  private final UnsafeBuffer egressBuffer = new UnsafeBuffer(new byte[8 * 1024]);
  private final UnsafeBuffer refDataEventBuffer = new UnsafeBuffer(new byte[8 * 1024]);
  private final byte[] orderIdScratch = new byte[OrderState.ORDER_ID_LENGTH];
  private final byte[] execIdScratch = new byte[OrderState.ORDER_ID_LENGTH];
  private final byte[] clOrdIdScratch = new byte[OrderState.CL_ORD_ID_LENGTH];
  private final byte[] symbolScratch = new byte[OrderState.SYMBOL_LENGTH];
  private final byte[] accountCodeScratch = new byte[AccountStore.MAX_ACCOUNT_CODE_LENGTH];
  // Current-NewOrderSingle currency bytes. Extracted once at the top of handleNewOrderSingle
  // and read by both the happy-path encoders and the reject path so the rejected
  // ExecutionReport can ship the correct currency (not stale bytes left in egressBuffer from
  // the previous message).
  private byte currencyByte0;
  private byte currencyByte1;
  private byte currencyByte2;
  // Reusable UnsafeBuffer wrappers around orderIdScratch / execIdScratch so the hot-path
  // IdGenerator.nextInto(...) call does not allocate on every NewOrderSingle.
  private final UnsafeBuffer orderIdScratchBuffer = new UnsafeBuffer(orderIdScratch);
  private final UnsafeBuffer execIdScratchBuffer = new UnsafeBuffer(execIdScratch);

  // ===== Snapshot staging buffers =====
  private final MutableDirectBuffer snapshotHeaderBuf = new ExpandableArrayBuffer(64);
  private final MutableDirectBuffer eventSeqSnapBuf = new ExpandableArrayBuffer(64);
  private final MutableDirectBuffer idGenSnapBuf = new ExpandableArrayBuffer(256);
  private final MutableDirectBuffer accountSnapBuf = new ExpandableArrayBuffer(64 * 1024);
  private final MutableDirectBuffer currencySnapBuf = new ExpandableArrayBuffer(8 * 1024);
  private final MutableDirectBuffer riskLimitSnapBuf = new ExpandableArrayBuffer(64 * 1024);
  private final MutableDirectBuffer orderBookSnapBuf = new ExpandableArrayBuffer(8 * 1024 * 1024);

  // Lengths populated by encodeSnapshotFragments().
  private int snapshotHeaderLen;
  private int eventSeqSnapLen;
  private int idGenSnapLen;
  private int accountSnapLen;
  private int currencySnapLen;
  private int riskLimitSnapLen;
  private int orderBookSnapLen;

  // Reassembly buffer for onStart — captures the concatenated fragments delivered by Image.poll
  // so we can walk them in a single pass inside loadSnapshot(). Grown on demand at startup.
  private final MutableDirectBuffer snapshotReassemblyBuf =
      new ExpandableArrayBuffer(16 * 1024 * 1024);
  private int snapshotReassemblyOffset;

  private final CRC32C crc = new CRC32C();

  // Snapshot-walk bookkeeping — mutated only inside loadSnapshot / applySnapshotFragment.
  private boolean eventSeqFragmentSeen;
  private boolean idGenFragmentSeen;
  private boolean orderBookFragmentSeen;
  private boolean accountFragmentSeen;
  private boolean currencyFragmentSeen;
  private boolean riskLimitFragmentSeen;
  private boolean orderIdGenRestored;
  private boolean execIdGenRestored;

  private Cluster cluster;

  public TradingClusteredService(
      final IdGenerator orderIdGen,
      final IdGenerator execIdGen,
      final OrderBook orderBook,
      final EventSequencer eventSequencer,
      final EventJournal eventJournal,
      final AccountStore accountStore,
      final CurrencyStore currencyStore,
      final RiskLimitStore riskLimitStore,
      final ReferenceDataRegistry referenceDataRegistry) {
    this.orderIdGen = notNull(orderIdGen, "orderIdGen");
    this.execIdGen = notNull(execIdGen, "execIdGen");
    this.orderBook = notNull(orderBook, "orderBook");
    this.eventSequencer = notNull(eventSequencer, "eventSequencer");
    this.eventJournal = notNull(eventJournal, "eventJournal");
    this.accountStore = notNull(accountStore, "accountStore");
    this.currencyStore = notNull(currencyStore, "currencyStore");
    this.riskLimitStore = notNull(riskLimitStore, "riskLimitStore");
    this.referenceDataRegistry = notNull(referenceDataRegistry, "referenceDataRegistry");
    // Consistency check: the registry must be backed by the same concrete store instances we
    // hold a direct reference to. Otherwise NewOrderSingle validation would read from one
    // object graph while ref-data commands mutate another, or a snapshot restore could put the
    // registry and the direct references out of sync. Fail fast at construction time.
    requireSameStore(
        referenceDataRegistry, AccountStore.SNAPSHOT_TEMPLATE_ID, accountStore, "accountStore");
    requireSameStore(
        referenceDataRegistry, CurrencyStore.SNAPSHOT_TEMPLATE_ID, currencyStore, "currencyStore");
    requireSameStore(
        referenceDataRegistry,
        RiskLimitStore.SNAPSHOT_TEMPLATE_ID,
        riskLimitStore,
        "riskLimitStore");
  }

  private static void requireSameStore(
      final ReferenceDataRegistry registry,
      final int snapshotTemplateId,
      final Object expected,
      final String name) {
    final Object registered = registry.storeForSnapshotTemplateId(snapshotTemplateId);
    if (registered != expected) {
      throw new IllegalArgumentException(
          name
              + " must be the same instance registered in the ReferenceDataRegistry (templateId "
              + snapshotTemplateId
              + ")");
    }
  }

  private static <T> T notNull(final T value, final String name) {
    if (value == null) {
      throw new NullPointerException(name + " must not be null");
    }
    return value;
  }

  // ===========================================================================
  // ClusteredService lifecycle
  // ===========================================================================

  @Override
  public void onStart(final Cluster cluster, final Image snapshotImage) {
    this.cluster = cluster;
    if (snapshotImage == null) {
      return;
    }
    // Reassemble the image into a single contiguous buffer, then walk it once through
    // loadSnapshot(). Aeron Cluster delivers snapshot fragments in the order they were offered,
    // so the concatenated bytes match the publish order.
    snapshotReassemblyOffset = 0;
    final FragmentHandler appender =
        (final DirectBuffer buffer, final int offset, final int length, final Header header) -> {
          snapshotReassemblyBuf.putBytes(snapshotReassemblyOffset, buffer, offset, length);
          snapshotReassemblyOffset += length;
        };
    // Drain until Aeron signals end-of-stream. An empty poll is a transient "no fragments
    // queued yet" signal — idle the cluster's IdleStrategy and try again rather than exiting
    // early (which would leave a half-restored snapshot). Bound the empty-poll count so a
    // corrupted image that never signals end-of-stream fails startup loudly rather than
    // hanging the cluster duty cycle forever.
    int emptyPolls = 0;
    while (!snapshotImage.isEndOfStream()) {
      if (snapshotImage.poll(appender, Integer.MAX_VALUE) == 0) {
        if (++emptyPolls > MAX_SNAPSHOT_EMPTY_POLLS) {
          throw new IllegalStateException(
              "snapshot reassembly stalled after "
                  + MAX_SNAPSHOT_EMPTY_POLLS
                  + " consecutive empty polls — image may be corrupted");
        }
        cluster.idleStrategy().idle();
      } else {
        emptyPolls = 0;
      }
    }
    if (snapshotReassemblyOffset > 0) {
      loadSnapshot(snapshotReassemblyBuf, 0, snapshotReassemblyOffset);
    }
  }

  @Override
  public void onSessionOpen(final ClientSession session, final long timestamp) {
    // Phase 1: no session state. Future: authenticate, wire session → trader / desk mapping.
  }

  @Override
  public void onSessionClose(
      final ClientSession session, final long timestamp, final CloseReason closeReason) {
    // Phase 1: no session state to tear down.
  }

  @Override
  public void onSessionMessage(
      final ClientSession session,
      final long timestamp,
      final DirectBuffer buffer,
      final int offset,
      final int length,
      final Header header) {
    headerDecoder.wrap(buffer, offset);
    final int templateId = headerDecoder.templateId();

    // 1. Try ref-data command dispatch (templateIds 11..16).
    final long refDataSeqNo = eventSequencer.currentSequence() + 1L;
    final int refDataEventLen =
        referenceDataRegistry.dispatchCommand(
            headerDecoder, buffer, offset, length, refDataEventBuffer, 0, refDataSeqNo, timestamp);
    if (refDataEventLen > 0) {
      // Commit the sequence number (currentSequence() advances).
      eventSequencer.nextSequence();
      // Append to journal for projection catch-up; read the event templateId from the emitted
      // buffer rather than the command header.
      appendToJournal(refDataSeqNo, refDataEventBuffer, 0, refDataEventLen);
      offerToSession(session, refDataEventBuffer, 0, refDataEventLen);
      return;
    }
    // refDataEventLen == NOT_HANDLED → fall through to trading command dispatch.

    switch (templateId) {
      case NewOrderSingleDecoder.TEMPLATE_ID ->
          handleNewOrderSingle(session, timestamp, buffer, offset);
      default -> {
        // Unknown templateId — silently drop in Phase 1. A future PR will add structured logging
        // via GFLog.
      }
    }
  }

  @Override
  public void onTimerEvent(final long correlationId, final long timestamp) {
    // Phase 1: no scheduled timers. Future: RFQ expiry, daily rollover, etc.
  }

  @Override
  public void onTakeSnapshot(final ExclusivePublication snapshotPublication) {
    encodeSnapshotFragments(cluster == null ? 0L : cluster.time());
    offerFragment(snapshotPublication, snapshotHeaderBuf, snapshotHeaderLen);
    offerFragment(snapshotPublication, eventSeqSnapBuf, eventSeqSnapLen);
    offerFragment(snapshotPublication, idGenSnapBuf, idGenSnapLen);
    offerFragment(snapshotPublication, accountSnapBuf, accountSnapLen);
    offerFragment(snapshotPublication, currencySnapBuf, currencySnapLen);
    offerFragment(snapshotPublication, riskLimitSnapBuf, riskLimitSnapLen);
    offerFragment(snapshotPublication, orderBookSnapBuf, orderBookSnapLen);
  }

  @Override
  public void onRoleChange(final Cluster.Role newRole) {
    // Phase 1: no role-specific behaviour. Future: warm/cold projection rebuild on LEADER promote.
  }

  @Override
  public void onTerminate(final Cluster cluster) {
    // Phase 1: nothing to release; buffers are on-heap and GC-managed.
  }

  // ===========================================================================
  // NewOrderSingle handler
  // ===========================================================================

  private void handleNewOrderSingle(
      final ClientSession session,
      final long timestamp,
      final DirectBuffer buffer,
      final int offset) {
    nosDecoder.wrap(
        buffer,
        offset + MessageHeaderDecoder.ENCODED_LENGTH,
        headerDecoder.blockLength(),
        headerDecoder.version());

    // Extract primitive fields + copy char arrays into scratch buffers.
    final long orderQty = nosDecoder.orderQty();
    final var ordType = nosDecoder.ordType();
    final long price = nosDecoder.price();
    final var side = nosDecoder.side();
    nosDecoder.getClOrdId(clOrdIdScratch, 0);
    nosDecoder.getSymbol(symbolScratch, 0);
    final int symbolLen = trimTrailingZeros(symbolScratch, OrderState.SYMBOL_LENGTH);
    nosDecoder.getAccountCode(accountCodeScratch, 0);
    final int accountCodeLen =
        trimTrailingZeros(accountCodeScratch, AccountStore.MAX_ACCOUNT_CODE_LENGTH);
    final byte ccy0 = nosDecoder.currency(0);
    final byte ccy1 = nosDecoder.currency(1);
    final byte ccy2 = nosDecoder.currency(2);
    // Stash on the service so emitOrderRejected can write them into the ER reject — the shared
    // egressBuffer would otherwise leak the previous message's currency bytes.
    currencyByte0 = ccy0;
    currencyByte1 = ccy1;
    currencyByte2 = ccy2;

    final var account =
        validateNewOrder(
            session,
            timestamp,
            side,
            ordType,
            orderQty,
            price,
            symbolLen,
            accountCodeLen,
            ccy0,
            ccy1,
            ccy2);
    if (account == null) {
      return;
    }
    admitNewOrder(session, timestamp, account, side, ordType, orderQty, price, ccy0, ccy1, ccy2);
  }

  /**
   * Run every pre-trade validation for a decoded NewOrderSingle. On the first failure, emits the
   * corresponding {@code OrderRejectedEvent} + {@code ExecutionReport(Rejected)} and returns {@code
   * null}. On success returns the resolved {@link AccountState} — the caller reuses it for the
   * happy path to avoid a second lookup.
   */
  private AccountState validateNewOrder(
      final ClientSession session,
      final long timestamp,
      final SideEnum side,
      final OrdTypeEnum ordType,
      final long orderQty,
      final long price,
      final int symbolLen,
      final int accountCodeLen,
      final byte ccy0,
      final byte ccy1,
      final byte ccy2) {
    if (symbolLen == 0) {
      emitOrderRejected(
          session, timestamp, side, RejectReasonEnum.UnknownSymbol, "symbol is empty");
      return null;
    }
    if (orderQty <= 0L) {
      emitOrderRejected(
          session, timestamp, side, RejectReasonEnum.InvalidQuantity, "orderQty must be > 0");
      return null;
    }
    if (ordType == OrdTypeEnum.Limit && price <= 0L) {
      emitOrderRejected(
          session, timestamp, side, RejectReasonEnum.InvalidPrice, "limit price must be > 0");
      return null;
    }
    if (accountCodeLen == 0) {
      emitOrderRejected(
          session, timestamp, side, RejectReasonEnum.AccountNotFound, "accountCode is empty");
      return null;
    }
    final var account = accountStore.getByCodeBytes(accountCodeScratch, 0, accountCodeLen);
    if (account == null) {
      emitOrderRejected(
          session,
          timestamp,
          side,
          RejectReasonEnum.AccountNotFound,
          "account not in AccountStore");
      return null;
    }
    if (account.status() != AccountStatusEnum.Active) {
      emitOrderRejected(
          session, timestamp, side, RejectReasonEnum.AccountSuspended, "account not active");
      return null;
    }
    if (!account.canTrade()) {
      emitOrderRejected(
          session,
          timestamp,
          side,
          RejectReasonEnum.AccountNoTradePermission,
          "account lacks CAN_TRADE");
      return null;
    }
    final int ccyPacked = CurrencyStore.packCodeOrInvalid(ccy0, ccy1, ccy2);
    if (ccyPacked == CurrencyStore.INVALID_PACKED_CODE) {
      emitOrderRejected(
          session,
          timestamp,
          side,
          RejectReasonEnum.InvalidCurrencyCode,
          "currency is not 3 uppercase ASCII letters");
      return null;
    }
    if (!currencyStore.contains(ccyPacked)) {
      emitOrderRejected(
          session,
          timestamp,
          side,
          RejectReasonEnum.UnknownCurrency,
          "currency not in CurrencyStore");
      return null;
    }
    final var riskLimit = riskLimitStore.get(account.accountId());
    if (riskLimit != null && riskLimit.maxOrderSize() > 0L && orderQty > riskLimit.maxOrderSize()) {
      emitOrderRejected(
          session,
          timestamp,
          side,
          RejectReasonEnum.OrderExceedsMaxSize,
          "orderQty exceeds account maxOrderSize");
      return null;
    }
    return account;
  }

  /**
   * Happy path: generate ids, acquire a pooled {@link OrderState}, populate it from the decoded
   * command, and emit {@code OrderCreatedEvent} (journaled) + {@code ExecutionReport(New)} (session
   * only). On pool exhaustion emits a {@code BookFull} rejection instead.
   */
  private void admitNewOrder(
      final ClientSession session,
      final long timestamp,
      final AccountState account,
      final SideEnum side,
      final OrdTypeEnum ordType,
      final long orderQty,
      final long price,
      final byte ccy0,
      final byte ccy1,
      final byte ccy2) {
    // Generate ids. nextInto advances the counter; currentCounter() after the call gives the
    // counter value that was just assigned — we use it as the primitive OrderBook key. The
    // scratch wrappers (orderIdScratchBuffer / execIdScratchBuffer) are pre-allocated fields so
    // this path performs zero heap allocation.
    orderIdGen.nextInto(orderIdScratchBuffer, 0);
    final long orderKey = orderIdGen.currentCounter();
    execIdGen.nextInto(execIdScratchBuffer, 0);

    final var state = orderBook.acquire(orderKey);
    if (state == null) {
      // Pool exhaustion — emit BookFull reject.
      emitOrderRejected(
          session, timestamp, side, RejectReasonEnum.BookFull, "order book pool exhausted");
      return;
    }
    state.setOrderIdBytes(orderIdScratch, 0);
    state.setClOrdIdBytes(clOrdIdScratch, 0);
    state.setSymbolBytes(symbolScratch, 0);
    state.setAccountId(account.accountId());
    state.setSide(side);
    state.setOrdType(ordType);
    state.setTimeInForce(nosDecoder.timeInForce());
    state.setPrice(price);
    state.setOrderQty(orderQty);
    state.setLeavesQty(orderQty);
    state.setCumQty(0L);
    state.setOrdStatus(OrdStatusEnum.New);
    state.setTransactTime(timestamp);

    // Emit OrderCreatedEvent → journal + session.
    final long seqNo = eventSequencer.nextSequence();
    final int eventLen = encodeOrderCreatedEvent(seqNo, timestamp, state, ccy0, ccy1, ccy2);
    appendToJournal(seqNo, egressBuffer, 0, eventLen);
    offerToSession(session, egressBuffer, 0, eventLen);

    // Emit ExecutionReport(New) ACK → session only (not journaled — ER is a transport-level
    // reply, not a domain event).
    final int erLen = encodeExecutionReportNew(timestamp, state, ccy0, ccy1, ccy2);
    offerToSession(session, egressBuffer, 0, erLen);
  }

  private int encodeOrderCreatedEvent(
      final long seqNo,
      final long timestamp,
      final OrderState state,
      final byte ccy0,
      final byte ccy1,
      final byte ccy2) {
    orderCreatedEncoder.wrapAndApplyHeader(egressBuffer, 0, headerEncoder);
    orderCreatedEncoder.sequenceNumber(seqNo);
    orderCreatedEncoder.timestamp(timestamp);
    state.copyOrderIdTo(orderIdScratch, 0);
    orderCreatedEncoder.putOrderId(orderIdScratch, 0);
    state.copyClOrdIdTo(clOrdIdScratch, 0);
    orderCreatedEncoder.putClOrdId(clOrdIdScratch, 0);
    state.copySymbolTo(symbolScratch, 0);
    orderCreatedEncoder.putSymbol(symbolScratch, 0);
    orderCreatedEncoder.side(state.side());
    orderCreatedEncoder.ordType(state.ordType());
    orderCreatedEncoder.price(state.price());
    orderCreatedEncoder.orderQty(state.orderQty());
    orderCreatedEncoder.putCurrency(ccy0, ccy1, ccy2);
    return MessageHeaderEncoder.ENCODED_LENGTH + orderCreatedEncoder.encodedLength();
  }

  private int encodeExecutionReportNew(
      final long timestamp,
      final OrderState state,
      final byte ccy0,
      final byte ccy1,
      final byte ccy2) {
    erEncoder.wrapAndApplyHeader(egressBuffer, 0, headerEncoder);
    state.copyOrderIdTo(orderIdScratch, 0);
    erEncoder.putOrderId(orderIdScratch, 0);
    erEncoder.putExecId(execIdScratch, 0);
    state.copyClOrdIdTo(clOrdIdScratch, 0);
    erEncoder.putClOrdId(clOrdIdScratch, 0);
    erEncoder.execType(ExecTypeEnum.New);
    erEncoder.ordStatus(OrdStatusEnum.New);
    state.copySymbolTo(symbolScratch, 0);
    erEncoder.putSymbol(symbolScratch, 0);
    erEncoder.side(state.side());
    erEncoder.leavesQty(state.leavesQty());
    erEncoder.cumQty(state.cumQty());
    erEncoder.avgPx(0L);
    erEncoder.transactTime(timestamp);
    erEncoder.putCurrency(ccy0, ccy1, ccy2);
    return MessageHeaderEncoder.ENCODED_LENGTH + erEncoder.encodedLength();
  }

  private void emitOrderRejected(
      final ClientSession session,
      final long timestamp,
      final SideEnum side,
      final RejectReasonEnum reason,
      final String text) {
    final long seqNo = eventSequencer.nextSequence();
    orderRejectedEncoder.wrapAndApplyHeader(egressBuffer, 0, headerEncoder);
    orderRejectedEncoder.sequenceNumber(seqNo);
    orderRejectedEncoder.timestamp(timestamp);
    orderRejectedEncoder.putClOrdId(clOrdIdScratch, 0);
    // Symbol may be zero-padded from the decoder; pass the scratch verbatim.
    orderRejectedEncoder.putSymbol(symbolScratch, 0);
    orderRejectedEncoder.side(side);
    orderRejectedEncoder.rejectReason(reason);
    // Account code may be empty — still ship the 16-byte scratch (zero-padded tail is valid SBE).
    orderRejectedEncoder.putAccountCode(accountCodeScratch, 0);
    orderRejectedEncoder.text(text);
    final int eventLen = MessageHeaderEncoder.ENCODED_LENGTH + orderRejectedEncoder.encodedLength();
    appendToJournal(seqNo, egressBuffer, 0, eventLen);
    offerToSession(session, egressBuffer, 0, eventLen);

    // Transport-level ExecutionReport(Rejected) ACK.
    erEncoder.wrapAndApplyHeader(egressBuffer, 0, headerEncoder);
    // OrderID is unassigned for a reject — leave the 20-byte scratch zero-filled.
    java.util.Arrays.fill(orderIdScratch, (byte) 0);
    erEncoder.putOrderId(orderIdScratch, 0);
    java.util.Arrays.fill(execIdScratch, (byte) 0);
    erEncoder.putExecId(execIdScratch, 0);
    erEncoder.putClOrdId(clOrdIdScratch, 0);
    erEncoder.execType(ExecTypeEnum.Rejected);
    erEncoder.ordStatus(OrdStatusEnum.Rejected);
    erEncoder.putSymbol(symbolScratch, 0);
    erEncoder.side(side);
    erEncoder.leavesQty(0L);
    erEncoder.cumQty(0L);
    erEncoder.avgPx(0L);
    erEncoder.transactTime(timestamp);
    // Always write the current NOS's currency bytes so the rejected ER never leaks stale
    // currency bytes from the previous message in the shared egressBuffer.
    erEncoder.putCurrency(currencyByte0, currencyByte1, currencyByte2);
    erEncoder.text(text);
    final int erLen = MessageHeaderEncoder.ENCODED_LENGTH + erEncoder.encodedLength();
    offerToSession(session, egressBuffer, 0, erLen);
  }

  // ===========================================================================
  // Journal + session offer helpers
  // ===========================================================================

  private void appendToJournal(
      final long seqNo, final DirectBuffer src, final int srcOffset, final int srcLength) {
    // Read the event's templateId from the SBE header at src[srcOffset..] so projections can
    // dispatch by type without re-wrapping the header themselves. Uses the dedicated
    // journalHeaderDecoder so the inbound command's headerDecoder wrap state is preserved.
    journalHeaderDecoder.wrap(src, srcOffset);
    final int eventType = journalHeaderDecoder.templateId();
    eventJournal.append(seqNo, eventType, src, srcOffset, srcLength);
  }

  private void offerToSession(
      final ClientSession session, final DirectBuffer src, final int offset, final int length) {
    if (session == null) {
      return; // Unit tests may pass null for the unused ref-data session case.
    }
    // Phase-1 back-pressure strategy: a bounded retry on the duty-cycle thread. A single slow
    // client can cause head-of-line blocking for other sessions while we retry; a follow-up
    // issue will move session egress off the duty cycle. Crucially, if retries exhaust or the
    // session returns a non-retryable result (NOT_CONNECTED / CLOSED / MAX_POSITION_EXCEEDED),
    // we MUST NOT silently drop the reply — the underlying event is already journaled, so a
    // client that does not see an ACK could safely replay a duplicate business command. Close
    // the session instead, which forces the client to reconnect and resync from the journal.
    for (int attempt = 0; attempt < MAX_BACKPRESSURE_RETRY; attempt++) {
      final long result = session.offer(src, offset, length);
      if (result >= 0L || result == MOCKED_OFFER) {
        return;
      }
      if (result == BACK_PRESSURED || result == ADMIN_ACTION) {
        if (cluster != null) {
          cluster.idleStrategy().idle();
        }
        continue;
      }
      // Non-retryable — quarantine the session so the cluster framework tears it down rather
      // than leaving the client in an inconsistent state.
      session.close();
      return;
    }
    // Retry exhausted on persistent BACK_PRESSURED / ADMIN_ACTION — same treatment.
    session.close();
  }

  private void offerFragment(
      final ExclusivePublication pub, final MutableDirectBuffer buf, final int length) {
    if (pub == null) {
      return; // Test path uses the pre-encoded fragments directly from the scratch buffers.
    }
    // Unlike offerToSession, a failed snapshot offer is non-recoverable: a truncated snapshot
    // leaves the cluster unable to recover on restart. Throw on any non-retryable return code
    // (NOT_CONNECTED / CLOSED / MAX_POSITION_EXCEEDED) and on retry exhaustion so the cluster
    // framework surfaces the failure rather than silently shipping a corrupted snapshot.
    for (int attempt = 0; attempt < MAX_BACKPRESSURE_RETRY; attempt++) {
      final long result = pub.offer(buf, 0, length);
      if (result >= 0L) {
        return;
      }
      if (result == BACK_PRESSURED || result == ADMIN_ACTION) {
        if (cluster != null) {
          cluster.idleStrategy().idle();
        }
        continue;
      }
      throw new IllegalStateException("snapshot fragment offer failed with result " + result);
    }
    throw new IllegalStateException(
        "snapshot fragment offer retry exhausted after " + MAX_BACKPRESSURE_RETRY + " attempts");
  }

  // ===========================================================================
  // Snapshot encode (package-private for tests)
  // ===========================================================================

  /**
   * Encode every snapshot fragment into the per-store staging buffers and populate the {@code
   * *SnapLen} fields. After this returns, the staging buffers hold (in publish order):
   *
   * <pre>
   *   [snapshotHeaderBuf][eventSeqSnapBuf][idGenSnapBuf][accountSnapBuf][currencySnapBuf]
   *   [riskLimitSnapBuf][orderBookSnapBuf]
   * </pre>
   *
   * <p>The header's {@code checksum} field is a CRC32C over the six body fragments concatenated in
   * publish order (the header itself is not covered, which matches the exchange-core idiom —
   * checksum validates what follows).
   */
  void encodeSnapshotFragments(final long snapshotTimestamp) {
    // 1. EventSequencer — SBE message is one long field (next-sequence-to-assign).
    eventSeqSnapEncoder.wrapAndApplyHeader(eventSeqSnapBuf, 0, headerEncoder);
    eventSeqSnapEncoder.nextSequence(eventSequencer.currentSequence() + 1L);
    eventSeqSnapLen = MessageHeaderEncoder.ENCODED_LENGTH + eventSeqSnapEncoder.encodedLength();

    // 2. IdGeneratorSnapshot — two entries (ORD, EXE).
    idGenSnapEncoder.wrapAndApplyHeader(idGenSnapBuf, 0, headerEncoder);
    final var idGenGroup = idGenSnapEncoder.noGeneratorsCount(2);
    idGenGroup.next();
    idGenGroup.prefix(orderIdGen.prefix());
    idGenGroup.counter(orderIdGen.currentCounter());
    idGenGroup.next();
    idGenGroup.prefix(execIdGen.prefix());
    idGenGroup.counter(execIdGen.currentCounter());
    idGenSnapLen = MessageHeaderEncoder.ENCODED_LENGTH + idGenSnapEncoder.encodedLength();

    // 3-5. Ref-data stores — each returns the total bytes including header.
    accountSnapLen = accountStore.snapshotTo(accountSnapBuf, 0);
    currencySnapLen = currencyStore.snapshotTo(currencySnapBuf, 0);
    riskLimitSnapLen = riskLimitStore.snapshotTo(riskLimitSnapBuf, 0);

    // 6. OrderBookSnapshot.
    orderBookSnapLen = orderBook.snapshotTo(orderBookSnapBuf, 0);

    // CRC32C over the six body fragments in publish order.
    crc.reset();
    crc.update(eventSeqSnapBuf.byteArray(), 0, eventSeqSnapLen);
    crc.update(idGenSnapBuf.byteArray(), 0, idGenSnapLen);
    crc.update(accountSnapBuf.byteArray(), 0, accountSnapLen);
    crc.update(currencySnapBuf.byteArray(), 0, currencySnapLen);
    crc.update(riskLimitSnapBuf.byteArray(), 0, riskLimitSnapLen);
    crc.update(orderBookSnapBuf.byteArray(), 0, orderBookSnapLen);
    final int checksum = (int) crc.getValue();

    final long totalBody =
        (long) eventSeqSnapLen
            + idGenSnapLen
            + accountSnapLen
            + currencySnapLen
            + riskLimitSnapLen
            + orderBookSnapLen;

    // Finally, encode the SnapshotTaken header.
    snapshotTakenEncoder.wrapAndApplyHeader(snapshotHeaderBuf, 0, headerEncoder);
    snapshotTakenEncoder.lastSequenceNumber(eventSequencer.currentSequence());
    snapshotTakenEncoder.snapshotTimestamp(snapshotTimestamp);
    snapshotTakenEncoder.snapshotVersion(SUPPORTED_SNAPSHOT_VERSION);
    snapshotTakenEncoder.storeCount((short) SNAPSHOT_STORE_COUNT);
    snapshotTakenEncoder.totalByteLength(totalBody);
    // checksum is stored as an unsigned 32-bit in the SBE schema; widen via masked long.
    snapshotTakenEncoder.checksum(Integer.toUnsignedLong(checksum));
    snapshotHeaderLen = MessageHeaderEncoder.ENCODED_LENGTH + snapshotTakenEncoder.encodedLength();
  }

  // Diagnostic accessors for tests — the lengths are populated by encodeSnapshotFragments().
  int snapshotHeaderLength() {
    return snapshotHeaderLen;
  }

  int eventSeqSnapLength() {
    return eventSeqSnapLen;
  }

  int idGenSnapLength() {
    return idGenSnapLen;
  }

  int accountSnapLength() {
    return accountSnapLen;
  }

  int currencySnapLength() {
    return currencySnapLen;
  }

  int riskLimitSnapLength() {
    return riskLimitSnapLen;
  }

  int orderBookSnapLength() {
    return orderBookSnapLen;
  }

  MutableDirectBuffer snapshotHeaderBuffer() {
    return snapshotHeaderBuf;
  }

  MutableDirectBuffer eventSeqSnapBuffer() {
    return eventSeqSnapBuf;
  }

  MutableDirectBuffer idGenSnapBuffer() {
    return idGenSnapBuf;
  }

  MutableDirectBuffer accountSnapBuffer() {
    return accountSnapBuf;
  }

  MutableDirectBuffer currencySnapBuffer() {
    return currencySnapBuf;
  }

  MutableDirectBuffer riskLimitSnapBuffer() {
    return riskLimitSnapBuf;
  }

  MutableDirectBuffer orderBookSnapBuffer() {
    return orderBookSnapBuf;
  }

  // ===========================================================================
  // Snapshot restore (package-private for tests)
  // ===========================================================================

  /**
   * Walk a single contiguous buffer containing all seven snapshot fragments in publish order and
   * apply them to the live state. Verifies the CRC32C over the six body fragments against the
   * checksum embedded in the {@code SnapshotTaken} header.
   *
   * <p>Test helper. Production uses {@link #onStart(Cluster, Image)} which reassembles the image
   * and then calls this method.
   *
   * @throws IllegalStateException if the first fragment is not {@code SnapshotTaken}, or the CRC
   *     does not match, or an unknown fragment templateId is encountered
   */
  void loadSnapshot(final DirectBuffer src, final int offset, final int length) {
    // 1. First fragment MUST be SnapshotTaken — captures expected body length + checksum.
    headerDecoder.wrap(src, offset);
    if (headerDecoder.templateId() != SnapshotTakenDecoder.TEMPLATE_ID) {
      throw new IllegalStateException(
          "snapshot must begin with SnapshotTaken (200), got templateId "
              + headerDecoder.templateId());
    }
    snapshotTakenDecoder.wrap(
        src,
        offset + MessageHeaderDecoder.ENCODED_LENGTH,
        headerDecoder.blockLength(),
        headerDecoder.version());
    final long snapshotVersion = snapshotTakenDecoder.snapshotVersion();
    if (snapshotVersion != SUPPORTED_SNAPSHOT_VERSION) {
      throw new IllegalStateException(
          "unsupported snapshotVersion "
              + snapshotVersion
              + ", only "
              + SUPPORTED_SNAPSHOT_VERSION
              + " is supported");
    }
    final int expectedStoreCount = snapshotTakenDecoder.storeCount();
    final long expectedBodyLength = snapshotTakenDecoder.totalByteLength();
    final long expectedChecksum = snapshotTakenDecoder.checksum();
    // Use the wire block length from the header (not the compile-time constant) so the walk
    // survives a future forward-compatible schema extension of SnapshotTaken.
    final int headerFragmentLen = MessageHeaderDecoder.ENCODED_LENGTH + headerDecoder.blockLength();
    final int bodyStart = offset + headerFragmentLen;
    // Bounds check in long arithmetic — a corrupted / malicious header could carry a
    // totalByteLength that overflows int, and silently casting would misindex the walk.
    final long availableBodyLength = (long) length - headerFragmentLen;
    if (expectedBodyLength < 0L || expectedBodyLength > availableBodyLength) {
      throw new IllegalStateException(
          "snapshot body length "
              + expectedBodyLength
              + " out of range for available buffer length "
              + availableBodyLength);
    }
    if (expectedBodyLength > Integer.MAX_VALUE - bodyStart) {
      throw new IllegalStateException(
          "snapshot body length "
              + expectedBodyLength
              + " overflows int cursor at bodyStart "
              + bodyStart);
    }
    final int bodyEnd = bodyStart + (int) expectedBodyLength;

    // 2. Reset destination ref-data state so smaller snapshots don't leave orphans behind.
    referenceDataRegistry.resetAll();
    orderBook.clear();
    // Track whether each of the six required fragments has been seen so we can reject
    // CRC-valid but semantically incomplete snapshots (missing or duplicated fragments).
    eventSeqFragmentSeen = false;
    idGenFragmentSeen = false;
    orderBookFragmentSeen = false;
    accountFragmentSeen = false;
    currencyFragmentSeen = false;
    riskLimitFragmentSeen = false;
    orderIdGenRestored = false;
    execIdGenRestored = false;

    // 3. Walk body fragments in publish order, dispatching each by templateId and computing CRC
    //    as we go.
    crc.reset();
    int cursor = bodyStart;
    int fragmentCount = 0;
    while (cursor < bodyEnd) {
      headerDecoder.wrap(src, cursor);
      final int templateId = headerDecoder.templateId();
      final int consumed = applySnapshotFragment(templateId, src, cursor);
      if (consumed <= 0) {
        throw new IllegalStateException(
            "unknown or malformed snapshot fragment: templateId=" + templateId);
      }
      // Update CRC over the raw bytes of this fragment. When the DirectBuffer is backed by a
      // byte[] (the production path uses ExpandableArrayBuffer, tests use UnsafeBuffer over a
      // byte[]), use the array fast path with wrapAdjustment() added to cursor so a non-zero
      // wrap offset is handled correctly. Falls back to per-byte for native-memory buffers.
      final byte[] backing = src.byteArray();
      if (backing != null) {
        crc.update(backing, src.wrapAdjustment() + cursor, consumed);
      } else {
        for (int i = 0; i < consumed; i++) {
          crc.update(src.getByte(cursor + i) & 0xFF);
        }
      }
      cursor += consumed;
      fragmentCount++;
    }
    if (cursor != bodyEnd) {
      throw new IllegalStateException(
          "snapshot body walk ended at " + cursor + " but expected " + bodyEnd);
    }
    if (fragmentCount != expectedStoreCount) {
      throw new IllegalStateException(
          "snapshot storeCount mismatch: header said "
              + expectedStoreCount
              + " but walked "
              + fragmentCount);
    }
    if (!eventSeqFragmentSeen
        || !idGenFragmentSeen
        || !orderBookFragmentSeen
        || !accountFragmentSeen
        || !currencyFragmentSeen
        || !riskLimitFragmentSeen) {
      throw new IllegalStateException(
          "snapshot missing required fragments"
              + " (eventSeq="
              + eventSeqFragmentSeen
              + ", idGen="
              + idGenFragmentSeen
              + ", account="
              + accountFragmentSeen
              + ", currency="
              + currencyFragmentSeen
              + ", riskLimit="
              + riskLimitFragmentSeen
              + ", orderBook="
              + orderBookFragmentSeen
              + ")");
    }
    if (!orderIdGenRestored || !execIdGenRestored) {
      throw new IllegalStateException(
          "snapshot IdGenerator fragment missing ORD or EXE counter (orderIdGenRestored="
              + orderIdGenRestored
              + ", execIdGenRestored="
              + execIdGenRestored
              + ")");
    }
    final long actualChecksum = Integer.toUnsignedLong((int) crc.getValue());
    if (actualChecksum != expectedChecksum) {
      throw new IllegalStateException(
          "snapshot CRC32C mismatch: expected "
              + Long.toHexString(expectedChecksum)
              + ", got "
              + Long.toHexString(actualChecksum));
    }
  }

  private int applySnapshotFragment(
      final int templateId, final DirectBuffer src, final int offset) {
    if (templateId == EventSequencerSnapshotDecoder.TEMPLATE_ID) {
      if (eventSeqFragmentSeen) {
        throw new IllegalStateException("duplicate EventSequencerSnapshot fragment in snapshot");
      }
      eventSeqFragmentSeen = true;
      final int wireBlockLength = headerDecoder.blockLength();
      eventSeqSnapDecoder.wrap(
          src,
          offset + MessageHeaderDecoder.ENCODED_LENGTH,
          wireBlockLength,
          headerDecoder.version());
      // Route through the SBE decoder (not eventSequencer.loadFrom, which reads raw bytes)
      // so future block-length padding does not silently corrupt the restored counter.
      eventSequencer.setNextSequence(eventSeqSnapDecoder.nextSequence());
      return MessageHeaderDecoder.ENCODED_LENGTH + wireBlockLength;
    }
    if (templateId == IdGeneratorSnapshotDecoder.TEMPLATE_ID) {
      if (idGenFragmentSeen) {
        throw new IllegalStateException("duplicate IdGeneratorSnapshot fragment in snapshot");
      }
      idGenFragmentSeen = true;
      idGenSnapDecoder.wrap(
          src,
          offset + MessageHeaderDecoder.ENCODED_LENGTH,
          headerDecoder.blockLength(),
          headerDecoder.version());
      final IdGeneratorSnapshotDecoder.NoGeneratorsDecoder group = idGenSnapDecoder.noGenerators();
      while (group.hasNext()) {
        group.next();
        final long counter = group.counter();
        if (prefixMatches(group, orderIdGen.prefix())) {
          orderIdGen.setCounter(counter);
          orderIdGenRestored = true;
        } else if (prefixMatches(group, execIdGen.prefix())) {
          execIdGen.setCounter(counter);
          execIdGenRestored = true;
        } else {
          // Snapshot carries an IdGenerator prefix we don't recognize — refuse to silently drop
          // it, since lost counter state would break determinism on the next command dispatch.
          // Use a fixed message (no allocation of prefix bytes or String decode on this path).
          throw new IllegalStateException(UNREGISTERED_ID_PREFIX_MESSAGE);
        }
      }
      return MessageHeaderDecoder.ENCODED_LENGTH + idGenSnapDecoder.encodedLength();
    }
    if (templateId == OrderBookSnapshotDecoder.TEMPLATE_ID) {
      if (orderBookFragmentSeen) {
        throw new IllegalStateException("duplicate OrderBookSnapshot fragment in snapshot");
      }
      orderBookFragmentSeen = true;
      return orderBook.restoreFrom(src, offset);
    }
    // Ref-data snapshots (Account 201, Currency 208, RiskLimit 209) route via the registry.
    if (templateId == AccountSnapshotDecoder.TEMPLATE_ID) {
      if (accountFragmentSeen) {
        throw new IllegalStateException("duplicate AccountSnapshot fragment in snapshot");
      }
      accountFragmentSeen = true;
    } else if (templateId == CurrencySnapshotDecoder.TEMPLATE_ID) {
      if (currencyFragmentSeen) {
        throw new IllegalStateException("duplicate CurrencySnapshot fragment in snapshot");
      }
      currencyFragmentSeen = true;
    } else if (templateId == RiskLimitSnapshotDecoder.TEMPLATE_ID) {
      if (riskLimitFragmentSeen) {
        throw new IllegalStateException("duplicate RiskLimitSnapshot fragment in snapshot");
      }
      riskLimitFragmentSeen = true;
    }
    final int consumed = referenceDataRegistry.restoreFragment(headerDecoder, src, offset);
    if (consumed != ReferenceDataRegistry.NOT_HANDLED) {
      return consumed;
    }
    return NOT_HANDLED;
  }

  private static final String UNREGISTERED_ID_PREFIX_MESSAGE =
      "IdGeneratorSnapshot contains an unregistered prefix (expected ORD or EXE)";

  // ===========================================================================
  // Misc
  // ===========================================================================

  private static int trimTrailingZeros(final byte[] bytes, final int upToLength) {
    int len = upToLength;
    while (len > 0 && bytes[len - 1] == 0) {
      len--;
    }
    return len;
  }

  /**
   * Compare the full 8-byte prefix carried by an {@code IdGeneratorSnapshot} record against a known
   * generator's prefix (e.g. {@code "ORD"}). Trailing zero-padding on the wire side counts as "end
   * of prefix". Returns {@code true} on an exact match.
   */
  private static boolean prefixMatches(
      final IdGeneratorSnapshotDecoder.NoGeneratorsDecoder group, final String prefix) {
    final int prefixLen = prefix.length();
    if (prefixLen > IdGenerator.MAX_PREFIX_LENGTH) {
      return false;
    }
    for (int i = 0; i < prefixLen; i++) {
      if (group.prefix(i) != (byte) prefix.charAt(i)) {
        return false;
      }
    }
    for (int i = prefixLen; i < IdGenerator.MAX_PREFIX_LENGTH; i++) {
      if (group.prefix(i) != 0) {
        return false;
      }
    }
    return true;
  }
}
