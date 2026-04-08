package com.trading.engine.cluster;

import com.trading.engine.cluster.journal.EventJournal;
import com.trading.engine.cluster.refdata.AccountState;
import com.trading.engine.cluster.refdata.AccountStore;
import com.trading.engine.cluster.refdata.CurrencyStore;
import com.trading.engine.cluster.refdata.ReferenceDataRegistry;
import com.trading.engine.cluster.refdata.RiskLimitState;
import com.trading.engine.cluster.refdata.RiskLimitStore;
import com.trading.engine.cluster.sequencer.EventSequencer;
import com.trading.engine.messages.sbe.AccountStatusEnum;
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
import com.trading.engine.messages.sbe.SideEnum;
import com.trading.engine.messages.sbe.SnapshotTakenDecoder;
import com.trading.engine.messages.sbe.SnapshotTakenEncoder;
import io.aeron.ExclusivePublication;
import io.aeron.Image;
import io.aeron.Publication;
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
  private static final int MAX_BACKPRESSURE_RETRY = 3_000;
  private static final long NANOS_PER_MILLI = 1_000_000L;

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
  private final UnsafeBuffer egressBuffer = new UnsafeBuffer(new byte[2048]);
  private final UnsafeBuffer refDataEventBuffer = new UnsafeBuffer(new byte[2048]);
  private final byte[] orderIdScratch = new byte[OrderState.ORDER_ID_LENGTH];
  private final byte[] execIdScratch = new byte[OrderState.ORDER_ID_LENGTH];
  private final byte[] clOrdIdScratch = new byte[OrderState.CL_ORD_ID_LENGTH];
  private final byte[] symbolScratch = new byte[OrderState.SYMBOL_LENGTH];
  private final byte[] accountCodeScratch = new byte[AccountStore.MAX_ACCOUNT_CODE_LENGTH];
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
    // early (which would leave a half-restored snapshot).
    while (!snapshotImage.isEndOfStream()) {
      if (snapshotImage.poll(appender, Integer.MAX_VALUE) == 0) {
        cluster.idleStrategy().idle();
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
      case NewOrderSingleDecoder.TEMPLATE_ID:
        handleNewOrderSingle(session, timestamp, buffer, offset);
        return;
      default:
        // Unknown templateId — silently drop in Phase 1. A future PR will add structured logging
        // via GFLog.
        return;
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
    final OrdTypeEnum ordType = nosDecoder.ordType();
    final long price = nosDecoder.price();
    final SideEnum side = nosDecoder.side();
    nosDecoder.getClOrdId(clOrdIdScratch, 0);
    nosDecoder.getSymbol(symbolScratch, 0);
    final int symbolLen = trimTrailingZeros(symbolScratch, OrderState.SYMBOL_LENGTH);
    nosDecoder.getAccountCode(accountCodeScratch, 0);
    final int accountCodeLen =
        trimTrailingZeros(accountCodeScratch, AccountStore.MAX_ACCOUNT_CODE_LENGTH);
    final byte ccy0 = nosDecoder.currency(0);
    final byte ccy1 = nosDecoder.currency(1);
    final byte ccy2 = nosDecoder.currency(2);

    // ========== Validation ==========
    if (symbolLen == 0) {
      emitOrderRejected(
          session, timestamp, side, RejectReasonEnum.UnknownSymbol, "symbol is empty");
      return;
    }
    if (orderQty <= 0L) {
      emitOrderRejected(
          session, timestamp, side, RejectReasonEnum.InvalidQuantity, "orderQty must be > 0");
      return;
    }
    if (ordType == OrdTypeEnum.Limit && price <= 0L) {
      emitOrderRejected(
          session, timestamp, side, RejectReasonEnum.InvalidPrice, "limit price must be > 0");
      return;
    }
    if (accountCodeLen == 0) {
      emitOrderRejected(
          session, timestamp, side, RejectReasonEnum.AccountNotFound, "accountCode is empty");
      return;
    }
    final AccountState account = accountStore.getByCodeBytes(accountCodeScratch, 0, accountCodeLen);
    if (account == null) {
      emitOrderRejected(
          session,
          timestamp,
          side,
          RejectReasonEnum.AccountNotFound,
          "account not in AccountStore");
      return;
    }
    if (account.status() != AccountStatusEnum.Active) {
      emitOrderRejected(
          session, timestamp, side, RejectReasonEnum.AccountSuspended, "account not active");
      return;
    }
    if (!account.canTrade()) {
      emitOrderRejected(
          session,
          timestamp,
          side,
          RejectReasonEnum.AccountNoTradePermission,
          "account lacks CAN_TRADE");
      return;
    }
    final int ccyPacked = CurrencyStore.packCodeOrInvalid(ccy0, ccy1, ccy2);
    if (ccyPacked == CurrencyStore.INVALID_PACKED_CODE) {
      emitOrderRejected(
          session,
          timestamp,
          side,
          RejectReasonEnum.InvalidCurrencyCode,
          "currency is not 3 uppercase ASCII letters");
      return;
    }
    if (!currencyStore.contains(ccyPacked)) {
      emitOrderRejected(
          session,
          timestamp,
          side,
          RejectReasonEnum.UnknownCurrency,
          "currency not in CurrencyStore");
      return;
    }
    final RiskLimitState riskLimit = riskLimitStore.get(account.accountId());
    if (riskLimit != null && riskLimit.maxOrderSize() > 0L && orderQty > riskLimit.maxOrderSize()) {
      emitOrderRejected(
          session,
          timestamp,
          side,
          RejectReasonEnum.OrderExceedsMaxSize,
          "orderQty exceeds account maxOrderSize");
      return;
    }

    // ========== Happy path ==========
    // Generate ids. nextInto advances the counter; currentCounter() after the call gives the
    // counter value that was just assigned — we use it as the primitive OrderBook key. The
    // scratch wrappers (orderIdScratchBuffer / execIdScratchBuffer) are pre-allocated fields so
    // this path performs zero heap allocation.
    orderIdGen.nextInto(orderIdScratchBuffer, 0);
    final long orderKey = orderIdGen.currentCounter();
    execIdGen.nextInto(execIdScratchBuffer, 0);

    final OrderState state = orderBook.acquire(orderKey);
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
    // client can cause head-of-line blocking for other sessions while we retry, which the
    // cluster framework's idle strategy will eventually idle away — but it is still a real
    // concern for production. A follow-up issue (APP-? async egress queue) will move session
    // egress off the duty cycle. For now, cap retries at MAX_BACKPRESSURE_RETRY and drop the
    // message on exhaustion; the underlying event is already durable in the journal so the
    // delivered-to-client invariant is weaker than the sequenced-in-log invariant.
    for (int attempt = 0; attempt < MAX_BACKPRESSURE_RETRY; attempt++) {
      final long result = session.offer(src, offset, length);
      if (result >= 0L || result == Publication.MOCKED_OFFER_VALUE_RESULT) {
        return;
      }
      if (result == Publication.BACK_PRESSURED || result == Publication.ADMIN_ACTION) {
        if (cluster != null) {
          cluster.idleStrategy().idle();
        }
        continue;
      }
      // NOT_CONNECTED / CLOSED / MAX_POSITION_EXCEEDED — give up for this message; the cluster
      // framework will tear down the session.
      return;
    }
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
      if (result == Publication.BACK_PRESSURED || result == Publication.ADMIN_ACTION) {
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
    final IdGeneratorSnapshotEncoder.NoGeneratorsEncoder idGenGroup =
        idGenSnapEncoder.noGeneratorsCount(2);
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
    snapshotTakenEncoder.snapshotVersion(1L);
    snapshotTakenEncoder.storeCount((short) 6);
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
    final long expectedBodyLength = snapshotTakenDecoder.totalByteLength();
    final long expectedChecksum = snapshotTakenDecoder.checksum();
    // Use the wire block length from the header (not the compile-time constant) so the walk
    // survives a future forward-compatible schema extension of SnapshotTaken.
    final int headerFragmentLen = MessageHeaderDecoder.ENCODED_LENGTH + headerDecoder.blockLength();
    final int bodyStart = offset + headerFragmentLen;
    final int bodyEnd = bodyStart + (int) expectedBodyLength;
    if (bodyEnd > offset + length) {
      throw new IllegalStateException(
          "snapshot body length "
              + expectedBodyLength
              + " exceeds available buffer length "
              + (length - headerFragmentLen));
    }

    // 2. Reset destination ref-data state so smaller snapshots don't leave orphans behind.
    referenceDataRegistry.resetAll();
    orderBook.clear();

    // 3. Walk body fragments in publish order, dispatching each by templateId and computing CRC
    //    as we go.
    crc.reset();
    int cursor = bodyStart;
    while (cursor < bodyEnd) {
      headerDecoder.wrap(src, cursor);
      final int templateId = headerDecoder.templateId();
      final int consumed = applySnapshotFragment(templateId, src, cursor);
      if (consumed <= 0) {
        throw new IllegalStateException(
            "unknown or malformed snapshot fragment: templateId=" + templateId);
      }
      // Update CRC over the raw bytes of this fragment.
      if (src.byteArray() != null) {
        crc.update(src.byteArray(), cursor, consumed);
      } else {
        // DirectBuffer without a backing array — byte-by-byte fallback. Not hot-path.
        for (int i = 0; i < consumed; i++) {
          crc.update(src.getByte(cursor + i) & 0xFF);
        }
      }
      cursor += consumed;
    }
    if (cursor != bodyEnd) {
      throw new IllegalStateException(
          "snapshot body walk ended at " + cursor + " but expected " + bodyEnd);
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
        } else if (prefixMatches(group, execIdGen.prefix())) {
          execIdGen.setCounter(counter);
        } else {
          // Snapshot carries an IdGenerator prefix we don't recognize — refuse to silently drop
          // it, since lost counter state would break determinism on the next command dispatch.
          final byte[] bytes = new byte[IdGenerator.MAX_PREFIX_LENGTH];
          for (int i = 0; i < IdGenerator.MAX_PREFIX_LENGTH; i++) {
            bytes[i] = group.prefix(i);
          }
          throw new IllegalStateException(
              "IdGeneratorSnapshot contains unregistered prefix '"
                  + new String(bytes, java.nio.charset.StandardCharsets.US_ASCII).trim()
                  + "'");
        }
      }
      return MessageHeaderDecoder.ENCODED_LENGTH + idGenSnapDecoder.encodedLength();
    }
    if (templateId == OrderBookSnapshotDecoder.TEMPLATE_ID) {
      return orderBook.restoreFrom(src, offset);
    }
    // Ref-data snapshots (Account 201, Currency 208, RiskLimit 209) route via the registry.
    final int consumed = referenceDataRegistry.restoreFragment(headerDecoder, src, offset);
    if (consumed != ReferenceDataRegistry.NOT_HANDLED) {
      return consumed;
    }
    return NOT_HANDLED;
  }

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

  // Publication.MOCKED_OFFER_VALUE_RESULT is not exposed as a constant in this Aeron version; the
  // ClientSession contract treats non-negative returns as success.
  private static final class Publication {
    static final long BACK_PRESSURED = io.aeron.Publication.BACK_PRESSURED;
    static final long NOT_CONNECTED = io.aeron.Publication.NOT_CONNECTED;
    static final long ADMIN_ACTION = io.aeron.Publication.ADMIN_ACTION;
    static final long CLOSED = io.aeron.Publication.CLOSED;
    static final long MAX_POSITION_EXCEEDED = io.aeron.Publication.MAX_POSITION_EXCEEDED;
    static final long MOCKED_OFFER_VALUE_RESULT = ClientSession.MOCKED_OFFER;

    private Publication() {}
  }
}
