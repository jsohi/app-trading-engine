package com.trading.engine.projections.quote;

import com.epam.deltix.gflog.api.Log;
import com.epam.deltix.gflog.api.LogFactory;
import com.trading.engine.messages.sbe.OrdTypeEnum;
import com.trading.engine.messages.sbe.OrderCreatedEventDecoder;
import com.trading.engine.messages.sbe.QuoteCreatedEventDecoder;
import com.trading.engine.messages.sbe.QuoteExpiredEventDecoder;
import com.trading.engine.messages.sbe.QuoteRejectedEventDecoder;
import com.trading.engine.messages.sbe.QuoteRequestedEventDecoder;
import com.trading.engine.messages.util.ByteArrayKey;
import com.trading.engine.projections.Projection;
import com.trading.engine.projections.ProjectionUtil;
import com.trading.engine.projections.SymbolPacker;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.StampedLock;
import org.agrona.DirectBuffer;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.collections.Object2ObjectHashMap;
import org.agrona.collections.ObjectHashSet;

/**
 * CQRS read-model projection tracking the full RFQ/quote lifecycle. Consumes {@code
 * QuoteRequestedEvent} (104), {@code QuoteCreatedEvent} (105), {@code QuoteRejectedEvent} (106),
 * {@code QuoteExpiredEvent} (107), and {@code OrderCreatedEvent} (100, for "Used" detection) from
 * the cluster event stream.
 *
 * <p><b>Write-side status:</b> events 104-107 are defined in the SBE schema but are not currently
 * emitted by any production code. This projection is built against the schema contract per CQRS
 * best practices (LMAX, exchange-core, Axon). A follow-up ticket must wire {@code
 * RfqCommandHandler} to emit these events via {@code EventSink} before the projection receives live
 * data.
 *
 * <p><b>Indexes:</b> four indexes are maintained for efficient query access:
 *
 * <ol>
 *   <li>Primary: quoteId &rarr; {@link QuoteView} ({@link Object2ObjectHashMap})
 *   <li>Secondary: quoteReqId &rarr; {@link QuoteView} ({@link Object2ObjectHashMap})
 *   <li>Tertiary: packed symbol (long) &rarr; {@link ObjectHashSet}{@code <QuoteView>} ({@link
 *       Long2ObjectHashMap})
 *   <li>Quaternary: accountCode &rarr; {@link ObjectHashSet}{@code <QuoteView>} ({@link
 *       Object2ObjectHashMap})
 * </ol>
 *
 * <p><b>Threading:</b> single-writer / multi-reader via {@link StampedLock}. The event-dispatch
 * thread acquires the write stamp in {@link #onEvent}. Query threads acquire pessimistic read
 * stamps in query methods (optimistic reads are unsafe with Agrona's non-concurrent collections).
 * Query methods return immutable {@link QuoteSnapshot} records — internal mutable {@link QuoteView}
 * instances are never leaked.
 *
 * <p><b>Allocation:</b> bounded per-entity allocation on the event path (one {@link QuoteView} per
 * quote, one {@link ByteArrayKey#copyOf()} per map entry). Zero allocation on lookups via
 * pre-allocated probe keys. Query methods allocate snapshots and lists (acceptable — off hot path).
 *
 * <p><b>Terminal state guards:</b> {@link QuoteStatus#Rejected}, {@link QuoteStatus#Expired}, and
 * {@link QuoteStatus#Used} are terminal. The QuoteExpired handler does not override Used or
 * Rejected; the OrderCreated "Used" handler only transitions from Active. See DD-5.
 *
 * <p><b>Counter visibility:</b> all diagnostic counters ({@code lastProcessedSeqNo}, {@code
 * eventsProcessed}, {@code errorCount}) are {@code volatile}, matching {@link
 * com.trading.engine.projections.account.AccountProjection}'s pattern. See DD-6.
 *
 * <p><b>Regulatory notes:</b> {@code createdAt} (from QuoteRequestedEvent.timestamp) serves as
 * MiFID II RTS 25 "receipt time." {@code lastUpdatedAt} (from QuoteCreatedEvent.timestamp) serves
 * as "response time." {@code responseLatencyNanos} provides the SLA metric. All timestamps are
 * epoch nanos, microsecond granularity, UTC.
 *
 * <p><b>Error handling:</b> all event processing is wrapped in a try-catch. Decode errors increment
 * {@link #errorCount()} and log via GFLog. The event is skipped (not rethrown) to prevent crashing
 * the {@link com.trading.engine.projections.EventConsumer}. {@link #lastProcessedSequence()} is
 * updated even on error.
 *
 * @see QuoteView
 * @see QuoteSnapshot
 * @see QuoteStatus
 * @see com.trading.engine.projections.EventConsumer
 */
public final class QuoteProjection implements Projection {

  private static final Log LOG = LogFactory.getLog(QuoteProjection.class);
  private static final float LOAD_FACTOR = 0.65f;

  // --- Primary and secondary indexes ---
  private final Object2ObjectHashMap<ByteArrayKey, QuoteView> byQuoteId;
  private final Object2ObjectHashMap<ByteArrayKey, QuoteView> byQuoteReqId;
  private final Long2ObjectHashMap<ObjectHashSet<QuoteView>> bySymbol;
  private final Object2ObjectHashMap<ByteArrayKey, ObjectHashSet<QuoteView>> byAccountCode;

  // --- Pre-allocated SBE flyweight decoders (reused per event) ---
  private final QuoteRequestedEventDecoder requestedDecoder = new QuoteRequestedEventDecoder();
  private final QuoteCreatedEventDecoder createdDecoder = new QuoteCreatedEventDecoder();
  private final QuoteRejectedEventDecoder rejectedDecoder = new QuoteRejectedEventDecoder();
  private final QuoteExpiredEventDecoder expiredDecoder = new QuoteExpiredEventDecoder();
  private final OrderCreatedEventDecoder orderCreatedDecoder = new OrderCreatedEventDecoder();

  // --- Pre-allocated probe keys (event-thread only) ---
  private final ByteArrayKey probeQuoteId = ByteArrayKey.emptyForLookup(20);
  private final ByteArrayKey probeQuoteReqId = ByteArrayKey.emptyForLookup(20);
  private final ByteArrayKey probeAccountCode = ByteArrayKey.emptyForLookup(16);

  // --- Pre-allocated scratch byte arrays for SBE field decoding ---
  private final byte[] scratchQuoteId = new byte[20];
  private final byte[] scratchQuoteReqId = new byte[20];
  private final byte[] scratchSymbol = new byte[8];
  private final byte[] scratchAccountCode = new byte[16];
  private final byte[] scratchText = new byte[64];
  private final byte[] scratchSettlDate = new byte[8];
  private final byte[] scratchCurrency = new byte[3];
  private final byte[] scratchSettlCurrency = new byte[3];

  // --- Concurrency ---
  private final StampedLock lock = new StampedLock();

  // --- Volatile counters: projections are NOT single-threaded Aeron agents — they serve
  // concurrent query threads. lastProcessedSequence() reads without lock, so volatile is required
  // for cross-thread visibility. Diagnostic methods also acquire read lock (belt-and-suspenders).
  // ---
  private volatile long lastProcessedSeqNo;
  private volatile long eventsProcessed;
  private volatile long errorCount;

  public QuoteProjection() {
    this(1024);
  }

  /**
   * Creates a QuoteProjection with the specified initial capacity for the primary indexes.
   *
   * @param initialCapacity expected number of quotes (determines initial map sizes)
   */
  public QuoteProjection(final int initialCapacity) {
    byQuoteId = new Object2ObjectHashMap<>(initialCapacity, LOAD_FACTOR);
    byQuoteReqId = new Object2ObjectHashMap<>(initialCapacity, LOAD_FACTOR);
    bySymbol = new Long2ObjectHashMap<>(64, LOAD_FACTOR);
    byAccountCode = new Object2ObjectHashMap<>(256, LOAD_FACTOR);
  }

  // ---------------------------------------------------------------------------
  // Projection interface
  // ---------------------------------------------------------------------------

  @Override
  public void onEvent(
      final long seqNo,
      final int eventType,
      final DirectBuffer buffer,
      final int offset,
      final int length) {
    final long stamp = lock.writeLock();
    try {
      switch (eventType) {
        case QuoteRequestedEventDecoder.TEMPLATE_ID ->
            onQuoteRequested(seqNo, buffer, offset, length);
        case QuoteCreatedEventDecoder.TEMPLATE_ID -> onQuoteCreated(seqNo, buffer, offset, length);
        case QuoteRejectedEventDecoder.TEMPLATE_ID ->
            onQuoteRejected(seqNo, buffer, offset, length);
        case QuoteExpiredEventDecoder.TEMPLATE_ID -> onQuoteExpired(seqNo, buffer, offset, length);
        case OrderCreatedEventDecoder.TEMPLATE_ID -> onOrderCreated(seqNo, buffer, offset, length);
        default -> {
          return;
        }
      }
      eventsProcessed++;
    } catch (final Exception e) {
      errorCount++;
      LOG.error()
          .append("QuoteProjection decode error seqNo=")
          .append(seqNo)
          .append(" eventType=")
          .append(eventType)
          .append(" ")
          .append(e)
          .commit();
    } finally {
      lastProcessedSeqNo = seqNo;
      lock.unlockWrite(stamp);
    }
  }

  @Override
  public long lastProcessedSequence() {
    return lastProcessedSeqNo;
  }

  @Override
  public void reset() {
    final long stamp = lock.writeLock();
    try {
      byQuoteId.clear();
      byQuoteReqId.clear();
      bySymbol.clear();
      byAccountCode.clear();
      lastProcessedSeqNo = 0;
      eventsProcessed = 0;
      errorCount = 0;
      LOG.info().append("QuoteProjection reset").commit();
    } finally {
      lock.unlockWrite(stamp);
    }
  }

  // ---------------------------------------------------------------------------
  // Event handlers (called under write lock)
  // ---------------------------------------------------------------------------

  private void onQuoteRequested(
      final long seqNo, final DirectBuffer buffer, final int offset, final int length) {
    requestedDecoder.wrap(
        buffer,
        offset,
        QuoteRequestedEventDecoder.BLOCK_LENGTH,
        QuoteRequestedEventDecoder.SCHEMA_VERSION);

    final int reqIdLen =
        ProjectionUtil.sbeStrLen(
            requestedDecoder.getQuoteReqId(scratchQuoteReqId, 0), scratchQuoteReqId);

    // Defensive: check if a view already exists for this quoteReqId
    probeQuoteReqId.set(scratchQuoteReqId, 0, reqIdLen);
    final var existing = byQuoteReqId.get(probeQuoteReqId);
    if (existing != null) {
      if (existing.status() == QuoteStatus.Requested) {
        // Duplicate 104 replay: update existing view in-place to preserve object identity
        // (byQuoteId may already reference this instance after a QuoteCreated)
        existing.setSequenceNumber(seqNo);
        existing.setLastUpdatedAt(requestedDecoder.timestamp());
      }
      // View in non-Requested state: no-op — preserve the later-state view
      return;
    }

    final int symbolLen =
        ProjectionUtil.sbeStrLen(requestedDecoder.getSymbol(scratchSymbol, 0), scratchSymbol);
    final int accountLen =
        ProjectionUtil.sbeStrLen(
            requestedDecoder.getAccountCode(scratchAccountCode, 0), scratchAccountCode);
    final int settlDateLen =
        ProjectionUtil.sbeStrLen(
            requestedDecoder.getSettlDate(scratchSettlDate, 0), scratchSettlDate);
    final int currLen =
        ProjectionUtil.sbeStrLen(requestedDecoder.getCurrency(scratchCurrency, 0), scratchCurrency);
    final int settlCurrLen =
        ProjectionUtil.sbeStrLen(
            requestedDecoder.getSettlCurrency(scratchSettlCurrency, 0), scratchSettlCurrency);

    final var view = new QuoteView();
    view.setQuoteReqId(scratchQuoteReqId, 0, reqIdLen);
    view.setSymbol(scratchSymbol, 0, symbolLen);
    view.setSide(requestedDecoder.side());
    view.setOrderQty(requestedDecoder.orderQty());
    view.setAccountCode(scratchAccountCode, 0, accountLen);
    view.setProductType(requestedDecoder.productType());
    view.setSettlDate(scratchSettlDate, 0, settlDateLen);
    view.setSettlType(requestedDecoder.settlType());
    view.setCurrency(scratchCurrency, 0, currLen);
    view.setSettlCurrency(scratchSettlCurrency, 0, settlCurrLen);
    view.setTenor(requestedDecoder.tenor());
    view.setStatus(QuoteStatus.Requested);
    view.setSequenceNumber(seqNo);
    view.setCreatedAt(requestedDecoder.timestamp());
    view.setLastUpdatedAt(requestedDecoder.timestamp());

    // Index by quoteReqId, symbol, accountCode (no quoteId yet)
    byQuoteReqId.put(probeQuoteReqId.copyOf(), view);
    addToSymbolIndex(view, scratchSymbol);
    addToAccountIndex(view, scratchAccountCode, accountLen);
  }

  private void onQuoteCreated(
      final long seqNo, final DirectBuffer buffer, final int offset, final int length) {
    createdDecoder.wrap(
        buffer,
        offset,
        QuoteCreatedEventDecoder.BLOCK_LENGTH,
        QuoteCreatedEventDecoder.SCHEMA_VERSION);

    final int quoteIdLen =
        ProjectionUtil.sbeStrLen(createdDecoder.getQuoteId(scratchQuoteId, 0), scratchQuoteId);
    final int reqIdLen =
        ProjectionUtil.sbeStrLen(
            createdDecoder.getQuoteReqId(scratchQuoteReqId, 0), scratchQuoteReqId);

    // Check for existing view by quoteReqId (from prior 104)
    probeQuoteReqId.set(scratchQuoteReqId, 0, reqIdLen);
    QuoteView view = byQuoteReqId.get(probeQuoteReqId);

    // Handle duplicate quoteId: if byQuoteId already has an entry, remove old from secondaries
    probeQuoteId.set(scratchQuoteId, 0, quoteIdLen);
    final var existingByQuoteId = byQuoteId.get(probeQuoteId);
    if (existingByQuoteId != null && existingByQuoteId != view) {
      removeFromSecondaryIndexes(existingByQuoteId);
      // Reuse probeQuoteReqId to avoid allocation — will be reset below for the new entry
      probeQuoteReqId.set(existingByQuoteId.quoteReqId(), 0, existingByQuoteId.quoteReqIdLen());
      byQuoteReqId.remove(probeQuoteReqId);
      // Re-set probe to the current event's quoteReqId (overwritten by duplicate cleanup above)
      probeQuoteReqId.set(scratchQuoteReqId, 0, reqIdLen);
    }

    final boolean isNewView = (view == null);
    if (isNewView) {
      view = new QuoteView();
      view.setQuoteReqId(scratchQuoteReqId, 0, reqIdLen);
      view.setCreatedAt(createdDecoder.timestamp());
      view.setResponseLatencyNanos(-1L); // sentinel: no prior 104

      // Decode symbol/accountCode/FX fields for new views only (secondary index keyed on these)
      final int symbolLen =
          ProjectionUtil.sbeStrLen(createdDecoder.getSymbol(scratchSymbol, 0), scratchSymbol);
      final int accountLen =
          ProjectionUtil.sbeStrLen(
              createdDecoder.getAccountCode(scratchAccountCode, 0), scratchAccountCode);
      final int settlDateLen =
          ProjectionUtil.sbeStrLen(
              createdDecoder.getSettlDate(scratchSettlDate, 0), scratchSettlDate);
      final int currLen =
          ProjectionUtil.sbeStrLen(createdDecoder.getCurrency(scratchCurrency, 0), scratchCurrency);
      final int settlCurrLen =
          ProjectionUtil.sbeStrLen(
              createdDecoder.getSettlCurrency(scratchSettlCurrency, 0), scratchSettlCurrency);

      view.setSymbol(scratchSymbol, 0, symbolLen);
      view.setSide(createdDecoder.side());
      view.setAccountCode(scratchAccountCode, 0, accountLen);
      view.setProductType(createdDecoder.productType());
      view.setSettlDate(scratchSettlDate, 0, settlDateLen);
      view.setSettlType(createdDecoder.settlType());
      view.setCurrency(scratchCurrency, 0, currLen);
      view.setSettlCurrency(scratchSettlCurrency, 0, settlCurrLen);
      view.setTenor(createdDecoder.tenor());

      // Index new view in secondary maps (must happen inside this block where accountLen is scoped)
      byQuoteReqId.put(probeQuoteReqId.copyOf(), view);
      addToSymbolIndex(view, scratchSymbol);
      addToAccountIndex(view, scratchAccountCode, accountLen);
    } else if (view.status() == QuoteStatus.Requested) {
      // Only compute latency on Requested→Active transition (not on duplicate 105 replay)
      view.setResponseLatencyNanos(createdDecoder.timestamp() - view.createdAt());
    }

    // Pricing fields always updated (these are the core QuoteCreated payload)
    view.setQuoteId(scratchQuoteId, 0, quoteIdLen);
    view.setBidPx(createdDecoder.bidPx());
    view.setOfferPx(createdDecoder.offerPx());
    view.setBidSize(createdDecoder.bidSize());
    view.setOfferSize(createdDecoder.offerSize());
    view.setValidUntil(createdDecoder.validUntil());
    view.setSwapPoints(createdDecoder.swapPoints());
    view.setStatus(QuoteStatus.Active);
    view.setSequenceNumber(seqNo);
    view.setLastUpdatedAt(createdDecoder.timestamp());

    // Index in byQuoteId — avoid redundant put if view is already correctly mapped
    if (existingByQuoteId != view) {
      byQuoteId.put(probeQuoteId.copyOf(), view);
    }
  }

  private void onQuoteRejected(
      final long seqNo, final DirectBuffer buffer, final int offset, final int length) {
    rejectedDecoder.wrap(
        buffer,
        offset,
        QuoteRejectedEventDecoder.BLOCK_LENGTH,
        QuoteRejectedEventDecoder.SCHEMA_VERSION);

    final int reqIdLen =
        ProjectionUtil.sbeStrLen(
            rejectedDecoder.getQuoteReqId(scratchQuoteReqId, 0), scratchQuoteReqId);
    final int textLen =
        ProjectionUtil.sbeStrLen(rejectedDecoder.getText(scratchText, 0), scratchText);

    probeQuoteReqId.set(scratchQuoteReqId, 0, reqIdLen);
    QuoteView view = byQuoteReqId.get(probeQuoteReqId);

    if (view != null) {
      // Terminal state guard: do NOT overwrite Used or Expired — preserve timestamps for
      // purgeTerminal eviction accuracy and MiFID II RTS 25 audit trail
      if (view.isTerminal()) {
        return;
      }
      // Update only status, rejectReason, text — preserve existing fields
      view.setStatus(QuoteStatus.Rejected);
      view.setRejectReason(rejectedDecoder.quoteRejectReason());
      view.setText(scratchText, 0, textLen);
      view.setSequenceNumber(seqNo);
      view.setLastUpdatedAt(rejectedDecoder.timestamp());
    } else {
      // No prior 104: create a new view
      final int symbolLen =
          ProjectionUtil.sbeStrLen(rejectedDecoder.getSymbol(scratchSymbol, 0), scratchSymbol);
      final int accountLen =
          ProjectionUtil.sbeStrLen(
              rejectedDecoder.getAccountCode(scratchAccountCode, 0), scratchAccountCode);

      view = new QuoteView();
      view.setQuoteReqId(scratchQuoteReqId, 0, reqIdLen);
      view.setSymbol(scratchSymbol, 0, symbolLen);
      view.setSide(rejectedDecoder.side());
      view.setAccountCode(scratchAccountCode, 0, accountLen);
      view.setProductType(rejectedDecoder.productType());
      view.setStatus(QuoteStatus.Rejected);
      view.setRejectReason(rejectedDecoder.quoteRejectReason());
      view.setText(scratchText, 0, textLen);
      view.setSequenceNumber(seqNo);
      view.setCreatedAt(rejectedDecoder.timestamp());
      view.setLastUpdatedAt(rejectedDecoder.timestamp());

      byQuoteReqId.put(probeQuoteReqId.copyOf(), view);
      addToSymbolIndex(view, scratchSymbol);
      addToAccountIndex(view, scratchAccountCode, accountLen);
    }
  }

  private void onQuoteExpired(
      final long seqNo, final DirectBuffer buffer, final int offset, final int length) {
    expiredDecoder.wrap(
        buffer,
        offset,
        QuoteExpiredEventDecoder.BLOCK_LENGTH,
        QuoteExpiredEventDecoder.SCHEMA_VERSION);

    // Try lookup by quoteId first
    final int quoteIdLen =
        ProjectionUtil.sbeStrLen(expiredDecoder.getQuoteId(scratchQuoteId, 0), scratchQuoteId);
    QuoteView view = null;
    if (quoteIdLen > 0) {
      probeQuoteId.set(scratchQuoteId, 0, quoteIdLen);
      view = byQuoteId.get(probeQuoteId);
    }

    // Fallback to quoteReqId (covers Requested-only quotes that have no quoteId)
    if (view == null) {
      final int reqIdLen =
          ProjectionUtil.sbeStrLen(
              expiredDecoder.getQuoteReqId(scratchQuoteReqId, 0), scratchQuoteReqId);
      probeQuoteReqId.set(scratchQuoteReqId, 0, reqIdLen);
      view = byQuoteReqId.get(probeQuoteReqId);
    }

    if (view == null) {
      return; // Unknown quote — silently drop
    }

    // Terminal state guard: do NOT override Used or Rejected — preserve timestamps for
    // purgeTerminal eviction accuracy and MiFID II RTS 25 audit trail
    if (view.status() != QuoteStatus.Active && view.status() != QuoteStatus.Requested) {
      return;
    }
    view.setStatus(QuoteStatus.Expired);
    view.setSequenceNumber(seqNo);
    view.setLastUpdatedAt(expiredDecoder.timestamp());
  }

  private void onOrderCreated(
      final long seqNo, final DirectBuffer buffer, final int offset, final int length) {
    orderCreatedDecoder.wrap(
        buffer,
        offset,
        OrderCreatedEventDecoder.BLOCK_LENGTH,
        OrderCreatedEventDecoder.SCHEMA_VERSION);

    final int quoteIdLen =
        ProjectionUtil.sbeStrLen(orderCreatedDecoder.getQuoteId(scratchQuoteId, 0), scratchQuoteId);
    if (quoteIdLen == 0) {
      return; // No quoteId — not a previously-quoted order
    }
    if (orderCreatedDecoder.ordType() != OrdTypeEnum.PreviouslyQuoted) {
      return; // Not a quote-accept order
    }

    probeQuoteId.set(scratchQuoteId, 0, quoteIdLen);
    final var view = byQuoteId.get(probeQuoteId);
    if (view != null && view.status() == QuoteStatus.Active) {
      view.setStatus(QuoteStatus.Used);
      view.setSequenceNumber(seqNo);
      view.setLastUpdatedAt(orderCreatedDecoder.timestamp());
    }
  }

  // ---------------------------------------------------------------------------
  // Index maintenance
  // ---------------------------------------------------------------------------

  private void addToSymbolIndex(final QuoteView view, final byte[] symbolBytes) {
    final long symbolPacked = SymbolPacker.pack(symbolBytes, 0);
    ObjectHashSet<QuoteView> symbolSet = bySymbol.get(symbolPacked);
    if (symbolSet == null) {
      symbolSet = new ObjectHashSet<>(64, LOAD_FACTOR);
      bySymbol.put(symbolPacked, symbolSet);
    }
    symbolSet.add(view);
  }

  private void addToAccountIndex(
      final QuoteView view, final byte[] accountBytes, final int accountLen) {
    probeAccountCode.set(accountBytes, 0, accountLen);
    ObjectHashSet<QuoteView> accountSet = byAccountCode.get(probeAccountCode);
    if (accountSet == null) {
      accountSet = new ObjectHashSet<>(16, LOAD_FACTOR);
      byAccountCode.put(probeAccountCode.copyOf(), accountSet);
    }
    accountSet.add(view);
  }

  /**
   * Removes a view from all secondary indexes (byQuoteReqId is NOT touched — caller handles it).
   * Used before overwriting a duplicate quoteId or duplicate quoteReqId to prevent phantom entries
   * in ObjectHashSet indexes (which use identity equality).
   */
  private void removeFromSecondaryIndexes(final QuoteView view) {
    // Remove from symbol index
    final long symbolPacked = SymbolPacker.pack(view.symbol(), 0);
    final var symbolSet = bySymbol.get(symbolPacked);
    if (symbolSet != null) {
      symbolSet.remove(view);
    }

    // Remove from account index
    probeAccountCode.set(view.accountCode(), 0, view.accountCodeLen());
    final var accountSet = byAccountCode.get(probeAccountCode);
    if (accountSet != null) {
      accountSet.remove(view);
    }
  }

  // ---------------------------------------------------------------------------
  // Terminal eviction (DD-11)
  // ---------------------------------------------------------------------------

  /**
   * Evicts terminal quotes (Rejected, Expired, Used) whose {@code lastUpdatedAt} is older than the
   * given cutoff timestamp. Called by an external timer, not by the projection itself.
   *
   * <p><b>Safe for replay:</b> eviction only removes from the in-memory projection, NOT from the
   * durable Aeron Archive event log (which is never truncated). A full replay after restart
   * reconstructs all historical quotes, and the timer resumes eviction.
   *
   * <p><b>Threading:</b> acquires write lock for the entire operation.
   *
   * <p><b>Allocation:</b> iterates with Agrona's cached flyweight iterator (zero allocation after
   * first call); removal is O(1) per map per view.
   *
   * @param olderThanTimestamp epoch nanos cutoff — terminal quotes with {@code lastUpdatedAt <
   *     olderThanTimestamp} are evicted
   * @return the number of quotes evicted
   */
  public int purgeTerminal(final long olderThanTimestamp) {
    int evicted = 0;
    final long stamp = lock.writeLock();
    try {
      final var it = byQuoteReqId.values().iterator();
      while (it.hasNext()) {
        final var view = it.next();
        if (view.isTerminal() && view.lastUpdatedAt() < olderThanTimestamp) {
          it.remove(); // Remove from byQuoteReqId

          // Remove from byQuoteId (if quoteId was assigned)
          if (view.quoteIdLen() > 0) {
            probeQuoteId.set(view.quoteId(), 0, view.quoteIdLen());
            byQuoteId.remove(probeQuoteId);
          }

          // Remove from secondary indexes
          removeFromSecondaryIndexes(view);
          evicted++;
        }
      }
      if (evicted > 0) {
        LOG.info()
            .append("QuoteProjection purged ")
            .append(evicted)
            .append(" terminal quotes")
            .commit();
      }
    } finally {
      lock.unlockWrite(stamp);
    }
    return evicted;
  }

  // ---------------------------------------------------------------------------
  // Query methods (acquire read stamp, return immutable snapshots)
  // ---------------------------------------------------------------------------

  /**
   * Looks up a quote by quote identifier.
   *
   * @param quoteId the quote ID (FIX tag 117)
   * @return the quote snapshot, or {@code null} if not found
   */
  public QuoteSnapshot getQuote(final String quoteId) {
    final var key = keyFromString(quoteId, 20);
    final long stamp = lock.readLock();
    try {
      final var view = byQuoteId.get(key);
      return view != null ? QuoteSnapshot.from(view) : null;
    } finally {
      lock.unlockRead(stamp);
    }
  }

  /**
   * Looks up a quote by quote request identifier.
   *
   * @param quoteReqId the quote request ID (FIX tag 131)
   * @return the quote snapshot, or {@code null} if not found
   */
  public QuoteSnapshot getQuoteByReqId(final String quoteReqId) {
    final var key = keyFromString(quoteReqId, 20);
    final long stamp = lock.readLock();
    try {
      final var view = byQuoteReqId.get(key);
      return view != null ? QuoteSnapshot.from(view) : null;
    } finally {
      lock.unlockRead(stamp);
    }
  }

  /**
   * Returns all quotes in {@link QuoteStatus#Active} state.
   *
   * @return list of active quote snapshots
   */
  public List<QuoteSnapshot> getActiveQuotes() {
    final var result = new ArrayList<QuoteSnapshot>();
    final long stamp = lock.readLock();
    try {
      byQuoteId
          .values()
          .forEach(
              v -> {
                if (v.isActive()) {
                  result.add(QuoteSnapshot.from(v));
                }
              });
    } finally {
      lock.unlockRead(stamp);
    }
    return result;
  }

  /**
   * Returns all in-flight quotes ({@link QuoteStatus#Requested} or {@link QuoteStatus#Active}).
   *
   * @return list of in-flight quote snapshots
   */
  public List<QuoteSnapshot> getInFlightQuotes() {
    final var result = new ArrayList<QuoteSnapshot>();
    final long stamp = lock.readLock();
    try {
      byQuoteReqId
          .values()
          .forEach(
              v -> {
                if (v.isInFlight()) {
                  result.add(QuoteSnapshot.from(v));
                }
              });
    } finally {
      lock.unlockRead(stamp);
    }
    return result;
  }

  /**
   * Returns all quotes for the given symbol.
   *
   * @param symbol the instrument symbol (FIX tag 55)
   * @return list of quote snapshots (empty if no quotes for this symbol)
   */
  public List<QuoteSnapshot> getQuotesBySymbol(final String symbol) {
    final long symbolPacked = SymbolPacker.pack(symbol);
    final var result = new ArrayList<QuoteSnapshot>();
    final long stamp = lock.readLock();
    try {
      final var set = bySymbol.get(symbolPacked);
      if (set != null) {
        set.forEach(v -> result.add(QuoteSnapshot.from(v)));
      }
    } finally {
      lock.unlockRead(stamp);
    }
    return result;
  }

  /**
   * Returns all quotes for the given account.
   *
   * @param accountCode the account code (FIX tag 1)
   * @return list of quote snapshots (empty if no quotes for this account)
   */
  public List<QuoteSnapshot> getQuotesByAccount(final String accountCode) {
    final var key = keyFromString(accountCode, 16);
    final var result = new ArrayList<QuoteSnapshot>();
    final long stamp = lock.readLock();
    try {
      final var set = byAccountCode.get(key);
      if (set != null) {
        set.forEach(v -> result.add(QuoteSnapshot.from(v)));
      }
    } finally {
      lock.unlockRead(stamp);
    }
    return result;
  }

  /**
   * Returns all quotes matching the given lifecycle status.
   *
   * @param status the lifecycle status to filter by
   * @return list of matching quote snapshots
   */
  public List<QuoteSnapshot> getQuotesByStatus(final QuoteStatus status) {
    final var result = new ArrayList<QuoteSnapshot>();
    final long stamp = lock.readLock();
    try {
      byQuoteReqId
          .values()
          .forEach(
              v -> {
                if (v.status() == status) {
                  result.add(QuoteSnapshot.from(v));
                }
              });
    } finally {
      lock.unlockRead(stamp);
    }
    return result;
  }

  /**
   * Returns the total number of tracked quotes (all statuses). Uses {@code byQuoteReqId.size()}
   * since every quote has a quoteReqId (see DD-12).
   *
   * @return the quote count
   */
  public int size() {
    final long stamp = lock.readLock();
    try {
      return byQuoteReqId.size();
    } finally {
      lock.unlockRead(stamp);
    }
  }

  /**
   * Returns the total number of events that caused a decode or processing error.
   *
   * @return the error count
   */
  public long errorCount() {
    final long stamp = lock.readLock();
    try {
      return errorCount;
    } finally {
      lock.unlockRead(stamp);
    }
  }

  /**
   * Returns the total number of events successfully processed.
   *
   * @return the events processed count
   */
  public long eventsProcessed() {
    final long stamp = lock.readLock();
    try {
      return eventsProcessed;
    } finally {
      lock.unlockRead(stamp);
    }
  }

  // ---------------------------------------------------------------------------
  // Utilities
  // ---------------------------------------------------------------------------

  /**
   * Creates a ByteArrayKey from a String, NUL-padded to the given maxLength. Used on the query path
   * (allocation acceptable).
   */
  private static ByteArrayKey keyFromString(final String value, final int maxLength) {
    final var padded = new byte[maxLength];
    final var ascii = value.getBytes(StandardCharsets.US_ASCII);
    final int copyLen = Math.min(ascii.length, maxLength);
    System.arraycopy(ascii, 0, padded, 0, copyLen);
    return ByteArrayKey.copyOf(padded, 0, ProjectionUtil.sbeStrLen(maxLength, padded));
  }
}
