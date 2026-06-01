package com.trading.engine.projections.risklimits;

import com.epam.deltix.gflog.api.Log;
import com.epam.deltix.gflog.api.LogFactory;
import com.trading.engine.messages.sbe.RiskLimitChangedEventDecoder;
import com.trading.engine.messages.sbe.RiskLimitLoadedEventDecoder;
import com.trading.engine.projections.Projection;
import java.util.concurrent.locks.StampedLock;
import org.agrona.DirectBuffer;
import org.agrona.collections.Long2ObjectHashMap;

/**
 * CQRS read-model projection over per-account risk limits (APP-62 §A). Consumes {@code
 * RiskLimitLoadedEvent} (SBE template 115) to upsert per-account {@link RiskLimitRecordView}
 * snapshots and {@code RiskLimitChangedEvent} (SBE template 119) for audit-trail counters.
 *
 * <p><b>Index.</b> Primary index keyed by numeric {@code accountId} → {@link RiskLimitRecordView}.
 * No secondary {@code accountCode} index — the cluster's {@code RiskLimitLoadedEvent} schema does
 * not carry an account code, so callers wanting accountCode-keyed lookups must join via {@link
 * com.trading.engine.projections.account.AccountProjection} (the bridge uses this pattern for the
 * fix-client-bridge account-limits surface).
 *
 * <p><b>Threading.</b> Single-writer / multi-reader via {@link StampedLock}. The event-dispatch
 * thread (the {@code EventConsumer} poll loop) acquires the write stamp inside {@link #onEvent};
 * query threads acquire pessimistic read stamps. Optimistic reads are unsafe against Agrona's
 * non-concurrent collections.
 *
 * <p><b>Allocation.</b> One {@link RiskLimitRecordView} per upsert (acceptable on the read side per
 * the {@link Projection} interface contract). Zero allocation on the read path beyond the record
 * dereference. SBE flyweight decoders are pre-allocated.
 *
 * <p><b>Recovery.</b> Full replay from Aeron Archive position 0. No snapshots (per CLAUDE.md
 * "Projections never snapshot"); replay rebuilds the index from the immutable event log.
 *
 * <p><b>Error handling.</b> Decode errors increment {@link #errorCount()} and are logged via GFLog.
 * The event is skipped (not rethrown) to prevent crashing the {@link
 * com.trading.engine.projections.EventConsumer} poll loop.
 *
 * @see RiskLimitRecordView
 * @see com.trading.engine.projections.EventConsumer
 */
public final class RiskLimitProjection implements Projection {

  private static final Log LOG = LogFactory.getLog(RiskLimitProjection.class);
  private static final float LOAD_FACTOR = 0.65f;

  // --- Pre-allocated SBE decoders (event-thread only) ---
  private final RiskLimitLoadedEventDecoder loadedDecoder = new RiskLimitLoadedEventDecoder();
  private final RiskLimitChangedEventDecoder changedDecoder = new RiskLimitChangedEventDecoder();

  // --- Primary index: accountId → latest snapshot ---
  private final Long2ObjectHashMap<RiskLimitRecordView> byAccountId;

  // --- Concurrency ---
  private final StampedLock lock = new StampedLock();

  // --- Counters (volatile for cross-thread visibility without requiring read lock) ---
  private volatile long lastProcessedSeqNo;
  private volatile long eventsProcessed;
  private volatile long errorCount;
  private volatile long changedEventCount;

  /** Creates a RiskLimitProjection with default initial capacity (256 accounts). */
  public RiskLimitProjection() {
    this(256);
  }

  /**
   * Creates a RiskLimitProjection with the specified initial capacity for the primary index.
   *
   * @param initialCapacity expected number of accounts (determines initial map size)
   */
  public RiskLimitProjection(final int initialCapacity) {
    byAccountId = new Long2ObjectHashMap<>(initialCapacity, LOAD_FACTOR);
  }

  // ---------------------------------------------------------------------------
  // Projection interface
  // ---------------------------------------------------------------------------

  /**
   * Dispatches the inbound event to the matching decoder under the write lock. Unregistered
   * template ids (anything other than {@code RiskLimitLoadedEvent} 115 and {@code
   * RiskLimitChangedEvent} 119) are skipped without bumping {@link #eventsProcessed} — the
   * projection only counts events it acts on.
   *
   * <p><b>Cursor semantics.</b> {@link #lastProcessedSeqNo} advances in the {@code finally} block
   * for EVERY call — including unregistered template skips and decode errors — because the position
   * cursor reflects "we received and inspected seqNo N" regardless of whether we mutated state.
   * This matches the standard CQRS projection contract used by {@link
   * com.trading.engine.projections.EventConsumer}: the cursor is a high-water mark of consumed
   * events, NOT a count of applied state mutations (which are tracked separately via {@link
   * #eventsProcessed}). Without this invariant a downstream lag monitor would erroneously report
   * the projection as stale every time a foreign-template event flowed through the stream.
   *
   * @param seqNo monotonically increasing global event sequence number
   * @param eventType SBE template id of the event
   * @param buffer source buffer holding the SBE-encoded event body
   * @param offset start offset of the body in {@code buffer}
   * @param length body length in bytes (unused — decoders read their own length from the SBE
   *     header)
   */
  @Override
  public void onEvent(
      final long seqNo,
      final int eventType,
      final DirectBuffer buffer,
      final int offset,
      final int length) {
    // Gemini R3 fix: the EventConsumer dispatches EVERY domain event here (OrderCreated /
    // OrderFilled / OrderCanceled / QuoteCreated / etc.), but this projection only mutates state
    // on tpl 115 + 119. Acquiring the write stamp on every event regardless of template would
    // serialize concurrent readers against unrelated egress traffic. Fast-path: peek the eventType
    // BEFORE the lock; if it's not one we mutate state for, advance the volatile cursor and
    // return without acquiring the lock. The lock now only covers our two real upsert templates.
    if (eventType != RiskLimitLoadedEventDecoder.TEMPLATE_ID
        && eventType != RiskLimitChangedEventDecoder.TEMPLATE_ID) {
      lastProcessedSeqNo = seqNo; // volatile write — readers' lastProcessedSequence() stays fresh
      return; // unregistered type — skip without bumping eventsProcessed; no lock acquired
    }
    final long stamp = lock.writeLock();
    try {
      switch (eventType) {
        case RiskLimitLoadedEventDecoder.TEMPLATE_ID -> onRiskLimitLoaded(seqNo, buffer, offset);
        case RiskLimitChangedEventDecoder.TEMPLATE_ID -> onRiskLimitChanged(seqNo, buffer, offset);
        default -> {
          // Unreachable: the pre-lock filter above returns for any non-mutating template. Kept
          // as a defensive arm so any future template-ID added to the outer if becomes a
          // compile-time-visible omission here.
          return;
        }
      }
      eventsProcessed++;
    } catch (final Exception e) {
      errorCount++;
      LOG.error()
          .append("RiskLimitProjection decode error seqNo=")
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

  /**
   * {@inheritDoc}
   *
   * <p>Volatile read — no {@link StampedLock} acquisition needed for a single scalar field. The
   * {@code volatile} qualifier on {@link #lastProcessedSeqNo} is sufficient for cross-thread
   * visibility; the {@link StampedLock} is reserved for compound state (the {@link #byAccountId}
   * map) where lock-free reads would race against ongoing structural mutation.
   */
  @Override
  public long lastProcessedSequence() {
    return lastProcessedSeqNo;
  }

  @Override
  public void reset() {
    final long stamp = lock.writeLock();
    try {
      byAccountId.clear();
      lastProcessedSeqNo = 0L;
      eventsProcessed = 0L;
      errorCount = 0L;
      changedEventCount = 0L;
      LOG.info().append("RiskLimitProjection reset").commit();
    } finally {
      lock.unlockWrite(stamp);
    }
  }

  // ---------------------------------------------------------------------------
  // Event handlers (called under write lock)
  // ---------------------------------------------------------------------------

  /**
   * Decode a {@code RiskLimitLoadedEvent} (template 115) and upsert the corresponding {@link
   * RiskLimitRecordView}. Upsert semantics: a second load for the same {@code accountId} overwrites
   * the prior record (last-write-wins, matching the cluster's {@code RiskLimitStore.put}).
   */
  private void onRiskLimitLoaded(final long seqNo, final DirectBuffer buffer, final int offset) {
    loadedDecoder.wrap(
        buffer,
        offset,
        RiskLimitLoadedEventDecoder.BLOCK_LENGTH,
        RiskLimitLoadedEventDecoder.SCHEMA_VERSION);

    final long accountId = loadedDecoder.accountId();
    final var record =
        new RiskLimitRecordView(
            accountId,
            loadedDecoder.maxOrderSize(),
            loadedDecoder.maxOrderNotional(),
            loadedDecoder.maxDailyVolume(),
            loadedDecoder.maxOrdersPerSecond(),
            loadedDecoder.maxLongPosition(),
            loadedDecoder.maxShortPosition(),
            loadedDecoder.positionLimitEnabled() != 0,
            loadedDecoder.priceDeviationBps(),
            loadedDecoder.fatFingerEnabled() != 0,
            loadedDecoder.fatFingerFailClosed() != 0,
            loadedDecoder.idleSessionTimeoutNanos(),
            loadedDecoder.status(),
            loadedDecoder.transactTime(),
            seqNo);
    byAccountId.put(accountId, record);
  }

  /**
   * Decode a {@code RiskLimitChangedEvent} (template 119) and bump the audit counter. Per APP-62
   * §D, every {@code LoadRiskLimit} that updates an existing record emits BOTH a {@code
   * RiskLimitLoadedEvent} (consumed by {@link #onRiskLimitLoaded} for the full snapshot) AND one
   * {@code RiskLimitChangedEvent} per modified field (audit trail). The projection therefore
   * mutates state only on the loaded event; the changed event is observed for counters but does not
   * duplicate the upsert.
   */
  private void onRiskLimitChanged(final long seqNo, final DirectBuffer buffer, final int offset) {
    changedDecoder.wrap(
        buffer,
        offset,
        RiskLimitChangedEventDecoder.BLOCK_LENGTH,
        RiskLimitChangedEventDecoder.SCHEMA_VERSION);
    changedEventCount++;
  }

  // ---------------------------------------------------------------------------
  // Query methods
  // ---------------------------------------------------------------------------

  /**
   * Look up the current risk-limit record for the given numeric account id.
   *
   * @param accountId numeric account identifier
   * @return the immutable record, or {@code null} if no limit has been loaded for this account
   */
  public RiskLimitRecordView getByAccountId(final long accountId) {
    final long stamp = lock.readLock();
    try {
      return byAccountId.get(accountId);
    } finally {
      lock.unlockRead(stamp);
    }
  }

  /**
   * Returns the number of loaded risk-limit records (accounts seen on the LoadedEvent stream).
   *
   * <p>Acquires the {@link StampedLock} read stamp because Agrona's {@code Long2ObjectHashMap.size}
   * field is a plain (non-volatile) {@code int} mutated by {@code put} under the write lock; a
   * reader without a happens-before edge to the writer could otherwise observe a stale value
   * indefinitely on weakly-ordered platforms. This is a cold-path observability call invoked from
   * health endpoints — the read-lock cost is negligible vs. the correctness guarantee.
   *
   * @return the index size (exact at the instant of the lock acquisition)
   */
  public int size() {
    final long stamp = lock.readLock();
    try {
      return byAccountId.size();
    } finally {
      lock.unlockRead(stamp);
    }
  }

  /**
   * Returns the cumulative count of {@code RiskLimitChangedEvent}s consumed since the last {@link
   * #reset()}.
   *
   * <p>Volatile read — no {@link StampedLock} acquisition needed for a single scalar field.
   *
   * @return the change-event count
   */
  public long changedEventCount() {
    return changedEventCount;
  }

  /**
   * Returns the cumulative count of decode/processing errors since the last {@link #reset()}.
   *
   * <p>Volatile read — no {@link StampedLock} acquisition needed for a single scalar field.
   *
   * @return the error count
   */
  public long errorCount() {
    return errorCount;
  }

  /**
   * Returns the cumulative count of events successfully processed since the last {@link #reset()}.
   *
   * <p>Volatile read — no {@link StampedLock} acquisition needed for a single scalar field.
   *
   * @return the events-processed count
   */
  public long eventsProcessed() {
    return eventsProcessed;
  }
}
