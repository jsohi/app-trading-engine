package com.trading.engine.projections.account;

import com.epam.deltix.gflog.api.Log;
import com.epam.deltix.gflog.api.LogFactory;
import com.trading.engine.messages.sbe.AccountLoadRejectedEventDecoder;
import com.trading.engine.messages.sbe.AccountLoadedEventDecoder;
import com.trading.engine.projections.ByteArrayKey;
import com.trading.engine.projections.Projection;
import com.trading.engine.projections.ProjectionUtil;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.locks.StampedLock;
import org.agrona.DirectBuffer;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.collections.Object2ObjectHashMap;

/**
 * CQRS read-model projection for account reference data. Consumes {@code AccountLoadedEvent} (110)
 * and {@code AccountLoadRejectedEvent} (111) from the cluster event stream.
 *
 * <p><b>Indexes:</b> two indexes are maintained for efficient query access:
 *
 * <ol>
 *   <li>Primary: accountId (long) → {@link AccountView} ({@link Long2ObjectHashMap})
 *   <li>Secondary: accountCode → {@link AccountView} ({@link Object2ObjectHashMap})
 * </ol>
 *
 * <p><b>Design rationale:</b> primary index uses {@link Long2ObjectHashMap} (not {@link
 * Object2ObjectHashMap}{@code <ByteArrayKey, ...>} as in {@link
 * com.trading.engine.projections.order.OrderProjection}) because {@code accountId} is a numeric
 * long, avoiding boxing overhead. Accounts support upsert semantics (same {@code accountId}
 * re-loaded with updated fields), unlike orders which are write-once.
 *
 * <p><b>Threading:</b> single-writer / multi-reader via {@link StampedLock}. The event-dispatch
 * thread acquires the write stamp in {@link #onEvent}. Query threads acquire pessimistic read
 * stamps in query methods (optimistic reads are unsafe with Agrona's non-concurrent collections).
 * Query methods return immutable {@link AccountReadModel} records — internal mutable {@link
 * AccountView} instances are never leaked. Pre-allocated {@code probeAccountCode} is event-thread
 * only — query methods allocate their own {@link ByteArrayKey} via {@code keyFromString()}.
 *
 * <p><b>Allocation:</b> bounded per-entity allocation on the event path (one {@link AccountView}
 * per account on first load, one {@link ByteArrayKey#copyOf()} per secondary index entry). Zero
 * allocation on upsert with same code. Query methods allocate snapshots and lists under the read
 * lock (acceptable — off hot path, per {@link Projection} interface contract).
 *
 * <p><b>Recovery:</b> full replay from Aeron Archive position 0. No snapshots. Account replay is
 * O(total AccountLoadedEvents), bounded by account cardinality x re-load frequency. Expected
 * sub-second even at 50k accounts.
 *
 * <p><b>Account status changes</b> are modeled as re-loads (new {@code AccountLoadedEvent} with
 * updated fields), not as dedicated {@code StatusChanged} events. {@code AccountLoadedEvent} (110)
 * is emitted per-record even for {@code LoadAccountBatch} (template 12) commands — see {@code
 * LoadAccountBatchHandler}. If a dedicated {@code AccountSuspendedEvent} or {@code
 * AccountClosedEvent} is added in a future schema version, this projection must register for those
 * template IDs.
 *
 * <p><b>Error handling:</b> all event processing is wrapped in a try-catch. Decode errors increment
 * {@link #errorCount()} and log via GFLog. The event is skipped (not rethrown) to prevent crashing
 * the {@link com.trading.engine.projections.EventConsumer}. {@link #lastProcessedSequence()} is
 * updated even on error.
 *
 * @see AccountView
 * @see AccountReadModel
 * @see com.trading.engine.projections.EventConsumer
 * @see com.trading.engine.projections.ProjectionRegistry
 */
public final class AccountProjection implements Projection {

  private static final Log LOG = LogFactory.getLog(AccountProjection.class);
  private static final float LOAD_FACTOR = 0.65f;

  // --- Primary and secondary indexes ---
  private final Long2ObjectHashMap<AccountView> byAccountId;
  private final Object2ObjectHashMap<ByteArrayKey, AccountView> byAccountCode;

  // --- Pre-allocated SBE flyweight decoders (reused per event) ---
  private final AccountLoadedEventDecoder loadedDecoder = new AccountLoadedEventDecoder();
  private final AccountLoadRejectedEventDecoder rejectedDecoder =
      new AccountLoadRejectedEventDecoder();

  // --- Pre-allocated probe key (event-thread only — query methods must NOT use this) ---
  private final ByteArrayKey probeAccountCode = ByteArrayKey.emptyForLookup(16);

  // --- Pre-allocated scratch byte arrays for SBE field decoding ---
  private final byte[] scratchAccountCode = new byte[16];
  private final byte[] scratchAccountName = new byte[64];
  private final byte[] scratchBaseCurrency = new byte[3];

  // --- Concurrency ---
  private final StampedLock lock = new StampedLock();

  // --- Counters ---
  private long lastProcessedSeqNo;
  private long eventsProcessed;
  private long errorCount;
  private int rejectCount;

  /**
   * Creates an AccountProjection with a default initial capacity of 256 accounts.
   *
   * <p>Suitable for development and small deployments. Operators loading 10k+ accounts should use
   * {@link #AccountProjection(int)} with a higher capacity to avoid resize churn during startup
   * replay.
   */
  public AccountProjection() {
    this(256);
  }

  /**
   * Creates an AccountProjection with the specified initial capacity for both indexes.
   *
   * @param initialCapacity expected number of accounts (determines initial map sizes)
   */
  public AccountProjection(final int initialCapacity) {
    byAccountId = new Long2ObjectHashMap<>(initialCapacity, LOAD_FACTOR);
    byAccountCode = new Object2ObjectHashMap<>(initialCapacity, LOAD_FACTOR);
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
        case AccountLoadedEventDecoder.TEMPLATE_ID ->
            onAccountLoaded(seqNo, buffer, offset, length);
        case AccountLoadRejectedEventDecoder.TEMPLATE_ID ->
            onAccountLoadRejected(seqNo, buffer, offset, length);
        default -> {
          return; // EventConsumer only dispatches registered types — do not count as processed
        }
      }
      eventsProcessed++;
    } catch (final Exception e) {
      errorCount++;
      LOG.error()
          .append("AccountProjection decode error seqNo=")
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
      byAccountId.clear();
      byAccountCode.clear();
      lastProcessedSeqNo = 0;
      eventsProcessed = 0;
      errorCount = 0;
      rejectCount = 0;
      LOG.info().append("AccountProjection reset").commit();
    } finally {
      lock.unlockWrite(stamp);
    }
  }

  // ---------------------------------------------------------------------------
  // Event handlers (called under write lock)
  // ---------------------------------------------------------------------------

  private void onAccountLoaded(
      final long seqNo, final DirectBuffer buffer, final int offset, final int length) {
    loadedDecoder.wrap(
        buffer,
        offset,
        AccountLoadedEventDecoder.BLOCK_LENGTH,
        AccountLoadedEventDecoder.SCHEMA_VERSION);

    final long accountId = loadedDecoder.accountId();
    final int accountCodeLen =
        ProjectionUtil.sbeStrLen(
            loadedDecoder.getAccountCode(scratchAccountCode, 0), scratchAccountCode);

    // Guard: accounts without codes are not queryable.
    // Throw so onEvent's catch block increments errorCount without incrementing eventsProcessed.
    if (accountCodeLen <= 0) {
      throw new IllegalArgumentException("Empty account code for accountId=" + accountId);
    }

    final int accountNameLen =
        ProjectionUtil.sbeStrLen(
            loadedDecoder.getAccountName(scratchAccountName, 0), scratchAccountName);
    final int baseCurrencyLen =
        ProjectionUtil.sbeStrLen(
            loadedDecoder.getBaseCurrency(scratchBaseCurrency, 0), scratchBaseCurrency);

    // Check for existing view (upsert path)
    final AccountView existing = byAccountId.get(accountId);

    if (existing != null) {
      // Upsert: check if account code changed
      final int oldLen = existing.accountCodeLen();
      final boolean codeChanged;
      if (oldLen != accountCodeLen) {
        codeChanged = true;
      } else {
        codeChanged =
            Arrays.mismatch(
                    scratchAccountCode, 0, accountCodeLen, existing.accountCode(), 0, oldLen)
                != -1;
      }

      if (codeChanged) {
        // Remove old secondary index entry, but only if it still points to this view.
        // Another account may have hijacked the code (last-write-wins), so unconditional
        // removal would corrupt the index for that other account.
        probeAccountCode.set(existing.accountCode(), 0, oldLen);
        if (byAccountCode.get(probeAccountCode) == existing) {
          byAccountCode.remove(probeAccountCode);
        }
        // Insert new secondary index entry
        byAccountCode.put(ByteArrayKey.copyOf(scratchAccountCode, 0, accountCodeLen), existing);
      } else {
        // Code did NOT change. Ensure the index still points to us (last-write-wins reclaim)
        // in case another account hijacked our code and then changed away from it.
        probeAccountCode.set(scratchAccountCode, 0, accountCodeLen);
        if (byAccountCode.get(probeAccountCode) != existing) {
          byAccountCode.put(ByteArrayKey.copyOf(scratchAccountCode, 0, accountCodeLen), existing);
        }
      }

      populateView(existing, accountId, accountCodeLen, accountNameLen, baseCurrencyLen, seqNo);
    } else {
      // Insert: new account
      final AccountView view = new AccountView();
      populateView(view, accountId, accountCodeLen, accountNameLen, baseCurrencyLen, seqNo);
      byAccountId.put(accountId, view);
      byAccountCode.put(ByteArrayKey.copyOf(scratchAccountCode, 0, accountCodeLen), view);
    }
  }

  /**
   * Populates all fields on the view from the current {@code loadedDecoder} state and scratch
   * buffers.
   */
  private void populateView(
      final AccountView view,
      final long accountId,
      final int accountCodeLen,
      final int accountNameLen,
      final int baseCurrencyLen,
      final long seqNo) {
    view.setAccountId(accountId);
    view.setParentAccountId(loadedDecoder.parentAccountId());
    view.setAccountCode(scratchAccountCode, 0, accountCodeLen);
    view.setAcctIdSource(loadedDecoder.acctIdSource());
    view.setAccountName(scratchAccountName, 0, accountNameLen);
    view.setAccountType(loadedDecoder.accountType());
    view.setBaseCurrency(scratchBaseCurrency, 0, baseCurrencyLen);
    view.setStatus(loadedDecoder.status());
    view.setComplianceStatus(loadedDecoder.complianceStatus());
    view.setCapabilities(loadedDecoder.capabilities());
    view.setTransactTime(loadedDecoder.transactTime());
    view.setSequenceNumber(seqNo);
    view.setLastUpdatedAt(loadedDecoder.timestamp());
  }

  private void onAccountLoadRejected(
      final long seqNo, final DirectBuffer buffer, final int offset, final int length) {
    rejectedDecoder.wrap(
        buffer,
        offset,
        AccountLoadRejectedEventDecoder.BLOCK_LENGTH,
        AccountLoadRejectedEventDecoder.SCHEMA_VERSION);

    rejectCount++;

    // Decode accountCode into scratch buffer to avoid SBE convenience method String allocation.
    // GFLog has no append(byte[], offset, length), so append char-by-char for zero allocation.
    rejectedDecoder.getAccountCode(scratchAccountCode, 0);
    final int rejCodeLen = ProjectionUtil.sbeStrLen(scratchAccountCode.length, scratchAccountCode);

    final var entry = LOG.warn().append("AccountProjection: account load rejected, accountCode=");
    for (int i = 0; i < rejCodeLen; i++) {
      entry.append((char) scratchAccountCode[i]);
    }
    entry.append(" reason=").append(rejectedDecoder.rejectReason().name()).commit();
  }

  // ---------------------------------------------------------------------------
  // Query methods (acquire read stamp, return immutable snapshots)
  // ---------------------------------------------------------------------------

  /**
   * Looks up an account by numeric account identifier.
   *
   * @param accountId the account ID (custom tag 10024)
   * @return the account read model, or {@code null} if not found
   */
  public AccountReadModel getByAccountId(final long accountId) {
    final long stamp = lock.readLock();
    try {
      final AccountView view = byAccountId.get(accountId);
      return view != null ? AccountReadModel.from(view) : null;
    } finally {
      lock.unlockRead(stamp);
    }
  }

  /**
   * Looks up an account by account code string. Allocates a {@link ByteArrayKey} on every call
   * (query path — never uses the pre-allocated probe key, which is event-thread only). Returns
   * {@code null} for null or overlength codes (SBE Account field is char[16]).
   *
   * @param accountCode the account code (FIX tag 1), at most 16 characters
   * @return the account read model, or {@code null} if not found or code exceeds max length
   */
  public AccountReadModel getByAccountCode(final String accountCode) {
    if (accountCode == null || accountCode.length() > 16) {
      return null;
    }
    final ByteArrayKey key = keyFromString(accountCode, 16);
    final long stamp = lock.readLock();
    try {
      final AccountView view = byAccountCode.get(key);
      return view != null ? AccountReadModel.from(view) : null;
    } finally {
      lock.unlockRead(stamp);
    }
  }

  /**
   * Returns all loaded accounts as immutable read models. O(n) scan + n snapshot allocations under
   * the read lock. Acceptable for diagnostic/query path.
   *
   * @return list of all account read models
   */
  public List<AccountReadModel> getAll() {
    final List<AccountReadModel> result = new ArrayList<>();
    final long stamp = lock.readLock();
    try {
      byAccountId.values().forEach(v -> result.add(AccountReadModel.from(v)));
    } finally {
      lock.unlockRead(stamp);
    }
    return result;
  }

  /**
   * Returns all accounts with status {@link
   * com.trading.engine.messages.sbe.AccountStatusEnum#Active}. O(n) full scan of all accounts.
   *
   * @return list of active account read models
   */
  public List<AccountReadModel> getActiveAccounts() {
    final List<AccountReadModel> result = new ArrayList<>();
    final long stamp = lock.readLock();
    try {
      byAccountId
          .values()
          .forEach(
              v -> {
                if (v.isActive()) {
                  result.add(AccountReadModel.from(v));
                }
              });
    } finally {
      lock.unlockRead(stamp);
    }
    return result;
  }

  /**
   * Returns the number of loaded accounts (does NOT include rejected accounts).
   *
   * @return the account count
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
   * Returns the number of account load rejection events received.
   *
   * @return the reject count
   */
  public int rejectCount() {
    final long stamp = lock.readLock();
    try {
      return rejectCount;
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
   * Creates a {@link ByteArrayKey} from a String, NUL-padded to the given maxLength. Used on the
   * query path (allocation acceptable). Produces a key with trimmed length matching the insert-path
   * key construction.
   */
  private static ByteArrayKey keyFromString(final String value, final int maxLength) {
    final byte[] padded = new byte[maxLength];
    final byte[] ascii = value.getBytes(StandardCharsets.US_ASCII);
    final int copyLen = Math.min(ascii.length, maxLength);
    System.arraycopy(ascii, 0, padded, 0, copyLen);
    return ByteArrayKey.copyOf(padded, 0, ProjectionUtil.sbeStrLen(maxLength, padded));
  }
}
