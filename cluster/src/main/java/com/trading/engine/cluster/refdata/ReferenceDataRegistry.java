package com.trading.engine.cluster.refdata;

import com.trading.engine.messages.sbe.MessageHeaderDecoder;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Int2ObjectHashMap;
import org.agrona.collections.IntObjConsumer;
import org.agrona.collections.ObjectHashSet;

/**
 * Central registry for reference-data stores and their command loaders. The cluster's {@code
 * TradingClusteredService} (created in APP-8, Wave 4) constructs one of these, registers every
 * store + loader at startup, and then routes inbound commands and snapshot fragments here:
 *
 * <pre>
 *   onSessionMessage(...) → registry.dispatchCommand(header, src, ..., eventDst, ...)
 *   onTakeSnapshot(...)   → registry.snapshotAll(snapshotBuffer, 0)
 *   onLoadSnapshot(...)   → registry.resetAll(); for each fragment: registry.restoreFragment(...)
 * </pre>
 *
 * <p>Future ref-data PRs (APP-128 venues, APP-129 parties, APP-125 calendars, instruments, symbols,
 * …) implement {@link ReferenceDataStore} + {@link ReferenceDataLoader} and call {@link
 * #registerStore} / {@link #registerLoader}; no changes to the registry or to {@code
 * TradingClusteredService} are required to add a new ref-data type.
 *
 * <p><b>Threading.</b> Single-threaded by contract — registration runs at startup, dispatch runs on
 * the cluster duty-cycle thread. No synchronization, no {@code volatile}. {@link #snapshotAll}
 * relies on {@code snapshotKeysFillIdx} being reset at the start of every call; concurrent or
 * re-entrant invocation would corrupt that counter.
 *
 * <p><b>Allocation.</b> Dispatch and snapshot iteration are zero-allocation: primitive-keyed Agrona
 * maps for routing, pre-allocated set for distinct stores. Snapshot save/restore inside each store
 * may allocate per the project exemption.
 */
public final class ReferenceDataRegistry {

  /** Sentinel returned by {@link #dispatchCommand} when the templateId is not registered. */
  public static final int NOT_HANDLED = -1;

  /**
   * Initial size of {@link #snapshotKeysScratch}. Practical upper bound on the number of distinct
   * ref-data stores registered in a single cluster; the scratch grows on demand if this turns out
   * to be too small.
   */
  private static final int INITIAL_STORE_SCRATCH_CAPACITY = 16;

  private final Int2ObjectHashMap<ReferenceDataStore> storesBySnapshotTemplateId =
      new Int2ObjectHashMap<>();
  private final Int2ObjectHashMap<ReferenceDataLoader> loadersByCommandTemplateId =
      new Int2ObjectHashMap<>();
  private final Int2ObjectHashMap<ReferenceDataBatchLoader> batchLoadersByBatchTemplateId =
      new Int2ObjectHashMap<>();
  private final ObjectHashSet<ReferenceDataStore> distinctStores = new ObjectHashSet<>();

  // Scratch for draining storesBySnapshotTemplateId in deterministic-order snapshotAll().
  // Grows on demand when the registry exceeds the initial capacity (unlikely, but defensive).
  // Stored IntObjConsumer avoids the per-call KeyIterator allocation of the Iterator API.
  private int[] snapshotKeysScratch = new int[INITIAL_STORE_SCRATCH_CAPACITY];
  private int snapshotKeysFillIdx;
  private final IntObjConsumer<ReferenceDataStore> snapshotKeyCollector =
      (templateId, store) -> snapshotKeysScratch[snapshotKeysFillIdx++] = templateId;

  /**
   * Register a store. Stores are indexed by their {@link ReferenceDataStore#snapshotTemplateId()}
   * for snapshot routing and added to a distinct-set for {@link #snapshotAll} iteration.
   *
   * @throws NullPointerException if {@code store} is null
   * @throws IllegalArgumentException if another store is already registered for the same snapshot
   *     templateId
   */
  public void registerStore(final ReferenceDataStore store) {
    if (store == null) {
      throw new NullPointerException("store must not be null");
    }
    final int templateId = store.snapshotTemplateId();
    if (storesBySnapshotTemplateId.containsKey(templateId)) {
      throw new IllegalArgumentException(
          "store already registered for snapshot templateId " + templateId);
    }
    storesBySnapshotTemplateId.put(templateId, store);
    distinctStores.add(store);
  }

  /**
   * Register a single-record command loader.
   *
   * @throws NullPointerException if {@code loader} is null
   * @throws IllegalArgumentException if another loader is already registered for the same command
   *     templateId
   */
  public void registerLoader(final ReferenceDataLoader loader) {
    if (loader == null) {
      throw new NullPointerException("loader must not be null");
    }
    final int templateId = loader.commandTemplateId();
    if (loadersByCommandTemplateId.containsKey(templateId)) {
      throw new IllegalArgumentException(
          "loader already registered for command templateId " + templateId);
    }
    loadersByCommandTemplateId.put(templateId, loader);
  }

  /**
   * Register a batch command loader. Batch templateIds are kept in a separate map so a single
   * commandTemplateId can have both a single-record loader (for admin updates) and a batch loader
   * (for start-of-day) if needed.
   *
   * @throws NullPointerException if {@code loader} is null
   * @throws IllegalArgumentException if another batch loader is already registered for the same
   *     templateId
   */
  public void registerBatchLoader(final ReferenceDataBatchLoader loader) {
    if (loader == null) {
      throw new NullPointerException("loader must not be null");
    }
    final int templateId = loader.batchCommandTemplateId();
    if (batchLoadersByBatchTemplateId.containsKey(templateId)) {
      throw new IllegalArgumentException(
          "batch loader already registered for templateId " + templateId);
    }
    batchLoadersByBatchTemplateId.put(templateId, loader);
  }

  // ---------------------------------------------------------------------------
  // Command dispatch
  // ---------------------------------------------------------------------------

  /**
   * Dispatch one inbound command. Looks at {@code header.templateId()}, routes to the matching
   * single-record loader OR batch loader, and writes the resulting event(s) into {@code eventDst}
   * starting at {@code eventDstOffset}.
   *
   * @return total bytes of emitted events, or {@link #NOT_HANDLED} if {@code header.templateId()}
   *     is not a registered ref-data command (the caller may try other dispatch tables)
   */
  public int dispatchCommand(
      final MessageHeaderDecoder header,
      final DirectBuffer src,
      final int srcOffset,
      final int srcLength,
      final MutableDirectBuffer eventDst,
      final int eventDstOffset,
      final long sequenceNumber,
      final long clusterTimestampNanos) {
    final int templateId = header.templateId();
    final ReferenceDataLoader loader = loadersByCommandTemplateId.get(templateId);
    if (loader != null) {
      return loader.onCommand(
          header,
          src,
          srcOffset,
          srcLength,
          eventDst,
          eventDstOffset,
          sequenceNumber,
          clusterTimestampNanos);
    }
    final ReferenceDataBatchLoader batchLoader = batchLoadersByBatchTemplateId.get(templateId);
    if (batchLoader != null) {
      return batchLoader.onBatchCommand(
          header,
          src,
          srcOffset,
          srcLength,
          eventDst,
          eventDstOffset,
          sequenceNumber,
          clusterTimestampNanos);
    }
    return NOT_HANDLED;
  }

  // ---------------------------------------------------------------------------
  // Snapshot orchestration
  // ---------------------------------------------------------------------------

  /**
   * Serialize every registered store into {@code dst} starting at {@code offset}. Each store's SBE
   * message is written contiguously, in ascending {@link ReferenceDataStore#snapshotTemplateId()}
   * order so the byte output is deterministic across runs and across leaders. Returns total bytes
   * written across all stores.
   */
  public int snapshotAll(final MutableDirectBuffer dst, final int offset) {
    // Drain via a stored IntObjConsumer — no KeyIterator allocation on the snapshot path.
    // Scratch grows on demand if the registry grows beyond the initial size.
    final int storeCount = storesBySnapshotTemplateId.size();
    if (snapshotKeysScratch.length < storeCount) {
      snapshotKeysScratch = new int[storeCount];
    }
    snapshotKeysFillIdx = 0;
    storesBySnapshotTemplateId.forEachInt(snapshotKeyCollector);
    java.util.Arrays.sort(snapshotKeysScratch, 0, storeCount);

    int written = 0;
    for (int i = 0; i < storeCount; i++) {
      final ReferenceDataStore store = storesBySnapshotTemplateId.get(snapshotKeysScratch[i]);
      written += store.snapshotTo(dst, offset + written);
    }
    return written;
  }

  /**
   * Drop in-memory state in every registered store. Called by the cluster's {@code onLoadSnapshot}
   * at the start of recovery, before any fragment is replayed.
   */
  public void resetAll() {
    for (final ReferenceDataStore store : distinctStores) {
      store.clear();
    }
  }

  /**
   * Replay one snapshot fragment. The caller has already wrapped {@code header} over the fragment
   * at {@code src[offset..]}; this method routes by {@code header.templateId()} to the store
   * registered for that snapshot templateId and calls its {@link ReferenceDataStore#restoreFrom}.
   *
   * @return number of bytes consumed by the fragment, or {@link #NOT_HANDLED} if no store is
   *     registered for {@code header.templateId()}
   */
  public int restoreFragment(
      final MessageHeaderDecoder header, final DirectBuffer src, final int offset) {
    final ReferenceDataStore store = storesBySnapshotTemplateId.get(header.templateId());
    if (store == null) {
      return NOT_HANDLED;
    }
    return store.restoreFrom(src, offset);
  }

  // ---------------------------------------------------------------------------
  // Diagnostics
  // ---------------------------------------------------------------------------

  public int storeCount() {
    return distinctStores.size();
  }

  /**
   * Look up the registered store for a given snapshot templateId. Returns {@code null} if no store
   * is registered for that id. Used by consumers (e.g. {@code TradingClusteredService}'s
   * constructor-time wiring assertion) to verify that a concrete store reference held by the
   * consumer is the same instance that is registered in the dispatch tables.
   */
  public ReferenceDataStore storeForSnapshotTemplateId(final int templateId) {
    return storesBySnapshotTemplateId.get(templateId);
  }

  public int loaderCount() {
    return loadersByCommandTemplateId.size();
  }

  public int batchLoaderCount() {
    return batchLoadersByBatchTemplateId.size();
  }
}
