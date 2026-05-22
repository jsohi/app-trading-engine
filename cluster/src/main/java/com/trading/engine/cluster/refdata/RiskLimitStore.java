package com.trading.engine.cluster.refdata;

import com.trading.engine.messages.sbe.MessageHeaderDecoder;
import com.trading.engine.messages.sbe.MessageHeaderEncoder;
import com.trading.engine.messages.sbe.RiskLimitSnapshotDecoder;
import com.trading.engine.messages.sbe.RiskLimitSnapshotEncoder;
import com.trading.engine.messages.sbe.RiskLimitSnapshotEncoder.NoRiskLimitsEncoder;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.collections.LongObjConsumer;

/**
 * Replicated in-cluster risk-limit store, keyed by {@code accountId}. Per industry standard (CME
 * Globex Credit Controls, Eurex T7 pre-trade risk, exchange-core), risk limits live in a dedicated
 * store separate from {@link AccountStore} so they can change on a different cadence and (in a
 * future PR) extend to hierarchical scoping (firm > desk > trader > account).
 *
 * <p>Snapshot determinism: records are written in ascending {@code accountId} order via a sorted
 * {@link LongArrayList}, never the hash map's natural order.
 *
 * <p>Hot-path lookup is {@code O(1)} via {@link Long2ObjectHashMap#get(long)} — zero allocation, no
 * boxing.
 *
 * <p><b>Threading.</b> Not thread-safe — single-threaded cluster duty cycle only. {@link
 * #snapshotTo} relies on {@code snapshotKeysFillIdx} being reset at the start of every call;
 * concurrent or re-entrant invocation would corrupt that counter.
 */
public final class RiskLimitStore implements ReferenceDataStore {

  /** SBE template id for {@code RiskLimitSnapshot}. */
  public static final int SNAPSHOT_TEMPLATE_ID = RiskLimitSnapshotEncoder.TEMPLATE_ID;

  private static final int INITIAL_CAPACITY = 4096;
  private static final float LOAD_FACTOR = 0.65f;

  private final Long2ObjectHashMap<RiskLimitState> byAccountId =
      new Long2ObjectHashMap<>(INITIAL_CAPACITY, LOAD_FACTOR);

  // Pre-allocated SBE flyweights.
  private final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
  private final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();
  private final RiskLimitSnapshotEncoder snapshotEncoder = new RiskLimitSnapshotEncoder();
  private final RiskLimitSnapshotDecoder snapshotDecoder = new RiskLimitSnapshotDecoder();

  // Scratch for draining byAccountId in deterministic-order snapshot encoding. Grows on
  // demand when the map exceeds the current array. Stored LongObjConsumer avoids the
  // per-call KeyIterator allocation of the Iterator API.
  private long[] snapshotKeysScratch = new long[INITIAL_CAPACITY];
  private int snapshotKeysFillIdx;
  private final LongObjConsumer<RiskLimitState> snapshotKeyCollector =
      (key, state) -> snapshotKeysScratch[snapshotKeysFillIdx++] = key;

  @Override
  public int snapshotTemplateId() {
    return SNAPSHOT_TEMPLATE_ID;
  }

  @Override
  public int size() {
    return byAccountId.size();
  }

  @Override
  public void clear() {
    byAccountId.clear();
  }

  // ---------------------------------------------------------------------------
  // Hot-path
  // ---------------------------------------------------------------------------

  /** O(1) lookup by accountId. Zero allocation. */
  public RiskLimitState get(final long accountId) {
    return byAccountId.get(accountId);
  }

  public boolean contains(final long accountId) {
    return byAccountId.containsKey(accountId);
  }

  /** Insert or overwrite a risk-limit record. The state is stored by reference. */
  public void put(final RiskLimitState state) {
    byAccountId.put(state.accountId(), state);
  }

  // ---------------------------------------------------------------------------
  // Snapshot save / restore
  // ---------------------------------------------------------------------------

  @Override
  public int snapshotTo(final MutableDirectBuffer dst, final int offset) {
    snapshotEncoder.wrapAndApplyHeader(dst, offset, headerEncoder);
    final int recordCount = byAccountId.size();
    final NoRiskLimitsEncoder group = snapshotEncoder.noRiskLimitsCount(recordCount);

    if (recordCount > 0) {
      // Drain via a stored LongObjConsumer — no KeyIterator allocation on the snapshot path.
      // Scratch grows on demand once when the map first outgrows INITIAL_CAPACITY.
      if (snapshotKeysScratch.length < recordCount) {
        snapshotKeysScratch = new long[recordCount];
      }
      snapshotKeysFillIdx = 0;
      byAccountId.forEachLong(snapshotKeyCollector);
      java.util.Arrays.sort(snapshotKeysScratch, 0, recordCount);

      for (int i = 0; i < recordCount; i++) {
        final long id = snapshotKeysScratch[i];
        final RiskLimitState state = byAccountId.get(id);
        group.next();
        group.accountId(state.accountId());
        group.maxOrderSize(state.maxOrderSize());
        group.maxOrderNotional(state.maxOrderNotional());
        group.maxDailyVolume(state.maxDailyVolume());
        group.maxDailyLossBps(state.maxDailyLossBps());
        // SBE encoder for uint32 takes long-widened value; State stores it as long for uniformity.
        group.maxOrdersPerSecond(state.maxOrdersPerSecond());
        group.status(state.status());
        group.transactTime(state.transactTime());
      }
    }

    return MessageHeaderEncoder.ENCODED_LENGTH + snapshotEncoder.encodedLength();
  }

  @Override
  public int restoreFrom(final DirectBuffer src, final int offset) {
    headerDecoder.wrap(src, offset);
    snapshotDecoder.wrap(
        src,
        offset + MessageHeaderDecoder.ENCODED_LENGTH,
        headerDecoder.blockLength(),
        headerDecoder.version());
    // Defensive: drop existing limits so a smaller/empty snapshot doesn't leave stale
    // entries behind.
    clear();

    final RiskLimitSnapshotDecoder.NoRiskLimitsDecoder group = snapshotDecoder.noRiskLimits();
    while (group.hasNext()) {
      group.next();
      final RiskLimitState state = new RiskLimitState();
      state.setAccountId(group.accountId());
      state.setMaxOrderSize(group.maxOrderSize());
      state.setMaxOrderNotional(group.maxOrderNotional());
      state.setMaxDailyVolume(group.maxDailyVolume());
      state.setMaxDailyLossBps(group.maxDailyLossBps());
      state.setMaxOrdersPerSecond(group.maxOrdersPerSecond());
      state.setStatus(group.status());
      state.setTransactTime(group.transactTime());
      byAccountId.put(state.accountId(), state);
    }

    return MessageHeaderDecoder.ENCODED_LENGTH + snapshotDecoder.encodedLength();
  }
}
