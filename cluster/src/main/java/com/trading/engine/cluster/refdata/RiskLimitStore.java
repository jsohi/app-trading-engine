package com.trading.engine.cluster.refdata;

import com.trading.engine.messages.sbe.MessageHeaderDecoder;
import com.trading.engine.messages.sbe.MessageHeaderEncoder;
import com.trading.engine.messages.sbe.RiskLimitSnapshotDecoder;
import com.trading.engine.messages.sbe.RiskLimitSnapshotEncoder;
import com.trading.engine.messages.sbe.RiskLimitSnapshotEncoder.NoRiskLimitsEncoder;
import java.util.Arrays;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.collections.LongObjConsumer;

/**
 * Replicated in-cluster risk-limit store, keyed by {@code accountId}. Per industry standard (CME
 * Globex Credit Controls, Eurex T7 pre-trade risk, exchange-core), risk limits live in a dedicated
 * store separate from {@link AccountStore} so they can change on a different cadence and (in a
 * future PR) extend to hierarchical scoping (firm &gt; desk &gt; trader &gt; account).
 *
 * <p>Snapshot determinism: records are written in ascending {@code accountId} order via a sorted
 * scratch array, never the hash map's natural order.
 *
 * <p>Hot-path lookup is {@code O(1)} via {@link Long2ObjectHashMap#get(long)} — zero allocation, no
 * boxing.
 *
 * <p><b>Threading.</b> Not thread-safe — single-threaded cluster duty cycle only. {@link
 * #snapshotTo} relies on {@code snapshotKeysFillIdx} being reset at the start of every call;
 * concurrent or re-entrant invocation would corrupt that counter.
 *
 * <p>APP-62: extended to carry the 9 new risk-limit fields (position L/S caps, fat-finger knobs,
 * per-account idle timeout, 4-eyes proposer/approver identifiers). {@code maxDailyLossBps} field
 * was removed from the schema this PR — re-added by APP-180 once filled position + mark price are
 * produced by the matching engine.
 *
 * <p><b>Snapshot scratch growth.</b> {@link #snapshotTo} uses a {@code long[] snapshotKeysScratch}
 * for deterministic key ordering. The class is documented as "zero allocation after construction"
 * for the hot lookup path ({@link #get(long)}, {@link #put(RiskLimitState)}). One exception: if the
 * map grows past the scratch length, the scratch is reallocated on first snapshot — this happens at
 * most once per "high water mark" and never on the duty-cycle hot path because snapshots run on a
 * separate cluster cycle. Pre-size {@code INITIAL_CAPACITY = 4096} accommodates typical deployments
 * without ever growing.
 */
public final class RiskLimitStore implements ReferenceDataStore {

  /** SBE template id for {@code RiskLimitSnapshot}. */
  public static final int SNAPSHOT_TEMPLATE_ID = RiskLimitSnapshotEncoder.TEMPLATE_ID;

  private static final int INITIAL_CAPACITY = 4096;
  private static final float LOAD_FACTOR = 0.65f;

  /**
   * Account-identifier byte length for proposerId / approverId — bounded by the SBE {@code Account}
   * char[16] type.
   */
  private static final int ACCOUNT_ID_BYTE_LEN = 16;

  private final Long2ObjectHashMap<RiskLimitState> byAccountId =
      new Long2ObjectHashMap<>(INITIAL_CAPACITY, LOAD_FACTOR);

  // Pre-allocated SBE flyweights.
  private final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
  private final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();
  private final RiskLimitSnapshotEncoder snapshotEncoder = new RiskLimitSnapshotEncoder();
  private final RiskLimitSnapshotDecoder snapshotDecoder = new RiskLimitSnapshotDecoder();

  // Scratch for draining byAccountId in deterministic-order snapshot encoding.
  private long[] snapshotKeysScratch = new long[INITIAL_CAPACITY];
  private int snapshotKeysFillIdx;
  private final LongObjConsumer<RiskLimitState> snapshotKeyCollector =
      (key, state) -> snapshotKeysScratch[snapshotKeysFillIdx++] = key;

  // Scratch for the restore path's proposerId / approverId byte reads (zero-alloc steady state).
  private final byte[] proposerIdScratch = new byte[ACCOUNT_ID_BYTE_LEN];
  private final byte[] approverIdScratch = new byte[ACCOUNT_ID_BYTE_LEN];

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
  public RiskLimitState get(long accountId) {
    return byAccountId.get(accountId);
  }

  public boolean contains(long accountId) {
    return byAccountId.containsKey(accountId);
  }

  /** Insert or overwrite a risk-limit record. The state is stored by reference. */
  public void put(RiskLimitState state) {
    byAccountId.put(state.accountId(), state);
  }

  // ---------------------------------------------------------------------------
  // Snapshot save / restore
  // ---------------------------------------------------------------------------

  @Override
  public int snapshotTo(MutableDirectBuffer dst, int offset) {
    snapshotEncoder.wrapAndApplyHeader(dst, offset, headerEncoder);
    int recordCount = byAccountId.size();
    final var group = snapshotEncoder.noRiskLimitsCount(recordCount);

    if (recordCount > 0) {
      if (snapshotKeysScratch.length < recordCount) {
        snapshotKeysScratch = new long[recordCount];
      }
      snapshotKeysFillIdx = 0;
      byAccountId.forEachLong(snapshotKeyCollector);
      Arrays.sort(snapshotKeysScratch, 0, recordCount);

      for (int i = 0; i < recordCount; i++) {
        long id = snapshotKeysScratch[i];
        final var state = byAccountId.get(id);
        encodeRecord(group, state);
      }
    }

    return MessageHeaderEncoder.ENCODED_LENGTH + snapshotEncoder.encodedLength();
  }

  private void encodeRecord(NoRiskLimitsEncoder group, RiskLimitState state) {
    group.next();
    group.accountId(state.accountId());
    group.maxOrderSize(state.maxOrderSize());
    group.maxOrderNotional(state.maxOrderNotional());
    group.maxDailyVolume(state.maxDailyVolume());
    // SBE uint32 encoder takes long for widening.
    group.maxOrdersPerSecond(state.maxOrdersPerSecond());
    group.maxLongPosition(state.maxLongPosition());
    group.maxShortPosition(state.maxShortPosition());
    group.positionLimitEnabled((short) (state.positionLimitEnabled() ? 1 : 0));
    group.priceDeviationBps(state.priceDeviationBps());
    group.fatFingerEnabled((short) (state.fatFingerEnabled() ? 1 : 0));
    group.fatFingerFailClosed((short) (state.fatFingerFailClosed() ? 1 : 0));
    group.idleSessionTimeoutNanos(state.idleSessionTimeoutNanos());
    group.putProposerId(state.proposerId(), 0);
    group.putApproverId(state.approverId(), 0);
    group.status(state.status());
    group.transactTime(state.transactTime());
  }

  @Override
  public int restoreFrom(DirectBuffer src, int offset) {
    headerDecoder.wrap(src, offset);
    snapshotDecoder.wrap(
        src,
        offset + MessageHeaderDecoder.ENCODED_LENGTH,
        headerDecoder.blockLength(),
        headerDecoder.version());
    // Defensive: drop existing limits so a smaller/empty snapshot doesn't leave stale entries.
    clear();

    final var group = snapshotDecoder.noRiskLimits();
    while (group.hasNext()) {
      group.next();
      final var state = new RiskLimitState();
      state.setAccountId(group.accountId());
      state.setMaxOrderSize(group.maxOrderSize());
      state.setMaxOrderNotional(group.maxOrderNotional());
      state.setMaxDailyVolume(group.maxDailyVolume());
      state.setMaxOrdersPerSecond(group.maxOrdersPerSecond());
      state.setMaxLongPosition(group.maxLongPosition());
      state.setMaxShortPosition(group.maxShortPosition());
      state.setPositionLimitEnabled(group.positionLimitEnabled() != 0);
      state.setPriceDeviationBps(group.priceDeviationBps());
      state.setFatFingerEnabled(group.fatFingerEnabled() != 0);
      state.setFatFingerFailClosed(group.fatFingerFailClosed() != 0);
      state.setIdleSessionTimeoutNanos(group.idleSessionTimeoutNanos());
      group.getProposerId(proposerIdScratch, 0);
      state.setProposerId(proposerIdScratch, 0, ACCOUNT_ID_BYTE_LEN);
      group.getApproverId(approverIdScratch, 0);
      state.setApproverId(approverIdScratch, 0, ACCOUNT_ID_BYTE_LEN);
      state.setStatus(group.status());
      state.setTransactTime(group.transactTime());
      byAccountId.put(state.accountId(), state);
    }

    return MessageHeaderDecoder.ENCODED_LENGTH + snapshotDecoder.encodedLength();
  }
}
