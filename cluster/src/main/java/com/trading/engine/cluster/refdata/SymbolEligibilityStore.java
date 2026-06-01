package com.trading.engine.cluster.refdata;

import com.trading.engine.messages.sbe.MessageHeaderDecoder;
import com.trading.engine.messages.sbe.MessageHeaderEncoder;
import com.trading.engine.messages.sbe.SymbolEligibilitySnapshotDecoder;
import com.trading.engine.messages.sbe.SymbolEligibilitySnapshotEncoder;
import com.trading.engine.messages.sbe.SymbolEligibilitySnapshotEncoder.NoEntriesEncoder;
import java.util.Arrays;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.collections.LongObjConsumer;

/**
 * Replicated in-cluster symbol-eligibility store, keyed by the packed-{@code long} {@code
 * symbolHash} (8-byte FIX tag 55 Symbol field interpreted as a little-endian {@code long}). APP-62
 * §G — provides the restricted-symbol / short-sale-restricted lookup that drives the
 * order-admission check 11g (SEC 15c3-5(c)(1)(ii)); APP-62 §I — also carries the per-symbol
 * fat-finger override because both knobs are symbol-keyed reference data on the same operational
 * cadence.
 *
 * <p>The packing scheme matches {@code
 * com.trading.engine.cluster.handler.NewOrderSingleHandler#packSymbolKey(byte[], int)} so the same
 * key derived from an inbound NewOrderSingle's Symbol field lands directly in this store without
 * re-derivation.
 *
 * <p>Snapshot determinism: records are written in ascending {@code symbolHash} order via a sorted
 * scratch array, never the hash map's natural order.
 *
 * <p>Hot-path lookup is {@code O(1)} via {@link Long2ObjectHashMap#get(long)} — zero allocation, no
 * boxing.
 *
 * <p><b>Threading.</b> Not thread-safe — single-threaded cluster duty cycle only. {@link
 * #snapshotTo} relies on {@code snapshotKeysFillIdx} being reset at the start of every call;
 * concurrent or re-entrant invocation would corrupt that counter.
 *
 * <p><b>Allocation scope.</b> The "zero allocation after construction" guarantee covers ONLY the
 * order-admission hot path — {@link #get(long)} and {@link #put(SymbolEligibilityState)} — both of
 * which run on the cluster duty cycle. Cold paths intentionally allocate:
 *
 * <ul>
 *   <li>{@link #snapshotTo}'s {@code long[] snapshotKeysScratch} re-allocates on first snapshot if
 *       the map grows past the scratch length. Bounded to "once per high-water mark" and never runs
 *       on the duty-cycle hot path (snapshots run on a separate cluster cycle). Pre-size {@code
 *       INITIAL_CAPACITY = 8192} accommodates typical equity-universe deployments (Reg SHO
 *       threshold list + IPO restrictions) without ever growing.
 *   <li>{@link #restoreFrom} allocates one {@link SymbolEligibilityState} per record. Runs once on
 *       cluster startup (Aeron Cluster snapshot replay) — bounded by the snapshot's record count,
 *       not the order-rate. Same idiom as the equivalent allocation in {@link
 *       RiskLimitStore#restoreFrom}.
 * </ul>
 */
public final class SymbolEligibilityStore implements ReferenceDataStore {

  /** SBE template id for {@code SymbolEligibilitySnapshot}. */
  public static final int SNAPSHOT_TEMPLATE_ID = SymbolEligibilitySnapshotEncoder.TEMPLATE_ID;

  private static final int INITIAL_CAPACITY = 8192;
  private static final float LOAD_FACTOR = 0.65f;

  private final Long2ObjectHashMap<SymbolEligibilityState> byHash =
      new Long2ObjectHashMap<>(INITIAL_CAPACITY, LOAD_FACTOR);

  // Pre-allocated SBE flyweights — reused across snapshot save / restore.
  private final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
  private final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();
  private final SymbolEligibilitySnapshotEncoder snapshotEncoder =
      new SymbolEligibilitySnapshotEncoder();
  private final SymbolEligibilitySnapshotDecoder snapshotDecoder =
      new SymbolEligibilitySnapshotDecoder();

  // Scratch for draining byHash in deterministic-order snapshot encoding. Grows on demand once
  // when the map exceeds INITIAL_CAPACITY. Stored LongObjConsumer avoids the per-call KeyIterator
  // allocation of the Iterator API.
  private long[] snapshotKeysScratch = new long[INITIAL_CAPACITY];
  private int snapshotKeysFillIdx;
  private final LongObjConsumer<SymbolEligibilityState> snapshotKeyCollector =
      (key, state) -> snapshotKeysScratch[snapshotKeysFillIdx++] = key;

  @Override
  public int snapshotTemplateId() {
    return SNAPSHOT_TEMPLATE_ID;
  }

  @Override
  public int size() {
    return byHash.size();
  }

  @Override
  public void clear() {
    byHash.clear();
  }

  // ---------------------------------------------------------------------------
  // Hot-path
  // ---------------------------------------------------------------------------

  /** O(1) lookup by packed {@code symbolHash}. Zero allocation. */
  public SymbolEligibilityState get(long symbolHash) {
    return byHash.get(symbolHash);
  }

  public boolean contains(long symbolHash) {
    return byHash.containsKey(symbolHash);
  }

  /**
   * Insert or overwrite an eligibility record. The state is stored by reference; the caller must
   * not retain or mutate it after this call. Upsert is idempotent — re-loading the same symbol
   * overwrites the existing record.
   *
   * @throws NullPointerException if {@code state} is null
   */
  public void put(final SymbolEligibilityState state) {
    if (state == null) {
      throw new NullPointerException("state must not be null");
    }
    byHash.put(state.symbolHash(), state);
  }

  // ---------------------------------------------------------------------------
  // Snapshot save / restore
  // ---------------------------------------------------------------------------

  @Override
  public int snapshotTo(final MutableDirectBuffer dst, final int offset) {
    snapshotEncoder.wrapAndApplyHeader(dst, offset, headerEncoder);
    int recordCount = byHash.size();
    final var group = snapshotEncoder.noEntriesCount(recordCount);

    if (recordCount > 0) {
      if (snapshotKeysScratch.length < recordCount) {
        snapshotKeysScratch = new long[recordCount];
      }
      snapshotKeysFillIdx = 0;
      byHash.forEachLong(snapshotKeyCollector);
      Arrays.sort(snapshotKeysScratch, 0, recordCount);

      for (int i = 0; i < recordCount; i++) {
        long key = snapshotKeysScratch[i];
        final var state = byHash.get(key);
        encodeRecord(group, state);
      }
    }

    return MessageHeaderEncoder.ENCODED_LENGTH + snapshotEncoder.encodedLength();
  }

  private static void encodeRecord(final NoEntriesEncoder group, final SymbolEligibilityState s) {
    group.next();
    group.symbolHash(s.symbolHash());
    group.tradingAllowed((short) (s.tradingAllowed() ? 1 : 0));
    group.shortSaleAllowed((short) (s.shortSaleAllowed() ? 1 : 0));
    // SBE uint32 encoder accepts a long; the value is widened on the wire.
    group.priceDeviationBpsOverride(s.priceDeviationBpsOverride());
    group.asOfTimestamp(s.asOfTimestamp());
  }

  @Override
  public int restoreFrom(final DirectBuffer src, final int offset) {
    headerDecoder.wrap(src, offset);
    snapshotDecoder.wrap(
        src,
        offset + MessageHeaderDecoder.ENCODED_LENGTH,
        headerDecoder.blockLength(),
        headerDecoder.version());
    // Defensive: drop existing entries so a smaller/empty snapshot doesn't leave stale records.
    clear();

    final var group = snapshotDecoder.noEntries();
    while (group.hasNext()) {
      group.next();
      // Allocation: one SymbolEligibilityState per record on the cold snapshot-restore path.
      // Bounded by the snapshot's record count (typical: a few thousand symbols); runs once on
      // cluster startup before the duty cycle is admitting orders, so the per-record alloc never
      // crosses the order-admission hot path. Documented in the class-level Javadoc carve-out.
      final var state = new SymbolEligibilityState();
      state.setSymbolHash(group.symbolHash());
      state.setTradingAllowed(group.tradingAllowed() != 0);
      state.setShortSaleAllowed(group.shortSaleAllowed() != 0);
      state.setPriceDeviationBpsOverride(group.priceDeviationBpsOverride());
      state.setAsOfTimestamp(group.asOfTimestamp());
      // Snapshot carries the packed hash but not the raw 8 bytes — derive symbolBytes from the
      // packed long so downstream consumers (projection / debug emit) have the printable form
      // available without inverting the pack on every read. The packing is the inverse of
      // NewOrderSingleHandler.packSymbolKey (little-endian byte unpack).
      unpackSymbolBytes(state.symbolHash(), state.symbolBytes());
      byHash.put(state.symbolHash(), state);
    }

    return MessageHeaderDecoder.ENCODED_LENGTH + snapshotDecoder.encodedLength();
  }

  /**
   * Inverse of {@code NewOrderSingleHandler#packSymbolKey} — unpacks an 8-byte little-endian {@code
   * long} back into its 8 source bytes. Used on the snapshot restore path so the in-memory {@code
   * SymbolEligibilityState} keeps a printable symbol available even when the snapshot stream omits
   * the raw bytes (the snapshot carries only the packed hash to save 8 bytes per record at scale).
   *
   * @param packed the packed-{@code long} symbol hash
   * @param dst 8-byte destination buffer (mutated in place)
   */
  static void unpackSymbolBytes(long packed, final byte[] dst) {
    dst[0] = (byte) (packed & 0xFFL);
    dst[1] = (byte) ((packed >>> 8) & 0xFFL);
    dst[2] = (byte) ((packed >>> 16) & 0xFFL);
    dst[3] = (byte) ((packed >>> 24) & 0xFFL);
    dst[4] = (byte) ((packed >>> 32) & 0xFFL);
    dst[5] = (byte) ((packed >>> 40) & 0xFFL);
    dst[6] = (byte) ((packed >>> 48) & 0xFFL);
    dst[7] = (byte) ((packed >>> 56) & 0xFFL);
  }
}
