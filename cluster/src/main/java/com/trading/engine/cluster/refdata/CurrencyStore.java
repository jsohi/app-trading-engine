package com.trading.engine.cluster.refdata;

import com.trading.engine.messages.sbe.CurrencySnapshotDecoder;
import com.trading.engine.messages.sbe.CurrencySnapshotEncoder;
import com.trading.engine.messages.sbe.CurrencySnapshotEncoder.NoCurrenciesEncoder;
import com.trading.engine.messages.sbe.MessageHeaderDecoder;
import com.trading.engine.messages.sbe.MessageHeaderEncoder;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Int2ObjectHashMap;
import org.agrona.collections.IntObjConsumer;

/**
 * Replicated in-cluster ISO 4217 currency master, indexed by ASCII code packed into an int.
 *
 * <p><b>Why packed-int keying.</b> Currency codes are exactly 3 ASCII bytes. Packing them into a
 * single {@code int} ({@code (b0 << 16) | (b1 << 8) | b2}) lets us use Agrona's primitive-keyed
 * {@link Int2ObjectHashMap} directly — no boxing on lookup, no allocation, and we sidestep the
 * Agrona {@code UnsafeBuffer.hashCode()} gotcha entirely (UnsafeBuffer's inherited {@code Object}
 * identity-based hashing makes it unsafe as a hash-map key).
 *
 * <p><b>Validation.</b> {@link #put} accepts only uppercase ASCII A-Z. Invalid bytes throw {@link
 * IllegalArgumentException} — the loader validates first and only calls {@code put} with
 * already-validated bytes, so this check is a defensive tripwire.
 *
 * <p><b>Snapshot determinism.</b> {@link #snapshotTo} iterates a sorted key array (NOT the hash
 * map's natural order, which Agrona does not guarantee stable across JVM versions or runs).
 * Iterating in ascending packed-int order is equivalent to iterating in lex-ASCII order over the
 * codes.
 *
 * <p><b>Threading.</b> Single-threaded — cluster duty cycle only. Hot-path lookups ({@link
 * #get(int)}, {@link #getByCode(DirectBuffer, int)}) are zero-allocation. {@link #snapshotTo}
 * relies on {@code snapshotKeysFillIdx} being reset at the start of every call; concurrent or
 * re-entrant invocation would corrupt that counter.
 */
public final class CurrencyStore implements ReferenceDataStore {

  /** SBE template id for {@code CurrencySnapshot}. */
  public static final int SNAPSHOT_TEMPLATE_ID = CurrencySnapshotEncoder.TEMPLATE_ID;

  /** Encoded length of one record's name field, matching the SBE schema. */
  private static final int NAME_LENGTH = 64;

  private static final int INITIAL_CAPACITY = 256;
  private static final float LOAD_FACTOR = 0.65f;

  private final Int2ObjectHashMap<CurrencyState> byCode =
      new Int2ObjectHashMap<>(INITIAL_CAPACITY, LOAD_FACTOR);

  // Pre-allocated SBE flyweights — reused across snapshot save / restore.
  private final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
  private final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();
  private final CurrencySnapshotEncoder snapshotEncoder = new CurrencySnapshotEncoder();
  private final CurrencySnapshotDecoder snapshotDecoder = new CurrencySnapshotDecoder();

  // Scratch byte buffers used during snapshot encode (avoids stack allocation in put loop).
  private final byte[] scratchName = new byte[NAME_LENGTH];

  // Scratch for draining byCode in deterministic-order snapshot encoding. Grows on demand
  // once when the map exceeds INITIAL_CAPACITY. Stored IntObjConsumer avoids the per-call
  // KeyIterator allocation of the Iterator API.
  private int[] snapshotKeysScratch = new int[INITIAL_CAPACITY];
  private int snapshotKeysFillIdx;
  private final IntObjConsumer<CurrencyState> snapshotKeyCollector =
      (key, state) -> snapshotKeysScratch[snapshotKeysFillIdx++] = key;

  @Override
  public int snapshotTemplateId() {
    return SNAPSHOT_TEMPLATE_ID;
  }

  @Override
  public int size() {
    return byCode.size();
  }

  @Override
  public void clear() {
    byCode.clear();
  }

  /** Sentinel returned by {@link #packCodeOrInvalid} when any byte is outside A-Z. */
  public static final int INVALID_PACKED_CODE = -1;

  /**
   * Pack a 3-byte ASCII code into a single int. Validates that all three bytes are uppercase A-Z.
   * Returns the packed key.
   *
   * @throws IllegalArgumentException if any byte is outside A-Z
   */
  public static int packCode(final byte b0, final byte b1, final byte b2) {
    requireUpperAlpha(b0);
    requireUpperAlpha(b1);
    requireUpperAlpha(b2);
    return ((b0 & 0xFF) << 16) | ((b1 & 0xFF) << 8) | (b2 & 0xFF);
  }

  /**
   * Non-throwing variant of {@link #packCode(byte, byte, byte)} for hot-path callers that need to
   * branch on validity rather than catch an exception. Returns {@link #INVALID_PACKED_CODE} when
   * any byte is outside A-Z; the sentinel is unreachable as a real packed code because negative
   * ints are never produced by the bit-pack formula.
   */
  public static int packCodeOrInvalid(final byte b0, final byte b1, final byte b2) {
    if (!isUpperAlpha(b0) || !isUpperAlpha(b1) || !isUpperAlpha(b2)) {
      return INVALID_PACKED_CODE;
    }
    return ((b0 & 0xFF) << 16) | ((b1 & 0xFF) << 8) | (b2 & 0xFF);
  }

  private static boolean isUpperAlpha(final byte b) {
    return b >= 'A' && b <= 'Z';
  }

  /** Pack the first 3 bytes at {@code src[offset..]} into a key. */
  public static int packCode(final DirectBuffer src, final int offset) {
    return packCode(src.getByte(offset), src.getByte(offset + 1), src.getByte(offset + 2));
  }

  private static void requireUpperAlpha(final byte b) {
    if (b < 'A' || b > 'Z') {
      throw new IllegalArgumentException(
          "currency code byte must be uppercase A-Z, was 0x" + Integer.toHexString(b & 0xFF));
    }
  }

  // ---------------------------------------------------------------------------
  // Hot-path lookups
  // ---------------------------------------------------------------------------

  /** O(1) lookup by packed-int key. Zero allocation. */
  public CurrencyState get(final int packedKey) {
    return byCode.get(packedKey);
  }

  /** O(1) lookup by 3-byte slice in a {@link DirectBuffer}. Zero allocation. */
  public CurrencyState getByCode(final DirectBuffer src, final int offset) {
    return byCode.get(packCode(src, offset));
  }

  public boolean contains(final int packedKey) {
    return byCode.containsKey(packedKey);
  }

  // ---------------------------------------------------------------------------
  // Upsert (called from LoadCurrencyHandler / restoreFrom)
  // ---------------------------------------------------------------------------

  /**
   * Insert or overwrite a currency. The {@code state} instance is stored by reference; the caller
   * must not retain or mutate it after this call. Upsert is idempotent — re-loading the same code
   * overwrites the existing record.
   *
   * <p>Validates that {@code packedKey} matches the bytes inside {@code state.ccyCode}. A
   * mismatched call would silently re-key the record after a snapshot round-trip (snapshot
   * serializes the bytes inside the state, restore re-derives the packed key from those bytes) and
   * break runtime lookups against the original packedKey. The check is a defensive tripwire —
   * well-formed callers (this module's loaders) always pass matching values.
   *
   * @throws NullPointerException if {@code state} is null
   * @throws IllegalArgumentException if {@code packedKey} does not match {@code
   *     packCode(state.ccyCode...)}
   * @return {@code true} if a record was overwritten, {@code false} if newly inserted
   */
  public boolean put(final int packedKey, final CurrencyState state) {
    if (state == null) {
      throw new NullPointerException("state must not be null");
    }
    final int statePackedKey =
        packCode(state.ccyCodeByte(0), state.ccyCodeByte(1), state.ccyCodeByte(2));
    if (statePackedKey != packedKey) {
      throw new IllegalArgumentException(
          "packedKey "
              + packedKey
              + " does not match CurrencyState.ccyCode (packs to "
              + statePackedKey
              + ")");
    }
    return byCode.put(packedKey, state) != null;
  }

  // ---------------------------------------------------------------------------
  // Snapshot save / restore
  // ---------------------------------------------------------------------------

  @Override
  public int snapshotTo(final MutableDirectBuffer dst, final int offset) {
    snapshotEncoder.wrapAndApplyHeader(dst, offset, headerEncoder);
    final int recordCount = byCode.size();
    final NoCurrenciesEncoder group = snapshotEncoder.noCurrenciesCount(recordCount);

    if (recordCount > 0) {
      // Drain via a stored IntObjConsumer — no KeyIterator allocation on the snapshot path.
      // Scratch grows on demand once when the map first outgrows INITIAL_CAPACITY.
      if (snapshotKeysScratch.length < recordCount) {
        snapshotKeysScratch = new int[recordCount];
      }
      snapshotKeysFillIdx = 0;
      byCode.forEachInt(snapshotKeyCollector);
      java.util.Arrays.sort(snapshotKeysScratch, 0, recordCount);

      for (int i = 0; i < recordCount; i++) {
        final int key = snapshotKeysScratch[i];
        final CurrencyState state = byCode.get(key);
        group.next();
        group.putCcyCode(state.ccyCodeByte(0), state.ccyCodeByte(1), state.ccyCodeByte(2));
        group.isoNumeric(state.isoNumeric());
        // Pad name to fixed 64 bytes (System.arraycopy + Arrays.fill).
        final int nameLen = state.copyNameTo(scratchName, 0);
        if (nameLen < NAME_LENGTH) {
          java.util.Arrays.fill(scratchName, nameLen, NAME_LENGTH, (byte) 0);
        }
        group.putName(scratchName, 0);
        group.decimals((short) state.decimals());
        group.currencyClass(state.currencyClass());
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
    // Defensive: drop pre-existing entries so a smaller/empty snapshot doesn't leave stale
    // currencies behind.
    clear();

    final CurrencySnapshotDecoder.NoCurrenciesDecoder group = snapshotDecoder.noCurrencies();
    while (group.hasNext()) {
      group.next();
      final CurrencyState state = new CurrencyState();
      // Read the 3-byte code via primitive accessors (no allocation, no String).
      final byte b0 = group.ccyCode(0);
      final byte b1 = group.ccyCode(1);
      final byte b2 = group.ccyCode(2);
      state.setCcyCode(b0, b1, b2);
      state.setIsoNumeric(group.isoNumeric());
      // Decode the name into the reusable scratch buffer (no per-record allocation).
      group.getName(scratchName, 0);
      state.setName(scratchName, 0, RefDataUtils.trimTrailingZeros(scratchName, NAME_LENGTH));
      state.setDecimals(group.decimals());
      state.setCurrencyClass(group.currencyClass());
      state.setStatus(group.status());
      state.setTransactTime(group.transactTime());
      byCode.put(packCode(b0, b1, b2), state);
    }

    return MessageHeaderDecoder.ENCODED_LENGTH + snapshotDecoder.encodedLength();
  }
}
