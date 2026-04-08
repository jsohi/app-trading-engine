package com.trading.engine.cluster.refdata;

import com.trading.engine.messages.sbe.AccountSnapshotDecoder;
import com.trading.engine.messages.sbe.AccountSnapshotEncoder;
import com.trading.engine.messages.sbe.AccountSnapshotEncoder.NoAccountsEncoder;
import com.trading.engine.messages.sbe.MessageHeaderDecoder;
import com.trading.engine.messages.sbe.MessageHeaderEncoder;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.collections.Object2ObjectHashMap;

/**
 * Replicated in-cluster account master, dual-indexed by numeric {@code accountId} (primary, hot
 * path) and by {@code accountCode} bytes (secondary, cold lookups for FIX logon and admin
 * operations).
 *
 * <p><b>Hot vs cold split.</b> Per industry standard (exchange-core, CME Globex), the gateway
 * resolves {@code accountCode} → {@code accountId} once at session logon and then only passes
 * {@code accountId} on the order path. {@link #get(long)} is the hot path; {@link
 * #getByCode(DirectBuffer, int, int)} is the cold path.
 *
 * <p><b>Secondary index implementation.</b> Uses {@link ByteArrayKey} (defensive-copy wrapper)
 * rather than Agrona's {@code UnsafeBuffer} as the map key. UnsafeBuffer DOES content-hash in
 * Agrona 2.4.0+, but it holds a reference to the source byte[] — using a slice of a recycled SBE
 * buffer as a key would silently corrupt the map when the source is reused. The hot-path lookup
 * uses a single reusable probe key ({@link #lookupKey}) mutated in place; insertions use fresh
 * defensive-copy {@link ByteArrayKey} instances so the map remains independent of any reused
 * buffer's lifecycle.
 *
 * <p><b>Snapshot determinism.</b> {@link #snapshotTo} iterates a sorted {@link LongArrayList} of
 * {@code accountId}s for deterministic byte output (Agrona hash-map iteration order is not
 * guaranteed stable). On {@link #restoreFrom} the secondary index is rebuilt with fresh {@code
 * ByteArrayKey} instances per record.
 */
public final class AccountStore implements ReferenceDataStore {

  /** SBE template id for {@code AccountSnapshot}. */
  public static final int SNAPSHOT_TEMPLATE_ID = AccountSnapshotEncoder.TEMPLATE_ID;

  /** Maximum bytes the secondary index lookup probe can hold (matches the SBE Account type). */
  public static final int MAX_ACCOUNT_CODE_LENGTH = 16;

  /** Encoded length of one record's name field, matching the SBE schema. */
  private static final int NAME_LENGTH = 64;

  private static final int INITIAL_CAPACITY = 4096;
  private static final float LOAD_FACTOR = 0.65f;

  private final Long2ObjectHashMap<AccountState> byId =
      new Long2ObjectHashMap<>(INITIAL_CAPACITY, LOAD_FACTOR);
  private final Object2ObjectHashMap<ByteArrayKey, AccountState> byCode =
      new Object2ObjectHashMap<>(INITIAL_CAPACITY, LOAD_FACTOR);

  /**
   * Sidecar map: accountId → the ByteArrayKey that lives in {@link #byCode} for that account.
   * Decouples the secondary-index key bytes from {@link AccountState#accountCode}, so when a loader
   * mutates the state's accountCode in place, the key here still holds the OLD bytes and can be
   * used to remove the stale {@link #byCode} entry. Also lets us REUSE the same {@link
   * ByteArrayKey} instance across upserts of the same account — no per-put allocation.
   */
  private final Long2ObjectHashMap<ByteArrayKey> codeKeyByAccountId =
      new Long2ObjectHashMap<>(INITIAL_CAPACITY, LOAD_FACTOR);

  /** Reusable lookup probe — mutated in place by {@link #getByCode}. NEVER inserted. */
  private final ByteArrayKey lookupKey = ByteArrayKey.emptyForLookup(MAX_ACCOUNT_CODE_LENGTH);

  // Pre-allocated SBE flyweights.
  private final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
  private final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();
  private final AccountSnapshotEncoder snapshotEncoder = new AccountSnapshotEncoder();
  private final AccountSnapshotDecoder snapshotDecoder = new AccountSnapshotDecoder();

  // Snapshot scratch buffers for fixed-length char fields.
  private final byte[] codeScratch = new byte[MAX_ACCOUNT_CODE_LENGTH];
  private final byte[] nameScratch = new byte[NAME_LENGTH];

  @Override
  public int snapshotTemplateId() {
    return SNAPSHOT_TEMPLATE_ID;
  }

  @Override
  public int size() {
    return byId.size();
  }

  @Override
  public void clear() {
    byId.clear();
    byCode.clear();
    codeKeyByAccountId.clear();
  }

  // ---------------------------------------------------------------------------
  // Hot-path lookups
  // ---------------------------------------------------------------------------

  /** Primary hot-path lookup by numeric accountId. O(1), zero allocation. */
  public AccountState get(final long accountId) {
    return byId.get(accountId);
  }

  public boolean contains(final long accountId) {
    return byId.containsKey(accountId);
  }

  /**
   * Cold-path lookup by accountCode bytes. Uses the reusable {@link #lookupKey} — single- threaded,
   * NOT reentrant. Returns {@code null} if no account with that code exists.
   *
   * @throws IndexOutOfBoundsException if {@code length > MAX_ACCOUNT_CODE_LENGTH}
   */
  public AccountState getByCode(final DirectBuffer src, final int offset, final int length) {
    lookupKey.set(src, offset, length);
    return byCode.get(lookupKey);
  }

  public boolean containsCode(final DirectBuffer src, final int offset, final int length) {
    lookupKey.set(src, offset, length);
    return byCode.containsKey(lookupKey);
  }

  /**
   * Cold-path lookup variant that takes a {@code byte[]} source. Same single-threaded non-reentrant
   * constraint as {@link #getByCode(DirectBuffer, int, int)}.
   */
  public AccountState getByCodeBytes(final byte[] src, final int offset, final int length) {
    lookupKey.set(src, offset, length);
    return byCode.get(lookupKey);
  }

  // ---------------------------------------------------------------------------
  // Upsert
  // ---------------------------------------------------------------------------

  /**
   * Insert or overwrite an account. Updates BOTH indexes atomically. Zero allocation on the
   * overwrite path; one-time {@link ByteArrayKey} allocation on first insert per account.
   *
   * <p><b>Aliasing-safety</b> (the previously-broken case): the loader pattern is "fetch existing
   * AccountState from {@link #get(long)}, mutate its accountCode in place, then call {@code put}".
   * By the time this method runs, {@code state.accountCode} already holds the NEW bytes — the OLD
   * bytes are gone. We avoid relying on {@code state} for the old bytes by keeping the
   * secondary-index key in the {@link #codeKeyByAccountId} sidecar map. The sidecar's {@link
   * ByteArrayKey} is a separate object whose {@code data} byte[] is independent of {@code
   * state.accountCode}, so it still holds the OLD bytes when we remove from {@link #byCode}.
   *
   * <p><b>Sequence:</b>
   *
   * <ol>
   *   <li>Look up the existing {@link ByteArrayKey} in the sidecar map.
   *   <li>If present, remove the {@link #byCode} entry using that key (still holds OLD bytes).
   *   <li>Read the NEW bytes out of {@code state.accountCode}.
   *   <li>Mutate the existing key in place (or allocate a fresh one on first insert).
   *   <li>Re-insert into {@link #byCode} with the same value reference and the now-new key.
   * </ol>
   *
   * <p>Caller is responsible for rejecting duplicate-code-different-id at validation time (via
   * {@link LoadAccountHandler}); this method assumes the caller has already verified that no other
   * accountId already owns the new code. {@link #restoreFrom} also calls this method but trusts the
   * snapshot bytes — snapshots are produced by {@link #snapshotTo} on a previously validated state,
   * so duplicate codes cannot be present in a well-formed snapshot.
   */
  public void put(final AccountState state) {
    final long accountId = state.accountId();
    ByteArrayKey codeKey = codeKeyByAccountId.get(accountId);
    if (codeKey != null) {
      // Existing entry — remove from byCode using the OLD bytes (still in codeKey, NOT in
      // state.accountCode which the loader has already mutated to the NEW bytes).
      byCode.remove(codeKey);
      // Mutate the existing key in place to the NEW bytes (zero allocation).
      final int len = state.copyAccountCodeTo(codeScratch, 0);
      codeKey.set(codeScratch, 0, len);
    } else {
      // First insert — allocate one ByteArrayKey for this account, sized to the max account
      // code length so subsequent upserts with a longer code can mutate the key in place.
      final int len = state.copyAccountCodeTo(codeScratch, 0);
      codeKey = ByteArrayKey.copyOfWithCapacity(codeScratch, 0, len, MAX_ACCOUNT_CODE_LENGTH);
      codeKeyByAccountId.put(accountId, codeKey);
    }
    byId.put(accountId, state);
    byCode.put(codeKey, state);
  }

  // ---------------------------------------------------------------------------
  // Snapshot save / restore
  // ---------------------------------------------------------------------------

  @Override
  public int snapshotTo(final MutableDirectBuffer dst, final int offset) {
    snapshotEncoder.wrapAndApplyHeader(dst, offset, headerEncoder);
    final int recordCount = byId.size();
    final NoAccountsEncoder group = snapshotEncoder.noAccountsCount(recordCount);

    if (recordCount > 0) {
      // Sorted iteration for deterministic snapshot output. Drain via Agrona's primitive
      // KeyIterator.nextLong() (no per-element Long boxing) into a primitive long[], then sort.
      // Snapshot path — allocation is allowed.
      final long[] sortedIds = new long[recordCount];
      final Long2ObjectHashMap<AccountState>.KeyIterator keyIt = byId.keySet().iterator();
      int idx = 0;
      while (keyIt.hasNext()) {
        sortedIds[idx++] = keyIt.nextLong();
      }
      java.util.Arrays.sort(sortedIds);

      for (int i = 0; i < recordCount; i++) {
        final long id = sortedIds[i];
        final AccountState state = byId.get(id);
        group.next();
        group.accountId(state.accountId());
        group.parentAccountId(state.parentAccountId());
        // Pad accountCode to fixed 16 bytes (Account char[16]) — System.arraycopy + Arrays.fill
        // is faster than a byte-loop and lets the JIT/intrinsics shine.
        final int codeLen = state.copyAccountCodeTo(codeScratch, 0);
        if (codeLen < MAX_ACCOUNT_CODE_LENGTH) {
          java.util.Arrays.fill(codeScratch, codeLen, MAX_ACCOUNT_CODE_LENGTH, (byte) 0);
        }
        group.putAccountCode(codeScratch, 0);
        group.acctIdSource(state.acctIdSource());
        final int nameLen = state.copyAccountNameTo(nameScratch, 0);
        if (nameLen < NAME_LENGTH) {
          java.util.Arrays.fill(nameScratch, nameLen, NAME_LENGTH, (byte) 0);
        }
        group.putAccountName(nameScratch, 0);
        group.accountType(state.accountType());
        group.putBaseCurrency(
            state.baseCurrencyByte(0), state.baseCurrencyByte(1), state.baseCurrencyByte(2));
        group.status(state.status());
        group.complianceStatus(state.complianceStatus());
        group.capabilities(state.capabilities());
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
    // Defensive: drop any pre-existing state so a smaller / empty snapshot doesn't leave
    // orphan accounts behind. The registry's resetAll() also calls clear() but doing it
    // here makes restoreFrom safe to call standalone.
    clear();

    final AccountSnapshotDecoder.NoAccountsDecoder group = snapshotDecoder.noAccounts();
    while (group.hasNext()) {
      group.next();
      final AccountState state = new AccountState();
      state.setAccountId(group.accountId());
      state.setParentAccountId(group.parentAccountId());
      group.getAccountCode(codeScratch, 0);
      state.setAccountCode(
          codeScratch, 0, RefDataUtils.trimTrailingZeros(codeScratch, MAX_ACCOUNT_CODE_LENGTH));
      state.setAcctIdSource(group.acctIdSource());
      group.getAccountName(nameScratch, 0);
      state.setAccountName(
          nameScratch, 0, RefDataUtils.trimTrailingZeros(nameScratch, NAME_LENGTH));
      state.setAccountType(group.accountType());
      state.setBaseCurrency(group.baseCurrency(0), group.baseCurrency(1), group.baseCurrency(2));
      state.setStatus(group.status());
      state.setComplianceStatus(group.complianceStatus());
      state.setCapabilities(group.capabilities());
      state.setTransactTime(group.transactTime());
      put(state);
    }

    return MessageHeaderDecoder.ENCODED_LENGTH + snapshotDecoder.encodedLength();
  }
}
