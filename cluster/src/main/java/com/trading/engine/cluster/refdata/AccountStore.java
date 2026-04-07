package com.trading.engine.cluster.refdata;

import com.trading.engine.messages.sbe.AccountSnapshotDecoder;
import com.trading.engine.messages.sbe.AccountSnapshotEncoder;
import com.trading.engine.messages.sbe.AccountSnapshotEncoder.NoAccountsEncoder;
import com.trading.engine.messages.sbe.MessageHeaderDecoder;
import com.trading.engine.messages.sbe.MessageHeaderEncoder;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.collections.LongArrayList;
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
 * <p><b>Secondary index implementation.</b> Uses {@link ByteArrayKey} (content-hashing wrapper)
 * rather than Agrona's {@code UnsafeBuffer} as the map key, because UnsafeBuffer inherits {@code
 * Object} identity-based hashing — the textbook Agrona gotcha. The hot-path lookup uses a single
 * reusable probe key ({@link #lookupKey}) mutated in place; insertions use fresh defensive-copy
 * {@link ByteArrayKey} instances so the map is independent of the source bytes.
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
   * Insert or overwrite an account. Updates BOTH indexes atomically. If an existing account with
   * the same {@code accountId} has a different {@code accountCode}, the old code's secondary index
   * entry is removed first.
   *
   * <p>The new account-code bytes are defensively copied into a fresh {@link ByteArrayKey} for the
   * secondary index so the map remains independent of any reused source buffer.
   *
   * <p>Caller is responsible for rejecting duplicate-code-different-id at validation time (via
   * {@link LoadAccountHandler}); this method assumes the caller has already verified that no other
   * accountId already owns the new code.
   */
  public void put(final AccountState state) {
    final long accountId = state.accountId();
    final AccountState previous = byId.get(accountId);
    if (previous != null) {
      // Remove the previous secondary-index entry. Use the reusable lookup probe — single-
      // threaded cluster duty cycle, no contention.
      copyStateCodeIntoLookupKey(previous);
      byCode.remove(lookupKey);
    }
    byId.put(accountId, state);
    // Defensive-copy the code into a fresh ByteArrayKey for stable map identity.
    copyStateCodeIntoCodeScratch(state);
    final ByteArrayKey insertKey = ByteArrayKey.copyOf(codeScratch, 0, state.accountCodeLength());
    byCode.put(insertKey, state);
  }

  private void copyStateCodeIntoLookupKey(final AccountState state) {
    final int len = state.accountCodeLength();
    for (int i = 0; i < len; i++) {
      codeScratch[i] = state.accountCodeByte(i);
    }
    lookupKey.set(codeScratch, 0, len);
  }

  private void copyStateCodeIntoCodeScratch(final AccountState state) {
    final int len = state.accountCodeLength();
    for (int i = 0; i < len; i++) {
      codeScratch[i] = state.accountCodeByte(i);
    }
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
      // Sorted iteration for deterministic snapshot output.
      final LongArrayList sortedIds = new LongArrayList(recordCount, Long.MIN_VALUE);
      for (final long id : byId.keySet()) {
        sortedIds.addLong(id);
      }
      sortLongAscending(sortedIds);

      for (int i = 0; i < recordCount; i++) {
        final long id = sortedIds.getLong(i);
        final AccountState state = byId.get(id);
        group.next();
        group.accountId(state.accountId());
        group.parentAccountId(state.parentAccountId());
        // Pad accountCode to fixed 16 bytes (Account char[16]).
        for (int j = 0; j < MAX_ACCOUNT_CODE_LENGTH; j++) {
          codeScratch[j] = j < state.accountCodeLength() ? state.accountCodeByte(j) : (byte) 0;
        }
        group.putAccountCode(codeScratch, 0);
        group.acctIdSource(state.acctIdSource());
        for (int j = 0; j < NAME_LENGTH; j++) {
          nameScratch[j] = j < state.accountNameLength() ? state.accountNameByte(j) : (byte) 0;
        }
        group.putAccountName(nameScratch, 0);
        group.accountType(state.accountType());
        group.putBaseCurrency(
            state.baseCurrencyByte(0), state.baseCurrencyByte(1), state.baseCurrencyByte(2));
        group.status(state.status());
        group.complianceStatus(state.complianceStatus());
        group.capabilities(state.capabilities());
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

    final AccountSnapshotDecoder.NoAccountsDecoder group = snapshotDecoder.noAccounts();
    while (group.hasNext()) {
      group.next();
      final AccountState state = new AccountState();
      state.setAccountId(group.accountId());
      state.setParentAccountId(group.parentAccountId());
      group.getAccountCode(codeScratch, 0);
      state.setAccountCode(codeScratch, 0, trimTrailingZeros(codeScratch, MAX_ACCOUNT_CODE_LENGTH));
      state.setAcctIdSource(group.acctIdSource());
      group.getAccountName(nameScratch, 0);
      state.setAccountName(nameScratch, 0, trimTrailingZeros(nameScratch, NAME_LENGTH));
      state.setAccountType(group.accountType());
      state.setBaseCurrency(group.baseCurrency(0), group.baseCurrency(1), group.baseCurrency(2));
      state.setStatus(group.status());
      state.setComplianceStatus(group.complianceStatus());
      state.setCapabilities(group.capabilities());
      put(state);
    }

    return MessageHeaderDecoder.ENCODED_LENGTH + snapshotDecoder.encodedLength();
  }

  private static int trimTrailingZeros(final byte[] bytes, final int upToLength) {
    int len = upToLength;
    while (len > 0 && bytes[len - 1] == 0) {
      len--;
    }
    return len;
  }

  /** In-place ascending sort of a {@link LongArrayList} (insertion sort, fine for typical N). */
  private static void sortLongAscending(final LongArrayList list) {
    final int n = list.size();
    for (int i = 1; i < n; i++) {
      final long key = list.getLong(i);
      int j = i - 1;
      while (j >= 0 && list.getLong(j) > key) {
        list.setLong(j + 1, list.getLong(j));
        j--;
      }
      list.setLong(j + 1, key);
    }
  }
}
