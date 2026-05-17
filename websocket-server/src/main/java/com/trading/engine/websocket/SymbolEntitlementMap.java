package com.trading.engine.websocket;

import com.trading.engine.projections.SymbolPacker;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.collections.LongHashSet;
import org.agrona.collections.ObjectHashSet;

/**
 * Startup-loaded per-symbol → permitted-account-codes entitlement map.
 *
 * <p>Sourced from the {@code symbols.yaml} fixture under {@code
 * integration-tests/e2e/data/symbols.yaml} (a Phase 3 follow-up YAML loader will wire this in
 * production; the dev cohort lists all major-FX pairs as permitted to {@code ACME}). Loaded ONCE at
 * launcher boot; immutable after construction so the drain thread can read without any
 * synchronisation.
 *
 * <p><b>Threading model.</b> Effectively immutable after construction. The two lookup methods
 * {@link #entitledSymbolsFor(String)} and {@link #permittedAccountsFor(long)} are safe to call from
 * any thread without synchronisation — the backing {@code Long2ObjectHashMap} + {@code
 * ObjectHashSet} are populated in the constructor and never mutated afterwards.
 *
 * <p><b>Allocation.</b> Zero allocation on lookup at steady state. The constructor allocates the
 * two backing maps and the per-account {@code ObjectHashSet} of packed-symbol longs; these live for
 * the lifetime of the websocket-server process.
 *
 * <p><b>Design rationale.</b>
 *
 * <ul>
 *   <li><b>Two-way lookup.</b> The hot path is {@code permittedAccountsFor(packedSymbol)} — called
 *       by {@code SubscriptionFilter.matches(...)} to gate egress on the per-account entitlement
 *       bitmap. The cold path is {@code entitledSymbolsFor(accountCode)} — called once per session
 *       at auth time to build the session's {@code entitledSymbolsByAccount} {@code LongHashSet}.
 *       Both lookups are O(1) on a primitive-keyed Agrona map.
 *   <li><b>Packed-symbol long keys.</b> Symbols are 8-byte fixed-width ASCII in the SBE wire
 *       contract; packing into a {@code long} via {@link SymbolPacker#pack(String)} eliminates the
 *       {@code String} hash + per-call allocation that {@code Map<String, ?>} would require on
 *       every egress fragment.
 *   <li><b>{@code ObjectHashSet<String>} per symbol.</b> Account codes are sparse and string- typed
 *       on the YAML side; the per-symbol set membership check ({@code contains(account)}) is the
 *       actual entitlement gate. A small Agrona {@code ObjectHashSet} avoids the {@code
 *       java.util.HashSet} synchronisation overhead and keeps the lookup zero-alloc.
 * </ul>
 *
 * <p><b>Dependencies.</b> {@link SymbolPacker} for the 8-byte ASCII → {@code long} packing; Agrona
 * collections for the primitive-keyed map.
 *
 * @see SubscriptionFilter
 */
public final class SymbolEntitlementMap {

  /**
   * Per packed-symbol set of permitted account codes. Populated in the constructor; never mutated
   * afterwards.
   */
  private final Long2ObjectHashMap<ObjectHashSet<String>> symbolToAccounts;

  /**
   * Per account code → primitive packed-symbol set the account is entitled to receive. Inverted
   * index built at construction so per-session auth-time lookup is O(1). {@link LongHashSet} (NOT
   * {@code ObjectHashSet<Long>}) so the {@link WebSocketSession#publishEntitledSymbols} iteration
   * can use the primitive {@code addAll(LongHashSet)} path — zero {@link Long} boxing across the
   * auth/resume path (Agent A review F-1: reconnect storms make this iteration frequent enough to
   * matter).
   */
  private final Map<String, LongHashSet> accountToSymbols;

  /**
   * Empty fallback returned by {@link #permittedAccountsFor(long)} when the symbol is not in the
   * map. Pre-allocated; never mutated.
   */
  private static final ObjectHashSet<String> EMPTY_ACCOUNTS = new ObjectHashSet<>(1);

  /**
   * Shared zero-capacity empty fallback returned by {@link #entitledSymbolsFor(String)} when the
   * account has no entitlements. Pre-allocated once at class init; callers MUST treat the returned
   * set as read-only. Agrona has no immutable {@link LongHashSet} variant, so the convention is
   * enforced by the public Javadoc + by the only caller ({@link
   * WebSocketSession#publishEntitledSymbols}) using a read-only {@code addAll(LongHashSet)}
   * traversal.
   */
  private static final LongHashSet EMPTY_SYMBOLS = new LongHashSet(0);

  /**
   * Constructs the entitlement map from a {@code symbol → list-of-accounts} mapping (typically
   * parsed from {@code symbols.yaml}). Validates non-empty inputs and computes the inverted index
   * in O(N × M) time where N = number of symbols and M = average accounts-per-symbol.
   *
   * @param symbolEntitlements the raw {@code symbol → permitted-account-codes} mapping. Each symbol
   *     key is an unpacked ASCII string (e.g. {@code "EURUSD"}); each list contains the permitted
   *     account-code strings (e.g. {@code ["ACME", "GLOBEX"]}). Both must be non-null and
   *     non-empty; the constructor throws {@link IllegalArgumentException} otherwise.
   */
  public SymbolEntitlementMap(final Map<String, List<String>> symbolEntitlements) {
    Objects.requireNonNull(symbolEntitlements, "symbolEntitlements");
    if (symbolEntitlements.isEmpty()) {
      throw new IllegalArgumentException(
          "symbolEntitlements must not be empty — at least one symbol must be configured");
    }
    this.symbolToAccounts = new Long2ObjectHashMap<>(symbolEntitlements.size() * 2, 0.55f);
    this.accountToSymbols = new LinkedHashMap<>();

    for (final Map.Entry<String, List<String>> entry : symbolEntitlements.entrySet()) {
      final String symbol = Objects.requireNonNull(entry.getKey(), "symbol");
      final List<String> accounts = Objects.requireNonNull(entry.getValue(), "accounts");
      if (accounts.isEmpty()) {
        throw new IllegalArgumentException(
            "symbol " + symbol + " must have at least one permitted account");
      }
      final long packed = SymbolPacker.pack(symbol);
      final ObjectHashSet<String> accountSet = new ObjectHashSet<>(accounts.size() * 2);
      for (final String account : accounts) {
        accountSet.add(Objects.requireNonNull(account, "account"));
        accountToSymbols.computeIfAbsent(account, k -> new LongHashSet(4)).add(packed);
      }
      this.symbolToAccounts.put(packed, accountSet);
    }
  }

  /**
   * Returns the set of account codes permitted to receive market data for the given symbol. O(1)
   * primitive-keyed lookup; zero allocation. Returns an immutable empty set if the symbol is not in
   * the map (defensive default — an unknown symbol entitles no accounts).
   *
   * @param packedSymbol the 8-byte symbol packed into a {@code long} via {@link
   *     SymbolPacker#pack(String)}.
   * @return the (read-only) set of permitted account codes; empty if symbol unknown.
   */
  public ObjectHashSet<String> permittedAccountsFor(final long packedSymbol) {
    final ObjectHashSet<String> set = symbolToAccounts.get(packedSymbol);
    return set != null ? set : EMPTY_ACCOUNTS;
  }

  /**
   * Returns the primitive set of packed-symbol longs the given account code is entitled to receive.
   * O(1) lookup. Returns a shared zero-capacity {@link LongHashSet} sentinel if the account has no
   * entitlements — callers MUST NOT mutate the returned set (Agrona has no immutable {@code
   * LongHashSet} variant; the read-only contract is enforced by convention + by the single in-tree
   * caller {@link WebSocketSession#publishEntitledSymbols}).
   *
   * <p>Used at session auth time to populate the session's per-channel entitlement {@link
   * LongHashSet} via a primitive {@code addAll} — zero {@link Long} boxing across the auth/resume
   * hot path (Agent A review F-1: reconnect storms make this iteration frequent enough that boxing
   * the entitled-symbol set per element materially matters).
   *
   * @param accountCode the account code (e.g. {@code "ACME"}).
   * @return the (read-only) primitive set of packed symbols; the shared empty sentinel if account
   *     unknown.
   */
  public LongHashSet entitledSymbolsFor(final String accountCode) {
    final LongHashSet set = accountToSymbols.get(accountCode);
    return set != null ? set : EMPTY_SYMBOLS;
  }

  /**
   * @return the total number of distinct symbols in the entitlement map.
   */
  public int symbolCount() {
    return symbolToAccounts.size();
  }

  /**
   * @return the total number of distinct accounts in the entitlement map.
   */
  public int accountCount() {
    return accountToSymbols.size();
  }

  /**
   * Returns an unmodifiable view of the accounts that appear anywhere in the map. Cold path — used
   * by diagnostics + audit logging only. Allocates an unmodifiable view wrapper; not suitable for
   * hot-path use.
   *
   * @return read-only set of every configured account code.
   */
  public Set<String> allAccounts() {
    return Collections.unmodifiableSet(accountToSymbols.keySet());
  }
}
