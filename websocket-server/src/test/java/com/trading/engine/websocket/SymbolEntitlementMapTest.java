package com.trading.engine.websocket;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.trading.engine.projections.SymbolPacker;
import java.util.List;
import java.util.Map;
import org.agrona.collections.LongHashSet;
import org.agrona.collections.ObjectHashSet;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link SymbolEntitlementMap}.
 *
 * <p>Covers the two-directional lookup contract: forward ({@code permittedAccountsFor}) and
 * inverted ({@code entitledSymbolsFor}), as well as constructor validation and the auxiliary
 * accessor methods ({@code symbolCount}, {@code accountCount}, {@code allAccounts}).
 *
 * <p><b>Threading:</b> Single-threaded test execution; {@link SymbolEntitlementMap} is effectively
 * immutable after construction so no synchronisation is needed in these tests.
 *
 * <p><b>Fixture strategy:</b> All fixtures use {@link Map#of} / {@link List#of} for brevity. Symbol
 * packing is performed via {@link SymbolPacker#pack(String)} to mirror what the production code
 * does internally, ensuring the test comparisons match the real key space.
 */
final class SymbolEntitlementMapTest {

  // -------------------------------------------------------------------------
  // 1. Constructor validation
  // -------------------------------------------------------------------------

  @Test
  void constructor_nullMap_throwsNullPointerException() {
    assertThrows(NullPointerException.class, () -> new SymbolEntitlementMap(null));
  }

  @Test
  void constructor_emptyMap_throwsIllegalArgumentException() {
    assertThrows(IllegalArgumentException.class, () -> new SymbolEntitlementMap(Map.of()));
  }

  @Test
  void constructor_symbolWithEmptyAccounts_throwsIllegalArgumentException() {
    // A symbol entry is present but its account list is empty — must be rejected.
    final var input = Map.of("EURUSD", List.<String>of());
    assertThrows(IllegalArgumentException.class, () -> new SymbolEntitlementMap(input));
  }

  // -------------------------------------------------------------------------
  // 2. Forward lookup — permittedAccountsFor
  // -------------------------------------------------------------------------

  @Test
  void permittedAccountsFor_knownSymbol_returnsConfiguredAccounts() {
    final var map = new SymbolEntitlementMap(Map.of("EURUSD", List.of("ACME", "GLOBEX")));

    final ObjectHashSet<String> accounts = map.permittedAccountsFor(SymbolPacker.pack("EURUSD"));

    assertNotNull(accounts);
    assertEquals(2, accounts.size());
    assertTrue(accounts.contains("ACME"), "Expected ACME in permitted accounts for EURUSD");
    assertTrue(accounts.contains("GLOBEX"), "Expected GLOBEX in permitted accounts for EURUSD");
  }

  @Test
  void permittedAccountsFor_unknownSymbol_returnsEmptySet() {
    final var map = new SymbolEntitlementMap(Map.of("EURUSD", List.of("ACME")));

    final ObjectHashSet<String> result = map.permittedAccountsFor(SymbolPacker.pack("XXXYYY"));

    assertNotNull(result, "Must return empty set, not null, for unknown symbols");
    assertTrue(result.isEmpty(), "Unknown symbol must yield empty account set");
  }

  // -------------------------------------------------------------------------
  // 3. Inverse lookup — entitledSymbolsFor
  // -------------------------------------------------------------------------

  @Test
  void entitledSymbolsFor_knownAccount_returnsAllEntitledSymbols() {
    final var map =
        new SymbolEntitlementMap(
            Map.of(
                "EURUSD", List.of("ACME"),
                "GBPUSD", List.of("ACME"),
                "USDJPY", List.of("GLOBEX")));

    final LongHashSet acmeSymbols = map.entitledSymbolsFor("ACME");

    assertNotNull(acmeSymbols);
    assertEquals(2, acmeSymbols.size(), "ACME should be entitled to exactly 2 symbols");
    assertTrue(
        acmeSymbols.contains(SymbolPacker.pack("EURUSD")), "ACME must be entitled to EURUSD");
    assertTrue(
        acmeSymbols.contains(SymbolPacker.pack("GBPUSD")), "ACME must be entitled to GBPUSD");
    assertFalse(
        acmeSymbols.contains(SymbolPacker.pack("USDJPY")),
        "ACME must NOT be entitled to USDJPY (GLOBEX only)");
  }

  @Test
  void entitledSymbolsFor_unknownAccount_returnsEmptySet() {
    final var map = new SymbolEntitlementMap(Map.of("EURUSD", List.of("ACME")));

    final LongHashSet result = map.entitledSymbolsFor("UNKNOWN_ACCT");

    assertNotNull(result, "Must return empty set, not null, for unknown accounts");
    assertTrue(result.isEmpty(), "Unknown account must yield empty symbol set");
  }

  // -------------------------------------------------------------------------
  // 4. Auxiliary accessors
  // -------------------------------------------------------------------------

  @Test
  void symbolCount_returnsConfiguredSymbolCount() {
    final var map =
        new SymbolEntitlementMap(
            Map.of(
                "EURUSD", List.of("ACME"),
                "GBPUSD", List.of("GLOBEX"),
                "USDJPY", List.of("ACME", "GLOBEX")));

    assertEquals(3, map.symbolCount());
  }

  @Test
  void accountCount_returnsConfiguredAccountCount() {
    // ACME appears in both symbols; GLOBEX appears in one.
    // The inverted index must deduplicate: accountCount == 2.
    final var map =
        new SymbolEntitlementMap(
            Map.of(
                "EURUSD", List.of("ACME", "GLOBEX"),
                "GBPUSD", List.of("ACME")));

    assertEquals(2, map.accountCount(), "Deduped account count should be 2 (ACME + GLOBEX)");
  }

  @Test
  void allAccounts_returnsUnmodifiableViewOfConfiguredAccounts() {
    final var map =
        new SymbolEntitlementMap(
            Map.of(
                "EURUSD", List.of("ACME", "GLOBEX"),
                "GBPUSD", List.of("ACME")));

    final var allAccounts = map.allAccounts();

    // Content assertions
    assertEquals(2, allAccounts.size());
    assertTrue(allAccounts.contains("ACME"));
    assertTrue(allAccounts.contains("GLOBEX"));

    // Immutability assertion — mutating the returned view must be rejected
    assertThrows(
        UnsupportedOperationException.class,
        () -> allAccounts.add("INTRUDER"),
        "allAccounts() must return an unmodifiable view");
  }

  // -------------------------------------------------------------------------
  // 5. Richer multi-symbol / multi-account inverted-index correctness
  // -------------------------------------------------------------------------

  @Test
  void multiSymbolMultiAccount_invertedIndexCorrect() {
    // 3 symbols, 3 accounts with overlapping entitlements:
    //   EURUSD -> ACME, GLOBEX
    //   GBPUSD -> ACME, PRIME
    //   USDJPY -> GLOBEX, PRIME
    final var map =
        new SymbolEntitlementMap(
            Map.of(
                "EURUSD", List.of("ACME", "GLOBEX"),
                "GBPUSD", List.of("ACME", "PRIME"),
                "USDJPY", List.of("GLOBEX", "PRIME")));

    final long eurUsd = SymbolPacker.pack("EURUSD");
    final long gbpUsd = SymbolPacker.pack("GBPUSD");
    final long usdJpy = SymbolPacker.pack("USDJPY");

    // --- Spot-check 1: GLOBEX is entitled to EURUSD and USDJPY but NOT GBPUSD ---
    final LongHashSet globexSymbols = map.entitledSymbolsFor("GLOBEX");
    assertEquals(2, globexSymbols.size(), "GLOBEX: expected 2 entitled symbols");
    assertTrue(globexSymbols.contains(eurUsd), "GLOBEX entitled to EURUSD");
    assertTrue(globexSymbols.contains(usdJpy), "GLOBEX entitled to USDJPY");
    assertFalse(globexSymbols.contains(gbpUsd), "GLOBEX NOT entitled to GBPUSD");

    // --- Spot-check 2: PRIME is entitled to GBPUSD and USDJPY but NOT EURUSD ---
    final LongHashSet primeSymbols = map.entitledSymbolsFor("PRIME");
    assertEquals(2, primeSymbols.size(), "PRIME: expected 2 entitled symbols");
    assertTrue(primeSymbols.contains(gbpUsd), "PRIME entitled to GBPUSD");
    assertTrue(primeSymbols.contains(usdJpy), "PRIME entitled to USDJPY");
    assertFalse(primeSymbols.contains(eurUsd), "PRIME NOT entitled to EURUSD");

    // --- Forward-lookup cross-check: GBPUSD permits ACME and PRIME only ---
    final ObjectHashSet<String> gbpAccounts = map.permittedAccountsFor(gbpUsd);
    assertEquals(2, gbpAccounts.size());
    assertTrue(gbpAccounts.contains("ACME"));
    assertTrue(gbpAccounts.contains("PRIME"));
    assertFalse(gbpAccounts.contains("GLOBEX"), "GLOBEX must NOT be permitted for GBPUSD");

    // --- Auxiliary counts ---
    assertEquals(3, map.symbolCount());
    assertEquals(3, map.accountCount());
  }
}
