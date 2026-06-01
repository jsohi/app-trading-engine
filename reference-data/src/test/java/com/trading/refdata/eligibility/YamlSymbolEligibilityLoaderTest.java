package com.trading.refdata.eligibility;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.trading.refdata.ReferenceDataLoadException;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.util.Objects;
import org.junit.jupiter.api.Test;

/**
 * Tests YAML loading, validation, and edge-case handling for {@link YamlSymbolEligibilityLoader}.
 */
final class YamlSymbolEligibilityLoaderTest {

  private Path testResource(final String name) {
    final var url = Objects.requireNonNull(getClass().getClassLoader().getResource(name), name);
    try {
      return Path.of(url.toURI());
    } catch (final URISyntaxException e) {
      throw new AssertionError("invalid test resource URI: " + url, e);
    }
  }

  @Test
  void loadValidEligibilities() throws Exception {
    final var loader = new YamlSymbolEligibilityLoader(testResource("eligibilities-valid.yaml"));
    final var records = loader.load();

    assertEquals(2, records.size());

    final var first = records.get(0);
    assertEquals("EURUSD", first.symbol());
    assertTrue(first.tradingAllowed());
    assertTrue(first.shortSaleAllowed());
    assertEquals(250L, first.priceDeviationBpsOverride());
    // CLAUDE.md Rule 9: loader emits 0L sentinel; cluster restamps at ingest.
    assertEquals(0L, first.asOfTimestamp());

    final var second = records.get(1);
    assertEquals("GBPUSD", second.symbol());
    assertFalse(second.tradingAllowed());
    assertFalse(second.shortSaleAllowed());
    assertEquals(0L, second.priceDeviationBpsOverride());
    // Both records share the 0L sentinel — cluster will assign the authoritative timestamp.
    assertEquals(0L, second.asOfTimestamp());
  }

  @Test
  void loadEmptyFile() throws Exception {
    final var loader = new YamlSymbolEligibilityLoader(testResource("eligibilities-empty.yaml"));
    final var records = loader.load();
    assertTrue(records.isEmpty());
  }

  @Test
  void loadMissingFileThrows() {
    final var loader =
        new YamlSymbolEligibilityLoader(Path.of("/nonexistent/restricted-symbols.yaml"));
    final var ex = assertThrows(ReferenceDataLoadException.class, loader::load);
    assertEquals("SymbolEligibility", ex.entityType());
    assertTrue(ex.getMessage().contains("cannot read"));
  }

  @Test
  void loadMalformedYamlThrows() {
    final var loader =
        new YamlSymbolEligibilityLoader(testResource("eligibilities-malformed.yaml"));
    final var ex = assertThrows(ReferenceDataLoadException.class, loader::load);
    assertEquals("SymbolEligibility", ex.entityType());
    assertTrue(ex.getMessage().contains("malformed YAML"));
  }

  @Test
  void loadDuplicateSymbolThrows() {
    final var loader =
        new YamlSymbolEligibilityLoader(testResource("eligibilities-duplicate-symbol.yaml"));
    final var ex = assertThrows(ReferenceDataLoadException.class, loader::load);
    assertEquals("SymbolEligibility", ex.entityType());
    assertTrue(ex.getMessage().contains("duplicate symbol"));
  }

  @Test
  void loadMissingSymbolThrows() {
    final var loader =
        new YamlSymbolEligibilityLoader(testResource("eligibilities-missing-symbol.yaml"));
    final var ex = assertThrows(ReferenceDataLoadException.class, loader::load);
    assertEquals("SymbolEligibility", ex.entityType());
    assertTrue(ex.getMessage().contains("symbol"));
  }

  /**
   * The §G policy is fail-closed: omitted boolean flags default to {@code false} so a careless
   * fixture cannot accidentally widen the trading / short-sale allowlist. {@code
   * priceDeviationBpsOverride} defaults to {@code 0} (no override; per-account knob applies).
   */
  @Test
  void loadDefaultsBooleansFalseAndOverrideZero() throws Exception {
    final var loader = new YamlSymbolEligibilityLoader(testResource("eligibilities-defaults.yaml"));
    final var records = loader.load();

    assertEquals(1, records.size());
    final var record = records.get(0);
    assertEquals("EURUSD", record.symbol());
    assertTrue(record.tradingAllowed());
    assertTrue(record.shortSaleAllowed());
    // priceDeviationBpsOverride omitted → 0
    assertEquals(0L, record.priceDeviationBpsOverride());
  }

  @Test
  void sourceNameReturnsFileName() {
    final var loader =
        new YamlSymbolEligibilityLoader(Path.of("/some/path/restricted-symbols.yaml"));
    assertEquals("restricted-symbols.yaml", loader.sourceName());
  }

  @Test
  void recordRejectsBlankSymbol() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new SymbolEligibilityRecord("   ", true, true, 0L, 0L));
  }

  @Test
  void recordRejectsOversizedSymbol() {
    // SBE Symbol fixed-length is 8 bytes; a 9-char symbol must be rejected.
    assertThrows(
        IllegalArgumentException.class,
        () -> new SymbolEligibilityRecord("EURUSDXXX", true, true, 0L, 0L));
  }

  @Test
  void recordRejectsNegativeOverride() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new SymbolEligibilityRecord("EURUSD", true, true, -1L, 0L));
  }

  /**
   * SBE field {@code priceDeviationBpsOverride} is {@code uint32} (max representable {@code
   * 4_294_967_294}; SBE convention reserves {@code 4_294_967_295} as the null sentinel). Values
   * above the bound would silently truncate to the low 32 bits on encode, corrupting the per-symbol
   * fat-finger override that gates order acceptance. We reject at the POJO boundary.
   */
  @Test
  void recordRejectsPriceDeviationOverrideAboveUint32Max() {
    final var ex =
        assertThrows(
            IllegalArgumentException.class,
            () -> new SymbolEligibilityRecord("EURUSD", true, true, 5_000_000_000L, 0L));
    assertTrue(
        ex.getMessage().contains("uint32 max"),
        "expected uint32-max message, got: " + ex.getMessage());
    assertTrue(
        ex.getMessage().contains("5000000000"),
        "expected offending value in message, got: " + ex.getMessage());
  }

  /** The boundary value itself is accepted; only values strictly above {@code 4_294_967_294}. */
  @Test
  void recordAcceptsPriceDeviationOverrideAtUint32MaxBoundary() {
    final var record =
        new SymbolEligibilityRecord(
            "EURUSD", true, true, SymbolEligibilityRecord.MAX_PRICE_DEVIATION_BPS_OVERRIDE, 0L);
    assertEquals(4_294_967_294L, record.priceDeviationBpsOverride());
  }
}
