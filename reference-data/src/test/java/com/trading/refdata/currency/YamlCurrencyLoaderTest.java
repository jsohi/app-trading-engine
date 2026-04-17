package com.trading.refdata.currency;

import static org.junit.jupiter.api.Assertions.*;

import com.trading.refdata.ReferenceDataLoadException;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.util.Objects;
import org.junit.jupiter.api.Test;

/** Tests YAML loading, validation, and edge-case handling for {@link YamlCurrencyLoader}. */
final class YamlCurrencyLoaderTest {

  private Path testResource(final String name) {
    final var url = Objects.requireNonNull(getClass().getClassLoader().getResource(name), name);
    try {
      return Path.of(url.toURI());
    } catch (final URISyntaxException e) {
      throw new AssertionError("invalid test resource URI: " + url, e);
    }
  }

  @Test
  void loadValidCurrencies() throws Exception {
    final var loader = new YamlCurrencyLoader(testResource("currencies-valid.yaml"));
    final var records = loader.load();

    assertEquals(2, records.size());

    final var usd = records.get(0);
    assertEquals("USD", usd.ccyCode());
    assertEquals(840, usd.isoNumeric());
    assertEquals("US Dollar", usd.name());
    assertEquals(2, usd.decimals());
    assertEquals("Fiat", usd.currencyClass());
    assertEquals("Active", usd.status());

    final var eur = records.get(1);
    assertEquals("EUR", eur.ccyCode());
    assertEquals(978, eur.isoNumeric());
    assertEquals("Euro", eur.name());
    assertEquals(2, eur.decimals());
    assertEquals("Fiat", eur.currencyClass());
    assertEquals("Suspended", eur.status());
  }

  @Test
  void loadEmptyFile() throws Exception {
    final var loader = new YamlCurrencyLoader(testResource("currencies-empty.yaml"));
    final var records = loader.load();

    assertTrue(records.isEmpty());
  }

  @Test
  void loadMalformedYamlThrows() {
    final var loader = new YamlCurrencyLoader(testResource("currencies-malformed.yaml"));

    final var ex = assertThrows(ReferenceDataLoadException.class, loader::load);
    assertEquals("Currency", ex.entityType());
    assertTrue(ex.getMessage().contains("malformed YAML"));
  }

  @Test
  void loadDuplicateCcyCodeThrows() {
    final var loader = new YamlCurrencyLoader(testResource("currencies-duplicate.yaml"));

    final var ex = assertThrows(ReferenceDataLoadException.class, loader::load);
    assertEquals("Currency", ex.entityType());
    assertTrue(ex.getMessage().contains("duplicate ccyCode"));
    assertTrue(ex.getMessage().contains("USD"));
  }

  @Test
  void loadInvalidIsoNumericThrows() {
    final var loader = new YamlCurrencyLoader(testResource("currencies-invalid-iso.yaml"));

    final var ex = assertThrows(ReferenceDataLoadException.class, loader::load);
    assertEquals("Currency", ex.entityType());
    assertTrue(ex.getMessage().contains("isoNumeric must be in [1, 999]"));
  }

  @Test
  void loadInvalidDecimalsThrows() {
    final var loader = new YamlCurrencyLoader(testResource("currencies-invalid-decimals.yaml"));

    final var ex = assertThrows(ReferenceDataLoadException.class, loader::load);
    assertEquals("Currency", ex.entityType());
    assertTrue(ex.getMessage().contains("decimals must be in [0, 18]"));
  }

  @Test
  void loadLowercaseCcyCodeThrows() {
    final var loader = new YamlCurrencyLoader(testResource("currencies-invalid-code.yaml"));

    final var ex = assertThrows(ReferenceDataLoadException.class, loader::load);
    assertEquals("Currency", ex.entityType());
    assertTrue(ex.getMessage().contains("uppercase ASCII"));
  }

  @Test
  void loadMissingRequiredFieldThrows() {
    final var loader = new YamlCurrencyLoader(testResource("currencies-missing-field.yaml"));

    final var ex = assertThrows(ReferenceDataLoadException.class, loader::load);
    assertEquals("Currency", ex.entityType());
    assertTrue(ex.getMessage().contains("name"));
  }

  @Test
  void loadMissingFileThrows() {
    final var loader = new YamlCurrencyLoader(Path.of("/nonexistent/currencies.yaml"));

    final var ex = assertThrows(ReferenceDataLoadException.class, loader::load);
    assertEquals("Currency", ex.entityType());
    assertTrue(ex.getMessage().contains("cannot read"));
  }

  @Test
  void sourceNameReturnsFileName() {
    final var loader = new YamlCurrencyLoader(Path.of("/some/path/currencies.yaml"));
    assertEquals("currencies.yaml", loader.sourceName());
  }

  @Test
  void loadBlankCcyCodeThrows() {
    final var loader = new YamlCurrencyLoader(testResource("currencies-blank-code.yaml"));

    final var ex = assertThrows(ReferenceDataLoadException.class, loader::load);
    assertEquals("Currency", ex.entityType());
    assertTrue(ex.getMessage().contains("must not be blank"));
  }

  @Test
  void recordRejectsInvalidIsoNumeric() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new CurrencyRecord("USD", 0, "US Dollar", 2, "Fiat", "Active"));
  }

  @Test
  void recordRejectsInvalidDecimals() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new CurrencyRecord("USD", 840, "US Dollar", 19, "Fiat", "Active"));
  }

  @Test
  void recordRejectsNullName() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new CurrencyRecord("USD", 840, null, 2, "Fiat", "Active"));
  }

  @Test
  void loadDefaultsOptionalStatusToActive() throws Exception {
    final var loader = new YamlCurrencyLoader(testResource("currencies-defaults.yaml"));
    final var records = loader.load();

    assertEquals(1, records.size());
    assertEquals("Active", records.get(0).status());
  }
}
