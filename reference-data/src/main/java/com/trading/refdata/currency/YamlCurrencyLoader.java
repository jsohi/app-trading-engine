package com.trading.refdata.currency;

import com.trading.refdata.ReferenceDataLoadException;
import com.trading.refdata.spi.ReferenceDataLoader;
import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

/**
 * Loads currency records from a YAML file.
 *
 * <p>Expected format:
 *
 * <pre>
 * currencies:
 *   - ccyCode: "USD"
 *     isoNumeric: 840
 *     name: "US Dollar"
 *     decimals: 2
 *     currencyClass: "Fiat"
 *     status: "Active"
 * </pre>
 *
 * <p>Validates: ccyCode is exactly 3 uppercase ASCII, isoNumeric in [1,&nbsp;999], decimals in
 * [0,&nbsp;18]. Rejects duplicates by {@code ccyCode}.
 *
 * <p>Not thread-safe — single-threaded startup use only.
 */
public final class YamlCurrencyLoader implements ReferenceDataLoader<CurrencyRecord> {

  private static final Logger LOG = LoggerFactory.getLogger(YamlCurrencyLoader.class);
  private static final String ENTITY_TYPE = "Currency";

  private static final Yaml YAML = new Yaml();

  private final Path filePath;

  /**
   * Creates a loader that reads currencies from the given YAML file.
   *
   * @param filePath path to the YAML file; must be non-null and readable
   */
  public YamlCurrencyLoader(final Path filePath) {
    this.filePath = filePath;
  }

  /** {@inheritDoc} */
  @Override
  public List<CurrencyRecord> load() throws ReferenceDataLoadException {
    LOG.info("Loading currencies from {}", filePath);

    final Object parsed;
    try (final Reader reader = Files.newBufferedReader(filePath, StandardCharsets.UTF_8)) {
      parsed = YAML.load(reader);
    } catch (final IOException e) {
      throw new ReferenceDataLoadException(ENTITY_TYPE, "cannot read " + filePath, e);
    } catch (final Exception e) {
      throw new ReferenceDataLoadException(ENTITY_TYPE, "malformed YAML in " + filePath, e);
    }

    if (parsed == null) {
      return List.of();
    }
    if (!(parsed instanceof Map<?, ?> rootMap)) {
      throw new ReferenceDataLoadException(
          ENTITY_TYPE,
          "YAML root must be a map, got " + parsed.getClass().getSimpleName() + " in " + filePath);
    }

    @SuppressWarnings("unchecked")
    final var root = (Map<String, Object>) rootMap;

    if (!root.containsKey("currencies")) {
      return List.of();
    }

    final var rawList = root.get("currencies");
    if (!(rawList instanceof List<?> entries)) {
      throw new ReferenceDataLoadException(
          ENTITY_TYPE, "'currencies' must be a list in " + filePath);
    }

    final List<CurrencyRecord> records = new ArrayList<>(entries.size());
    final Set<String> seenCodes = new HashSet<>();

    for (int i = 0; i < entries.size(); i++) {
      if (!(entries.get(i) instanceof Map<?, ?> map)) {
        throw new ReferenceDataLoadException(
            ENTITY_TYPE, "entry " + i + " is not a map in " + filePath);
      }

      @SuppressWarnings("unchecked")
      final var entry = (Map<String, Object>) map;

      final var record = toRecord(entry, i);

      if (!seenCodes.add(record.ccyCode())) {
        throw new ReferenceDataLoadException(
            ENTITY_TYPE,
            "duplicate ccyCode '" + record.ccyCode() + "' at entry " + i + " in " + filePath);
      }

      records.add(record);
    }

    LOG.info("Loaded {} currencies from {}", records.size(), filePath);
    return List.copyOf(records);
  }

  /** {@inheritDoc} */
  @Override
  public String sourceName() {
    return filePath.getFileName().toString();
  }

  private CurrencyRecord toRecord(final Map<String, Object> entry, final int index)
      throws ReferenceDataLoadException {
    try {
      final String ccyCode = requireString(entry, "ccyCode");
      validateCcyCode(ccyCode);

      final int isoNumeric = requireInt(entry, "isoNumeric");
      if (isoNumeric < 1 || isoNumeric > 999) {
        throw new ReferenceDataLoadException(
            ENTITY_TYPE, "isoNumeric must be in [1, 999], got " + isoNumeric);
      }

      final String name = requireString(entry, "name");

      final int decimals = requireInt(entry, "decimals");
      if (decimals < 0 || decimals > 18) {
        throw new ReferenceDataLoadException(
            ENTITY_TYPE, "decimals must be in [0, 18], got " + decimals);
      }

      final String currencyClass = requireString(entry, "currencyClass");
      final String status = stringOrDefault(entry, "status", "Active");

      return new CurrencyRecord(ccyCode, isoNumeric, name, decimals, currencyClass, status);
    } catch (final ReferenceDataLoadException e) {
      throw e;
    } catch (final IllegalArgumentException e) {
      // Re-wrap compact constructor validation failures
      throw new ReferenceDataLoadException(
          ENTITY_TYPE, "invalid entry " + index + " in " + filePath + ": " + e.getMessage());
    } catch (final Exception e) {
      throw new ReferenceDataLoadException(
          ENTITY_TYPE, "invalid entry " + index + " in " + filePath, e);
    }
  }

  /** Validates that {@code ccyCode} is exactly 3 uppercase ASCII characters (A–Z). */
  private static void validateCcyCode(final String ccyCode) throws ReferenceDataLoadException {
    if (ccyCode.length() != 3) {
      throw new ReferenceDataLoadException(
          ENTITY_TYPE, "ccyCode must be exactly 3 characters, got '" + ccyCode + "'");
    }
    for (int i = 0; i < 3; i++) {
      final char c = ccyCode.charAt(i);
      if (c < 'A' || c > 'Z') {
        throw new ReferenceDataLoadException(
            ENTITY_TYPE, "ccyCode must be uppercase ASCII, got '" + ccyCode + "'");
      }
    }
  }

  private static int requireInt(final Map<String, Object> map, final String key)
      throws ReferenceDataLoadException {
    final var value = map.get(key);
    if (value == null) {
      throw new ReferenceDataLoadException(ENTITY_TYPE, "missing required field '" + key + "'");
    }
    if (value instanceof Number n) {
      return n.intValue();
    }
    try {
      return Integer.parseInt(value.toString());
    } catch (final NumberFormatException e) {
      throw new ReferenceDataLoadException(
          ENTITY_TYPE, "field '" + key + "' is not a valid number: '" + value + "'", e);
    }
  }

  private static String requireString(final Map<String, Object> map, final String key)
      throws ReferenceDataLoadException {
    final var value = map.get(key);
    if (value == null) {
      throw new ReferenceDataLoadException(ENTITY_TYPE, "missing required field '" + key + "'");
    }
    final var str = value.toString();
    if (str.isBlank()) {
      throw new ReferenceDataLoadException(ENTITY_TYPE, "field '" + key + "' must not be blank");
    }
    return str;
  }

  private static String stringOrDefault(
      final Map<String, Object> map, final String key, final String defaultValue) {
    final var value = map.get(key);
    if (value == null) {
      return defaultValue;
    }
    final var str = value.toString();
    return str.isBlank() ? defaultValue : str;
  }
}
