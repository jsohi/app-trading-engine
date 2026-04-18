package com.trading.refdata.currency;

import com.trading.refdata.ReferenceDataLoadException;
import com.trading.refdata.spi.ReferenceDataLoader;
import java.io.IOException;
import java.io.Reader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.agrona.collections.IntHashSet;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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
 * [0,&nbsp;18]. Rejects duplicates by {@code ccyCode} and {@code isoNumeric}.
 *
 * <p>Not thread-safe — single-threaded startup use only.
 */
public final class YamlCurrencyLoader implements ReferenceDataLoader<CurrencyRecord> {

  private static final Logger LOG = LogManager.getLogger(YamlCurrencyLoader.class);
  private static final String ENTITY_TYPE = "Currency";

  // Instance field — SnakeYAML Yaml is NOT thread-safe (holds internal parsing state).
  private final Yaml yaml = new Yaml();

  private final Path filePath;

  /**
   * Creates a loader that reads currencies from the given YAML file.
   *
   * @param filePath path to the YAML file; must be non-null and readable
   */
  public YamlCurrencyLoader(final Path filePath) {
    this.filePath = Objects.requireNonNull(filePath, "filePath");
  }

  /** {@inheritDoc} */
  @Override
  public List<CurrencyRecord> load() throws ReferenceDataLoadException {
    LOG.info("Loading currencies from {}", filePath);

    final Object parsed;
    try (final Reader reader = Files.newBufferedReader(filePath, StandardCharsets.UTF_8)) {
      parsed = yaml.load(reader);
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
      throw new ReferenceDataLoadException(
          ENTITY_TYPE, "missing required root key 'currencies' in " + filePath);
    }

    final var rawList = root.get("currencies");
    if (!(rawList instanceof List<?> entries)) {
      throw new ReferenceDataLoadException(
          ENTITY_TYPE, "'currencies' must be a list in " + filePath);
    }

    final List<CurrencyRecord> records = new ArrayList<>(entries.size());
    final Set<String> seenCodes = new HashSet<>();
    // Agrona IntHashSet avoids autoboxing int → Integer on every add()
    final IntHashSet seenIsoNumerics = new IntHashSet();

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

      if (!seenIsoNumerics.add(record.isoNumeric())) {
        throw new ReferenceDataLoadException(
            ENTITY_TYPE,
            "duplicate isoNumeric " + record.isoNumeric() + " at entry " + i + " in " + filePath);
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
      final int isoNumeric = requireInt(entry, "isoNumeric");
      final String name = requireString(entry, "name");
      final int decimals = requireInt(entry, "decimals");
      final String currencyClass = requireString(entry, "currencyClass");
      final String status = requireStringOrDefault(entry, "status", "Active");

      // CurrencyRecord compact constructor validates all field constraints
      return new CurrencyRecord(ccyCode, isoNumeric, name, decimals, currencyClass, status);
    } catch (final ReferenceDataLoadException e) {
      throw e;
    } catch (final IllegalArgumentException e) {
      // Re-wrap compact constructor validation failures — preserve cause for diagnostics
      throw new ReferenceDataLoadException(
          ENTITY_TYPE, "invalid entry " + index + " in " + filePath + ": " + e.getMessage(), e);
    } catch (final Exception e) {
      throw new ReferenceDataLoadException(
          ENTITY_TYPE, "invalid entry " + index + " in " + filePath, e);
    }
  }

  /**
   * Extracts a required integral int value, rejecting fractional numbers to prevent silent
   * truncation. Same rationale as {@code YamlRiskLimitLoader.requireIntegralLong}.
   */
  private static int requireInt(final Map<String, Object> map, final String key)
      throws ReferenceDataLoadException {
    final var value = map.get(key);
    if (value == null) {
      throw new ReferenceDataLoadException(ENTITY_TYPE, "missing required field '" + key + "'");
    }
    if (value instanceof Integer i) {
      return i;
    }
    if (value instanceof Short s) {
      return s.intValue();
    }
    if (value instanceof Byte b) {
      return b.intValue();
    }
    if (value instanceof Long l) {
      if (l < Integer.MIN_VALUE || l > Integer.MAX_VALUE) {
        throw new ReferenceDataLoadException(
            ENTITY_TYPE, "field '" + key + "' overflows int range: '" + value + "'");
      }
      return l.intValue();
    }
    if (value instanceof BigInteger bi) {
      try {
        return bi.intValueExact();
      } catch (final ArithmeticException e) {
        throw new ReferenceDataLoadException(
            ENTITY_TYPE, "field '" + key + "' overflows int range: '" + value + "'", e);
      }
    }
    if (value instanceof BigDecimal bd) {
      try {
        return bd.intValueExact();
      } catch (final ArithmeticException e) {
        throw new ReferenceDataLoadException(
            ENTITY_TYPE, "field '" + key + "' is not an integral number: '" + value + "'", e);
      }
    }
    if (value instanceof Double || value instanceof Float) {
      throw new ReferenceDataLoadException(
          ENTITY_TYPE,
          "field '" + key + "' is a fractional number (use an integer): '" + value + "'");
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
    if (!(value instanceof String str)) {
      throw new ReferenceDataLoadException(
          ENTITY_TYPE,
          "field '" + key + "' must be a string, got " + value.getClass().getSimpleName());
    }
    if (str.isBlank()) {
      throw new ReferenceDataLoadException(ENTITY_TYPE, "field '" + key + "' must not be blank");
    }
    return str;
  }

  /**
   * Returns a non-blank string value, or the default if the key is absent. An explicitly blank or
   * non-string value is treated as invalid and throws. Explicit YAML {@code null} is distinguished
   * from a missing key via {@link Map#containsKey}.
   */
  private static String requireStringOrDefault(
      final Map<String, Object> map, final String key, final String defaultValue)
      throws ReferenceDataLoadException {
    if (!map.containsKey(key)) {
      return defaultValue;
    }
    final var value = map.get(key);
    if (value == null) {
      throw new ReferenceDataLoadException(
          ENTITY_TYPE, "field '" + key + "' must not be null (omit the key for the default)");
    }
    if (!(value instanceof String str)) {
      throw new ReferenceDataLoadException(
          ENTITY_TYPE,
          "field '" + key + "' must be a string, got " + value.getClass().getSimpleName());
    }
    if (str.isBlank()) {
      throw new ReferenceDataLoadException(ENTITY_TYPE, "field '" + key + "' must not be blank");
    }
    return str;
  }
}
