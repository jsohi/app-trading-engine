package com.trading.refdata.risklimit;

import com.trading.refdata.ReferenceDataLoadException;
import com.trading.refdata.spi.ReferenceDataLoader;
import com.trading.refdata.spi.StatusValidator;
import java.io.IOException;
import java.io.Reader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.agrona.collections.LongHashSet;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.yaml.snakeyaml.Yaml;

/**
 * Loads risk-limit records from a YAML file.
 *
 * <p>Expected format:
 *
 * <pre>
 * riskLimits:
 *   - accountId: 1
 *     maxOrderSize: 1000000000
 *     maxOrderNotional: 0
 *     maxDailyVolume: 0
 *     status: "Active"
 * </pre>
 *
 * <p>Validates: {@code accountId &gt; 0}; all limit values &ge; 0. Rejects duplicates by {@code
 * accountId} (one limit per account). Rejects fractional numeric values.
 *
 * <p>APP-62: {@code maxDailyLossBps} was removed from the schema in this PR. If a stale fixture
 * still carries the key, the loader emits a {@code WARN}-level log entry naming the entry index and
 * ignores the value. The field will return in APP-180 when mark price + filled position are
 * available.
 *
 * <p>Not thread-safe — single-threaded startup use only.
 */
public final class YamlRiskLimitLoader implements ReferenceDataLoader<RiskLimitRecord> {

  private static final Logger LOG = LogManager.getLogger(YamlRiskLimitLoader.class);
  private static final String ENTITY_TYPE = "RiskLimit";

  // Instance field — SnakeYAML Yaml is NOT thread-safe (holds internal parsing state).
  private final Yaml yaml = new Yaml();

  private final Path filePath;

  /**
   * Creates a loader that reads risk limits from the given YAML file.
   *
   * @param filePath path to the YAML file; must be non-null and readable
   */
  public YamlRiskLimitLoader(final Path filePath) {
    this.filePath = Objects.requireNonNull(filePath, "filePath");
  }

  /** {@inheritDoc} */
  @Override
  public List<RiskLimitRecord> load() throws ReferenceDataLoadException {
    LOG.info("Loading risk limits from {}", filePath);

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

    if (!root.containsKey("riskLimits")) {
      throw new ReferenceDataLoadException(
          ENTITY_TYPE, "missing required root key 'riskLimits' in " + filePath);
    }

    final var rawList = root.get("riskLimits");
    if (!(rawList instanceof List<?> entries)) {
      throw new ReferenceDataLoadException(
          ENTITY_TYPE, "'riskLimits' must be a list in " + filePath);
    }

    final List<RiskLimitRecord> records = new ArrayList<>(entries.size());
    // Agrona LongHashSet avoids autoboxing long → Long on every add()
    final LongHashSet seenAccountIds = new LongHashSet();

    for (int i = 0; i < entries.size(); i++) {
      if (!(entries.get(i) instanceof Map<?, ?> map)) {
        throw new ReferenceDataLoadException(
            ENTITY_TYPE, "entry " + i + " is not a map in " + filePath);
      }

      @SuppressWarnings("unchecked")
      final var entry = (Map<String, Object>) map;

      final var record = toRecord(entry, i);

      if (!seenAccountIds.add(record.accountId())) {
        throw new ReferenceDataLoadException(
            ENTITY_TYPE,
            "duplicate accountId " + record.accountId() + " at entry " + i + " in " + filePath);
      }

      records.add(record);
    }

    LOG.info("Loaded {} risk limits from {}", records.size(), filePath);
    return List.copyOf(records);
  }

  /** {@inheritDoc} */
  @Override
  public String sourceName() {
    return filePath.getFileName().toString();
  }

  private RiskLimitRecord toRecord(final Map<String, Object> entry, final int index)
      throws ReferenceDataLoadException {
    try {
      final long accountId = requireLong(entry, "accountId");
      final long maxOrderSize = toLong(entry, "maxOrderSize");
      final long maxOrderNotional = toLong(entry, "maxOrderNotional");
      final long maxDailyVolume = toLong(entry, "maxDailyVolume");
      // APP-62: maxDailyLossBps removed; loudly WARN if a stale fixture still carries the key so
      // operators notice configuration drift rather than running with a control they expect.
      if (entry.containsKey("maxDailyLossBps")) {
        LOG.warn(
            "RiskLimit entry {} in {} carries deprecated field 'maxDailyLossBps' (removed by APP-62; returns in APP-180). Value is ignored.",
            index,
            filePath);
      }
      final String status = requireStringOrDefault(entry, "status", "Active");
      StatusValidator.validateStatus(status, ENTITY_TYPE);

      // RiskLimitRecord compact constructor validates remaining constraints
      return new RiskLimitRecord(accountId, maxOrderSize, maxOrderNotional, maxDailyVolume, status);
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
   * Extracts an integral long value from the map. Returns 0 if the key is absent. Rejects
   * fractional {@link Double}/{@link Float} values to prevent silent truncation of financial data.
   * Accepts {@link BigDecimal} only if it represents an exact integer.
   */
  private static long toLong(final Map<String, Object> map, final String key)
      throws ReferenceDataLoadException {
    if (!map.containsKey(key)) {
      return 0L;
    }
    final var value = map.get(key);
    if (value == null) {
      throw new ReferenceDataLoadException(
          ENTITY_TYPE, "field '" + key + "' must not be null (omit the key for the default of 0)");
    }
    return requireIntegralLong(value, key);
  }

  /**
   * Extracts a required integral long value from the map. Throws if the key is absent. Rejects
   * fractional values to prevent silent truncation.
   */
  private static long requireLong(final Map<String, Object> map, final String key)
      throws ReferenceDataLoadException {
    final var value = map.get(key);
    if (value == null) {
      throw new ReferenceDataLoadException(ENTITY_TYPE, "missing required field '" + key + "'");
    }
    return requireIntegralLong(value, key);
  }

  /**
   * Converts a YAML-parsed value to a long, rejecting fractional numbers to prevent silent
   * truncation. Accepts {@link Byte}, {@link Short}, {@link Integer}, {@link Long} directly.
   * Accepts {@link BigInteger} via {@link BigInteger#longValueExact()}. Accepts {@link BigDecimal}
   * only if it has no fractional part ({@link BigDecimal#longValueExact()}). Rejects {@link Double}
   * and {@link Float} — YAML decimal literals like {@code 1.5} would silently truncate to {@code 1}
   * with {@link Number#longValue()}, corrupting fixed-point financial data.
   */
  private static long requireIntegralLong(final Object value, final String key)
      throws ReferenceDataLoadException {
    if (value instanceof Long l) {
      return l;
    }
    if (value instanceof Integer i) {
      return i.longValue();
    }
    if (value instanceof Short s) {
      return s.longValue();
    }
    if (value instanceof Byte b) {
      return b.longValue();
    }
    if (value instanceof BigInteger bi) {
      try {
        return bi.longValueExact();
      } catch (final ArithmeticException e) {
        throw new ReferenceDataLoadException(
            ENTITY_TYPE, "field '" + key + "' overflows long range: '" + value + "'", e);
      }
    }
    if (value instanceof BigDecimal bd) {
      try {
        return bd.longValueExact();
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
    // Fall back to string parsing for any other type
    try {
      return Long.parseLong(value.toString());
    } catch (final NumberFormatException e) {
      throw new ReferenceDataLoadException(
          ENTITY_TYPE, "field '" + key + "' is not a valid number: '" + value + "'", e);
    }
  }

  /**
   * Returns a non-blank string value, or the default if the key is absent. An explicitly blank,
   * null, or non-string value is treated as invalid and throws. Explicit YAML {@code null} is
   * distinguished from a missing key via {@link Map#containsKey}.
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
