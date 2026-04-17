package com.trading.refdata.risklimit;

import com.trading.refdata.ReferenceDataLoadException;
import com.trading.refdata.spi.ReferenceDataLoader;
import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.agrona.collections.LongHashSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
 *     maxDailyLossBps: 50
 *     status: "Active"
 * </pre>
 *
 * <p>Validates: accountId &gt; 0, all limits &ge; 0, maxDailyLossBps fits uint32 range. Rejects
 * duplicates by {@code accountId} (one limit per account).
 *
 * <p>Not thread-safe — single-threaded startup use only.
 */
public final class YamlRiskLimitLoader implements ReferenceDataLoader<RiskLimitRecord> {

  private static final Logger LOG = LoggerFactory.getLogger(YamlRiskLimitLoader.class);
  private static final String ENTITY_TYPE = "RiskLimit";

  private static final Yaml YAML = new Yaml();

  private final Path filePath;

  /**
   * Creates a loader that reads risk limits from the given YAML file.
   *
   * @param filePath path to the YAML file; must be non-null and readable
   */
  public YamlRiskLimitLoader(final Path filePath) {
    this.filePath = filePath;
  }

  /** {@inheritDoc} */
  @Override
  public List<RiskLimitRecord> load() throws ReferenceDataLoadException {
    LOG.info("Loading risk limits from {}", filePath);

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

    if (!root.containsKey("riskLimits")) {
      return List.of();
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
      final long maxDailyLossBps = toLong(entry, "maxDailyLossBps");
      final String status = stringOrDefault(entry, "status", "Active");

      // RiskLimitRecord compact constructor validates all constraints
      return new RiskLimitRecord(
          accountId, maxOrderSize, maxOrderNotional, maxDailyVolume, maxDailyLossBps, status);
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

  private static long toLong(final Map<String, Object> map, final String key)
      throws ReferenceDataLoadException {
    final var value = map.get(key);
    if (value == null) {
      return 0L;
    }
    if (value instanceof Number n) {
      return n.longValue();
    }
    try {
      return Long.parseLong(value.toString());
    } catch (final NumberFormatException e) {
      throw new ReferenceDataLoadException(
          ENTITY_TYPE, "field '" + key + "' is not a valid number: '" + value + "'", e);
    }
  }

  private static long requireLong(final Map<String, Object> map, final String key)
      throws ReferenceDataLoadException {
    final var value = map.get(key);
    if (value == null) {
      throw new ReferenceDataLoadException(ENTITY_TYPE, "missing required field '" + key + "'");
    }
    if (value instanceof Number n) {
      return n.longValue();
    }
    try {
      return Long.parseLong(value.toString());
    } catch (final NumberFormatException e) {
      throw new ReferenceDataLoadException(
          ENTITY_TYPE, "field '" + key + "' is not a valid number: '" + value + "'", e);
    }
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
