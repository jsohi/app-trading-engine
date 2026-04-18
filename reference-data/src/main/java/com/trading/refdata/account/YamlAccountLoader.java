package com.trading.refdata.account;

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
import org.agrona.collections.LongHashSet;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.yaml.snakeyaml.Yaml;

/**
 * Loads account records from a YAML file.
 *
 * <p>Expected format:
 *
 * <pre>
 * accounts:
 *   - accountId: 1
 *     parentAccountId: 0
 *     accountCode: "ACME-001"
 *     acctIdSource: "Internal"
 *     accountName: "Acme Capital"
 *     accountType: "Client"
 *     baseCurrency: "USD"
 *     status: "Active"
 *     complianceStatus: "OK"
 *     capabilities: 3
 * </pre>
 *
 * <p>Validates: accountId &gt; 0, accountCode unique, accountId unique. Rejects fractional numeric
 * values. Optional fields default to: acctIdSource=Internal, status=Active, complianceStatus=OK,
 * parentAccountId=0, capabilities=0.
 *
 * <p>Not thread-safe — single-threaded startup use only.
 */
public final class YamlAccountLoader implements ReferenceDataLoader<AccountRecord> {

  private static final Logger LOG = LogManager.getLogger(YamlAccountLoader.class);
  private static final String ENTITY_TYPE = "Account";

  // Instance field — SnakeYAML Yaml is NOT thread-safe (holds internal parsing state).
  private final Yaml yaml = new Yaml();

  private final Path filePath;

  /**
   * Creates a loader that reads accounts from the given YAML file.
   *
   * @param filePath path to the YAML file; must be non-null and readable
   */
  public YamlAccountLoader(final Path filePath) {
    this.filePath = Objects.requireNonNull(filePath, "filePath");
  }

  /** {@inheritDoc} */
  @Override
  public List<AccountRecord> load() throws ReferenceDataLoadException {
    LOG.info("Loading accounts from {}", filePath);

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

    if (!root.containsKey("accounts")) {
      throw new ReferenceDataLoadException(
          ENTITY_TYPE, "missing required root key 'accounts' in " + filePath);
    }

    final var rawList = root.get("accounts");
    if (!(rawList instanceof List<?> entries)) {
      throw new ReferenceDataLoadException(ENTITY_TYPE, "'accounts' must be a list in " + filePath);
    }

    final var records = new ArrayList<AccountRecord>(entries.size());
    final var seenCodes = new HashSet<String>();
    // Agrona LongHashSet avoids autoboxing long → Long on every add()
    final var seenIds = new LongHashSet();

    for (int i = 0; i < entries.size(); i++) {
      if (!(entries.get(i) instanceof Map<?, ?> map)) {
        throw new ReferenceDataLoadException(
            ENTITY_TYPE, "entry " + i + " is not a map in " + filePath);
      }

      @SuppressWarnings("unchecked")
      final var entry = (Map<String, Object>) map;

      final var record = toRecord(entry, i);

      if (!seenIds.add(record.accountId())) {
        throw new ReferenceDataLoadException(
            ENTITY_TYPE,
            "duplicate accountId " + record.accountId() + " at entry " + i + " in " + filePath);
      }

      if (!seenCodes.add(record.accountCode())) {
        throw new ReferenceDataLoadException(
            ENTITY_TYPE,
            "duplicate accountCode '"
                + record.accountCode()
                + "' at entry "
                + i
                + " in "
                + filePath);
      }

      records.add(record);
    }

    LOG.info("Loaded {} accounts from {}", records.size(), filePath);
    return List.copyOf(records);
  }

  /** {@inheritDoc} */
  @Override
  public String sourceName() {
    return filePath.getFileName().toString();
  }

  private AccountRecord toRecord(final Map<String, Object> entry, final int index)
      throws ReferenceDataLoadException {
    try {
      long accountId = requireLong(entry, "accountId");
      return new AccountRecord(
          accountId,
          toLong(entry, "parentAccountId"),
          requireString(entry, "accountCode"),
          requireStringOrDefault(entry, "acctIdSource", "Internal"),
          requireString(entry, "accountName"),
          requireString(entry, "accountType"),
          requireString(entry, "baseCurrency"),
          requireStringOrDefault(entry, "status", "Active"),
          requireStringOrDefault(entry, "complianceStatus", "OK"),
          toLong(entry, "capabilities"));
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
   * Extracts an optional integral long value. Returns 0 if absent. Rejects fractional values.
   * Distinguishes missing key from explicit YAML {@code null} via {@link Map#containsKey}.
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

  /** Extracts a required integral long value. Throws if absent. Rejects fractional values. */
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
   * only if it has no fractional part. Rejects {@link Double} and {@link Float}.
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
    try {
      return Long.parseLong(value.toString());
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
