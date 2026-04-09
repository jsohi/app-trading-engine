package com.trading.refdata.account;

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

/** Loads account records from a YAML file. */
public final class YamlAccountLoader implements ReferenceDataLoader<AccountRecord> {

  private static final Logger LOG = LoggerFactory.getLogger(YamlAccountLoader.class);
  private static final String ENTITY_TYPE = "Account";

  private final Path filePath;

  public YamlAccountLoader(final Path filePath) {
    this.filePath = filePath;
  }

  @Override
  public List<AccountRecord> load() throws ReferenceDataLoadException {
    LOG.info("Loading accounts from {}", filePath);

    final Object parsed;
    try (final Reader reader = Files.newBufferedReader(filePath, StandardCharsets.UTF_8)) {
      parsed = new Yaml().load(reader);
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
      return List.of();
    }

    final var rawList = root.get("accounts");
    if (!(rawList instanceof List<?> entries)) {
      throw new ReferenceDataLoadException(ENTITY_TYPE, "'accounts' must be a list in " + filePath);
    }

    final List<AccountRecord> records = new ArrayList<>(entries.size());
    final Set<String> seenCodes = new HashSet<>();
    final Set<Long> seenIds = new HashSet<>();

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

  @Override
  public String sourceName() {
    return filePath.getFileName().toString();
  }

  private AccountRecord toRecord(final Map<String, Object> entry, final int index)
      throws ReferenceDataLoadException {
    try {
      return new AccountRecord(
          requireLong(entry, "accountId"),
          toLong(entry, "parentAccountId"),
          requireString(entry, "accountCode"),
          stringOrDefault(entry, "acctIdSource", "Internal"),
          requireString(entry, "accountName"),
          requireString(entry, "accountType"),
          requireString(entry, "baseCurrency"),
          stringOrDefault(entry, "status", "Active"),
          stringOrDefault(entry, "complianceStatus", "OK"),
          toLong(entry, "capabilities"));
    } catch (final ReferenceDataLoadException e) {
      throw e;
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
          "Account", "field '" + key + "' is not a valid number: '" + value + "'", e);
    }
  }

  private static long requireLong(final Map<String, Object> map, final String key)
      throws ReferenceDataLoadException {
    final var value = map.get(key);
    if (value == null) {
      throw new ReferenceDataLoadException("Account", "missing required field '" + key + "'");
    }
    if (value instanceof Number n) {
      return n.longValue();
    }
    try {
      return Long.parseLong(value.toString());
    } catch (final NumberFormatException e) {
      throw new ReferenceDataLoadException(
          "Account", "field '" + key + "' is not a valid number: '" + value + "'", e);
    }
  }

  private static String requireString(final Map<String, Object> map, final String key)
      throws ReferenceDataLoadException {
    final var value = map.get(key);
    if (value == null) {
      throw new ReferenceDataLoadException("Account", "missing required field '" + key + "'");
    }
    final var str = value.toString();
    if (str.isBlank()) {
      throw new ReferenceDataLoadException("Account", "field '" + key + "' must not be blank");
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
