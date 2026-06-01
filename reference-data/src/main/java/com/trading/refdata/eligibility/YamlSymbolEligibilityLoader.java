package com.trading.refdata.eligibility;

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
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.yaml.snakeyaml.Yaml;

/**
 * Loads symbol-eligibility records from a YAML file (APP-62 §G).
 *
 * <p>Expected format:
 *
 * <pre>
 * symbolEligibilities:
 *   - symbol: EURUSD
 *     tradingAllowed: true
 *     shortSaleAllowed: true
 *     priceDeviationBpsOverride: 0
 * </pre>
 *
 * <p>Validates: {@code symbol} non-blank, ASCII, &le; 8 characters (matches SBE {@code Symbol}
 * fixed-length type); {@code priceDeviationBpsOverride} &ge; 0; rejects duplicate symbols (one
 * record per symbol). {@code tradingAllowed} and {@code shortSaleAllowed} default to {@code false}
 * if omitted — the §G policy is fail-closed, so a missing flag must be treated as restricted.
 * {@code priceDeviationBpsOverride} defaults to {@code 0} (no per-symbol override; the per-account
 * knob applies).
 *
 * <p>Each emitted {@link SymbolEligibilityRecord} carries an {@code asOfTimestamp} of {@code 0L}
 * (sentinel meaning "stamped at cluster ingest"). Per CLAUDE.md Rule 9, the loader runs outside the
 * cluster but is forbidden from using direct wall-clock; the authoritative timestamp for both the
 * in-cluster {@code SymbolEligibilityState.asOfTimestamp} and the wire {@code transactTime} on the
 * emitted {@code SymbolEligibilityLoadedEvent} is taken from the cluster's deterministic timestamp
 * in {@code onSessionMessage}. The encoder ({@link SymbolEligibilityCommandEncoder}) likewise
 * hardcodes the batch envelope {@code transactTime} to {@code 0L}, so the record's field flows
 * nowhere on the wire — it is preserved on the POJO only for symmetry with the SBE codec API.
 *
 * <p><b>Threading.</b> Not thread-safe — single-threaded startup use only. SnakeYAML's {@link Yaml}
 * holds internal parsing state and must not be shared across threads.
 *
 * <p><b>Allocation.</b> Acceptable per-load (startup path, not hot path). The returned list is
 * defensively copied via {@link List#copyOf}.
 *
 * @see com.trading.refdata.risklimit.YamlRiskLimitLoader — sister loader this class mirrors
 *     line-for-line on the YAML parsing and validation contract.
 */
public final class YamlSymbolEligibilityLoader
    implements ReferenceDataLoader<SymbolEligibilityRecord> {

  private static final Logger LOG = LogManager.getLogger(YamlSymbolEligibilityLoader.class);
  private static final String ENTITY_TYPE = "SymbolEligibility";

  // Instance field — SnakeYAML Yaml is NOT thread-safe (holds internal parsing state).
  private final Yaml yaml = new Yaml();

  private final Path filePath;

  /**
   * Creates a loader that reads symbol-eligibility records from the given YAML file.
   *
   * @param filePath path to the YAML file; must be non-null and readable
   * @throws NullPointerException if {@code filePath} is null
   */
  public YamlSymbolEligibilityLoader(final Path filePath) {
    this.filePath = Objects.requireNonNull(filePath, "filePath");
  }

  /**
   * Loads all symbol-eligibility records from the configured YAML file.
   *
   * @return immutable list of validated records (empty if the file declares no records)
   * @throws ReferenceDataLoadException if the file cannot be read, parsed, or contains any invalid
   *     entry (missing required field, duplicate symbol, fractional numeric, etc.)
   */
  @Override
  public List<SymbolEligibilityRecord> load() throws ReferenceDataLoadException {
    LOG.info("Loading symbol eligibilities from {}", filePath);

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

    if (!root.containsKey("symbolEligibilities")) {
      throw new ReferenceDataLoadException(
          ENTITY_TYPE, "missing required root key 'symbolEligibilities' in " + filePath);
    }

    final var rawList = root.get("symbolEligibilities");
    if (!(rawList instanceof List<?> entries)) {
      throw new ReferenceDataLoadException(
          ENTITY_TYPE, "'symbolEligibilities' must be a list in " + filePath);
    }

    final List<SymbolEligibilityRecord> records = new ArrayList<>(entries.size());
    // Symbols are short ASCII strings — interning via the small initial capacity HashSet is cheap
    // and the only contended path is single-threaded startup, so allocation cost is irrelevant.
    final Set<String> seenSymbols = new HashSet<>(entries.size());
    // CLAUDE.md Rule 9: no direct wall-clock outside the cluster. The loader emits 0L as the
    // record-level asOfTimestamp; the cluster's LoadSymbolEligibilityBatchHandler restamps both
    // SymbolEligibilityState.asOfTimestamp and the wire transactTime on the emitted
    // SymbolEligibilityLoadedEvent with its deterministic cluster timestamp on ingest. The encoder
    // mirrors this contract by hardcoding the batch transactTime to 0L.
    long asOfTimestamp = 0L;

    for (int i = 0; i < entries.size(); i++) {
      if (!(entries.get(i) instanceof Map<?, ?> map)) {
        throw new ReferenceDataLoadException(
            ENTITY_TYPE, "entry " + i + " is not a map in " + filePath);
      }

      @SuppressWarnings("unchecked")
      final var entry = (Map<String, Object>) map;

      final var record = toRecord(entry, i, asOfTimestamp);

      if (!seenSymbols.add(record.symbol())) {
        throw new ReferenceDataLoadException(
            ENTITY_TYPE,
            "duplicate symbol '" + record.symbol() + "' at entry " + i + " in " + filePath);
      }

      records.add(record);
    }

    LOG.info("Loaded {} symbol eligibilities from {}", records.size(), filePath);
    return List.copyOf(records);
  }

  /** {@inheritDoc} */
  @Override
  public String sourceName() {
    return filePath.getFileName().toString();
  }

  private SymbolEligibilityRecord toRecord(
      final Map<String, Object> entry, int index, long asOfTimestamp)
      throws ReferenceDataLoadException {
    try {
      final var symbol = requireNonBlankString(entry, "symbol");
      // Primitives intentionally bare (no `final`) per CLAUDE.md memory
      // feedback_final_primitives_autoboxing.md.
      boolean tradingAllowed = toBoolean(entry, "tradingAllowed");
      boolean shortSaleAllowed = toBoolean(entry, "shortSaleAllowed");
      long priceDeviationBpsOverride = toLong(entry, "priceDeviationBpsOverride");

      // Record compact constructor validates remaining constraints (symbol length, non-negative).
      return new SymbolEligibilityRecord(
          symbol, tradingAllowed, shortSaleAllowed, priceDeviationBpsOverride, asOfTimestamp);
    } catch (final ReferenceDataLoadException e) {
      throw e;
    } catch (final IllegalArgumentException e) {
      // Re-wrap compact constructor validation failures — preserve cause for diagnostics.
      throw new ReferenceDataLoadException(
          ENTITY_TYPE, "invalid entry " + index + " in " + filePath + ": " + e.getMessage(), e);
    } catch (final RuntimeException e) {
      // Narrowed from Exception to RuntimeException so checked exceptions surface with their
      // original type for diagnosability rather than being silently wrapped.
      throw new ReferenceDataLoadException(
          ENTITY_TYPE, "invalid entry " + index + " in " + filePath, e);
    }
  }

  /**
   * Extracts an integral long value from the map. Returns 0 if the key is absent. Rejects
   * fractional {@link Double}/{@link Float} values to prevent silent truncation.
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
   * Converts a YAML-parsed value to a long, rejecting fractional numbers to prevent silent
   * truncation. Mirrors {@link com.trading.refdata.risklimit.YamlRiskLimitLoader}'s contract:
   * accepts {@link Byte}, {@link Short}, {@link Integer}, {@link Long}, {@link BigInteger}, and
   * exact-integer {@link BigDecimal}; rejects {@link Double}/{@link Float} to prevent silent
   * truncation of financial data.
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

  /**
   * Extracts a boolean flag from the map. Returns {@code false} if the key is absent — the §G
   * policy is fail-closed, so a missing flag is treated as restricted. Accepts {@link Boolean}
   * directly and case-insensitive {@code "true"}/{@code "false"} strings.
   */
  private static boolean toBoolean(final Map<String, Object> map, final String key)
      throws ReferenceDataLoadException {
    if (!map.containsKey(key)) {
      return false;
    }
    final var value = map.get(key);
    if (value == null) {
      throw new ReferenceDataLoadException(
          ENTITY_TYPE,
          "field '" + key + "' must not be null (omit the key for the default of false)");
    }
    if (value instanceof Boolean b) {
      return b;
    }
    if (value instanceof String s) {
      // Locale.ROOT — avoid Turkish-locale "I"/"i" surprises in case the JVM default locale drifts.
      final var lower = s.toLowerCase(Locale.ROOT);
      if (lower.equals("true")) {
        return true;
      }
      if (lower.equals("false")) {
        return false;
      }
    }
    throw new ReferenceDataLoadException(
        ENTITY_TYPE,
        "field '"
            + key
            + "' must be a boolean (true/false), got "
            + value.getClass().getSimpleName()
            + ": '"
            + value
            + "'");
  }

  /** Returns a non-blank string value. Throws if absent, null, blank, or non-string. */
  private static String requireNonBlankString(final Map<String, Object> map, final String key)
      throws ReferenceDataLoadException {
    if (!map.containsKey(key)) {
      throw new ReferenceDataLoadException(ENTITY_TYPE, "missing required field '" + key + "'");
    }
    final var value = map.get(key);
    if (value == null) {
      throw new ReferenceDataLoadException(ENTITY_TYPE, "field '" + key + "' must not be null");
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
