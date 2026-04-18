package com.trading.engine.e2e;

import com.trading.engine.fix.OrdType;
import com.trading.engine.fix.Side;
import com.trading.engine.fix.TimeInForce;
import com.trading.engine.messages.FixedPointScale;
import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;
import uk.co.real_logic.artio.fields.ReadOnlyDecimalFloat;

/**
 * Loads E2E test scenarios from a YAML file. Follows the same SnakeYAML parsing pattern as {@link
 * com.trading.refdata.account.YamlAccountLoader} — parse root map, validate root key, iterate
 * entries — but uses a static factory (not instance-based) since E2E scenarios are test
 * configuration, not production reference data.
 *
 * <p><b>Security:</b> Uses {@link SafeConstructor} to restrict deserialization to safe types
 * (strings, numbers, lists, maps). This prevents arbitrary Java object instantiation via YAML tags
 * (CVE-2022-1471).
 *
 * <p><b>Not thread-safe</b> — single-threaded startup use only.
 *
 * @see NosScenario
 */
public final class E2EScenarioLoader {

  private static final Logger LOG = LogManager.getLogger(E2EScenarioLoader.class);
  private static final String ROOT_KEY = "scenarios";

  /** All valid YAML keys for a scenario entry — used for typo detection. */
  private static final Set<String> KNOWN_KEYS =
      Set.of(
          "name",
          "type",
          "expectedOutcome",
          "accountCode",
          "symbol",
          "currency",
          "side",
          "ordType",
          "timeInForce",
          "priceValue",
          "priceScale",
          "qtyValue",
          "qtyScale",
          "expectedRejectText");

  private E2EScenarioLoader() {}

  /**
   * Loads and validates all scenarios from the given YAML file.
   *
   * <p>Follows the same YAML root-parsing boilerplate as {@code YamlAccountLoader}:
   *
   * <ol>
   *   <li>{@code try (Reader reader = Files.newBufferedReader(path, UTF_8))}
   *   <li>Root null check (empty file → throw)
   *   <li>{@code instanceof Map<?, ?>} check on root object
   *   <li>{@code @SuppressWarnings("unchecked")} cast to {@code Map<String, Object>}
   *   <li>Root key ("scenarios") presence check
   *   <li>{@code instanceof List<?>} check on scenarios list
   *   <li>Per-entry {@code instanceof Map<?, ?>} check with index
   * </ol>
   *
   * <p><b>YAML type conversion:</b> SnakeYAML parses small integers as {@code Integer}, not {@code
   * Long}. The loader uses {@code requireIntegralLong()} (same as {@code YamlAccountLoader}) to
   * handle {@code Integer → long} promotion.
   *
   * @param path absolute path to e2e-scenarios.yaml
   * @return unmodifiable list of validated scenarios (at least one)
   * @throws E2EScenarioLoadException if the file contains zero scenarios, the YAML is malformed, or
   *     required fields are missing
   */
  public static List<NosScenario> load(final Path path) {
    LOG.info("Loading E2E scenarios from {}", path);

    final Object parsed;
    final var yaml = new Yaml(new SafeConstructor(new LoaderOptions()));
    try (final Reader reader = Files.newBufferedReader(path, StandardCharsets.UTF_8)) {
      parsed = yaml.load(reader);
    } catch (final IOException e) {
      throw new E2EScenarioLoadException("cannot read " + path, e);
    } catch (final Exception e) {
      throw new E2EScenarioLoadException("malformed YAML in " + path, e);
    }

    if (parsed == null) {
      throw new E2EScenarioLoadException("empty YAML file: " + path);
    }
    if (!(parsed instanceof Map<?, ?> rootMap)) {
      throw new E2EScenarioLoadException(
          "YAML root must be a map, got " + parsed.getClass().getSimpleName() + " in " + path);
    }

    @SuppressWarnings("unchecked")
    final var root = (Map<String, Object>) rootMap;

    if (!root.containsKey(ROOT_KEY)) {
      throw new E2EScenarioLoadException("missing required root key '" + ROOT_KEY + "' in " + path);
    }

    final var rawList = root.get(ROOT_KEY);
    if (!(rawList instanceof List<?> entries)) {
      throw new E2EScenarioLoadException("'" + ROOT_KEY + "' must be a list in " + path);
    }

    final var scenarios = new ArrayList<NosScenario>(entries.size());

    for (int i = 0; i < entries.size(); i++) {
      if (!(entries.get(i) instanceof Map<?, ?> map)) {
        throw new E2EScenarioLoadException("entry " + i + " is not a map in " + path);
      }

      @SuppressWarnings("unchecked")
      final var entry = (Map<String, Object>) map;

      scenarios.add(toNosScenario(entry, i, path));
    }

    if (scenarios.isEmpty()) {
      throw new E2EScenarioLoadException(
          "e2e-scenarios.yaml contains zero scenarios — at least one required");
    }

    LOG.info("Loaded {} E2E scenarios from {}", scenarios.size(), path);
    return List.copyOf(scenarios);
  }

  // ===========================================================================
  // Per-entry parsing
  // ===========================================================================

  private static NosScenario toNosScenario(
      final Map<String, Object> entry, final int index, final Path path) {

    final var name = requireString(entry, "name");

    // Warn on unknown YAML keys (catches typos like "priceValu")
    for (final var key : entry.keySet()) {
      if (!KNOWN_KEYS.contains(key)) {
        LOG.warn(
            "Unknown field '{}' in scenario '{}' (entry {}) — possible typo?", key, name, index);
      }
    }

    final var type = toScenarioType(requireString(entry, "type"));
    final var expectedOutcome = toExpectedOutcome(requireString(entry, "expectedOutcome"));
    final var accountCode = requireString(entry, "accountCode");
    final var symbol = requireString(entry, "symbol");
    final var currency = requireString(entry, "currency");
    final var side = toSide(requireString(entry, "side"));
    final var ordType = toOrdType(requireString(entry, "ordType"));
    final var timeInForce = toTimeInForce(requireString(entry, "timeInForce"));

    // Price — optional for Market orders
    final boolean hasPrice = entry.containsKey("priceValue");
    final long priceValue;
    final int priceScale;
    final long priceFixedPoint;
    if (hasPrice) {
      priceValue = requireIntegralLong(entry, "priceValue");
      priceScale = requireInt(entry, "priceScale");
      priceFixedPoint = FixedPointScale.toFixedPoint(priceValue, priceScale);
    } else {
      priceValue = 0;
      priceScale = 0;
      priceFixedPoint = FixedPointScale.PRICE_NOT_AVAILABLE;
    }

    // Quantity — always required
    final long qtyValue = requireIntegralLong(entry, "qtyValue");
    final int qtyScale = requireInt(entry, "qtyScale");
    final long qtyFixedPoint = FixedPointScale.toFixedPoint(qtyValue, qtyScale);

    // Reject text — required for REJECTED, null otherwise
    final String expectedRejectText;
    if (entry.containsKey("expectedRejectText")) {
      expectedRejectText = requireString(entry, "expectedRejectText");
    } else {
      expectedRejectText = null;
    }

    return new NosScenario(
        name,
        type,
        expectedOutcome,
        accountCode,
        symbol,
        currency,
        side,
        ordType,
        timeInForce,
        priceValue,
        priceScale,
        hasPrice,
        qtyValue,
        qtyScale,
        priceFixedPoint,
        qtyFixedPoint,
        expectedRejectText);
  }

  // ===========================================================================
  // Enum mapping — explicit switch, case-sensitive, matching YAML values exactly
  // ===========================================================================

  private static NosScenario.ScenarioType toScenarioType(final String value) {
    return switch (value) {
      case "NewOrderSingle" -> NosScenario.ScenarioType.NEW_ORDER_SINGLE;
      default -> throw new E2EScenarioLoadException("unknown scenario type: '" + value + "'");
    };
  }

  private static NosScenario.ExpectedOutcome toExpectedOutcome(final String value) {
    return switch (value) {
      case "New" -> NosScenario.ExpectedOutcome.NEW;
      case "Rejected" -> NosScenario.ExpectedOutcome.REJECTED;
      default -> throw new E2EScenarioLoadException("unknown expectedOutcome: '" + value + "'");
    };
  }

  private static Side toSide(final String value) {
    return switch (value) {
      case "Buy" -> Side.BUY; // FIX wire: '1'
      case "Sell" -> Side.SELL; // FIX wire: '2'
      default -> throw new E2EScenarioLoadException("unknown side: '" + value + "'");
    };
  }

  private static OrdType toOrdType(final String value) {
    return switch (value) {
      case "Limit" -> OrdType.LIMIT; // FIX wire: '2'
      case "Market" -> OrdType.MARKET; // FIX wire: '1'
      default -> throw new E2EScenarioLoadException("unknown ordType: '" + value + "'");
    };
  }

  private static TimeInForce toTimeInForce(final String value) {
    return switch (value) {
      case "Day" -> TimeInForce.DAY; // FIX wire: '0'
      case "IOC" -> TimeInForce.IMMEDIATE_OR_CANCEL; // FIX wire: '3'
      case "GTC" -> TimeInForce.GOOD_TILL_CANCEL; // FIX wire: '1'
      default -> throw new E2EScenarioLoadException("unknown timeInForce: '" + value + "'");
    };
  }

  // ===========================================================================
  // YAML field extraction helpers — same pattern as YamlAccountLoader, with
  // E2EScenarioLoadException instead of ReferenceDataLoadException
  // ===========================================================================

  private static String requireString(final Map<String, Object> map, final String key) {
    final var value = map.get(key);
    if (value == null) {
      throw new E2EScenarioLoadException("missing required field '" + key + "'");
    }
    if (!(value instanceof String s)) {
      throw new E2EScenarioLoadException(
          "field '" + key + "' must be a string, got: " + value.getClass().getSimpleName());
    }
    return s;
  }

  /**
   * Extracts a {@code long} from the map, handling SnakeYAML's Integer/Long parsing — small values
   * (within int range) arrive as {@link Integer}, large values as {@link Long}.
   */
  private static long requireIntegralLong(final Map<String, Object> map, final String key) {
    final var value = map.get(key);
    if (value == null) {
      throw new E2EScenarioLoadException("missing required field '" + key + "'");
    }
    if (value instanceof Long l) {
      return l;
    }
    if (value instanceof Integer i) {
      return i.longValue();
    }
    throw new E2EScenarioLoadException(
        "field '" + key + "' must be a number, got: " + value.getClass().getSimpleName());
  }

  private static int requireInt(final Map<String, Object> map, final String key) {
    final var value = map.get(key);
    if (value == null) {
      throw new E2EScenarioLoadException("missing required field '" + key + "'");
    }
    if (value instanceof Integer i) {
      return i;
    }
    throw new E2EScenarioLoadException(
        "field '" + key + "' must be an integer, got: " + value.getClass().getSimpleName());
  }

  /**
   * Converts an Artio DecimalFloat (from ER decoder) to the engine's int64 fixed-point form.
   *
   * <p>The gateway's {@code SbeToFixTranslator.toDecimalFloat()} always writes with {@code
   * scale=8}, so the ER's DecimalFloat will have {@code shift = 8 - 8 = 0} and the multiplication
   * is a no-op. This method handles arbitrary scales for robustness (e.g., if Artio normalizes
   * trailing zeros).
   *
   * @param df the Artio DecimalFloat from the ER decoder
   * @return int64 fixed-point representation
   * @throws IllegalArgumentException if {@code df.scale()} is outside {@code [0, 8]}
   */
  static long toFixedPoint(final ReadOnlyDecimalFloat df) {
    return FixedPointScale.toFixedPoint(df.value(), df.scale());
  }
}
