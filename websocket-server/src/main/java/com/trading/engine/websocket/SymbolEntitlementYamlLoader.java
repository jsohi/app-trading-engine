package com.trading.engine.websocket;

import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.yaml.snakeyaml.Yaml;

/**
 * Loads the per-symbol → permitted-account-codes mapping from a YAML file and constructs a {@link
 * SymbolEntitlementMap}. Mirrors the {@code YamlAccountLoader} pattern in the reference-data module
 * — startup-only, single-threaded, fails loud on malformed input so a mis-configured launcher
 * cannot silently fall back to "deny everything" (which would be operationally indistinguishable
 * from a working but mis-mapped tenant).
 *
 * <p><b>Expected YAML format.</b>
 *
 * <pre>{@code
 * symbols:
 *   EURUSD: [ACME, GLOBEX]
 *   GBPUSD: [ACME]
 *   USDJPY: [ACME]
 * }</pre>
 *
 * <p>Validations: top-level {@code symbols:} key present and non-empty; each entry's symbol key is
 * a non-null, non-blank string of ≤ 8 ASCII characters (the SBE wire constraint); each entry's
 * account list is non-null and contains at least one non-blank account code. Any violation throws
 * {@link IllegalArgumentException} with the offending entry quoted.
 *
 * <p><b>Threading model.</b> Not thread-safe — startup use only. The constructed {@link
 * SymbolEntitlementMap} IS thread-safe (immutable after construction).
 *
 * <p><b>Allocation.</b> Cold path — launcher startup only. {@link Yaml} parsing allocates per
 * invocation; the resulting {@link SymbolEntitlementMap} lives for the lifetime of the
 * websocket-server process.
 */
public final class SymbolEntitlementYamlLoader {

  private static final Logger LOG = LogManager.getLogger(SymbolEntitlementYamlLoader.class);

  /** Top-level YAML key — must match the documented schema above. */
  private static final String TOP_LEVEL_KEY = "symbols";

  /** Maximum symbol length (SBE wire encoding is char[8] ASCII). */
  private static final int MAX_SYMBOL_LENGTH = 8;

  // Instance field — SnakeYAML Yaml is NOT thread-safe.
  //
  // SECURITY: the no-arg Yaml() constructor uses SafeConstructor since SnakeYAML 2.0 (CVE-2022-
  // 1471 remediation). The project pins SnakeYAML at 2.6 via gradle/libs.versions.toml — verified
  // by the OWASP dependencyCheckAnalyze CI gate. Do NOT downgrade SnakeYAML below 2.0 here
  // without re-introducing this risk; if a future swap is needed, prefer the explicit form
  // `new Yaml(new SafeConstructor(new LoaderOptions()))` to make the intent audit-proof.
  // Agent B review F-5.
  private final Yaml yaml = new Yaml();

  private final Path filePath;

  /**
   * @param filePath absolute path to the {@code symbols.yaml} file; must exist + be readable
   */
  public SymbolEntitlementYamlLoader(final Path filePath) {
    this.filePath = Objects.requireNonNull(filePath, "filePath");
  }

  /**
   * Parse the YAML file and build a {@link SymbolEntitlementMap}.
   *
   * @return a populated, immutable {@link SymbolEntitlementMap}
   * @throws IOException if the YAML file is missing or unreadable
   * @throws IllegalArgumentException if the file content fails the documented format / validation
   */
  public SymbolEntitlementMap load() throws IOException {
    LOG.info("Loading symbol entitlements from {}", filePath);
    final Map<String, List<String>> raw = parseYaml();
    final var map = new SymbolEntitlementMap(raw);
    LOG.info(
        "Loaded {} symbol(s) covering {} distinct account(s)",
        map.symbolCount(),
        map.accountCount());
    return map;
  }

  private Map<String, List<String>> parseYaml() throws IOException {
    try (final Reader reader = Files.newBufferedReader(filePath, StandardCharsets.UTF_8)) {
      final Object root = yaml.load(reader);
      if (!(root instanceof Map<?, ?> rootMap)) {
        throw new IllegalArgumentException(
            "symbols.yaml root must be a YAML mapping with top-level '" + TOP_LEVEL_KEY + "' key");
      }
      final Object symbolsObj = rootMap.get(TOP_LEVEL_KEY);
      if (!(symbolsObj instanceof Map<?, ?> symbolsMap)) {
        throw new IllegalArgumentException(
            "symbols.yaml must have a '" + TOP_LEVEL_KEY + ":' top-level mapping");
      }
      if (symbolsMap.isEmpty()) {
        throw new IllegalArgumentException(
            "symbols.yaml '" + TOP_LEVEL_KEY + "' mapping is empty — at least one symbol required");
      }

      final Map<String, List<String>> result = new LinkedHashMap<>(symbolsMap.size() * 2);
      for (final Map.Entry<?, ?> entry : symbolsMap.entrySet()) {
        final String symbol = validateSymbolKey(entry.getKey());
        final List<String> accounts = validateAccountList(symbol, entry.getValue());
        result.put(symbol, accounts);
      }
      return result;
    }
  }

  private static String validateSymbolKey(final Object key) {
    if (!(key instanceof String s) || s.isBlank()) {
      throw new IllegalArgumentException(
          "symbols.yaml: symbol key must be a non-blank string, got: " + key);
    }
    final String trimmed = s.trim();
    if (trimmed.length() > MAX_SYMBOL_LENGTH) {
      throw new IllegalArgumentException(
          "symbols.yaml: symbol '"
              + trimmed
              + "' exceeds the SBE wire limit of "
              + MAX_SYMBOL_LENGTH
              + " characters");
    }
    for (int i = 0; i < trimmed.length(); i++) {
      final char c = trimmed.charAt(i);
      if (c < 0x20 || c > 0x7E) {
        throw new IllegalArgumentException(
            "symbols.yaml: symbol '"
                + trimmed
                + "' contains a non-printable / non-ASCII character");
      }
    }
    return trimmed;
  }

  private static List<String> validateAccountList(final String symbol, final Object accountsObj) {
    if (!(accountsObj instanceof List<?> list)) {
      throw new IllegalArgumentException(
          "symbols.yaml: '" + symbol + "' value must be a YAML sequence of account codes");
    }
    if (list.isEmpty()) {
      throw new IllegalArgumentException(
          "symbols.yaml: '" + symbol + "' must have at least one permitted account");
    }
    final List<String> accounts = new ArrayList<>(list.size());
    for (final Object item : list) {
      if (!(item instanceof String s) || s.isBlank()) {
        throw new IllegalArgumentException(
            "symbols.yaml: '" + symbol + "' contains a non-string or blank account code: " + item);
      }
      accounts.add(s.trim());
    }
    return accounts;
  }
}
