package com.trading.engine.e2e;

import com.trading.engine.fix.OrdType;
import com.trading.engine.fix.Side;
import com.trading.engine.fix.TimeInForce;
import com.trading.engine.messages.FixedPointScale;
import java.util.Locale;
import java.util.Optional;

/**
 * Pure POJO + parser for {@link E2EFixTestClient} command-line arguments.
 *
 * <p>Hermetic by construction — has no dependency on Artio, Aeron, or any FIX runtime, so the
 * parser is unit-testable in isolation without booting an embedded media driver. Mirrors the "args
 * object" idiom used by exchange-core's standalone test harnesses.
 *
 * <p><b>Two run modes</b> are supported:
 *
 * <ul>
 *   <li>{@link RunMode#YAML} — the original data-driven mode. Triggered by {@code --data-dir
 *       <path>} with no {@code --scenario} flag. Loads scenarios from {@code
 *       <data-dir>/e2e-scenarios.yaml}.
 *   <li>{@link RunMode#CLI} — single-order or matched-pair mode added for the full-stack-e2e
 *       Playwright suite (plan §8 tests 3/4/7). Triggered by {@code --scenario <single|match>}.
 *       Required: {@code --clord-id} (deterministic, used by the spec to grep the resulting row).
 *       Optional: {@code --symbol}, {@code --side}, {@code --qty}, {@code --price}, {@code
 *       --account}, {@code --currency} — sane defaults applied when omitted.
 * </ul>
 *
 * <p><b>Threading:</b> Immutable record — safe to share across threads.
 *
 * <p><b>Allocation:</b> One allocation per parse() call — this is a CLI startup path, not a hot
 * path.
 *
 * <p><b>Exit codes:</b> {@link #parse(String[])} throws {@link ArgsParseException} on malformed
 * input; the caller maps that to {@link E2EFixTestClient#EXIT_EXCEPTION}.
 */
public record E2EFixTestClientArgs(
    String host,
    int port,
    String senderCompId,
    String targetCompId,
    Optional<String> dataDir,
    RunMode runMode,
    Optional<CliScenarioSpec> cliScenario) {

  /** Run-mode discriminator. */
  public enum RunMode {
    /** Load scenarios from {@code <data-dir>/e2e-scenarios.yaml} and run each in sequence. */
    YAML,
    /**
     * Run a single CLI-specified scenario ({@code --scenario single}) or a matched pair ({@code
     * --scenario match}) intended to produce a fill.
     */
    CLI
  }

  /** Compact constructor — invariant validation. */
  public E2EFixTestClientArgs {
    if (host == null || host.isBlank()) {
      throw new IllegalArgumentException("host must not be blank");
    }
    if (port < 1 || port > 65535) {
      throw new IllegalArgumentException("port must be 1..65535, got " + port);
    }
    if (senderCompId == null || senderCompId.isBlank()) {
      throw new IllegalArgumentException("senderCompId must not be blank");
    }
    if (targetCompId == null || targetCompId.isBlank()) {
      throw new IllegalArgumentException("targetCompId must not be blank");
    }
    if (runMode == RunMode.YAML && dataDir.isEmpty()) {
      throw new IllegalArgumentException("YAML mode requires --data-dir");
    }
    if (runMode == RunMode.CLI && cliScenario.isEmpty()) {
      throw new IllegalArgumentException("CLI mode requires a parsed CliScenarioSpec");
    }
  }

  /**
   * Parses CLI arguments. Pair-wise parser ({@code --flag value}) for compatibility with the
   * pre-existing E2EFixTestClient.main loop.
   *
   * <p>Defaults (applied when the corresponding flag is omitted):
   *
   * <ul>
   *   <li>{@code host=localhost}, {@code port=19880}, {@code senderCompId=CLIENT1}, {@code
   *       targetCompId=TRADING}
   *   <li>For {@code --scenario single}: symbol=EURUSD, side=Buy, qty=1.0, price=1.05000000,
   *       account=ACME, currency=USD, ordType=Limit, tif=Day
   *   <li>For {@code --scenario match}: same defaults; the harness emits two opposing orders
   *       sharing a ClOrdID stem (cf. {@link CliScenarioSpec#matchClOrdIds()}).
   * </ul>
   *
   * @param args raw process args
   * @return parsed args
   * @throws ArgsParseException on unknown flag, missing required value, or malformed value
   */
  public static E2EFixTestClientArgs parse(final String[] args) {
    String host = "localhost";
    int port = 19880;
    String senderCompId = "CLIENT1";
    String targetCompId = "TRADING";
    String dataDir = null;
    String scenario = null;
    String clOrdId = null;
    String symbol = "EURUSD";
    String side = "buy";
    String currency = "USD";
    String account = "ACME";
    String ordType = "limit";
    String tif = "day";
    String qty = "1.0";
    String price = "1.05000000";

    int i = 0;
    while (i < args.length) {
      final var flag = args[i];
      // Boolean flags would be parsed here; this client has none.
      if (i + 1 >= args.length) {
        throw new ArgsParseException("flag '" + flag + "' is missing its value");
      }
      final var val = args[i + 1];
      switch (flag) {
        case "--host" -> host = val;
        case "--port" -> port = parseIntFlag(flag, val);
        case "--sender-comp-id" -> senderCompId = val;
        case "--target-comp-id" -> targetCompId = val;
        case "--data-dir" -> dataDir = val;
        case "--scenario" -> scenario = val;
        case "--clord-id" -> clOrdId = val;
        case "--symbol" -> symbol = val;
        case "--side" -> side = val;
        case "--qty" -> qty = val;
        case "--price" -> price = val;
        case "--account" -> account = val;
        case "--currency" -> currency = val;
        case "--ord-type" -> ordType = val;
        case "--tif" -> tif = val;
        default -> throw new ArgsParseException("unknown flag: " + flag);
      }
      i += 2;
    }

    if (scenario == null && dataDir == null) {
      throw new ArgsParseException(
          "one of --scenario <single|match> or --data-dir <path> required");
    }
    if (scenario != null && dataDir != null) {
      throw new ArgsParseException("--scenario and --data-dir are mutually exclusive");
    }

    if (scenario != null) {
      if (clOrdId == null || clOrdId.isBlank()) {
        throw new ArgsParseException("--clord-id is required in CLI mode");
      }
      final var spec =
          CliScenarioSpec.of(
              scenario, clOrdId, symbol, side, ordType, tif, qty, price, account, currency);
      return new E2EFixTestClientArgs(
          host, port, senderCompId, targetCompId, Optional.empty(), RunMode.CLI, Optional.of(spec));
    }
    return new E2EFixTestClientArgs(
        host,
        port,
        senderCompId,
        targetCompId,
        Optional.of(dataDir),
        RunMode.YAML,
        Optional.empty());
  }

  private static int parseIntFlag(final String flag, final String val) {
    try {
      return Integer.parseInt(val);
    } catch (final NumberFormatException e) {
      throw new ArgsParseException(flag + " must be an integer, got '" + val + "'");
    }
  }

  /** Fatal CLI-parse error — caller maps to {@code EXIT_EXCEPTION} (4). */
  public static final class ArgsParseException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public ArgsParseException(final String message) {
      super(message);
    }
  }

  /**
   * CLI scenario specification — a typed view of the {@code --scenario}, {@code --clord-id} and
   * order-detail flags. Constructed only in {@link RunMode#CLI}.
   *
   * <p>The two scenario kinds:
   *
   * <ul>
   *   <li>{@link ScenarioKind#SINGLE} — one NewOrderSingle with the given fields.
   *   <li>{@link ScenarioKind#MATCH} — two opposing NOS messages with deterministic {@code
   *       <clOrdId>-buy} and {@code <clOrdId>-sell} suffixes (so Playwright spec 4 can grep the
   *       resulting fill in PositionsBlotter).
   * </ul>
   *
   * <p>{@code priceFixedPoint} / {@code qtyFixedPoint} are pre-computed at parse time against
   * {@link FixedPointScale#PRICE_SCALE} (the project's universal fixed-point scale used for both
   * prices and quantities) so the runtime hot path is allocation-free.
   */
  public record CliScenarioSpec(
      ScenarioKind kind,
      String clOrdId,
      String symbol,
      Side side,
      OrdType ordType,
      TimeInForce timeInForce,
      String account,
      String currency,
      long qtyFixedPoint,
      long priceFixedPoint,
      boolean hasPrice) {

    /** Scenario kind. */
    public enum ScenarioKind {
      /** One NOS with the supplied fields. */
      SINGLE,
      /** Two opposing NOS (buy + sell) sharing a ClOrdID stem to produce a fill. */
      MATCH
    }

    public CliScenarioSpec {
      if (clOrdId == null || clOrdId.isBlank()) {
        throw new IllegalArgumentException("clOrdId must not be blank");
      }
      if (symbol == null || symbol.isBlank()) {
        throw new IllegalArgumentException("symbol must not be blank");
      }
      if (account == null || account.isBlank()) {
        throw new IllegalArgumentException("account must not be blank");
      }
      if (currency == null || currency.isBlank()) {
        throw new IllegalArgumentException("currency must not be blank");
      }
      if (qtyFixedPoint <= 0) {
        throw new IllegalArgumentException("qtyFixedPoint must be > 0, got " + qtyFixedPoint);
      }
      if (hasPrice && priceFixedPoint <= 0) {
        throw new IllegalArgumentException(
            "priceFixedPoint must be > 0 when hasPrice, got " + priceFixedPoint);
      }
      if (ordType == OrdType.LIMIT && !hasPrice) {
        throw new IllegalArgumentException("Limit order requires --price");
      }
    }

    /**
     * Factory that resolves the user-facing string flags into the typed enums + fixed-point fields.
     * Throws {@link ArgsParseException} on malformed values so the CLI surface produces one
     * consistent error class.
     */
    public static CliScenarioSpec of(
        final String scenarioFlag,
        final String clOrdId,
        final String symbol,
        final String sideStr,
        final String ordTypeStr,
        final String tifStr,
        final String qtyStr,
        final String priceStr,
        final String account,
        final String currency) {

      final var kind = parseKind(scenarioFlag);
      final var side = parseSide(sideStr);
      final var ordType = parseOrdType(ordTypeStr);
      final var tif = parseTif(tifStr);
      final long qtyFp = parseFixedPointDecimal("--qty", qtyStr);
      final boolean hasPrice = ordType == OrdType.LIMIT;
      final long priceFp = hasPrice ? parseFixedPointDecimal("--price", priceStr) : 0L;
      return new CliScenarioSpec(
          kind, clOrdId, symbol, side, ordType, tif, account, currency, qtyFp, priceFp, hasPrice);
    }

    /**
     * For {@link ScenarioKind#MATCH}, returns the two ClOrdIDs to use for the buy and sell legs.
     * Stable suffix discipline so the Playwright spec can deterministically grep either leg.
     *
     * @return {@code [buyClOrdId, sellClOrdId]} as a 2-element array
     */
    public String[] matchClOrdIds() {
      if (kind != ScenarioKind.MATCH) {
        throw new IllegalStateException("matchClOrdIds() requires MATCH kind, got " + kind);
      }
      return new String[] {clOrdId + "-buy", clOrdId + "-sell"};
    }

    private static ScenarioKind parseKind(final String s) {
      return switch (s.toLowerCase(Locale.ROOT)) {
        case "single" -> ScenarioKind.SINGLE;
        case "match" -> ScenarioKind.MATCH;
        default ->
            throw new ArgsParseException("--scenario must be 'single' or 'match', got '" + s + "'");
      };
    }

    private static Side parseSide(final String s) {
      return switch (s.toLowerCase(Locale.ROOT)) {
        case "buy", "b" -> Side.BUY;
        case "sell", "s" -> Side.SELL;
        default -> throw new ArgsParseException("--side must be buy|sell, got '" + s + "'");
      };
    }

    private static OrdType parseOrdType(final String s) {
      return switch (s.toLowerCase(Locale.ROOT)) {
        case "limit", "l" -> OrdType.LIMIT;
        case "market", "m" -> OrdType.MARKET;
        default -> throw new ArgsParseException("--ord-type must be limit|market, got '" + s + "'");
      };
    }

    private static TimeInForce parseTif(final String s) {
      return switch (s.toLowerCase(Locale.ROOT)) {
        case "day" -> TimeInForce.DAY;
        case "ioc" -> TimeInForce.IMMEDIATE_OR_CANCEL;
        case "gtc" -> TimeInForce.GOOD_TILL_CANCEL;
        default -> throw new ArgsParseException("--tif must be day|ioc|gtc, got '" + s + "'");
      };
    }

    /**
     * Parses a decimal string (e.g. {@code "1.05"}) into a long fixed-point value at the project
     * pricing scale. Composes a {@code (value, scale)} pair from the textual decimal then delegates
     * to {@link FixedPointScale#toFixedPoint(long, int)} for the actual scaling + overflow check
     * (single source of truth for the conversion math).
     */
    private static long parseFixedPointDecimal(final String flag, final String s) {
      if (s == null || s.isBlank()) {
        throw new ArgsParseException(flag + " must not be blank");
      }
      // Reject scientific notation / spaces / negatives — only plain non-negative decimal.
      if (!s.matches("^[0-9]+(\\.[0-9]+)?$")) {
        throw new ArgsParseException(
            flag + " must be a non-negative decimal (e.g. '1.05'), got '" + s + "'");
      }
      final int dot = s.indexOf('.');
      final long unscaled;
      final int scale;
      try {
        if (dot < 0) {
          unscaled = Long.parseLong(s);
          scale = 0;
        } else {
          // "1.05" → unscaled=105, scale=2 — strip the dot, parse as one long.
          // This way fractional and whole components share an overflow guard via
          // Long.parseLong, no manual composition.
          final var compact = s.substring(0, dot) + s.substring(dot + 1);
          unscaled = Long.parseLong(compact);
          scale = s.length() - dot - 1;
        }
      } catch (final NumberFormatException e) {
        throw new ArgsParseException(flag + " overflow / parse failure: '" + s + "'");
      }
      if (scale > FixedPointScale.SCALE_DIGITS) {
        throw new ArgsParseException(
            flag
                + " has more than "
                + FixedPointScale.SCALE_DIGITS
                + " fractional digits: '"
                + s
                + "'");
      }
      try {
        return FixedPointScale.toFixedPoint(unscaled, scale);
      } catch (final ArithmeticException e) {
        throw new ArgsParseException(flag + " overflows long fixed-point: '" + s + "'");
      }
    }
  }
}
