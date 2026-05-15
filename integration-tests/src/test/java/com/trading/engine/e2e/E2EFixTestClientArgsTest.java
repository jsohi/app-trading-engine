package com.trading.engine.e2e;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.trading.engine.e2e.E2EFixTestClientArgs.ArgsParseException;
import com.trading.engine.e2e.E2EFixTestClientArgs.CliScenarioSpec.ScenarioKind;
import com.trading.engine.e2e.E2EFixTestClientArgs.RunMode;
import com.trading.engine.fix.OrdType;
import com.trading.engine.fix.Side;
import com.trading.engine.fix.TimeInForce;
import com.trading.engine.messages.FixedPointScale;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Hermetic unit tests for {@link E2EFixTestClientArgs}.
 *
 * <p>Pure-Java parser exercise — does NOT boot Aeron/Artio/MediaDriver. The 10-second class-level
 * timeout is a defensive guard: if a future change introduces an accidental I/O dependency, the
 * test fails loud rather than hanging the CI shard.
 */
@Timeout(value = 10, unit = TimeUnit.SECONDS)
final class E2EFixTestClientArgsTest {

  @Test
  void yamlMode_defaultsApplied_whenOnlyDataDirGiven() {
    final var args = E2EFixTestClientArgs.parse(new String[] {"--data-dir", "/some/path"});
    assertEquals(RunMode.YAML, args.runMode());
    assertEquals("/some/path", args.dataDir().orElseThrow());
    assertTrue(args.cliScenario().isEmpty());
    assertEquals("localhost", args.host());
    assertEquals(19880, args.port());
    assertEquals("CLIENT1", args.senderCompId());
    assertEquals("TRADING", args.targetCompId());
  }

  @Test
  void yamlMode_overridesAllFlags() {
    final var args =
        E2EFixTestClientArgs.parse(
            new String[] {
              "--host", "10.0.0.1",
              "--port", "20000",
              "--sender-comp-id", "FOO",
              "--target-comp-id", "BAR",
              "--data-dir", "/x"
            });
    assertEquals("10.0.0.1", args.host());
    assertEquals(20000, args.port());
    assertEquals("FOO", args.senderCompId());
    assertEquals("BAR", args.targetCompId());
    assertEquals("/x", args.dataDir().orElseThrow());
  }

  @Test
  void cliMode_singleScenario_minimalFlags() {
    final var args =
        E2EFixTestClientArgs.parse(new String[] {"--scenario", "single", "--clord-id", "E2E-X"});
    assertEquals(RunMode.CLI, args.runMode());
    assertTrue(args.dataDir().isEmpty());
    final var spec = args.cliScenario().orElseThrow();
    assertEquals(ScenarioKind.SINGLE, spec.kind());
    assertEquals("E2E-X", spec.clOrdId());
    // Defaults
    assertEquals("EURUSD", spec.symbol());
    assertEquals(Side.BUY, spec.side());
    assertEquals(OrdType.LIMIT, spec.ordType());
    assertEquals(TimeInForce.DAY, spec.timeInForce());
    assertEquals("ACME", spec.account());
    assertEquals("USD", spec.currency());
    assertTrue(spec.hasPrice());
    // 1.0 * 10^8 = 100_000_000
    assertEquals(FixedPointScale.PRICE_SCALE, spec.qtyFixedPoint());
    // 1.05000000 * 10^8 = 105_000_000
    assertEquals(105_000_000L, spec.priceFixedPoint());
  }

  @Test
  void cliMode_matchScenario_emitsBuySellSuffixes() {
    final var args =
        E2EFixTestClientArgs.parse(new String[] {"--scenario", "match", "--clord-id", "E2E-Y"});
    final var spec = args.cliScenario().orElseThrow();
    assertEquals(ScenarioKind.MATCH, spec.kind());
    final var ids = spec.matchClOrdIds();
    assertEquals(2, ids.length);
    assertEquals("E2E-Y-buy", ids[0]);
    assertEquals("E2E-Y-sell", ids[1]);
  }

  @Test
  void cliMode_marketOrder_omitsPrice() {
    final var args =
        E2EFixTestClientArgs.parse(
            new String[] {
              "--scenario", "single",
              "--clord-id", "E2E-MKT",
              "--ord-type", "market"
            });
    final var spec = args.cliScenario().orElseThrow();
    assertEquals(OrdType.MARKET, spec.ordType());
    assertFalse(spec.hasPrice());
    assertEquals(0L, spec.priceFixedPoint());
  }

  @Test
  void cliMode_pricedQty_parsedExactly() {
    final var args =
        E2EFixTestClientArgs.parse(
            new String[] {
              "--scenario", "single",
              "--clord-id", "E2E-Z",
              "--symbol", "GBPUSD",
              "--side", "sell",
              "--qty", "2.5",
              "--price", "1.27345678"
            });
    final var spec = args.cliScenario().orElseThrow();
    assertEquals("GBPUSD", spec.symbol());
    assertEquals(Side.SELL, spec.side());
    assertEquals(250_000_000L, spec.qtyFixedPoint());
    assertEquals(127_345_678L, spec.priceFixedPoint());
  }

  @Test
  void cliMode_caseInsensitiveEnumFlags() {
    final var args =
        E2EFixTestClientArgs.parse(
            new String[] {
              "--scenario", "SINGLE",
              "--clord-id", "X",
              "--side", "BUY",
              "--ord-type", "LIMIT",
              "--tif", "IOC"
            });
    final var spec = args.cliScenario().orElseThrow();
    assertEquals(ScenarioKind.SINGLE, spec.kind());
    assertEquals(Side.BUY, spec.side());
    assertEquals(OrdType.LIMIT, spec.ordType());
    assertEquals(TimeInForce.IMMEDIATE_OR_CANCEL, spec.timeInForce());
  }

  @Test
  void rejects_neitherScenarioNorDataDir() {
    final var ex =
        assertThrows(ArgsParseException.class, () -> E2EFixTestClientArgs.parse(new String[] {}));
    assertTrue(ex.getMessage().contains("--scenario"));
  }

  @Test
  void rejects_scenarioAndDataDirTogether() {
    final var ex =
        assertThrows(
            ArgsParseException.class,
            () ->
                E2EFixTestClientArgs.parse(
                    new String[] {
                      "--scenario", "single",
                      "--clord-id", "X",
                      "--data-dir", "/x"
                    }));
    assertTrue(ex.getMessage().contains("mutually exclusive"));
  }

  @Test
  void rejects_scenarioWithoutClOrdId() {
    final var ex =
        assertThrows(
            ArgsParseException.class,
            () -> E2EFixTestClientArgs.parse(new String[] {"--scenario", "single"}));
    assertTrue(ex.getMessage().contains("--clord-id"));
  }

  @Test
  void rejects_unknownFlag() {
    final var ex =
        assertThrows(
            ArgsParseException.class,
            () -> E2EFixTestClientArgs.parse(new String[] {"--data-dir", "/x", "--bogus", "v"}));
    assertTrue(ex.getMessage().contains("--bogus"));
  }

  @Test
  void rejects_flagWithoutValue() {
    final var ex =
        assertThrows(
            ArgsParseException.class,
            () -> E2EFixTestClientArgs.parse(new String[] {"--data-dir"}));
    assertTrue(ex.getMessage().contains("missing"));
  }

  @Test
  void rejects_invalidPort() {
    assertThrows(
        ArgsParseException.class,
        () ->
            E2EFixTestClientArgs.parse(
                new String[] {"--data-dir", "/x", "--port", "not-a-number"}));
  }

  @Test
  void rejects_invalidScenarioValue() {
    assertThrows(
        ArgsParseException.class,
        () -> E2EFixTestClientArgs.parse(new String[] {"--scenario", "bogus", "--clord-id", "X"}));
  }

  @Test
  void rejects_invalidSide() {
    assertThrows(
        ArgsParseException.class,
        () ->
            E2EFixTestClientArgs.parse(
                new String[] {"--scenario", "single", "--clord-id", "X", "--side", "buyish"}));
  }

  @Test
  void rejects_negativePrice() {
    assertThrows(
        ArgsParseException.class,
        () ->
            E2EFixTestClientArgs.parse(
                new String[] {"--scenario", "single", "--clord-id", "X", "--price", "-1.0"}));
  }

  @Test
  void rejects_priceWithMoreThan8FractionalDigits() {
    assertThrows(
        ArgsParseException.class,
        () ->
            E2EFixTestClientArgs.parse(
                new String[] {
                  "--scenario", "single", "--clord-id", "X", "--price", "1.123456789"
                }));
  }

  @Test
  void rejects_scientificNotationPrice() {
    assertThrows(
        ArgsParseException.class,
        () ->
            E2EFixTestClientArgs.parse(
                new String[] {"--scenario", "single", "--clord-id", "X", "--price", "1e2"}));
  }

  @Test
  void rejects_limitOrderWithoutPriceWhenPriceFlagAbsent_inMatchScenario_butDefaultsApply() {
    // Defaults supply price=1.05; explicit empty would be required to break it. Sanity check that
    // the default path produces a valid Limit with hasPrice=true.
    final var args =
        E2EFixTestClientArgs.parse(new String[] {"--scenario", "match", "--clord-id", "X"});
    assertTrue(args.cliScenario().orElseThrow().hasPrice());
  }
}
