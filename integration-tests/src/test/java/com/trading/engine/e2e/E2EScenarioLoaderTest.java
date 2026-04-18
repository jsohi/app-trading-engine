package com.trading.engine.e2e;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.trading.engine.fix.OrdType;
import com.trading.engine.fix.Side;
import com.trading.engine.fix.TimeInForce;
import com.trading.engine.messages.FixedPointScale;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests for {@link E2EScenarioLoader} — validates YAML parsing, enum mapping, fixed-point
 * conversion, and error handling for E2E test scenario definitions.
 */
final class E2EScenarioLoaderTest {

  @TempDir Path tempDir;

  @Test
  void load_validYaml_parsesAllScenarios() throws IOException {
    final var yaml =
        """
        scenarios:
          - name: "Limit Buy"
            type: "NewOrderSingle"
            expectedOutcome: "New"
            accountCode: "ACME"
            symbol: "EURUSD"
            currency: "USD"
            side: "Buy"
            ordType: "Limit"
            timeInForce: "Day"
            priceValue: 105
            priceScale: 2
            qtyValue: 1
            qtyScale: 0
          - name: "Reject"
            type: "NewOrderSingle"
            expectedOutcome: "Rejected"
            accountCode: "LOCKED"
            symbol: "EURUSD"
            currency: "USD"
            side: "Sell"
            ordType: "Limit"
            timeInForce: "Day"
            priceValue: 200
            priceScale: 2
            qtyValue: 5
            qtyScale: 0
            expectedRejectText: "account not active"
        """;
    final var file = writeYaml(yaml);
    final var scenarios = E2EScenarioLoader.load(file);

    assertEquals(2, scenarios.size());

    // First scenario — happy path
    final var s0 = scenarios.get(0);
    assertEquals("Limit Buy", s0.name());
    assertEquals(NosScenario.ScenarioType.NEW_ORDER_SINGLE, s0.type());
    assertEquals(NosScenario.ExpectedOutcome.NEW, s0.expectedOutcome());
    assertEquals("ACME", s0.accountCode());
    assertEquals("EURUSD", s0.symbol());
    assertEquals("USD", s0.currency());
    assertEquals(Side.BUY, s0.side());
    assertEquals(OrdType.LIMIT, s0.ordType());
    assertEquals(TimeInForce.DAY, s0.timeInForce());
    assertTrue(s0.hasPrice());
    assertEquals(105, s0.priceValue());
    assertEquals(2, s0.priceScale());
    // 105 with scale 2 → 1.05 → 105_000_000 fixed-point
    assertEquals(105_000_000L, s0.priceFixedPoint());
    assertEquals(1, s0.qtyValue());
    assertEquals(0, s0.qtyScale());
    assertEquals(100_000_000L, s0.qtyFixedPoint());
    assertNull(s0.expectedRejectText());

    // Second scenario — reject
    final var s1 = scenarios.get(1);
    assertEquals("Reject", s1.name());
    assertEquals(NosScenario.ExpectedOutcome.REJECTED, s1.expectedOutcome());
    assertEquals(Side.SELL, s1.side());
    assertEquals("account not active", s1.expectedRejectText());
  }

  @Test
  void load_marketOrder_omitsPriceFields() throws IOException {
    final var yaml =
        """
        scenarios:
          - name: "Market Buy"
            type: "NewOrderSingle"
            expectedOutcome: "New"
            accountCode: "ACME"
            symbol: "EURUSD"
            currency: "USD"
            side: "Buy"
            ordType: "Market"
            timeInForce: "Day"
            qtyValue: 1
            qtyScale: 0
        """;
    final var file = writeYaml(yaml);
    final var scenarios = E2EScenarioLoader.load(file);

    assertEquals(1, scenarios.size());
    final var s = scenarios.get(0);
    assertFalse(s.hasPrice());
    assertEquals(0, s.priceValue());
    assertEquals(0, s.priceScale());
    assertEquals(FixedPointScale.PRICE_NOT_AVAILABLE, s.priceFixedPoint());
    assertEquals(OrdType.MARKET, s.ordType());
  }

  @Test
  void load_missingRequiredField_throwsLoadException() throws IOException {
    final var yaml =
        """
        scenarios:
          - type: "NewOrderSingle"
            expectedOutcome: "New"
            accountCode: "ACME"
            symbol: "EURUSD"
            currency: "USD"
            side: "Buy"
            ordType: "Limit"
            timeInForce: "Day"
            priceValue: 105
            priceScale: 2
            qtyValue: 1
            qtyScale: 0
        """;
    final var file = writeYaml(yaml);
    final var ex = assertThrows(E2EScenarioLoadException.class, () -> E2EScenarioLoader.load(file));
    assertTrue(ex.getMessage().contains("name"), "Expected error to mention 'name': " + ex);
  }

  @Test
  void load_unknownEnumValue_throwsLoadException() throws IOException {
    final var yaml =
        """
        scenarios:
          - name: "Bad side"
            type: "NewOrderSingle"
            expectedOutcome: "New"
            accountCode: "ACME"
            symbol: "EURUSD"
            currency: "USD"
            side: "Short"
            ordType: "Limit"
            timeInForce: "Day"
            priceValue: 105
            priceScale: 2
            qtyValue: 1
            qtyScale: 0
        """;
    final var file = writeYaml(yaml);
    final var ex = assertThrows(E2EScenarioLoadException.class, () -> E2EScenarioLoader.load(file));
    assertTrue(ex.getMessage().contains("Short"), "Expected error to mention 'Short': " + ex);
  }

  @Test
  void load_zeroScenarios_throwsLoadException() throws IOException {
    final var yaml = "scenarios: []\n";
    final var file = writeYaml(yaml);
    final var ex = assertThrows(E2EScenarioLoadException.class, () -> E2EScenarioLoader.load(file));
    assertTrue(ex.getMessage().contains("zero scenarios"), "Expected 'zero scenarios': " + ex);
  }

  @Test
  void load_integerToLongPromotion_handlesSmallValues() throws IOException {
    // SnakeYAML parses small numbers as Integer, not Long.
    // This test verifies the loader handles Integer → long correctly.
    final var yaml =
        """
        scenarios:
          - name: "Small price"
            type: "NewOrderSingle"
            expectedOutcome: "New"
            accountCode: "ACME"
            symbol: "EURUSD"
            currency: "USD"
            side: "Buy"
            ordType: "Limit"
            timeInForce: "Day"
            priceValue: 1
            priceScale: 0
            qtyValue: 1
            qtyScale: 0
        """;
    final var file = writeYaml(yaml);
    final var scenarios = E2EScenarioLoader.load(file);

    assertEquals(1, scenarios.size());
    // priceValue=1, priceScale=0 → 1 * 10^8 = 100_000_000
    assertEquals(100_000_000L, scenarios.get(0).priceFixedPoint());
  }

  private Path writeYaml(final String content) throws IOException {
    final var file = tempDir.resolve("e2e-scenarios.yaml");
    Files.writeString(file, content, StandardCharsets.UTF_8);
    return file;
  }
}
