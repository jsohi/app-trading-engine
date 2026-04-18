package com.trading.engine.e2e;

import com.trading.engine.fix.OrdType;
import com.trading.engine.fix.Side;
import com.trading.engine.fix.TimeInForce;
import com.trading.engine.messages.FixedPointScale;
import java.util.Objects;

/**
 * Immutable value object representing a single NewOrderSingle E2E test scenario. All fields needed
 * to construct a FIX NOS (tag 35=D) and validate the resulting ExecutionReport (tag 35=8).
 *
 * <p>Intentionally flat (17 fields) — a FIX NOS has 30+ optional fields; this covers the core set
 * exercised by the current engine.
 *
 * <p><b>Threading:</b> Immutable record — safe to share across threads.
 *
 * <p><b>Allocation:</b> One allocation at construction; no allocation after.
 *
 * <p>{@code priceFixedPoint} and {@code qtyFixedPoint} are pre-computed by {@link
 * E2EScenarioLoader#load} — the compact constructor validates invariants but does not compute
 * derived values (Java records require all components in the canonical constructor).
 *
 * @param name human-readable scenario name for logging (e.g., "NOS happy path — Limit Buy EURUSD")
 * @param type scenario type discriminator — currently only {@link ScenarioType#NEW_ORDER_SINGLE}
 * @param expectedOutcome expected ER outcome — {@link ExpectedOutcome#NEW} or {@link
 *     ExpectedOutcome#REJECTED}
 * @param accountCode account code matching accounts.yaml (e.g., "ACME", "LOCKED")
 * @param symbol instrument symbol (e.g., "EURUSD") — not yet ref-data validated (APP-128)
 * @param currency settlement currency (e.g., "USD") — must exist in currencies.yaml
 * @param side Artio {@link Side} enum — Buy or Sell
 * @param ordType Artio {@link OrdType} enum — Limit or Market
 * @param timeInForce Artio {@link TimeInForce} enum — Day, IOC, GTC
 * @param priceValue unscaled price value from YAML (e.g., 105 for "1.05"); 0 if no price
 * @param priceScale number of decimal digits in {@code priceValue}; 0 if no price
 * @param hasPrice true if price fields are present (Limit orders); false for Market orders
 * @param qtyValue unscaled quantity value from YAML (e.g., 1 for "1.0")
 * @param qtyScale number of decimal digits in {@code qtyValue}
 * @param priceFixedPoint pre-computed via {@link FixedPointScale#toFixedPoint(long, int)}; {@link
 *     FixedPointScale#PRICE_NOT_AVAILABLE} for Market orders
 * @param qtyFixedPoint pre-computed via {@link FixedPointScale#toFixedPoint(long, int)}
 * @param expectedRejectText null for happy path ("New"); required for "Rejected" — matched as
 *     substring against ER Text (tag 58)
 */
public record NosScenario(
    String name,
    ScenarioType type,
    ExpectedOutcome expectedOutcome,
    String accountCode,
    String symbol,
    String currency,
    Side side,
    OrdType ordType,
    TimeInForce timeInForce,
    long priceValue,
    int priceScale,
    boolean hasPrice,
    long qtyValue,
    int qtyScale,
    long priceFixedPoint,
    long qtyFixedPoint,
    String expectedRejectText) {

  /** Compact constructor — validates invariants (does NOT compute derived fields). */
  public NosScenario {
    if (name == null || name.isBlank()) {
      throw new IllegalArgumentException("scenario name must not be blank");
    }
    Objects.requireNonNull(type, "type");
    Objects.requireNonNull(expectedOutcome, "expectedOutcome");
    Objects.requireNonNull(accountCode, "accountCode");
    Objects.requireNonNull(symbol, "symbol");
    Objects.requireNonNull(currency, "currency");
    Objects.requireNonNull(side, "side");
    Objects.requireNonNull(ordType, "ordType");
    Objects.requireNonNull(timeInForce, "timeInForce");
    if (expectedOutcome == ExpectedOutcome.REJECTED && expectedRejectText == null) {
      throw new IllegalArgumentException("expectedRejectText required for Rejected outcome");
    }
    if (ordType == OrdType.LIMIT && !hasPrice) {
      throw new IllegalArgumentException("priceValue/priceScale required for Limit orders");
    }
    if (hasPrice && priceFixedPoint == FixedPointScale.PRICE_NOT_AVAILABLE) {
      throw new IllegalArgumentException("priceFixedPoint sentinel conflicts with hasPrice=true");
    }
  }

  /** Scenario message type discriminator. */
  public enum ScenarioType {
    NEW_ORDER_SINGLE
  }

  /** Expected ExecutionReport outcome. */
  public enum ExpectedOutcome {
    /** ExecType (tag 150) = '0', OrdStatus (tag 39) = '0'. */
    NEW,
    /** ExecType (tag 150) = '8', OrdStatus (tag 39) = '8'. */
    REJECTED
  }
}
