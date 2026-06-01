package com.trading.engine.fixbridge.transport;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.trading.engine.fixbridge.json.BrowserEvent;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link RiskLimitToBrowserAdapter} — APP-62 §A bridge wiring narrowing helper.
 *
 * <p>Covers: typical mid-range narrowing, overflow clamping to {@link Integer#MAX_VALUE}, and
 * negative-input safety (defensive: clamps to 0).
 */
final class RiskLimitToBrowserAdapterTest {

  @Test
  void toBrowserLimits_normalValues_emitsExactRecord() {
    final BrowserEvent.AccountLimits limits =
        RiskLimitToBrowserAdapter.toBrowserLimits("ACME", 100L, 500_000L, 250L, 20L);

    assertEquals("ACME", limits.account());
    assertEquals(100L, limits.maxQtyInt64());
    assertEquals(500_000L, limits.maxNotionalInt64());
    assertEquals(250, limits.priceDeviationBps());
    assertEquals(20, limits.maxOrdersPerSecond());
  }

  @Test
  void toBrowserLimits_priceDeviationBpsOverflow_clampsToIntegerMax() {
    final BrowserEvent.AccountLimits limits =
        RiskLimitToBrowserAdapter.toBrowserLimits(
            "ACME", 100L, 500_000L, (long) Integer.MAX_VALUE + 1L, 20L);
    assertEquals(Integer.MAX_VALUE, limits.priceDeviationBps());
  }

  @Test
  void toBrowserLimits_maxOrdersPerSecondOverflow_clampsToIntegerMax() {
    final BrowserEvent.AccountLimits limits =
        RiskLimitToBrowserAdapter.toBrowserLimits(
            "ACME", 100L, 500_000L, 250L, (long) Integer.MAX_VALUE + 1L);
    assertEquals(Integer.MAX_VALUE, limits.maxOrdersPerSecond());
  }

  @Test
  void toBrowserLimits_negativeInputs_clampToZero() {
    final BrowserEvent.AccountLimits limits =
        RiskLimitToBrowserAdapter.toBrowserLimits("ACME", 100L, 500_000L, -1L, -42L);
    assertEquals(0, limits.priceDeviationBps());
    assertEquals(0, limits.maxOrdersPerSecond());
  }
}
