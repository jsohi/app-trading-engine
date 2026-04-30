package com.trading.engine.fixbridge.translator;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.trading.engine.fixbridge.json.MutableParsedMessage;
import com.trading.engine.gateway.FixedPoint;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import org.junit.jupiter.api.Test;
import uk.co.real_logic.artio.fields.DecimalFloat;

/**
 * Verifies the bind / reset semantics of {@link QuoteSnapshot}.
 *
 * <p>Test naming follows {@code methodUnderTest_scenario_expectedBehavior}. Threading model is
 * single-threaded by design (per class Javadoc); tests are correspondingly single-threaded.
 */
final class QuoteSnapshotTest {

  @Test
  void newSnapshot_isUnbound() {
    final var s = new QuoteSnapshot();
    assertFalse(s.isBound());
    assertEquals(QuoteSnapshot.FREE, s.symbolLen());
    assertEquals((byte) 0, s.side());
    assertEquals(0L, s.qtyInt64());
    assertEquals(0L, s.expiryNs());
  }

  @Test
  void bindWithDecimalFloats_recordsSymbolBytesAndPrices() {
    final var s = new QuoteSnapshot();
    final var symbol = "EURUSD".getBytes(StandardCharsets.US_ASCII);
    final var bid = new DecimalFloat();
    final var ask = new DecimalFloat();
    FixedPoint.toDecimalFloat(110_000_000L, bid); // 1.10
    FixedPoint.toDecimalFloat(110_500_000L, ask); // 1.105

    s.bind(
        symbol,
        0,
        symbol.length,
        MutableParsedMessage.SIDE_BUY,
        100_000_000_000L /* 1000.0 */,
        bid,
        ask,
        9_999_999_999_999_999L);

    assertTrue(s.isBound());
    assertEquals(6, s.symbolLen());
    assertArrayEquals(
        Arrays.copyOf(symbol, QuoteSnapshot.SYMBOL_CAPACITY),
        Arrays.copyOf(s.symbolBytes, QuoteSnapshot.SYMBOL_CAPACITY));
    assertEquals(MutableParsedMessage.SIDE_BUY, s.side());
    assertEquals(100_000_000_000L, s.qtyInt64());
    // FixedPoint.toDecimalFloat sets (110_000_000, 8); Artio normalises trailing zeros so the
    // canonical form is (11, 1) — i.e. 1.1. Equivalence holds via DecimalFloat#equals or by
    // comparing value × 10^(8 - scale).
    assertEquals(11L, s.bid.value());
    assertEquals(1, s.bid.scale());
    // Ask 1.105 → (110_500_000, 8) normalises to (1105, 3).
    assertEquals(1105L, s.ask.value());
    assertEquals(3, s.ask.scale());
    assertEquals(9_999_999_999_999_999L, s.expiryNs());
  }

  @Test
  void bindWithRawValues_supportsAlternateScales() {
    final var s = new QuoteSnapshot();
    final var symbol = "USDJPY".getBytes(StandardCharsets.US_ASCII);

    // Raw value/scale path lets the caller skip DecimalFloat allocation if they already have
    // primitive (mantissa, scale) pairs.
    s.bind(
        symbol,
        0,
        symbol.length,
        MutableParsedMessage.SIDE_SELL,
        500_000_000L /* 5.0 */,
        15025L,
        2,
        15030L,
        2,
        7_777L);

    assertTrue(s.isBound());
    assertEquals(MutableParsedMessage.SIDE_SELL, s.side());
    // DecimalFloat.set() normalises trailing zeros: 15025 / scale=2 has none, so unchanged.
    assertEquals(15025L, s.bid.value());
    assertEquals(2, s.bid.scale());
    // 15030 / scale=2 → 1503 / scale=1 after trailing-zero strip.
    assertEquals(1503L, s.ask.value());
    assertEquals(1, s.ask.scale());
  }

  @Test
  void bindWithNullDecimalFloats_treatsAsZero() {
    final var s = new QuoteSnapshot();
    final var symbol = "X".getBytes(StandardCharsets.US_ASCII);

    s.bind(symbol, 0, 1, MutableParsedMessage.SIDE_BUY, 0L, null, null, 1L);

    assertTrue(s.isBound());
    assertEquals(0L, s.bid.value());
    assertEquals(0, s.bid.scale());
    assertEquals(0L, s.ask.value());
  }

  @Test
  void bind_zeroOrNegativeSymbolLen_throws() {
    final var s = new QuoteSnapshot();
    final var symbol = "EURUSD".getBytes(StandardCharsets.US_ASCII);
    assertThrows(
        IllegalArgumentException.class,
        () -> s.bind(symbol, 0, 0, MutableParsedMessage.SIDE_BUY, 0L, null, null, 1L));
    assertThrows(
        IllegalArgumentException.class,
        () -> s.bind(symbol, 0, -1, MutableParsedMessage.SIDE_BUY, 0L, null, null, 1L));
  }

  @Test
  void bind_overflowingSymbolLen_throws() {
    final var s = new QuoteSnapshot();
    final var symbol = new byte[QuoteSnapshot.SYMBOL_CAPACITY + 1];
    Arrays.fill(symbol, (byte) 'A');
    assertThrows(
        IllegalArgumentException.class,
        () -> s.bind(symbol, 0, symbol.length, MutableParsedMessage.SIDE_BUY, 0L, null, null, 1L));
  }

  @Test
  void reset_returnsToUnboundState() {
    final var s = new QuoteSnapshot();
    final var symbol = "EURUSD".getBytes(StandardCharsets.US_ASCII);
    final var bid = new DecimalFloat();
    FixedPoint.toDecimalFloat(110_000_000L, bid);

    s.bind(symbol, 0, symbol.length, MutableParsedMessage.SIDE_BUY, 1L, bid, bid, 42L);
    assertTrue(s.isBound());

    s.reset();
    assertFalse(s.isBound());
    assertEquals(QuoteSnapshot.FREE, s.symbolLen());
    assertEquals((byte) 0, s.side());
    assertEquals(0L, s.qtyInt64());
    assertEquals(0L, s.expiryNs());
    assertEquals(0L, s.bid.value());
    assertEquals(0, s.bid.scale());
    assertEquals(0L, s.ask.value());
  }

  @Test
  void rebind_overwritesPreviousState() {
    final var s = new QuoteSnapshot();
    final var eur = "EURUSD".getBytes(StandardCharsets.US_ASCII);
    final var gbp = "GBPUSD".getBytes(StandardCharsets.US_ASCII);

    s.bind(eur, 0, eur.length, MutableParsedMessage.SIDE_BUY, 100L, null, null, 1L);
    s.bind(gbp, 0, gbp.length, MutableParsedMessage.SIDE_SELL, 200L, null, null, 2L);

    assertEquals(MutableParsedMessage.SIDE_SELL, s.side());
    assertEquals(200L, s.qtyInt64());
    // Symbol bytes should now read GBPUSD.
    final var view = new byte[6];
    System.arraycopy(s.symbolBytes, 0, view, 0, 6);
    assertArrayEquals(gbp, view);
  }
}
