package com.trading.engine.fixbridge.translator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.trading.engine.fix.builder.QuoteEncoder;
import com.trading.engine.fix.decoder.QuoteDecoder;
import com.trading.engine.gateway.FixedPoint;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.nio.charset.StandardCharsets;
import org.agrona.concurrent.EpochNanoClock;
import org.junit.jupiter.api.Test;
import uk.co.real_logic.artio.fields.DecimalFloat;
import uk.co.real_logic.artio.util.MutableAsciiBuffer;

/**
 * Verifies the four locked §8 paths for quote expiry computation in {@link FixToJsonTranslator}:
 *
 * <ol>
 *   <li>{@code ValidUntilTime} present and parseable, comfortably in the future → pass-through.
 *   <li>{@code ValidUntilTime} present but parses to a time earlier than {@code wallClock +
 *       EXPIRY_SAFETY_MARGIN_NS} → fallback {@code wallClock + DEFAULT_EXPIRY_NS}.
 *   <li>{@code ValidUntilTime} absent on the FIX wire → fallback.
 *   <li>{@code ValidUntilTime} present but malformed (e.g. random text) → fallback.
 * </ol>
 */
final class FixToJsonQuoteExpiryTest {

  /** Stable wall-clock for deterministic expiry computations (2024-04-07T12:00:00Z). */
  private static final long FIXED_NS = 1_712_491_200_000_000_000L;

  private static final EpochNanoClock FIXED_CLOCK = () -> FIXED_NS;

  // ---------------------------------------------------------------------------
  // Helpers — minimal Quote decoder factory.
  // ---------------------------------------------------------------------------

  private static QuoteDecoder buildQuote(
      final byte[] validUntilTimeOrNull, final boolean injectMalformedTs) {
    final var enc = new QuoteEncoder();
    enc.quoteReqID("R-1");
    enc.quoteID("Q-1");
    enc.instrument().symbol("EURUSD");
    enc.side('1'); // Buy
    final var bid = new DecimalFloat();
    final var offer = new DecimalFloat();
    FixedPoint.toDecimalFloat(110_000_000L, bid);
    FixedPoint.toDecimalFloat(110_500_000L, offer);
    enc.bidPx(bid);
    enc.offerPx(offer);
    final var qty = new DecimalFloat();
    FixedPoint.toDecimalFloat(100_000_000_000L, qty);
    enc.orderQtyData().orderQty(qty);
    if (validUntilTimeOrNull != null) {
      enc.validUntilTime(validUntilTimeOrNull, 0, validUntilTimeOrNull.length);
    }
    final var hdr = enc.header();
    hdr.senderCompID("ACCEPTOR").targetCompID("BRIDGE").msgSeqNum(1);
    hdr.sendingTime("20240407-12:00:00".getBytes(StandardCharsets.US_ASCII));

    final var wire = new MutableAsciiBuffer(new byte[4096]);
    final long encoded = enc.encode(wire, 0);
    final int wireOffset = (int) (encoded >>> 32);
    final int wireLen = (int) encoded;

    if (injectMalformedTs && validUntilTimeOrNull != null) {
      // Replace the bytes following "62=" up to the next SOH with junk to simulate a corrupt
      // ValidUntilTime field arriving over the wire.
      final byte SOH = 0x01;
      for (int i = wireOffset; i < wireOffset + wireLen - 3; i++) {
        if (wire.getByte(i) == '6' && wire.getByte(i + 1) == '2' && wire.getByte(i + 2) == '=') {
          // Walk forward past "62=" and corrupt the next 8 chars unless we hit SOH first.
          int j = i + 3;
          while (j < wireOffset + wireLen && wire.getByte(j) != SOH) {
            wire.putByte(j, (byte) 'X');
            j++;
          }
          break;
        }
      }
    }

    final var view = new MutableAsciiBuffer(new byte[wireLen]);
    wire.getBytes(wireOffset, view, 0, wireLen);
    final var dec = new QuoteDecoder();
    dec.decode(view, 0, view.capacity());
    return dec;
  }

  private static String readAll(final ByteBuf buf) {
    final var out = new byte[buf.readableBytes()];
    buf.readBytes(out);
    return new String(out, StandardCharsets.UTF_8);
  }

  private static long parseExpiryNs(final String json) {
    final int idx = json.indexOf("\"expiryNs\":");
    if (idx < 0) {
      throw new AssertionError("no expiryNs in: " + json);
    }
    final int start = idx + "\"expiryNs\":".length();
    int end = start;
    while (end < json.length() && json.charAt(end) != ',' && json.charAt(end) != '}') {
      end++;
    }
    return Long.parseLong(json.substring(start, end));
  }

  // ===========================================================================
  // Path 1 — ValidUntilTime in the future, comfortably past the safety margin.
  // ===========================================================================

  @Test
  void translateQuote_validUntilTimeFarFuture_passesThrough() {
    // 2099-01-01T00:00:00Z = 4_070_908_800 epoch-seconds × 1e9 ns
    final long expectedNs = 4_070_908_800_000_000_000L;
    final var dec = buildQuote("20990101-00:00:00".getBytes(StandardCharsets.US_ASCII), false);

    final var out = Unpooled.buffer(512);
    final var t = new FixToJsonTranslator(new DecimalStringEmitter(), FIXED_CLOCK);
    t.translateQuote(dec, out);
    final var json = readAll(out);
    assertEquals(expectedNs, parseExpiryNs(json));
  }

  // ===========================================================================
  // Path 2 — ValidUntilTime in the past relative to wallClock + safety margin.
  // ===========================================================================

  @Test
  void translateQuote_validUntilTimePast_fallsBackToDefault() {
    // 1990-01-01T00:00:00Z is well before FIXED_NS (2024) — within the safety margin.
    final var dec = buildQuote("19900101-00:00:00".getBytes(StandardCharsets.US_ASCII), false);

    final var out = Unpooled.buffer(512);
    final var t = new FixToJsonTranslator(new DecimalStringEmitter(), FIXED_CLOCK);
    t.translateQuote(dec, out);
    final var json = readAll(out);
    final long expected = FIXED_NS + FixToJsonTranslator.DEFAULT_EXPIRY_NS;
    assertEquals(expected, parseExpiryNs(json));
  }

  // ===========================================================================
  // Path 3 — ValidUntilTime absent.
  // ===========================================================================

  @Test
  void translateQuote_validUntilTimeAbsent_fallsBackToDefault() {
    final var dec = buildQuote(null, false);
    assertTrue(!dec.hasValidUntilTime(), "test setup: encoder must omit ValidUntilTime");

    final var out = Unpooled.buffer(512);
    final var t = new FixToJsonTranslator(new DecimalStringEmitter(), FIXED_CLOCK);
    t.translateQuote(dec, out);
    final var json = readAll(out);
    final long expected = FIXED_NS + FixToJsonTranslator.DEFAULT_EXPIRY_NS;
    assertEquals(expected, parseExpiryNs(json));
  }

  // ===========================================================================
  // Path 4 — ValidUntilTime malformed (corrupted on the wire).
  // ===========================================================================

  @Test
  void translateQuote_validUntilTimeMalformed_fallsBackToDefault() {
    final var dec = buildQuote("20990101-00:00:00".getBytes(StandardCharsets.US_ASCII), true);

    final var out = Unpooled.buffer(512);
    final var t = new FixToJsonTranslator(new DecimalStringEmitter(), FIXED_CLOCK);
    t.translateQuote(dec, out);
    final var json = readAll(out);
    final long expected = FIXED_NS + FixToJsonTranslator.DEFAULT_EXPIRY_NS;
    assertEquals(expected, parseExpiryNs(json));
  }

  // ===========================================================================
  // Boundary: parsed expiry exactly within the safety margin → fallback.
  // ===========================================================================

  @Test
  void translateQuote_validUntilTimeBarelyAheadInsideMargin_fallsBackToDefault() {
    // Make wallClock very high so the parsed 2099 timestamp is comfortably ahead, but use a
    // wall-clock that puts parsed within the safety margin.
    final long clockJustBeforeExpiry = 4_070_908_800_000_000_000L - 25_000_000L; // 25 ms before
    // Cannot use `final var` here: a bare lambda has no inferable target type. The explicit
    // EpochNanoClock interface annotation IS the target type.
    final EpochNanoClock c = () -> clockJustBeforeExpiry;
    final var dec = buildQuote("20990101-00:00:00".getBytes(StandardCharsets.US_ASCII), false);

    final var out = Unpooled.buffer(512);
    final var t = new FixToJsonTranslator(new DecimalStringEmitter(), c);
    t.translateQuote(dec, out);
    final var json = readAll(out);
    // Inside safety margin (25 ms < 50 ms) → fallback.
    final long expected = clockJustBeforeExpiry + FixToJsonTranslator.DEFAULT_EXPIRY_NS;
    assertEquals(expected, parseExpiryNs(json));
  }
}
