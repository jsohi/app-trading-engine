package com.trading.engine.fixbridge.translator;

import com.trading.engine.gateway.FixedPoint;
import io.netty.buffer.ByteBuf;
import java.math.BigDecimal;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.artio.fields.ReadOnlyDecimalFloat;

/**
 * Zero-allocation ASCII decimal-string emitter for outbound JSON events.
 *
 * <p><b>Purpose.</b> Convert a fixed-point int64 (scale {@code 10^-8}) or an Artio {@link
 * ReadOnlyDecimalFloat} into the canonical JSON wire form used by {@code BrowserEventWriter} —
 * always a signed integer part, a {@code .}, then exactly 8 fractional digits (e.g. {@code
 * 123.45000000}). The 8-digit pad is the contract clients depend on so that decimal-string parsing
 * never sees a varying scale.
 *
 * <p><b>Threading.</b> Per-instance — each emitter owns a small {@code byte[]} scratch and a
 * thin-wrapper {@link UnsafeBuffer} reused across calls. NOT thread-safe; one instance per Netty
 * worker (Phase 6 wires this).
 *
 * <p><b>Allocation.</b> Zero on every public method after construction. Numeric → ASCII conversion
 * uses Agrona's {@link MutableDirectBuffer#putLongAscii} on a pre-allocated 32-byte heap scratch
 * and then bulk-copies the produced bytes into the destination Netty {@link ByteBuf} with {@code
 * writeBytes}. {@link Long#MIN_VALUE} is rejected up front — Agrona's {@code putLongAscii} writes
 * its literal "{@code -9223372036854775808}" which would round-trip to a value that is NOT the
 * negation of itself, so we forbid the input rather than silently accept an asymmetric range.
 *
 * <p><b>Lifecycle.</b> Per-handler instance, allocated in {@code BrowserSessionHandler}'s ctor.
 *
 * <p><b>Dependencies.</b> Agrona ({@link MutableDirectBuffer}, {@link UnsafeBuffer}), Netty ({@link
 * ByteBuf}), and Artio ({@link ReadOnlyDecimalFloat}).
 *
 * <p><b>No floating point.</b> No {@code String.format}, no {@link BigDecimal}, no {@code
 * Double.toString}. Verified by {@code DecimalStringEmitterAllocTest}.
 *
 * <p><b>Range note for {@link #emitDecimalFloat}.</b> Artio's {@code DecimalFloat} caps the
 * absolute value at {@code 10^18 - 1} ({@code ReadOnlyDecimalFloat.VALUE_MAX_VAL}); per locked §9,
 * instances passed to {@code emitDecimalFloat} MUST be sourced from {@code
 * FixedPoint.toDecimalFloat(int64Value, df)}, which silently constrains the input to that range.
 * The emitter additionally rejects {@link Long#MIN_VALUE} (the Artio NaN sentinel) and negative
 * scales as defensive checks against a malformed {@code DecimalFloat} produced via the deprecated
 * raw {@code value(long)} / {@code scale(int)} setters.
 */
public final class DecimalStringEmitter {

  /**
   * Per fixed-point convention — sourced from the canonical {@link FixedPoint#FIXED_POINT_SCALE}
   * (locked §9: every JSON↔int64↔DecimalFloat chain MUST go through {@link FixedPoint}). Re-exposed
   * as a {@code public} alias so existing test references continue to compile without coupling to
   * {@code :gateway} via additional imports.
   */
  public static final int FIXED_POINT_SCALE = FixedPoint.FIXED_POINT_SCALE;

  /** {@code 10^FIXED_POINT_SCALE} — sourced from {@link FixedPoint#PRICE_SCALE}. */
  private static final long PRICE_SCALE = FixedPoint.PRICE_SCALE;

  /**
   * Maximum {@link ReadOnlyDecimalFloat#scale()} value the emitter accepts. The internal {@code
   * pow10} helper supports exponents up to 18; the over-scale path uses {@code pow10(scale -
   * FIXED_POINT_SCALE)}, so {@code scale} is bounded at {@code 18 + FIXED_POINT_SCALE = 26}. Any
   * larger scale can only originate from a {@link uk.co.real_logic.artio.fields.DecimalFloat}
   * populated through the deprecated raw {@code value(long)}/{@code scale(int)} setters; production
   * callers go through {@code FixedPoint.toDecimalFloat} which constrains scale.
   */
  private static final int MAX_DECIMAL_SCALE = 26;

  /**
   * Scratch capacity. Must hold the worst-case ASCII rendering of a signed long: {@code -} + {@code
   * 19 digits} + {@code .} + {@code 8 frac digits} = 29 bytes; 32 buys headroom and aligns.
   */
  private static final int SCRATCH_CAPACITY = 32;

  // 19 = digit count of Long.MAX_VALUE.
  private static final int MAX_LONG_DIGITS = 19;

  // Per-instance scratch — heap byte[] wrapped once by an UnsafeBuffer so Agrona's
  // putLongAscii can write into it. Long.MIN_VALUE-tolerant capacity (32 bytes) and aligned
  // — see SCRATCH_CAPACITY.
  private final byte[] scratch = new byte[SCRATCH_CAPACITY];
  private final UnsafeBuffer scratchView = new UnsafeBuffer(scratch);

  /**
   * Emit the ASCII decimal representation of an int64 fixed-point value (scale {@code 10^-8}) into
   * {@code dst}. Always writes exactly 8 fractional digits, padding with {@code 0} as needed.
   *
   * <p>Examples:
   *
   * <pre>
   *   0           → "0.00000000"
   *   1           → "0.00000001"
   *   100_000_000 → "1.00000000"
   *   -150_000_000 → "-1.50000000"
   *   Long.MAX_VALUE → "92233720368.54775807"
   * </pre>
   *
   * @param int64Value fixed-point int64 value
   * @param dst destination Netty buffer; bytes are appended at {@code dst.writerIndex()}
   * @return number of bytes written to {@code dst}
   * @throws IllegalArgumentException if {@code int64Value == Long.MIN_VALUE}; that input is
   *     forbidden because the absolute value is not representable in {@code long}
   */
  public int emitInt64FixedPoint(final long int64Value, final ByteBuf dst) {
    if (int64Value == Long.MIN_VALUE) {
      // -Long.MIN_VALUE overflows a long; reject up front rather than emit an asymmetric value.
      throw new IllegalArgumentException("Long.MIN_VALUE is not representable in fixed-point");
    }

    // Split into sign, whole part, and frac part.
    final long magnitude;
    final boolean negative;
    if (int64Value < 0L) {
      negative = true;
      magnitude = -int64Value;
    } else {
      negative = false;
      magnitude = int64Value;
    }

    final long whole = magnitude / PRICE_SCALE;
    final long frac = magnitude - whole * PRICE_SCALE; // always in [0, PRICE_SCALE)

    return writeSignedDecimal(negative, whole, frac, dst);
  }

  /**
   * Emit the ASCII decimal representation of an Artio {@link ReadOnlyDecimalFloat} ({@code value ×
   * 10^-scale}) into {@code dst}. Always writes exactly 8 fractional digits.
   *
   * <p>The implementation uses {@code df.value()} and {@code df.scale()} directly — it does NOT
   * normalise via {@code FixedPoint.toInt64}, so a {@code DecimalFloat} carrying a value that
   * cannot be represented in fixed-point (scale finer than 10^-8 with non-zero remainder) is
   * silently truncated. Production callers are required to source their {@code DecimalFloat} from
   * {@code FixedPoint.toDecimalFloat(int64Value, df)} to make the round-trip exact.
   *
   * @param df Artio decimal-float; must not be null
   * @param dst destination Netty buffer
   * @return number of bytes written
   * @throws IllegalArgumentException if {@code df.scale() < 0} or {@code df.value() ==
   *     Long.MIN_VALUE} (the absolute value is not representable in {@code long})
   */
  public int emitDecimalFloat(final ReadOnlyDecimalFloat df, final ByteBuf dst) {
    final long value = df.value();
    final int scale = df.scale();
    if (scale < 0) {
      throw new IllegalArgumentException("DecimalFloat with negative scale: scale=" + scale);
    }
    if (scale > MAX_DECIMAL_SCALE) {
      // Defensive: scale > MAX_DECIMAL_SCALE would overflow pow10(scale - FIXED_POINT_SCALE).
      // Artio's ReadOnlyDecimalFloat caps practical scale at ~18 via VALUE_MAX_VAL=10^18-1, so a
      // scale this large can only arrive via the deprecated raw value(long)/scale(int) setters.
      throw new IllegalArgumentException(
          "DecimalFloat scale exceeds maximum representable: scale="
              + scale
              + " (max="
              + MAX_DECIMAL_SCALE
              + "); was the DecimalFloat populated via deprecated raw setters?");
    }
    if (value == Long.MIN_VALUE) {
      throw new IllegalArgumentException("Long.MIN_VALUE is not representable in fixed-point");
    }

    final long magnitude;
    final boolean negative;
    if (value < 0L) {
      negative = true;
      magnitude = -value;
    } else {
      negative = false;
      magnitude = value;
    }

    // Convert to (whole, frac<10^8>) pair. Agrona's POW10 limit is 10^18 < Long.MAX_VALUE.
    final long whole;
    final long frac;
    if (scale == FIXED_POINT_SCALE) {
      whole = magnitude / PRICE_SCALE;
      frac = magnitude - whole * PRICE_SCALE;
    } else if (scale < FIXED_POINT_SCALE) {
      // E.g. scale=2 → "150.25" stored as (15025, 2). Multiply frac up to 10^-8 alignment.
      final long divisor = pow10(scale);
      final long w = magnitude / divisor;
      final long fracInGivenScale = magnitude - w * divisor;
      whole = w;
      // shift frac into 10^-8 magnitude.
      frac = fracInGivenScale * pow10(FIXED_POINT_SCALE - scale);
    } else {
      // scale > 8: divide magnitude down to 10^-8 alignment, truncating finer precision.
      final long shift = pow10(scale - FIXED_POINT_SCALE);
      // Total int64-fixed-point magnitude.
      final long fixedMagnitude = magnitude / shift;
      whole = fixedMagnitude / PRICE_SCALE;
      frac = fixedMagnitude - whole * PRICE_SCALE;
    }

    return writeSignedDecimal(negative, whole, frac, dst);
  }

  // ---------------------------------------------------------------------------
  // Private rendering helpers.
  // ---------------------------------------------------------------------------

  /**
   * Render the decomposition (sign, whole, frac) into {@code dst}. The frac argument MUST already
   * be in the 10^-8 magnitude (i.e. {@code 0 <= frac < PRICE_SCALE}).
   *
   * @return number of bytes written
   */
  private int writeSignedDecimal(
      final boolean negative, final long whole, final long frac, final ByteBuf dst) {
    // CLAUDE.md loop-accumulator carve-out: `written` accumulates byte counts across the digit
    // emission loop; mutable by design.
    int written = 0;
    if (negative && (whole != 0L || frac != 0L)) {
      // Suppress sign on negative-zero: -0 emitted as "0.00000000" by convention.
      dst.writeByte('-');
      written++;
    }

    // Whole part — Agrona's putLongAscii into the per-instance scratch; bulk-copy into dst.
    // putLongAscii on a non-negative long writes only digits (no sign), which is what we want.
    final int wholeDigits = scratchView.putLongAscii(0, whole);
    dst.writeBytes(scratch, 0, wholeDigits);
    written += wholeDigits;

    dst.writeByte('.');
    written++;

    // Frac part — emit eight digits, left-padded with '0'.
    if (frac == 0L) {
      // Fast path: skip the scratch buffer entirely and write FIXED_POINT_SCALE zero bytes
      // directly.
      for (int i = 0; i < FIXED_POINT_SCALE; i++) {
        dst.writeByte('0');
      }
      written += FIXED_POINT_SCALE;
      return written;
    }

    // Render frac via putLongAscii (writes |frac| as the minimum-width digit run) then prefix-pad
    // to FIXED_POINT_SCALE bytes.
    final int fracDigits = scratchView.putLongAscii(0, frac);
    final int padZeros = FIXED_POINT_SCALE - fracDigits;
    for (int i = 0; i < padZeros; i++) {
      dst.writeByte('0');
    }
    written += padZeros;
    dst.writeBytes(scratch, 0, fracDigits);
    written += fracDigits;

    return written;
  }

  /**
   * Returns {@code 10^exp} as a {@code long}. {@code exp} must satisfy {@code 0 <= exp <= 18}; any
   * value outside that range throws.
   */
  private static long pow10(final int exp) {
    if (exp < 0 || exp > 18) {
      throw new IllegalArgumentException("pow10 exponent out of range: " + exp);
    }
    // CLAUDE.md loop-accumulator carve-out: `result` is multiplied across the loop body.
    long result = 1L;
    for (int i = 0; i < exp; i++) {
      result *= 10L;
    }
    return result;
  }

  /**
   * Returns the per-instance scratch capacity. Visible for the alloc test which asserts that the
   * configured size accommodates a signed Long.MAX_VALUE without growth.
   *
   * @return capacity in bytes
   */
  static int scratchCapacity() {
    return SCRATCH_CAPACITY;
  }

  /**
   * @return the Long.MAX_VALUE digit count constant — visible for compile-time sanity asserts.
   */
  static int maxLongDigits() {
    return MAX_LONG_DIGITS;
  }
}
