package com.trading.engine.fixbridge.rawfix;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link PiiMask}.
 *
 * <p>Covers construction (null/invalid tags, deduplication, sort, defensive-copy), {@link
 * PiiMask#isMasked(int)} lookup (hit, miss, empty masklist), the {@link PiiMask#withDefaultMask()}
 * factory (jurisdiction tag set), and the full {@link PiiMask#mask(byte[], int, int, byte[], int)}
 * masking contract: correct byte substitution, value preservation, offset handling (src and dst),
 * in-place masking ({@code src == dst}), return value, edge cases (empty value, multibyte UTF-8, no
 * trailing pipe, large tag number), and error paths (null, OOB, negative offset).
 *
 * <p>Wire format under test: SOH already substituted as {@code '|'} per the {@link
 * com.trading.engine.fixbridge.json.BrowserEvent.RawFix} contract — i.e. {@code
 * <digits>=<value>|<digits>=<value>|...}.
 *
 * <p>Threading: not thread-safe per JUnit 5 conventions. Each test constructs its own {@link
 * PiiMask} instance. No shared mutable state.
 *
 * <p>Allocation: tests freely allocate — zero-alloc constraint is verified in {@link
 * PiiMaskAllocTest}.
 */
final class PiiMaskTest {

  // ---------------------------------------------------------------------------
  // Construction.
  // ---------------------------------------------------------------------------

  @Test
  void ctor_nullTags_throwsNPE() {
    assertThrows(NullPointerException.class, () -> new PiiMask(null));
  }

  @Test
  void ctor_negativeTag_throwsIAE() {
    assertThrows(IllegalArgumentException.class, () -> new PiiMask(new int[] {-1}));
  }

  @Test
  void ctor_zeroTag_throwsIAE() {
    assertThrows(IllegalArgumentException.class, () -> new PiiMask(new int[] {0}));
  }

  /** Input {@code [1,1,1]} should deduplicate to a masklist of exactly {@code [1]}. */
  @Test
  void ctor_duplicateTags_dedupes() {
    final var mask = new PiiMask(new int[] {1, 1, 1});
    final int[] visible = mask.tagsVisibleForTesting();
    assertArrayEquals(new int[] {1}, visible, "expected [1] after dedup");
  }

  /** Input {@code [802,1,448]} must be stored sorted as {@code [1,448,802]}. */
  @Test
  void ctor_unsortedTags_storedSorted() {
    final var mask = new PiiMask(new int[] {802, 1, 448});
    final int[] visible = mask.tagsVisibleForTesting();
    assertArrayEquals(new int[] {1, 448, 802}, visible, "expected sorted [1,448,802]");
  }

  /**
   * Mutating the array passed to the constructor after construction must NOT change the result of
   * {@link PiiMask#tagsVisibleForTesting()} — confirms the defensive copy.
   */
  @Test
  void ctor_isDefensive() {
    final int[] input = {1, 448, 802};
    final var mask = new PiiMask(input);

    // Mutate after construction.
    input[0] = 999;
    input[1] = 999;
    input[2] = 999;

    final int[] visible = mask.tagsVisibleForTesting();
    // Sorted input was [1,448,802]; mutations must not propagate.
    assertArrayEquals(
        new int[] {1, 448, 802}, visible, "masklist should not reflect post-ctor mutation");
  }

  // ---------------------------------------------------------------------------
  // isMasked lookup.
  // ---------------------------------------------------------------------------

  @Test
  void isMasked_inMasklist_true() {
    final var mask = new PiiMask(new int[] {1, 448});
    assertTrue(mask.isMasked(1));
    assertTrue(mask.isMasked(448));
  }

  @Test
  void isMasked_notInMasklist_false() {
    final var mask = new PiiMask(new int[] {1, 448});
    assertFalse(mask.isMasked(55)); // Symbol — not in list
    assertFalse(mask.isMasked(11)); // ClOrdID — not in list
  }

  /** An empty masklist must never return true for any tag. */
  @Test
  void isMasked_emptyMask_alwaysFalse() {
    final var mask = new PiiMask(new int[] {});
    assertFalse(mask.isMasked(1));
    assertFalse(mask.isMasked(55));
    assertFalse(mask.isMasked(Integer.MAX_VALUE));
  }

  // ---------------------------------------------------------------------------
  // withDefaultMask factory.
  // ---------------------------------------------------------------------------

  /**
   * Every tag in the §3.5 jurisdiction set must be reported as masked. The full set is {@code [1,
   * 95, 96, 106, 109, 110, 116, 375, 448, 449, 802]}.
   */
  @Test
  void withDefaultMask_includesAllJurisdictionTags() {
    final var mask = PiiMask.withDefaultMask();
    final int[] expected = {1, 95, 96, 106, 109, 110, 116, 375, 448, 449, 802};
    for (final int tag : expected) {
      assertTrue(mask.isMasked(tag), "expected tag " + tag + " to be masked by default mask");
    }
  }

  // ---------------------------------------------------------------------------
  // Masking behaviour.
  // ---------------------------------------------------------------------------

  /**
   * A single Account field {@code "1=ACME|"} must produce {@code "1=****|"}: value bytes replaced
   * with stars, tag digits, {@code '='}, and {@code '|'} preserved verbatim.
   */
  @Test
  void mask_singleAccountField_replacesValueWithStars() {
    final var mask = new PiiMask(new int[] {1});
    final byte[] src = "1=ACME|".getBytes(StandardCharsets.US_ASCII);
    final byte[] dst = new byte[src.length];

    final int written = mask.mask(src, 0, src.length, dst, 0);

    assertEquals(src.length, written);
    assertArrayEquals("1=****|".getBytes(StandardCharsets.US_ASCII), dst);
  }

  /**
   * A field with a tag NOT in the masklist ({@code "55=EURUSD|"}) must be copied verbatim. Symbol
   * (tag 55) is not a PII tag.
   */
  @Test
  void mask_nonMaskedField_passthrough() {
    final var mask = new PiiMask(new int[] {1});
    final byte[] src = "55=EURUSD|".getBytes(StandardCharsets.US_ASCII);
    final byte[] dst = new byte[src.length];

    mask.mask(src, 0, src.length, dst, 0);

    assertArrayEquals("55=EURUSD|".getBytes(StandardCharsets.US_ASCII), dst);
  }

  /**
   * A realistic NewOrderSingle message with multiple fields: only the Account field value (tag 1)
   * must be redacted. All other field values are passed through verbatim.
   *
   * <p>Input: {@code
   * "8=FIX.4.4|9=80|35=D|49=SENDER|56=TARGET|11=C-1|55=EURUSD|54=1|38=1000|1=ACME|10=123|"}
   */
  @Test
  void mask_mixedFields_masksOnlyConfiguredOnes() {
    final var mask = PiiMask.withDefaultMask();
    final String input =
        "8=FIX.4.4|9=80|35=D|49=SENDER|56=TARGET|11=C-1|55=EURUSD|54=1|38=1000|1=ACME|10=123|";
    final String expected =
        "8=FIX.4.4|9=80|35=D|49=SENDER|56=TARGET|11=C-1|55=EURUSD|54=1|38=1000|1=****|10=123|";
    final byte[] src = input.getBytes(StandardCharsets.US_ASCII);
    final byte[] dst = new byte[src.length];

    mask.mask(src, 0, src.length, dst, 0);

    assertArrayEquals(expected.getBytes(StandardCharsets.US_ASCII), dst);
  }

  /**
   * An Account field with an empty value {@code "1=|"} must produce {@code "1=|"}: no value bytes
   * to overwrite, so the output is identical to the input.
   */
  @Test
  void mask_emptyValue_zeroStars() {
    final var mask = new PiiMask(new int[] {1});
    final byte[] src = "1=|".getBytes(StandardCharsets.US_ASCII);
    final byte[] dst = new byte[src.length];

    mask.mask(src, 0, src.length, dst, 0);

    assertArrayEquals("1=|".getBytes(StandardCharsets.US_ASCII), dst);
  }

  /**
   * A value containing a UTF-8 multibyte sequence (e.g. {@code ä} encoded as {@code 0xC3 0xA4})
   * must be masked at the byte level, not the codepoint level. Both bytes become {@code '*'}.
   *
   * <p>Input bytes: {@code '1', '=', 0xC3, 0xA4, '|'} (i.e. {@code "1=ä|"} in UTF-8). Expected:
   * {@code '1', '=', '*', '*', '|'}.
   */
  @Test
  void mask_multibyteAccountValue_preservesByteLength() {
    final var mask = new PiiMask(new int[] {1});
    // Build src manually so we control the exact bytes.
    final byte[] src = {(byte) '1', (byte) '=', (byte) 0xC3, (byte) 0xA4, (byte) '|'};
    final byte[] dst = new byte[src.length];

    mask.mask(src, 0, src.length, dst, 0);

    final byte[] expected = {(byte) '1', (byte) '=', (byte) '*', (byte) '*', (byte) '|'};
    assertArrayEquals(expected, dst, "multibyte value bytes must each become '*'");
  }

  /**
   * When {@code srcOff > 0}, only the slice starting at {@code srcOff} is processed. Bytes before
   * {@code srcOff} in {@code src} are not read, and bytes before {@code dstOff} in {@code dst} are
   * not written.
   *
   * <p>Strategy: pad src with 5 leading garbage bytes, then verify the output slice at {@code
   * dst[10..10+srcLen)} is correctly masked, and the dst bytes before {@code dstOff=10} remain
   * zero.
   */
  @Test
  void mask_offsetIntoSrc_honored() {
    final var mask = new PiiMask(new int[] {1});
    final byte[] payload = "1=ACME|".getBytes(StandardCharsets.US_ASCII);
    // 5 garbage bytes prepended in src.
    final byte[] src = new byte[5 + payload.length];
    Arrays.fill(src, 0, 5, (byte) 0xFF); // garbage
    System.arraycopy(payload, 0, src, 5, payload.length);

    // dst large enough for dstOff=10 + payload.length
    final byte[] dst = new byte[10 + payload.length];
    mask.mask(src, 5, payload.length, dst, 10);

    // Bytes before dstOff must be untouched (zero-initialised).
    for (int i = 0; i < 10; i++) {
      assertEquals(0, dst[i], "dst[" + i + "] before dstOff must be zero");
    }
    // Payload region must be masked correctly.
    final byte[] slice = Arrays.copyOfRange(dst, 10, 10 + payload.length);
    assertArrayEquals("1=****|".getBytes(StandardCharsets.US_ASCII), slice);
  }

  /**
   * When {@code dstOff > 0}, the output is written starting at {@code dstOff} in the destination
   * buffer. Bytes before {@code dstOff} remain untouched.
   */
  @Test
  void mask_offsetIntoDst_honored() {
    final var mask = new PiiMask(new int[] {1});
    final byte[] src = "1=ACME|".getBytes(StandardCharsets.US_ASCII);
    final int dstOff = 10;
    final byte[] dst = new byte[dstOff + src.length];

    mask.mask(src, 0, src.length, dst, dstOff);

    for (int i = 0; i < dstOff; i++) {
      assertEquals(0, dst[i], "dst[" + i + "] before dstOff must be zero");
    }
    final byte[] slice = Arrays.copyOfRange(dst, dstOff, dstOff + src.length);
    assertArrayEquals("1=****|".getBytes(StandardCharsets.US_ASCII), slice);
  }

  /**
   * Passing the same array as both {@code src} and {@code dst} with the same offset must produce a
   * correct in-place mask. The production code writes each byte before reading the next, so
   * in-place aliasing is safe per the class contract.
   */
  @Test
  void mask_dstAndSrcSameArray_inplace_works() {
    final var mask = new PiiMask(new int[] {1});
    final byte[] buf = "1=ACME|".getBytes(StandardCharsets.US_ASCII);

    mask.mask(buf, 0, buf.length, buf, 0);

    assertArrayEquals("1=****|".getBytes(StandardCharsets.US_ASCII), buf);
  }

  /** {@link PiiMask#mask} must always return {@code srcLen}. */
  @Test
  void mask_returnsSrcLen() {
    final var mask = new PiiMask(new int[] {1});
    final byte[] src = "1=ACME|55=EURUSD|".getBytes(StandardCharsets.US_ASCII);
    final byte[] dst = new byte[src.length];

    final int result = mask.mask(src, 0, src.length, dst, 0);

    assertEquals(src.length, result);
  }

  /** {@code srcLen > src.length} must throw {@link IndexOutOfBoundsException}. */
  @Test
  void mask_oobSrc_throwsIOOBE() {
    final var mask = new PiiMask(new int[] {1});
    final byte[] src = "1=ACME|".getBytes(StandardCharsets.US_ASCII);
    final byte[] dst = new byte[src.length + 1];

    assertThrows(IndexOutOfBoundsException.class, () -> mask.mask(src, 0, src.length + 1, dst, 0));
  }

  /** {@code dstOff + srcLen > dst.length} must throw {@link IndexOutOfBoundsException}. */
  @Test
  void mask_oobDst_throwsIOOBE() {
    final var mask = new PiiMask(new int[] {1});
    final byte[] src = "1=ACME|".getBytes(StandardCharsets.US_ASCII);
    // dst is exactly src.length - 1 bytes starting at offset 0, so dstOff+srcLen overflows.
    final byte[] dst = new byte[src.length - 1];

    assertThrows(IndexOutOfBoundsException.class, () -> mask.mask(src, 0, src.length, dst, 0));
  }

  /** A negative {@code srcOff} must throw {@link IndexOutOfBoundsException}. */
  @Test
  void mask_negativeOffset_throwsIOOBE() {
    final var mask = new PiiMask(new int[] {1});
    final byte[] src = "1=ACME|".getBytes(StandardCharsets.US_ASCII);
    final byte[] dst = new byte[src.length];

    assertThrows(IndexOutOfBoundsException.class, () -> mask.mask(src, -1, src.length, dst, 0));
  }

  @Test
  void mask_nullSrc_throwsNPE() {
    final var mask = new PiiMask(new int[] {1});
    final byte[] dst = new byte[8];

    assertThrows(NullPointerException.class, () -> mask.mask(null, 0, 0, dst, 0));
  }

  @Test
  void mask_nullDst_throwsNPE() {
    final var mask = new PiiMask(new int[] {1});
    final byte[] src = "1=ACME|".getBytes(StandardCharsets.US_ASCII);

    assertThrows(NullPointerException.class, () -> mask.mask(src, 0, src.length, null, 0));
  }

  /**
   * Malformed input with no {@code '='} separator (e.g. {@code "abc|"}) must be copied verbatim
   * without throwing. The production code contract is "do no worse than passthrough."
   */
  @Test
  void mask_malformedField_passthrough() {
    final var mask = new PiiMask(new int[] {1});
    final byte[] src = "abc|".getBytes(StandardCharsets.US_ASCII);
    final byte[] dst = new byte[src.length];

    mask.mask(src, 0, src.length, dst, 0);

    assertArrayEquals("abc|".getBytes(StandardCharsets.US_ASCII), dst);
  }

  /**
   * Input without a trailing pipe (e.g. {@code "1=ACME"}) must still be masked. The field-end scan
   * stops at end-of-buffer, which acts as the implicit field terminator.
   */
  @Test
  void mask_noTrailingPipe_handlesGracefully() {
    final var mask = new PiiMask(new int[] {1});
    final byte[] src = "1=ACME".getBytes(StandardCharsets.US_ASCII);
    final byte[] dst = new byte[src.length];

    mask.mask(src, 0, src.length, dst, 0);

    assertArrayEquals("1=****".getBytes(StandardCharsets.US_ASCII), dst);
  }

  /**
   * Tag 802 (NoPartyIDs) is in the default masklist. A field {@code "802=5|"} must produce {@code
   * "802=*|"}: the single-digit value is replaced with one star.
   */
  @Test
  void mask_largeTagNumber_works() {
    final var mask = PiiMask.withDefaultMask();
    final byte[] src = "802=5|".getBytes(StandardCharsets.US_ASCII);
    final byte[] dst = new byte[src.length];

    mask.mask(src, 0, src.length, dst, 0);

    assertArrayEquals("802=*|".getBytes(StandardCharsets.US_ASCII), dst);
  }
}
