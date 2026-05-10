package com.trading.engine.fixbridge.rawfix;

import java.util.Arrays;

/**
 * Byte-length-preserving FIX 4.4 PII redaction (§3.5).
 *
 * <p><b>Purpose.</b> Mask the values of regulated-PII FIX tags before a debug-mode {@code RawFix}
 * event is emitted to the browser. Default masklist covers the FIX 4.4 PII set required by GDPR
 * Art. 32, FINRA 4530, and MiFID II RTS 22 §3.5: tags {@code 1} (Account), {@code 109}
 * (OnBehalfOfCompID), {@code 110} (OnBehalfOfSubID), {@code 116} (OnBehalfOfLocationID), {@code
 * 448} (PartyID), {@code 449} (PartyIDSource), {@code 802} (NoPartyIDs), {@code 375}
 * (ContraBroker), {@code 106} (Issuer), {@code 95}/{@code 96} (EncodedText). The masklist is
 * config-driven; jurisdictions add or override at deployment time.
 *
 * <p><b>Wire input.</b> A FIX message delimited EITHER by raw {@code SOH} (0x01) bytes from Artio
 * OR by {@code '|'} (0x7C) bytes (the JSON-friendly substitution used in the {@link
 * com.trading.engine.fixbridge.json.BrowserEvent.RawFix} string payload). The field-terminator scan
 * accepts both bytes interchangeably so the masker can be invoked at any point in the RawFixTap →
 * BrowserEventWriter pipeline regardless of whether SOH→{@code '|'} substitution has already been
 * applied. The terminator byte is preserved verbatim in the output (an SOH-delimited input produces
 * an SOH-delimited output; the writer applies SOH→{@code '|'} substitution at serialisation time
 * per {@link com.trading.engine.fixbridge.json.Utf8JsonStringEmitter#appendRawFixBytes}). Tag/value
 * structure: {@code <digits>=<value><term><digits>=<value><term>...<digits>=<value><term>} where
 * {@code <term>} is either 0x01 or 0x7C; trailing terminator optional.
 *
 * <p><b>Output guarantee.</b> The destination buffer has the same byte length as the source. Masked
 * tag values are overwritten with {@code '*'} (0x2A), one star per source byte — preserving
 * byte-length for UTF-8 multibyte values too (an Account name with an umlaut still masks correctly
 * because we operate on bytes, not codepoints). Tag numbers, the {@code '='} separator, and the
 * field terminator (either {@code '|'} 0x7C or SOH 0x01) are preserved verbatim — the source
 * terminator byte is echoed unchanged into the destination.
 *
 * <p><b>Threading.</b> {@link #mask(byte[], int, int, byte[], int)} is stateless w.r.t. instance
 * fields after construction (only reads the immutable {@link #sortedTagsToMask}), so a single
 * instance is safe to share across threads. Per-instance allocation is bounded by the constructor.
 *
 * <p><b>Allocation.</b> Constructor allocates one {@code int[]} (the sorted masklist). {@link
 * #mask(byte[], int, int, byte[], int)} is zero-alloc.
 *
 * <p><b>Lifecycle.</b> One instance per bridge process; injected into {@code RawFixTap}.
 *
 * <p><b>Dependencies.</b> JDK only.
 */
public final class PiiMask {

  /**
   * Default FIX 4.4 PII masklist (sorted ascending). Sourced from §3.5 — covers regulated-PII tags
   * from GDPR / FINRA / MiFID II.
   */
  public static final int[] DEFAULT_MASK_TAGS = {
    1, // Account
    95, // EncodedTextLen
    96, // EncodedText
    106, // Issuer
    109, // OnBehalfOfCompID
    110, // OnBehalfOfSubID
    116, // OnBehalfOfLocationID
    375, // ContraBroker
    448, // PartyID
    449, // PartyIDSource
    802, // NoPartyIDs (count of party-ID repeating group)
  };

  /** Star byte used as the redaction character. */
  private static final byte STAR = (byte) '*';

  /** Equals byte separating tag from value. */
  private static final byte EQUALS = (byte) '=';

  /** Pipe byte used as the SOH-replacement field separator (post-substitution input). */
  private static final byte PIPE = (byte) '|';

  /**
   * Raw FIX SOH field separator (pre-substitution input from Artio). The masker accepts both {@link
   * #PIPE} and {@link #SOH} as field terminators so it can run on either pre- or post-substitution
   * buffers — preventing the §3.5 PII-redaction regression where raw Artio bytes (SOH-delimited)
   * were treated as one un-masked field.
   */
  private static final byte SOH = (byte) 0x01;

  private final int[] sortedTagsToMask;

  /**
   * Construct a masker with a custom tag list. Defensive copy + sort so the caller cannot mutate
   * the masklist after construction and {@link #isMasked(int)} can use binary search.
   *
   * @param tagsToMask FIX tag numbers to mask (unsorted, may contain duplicates which are
   *     deduplicated)
   * @throws NullPointerException if {@code tagsToMask} is null
   * @throws IllegalArgumentException if any tag is non-positive
   */
  public PiiMask(final int[] tagsToMask) {
    if (tagsToMask == null) {
      throw new NullPointerException("tagsToMask must not be null");
    }
    final int[] copy = Arrays.copyOf(tagsToMask, tagsToMask.length);
    Arrays.sort(copy);
    int distinct = 0;
    int prev = -1;
    for (int i = 0; i < copy.length; i++) {
      if (copy[i] <= 0) {
        throw new IllegalArgumentException("FIX tag must be > 0, was " + copy[i]);
      }
      if (copy[i] != prev) {
        copy[distinct++] = copy[i];
        prev = copy[i];
      }
    }
    this.sortedTagsToMask = Arrays.copyOf(copy, distinct);
  }

  /**
   * Construct a masker with the {@link #DEFAULT_MASK_TAGS} list.
   *
   * @return a masker pre-loaded with the default jurisdiction-spanning PII set
   */
  public static PiiMask withDefaultMask() {
    return new PiiMask(DEFAULT_MASK_TAGS);
  }

  /**
   * Check whether a tag number is in the masklist. Binary-search; zero-alloc.
   *
   * @param tag FIX tag number
   * @return {@code true} iff this tag's value should be redacted
   */
  public boolean isMasked(final int tag) {
    return Arrays.binarySearch(sortedTagsToMask, tag) >= 0;
  }

  /**
   * Mask a FIX message in {@code src} into {@code dst}. Copies non-masked bytes verbatim;
   * overwrites masked-tag value bytes with {@code '*'}. Output length == input length.
   *
   * <p>Behaviour on malformed input: bytes that don't conform to the {@code <digits>=<value>|}
   * shape are copied verbatim — this method does NOT validate FIX structure. Upstream code (Artio)
   * is the authoritative parser; this method's contract is "do no worse than passthrough if the
   * input is malformed."
   *
   * @param src source buffer; field terminators may be either raw SOH (0x01) or {@code '|'} (0x7C)
   *     — both are recognised, and the source terminator byte is preserved verbatim in the output
   * @param srcOff start offset in {@code src}
   * @param srcLen number of bytes to read from {@code src}
   * @param dst destination buffer; MUST have at least {@code srcLen} bytes available from {@code
   *     dstOff}
   * @param dstOff start offset in {@code dst}
   * @return {@code srcLen} (same as the input length — the wire-format invariant)
   * @throws NullPointerException if {@code src} or {@code dst} is null
   * @throws IndexOutOfBoundsException if either slice falls outside its buffer
   */
  public int mask(
      final byte[] src, final int srcOff, final int srcLen, final byte[] dst, final int dstOff) {
    if (src == null) {
      throw new NullPointerException("src must not be null");
    }
    if (dst == null) {
      throw new NullPointerException("dst must not be null");
    }
    if (srcOff < 0 || srcLen < 0 || srcOff + srcLen > src.length) {
      throw new IndexOutOfBoundsException(
          "src slice [" + srcOff + "," + (srcOff + srcLen) + ") out of bounds, len=" + src.length);
    }
    if (dstOff < 0 || dstOff + srcLen > dst.length) {
      throw new IndexOutOfBoundsException(
          "dst slice [" + dstOff + "," + (dstOff + srcLen) + ") out of bounds, len=" + dst.length);
    }

    // Loop scan pointer p mutated across the field walk per CLAUDE.md carve-out for tight
    // buffer scans.
    int p = srcOff;
    final int end = srcOff + srcLen;
    while (p < end) {
      // Field starts at p. Parse tag-digit run until '='.
      // Tag-digit accumulator mutates within this iteration.
      int tag = 0;
      boolean sawDigit = false;
      int digitsEnd = p;
      while (digitsEnd < end) {
        final byte b = src[digitsEnd];
        if (b == EQUALS) {
          break;
        }
        if (b < '0' || b > '9') {
          // Malformed field — passthrough. Copy from p to next pipe (or end).
          break;
        }
        tag = tag * 10 + (b - '0');
        sawDigit = true;
        digitsEnd++;
      }

      // Find the field terminator (next '|' OR SOH, or end-of-buffer). Accepting both
      // terminators lets the masker run on raw Artio bytes (SOH-delimited) as well as
      // post-substitution bytes (pipe-delimited) — see class Javadoc.
      int fieldEnd = digitsEnd;
      while (fieldEnd < end && src[fieldEnd] != PIPE && src[fieldEnd] != SOH) {
        fieldEnd++;
      }
      // Include the trailing terminator if present so the next iteration starts on the next field.
      final int nextStart = (fieldEnd < end) ? fieldEnd + 1 : fieldEnd;

      if (!sawDigit || digitsEnd >= end || src[digitsEnd] != EQUALS) {
        // Malformed — copy verbatim.
        copy(src, p, dst, dstOff + (p - srcOff), nextStart - p);
        p = nextStart;
        continue;
      }

      // Copy tag digits + '=' verbatim.
      final int tagBytes = digitsEnd - p + 1; // includes '='
      copy(src, p, dst, dstOff + (p - srcOff), tagBytes);

      // Value spans (digitsEnd + 1) to fieldEnd. Mask or copy based on tag membership.
      final int valStart = digitsEnd + 1;
      final int valLen = fieldEnd - valStart;
      if (isMasked(tag)) {
        for (int i = 0; i < valLen; i++) {
          dst[dstOff + (valStart - srcOff) + i] = STAR;
        }
      } else {
        copy(src, valStart, dst, dstOff + (valStart - srcOff), valLen);
      }

      // Copy the trailing terminator (SOH or PIPE) verbatim if it exists. Preserving the source
      // terminator byte lets the writer apply SOH→'|' substitution at serialisation time without
      // double-substitution.
      if (fieldEnd < end) {
        dst[dstOff + (fieldEnd - srcOff)] = src[fieldEnd];
      }
      p = nextStart;
    }
    return srcLen;
  }

  /**
   * Returns the masklist as a defensive copy (sorted ascending). For test assertions only —
   * production code calls {@link #isMasked(int)} on the hot path.
   *
   * @return defensive copy of the sorted masklist
   */
  public int[] tagsVisibleForTesting() {
    return Arrays.copyOf(sortedTagsToMask, sortedTagsToMask.length);
  }

  private static void copy(
      final byte[] src, final int srcOff, final byte[] dst, final int dstOff, final int len) {
    System.arraycopy(src, srcOff, dst, dstOff, len);
  }
}
