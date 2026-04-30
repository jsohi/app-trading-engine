package com.trading.engine.fixbridge.json;

import com.trading.engine.gateway.FixedPoint;
import io.netty.buffer.ByteBuf;
import java.nio.charset.StandardCharsets;

/**
 * Strict, zero-allocation JSON parser for the six inbound browser-to-bridge message types.
 *
 * <p><b>Purpose.</b> Decode a single inbound JSON frame from a Netty {@link ByteBuf} into a
 * pre-allocated {@link MutableParsedMessage} flyweight, performing all required validation up
 * front. Translators (Phase 4) operate exclusively on the flyweight so they never see the raw JSON
 * bytes.
 *
 * <p><b>Threading.</b> Stateless — every method is {@code static}. Safe to share across threads.
 *
 * <p><b>Allocation.</b> Zero on the hot path. The parser uses no {@code String}, no {@code
 * BigDecimal}, no autoboxing, no temporary arrays. Numeric decoding is done digit-by-digit on the
 * scratch buffer; field-name matching is done byte-by-byte against pre-computed {@code byte[]}
 * keyword constants.
 *
 * <p><b>Lifecycle.</b> Class-init only.
 *
 * <p><b>Dependencies.</b> Netty {@link ByteBuf} (read-only API).
 *
 * <p><b>Strictness.</b>
 *
 * <ul>
 *   <li>Total frame size must be {@code <=} {@link #MAX_BYTES}; oversize → {@link
 *       JsonParseException#TOO_LARGE}.
 *   <li>Top level must be a single JSON object containing a {@code "type"} key whose value is a
 *       JSON string naming a known message kind. Field order is not constrained — dispatch is
 *       performed once {@code "type"} is observed, regardless of position.
 *   <li>Nested objects/arrays are rejected as values; the parser reads exactly one top-level object
 *       whose values must all be JSON strings (any deeper structure → {@link
 *       JsonParseException#MALFORMED}).
 *   <li>Unknown top-level keys are rejected (forward-compat is opt-in: any new field requires a
 *       parser change).
 *   <li>Decimal-string fields with {@code >} 8 fractional digits → {@link
 *       JsonParseException#PRICE_PRECISION} (locked §3).
 *   <li>Trailing data after the closing {@code }} is rejected as {@link
 *       JsonParseException#MALFORMED}.
 * </ul>
 *
 * <p><b>Encoding.</b> The parser tolerates UTF-8 in string values (it byte-copies them verbatim
 * into {@link MutableParsedMessage#scratch}), but field names and enum values are interpreted as
 * 7-bit ASCII. Invalid escape sequences and bare control bytes inside string literals are rejected.
 */
public final class BrowserMessageReader {

  /**
   * Maximum frame size in bytes (64 KiB). Mirrors {@code maxJsonBytes} in {@link
   * com.trading.engine.fixbridge.FixClientBridgeConfig} and the Netty {@code JsonObjectDecoder}
   * cap.
   */
  public static final int MAX_BYTES = 65536;

  /**
   * Per fixed-point convention — sourced from {@link FixedPoint#FIXED_POINT_SCALE} so a future
   * scale-factor change in the canonical location propagates here automatically (locked §9).
   */
  private static final int FIXED_POINT_SCALE = FixedPoint.FIXED_POINT_SCALE;

  // ---------------------------------------------------------------------------
  // Pre-computed keyword constants. Stored as byte[] so matching is a tight
  // byte-for-byte loop with no String construction.
  // ---------------------------------------------------------------------------

  // Keys
  private static final byte[] K_TYPE = bytes("type");
  private static final byte[] K_TOKEN = bytes("token");
  private static final byte[] K_REQ_ID = bytes("reqId");
  private static final byte[] K_SYMBOL = bytes("symbol");
  private static final byte[] K_SIDE = bytes("side");
  private static final byte[] K_QTY = bytes("qty");
  private static final byte[] K_PRICE = bytes("price");
  private static final byte[] K_CL_ORD_ID = bytes("clOrdId");
  private static final byte[] K_ORIG_CL_ORD_ID = bytes("origClOrdId");
  private static final byte[] K_QUOTE_ID = bytes("quoteId");
  private static final byte[] K_ORD_TYPE = bytes("ordType");
  private static final byte[] K_TIME_IN_FORCE = bytes("timeInForce");
  private static final byte[] K_ACCOUNT = bytes("account");

  // Type values
  private static final byte[] V_AUTH = bytes("Auth");
  private static final byte[] V_QUOTE_REQUEST = bytes("QuoteRequest");
  private static final byte[] V_ACCEPT_QUOTE = bytes("AcceptQuote");
  private static final byte[] V_REJECT_QUOTE = bytes("RejectQuote");
  private static final byte[] V_NEW_ORDER_SINGLE = bytes("NewOrderSingle");
  private static final byte[] V_CANCEL_ORDER = bytes("CancelOrder");

  // Side values
  private static final byte[] V_BUY = bytes("Buy");
  private static final byte[] V_SELL = bytes("Sell");

  // OrdType values
  private static final byte[] V_MARKET = bytes("Market");
  private static final byte[] V_LIMIT = bytes("Limit");

  // TimeInForce values (FIX 4.4 enum — see com.trading.engine.fixbridge.json.MutableParsedMessage)
  private static final byte[] V_DAY = bytes("DAY");
  private static final byte[] V_GTC = bytes("GTC");
  private static final byte[] V_IOC = bytes("IOC");
  private static final byte[] V_FOK = bytes("FOK");
  private static final byte[] V_GTD = bytes("GTD");

  private BrowserMessageReader() {}

  /**
   * Parses the contents of {@code src} into {@code out}. The caller MUST have already validated
   * that {@code src.readableBytes() <=} {@link #MAX_BYTES}; this method also re-checks defensively.
   *
   * <p>On success, {@code out} is mutated with the decoded fields and the method returns the
   * message type ({@code MutableParsedMessage.TYPE_*}). On failure the contents of {@code out} are
   * unspecified; the caller MUST treat the flyweight as dirty and call {@link
   * MutableParsedMessage#reset()} before the next parse attempt.
   *
   * <p>Reads from {@code src} via {@link ByteBuf#getByte(int)} only — the read index is NOT
   * advanced. The caller controls when (or whether) to skip past the consumed bytes.
   *
   * @param src inbound frame buffer; the parser inspects the readable region in place
   * @param out caller-owned flyweight; reset by this method before population
   * @return one of the {@code MutableParsedMessage.TYPE_*} sentinels
   * @throws JsonParseException one of the four singleton instances on validation failure
   */
  public static int parse(final ByteBuf src, final MutableParsedMessage out) {
    final int srcLen = src.readableBytes();
    if (srcLen > MAX_BYTES) {
      throw JsonParseException.TOO_LARGE;
    }

    out.reset();

    // Copy verbatim into the flyweight scratch so all downstream slices reference a single
    // heap byte[] (zero-alloc, simpler ownership).
    src.getBytes(src.readerIndex(), out.scratch, 0, srcLen);

    final byte[] buf = out.scratch;
    // Buffer scan pointer mutated across the parse loop.
    int p = skipWs(buf, 0, srcLen);

    if (p >= srcLen || buf[p] != '{') {
      throw JsonParseException.MALFORMED;
    }
    p++;
    int depth = 1;

    boolean first = true;
    while (true) {
      p = skipWs(buf, p, srcLen);
      if (p >= srcLen) {
        throw JsonParseException.MALFORMED;
      }
      if (buf[p] == '}') {
        p++;
        depth--;
        break;
      }

      if (!first) {
        if (buf[p] != ',') {
          throw JsonParseException.MALFORMED;
        }
        p++;
        p = skipWs(buf, p, srcLen);
      }
      first = false;

      // --- key ---
      if (p >= srcLen || buf[p] != '"') {
        throw JsonParseException.MALFORMED;
      }
      final int keyStart = p + 1;
      final int keyEnd = scanStringEnd(buf, keyStart, srcLen);
      // No escapes allowed in keys (they must be plain ASCII).
      for (int i = keyStart; i < keyEnd; i++) {
        if (buf[i] == '\\') {
          throw JsonParseException.MALFORMED;
        }
      }
      p = keyEnd + 1; // past closing quote
      p = skipWs(buf, p, srcLen);

      if (p >= srcLen || buf[p] != ':') {
        throw JsonParseException.MALFORMED;
      }
      p++;
      p = skipWs(buf, p, srcLen);

      // --- value: must be a JSON string for every supported field ---
      if (p >= srcLen) {
        throw JsonParseException.MALFORMED;
      }
      if (buf[p] == '{' || buf[p] == '[') {
        // No supported field is a nested structure — every field value is a JSON string. Nested
        // objects/arrays exceed the wire-protocol contract (which enforces a max depth of 2 via
        // top-level-object → primitive value) and are surfaced as malformed.
        throw JsonParseException.MALFORMED;
      }
      if (buf[p] != '"') {
        // Numbers, booleans, null are not used in our wire protocol.
        throw JsonParseException.MALFORMED;
      }
      final int valStart = p + 1;
      final int valEnd = scanStringEnd(buf, valStart, srcLen);
      // String literals must be free of un-escaped backslash; the wire protocol uses pure ASCII
      // values (FIX symbols, decimals, JWTs) so any backslash is suspect.
      for (int i = valStart; i < valEnd; i++) {
        if (buf[i] == '\\') {
          throw JsonParseException.MALFORMED;
        }
      }
      final int valLen = valEnd - valStart;
      p = valEnd + 1; // past closing quote

      // --- dispatch ---
      dispatchKey(buf, keyStart, keyEnd - keyStart, valStart, valLen, out);
    }

    // Trailing whitespace tolerated; trailing data is not.
    p = skipWs(buf, p, srcLen);
    if (p != srcLen) {
      throw JsonParseException.MALFORMED;
    }
    if (depth != 0) {
      throw JsonParseException.MALFORMED;
    }

    if (out.type == MutableParsedMessage.TYPE_NONE) {
      // No "type" field was present.
      throw JsonParseException.UNKNOWN_TYPE;
    }
    return out.type;
  }

  // ---------------------------------------------------------------------------
  // Key dispatch.
  // ---------------------------------------------------------------------------

  /**
   * Match the key bytes against the known set and stash the value slice into the matching primitive
   * header on {@code out}. Unknown keys → {@link JsonParseException#MALFORMED}.
   */
  private static void dispatchKey(
      final byte[] buf,
      final int keyOff,
      final int keyLen,
      final int valOff,
      final int valLen,
      final MutableParsedMessage out) {
    if (eq(buf, keyOff, keyLen, K_TYPE)) {
      out.type = decodeType(buf, valOff, valLen);
      return;
    }
    if (eq(buf, keyOff, keyLen, K_TOKEN)) {
      out.tokenOff = valOff;
      out.tokenLen = valLen;
      return;
    }
    if (eq(buf, keyOff, keyLen, K_REQ_ID)) {
      out.reqIdOff = valOff;
      out.reqIdLen = valLen;
      return;
    }
    if (eq(buf, keyOff, keyLen, K_SYMBOL)) {
      out.symbolOff = valOff;
      out.symbolLen = valLen;
      return;
    }
    if (eq(buf, keyOff, keyLen, K_SIDE)) {
      out.side = decodeSide(buf, valOff, valLen);
      return;
    }
    if (eq(buf, keyOff, keyLen, K_QTY)) {
      out.qtyOff = valOff;
      out.qtyLen = valLen;
      out.qty = decodeFixedPoint(buf, valOff, valLen);
      return;
    }
    if (eq(buf, keyOff, keyLen, K_PRICE)) {
      out.priceOff = valOff;
      out.priceLen = valLen;
      // Validate precision but do NOT eagerly produce an int64 for price — the translator wants
      // the raw ASCII to round-trip into Artio's DecimalFloat parser without double-rounding.
      validateFixedPointPrecision(buf, valOff, valLen);
      return;
    }
    if (eq(buf, keyOff, keyLen, K_CL_ORD_ID)) {
      out.clOrdIdOff = valOff;
      out.clOrdIdLen = valLen;
      return;
    }
    if (eq(buf, keyOff, keyLen, K_ORIG_CL_ORD_ID)) {
      out.origClOrdIdOff = valOff;
      out.origClOrdIdLen = valLen;
      return;
    }
    if (eq(buf, keyOff, keyLen, K_QUOTE_ID)) {
      out.quoteIdOff = valOff;
      out.quoteIdLen = valLen;
      return;
    }
    if (eq(buf, keyOff, keyLen, K_ORD_TYPE)) {
      out.ordType = decodeOrdType(buf, valOff, valLen);
      return;
    }
    if (eq(buf, keyOff, keyLen, K_TIME_IN_FORCE)) {
      out.timeInForce = decodeTif(buf, valOff, valLen);
      return;
    }
    if (eq(buf, keyOff, keyLen, K_ACCOUNT)) {
      out.accountOff = valOff;
      out.accountLen = valLen;
      return;
    }
    // Unknown top-level key.
    throw JsonParseException.MALFORMED;
  }

  // ---------------------------------------------------------------------------
  // Enum decoders.
  // ---------------------------------------------------------------------------

  private static int decodeType(final byte[] buf, final int off, final int len) {
    if (len < 1) {
      throw JsonParseException.UNKNOWN_TYPE;
    }
    // Switch on first byte to fan out; second byte disambiguates Auth/AcceptQuote.
    switch (buf[off]) {
      case 'A':
        if (len >= 2 && buf[off + 1] == 'u') {
          return eq(buf, off, len, V_AUTH) ? MutableParsedMessage.TYPE_AUTH : badType();
        }
        if (len >= 2 && buf[off + 1] == 'c') {
          return eq(buf, off, len, V_ACCEPT_QUOTE)
              ? MutableParsedMessage.TYPE_ACCEPT_QUOTE
              : badType();
        }
        return badType();
      case 'Q':
        return eq(buf, off, len, V_QUOTE_REQUEST)
            ? MutableParsedMessage.TYPE_QUOTE_REQUEST
            : badType();
      case 'R':
        return eq(buf, off, len, V_REJECT_QUOTE)
            ? MutableParsedMessage.TYPE_REJECT_QUOTE
            : badType();
      case 'N':
        return eq(buf, off, len, V_NEW_ORDER_SINGLE)
            ? MutableParsedMessage.TYPE_NEW_ORDER_SINGLE
            : badType();
      case 'C':
        return eq(buf, off, len, V_CANCEL_ORDER)
            ? MutableParsedMessage.TYPE_CANCEL_ORDER
            : badType();
      default:
        return badType();
    }
  }

  private static int badType() {
    throw JsonParseException.UNKNOWN_TYPE;
  }

  private static byte decodeSide(final byte[] buf, final int off, final int len) {
    if (eq(buf, off, len, V_BUY)) {
      return MutableParsedMessage.SIDE_BUY;
    }
    if (eq(buf, off, len, V_SELL)) {
      return MutableParsedMessage.SIDE_SELL;
    }
    throw JsonParseException.MALFORMED;
  }

  private static byte decodeOrdType(final byte[] buf, final int off, final int len) {
    if (eq(buf, off, len, V_MARKET)) {
      return MutableParsedMessage.ORDTYPE_MARKET;
    }
    if (eq(buf, off, len, V_LIMIT)) {
      return MutableParsedMessage.ORDTYPE_LIMIT;
    }
    throw JsonParseException.MALFORMED;
  }

  private static byte decodeTif(final byte[] buf, final int off, final int len) {
    if (eq(buf, off, len, V_DAY)) {
      return MutableParsedMessage.TIF_DAY;
    }
    if (eq(buf, off, len, V_GTC)) {
      return MutableParsedMessage.TIF_GTC;
    }
    if (eq(buf, off, len, V_IOC)) {
      return MutableParsedMessage.TIF_IOC;
    }
    if (eq(buf, off, len, V_FOK)) {
      return MutableParsedMessage.TIF_FOK;
    }
    if (eq(buf, off, len, V_GTD)) {
      return MutableParsedMessage.TIF_GTD;
    }
    throw JsonParseException.MALFORMED;
  }

  // ---------------------------------------------------------------------------
  // Decimal-string handling.
  // ---------------------------------------------------------------------------

  /**
   * Decodes an ASCII decimal slice into a fixed-point int64 with implicit scale 10^-8. Rejects
   * overflow (>{@link Long#MAX_VALUE}) as {@link JsonParseException#MALFORMED} and rejects more
   * than 8 fractional digits as {@link JsonParseException#PRICE_PRECISION}.
   */
  private static long decodeFixedPoint(final byte[] buf, final int off, final int len) {
    if (len <= 0) {
      throw JsonParseException.MALFORMED;
    }
    int p = off;
    final int end = off + len;
    final boolean negative;
    if (buf[p] == '-') {
      negative = true;
      p++;
    } else {
      negative = false;
    }
    if (p >= end) {
      throw JsonParseException.MALFORMED;
    }

    // --- whole part ---
    long whole = 0L;
    boolean sawWholeDigit = false;
    while (p < end) {
      final byte b = buf[p];
      if (b == '.') {
        break;
      }
      if (b < '0' || b > '9') {
        throw JsonParseException.MALFORMED;
      }
      final int d = b - '0';
      // Detect overflow: whole > (Long.MAX_VALUE - d) / 10
      if (whole > (Long.MAX_VALUE - d) / 10L) {
        throw JsonParseException.MALFORMED;
      }
      whole = whole * 10L + d;
      sawWholeDigit = true;
      p++;
    }

    // --- optional fractional part ---
    // fracDigits and frac mutate across the digit loop.
    int fracDigits = 0;
    long frac = 0L;
    if (p < end && buf[p] == '.') {
      p++;
      // RFC 8259 §6: a JSON number with a `.` MUST be followed by at least one digit. Strictness
      // contract from the class-level Javadoc — `5.` is rejected as MALFORMED.
      if (p >= end) {
        throw JsonParseException.MALFORMED;
      }
      while (p < end) {
        final byte b = buf[p];
        if (b < '0' || b > '9') {
          throw JsonParseException.MALFORMED;
        }
        if (fracDigits >= FIXED_POINT_SCALE) {
          // 9th fractional digit. If it's a zero we tolerate (still exact at 10^-8); otherwise
          // reject as PRICE_PRECISION per locked §3.
          if (b != '0') {
            throw JsonParseException.PRICE_PRECISION;
          }
          fracDigits++;
          p++;
          continue;
        }
        frac = frac * 10L + (b - '0');
        fracDigits++;
        p++;
      }
    }

    if (!sawWholeDigit && fracDigits == 0) {
      throw JsonParseException.MALFORMED;
    }

    // Pad fraction to exactly 8 digits.
    final int padDigits =
        FIXED_POINT_SCALE - (fracDigits > FIXED_POINT_SCALE ? FIXED_POINT_SCALE : fracDigits);
    for (int i = 0; i < padDigits; i++) {
      frac *= 10L;
    }

    // Compose: whole * 10^8 + frac. Detect overflow.
    final long pow10 = 100_000_000L;
    if (whole > Long.MAX_VALUE / pow10) {
      throw JsonParseException.MALFORMED;
    }
    final long scaledWhole = whole * pow10;
    if (scaledWhole > Long.MAX_VALUE - frac) {
      throw JsonParseException.MALFORMED;
    }
    final long magnitude = scaledWhole + frac;
    if (negative) {
      // -magnitude can never overflow because magnitude is in [0, Long.MAX_VALUE].
      return -magnitude;
    }
    return magnitude;
  }

  /**
   * Validate-only counterpart to {@link #decodeFixedPoint} — used for fields where the translator
   * wants to forward the raw ASCII to FIX. Throws {@link JsonParseException#PRICE_PRECISION} on
   * &gt;8 nonzero fractional digits and {@link JsonParseException#MALFORMED} on bad shape.
   */
  private static void validateFixedPointPrecision(final byte[] buf, final int off, final int len) {
    if (len <= 0) {
      throw JsonParseException.MALFORMED;
    }
    int p = off;
    final int end = off + len;
    if (buf[p] == '-') {
      p++;
    }
    if (p >= end) {
      throw JsonParseException.MALFORMED;
    }
    boolean sawDigit = false;
    while (p < end && buf[p] != '.') {
      final byte b = buf[p];
      if (b < '0' || b > '9') {
        throw JsonParseException.MALFORMED;
      }
      sawDigit = true;
      p++;
    }
    int fracDigits = 0;
    if (p < end && buf[p] == '.') {
      p++;
      // RFC 8259 §6: a `.` must be followed by ≥1 fractional digit. Reject "5." here too so the
      // validate-only path stays consistent with decodeFixedPoint.
      if (p >= end) {
        throw JsonParseException.MALFORMED;
      }
      while (p < end) {
        final byte b = buf[p];
        if (b < '0' || b > '9') {
          throw JsonParseException.MALFORMED;
        }
        if (fracDigits >= FIXED_POINT_SCALE && b != '0') {
          throw JsonParseException.PRICE_PRECISION;
        }
        fracDigits++;
        sawDigit = true;
        p++;
      }
    }
    if (!sawDigit) {
      throw JsonParseException.MALFORMED;
    }
  }

  // ---------------------------------------------------------------------------
  // Low-level scanning helpers.
  // ---------------------------------------------------------------------------

  /**
   * Scan until the closing un-escaped {@code "} byte. The opening quote is at {@code start - 1};
   * scanning starts at {@code start}.
   *
   * @return offset of the closing quote
   */
  private static int scanStringEnd(final byte[] buf, final int start, final int end) {
    for (int i = start; i < end; i++) {
      final byte b = buf[i];
      if (b == '"') {
        return i;
      }
      // Reject control bytes. JSON forbids unescaped 0x00..0x1F in strings.
      if (b >= 0 && b < 0x20) {
        throw JsonParseException.MALFORMED;
      }
    }
    throw JsonParseException.MALFORMED;
  }

  private static int skipWs(final byte[] buf, final int start, final int end) {
    int p = start;
    while (p < end) {
      final byte b = buf[p];
      if (b != ' ' && b != '\t' && b != '\n' && b != '\r') {
        return p;
      }
      p++;
    }
    return p;
  }

  private static boolean eq(final byte[] buf, final int off, final int len, final byte[] needle) {
    if (len != needle.length) {
      return false;
    }
    for (int i = 0; i < len; i++) {
      if (buf[off + i] != needle[i]) {
        return false;
      }
    }
    return true;
  }

  private static byte[] bytes(final String s) {
    return s.getBytes(StandardCharsets.US_ASCII);
  }
}
