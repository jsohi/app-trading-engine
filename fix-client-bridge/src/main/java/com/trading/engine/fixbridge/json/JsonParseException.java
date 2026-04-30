package com.trading.engine.fixbridge.json;

/**
 * Sentinel-singleton exception class thrown by {@link BrowserMessageReader} on inbound JSON parse
 * failures, and by {@code JsonToFixTranslator} on inbound translation failures that share the same
 * taxonomy (currently only {@link #PRICE_PRECISION} — locked §3).
 *
 * <p><b>Purpose.</b> Distinguishes the four failure classes the inbound side surfaces — {@link
 * #MALFORMED}, {@link #UNKNOWN_TYPE}, {@link #TOO_LARGE}, {@link #PRICE_PRECISION} — without
 * allocating a fresh exception per failed message. The caller switches on identity (e.g. {@code if
 * (e == JsonParseException.MALFORMED)}) to decide how to respond on the wire.
 *
 * <p><b>Threading.</b> The singletons are immutable and have a pre-set message + no stack trace, so
 * they are safe to share across all Netty worker threads. They behave like Java's "stackless"
 * pattern (see {@code org.agrona.DirectBufferIndexException} and similar).
 *
 * <p><b>Allocation.</b> Zero — the four singletons are loaded once at class-init time and reused
 * forever. Throwing them does not allocate a stack trace; {@code writableStackTrace=false} is
 * passed to the {@link RuntimeException} constructor. Construction outside this class is forbidden:
 * the constructor is private and the class is {@code final}.
 *
 * <p><b>Lifecycle.</b> Class-init time only. No close / dispose semantics.
 *
 * <p><b>Dependencies.</b> JDK only.
 *
 * <p><b>Visibility.</b> Public so that both {@link BrowserMessageReader} (in this package) and
 * {@code com.trading.engine.fixbridge.translator.JsonToFixTranslator} can throw the same singletons
 * — the dispatcher (Phase 6) catches a single exception type and switches on identity for the
 * wire-protocol response. The constructor remains private; the only valid instances are the four
 * declared singletons.
 */
public final class JsonParseException extends RuntimeException {

  private static final long serialVersionUID = 1L;

  /** JSON could not be parsed (truncated brace, bad escape, non-UTF-8 byte, malformed number). */
  public static final JsonParseException MALFORMED = new JsonParseException("malformed");

  /** Top-level {@code "type"} value did not match a known message kind. */
  public static final JsonParseException UNKNOWN_TYPE = new JsonParseException("unknown-type");

  /** Frame size exceeds {@link BrowserMessageReader#MAX_BYTES} (64 KiB). */
  public static final JsonParseException TOO_LARGE = new JsonParseException("too-large");

  /** Decimal-string field has more than 8 fractional digits (locked §3). */
  public static final JsonParseException PRICE_PRECISION =
      new JsonParseException("price-precision");

  private JsonParseException(final String reason) {
    // writableStackTrace=false: skip Throwable.fillInStackTrace, which is the dominant
    // alloc cost when an exception is thrown. The reason String is enough for diagnosis
    // — context (offset, frame ID) is logged by the caller from its own state.
    super(reason, null, /* enableSuppression= */ false, /* writableStackTrace= */ false);
  }

  /**
   * Returns the reason tag matching this singleton (one of {@code "malformed"}, {@code
   * "unknown-type"}, {@code "too-large"}, {@code "price-precision"}).
   *
   * @return reason tag suitable for inclusion in an outbound {@code Error{reason:"..."}} event
   */
  public String reason() {
    return getMessage();
  }
}
