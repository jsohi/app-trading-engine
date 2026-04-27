package com.trading.engine.websocket;

import com.trading.engine.projections.SymbolPacker;

/**
 * Extracts the packed symbol ({@code long}) from egress SBE message payloads using pre-computed
 * fixed offsets, enabling zero-allocation symbol lookup in the {@link SubscriptionFilter} on the
 * drain path.
 *
 * <p>Each SBE event template places the 8-byte {@code Symbol} field (FIX tag 55) at a different
 * offset within the message body. This class maps templateId to the absolute byte offset (SBE
 * header + field encoding offset) and delegates to {@link SymbolPacker#pack(byte[], int)} for
 * little-endian packing into a {@code long}.
 *
 * <p><b>Offsets verified from generated SBE decoders</b> via {@code symbolEncodingOffset()} on each
 * decoder class. The absolute offset is {@code MessageHeaderDecoder.ENCODED_LENGTH (8) +
 * symbolEncodingOffset()}.
 *
 * <p><b>Threading.</b> All methods are stateless and thread-safe.
 *
 * <p><b>Allocation.</b> Zero allocation — delegates to {@link SymbolPacker#pack(byte[], int)}.
 *
 * @see SymbolPacker
 * @see SubscriptionFilter
 * @see <a href="docs/websocket-architecture.md">WebSocket Architecture — Section 1</a>
 */
public final class SymbolExtractor {

  /**
   * Sentinel value indicating the template has no extractable symbol field. Zero is safe because
   * {@link SymbolPacker#pack(String)} rejects empty/null symbols, and valid ASCII symbols always
   * produce a non-zero packed long (at least one non-NUL byte in the 8-byte field).
   */
  public static final long UNKNOWN_SYMBOL = 0L;

  /** SBE message header length in bytes. */
  private static final int HEADER_SIZE = 8;

  /** Symbol field length in bytes (SBE char[8]). */
  private static final int SYMBOL_LENGTH = 8;

  // Absolute symbol offsets: HEADER_SIZE + symbolEncodingOffset() from generated decoders.
  // Verified from: OrderCreatedEventDecoder, OrderRejectedEventDecoder, etc.
  private static final int OFFSET_ORDER_CREATED = HEADER_SIZE + 76; // template 100
  private static final int OFFSET_ORDER_REJECTED = HEADER_SIZE + 36; // template 101
  private static final int OFFSET_ORDER_FILLED = HEADER_SIZE + 76; // template 102
  private static final int OFFSET_ORDER_CANCELED = HEADER_SIZE + 76; // template 103
  private static final int OFFSET_QUOTE_REQUESTED = HEADER_SIZE + 36; // template 104
  private static final int OFFSET_QUOTE_CREATED = HEADER_SIZE + 56; // template 105
  private static final int OFFSET_QUOTE_REJECTED = HEADER_SIZE + 36; // template 106
  private static final int OFFSET_QUOTE_EXPIRED = HEADER_SIZE + 56; // template 107
  private static final int OFFSET_PRICE_RESPONSE = HEADER_SIZE + 20; // template 51

  private SymbolExtractor() {}

  /**
   * Extract the packed symbol from a raw SBE message payload.
   *
   * <p>Returns {@link #UNKNOWN_SYMBOL} if:
   *
   * <ul>
   *   <li>The templateId has no symbol field (110, 111, 112, 204)
   *   <li>The templateId is not a recognized egress template
   *   <li>The payload is truncated (bounds check fails)
   * </ul>
   *
   * @param templateId the SBE templateId from the message header
   * @param sbePayload the raw SBE message bytes (header + body)
   * @param offset the start offset of the SBE message within the byte array
   * @param length the total length of the SBE message
   * @return the packed symbol as a little-endian {@code long}, or {@link #UNKNOWN_SYMBOL}
   */
  public static long extractPackedSymbol(
      final int templateId, final byte[] sbePayload, final int offset, final int length) {

    if (offset < 0 || length < 0) {
      return UNKNOWN_SYMBOL;
    }

    final int symbolOffset = absoluteSymbolOffset(templateId);
    if (symbolOffset < 0) {
      return UNKNOWN_SYMBOL;
    }

    // Bounds check against actual array length — a caller may pass offset/length that exceed the
    // backing array. A truncated message must not crash the drain loop for all sessions.
    final int endIndex = offset + symbolOffset + SYMBOL_LENGTH;
    if (endIndex > sbePayload.length || symbolOffset + SYMBOL_LENGTH > length) {
      return UNKNOWN_SYMBOL;
    }

    return SymbolPacker.pack(sbePayload, offset + symbolOffset);
  }

  /**
   * Returns the absolute byte offset of the symbol field for the given templateId, or -1 if the
   * template has no extractable symbol field.
   *
   * @param templateId the SBE templateId
   * @return absolute offset from message start, or -1
   */
  static int absoluteSymbolOffset(final int templateId) {
    return switch (templateId) {
      case 100 -> OFFSET_ORDER_CREATED;
      case 101 -> OFFSET_ORDER_REJECTED;
      case 102 -> OFFSET_ORDER_FILLED;
      case 103 -> OFFSET_ORDER_CANCELED;
      case 104 -> OFFSET_QUOTE_REQUESTED;
      case 105 -> OFFSET_QUOTE_CREATED;
      case 106 -> OFFSET_QUOTE_REJECTED;
      case 107 -> OFFSET_QUOTE_EXPIRED;
      case 51 -> OFFSET_PRICE_RESPONSE;
      // Templates without a fixed-offset symbol field:
      // 110 (AccountLoaded), 111 (AccountLoadRejected) — no symbol
      // 112 (OrderCancelRejected) — no symbol
      // 204 (PositionSnapshot) — symbol in repeating group, not extractable
      // 108/109/113-116 — internal events, never delivered to WebSocket clients
      default -> -1;
    };
  }
}
