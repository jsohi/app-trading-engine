package com.trading.engine.websocket;

import java.nio.charset.StandardCharsets;

/**
 * Extracts the account code ({@code String}) from egress SBE message payloads using pre-computed
 * fixed offsets, enabling account-level entitlement filtering on the drain path.
 *
 * <p>The architecture doc requires: "SubscriptionFilter + UserEntitlementService restrict events to
 * entitled accounts." This class extracts the {@code accountCode} field (FIX tag 1, char[16]) from
 * events that have one, so the drain handler can check {@code session.entitledAccounts().contains
 * (accountCode)}.
 *
 * <p><b>Offsets verified from generated SBE decoders</b> via {@code accountCodeEncodingOffset()} on
 * each decoder class. The absolute offset is {@code MessageHeaderDecoder.ENCODED_LENGTH (8) +
 * accountCodeEncodingOffset()}.
 *
 * <p><b>Threading.</b> All methods are stateless and thread-safe.
 *
 * <p><b>Allocation.</b> Allocates a {@link String} per extraction. This is acceptable because the
 * account check runs on the drain path only when the {@link SubscriptionFilter} already matched
 * (cold path relative to filter misses).
 *
 * @see SubscriptionFilter
 * @see <a href="docs/websocket-architecture.md">WebSocket Architecture — Section 4</a>
 */
public final class AccountExtractor {

  /** SBE message header length in bytes. */
  private static final int HEADER_SIZE = 8;

  /** Account code field length in bytes (SBE char[16]). */
  private static final int ACCOUNT_CODE_LENGTH = 16;

  // Absolute accountCode offsets: HEADER_SIZE + accountCodeEncodingOffset() from generated
  // decoders.
  private static final int OFFSET_ORDER_CREATED = HEADER_SIZE + 123; // template 100
  private static final int OFFSET_ORDER_REJECTED = HEADER_SIZE + 46; // template 101
  private static final int OFFSET_ORDER_FILLED = HEADER_SIZE + 134; // template 102
  // template 103 (OrderCanceled) — NO accountCode field
  private static final int OFFSET_QUOTE_REQUESTED = HEADER_SIZE + 53; // template 104
  private static final int OFFSET_QUOTE_CREATED = HEADER_SIZE + 65; // template 105
  private static final int OFFSET_QUOTE_REJECTED = HEADER_SIZE + 45; // template 106
  private static final int OFFSET_QUOTE_EXPIRED = HEADER_SIZE + 65; // template 107
  private static final int OFFSET_ACCOUNT_LOADED = HEADER_SIZE + 32; // template 110
  private static final int OFFSET_ACCOUNT_LOAD_REJECTED = HEADER_SIZE + 16; // template 111
  private static final int OFFSET_ORDER_CANCEL_REJECTED = HEADER_SIZE + 79; // template 112

  private AccountExtractor() {}

  /**
   * Extract the account code from a raw SBE message payload.
   *
   * <p>Returns {@code null} if:
   *
   * <ul>
   *   <li>The templateId has no account code field (51, 103, 204)
   *   <li>The templateId is not a recognized egress template
   *   <li>The payload is truncated (bounds check fails)
   * </ul>
   *
   * <p>The returned string is trimmed of trailing NUL bytes, matching the SBE char[16] encoding
   * where shorter account codes are NUL-padded on the right.
   *
   * @param templateId the SBE templateId from the message header
   * @param sbePayload the raw SBE message bytes (header + body)
   * @param offset the start offset of the SBE message within the byte array
   * @param length the total length of the SBE message
   * @return the trimmed account code, or {@code null} if the template has no account field or the
   *     payload is truncated. Returns an empty string if the account field is all NUL bytes.
   */
  public static String extractAccountCode(
      final int templateId, final byte[] sbePayload, final int offset, final int length) {

    if (offset < 0 || length < 0 || offset > sbePayload.length) {
      return null;
    }

    final int accountOffset = absoluteAccountOffset(templateId);
    if (accountOffset < 0) {
      return null;
    }

    // Bounds check: use subtraction to avoid integer overflow on pathological offset values.
    if (accountOffset + ACCOUNT_CODE_LENGTH > length
        || accountOffset + ACCOUNT_CODE_LENGTH > sbePayload.length - offset) {
      return null;
    }

    return trimmedAscii(sbePayload, offset + accountOffset, ACCOUNT_CODE_LENGTH);
  }

  /**
   * Returns the absolute byte offset of the accountCode field for the given templateId, or -1 if
   * the template has no account code field.
   *
   * @param templateId the SBE templateId
   * @return absolute offset from message start, or -1
   */
  static int absoluteAccountOffset(final int templateId) {
    return switch (templateId) {
      case 100 -> OFFSET_ORDER_CREATED;
      case 101 -> OFFSET_ORDER_REJECTED;
      case 102 -> OFFSET_ORDER_FILLED;
      // 103 (OrderCanceled) — no accountCode field
      case 104 -> OFFSET_QUOTE_REQUESTED;
      case 105 -> OFFSET_QUOTE_CREATED;
      case 106 -> OFFSET_QUOTE_REJECTED;
      case 107 -> OFFSET_QUOTE_EXPIRED;
      case 110 -> OFFSET_ACCOUNT_LOADED;
      case 111 -> OFFSET_ACCOUNT_LOAD_REJECTED;
      case 112 -> OFFSET_ORDER_CANCEL_REJECTED;
      // 51 (PriceResponse) — prices are not account-specific
      // 204 (PositionSnapshot) — account in repeating group, not extractable with fixed offset
      default -> -1;
    };
  }

  /**
   * Extract a trimmed ASCII string from a byte array, removing trailing NUL bytes. Matches SBE
   * char[N] encoding where shorter strings are NUL-padded on the right.
   *
   * @param src the source byte array
   * @param offset start offset
   * @param maxLength maximum field length
   * @return the trimmed string, never null (may be empty)
   */
  private static String trimmedAscii(final byte[] src, final int offset, final int maxLength) {
    int end = maxLength;
    while (end > 0 && src[offset + end - 1] == 0) {
      end--;
    }
    if (end == 0) {
      return "";
    }
    return new String(src, offset, end, StandardCharsets.US_ASCII);
  }
}
