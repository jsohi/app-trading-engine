package com.trading.engine.websocket;

import com.trading.engine.messages.sbe.WebSocketErrorCode;
import java.nio.charset.StandardCharsets;

/**
 * Predefined error text strings per {@link WebSocketErrorCode} value.
 *
 * <p>Error text sent to browser clients is restricted to this registry — no free-form text, no
 * stack traces, no dynamic content. This prevents information leakage per {@code
 * docs/websocket-architecture.md} Section 4 (error sanitization).
 *
 * <p>Strings are pre-encoded as {@code byte[]} for zero-allocation SBE writes via {@link
 * com.trading.engine.messages.sbe.WebSocketErrorEncoder#putErrorText(byte[], int, int)}. The
 * returned arrays are shared instances — callers must NOT modify them. This is a deliberate
 * performance choice: defensive copies would allocate on every error frame.
 *
 * <p><b>Thread safety.</b> All fields are final and immutable. Safe to share across all threads.
 *
 * <p><b>Allocation.</b> Zero allocation after class loading. All byte arrays are pre-computed in
 * the static initializer.
 */
public final class ErrorTextRegistry {

  /** Array size derived from the highest enum value to avoid hardcoded magic numbers. */
  private static final int MAX_CODE_VALUE = maxEnumValue();

  private static final byte[][] TEXTS = new byte[MAX_CODE_VALUE + 1][];

  private static int maxEnumValue() {
    int max = 0;
    for (final WebSocketErrorCode code : WebSocketErrorCode.values()) {
      if (code != WebSocketErrorCode.NULL_VAL && code.value() > max) {
        max = code.value();
      }
    }
    return max;
  }

  static {
    register(WebSocketErrorCode.AuthenticationFailed, "Authentication failed");
    register(WebSocketErrorCode.AuthorizationFailed, "Account not entitled");
    register(WebSocketErrorCode.RateLimitExceeded, "Rate limit exceeded");
    register(WebSocketErrorCode.SessionExpired, "Session expired");
    register(WebSocketErrorCode.InvalidSubscription, "Invalid subscription");
    register(WebSocketErrorCode.HeartbeatTimeout, "Heartbeat timeout");
    register(WebSocketErrorCode.BufferOverflow, "Buffer overflow");
    register(WebSocketErrorCode.VersionMismatch, "Protocol version mismatch");
    register(WebSocketErrorCode.SlowConsumer, "Slow consumer disconnected");
    register(WebSocketErrorCode.ServerShutdown, "Server shutting down");
    register(WebSocketErrorCode.CommandRejected, "Command rejected");
    register(WebSocketErrorCode.SnapshotEntityTooLarge, "Snapshot entity too large");
  }

  private static void register(final WebSocketErrorCode code, final String text) {
    TEXTS[code.value()] = text.getBytes(StandardCharsets.UTF_8);
  }

  private ErrorTextRegistry() {}

  /** Fallback text for unknown, corrupted, or NULL_VAL error codes. */
  private static final byte[] UNKNOWN = "Unknown error".getBytes(StandardCharsets.UTF_8);

  /**
   * Returns the pre-encoded UTF-8 bytes for the given error code.
   *
   * <p>The returned array is a shared instance — callers must NOT modify it. This is a deliberate
   * zero-allocation design for the SBE write path.
   *
   * @param code the WebSocket error code (may be NULL_VAL or an unknown future value)
   * @return pre-encoded error text bytes, never null. Returns "Unknown error" for unregistered
   *     codes.
   */
  public static byte[] textFor(final WebSocketErrorCode code) {
    if (code == null) {
      return UNKNOWN;
    }
    final int idx = code.value();
    if (idx < 0 || idx >= TEXTS.length || TEXTS[idx] == null) {
      return UNKNOWN;
    }
    return TEXTS[idx];
  }

  /**
   * Returns the text length for the given error code, suitable for SBE varData length field.
   *
   * @param code the WebSocket error code
   * @return the byte length of the predefined error text
   */
  public static int textLength(final WebSocketErrorCode code) {
    return textFor(code).length;
  }
}
