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
 * com.trading.engine.messages.sbe.WebSocketErrorEncoder#putErrorText(byte[], int, int)}.
 *
 * <p><b>Thread safety.</b> All fields are final and immutable. Safe to share across all threads.
 */
public final class ErrorTextRegistry {

  private static final byte[][] TEXTS = new byte[13][];

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

  private static final byte[] UNKNOWN = "Unknown error".getBytes(StandardCharsets.UTF_8);

  /**
   * Returns the pre-encoded UTF-8 bytes for the given error code.
   *
   * <p>The returned array is a shared instance — callers must NOT modify it.
   *
   * @param code the WebSocket error code
   * @return pre-encoded error text bytes, never null
   */
  public static byte[] textFor(final WebSocketErrorCode code) {
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
