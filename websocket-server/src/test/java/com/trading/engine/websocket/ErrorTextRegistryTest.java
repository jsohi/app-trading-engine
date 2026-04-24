package com.trading.engine.websocket;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.trading.engine.messages.sbe.WebSocketErrorCode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * Tests for {@link ErrorTextRegistry} — verifies all error codes have predefined text and that no
 * free-form text leaks to clients.
 */
final class ErrorTextRegistryTest {

  @ParameterizedTest
  @EnumSource(value = WebSocketErrorCode.class, names = "NULL_VAL", mode = EnumSource.Mode.EXCLUDE)
  void textFor_allNonNullCodes_returnsNonEmptyBytes(final WebSocketErrorCode code) {
    final byte[] text = ErrorTextRegistry.textFor(code);
    assertNotNull(text, "No predefined text for error code: " + code);
    assertTrue(text.length > 0, "Empty text for error code: " + code);
  }

  @ParameterizedTest
  @EnumSource(value = WebSocketErrorCode.class, names = "NULL_VAL", mode = EnumSource.Mode.EXCLUDE)
  void textLength_allNonNullCodes_matchesByteArrayLength(final WebSocketErrorCode code) {
    assertEquals(ErrorTextRegistry.textFor(code).length, ErrorTextRegistry.textLength(code));
  }

  @Test
  void textFor_authenticationFailed_hasExpectedText() {
    assertEquals(
        "Authentication failed",
        new String(ErrorTextRegistry.textFor(WebSocketErrorCode.AuthenticationFailed)));
  }

  @Test
  void textFor_serverShutdown_hasExpectedText() {
    assertEquals(
        "Server shutting down",
        new String(ErrorTextRegistry.textFor(WebSocketErrorCode.ServerShutdown)));
  }

  @Test
  void textFor_slowConsumer_hasExpectedText() {
    assertEquals(
        "Slow consumer disconnected",
        new String(ErrorTextRegistry.textFor(WebSocketErrorCode.SlowConsumer)));
  }

  @Test
  void textFor_nullVal_returnsUnknownFallback() {
    final byte[] text = ErrorTextRegistry.textFor(WebSocketErrorCode.NULL_VAL);
    assertEquals("Unknown error", new String(text));
  }
}
