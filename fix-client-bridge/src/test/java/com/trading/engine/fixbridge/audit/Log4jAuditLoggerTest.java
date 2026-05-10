package com.trading.engine.fixbridge.audit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.message.Message;
import org.apache.logging.log4j.spi.AbstractLogger;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link Log4jAuditLogger}.
 *
 * <p>The collaborator under test wraps a Log4j2 {@link org.apache.logging.log4j.Logger}; tests
 * inject a hand-rolled subclass of {@link AbstractLogger} ({@link CapturingLogger}) that captures
 * every {@code info(String)} call so assertions can inspect the JSON line directly. Mockito is not
 * available on this module's test classpath, so the capture sink is a real (lightweight) subclass.
 *
 * <p>Coverage:
 *
 * <ul>
 *   <li>First record's {@code prevSha256} is the all-zero hash (chain genesis).
 *   <li>Second record's {@code prevSha256} equals the first record's {@code sha256} (chain link).
 *   <li>Emitted {@code sha256} matches an independent re-hash of the JSON body.
 *   <li>All 18 {@link AuditLogger#record} parameters appear in the emitted JSON.
 *   <li>{@link Log4jAuditLogger#isWritable()} reflects the wrapped logger's INFO-enabled state.
 *   <li>JSON escaping handles quote, backslash, and control characters per RFC 8259.
 *   <li>Null collaborator at construction is rejected.
 * </ul>
 */
final class Log4jAuditLoggerTest {

  private static final String ZERO_HASH_HEX = "0".repeat(64);

  // ---------------------------------------------------------------------------
  // Constructor
  // ---------------------------------------------------------------------------

  @Test
  void constructor_nullLogger_throwsNullPointerException() {
    assertThrows(NullPointerException.class, () -> new Log4jAuditLogger(null));
  }

  // ---------------------------------------------------------------------------
  // isWritable
  // ---------------------------------------------------------------------------

  @Test
  void isWritable_loggerInfoEnabled_returnsTrue() {
    final var sink = new CapturingLogger(true);
    final var auditLogger = new Log4jAuditLogger(sink);
    assertTrue(auditLogger.isWritable());
  }

  @Test
  void isWritable_loggerInfoDisabled_returnsFalse() {
    final var sink = new CapturingLogger(false);
    final var auditLogger = new Log4jAuditLogger(sink);
    assertFalse(auditLogger.isWritable());
  }

  // ---------------------------------------------------------------------------
  // Hash chain — first record's prevSha256 is the all-zero hash
  // ---------------------------------------------------------------------------

  @Test
  void record_firstRecord_prevSha256IsAllZeros() {
    final var sink = new CapturingLogger(true);
    final var auditLogger = new Log4jAuditLogger(sink);

    auditLogger.record(
        1_000L,
        "user-1",
        "jti-1",
        "127.0.0.1",
        AuditAction.AUTH_SUCCESS,
        null,
        null,
        0L,
        0L,
        null,
        null,
        null,
        null,
        null,
        null,
        "ok",
        null,
        null);

    assertEquals(1, sink.lines.size(), "exactly one JSON line emitted");
    final var line = sink.lines.get(0);
    final var prev = extractStringField(line, "prevSha256");
    assertEquals(ZERO_HASH_HEX, prev, "first record's prevSha256 must be 64 hex zeros");
  }

  // ---------------------------------------------------------------------------
  // Hash chain — second record's prevSha256 == first record's sha256
  // ---------------------------------------------------------------------------

  @Test
  void record_secondRecord_prevSha256EqualsFirstRecordSha256() {
    final var sink = new CapturingLogger(true);
    final var auditLogger = new Log4jAuditLogger(sink);

    auditLogger.record(
        1L,
        "u",
        "j",
        "ip",
        AuditAction.AUTH_SUCCESS,
        null,
        null,
        0L,
        0L,
        null,
        null,
        null,
        null,
        null,
        null,
        "ok",
        null,
        null);
    auditLogger.record(
        2L,
        "u",
        "j",
        "ip",
        AuditAction.QUOTE_REQUEST_RECEIVED,
        "EURUSD",
        "Buy",
        100_000_000L,
        108_000_000L,
        "Limit",
        "GTC",
        "ACCT-1",
        "C-1",
        null,
        "Q-7",
        "ok",
        null,
        null);

    assertEquals(2, sink.lines.size());
    final var firstSha = extractStringField(sink.lines.get(0), "sha256");
    final var secondPrev = extractStringField(sink.lines.get(1), "prevSha256");
    assertEquals(firstSha, secondPrev, "chain link broken — second prev must equal first sha");
    final var secondSha = extractStringField(sink.lines.get(1), "sha256");
    assertFalse(firstSha.equals(secondSha), "different inputs must yield different SHA-256");
  }

  // ---------------------------------------------------------------------------
  // Hash chain — sha256 over the body is reproducible by an independent verifier
  // ---------------------------------------------------------------------------

  @Test
  void record_emittedSha256_matchesIndependentReHash() throws Exception {
    final var sink = new CapturingLogger(true);
    final var auditLogger = new Log4jAuditLogger(sink);

    auditLogger.record(
        42L,
        "u",
        "j",
        "ip",
        AuditAction.NEW_ORDER_RECEIVED,
        "GBPUSD",
        "Sell",
        1L,
        2L,
        "Limit",
        "IOC",
        "ACCT",
        "C-9",
        "C-8",
        "Q-1",
        "ok",
        null,
        "00-tp-tp-01");

    final var line = sink.lines.get(0);
    final var idx = line.indexOf(",\"sha256\":");
    assertTrue(idx > 0, "sha256 field must exist");
    final var body = line.substring(0, idx);
    final var bodyBytes = body.getBytes(StandardCharsets.UTF_8);
    final var md = MessageDigest.getInstance("SHA-256");
    final var expected = HexFormat.of().formatHex(md.digest(bodyBytes));
    final var actual = extractStringField(line, "sha256");
    assertEquals(expected, actual, "emitted sha256 must match independent re-hash of the body");
  }

  // ---------------------------------------------------------------------------
  // Field coverage — every one of the 18 fields appears in the JSON
  // ---------------------------------------------------------------------------

  @Test
  void record_allFieldsPopulated_appearInJson() {
    final var sink = new CapturingLogger(true);
    final var auditLogger = new Log4jAuditLogger(sink);

    auditLogger.record(
        1_700_000_000_000_000_000L,
        "user-123",
        "jti-abc",
        "192.168.1.100",
        AuditAction.NEW_ORDER_RECEIVED,
        "EURUSD",
        "Buy",
        100_000_000L,
        108_000_000L,
        "Limit",
        "GTC",
        "ACCT-1",
        "C-1",
        "C-0",
        "Q-42",
        "ok",
        "no-failure",
        "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01");

    final var line = sink.lines.get(0);
    assertTrue(line.contains("\"tsNs\":1700000000000000000"), "tsNs missing");
    assertTrue(line.contains("\"qty\":100000000"), "qty missing");
    assertTrue(line.contains("\"price\":108000000"), "price missing");
    assertEquals("user-123", extractStringField(line, "userId"));
    assertEquals("jti-abc", extractStringField(line, "jti"));
    assertEquals("192.168.1.100", extractStringField(line, "sourceIp"));
    assertEquals("new_order_received", extractStringField(line, "action"));
    assertEquals("EURUSD", extractStringField(line, "symbol"));
    assertEquals("Buy", extractStringField(line, "side"));
    assertEquals("Limit", extractStringField(line, "ordType"));
    assertEquals("GTC", extractStringField(line, "tif"));
    assertEquals("ACCT-1", extractStringField(line, "account"));
    assertEquals("C-1", extractStringField(line, "clOrdId"));
    assertEquals("C-0", extractStringField(line, "origClOrdId"));
    assertEquals("Q-42", extractStringField(line, "quoteId"));
    assertEquals("ok", extractStringField(line, "result"));
    assertEquals("no-failure", extractStringField(line, "failureReason"));
    assertEquals(
        "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
        extractStringField(line, "traceparent"));
    assertEquals(ZERO_HASH_HEX, extractStringField(line, "prevSha256"));
    assertNotNull(extractStringField(line, "sha256"));
  }

  @Test
  void record_nullStringFields_emittedAsJsonNull() {
    final var sink = new CapturingLogger(true);
    final var auditLogger = new Log4jAuditLogger(sink);

    auditLogger.record(
        0L,
        null,
        null,
        null,
        AuditAction.AUTH_FAIL,
        null,
        null,
        0L,
        0L,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null);

    final var line = sink.lines.get(0);
    assertTrue(line.contains("\"userId\":null"), "null userId must serialise as JSON null");
    assertTrue(line.contains("\"jti\":null"));
    assertTrue(line.contains("\"sourceIp\":null"));
    assertTrue(line.contains("\"symbol\":null"));
    assertTrue(line.contains("\"side\":null"));
    assertTrue(line.contains("\"traceparent\":null"));
    assertEquals("auth_fail", extractStringField(line, "action"));
  }

  // ---------------------------------------------------------------------------
  // JSON escaping — quote, backslash, control characters
  // ---------------------------------------------------------------------------

  @Test
  void record_userIdContainingQuotesAndBackslash_isEscaped() {
    final var sink = new CapturingLogger(true);
    final var auditLogger = new Log4jAuditLogger(sink);

    auditLogger.record(
        0L,
        "evil\"user\\name",
        null,
        null,
        AuditAction.AUTH_FAIL,
        null,
        null,
        0L,
        0L,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null);

    final var line = sink.lines.get(0);
    assertTrue(line.contains("\"userId\":\"evil\\\"user\\\\name\""), "escaping wrong: " + line);
  }

  @Test
  void record_userIdContainingControlChars_isEscaped() {
    final var sink = new CapturingLogger(true);
    final var auditLogger = new Log4jAuditLogger(sink);

    auditLogger.record(
        0L,
        "a\nb\tc",
        null,
        null,
        AuditAction.AUTH_FAIL,
        null,
        null,
        0L,
        0L,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null);

    final var line = sink.lines.get(0);
    assertTrue(line.contains("\"userId\":\"a\\nb\\tc\\u0001\""), "control escaping wrong: " + line);
  }

  // ---------------------------------------------------------------------------
  // Helpers — minimal capturing Logger via AbstractLogger SPI extension
  // ---------------------------------------------------------------------------

  /**
   * Capturing Log4j2 logger — extends {@link AbstractLogger} (Log4j2's official extension point for
   * custom loggers) and intercepts every {@code info(CharSequence)} / {@code info(String)} dispatch
   * via the central {@link #logMessage} hook. All other AbstractLogger overloads route through the
   * same hook, so this captures every variant of {@code info(...)} the production code might call.
   *
   * <p>The {@code isEnabled(...)} family answers based on the {@code enabled} ctor flag — used to
   * exercise both branches of {@link Log4jAuditLogger#isWritable()}.
   */
  private static final class CapturingLogger extends AbstractLogger {
    private static final long serialVersionUID = 1L;

    final List<String> lines = new ArrayList<>();
    private final boolean enabled;

    CapturingLogger(final boolean enabled) {
      super("audit-test", null);
      this.enabled = enabled;
    }

    @Override
    public Level getLevel() {
      return enabled ? Level.INFO : Level.OFF;
    }

    @Override
    public boolean isEnabled(
        final Level level, final Marker marker, final Message message, final Throwable t) {
      return enabled;
    }

    @Override
    public boolean isEnabled(
        final Level level, final Marker marker, final CharSequence message, final Throwable t) {
      return enabled;
    }

    @Override
    public boolean isEnabled(
        final Level level, final Marker marker, final Object message, final Throwable t) {
      return enabled;
    }

    @Override
    public boolean isEnabled(
        final Level level, final Marker marker, final String message, final Throwable t) {
      return enabled;
    }

    @Override
    public boolean isEnabled(final Level level, final Marker marker, final String message) {
      return enabled;
    }

    @Override
    public boolean isEnabled(
        final Level level, final Marker marker, final String message, final Object... params) {
      return enabled;
    }

    @Override
    public boolean isEnabled(
        final Level level, final Marker marker, final String message, final Object p0) {
      return enabled;
    }

    @Override
    public boolean isEnabled(
        final Level level,
        final Marker marker,
        final String message,
        final Object p0,
        final Object p1) {
      return enabled;
    }

    @Override
    public boolean isEnabled(
        final Level level,
        final Marker marker,
        final String message,
        final Object p0,
        final Object p1,
        final Object p2) {
      return enabled;
    }

    @Override
    public boolean isEnabled(
        final Level level,
        final Marker marker,
        final String message,
        final Object p0,
        final Object p1,
        final Object p2,
        final Object p3) {
      return enabled;
    }

    @Override
    public boolean isEnabled(
        final Level level,
        final Marker marker,
        final String message,
        final Object p0,
        final Object p1,
        final Object p2,
        final Object p3,
        final Object p4) {
      return enabled;
    }

    @Override
    public boolean isEnabled(
        final Level level,
        final Marker marker,
        final String message,
        final Object p0,
        final Object p1,
        final Object p2,
        final Object p3,
        final Object p4,
        final Object p5) {
      return enabled;
    }

    @Override
    public boolean isEnabled(
        final Level level,
        final Marker marker,
        final String message,
        final Object p0,
        final Object p1,
        final Object p2,
        final Object p3,
        final Object p4,
        final Object p5,
        final Object p6) {
      return enabled;
    }

    @Override
    public boolean isEnabled(
        final Level level,
        final Marker marker,
        final String message,
        final Object p0,
        final Object p1,
        final Object p2,
        final Object p3,
        final Object p4,
        final Object p5,
        final Object p6,
        final Object p7) {
      return enabled;
    }

    @Override
    public boolean isEnabled(
        final Level level,
        final Marker marker,
        final String message,
        final Object p0,
        final Object p1,
        final Object p2,
        final Object p3,
        final Object p4,
        final Object p5,
        final Object p6,
        final Object p7,
        final Object p8) {
      return enabled;
    }

    @Override
    public boolean isEnabled(
        final Level level,
        final Marker marker,
        final String message,
        final Object p0,
        final Object p1,
        final Object p2,
        final Object p3,
        final Object p4,
        final Object p5,
        final Object p6,
        final Object p7,
        final Object p8,
        final Object p9) {
      return enabled;
    }

    @Override
    public void logMessage(
        final String fqcn,
        final Level level,
        final Marker marker,
        final Message message,
        final Throwable t) {
      // Captures the formatted message — production code calls logger.info(String) which routes
      // through here as a SimpleMessage holding the JSON line.
      lines.add(message.getFormattedMessage());
    }
  }

  /**
   * Extract the value of a JSON string field {@code "key":"value"} from a single-line JSON
   * document. Tolerant of escaped quotes inside the value via a left-to-right scan that respects
   * backslash escaping. Throws {@link AssertionError} if the field is missing.
   */
  private static String extractStringField(final String json, final String key) {
    final var marker = "\"" + key + "\":\"";
    final var start = json.indexOf(marker);
    assertTrue(start >= 0, "field '" + key + "' missing from JSON: " + json);
    final var valueStart = start + marker.length();
    int p = valueStart;
    final var sb = new StringBuilder();
    while (p < json.length()) {
      final char c = json.charAt(p);
      if (c == '\\' && p + 1 < json.length()) {
        final char next = json.charAt(p + 1);
        switch (next) {
          case '"' -> sb.append('"');
          case '\\' -> sb.append('\\');
          case 'n' -> sb.append('\n');
          case 't' -> sb.append('\t');
          case 'r' -> sb.append('\r');
          case 'b' -> sb.append('\b');
          case 'f' -> sb.append('\f');
          case 'u' -> {
            final var hex = json.substring(p + 2, p + 6);
            sb.append((char) Integer.parseInt(hex, 16));
            p += 4;
          }
          default -> sb.append(next);
        }
        p += 2;
        continue;
      }
      if (c == '"') {
        return sb.toString();
      }
      sb.append(c);
      p++;
    }
    throw new AssertionError("unterminated string for key '" + key + "' in JSON: " + json);
  }
}
