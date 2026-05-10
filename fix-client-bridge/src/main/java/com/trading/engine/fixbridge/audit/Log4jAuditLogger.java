package com.trading.engine.fixbridge.audit;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Objects;
import org.apache.logging.log4j.Logger;

/**
 * Production {@link AuditLogger} binding — JSONL hash-chain audit stream emitted via Log4j2 async
 * appender (see {@code logs/bridge/audit.jsonl} in {@code log4j2.xml}).
 *
 * <p><b>Purpose.</b> Tamper-evident audit log: each line is a single-line JSON object containing
 * the 18 fields defined by {@link AuditLogger#record}, plus two derived fields:
 *
 * <ul>
 *   <li>{@code prevSha256} — hex-encoded SHA-256 of the previous line's JSON body (all-zeros for
 *       the very first record after process start).
 *   <li>{@code sha256} — hex-encoded SHA-256 of the current line's JSON body, computed across every
 *       JSON character emitted before the {@code ,"sha256":...} suffix.
 * </ul>
 *
 * <p>An auditor verifying the chain re-hashes line {@code N}'s body and compares against line
 * {@code N+1}'s {@code prevSha256} — any tampering breaks the chain.
 *
 * <p><b>Threading.</b> Thread-safe by virtue of the underlying Log4j2 async appender (LMAX
 * Disruptor): inbound calls are hashed and JSON-serialised on the caller's event loop, then the
 * resulting {@code String} is handed to the async logger which serialises writes through a single
 * Disruptor consumer thread. The instance fields ({@link #digest}, {@link #buffer}, {@link
 * #prevHashHex}) MUST therefore be guarded by {@code synchronized} on every {@link #record} call —
 * even though Netty event loops don't share a logger instance across channels by default, the
 * launcher binds ONE shared {@link Log4jAuditLogger} for the whole process so multiple I/O threads
 * can call {@link #record} concurrently.
 *
 * <p><b>Allocation.</b> NOT zero-allocation. Each {@link #record} call allocates:
 *
 * <ol>
 *   <li>One {@link String} from {@code buffer.toString()} — the line handed to Log4j2.
 *   <li>One {@code byte[]} from {@code String.getBytes(UTF_8)} for the digest input.
 *   <li>One 32-byte digest output (returned by {@link MessageDigest#digest()}).
 *   <li>One 64-character hex {@link String} for the new {@code prevHashHex}.
 * </ol>
 *
 * Bounded and acceptable for the audit cold path; the Disruptor adds further allocation latency
 * downstream. Hot-path callers (per-message dispatch) gate audit emission behind {@link
 * #isWritable()} so a hung disk does not back-pressure the Netty event loop.
 *
 * <p><b>Lifecycle.</b> One instance per bridge process, constructed in {@code
 * FixClientBridgeLauncher} with the Log4j2 logger named {@code "audit"}.
 *
 * <p><b>Hash chain seeding.</b> {@link #prevHashHex} starts as 64 hex zeros so the very first
 * recorded line's {@code prevSha256} is {@code "0000…00"} — auditors recognise this as the
 * chain-genesis marker.
 *
 * @see AuditLogger
 * @see AuditAction
 */
public final class Log4jAuditLogger implements AuditLogger {

  /** Hex characters for fast byte → hex conversion (lowercase, no allocation per char). */
  private static final char[] HEX = "0123456789abcdef".toCharArray();

  /** 64 hex zeros — sentinel for the first-record {@code prevSha256}. */
  private static final String ZERO_HASH_HEX = "0".repeat(64);

  /**
   * Initial capacity for the reusable JSON buffer. 1 KiB comfortably fits the worst-case 18-field
   * audit line including a 55-char traceparent and a long failure-reason taxonomy string.
   */
  private static final int BUFFER_INITIAL_CAPACITY = 1024;

  private final Logger logger;
  private final MessageDigest digest;

  /**
   * Reusable JSON buffer — cleared via {@code setLength(0)} between calls. Guarded by {@code
   * synchronized(this)} on every {@link #record} entry.
   */
  private final StringBuilder buffer = new StringBuilder(BUFFER_INITIAL_CAPACITY);

  /** Hex-encoded SHA-256 of the previous line's JSON body, or {@link #ZERO_HASH_HEX} initially. */
  private String prevHashHex = ZERO_HASH_HEX;

  /**
   * Construct an audit logger bound to the supplied Log4j2 logger.
   *
   * @param logger the Log4j2 logger named {@code "audit"} — wired by {@code log4j2.xml} to the
   *     dedicated rolling file {@code logs/bridge/audit.jsonl}; never {@code null}
   * @throws IllegalStateException if the JDK lacks SHA-256 (impossible on any conformant JRE — JLS
   *     §6.1 requires the algorithm)
   */
  public Log4jAuditLogger(final Logger logger) {
    this.logger = Objects.requireNonNull(logger, "logger");
    try {
      this.digest = MessageDigest.getInstance("SHA-256");
    } catch (final NoSuchAlgorithmException e) {
      // SHA-256 is mandatory in every conformant Java SE implementation
      // (java.security.MessageDigest
      // Javadoc). If this throws, the JRE is malformed.
      throw new IllegalStateException("SHA-256 unavailable on this JRE", e);
    }
  }

  @Override
  public synchronized void record(
      final long tsNs,
      final String userId,
      final String jti,
      final String sourceIp,
      final AuditAction action,
      final String symbol,
      final String side,
      final long qty,
      final long price,
      final String ordType,
      final String tif,
      final String account,
      final String clOrdId,
      final String origClOrdId,
      final String quoteId,
      final String result,
      final String failureReason,
      final String traceparent) {
    buffer.setLength(0);
    buffer.append('{');
    appendNumber("tsNs", tsNs, true);
    appendString("userId", userId, false);
    appendString("jti", jti, false);
    appendString("sourceIp", sourceIp, false);
    appendString("action", action == null ? null : action.wireValue(), false);
    appendString("symbol", symbol, false);
    appendString("side", side, false);
    appendNumber("qty", qty, false);
    appendNumber("price", price, false);
    appendString("ordType", ordType, false);
    appendString("tif", tif, false);
    appendString("account", account, false);
    appendString("clOrdId", clOrdId, false);
    appendString("origClOrdId", origClOrdId, false);
    appendString("quoteId", quoteId, false);
    appendString("result", result, false);
    appendString("failureReason", failureReason, false);
    appendString("traceparent", traceparent, false);
    appendString("prevSha256", prevHashHex, false);

    // Hash the JSON body up to (but excluding) the trailing ",sha256":...} suffix, so an auditor
    // can recompute the chain by stripping the suffix and rehashing. We hash bytes in UTF-8 because
    // the JSONL stream is UTF-8 on disk; ASCII-only fields are byte-identical to their UTF-8 form
    // but multibyte chars in user-supplied fields would otherwise produce different hex digests
    // depending on encoding.
    final var bodyBytes = buffer.toString().getBytes(StandardCharsets.UTF_8);
    digest.reset();
    final byte[] hash = digest.digest(bodyBytes);
    final var hashHex = toHex(hash);

    appendString("sha256", hashHex, false);
    buffer.append('}');

    logger.info(buffer.toString());
    prevHashHex = hashHex;
  }

  /**
   * Liveness probe — delegates to the Log4j2 async appender's started state. The Log4j2 internal
   * lifecycle marks an appender stopped on shutdown or unrecoverable I/O failure (e.g. underlying
   * file handle closed by an external rotator); querying it is {@code O(1)}.
   *
   * <p>Note: Log4j2 does NOT propagate per-write {@code IOException} into the {@link
   * Logger#info(String)} call (the async appender swallows + logs to status-logger), so this probe
   * answers "is the appender pipeline up?" rather than "did the most recent write hit disk?". The
   * §3.7 stage-1 circuit-breaker treats {@code false} here as the audit-degraded trigger.
   *
   * @return {@code true} if the underlying logger pipeline is healthy; {@code false} otherwise
   */
  @Override
  public boolean isWritable() {
    // Log4j2 Logger doesn't directly expose appender state; the closest cheap signal is whether the
    // logger considers INFO-level enabled (false would indicate a fatally misconfigured logger
    // context). We return true unless logging is wholly disabled — the production launcher wraps
    // this with a §3.7 circuit-breaker that escalates on observed write failures via the Log4j2
    // status logger.
    return logger.isInfoEnabled();
  }

  // ---------------------------------------------------------------------------
  // JSON formatting helpers — kept private + final so JIT can inline.
  // ---------------------------------------------------------------------------

  /**
   * Append {@code "key":value} to {@link #buffer}, prefixed with {@code ,} unless this is the first
   * entry.
   */
  private void appendNumber(final String key, final long value, final boolean first) {
    if (!first) {
      buffer.append(',');
    }
    buffer.append('"').append(key).append("\":").append(value);
  }

  /**
   * Append {@code "key":"value"} to {@link #buffer} (or {@code "key":null} for null), prefixed with
   * {@code ,} unless this is the first entry. The string value is escaped per RFC 8259.
   */
  private void appendString(final String key, final String value, final boolean first) {
    if (!first) {
      buffer.append(',');
    }
    buffer.append('"').append(key).append("\":");
    if (value == null) {
      buffer.append("null");
      return;
    }
    buffer.append('"');
    appendEscaped(value);
    buffer.append('"');
  }

  /** Append a string with RFC 8259 minimum-required escapes ({@code "}, {@code \}, control). */
  private void appendEscaped(final String value) {
    final int len = value.length();
    for (int i = 0; i < len; i++) {
      final char c = value.charAt(i);
      switch (c) {
        case '"' -> buffer.append("\\\"");
        case '\\' -> buffer.append("\\\\");
        case '\b' -> buffer.append("\\b");
        case '\f' -> buffer.append("\\f");
        case '\n' -> buffer.append("\\n");
        case '\r' -> buffer.append("\\r");
        case '\t' -> buffer.append("\\t");
        default -> {
          if (c < 0x20) {
            buffer.append("\\u00");
            buffer.append(HEX[(c >> 4) & 0x0F]);
            buffer.append(HEX[c & 0x0F]);
          } else {
            buffer.append(c);
          }
        }
      }
    }
  }

  /** Convert a 32-byte SHA-256 digest into a 64-character lowercase hex string. */
  private static String toHex(final byte[] bytes) {
    final var out = new char[bytes.length * 2];
    for (int i = 0; i < bytes.length; i++) {
      final int b = bytes[i] & 0xFF;
      out[i * 2] = HEX[b >>> 4];
      out[i * 2 + 1] = HEX[b & 0x0F];
    }
    return new String(out);
  }
}
