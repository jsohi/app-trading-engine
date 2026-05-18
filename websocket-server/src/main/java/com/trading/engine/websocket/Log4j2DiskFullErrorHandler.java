package com.trading.engine.websocket;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.io.IOException;
import java.util.Objects;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.ErrorHandler;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.ConsoleAppender;

/**
 * Log4j2 {@link ErrorHandler} that detects disk-full / IO-failure conditions raised by an
 * underlying appender (typically a {@code RollingFileAppender} on the audit / general logger),
 * reroutes the offending {@link LogEvent} to a fallback {@link ConsoleAppender} so the process
 * keeps logging, and increments a {@link Counter} tagged with the originating appender name and a
 * categorical {@code kind} so the operator can alert on the spike.
 *
 * <p><b>Design rationale.</b> Production trading systems must NEVER lose log lines silently.
 * Log4j2's default {@code DefaultErrorHandler} prints to {@code System.err} via the {@code
 * StatusLogger} once-per-minute throttle — which means a sustained disk-full event drops almost
 * everything after the first minute. This handler:
 *
 * <ol>
 *   <li>Classifies the error (disk-full vs other) by walking the cause chain looking for an {@link
 *       IOException} whose message matches a small allow-list of OS-level disk / FS failures ("No
 *       space", "disk full", "Read-only file system", "Permission denied").
 *   <li>For disk-full hits, calls {@link ConsoleAppender#append(LogEvent)} directly so the event
 *       reaches stderr/stdout regardless of the failed appender's state. The replacement is
 *       lossless from this point onward.
 *   <li>For non-disk errors, delegates to the wrapped handler (preserving Log4j2 default semantics)
 *       so we don't accidentally suppress diagnostics for layout/encoding/network appender
 *       failures.
 *   <li>Increments {@code log.appender.failure} with tags {@code appender}+{@code kind} so a
 *       Prometheus / Grafana alert can fire on the first occurrence.
 *   <li>If the {@link ConsoleAppender} fallback itself throws (e.g. stderr closed / container
 *       stdout pipe broken), writes a single line directly to {@link System#err} so we never
 *       silently swallow.
 * </ol>
 *
 * <p><b>Threading.</b> Called from Log4j2 internal appender threads (which for asynchronous loggers
 * means the LMAX Disruptor consumer thread, and for synchronous appenders the calling application
 * thread). MUST be thread-safe. All state is final after construction; Micrometer {@link
 * Counter#increment()} is atomic; {@link ConsoleAppender#append(LogEvent)} is documented
 * thread-safe under the Log4j2 contract.
 *
 * <p><b>Lifecycle.</b> Constructed once per non-console appender at process bootstrap by {@link
 * #installAll(LoggerContext, ConsoleAppender, MeterRegistry)} and bound via {@link
 * Appender#setHandler(ErrorHandler)}. The handler holds no resources requiring shutdown.
 *
 * <p><b>Allocation.</b> The hot path ({@link #error(String, LogEvent, Throwable)}) is allocation-
 * free: the cause walk uses a local reference, the kind classification returns interned string
 * constants, and Micrometer tagged counters were pre-registered at construction. The non-{@link
 * LogEvent} overload ({@link #error(String, Throwable)}) cannot reroute because there is no event
 * to forward, so it only classifies, counts, and delegates.
 *
 * @see ErrorHandler
 * @see ConsoleAppender
 * @see WebSocketMetrics#logAppenderFailure(String, String)
 */
public final class Log4j2DiskFullErrorHandler implements ErrorHandler {

  /** Counter name; tagged with {@code appender} (origin) + {@code kind} (classification). */
  public static final String COUNTER_NAME = "log.appender.failure";

  /** Classification tag value when the cause chain contains a recognised disk/FS failure. */
  public static final String KIND_DISK_FULL = "disk_full";

  /** Classification tag value for any other appender error (delegated to the wrapped handler). */
  public static final String KIND_OTHER = "other";

  /**
   * Substrings checked against the {@link IOException} message (lower-cased, ASCII-only). These are
   * the four canonical disk / filesystem errors raised by JDK file IO on Linux/macOS:
   *
   * <ul>
   *   <li>{@code "no space"} — {@code ENOSPC} ("No space left on device").
   *   <li>{@code "disk full"} — Windows / NFS variant.
   *   <li>{@code "read-only file system"} — {@code EROFS} (typical after device remount).
   *   <li>{@code "permission denied"} — {@code EACCES} (config drift / chmod regression).
   * </ul>
   *
   * The check is substring-based rather than exception-class based because the JDK wraps these
   * errno values inside generic {@link IOException} with locale-independent C messages from {@code
   * errno.h}; there is no dedicated exception subclass for ENOSPC.
   */
  private static final String[] DISK_FULL_MARKERS = {
    "no space", "disk full", "read-only file system", "permission denied"
  };

  private final Appender fallback;
  private final Counter diskFullCounter;
  private final Counter otherCounter;
  private final String appenderName;
  private final ErrorHandler wrapped;

  /**
   * Build a handler that reroutes disk-full errors from a single appender to the supplied {@link
   * ConsoleAppender} and accounts every error against {@code log.appender.failure}.
   *
   * @param fallback the rescue {@link Appender} (production: a {@link ConsoleAppender}) — must be
   *     started and not the same appender being wrapped (the install path skips console appenders
   *     to avoid self-reroute); must not be {@code null}. Accepted as {@link Appender} rather than
   *     {@link ConsoleAppender} so test doubles can supply a recording stub ({@code
   *     ConsoleAppender} is {@code final} and not subclassable).
   * @param registry the Micrometer registry that owns the {@code log.appender.failure} counter
   *     family; must not be {@code null}
   * @param appenderName the name of the underlying appender this handler is bound to — used as the
   *     {@code appender=} tag value; must not be {@code null} or blank
   * @param wrapped the pre-existing {@link ErrorHandler} on the underlying appender (typically
   *     Log4j2's {@code DefaultErrorHandler}); {@code null} is tolerated and treated as a no-op
   *     delegate so install-time wiring stays simple
   * @throws NullPointerException if {@code fallback}, {@code registry}, or {@code appenderName} is
   *     {@code null}
   * @throws IllegalArgumentException if {@code appenderName} is blank
   */
  public Log4j2DiskFullErrorHandler(
      final Appender fallback,
      final MeterRegistry registry,
      final String appenderName,
      final ErrorHandler wrapped) {
    this.fallback = Objects.requireNonNull(fallback, "fallback");
    Objects.requireNonNull(registry, "registry");
    this.appenderName = Objects.requireNonNull(appenderName, "appenderName");
    if (appenderName.isBlank()) {
      throw new IllegalArgumentException("appenderName must not be blank");
    }
    this.wrapped = wrapped; // nullable on purpose — no-op delegate
    this.diskFullCounter =
        Counter.builder(COUNTER_NAME)
            .tag("appender", appenderName)
            .tag("kind", KIND_DISK_FULL)
            .description(
                "Log4j2 appender errors classified as disk-full / read-only FS / permission "
                    + "denied; the event was rerouted to ConsoleAppender. Tagged by appender.")
            .register(registry);
    this.otherCounter =
        Counter.builder(COUNTER_NAME)
            .tag("appender", appenderName)
            .tag("kind", KIND_OTHER)
            .description(
                "Log4j2 appender errors that did NOT match a disk-full marker; delegated to the "
                    + "wrapped default error handler. Tagged by appender.")
            .register(registry);
  }

  /**
   * Three-arg constructor used by the production {@link #installAll(LoggerContext, ConsoleAppender,
   * MeterRegistry)} bootstrap when there is no pre-existing handler to wrap (a freshly-built
   * appender starts with {@code null}). Delegates to the four-arg form with a {@code null} wrapped
   * handler.
   *
   * @param fallback the rescue {@link Appender} (production: {@link ConsoleAppender}); see four-arg
   *     overload
   * @param registry the Micrometer registry; see four-arg overload
   * @param appenderName the underlying appender name; see four-arg overload
   */
  public Log4j2DiskFullErrorHandler(
      final Appender fallback, final MeterRegistry registry, final String appenderName) {
    this(fallback, registry, appenderName, null);
  }

  /**
   * Visible-for-tests accessor returning the fallback appender bound at construction.
   *
   * @return the fallback {@link Appender}; never {@code null}
   */
  Appender fallback() {
    return fallback;
  }

  /**
   * Install a {@link Log4j2DiskFullErrorHandler} on every non-console appender in the active {@link
   * LoggerContext}.
   *
   * <p>Called once at process boot AFTER {@code LogManager.getContext(false)} has been resolved and
   * AFTER the Micrometer registry is constructed. Idempotent across boots within the same JVM only
   * insofar as Log4j2's reconfiguration replaces appenders — callers that reconfigure Log4j2 at
   * runtime must re-invoke this method.
   *
   * @param context the active {@link LoggerContext} whose configuration provides the appender map;
   *     must not be {@code null}
   * @param fallback the {@link ConsoleAppender} that receives rerouted events; must be started and
   *     must not be {@code null}
   * @param registry the Micrometer registry used to register the per-appender tagged counters; must
   *     not be {@code null}
   * @return the count of appenders that received a handler binding (console appenders are skipped
   *     to avoid self-reroute and are NOT counted in the return value)
   * @throws NullPointerException if any argument is {@code null}
   */
  public static int installAll(
      final LoggerContext context, final ConsoleAppender fallback, final MeterRegistry registry) {
    Objects.requireNonNull(context, "context");
    Objects.requireNonNull(fallback, "fallback");
    Objects.requireNonNull(registry, "registry");
    int installed = 0;
    for (final var entry : context.getConfiguration().getAppenders().entrySet()) {
      final var name = entry.getKey();
      final var appender = entry.getValue();
      // Skip ConsoleAppender — we don't want a console error to recurse into itself, and the
      // rescue path is meaningless when the failing appender IS the fallback.
      if (appender instanceof ConsoleAppender) {
        continue;
      }
      final var existing = appender.getHandler();
      appender.setHandler(new Log4j2DiskFullErrorHandler(fallback, registry, name, existing));
      installed++;
    }
    return installed;
  }

  /**
   * Receive a message-only error notification from the underlying appender. Without a {@link
   * LogEvent} we cannot reroute, so this overload only classifies + counts + delegates.
   *
   * @param msg the human-readable error message produced by the appender; never reformatted
   */
  @Override
  public void error(final String msg) {
    // No throwable, no event — best we can do is bump the "other" counter (no IOException to
    // classify) and pass through to the wrapped handler so the operator still sees the line.
    otherCounter.increment();
    if (wrapped != null) {
      wrapped.error(msg);
    }
  }

  /**
   * Receive an error with cause but no {@link LogEvent}. Classifies the cause, counts, and
   * delegates; cannot reroute (no event to forward).
   *
   * @param msg the appender's diagnostic message; passed through unchanged to the delegate
   * @param t the cause chain — walked to find an {@link IOException} whose message matches a
   *     disk-full marker; may be {@code null}
   */
  @Override
  public void error(final String msg, final Throwable t) {
    final var kind = classify(t);
    bump(kind);
    if (wrapped != null) {
      wrapped.error(msg, t);
    }
  }

  /**
   * Hot path. Receive an error WITH the originating {@link LogEvent}. On a disk-full
   * classification, reroute the event to the fallback {@link ConsoleAppender} so the log line
   * survives; on any other classification, delegate to the wrapped handler so default Log4j2
   * diagnostics still surface.
   *
   * @param msg the appender's diagnostic message
   * @param event the {@link LogEvent} that the underlying appender failed to write — forwarded
   *     verbatim to the fallback on disk-full; may be {@code null} (in which case we cannot reroute
   *     and fall through to {@code error(msg, t)} semantics)
   * @param t the cause chain triggering this handler; classified per {@link #classify(Throwable)}
   */
  @Override
  public void error(final String msg, final LogEvent event, final Throwable t) {
    final var kind = classify(t);
    bump(kind);
    if (KIND_DISK_FULL.equals(kind) && event != null) {
      try {
        fallback.append(event);
        // Successful reroute — intentionally do NOT delegate to wrapped: the wrapped handler
        // would write the same event to stderr a second time via DefaultErrorHandler's
        // StatusLogger path, doubling the operator's noise.
        return;
      } catch (final RuntimeException rerouteFailure) {
        // Last-resort breadcrumb — stderr is the only sink we have left. Single line, no
        // formatting, no Log4j2 call (which would recurse). DO NOT swallow.
        System.err.println(
            "Log4j2DiskFullErrorHandler: fallback ConsoleAppender failed for appender="
                + appenderName
                + " original="
                + (t == null ? "<null>" : t.getMessage())
                + " reroute="
                + rerouteFailure.getMessage());
      }
    }
    if (wrapped != null) {
      wrapped.error(msg, event, t);
    }
  }

  /**
   * Walk the cause chain looking for an {@link IOException} whose message (lower-cased) contains
   * one of the {@link #DISK_FULL_MARKERS}. Returns {@link #KIND_DISK_FULL} on the first hit, {@link
   * #KIND_OTHER} otherwise (including for a {@code null} throwable).
   *
   * <p>Bounded to 16 hops to defend against pathological self-referential chains.
   *
   * @param t the throwable to classify; may be {@code null}
   * @return {@link #KIND_DISK_FULL} or {@link #KIND_OTHER} — never {@code null}
   */
  static String classify(final Throwable t) {
    Throwable cursor = t;
    int hops = 0;
    while (cursor != null && hops < 16) {
      if (cursor instanceof IOException) {
        final var raw = cursor.getMessage();
        if (raw != null) {
          final var lower = raw.toLowerCase(java.util.Locale.ROOT);
          for (final var marker : DISK_FULL_MARKERS) {
            if (lower.contains(marker)) {
              return KIND_DISK_FULL;
            }
          }
        }
      }
      cursor = cursor.getCause();
      hops++;
    }
    return KIND_OTHER;
  }

  private void bump(final String kind) {
    if (KIND_DISK_FULL.equals(kind)) {
      diskFullCounter.increment();
    } else {
      otherCounter.increment();
    }
  }
}
