package com.trading.engine.websocket;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.ErrorHandler;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.LifeCycle;
import org.apache.logging.log4j.core.LifeCycle.State;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.ConsoleAppender;
import org.apache.logging.log4j.core.impl.Log4jLogEvent;
import org.apache.logging.log4j.message.SimpleMessage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link Log4j2DiskFullErrorHandler} — verifies classification of disk-full errors,
 * rerouting to the fallback {@link Appender}, Micrometer counter tagging, delegation to the wrapped
 * handler for non-disk errors, stderr-of-last-resort behaviour when the fallback itself throws, and
 * constructor null-safety.
 *
 * <p>{@link ConsoleAppender} is declared {@code final} in log4j-core and therefore cannot be
 * subclassed for spying. The handler's fallback type is widened to {@link Appender} so these tests
 * can supply a {@link RecordingAppender} stub. Production wiring still passes a real {@link
 * ConsoleAppender} via {@link Log4j2DiskFullErrorHandler#installAll}.
 */
final class Log4j2DiskFullErrorHandlerTest {

  private SimpleMeterRegistry registry;
  private RecordingAppender fallback;
  private RecordingErrorHandler wrapped;
  private Log4j2DiskFullErrorHandler handler;

  @BeforeEach
  void setUp() {
    registry = new SimpleMeterRegistry();
    fallback = new RecordingAppender("Fallback", false);
    fallback.start();
    wrapped = new RecordingErrorHandler();
    handler = new Log4j2DiskFullErrorHandler(fallback, registry, "TestAppender", wrapped);
  }

  @Test
  void constructor_nullFallback_throws() {
    assertThrows(
        NullPointerException.class,
        () -> new Log4j2DiskFullErrorHandler(null, registry, "A", wrapped));
  }

  @Test
  void constructor_nullRegistry_throws() {
    assertThrows(
        NullPointerException.class,
        () -> new Log4j2DiskFullErrorHandler(fallback, null, "A", wrapped));
  }

  @Test
  void constructor_nullAppenderName_throws() {
    assertThrows(
        NullPointerException.class,
        () -> new Log4j2DiskFullErrorHandler(fallback, registry, null, wrapped));
  }

  @Test
  void constructor_blankAppenderName_throws() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new Log4j2DiskFullErrorHandler(fallback, registry, "  ", wrapped));
  }

  @Test
  void constructor_nullWrappedHandler_isPermitted() {
    final var h = new Log4j2DiskFullErrorHandler(fallback, registry, "A");
    h.error("msg-only");
    // Just verifying no NPE on the null-wrapped path; the "other" counter for A should be 1.
    assertEquals(1.0, counter("A", Log4j2DiskFullErrorHandler.KIND_OTHER), 0.0);
  }

  @Test
  void error_msgEventThrowable_diskFullEnospc_reroutesToConsole() {
    final var event = newEvent("audit row");
    handler.error("audit failed", event, new IOException("ENOSPC: No space left on device"));

    assertEquals(1, fallback.events.size(), "fallback Appender should have received reroute");
    assertSame(event, fallback.events.get(0));
    assertEquals(
        1.0,
        counter("TestAppender", Log4j2DiskFullErrorHandler.KIND_DISK_FULL),
        0.0,
        "disk_full counter for TestAppender should be 1");
    assertEquals(
        0.0,
        counter("TestAppender", Log4j2DiskFullErrorHandler.KIND_OTHER),
        0.0,
        "other counter should be 0 on a disk-full classification");
    assertTrue(wrapped.calls.isEmpty(), "wrapped handler must NOT be called on successful reroute");
  }

  @Test
  void error_msgEventThrowable_diskFullDiskFull_reroutesToConsole() {
    final var event = newEvent("row");
    handler.error("io fail", event, new IOException("operation failed: disk full"));
    assertEquals(1, fallback.events.size());
    assertEquals(1.0, counter("TestAppender", Log4j2DiskFullErrorHandler.KIND_DISK_FULL), 0.0);
  }

  @Test
  void error_msgEventThrowable_diskFullReadOnly_reroutesToConsole() {
    final var event = newEvent("row");
    handler.error("io fail", event, new IOException("Read-only file system"));
    assertEquals(1, fallback.events.size());
    assertEquals(1.0, counter("TestAppender", Log4j2DiskFullErrorHandler.KIND_DISK_FULL), 0.0);
  }

  @Test
  void error_msgEventThrowable_diskFullPermissionDenied_reroutesToConsole() {
    final var event = newEvent("row");
    handler.error("io fail", event, new IOException("Permission denied"));
    assertEquals(1, fallback.events.size());
    assertEquals(1.0, counter("TestAppender", Log4j2DiskFullErrorHandler.KIND_DISK_FULL), 0.0);
  }

  @Test
  void error_msgEventThrowable_diskFullNested_walksCauseChain() {
    final var event = newEvent("row");
    final var root = new IOException("No space left on device");
    final var mid = new RuntimeException("appender wrap", root);
    final var top = new IllegalStateException("dispatcher", mid);
    handler.error("io fail", event, top);
    assertEquals(1, fallback.events.size(), "should walk cause chain to find IOException");
    assertEquals(1.0, counter("TestAppender", Log4j2DiskFullErrorHandler.KIND_DISK_FULL), 0.0);
  }

  @Test
  void error_msgEventThrowable_nonDiskError_delegatesToWrappedAndTagsOther() {
    final var event = newEvent("row");
    final var cause = new RuntimeException("layout broken");
    handler.error("fmt fail", event, cause);

    assertTrue(fallback.events.isEmpty(), "fallback must NOT receive non-disk events");
    assertEquals(1, wrapped.calls.size(), "wrapped handler must receive non-disk error");
    assertSame(event, wrapped.calls.get(0).event);
    assertSame(cause, wrapped.calls.get(0).throwable);
    assertEquals(
        1.0,
        counter("TestAppender", Log4j2DiskFullErrorHandler.KIND_OTHER),
        0.0,
        "kind=other counter should be 1");
    assertEquals(0.0, counter("TestAppender", Log4j2DiskFullErrorHandler.KIND_DISK_FULL), 0.0);
  }

  @Test
  void error_msgEventThrowable_nullThrowable_classifiedAsOther() {
    final var event = newEvent("row");
    handler.error("orphan", event, null);
    assertEquals(1.0, counter("TestAppender", Log4j2DiskFullErrorHandler.KIND_OTHER), 0.0);
    assertEquals(1, wrapped.calls.size());
  }

  @Test
  void error_msgEventThrowable_nullEventOnDiskFull_cannotReroute_delegates() {
    handler.error("orphan", null, new IOException("No space left on device"));
    assertTrue(fallback.events.isEmpty(), "no event → cannot reroute");
    // Counter still bumps on disk_full classification
    assertEquals(1.0, counter("TestAppender", Log4j2DiskFullErrorHandler.KIND_DISK_FULL), 0.0);
    // Wrapped handler IS invoked because we couldn't reroute
    assertEquals(1, wrapped.calls.size());
  }

  @Test
  void error_msgEventThrowable_fallbackThrows_writesToStderrAndDelegates() {
    final var event = newEvent("row");
    final var explodingFallback = new RecordingAppender("Exploding", true);
    explodingFallback.start();
    final var h =
        new Log4j2DiskFullErrorHandler(explodingFallback, registry, "TestAppender", wrapped);

    // Capture stderr around the call to verify the breadcrumb landed there.
    final var origErr = System.err;
    final var buf = new ByteArrayOutputStream();
    System.setErr(new PrintStream(buf));
    try {
      h.error("io fail", event, new IOException("No space left on device"));
    } finally {
      System.setErr(origErr);
    }

    final var stderrOut = buf.toString();
    assertTrue(
        stderrOut.contains("Log4j2DiskFullErrorHandler"),
        "stderr breadcrumb should mention handler class; got: " + stderrOut);
    assertTrue(
        stderrOut.contains("TestAppender"),
        "stderr breadcrumb should mention appender name; got: " + stderrOut);
    // After fallback failed we still delegate so wrapped handler sees the original error.
    assertEquals(1, wrapped.calls.size(), "wrapped handler must be called after fallback failure");
  }

  @Test
  void error_msgThrowable_overload_classifiesAndDelegates() {
    handler.error("io fail", new IOException("No space left on device"));
    // No event → no reroute possible — but the counter still classifies as disk_full.
    assertTrue(fallback.events.isEmpty());
    assertEquals(1.0, counter("TestAppender", Log4j2DiskFullErrorHandler.KIND_DISK_FULL), 0.0);
    assertEquals(1, wrapped.calls.size());
    assertNull(wrapped.calls.get(0).event);
  }

  @Test
  void error_msgOnly_overload_alwaysOther_delegates() {
    handler.error("opaque");
    assertEquals(1.0, counter("TestAppender", Log4j2DiskFullErrorHandler.KIND_OTHER), 0.0);
    assertEquals(1, wrapped.calls.size());
    assertNull(wrapped.calls.get(0).event);
    assertNull(wrapped.calls.get(0).throwable);
  }

  @Test
  void classify_nullThrowable_returnsOther() {
    assertEquals(Log4j2DiskFullErrorHandler.KIND_OTHER, Log4j2DiskFullErrorHandler.classify(null));
  }

  @Test
  void classify_ioExceptionWithoutMarker_returnsOther() {
    assertEquals(
        Log4j2DiskFullErrorHandler.KIND_OTHER,
        Log4j2DiskFullErrorHandler.classify(new IOException("connection reset")));
  }

  @Test
  void classify_runtimeWithIoCause_walksToFindMarker() {
    final var io = new IOException("ENOSPC: No space left on device");
    final var wrap = new RuntimeException("appender", io);
    assertEquals(
        Log4j2DiskFullErrorHandler.KIND_DISK_FULL, Log4j2DiskFullErrorHandler.classify(wrap));
  }

  @Test
  void classify_cycle_doesNotInfiniteLoop() {
    final var a = new RuntimeException("a");
    final var b = new RuntimeException("b", a);
    a.initCause(b); // intentional cycle
    // Should terminate via the 16-hop bound and return OTHER (no IOException ever matches).
    assertEquals(Log4j2DiskFullErrorHandler.KIND_OTHER, Log4j2DiskFullErrorHandler.classify(a));
  }

  @Test
  void publicConstants_pinned() {
    // Pin the public counter / kind constants so a typo can't silently break the Prometheus
    // dashboard or the Grafana alert rule.
    assertEquals("log.appender.failure", Log4j2DiskFullErrorHandler.COUNTER_NAME);
    assertEquals("disk_full", Log4j2DiskFullErrorHandler.KIND_DISK_FULL);
    assertEquals("other", Log4j2DiskFullErrorHandler.KIND_OTHER);
  }

  // ----------------------------------------------------------------------------------------------
  // helpers
  // ----------------------------------------------------------------------------------------------

  private double counter(final String appenderTag, final String kind) {
    final var c =
        registry
            .find(Log4j2DiskFullErrorHandler.COUNTER_NAME)
            .tag("appender", appenderTag)
            .tag("kind", kind)
            .counter();
    assertNotNull(c, "counter must be registered for appender=" + appenderTag + " kind=" + kind);
    return c.count();
  }

  private static LogEvent newEvent(final String msg) {
    return Log4jLogEvent.newBuilder()
        .setLoggerName("test")
        .setLevel(Level.ERROR)
        .setMessage(new SimpleMessage(msg))
        .build();
  }

  /**
   * Minimal in-memory {@link Appender} that records every {@link LogEvent} it receives — used as a
   * test double for the fallback {@link ConsoleAppender}. When {@code explode=true} it throws on
   * {@link #append(LogEvent)} to exercise the stderr breadcrumb path.
   */
  private static final class RecordingAppender implements Appender {
    final List<LogEvent> events = new ArrayList<>();
    private final String name;
    private final boolean explode;
    private volatile State state = State.INITIALIZED;
    private volatile ErrorHandler handler;

    RecordingAppender(final String name, final boolean explode) {
      this.name = name;
      this.explode = explode;
    }

    @Override
    public void append(final LogEvent event) {
      if (explode) {
        throw new RuntimeException("fallback boom");
      }
      events.add(event);
    }

    @Override
    public String getName() {
      return name;
    }

    @Override
    public Layout<? extends Serializable> getLayout() {
      return null;
    }

    @Override
    public boolean ignoreExceptions() {
      return false;
    }

    @Override
    public ErrorHandler getHandler() {
      return handler;
    }

    @Override
    public void setHandler(final ErrorHandler errorHandler) {
      this.handler = errorHandler;
    }

    @Override
    public void start() {
      state = State.STARTED;
    }

    @Override
    public void stop() {
      state = State.STOPPED;
    }

    @Override
    public boolean isStarted() {
      return state == State.STARTED;
    }

    @Override
    public boolean isStopped() {
      return state == State.STOPPED;
    }

    @Override
    public State getState() {
      return state;
    }

    @Override
    public void initialize() {
      // no-op
    }
  }

  /**
   * Captured calls into the wrapped {@link ErrorHandler} so tests can assert delegation occurred
   * (or didn't).
   */
  private static final class RecordingErrorHandler implements ErrorHandler {
    final List<Call> calls = new ArrayList<>();

    @Override
    public void error(final String msg) {
      calls.add(new Call(msg, null, null));
    }

    @Override
    public void error(final String msg, final Throwable t) {
      calls.add(new Call(msg, null, t));
    }

    @Override
    public void error(final String msg, final LogEvent event, final Throwable t) {
      calls.add(new Call(msg, event, t));
    }
  }

  private record Call(String msg, LogEvent event, Throwable throwable) {}

  // Silences unused-import warning for Filter / Layout / LifeCycle in environments where
  // the helper Appender's reduced interface still compiles. They are required to satisfy the
  // log4j-core API surface.
  @SuppressWarnings("unused")
  private static final Class<?>[] UNUSED_TYPE_PINS = {Filter.class, Layout.class, LifeCycle.class};
}
