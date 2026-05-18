package com.trading.engine.websocket;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.trading.engine.messages.sbe.CommandAckStatus;
import com.trading.engine.projections.SymbolPacker;
import com.trading.engine.testsupport.clock.ControllableNanoClock;
import com.trading.engine.testsupport.sbe.SbeTestEncoder;
import io.netty.channel.DefaultChannelId;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import io.netty.util.ResourceLeakDetector;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.ManyToOneConcurrentArrayQueue;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Verifies the cold-path {@code ws.drain.cycle} OpenTelemetry span emitted by {@link
 * WebSocketDrainHandler#drain()} (APP-244 Phase 3 C.5).
 *
 * <p>Wires an {@link SdkTracerProvider} backed by {@link InMemorySpanExporter} into the
 * drain-handler via the test-only constructor that takes an explicit {@link Tracer}. This avoids
 * touching the JVM-global {@code GlobalOpenTelemetry} singleton (which would race with
 * parallel-running tests in the same JVM and is also append-only — {@code GlobalOpenTelemetry.set}
 * is one-shot per process).
 *
 * <p>Two scenarios are asserted:
 *
 * <ol>
 *   <li>{@code drain_emptyQueue_emitsNoSpan} — calling {@link WebSocketDrainHandler#drain()} with
 *       nothing in the queue MUST NOT emit any span (zero-allocation no-op path).
 *   <li>{@code drain_oneEntry_emitsOneSpanWithCorrectAttributes} — a single enqueued frame MUST
 *       produce exactly one span named {@code ws.drain.cycle} carrying {@code drain.cycle.items=1}
 *       and a positive {@code drain.cycle.duration_nanos} attribute.
 * </ol>
 *
 * <p>Threading: single-threaded JUnit harness — drain is invoked synchronously on the test thread.
 */
final class WebSocketDrainHandlerOtelSpanTest {

  /** Queue/pool capacity — power of 2 for ManyToOneConcurrentArrayQueue. */
  private static final int CAPACITY = 4;

  /** Maximum SBE message size per entry. */
  private static final int MAX_MESSAGE_SIZE = 1024;

  private ManyToOneConcurrentArrayQueue<EgressEntry> queue;
  private ManyToOneConcurrentArrayQueue<EgressEntry> returnQueue;
  private ManyToOneConcurrentArrayQueue<EgressEntry> ackQueue;
  private CommandEntryPool commandEntryPool;
  private WebSocketMetrics metrics;
  private WebSocketEgressListener egressListener;
  private WebSocketSessionManager sessionManager;
  private WebSocketDrainHandler drainHandler;
  private MutableDirectBuffer sbeBuffer;

  private InMemorySpanExporter spanExporter;
  private SdkTracerProvider tracerProvider;

  /** Channels opened during the test — closed in {@link #tearDown()}. */
  private final List<EmbeddedChannel> openChannels = new ArrayList<>();

  @BeforeAll
  static void enableLeakDetection() {
    ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID);
  }

  @BeforeEach
  void setUp() {
    queue = new ManyToOneConcurrentArrayQueue<>(CAPACITY);
    returnQueue = new ManyToOneConcurrentArrayQueue<>(CAPACITY);
    ackQueue = new ManyToOneConcurrentArrayQueue<>(CAPACITY);
    commandEntryPool = new CommandEntryPool(CAPACITY, MAX_MESSAGE_SIZE);
    metrics = WebSocketMetrics.createWithDefaults();
    egressListener =
        new WebSocketEgressListener(queue, returnQueue, metrics, CAPACITY, MAX_MESSAGE_SIZE);
    sbeBuffer = new ExpandableArrayBuffer(MAX_MESSAGE_SIZE);

    final var config = WebSocketServerConfig.builder().build();
    final var clock = new ControllableNanoClock(1_000_000_000L);
    sessionManager = new WebSocketSessionManager(config, metrics, clock);

    // Build a real OTel SDK with InMemorySpanExporter so we can assert span emission per-test.
    // SimpleSpanProcessor (not BatchSpanProcessor) so end() flushes synchronously — no need
    // to await a batch interval before reading spanExporter.getFinishedSpanItems().
    spanExporter = InMemorySpanExporter.create();
    tracerProvider =
        SdkTracerProvider.builder()
            .addSpanProcessor(SimpleSpanProcessor.create(spanExporter))
            .build();
    final Tracer tracer = tracerProvider.get(WebSocketDrainHandler.OTEL_INSTRUMENTATION_SCOPE);

    drainHandler =
        new WebSocketDrainHandler(
            queue,
            ackQueue,
            commandEntryPool,
            egressListener,
            sessionManager,
            metrics,
            clock,
            tracer);
  }

  @AfterEach
  void tearDown() {
    for (final var ch : openChannels) {
      ch.finishAndReleaseAll();
    }
    openChannels.clear();
    if (tracerProvider != null) {
      tracerProvider.close();
    }
  }

  private EmbeddedChannel createSessionChannel() {
    final var ch = new EmbeddedChannel(DefaultChannelId.newInstance());
    openChannels.add(ch);
    final var session = sessionManager.tryRegister(ch);
    session.initSubscriptionFilter(100, metrics);
    session.subscriptionFilter().addSubscription(0L, 0x1F);
    session.subscriptionFilter().addSubscription(SymbolPacker.pack("EURUSD  "), 0x1F);
    session.entitledAccounts(Set.of("TEST-ACCT"));
    return ch;
  }

  private void enqueueEntry(final int templateId, final int length) {
    final var entry = new EgressEntry(MAX_MESSAGE_SIZE);
    sbeBuffer.getBytes(0, entry.bytes(), 0, length);
    entry.setMetadata(length, templateId);
    final boolean offered = queue.offer(entry);
    assertTrue(offered, "Must be able to offer entry to queue");
  }

  /**
   * Idle drain path — queue empty, no sessions written to. The drain handler MUST NOT call into the
   * tracer at all (zero-allocation invariant for the no-op cycle).
   */
  @Test
  void drain_emptyQueue_emitsNoSpan() {
    createSessionChannel();

    drainHandler.drain();

    final var spans = spanExporter.getFinishedSpanItems();
    assertTrue(
        spans.isEmpty(),
        "Idle drain (queue + ackQueue both empty) MUST NOT emit any span; got " + spans.size());
  }

  /**
   * Non-empty drain — one frame in the queue triggers fan-out to one session, ending the cycle with
   * {@code drained=1}. The drain handler MUST emit exactly one {@code ws.drain.cycle} span with
   * {@code drain.cycle.items=1} and a positive {@code drain.cycle.duration_nanos}.
   */
  @Test
  void drain_oneEntry_emitsOneSpanWithCorrectAttributes() {
    final var ch = createSessionChannel();
    final int length = SbeTestEncoder.encodeCommandAck(sbeBuffer, 0, 1L, CommandAckStatus.Accepted);
    enqueueEntry(70, length);

    drainHandler.drain();

    // Consume the outbound frame so the leak detector stays happy.
    final var frame = (BinaryWebSocketFrame) ch.readOutbound();
    assertNotNull(frame, "Drain must produce one outbound frame");
    frame.release();

    final var spans = spanExporter.getFinishedSpanItems();
    assertEquals(1, spans.size(), "Drain with one frame must emit exactly one span");

    final SpanData span = spans.get(0);
    assertEquals(
        WebSocketDrainHandler.drainCycleSpanName(),
        span.getName(),
        "Span name must equal the ws.drain.cycle constant");

    final var attrs = span.getAttributes();
    final Long items =
        attrs.get(
            io.opentelemetry.api.common.AttributeKey.longKey(
                WebSocketDrainHandler.attrDrainCycleItems()));
    assertNotNull(items, "Span must carry drain.cycle.items attribute");
    assertEquals(1L, items.longValue(), "drain.cycle.items must equal the number drained (1)");

    final Long durationNanos =
        attrs.get(
            io.opentelemetry.api.common.AttributeKey.longKey(
                WebSocketDrainHandler.attrDrainCycleDurationNanos()));
    assertNotNull(durationNanos, "Span must carry drain.cycle.duration_nanos attribute");
    assertTrue(
        durationNanos.longValue() >= 0L,
        "drain.cycle.duration_nanos must be non-negative; got " + durationNanos);
  }
}
