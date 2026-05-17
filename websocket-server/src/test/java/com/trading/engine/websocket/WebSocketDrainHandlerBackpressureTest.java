package com.trading.engine.websocket;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.trading.engine.messages.sbe.CommandAckStatus;
import com.trading.engine.projections.SymbolPacker;
import com.trading.engine.testsupport.clock.ControllableNanoClock;
import com.trading.engine.testsupport.sbe.SbeTestEncoder;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.netty.buffer.Unpooled;
import io.netty.channel.DefaultChannelId;
import io.netty.channel.WriteBufferWaterMark;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import io.netty.util.ReferenceCounted;
import io.netty.util.ResourceLeakDetector;
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
 * Back-pressure guard tests for {@link WebSocketDrainHandler} on the reliable path.
 *
 * <p>Validates the {@code !ch.isWritable()} drop path in {@code writeReliableToSession}:
 *
 * <ul>
 *   <li>Frame is NOT written to the channel (no {@link BinaryWebSocketFrame} in the outbound
 *       queue).
 *   <li>{@code websocket.egress.dropped.channel-not-writable} counter increments on every drop.
 *   <li>The reliable sequence counter does NOT advance — no seqNo is burned on a frame the client
 *       never received, per the Javadoc back-pressure guard contract.
 *   <li>The {@link ReliableStreamTracker} does NOT capture the dropped frame — lookup at seqNo=1
 *       returns -1.
 *   <li>A second drop within 1 s (clock frozen) still increments the metric counter by 1 even
 *       though the warn log is suppressed.
 * </ul>
 *
 * <p><b>Making the channel non-writable.</b> {@link EmbeddedChannel#isWritable()} is {@code true}
 * by default. Setting {@code WriteBufferWaterMark(0, 0)} brings the high water mark to zero bytes;
 * any pending unflushed write then trips the water mark and makes {@code isWritable()} return
 * {@code false}. We prime the channel by writing a single-byte placeholder without flushing.
 *
 * <p><b>Counter introspection.</b> {@link WebSocketMetrics} is constructed with an explicit {@link
 * SimpleMeterRegistry} so {@code registry.get(name).counter().count()} returns the current value.
 * This matches the pattern used in {@code WebSocketMetricsTest} and {@code
 * SubscriptionFilterEmptyTest}.
 *
 * <p><b>Threading.</b> All actions run on the test (JUnit) thread — {@link EmbeddedChannel} is
 * synchronous and the drain handler contract is single-threaded Netty event-loop.
 *
 * @see WebSocketDrainHandler#writeReliableToAllChannels
 */
final class WebSocketDrainHandlerBackpressureTest {

  /** Queue / pool capacity — power of two for {@link ManyToOneConcurrentArrayQueue}. */
  private static final int CAPACITY = 4;

  /** Max SBE message size per entry. */
  private static final int MAX_MESSAGE_SIZE = 1024;

  /**
   * Frame size for {@link ReliableStreamTracker} — must be {@code > SLOT_HEADER_SIZE} (16). 512
   * bytes gives ample room for any CommandAck or OrderCreatedEvent payload.
   */
  private static final int TRACKER_FRAME_SIZE = 512;

  /** Ring capacity for the tracker — must be a positive power of two. */
  private static final int TRACKER_CAPACITY = 4;

  /** Metric name for the channel-not-writable drop counter. */
  private static final String METRIC_EGRESS_DROPPED =
      "websocket.egress.dropped.channel-not-writable";

  private SimpleMeterRegistry registry;
  private ManyToOneConcurrentArrayQueue<EgressEntry> queue;
  private ManyToOneConcurrentArrayQueue<EgressEntry> returnQueue;
  private WebSocketMetrics metrics;
  private WebSocketEgressListener egressListener;
  private WebSocketSessionManager sessionManager;
  private WebSocketDrainHandler drainHandler;
  private ControllableNanoClock clock;
  private MutableDirectBuffer sbeBuffer;

  /** Channels opened during the test — closed in {@link #tearDown()}. */
  private final List<EmbeddedChannel> openChannels = new ArrayList<>();

  @BeforeAll
  static void enableLeakDetection() {
    ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID);
  }

  @BeforeEach
  void setUp() {
    // Explicit registry so tests can read counter values via registry.get(name).counter().count().
    registry = new SimpleMeterRegistry();
    clock = new ControllableNanoClock(0L);
    queue = new ManyToOneConcurrentArrayQueue<>(CAPACITY);
    returnQueue = new ManyToOneConcurrentArrayQueue<>(CAPACITY);
    metrics = new WebSocketMetrics(registry);
    egressListener =
        new WebSocketEgressListener(queue, returnQueue, metrics, CAPACITY, MAX_MESSAGE_SIZE);
    sbeBuffer = new ExpandableArrayBuffer(MAX_MESSAGE_SIZE);

    final var config = WebSocketServerConfig.builder().build();
    sessionManager = new WebSocketSessionManager(config, metrics, clock);
    drainHandler = new WebSocketDrainHandler(queue, egressListener, sessionManager, metrics, clock);
  }

  @AfterEach
  void tearDown() {
    for (final var ch : openChannels) {
      ch.finishAndReleaseAll();
    }
    openChannels.clear();
  }

  // ---------------------------------------------------------------------------
  // Test helpers
  // ---------------------------------------------------------------------------

  /**
   * Create an {@link EmbeddedChannel} with a unique {@link DefaultChannelId}, register a session,
   * initialise the subscription filter with a wildcard match (all symbols + all event types), set
   * the entitled accounts, and initialise the {@link ReliableStreamTracker}.
   *
   * @return the created channel (the associated session is registered in the session manager)
   */
  private EmbeddedChannel createTrackedSession() {
    final var ch = new EmbeddedChannel(DefaultChannelId.newInstance());
    openChannels.add(ch);

    final var session = sessionManager.tryRegister(ch);
    assertTrue(session != null, "Session must be created (not null)");

    // Wildcard subscription: globalEventBitMask = 0x1F (all bits set) so CommandAck (templateId=70)
    // passes through without filtering. subscriptionFilter must be non-null for the drain handler
    // to deliver reliable frames (null filter skips the session as pre-auth).
    session.initSubscriptionFilter(100, metrics);
    session.subscriptionFilter().addSubscription(0L, 0x1F);
    session.subscriptionFilter().addSubscription(SymbolPacker.pack("EURUSD  "), 0x1F);

    // Grant an entitled account so account-entitlement check passes in writeReliableToAllChannels.
    session.entitledAccounts(Set.of("TEST-ACCT"));

    // Init reliable-stream tracker so the capture path in writeReliableToSession is exercised.
    session.initReliableStreamTracker(TRACKER_CAPACITY, TRACKER_FRAME_SIZE, metrics);

    return ch;
  }

  /**
   * Force the channel into a non-writable state:
   *
   * <ol>
   *   <li>Set {@code WriteBufferWaterMark(0, 0)} — high water mark = 0 bytes.
   *   <li>Write one pending byte (unflushed) — any pending byte trips the water mark.
   * </ol>
   *
   * After this call {@code ch.isWritable()} returns {@code false}.
   *
   * @param ch the channel to make non-writable
   */
  private static void makeNonWritable(final EmbeddedChannel ch) {
    ch.config().setWriteBufferWaterMark(new WriteBufferWaterMark(0, 0));
    // Unflushed write — no flush() / flushOutbound() call yet. The pending byte trips the
    // watermark and makes isWritable() return false.
    ch.write(Unpooled.wrappedBuffer(new byte[] {0x00}));
  }

  /**
   * Flush the primer byte injected by {@link #makeNonWritable(EmbeddedChannel)} out of the channel
   * and release it. Must be called before asserting on drain-handler outbound writes so the primer
   * does not masquerade as a drain-handler write.
   *
   * @param ch the channel to flush and clear the primer from
   */
  private static void flushAndDiscardPrimer(final EmbeddedChannel ch) {
    ch.flushOutbound();
    final Object primer = ch.readOutbound();
    if (primer instanceof ReferenceCounted rc) {
      rc.release();
    }
  }

  /**
   * Encode a {@code CommandAck(Accepted)} (templateId = 70, reliable per {@link
   * EgressEntry#isReliable()}) into the shared {@link #sbeBuffer}.
   *
   * @return the encoded length
   */
  private int encodeCommandAck() {
    return SbeTestEncoder.encodeCommandAck(sbeBuffer, 0, 1L, CommandAckStatus.Accepted);
  }

  /**
   * Create a new {@link EgressEntry}, copy the encoded bytes from the shared SBE buffer, and offer
   * it to the egress queue.
   *
   * @param templateId the SBE templateId to set on the entry
   * @param length the message length
   */
  private void enqueueEntry(final int templateId, final int length) {
    final var entry = new EgressEntry(MAX_MESSAGE_SIZE);
    sbeBuffer.getBytes(0, entry.bytes(), 0, length);
    entry.setMetadata(length, templateId);
    assertTrue(queue.offer(entry), "Must be able to offer entry to queue");
  }

  /** Read the current value of {@link #METRIC_EGRESS_DROPPED} from the {@link #registry}. */
  private double droppedCount() {
    return registry.get(METRIC_EGRESS_DROPPED).counter().count();
  }

  // ---------------------------------------------------------------------------
  // Tests
  // ---------------------------------------------------------------------------

  /**
   * drain_reliableFrame_channelNotWritable_frameDropped: the drain handler must NOT write any
   * {@link BinaryWebSocketFrame} when the channel reports {@code !isWritable()}.
   */
  @Test
  void drain_reliableFrame_channelNotWritable_frameDropped() {
    final var ch = createTrackedSession();
    makeNonWritable(ch);
    assertTrue(!ch.isWritable(), "Pre-condition: channel must report !isWritable()");

    final int length = encodeCommandAck();
    enqueueEntry(70, length); // templateId=70 (CommandAck) — isReliable() returns true

    drainHandler.drain();

    // Flush and discard the primer byte so we don't mistake it for a drain-handler write.
    flushAndDiscardPrimer(ch);

    // No BinaryWebSocketFrame must appear after the primer.
    final Object frame = ch.readOutbound();
    assertNull(frame, "No BinaryWebSocketFrame must be written when channel !isWritable()");
  }

  /**
   * drain_reliableFrame_channelNotWritable_droppedCounterIncremented: the {@code
   * websocket.egress.dropped.channel-not-writable} counter must increment by exactly 1 per dropped
   * frame.
   */
  @Test
  void drain_reliableFrame_channelNotWritable_droppedCounterIncremented() {
    final var ch = createTrackedSession();
    makeNonWritable(ch);

    final int length = encodeCommandAck();
    enqueueEntry(70, length);

    final double before = droppedCount();
    drainHandler.drain();
    final double after = droppedCount();

    assertEquals(
        before + 1.0,
        after,
        1e-9,
        "egress.dropped.channel-not-writable must increment by 1 per dropped reliable frame");
  }

  /**
   * drain_reliableFrame_channelNotWritable_seqCounterNotAdvanced: the reliable sequence counter
   * must NOT advance when the frame is dropped — no seqNo is burned on a frame the client never
   * received. Seqno stays at 0 (the pre-drain baseline).
   */
  @Test
  void drain_reliableFrame_channelNotWritable_seqCounterNotAdvanced() {
    final var ch = createTrackedSession();
    makeNonWritable(ch);

    final var session = sessionManager.findSession(ch);
    final long seqBefore = session.reliableSeqCounter();

    final int length = encodeCommandAck();
    enqueueEntry(70, length);

    drainHandler.drain();

    final long seqAfter = session.reliableSeqCounter();
    assertEquals(
        seqBefore,
        seqAfter,
        "Reliable seqNo counter must NOT advance when frame is dropped due to !isWritable() — "
            + "burning a seqNo on a dropped frame would create a gap the client cannot fill");
  }

  /**
   * drain_reliableFrame_channelNotWritable_trackerDoesNotCaptureFrame: the {@link
   * ReliableStreamTracker} must NOT capture a frame that was dropped before seqNo assignment.
   * {@code tracker.lookupLength(1)} must return -1.
   */
  @Test
  void drain_reliableFrame_channelNotWritable_trackerDoesNotCaptureFrame() {
    final var ch = createTrackedSession();
    makeNonWritable(ch);

    final var session = sessionManager.findSession(ch);
    final var tracker = session.reliableStreamTracker();

    final int length = encodeCommandAck();
    enqueueEntry(70, length);

    drainHandler.drain();

    // seqNo=1 is the first sequence number that would have been assigned if the drop check had
    // NOT fired before nextReliableSeqNo(). lookupLength returns -1 if the slot is empty.
    assertEquals(
        -1,
        tracker.lookupLength(1L),
        "ReliableStreamTracker must NOT capture a frame dropped before seqNo assignment — "
            + "phantom capture would deliver a ghost frame to the client on gap-request replay");
  }

  /**
   * drain_reliableFrame_channelNotWritable_secondDropIncrementCounter: a second drop within 1 s
   * (clock frozen at 0) must still increment the counter by 1 even though the warn log is
   * suppressed (the per-session 1-warn-per-second rate limit applies only to the log, not the
   * metric). Total counter must be 2 after two drops.
   */
  @Test
  void drain_reliableFrame_channelNotWritable_secondDropIncrementCounter() {
    final var ch = createTrackedSession();
    makeNonWritable(ch);

    final int length = encodeCommandAck();

    // First drop.
    enqueueEntry(70, length);
    drainHandler.drain();

    // Second drop — clock is still at 0 (frozen), within the 1-second warn suppression window.
    enqueueEntry(70, length);
    drainHandler.drain();

    assertEquals(
        2.0,
        droppedCount(),
        1e-9,
        "egress.dropped.channel-not-writable must reflect EVERY drop even when warn is suppressed "
            + "— the metric must be monotonic regardless of the log rate limit");
  }
}
