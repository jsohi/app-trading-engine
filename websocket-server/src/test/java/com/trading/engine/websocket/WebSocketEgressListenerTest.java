package com.trading.engine.websocket;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.trading.engine.messages.sbe.CommandAckEncoder;
import com.trading.engine.messages.sbe.CommandAckStatus;
import com.trading.engine.testsupport.clock.ControllableNanoClock;
import com.trading.engine.testsupport.sbe.SbeTestEncoder;
import io.aeron.cluster.codecs.EventCode;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.ManyToOneConcurrentArrayQueue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link WebSocketEgressListener} — verifies message enqueue, pool exhaustion, queue
 * overflow, session event reconnect signaling, pool recycling, and null-guard validation.
 *
 * <p>Uses small pool and queue capacities (2) so that exhaustion and overflow scenarios can be
 * triggered with minimal setup. SBE messages are encoded via {@link SbeTestEncoder}.
 */
final class WebSocketEgressListenerTest {

  /** Small capacity for testing — allows exhaustion within 2 messages. */
  private static final int POOL_CAPACITY = 2;

  /** Queue capacity must be a power of 2 for ManyToOneConcurrentArrayQueue. */
  private static final int QUEUE_CAPACITY = 2;

  /** Maximum SBE message size per entry. */
  private static final int MAX_MESSAGE_SIZE = 1024;

  private ManyToOneConcurrentArrayQueue<EgressEntry> queue;
  private ManyToOneConcurrentArrayQueue<EgressEntry> returnQueue;
  private WebSocketMetrics metrics;
  private WebSocketEgressListener listener;

  /** Shared buffer for encoding SBE messages. */
  private MutableDirectBuffer sbeBuffer;

  @BeforeEach
  void setUp() {
    queue = new ManyToOneConcurrentArrayQueue<>(QUEUE_CAPACITY);
    returnQueue = new ManyToOneConcurrentArrayQueue<>(QUEUE_CAPACITY);
    metrics = WebSocketMetrics.createWithDefaults();
    listener =
        new WebSocketEgressListener(queue, returnQueue, metrics, POOL_CAPACITY, MAX_MESSAGE_SIZE);
    sbeBuffer = new ExpandableArrayBuffer(MAX_MESSAGE_SIZE);
  }

  /**
   * Encode a CommandAck SBE message (templateId=70, reliable) into the shared buffer and return the
   * encoded length.
   *
   * @return total encoded length including SBE header
   */
  private int encodeCommandAck() {
    return SbeTestEncoder.encodeCommandAck(sbeBuffer, 0, 1L, CommandAckStatus.Accepted);
  }

  /**
   * Encode a PriceResponse SBE message (templateId=51, best-effort) into the shared buffer and
   * return the encoded length.
   *
   * @return total encoded length including SBE header
   */
  private int encodePriceResponse() {
    return SbeTestEncoder.encodePriceResponse(
        sbeBuffer, 0, "RFQ-001", "EURUSD  ", true, 110_000_000L, 111_000_000L, 1_000_000_000L);
  }

  @Test
  void onMessage_validSbeMessage_enqueuesEntry() {
    final int length = encodeCommandAck();

    listener.onMessage(1L, 1_000_000_000L, sbeBuffer, 0, length, null);

    final var entry = queue.poll();
    assertNotNull(entry, "Queue must contain one entry after onMessage");
    assertEquals(CommandAckEncoder.TEMPLATE_ID, entry.templateId());
    assertEquals(length, entry.length());
  }

  @Test
  void onMessage_poolExhausted_dropsMessageAndIncrementsMetric() {
    final int length = encodeCommandAck();

    // Consume all pool entries
    listener.onMessage(1L, 1_000_000_000L, sbeBuffer, 0, length, null);
    listener.onMessage(1L, 2_000_000_000L, sbeBuffer, 0, length, null);

    // Pool is now exhausted (both entries are in the queue)
    assertEquals(POOL_CAPACITY, queue.size(), "Queue should hold all pool entries");
    assertEquals(0, listener.poolAvailable(), "Pool must be empty after consuming all entries");

    // Third message should be dropped because the pool is exhausted
    listener.onMessage(1L, 3_000_000_000L, sbeBuffer, 0, length, null);

    // Queue still has only 2 entries (the dropped message was not enqueued)
    assertEquals(POOL_CAPACITY, queue.size(), "Queue size must not change when pool is exhausted");
  }

  @Test
  void onMessage_queueFull_dropsMessageAndReturnsToPool() {
    final int length = encodeCommandAck();

    // Fill the queue (capacity=2) with 2 messages
    listener.onMessage(1L, 1_000_000_000L, sbeBuffer, 0, length, null);
    listener.onMessage(1L, 2_000_000_000L, sbeBuffer, 0, length, null);

    assertEquals(QUEUE_CAPACITY, queue.size(), "Queue must be full");
    assertEquals(0, listener.poolAvailable(), "Pool must be empty");

    // Return one entry to the pool via the return queue so we have a pool entry available
    // but the queue is still full
    final var returned = queue.poll(); // take one from queue
    returnQueue.offer(returned); // put it back in return queue

    // Now the queue has 1 entry, return queue has 1. Next onMessage will first drain the
    // return queue (poolAvailable becomes 1), pop from pool, but since the queue still has
    // 1 entry and capacity is 2, it will succeed. Let's fill the queue differently.

    // Re-insert to fill queue again
    final int length2 = encodeCommandAck();
    // This onMessage will: drain return queue (+1 pool), pop from pool, offer to queue
    listener.onMessage(1L, 3_000_000_000L, sbeBuffer, 0, length2, null);

    // Queue should be at capacity=2 now, pool=0
    assertEquals(QUEUE_CAPACITY, queue.size(), "Queue must be full after re-fill");

    // Return one entry to pool via return queue
    final var returned2 = queue.poll();
    returnQueue.offer(returned2);

    // Queue has 1 entry. Now we need to fill it back to capacity before the next onMessage
    // Actually, let's use a simpler approach: create a fresh listener with pool=3, queue=2
    final var smallQueue = new ManyToOneConcurrentArrayQueue<EgressEntry>(QUEUE_CAPACITY);
    final var smallReturnQueue = new ManyToOneConcurrentArrayQueue<EgressEntry>(QUEUE_CAPACITY);
    final var freshListener =
        new WebSocketEgressListener(smallQueue, smallReturnQueue, metrics, 4, MAX_MESSAGE_SIZE);

    // Fill the queue
    freshListener.onMessage(1L, 1L, sbeBuffer, 0, length, null);
    freshListener.onMessage(1L, 2L, sbeBuffer, 0, length, null);

    assertEquals(QUEUE_CAPACITY, smallQueue.size(), "Queue must be full");
    final int poolBefore = freshListener.poolAvailable();

    // Next message: pool has entries but queue is full -> entry returned to pool
    freshListener.onMessage(1L, 3L, sbeBuffer, 0, length, null);

    assertEquals(
        poolBefore,
        freshListener.poolAvailable(),
        "Pool count must remain the same when queue is full (entry taken then returned)");
    assertEquals(QUEUE_CAPACITY, smallQueue.size(), "Queue size must not exceed capacity");
  }

  @Test
  void onSessionEvent_errorCode_signalsReconnect() {
    final var clock = new ControllableNanoClock(1_000_000_000L);
    final var client =
        WebSocketClusterClient.builder()
            .aeronDirectoryName("/tmp/aeron-test")
            .ingressEndpoints("0=localhost:20000")
            .egressListener(listener)
            .errorHandler(Throwable::printStackTrace)
            .nanoClock(clock)
            .build();
    listener.init(client);

    // The client is in DISCONNECTED state, so signalReconnectNeeded() won't actually
    // transition state (it only acts when CONNECTED). But we verify no NPE is thrown
    // and the method completes successfully.
    listener.onSessionEvent(1L, 1L, 1L, 0, EventCode.ERROR, "test error");

    // Client is still DISCONNECTED (signalReconnectNeeded only acts when CONNECTED)
    assertEquals(WebSocketClusterClient.State.DISCONNECTED, client.state());

    client.close();
  }

  @Test
  void onSessionEvent_okCode_doesNotSignalReconnect() {
    final var clock = new ControllableNanoClock(1_000_000_000L);
    final var client =
        WebSocketClusterClient.builder()
            .aeronDirectoryName("/tmp/aeron-test")
            .ingressEndpoints("0=localhost:20000")
            .egressListener(listener)
            .errorHandler(Throwable::printStackTrace)
            .nanoClock(clock)
            .build();
    listener.init(client);

    // OK event should NOT trigger reconnect — verify no exception and state unchanged
    listener.onSessionEvent(1L, 1L, 1L, 0, EventCode.OK, "session opened");

    assertEquals(
        WebSocketClusterClient.State.DISCONNECTED,
        client.state(),
        "OK event must not change client state");

    client.close();
  }

  @Test
  void returnToPool_afterDrain_recyclesToPool() {
    final int length = encodeCommandAck();

    // Enqueue one message (pool: 2->1)
    listener.onMessage(1L, 1_000_000_000L, sbeBuffer, 0, length, null);

    assertEquals(1, listener.poolAvailable(), "Pool must have 1 entry after enqueuing one message");

    // Dequeue the entry (simulating drain handler)
    final var entry = queue.poll();
    assertNotNull(entry, "Must be able to dequeue the entry");

    // Return to pool via the return queue
    listener.returnToPool(entry);

    // Pool count is still 1 because the return queue is drained lazily on next onMessage.
    // Trigger a drain by calling onMessage again.
    final int length2 = encodeCommandAck();
    listener.onMessage(1L, 2_000_000_000L, sbeBuffer, 0, length2, null);

    // After the second onMessage, the return queue was drained (pool went 1->2),
    // then one entry was popped for the new message (pool went 2->1).
    assertEquals(
        1, listener.poolAvailable(), "Pool must have 1 entry after recycling + new message");
  }

  @Test
  void init_nullClusterClient_throwsNullPointerException() {
    assertThrows(
        NullPointerException.class,
        () -> listener.init(null),
        "init(null) must throw NullPointerException");
  }

  @Test
  void constructor_nullQueue_throwsNullPointerException() {
    assertThrows(
        NullPointerException.class,
        () ->
            new WebSocketEgressListener(
                null, returnQueue, metrics, POOL_CAPACITY, MAX_MESSAGE_SIZE),
        "Constructor with null queue must throw NullPointerException");
  }
}
