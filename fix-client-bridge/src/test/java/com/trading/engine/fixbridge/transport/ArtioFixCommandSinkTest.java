package com.trading.engine.fixbridge.transport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.trading.engine.fixbridge.json.BrowserEvent;
import com.trading.engine.fixbridge.json.BrowserMessageReader;
import com.trading.engine.fixbridge.json.MutableParsedMessage;
import com.trading.engine.fixbridge.json.OrderRejectReason;
import com.trading.engine.fixbridge.quote.SessionId;
import com.trading.engine.fixbridge.ratelimit.PerTypeRateLimiter;
import com.trading.engine.fixbridge.translator.JsonToFixTranslator;
import com.trading.engine.fixbridge.translator.QuoteSnapshot;
import com.trading.engine.websocket.JwtValidator.ValidatedClaims;
import io.netty.buffer.Unpooled;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.agrona.concurrent.EpochNanoClock;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import uk.co.real_logic.artio.builder.Encoder;

/**
 * Unit tests for {@link ArtioFixCommandSink} — the production {@link FixCommandSink} impl.
 *
 * <p>Verifies:
 *
 * <ul>
 *   <li>Happy paths: QuoteRequest, NewOrderSingle, CancelOrder, OrderStatusRequest — all delegate
 *       to the translator and propagate the position returned by the {@link FixSessionAdapter}.
 *   <li>AcceptQuote happy path (snapshot present): translates, sends, evicts cache, returns
 *       position from the adapter.
 *   <li>AcceptQuote miss (snapshot absent): enqueues {@link BrowserEvent.OrderReject} with reason
 *       {@code QUOTE_EXPIRED}; returns {@link FixCommandSink#NO_SEND}; adapter not called.
 *   <li>RejectQuote: evicts cache slot; no FIX wire activity; returns {@link
 *       FixCommandSink#NO_SEND}.
 *   <li>Position values are propagated verbatim from the {@link FixSessionAdapter} to the caller.
 * </ul>
 *
 * <p><b>Threading.</b> Single-threaded — test-only.
 *
 * <p><b>Allocation.</b> Test-only — allocation is acceptable.
 */
final class ArtioFixCommandSinkTest {

  /** Fixed wall-clock for deterministic translator behaviour (FIX TransactTime stamps). */
  private static final EpochNanoClock FIXED_CLOCK = () -> 1_712_491_200_000_000_000L;

  /** Bridge process tag used across tests (24-bit hex value for locked §4 ClOrdID). */
  private static final long INSTANCE_TAG = 0xABCDEFL;

  /**
   * Per-session token used across tests. Production code receives this from a launcher-owned {@code
   * AtomicLong} sequence; tests pin a stable value so ClOrdID assertions are deterministic.
   */
  private static final long SESSION_TOKEN = 0x12345L;

  // ---------------------------------------------------------------------------
  // Test doubles.
  // ---------------------------------------------------------------------------

  /**
   * Recording {@link FixSessionAdapter} that captures every encoder handed to it and returns a
   * configurable position.
   */
  private static final class CapturingAdapter implements FixSessionAdapter {

    Encoder lastEncoder;
    int callCount;
    long fixedPosition;

    CapturingAdapter(final long fixedPosition) {
      this.fixedPosition = fixedPosition;
    }

    @Override
    public long trySend(final Encoder encoder) {
      this.lastEncoder = encoder;
      this.callCount++;
      return fixedPosition;
    }
  }

  /**
   * In-memory {@link QuoteSnapshotCache} backed by a {@link Map}. Uses the quoteId string as the
   * key to keep lookups simple in tests. Tracks eviction calls separately.
   */
  private static final class MapQuoteSnapshotCache implements QuoteSnapshotCache {

    private final Map<String, QuoteSnapshot> store = new HashMap<>();
    int evictCallCount;
    String lastEvictedKey;

    void put(final String quoteId, final QuoteSnapshot snapshot) {
      store.put(quoteId, snapshot);
    }

    @Override
    public QuoteSnapshot lookup(final byte[] buf, final int off, final int len) {
      final var key = new String(buf, off, len, StandardCharsets.US_ASCII);
      return store.get(key);
    }

    @Override
    public void evict(final byte[] buf, final int off, final int len) {
      lastEvictedKey = new String(buf, off, len, StandardCharsets.US_ASCII);
      store.remove(lastEvictedKey);
      evictCallCount++;
    }
  }

  // ---------------------------------------------------------------------------
  // Shared fixtures.
  // ---------------------------------------------------------------------------

  private BridgeSession session;
  private CapturingAdapter adapter;
  private MapQuoteSnapshotCache cache;
  private ArtioFixCommandSink sink;

  @BeforeEach
  void setUp() {
    final var queue = new OutboundQueue(64);
    final var claims =
        new ValidatedClaims(
            "user-1", "jti-1", List.of("ACME-001"), Long.MAX_VALUE, true, List.of());
    final var limiter = new PerTypeRateLimiter(0L);
    session =
        new BridgeSession(
            new SessionId("session-test-001"),
            claims,
            InetAddress.getLoopbackAddress(),
            queue,
            limiter);

    adapter = new CapturingAdapter(42L); // default: success position 42
    cache = new MapQuoteSnapshotCache();
    final var translator = new JsonToFixTranslator(FIXED_CLOCK);
    sink =
        new ArtioFixCommandSink(session, adapter, translator, cache, INSTANCE_TAG, SESSION_TOKEN);
  }

  // ---------------------------------------------------------------------------
  // Helpers.
  // ---------------------------------------------------------------------------

  /** Parse a JSON frame via {@link BrowserMessageReader} into a flyweight. */
  private static MutableParsedMessage parse(final String json) {
    final var msg = new MutableParsedMessage();
    final var src = Unpooled.wrappedBuffer(json.getBytes(StandardCharsets.UTF_8));
    BrowserMessageReader.parse(src, msg);
    return msg;
  }

  /**
   * Build a bound {@link QuoteSnapshot} for EURUSD with canned values — sufficient for the
   * AcceptQuote happy-path.
   */
  private static QuoteSnapshot boundSnapshot() {
    final var snap = new QuoteSnapshot();
    final byte[] sym = "EURUSD".getBytes(StandardCharsets.US_ASCII);
    snap.bind(
        sym,
        0,
        sym.length,
        MutableParsedMessage.SIDE_BUY,
        /* qtyInt64 */ 100_000_000L,
        /* bidValue */ 110_000_000L, /* bidScale */
        8,
        /* askValue */ 110_100_000L, /* askScale */
        8,
        Long.MAX_VALUE);
    return snap;
  }

  // ---------------------------------------------------------------------------
  // QuoteRequest happy path.
  // ---------------------------------------------------------------------------

  @Test
  void sendQuoteRequest_happyPath_delegatesToAdapterAndReturnsPosition() {
    final var parsed =
        parse(
            "{\"type\":\"QuoteRequest\",\"reqId\":\"R-1\","
                + "\"symbol\":\"EURUSD\",\"side\":\"Buy\",\"qty\":\"1000000\"}");

    final long position = sink.sendQuoteRequest(parsed, 1_000L);

    assertEquals(42L, position, "position must equal adapter's return value");
    assertEquals(1, adapter.callCount, "adapter must be called exactly once");
    assertNotNull(adapter.lastEncoder, "adapter must receive a non-null encoder");
  }

  @Test
  void sendQuoteRequest_adapterReturnsBackpressure_positionPropagated() {
    adapter.fixedPosition = FixCommandSink.NO_SEND;
    final var parsed =
        parse(
            "{\"type\":\"QuoteRequest\",\"reqId\":\"R-2\","
                + "\"symbol\":\"EURUSD\",\"side\":\"Sell\",\"qty\":\"500000\"}");

    final long position = sink.sendQuoteRequest(parsed, 2_000L);

    assertEquals(FixCommandSink.NO_SEND, position);
    assertEquals(1, adapter.callCount);
  }

  // ---------------------------------------------------------------------------
  // NewOrderSingle happy path.
  // ---------------------------------------------------------------------------

  @Test
  void sendNewOrderSingle_happyPath_delegatesToAdapterAndReturnsPosition() {
    final var parsed =
        parse(
            "{\"type\":\"NewOrderSingle\",\"clOrdId\":\"CLI-1\","
                + "\"symbol\":\"EURUSD\",\"side\":\"Buy\",\"qty\":\"100000\","
                + "\"price\":\"1.10000000\",\"ordType\":\"Limit\",\"timeInForce\":\"GTC\"}");

    final long position = sink.sendNewOrderSingle(parsed, 3_000L);

    assertEquals(42L, position);
    assertEquals(1, adapter.callCount);
    assertNotNull(adapter.lastEncoder);
  }

  @Test
  void sendNewOrderSingle_positionFromAdapter_isReturnedVerbatim() {
    adapter.fixedPosition = 9999L;
    final var parsed =
        parse(
            "{\"type\":\"NewOrderSingle\",\"clOrdId\":\"CLI-2\","
                + "\"symbol\":\"USDJPY\",\"side\":\"Sell\",\"qty\":\"50000\","
                + "\"ordType\":\"Market\"}");

    final long position = sink.sendNewOrderSingle(parsed, 4_000L);

    assertEquals(9999L, position);
  }

  // ---------------------------------------------------------------------------
  // CancelOrder happy path.
  // ---------------------------------------------------------------------------

  @Test
  void sendCancelOrder_happyPath_delegatesToAdapterAndReturnsPosition() {
    final var parsed =
        parse(
            "{\"type\":\"CancelOrder\",\"origClOrdId\":\"CLI-1\","
                + "\"clOrdId\":\"CLI-2\",\"symbol\":\"EURUSD\",\"side\":\"Buy\"}");

    final long position = sink.sendCancelOrder(parsed, 5_000L);

    assertEquals(42L, position);
    assertEquals(1, adapter.callCount);
    assertNotNull(adapter.lastEncoder);
  }

  @Test
  void sendCancelOrder_adapterPositionPropagated() {
    adapter.fixedPosition = 7L;
    final var parsed =
        parse(
            "{\"type\":\"CancelOrder\",\"origClOrdId\":\"CLI-3\","
                + "\"symbol\":\"GBPUSD\",\"side\":\"Sell\"}");

    final long position = sink.sendCancelOrder(parsed, 6_000L);

    assertEquals(7L, position);
  }

  // ---------------------------------------------------------------------------
  // OrderStatusRequest — always NO_SEND (projection side).
  // ---------------------------------------------------------------------------

  @Test
  void sendOrderStatusRequest_alwaysReturnsNoSend() {
    final var parsed = parse("{\"type\":\"OrderStatusRequest\",\"clOrdId\":\"CLI-1\"}");

    final long position = sink.sendOrderStatusRequest(parsed, 7_000L);

    assertEquals(FixCommandSink.NO_SEND, position, "OrderStatusRequest must never touch FIX wire");
    assertEquals(0, adapter.callCount, "adapter must NOT be called for OrderStatusRequest");
  }

  // ---------------------------------------------------------------------------
  // AcceptQuote — happy path (snapshot present).
  // ---------------------------------------------------------------------------

  @Test
  void sendAcceptQuote_snapshotPresent_translatesAndSendsAndEvicts() {
    final String quoteId = "Q-001";
    cache.put(quoteId, boundSnapshot());

    final var parsed = parse("{\"type\":\"AcceptQuote\",\"quoteId\":\"" + quoteId + "\"}");

    final long position = sink.sendAcceptQuote(parsed, 8_000L);

    assertEquals(42L, position, "position must equal adapter return value");
    assertEquals(1, adapter.callCount, "adapter must be called once on cache hit");
    assertNotNull(adapter.lastEncoder, "encoder must be forwarded to adapter");
    assertEquals(1, cache.evictCallCount, "cache slot must be evicted after successful send");
    assertEquals(quoteId, cache.lastEvictedKey, "evicted key must match quoteId");
  }

  @Test
  void sendAcceptQuote_snapshotPresent_backpressureFromAdapter_doesNotEvict() {
    // When trySend returns backpressure, the cache slot must NOT be evicted (locked §2).
    adapter.fixedPosition = FixCommandSink.NO_SEND;
    final String quoteId = "Q-002";
    cache.put(quoteId, boundSnapshot());

    final var parsed = parse("{\"type\":\"AcceptQuote\",\"quoteId\":\"" + quoteId + "\"}");

    final long position = sink.sendAcceptQuote(parsed, 9_000L);

    assertEquals(FixCommandSink.NO_SEND, position);
    assertEquals(0, cache.evictCallCount, "cache must NOT be evicted on backpressure");
  }

  // ---------------------------------------------------------------------------
  // AcceptQuote — miss path (snapshot absent).
  // ---------------------------------------------------------------------------

  @Test
  void sendAcceptQuote_snapshotAbsent_enqueuesOrderRejectAndReturnsNoSend() {
    // Cache is empty — no snapshot for the quoteId.
    final var parsed =
        parse("{\"type\":\"AcceptQuote\",\"quoteId\":\"Q-MISSING\",\"clOrdId\":\"CLI-10\"}");

    final long position = sink.sendAcceptQuote(parsed, 10_000L);

    assertEquals(FixCommandSink.NO_SEND, position, "must return NO_SEND on cache miss");
    assertEquals(0, adapter.callCount, "adapter must NOT be called on cache miss");

    // Verify the OrderReject was enqueued on the session's outbound queue.
    final var queued = session.outboundQueue().poll();
    assertNotNull(queued, "OrderReject must be enqueued");
    final var reject = (BrowserEvent.OrderReject) queued;
    assertEquals(
        OrderRejectReason.QUOTE_EXPIRED.wireValue(),
        reject.reason(),
        "reject reason must be QUOTE_EXPIRED");
    assertEquals("CLI-10", reject.clOrdId(), "clOrdId must be the browser-supplied value");
  }

  @Test
  void sendAcceptQuote_snapshotAbsent_noClOrdIdInMessage_mintedClOrdIdUsed() {
    // Message without a browser-supplied clOrdId — sink mints one.
    final var parsed = parse("{\"type\":\"AcceptQuote\",\"quoteId\":\"Q-MISSING2\"}");

    sink.sendAcceptQuote(parsed, 11_000L);

    final var reject = (BrowserEvent.OrderReject) session.outboundQueue().poll();
    assertNotNull(reject, "OrderReject must be enqueued even without browser clOrdId");
    assertNotNull(reject.clOrdId(), "minted clOrdId must be non-null");
    assertEquals(
        JsonToFixTranslator.CLORDID_LENGTH,
        reject.clOrdId().length(),
        "minted clOrdId must be exactly 20 bytes per locked §4");
  }

  // ---------------------------------------------------------------------------
  // RejectQuote — evicts cache; no FIX wire activity.
  // ---------------------------------------------------------------------------

  @Test
  void handleRejectQuote_snapshotPresent_evictsAndReturnsNoSend() {
    final String quoteId = "Q-REJ-001";
    cache.put(quoteId, boundSnapshot());

    final var parsed = parse("{\"type\":\"RejectQuote\",\"quoteId\":\"" + quoteId + "\"}");

    final long position = sink.handleRejectQuote(parsed, 12_000L);

    assertEquals(
        FixCommandSink.NO_SEND, position, "RejectQuote must never produce FIX wire activity");
    assertEquals(0, adapter.callCount, "adapter must NOT be called for RejectQuote");
    assertEquals(1, cache.evictCallCount, "cache slot must be evicted");
    assertEquals(quoteId, cache.lastEvictedKey);
  }

  @Test
  void handleRejectQuote_snapshotAbsent_noopEvictAndReturnsNoSend() {
    // Evicting a non-existent entry is a safe no-op (per QuoteSnapshotCache contract).
    final var parsed = parse("{\"type\":\"RejectQuote\",\"quoteId\":\"Q-REJ-MISSING\"}");

    final long position = sink.handleRejectQuote(parsed, 13_000L);

    assertEquals(FixCommandSink.NO_SEND, position);
    assertEquals(0, adapter.callCount);
    // evictCallCount is 1 because our map-backed cache always increments even for missing key.
    assertEquals(1, cache.evictCallCount);
  }

  // ---------------------------------------------------------------------------
  // Constructor null-checks.
  // ---------------------------------------------------------------------------

  @Test
  void constructor_nullSession_throws() {
    final var translator = new JsonToFixTranslator(FIXED_CLOCK);
    org.junit.jupiter.api.Assertions.assertThrows(
        NullPointerException.class,
        () ->
            new ArtioFixCommandSink(null, adapter, translator, cache, INSTANCE_TAG, SESSION_TOKEN));
  }

  @Test
  void constructor_nullFixSession_throws() {
    final var translator = new JsonToFixTranslator(FIXED_CLOCK);
    org.junit.jupiter.api.Assertions.assertThrows(
        NullPointerException.class,
        () ->
            new ArtioFixCommandSink(session, null, translator, cache, INSTANCE_TAG, SESSION_TOKEN));
  }

  @Test
  void constructor_nullTranslator_throws() {
    org.junit.jupiter.api.Assertions.assertThrows(
        NullPointerException.class,
        () -> new ArtioFixCommandSink(session, adapter, null, cache, INSTANCE_TAG, SESSION_TOKEN));
  }

  @Test
  void constructor_nullQuoteCache_throws() {
    final var translator = new JsonToFixTranslator(FIXED_CLOCK);
    org.junit.jupiter.api.Assertions.assertThrows(
        NullPointerException.class,
        () ->
            new ArtioFixCommandSink(
                session, adapter, translator, null, INSTANCE_TAG, SESSION_TOKEN));
  }
}
