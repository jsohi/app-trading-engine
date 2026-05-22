package com.trading.engine.cluster.handler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.trading.engine.cluster.IdGenerator;
import com.trading.engine.cluster.OrderBook;
import com.trading.engine.cluster.journal.EventJournal;
import com.trading.engine.cluster.sequencer.EventSequencer;
import com.trading.engine.cluster.state.TradingState;
import com.trading.engine.messages.sbe.HaltTradingCommandDecoder;
import com.trading.engine.messages.sbe.HaltTradingCommandEncoder;
import com.trading.engine.messages.sbe.MessageHeaderDecoder;
import com.trading.engine.messages.sbe.MessageHeaderEncoder;
import com.trading.engine.messages.sbe.TradingHaltActivatedEventDecoder;
import com.trading.engine.testsupport.aeron.FakeClientSession;
import com.trading.engine.testsupport.aeron.FakeCluster;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link HaltTradingCommandHandler} (template 17 → event 117).
 *
 * <p>Exercises: flag mutation idempotency, the {@code previouslyHalted} audit field, reason
 * round-trip, {@code adminSessionId} propagation, null-session sentinel, and cluster-timestamp
 * propagation through {@link EventSink#emit}.
 *
 * <p><b>Threading:</b> single-threaded — matches the cluster duty-cycle invariant.
 *
 * <p><b>Allocation:</b> test infrastructure (encoders, decoders, scratch buffers) allocates freely;
 * the handler under test is zero-alloc after construction.
 */
class HaltTradingCommandHandlerTest {

  // -------------------------------------------------------------------------
  // Constants
  // -------------------------------------------------------------------------

  /** Representative cluster timestamp — epoch nanos 2023-11-15T00:00:00Z. */
  private static final long TS = 1_700_000_000_000_000_000L;

  /** Reason payload fits the 64-byte fixed Text field. */
  private static final String REASON = "feed-degraded";

  // -------------------------------------------------------------------------
  // Fixtures
  // -------------------------------------------------------------------------

  private TradingState tradingState;
  private EventSink eventSink;
  private HaltTradingCommandHandler handler;
  private FakeClientSession session;
  private FakeCluster fakeCluster;

  /** Scratch buffer large enough for a full command message (header + 72-byte block). */
  private final UnsafeBuffer cmdBuf = new UnsafeBuffer(new byte[256]);

  /** Decoder for reading emitted event bytes back. */
  private final TradingHaltActivatedEventDecoder eventDecoder =
      new TradingHaltActivatedEventDecoder();

  /** Header decoder used to inspect emitted bytes. */
  private final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();

  @BeforeEach
  void setUp() {
    final var orderBook = new OrderBook(128);
    final var orderIdGen = new IdGenerator("ORD");
    final var execIdGen = new IdGenerator("EXE");
    final var quoteIdGen = new IdGenerator("QTE");
    tradingState = new TradingState(orderBook, orderIdGen, execIdGen, quoteIdGen);

    final var sequencer = new EventSequencer();
    final var journal = new EventJournal(64);
    eventSink = new EventSink(sequencer, journal);

    fakeCluster = new FakeCluster(0L);
    session = new FakeClientSession(42L);
    fakeCluster.addClientSession(session);
    eventSink.setCluster(fakeCluster);

    handler = new HaltTradingCommandHandler(tradingState);
  }

  // -------------------------------------------------------------------------
  // Encode helper
  // -------------------------------------------------------------------------

  /**
   * Encodes a {@code HaltTradingCommand} (template 17) with the given reason string into {@link
   * #cmdBuf} at offset 0 and returns the total encoded length (header + block).
   *
   * @param reason the halt reason string (max 64 ASCII chars); may be empty
   * @return total encoded length
   */
  private int encodeHaltCommand(final String reason) {
    final var hdrEncoder = new MessageHeaderEncoder();
    final var cmdEncoder = new HaltTradingCommandEncoder();
    cmdEncoder.wrapAndApplyHeader(cmdBuf, 0, hdrEncoder);
    cmdEncoder.reason(reason);
    cmdEncoder.transactTime(TS);
    return MessageHeaderEncoder.ENCODED_LENGTH + HaltTradingCommandEncoder.BLOCK_LENGTH;
  }

  /**
   * Dispatches a {@code HaltTradingCommand} with the given reason and cluster timestamp via the
   * handler, using {@link #session} as the originating session.
   *
   * @param reason the halt reason string
   * @param clusterTimestamp the cluster-assigned timestamp in epoch nanos
   */
  private void dispatchHalt(final String reason, final long clusterTimestamp) {
    final int len = encodeHaltCommand(reason);
    handler.onCommand(
        session,
        clusterTimestamp,
        cmdBuf,
        0,
        len,
        HaltTradingCommandDecoder.BLOCK_LENGTH,
        HaltTradingCommandDecoder.SCHEMA_VERSION,
        eventSink);
  }

  /**
   * Dispatches a {@code HaltTradingCommand} using {@link #TS} as the cluster timestamp, with the
   * given reason.
   *
   * @param reason the halt reason string
   */
  private void dispatchHalt(final String reason) {
    dispatchHalt(reason, TS);
  }

  /**
   * Wraps the first captured message bytes in {@link #eventDecoder} and returns it for field
   * assertions.
   *
   * @param msgIndex index into {@link FakeClientSession#messages}
   * @return the decoder positioned at the event body
   */
  private TradingHaltActivatedEventDecoder decodeEvent(final int msgIndex) {
    final var buf = new UnsafeBuffer(session.messages.get(msgIndex));
    return eventDecoder.wrapAndApplyHeader(buf, 0, headerDecoder);
  }

  // =========================================================================
  // Test 1 — first halt: flag set to true, event previouslyHalted == 0
  // =========================================================================

  /**
   * When the cluster is in the default cleared state ({@code tradingHalted=false}), the first
   * {@code HaltTradingCommand} must set the flag to {@code true} and emit a {@code
   * TradingHaltActivatedEvent} with {@code previouslyHalted == 0}.
   */
  @Test
  void firstHalt_setsFlag_andEmitsEventWithPreviouslyHaltedZero() {
    assertFalse(tradingState.isTradingHalted(), "initial state must be not halted");

    dispatchHalt(REASON);

    assertTrue(tradingState.isTradingHalted(), "flag must be set to true after halt command");
    assertEquals(1, session.messages.size(), "exactly one event must be emitted");

    final var evt = decodeEvent(0);
    assertEquals(
        (short) 0, evt.previouslyHalted(), "previouslyHalted must be 0 for a real halt transition");
  }

  // =========================================================================
  // Test 2 — second halt: flag stays true, event previouslyHalted == 1
  // =========================================================================

  /**
   * Issuing a second {@code HaltTradingCommand} while already halted is a no-op for the flag (it
   * stays {@code true}) but must still emit a {@code TradingHaltActivatedEvent} with {@code
   * previouslyHalted == 1}. This matches the LMAX / CME ops-audit convention.
   */
  @Test
  void secondHalt_isNoOpForFlag_butStillEmitsEvent_previouslyHaltedOne() {
    // First halt — real transition.
    dispatchHalt(REASON);
    assertTrue(tradingState.isTradingHalted(), "flag must be true after first halt");
    assertEquals(1, session.messages.size(), "one event after first halt");

    // Second halt — no-op for state, still emits.
    dispatchHalt("re-halt");
    assertTrue(tradingState.isTradingHalted(), "flag must stay true on re-halt");
    assertEquals(2, session.messages.size(), "two events after second halt");

    final var evt = decodeEvent(1);
    assertEquals(
        (short) 1, evt.previouslyHalted(), "previouslyHalted must be 1 for an idempotent re-halt");
  }

  // =========================================================================
  // Test 3 — commandTemplateId: returns 17
  // =========================================================================

  /**
   * {@link HaltTradingCommandHandler#commandTemplateId()} must return the SBE template ID 17
   * ({@code HaltTradingCommand}). The dispatcher relies on this value to route inbound buffers.
   */
  @Test
  void commandTemplateId_returnsExpected() {
    assertEquals(
        HaltTradingCommandDecoder.TEMPLATE_ID,
        handler.commandTemplateId(),
        "HaltTradingCommandHandler must report template ID 17");
  }

  // =========================================================================
  // Test 4 — reason round-trip: "feed-degraded" survives encode → emit → decode
  // =========================================================================

  /**
   * The reason string {@code "feed-degraded"} must survive the full encode → handler → emit →
   * decode pipeline. The 64-byte fixed SBE field is null-padded; the decoder's {@link
   * TradingHaltActivatedEventDecoder#reason()} strips trailing zero bytes, so the round-tripped
   * value must equal the original string.
   */
  @Test
  void eventCarriesReason_roundTrip() {
    dispatchHalt(REASON);

    final var evt = decodeEvent(0);
    assertEquals(
        REASON, evt.reason(), "reason must round-trip through the 64-byte fixed Text field");
  }

  // =========================================================================
  // Test 5 — adminSessionId: propagated from ClientSession.id()
  // =========================================================================

  /**
   * The emitted {@code TradingHaltActivatedEvent} must carry the {@code adminSessionId} equal to
   * the {@link io.aeron.cluster.service.ClientSession#id()} of the originating session. The session
   * was constructed with id=42 in {@link #setUp}.
   */
  @Test
  void eventCarriesAdminSessionId_fromClientSession() {
    dispatchHalt(REASON);

    final var evt = decodeEvent(0);
    assertEquals(
        42L,
        evt.adminSessionId(),
        "adminSessionId must equal the originating ClientSession.id() == 42");
  }

  // =========================================================================
  // Test 6 — null session: adminSessionId == 0 (sentinel)
  // =========================================================================

  /**
   * When a {@code null} session is passed (the test-only path documented in the handler), the
   * emitted event must carry {@code adminSessionId == 0}. Aeron Cluster never assigns session id 0,
   * so 0 is the correct sentinel for "no originating session".
   */
  @Test
  void nullSession_emitsZeroAdminSessionId() {
    final int len = encodeHaltCommand(REASON);
    handler.onCommand(
        null /* null session — test path */,
        TS,
        cmdBuf,
        0,
        len,
        HaltTradingCommandDecoder.BLOCK_LENGTH,
        HaltTradingCommandDecoder.SCHEMA_VERSION,
        eventSink);

    // EventSink broadcasts to all registered sessions; session (id=42) still receives the event.
    assertEquals(1, session.messages.size(), "event must be broadcast even for null session");

    final var evt = decodeEvent(0);
    assertEquals(0L, evt.adminSessionId(), "null session must produce adminSessionId sentinel 0");
  }

  // =========================================================================
  // Test 7 — cluster timestamp propagation into EventSink.emit
  // =========================================================================

  /**
   * The {@code clusterTimestamp} argument passed to {@link HaltTradingCommandHandler#onCommand}
   * must be forwarded verbatim to {@link EventSink#emit}, which stamps it into the event's {@code
   * timestamp} field (body offset 8). This is the only authoritative time source inside the cluster
   * service (no wall-clock).
   */
  @Test
  void emit_passesClusterTimestamp_toEventSink() {
    // Distinct from TS to confirm the value flows through, not a default.
    final long customTs = 8_888_000_000_000_000_000L;

    dispatchHalt(REASON, customTs);

    final var evt = decodeEvent(0);
    assertEquals(
        customTs,
        evt.timestamp(),
        "EventSink must stamp the cluster timestamp into the event timestamp field");
  }
}
