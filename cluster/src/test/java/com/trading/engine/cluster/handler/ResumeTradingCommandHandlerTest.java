package com.trading.engine.cluster.handler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.trading.engine.cluster.IdGenerator;
import com.trading.engine.cluster.OrderBook;
import com.trading.engine.cluster.journal.EventJournal;
import com.trading.engine.cluster.sequencer.EventSequencer;
import com.trading.engine.cluster.state.TradingState;
import com.trading.engine.messages.sbe.MessageHeaderDecoder;
import com.trading.engine.messages.sbe.MessageHeaderEncoder;
import com.trading.engine.messages.sbe.ResumeTradingCommandDecoder;
import com.trading.engine.messages.sbe.ResumeTradingCommandEncoder;
import com.trading.engine.messages.sbe.TradingHaltClearedEventDecoder;
import com.trading.engine.testsupport.aeron.FakeClientSession;
import com.trading.engine.testsupport.aeron.FakeCluster;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link ResumeTradingCommandHandler} (template 18 → event 118).
 *
 * <p>Mirror of {@link HaltTradingCommandHandlerTest}: exercises flag mutation idempotency, the
 * {@code previouslyHalted} audit field, reason round-trip, {@code adminSessionId} propagation,
 * null-session sentinel, and cluster-timestamp propagation through {@link EventSink#emit}.
 *
 * <p>All tests start from the halted state ({@code tradingHalted=true}) so the first resume is a
 * real flag transition rather than an idempotent no-op.
 *
 * <p><b>Threading:</b> single-threaded — matches the cluster duty-cycle invariant.
 *
 * <p><b>Allocation:</b> test infrastructure allocates freely; the handler under test is zero-alloc
 * after construction.
 */
class ResumeTradingCommandHandlerTest {

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
  private ResumeTradingCommandHandler handler;
  private FakeClientSession session;
  private FakeCluster fakeCluster;

  /** Scratch buffer large enough for a full command message (header + 72-byte block). */
  private final UnsafeBuffer cmdBuf = new UnsafeBuffer(new byte[256]);

  /** Decoder for reading emitted event bytes back. */
  private final TradingHaltClearedEventDecoder eventDecoder = new TradingHaltClearedEventDecoder();

  /** Header decoder used to inspect emitted bytes. */
  private final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();

  @BeforeEach
  void setUp() {
    final var orderBook = new OrderBook(128);
    final var orderIdGen = new IdGenerator("ORD");
    final var execIdGen = new IdGenerator("EXE");
    final var quoteIdGen = new IdGenerator("QTE");
    tradingState = new TradingState(orderBook, orderIdGen, execIdGen, quoteIdGen);

    // Start from halted state so the first resume is a real transition (previouslyHalted=1).
    tradingState.setTradingHalted(true);

    final var sequencer = new EventSequencer();
    final var journal = new EventJournal(64);
    eventSink = new EventSink(sequencer, journal);

    fakeCluster = new FakeCluster(0L);
    session = new FakeClientSession(12345L);
    fakeCluster.addClientSession(session);
    eventSink.setCluster(fakeCluster);

    handler = new ResumeTradingCommandHandler(tradingState);
  }

  // -------------------------------------------------------------------------
  // Encode helper
  // -------------------------------------------------------------------------

  /**
   * Encodes a {@code ResumeTradingCommand} (template 18) with the given reason string into {@link
   * #cmdBuf} at offset 0 and returns the total encoded length (header + block).
   *
   * @param reason the resume reason string (max 64 ASCII chars); may be empty
   * @return total encoded length
   */
  private int encodeResumeCommand(final String reason) {
    final var hdrEncoder = new MessageHeaderEncoder();
    final var cmdEncoder = new ResumeTradingCommandEncoder();
    cmdEncoder.wrapAndApplyHeader(cmdBuf, 0, hdrEncoder);
    cmdEncoder.reason(reason);
    cmdEncoder.transactTime(TS);
    return MessageHeaderEncoder.ENCODED_LENGTH + ResumeTradingCommandEncoder.BLOCK_LENGTH;
  }

  /**
   * Dispatches a {@code ResumeTradingCommand} with the given reason and cluster timestamp via the
   * handler, using {@link #session} as the originating session.
   *
   * @param reason the resume reason string
   * @param clusterTimestamp the cluster-assigned timestamp in epoch nanos
   */
  private void dispatchResume(final String reason, final long clusterTimestamp) {
    final int len = encodeResumeCommand(reason);
    handler.onCommand(
        session,
        clusterTimestamp,
        cmdBuf,
        0,
        len,
        ResumeTradingCommandDecoder.BLOCK_LENGTH,
        ResumeTradingCommandDecoder.SCHEMA_VERSION,
        eventSink);
  }

  /**
   * Dispatches a {@code ResumeTradingCommand} using {@link #TS} as the cluster timestamp, with the
   * given reason.
   *
   * @param reason the resume reason string
   */
  private void dispatchResume(final String reason) {
    dispatchResume(reason, TS);
  }

  /**
   * Wraps the captured message bytes at the given index in {@link #eventDecoder} and returns it for
   * field assertions.
   *
   * @param msgIndex index into {@link FakeClientSession#messages}
   * @return the decoder positioned at the event body
   */
  private TradingHaltClearedEventDecoder decodeEvent(final int msgIndex) {
    final var buf = new UnsafeBuffer(session.messages.get(msgIndex));
    return eventDecoder.wrapAndApplyHeader(buf, 0, headerDecoder);
  }

  // =========================================================================
  // Test 1 — first resume: flag cleared, event previouslyHalted == 1
  // =========================================================================

  /**
   * Starting from halted state ({@code tradingHalted=true}, set in {@link #setUp}), the first
   * {@code ResumeTradingCommand} must clear the flag to {@code false} and emit a {@code
   * TradingHaltClearedEvent} with {@code previouslyHalted == 1}.
   */
  @Test
  void firstResume_clearsFlag_andEmitsEventWithPreviouslyHaltedOne() {
    assertTrue(tradingState.isTradingHalted(), "initial state must be halted for this test");

    dispatchResume(REASON);

    assertFalse(tradingState.isTradingHalted(), "flag must be cleared to false after resume");
    assertEquals(1, session.messages.size(), "exactly one event must be emitted");

    final var evt = decodeEvent(0);
    assertEquals(
        (short) 1,
        evt.previouslyHalted(),
        "previouslyHalted must be 1 for a real resume transition");
  }

  // =========================================================================
  // Test 2 — second resume: flag stays false, event previouslyHalted == 0
  // =========================================================================

  /**
   * Issuing a second {@code ResumeTradingCommand} while already cleared is a no-op for the flag (it
   * stays {@code false}) but must still emit a {@code TradingHaltClearedEvent} with {@code
   * previouslyHalted == 0}. This is the idempotency audit convention mirror of {@link
   * HaltTradingCommandHandler}.
   */
  @Test
  void secondResume_isNoOpForFlag_butStillEmitsEvent_previouslyHaltedZero() {
    // First resume — real transition from halted.
    dispatchResume(REASON);
    assertFalse(tradingState.isTradingHalted(), "flag must be false after first resume");
    assertEquals(1, session.messages.size(), "one event after first resume");

    // Second resume — no-op for state, still emits.
    dispatchResume("re-clear");
    assertFalse(tradingState.isTradingHalted(), "flag must stay false on re-resume");
    assertEquals(2, session.messages.size(), "two events after second resume");

    final var evt = decodeEvent(1);
    assertEquals(
        (short) 0,
        evt.previouslyHalted(),
        "previouslyHalted must be 0 for an idempotent re-resume");
  }

  // =========================================================================
  // Test 3 — commandTemplateId: returns 18
  // =========================================================================

  /**
   * {@link ResumeTradingCommandHandler#commandTemplateId()} must return the SBE template ID 18
   * ({@code ResumeTradingCommand}). The dispatcher relies on this value to route inbound buffers.
   */
  @Test
  void commandTemplateId_returnsExpected() {
    assertEquals(
        ResumeTradingCommandDecoder.TEMPLATE_ID,
        handler.commandTemplateId(),
        "ResumeTradingCommandHandler must report template ID 18");
  }

  // =========================================================================
  // Test 4 — reason round-trip: "feed-degraded" survives encode → emit → decode
  // =========================================================================

  /**
   * The reason string {@code "feed-degraded"} must survive the full encode → handler → emit →
   * decode pipeline. The 64-byte fixed SBE field is null-padded; the decoder's {@link
   * TradingHaltClearedEventDecoder#reason()} strips trailing zero bytes, so the round-tripped value
   * must equal the original string.
   */
  @Test
  void eventCarriesReason_roundTrip() {
    dispatchResume(REASON);

    final var evt = decodeEvent(0);
    assertEquals(
        REASON, evt.reason(), "reason must round-trip through the 64-byte fixed Text field");
  }

  // =========================================================================
  // Test 5 — adminSessionId: propagated from ClientSession.id()
  // =========================================================================

  /**
   * The emitted {@code TradingHaltClearedEvent} must carry the {@code adminSessionId} equal to the
   * {@link io.aeron.cluster.service.ClientSession#id()} of the originating session. The session was
   * constructed with id=12345 in {@link #setUp}.
   */
  @Test
  void eventCarriesAdminSessionId_fromClientSession() {
    dispatchResume(REASON);

    final var evt = decodeEvent(0);
    assertEquals(
        12345L,
        evt.adminSessionId(),
        "adminSessionId must equal the originating ClientSession.id() == 12345");
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
    final int len = encodeResumeCommand(REASON);
    handler.onCommand(
        null /* null session — test path */,
        TS,
        cmdBuf,
        0,
        len,
        ResumeTradingCommandDecoder.BLOCK_LENGTH,
        ResumeTradingCommandDecoder.SCHEMA_VERSION,
        eventSink);

    // EventSink broadcasts to all registered sessions; session (id=12345) still receives the event.
    assertEquals(1, session.messages.size(), "event must be broadcast even for null session");

    final var evt = decodeEvent(0);
    assertEquals(0L, evt.adminSessionId(), "null session must produce adminSessionId sentinel 0");
  }

  // =========================================================================
  // Test 7 — cluster timestamp propagation into EventSink.emit
  // =========================================================================

  /**
   * The {@code clusterTimestamp} argument passed to {@link ResumeTradingCommandHandler#onCommand}
   * must be forwarded verbatim to {@link EventSink#emit}, which stamps it into the event's {@code
   * timestamp} field (body offset 8). This is the only authoritative time source inside the cluster
   * service (no wall-clock).
   */
  @Test
  void emit_passesClusterTimestamp_toEventSink() {
    // Distinct from TS to confirm the value flows through, not a default.
    final long customTs = 8_888_000_000_000_000_000L;

    dispatchResume(REASON, customTs);

    final var evt = decodeEvent(0);
    assertEquals(
        customTs,
        evt.timestamp(),
        "EventSink must stamp the cluster timestamp into the event timestamp field");
  }
}
