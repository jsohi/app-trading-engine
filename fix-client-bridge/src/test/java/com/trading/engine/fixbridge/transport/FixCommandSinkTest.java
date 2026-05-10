package com.trading.engine.fixbridge.transport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

import com.trading.engine.fixbridge.json.MutableParsedMessage;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link FixCommandSink} — the per-session FIX wire-send SAM seam.
 *
 * <p>Verifies:
 *
 * <ul>
 *   <li>{@link FixCommandSink#NOOP} returns {@link FixCommandSink#NO_SEND} for every {@code send*}
 *       method.
 *   <li>{@link FixCommandSink.Factory#NOOP} always returns {@link FixCommandSink#NOOP}.
 *   <li>A custom recording impl receives the right {@code (parsed, nowNs)} args and returns the
 *       configured return value.
 * </ul>
 *
 * <p><b>Threading.</b> Single-threaded — test-only.
 *
 * <p><b>Allocation.</b> Test-only — allocation is acceptable.
 */
final class FixCommandSinkTest {

  // ---------------------------------------------------------------------------
  // Recording implementation used by the custom-impl tests.
  // ---------------------------------------------------------------------------

  /** Recording {@link FixCommandSink} that captures the last call made to each method. */
  private static final class RecordingSink implements FixCommandSink {

    MutableParsedMessage lastParsed;
    long lastNowNs;
    String lastMethod;
    long returnValue;

    RecordingSink(final long returnValue) {
      this.returnValue = returnValue;
    }

    @Override
    public long sendQuoteRequest(final MutableParsedMessage parsed, final long nowNs) {
      record("sendQuoteRequest", parsed, nowNs);
      return returnValue;
    }

    @Override
    public long sendAcceptQuote(final MutableParsedMessage parsed, final long nowNs) {
      record("sendAcceptQuote", parsed, nowNs);
      return returnValue;
    }

    @Override
    public long handleRejectQuote(final MutableParsedMessage parsed, final long nowNs) {
      record("handleRejectQuote", parsed, nowNs);
      return returnValue;
    }

    @Override
    public long sendNewOrderSingle(final MutableParsedMessage parsed, final long nowNs) {
      record("sendNewOrderSingle", parsed, nowNs);
      return returnValue;
    }

    @Override
    public long sendCancelOrder(final MutableParsedMessage parsed, final long nowNs) {
      record("sendCancelOrder", parsed, nowNs);
      return returnValue;
    }

    @Override
    public long sendOrderStatusRequest(final MutableParsedMessage parsed, final long nowNs) {
      record("sendOrderStatusRequest", parsed, nowNs);
      return returnValue;
    }

    private void record(final String method, final MutableParsedMessage p, final long ns) {
      this.lastMethod = method;
      this.lastParsed = p;
      this.lastNowNs = ns;
    }
  }

  // ---------------------------------------------------------------------------
  // NOOP constant.
  // ---------------------------------------------------------------------------

  @Test
  void noop_sendQuoteRequest_returnsNoSend() {
    final var parsed = new MutableParsedMessage();
    final long result = FixCommandSink.NOOP.sendQuoteRequest(parsed, 123L);
    assertEquals(FixCommandSink.NO_SEND, result);
  }

  @Test
  void noop_sendAcceptQuote_returnsNoSend() {
    final var parsed = new MutableParsedMessage();
    final long result = FixCommandSink.NOOP.sendAcceptQuote(parsed, 456L);
    assertEquals(FixCommandSink.NO_SEND, result);
  }

  @Test
  void noop_handleRejectQuote_returnsNoSend() {
    final var parsed = new MutableParsedMessage();
    final long result = FixCommandSink.NOOP.handleRejectQuote(parsed, 789L);
    assertEquals(FixCommandSink.NO_SEND, result);
  }

  @Test
  void noop_sendNewOrderSingle_returnsNoSend() {
    final var parsed = new MutableParsedMessage();
    final long result = FixCommandSink.NOOP.sendNewOrderSingle(parsed, 1L);
    assertEquals(FixCommandSink.NO_SEND, result);
  }

  @Test
  void noop_sendCancelOrder_returnsNoSend() {
    final var parsed = new MutableParsedMessage();
    final long result = FixCommandSink.NOOP.sendCancelOrder(parsed, 2L);
    assertEquals(FixCommandSink.NO_SEND, result);
  }

  @Test
  void noop_sendOrderStatusRequest_returnsNoSend() {
    final var parsed = new MutableParsedMessage();
    final long result = FixCommandSink.NOOP.sendOrderStatusRequest(parsed, 3L);
    assertEquals(FixCommandSink.NO_SEND, result);
  }

  @Test
  void noSend_sentinelIsMinusOne() {
    assertEquals(-1L, FixCommandSink.NO_SEND);
  }

  // ---------------------------------------------------------------------------
  // Factory.NOOP.
  // ---------------------------------------------------------------------------

  @Test
  void factoryNoop_create_returnsNoop() {
    // create() needs a BridgeSession — pass null because Factory.NOOP ignores its argument.
    final var sink = FixCommandSink.Factory.NOOP.create(null);
    assertSame(FixCommandSink.NOOP, sink);
  }

  // ---------------------------------------------------------------------------
  // Custom recording impl.
  // ---------------------------------------------------------------------------

  @Test
  void customImpl_sendQuoteRequest_receivesCorrectArgsAndReturnValue() {
    final long expectedReturn = 42L;
    final long nowNs = 999_000_000L;
    final var parsed = new MutableParsedMessage();
    parsed.type = MutableParsedMessage.TYPE_QUOTE_REQUEST;

    final var sink = new RecordingSink(expectedReturn);
    final long result = sink.sendQuoteRequest(parsed, nowNs);

    assertEquals(expectedReturn, result);
    assertEquals("sendQuoteRequest", sink.lastMethod);
    assertSame(parsed, sink.lastParsed);
    assertEquals(nowNs, sink.lastNowNs);
  }

  @Test
  void customImpl_sendAcceptQuote_receivesCorrectArgsAndReturnValue() {
    final long expectedReturn = 77L;
    final long nowNs = 1_000_000L;
    final var parsed = new MutableParsedMessage();
    parsed.type = MutableParsedMessage.TYPE_ACCEPT_QUOTE;

    final var sink = new RecordingSink(expectedReturn);
    final long result = sink.sendAcceptQuote(parsed, nowNs);

    assertEquals(expectedReturn, result);
    assertEquals("sendAcceptQuote", sink.lastMethod);
    assertSame(parsed, sink.lastParsed);
    assertEquals(nowNs, sink.lastNowNs);
  }

  @Test
  void customImpl_handleRejectQuote_receivesCorrectArgsAndReturnValue() {
    final long expectedReturn = FixCommandSink.NO_SEND;
    final long nowNs = 2_000_000L;
    final var parsed = new MutableParsedMessage();
    parsed.type = MutableParsedMessage.TYPE_REJECT_QUOTE;

    final var sink = new RecordingSink(expectedReturn);
    final long result = sink.handleRejectQuote(parsed, nowNs);

    assertEquals(expectedReturn, result);
    assertEquals("handleRejectQuote", sink.lastMethod);
    assertSame(parsed, sink.lastParsed);
    assertEquals(nowNs, sink.lastNowNs);
  }

  @Test
  void customImpl_sendNewOrderSingle_receivesCorrectArgsAndReturnValue() {
    final long expectedReturn = 100L;
    final long nowNs = 3_000_000L;
    final var parsed = new MutableParsedMessage();
    parsed.type = MutableParsedMessage.TYPE_NEW_ORDER_SINGLE;

    final var sink = new RecordingSink(expectedReturn);
    final long result = sink.sendNewOrderSingle(parsed, nowNs);

    assertEquals(expectedReturn, result);
    assertEquals("sendNewOrderSingle", sink.lastMethod);
    assertSame(parsed, sink.lastParsed);
    assertEquals(nowNs, sink.lastNowNs);
  }

  @Test
  void customImpl_sendCancelOrder_receivesCorrectArgsAndReturnValue() {
    final long expectedReturn = 55L;
    final long nowNs = 4_000_000L;
    final var parsed = new MutableParsedMessage();
    parsed.type = MutableParsedMessage.TYPE_CANCEL_ORDER;

    final var sink = new RecordingSink(expectedReturn);
    final long result = sink.sendCancelOrder(parsed, nowNs);

    assertEquals(expectedReturn, result);
    assertEquals("sendCancelOrder", sink.lastMethod);
    assertSame(parsed, sink.lastParsed);
    assertEquals(nowNs, sink.lastNowNs);
  }

  @Test
  void customImpl_sendOrderStatusRequest_receivesCorrectArgsAndReturnValue() {
    final long expectedReturn = FixCommandSink.NO_SEND;
    final long nowNs = 5_000_000L;
    final var parsed = new MutableParsedMessage();
    parsed.type = MutableParsedMessage.TYPE_ORDER_STATUS_REQUEST;

    final var sink = new RecordingSink(expectedReturn);
    final long result = sink.sendOrderStatusRequest(parsed, nowNs);

    assertEquals(expectedReturn, result);
    assertEquals("sendOrderStatusRequest", sink.lastMethod);
    assertSame(parsed, sink.lastParsed);
    assertEquals(nowNs, sink.lastNowNs);
  }
}
