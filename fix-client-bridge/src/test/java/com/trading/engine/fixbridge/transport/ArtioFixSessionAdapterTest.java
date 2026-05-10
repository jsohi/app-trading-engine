package com.trading.engine.fixbridge.transport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.trading.engine.fix.builder.NewOrderSingleEncoder;
import org.junit.jupiter.api.Test;
import uk.co.real_logic.artio.builder.Encoder;

/**
 * Unit tests for {@link ArtioFixSessionAdapter}.
 *
 * <p>Verifies the adapter:
 *
 * <ul>
 *   <li>Forwards every {@link FixSessionAdapter#trySend(Encoder)} call to the bound Artio-side
 *       delegate, propagating both the encoder reference and the return value verbatim.
 *   <li>Normalises negative return values to {@link FixCommandSink#NO_SEND} so the dispatcher sees
 *       a single canonical sentinel for "no wire activity occurred".
 *   <li>Rejects null constructor arguments with {@link NullPointerException}.
 * </ul>
 *
 * <p>The Artio {@code Session} class is final-ish with a package-private constructor, so the tests
 * use the package-private {@link ArtioFixSessionAdapter.TrySendFn} test seam rather than mocking
 * {@code Session} directly. Production wiring is unaffected — both constructors funnel through the
 * same {@link ArtioFixSessionAdapter#trySend(Encoder)} normalisation path.
 *
 * <p><b>Threading.</b> Single-threaded — test-only.
 *
 * <p><b>Allocation.</b> Test-only — allocation is acceptable.
 */
final class ArtioFixSessionAdapterTest {

  /**
   * Hand-rolled {@link ArtioFixSessionAdapter.TrySendFn} that records the encoder it received and
   * returns a configurable position — sufficient to verify the adapter's call-propagation contract
   * without touching Artio's runtime.
   */
  private static final class CapturingTrySend implements ArtioFixSessionAdapter.TrySendFn {

    Encoder lastEncoder;
    int callCount;
    long fixedPosition;

    CapturingTrySend(final long fixedPosition) {
      this.fixedPosition = fixedPosition;
    }

    @Override
    public long trySend(final Encoder encoder) {
      this.lastEncoder = encoder;
      this.callCount++;
      return fixedPosition;
    }
  }

  // ---------------------------------------------------------------------------
  // Happy path — delegate return value is non-negative.
  // ---------------------------------------------------------------------------

  @Test
  void trySend_delegateReturnsPosition_propagatedVerbatim() {
    final var capture = new CapturingTrySend(123L);
    final var adapter = new ArtioFixSessionAdapter(capture);
    final var encoder = new NewOrderSingleEncoder();

    final long position = adapter.trySend(encoder);

    assertEquals(123L, position, "non-negative position must be forwarded unchanged");
    assertEquals(1, capture.callCount, "delegate must be invoked exactly once");
    assertSame(encoder, capture.lastEncoder, "delegate must receive the same encoder reference");
  }

  @Test
  void trySend_delegateReturnsZero_propagatedAsZero() {
    // Zero is a valid Artio send-position (start of the first publication slot) and must be
    // treated as success — NOT collapsed to NO_SEND.
    final var capture = new CapturingTrySend(0L);
    final var adapter = new ArtioFixSessionAdapter(capture);

    final long position = adapter.trySend(new NewOrderSingleEncoder());

    assertEquals(0L, position, "zero is a valid send-position and must NOT become NO_SEND");
  }

  // ---------------------------------------------------------------------------
  // Backpressure / session-down — every negative return collapses to NO_SEND.
  // ---------------------------------------------------------------------------

  @Test
  void trySend_delegateReturnsNegativeOne_normalisedToNoSend() {
    final var capture = new CapturingTrySend(-1L);
    final var adapter = new ArtioFixSessionAdapter(capture);

    final long position = adapter.trySend(new NewOrderSingleEncoder());

    assertEquals(FixCommandSink.NO_SEND, position, "-1 must normalise to NO_SEND");
  }

  @Test
  void trySend_delegateReturnsArbitraryNegative_normalisedToNoSend() {
    // Artio uses several distinct negative codes (BACK_PRESSURED, ADMIN_ACTION, NOT_CONNECTED,
    // CLOSED, MAX_POSITION_EXCEEDED, ...). The dispatcher only cares "did wire activity happen?"
    // so the adapter collapses every negative value to a single canonical sentinel.
    final var capture = new CapturingTrySend(-12345L);
    final var adapter = new ArtioFixSessionAdapter(capture);

    final long position = adapter.trySend(new NewOrderSingleEncoder());

    assertEquals(FixCommandSink.NO_SEND, position, "any negative value must normalise to NO_SEND");
  }

  // ---------------------------------------------------------------------------
  // Multi-call propagation — the adapter holds no per-call state.
  // ---------------------------------------------------------------------------

  @Test
  void trySend_multipleCalls_eachIndependentlyForwarded() {
    final var capture = new CapturingTrySend(50L);
    final var adapter = new ArtioFixSessionAdapter(capture);
    final var first = new NewOrderSingleEncoder();
    final var second = new NewOrderSingleEncoder();

    final long pos1 = adapter.trySend(first);
    capture.fixedPosition = 60L;
    final long pos2 = adapter.trySend(second);

    assertEquals(50L, pos1, "first call returns first delegate value");
    assertEquals(60L, pos2, "second call returns updated delegate value");
    assertEquals(2, capture.callCount, "delegate invoked exactly twice");
    assertSame(second, capture.lastEncoder, "delegate captures most-recent encoder");
  }

  // ---------------------------------------------------------------------------
  // Null-argument validation.
  // ---------------------------------------------------------------------------

  @Test
  void constructor_nullSession_throwsNpe() {
    final var npe =
        assertThrows(
            NullPointerException.class,
            () -> new ArtioFixSessionAdapter((uk.co.real_logic.artio.session.Session) null),
            "null Artio session must be rejected");
    assertNotNull(npe.getMessage(), "NPE must carry a diagnostic message");
  }

  @Test
  void constructor_nullDelegate_throwsNpe() {
    final var npe =
        assertThrows(
            NullPointerException.class,
            () -> new ArtioFixSessionAdapter((ArtioFixSessionAdapter.TrySendFn) null),
            "null TrySendFn must be rejected");
    assertNotNull(npe.getMessage(), "NPE must carry a diagnostic message");
  }
}
