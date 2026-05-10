package com.trading.engine.fixbridge.transport;

import uk.co.real_logic.artio.builder.Encoder;

/**
 * SAM seam that abstracts the Artio {@code Session#trySend(Encoder)} call. One instance per
 * authenticated browser session; bound at the launcher boundary by a method-reference to the Artio
 * {@code Session} allocated during FIX logon.
 *
 * <p><b>Why this indirection?</b> Artio's {@code Session} is a {@code final} class inside a
 * dependency-heavy JAR. Importing it directly into {@link ArtioFixCommandSink} would drag Artio
 * into every unit test that exercises the sink. A one-method SAM keeps {@code ArtioFixCommandSink}
 * and its tests entirely Artio-free; the launcher supplies {@code session::trySend} as the real
 * impl.
 *
 * <p><b>Threading.</b> Implementations must be called only from the per-session Netty event loop.
 * The Artio Session is not thread-safe; the SAM contract does not add thread-safety.
 *
 * <p><b>Allocation.</b> The method-reference binding at the launcher boundary creates one object.
 * The {@code trySend} call itself is zero-allocation on the hot path (Artio serialises directly to
 * its owned buffer and returns a position long).
 *
 * <p><b>Return value.</b> A non-negative long is the Artio send-position (byte offset into the
 * underlying {@code Publication}). The {@link ArtioFixCommandSink} propagates this value back to
 * the dispatcher verbatim; the dispatcher may inspect it for backpressure detection. The constant
 * {@link FixCommandSink#NO_SEND} ({@code -1L}) is returned by {@link #NOOP} and signals "no wire
 * activity occurred".
 *
 * @see ArtioFixCommandSink
 * @see FixCommandSink
 */
@FunctionalInterface
public interface FixSessionAdapter {

  /**
   * Invoke Artio's {@code Session#trySend} for the supplied encoder. The encoder must already be
   * fully populated — this method merely delegates to the underlying Artio call.
   *
   * @param encoder fully-populated Artio FIX 4.4 encoder; never {@code null}
   * @return Artio send-position ({@code >= 0}) on success, or {@link FixCommandSink#NO_SEND} when
   *     the FIX session is down or experiencing backpressure
   */
  long trySend(Encoder encoder);

  /**
   * No-op adapter that returns {@link FixCommandSink#NO_SEND} for every call. Used as the default
   * until the launcher wires the real Artio session, and in unit tests that exercise the {@link
   * ArtioFixCommandSink} logic without an Artio Session.
   */
  FixSessionAdapter NOOP = encoder -> FixCommandSink.NO_SEND;
}
