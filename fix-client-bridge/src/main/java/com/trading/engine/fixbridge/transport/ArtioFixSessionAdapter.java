package com.trading.engine.fixbridge.transport;

import uk.co.real_logic.artio.builder.Encoder;
import uk.co.real_logic.artio.session.Session;

/**
 * Production {@link FixSessionAdapter} impl that wraps an Artio {@link Session} and forwards every
 * {@link #trySend(Encoder)} call to {@link Session#trySend(Encoder)}.
 *
 * <p><b>Purpose.</b> Replaces {@link FixSessionAdapter#NOOP} for production. One instance per
 * authenticated browser session — the bridge runs as a FIX 4.4 <em>initiator</em> to the gateway
 * (the FIX acceptor), and each browser session gets its own initiator-side Artio {@link Session}
 * minted at JWT-auth-success time by the launcher (see {@code BridgeNettyBootstrap}).
 *
 * <p><b>Why a class wrapper rather than a bare {@code session::trySend} method reference?</b>
 * Production wiring is free to bind {@code session::trySend} directly via method reference at the
 * launcher boundary — that is the cheapest possible binding (one capturing-lambda allocation at
 * session creation, zero per-call allocation thereafter) and is the recommended path for code that
 * needs nothing beyond the {@code trySend} call. This wrapper is offered as the typed alternative
 * for code paths that benefit from an explicit class — diagnostics, lifecycle hooks, or future
 * extension points (e.g. wrapping {@code trySend} with metrics/logging without changing the SAM
 * contract). Both paths are equally valid; the launcher chooses per its needs.
 *
 * <p><b>Testability.</b> Artio's {@link Session} is a {@code final}-ish class with a
 * package-private constructor, so it cannot be subclassed or instantiated from test code without
 * pulling in the full Artio runtime (FixEngine, FixLibrary, MediaDriver, ...). To keep {@code
 * ArtioFixSessionAdapter} unit-testable in isolation, the class accepts a tiny package- private
 * {@link TrySendFn} SAM in addition to its public {@link Session}-taking constructor. Tests inject
 * a hand-rolled {@code TrySendFn} double and assert call propagation; production wires the concrete
 * {@link Session#trySend(Encoder)} method reference. Both paths funnel through {@link
 * #trySend(Encoder)} so the production code path is exercised by every test.
 *
 * <p><b>Threading.</b> Not thread-safe. Artio's {@link Session} methods must be invoked from a
 * single thread — typically the {@code FixLibrary}'s polling thread, or, in this bridge's
 * deployment, the per-session Netty channel event loop that owns the FIX session. The bridge
 * dispatcher invariant pins one Artio {@link Session} to one Netty channel, so the single-thread
 * invariant is preserved by construction.
 *
 * <p><b>Allocation.</b> Zero on the hot path. Construction allocates one wrapper instance plus
 * either the captured {@link Session} reference or the {@link TrySendFn} method-reference object;
 * subsequent {@link #trySend(Encoder)} calls perform a single virtual dispatch and propagate the
 * Artio send-position long verbatim with no heap allocation. (Artio's {@code trySend} serialises
 * the encoder directly into its owned outbound publication buffer.)
 *
 * <p><b>Return value.</b> A non-negative long is the Artio send-position (byte offset into the
 * underlying {@code Publication}); {@link FixCommandSink#NO_SEND} ({@code -1L}) is returned when
 * Artio reports backpressure or session-down. The dispatcher inspects this value for backpressure
 * detection.
 *
 * @see FixSessionAdapter
 * @see ArtioFixCommandSink
 */
public final class ArtioFixSessionAdapter implements FixSessionAdapter {

  /**
   * One-method seam over {@link Session#trySend(Encoder)}. Production binds {@link
   * Session#trySend(Encoder)} directly; tests bind a hand-rolled lambda. Package-private so it
   * cannot be confused with the public {@link FixSessionAdapter} contract — this seam exists
   * <em>solely</em> to test-isolate the wrapper from Artio's final {@code Session} class.
   */
  @FunctionalInterface
  interface TrySendFn {

    /**
     * Forward {@code encoder} to the underlying Artio {@code Session#trySend} call.
     *
     * @param encoder fully populated Artio encoder; never {@code null}
     * @return Artio send-position ({@code >= 0}) on success; {@code <0} on backpressure /
     *     session-down. Negative values are normalised to {@link FixCommandSink#NO_SEND} by {@link
     *     ArtioFixSessionAdapter#trySend(Encoder)}.
     */
    long trySend(Encoder encoder);
  }

  /** Method-reference (or test-double) bound at construction. Never null. */
  private final TrySendFn delegate;

  /**
   * Production constructor — wraps a real Artio {@link Session}. Binds {@code session::trySend} as
   * the delegate so every call to {@link #trySend(Encoder)} forwards directly to the Artio
   * implementation.
   *
   * @param artioSession the per-session Artio {@link Session} owned by this browser session; must
   *     be non-null. Lifecycle is managed by the launcher — the {@link Session} is created at
   *     auth-success and torn down on channel close.
   * @throws NullPointerException if {@code artioSession} is null
   */
  public ArtioFixSessionAdapter(final Session artioSession) {
    if (artioSession == null) {
      throw new NullPointerException("artioSession must not be null");
    }
    // Method reference allocates one capturing object at construction — zero per-call alloc
    // thereafter. Equivalent to `(e) -> artioSession.trySend(e)` but with one fewer indirection
    // and a stable identity that the JIT can devirtualise.
    this.delegate = artioSession::trySend;
  }

  /**
   * Test-only constructor — accepts a hand-rolled {@link TrySendFn} so unit tests can verify the
   * adapter's call-propagation contract without an Artio runtime. Package-private to prevent
   * production callers from bypassing the {@link Session}-based wiring.
   *
   * @param delegate hand-rolled SAM double; must be non-null
   * @throws NullPointerException if {@code delegate} is null
   */
  ArtioFixSessionAdapter(final TrySendFn delegate) {
    if (delegate == null) {
      throw new NullPointerException("delegate must not be null");
    }
    this.delegate = delegate;
  }

  /**
   * {@inheritDoc}
   *
   * <p>Forwards the encoder to the bound delegate ({@code Session::trySend} in production). Any
   * negative return value is normalised to {@link FixCommandSink#NO_SEND} so the dispatcher
   * receives a single canonical sentinel for "no wire activity occurred" — Artio itself can return
   * any negative value (back-pressure positions, claim-failure codes, etc.) and the dispatcher
   * should not need to know which.
   *
   * @return {@code >= 0}: Artio send-position on success; {@link FixCommandSink#NO_SEND}: on
   *     backpressure or session-down
   * @throws NullPointerException if {@code encoder} is null (delegated from Artio)
   */
  @Override
  public long trySend(final Encoder encoder) {
    final long position = delegate.trySend(encoder);
    return position < 0L ? FixCommandSink.NO_SEND : position;
  }
}
