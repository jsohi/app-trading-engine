package com.trading.engine.testsupport.aeron;

import io.aeron.DirectBufferVector;
import io.aeron.Publication;
import io.aeron.cluster.service.ClientSession;
import io.aeron.logbuffer.BufferClaim;
import java.util.ArrayList;
import java.util.List;
import org.agrona.DirectBuffer;

/**
 * Test double for Aeron {@link ClientSession} that captures all offered messages as defensive
 * byte-array copies.
 *
 * <p>Supports configurable back-pressure simulation via {@link #pendingBackpressures} and {@link
 * #alwaysBackpressured}.
 *
 * <p>Not thread-safe — intended for single-threaded cluster service tests.
 *
 * <p>Allocates a new {@code byte[]} copy per {@link #offer} call (defensive copy).
 *
 * <p><b>Field visibility:</b> All tracking fields are {@code public} to preserve the existing
 * direct-field-access pattern from test call sites. This is deliberate for test-only code — no
 * getters needed.
 *
 * @see FakeCluster
 */
public final class FakeClientSession implements ClientSession {

  /** Captured messages — each entry is a defensive byte[] copy of an offer() call. */
  public final List<byte[]> messages = new ArrayList<>();

  /** Number of remaining back-pressure responses before normal flow resumes. */
  public int pendingBackpressures;

  /** When true, all offer() calls return {@link Publication#BACK_PRESSURED} unconditionally. */
  public boolean alwaysBackpressured;

  /** Set to true when {@link #close()} is called. */
  public boolean closed;

  private final long sessionId;

  /**
   * Creates a fake session with the specified ID.
   *
   * @param sessionId the session ID returned by {@link #id()}
   */
  public FakeClientSession(final long sessionId) {
    this.sessionId = sessionId;
  }

  /** Creates a fake session with the default ID (42). */
  public FakeClientSession() {
    this(42L);
  }

  /** {@inheritDoc} */
  @Override
  public long id() {
    return sessionId;
  }

  /** {@inheritDoc} */
  @Override
  public int responseStreamId() {
    return 0;
  }

  /** {@inheritDoc} */
  @Override
  public String responseChannel() {
    return "aeron:ipc";
  }

  /** {@inheritDoc} */
  @Override
  public byte[] encodedPrincipal() {
    return new byte[0];
  }

  /** {@inheritDoc} */
  @Override
  public void close() {
    closed = true;
  }

  /** {@inheritDoc} */
  @Override
  public boolean isClosing() {
    return closed;
  }

  /**
   * Records the offered message as a defensive byte-array copy, or returns {@link
   * Publication#BACK_PRESSURED} if back-pressure is configured.
   *
   * @param buffer the buffer containing the message
   * @param offset the offset within the buffer
   * @param length the length of the message
   * @return 1L on success, or {@link Publication#BACK_PRESSURED} when simulating back-pressure
   */
  @Override
  public long offer(final DirectBuffer buffer, final int offset, final int length) {
    if (alwaysBackpressured) {
      return Publication.BACK_PRESSURED;
    }
    if (pendingBackpressures > 0) {
      pendingBackpressures--;
      return Publication.BACK_PRESSURED;
    }
    final byte[] copy = new byte[length];
    buffer.getBytes(offset, copy);
    messages.add(copy);
    return 1L;
  }

  /**
   * Delegates to {@link #offer(DirectBuffer, int, int)} using the first vector.
   *
   * @param vectors the buffer vectors to offer
   * @return result of offering the first vector
   */
  @Override
  public long offer(final DirectBufferVector[] vectors) {
    return offer(vectors[0].buffer(), vectors[0].offset(), vectors[0].length());
  }

  /**
   * Not supported — always throws {@link UnsupportedOperationException}.
   *
   * @param length claim length
   * @param bufferClaim claim to populate
   * @return never returns
   * @throws UnsupportedOperationException always
   */
  @Override
  public long tryClaim(final int length, final BufferClaim bufferClaim) {
    throw new UnsupportedOperationException();
  }
}
