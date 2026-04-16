package com.trading.engine.orchestrator;

import io.aeron.Publication;
import java.util.ArrayList;
import java.util.List;
import org.agrona.DirectBuffer;
import org.agrona.collections.LongArrayList;

/**
 * Test fake for the {@link Publisher} SAM that records every {@code publish()} call. Defensively
 * copies the offered byte slice so the system-under-test can reuse its encoding buffer between
 * calls without corrupting the test's captured snapshot.
 *
 * <p><b>Allocation:</b> not zero-alloc — uses {@code ArrayList} + per-call {@code byte[]} copies.
 * Acceptable for test-only usage; do NOT exercise this fake from {@code
 * OrchestratorNoAllocationTest} (use {@code (buf, off, len) -> 1L} no-op there instead).
 *
 * <p><b>Scope:</b> {@code orchestrator/src/test/java} only — promote to {@code test-support} module
 * if a second consumer (e.g., APP-33 integration tests) appears.
 *
 * <p><b>Threading:</b> not thread-safe — single-threaded test usage only.
 */
public final class RecordingPublisher implements Publisher {

  private final List<byte[]> capturedBuffers = new ArrayList<>();
  private final LongArrayList capturedReturnValues = new LongArrayList();
  private long nextReturn = 1L;

  /** Override the value returned by every subsequent {@link #publish} call. */
  public void setReturnValue(final long value) {
    this.nextReturn = value;
  }

  /** Number of {@link #publish} calls received since construction. */
  public int callCount() {
    return capturedBuffers.size();
  }

  /**
   * Defensive copy of the bytes from {@code publish} call {@code i} (length-truncated to the length
   * argument passed at the call site).
   */
  public byte[] capturedBufferBytes(final int callIndex) {
    return capturedBuffers.get(callIndex);
  }

  /** The value returned by {@link #publish} call {@code i}. */
  public long capturedReturnValue(final int callIndex) {
    return capturedReturnValues.getLong(callIndex);
  }

  /**
   * Records the offered slice and returns the configured value.
   *
   * @return whatever {@link #setReturnValue} was last called with; defaults to {@code 1L} (Aeron
   *     convention for "publication position written" / non-error). Test cases that exercise {@link
   *     OrchestratorService#offerWithRetry retry/terminal logic} should call {@link
   *     #setReturnValue} with {@link Publication#BACK_PRESSURED} or {@link
   *     Publication#NOT_CONNECTED}.
   */
  @Override
  public long publish(final DirectBuffer buffer, final int offset, final int length) {
    final var copy = new byte[length];
    buffer.getBytes(offset, copy, 0, length);
    capturedBuffers.add(copy);
    capturedReturnValues.addLong(nextReturn);
    return nextReturn;
  }
}
