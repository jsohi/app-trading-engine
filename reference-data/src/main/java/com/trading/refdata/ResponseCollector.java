package com.trading.refdata;

import java.util.ArrayList;
import java.util.List;

/**
 * Collects cluster ack/reject responses for a batch of reference data commands. The launcher wires
 * the cluster egress listener to call {@link #onLoaded()} or {@link #onRejected(String)} for each
 * response.
 *
 * <p>Not thread-safe — must be called from the same thread as the orchestrator poll loop.
 */
public final class ResponseCollector {

  private int expectedCount = -1;
  private int loadedCount;
  private int rejectedCount;
  private final List<String> rejectionReasons = new ArrayList<>();

  /** Set the number of ack responses expected for the current batch. */
  public void expectResponses(final int count) {
    if (count < 0) {
      throw new IllegalArgumentException("expected count must be >= 0, got " + count);
    }
    expectedCount = count;
    loadedCount = 0;
    rejectedCount = 0;
    rejectionReasons.clear();
  }

  /** Called by the egress listener when a record was successfully loaded. */
  public void onLoaded() {
    loadedCount++;
  }

  /** Called by the egress listener when a record was rejected. */
  public void onRejected(final String reason) {
    rejectedCount++;
    rejectionReasons.add(reason);
  }

  /** True when all expected responses have been received. Returns false before initialization. */
  public boolean isComplete() {
    return expectedCount >= 0 && (loadedCount + rejectedCount) >= expectedCount;
  }

  /** True if any record was rejected. */
  public boolean hasRejections() {
    return rejectedCount > 0;
  }

  public int expectedCount() {
    return expectedCount;
  }

  public int loadedCount() {
    return loadedCount;
  }

  public int rejectedCount() {
    return rejectedCount;
  }

  public List<String> rejectionReasons() {
    return List.copyOf(rejectionReasons);
  }
}
