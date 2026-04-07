package com.trading.engine.cluster.journal;

import org.agrona.DirectBuffer;

/**
 * Callback interface for consumers walking the {@link EventJournal}. Called once per event during
 * {@link EventJournal#replayFrom(long, EventReplayHandler)} in ascending sequence-number order.
 *
 * <p>Implementations are allocated once at startup (typically one per projection / consumer) and
 * reused across every replay pass. The {@code buffer} argument is the journal's internal {@link
 * org.agrona.ExpandableArrayBuffer} storing the event payload; the handler MUST treat the bytes as
 * read-only and MUST NOT retain a reference past the call — the underlying slot may be overwritten
 * by a subsequent {@link EventJournal#append} on the next cluster duty-cycle tick. Copy any bytes
 * the handler needs to keep.
 *
 * <p>Single-threaded by contract: replay runs on the cluster duty cycle thread. The handler must
 * not block — it's called inside the cluster's log-replay path.
 */
@FunctionalInterface
public interface EventReplayHandler {

  /**
   * Receive one event from the journal.
   *
   * @param seqNo the event's sequence number (always {@code >= 1})
   * @param eventType raw {@code EventTypeEnum} wire value from the SBE schema (uint16, e.g. 100 for
   *     OrderCreated); consumers can match against {@code
   *     com.trading.engine.messages.sbe.EventTypeEnum} constants if they need typed access
   * @param buffer the journal's internal payload buffer for this entry — read-only, do not retain
   * @param offset start offset of the payload bytes inside {@code buffer}
   * @param length number of payload bytes starting at {@code offset}
   */
  void onEvent(long seqNo, int eventType, DirectBuffer buffer, int offset, int length);
}
