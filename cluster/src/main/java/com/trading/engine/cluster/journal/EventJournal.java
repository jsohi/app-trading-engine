package com.trading.engine.cluster.journal;

import java.nio.ByteOrder;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Long2ObjectHashMap;

/**
 * Bounded in-memory ring buffer of recent domain events, indexed by sequence number. The
 * cluster-side counterpart of {@link com.trading.engine.cluster.sequencer.EventSequencer}: one
 * utility assigns the sequence numbers, the other stores the events they name. Used by late-joining
 * projections to catch up via {@link #replayFrom} and by Aeron Cluster's snapshot-then-replay
 * recovery model via {@link #snapshotTo} / {@link #restoreFrom}.
 *
 * <p>Semantics:
 *
 * <ul>
 *   <li>Fixed capacity chosen at construction; default 65,536 (2^16) entries
 *   <li>Append-only: new events overwrite the oldest slot when the ring fills
 *   <li>O(1) lookup by sequence number via an Agrona {@link Long2ObjectHashMap} index
 *   <li>Single-threaded — cluster duty cycle only, no synchronisation, no volatile
 *   <li>Deterministic — no wall-clock, no randomness; replay of the same {@code append} sequence
 *       produces an identical journal
 *   <li>Zero allocation after construction — all slots, payload buffers, and the index are
 *       pre-allocated; {@code append} / {@code replayFrom} / {@code get} do not allocate
 * </ul>
 *
 * <p><b>Capacity must be a power of two.</b> The ring uses bitwise-AND indexing ({@code writeIdx &
 * capacityMask}) rather than modulo, which is the LMAX Disruptor / Aeron house idiom. The
 * constructor rejects non-power-of-two and non-positive values with {@link
 * IllegalArgumentException}.
 *
 * <p>Snapshot state survives failover via {@link #snapshotTo} / {@link #restoreFrom}. The
 * serialized form is a flat, ascending-sequence list of entries with a fixed header (size,
 * lowestSequence, highestSequence), little-endian. Capacity is deliberately NOT in the wire format
 * — it is a configuration parameter, and the cluster framework constructs the journal at the
 * configured capacity before calling {@link #restoreFrom}. The {@link Long2ObjectHashMap} index is
 * rebuilt on restore — per Aeron Cluster snapshot guidance we serialize the state shape, not the
 * implementation shape. No SBE wrapper in this PR; the eventual cluster framework consumer (APP-8,
 * Wave 4) will wrap the raw bytes in an SBE {@code EventJournalSnapshot} template (207 is
 * reserved).
 */
public final class EventJournal {

  /** Default ring capacity: 2^16 (65,536) entries. */
  public static final int DEFAULT_CAPACITY = 65_536;

  /**
   * Snapshot header: size(int) + lowestSequence(long) + highestSequence(long). Entries follow
   * immediately after the header in ascending sequence order. Capacity is intentionally absent —
   * see class Javadoc.
   */
  public static final int SNAPSHOT_HEADER_LENGTH = Integer.BYTES + Long.BYTES * 2;

  private final int capacity;
  private final int capacityMask;
  private final EventEntry[] ring;
  private final Long2ObjectHashMap<EventEntry> index;

  /** Next slot to overwrite. Wraps via {@code writeIdx = (writeIdx + 1) & capacityMask}. */
  private int writeIdx;

  /** Number of entries currently in the ring, 0..capacity. Saturates at capacity once full. */
  private int size;

  /** Oldest sequence number in the current window, or 0 when empty. */
  private long lowestSequence;

  /** Newest sequence number in the current window, or 0 when empty. */
  private long highestSequence;

  /** Construct an empty journal with the default capacity. */
  public EventJournal() {
    this(DEFAULT_CAPACITY);
  }

  /**
   * Construct an empty journal with the supplied capacity.
   *
   * @param capacity ring size, must be {@code >= 2} and a power of two. Capacity 1 is rejected
   *     because the ring-eviction math reads the "next slot" to derive the new {@link
   *     #lowestSequence()}, and with a single slot "next" wraps back to the same slot being evicted
   *     — the caller would see a stale value. A bounded journal with only one retained event is
   *     also not a useful configuration in practice.
   * @throws IllegalArgumentException if {@code capacity} is less than 2 or not a power of two
   */
  public EventJournal(int capacity) {
    if (capacity < 2 || (capacity & (capacity - 1)) != 0) {
      throw new IllegalArgumentException(
          "EventJournal capacity must be a power of two >= 2, was " + capacity);
    }
    this.capacity = capacity;
    this.capacityMask = capacity - 1;
    this.ring = new EventEntry[capacity];
    for (int i = 0; i < capacity; i++) {
      this.ring[i] = new EventEntry();
    }
    // Pre-size the index to avoid rehash during the cluster duty cycle. Load factor 0.65 means
    // the map's internal table is sized ~= capacity / 0.65, which is capacity * 1.54. For
    // capacity=65,536 that's ~100 k table entries — well within memory budget and guaranteed to
    // never rehash during normal operation.
    this.index = new Long2ObjectHashMap<>(capacity, 0.65f);
    this.writeIdx = 0;
    this.size = 0;
    this.lowestSequence = 0L;
    this.highestSequence = 0L;
  }

  // ---------------------------------------------------------------------------
  // Hot-path API
  // ---------------------------------------------------------------------------

  /**
   * Append a new event to the journal. If the ring is full, overwrites the oldest slot (and removes
   * its sequence number from the lookup index). Called exactly once per domain event from the
   * cluster duty cycle. Zero allocation.
   *
   * <p>Preconditions (enforced, throw {@link IllegalArgumentException} on violation):
   *
   * <ul>
   *   <li>{@code seqNo >= 1} — {@code 0} is reserved as the "unused slot" sentinel inside {@link
   *       EventEntry}
   *   <li>{@code srcLength >= 0}
   * </ul>
   *
   * @param seqNo sequence number assigned by {@link
   *     com.trading.engine.cluster.sequencer.EventSequencer}
   * @param eventType raw {@code EventTypeEnum} wire value from the SBE schema
   * @param src source buffer containing the serialized event bytes
   * @param srcOffset start offset of the event bytes inside {@code src}
   * @param srcLength number of event bytes to copy
   */
  public void append(long seqNo, int eventType, DirectBuffer src, int srcOffset, int srcLength) {
    if (seqNo < 1L) {
      throw new IllegalArgumentException("EventJournal seqNo must be >= 1, was " + seqNo);
    }
    if (srcLength < 0) {
      throw new IllegalArgumentException("EventJournal srcLength must be >= 0, was " + srcLength);
    }
    if (srcLength > EventEntry.MAX_PAYLOAD_LENGTH) {
      throw new IllegalArgumentException(
          "EventJournal srcLength must be <= "
              + EventEntry.MAX_PAYLOAD_LENGTH
              + ", was "
              + srcLength);
    }
    final EventEntry slot = ring[writeIdx];

    if (size == capacity) {
      // Ring is full — evict the slot we're about to overwrite and bump lowestSequence to the
      // new oldest (which is the slot AFTER the one we're overwriting — that slot currently
      // holds the second-oldest entry and becomes the oldest once we reuse this one).
      index.remove(slot.sequenceNumber());
      lowestSequence = ring[(writeIdx + 1) & capacityMask].sequenceNumber();
    } else if (size == 0) {
      // First ever append — initialise the window low bound.
      lowestSequence = seqNo;
    }

    slot.set(seqNo, eventType, src, srcOffset, srcLength);
    index.put(seqNo, slot);

    writeIdx = (writeIdx + 1) & capacityMask;
    if (size < capacity) {
      size++;
    }
    highestSequence = seqNo;
  }

  /**
   * Walk the journal in ascending sequence order from {@code fromSeqNo} (inclusive) to {@link
   * #highestSequence()} (inclusive), invoking {@code handler.onEvent(...)} for each event. Zero
   * allocation.
   *
   * <p>If {@code fromSeqNo} is below {@link #lowestSequence()} (the consumer fell behind further
   * than the journal buffered), replay silently clamps to {@code lowestSequence} and delivers only
   * the events still available — no exception. Consumers detecting a gap compare {@code fromSeqNo}
   * against {@code lowestSequence()} themselves.
   *
   * <p>If {@code fromSeqNo > highestSequence()} (consumer is already caught up), returns 0 and the
   * handler is not called. {@code fromSeqNo <= 0} is legal and behaves like {@code fromSeqNo == 1}
   * — the entire current window is delivered.
   *
   * @return number of events delivered
   */
  public int replayFrom(long fromSeqNo, EventReplayHandler handler) {
    if (size == 0 || fromSeqNo > highestSequence) {
      return 0;
    }
    // Walk the ring directly in ascending sequence order rather than iterating the seq-number
    // range — symmetric with snapshotTo, O(size) rather than O(highestSequence - fromSeqNo),
    // and doesn't assume seqNos are dense. The oldest live slot is at writeIdx when the ring
    // is full (next write evicts it); otherwise it's at slot 0.
    final int startIdx = (size == capacity) ? writeIdx : 0;
    int count = 0;
    for (int i = 0; i < size; i++) {
      final EventEntry entry = ring[(startIdx + i) & capacityMask];
      if (entry.sequenceNumber() >= fromSeqNo) {
        handler.onEvent(
            entry.sequenceNumber(), entry.eventType(), entry.payload(), 0, entry.payloadLength());
        count++;
      }
    }
    return count;
  }

  /**
   * Look up an entry by sequence number. O(1). Returns {@code null} if the sequence number is
   * outside the current window (either too old to be retained or not yet appended).
   *
   * <p>The returned entry is the journal's internal slot — read-only, do not retain a reference
   * past the current call, the slot may be overwritten on the next {@link #append}.
   */
  public EventEntry get(long seqNo) {
    return index.get(seqNo);
  }

  // ---------------------------------------------------------------------------
  // Diagnostics
  // ---------------------------------------------------------------------------

  public int size() {
    return size;
  }

  public int capacity() {
    return capacity;
  }

  public long lowestSequence() {
    return lowestSequence;
  }

  public long highestSequence() {
    return highestSequence;
  }

  public boolean isEmpty() {
    return size == 0;
  }

  // ---------------------------------------------------------------------------
  // Snapshot save / restore
  // ---------------------------------------------------------------------------

  /**
   * Serialize the entire journal state to {@code dst} at {@code offset}. Writes a header (size,
   * lowestSequence, highestSequence) followed by {@code size} entries in ascending sequence-number
   * order. Little-endian. Returns total bytes written.
   *
   * <p>The serialized form is a flat list, not a map structure — consistent with Aeron Cluster
   * guidance to serialize state shape rather than implementation shape. Capacity is intentionally
   * absent from the wire format; see class Javadoc. On {@link #restoreFrom}, the {@link
   * Long2ObjectHashMap} index is rebuilt from the flat list.
   */
  public int snapshotTo(MutableDirectBuffer dst, int offset) {
    dst.putInt(offset, size, ByteOrder.LITTLE_ENDIAN);
    dst.putLong(offset + Integer.BYTES, lowestSequence, ByteOrder.LITTLE_ENDIAN);
    dst.putLong(offset + Integer.BYTES + Long.BYTES, highestSequence, ByteOrder.LITTLE_ENDIAN);

    int written = SNAPSHOT_HEADER_LENGTH;
    if (size > 0) {
      // Walk the ring in ascending sequence order. When the ring is full, the oldest slot is
      // the one just ahead of writeIdx (circularly); when it's not full, entries occupy
      // slots [0, size) and the oldest is at 0.
      final int startIdx = (size == capacity) ? writeIdx : 0;
      for (int i = 0; i < size; i++) {
        final EventEntry entry = ring[(startIdx + i) & capacityMask];
        written += entry.writeTo(dst, offset + written);
      }
    }
    return written;
  }

  /**
   * Restore the journal state from {@code src} at {@code offset}, overwriting any current state.
   * The cluster framework is responsible for constructing the journal at the right capacity before
   * calling restore — capacity is not in the wire format.
   *
   * <p>Validates the header consistency (size in range, lowest/highest in legal relation) and the
   * per-entry sequence monotonicity. A corrupted snapshot is rejected with {@link
   * IllegalStateException}.
   *
   * <p>After restore, the live entries occupy {@code ring[0..size)} linearly (the lowest sequence
   * is at slot 0). {@code writeIdx} is set to {@code size & capacityMask} which is the next slot to
   * overwrite — equal to {@code size} for an under-full ring, and 0 for a full ring (where the next
   * append correctly evicts slot 0, the oldest).
   *
   * @return number of bytes consumed
   */
  public int restoreFrom(DirectBuffer src, int offset) {
    final int snapshotSize = src.getInt(offset, ByteOrder.LITTLE_ENDIAN);
    final long snapshotLowest = src.getLong(offset + Integer.BYTES, ByteOrder.LITTLE_ENDIAN);
    final long snapshotHighest =
        src.getLong(offset + Integer.BYTES + Long.BYTES, ByteOrder.LITTLE_ENDIAN);

    if (snapshotSize < 0 || snapshotSize > capacity) {
      throw new IllegalStateException(
          "EventJournal snapshot size out of range [0, " + capacity + "], was " + snapshotSize);
    }
    if (snapshotSize == 0) {
      if (snapshotLowest != 0L || snapshotHighest != 0L) {
        throw new IllegalStateException(
            "EventJournal empty snapshot must have lowest=highest=0, was lowest="
                + snapshotLowest
                + " highest="
                + snapshotHighest);
      }
    } else {
      if (snapshotLowest < 1L) {
        throw new IllegalStateException(
            "EventJournal non-empty snapshot lowestSequence must be >= 1, was " + snapshotLowest);
      }
      if (snapshotHighest < snapshotLowest) {
        throw new IllegalStateException(
            "EventJournal snapshot highestSequence "
                + snapshotHighest
                + " must be >= lowestSequence "
                + snapshotLowest);
      }
    }

    // Clear state. We reuse the pre-allocated EventEntry instances in the ring; clear() resets
    // their seqNo sentinel to 0 so stale data from a previous restore cycle doesn't leak.
    index.clear();
    for (int i = 0; i < capacity; i++) {
      ring[i].clear();
    }

    int consumed = SNAPSHOT_HEADER_LENGTH;
    long previousSeqNo = 0L;
    for (int i = 0; i < snapshotSize; i++) {
      final EventEntry entry = ring[i];
      consumed += entry.readFrom(src, offset + consumed);
      final long entrySeqNo = entry.sequenceNumber();
      if (entrySeqNo <= previousSeqNo) {
        throw new IllegalStateException(
            "EventJournal snapshot entries must be strictly ascending: entry "
                + i
                + " seqNo "
                + entrySeqNo
                + " not greater than previous "
                + previousSeqNo);
      }
      if (i == 0 && entrySeqNo != snapshotLowest) {
        throw new IllegalStateException(
            "EventJournal first restored entry seqNo "
                + entrySeqNo
                + " does not match snapshot lowestSequence "
                + snapshotLowest);
      }
      if (i == snapshotSize - 1 && entrySeqNo != snapshotHighest) {
        throw new IllegalStateException(
            "EventJournal last restored entry seqNo "
                + entrySeqNo
                + " does not match snapshot highestSequence "
                + snapshotHighest);
      }
      index.put(entrySeqNo, entry);
      previousSeqNo = entrySeqNo;
    }

    this.size = snapshotSize;
    this.lowestSequence = snapshotLowest;
    this.highestSequence = snapshotHighest;
    // Next slot to overwrite. For size < capacity this equals size (entries occupy [0, size), so
    // the next free slot is at index size). For size == capacity this masks down to 0, which
    // correctly evicts slot 0 (the oldest, since restore packs ascending from slot 0) on the next
    // append.
    this.writeIdx = snapshotSize & capacityMask;
    return consumed;
  }
}
