package com.trading.engine.cluster.refdata;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

/**
 * A replicated, single-threaded reference-data store inside the cluster. Concrete stores
 * (AccountStore, CurrencyStore, RiskLimitStore, future VenueStore / PartyStore / CalendarStore /
 * SymbolStore, …) implement this interface so the {@link ReferenceDataRegistry} can drive snapshot
 * save/restore generically without knowing about each store's domain shape.
 *
 * <p><b>Snapshot framing.</b> {@link #snapshotTo(MutableDirectBuffer, int)} writes a complete SBE
 * message (header + body) starting at {@code dst[offset..]} and returns the total bytes written.
 * {@link #restoreFrom(DirectBuffer, int)} reads the matching SBE message starting at {@code
 * src[offset..]} (where the SBE header has already been positioned by the caller) and returns the
 * bytes consumed. The store is responsible for its own SBE template — encoder, decoder,
 * repeating-group iteration, field copy.
 *
 * <p><b>Determinism.</b> Implementations MUST iterate records in a deterministic order (e.g.,
 * sorted by primary key) when snapshotting. Agrona hash-map iteration order is not guaranteed
 * stable across JVM versions, so concrete stores walk a sorted key array.
 *
 * <p><b>Threading.</b> Single-threaded by contract — the cluster duty cycle is the only mutator and
 * reader. No synchronization, no {@code volatile}.
 *
 * <p><b>Allocation.</b> The hot-path APIs (concrete stores' {@code get} / {@code contains} / {@code
 * getByCode}) are zero-allocation. Snapshot save/restore is allowed to allocate (it runs once per
 * recovery, not per duty-cycle iteration) per the project's CLAUDE.md exemption.
 */
public interface ReferenceDataStore {

  /**
   * SBE template id of the snapshot message this store reads/writes. Used by {@link
   * ReferenceDataRegistry} to dispatch incoming snapshot fragments to the correct store on cluster
   * recovery.
   */
  int snapshotTemplateId();

  /** Number of records currently held. */
  int size();

  /**
   * Drop all in-memory state. Called by the registry at the start of a full snapshot restore before
   * any {@link #restoreFrom} fragment is replayed.
   */
  void clear();

  /**
   * Serialize the entire store into {@code dst} starting at {@code offset}. Writes a complete SBE
   * message (8-byte header + repeating-group body). Records MUST be written in deterministic order
   * so the byte output is identical for the same logical state across JVM versions and runs.
   *
   * @return number of bytes written, including the SBE header
   */
  int snapshotTo(MutableDirectBuffer dst, int offset);

  /**
   * Restore state from one snapshot SBE message at {@code src[offset..]}. The caller has already
   * verified that the message's templateId matches {@link #snapshotTemplateId()}.
   *
   * <p>Implementations MUST be self-sufficient: call {@link #clear()} (or otherwise drop all
   * pre-existing state) at the start so that restoring a smaller / empty snapshot over a populated
   * store doesn't leave orphan rows behind. The registry's {@link ReferenceDataRegistry#resetAll}
   * also clears every store before replay, but defensive clearing here keeps {@code restoreFrom}
   * safe to call standalone (e.g. from a unit test or a future single-store recovery path).
   *
   * @return number of bytes consumed, including the SBE header
   */
  int restoreFrom(DirectBuffer src, int offset);
}
