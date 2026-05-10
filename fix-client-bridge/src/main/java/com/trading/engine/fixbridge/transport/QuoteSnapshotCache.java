package com.trading.engine.fixbridge.transport;

import com.trading.engine.fixbridge.translator.QuoteSnapshot;

/**
 * Per-session quote-snapshot cache. Maps an inbound {@code quoteId} byte-slice to a bound {@link
 * QuoteSnapshot} that carries all structural fields needed to translate an {@code AcceptQuote} into
 * a FIX {@code NewOrderSingle (35=D)} with {@code OrdType=D (Previously Quoted)}.
 *
 * <p><b>Why a SAM seam?</b> The real cache implementation is backed by the per-session quote map
 * that is populated when {@code Quote} events arrive from the gateway; that map lives at the
 * launcher level and cannot be imported into {@link ArtioFixCommandSink} without coupling the
 * bridge module to the gateway's runtime state. This SAM lets the sink stay cache-implementation-
 * free while the launcher binds the live {@code SessionQuoteIndex}-backed impl at startup.
 *
 * <p><b>Threading.</b> Implementations must be called only from the per-session Netty event loop.
 * The underlying quote map is single-threaded by the event-loop ownership invariant.
 *
 * <p><b>Allocation.</b> {@link #lookup} returns a reference to a pre-allocated {@link
 * QuoteSnapshot} pool entry — no allocation on the hot path. {@link #evict} performs in-place reset
 * ({@link QuoteSnapshot#reset()}) — also zero-allocation.
 *
 * @see ArtioFixCommandSink
 * @see QuoteSnapshot
 */
public interface QuoteSnapshotCache {

  /**
   * Look up the snapshot for the given {@code quoteId} byte slice. Returns the bound {@link
   * QuoteSnapshot} if the quote is present and not yet evicted, or {@code null} on a cache miss
   * (quote expired, unknown, or already accepted/rejected).
   *
   * <p>The returned snapshot is a <em>borrow</em> — the caller must not hold a reference beyond the
   * current dispatch cycle, because {@link #evict} or a subsequent {@link #lookup} may recycle the
   * pool slot.
   *
   * @param buf byte array containing the quoteId
   * @param off offset of the quoteId within {@code buf}
   * @param len length of the quoteId in bytes
   * @return bound {@link QuoteSnapshot} on cache hit; {@code null} on miss
   */
  QuoteSnapshot lookup(byte[] buf, int off, int len);

  /**
   * Evict the snapshot for the given {@code quoteId} byte slice, returning the pool slot to the
   * free list. No-op if the quoteId is not present (safe to call redundantly after miss).
   *
   * @param buf byte array containing the quoteId
   * @param off offset of the quoteId within {@code buf}
   * @param len length of the quoteId in bytes
   */
  void evict(byte[] buf, int off, int len);

  /**
   * No-op cache used in tests and at bootstrap. {@link #lookup} always returns {@code null}; {@link
   * #evict} is a no-op.
   */
  QuoteSnapshotCache NOOP =
      new QuoteSnapshotCache() {
        @Override
        public QuoteSnapshot lookup(final byte[] buf, final int off, final int len) {
          return null;
        }

        @Override
        public void evict(final byte[] buf, final int off, final int len) {
          // intentional no-op
        }
      };
}
