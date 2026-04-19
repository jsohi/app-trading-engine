package com.trading.engine.orchestrator;

import com.epam.deltix.gflog.api.Log;
import com.epam.deltix.gflog.api.LogFactory;
import com.trading.engine.messages.sbe.PriceResponseDecoder;
import com.trading.engine.messages.sbe.QuoteRequestDecoder;
import com.trading.engine.messages.util.ByteArrayKey;
import org.agrona.DirectBuffer;
import org.agrona.collections.Object2ObjectHashMap;

/**
 * Pre-allocated object pool and lifecycle state machine for RFQ orchestration. Manages the
 * allocation, state transitions, timeout expiry, and release of {@link RfqState} flyweight slots.
 *
 * <h3>State transition table</h3>
 *
 * <pre>
 *   FREE ──→ PENDING_PRICE ──→ QUOTED ──→ PENDING_VALIDATION ──→ COMPLETED
 *                 │                │                │
 *                 ├──→ REJECTED    ├──→ REJECTED    ├──→ REJECTED
 *                 └──→ EXPIRED     └──→ EXPIRED     └──→ EXPIRED
 * </pre>
 *
 * <p>Terminal states ({@link RfqState.State#COMPLETED}, {@link RfqState.State#REJECTED}, {@link
 * RfqState.State#EXPIRED}) immediately remove map entries. Pool slot release is either immediate
 * (for COMPLETED/REJECTED) or deferred to a {@code finally} block after the reap callback (for
 * EXPIRED, so the callback can read RfqState fields before the slot is released and reused).
 *
 * <h3>Lookup maps</h3>
 *
 * <ul>
 *   <li>{@code byQuoteReqId}: quoteReqId → RfqState. Populated at PENDING_PRICE, removed at
 *       terminal.
 *   <li>{@code byQuoteId}: quoteId → RfqState. Populated at QUOTED (when quoteId is generated),
 *       removed at terminal.
 * </ul>
 *
 * <h3>Per-state timeouts (aligned with docs/state-machines.md)</h3>
 *
 * <ul>
 *   <li>PENDING_PRICE: 5s (configurable)
 *   <li>QUOTED: 30s (configurable)
 *   <li>PENDING_VALIDATION: 5s (configurable)
 * </ul>
 *
 * <p><b>Threading:</b> not thread-safe — single-threaded orchestrator duty cycle only.
 *
 * <p><b>Allocation:</b> zero allocation after construction. All pool slots, ByteArrayKey instances,
 * probe keys, and Agrona maps are pre-allocated. Transition methods use probe-based lookups (zero
 * allocation) and overwrite pre-allocated owned keys.
 *
 * <p><b>Pool strategy:</b> linear scan from {@code reapCursor} position for free slot acquisition.
 * O(n) worst case but amortized by cursor advancement. 10K entries × 1-byte state check ≈ 500ns
 * worst case on modern hardware. APP-213 tracks linked free-list optimization for O(1) acquisition.
 *
 * @see RfqState
 * @see OrchestratorService
 */
public final class RfqStateMachine {

  private static final Log LOG = LogFactory.getLog(RfqStateMachine.class);

  /**
   * Functional callback interface invoked for each RFQ that expires during the {@link
   * #reapExpired(long, ReapCallback)} sweep. Implementations should encode and attempt to publish a
   * QuoteRequestReject notification to the gateway. The pool slot is released automatically by the
   * state machine in a {@code finally} block after the callback returns (or throws).
   */
  @FunctionalInterface
  public interface ReapCallback {

    /**
     * Called when an RFQ has been transitioned to {@link RfqState.State#EXPIRED} during the reap
     * sweep. The RfqState fields are valid for the duration of this callback. The pool slot is
     * released automatically by the state machine after this method returns — the callback must NOT
     * call {@code releaseAfterReap}.
     *
     * @param state the expired RfqState — valid only for the duration of this callback
     */
    void onRfqExpired(RfqState state);
  }

  // ===========================================================================
  // Pool and lookup structures
  // ===========================================================================

  private final RfqState[] pool;
  private final Object2ObjectHashMap<ByteArrayKey, RfqState> byQuoteReqId;
  private final Object2ObjectHashMap<ByteArrayKey, RfqState> byQuoteId;

  /** Pre-allocated probe key for zero-alloc lookups by quoteReqId. Never inserted into a map. */
  private final ByteArrayKey lookupProbeReqId;

  /** Pre-allocated probe key for zero-alloc lookups by quoteId. Never inserted into a map. */
  private final ByteArrayKey lookupProbeQuoteId;

  private final long pendingPriceTimeoutNanos;
  private final long quotedTimeoutNanos;
  private final long pendingValidationTimeoutNanos;

  private int activeCount;
  private int reapCursor;

  /**
   * Constructs the state machine with a pre-allocated pool of the given capacity.
   *
   * @param maxActiveRfqs maximum number of concurrently active RFQs (pool size)
   * @param pendingPriceTimeoutNanos timeout for PENDING_PRICE state in nanoseconds
   * @param quotedTimeoutNanos timeout for QUOTED state in nanoseconds
   * @param pendingValidationTimeoutNanos timeout for PENDING_VALIDATION state in nanoseconds
   * @throws IllegalArgumentException if maxActiveRfqs is not positive or any timeout is not
   *     positive
   */
  public RfqStateMachine(
      final int maxActiveRfqs,
      final long pendingPriceTimeoutNanos,
      final long quotedTimeoutNanos,
      final long pendingValidationTimeoutNanos) {

    if (maxActiveRfqs <= 0) {
      throw new IllegalArgumentException("maxActiveRfqs must be positive: " + maxActiveRfqs);
    }
    if (pendingPriceTimeoutNanos <= 0) {
      throw new IllegalArgumentException(
          "pendingPriceTimeoutNanos must be positive: " + pendingPriceTimeoutNanos);
    }
    if (quotedTimeoutNanos <= 0) {
      throw new IllegalArgumentException(
          "quotedTimeoutNanos must be positive: " + quotedTimeoutNanos);
    }
    if (pendingValidationTimeoutNanos <= 0) {
      throw new IllegalArgumentException(
          "pendingValidationTimeoutNanos must be positive: " + pendingValidationTimeoutNanos);
    }

    this.pendingPriceTimeoutNanos = pendingPriceTimeoutNanos;
    this.quotedTimeoutNanos = quotedTimeoutNanos;
    this.pendingValidationTimeoutNanos = pendingValidationTimeoutNanos;

    this.pool = new RfqState[maxActiveRfqs];
    for (int i = 0; i < maxActiveRfqs; i++) {
      pool[i] = new RfqState(i);
    }

    // Agrona Object2ObjectHashMap with 0.55 load factor (matching PricingService pattern)
    this.byQuoteReqId = new Object2ObjectHashMap<>(maxActiveRfqs, 0.55f);
    this.byQuoteId = new Object2ObjectHashMap<>(maxActiveRfqs, 0.55f);

    this.lookupProbeReqId = ByteArrayKey.emptyForLookup(RfqState.QUOTE_REQ_ID_LENGTH);
    this.lookupProbeQuoteId = ByteArrayKey.emptyForLookup(RfqState.QUOTE_ID_LENGTH);

    this.activeCount = 0;
    this.reapCursor = 0;
  }

  // ===========================================================================
  // Transition methods
  // ===========================================================================

  /**
   * Acquires a pool slot and transitions a new RFQ to {@link RfqState.State#PENDING_PRICE}.
   * Populates the slot from the QuoteRequest decoder and inserts the quoteReqId into the lookup
   * map.
   *
   * <p><b>Caller contract.</b> The caller is responsible for filtering duplicate {@code
   * quoteReqId}s before invoking this method. {@code RfqStateMachine} assumes no non-terminal RFQ
   * already holds the same quoteReqId; if one does, the new slot's map entry will silently
   * overwrite the existing entry (since {@link ByteArrayKey} equality is content-based) and the
   * orphaned slot will only be reclaimed by the timeout reaper. The canonical caller {@link
   * OrchestratorService#onQuoteRequest} performs this filter with richer FIX semantics: re-delivery
   * RETRY on {@link RfqState.State#PENDING_PRICE} (re-publish PriceRequest) and collision REJECT
   * (with {@code TEXT_DUPLICATE_QUOTE_REQ_ID}) on the other active states. A defensive in-band
   * probe was considered and rejected (PR #45 review D5) because the caller's two-way semantics
   * cannot be expressed at this layer; duplicating only the reject branch would mask re-deliveries.
   *
   * @param decoder the pre-wrapped QuoteRequest decoder — must not be retained past this call
   * @param nowNanos current monotonic time from NanoClock
   * @return the acquired RfqState, or {@code null} if the pool is exhausted
   */
  public RfqState onQuoteRequest(final QuoteRequestDecoder decoder, final long nowNanos) {
    final var slot = acquireSlot();
    if (slot == null) {
      return null;
    }
    slot.populateFromQuoteRequest(decoder, nowNanos, pendingPriceTimeoutNanos);
    byQuoteReqId.put(slot.quoteReqIdKey(), slot);
    activeCount++;
    return slot;
  }

  /**
   * Transitions an RFQ from {@link RfqState.State#PENDING_PRICE} to {@link RfqState.State#QUOTED}.
   * Applies pricing data from the PriceResponse and stores the generated quoteId. Inserts the
   * quoteId into the lookup map.
   *
   * @param quoteReqIdBytes quoteReqId bytes for lookup
   * @param reqIdOffset offset into quoteReqIdBytes
   * @param reqIdLen length of quoteReqId bytes
   * @param decoder the pre-wrapped PriceResponse decoder
   * @param generatedQuoteId the generated quoteId bytes from OrchestratorIdGenerator
   * @param quoteIdOffset offset into generatedQuoteId
   * @param quoteIdLen length of quoteId bytes
   * @param nowNanos current monotonic time from NanoClock
   * @return the RfqState if transition succeeded, or {@code null} if not found or wrong state
   */
  public RfqState onPriceResponseAccepted(
      final byte[] quoteReqIdBytes,
      final int reqIdOffset,
      final int reqIdLen,
      final PriceResponseDecoder decoder,
      final byte[] generatedQuoteId,
      final int quoteIdOffset,
      final int quoteIdLen,
      final long nowNanos) {

    final var rfq = probeByQuoteReqId(quoteReqIdBytes, reqIdOffset, reqIdLen);
    if (rfq == null || rfq.state() != RfqState.State.PENDING_PRICE) {
      if (rfq != null) {
        LOG.warn()
            .append("onPriceResponseAccepted: unexpected state=")
            .append(rfq.state().name())
            .append(" for quoteReqId at poolIndex=")
            .append(rfq.poolIndex())
            .commit();
      }
      return null;
    }

    rfq.applyPriceResponse(decoder, nowNanos, quotedTimeoutNanos);
    rfq.setQuoteId(generatedQuoteId, quoteIdOffset, quoteIdLen);
    byQuoteId.put(rfq.quoteIdKey(), rfq);
    return rfq;
  }

  /**
   * Zero-probe overload of {@link #onPriceResponseAccepted(byte[], int, int, PriceResponseDecoder,
   * byte[], int, int, long)}. Skips the internal {@code probeByQuoteReqId} lookup — the caller
   * passes the {@link RfqState} reference directly (typically obtained from an earlier {@link
   * #findByQuoteReqId} call within the same duty-cycle iteration). Avoids a redundant hash-map walk
   * on the hot path (PR #45 review — Gemini R3).
   *
   * @param rfq the RFQ slot (must be in {@link RfqState.State#PENDING_PRICE}; caller
   *     responsibility)
   * @param decoder the pre-wrapped PriceResponse decoder
   * @param generatedQuoteId the generated quoteId bytes from OrchestratorIdGenerator
   * @param quoteIdOffset offset into generatedQuoteId
   * @param quoteIdLen length of quoteId bytes
   * @param nowNanos current monotonic time from NanoClock
   * @return the RfqState if transition succeeded, or {@code null} if wrong state
   */
  public RfqState onPriceResponseAccepted(
      final RfqState rfq,
      final PriceResponseDecoder decoder,
      final byte[] generatedQuoteId,
      final int quoteIdOffset,
      final int quoteIdLen,
      final long nowNanos) {

    if (rfq.state() != RfqState.State.PENDING_PRICE) {
      LOG.warn()
          .append("onPriceResponseAccepted(rfq): unexpected state=")
          .append(rfq.state().name())
          .append(" poolIndex=")
          .append(rfq.poolIndex())
          .commit();
      return null;
    }

    rfq.applyPriceResponse(decoder, nowNanos, quotedTimeoutNanos);
    rfq.setQuoteId(generatedQuoteId, quoteIdOffset, quoteIdLen);
    byQuoteId.put(rfq.quoteIdKey(), rfq);
    return rfq;
  }

  /**
   * Transitions an RFQ from {@link RfqState.State#PENDING_PRICE} to {@link
   * RfqState.State#REJECTED}. Removes from maps and releases the pool slot.
   *
   * <p><b>Note:</b> in the publish-before-mutate pattern, the handler encodes the
   * QuoteRequestReject BEFORE calling this method, so the pool slot is not yet released when
   * encoding occurs.
   *
   * @param quoteReqIdBytes quoteReqId bytes for lookup
   * @param offset offset into quoteReqIdBytes
   * @param len length of quoteReqId bytes
   * @return the RfqState if transition succeeded, or {@code null} if not found or wrong state
   */
  public RfqState onPriceResponseRejected(
      final byte[] quoteReqIdBytes, final int offset, final int len) {

    final var rfq = probeByQuoteReqId(quoteReqIdBytes, offset, len);
    if (rfq == null || rfq.state() != RfqState.State.PENDING_PRICE) {
      if (rfq != null) {
        LOG.warn()
            .append("onPriceResponseRejected: unexpected state=")
            .append(rfq.state().name())
            .append(" for quoteReqId at poolIndex=")
            .append(rfq.poolIndex())
            .commit();
      }
      return null;
    }

    rfq.setState(RfqState.State.REJECTED);
    byQuoteReqId.remove(rfq.quoteReqIdKey());
    releaseSlot(rfq);
    return rfq;
  }

  /**
   * Zero-probe overload of {@link #onPriceResponseRejected(byte[], int, int)}. Skips the lookup —
   * caller passes the {@link RfqState} reference directly (typically obtained from an earlier
   * {@link #findByQuoteReqId}). Avoids a redundant hash-map walk (PR #45 review — Gemini R3).
   *
   * @param rfq the RFQ slot (must be in {@link RfqState.State#PENDING_PRICE})
   * @return the RfqState if transition succeeded, or {@code null} if wrong state
   */
  public RfqState onPriceResponseRejected(final RfqState rfq) {
    if (rfq.state() != RfqState.State.PENDING_PRICE) {
      LOG.warn()
          .append("onPriceResponseRejected(rfq): unexpected state=")
          .append(rfq.state().name())
          .append(" poolIndex=")
          .append(rfq.poolIndex())
          .commit();
      return null;
    }
    rfq.setState(RfqState.State.REJECTED);
    byQuoteReqId.remove(rfq.quoteReqIdKey());
    releaseSlot(rfq);
    return rfq;
  }

  /**
   * Transitions an RFQ from {@link RfqState.State#QUOTED} to {@link
   * RfqState.State#PENDING_VALIDATION}. Stashes the raw NOS bytes for later cluster forwarding.
   *
   * @param quoteIdBytes quoteId bytes for lookup
   * @param offset offset into quoteIdBytes
   * @param len length of quoteId bytes
   * @param nosBuffer the NOS fragment buffer to stash
   * @param nosOffset offset into the NOS buffer
   * @param nosLength byte length of the NOS fragment
   * @param nowNanos current monotonic time
   * @return the RfqState if transition succeeded, or {@code null} if not found, wrong state, or NOS
   *     too large. <b>NOS-too-large path:</b> the RFQ slot stays in {@code QUOTED} so the caller
   *     can read identity fields (symbol, currency, etc.) for the rejection ExecutionReport. The
   *     caller MUST invoke {@link #rejectQuoted} after publishing the rejection to release the slot
   *     — otherwise the slot leaks until the QUOTED-timeout reaper runs.
   */
  public RfqState onNewOrderSingleWithQuote(
      final byte[] quoteIdBytes,
      final int offset,
      final int len,
      final DirectBuffer nosBuffer,
      final int nosOffset,
      final int nosLength,
      final long nowNanos) {

    final var rfq = probeByQuoteId(quoteIdBytes, offset, len);
    if (rfq == null || rfq.state() != RfqState.State.QUOTED) {
      if (rfq != null) {
        LOG.warn()
            .append("onNewOrderSingleWithQuote: unexpected state=")
            .append(rfq.state().name())
            .append(" for quoteId at poolIndex=")
            .append(rfq.poolIndex())
            .commit();
      }
      return null;
    }

    if (!rfq.stashNos(nosBuffer, nosOffset, nosLength)) {
      LOG.error()
          .append("NOS too large for stash buffer: length=")
          .append(nosLength)
          .append(" max=")
          .append(OrchestratorConstants.NOS_STASH_BUFFER_SIZE)
          .append(" poolIndex=")
          .append(rfq.poolIndex())
          .commit();
      // Slot stays in QUOTED state; the caller (OrchestratorService.onNewOrderSingle) needs to
      // read the RFQ identity fields (symbol, currency, etc.) to populate the rejection
      // ExecutionReport BEFORE the slot is released. The caller MUST invoke rejectQuoted()
      // after the rejection has been encoded and published — releasing the slot in-band here
      // would zero the identity fields and corrupt the rejection.
      return null;
    }

    rfq.setState(RfqState.State.PENDING_VALIDATION);
    rfq.setExpiryNanos(nowNanos + pendingValidationTimeoutNanos);
    return rfq;
  }

  /**
   * Transitions an RFQ from {@link RfqState.State#QUOTED} to {@link RfqState.State#REJECTED}.
   * Removes from maps and releases the pool slot. Used when the NOS is too large for the stash
   * buffer — the RFQ can never complete and should be released immediately rather than waiting for
   * the QUOTED timeout.
   *
   * @param quoteIdBytes quoteId bytes for lookup
   * @param offset offset into quoteIdBytes
   * @param len length of quoteId bytes
   * @return the RfqState if transition succeeded, or {@code null} if not found or wrong state
   */
  public RfqState rejectQuoted(final byte[] quoteIdBytes, final int offset, final int len) {

    final var rfq = probeByQuoteId(quoteIdBytes, offset, len);
    if (rfq == null || rfq.state() != RfqState.State.QUOTED) {
      if (rfq != null) {
        LOG.warn()
            .append("rejectQuoted: unexpected state=")
            .append(rfq.state().name())
            .append(" for quoteId at poolIndex=")
            .append(rfq.poolIndex())
            .commit();
      }
      return null;
    }

    rfq.setState(RfqState.State.REJECTED);
    removeFromMaps(rfq);
    releaseSlot(rfq);
    return rfq;
  }

  /**
   * Zero-probe overload of {@link #rejectQuoted(byte[], int, int)}. Skips the lookup — caller
   * passes the {@link RfqState} reference directly (typically obtained from an earlier {@link
   * #findByQuoteId}). Avoids a redundant hash-map walk (PR #45 review — Gemini R3).
   *
   * @param rfq the RFQ slot (must be in {@link RfqState.State#QUOTED})
   * @return the RfqState if transition succeeded, or {@code null} if wrong state
   */
  public RfqState rejectQuoted(final RfqState rfq) {
    if (rfq.state() != RfqState.State.QUOTED) {
      LOG.warn()
          .append("rejectQuoted(rfq): unexpected state=")
          .append(rfq.state().name())
          .append(" poolIndex=")
          .append(rfq.poolIndex())
          .commit();
      return null;
    }
    rfq.setState(RfqState.State.REJECTED);
    removeFromMaps(rfq);
    releaseSlot(rfq);
    return rfq;
  }

  /**
   * Transitions an RFQ from {@link RfqState.State#PENDING_VALIDATION} to {@link
   * RfqState.State#COMPLETED}. Removes from maps and releases the pool slot.
   *
   * @param quoteIdBytes quoteId bytes for lookup
   * @param offset offset into quoteIdBytes
   * @param len length of quoteId bytes
   * @return the RfqState if transition succeeded, or {@code null} if not found or wrong state
   */
  public RfqState onValidationValid(final byte[] quoteIdBytes, final int offset, final int len) {

    final var rfq = probeByQuoteId(quoteIdBytes, offset, len);
    if (rfq == null || rfq.state() != RfqState.State.PENDING_VALIDATION) {
      if (rfq != null) {
        LOG.warn()
            .append("onValidationValid: unexpected state=")
            .append(rfq.state().name())
            .append(" for quoteId at poolIndex=")
            .append(rfq.poolIndex())
            .commit();
      }
      return null;
    }

    rfq.setState(RfqState.State.COMPLETED);
    removeFromMaps(rfq);
    releaseSlot(rfq);
    return rfq;
  }

  /**
   * Zero-probe overload of {@link #onValidationValid(byte[], int, int)}. Skips the lookup — caller
   * passes the {@link RfqState} reference directly (typically obtained from an earlier {@link
   * #findByQuoteId}). Avoids a redundant hash-map walk (PR #45 review — Gemini R3).
   *
   * @param rfq the RFQ slot (must be in {@link RfqState.State#PENDING_VALIDATION})
   * @return the RfqState if transition succeeded, or {@code null} if wrong state
   */
  public RfqState onValidationValid(final RfqState rfq) {
    if (rfq.state() != RfqState.State.PENDING_VALIDATION) {
      LOG.warn()
          .append("onValidationValid(rfq): unexpected state=")
          .append(rfq.state().name())
          .append(" poolIndex=")
          .append(rfq.poolIndex())
          .commit();
      return null;
    }
    rfq.setState(RfqState.State.COMPLETED);
    removeFromMaps(rfq);
    releaseSlot(rfq);
    return rfq;
  }

  /**
   * Transitions an RFQ from {@link RfqState.State#PENDING_VALIDATION} to {@link
   * RfqState.State#REJECTED}. Removes from maps and releases the pool slot.
   *
   * @param quoteIdBytes quoteId bytes for lookup
   * @param offset offset into quoteIdBytes
   * @param len length of quoteId bytes
   * @return the RfqState if transition succeeded, or {@code null} if not found or wrong state
   */
  public RfqState onValidationInvalid(final byte[] quoteIdBytes, final int offset, final int len) {

    final var rfq = probeByQuoteId(quoteIdBytes, offset, len);
    if (rfq == null || rfq.state() != RfqState.State.PENDING_VALIDATION) {
      if (rfq != null) {
        LOG.warn()
            .append("onValidationInvalid: unexpected state=")
            .append(rfq.state().name())
            .append(" for quoteId at poolIndex=")
            .append(rfq.poolIndex())
            .commit();
      }
      return null;
    }

    rfq.setState(RfqState.State.REJECTED);
    removeFromMaps(rfq);
    releaseSlot(rfq);
    return rfq;
  }

  /**
   * Zero-probe overload of {@link #onValidationInvalid(byte[], int, int)}. Skips the lookup —
   * caller passes the {@link RfqState} reference directly (typically obtained from an earlier
   * {@link #findByQuoteId}). Avoids a redundant hash-map walk (PR #45 review — Gemini R3).
   *
   * @param rfq the RFQ slot (must be in {@link RfqState.State#PENDING_VALIDATION})
   * @return the RfqState if transition succeeded, or {@code null} if wrong state
   */
  public RfqState onValidationInvalid(final RfqState rfq) {
    if (rfq.state() != RfqState.State.PENDING_VALIDATION) {
      LOG.warn()
          .append("onValidationInvalid(rfq): unexpected state=")
          .append(rfq.state().name())
          .append(" poolIndex=")
          .append(rfq.poolIndex())
          .commit();
      return null;
    }
    rfq.setState(RfqState.State.REJECTED);
    removeFromMaps(rfq);
    releaseSlot(rfq);
    return rfq;
  }

  // ===========================================================================
  // Timeout reap
  // ===========================================================================

  /**
   * Incrementally sweeps the pool for active RFQs whose {@code expiryNanos} has elapsed.
   * Transitions each to {@link RfqState.State#EXPIRED}, removes from maps, invokes the callback,
   * and releases the pool slot in a {@code finally} block (preventing slot leaks if the callback
   * throws).
   *
   * <p>Bounded work per duty cycle: scans {@code min(max(activeCount * 2, pool.length / 3),
   * pool.length)} entries starting from the {@code reapCursor} position (ring-buffer style). The
   * {@code pool.length / 3} floor guarantees a full pool sweep in at most 3 passes, even when
   * {@code activeCount} is low — preventing sparse RFQs from missing their timeout deadlines.
   *
   * @param nowNanos current monotonic time
   * @param callback invoked for each expired RFQ
   * @return the number of RFQs expired in this sweep
   */
  public int reapExpired(final long nowNanos, final ReapCallback callback) {
    // Ceiling division (pool.length + 2) / 3 guarantees full coverage in exactly 3 sweeps,
    // ensuring sparse RFQs are checked within the smallest timeout window (5s) at 1s intervals.
    final int scanLimit = Math.min(Math.max(activeCount * 2, (pool.length + 2) / 3), pool.length);
    int expired = 0;

    for (int i = 0; i < scanLimit; i++) {
      final int idx = (reapCursor + i) % pool.length;
      final var rfq = pool[idx];

      if (rfq.isActive() && nowNanos >= rfq.expiryNanos()) {
        rfq.setState(RfqState.State.EXPIRED);
        removeFromMaps(rfq);
        try {
          callback.onRfqExpired(rfq);
        } finally {
          // Ensure slot is always released even if callback throws
          releaseSlot(rfq);
        }
        expired++;
      }
    }

    reapCursor = (reapCursor + scanLimit) % pool.length;
    return expired;
  }

  /**
   * Full (non-incremental) reap sweep. Used by {@code OrchestratorService.onClose()} for graceful
   * shutdown: transitions ALL active RFQs to EXPIRED and invokes the callback for each. Pool slots
   * are released in a {@code finally} block (preventing slot leaks if the callback throws).
   *
   * @param nowNanos current monotonic time
   * @param callback invoked for each expired RFQ
   * @return the number of RFQs expired
   */
  public int reapAll(final long nowNanos, final ReapCallback callback) {
    int expired = 0;
    for (int i = 0; i < pool.length; i++) {
      final var rfq = pool[i];
      if (rfq.isActive()) {
        rfq.setState(RfqState.State.EXPIRED);
        removeFromMaps(rfq);
        try {
          callback.onRfqExpired(rfq);
        } finally {
          // Ensure slot is always released even if callback throws
          releaseSlot(rfq);
        }
        expired++;
      }
    }
    return expired;
  }

  // ===========================================================================
  // Probe lookups (zero allocation)
  // ===========================================================================

  /**
   * Zero-allocation probe lookup by quoteReqId. Returns the RfqState if found, {@code null}
   * otherwise. Used by the orchestrator service to detect re-delivery (ABORT retry) in the {@code
   * onQuoteRequest} handler.
   *
   * @param quoteReqIdBytes quoteReqId bytes
   * @param offset offset into quoteReqIdBytes
   * @param len length of quoteReqId bytes
   * @return the RfqState if found, or {@code null}
   */
  public RfqState findByQuoteReqId(final byte[] quoteReqIdBytes, final int offset, final int len) {
    return probeByQuoteReqId(quoteReqIdBytes, offset, len);
  }

  /**
   * Zero-allocation probe lookup by quoteId. Returns the RfqState if found, {@code null} otherwise.
   * Used by the orchestrator service to detect re-delivery in the {@code onNewOrderSingle} handler.
   *
   * @param quoteIdBytes quoteId bytes
   * @param offset offset into quoteIdBytes
   * @param len length of quoteId bytes
   * @return the RfqState if found, or {@code null}
   */
  public RfqState findByQuoteId(final byte[] quoteIdBytes, final int offset, final int len) {
    return probeByQuoteId(quoteIdBytes, offset, len);
  }

  // ===========================================================================
  // Accessors
  // ===========================================================================

  /** Returns the number of currently active (non-FREE, non-terminal) RFQs in the pool. */
  public int activeCount() {
    return activeCount;
  }

  /** Returns the total pool capacity. */
  public int capacity() {
    return pool.length;
  }

  // ===========================================================================
  // Internal helpers
  // ===========================================================================

  /**
   * Acquires a free pool slot via linear scan from the reapCursor position. Returns {@code null} if
   * the pool is exhausted.
   */
  private RfqState acquireSlot() {
    for (int i = 0; i < pool.length; i++) {
      final int idx = (reapCursor + i) % pool.length;
      if (pool[idx].state() == RfqState.State.FREE) {
        reapCursor = (idx + 1) % pool.length;
        return pool[idx];
      }
    }
    return null;
  }

  /** Releases a slot back to the pool by resetting it to FREE and decrementing activeCount. */
  private void releaseSlot(final RfqState state) {
    state.reset();
    activeCount--;
  }

  /**
   * Removes an RfqState from both lookup maps. Safe to call even if the RFQ was only in the
   * quoteReqId map (before a quoteId was assigned).
   */
  private void removeFromMaps(final RfqState rfq) {
    byQuoteReqId.remove(rfq.quoteReqIdKey());
    // quoteId map may not contain this entry if the RFQ never reached QUOTED state
    if (rfq.quoteIdLen() > 0) {
      byQuoteId.remove(rfq.quoteIdKey());
    }
  }

  /** Zero-alloc probe lookup by quoteReqId using the pre-allocated probe key. */
  private RfqState probeByQuoteReqId(
      final byte[] quoteReqIdBytes, final int offset, final int len) {
    lookupProbeReqId.wrapForProbe(quoteReqIdBytes, offset, len);
    return byQuoteReqId.get(lookupProbeReqId);
  }

  /** Zero-alloc probe lookup by quoteId using the pre-allocated probe key. */
  private RfqState probeByQuoteId(final byte[] quoteIdBytes, final int offset, final int len) {
    lookupProbeQuoteId.wrapForProbe(quoteIdBytes, offset, len);
    return byQuoteId.get(lookupProbeQuoteId);
  }
}
