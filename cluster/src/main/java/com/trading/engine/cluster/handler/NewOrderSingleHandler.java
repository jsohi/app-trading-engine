package com.trading.engine.cluster.handler;

import com.epam.deltix.gflog.api.Log;
import com.epam.deltix.gflog.api.LogFactory;
import com.trading.engine.cluster.OrderState;
import com.trading.engine.cluster.metrics.RfqMetrics;
import com.trading.engine.cluster.refdata.AccountState;
import com.trading.engine.cluster.refdata.AccountStore;
import com.trading.engine.cluster.refdata.CurrencyStore;
import com.trading.engine.cluster.refdata.RiskLimitStore;
import com.trading.engine.cluster.state.RfqSlot;
import com.trading.engine.cluster.state.RfqStateMachine;
import com.trading.engine.cluster.state.TradingState;
import com.trading.engine.messages.sbe.AccountStatusEnum;
import com.trading.engine.messages.sbe.CancelReasonEnum;
import com.trading.engine.messages.sbe.ClOrdIdDedupSnapshotDecoder;
import com.trading.engine.messages.sbe.ClOrdIdDedupSnapshotEncoder;
import com.trading.engine.messages.sbe.MessageHeaderDecoder;
import com.trading.engine.messages.sbe.MessageHeaderEncoder;
import com.trading.engine.messages.sbe.NewOrderSingleDecoder;
import com.trading.engine.messages.sbe.OrdTypeEnum;
import com.trading.engine.messages.sbe.OrderCanceledEventEncoder;
import com.trading.engine.messages.sbe.OrderCreatedEventEncoder;
import com.trading.engine.messages.sbe.OrderRejectedEventEncoder;
import com.trading.engine.messages.sbe.ProductTypeEnum;
import com.trading.engine.messages.sbe.RejectReasonEnum;
import com.trading.engine.messages.sbe.RiskCheckEnum;
import com.trading.engine.messages.sbe.SettlTypeEnum;
import com.trading.engine.messages.sbe.SideEnum;
import com.trading.engine.messages.sbe.TenorEnum;
import com.trading.engine.messages.sbe.TimeInForceEnum;
import io.aeron.cluster.service.ClientSession;
import java.util.Objects;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Long2LongHashMap;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.collections.LongHashSet;
import org.agrona.collections.LongLongConsumer;
import org.agrona.concurrent.UnsafeBuffer;

/**
 * Handles NewOrderSingle (NOS) commands: decodes the inbound FIX-style command, validates it
 * against reference data and risk limits, and either emits an {@code OrderCreatedEvent} (happy
 * path) or an {@code OrderRejectedEvent} (validation failure). Extracted from the monolithic {@code
 * TradingClusteredService} to keep command dispatch handlers as focused, testable units.
 *
 * <p><b>Event-sourced two-phase flow:</b>
 *
 * <ol>
 *   <li><b>Phase A (before emit):</b> generate deterministic order/exec IDs via {@link
 *       TradingState#generateOrderId()} and {@link TradingState#generateExecId()}, then encode the
 *       {@code OrderCreatedEvent} into the pre-allocated egress buffer.
 *   <li><b>Phase B (after emit):</b> apply state via {@link TradingState#applyOrderCreated} —
 *       acquires a pool slot and populates {@link OrderState} from the event data. If pool
 *       acquisition fails at this stage (should not happen due to the pre-validation guard), an
 *       {@link IllegalStateException} is thrown to trigger Aeron Cluster failover.
 * </ol>
 *
 * <p><b>Dedup (APP-206):</b> Before any validation, the handler hashes {@code (sessionId, clOrdId)}
 * into a {@link Long2LongHashMap} keyed by FNV-1a 64-bit hash → first-seen cluster timestamp. A
 * second submission within the {@code CLORDID_DEDUP_WINDOW_NS} (24 h) window is rejected with
 * {@link RejectReasonEnum#DuplicateClOrdId} — even if the first attempt was itself rejected by
 * downstream validation, matching the LMAX / CME "ClOrdID consumed on first sight" semantics.
 *
 * <p><b>Validation (12 pre-trade checks plus §9.2a quote-acceptance peek):</b>
 *
 * <ol>
 *   <li>Symbol must not be empty (UnknownSymbol)
 *   <li>OrderQty must be positive (InvalidQuantity)
 *   <li>Limit orders must have positive price (InvalidPrice)
 *   <li>AccountCode must not be empty (AccountNotFound)
 *   <li>Account must exist in {@link AccountStore} (AccountNotFound)
 *   <li>Account status must be Active (AccountSuspended)
 *   <li>Account must have CAN_TRADE permission (AccountNoTradePermission)
 *   <li>Currency must be 3 uppercase ASCII letters (InvalidCurrencyCode)
 *   <li>Currency must exist in {@link CurrencyStore} (UnknownCurrency)
 *   <li>(NEW per APP-232 §9.2a) NOS-with-quoteId peek phase: when {@code ordType=PreviouslyQuoted}
 *       and {@code quoteId} is non-empty, the order is matched against an active QUOTED RFQ slot
 *       via {@link RfqStateMachine#peekByQuoteId}; rejects on unknown / expired quote, side
 *       mismatch, or price/qty bps tolerance breach
 *   <li>OrderQty must not exceed account maxOrderSize risk limit (OrderExceedsMaxSize)
 *   <li>Order book must not be full (BookFull) — checked before generating IDs to avoid wasting
 *       deterministic counter space
 * </ol>
 *
 * <p>If checks 1–10 pass, the §9.2a slot reference is cached in {@link #pendingQuoteAcceptSlot}; if
 * checks 11–12 then reject, the slot is left intact in QUOTED state for client retry. The slot
 * transitions atomically to ACCEPTED + release as step 13 of {@link #admitNewOrder} after {@link
 * TradingState#applyOrderCreated} succeeds.
 *
 * <p><b>Threading:</b> single-threaded cluster duty cycle. No synchronization required.
 *
 * <p><b>Allocation:</b> zero allocation after construction. All SBE flyweight decoders, encoders,
 * and scratch byte arrays are pre-allocated as instance fields.
 *
 * @see CommandHandler
 * @see EventSink
 * @see TradingState
 */
public final class NewOrderSingleHandler implements CommandHandler, SessionMetricsRecorder {

  /** Zero-allocation GFLog logger (APP-151 phase 5 — per-session metrics summary on close). */
  private static final Log LOG = LogFactory.getLog(NewOrderSingleHandler.class);

  // -- Pre-allocated SBE flyweights (zero-allocation hot path) --
  private final NewOrderSingleDecoder nosDecoder = new NewOrderSingleDecoder();
  private final OrderCreatedEventEncoder orderCreatedEncoder = new OrderCreatedEventEncoder();
  private final OrderRejectedEventEncoder orderRejectedEncoder = new OrderRejectedEventEncoder();
  private final OrderCanceledEventEncoder orderCanceledEncoder = new OrderCanceledEventEncoder();
  private final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();

  /**
   * Pre-allocated codec for {@link ClOrdIdDedupSnapshotEncoder} writes during {@link
   * #snapshotDedupTo}. Snapshot encode is OFF the steady-state hot path (cluster snapshots fire
   * minutes apart), but the codec is still pre-allocated to keep the handler honest on the
   * "zero-allocation after construction" contract that the rest of the class observes.
   */
  private final ClOrdIdDedupSnapshotEncoder clOrdIdDedupSnapEncoder =
      new ClOrdIdDedupSnapshotEncoder();

  /** Pre-allocated codec for the restore path ({@link #restoreDedupFrom}). */
  private final ClOrdIdDedupSnapshotDecoder clOrdIdDedupSnapDecoder =
      new ClOrdIdDedupSnapshotDecoder();

  /** Egress buffer for encoding domain events. Sized to accommodate the largest event. */
  private final UnsafeBuffer egressBuffer = new UnsafeBuffer(new byte[8 * 1024]);

  // -- Scratch buffers for char-array fields (SBE fixed-length character arrays) --

  /** ClOrdID scratch — FIX tag 11, 20-byte fixed-length ASCII. */
  private final byte[] clOrdIdScratch = new byte[20];

  /** Symbol scratch — FIX tag 55, 8-byte fixed-length ASCII. */
  private final byte[] symbolScratch = new byte[8];

  /** AccountCode scratch — FIX tag 1, 16-byte fixed-length ASCII. */
  private final byte[] accountCodeScratch = new byte[16];

  /** QuoteId scratch — FIX tag 117, 20-byte fixed-length ASCII. */
  private final byte[] quoteIdScratch = new byte[20];

  /** SettlDate scratch — FIX tag 64, 8-byte fixed-length ASCII. */
  private final byte[] settlDateScratch = new byte[8];

  // -- Currency bytes stashed for the reject path --
  // The reject encoder needs currency bytes that were extracted from the NOS decoder.
  // We stash them on instance fields so emitOrderRejected can access them even though
  // the reject may be emitted from validateNewOrder (which does not pass currency bytes).
  private byte currencyByte0;
  private byte currencyByte1;
  private byte currencyByte2;

  // -- APP-62 §D — orderQty and price stashed for the reject path --
  // OrderRejectedEvent (template 101) now carries the submitted orderQty and price as part of
  // the SEC 15c3-5(b) audit reconstruction contract. Same rationale as currency bytes above:
  // stashed on instance fields so emitOrderRejected can write them regardless of which
  // validation branch fired.
  private long stashedOrderQty;
  private long stashedPrice;

  // -- Injected dependencies --
  private final TradingState tradingState;
  private final AccountStore accountStore;
  private final CurrencyStore currencyStore;
  private final RiskLimitStore riskLimitStore;

  // ===========================================================================
  // ClOrdID dedup (APP-206)
  //
  // Per FIX 4.4: ClOrdID (tag 11) must be unique per session for the trading
  // day. A duplicate ClOrdID — even after the original order was rejected —
  // is treated as a protocol error and rejected with
  // {@link RejectReasonEnum#DuplicateClOrdId} (5).
  //
  // Storage: {@link Long2LongHashMap} keyed by a 64-bit hash of
  // {@code (session.id, effective-clOrdId-bytes)} → first-seen cluster
  // timestamp (epoch nanos). The 24-hour window matches the FIX trading-day
  // boundary; entries outside the window are evicted lazily on the next
  // dedup-key insert that crosses the size watermark.
  //
  // Hash-collision risk: 64-bit hash space + 60K active entries gives
  // P(collision) ≈ 9.7e-11 — well below the noise floor of every other
  // failure mode in the pipeline. The trade-off buys hot-path zero-alloc:
  // a per-session {@code ObjectHashSet<byte[]>} would require byte-array
  // boxing on every put + AsciiSequenceView allocation per query.
  //
  // Snapshot persistence: the registry IS persisted across cluster
  // snapshot/restore via {@link #snapshotDedupTo} / {@link #restoreDedupFrom}
  // (SBE template 210 ClOrdIdDedupSnapshot, wired into
  // TradingClusteredService.encodeSnapshotFragments / applySnapshotFragment).
  // After restore, the registry contains every (sessionId, clOrdId) pair
  // observed before the snapshot point that is still inside the 24h window —
  // closing the previously-noted duplicate-admission window.
  // ===========================================================================

  /** Dedup window matching the FIX trading-day boundary. */
  static final long CLORDID_DEDUP_WINDOW_NS = 24L * 3600L * 1_000_000_000L;

  /**
   * Hard cap above which the registry refuses new inserts (fail-closed with {@code BookFull}) to
   * bound memory and protect the cluster from runaway-session insert storms. Capped at 60 000 — the
   * SBE snapshot schema ({@code ClOrdIdDedupSnapshot}, template 210) uses the standard {@code
   * groupSizeEncoding} composite whose {@code numInGroup} is uint16, hard-limiting the snapshot
   * encode to 65 535 entries; 60 000 leaves comfortable headroom against late inserts arriving
   * between cap-check and snapshot encode. At 60 000 the registry retains ~2 MB off-heap and
   * eviction walks remain bounded by the cap, not by total throughput.
   *
   * <p>If a deployment ever needs a higher cap, the snapshot path must first be refactored to chunk
   * the registry across multiple group-sized fragments (or switch to a vardata-bytes encoding) —
   * SBE does not allow {@code numInGroup} larger than uint16. Tracked under APP-62 risk-engine
   * umbrella ("snapshot persistence" section).
   */
  static final int CLORDID_DEDUP_MAX_SIZE = 60_000;

  /** Sentinel returned by the Long2LongHashMap when a key is absent. */
  static final long CLORDID_DEDUP_MISSING = Long.MIN_VALUE;

  /**
   * Minimum interval between lazy eviction scans (60 s). Without this throttle, a registry that is
   * at the cap AND receiving sustained traffic of NEW (not refreshed) keys would trigger a full
   * O(N) eviction walk on every NOS — a "death spiral" where tail latency degrades as throughput
   * climbs. The 60 s gate guarantees the O(N) scan runs at most once per minute, so the amortised
   * hot-path cost stays bounded regardless of insert rate.
   */
  static final long CLORDID_EVICTION_INTERVAL_NS = 60L * 1_000_000_000L;

  /**
   * {@code (sessionId, clOrdIdHash)} → first-seen cluster timestamp (epoch nanos). Pre-sized to the
   * watermark to avoid rehash thrash during steady- state operation; growth past the watermark
   * triggers lazy eviction.
   *
   * <p>Package-private for direct-size assertions in {@link NewOrderSingleHandlerClOrdIdDedupTest}.
   */
  final Long2LongHashMap clOrdIdRegistry =
      new Long2LongHashMap(CLORDID_DEDUP_MAX_SIZE * 2, 0.65f, CLORDID_DEDUP_MISSING);

  /**
   * Per-account rate-limit state for APP-62 slice 2. Key = accountId. Value = packed long: upper 32
   * bits hold the current 1-second bucket index ({@code clusterTimestamp / 1_000_000_000}), lower
   * 32 bits hold the per-bucket admission count. Storing both in one {@link Long2LongHashMap} entry
   * avoids any companion-object allocation; the bucket index uses uint32 semantics under masking so
   * roll-over only matters in year 2106.
   *
   * <p>NOT persisted in snapshots: a 1-second window is short enough that the worst-case restart
   * over-allowance is ≤ 1 × maxOrdersPerSecond admissions, which is acceptable for a circuit
   * breaker (not an audit-grade quota). Persisting it would couple rate state to the snapshot
   * cadence with no operational upside.
   *
   * <p>Initial capacity sized for ~4096 active accounts (matches the order-book pool magnitude —
   * most deployments will be far smaller). Sentinel uses {@link #ACCOUNT_RATE_STATE_MISSING}
   * (Long.MIN_VALUE) to be unambiguously distinct from any valid packed value.
   */
  static final long ACCOUNT_RATE_STATE_MISSING = Long.MIN_VALUE;

  private static final int ACCOUNT_RATE_STATE_INITIAL_CAPACITY = 4096;

  private static final long NANOS_PER_SECOND = 1_000_000_000L;

  /** Mask to extract the 32-bit count from the packed rate-state value. */
  private static final long RATE_COUNT_MASK = 0xFFFF_FFFFL;

  final Long2LongHashMap accountRateState =
      new Long2LongHashMap(ACCOUNT_RATE_STATE_INITIAL_CAPACITY, 0.65f, ACCOUNT_RATE_STATE_MISSING);

  /**
   * Per-account daily admitted-volume state for APP-62 slice 3 ({@code maxDailyVolume} check). Key
   * = accountId. Value = packed long: upper 16 bits hold the UTC day bucket ({@code
   * clusterTimestamp / NANOS_PER_DAY}, 65 536 days ≈ year 2149 — sufficient lifetime), lower 48
   * bits hold the cumulative admitted qty in fixed-point 10⁻⁸ (max ~2.8×10¹⁴ fixed-point units ≈
   * 2.8 million base units — sufficient for any realistic single-account daily cap including
   * institutional FX where 100M-notional days are common).
   *
   * <p>Phase-1 semantics: tracks ADMITTED quantity (orders that passed all pre-trade checks), not
   * FILLED. This is the conservative direction — an admitted-but-unfilled order still represents
   * risk capacity consumed against the limit, so a runaway algorithm is bounded even if its orders
   * never trade. A future slice 4 may refine to track fills (deduct on cancel / expire) if
   * operations wants a true position-based check.
   *
   * <p>NOT persisted in snapshots in this slice — restart resets the day's accumulator. Worst- case
   * over-allowance per cluster restart is {@code maxDailyVolume}, which is acceptable for a circuit
   * breaker (not a regulatory quota).
   */
  static final long ACCOUNT_DAILY_VOLUME_MISSING = Long.MIN_VALUE;

  private static final int ACCOUNT_DAILY_VOLUME_INITIAL_CAPACITY = 4096;

  /** Nanoseconds in a UTC day. */
  static final long NANOS_PER_DAY = 86_400L * NANOS_PER_SECOND;

  /** Mask to extract the 48-bit cumulative-qty from the packed daily-volume entry. */
  static final long DAILY_VOLUME_QTY_MASK = 0x0000_FFFF_FFFF_FFFFL;

  /** Bit-shift for the 16-bit day-bucket field in the packed daily-volume entry. */
  static final int DAILY_VOLUME_DAY_SHIFT = 48;

  /**
   * Max cumulative-qty value representable in the lower 48 bits ({@code 2⁴⁸ − 1} ≈ 2.8×10¹⁴
   * fixed-point units, or ~2.8 million base units at scale 10⁻⁸). Any admission that would push the
   * cumulative beyond this saturates and rejects, defending against pathological inputs that bypass
   * {@link RiskLimitState#maxDailyVolume} via cumulative overflow (e.g., {@code maxDailyVolume == 0
   * == unlimited} configurations with adversarial sequencing). The 48-bit allocation was chosen
   * (over the original 40-bit) to comfortably cover institutional FX day-cap sizes (100M-notional+
   * desks) without overflow.
   */
  static final long DAILY_VOLUME_QTY_SATURATION = DAILY_VOLUME_QTY_MASK;

  final Long2LongHashMap accountDailyVolumeState =
      new Long2LongHashMap(
          ACCOUNT_DAILY_VOLUME_INITIAL_CAPACITY, 0.65f, ACCOUNT_DAILY_VOLUME_MISSING);

  // ===========================================================================
  // APP-62 §4 — per-(account, symbol) WORKING long / short position state
  //
  // The position-limit check (11e) bounds each account's maximum simultaneous
  // working buy quantity and maximum working sell quantity per symbol, separately
  // (CME PTRM Long-Qty / Short-Qty convention; matches plan §3.1 / §3.3).
  //
  // Storage: outer Long2ObjectHashMap keyed by accountId → inner Long2LongHashMap
  // keyed by packed symbolHash → working quantity (fixed-point 10⁻⁸).
  // The outer map allocates an inner map exactly once per account on first
  // admission against that account's symbol; subsequent admissions reuse it.
  // First-touch is per-account-per-symbol, not the order-matching hot path.
  //
  // Sentinel: 0L missing-value (no working quantity for that (account, symbol)).
  // The hot path uses primitive long get/put; no boxing.
  //
  // Inner maps are sized to 64 entries × 0.55 load factor (Agrona default) =
  // ~35 symbols per account before rehashing. Adequate for typical FX desks.
  // ===========================================================================

  /** Initial inner-map capacity for per-account working position scratch. */
  private static final int WORKING_POSITION_INNER_INITIAL_CAPACITY = 64;

  /** Sentinel: missing means "no working long quantity recorded for (account, symbol)." */
  static final long WORKING_POSITION_MISSING = 0L;

  /** Per-(account, symbol) working LONG quantity in fixed-point 10⁻⁸ (APP-62 §4). */
  final Long2ObjectHashMap<Long2LongHashMap> accountSymbolWorkingLong =
      new Long2ObjectHashMap<>(1024, 0.65f);

  /** Per-(account, symbol) working SHORT quantity in fixed-point 10⁻⁸ (APP-62 §4). */
  final Long2ObjectHashMap<Long2LongHashMap> accountSymbolWorkingShort =
      new Long2ObjectHashMap<>(1024, 0.65f);

  // ===========================================================================
  // APP-62 §5 — per-symbol fat-finger reference price cache + staleness clock
  //
  // Check 11f rejects limit orders whose price deviates from the cluster's
  // last known mid-quote for the symbol by more than priceDeviationBps. Industry
  // standard per CME PTRM price-band convention, adapted to FIX 4.4 (tag 103 = 99).
  //
  // Reference source: PriceResponse (template 51) flows through
  // TradingClusteredService.commandHandlers[51] (Raft-replicated, deterministic).
  // Updates land via updateLastQuotedMid; admit path looks up via the cache.
  //
  // Sentinels:
  //   - lastQuotedMidPrice missing = Long.MIN_VALUE (distinct from 0L which is
  //     the legitimate zero-price for market orders).
  //   - lastQuotedMidAsOfNanos missing = 0L (cluster ts is positive epoch-nanos).
  //
  // Staleness window: 5 min default. A reference older than this OR with
  // lastTs > clusterTimestamp (replay edge case) is treated as "no reference";
  // RiskLimit.fatFingerFailClosed then decides reject vs admit.
  // ===========================================================================

  /** Sentinel: no last-quoted-mid recorded for this symbol. */
  static final long LAST_PRICE_MISSING = Long.MIN_VALUE;

  /** Sentinel: no timestamp recorded. Cluster ts is positive epoch-nanos so 0 is unambiguous. */
  static final long LAST_PRICE_TIMESTAMP_MISSING = 0L;

  /**
   * Staleness window for the last-quoted-mid reference. A reference older than this is treated as
   * "no reference" by check 11f. Set to 5 minutes per plan §3.2 default.
   *
   * <p>TODO(APP-62): promote to {@code LauncherConfig.riskEngine.lastPriceStalenessNanos} so ops
   * can tune the window without a redeploy. Left as a constant here so the first §5 fat-finger
   * slice could land without entangling LauncherConfig plumbing.
   */
  static final long LAST_PRICE_STALENESS_NANOS = 5L * 60L * 1_000_000_000L;

  /**
   * Upper-bound guard on PriceResponse mid input. Prices above this are skipped as nonsense
   * (defends against pricing-service malfunctions that could otherwise poison the fat-finger
   * reference). Fixed-point 10⁻⁸; corresponds to ~10¹⁰ raw price units.
   *
   * <p>TODO(APP-62): promote to {@code LauncherConfig.riskEngine.maxReasonablePrice} alongside the
   * staleness knob above.
   */
  static final long MAX_REASONABLE_PRICE = 1_000_000_000_000_000_000L;

  /** Per-symbol last-quoted mid price (fixed-point 10⁻⁸). */
  final Long2LongHashMap lastQuotedMidPrice = new Long2LongHashMap(1024, 0.65f, LAST_PRICE_MISSING);

  /** Per-symbol cluster timestamp when the last mid was recorded. */
  final Long2LongHashMap lastQuotedMidAsOfNanos =
      new Long2LongHashMap(1024, 0.65f, LAST_PRICE_TIMESTAMP_MISSING);

  // ===========================================================================
  // Session → orderKey tracking (APP-151 phase 1 — session-disconnect orphan cancel)
  //
  // Maps cluster session id (the Aeron {@code ClientSession#id()}) to the set of
  // outstanding orderKeys placed by that session. Populated on every successful
  // {@link #admitNewOrder}; iterated on {@link #onSessionClose} to emit one
  // {@code OrderCanceledEvent} per orphan order before the session goes away.
  //
  // Phase-1 scope: in-memory only. NOT snapshot-persisted. Rationale:
  //
  //   - On cluster restart, Aeron Cluster re-invokes onSessionOpen for each
  //     surviving session as part of snapshot load; the handler's
  //     onSessionOpen rebuilds an empty per-session set. The OrderBook IS
  //     snapshotted, so orders that crossed the restart retain their book
  //     slot — but the (session → orderKey) association is not in the
  //     snapshot, so those orders effectively lose the
  //     auto-cancel-on-disconnect property after restart.
  //   - DETERMINISM: Aeron Cluster takes snapshots at the same log position
  //     on every Raft replica (atomic across the cluster); every replica
  //     therefore rebuilds the same empty tracker on restore. The cancel
  //     emission count for a subsequent onSessionClose is identical across
  //     replicas, so the in-memory-only design does NOT break replay
  //     determinism. If snapshots were taken asynchronously per node, this
  //     design would be unsafe — but Aeron's Raft contract rules that out.
  //   - APP-151 phase 4 — {@link #sessionLastActivityNanos} (per-session idle
  //     clock) is ALSO in-memory only, NOT snapshot-persisted. On restore the
  //     activity map is empty; the FIRST onSessionMessage post-restore reseeds
  //     each surviving session's clock via {@link #recordSessionActivity}. An
  //     order that crossed a snapshot whose session never sends another command
  //     keeps its book slot indefinitely — neither the session-close nor the
  //     idle-timeout path will fire — until the future APP-153 admin-force
  //     cancel path lands. This is the same trade-off as the session→order
  //     tracker above; documented here so a future maintainer doesn't add a
  //     snapshot field thinking it closes a leak. Determinism is unaffected
  //     (Aeron atomic snapshots ⇒ every replica restores the same empty maps).
  //   - Worst-case impact of NOT snapshotting: orders that crossed a restart
  //     keep their book slot but lose the auto-cancel-on-disconnect AND the
  //     auto-cancel-on-idle properties until APP-153 admin cancel ships.
  //     This is strictly better than the pre-APP-151 behaviour (no
  //     auto-cancel at all) and acceptable for the current phase.
  //
  // Sizing: outer {@link Long2ObjectHashMap} initial capacity 4096. At load
  // factor 0.65 the map rehashes before reaching 4096 entries (around 2660
  // live sessions); 4096 is the initial-capacity HINT chosen to avoid rehash
  // thrash on typical deployments, NOT a hard cap. The map grows
  // automatically if a deployment legitimately sustains more concurrent
  // sessions. Backing arrays at this capacity are ~64 KB; per-session sets
  // are NOT pre-allocated per slot. Inner {@link LongHashSet} instances are
  // created lazily in {@link #onSessionOpen} (cold path) and released on
  // {@link #onSessionClose}, so steady-state memory is roughly (live
  // sessions) × (~1 KB per set with 64-entry capacity). The set auto-grows
  // if a session genuinely sustains more than 64 live orders.
  //
  // Untrack-on-terminal-event is NOT implemented in phase 1: explicit cancels,
  // fills, and rejects in later phases will remove keys from the per-session
  // set. Until that lands, the set grows monotonically across a session's
  // lifetime (bounded by total orders ever placed by that session) and the
  // {@code onSessionClose} scan walks all keys (the lookup-miss branch
  // silently skips already-released slots). TODO(APP-151): untrack on
  // terminal events (fill/cancel/reject) when phase 2 lands.
  // ===========================================================================

  /**
   * Initial capacity shared by both per-session maps ({@code sessionOrders} and {@code
   * sessionLastActivityNanos}) — both key by Aeron cluster session id, so one constant sizes both
   * coherently. 4096 covers Artio's default upper bound on concurrent sessions; the maps auto-grow
   * if a deployment legitimately sustains more.
   */
  private static final int SESSION_MAP_INITIAL_CAPACITY = 4096;

  /** Initial per-session order-set capacity. Grows automatically if exceeded. */
  static final int SESSION_ORDERS_PER_SESSION_CAPACITY = 64;

  /**
   * Hard cap on outstanding orders per session — defends against unbounded growth of the per-
   * session {@link LongHashSet} (Gemini HIGH from PR #82). At cap reached, new orders from that
   * session are rejected with {@link RejectReasonEnum#BookFull} and a descriptive text. Phase 1 has
   * no terminal-event untrack path, so the set grows monotonically across a session's lifetime;
   * this cap bounds the worst-case {@link #onSessionClose} iteration cost on the cluster duty-cycle
   * thread.
   *
   * <p>16 384 is ~16× typical exchange-grade session throughput (CME iLink caps clients around
   * 1000–2000 orders/second; 4 s of unfilled sustained traffic = 8 000 outstanding). Beyond this
   * limit is pathological. TODO(APP-151): phase 4+ adds terminal-event untrack (fill / explicit
   * cancel / expire) which lowers the steady-state size dramatically, and chunked deferred
   * cancellation on session close if real workloads ever approach the cap.
   */
  static final int SESSION_ORDERS_HARD_CAP = 16_384;

  /**
   * Load factor for the per-session {@link LongHashSet}. Matches the convention used by the other
   * Agrona maps on this class — keeps probe cost bounded while keeping memory overhead modest. The
   * set manages its own internal missing-value sentinel; no caller-supplied sentinel is needed
   * (Agrona's {@code LongHashSet(int, long)} signature does NOT exist — a {@code long} second arg
   * would silently widen to {@code float} and corrupt the load factor).
   */
  private static final float SESSION_ORDERS_LOAD_FACTOR = 0.65f;

  // ===========================================================================
  // Idle session timeout (APP-151 phase 4)
  //
  // Tracks the cluster timestamp of the last command observed on each open session. The
  // {@link com.trading.engine.cluster.TradingClusteredService} schedules a periodic global timer
  // (default 30 s) that calls {@link #onIdleScan}; the scan walks every entry and emits an
  // {@code OrderCanceledEvent} with {@code cancelReason=IdleTimeout} for every order belonging
  // to a session whose last-activity timestamp is older than {@link
  // #IDLE_SESSION_TIMEOUT_NANOS}. The FIX session itself is NOT closed (Artio owns FIX
  // lifecycle); only its outstanding orders are cancelled. A subsequent command on the same
  // session resets the activity timer naturally via {@link #recordSessionActivity}.
  // ===========================================================================

  /** Sentinel for "no last-activity timestamp recorded" on {@link #sessionLastActivityNanos}. */
  static final long IDLE_LAST_ACTIVITY_MISSING = Long.MIN_VALUE;

  /**
   * Default idle threshold: 5 minutes. A session that goes longer than this between commands has
   * its outstanding orders cancelled with {@link CancelReasonEnum#IdleTimeout}. The actual
   * threshold is supplied per-call by {@link
   * com.trading.engine.cluster.TradingClusteredService#onTimerEvent} so tests can override.
   */
  public static final long IDLE_SESSION_TIMEOUT_NANOS = 5L * 60L * 1_000_000_000L;

  /**
   * sessionId → cluster-timestamp of the most-recent command observed on that session. Updated by
   * {@link #recordSessionActivity}; cleared in {@link #onSessionClose} and after an idle scan
   * cancels the session's orders. Package-private for direct-state assertions in unit tests.
   */
  final Long2LongHashMap sessionLastActivityNanos =
      new Long2LongHashMap(
          SESSION_MAP_INITIAL_CAPACITY, SESSION_ORDERS_LOAD_FACTOR, IDLE_LAST_ACTIVITY_MISSING);

  /**
   * Pending-removal scratch — sessions identified as idle during a scan are collected here and
   * removed from {@link #sessionLastActivityNanos} after {@link Long2LongHashMap#forEachLong}
   * returns. Cannot mutate the map mid-iteration (probing/rehash breaks); collecting into this
   * pre-allocated set is the standard Agrona idiom.
   */
  private final LongHashSet idleScanPendingRemoval =
      new LongHashSet(SESSION_MAP_INITIAL_CAPACITY, SESSION_ORDERS_LOAD_FACTOR);

  // Per-scan scratch context for {@link #idleScanConsumer} — set by {@link #onIdleScan} just
  // before {@code forEachLong}, read by the consumer body {@link #idleScanVisit}, reset by the
  // try/finally in {@code onIdleScan}. Safe to be plain mutable fields because the cluster duty
  // cycle is single-threaded — no other code path observes them outside the scan window.
  // Same idiom as {@code EventSink.broadcastBuffer} (one-call mutable context for a final-field
  // consumer that runs synchronously inside the enclosing method).
  private long idleScanScratchCurrentTs;
  private long idleScanScratchThresholdTs;
  private EventSink idleScanScratchEventSink;

  /**
   * Pre-allocated forEach consumer for {@link #sessionLastActivityNanos}. Method-reference form
   * binds once at construction so the inner {@code forEach} adds no per-call lambda allocation.
   */
  private final LongLongConsumer idleScanConsumer = this::idleScanVisit;

  // ===========================================================================
  // Per-session metrics (APP-151 phase 5)
  //
  // Four per-session counters tracked by sessionId. Surfaced via GFLog on
  // {@link #onSessionClose} so operators get a one-line summary of every closed
  // session. NOT exposed via Aeron cluster counters in this slice (would couple
  // to deployment-time counter id allocation); GFLog is sufficient for the
  // observability use case and matches the project's hot-path logging idiom.
  //
  // Counters are stored in separate {@link Long2LongHashMap}s rather than a
  // pooled value class so reads/writes are single-field-update primitive ops.
  // Memory overhead at 4096-session capacity: ~64 KB per counter × 5 = ~320 KB
  // — same magnitude as the existing sessionOrders + sessionLastActivityNanos
  // maps, well within the cluster's memory budget.
  //
  // NOT snapshot-persisted — consistent with the other per-session state; a
  // session that crosses a snapshot loses its counter history. Acceptable
  // because metrics are observability, not state-machine semantics.
  // ===========================================================================

  /** Sentinel for counter "no entry" (real counters are always non-negative). */
  static final long METRIC_MISSING = Long.MIN_VALUE;

  /** sessionId → count of successful NOS admissions for that session. */
  final Long2LongHashMap sessionMetricOrdersAdmitted =
      new Long2LongHashMap(
          SESSION_MAP_INITIAL_CAPACITY, SESSION_ORDERS_LOAD_FACTOR, METRIC_MISSING);

  /** sessionId → count of NOS rejections (any RejectReasonEnum) for that session. */
  final Long2LongHashMap sessionMetricOrdersRejected =
      new Long2LongHashMap(
          SESSION_MAP_INITIAL_CAPACITY, SESSION_ORDERS_LOAD_FACTOR, METRIC_MISSING);

  /** sessionId → count of orders auto-cancelled on session disconnect for that session. */
  final Long2LongHashMap sessionMetricOrdersCancelledOnDisconnect =
      new Long2LongHashMap(
          SESSION_MAP_INITIAL_CAPACITY, SESSION_ORDERS_LOAD_FACTOR, METRIC_MISSING);

  /** sessionId → count of orders auto-cancelled on idle timeout for that session. */
  final Long2LongHashMap sessionMetricOrdersCancelledOnIdleTimeout =
      new Long2LongHashMap(
          SESSION_MAP_INITIAL_CAPACITY, SESSION_ORDERS_LOAD_FACTOR, METRIC_MISSING);

  /**
   * sessionId → count of QuoteRequest commands accepted from that session (APP-151 phase 5 —
   * completes the AC's "quote requests" counter). Incremented from {@link
   * QuoteRequestHandler#onCommand} via {@link #recordQuoteRequest}. NOT shared with the
   * RFQ-internal {@code RfqMetrics} (which are global, not per-session).
   */
  final Long2LongHashMap sessionMetricQuoteRequests =
      new Long2LongHashMap(
          SESSION_MAP_INITIAL_CAPACITY, SESSION_ORDERS_LOAD_FACTOR, METRIC_MISSING);

  /**
   * sessionId → set of orderKeys outstanding on that session. Package-private for direct-state
   * assertions in {@code NewOrderSingleHandlerSessionCloseTest}.
   */
  final Long2ObjectHashMap<LongHashSet> sessionOrders =
      new Long2ObjectHashMap<>(SESSION_MAP_INITIAL_CAPACITY, SESSION_ORDERS_LOAD_FACTOR);

  /**
   * Cluster timestamp at which {@link #evictExpiredClOrdIds} last ran. Initialised to {@code 0L}
   * (NOT {@link Long#MIN_VALUE}) so the first eviction is not blocked by the interval guard:
   * cluster timestamps are positive epoch nanos (≈ 1.7e18 in 2026), so {@code (clusterTimestamp -
   * 0L)} cleanly exceeds the 60 s interval. {@link Long#MIN_VALUE} would underflow the {@code
   * clusterTimestamp - lastEvictionTimestampNanos} subtraction because {@code 1.7e18 - (-9.2e18)}
   * overflows {@code long}.
   */
  private long lastEvictionTimestampNanos = 0L;

  /**
   * Optional injection from {@link com.trading.engine.cluster.TradingClusteredService} for plan
   * §9.2a quote-acceptance integration. When set, NOS commands carrying {@code
   * ordType=PreviouslyQuoted} and a non-empty quoteId are matched against an active QUOTED RFQ slot
   * via {@link RfqStateMachine#peekByQuoteId}, validated for side / price / qty match, and
   * atomically committed via {@link RfqStateMachine#commitAccept} after all NOS validations pass.
   * Null in tests that exercise the legacy single-leg flow.
   */
  private RfqStateMachine rfqStateMachine;

  /**
   * Cached metrics from the RfqStateMachine for §9.2a reject-path counter increments. Null when
   * {@link #rfqStateMachine} is null.
   */
  private RfqMetrics rfqMetrics;

  /**
   * Scratch field holding the QUOTED slot returned by {@link RfqStateMachine#peekByQuoteId} during
   * the peek phase. Cleared after commit (or on any reject path). Single-threaded duty cycle
   * invariant means this never races.
   */
  private RfqSlot pendingQuoteAcceptSlot;

  /**
   * Creates a NewOrderSingleHandler wired to the given cluster state and reference data stores.
   *
   * @param tradingState the event-sourced order lifecycle state (must not be null)
   * @param accountStore the account reference data store (must not be null)
   * @param currencyStore the currency reference data store (must not be null)
   * @param riskLimitStore the risk limit store (must not be null)
   */
  public NewOrderSingleHandler(
      final TradingState tradingState,
      final AccountStore accountStore,
      final CurrencyStore currencyStore,
      final RiskLimitStore riskLimitStore) {
    this.tradingState = Objects.requireNonNull(tradingState, "tradingState");
    this.accountStore = Objects.requireNonNull(accountStore, "accountStore");
    this.currencyStore = Objects.requireNonNull(currencyStore, "currencyStore");
    this.riskLimitStore = Objects.requireNonNull(riskLimitStore, "riskLimitStore");
  }

  /**
   * Optional: wires the RFQ state machine for plan §9.2a quote-acceptance integration. Called by
   * {@link com.trading.engine.cluster.TradingClusteredService} during construction.
   *
   * @param rfqStateMachine the cluster-side RFQ state machine
   * @param rfqMetrics observability counters for the RFQ path
   */
  public void wireRfqStateMachine(
      final RfqStateMachine rfqStateMachine, final RfqMetrics rfqMetrics) {
    this.rfqStateMachine = rfqStateMachine;
    this.rfqMetrics = rfqMetrics;
  }

  /** {@inheritDoc} */
  @Override
  public int commandTemplateId() {
    return NewOrderSingleDecoder.TEMPLATE_ID;
  }

  /**
   * {@inheritDoc}
   *
   * <p>Decodes the NOS command, runs 12 pre-trade checks (plus the §9.2a quote-acceptance peek),
   * and either emits an {@code OrderCreatedEvent} (happy path) or an {@code OrderRejectedEvent}
   * (validation failure).
   */
  @Override
  public void onCommand(
      final ClientSession session,
      final long clusterTimestamp,
      final DirectBuffer buffer,
      final int offset,
      final int length,
      final int blockLength,
      final int version,
      final EventSink eventSink) {

    // 0. Reset the per-call quote-accept scratch in case the previous onCommand left it set
    //    after a validation reject (the slot was never committed but the field could still hold
    //    a stale reference).
    pendingQuoteAcceptSlot = null;

    // 1. Wrap the decoder at the body portion of the SBE message.
    nosDecoder.wrap(buffer, offset + MessageHeaderDecoder.ENCODED_LENGTH, blockLength, version);

    // 2. Extract all fields from the decoded NOS command.
    final long orderQty = nosDecoder.orderQty();
    final var ordType = nosDecoder.ordType();
    final long price = nosDecoder.price();
    final var side = nosDecoder.side();
    final var timeInForce = nosDecoder.timeInForce();

    // APP-62 §D — stash orderQty + price on instance fields so any subsequent emitOrderRejected
    // call can populate the new OrderRejectedEvent audit fields without re-decoding.
    this.stashedOrderQty = orderQty;
    this.stashedPrice = price;

    nosDecoder.getClOrdId(clOrdIdScratch, 0);
    // Trim trailing zeros BEFORE the dedup hash so equivalent ASCII strings ("ABC\0\0..." vs the
    // same "ABC\0..." from a different SBE encoder padding) produce the same dedup key.
    final int clOrdIdLen = trimTrailingZeros(clOrdIdScratch, NewOrderSingleDecoder.clOrdIdLength());
    nosDecoder.getSymbol(symbolScratch, 0);
    final int symbolLen = trimTrailingZeros(symbolScratch, OrderState.SYMBOL_LENGTH);
    nosDecoder.getAccountCode(accountCodeScratch, 0);
    final int accountCodeLen =
        trimTrailingZeros(accountCodeScratch, AccountStore.MAX_ACCOUNT_CODE_LENGTH);

    final byte ccy0 = nosDecoder.currency(0);
    final byte ccy1 = nosDecoder.currency(1);
    final byte ccy2 = nosDecoder.currency(2);
    // Stash on fields so emitOrderRejected can write them into the rejected event —
    // avoids leaking the previous message's currency bytes from the shared egressBuffer.
    currencyByte0 = ccy0;
    currencyByte1 = ccy1;
    currencyByte2 = ccy2;

    // 2a. (APP-206) ClOrdID dedup — per FIX 4.4, ClOrdID must be unique per
    // session within the 24h trading-day window. Reject duplicates before any
    // other validation work runs (cheapest reject path + matches LMAX / CME
    // semantics where a ClOrdID is consumed on first sight regardless of
    // first-attempt outcome).
    final long sessionId = session != null ? session.id() : 0L;
    final long dedupKey = computeClOrdIdDedupKey(sessionId, clOrdIdScratch, 0, clOrdIdLen);
    final long previousSeenNanos = clOrdIdRegistry.get(dedupKey);
    // Short-circuit on CLORDID_DEDUP_MISSING (= Long.MIN_VALUE) BEFORE the subtraction —
    // (clusterTimestamp - Long.MIN_VALUE) overflows, so the missing-key check must precede the
    // window comparison to avoid a false-positive reject on the first submission.
    if (previousSeenNanos != CLORDID_DEDUP_MISSING
        && (clusterTimestamp - previousSeenNanos) < CLORDID_DEDUP_WINDOW_NS) {
      emitOrderRejected(
          eventSink,
          session,
          clusterTimestamp,
          side,
          RejectReasonEnum.DuplicateClOrdId,
          "duplicate ClOrdID within 24h window");
      return;
    }
    // Register (or refresh) the ClOrdID under the cluster timestamp. Hard cap (CodeRabbit PR #81
    // R3): the registry must never grow beyond CLORDID_DEDUP_MAX_SIZE. The previous "watermark
    // only" design allowed unbounded growth when all entries were inside the 24h window — every
    // subsequent eviction walk would then become O(total-live-keys), not O(watermark), and the
    // heap footprint would grow without bound.
    //
    // Two-tier policy on NEW (not refresh) inserts at cap:
    //   1. Try a throttled eviction (at most once per CLORDID_EVICTION_INTERVAL_NS). This is
    //      the death-spiral guard from the prior fix — under sustained at-cap NEW-key churn,
    //      we don't pay the O(N) walk on every single NOS, only on every interval boundary.
    //   2. If after the (possibly skipped) eviction the registry is STILL at cap, fail closed:
    //      reject the new order with BookFull. The 60K cap is a memory bound, not a per-order
    //      limit; if 60K legitimate orders are in flight within 24h, ops should size up
    //      CLORDID_DEDUP_MAX_SIZE rather than silently overflow the registry (but see the
    //      uint16 group-size constraint in the constant Javadoc above).
    if (previousSeenNanos == CLORDID_DEDUP_MISSING
        && clOrdIdRegistry.size() >= CLORDID_DEDUP_MAX_SIZE) {
      if ((clusterTimestamp - lastEvictionTimestampNanos) >= CLORDID_EVICTION_INTERVAL_NS) {
        evictExpiredClOrdIds(clusterTimestamp);
        lastEvictionTimestampNanos = clusterTimestamp;
      }
      if (clOrdIdRegistry.size() >= CLORDID_DEDUP_MAX_SIZE) {
        // Fail closed: eviction freed nothing (or was throttled away) and we cannot grow.
        // Reuse BookFull (FIX semantics: "we're out of slots for this order") with text
        // distinguishing this from the order-book-pool case.
        emitOrderRejected(
            eventSink,
            session,
            clusterTimestamp,
            side,
            RejectReasonEnum.BookFull,
            "ClOrdID dedup registry at capacity (60K within 24h window)");
        return;
      }
    }
    clOrdIdRegistry.put(dedupKey, clusterTimestamp);

    // 3. Validate — returns AccountState on success, null on rejection (already emitted).
    final var account =
        validateNewOrder(
            eventSink,
            session,
            clusterTimestamp,
            side,
            ordType,
            orderQty,
            price,
            symbolLen,
            accountCodeLen,
            ccy0,
            ccy1,
            ccy2);
    if (account == null) {
      // Defensive: if the peek phase (§9.2a step 10) cached a slot but a later check rejected
      // the order, drain the field here so the stale reference cannot leak into the next call.
      // The slot itself remains in QUOTED state (peek is read-only), so the client may retry.
      pendingQuoteAcceptSlot = null;
      return;
    }

    // 4. Happy path — admit the order.
    admitNewOrder(
        eventSink,
        session,
        clusterTimestamp,
        account,
        side,
        ordType,
        timeInForce,
        orderQty,
        price,
        ccy0,
        ccy1,
        ccy2);
  }

  // ===========================================================================
  // Validation — 12 pre-trade checks (plus §9.2a NOS-with-quoteId peek as check 10)
  // ===========================================================================

  /**
   * Runs every pre-trade validation for a decoded NewOrderSingle. On the first failure, emits the
   * corresponding {@code OrderRejectedEvent} via {@link EventSink} and returns {@code null}. On
   * success returns the resolved {@link AccountState} — the caller reuses it for the happy path to
   * avoid a second lookup.
   *
   * @param eventSink the sink for emitting rejection events
   * @param session the client session that sent the command
   * @param timestamp the cluster-assigned timestamp in epoch nanos
   * @param side the order side (FIX tag 54)
   * @param ordType the order type (FIX tag 40)
   * @param orderQty the order quantity (FIX tag 38) in fixed-point 10^-8
   * @param price the order price (FIX tag 44) in fixed-point 10^-8
   * @param symbolLen the trimmed symbol length (0 = empty)
   * @param accountCodeLen the trimmed account code length (0 = empty)
   * @param ccy0 currency byte 0 (FIX tag 15)
   * @param ccy1 currency byte 1
   * @param ccy2 currency byte 2
   * @return the resolved {@link AccountState} on success, or {@code null} if rejected
   */
  private AccountState validateNewOrder(
      final EventSink eventSink,
      final ClientSession session,
      final long timestamp,
      final SideEnum side,
      final OrdTypeEnum ordType,
      final long orderQty,
      final long price,
      final int symbolLen,
      final int accountCodeLen,
      final byte ccy0,
      final byte ccy1,
      final byte ccy2) {

    // 0. (APP-152) Trading-halt circuit breaker. Checked FIRST so a halted cluster wastes
    //    zero CPU on downstream validation. Operator sets via TradingState.setTradingHalted(true)
    //    — the gateway-side admin command path that calls into this is a later slice; today the
    //    state is only flipped via direct setter (tests). On halted: reject with TradingHalted
    //    and skip every other validation step.
    if (tradingState.isTradingHalted()) {
      emitOrderRejected(
          eventSink,
          session,
          timestamp,
          side,
          RejectReasonEnum.TradingHalted,
          "trading halted by operator");
      return null;
    }

    // 1. Symbol must not be empty.
    if (symbolLen == 0) {
      emitOrderRejected(
          eventSink, session, timestamp, side, RejectReasonEnum.UnknownSymbol, "symbol is empty");
      return null;
    }

    // 2. OrderQty must be positive.
    if (orderQty <= 0L) {
      emitOrderRejected(
          eventSink,
          session,
          timestamp,
          side,
          RejectReasonEnum.InvalidQuantity,
          "orderQty must be > 0");
      return null;
    }

    // 3. Limit orders must have positive price.
    if (ordType == OrdTypeEnum.Limit && price <= 0L) {
      emitOrderRejected(
          eventSink,
          session,
          timestamp,
          side,
          RejectReasonEnum.InvalidPrice,
          "limit price must be > 0");
      return null;
    }

    // 4. AccountCode must not be empty.
    if (accountCodeLen == 0) {
      emitOrderRejected(
          eventSink,
          session,
          timestamp,
          side,
          RejectReasonEnum.AccountNotFound,
          "accountCode is empty");
      return null;
    }

    // 5. Account must exist in AccountStore.
    final var account = accountStore.getByCodeBytes(accountCodeScratch, 0, accountCodeLen);
    if (account == null) {
      emitOrderRejected(
          eventSink,
          session,
          timestamp,
          side,
          RejectReasonEnum.AccountNotFound,
          "account not in AccountStore");
      return null;
    }

    // 5a. (APP-62 §E) Fail-closed boot — every account that can trade MUST have a RiskLimitRecord
    //     loaded. Without this guard the cluster would silently fail-open on cold boot or for any
    //     account that wasn't covered by the operator's most recent YAML, letting orders flow with
    //     no maxOrderSize / maxOrderNotional / maxDailyVolume / position / fat-finger gating.
    //     SEC 15c3-5 / MiFID II RTS 6 both treat the no-record case as a pre-trade-risk violation,
    //     not a fail-open default.
    //
    //     Downstream null-guards on `riskLimit != null && ...` remain defensive but are now dead
    //     code on the happy path — kept so a future refactor that moves the lookup earlier doesn't
    //     silently regress the existing checks.
    if (!riskLimitStore.contains(account.accountId())) {
      emitOrderRejectedWithBreachContext(
          eventSink,
          session,
          timestamp,
          side,
          RejectReasonEnum.RiskLimitsNotLoaded,
          "no risk-limit record loaded for account",
          RiskCheckEnum.RiskLimitsNotLoaded,
          0L,
          0L);
      return null;
    }

    // 6. Account status must be Active.
    if (account.status() != AccountStatusEnum.Active) {
      emitOrderRejected(
          eventSink,
          session,
          timestamp,
          side,
          RejectReasonEnum.AccountSuspended,
          "account not active");
      return null;
    }

    // 7. Account must have CAN_TRADE permission.
    if (!account.canTrade()) {
      emitOrderRejected(
          eventSink,
          session,
          timestamp,
          side,
          RejectReasonEnum.AccountNoTradePermission,
          "account lacks CAN_TRADE");
      return null;
    }

    // 8. Currency must be 3 uppercase ASCII letters.
    final int ccyPacked = CurrencyStore.packCodeOrInvalid(ccy0, ccy1, ccy2);
    if (ccyPacked == CurrencyStore.INVALID_PACKED_CODE) {
      emitOrderRejected(
          eventSink,
          session,
          timestamp,
          side,
          RejectReasonEnum.InvalidCurrencyCode,
          "currency is not 3 uppercase ASCII letters");
      return null;
    }

    // 9. Currency must exist in CurrencyStore.
    if (!currencyStore.contains(ccyPacked)) {
      emitOrderRejected(
          eventSink,
          session,
          timestamp,
          side,
          RejectReasonEnum.UnknownCurrency,
          "currency not in CurrencyStore");
      return null;
    }

    // 10. (NEW per APP-232 §9.2a) NOS-with-quoteId peek phase — read-only RFQ slot lookup.
    //     Slot is cached in pendingQuoteAcceptSlot; commit happens at the end of admitNewOrder
    //     so a later validation reject (#11/#12) leaves the QUOTED slot intact for client retry.
    pendingQuoteAcceptSlot = null;
    if (rfqStateMachine != null && ordType == OrdTypeEnum.PreviouslyQuoted) {
      nosDecoder.getQuoteId(quoteIdScratch, 0);
      // Defence-in-depth presence check: scan all 20 bytes via trimTrailingZeros (matches the
      // accountCodeLen / symbolLen pattern earlier in this method). A first-byte-nonzero check
      // would let a hostile input with `quoteId="\0..."` bypass §9.2a entirely.
      final int quoteIdLen = trimTrailingZeros(quoteIdScratch, RfqSlot.QUOTE_ID_LENGTH);
      if (quoteIdLen == 0) {
        // FIX protocol: ordType=PreviouslyQuoted REQUIRES a non-empty quoteId. An empty
        // quoteId on this path is a protocol violation — reject rather than silently
        // falling through to the normal-order path (which would let a client execute at
        // their own NOS price without an actual quote on file).
        emitOrderRejected(
            eventSink, session, timestamp, side, RejectReasonEnum.QuoteNotFound, "missing quoteId");
        if (rfqMetrics != null) {
          rfqMetrics.rejectUnknownQuote++;
        }
        return null;
      }
      {
        final var slot = rfqStateMachine.peekByQuoteId(quoteIdScratch, 0, RfqSlot.QUOTE_ID_LENGTH);
        if (slot == null) {
          emitOrderRejected(
              eventSink, session, timestamp, side, RejectReasonEnum.QuoteNotFound, "unknown quote");
          if (rfqMetrics != null) {
            rfqMetrics.rejectUnknownQuote++;
          }
          return null;
        }
        // Side mismatch is hard reject (no tolerance).
        if (slot.side != (byte) side.value()) {
          emitOrderRejected(
              eventSink, session, timestamp, side, RejectReasonEnum.QuoteNotFound, "side mismatch");
          if (rfqMetrics != null) {
            rfqMetrics.rejectQuoteSideMismatch++;
          }
          return null;
        }
        // Price tolerance (bps).
        final long quotedPx = side == SideEnum.Buy ? slot.offerPx : slot.bidPx;
        final long quotedSize = side == SideEnum.Buy ? slot.offerSize : slot.bidSize;
        // One-sided quote: missing price for the requested side. A trader hitting the
        // missing side (e.g., Buy against a bid-only quote) MUST be rejected — accepting
        // would let the trader execute at their own NOS price with no firm offer.
        if (quotedPx <= 0L) {
          emitOrderRejected(
              eventSink,
              session,
              timestamp,
              side,
              RejectReasonEnum.QuoteNotFound,
              "quote side missing");
          if (rfqMetrics != null) {
            rfqMetrics.rejectUnknownQuote++;
          }
          return null;
        }
        // Overflow guards:
        //   1) `price - quotedPx` can equal Long.MIN_VALUE on hostile input; Math.abs of
        //      that is still Long.MIN_VALUE (negative). Treat negative pxDelta as
        //      saturation → hard reject.
        //   2) `pxDelta * 10_000L` can overflow long when pxDelta is large; guard by
        //      saturating to Long.MAX_VALUE.
        final long pxDelta = Math.abs(price - quotedPx);
        final long pxDeltaBps =
            (pxDelta < 0L || pxDelta > Long.MAX_VALUE / 10_000L)
                ? Long.MAX_VALUE
                : pxDelta * 10_000L / quotedPx;
        if (pxDeltaBps > rfqStateMachine.acceptPriceToleranceBps()) {
          emitOrderRejected(
              eventSink,
              session,
              timestamp,
              side,
              RejectReasonEnum.QuoteNotFound,
              "price mismatch");
          if (rfqMetrics != null) {
            rfqMetrics.rejectQuotePriceMismatch++;
          }
          return null;
        }
        // Qty tolerance (bps). One-sided quote with missing size is also rejected via the
        // same path the price check uses — a quote with bidPx>0 but bidSize=0 is malformed.
        if (quotedSize <= 0L) {
          emitOrderRejected(
              eventSink,
              session,
              timestamp,
              side,
              RejectReasonEnum.QuoteNotFound,
              "quote size missing");
          if (rfqMetrics != null) {
            rfqMetrics.rejectUnknownQuote++;
          }
          return null;
        }
        // Overflow guards: see price-bps above. quotedSize > 0 guaranteed by the
        // missing-size reject above.
        final long qtyDelta = Math.abs(orderQty - quotedSize);
        final long qtyDeltaBps =
            (qtyDelta < 0L || qtyDelta > Long.MAX_VALUE / 10_000L)
                ? Long.MAX_VALUE
                : qtyDelta * 10_000L / quotedSize;
        if (qtyDeltaBps > rfqStateMachine.acceptQtyToleranceBps()) {
          emitOrderRejected(
              eventSink, session, timestamp, side, RejectReasonEnum.QuoteNotFound, "qty mismatch");
          if (rfqMetrics != null) {
            rfqMetrics.rejectQuoteQtyMismatch++;
          }
          return null;
        }
        pendingQuoteAcceptSlot = slot;
      }
    }

    // 11. OrderQty must not exceed account maxOrderSize risk limit.
    final var riskLimit = riskLimitStore.get(account.accountId());
    if (riskLimit != null && riskLimit.maxOrderSize() > 0L && orderQty > riskLimit.maxOrderSize()) {
      emitOrderRejected(
          eventSink,
          session,
          timestamp,
          side,
          RejectReasonEnum.OrderExceedsMaxSize,
          "orderQty exceeds account maxOrderSize");
      return null;
    }

    // 11b. (APP-62 first slice) OrderNotional (price × qty / PRICE_SCALE) must not exceed account
    //      maxOrderNotional risk limit. Skipped for Market orders (price=0) — notional cannot be
    //      computed at order entry and is bounded by maxOrderSize × prevailing price. Skipped
    //      when the account has no notional limit configured (maxOrderNotional == 0 == unlimited).
    //
    //      Overflow guard: orderQty × price can overflow long for large values
    //      (qty=1e8 * price=1e8 / 1e8 = 1e8 result — fine — but qty=1e10 * price=1e10 = 1e20
    //      overflows). Use Math.multiplyHigh + low-bits to detect overflow without floating-
    //      point; on overflow, treat the order as exceeding the limit and reject.
    if (riskLimit != null
        && riskLimit.maxOrderNotional() > 0L
        && ordType == OrdTypeEnum.Limit
        && price > 0L) {
      final long notional = computeNotionalSaturating(orderQty, price);
      if (notional > riskLimit.maxOrderNotional()) {
        emitOrderRejected(
            eventSink,
            session,
            timestamp,
            side,
            RejectReasonEnum.OrderExceedsMaxSize,
            "orderNotional exceeds account maxOrderNotional");
        return null;
      }
    }

    // 12. Order book must not be full — checked BEFORE generating IDs to avoid wasting
    //     deterministic counter space on an order that cannot be admitted. Also hoisted ahead of
    //     the rate-limit and daily-volume checks (11c, 11d) so a book-full reject does not
    //     consume rate-token or daily-volume capacity for an order that could never have been
    //     admitted anyway.
    if (tradingState.isOrderBookFull()) {
      emitOrderRejected(
          eventSink,
          session,
          timestamp,
          side,
          RejectReasonEnum.BookFull,
          "order book pool exhausted");
      return null;
    }

    // 12a. (APP-151 phase 3) Per-session order cap — bound the worst-case onSessionClose
    //      iteration on the duty cycle (Gemini HIGH from PR #82). Without a terminal-event
    //      untrack path (phase 4+), sessionOrders grows monotonically until session-close. A
    //      pathological session that admits >SESSION_ORDERS_HARD_CAP orders without any
    //      cancel/fill would force a multi-hundred-millisecond iteration on duty cycle,
    //      blocking the cluster heartbeat. Fail-closed at the cap with BookFull keeps the
    //      worst case bounded. Skipped on the null-session test path.
    if (session != null) {
      final var sessionSet = sessionOrders.get(session.id());
      if (sessionSet != null && sessionSet.size() >= SESSION_ORDERS_HARD_CAP) {
        emitOrderRejected(
            eventSink,
            session,
            timestamp,
            side,
            RejectReasonEnum.BookFull,
            "session order cap exceeded");
        return null;
      }
    }

    // 11c. (APP-62 slice 2) Per-account rate limit — at most maxOrdersPerSecond NewOrderSingle
    //      admissions per 1-second wall-clock-aligned window. Skipped when the account has no
    //      rate limit configured (maxOrdersPerSecond == 0 == unlimited).
    //
    //      The window aligns to absolute epoch seconds (cluster timestamp / 1e9), not a
    //      rolling window from first admission. This is deterministic (purely a function of
    //      the cluster-supplied timestamp), simple to reason about, and avoids the unbounded
    //      memory growth a true sliding window would require.
    //
    //      A rejected order does NOT consume rate-limit capacity (the check fires only on the
    //      pass path here; once admitted, the count increments). This matches the "rate of
    //      successful submissions" semantics most ops teams expect.
    if (riskLimit != null
        && riskLimit.maxOrdersPerSecond() > 0L
        && !tryConsumeRateToken(account.accountId(), riskLimit.maxOrdersPerSecond(), timestamp)) {
      emitOrderRejected(
          eventSink,
          session,
          timestamp,
          side,
          RejectReasonEnum.RateLimitExceeded,
          "account rate limit exceeded for current 1s window");
      return null;
    }

    // 11d. (APP-62 slice 3) Per-account daily-volume limit — cumulative admitted qty across
    //      the current UTC day must not exceed maxDailyVolume. Skipped when the account has
    //      no daily-volume limit configured (maxDailyVolume == 0 == unlimited).
    //
    //      Day alignment: bucket = clusterTimestamp / NANOS_PER_DAY (epoch days, UTC). Aligned
    //      to absolute epoch days for determinism. A day rollover resets the counter to the
    //      incoming order's qty.
    //
    //      Phase-1 semantics: tracks ADMITTED qty (orders that reached this check), not FILLED.
    //      Conservative — admitted-but-unfilled orders still represent risk capacity consumed
    //      against the limit. A future slice 4 may refine to deduct on cancel / expire.
    //
    //      A rejected order does NOT consume daily-volume capacity (check fires here; the
    //      accumulator increments only on the pass path).
    if (riskLimit != null
        && riskLimit.maxDailyVolume() > 0L
        && !tryConsumeDailyVolume(
            account.accountId(), riskLimit.maxDailyVolume(), orderQty, timestamp)) {
      emitOrderRejected(
          eventSink,
          session,
          timestamp,
          side,
          RejectReasonEnum.DailyVolumeExceeded,
          "order would exceed account maxDailyVolume for current UTC day");
      return null;
    }

    // 11e. (APP-62 §4) Per-(account, symbol) position limit — worst-case fill exposure check.
    //      Working LONG and working SHORT quantities are bounded independently against
    //      RiskLimit.maxLongPosition / maxShortPosition (CME PTRM Long-Qty / Short-Qty convention).
    //      The check gates only when positionLimitEnabled = true on the loaded risk record; the
    //      explicit boolean removes the "0 means disabled" ambiguity. Saturation overflow on the
    //      projected sum is treated as a breach via the strict `>` comparison.
    if (riskLimit != null && riskLimit.positionLimitEnabled()) {
      // Primitive locals intentionally bare (no `final`) per memory rule
      // feedback_final_primitives_autoboxing.md — `final` on primitives can hide autoboxing.
      long symbolHash = packSymbolKey(symbolScratch, 0);
      long currentLong = workingLongFor(account.accountId(), symbolHash);
      long currentShort = workingShortFor(account.accountId(), symbolHash);
      long projectedLong = side == SideEnum.Buy ? safeAdd(currentLong, orderQty) : currentLong;
      long projectedShort = side == SideEnum.Sell ? safeAdd(currentShort, orderQty) : currentShort;
      if (projectedLong > riskLimit.maxLongPosition()) {
        emitOrderRejectedWithBreachContext(
            eventSink,
            session,
            timestamp,
            side,
            RejectReasonEnum.PositionLimitExceeded,
            "projected working long position would exceed maxLongPosition",
            RiskCheckEnum.PositionLimit,
            riskLimit.maxLongPosition(),
            projectedLong);
        return null;
      }
      if (projectedShort > riskLimit.maxShortPosition()) {
        emitOrderRejectedWithBreachContext(
            eventSink,
            session,
            timestamp,
            side,
            RejectReasonEnum.PositionLimitExceeded,
            "projected working short position would exceed maxShortPosition",
            RiskCheckEnum.PositionLimit,
            riskLimit.maxShortPosition(),
            projectedShort);
        return null;
      }
    }

    // 11f. (APP-62 §5) Fat-finger — limit-priced orders are gated against the cluster's last
    //      known mid for the symbol. Tolerance is the per-account priceDeviationBps; if the
    //      account opted into fatFingerFailClosed (industry-standard default), missing or stale
    //      references cause a reject so the operator must publish a fresh quote before any
    //      limit can land. Market orders skip the check (no price to band).
    //
    //      Reference-price source: PriceResponse from the pricing service, replicated via Raft.
    //      Replay-safe staleness check guards against lastTs > clusterTimestamp (snapshot replay
    //      edge case) by treating that as "no reference".
    //
    //      Arithmetic: deviationBps = |price - lastMid| * 10_000 / lastMid. We use
    //      Math.unsignedMultiplyHigh to detect overflow in the (delta × 10_000) multiplication;
    //      a non-zero high word treats the projected deviation as Long.MAX_VALUE and rejects.
    if (riskLimit != null
        && riskLimit.fatFingerEnabled()
        && (ordType == OrdTypeEnum.Limit || ordType == OrdTypeEnum.PreviouslyQuoted)) {
      long symbolHash = packSymbolKey(symbolScratch, 0);
      long lastMid = lastQuotedMidPrice.get(symbolHash);
      long lastTs = lastQuotedMidAsOfNanos.get(symbolHash);
      // Reference is usable only if both maps have an entry AND the timestamp is in the past or
      // present relative to clusterTimestamp AND within the staleness window. The replay guard
      // (lastTs <= clusterTimestamp) preserves determinism — under replay the cache could be
      // restored from a snapshot taken after the current log position.
      boolean haveReference =
          lastMid != LAST_PRICE_MISSING
              && lastTs != LAST_PRICE_TIMESTAMP_MISSING
              && lastTs <= timestamp
              && (timestamp - lastTs) <= LAST_PRICE_STALENESS_NANOS;
      if (!haveReference) {
        if (riskLimit.fatFingerFailClosed()) {
          emitOrderRejectedWithBreachContext(
              eventSink,
              session,
              timestamp,
              side,
              RejectReasonEnum.PriceTooFarFromMarket,
              "no fat-finger reference price for symbol",
              RiskCheckEnum.FatFinger,
              riskLimit.priceDeviationBps(),
              0L);
          return null;
        }
        // fail-open mode: skip the check — operator-acknowledged risk
      } else {
        long delta = Math.abs(price - lastMid);
        // Overflow guard on (delta × 10_000): the high 64 bits of the unsigned product. Non-zero
        // means the product exceeds 2⁶³; treat as a runaway breach.
        long deltaHigh = Math.unsignedMultiplyHigh(delta, 10_000L);
        long deviationBps;
        if (deltaHigh != 0L) {
          deviationBps = Long.MAX_VALUE;
        } else {
          deviationBps = (delta * 10_000L) / lastMid;
        }
        if (deviationBps > riskLimit.priceDeviationBps()) {
          emitOrderRejectedWithBreachContext(
              eventSink,
              session,
              timestamp,
              side,
              RejectReasonEnum.PriceTooFarFromMarket,
              "price deviates from last quoted mid by more than priceDeviationBps",
              RiskCheckEnum.FatFinger,
              riskLimit.priceDeviationBps(),
              deviationBps);
          return null;
        }
      }
    }

    return account;
  }

  /**
   * Checks and atomically updates the per-account rate-limit state for the current 1-second window.
   * Returns {@code true} if the admission is within the limit (and increments the counter); {@code
   * false} if the limit was already reached for this window.
   *
   * <p>Window alignment: the current bucket is {@code clusterTimestamp / NANOS_PER_SECOND} (epoch
   * seconds). All admissions sharing the same bucket value contend against the same limit.
   *
   * <p><b>Determinism:</b> output is a pure function of the cluster timestamp + prior state.
   *
   * <p><b>Allocation:</b> none — the packed-long encoding avoids any companion object.
   *
   * @param accountId the validated account id (must already exist in {@link AccountStore})
   * @param limit the per-second admission cap (caller must ensure {@code > 0})
   * @param clusterTimestamp the cluster-assigned timestamp in epoch nanos
   * @return {@code true} if the admission fits within {@code limit} for the current window
   */
  private boolean tryConsumeRateToken(
      final long accountId, final long limit, final long clusterTimestamp) {
    final long bucketSec = clusterTimestamp / NANOS_PER_SECOND;
    final long prev = accountRateState.get(accountId);
    final long prevBucket;
    final long prevCount;
    if (prev == ACCOUNT_RATE_STATE_MISSING) {
      prevBucket = -1L; // sentinel forces window reset below
      prevCount = 0L;
    } else {
      prevBucket = prev >>> 32;
      prevCount = prev & RATE_COUNT_MASK;
    }

    final long nextCount;
    if (bucketSec != prevBucket) {
      // New 1-second window: reset to 1 admission.
      nextCount = 1L;
    } else if (prevCount >= limit) {
      // Limit already reached in this window — reject without mutating state, so a flurry of
      // rejected attempts cannot push the count further into "uint32 overflow" territory.
      return false;
    } else {
      nextCount = prevCount + 1L;
    }
    accountRateState.put(accountId, (bucketSec << 32) | (nextCount & RATE_COUNT_MASK));
    return true;
  }

  /**
   * Checks and atomically updates the per-account daily admitted-volume state for the current UTC
   * day. Returns {@code true} if {@code prevCumulative + orderQty <= limit} (and increments the
   * accumulator); {@code false} if the addition would exceed the limit or overflow the 40-bit
   * storage field.
   *
   * <p>Day alignment: bucket = {@code clusterTimestamp / NANOS_PER_DAY}. A day rollover resets the
   * accumulator to {@code orderQty}.
   *
   * <p><b>Determinism:</b> output is a pure function of the cluster timestamp + prior state.
   *
   * <p><b>Allocation:</b> none — the packed-long encoding avoids any companion object.
   *
   * <p><b>Saturation guard:</b> {@code DAILY_VOLUME_QTY_SATURATION} caps the cumulative at 2⁴⁸−1
   * fixed-point units. Any order that would push the cumulative past saturation rejects; this is
   * defensive against pathological inputs (e.g., {@code maxDailyVolume == 0 == unlimited} paired
   * with adversarial sequencing) and prevents silent bit-truncation when re-packing.
   *
   * @param accountId the validated account id
   * @param limit the daily-volume cap in fixed-point 10⁻⁸ (caller must ensure {@code > 0})
   * @param orderQty the order quantity in fixed-point 10⁻⁸ (caller must ensure {@code > 0})
   * @param clusterTimestamp the cluster-assigned timestamp in epoch nanos
   * @return {@code true} if the admission fits within {@code limit} for the current UTC day
   */
  private boolean tryConsumeDailyVolume(
      final long accountId, final long limit, final long orderQty, final long clusterTimestamp) {
    final long dayBucket = clusterTimestamp / NANOS_PER_DAY;
    final long prev = accountDailyVolumeState.get(accountId);
    final long prevDay;
    final long prevCumulative;
    if (prev == ACCOUNT_DAILY_VOLUME_MISSING) {
      // Sentinel forces a new-day path below; no prior cumulative to merge.
      prevDay = -1L;
      prevCumulative = 0L;
    } else {
      prevDay = prev >>> DAILY_VOLUME_DAY_SHIFT;
      prevCumulative = prev & DAILY_VOLUME_QTY_MASK;
    }

    final long nextCumulative;
    if (dayBucket != prevDay) {
      // New UTC day — reset accumulator to this single admission.
      nextCumulative = orderQty;
    } else {
      // Same day — guard against overflow of long addition AND saturation of the 40-bit
      // storage field. Long overflow is checked first because the limit field itself is a
      // long; relying on the >limit comparison alone could let a wrap-negative sum slip
      // through.
      if (orderQty > Long.MAX_VALUE - prevCumulative) {
        return false;
      }
      nextCumulative = prevCumulative + orderQty;
      if (nextCumulative > DAILY_VOLUME_QTY_SATURATION) {
        return false;
      }
    }
    if (nextCumulative > limit) {
      // Limit exceeded — reject without mutating state so subsequent attempts in the same day
      // are still measured against the original cumulative.
      return false;
    }
    accountDailyVolumeState.put(
        accountId,
        (dayBucket << DAILY_VOLUME_DAY_SHIFT) | (nextCumulative & DAILY_VOLUME_QTY_MASK));
    return true;
  }

  // ===========================================================================
  // Happy path — two-phase event-sourced admission
  // ===========================================================================

  /**
   * Admits a validated NewOrderSingle into the order book. Generates deterministic IDs, encodes and
   * emits the {@code OrderCreatedEvent}, then applies state from the event.
   *
   * <p><b>Phase A:</b> generate order/exec IDs, encode the event, emit via {@link EventSink}.
   *
   * <p><b>Phase B:</b> apply state via {@link TradingState#applyOrderCreated}.
   *
   * @param eventSink the event emission pipeline
   * @param session the client session for egress reply
   * @param timestamp the cluster-assigned timestamp in epoch nanos
   * @param account the validated account
   * @param side the order side (FIX tag 54)
   * @param ordType the order type (FIX tag 40)
   * @param timeInForce the time-in-force (FIX tag 59)
   * @param orderQty the order quantity (FIX tag 38) in fixed-point 10^-8
   * @param price the order price (FIX tag 44) in fixed-point 10^-8
   * @param ccy0 currency byte 0 (FIX tag 15)
   * @param ccy1 currency byte 1
   * @param ccy2 currency byte 2
   */
  private void admitNewOrder(
      final EventSink eventSink,
      final ClientSession session,
      final long timestamp,
      final AccountState account,
      final SideEnum side,
      final OrdTypeEnum ordType,
      final TimeInForceEnum timeInForce,
      final long orderQty,
      final long price,
      final byte ccy0,
      final byte ccy1,
      final byte ccy2) {

    // --- Phase A: generate IDs, encode event, emit ---

    final long orderKey = tradingState.generateOrderId();
    tradingState.generateExecId(); // exec ID bytes available via tradingState.execIdScratch()

    // Encode OrderCreatedEvent with all 19 fields.
    orderCreatedEncoder.wrapAndApplyHeader(egressBuffer, 0, headerEncoder);
    // sequenceNumber + timestamp written here as zero by design: EventSink overwrites both
    // fields with the authoritative cluster sequence + nanosecond timestamp during egress
    // publication (the cluster duty-cycle thread is the single point of monotonic ordering).
    // Writing them here keeps the SBE block layout dense + avoids re-wrap allocation.
    orderCreatedEncoder.sequenceNumber(0L);
    orderCreatedEncoder.timestamp(0L);
    orderCreatedEncoder.putOrderId(tradingState.orderIdScratch(), 0);
    orderCreatedEncoder.putExecId(tradingState.execIdScratch(), 0);
    orderCreatedEncoder.putClOrdId(clOrdIdScratch, 0);
    orderCreatedEncoder.putSymbol(symbolScratch, 0);
    orderCreatedEncoder.side(side);
    orderCreatedEncoder.ordType(ordType);
    orderCreatedEncoder.timeInForce(timeInForce);
    orderCreatedEncoder.price(price);
    orderCreatedEncoder.orderQty(orderQty);

    // QuoteId — copy from the NOS decoder (may be zero-padded if not present).
    nosDecoder.getQuoteId(quoteIdScratch, 0);
    orderCreatedEncoder.putQuoteId(quoteIdScratch, 0);

    orderCreatedEncoder.putAccountCode(accountCodeScratch, 0);
    orderCreatedEncoder.productType(safeProductType());

    // SettlDate — FIX tag 64, 8-byte fixed-length ASCII.
    nosDecoder.getSettlDate(settlDateScratch, 0);
    orderCreatedEncoder.putSettlDate(settlDateScratch, 0);
    orderCreatedEncoder.settlType(safeSettlType());

    // Currency — FIX tag 15, 3-byte fixed-length ASCII.
    orderCreatedEncoder.putCurrency(ccy0, ccy1, ccy2);

    // SettlCurrency — FIX tag 120, 3-byte fixed-length ASCII.
    final byte sc0 = nosDecoder.settlCurrency(0);
    final byte sc1 = nosDecoder.settlCurrency(1);
    final byte sc2 = nosDecoder.settlCurrency(2);
    orderCreatedEncoder.putSettlCurrency(sc0, sc1, sc2);

    orderCreatedEncoder.tenor(safeTenor());

    // Emit via EventSink — stamps seqNo + timestamp, appends to journal, offers to session.
    final int eventLen = MessageHeaderEncoder.ENCODED_LENGTH + orderCreatedEncoder.encodedLength();
    eventSink.emit(timestamp, egressBuffer, 0, eventLen);

    // --- Phase B: apply state derived from the emitted event ---

    final var state =
        tradingState.applyOrderCreated(
            orderKey,
            timestamp,
            tradingState.orderIdScratch(),
            0,
            side,
            ordType,
            timeInForce,
            price,
            orderQty,
            account.accountId(),
            clOrdIdScratch,
            0,
            symbolScratch,
            0,
            safeProductType());

    if (state == null) {
      // The pre-validation guard (isOrderBookFull) should prevent this. If we reach here, the
      // event has already been journaled but state cannot be applied — this is an internal
      // consistency failure. Throwing triggers Aeron Cluster failover, which is the correct
      // recovery action for a deterministic state machine.
      throw new IllegalStateException(
          "Order pool exhausted after event emitted — state machine inconsistency");
    }

    // 13. (NEW per APP-232 §9.2a) Quote-acceptance commit phase. Runs as the LAST step after
    //     OrderCreatedEvent has been journaled successfully. Atomic transition QUOTED→ACCEPTED +
    //     release; never observable in snapshot due to the single-threaded duty-cycle invariant.
    if (pendingQuoteAcceptSlot != null && rfqStateMachine != null) {
      rfqStateMachine.commitAccept(pendingQuoteAcceptSlot, timestamp, eventSink);
      pendingQuoteAcceptSlot = null;
    }

    // 14. (APP-151 phase 1) Track this orderKey under the session that placed it, so a subsequent
    //     hard-disconnect of that session can cancel the order automatically via onSessionClose.
    //     Test paths that pass a null session skip tracking (orphan-cancel only runs against
    //     real cluster sessions; unit tests for the admit path do not depend on this side effect).
    if (session != null) {
      trackSessionOrder(session.id(), orderKey);
      // APP-151 phase 5 — per-session metrics. Increment "orders admitted" counter for this
      // session.
      incrementSessionCounter(sessionMetricOrdersAdmitted, session.id());
    }

    // 15. (APP-62 §4) Apply the admitted order's quantity to the per-(account, symbol) working
    //     position counter. Check 11e in validateNewOrder has already verified the projected
    //     post-admission value does not exceed the configured cap. The increment is performed
    //     only AFTER all admission side-effects so a cancel-revert (see emitOrderCanceledEvent)
    //     can safely subtract the same delta. Symbol bytes are read from the handler's stashed
    //     symbolScratch — already populated during this onCommand pass.
    long symbolHashApp62 = packSymbolKey(symbolScratch, 0);
    applyWorkingPosition(account.accountId(), symbolHashApp62, side, orderQty);
  }

  /**
   * Pre-allocates the per-session {@link LongHashSet} for the given Aeron cluster session id, so
   * subsequent {@link #trackSessionOrder} calls on the admit hot path are guaranteed
   * zero-allocation. Called from {@link
   * com.trading.engine.cluster.TradingClusteredService#onSessionOpen} on every new cluster client
   * session. Idempotent — a re-open with an already-known sessionId is a no-op.
   *
   * <p><b>Why pre-allocate.</b> Allocating the per-session set lazily inside {@code
   * trackSessionOrder} would surface a fresh {@code new LongHashSet(...)} on the FIRST order from
   * any session — which is precisely the (already-latency-sensitive) order-admit hot path. Pulling
   * the alloc forward to session-open (a cold, rare event) is the same pattern Aeron itself uses
   * for per-session state.
   *
   * @param sessionId Aeron cluster session id ({@code ClientSession#id()})
   * @param clusterTimestamp cluster timestamp at session-open — seeds the idle-activity clock so a
   *     freshly-opened session is not immediately considered idle (APP-151 phase 4)
   */
  public void onSessionOpen(final long sessionId, final long clusterTimestamp) {
    if (sessionOrders.get(sessionId) == null) {
      sessionOrders.put(
          sessionId,
          new LongHashSet(SESSION_ORDERS_PER_SESSION_CAPACITY, SESSION_ORDERS_LOAD_FACTOR));
    }
    // APP-151 phase 4 — seed last-activity so a brand-new session is not immediately flagged idle.
    sessionLastActivityNanos.put(sessionId, clusterTimestamp);
    // APP-151 phase 5 — seed per-session counter entries at 0 so onSessionClose's GFLog summary
    // always reports concrete numbers (instead of MISSING) even for sessions that never sent any
    // commands. {@code put} (not {@code putIfAbsent}) is correct here: Aeron Cluster never
    // re-uses a session id (each new cluster connection gets a fresh monotonic id), so the two
    // operations are observationally equivalent on the production path. The seeding-with-put
    // pattern is consistent with the per-session lastActivity seeding above.
    sessionMetricOrdersAdmitted.put(sessionId, 0L);
    sessionMetricOrdersRejected.put(sessionId, 0L);
    sessionMetricOrdersCancelledOnDisconnect.put(sessionId, 0L);
    sessionMetricOrdersCancelledOnIdleTimeout.put(sessionId, 0L);
    sessionMetricQuoteRequests.put(sessionId, 0L);
  }

  /**
   * Records a QuoteRequest command observed for {@code sessionId} (APP-151 phase 5 — completes the
   * AC list "orders submitted, rejections, quote requests, cancel-on-disconnect"). Called from
   * {@link QuoteRequestHandler#onCommand} via the wired {@link SessionMetricsRecorder} seam.
   *
   * @param sessionId Aeron cluster session id observed sending a QuoteRequest
   */
  @Override
  public void recordQuoteRequest(final long sessionId) {
    incrementSessionCounter(sessionMetricQuoteRequests, sessionId);
  }

  /**
   * Increments the counter for {@code sessionId} in {@code metricMap} by 1. If the session has no
   * existing entry (e.g., a test that bypassed {@link #onSessionOpen}), seeds the counter at 1.
   * Zero-allocation: a single {@link Long2LongHashMap#get} + {@code put} pair.
   *
   * @param metricMap the counter map (one of the five {@code sessionMetric*} fields:
   *     ordersAdmitted, ordersRejected, ordersCancelledOnDisconnect, ordersCancelledOnIdleTimeout,
   *     quoteRequests)
   * @param sessionId Aeron cluster session id
   */
  private void incrementSessionCounter(final Long2LongHashMap metricMap, final long sessionId) {
    final long current = metricMap.get(sessionId);
    if (current == METRIC_MISSING) {
      // Production invariant: onSessionOpen seeds every counter at 0; reaching MISSING here means
      // a test path bypassed onSessionOpen. Seed at 1 to keep the count honest.
      metricMap.put(sessionId, 1L);
    } else {
      metricMap.put(sessionId, current + 1L);
    }
  }

  /**
   * Records observed activity on {@code sessionId} (APP-151 phase 4). Called by {@link
   * com.trading.engine.cluster.TradingClusteredService#onSessionMessage} at the top of dispatch
   * BEFORE any handler runs, so every CLIENT-session command — including refdata, halt/resume,
   * trading, and quote-request commands — resets the idle-activity clock for the source session.
   *
   * <p>Note: pricing-service responses arrive on the PRICING service's own cluster session id, so
   * they refresh the pricing-service session's clock — not the originating user session's. Timer
   * events and other internal ingress without a {@code ClientSession} are skipped at the dispatcher
   * level (null-session guard) and never reach this method.
   *
   * <p>Zero-allocation: {@link Long2LongHashMap#put} on existing key replaces in-place without
   * resize for any session that has been through {@link #onSessionOpen}.
   *
   * @param sessionId Aeron cluster session id ({@code ClientSession#id()})
   * @param clusterTimestamp the cluster-assigned timestamp in epoch nanos
   */
  public void recordSessionActivity(final long sessionId, final long clusterTimestamp) {
    sessionLastActivityNanos.put(sessionId, clusterTimestamp);
  }

  /**
   * Removes {@code orderKey} from the per-session set — the inverse of {@link #trackSessionOrder}.
   * Defensive hook for future terminal-event emitters (fill, explicit cancel via APP-65, expire) so
   * they can keep the per-session set bounded by *currently outstanding* orders instead of *all
   * orders ever placed*. Currently UNUSED in production — the cluster has no fill/expire emitter
   * yet; this method exists so the call site is ready when those tickets land, and so the
   * documented terminal-event-untrack contract has a concrete API to point at. Silently no-ops if
   * the session has no entry or the key was not tracked.
   *
   * <p>Package-private — only callable from sibling handlers within this package.
   *
   * @param sessionId Aeron cluster session id ({@code ClientSession#id()})
   * @param orderKey the order key to remove from this session's outstanding set
   */
  void untrackSessionOrder(final long sessionId, final long orderKey) {
    final var set = sessionOrders.get(sessionId);
    if (set != null) {
      set.remove(orderKey);
    }
  }

  /**
   * Records {@code orderKey} as outstanding on {@code sessionId}. The per-session {@link
   * LongHashSet} is expected to have been pre-allocated by {@link #onSessionOpen}; if it is missing
   * (test path bypassing onSessionOpen, or framework regression) the set is allocated lazily and a
   * comment in the source documents the production invariant.
   *
   * <p>Insert is idempotent ({@code LongHashSet.add} is set-semantics). In production the
   * idempotency is defensive only — {@link TradingState#generateOrderId} is monotonic so duplicate
   * orderKeys are unreachable; tests exercise the idempotent path.
   *
   * <p>Package-private to allow direct assertion in unit tests.
   *
   * @param sessionId Aeron cluster session id ({@code ClientSession#id()})
   * @param orderKey monotonic cluster order key from {@link TradingState#generateOrderId()}
   */
  void trackSessionOrder(final long sessionId, final long orderKey) {
    final var existing = sessionOrders.get(sessionId);
    final LongHashSet set;
    if (existing == null) {
      // Production invariant: TradingClusteredService.onSessionOpen always calls
      // this handler's onSessionOpen before any onCommand for that session, so this
      // branch is unreachable in production. Kept defensively for tests that
      // exercise admit paths without driving the full session lifecycle.
      set = new LongHashSet(SESSION_ORDERS_PER_SESSION_CAPACITY, SESSION_ORDERS_LOAD_FACTOR);
      sessionOrders.put(sessionId, set);
    } else {
      set = existing;
    }
    set.add(orderKey);
  }

  // ===========================================================================
  // Session close — orphan cancel (APP-151 phase 1)
  // ===========================================================================

  /**
   * Cancels every outstanding order placed by the given cluster session, emitting one {@code
   * OrderCanceledEvent} (template 103) per cancelled order and releasing each book slot back to
   * {@link com.trading.engine.cluster.OrderBook}.
   *
   * <p>Called from {@link com.trading.engine.cluster.TradingClusteredService#onSessionClose} after
   * the existing {@code RfqStateMachine.onSessionClose} delegate. Aeron Cluster guarantees this
   * runs on the single duty-cycle thread; no synchronisation needed.
   *
   * <p><b>Idempotency.</b> Double-close (rare — Aeron typically guarantees one terminal callback
   * per session, but defensive coding is cheap) sees an empty tracker entry and returns silently
   * without emitting events.
   *
   * <p><b>Race with book release.</b> An orderKey present in the tracker but absent from {@link
   * com.trading.engine.cluster.OrderBook#get(long)} indicates the order was already terminated
   * (filled / explicitly cancelled / book-evicted) but not yet untracked. Phase 1 has no explicit
   * untrack path on terminal events (phases 2+ add this); for now the lookup-miss branch silently
   * skips that key. Pre-prod traffic profile means this branch is exercised only by tests.
   *
   * <p><b>Allocation.</b> Zero allocation on the order-admit hot path (this method is itself the
   * cold session-close path). The detached {@link LongHashSet} owns a cached {@link
   * LongHashSet.LongIterator} field that is lazy-initialised on its first {@code iterator()} call;
   * because the set has been {@code remove}-d from the outer map and only iterated once before
   * becoming GC-eligible, that lazy-init fires exactly once per session-close that has tracked
   * orders — cost is one tiny allocation (~16 B), strictly off the order-admit hot path. The
   * detached set instance itself was allocated at {@link #onSessionOpen} (or, if {@code
   * onSessionOpen} was bypassed by a test fixture, in the defensive lazy branch of {@link
   * #trackSessionOrder}) — also a cold-path event.
   *
   * @param sessionId Aeron cluster session id whose orders should be cancelled
   * @param clusterTimestamp the cluster-assigned timestamp in epoch nanos
   * @param eventSink the event emission pipeline
   */
  public void onSessionClose(
      final long sessionId, final long clusterTimestamp, final EventSink eventSink) {
    Objects.requireNonNull(eventSink, "eventSink");
    cancelSessionOrders(sessionId, clusterTimestamp, eventSink, CancelReasonEnum.SessionDisconnect);
    // APP-151 phase 4 — clear the idle-activity entry so subsequent scans don't see a stale ts.
    sessionLastActivityNanos.remove(sessionId);
    // APP-151 phase 5 — emit a one-line GFLog summary of this session's lifetime activity, then
    // clear the per-session counters. Logged BEFORE clear so the values are still resident; the
    // remove() calls return MISSING for never-opened sessions and that's reflected as 0 in the
    // logged numbers (materialised through materialiseCounter, which maps the METRIC_MISSING
    // sentinel to 0).
    logSessionMetricsSummary(sessionId);
    sessionMetricOrdersAdmitted.remove(sessionId);
    sessionMetricOrdersRejected.remove(sessionId);
    sessionMetricOrdersCancelledOnDisconnect.remove(sessionId);
    sessionMetricOrdersCancelledOnIdleTimeout.remove(sessionId);
    sessionMetricQuoteRequests.remove(sessionId);
  }

  /**
   * Emit a one-line GFLog summary of the per-session metrics for {@code sessionId}. Called from
   * {@link #onSessionClose} as the final step. Zero-allocation: GFLog's builder API takes primitive
   * long appends; counter reads are single {@link Long2LongHashMap#get} ops.
   *
   * <p>Format: {@code Session {id} closed — orders: admitted={N} rejected={N}
   * canceled-on-disconnect={N} canceled-on-idle-timeout={N} quote-requests={N}}. Missing counters
   * (session that never sent commands) materialise as 0.
   *
   * @param sessionId Aeron cluster session id being closed
   */
  private void logSessionMetricsSummary(final long sessionId) {
    LOG.info()
        .append("Session ")
        .append(sessionId)
        .append(" closed — orders: admitted=")
        .append(materialiseCounter(sessionMetricOrdersAdmitted, sessionId))
        .append(" rejected=")
        .append(materialiseCounter(sessionMetricOrdersRejected, sessionId))
        .append(" canceled-on-disconnect=")
        .append(materialiseCounter(sessionMetricOrdersCancelledOnDisconnect, sessionId))
        .append(" canceled-on-idle-timeout=")
        .append(materialiseCounter(sessionMetricOrdersCancelledOnIdleTimeout, sessionId))
        .append(" quote-requests=")
        .append(materialiseCounter(sessionMetricQuoteRequests, sessionId))
        .commit();
  }

  /**
   * Read a counter for {@code sessionId} from {@code metricMap}, treating the {@link
   * #METRIC_MISSING} sentinel as 0 so the summary log line shows concrete numbers for sessions that
   * never sent any commands of that type.
   *
   * @param metricMap one of the five {@code sessionMetric*} maps
   * @param sessionId session whose counter to read
   * @return the count (0 if no entry)
   */
  private static long materialiseCounter(final Long2LongHashMap metricMap, final long sessionId) {
    final long value = metricMap.get(sessionId);
    return value == METRIC_MISSING ? 0L : value;
  }

  /**
   * Shared cancel-all-orders-for-session path used by both {@link #onSessionClose} (cancelReason =
   * {@code SessionDisconnect}) and {@link #onIdleScan} (cancelReason = {@code IdleTimeout}).
   * Removes the session's entry from {@link #sessionOrders}, iterates its orderKeys, emits one
   * {@code OrderCanceledEvent} per live order, and releases each pool slot. Stale orderKeys (whose
   * {@link OrderState} is no longer in the {@link com.trading.engine.cluster.OrderBook}) are
   * silently skipped — emit and apply stay coupled.
   *
   * @param sessionId session whose orders should be cancelled
   * @param clusterTimestamp cluster epoch-nanos to stamp on each emitted event
   * @param eventSink event emission pipeline
   * @param cancelReason {@link CancelReasonEnum#SessionDisconnect} or {@link
   *     CancelReasonEnum#IdleTimeout} today; phase 5+ may add more triggers
   */
  private void cancelSessionOrders(
      final long sessionId,
      final long clusterTimestamp,
      final EventSink eventSink,
      final CancelReasonEnum cancelReason) {
    final var orderKeys = sessionOrders.remove(sessionId);
    if (orderKeys == null || orderKeys.isEmpty()) {
      return;
    }
    // APP-151 phase 5 — choose which counter to bump per cancel based on the reason.
    final Long2LongHashMap counterMap;
    if (cancelReason == CancelReasonEnum.SessionDisconnect) {
      counterMap = sessionMetricOrdersCancelledOnDisconnect;
    } else if (cancelReason == CancelReasonEnum.IdleTimeout) {
      counterMap = sessionMetricOrdersCancelledOnIdleTimeout;
    } else {
      // ExplicitCancel / OperatorForce — future emitters will route through their own counters
      // (APP-65 / APP-153). Default to null so we don't silently mis-bucket those reasons here.
      counterMap = null;
    }
    final var it = orderKeys.iterator();
    while (it.hasNext()) {
      final long orderKey = it.nextValue();
      final var state = tradingState.orderBook().get(orderKey);
      if (state == null) {
        // Order already terminated by another path. Tracker had a stale key — skip silently
        // so emit and apply stay coupled: we never emit a cancel without an actual book release,
        // and we never release a slot for an order whose cancel event was not emitted.
        continue;
      }
      emitOrderCanceledEvent(eventSink, clusterTimestamp, state, cancelReason);
      tradingState.applyOrderCanceled(orderKey);
      if (counterMap != null) {
        incrementSessionCounter(counterMap, sessionId);
      }
    }
  }

  // ===========================================================================
  // Idle session timeout — APP-151 phase 4
  // ===========================================================================

  /**
   * Periodic scan invoked by {@link
   * com.trading.engine.cluster.TradingClusteredService#onTimerEvent} on the idle-scan timer. Any
   * session whose last-activity timestamp is older than {@code currentTimestamp - timeoutNanos} has
   * its outstanding orders cancelled with {@code cancelReason=IdleTimeout}; the session is NOT
   * closed (Artio owns FIX lifecycle), only its orders are released.
   *
   * <p><b>Iteration safety.</b> {@link Long2LongHashMap#forEachLong} cannot tolerate mid-iteration
   * modification of the map being walked. The consumer collects idle session ids into a
   * pre-allocated {@link #idleScanPendingRemoval} {@link LongHashSet}; after {@code forEachLong}
   * returns, a second pass removes them from {@link #sessionLastActivityNanos}. {@link
   * #sessionOrders} (a different map) IS modified during iteration via {@link
   * #cancelSessionOrders}, which is safe.
   *
   * <p><b>Allocation.</b> Zero allocation on the order-admit hot path (this method is itself the
   * cold scan path). The consumer is bound once at construction; {@code forEachLong} reuses
   * Agrona's primitive-consumer interface. The pending-removal set is pre-allocated and {@code
   * clear}-ed at the end of each scan. Its {@code iterator()} call (when at least one idle session
   * was found) lazy-inits Agrona's cached {@link LongHashSet.LongIterator} on the first call — same
   * one-time ~16 B allocation pattern documented on {@link #onSessionClose}, and like that path,
   * strictly off the order-admit hot path.
   *
   * @param currentTimestamp cluster timestamp at scan time (epoch nanos)
   * @param timeoutNanos idle threshold — sessions whose last activity was before {@code
   *     currentTimestamp - timeoutNanos} are cancelled
   * @param eventSink event emission pipeline
   */
  public void onIdleScan(
      final long currentTimestamp, final long timeoutNanos, final EventSink eventSink) {
    Objects.requireNonNull(eventSink, "eventSink");
    if (sessionLastActivityNanos.isEmpty()) {
      return;
    }
    idleScanScratchCurrentTs = currentTimestamp;
    idleScanScratchThresholdTs = currentTimestamp - timeoutNanos;
    idleScanScratchEventSink = eventSink;
    try {
      sessionLastActivityNanos.forEachLong(idleScanConsumer);
      // Second pass — remove timed-out sessions from the activity map (cannot do mid-iteration).
      if (!idleScanPendingRemoval.isEmpty()) {
        final var rit = idleScanPendingRemoval.iterator();
        while (rit.hasNext()) {
          sessionLastActivityNanos.remove(rit.nextValue());
        }
      }
    } finally {
      // Reset scratch state on EVERY exit path — including exceptions from cancelSessionOrders /
      // EventSink emission. Without this, a mid-scan throw would leave idleScanScratchEventSink
      // dangling (preventing GC of a closed sink) AND idleScanPendingRemoval populated with
      // stale ids that the NEXT scan would erroneously evict from the activity map.
      idleScanPendingRemoval.clear();
      idleScanScratchEventSink = null;
    }
  }

  /**
   * Inner-loop body for {@link #onIdleScan} — bound as the {@link
   * org.agrona.collections.LongLongConsumer} for {@link Long2LongHashMap#forEachLong}. Cancels all
   * orders for any session whose {@code lastActivity} predates {@link #idleScanScratchThresholdTs};
   * marks that session id for removal from {@link #sessionLastActivityNanos} after the iteration
   * completes.
   *
   * @param sessionId Aeron cluster session id (forEach key)
   * @param lastActivity cluster timestamp of the most recent observed activity for this session
   */
  private void idleScanVisit(final long sessionId, final long lastActivity) {
    // {@code Long2LongHashMap.forEachLong} does NOT visit absent entries, so the missing-sentinel
    // check is defensive (guards against any future seeding bug that puts MISSING explicitly).
    if (lastActivity == IDLE_LAST_ACTIVITY_MISSING || lastActivity >= idleScanScratchThresholdTs) {
      return;
    }
    cancelSessionOrders(
        sessionId,
        idleScanScratchCurrentTs,
        idleScanScratchEventSink,
        CancelReasonEnum.IdleTimeout);
    idleScanPendingRemoval.add(sessionId);
  }

  /**
   * Encodes and emits one {@code OrderCanceledEvent} (template 103) using the provided live {@link
   * OrderState}. Caller verifies non-null and runs the matching {@link
   * TradingState#applyOrderCanceled} after this method returns so emit and apply stay coupled.
   *
   * <p><b>OrigClOrdID.</b> Set equal to clOrdId. FIX 4.4 OrigClOrdID (tag 41) carries the prior
   * client-assigned id when a cancel originates from a counterparty cancel request; for
   * server-initiated cancels (session disconnect) there is no separate cancel request, so industry
   * convention is to echo the original clOrdId.
   *
   * <p><b>ProductType.</b> Emitted as {@code state.productType()} — the value the order was
   * admitted with, captured by APP-151 phase 3's addition of the {@code productType} field on
   * {@link OrderState}. Reverts to {@code NULL_VAL} only for orders that crossed a cluster snapshot
   * (the field is in-memory only — separate slice tracks snapshot persistence).
   *
   * @param eventSink the event emission pipeline
   * @param clusterTimestamp the cluster-assigned timestamp in epoch nanos
   * @param state the live OrderState (must be non-null; caller's responsibility)
   * @param cancelReason {@link CancelReasonEnum} value chosen by the caller — {@code
   *     SessionDisconnect} from {@link #onSessionClose}, {@code IdleTimeout} from {@link
   *     #onIdleScan}. Other values plug in as future emitters land (APP-65 / APP-153).
   */
  private void emitOrderCanceledEvent(
      final EventSink eventSink,
      final long clusterTimestamp,
      final OrderState state,
      final CancelReasonEnum cancelReason) {
    // Generate a fresh execId per cancel (FIX 4.4 §4.4.5 requires ExecID unique per ExecType per
    // day). Uses the same id-generator backing OrderCreatedEvent so the gateway no longer needs
    // to synthesise a sentinel.
    tradingState.generateExecId();

    orderCanceledEncoder.wrapAndApplyHeader(egressBuffer, 0, headerEncoder);
    // sequenceNumber + timestamp written as zero by design: EventSink.emit overwrites both fields
    // with the authoritative cluster sequence + nanosecond timestamp during egress publication
    // (same pattern as OrderCreatedEvent / OrderRejectedEvent — see admitNewOrder).
    orderCanceledEncoder.sequenceNumber(0L);
    orderCanceledEncoder.timestamp(0L);

    // orderId — copy from state via the shared clOrdIdScratch (both fields are 20-byte char).
    // SBE-generated putXxx(byte[], int) copies bytes immediately into the egress buffer, so we
    // can serially reuse the scratch for orderId / clOrdId / origClOrdId. Safe because the
    // cluster duty cycle is single-threaded — onSessionClose never interleaves with an admit.
    state.copyOrderIdTo(clOrdIdScratch, 0);
    orderCanceledEncoder.putOrderId(clOrdIdScratch, 0);

    // execId — from the fresh id minted above (lives in tradingState.execIdScratch()).
    orderCanceledEncoder.putExecId(tradingState.execIdScratch(), 0);

    state.copyClOrdIdTo(clOrdIdScratch, 0);
    orderCanceledEncoder.putClOrdId(clOrdIdScratch, 0);
    // OrigClOrdID echoes ClOrdID for server-initiated cancels — see Javadoc above.
    orderCanceledEncoder.putOrigClOrdId(clOrdIdScratch, 0);

    state.copySymbolTo(symbolScratch, 0);
    orderCanceledEncoder.putSymbol(symbolScratch, 0);

    orderCanceledEncoder.side(state.side());
    // cumQty from live state — non-zero only for partial-filled orders cancelled mid-life
    // (no such path exists today, but the field is wire-correct from the moment phase 4+ work
    // enables cancel-of-partially-filled).
    orderCanceledEncoder.cumQty(state.cumQty());
    orderCanceledEncoder.productType(state.productType());
    // Caller-supplied cancel reason — SessionDisconnect from onSessionClose, IdleTimeout from
    // onIdleScan, ExplicitCancel (APP-65) / OperatorForce (APP-153) from future emitters.
    orderCanceledEncoder.cancelReason(cancelReason);

    final int eventLen = MessageHeaderEncoder.ENCODED_LENGTH + orderCanceledEncoder.encodedLength();
    eventSink.emit(clusterTimestamp, egressBuffer, 0, eventLen);

    // APP-62 §4 — release the working position counter that was incremented at admit time.
    // symbolScratch was just populated by state.copySymbolTo above, so the packed hash is current.
    // The revert is idempotent (no-op when the inner map is missing); applies regardless of
    // cancelReason (SessionDisconnect / IdleTimeout / ExplicitCancel / OperatorForce all release
    // the working exposure).
    //
    // Subtract the LIVE working leaves (state.leavesQty() — set to orderQty at admit, decremented
    // by future fill emitters in APP-180). Today cumQty is always 0 so leavesQty == orderQty, but
    // once partial fills land, only the un-filled remainder is still "working"; the filled portion
    // was already decremented when the fill landed via applyFill (future). Subtracting the full
    // orderQty here would drive the counter negative and silently let the next admit exceed the
    // configured cap. Using state.leavesQty() keeps the future fill emitter responsible for one
    // field, not two — single source of truth.
    long leavesQtyApp62Cxl = state.leavesQty();
    if (leavesQtyApp62Cxl > 0L) {
      long symbolHashApp62Cxl = packSymbolKey(symbolScratch, 0);
      revertWorkingPosition(state.accountId(), symbolHashApp62Cxl, state.side(), leavesQtyApp62Cxl);
    }
  }

  // ===========================================================================
  // Rejection encoding + emission
  // ===========================================================================

  /**
   * Encodes and emits an {@code OrderRejectedEvent} via {@link EventSink}. Uses the pre-stashed
   * {@link #clOrdIdScratch}, {@link #symbolScratch}, {@link #accountCodeScratch}, currency bytes,
   * and APP-62 §D stashed {@code stashedOrderQty} / {@code stashedPrice} from the current NOS
   * decode pass. The breach-context fields ({@code limitValue}, {@code projectedValue}, {@code
   * checkId}) default to safe zero/null values; call {@link #emitOrderRejectedWithBreachContext} to
   * write non-default audit context.
   *
   * @param eventSink the event emission pipeline
   * @param session the client session for egress reply
   * @param timestamp the cluster-assigned timestamp in epoch nanos
   * @param side the order side (may be NULL_VAL if decode failed before side extraction)
   * @param reason the rejection reason enum
   * @param text human-readable rejection text (max 64 ASCII chars)
   */
  private void emitOrderRejected(
      final EventSink eventSink,
      final ClientSession session,
      final long timestamp,
      final SideEnum side,
      final RejectReasonEnum reason,
      final String text) {
    emitOrderRejectedWithBreachContext(
        eventSink, session, timestamp, side, reason, text, RiskCheckEnum.NULL_VAL, 0L, 0L);
  }

  /**
   * APP-62 §D — extended {@link #emitOrderRejected} that writes breach-context audit fields ({@code
   * limitValue}, {@code projectedValue}, {@code checkId}) onto {@code OrderRejectedEvent}.
   *
   * <p>Use from APP-62 validation checks 11e (position), 11f (fat-finger), 11g (symbol
   * eligibility), and 0a (RiskLimitsNotLoaded) where the audit semantics require explicit limit /
   * projected values for SEC 15c3-5(b) reconstruction. Existing call sites continue to use the
   * 6-arg {@link #emitOrderRejected} which delegates to this method with {@code
   * RiskCheckEnum.NULL_VAL} + {@code 0L} + {@code 0L} for the breach-context fields. The encoder
   * always writes the new {@code orderQty} / {@code price} / {@code limitValue} / {@code
   * projectedValue} / {@code checkId} fields on every reject — {@code orderQty} and {@code price}
   * come from {@link #stashedOrderQty} / {@link #stashedPrice} captured during NOS decode, so even
   * legacy reject paths now emit the rejected command's qty/price on the audit event (this is
   * explicit encoder writes — not SBE zero-padding).
   *
   * @param checkId the validation check that produced this reject (audit discriminator)
   * @param limitValue the configured limit at time of check, fixed-point (semantics depend on
   *     {@code checkId})
   * @param projectedValue the value that breached the limit, fixed-point
   */
  private void emitOrderRejectedWithBreachContext(
      final EventSink eventSink,
      final ClientSession session,
      final long timestamp,
      final SideEnum side,
      final RejectReasonEnum reason,
      final String text,
      final RiskCheckEnum checkId,
      final long limitValue,
      final long projectedValue) {

    // APP-151 phase 5 — per-session metrics. Increment "orders rejected" counter for this session.
    // Skipped on the null-session test path (matches admit-counter convention).
    if (session != null) {
      incrementSessionCounter(sessionMetricOrdersRejected, session.id());
    }

    orderRejectedEncoder.wrapAndApplyHeader(egressBuffer, 0, headerEncoder);
    // Authoritative sequenceNumber + timestamp stamped by EventSink at egress (see comment in
    // OrderCreatedEvent encode block above for full rationale).
    orderRejectedEncoder.sequenceNumber(0L);
    orderRejectedEncoder.timestamp(0L);
    orderRejectedEncoder.putClOrdId(clOrdIdScratch, 0);
    // Symbol may be zero-padded from the decoder; pass the scratch verbatim.
    orderRejectedEncoder.putSymbol(symbolScratch, 0);
    orderRejectedEncoder.side(side);
    orderRejectedEncoder.rejectReason(reason);
    // Account code may be empty — still ship the 16-byte scratch (zero-padded tail is valid SBE).
    orderRejectedEncoder.putAccountCode(accountCodeScratch, 0);
    // ProductType — use the decoded value if available. The decoder has been wrapped for all
    // reject paths (wrap happens before any validation), so the raw byte is always readable.
    // Use safeProductType() to handle unrecognized wire values gracefully.
    orderRejectedEncoder.productType(safeProductType());
    // Currency bytes — stashed from the current NOS decode pass.
    orderRejectedEncoder.putCurrency(currencyByte0, currencyByte1, currencyByte2);
    // APP-62 §D audit fields — orderQty/price echo the rejected command; limitValue/projectedValue
    // /checkId give a regulator-friendly reconstruction without log-join via clOrdId.
    orderRejectedEncoder.orderQty(stashedOrderQty);
    orderRejectedEncoder.price(stashedPrice);
    orderRejectedEncoder.limitValue(limitValue);
    orderRejectedEncoder.projectedValue(projectedValue);
    orderRejectedEncoder.checkId(checkId);
    orderRejectedEncoder.text(text);

    final int rejEventLen =
        MessageHeaderEncoder.ENCODED_LENGTH + orderRejectedEncoder.encodedLength();
    eventSink.emit(timestamp, egressBuffer, 0, rejEventLen);

    tradingState.applyOrderRejected(); // no-op — structural completeness
  }

  // ===========================================================================
  // Utility
  // ===========================================================================

  /**
   * Returns the effective length of a fixed-length SBE character array by trimming trailing zero
   * bytes. SBE pads fixed-length character fields with {@code 0x00}; this helper finds the last
   * non-zero byte to determine the logical string length.
   *
   * @param data the byte array to inspect
   * @param length the declared fixed length of the SBE field
   * @return the number of significant (non-zero) bytes from the start
   */
  private static int trimTrailingZeros(final byte[] data, final int length) {
    int end = length;
    while (end > 0 && data[end - 1] == 0) {
      end--;
    }
    return end;
  }

  // ===========================================================================
  // APP-62 §4 — position helpers
  // ===========================================================================

  /**
   * Packs an 8-byte SBE Symbol field (NUL-padded char[8]) into a little-endian {@code long} for use
   * as a primitive map key. Zero-allocation; matches {@code
   * com.trading.engine.projections.SymbolPacker.pack(byte[], int)} semantics so cluster and
   * projection state share the same symbol-to-key encoding without a cross-module dependency.
   *
   * @param src the symbol bytes (must be at least 8 bytes long)
   * @param offset the start offset of the 8-byte field
   * @return the packed symbol as a little-endian long
   */
  static long packSymbolKey(final byte[] src, final int offset) {
    return (src[offset] & 0xFFL)
        | ((src[offset + 1] & 0xFFL) << 8)
        | ((src[offset + 2] & 0xFFL) << 16)
        | ((src[offset + 3] & 0xFFL) << 24)
        | ((src[offset + 4] & 0xFFL) << 32)
        | ((src[offset + 5] & 0xFFL) << 40)
        | ((src[offset + 6] & 0xFFL) << 48)
        | ((src[offset + 7] & 0xFFL) << 56);
  }

  /**
   * Overflow-saturating addition. Saturates to {@link Long#MAX_VALUE} on positive overflow and
   * {@link Long#MIN_VALUE} on negative overflow. The caller treats saturation as a breach (compares
   * strictly {@code projected > limit}); no {@code ArithmeticException} is thrown because the hot
   * path is zero-allocation. Precondition: callers in this handler only pass non-negative {@code b}
   * (order quantity), so mixed-sign overflow is impossible by construction.
   */
  static long safeAdd(final long a, final long b) {
    assert b >= 0L : "safeAdd requires non-negative b (caller passes order quantity)";
    if (a > 0L && b > Long.MAX_VALUE - a) {
      return Long.MAX_VALUE;
    }
    if (a < 0L && b < Long.MIN_VALUE - a) {
      return Long.MIN_VALUE;
    }
    return a + b;
  }

  /**
   * Returns the current working LONG quantity for {@code (accountId, symbolHash)}, or {@link
   * #WORKING_POSITION_MISSING} (0) when no working buy has been admitted yet. Zero-allocation;
   * primitive long return.
   */
  long workingLongFor(final long accountId, final long symbolHash) {
    final var inner = accountSymbolWorkingLong.get(accountId);
    return inner == null ? WORKING_POSITION_MISSING : inner.get(symbolHash);
  }

  /** See {@link #workingLongFor} — symmetric for SELL side. */
  long workingShortFor(final long accountId, final long symbolHash) {
    final var inner = accountSymbolWorkingShort.get(accountId);
    return inner == null ? WORKING_POSITION_MISSING : inner.get(symbolHash);
  }

  /**
   * Adds {@code orderQty} (fixed-point 10⁻⁸) to the working LONG or SHORT counter for the given
   * {@code (accountId, symbolHash)}, depending on {@code side}. Called from the admit path after
   * Check 11e accepts the order.
   *
   * <p><b>Allocation.</b> Cold path: lazy first-touch only. One {@link Long2LongHashMap} per {@code
   * (accountId, side)} is allocated on the first admission against that account from that side;
   * every subsequent admit for the same {@code (accountId, side)} reuses the existing inner map and
   * is strictly zero-allocation. A follow-up slice can move the allocation to {@code LoadRiskLimit}
   * ingress (where the risk record is registered) so even the first-touch is off the admit path;
   * the lazy form here keeps the §4 slice standalone.
   */
  void applyWorkingPosition(
      final long accountId, final long symbolHash, final SideEnum side, final long orderQty) {
    final var outer = side == SideEnum.Buy ? accountSymbolWorkingLong : accountSymbolWorkingShort;
    var inner = outer.get(accountId);
    if (inner == null) {
      inner =
          new Long2LongHashMap(
              WORKING_POSITION_INNER_INITIAL_CAPACITY, 0.65f, WORKING_POSITION_MISSING);
      outer.put(accountId, inner);
    }
    inner.put(symbolHash, safeAdd(inner.get(symbolHash), orderQty));
  }

  /**
   * Subtracts {@code orderQty} from the working LONG or SHORT counter — inverse of {@link
   * #applyWorkingPosition}. Called from the cancel path (no-op if the inner map is absent;
   * underflow saturates at {@code 0L} to preserve invariant). Zero-allocation.
   */
  void revertWorkingPosition(
      final long accountId, final long symbolHash, final SideEnum side, final long orderQty) {
    final var outer = side == SideEnum.Buy ? accountSymbolWorkingLong : accountSymbolWorkingShort;
    final var inner = outer.get(accountId);
    if (inner == null) {
      return;
    }
    final long current = inner.get(symbolHash);
    if (current == WORKING_POSITION_MISSING) {
      return;
    }
    final long next = current - orderQty;
    // Agrona Long2LongHashMap rejects writing the missing-value sentinel via put(). When the
    // counter reaches zero we remove the key instead, restoring the "no working quantity"
    // semantics the sentinel encodes. Negative results (orderQty larger than current — should
    // never happen for matched admit↔revert pairs but defensive) are also coalesced to "absent".
    if (next <= 0L) {
      inner.remove(symbolHash);
    } else {
      inner.put(symbolHash, next);
    }
  }

  // ===========================================================================
  // APP-62 §5 — fat-finger helpers
  // ===========================================================================

  /**
   * Updates the per-symbol last-quoted mid + asOf-timestamp cache from a {@link
   * com.trading.engine.messages.sbe.PriceResponseDecoder PriceResponse} arrival. Skips on
   * crossed/locked markets (bid &gt;= ask), zero or negative sides, or out-of-range prices that
   * exceed {@link #MAX_REASONABLE_PRICE} (defends against pricing-service malfunctions). The
   * midpoint is computed in an overflow-safe form: {@code bid + (ask - bid) / 2}. Zero-allocation
   * after construction.
   *
   * <p>This method is called from the cluster's PriceResponse dispatch path (Raft-replicated), so
   * the cache mutation is deterministic across replicas.
   *
   * <p><b>OPERATIONAL GATE — KNOWN GAP.</b> As of the APP-62 §5 first slice this method has no
   * caller; the {@link com.trading.engine.cluster.handler.PriceResponseHandler
   * PriceResponseHandler} dispatch hook lands in a follow-up slice. Until that wires up, the {@link
   * #lastQuotedMidPrice} cache stays empty, and any {@code LoadRiskLimit} that sets {@code
   * fatFingerEnabled=true} with the industry-standard {@code fatFingerFailClosed=true} default will
   * reject EVERY limit / PreviouslyQuoted order (no reference → fail-closed). Test fixtures default
   * {@code fatFingerEnabled=false} so unit tests do not surface this, but production YAML loads
   * MUST keep {@code fatFingerEnabled=false} until the wire-up commit lands. The risk is documented
   * here so a future reviewer landing the hook can close this finding by removing this paragraph.
   *
   * <p>TODO(APP-62): wire PriceResponseHandler / TradingClusteredService dispatch on tpl 51
   * (PriceResponse) to call {@link #updateLastQuotedMid}; remove the OPERATIONAL GATE paragraph
   * above when the wire-up lands.
   *
   * @param symbolHash packed symbol key, see {@link #packSymbolKey}
   * @param bidPrice bid side, fixed-point 10⁻⁸; values &le; 0 cause a skip
   * @param askPrice ask side, fixed-point 10⁻⁸; values &le; 0 or {@code &lt;= bidPrice} cause a
   *     skip
   * @param clusterTimestamp cluster-assigned epoch-nanos at PriceResponse arrival
   */
  void updateLastQuotedMid(
      final long symbolHash,
      final long bidPrice,
      final long askPrice,
      final long clusterTimestamp) {
    // Skip crossed / locked / sentinel-zero markets — mid is non-meaningful for fat-finger.
    if (bidPrice <= 0L || askPrice <= 0L || bidPrice >= askPrice) {
      return;
    }
    // Upper-bound sanity: reject obviously-garbage prices before they pollute the cache.
    if (bidPrice > MAX_REASONABLE_PRICE || askPrice > MAX_REASONABLE_PRICE) {
      return;
    }
    // Overflow-safe midpoint: ask > bid > 0 and both bounded above by MAX_REASONABLE_PRICE so
    // (ask - bid) is safe; bid + (ask - bid)/2 cannot overflow inside the guarded range.
    long mid = bidPrice + (askPrice - bidPrice) / 2L;
    // Defense-in-depth — never let a sentinel value slip into the cache.
    if (mid == LAST_PRICE_MISSING) {
      return;
    }
    lastQuotedMidPrice.put(symbolHash, mid);
    lastQuotedMidAsOfNanos.put(symbolHash, clusterTimestamp);
  }

  /**
   * Fixed-point scale factor used by all prices, quantities, and notional values in the trading
   * engine: 10^8 (matches the SBE schema's {@code priceScale="100000000"} attribute). Kept as a
   * local constant here so the notional calculation stays self-contained — the cluster module has
   * no shared "prices.PRICE_SCALE" symbol today, and importing one cross-module would couple this
   * handler to a constants type it otherwise doesn't need.
   */
  static final long PRICE_SCALE = 100_000_000L;

  /**
   * Computes {@code (orderQty * price) / PRICE_SCALE} for the maxOrderNotional check, saturating to
   * {@link Long#MAX_VALUE} on intermediate overflow. Both inputs are fixed-point 10^-8 longs; the
   * naive multiply can exceed long for qty × price > ~9.2e18, which happens at qty=1e10 *
   * price=1e10 (10 billion units at $100 — well within hostile-input range for a fuzz test,
   * plausible for an accidental decimal-place mistake in production).
   *
   * <p>Algorithm: use {@link Math#multiplyHigh(long, long)} to detect the high 64 bits of the
   * 128-bit product. If high != 0 (or the product would be negative when both inputs are positive),
   * the multiplication overflowed signed-long range — saturate to {@code Long.MAX_VALUE} so the
   * downstream {@code > maxOrderNotional} check rejects the order. Otherwise return the divided
   * value. Pure primitive arithmetic, zero allocation.
   *
   * @param orderQty fixed-point 10^-8 quantity (positive)
   * @param price fixed-point 10^-8 price (positive)
   * @return the notional in fixed-point 10^-8, or {@link Long#MAX_VALUE} on overflow
   */
  static long computeNotionalSaturating(final long orderQty, final long price) {
    // multiplyHigh returns the high 64 bits of the 128-bit signed product. If those bits are
    // anything but 0 for two positive inputs, the low 64 bits don't represent the true value.
    final long high = Math.multiplyHigh(orderQty, price);
    if (high != 0L) {
      return Long.MAX_VALUE;
    }
    final long product = orderQty * price;
    // Defensive: if signed-long arithmetic wrapped past Long.MAX_VALUE (product < 0 when both
    // inputs are positive), saturate. multiplyHigh = 0 with product < 0 is impossible for
    // positive inputs but guard anyway in case a caller passes a negative price/qty.
    if (product < 0L) {
      return Long.MAX_VALUE;
    }
    return product / PRICE_SCALE;
  }

  /**
   * FNV-1a 64-bit hash over {@code (sessionId-as-8-bytes, clOrdId-bytes)} producing the dedup-map
   * key. Deterministic — replays produce identical keys, which is required for Aeron Cluster log
   * replay. Pure primitive arithmetic — zero allocation.
   *
   * <p>Collision probability — birthday approximation against the 64-bit hash space:
   *
   * <ul>
   *   <li>Globally across all sessions at the {@link #CLORDID_DEDUP_MAX_SIZE} watermark (60K
   *       entries): ≈ 9.7e-11.
   *   <li>Per-session (even spread across N sessions): ≈ 2.7e-10 / N². A single session would need
   *       ~5 billion unique ClOrdIDs in 24h to expect one collision.
   * </ul>
   *
   * <p>The trade-off vs a per-session {@code ObjectHashSet<byte[]>}-keyed structure: a true set
   * would box the byte[] on every put and allocate an {@code AsciiSequenceView} on every query,
   * both of which violate the cluster hot-path zero-allocation rule. The collision rate is well
   * below the noise floor of every other failure mode in the pipeline.
   *
   * @param sessionId the {@link ClientSession#id} of the originating session (or 0 in tests)
   * @param clOrdIdBytes the ClOrdID byte buffer ({@link #clOrdIdScratch})
   * @param offset starting offset into {@code clOrdIdBytes}
   * @param length the effective length (post-trim) of the ClOrdID
   * @return a 64-bit hash usable as a {@link Long2LongHashMap} key
   */
  static long computeClOrdIdDedupKey(
      final long sessionId, final byte[] clOrdIdBytes, final int offset, final int length) {
    // FNV-1a constants: offset basis 0xcbf29ce484222325L; prime 0x100000001b3L.
    long hash = 0xcbf29ce484222325L;
    // Mix in session ID bytes (big-endian) first so identical ClOrdIDs across sessions get
    // distinct keys.
    for (int i = 7; i >= 0; i--) {
      hash = (hash ^ ((sessionId >>> (i * 8)) & 0xFFL)) * 0x100000001b3L;
    }
    for (int i = 0; i < length; i++) {
      hash = (hash ^ (clOrdIdBytes[offset + i] & 0xFFL)) * 0x100000001b3L;
    }
    return hash;
  }

  /**
   * Walks {@link #clOrdIdRegistry} and removes entries whose first-seen timestamp falls outside the
   * {@link #CLORDID_DEDUP_WINDOW_NS} dedup window relative to {@code nowNs}. Invoked only when the
   * registry crosses {@link #CLORDID_DEDUP_MAX_SIZE} on a NEW insert (never on a refresh), so
   * steady-state hot-path cost stays O(1); eviction cost is amortized across the inserts that push
   * the registry past the watermark.
   *
   * <p>Iteration uses {@link Long2LongHashMap.KeySet}'s primitive iterator and reads each value via
   * {@code get(key)} — both primitive, both zero-boxing. Avoids {@code entrySet()} which wraps each
   * key/value pair in a {@code Map.Entry<Long, Long>} (boxes both sides).
   *
   * <p>This eviction path is off the steady-state hot path by design; the watermark guard ensures
   * it runs only when the registry has accumulated 60K+ entries, which is a rare event even on a
   * busy trading day (24h × 60K/24h = ~0.69 puts/sec sustained throughput).
   *
   * @param nowNs the current cluster timestamp in epoch nanos
   */
  private void evictExpiredClOrdIds(final long nowNs) {
    // Explicit Long2LongHashMap.KeyIterator type (rather than `final var`) so a future
    // maintainer cannot mistake this for a `java.util.Iterator<Long>` that would box on
    // .next(). `nextValue()` returns primitive long.
    final Long2LongHashMap.KeyIterator keyIter = clOrdIdRegistry.keySet().iterator();
    while (keyIter.hasNext()) {
      final long key = keyIter.nextValue();
      final long firstSeenNanos = clOrdIdRegistry.get(key);
      if ((nowNs - firstSeenNanos) >= CLORDID_DEDUP_WINDOW_NS) {
        keyIter.remove();
      }
    }
  }

  /**
   * Encodes the current ClOrdID dedup registry (the {@link #clOrdIdRegistry} map plus the {@link
   * #lastEvictionTimestampNanos} throttle anchor) into a {@code ClOrdIdDedupSnapshot} (template
   * 210) at the given buffer offset. Used by {@link
   * com.trading.engine.cluster.TradingClusteredService#encodeSnapshotFragments} during the cluster
   * snapshot path so the 24h ClOrdID-uniqueness contract survives snapshot+restore.
   *
   * <p>Without this fragment, after a cluster restart the dedup registry rebuilds empty and any
   * ClOrdID first seen before the snapshot but still inside the 24h dedup window would be admitted
   * again, breaking the idempotency guarantee the cluster claims to enforce.
   *
   * <p><b>Threading:</b> single-threaded cluster duty cycle (same constraint as {@code onCommand}).
   * No synchronization required.
   *
   * <p><b>Allocation:</b> none on the path; the SBE encoder and inner group flyweight are
   * pre-allocated at construction time. Iteration uses the primitive {@link
   * Long2LongHashMap.KeyIterator} (same pattern as {@link #evictExpiredClOrdIds}) so no {@code
   * Iterator<Long>} boxing occurs.
   *
   * @param buf destination buffer (must have room for the encoded snapshot)
   * @param offset start offset in {@code buf}
   * @return the total bytes written including the SBE message header
   */
  public int snapshotDedupTo(final MutableDirectBuffer buf, final int offset) {
    clOrdIdDedupSnapEncoder.wrapAndApplyHeader(buf, offset, headerEncoder);
    clOrdIdDedupSnapEncoder.lastEvictionTimestampNanos(lastEvictionTimestampNanos);
    // tradingHalted flag rides this template (rather than its own snapshot template) — see the
    // template description in trading-schema.xml for the rationale.
    clOrdIdDedupSnapEncoder.tradingHalted((short) (tradingState.isTradingHalted() ? 1 : 0));
    final var group = clOrdIdDedupSnapEncoder.noEntriesCount(clOrdIdRegistry.size());
    final Long2LongHashMap.KeyIterator keyIter = clOrdIdRegistry.keySet().iterator();
    while (keyIter.hasNext()) {
      final long key = keyIter.nextValue();
      final long firstSeen = clOrdIdRegistry.get(key);
      group.next();
      group.dedupKey(key);
      group.firstSeenTimestamp(firstSeen);
    }
    return MessageHeaderEncoder.ENCODED_LENGTH + clOrdIdDedupSnapEncoder.encodedLength();
  }

  /**
   * Restores the ClOrdID dedup registry from a previously-encoded {@code ClOrdIdDedupSnapshot}
   * (template 210). The current registry is cleared and replaced with the snapshot contents; {@link
   * #lastEvictionTimestampNanos} is restored verbatim so the post-snapshot eviction cadence matches
   * pre-snapshot behaviour.
   *
   * <p>Called by {@link com.trading.engine.cluster.TradingClusteredService#applySnapshotFragment}
   * on the snapshot restore path. {@code blockLength} and {@code version} come from the SBE message
   * header decoded by the caller.
   *
   * <p><b>Threading:</b> single-threaded cluster duty cycle. Snapshot restore runs at {@code
   * onStart} time before any commands are dispatched.
   *
   * <p><b>Allocation:</b> none on the path; decoder and group flyweight are pre-allocated.
   *
   * @param src source buffer
   * @param offset start offset of the SBE message BODY (header already consumed by caller)
   * @param blockLength SBE block length from the inbound message header
   * @param version SBE schema version from the inbound message header
   * @return the number of body bytes consumed (excludes the header consumed by the caller)
   */
  public int restoreDedupFrom(
      final DirectBuffer src, final int offset, final int blockLength, final int version) {
    clOrdIdRegistry.clear();
    clOrdIdDedupSnapDecoder.wrap(src, offset, blockLength, version);
    lastEvictionTimestampNanos = clOrdIdDedupSnapDecoder.lastEvictionTimestampNanos();
    // Restore the cluster-wide trading-halt flag — an operator-set halt persists across cluster
    // restart so the engine cannot silently resume admitting orders after a failover.
    tradingState.setTradingHalted(clOrdIdDedupSnapDecoder.tradingHalted() != 0);
    final var group = clOrdIdDedupSnapDecoder.noEntries();
    while (group.hasNext()) {
      group.next();
      clOrdIdRegistry.put(group.dedupKey(), group.firstSeenTimestamp());
    }
    return clOrdIdDedupSnapDecoder.encodedLength();
  }

  /**
   * Reads the productType field from the NOS decoder using the raw byte accessor and maps it to the
   * corresponding {@link ProductTypeEnum}. Returns {@link ProductTypeEnum#NULL_VAL} for any
   * unrecognized wire value (including 0, which SBE zero-fills on an unset field). This avoids the
   * {@link IllegalArgumentException} that {@code nosDecoder.productType()} throws for unknown
   * values.
   *
   * @return the resolved product type enum, or {@code NULL_VAL} if the wire value is unrecognized
   */
  private ProductTypeEnum safeProductType() {
    final short raw = nosDecoder.productTypeRaw();
    return switch (raw) {
      case 1 -> ProductTypeEnum.Spot;
      case 2 -> ProductTypeEnum.Forward;
      case 3 -> ProductTypeEnum.Swap;
      default -> ProductTypeEnum.NULL_VAL;
    };
  }

  /**
   * Reads the settlType field from the NOS decoder using the raw byte accessor and maps it to the
   * corresponding {@link SettlTypeEnum}. Returns {@link SettlTypeEnum#NULL_VAL} for any
   * unrecognized wire value. Zero-allocation switch — no exception-as-control-flow.
   *
   * @return the resolved settle type enum, or {@code NULL_VAL} if the wire value is unrecognized
   */
  private SettlTypeEnum safeSettlType() {
    final short raw = nosDecoder.settlTypeRaw();
    return switch (raw) {
      case 0 -> SettlTypeEnum.Regular;
      case 1 -> SettlTypeEnum.Cash;
      case 2 -> SettlTypeEnum.NextDay;
      case 3 -> SettlTypeEnum.TPlus2;
      case 4 -> SettlTypeEnum.TPlus3;
      case 5 -> SettlTypeEnum.TPlus4;
      case 6 -> SettlTypeEnum.Future;
      case 7 -> SettlTypeEnum.WhenAndIfIssued;
      case 8 -> SettlTypeEnum.SellersOption;
      case 9 -> SettlTypeEnum.TPlus5;
      case 10 -> SettlTypeEnum.BrokenDate;
      case 11 -> SettlTypeEnum.FXSpotNextDay;
      default -> SettlTypeEnum.NULL_VAL;
    };
  }

  /**
   * Reads the tenor field from the NOS decoder using the raw byte accessor and maps it to the
   * corresponding {@link TenorEnum}. Returns {@link TenorEnum#NULL_VAL} for any unrecognized wire
   * value. Zero-allocation switch — no exception-as-control-flow.
   *
   * @return the resolved tenor enum, or {@code NULL_VAL} if the wire value is unrecognized
   */
  private TenorEnum safeTenor() {
    final short raw = nosDecoder.tenorRaw();
    return switch (raw) {
      case 1 -> TenorEnum.ON;
      case 2 -> TenorEnum.TN;
      case 3 -> TenorEnum.SN;
      case 4 -> TenorEnum.W1;
      case 5 -> TenorEnum.W2;
      case 6 -> TenorEnum.M1;
      case 7 -> TenorEnum.M2;
      case 8 -> TenorEnum.M3;
      case 9 -> TenorEnum.M6;
      case 10 -> TenorEnum.M9;
      case 11 -> TenorEnum.Y1;
      case 12 -> TenorEnum.Y2;
      case 13 -> TenorEnum.IMM;
      case 14 -> TenorEnum.BRK;
      default -> TenorEnum.NULL_VAL;
    };
  }
}
