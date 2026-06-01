package com.trading.engine.queryservice;

import com.trading.engine.messages.sbe.AccountStatusEnum;
import com.trading.engine.projections.risklimits.RiskLimitRecordView;

/**
 * Mutable flyweight view over the per-account risk-limit fields returned by {@link
 * QueryService#getAccountLimits(long)}. Designed for the hot-path query pattern: callers hold a
 * single instance, repeatedly call {@link #populate(RiskLimitRecordView)} or {@link #reset()} on
 * it, and read fields without allocation. NOT a {@code record} because the caller-owned reuse
 * pattern requires mutation; the class is package-stable in its field-access contract.
 *
 * <p><b>TODO(APP-62):</b> this class is the future zero-allocation return surface for {@link
 * QueryService#getAccountLimits(String)} once the fix-client-bridge launcher wiring lands.
 * Currently the {@code getAccountLimits(String)} signature returns the immutable {@link
 * RiskLimitRecordView} produced directly by the projection, which is correct for the read-once
 * cold-boot path but allocates one {@code RiskLimitRecordView} per query. When the bridge's
 * steady-state per-AUTH_SUCCESS query path is wired through this view, callers will hold a single
 * pre-allocated instance and pay zero allocation per query. The wiring gap is documented in {@link
 * com.trading.engine.fixbridge.transport.ClusterAccountLimitsProvider} and {@link
 * com.trading.engine.fixbridge.transport.RiskLimitToBrowserAdapter}.
 *
 * <p><b>Threading.</b> NOT thread-safe — callers must externally synchronise if the view is shared
 * across threads. Typical usage is one instance per query-service binding (e.g., the
 * fix-client-bridge's account-limits cache pre-population path) on a single Netty event-loop
 * thread.
 *
 * <p><b>Allocation.</b> Zero allocation after construction. {@link #populate(RiskLimitRecordView)}
 * copies primitive fields from the source record by reference; {@link #reset()} writes sentinels
 * indicating "unpopulated".
 *
 * @see QueryService#getAccountLimits(long)
 * @see RiskLimitRecordView
 */
public final class AccountLimitsView {

  /** Sentinel value written by {@link #reset()} for the {@code accountId} field. */
  public static final long UNPOPULATED_ACCOUNT_ID = 0L;

  private long accountId;
  private long maxOrderSize;
  private long maxOrderNotional;
  private long maxDailyVolume;
  private long maxOrdersPerSecond;
  private long maxLongPosition;
  private long maxShortPosition;
  private boolean positionLimitEnabled;
  private long priceDeviationBps;
  private boolean fatFingerEnabled;
  private boolean fatFingerFailClosed;
  private long idleSessionTimeoutNanos;
  private AccountStatusEnum status;
  private long transactTime;
  private long sequenceNumber;
  private boolean populated;

  /** Creates an empty (unpopulated) view. Call {@link #populate} before reading fields. */
  public AccountLimitsView() {
    reset();
  }

  /**
   * Reset the view to "unpopulated" — sets the accountId to {@link #UNPOPULATED_ACCOUNT_ID}, the
   * status to {@code null}, and clears the populated flag. Numeric fields are zeroed so a partial
   * read between reset and a follow-up populate cannot surface stale data.
   */
  public void reset() {
    accountId = UNPOPULATED_ACCOUNT_ID;
    maxOrderSize = 0L;
    maxOrderNotional = 0L;
    maxDailyVolume = 0L;
    maxOrdersPerSecond = 0L;
    maxLongPosition = 0L;
    maxShortPosition = 0L;
    positionLimitEnabled = false;
    priceDeviationBps = 0L;
    fatFingerEnabled = false;
    fatFingerFailClosed = false;
    idleSessionTimeoutNanos = 0L;
    status = null;
    transactTime = 0L;
    sequenceNumber = 0L;
    populated = false;
  }

  /**
   * Populate this view from the immutable record returned by the projection. Caller-owned reuse:
   * the same view instance may be repeatedly re-populated for different accounts without
   * intermediate {@link #reset()} calls (each populate fully overwrites prior state).
   *
   * @param record the projection record; must not be null
   */
  public void populate(final RiskLimitRecordView record) {
    accountId = record.accountId();
    maxOrderSize = record.maxOrderSize();
    maxOrderNotional = record.maxOrderNotional();
    maxDailyVolume = record.maxDailyVolume();
    maxOrdersPerSecond = record.maxOrdersPerSecond();
    maxLongPosition = record.maxLongPosition();
    maxShortPosition = record.maxShortPosition();
    positionLimitEnabled = record.positionLimitEnabled();
    priceDeviationBps = record.priceDeviationBps();
    fatFingerEnabled = record.fatFingerEnabled();
    fatFingerFailClosed = record.fatFingerFailClosed();
    idleSessionTimeoutNanos = record.idleSessionTimeoutNanos();
    status = record.status();
    transactTime = record.transactTime();
    sequenceNumber = record.sequenceNumber();
    populated = true;
  }

  /**
   * @return {@code true} if this view holds a populated record (vs. reset / never-populated).
   */
  public boolean isPopulated() {
    return populated;
  }

  public long accountId() {
    return accountId;
  }

  public long maxOrderSize() {
    return maxOrderSize;
  }

  public long maxOrderNotional() {
    return maxOrderNotional;
  }

  public long maxDailyVolume() {
    return maxDailyVolume;
  }

  public long maxOrdersPerSecond() {
    return maxOrdersPerSecond;
  }

  public long maxLongPosition() {
    return maxLongPosition;
  }

  public long maxShortPosition() {
    return maxShortPosition;
  }

  public boolean positionLimitEnabled() {
    return positionLimitEnabled;
  }

  public long priceDeviationBps() {
    return priceDeviationBps;
  }

  public boolean fatFingerEnabled() {
    return fatFingerEnabled;
  }

  public boolean fatFingerFailClosed() {
    return fatFingerFailClosed;
  }

  public long idleSessionTimeoutNanos() {
    return idleSessionTimeoutNanos;
  }

  public AccountStatusEnum status() {
    return status;
  }

  public long transactTime() {
    return transactTime;
  }

  public long sequenceNumber() {
    return sequenceNumber;
  }
}
