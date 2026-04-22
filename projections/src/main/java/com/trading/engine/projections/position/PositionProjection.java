package com.trading.engine.projections.position;

import com.epam.deltix.gflog.api.Log;
import com.epam.deltix.gflog.api.LogFactory;
import com.trading.engine.messages.sbe.OrderFilledEventDecoder;
import com.trading.engine.messages.sbe.SideEnum;
import com.trading.engine.messages.util.ByteArrayKey;
import com.trading.engine.projections.Projection;
import com.trading.engine.projections.ProjectionUtil;
import com.trading.engine.projections.SymbolPacker;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.StampedLock;
import org.agrona.DirectBuffer;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.collections.Object2ObjectHashMap;
import org.agrona.collections.ObjectHashSet;

/**
 * CQRS read-model projection tracking net positions per (symbol, account, settlDate). Consumes
 * {@code OrderFilledEvent} (102) from the cluster event stream.
 *
 * <p><b>Position key:</b> positions are keyed by {@code (symbol, account, settlDate)}. FX positions
 * at different settlement dates (e.g. spot T+2 vs 1M forward) are NOT merged — this is standard FX
 * position tracking practice.
 *
 * <p><b>Swap handling:</b> when {@code OrderFilledEvent.noLegs().count() > 0}, each leg is
 * processed as a separate position update using the leg's own side, settlDate, price, and quantity.
 * The {@code accountCode} comes from the top-level event field (all legs share the parent order's
 * account).
 *
 * <p><b>Data structure:</b> two-level map:
 *
 * <ol>
 *   <li>Outer: {@link Long2ObjectHashMap} keyed by packed symbol (8 ASCII bytes → long)
 *   <li>Inner: {@link Object2ObjectHashMap}{@code <ByteArrayKey, PositionView>} keyed by composite
 *       {@code accountCode[16] + settlDate[8]} = 24 bytes
 * </ol>
 *
 * <p><b>Threading:</b> single-writer / multi-reader via {@link StampedLock}. Same pattern as {@link
 * com.trading.engine.projections.order.OrderProjection}.
 *
 * <p><b>VWAP calculation:</b> per-side average price uses 128-bit intermediate arithmetic via
 * {@link Math#multiplyHigh(long, long)} to prevent overflow for large FX notionals.
 *
 * @see PositionView
 * @see PositionSnapshot
 */
public final class PositionProjection implements Projection {

  private static final Log LOG = LogFactory.getLog(PositionProjection.class);
  private static final long PRICE_SCALE = 100_000_000L;
  private static final float LOAD_FACTOR = 0.65f;

  /** SBE Account char[16] fixed width — used for composite key to prevent collisions. */
  private static final int ACCOUNT_LENGTH = 16;

  /** SBE SettlDate char[8] fixed width — used for composite key to prevent collisions. */
  private static final int SETTL_DATE_LENGTH = 8;

  // --- Position map: symbol(long) → composite(account+settlDate) → PositionView ---
  private final Long2ObjectHashMap<Object2ObjectHashMap<ByteArrayKey, PositionView>> positions;

  // --- Secondary index: accountCode → set of PositionViews for O(1) account lookup ---
  private final Object2ObjectHashMap<ByteArrayKey, ObjectHashSet<PositionView>> byAccount;

  // --- Pre-allocated SBE decoder ---
  private final OrderFilledEventDecoder filledDecoder = new OrderFilledEventDecoder();

  // --- Pre-allocated probe keys ---
  private final ByteArrayKey probePositionKey = ByteArrayKey.emptyForLookup(24);
  private final ByteArrayKey probeAccountKey = ByteArrayKey.emptyForLookup(ACCOUNT_LENGTH);

  // --- Pre-allocated scratch arrays ---
  private final byte[] scratchAccountCode = new byte[16];
  private final byte[] scratchSettlDate = new byte[8];
  private final byte[] scratchSymbol = new byte[8];
  private final byte[] scratchCurrency = new byte[3];
  private final byte[] scratchSettlCurrency = new byte[3];

  // --- Concurrency ---
  private final StampedLock lock = new StampedLock();

  // --- Volatile counters: projections are NOT single-threaded Aeron agents — they serve
  // concurrent query threads. lastProcessedSequence() reads without lock, so volatile is required
  // for cross-thread visibility. Diagnostic methods also acquire read lock (belt-and-suspenders).
  // ---
  private volatile long lastProcessedSeqNo;
  private volatile long eventsProcessed;
  private volatile long errorCount;

  public PositionProjection() {
    positions = new Long2ObjectHashMap<>(64, LOAD_FACTOR);
    byAccount = new Object2ObjectHashMap<>(256, LOAD_FACTOR);
  }

  // ---------------------------------------------------------------------------
  // Projection interface
  // ---------------------------------------------------------------------------

  @Override
  public void onEvent(
      final long seqNo,
      final int eventType,
      final DirectBuffer buffer,
      final int offset,
      final int length) {
    final long stamp = lock.writeLock();
    try {
      if (eventType == OrderFilledEventDecoder.TEMPLATE_ID) {
        onOrderFilled(seqNo, buffer, offset, length);
        eventsProcessed++;
      }
    } catch (final Exception e) {
      errorCount++;
      LOG.error()
          .append("PositionProjection decode error seqNo=")
          .append(seqNo)
          .append(" eventType=")
          .append(eventType)
          .append(" ")
          .append(e)
          .commit();
    } finally {
      lastProcessedSeqNo = seqNo;
      lock.unlockWrite(stamp);
    }
  }

  @Override
  public long lastProcessedSequence() {
    return lastProcessedSeqNo;
  }

  @Override
  public void reset() {
    final long stamp = lock.writeLock();
    try {
      positions.clear();
      byAccount.clear();
      lastProcessedSeqNo = 0;
      eventsProcessed = 0;
      errorCount = 0;
      LOG.info().append("PositionProjection reset").commit();
    } finally {
      lock.unlockWrite(stamp);
    }
  }

  // ---------------------------------------------------------------------------
  // Event handler (called under write lock)
  // ---------------------------------------------------------------------------

  private void onOrderFilled(
      final long seqNo, final DirectBuffer buffer, final int offset, final int length) {
    filledDecoder.wrap(
        buffer,
        offset,
        OrderFilledEventDecoder.BLOCK_LENGTH,
        OrderFilledEventDecoder.SCHEMA_VERSION);

    // Decode top-level fields
    filledDecoder.getSymbol(scratchSymbol, 0);
    final long symbolPacked = SymbolPacker.pack(scratchSymbol, 0);
    final int accountLen =
        ProjectionUtil.sbeStrLen(
            filledDecoder.getAccountCode(scratchAccountCode, 0), scratchAccountCode);
    if (accountLen <= 0) {
      return; // Empty accountCode — skip silently
    }

    final long timestamp = filledDecoder.timestamp();

    final OrderFilledEventDecoder.NoLegsDecoder legs = filledDecoder.noLegs();
    if (legs.count() > 0) {
      // Swap: process each leg as a separate position update
      for (final OrderFilledEventDecoder.NoLegsDecoder leg : legs) {
        final SideEnum legSide = leg.legSide();
        final int legSettlDateLen = ProjectionUtil.sbeStrLen(8, readLegSettlDate(leg));
        final long legLastPx = leg.legLastPx();
        final long legLastQty = leg.legLastQty();
        readLegCurrency(leg);

        updatePosition(
            symbolPacked,
            scratchAccountCode,
            accountLen,
            scratchSettlDate,
            legSettlDateLen,
            legSide,
            legLastPx,
            legLastQty,
            scratchCurrency,
            ProjectionUtil.sbeStrLen(3, scratchCurrency),
            scratchSettlCurrency,
            ProjectionUtil.sbeStrLen(3, scratchSettlCurrency),
            seqNo,
            timestamp);
      }
    } else {
      // Single-leg: use top-level fields
      final SideEnum side = filledDecoder.side();
      final long lastPx = filledDecoder.lastPx();
      final long lastQty = filledDecoder.lastQty();
      final int settlDateLen =
          ProjectionUtil.sbeStrLen(
              filledDecoder.getSettlDate(scratchSettlDate, 0), scratchSettlDate);
      final int currLen =
          ProjectionUtil.sbeStrLen(filledDecoder.getCurrency(scratchCurrency, 0), scratchCurrency);
      final int settlCurrLen =
          ProjectionUtil.sbeStrLen(
              filledDecoder.getSettlCurrency(scratchSettlCurrency, 0), scratchSettlCurrency);

      updatePosition(
          symbolPacked,
          scratchAccountCode,
          accountLen,
          scratchSettlDate,
          settlDateLen,
          side,
          lastPx,
          lastQty,
          scratchCurrency,
          currLen,
          scratchSettlCurrency,
          settlCurrLen,
          seqNo,
          timestamp);
    }
  }

  private void updatePosition(
      final long symbolPacked,
      final byte[] accountCode,
      final int accountLen,
      final byte[] settlDate,
      final int settlDateLen,
      final SideEnum side,
      final long lastPx,
      final long lastQty,
      final byte[] currency,
      final int currLen,
      final byte[] settlCurrency,
      final int settlCurrLen,
      final long seqNo,
      final long timestamp) {

    // Build composite probe key: account[16] + settlDate[8] = 24 bytes (fixed-width to prevent
    // collisions from variable-length trimming — e.g., "ACM"+"20260412" vs "ACM2"+"0260412").
    probePositionKey.setComposite(accountCode, 0, ACCOUNT_LENGTH, settlDate, 0, SETTL_DATE_LENGTH);

    // Get or create inner map for this symbol
    Object2ObjectHashMap<ByteArrayKey, PositionView> inner = positions.get(symbolPacked);
    if (inner == null) {
      inner = new Object2ObjectHashMap<>(64, LOAD_FACTOR);
      positions.put(symbolPacked, inner);
    }

    // Get or create position view
    PositionView view = inner.get(probePositionKey);
    if (view == null) {
      view = new PositionView();
      view.setSymbolPacked(symbolPacked);
      view.setAccountCode(accountCode, 0, accountLen);
      view.setSettlDate(settlDate, 0, settlDateLen);
      view.setCurrency(currency, 0, currLen);
      view.setSettlCurrency(settlCurrency, 0, settlCurrLen);
      inner.put(probePositionKey.copyOf(), view);

      // Add to account secondary index
      probeAccountKey.set(accountCode, 0, ACCOUNT_LENGTH);
      ObjectHashSet<PositionView> accountSet = byAccount.get(probeAccountKey);
      if (accountSet == null) {
        accountSet = new ObjectHashSet<>(16, LOAD_FACTOR);
        byAccount.put(probeAccountKey.copyOf(), accountSet);
      }
      accountSet.add(view);
    } else {
      // Update currency fields if not set (first fill may not have had them)
      if (view.currencyLen() == 0 && currLen > 0) {
        view.setCurrency(currency, 0, currLen);
      }
      if (view.settlCurrencyLen() == 0 && settlCurrLen > 0) {
        view.setSettlCurrency(settlCurrency, 0, settlCurrLen);
      }
    }

    // Update quantities and notional
    final long fillNotional = ProjectionUtil.mulDiv(lastPx, lastQty, PRICE_SCALE);
    if (side == SideEnum.Buy) {
      view.setNetQty(view.netQty() + lastQty);
      view.setBuyQty(view.buyQty() + lastQty);
      view.setBuyCumNotional(view.buyCumNotional() + fillNotional);
    } else {
      view.setNetQty(view.netQty() - lastQty);
      view.setSellQty(view.sellQty() + lastQty);
      view.setSellCumNotional(view.sellCumNotional() + fillNotional);
    }

    view.setLastUpdatedAt(timestamp);
    view.setLastSequenceNumber(seqNo);
  }

  // ---------------------------------------------------------------------------
  // Leg field readers — work around SBE group decoder API
  // ---------------------------------------------------------------------------

  private byte[] readLegSettlDate(final OrderFilledEventDecoder.NoLegsDecoder leg) {
    leg.getLegSettlDate(scratchSettlDate, 0);
    return scratchSettlDate;
  }

  private void readLegCurrency(final OrderFilledEventDecoder.NoLegsDecoder leg) {
    leg.getLegCurrency(scratchCurrency, 0);
    // settlCurrency at leg level: use legCurrency as settlCurrency (legs don't have separate one)
    System.arraycopy(scratchCurrency, 0, scratchSettlCurrency, 0, 3);
  }

  // ---------------------------------------------------------------------------
  // Query methods (acquire read stamp, return immutable snapshots)
  // ---------------------------------------------------------------------------

  /**
   * Looks up a specific position by symbol, account, and settlement date.
   *
   * @param symbol the instrument symbol (FIX tag 55)
   * @param account the account code (FIX tag 1)
   * @param settlDate the settlement date YYYYMMDD (FIX tag 64)
   * @return the position snapshot, or {@code null} if not found
   */
  public PositionSnapshot getPosition(
      final String symbol, final String account, final String settlDate) {
    final long symbolPacked = SymbolPacker.pack(symbol);
    final ByteArrayKey key = compositeKeyFromStrings(account, settlDate);
    final long stamp = lock.readLock();
    try {
      return lookupAndSnapshot(symbolPacked, key, symbol);
    } finally {
      lock.unlockRead(stamp);
    }
  }

  /**
   * Returns all tracked positions.
   *
   * @return list of position snapshots
   */
  public List<PositionSnapshot> getAllPositions() {
    final List<PositionSnapshot> result = new ArrayList<>();
    final long stamp = lock.readLock();
    try {
      collectAllPositions(result);
    } finally {
      lock.unlockRead(stamp);
    }
    return result;
  }

  /**
   * Returns all positions for the given account across all symbols and settlement dates.
   *
   * @param account the account code (FIX tag 1)
   * @return list of position snapshots (empty if no positions for this account)
   */
  public List<PositionSnapshot> getPositionsByAccount(final String account) {
    // Pad to full 16-byte fixed-width for consistent lookup against the byAccount secondary index
    final byte[] accountBytes = padToLength(account, ACCOUNT_LENGTH);
    final List<PositionSnapshot> result = new ArrayList<>();
    final long stamp = lock.readLock();
    try {
      collectByAccount(accountBytes, result);
    } finally {
      lock.unlockRead(stamp);
    }
    return result;
  }

  /**
   * Returns all positions for the given symbol across all accounts and settlement dates.
   *
   * @param symbol the instrument symbol (FIX tag 55)
   * @return list of position snapshots (empty if no positions for this symbol)
   */
  public List<PositionSnapshot> getPositionsBySymbol(final String symbol) {
    final long symbolPacked = SymbolPacker.pack(symbol);
    final List<PositionSnapshot> result = new ArrayList<>();
    final long stamp = lock.readLock();
    try {
      collectBySymbol(symbolPacked, symbol, result);
    } finally {
      lock.unlockRead(stamp);
    }
    return result;
  }

  /**
   * Returns the total number of tracked positions.
   *
   * @return the position count
   */
  public int size() {
    final long stamp = lock.readLock();
    try {
      int count = 0;
      for (final Object2ObjectHashMap<ByteArrayKey, PositionView> inner : positions.values()) {
        count += inner.size();
      }
      return count;
    } finally {
      lock.unlockRead(stamp);
    }
  }

  /**
   * Returns the total number of events that caused a decode or processing error.
   *
   * @return the error count
   */
  public long errorCount() {
    final long stamp = lock.readLock();
    try {
      return errorCount;
    } finally {
      lock.unlockRead(stamp);
    }
  }

  /**
   * Returns the total number of events successfully processed.
   *
   * @return the events processed count
   */
  public long eventsProcessed() {
    final long stamp = lock.readLock();
    try {
      return eventsProcessed;
    } finally {
      lock.unlockRead(stamp);
    }
  }

  // ---------------------------------------------------------------------------
  // Query helpers
  // ---------------------------------------------------------------------------

  private PositionSnapshot lookupAndSnapshot(
      final long symbolPacked, final ByteArrayKey key, final String symbol) {
    final Object2ObjectHashMap<ByteArrayKey, PositionView> inner = positions.get(symbolPacked);
    if (inner == null) {
      return null;
    }
    final PositionView view = inner.get(key);
    if (view == null) {
      return null;
    }
    return createSnapshot(view, symbol);
  }

  private void collectAllPositions(final List<PositionSnapshot> result) {
    for (final Object2ObjectHashMap<ByteArrayKey, PositionView> inner : positions.values()) {
      for (final PositionView view : inner.values()) {
        result.add(createSnapshot(view, SymbolPacker.unpack(view.symbolPacked())));
      }
    }
  }

  private void collectByAccount(final byte[] accountBytes, final List<PositionSnapshot> result) {
    final ByteArrayKey accountKey = ByteArrayKey.copyOf(accountBytes, 0, ACCOUNT_LENGTH);
    final ObjectHashSet<PositionView> set = byAccount.get(accountKey);
    if (set != null) {
      set.forEach(v -> result.add(createSnapshot(v, SymbolPacker.unpack(v.symbolPacked()))));
    }
  }

  private void collectBySymbol(
      final long symbolPacked, final String symbol, final List<PositionSnapshot> result) {
    final Object2ObjectHashMap<ByteArrayKey, PositionView> inner = positions.get(symbolPacked);
    if (inner != null) {
      for (final PositionView view : inner.values()) {
        result.add(createSnapshot(view, symbol));
      }
    }
  }

  private PositionSnapshot createSnapshot(final PositionView view, final String symbol) {
    final long avgBuyPx =
        view.buyQty() > 0
            ? ProjectionUtil.mulDiv(view.buyCumNotional(), PRICE_SCALE, view.buyQty())
            : 0;
    final long avgSellPx =
        view.sellQty() > 0
            ? ProjectionUtil.mulDiv(view.sellCumNotional(), PRICE_SCALE, view.sellQty())
            : 0;
    return PositionSnapshot.from(view, symbol, avgBuyPx, avgSellPx);
  }

  // ---------------------------------------------------------------------------
  // Utilities
  // ---------------------------------------------------------------------------

  private static ByteArrayKey compositeKeyFromStrings(
      final String account, final String settlDate) {
    // Fixed-width 16+8=24 bytes to match event-path key construction and prevent collisions
    final byte[] accountBytes = padToLength(account, ACCOUNT_LENGTH);
    final byte[] settlDateBytes = padToLength(settlDate, SETTL_DATE_LENGTH);
    final byte[] composite = new byte[ACCOUNT_LENGTH + SETTL_DATE_LENGTH];
    System.arraycopy(accountBytes, 0, composite, 0, ACCOUNT_LENGTH);
    System.arraycopy(settlDateBytes, 0, composite, ACCOUNT_LENGTH, SETTL_DATE_LENGTH);
    return ByteArrayKey.copyOf(composite, 0, composite.length);
  }

  private static byte[] padToLength(final String value, final int length) {
    final byte[] padded = new byte[length];
    final byte[] ascii = value.getBytes(StandardCharsets.US_ASCII);
    System.arraycopy(ascii, 0, padded, 0, Math.min(ascii.length, length));
    return padded;
  }
}
