package com.trading.engine.queryservice;

import com.trading.engine.projections.ProjectionRegistry;
import com.trading.engine.projections.account.AccountProjection;
import com.trading.engine.projections.account.AccountReadModel;
import com.trading.engine.projections.order.OrderProjection;
import com.trading.engine.projections.order.OrderSnapshot;
import com.trading.engine.projections.position.PositionProjection;
import com.trading.engine.projections.position.PositionSnapshot;
import com.trading.engine.projections.quote.QuoteProjection;
import com.trading.engine.projections.quote.QuoteSnapshot;
import com.trading.engine.projections.quote.QuoteStatus;
import com.trading.engine.projections.risklimits.RiskLimitProjection;
import com.trading.engine.projections.risklimits.RiskLimitRecordView;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Unified read API aggregating all CQRS projections behind a single query surface. Designed for
 * consumption by the WebSocket server (Wave 7), REST endpoints, and monitoring dashboards.
 *
 * <p><b>Threading:</b> thread-safe — all methods delegate to underlying projection query methods,
 * each of which acquires its own {@link java.util.concurrent.locks.StampedLock} read stamp. No
 * internal state or synchronization. Multiple concurrent queries against different projections do
 * not contend with each other.
 *
 * <p><b>Allocation:</b> pure delegation — no additional allocation beyond what the underlying
 * projections produce (snapshot records, lists). Acceptable on query paths (off hot path).
 *
 * <p><b>Null contract:</b> all String parameters must be non-null ({@link NullPointerException} is
 * thrown for null arguments). Symbol parameters additionally reject empty or overlength values with
 * {@link IllegalArgumentException} (propagated from {@link
 * com.trading.engine.projections.SymbolPacker#pack(String)}).
 *
 * <p><b>Per-projection diagnostics:</b> {@code size()}, {@code errorCount()}, {@code
 * eventsProcessed()} remain accessible directly on the projection instances (not via this class).
 * This class is a query facade, not a monitoring API. Use {@link ProjectionRegistry} for
 * lag/health, or access projections directly for detailed counters.
 *
 * @see OrderProjection
 * @see PositionProjection
 * @see AccountProjection
 * @see QuoteProjection
 * @see ProjectionRegistry
 */
public final class QueryService {

  private final OrderProjection orderProjection;
  private final PositionProjection positionProjection;
  private final AccountProjection accountProjection;
  private final QuoteProjection quoteProjection;
  private final RiskLimitProjection riskLimitProjection;
  private final ProjectionRegistry registry;

  /**
   * Creates a QueryService backed by all five projections and a registry for health/lag.
   *
   * @param orderProjection the order read model; must not be null
   * @param positionProjection the position read model; must not be null
   * @param accountProjection the account read model; must not be null
   * @param quoteProjection the quote read model; must not be null
   * @param riskLimitProjection the APP-62 §A per-account risk-limit read model; must not be null
   * @param registry the projection registry for health and lag monitoring; must not be null
   * @throws NullPointerException if any argument is null
   */
  public QueryService(
      final OrderProjection orderProjection,
      final PositionProjection positionProjection,
      final AccountProjection accountProjection,
      final QuoteProjection quoteProjection,
      final RiskLimitProjection riskLimitProjection,
      final ProjectionRegistry registry) {
    this.orderProjection = Objects.requireNonNull(orderProjection, "orderProjection");
    this.positionProjection = Objects.requireNonNull(positionProjection, "positionProjection");
    this.accountProjection = Objects.requireNonNull(accountProjection, "accountProjection");
    this.quoteProjection = Objects.requireNonNull(quoteProjection, "quoteProjection");
    this.riskLimitProjection = Objects.requireNonNull(riskLimitProjection, "riskLimitProjection");
    this.registry = Objects.requireNonNull(registry, "registry");
    // APP-62 §A — register the risk-limit projection here so its lag + health are tracked by
    // the same ProjectionRegistry surface that powers {@link #isHealthy()} and {@link
    // #getLagSnapshot()}. The other four projections (order, position, account, quote) are
    // registered externally by the launcher / test bootstrap before constructing this service
    // — the constructor-side registration is intentional for risk-limits because it was added
    // after the original wiring contract was established and avoids touching every launcher.
    // APP-62 R11 LOW Agent B #8 — closes the observability gap surfaced in iter 1.
    this.registry.register("risk-limits", riskLimitProjection);
  }

  // ---------------------------------------------------------------------------
  // Order queries (delegate to OrderProjection)
  // ---------------------------------------------------------------------------

  /**
   * Looks up an order by exchange order identifier.
   *
   * @param orderId the exchange order ID (FIX tag 37)
   * @return the order snapshot, or {@code null} if not found
   */
  public OrderSnapshot getOrder(final String orderId) {
    return orderProjection.getOrder(orderId);
  }

  /**
   * Looks up an order by client order identifier.
   *
   * @param clOrdId the client order ID (FIX tag 11)
   * @return the order snapshot, or {@code null} if not found
   */
  public OrderSnapshot getOrderByClOrdId(final String clOrdId) {
    return orderProjection.getOrderByClOrdId(clOrdId);
  }

  /**
   * Returns all orders for the given account.
   *
   * @param accountCode the account code (FIX tag 1)
   * @return list of order snapshots (empty if no orders for this account)
   */
  public List<OrderSnapshot> getOrdersByAccount(final String accountCode) {
    return orderProjection.getOrdersByAccount(accountCode);
  }

  /**
   * Returns all orders for the given symbol.
   *
   * @param symbol the instrument symbol (FIX tag 55)
   * @return list of order snapshots (empty if no orders for this symbol)
   * @throws NullPointerException if symbol is null
   * @throws IllegalArgumentException if symbol is empty or exceeds 8 characters
   */
  public List<OrderSnapshot> getOrdersBySymbol(final String symbol) {
    return orderProjection.getOrdersBySymbol(symbol);
  }

  /**
   * Returns all orders in an active (non-terminal) state.
   *
   * @return list of order snapshots with status New or PartiallyFilled
   */
  public List<OrderSnapshot> getActiveOrders() {
    return orderProjection.getActiveOrders();
  }

  // ---------------------------------------------------------------------------
  // Position queries (delegate to PositionProjection)
  // ---------------------------------------------------------------------------

  /**
   * Looks up a position by symbol, account, and settlement date.
   *
   * @param symbol the instrument symbol (FIX tag 55)
   * @param account the account code (FIX tag 1)
   * @param settlDate the settlement date YYYYMMDD (FIX tag 64)
   * @return the position snapshot, or {@code null} if not found
   */
  public PositionSnapshot getPosition(
      final String symbol, final String account, final String settlDate) {
    return positionProjection.getPosition(symbol, account, settlDate);
  }

  /**
   * Returns all tracked positions.
   *
   * @return list of position snapshots
   */
  public List<PositionSnapshot> getAllPositions() {
    return positionProjection.getAllPositions();
  }

  /**
   * Returns all positions for the given account.
   *
   * @param account the account code (FIX tag 1)
   * @return list of position snapshots (empty if no positions for this account)
   */
  public List<PositionSnapshot> getPositionsByAccount(final String account) {
    return positionProjection.getPositionsByAccount(account);
  }

  /**
   * Returns all positions for the given symbol.
   *
   * @param symbol the instrument symbol (FIX tag 55)
   * @return list of position snapshots (empty if no positions for this symbol)
   * @throws NullPointerException if symbol is null
   * @throws IllegalArgumentException if symbol is empty or exceeds 8 characters
   */
  public List<PositionSnapshot> getPositionsBySymbol(final String symbol) {
    return positionProjection.getPositionsBySymbol(symbol);
  }

  // ---------------------------------------------------------------------------
  // Account queries (delegate to AccountProjection)
  // ---------------------------------------------------------------------------

  /**
   * Looks up an account by numeric account identifier.
   *
   * @param accountId the account ID
   * @return the account read model, or {@code null} if not found
   */
  public AccountReadModel getAccountById(final long accountId) {
    return accountProjection.getByAccountId(accountId);
  }

  /**
   * Looks up an account by account code.
   *
   * @param accountCode the account code (FIX tag 1)
   * @return the account read model, or {@code null} if not found
   */
  public AccountReadModel getAccountByCode(final String accountCode) {
    return accountProjection.getByAccountCode(accountCode);
  }

  /**
   * Returns all tracked accounts.
   *
   * @return list of account read models
   */
  public List<AccountReadModel> getAllAccounts() {
    return accountProjection.getAll();
  }

  /**
   * Returns all accounts in active status.
   *
   * @return list of active account read models
   */
  public List<AccountReadModel> getActiveAccounts() {
    return accountProjection.getActiveAccounts();
  }

  // ---------------------------------------------------------------------------
  // APP-62 §A — Risk-limit queries (delegate to RiskLimitProjection)
  // ---------------------------------------------------------------------------

  /**
   * Look up the per-account risk-limit snapshot by numeric account id (APP-62 §A). Returns the
   * immutable record from the projection or {@code null} if no {@code RiskLimitLoadedEvent} has
   * been seen for this account (cold boot / unprovisioned account).
   *
   * @param accountId numeric account identifier
   * @return the projection record or {@code null} when no limit has been loaded
   */
  public RiskLimitRecordView getAccountLimits(final long accountId) {
    return riskLimitProjection.getByAccountId(accountId);
  }

  /**
   * Look up the per-account risk-limit snapshot by account code (APP-62 §A bridge integration).
   * Joins {@link AccountProjection} on accountCode to resolve the {@code accountId}, then delegates
   * to {@link #getAccountLimits(long)}. Returns {@code null} when either the account is unknown OR
   * no risk-limit record has been loaded for the resolved id (the bridge interprets {@code null} as
   * "emit pessimistic zeros" per the existing fail-secure contract).
   *
   * @param accountCode the FIX-style account code (tag 1)
   * @return the projection record or {@code null} when account is unknown or no limit loaded
   */
  public RiskLimitRecordView getAccountLimits(final String accountCode) {
    Objects.requireNonNull(accountCode, "accountCode");
    final var account = accountProjection.getByAccountCode(accountCode);
    if (account == null) {
      return null;
    }
    return riskLimitProjection.getByAccountId(account.accountId());
  }

  // ---------------------------------------------------------------------------
  // Quote queries (delegate to QuoteProjection)
  // ---------------------------------------------------------------------------

  /**
   * Looks up a quote by quote identifier.
   *
   * @param quoteId the quote ID (FIX tag 117)
   * @return the quote snapshot, or {@code null} if not found
   */
  public QuoteSnapshot getQuote(final String quoteId) {
    return quoteProjection.getQuote(quoteId);
  }

  /**
   * Looks up a quote by quote request identifier.
   *
   * @param quoteReqId the quote request ID (FIX tag 131)
   * @return the quote snapshot, or {@code null} if not found
   */
  public QuoteSnapshot getQuoteByReqId(final String quoteReqId) {
    return quoteProjection.getQuoteByReqId(quoteReqId);
  }

  /**
   * Returns all quotes in {@link QuoteStatus#Active} state.
   *
   * @return list of active quote snapshots
   */
  public List<QuoteSnapshot> getActiveQuotes() {
    return quoteProjection.getActiveQuotes();
  }

  /**
   * Returns all in-flight quotes ({@link QuoteStatus#Requested} or {@link QuoteStatus#Active}).
   *
   * @return list of in-flight quote snapshots
   */
  public List<QuoteSnapshot> getInFlightQuotes() {
    return quoteProjection.getInFlightQuotes();
  }

  /**
   * Returns all quotes for the given symbol.
   *
   * @param symbol the instrument symbol (FIX tag 55)
   * @return list of quote snapshots (empty if no quotes for this symbol)
   * @throws NullPointerException if symbol is null
   * @throws IllegalArgumentException if symbol is empty or exceeds 8 characters
   */
  public List<QuoteSnapshot> getQuotesBySymbol(final String symbol) {
    return quoteProjection.getQuotesBySymbol(symbol);
  }

  /**
   * Returns all quotes for the given account.
   *
   * @param accountCode the account code (FIX tag 1)
   * @return list of quote snapshots (empty if no quotes for this account)
   */
  public List<QuoteSnapshot> getQuotesByAccount(final String accountCode) {
    return quoteProjection.getQuotesByAccount(accountCode);
  }

  /**
   * Returns all quotes matching the given lifecycle status.
   *
   * @param status the lifecycle status to filter by
   * @return list of matching quote snapshots
   */
  public List<QuoteSnapshot> getQuotesByStatus(final QuoteStatus status) {
    return quoteProjection.getQuotesByStatus(status);
  }

  // ---------------------------------------------------------------------------
  // Health & diagnostics (delegate to ProjectionRegistry)
  // ---------------------------------------------------------------------------

  /**
   * Returns {@code true} if all registered projections are within the configured lag threshold and
   * the event consumer is alive.
   *
   * @return {@code true} if the system is healthy
   */
  public boolean isHealthy() {
    return registry.isHealthy();
  }

  /**
   * Returns a point-in-time snapshot of per-projection lag (projection name &rarr; lag in
   * messages). Allocates a new map on every call — suitable for diagnostic endpoints, not hot-path
   * monitoring.
   *
   * @return map of projection name to lag (always non-negative)
   */
  public Map<String, Long> getLagSnapshot() {
    return registry.getLagSnapshot();
  }
}
