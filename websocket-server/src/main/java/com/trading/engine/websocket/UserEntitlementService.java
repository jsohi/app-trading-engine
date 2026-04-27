package com.trading.engine.websocket;

import com.trading.engine.messages.sbe.AccountStatusEnum;
import com.trading.engine.projections.account.AccountReadModel;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Validates account entitlements from JWT {@code accounts} claim against the cluster's account
 * projections via {@code QueryService}.
 *
 * <p>At authentication time, the JWT contains an {@code accounts} claim listing the account codes
 * the user is permitted to access. This service validates each code against the read-model: the
 * account must exist AND have {@link AccountStatusEnum#Active} status. Only validated accounts are
 * stored on the {@link WebSocketSession} for drain-path entitlement filtering.
 *
 * <p><b>Defense in depth.</b> The cluster independently validates account codes on every command
 * via {@code AccountStore.getByCode()}. This service provides the WebSocket layer's own check,
 * preventing unauthorized event delivery without relying solely on the cluster.
 *
 * <p><b>Threading.</b> Thread-safe — delegates to {@code QueryService} which acquires StampedLock
 * read stamps internally.
 *
 * <p><b>Allocation.</b> Allocates a {@link HashSet} per validation call. Acceptable — called once
 * at auth time (cold path), not per-message.
 *
 * <p><b>Caching.</b> No caching in this implementation. Account validation happens once at auth
 * time and again on re-auth (token refresh). The {@code QueryService} read path is O(1) per account
 * via the AccountProjection's secondary index.
 *
 * @see JwtValidator.ValidatedClaims
 * @see <a href="docs/websocket-architecture.md">WebSocket Architecture — Section 4</a>
 */
public final class UserEntitlementService {

  private static final Logger LOG = LogManager.getLogger(UserEntitlementService.class);

  private final Function<String, AccountReadModel> accountLookup;

  /**
   * Create a new entitlement service.
   *
   * <p>The account lookup function typically delegates to {@code QueryService::getAccountByCode}.
   * Using a functional interface instead of the full QueryService enables testability without
   * mocking frameworks (this project does not use Mockito).
   *
   * @param accountLookup function that maps an account code to its {@link AccountReadModel}, or
   *     null if the account is unknown. Typically {@code queryService::getAccountByCode}.
   * @throws NullPointerException if accountLookup is null
   */
  public UserEntitlementService(final Function<String, AccountReadModel> accountLookup) {
    this.accountLookup = Objects.requireNonNull(accountLookup, "accountLookup");
  }

  /**
   * Validate a list of account codes from the JWT {@code accounts} claim. Returns the subset of
   * codes that are known to the system and currently active.
   *
   * <p>Accounts that are unknown (not loaded via reference data), suspended, or closed are excluded
   * from the result. The caller should reject authentication if the result is empty (all accounts
   * invalid).
   *
   * @param accountCodes the list of account codes from the JWT {@code accounts} claim; must not be
   *     null
   * @return an unmodifiable set of validated active account codes; may be empty if all are
   *     invalid/inactive
   * @throws NullPointerException if accountCodes is null
   */
  public Set<String> validateAccounts(final List<String> accountCodes) {
    Objects.requireNonNull(accountCodes, "accountCodes");

    final var validated = new HashSet<String>();
    int rejected = 0;

    for (final String code : accountCodes) {
      if (code == null || code.isEmpty()) {
        rejected++;
        continue;
      }

      final var account = accountLookup.apply(code);
      if (account == null) {
        LOG.info("Account code not found in projections: {}", code);
        rejected++;
        continue;
      }

      if (account.status() != AccountStatusEnum.Active) {
        LOG.info("Account {} has non-active status: {}", code, account.status());
        rejected++;
        continue;
      }

      validated.add(code);
    }

    if (rejected > 0) {
      LOG.info(
          "Account validation: {}/{} active, {} rejected",
          validated.size(),
          accountCodes.size(),
          rejected);
    }

    return Set.copyOf(validated);
  }

  // TODO(APP-236): add symbol-level subscription entitlement check. Currently any authenticated
  // user can subscribe to any symbol. Architecture doc's entitlement model is account-based (JWT
  // accounts claim), not symbol-based. Symbol ACLs require a separate symbol-entitlement mapping
  // not yet in the data model.
}
