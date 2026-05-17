package com.trading.engine.websocket;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.trading.engine.messages.sbe.AccountStatusEnum;
import com.trading.engine.messages.sbe.AccountTypeEnum;
import com.trading.engine.messages.sbe.AcctIDSourceEnum;
import com.trading.engine.messages.sbe.ComplianceStatusEnum;
import com.trading.engine.projections.account.AccountReadModel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link UserEntitlementService} -- verifies account validation against account lookup,
 * including active/inactive filtering, unknown accounts, and empty claim handling.
 *
 * <p>Uses a simple {@link HashMap}-backed lookup function instead of mocking (this project does not
 * use Mockito).
 */
final class UserEntitlementServiceTest {

  // --- Active accounts ---

  @Test
  void validateAccounts_activeAccount_included() {
    final var lookup = lookupWith(activeAccount("ACME-001"));
    final var service = new UserEntitlementService(lookup::get);

    final Set<String> result = service.validateAccounts(List.of("ACME-001"));

    assertEquals(Set.of("ACME-001"), result);
  }

  @Test
  void validateAccounts_multipleActiveAccounts_allIncluded() {
    final var lookup = lookupWith(activeAccount("ACME-001"), activeAccount("HEDGE-002"));
    final var service = new UserEntitlementService(lookup::get);

    final Set<String> result = service.validateAccounts(List.of("ACME-001", "HEDGE-002"));

    assertEquals(Set.of("ACME-001", "HEDGE-002"), result);
  }

  // --- Inactive/suspended/closed accounts ---

  @Test
  void validateAccounts_suspendedAccount_excluded() {
    final var lookup = lookupWith(accountWithStatus("SUSP-001", AccountStatusEnum.Suspended));
    final var service = new UserEntitlementService(lookup::get);

    final Set<String> result = service.validateAccounts(List.of("SUSP-001"));

    assertTrue(result.isEmpty());
  }

  @Test
  void validateAccounts_closedAccount_excluded() {
    final var lookup = lookupWith(accountWithStatus("CLOSED-01", AccountStatusEnum.Closed));
    final var service = new UserEntitlementService(lookup::get);

    final Set<String> result = service.validateAccounts(List.of("CLOSED-01"));

    assertTrue(result.isEmpty());
  }

  // --- Unknown accounts ---

  @Test
  void validateAccounts_unknownAccount_excluded() {
    final var lookup = lookupWith(); // empty lookup
    final var service = new UserEntitlementService(lookup::get);

    final Set<String> result = service.validateAccounts(List.of("UNKNOWN"));

    assertTrue(result.isEmpty());
  }

  // --- Mixed results ---

  @Test
  void validateAccounts_mixedActiveAndInactive_onlyActiveIncluded() {
    final var lookup =
        lookupWith(
            activeAccount("ACME-001"), accountWithStatus("SUSP-001", AccountStatusEnum.Suspended));
    final var service = new UserEntitlementService(lookup::get);

    final Set<String> result = service.validateAccounts(List.of("ACME-001", "SUSP-001", "UNKNOWN"));

    assertEquals(Set.of("ACME-001"), result);
  }

  // --- Empty and null handling ---

  @Test
  void validateAccounts_emptyList_returnsEmptySet() {
    final var service = new UserEntitlementService(code -> null);

    final Set<String> result = service.validateAccounts(List.of());

    assertTrue(result.isEmpty());
  }

  @Test
  void validateAccounts_nullCodesInList_skipped() {
    final var lookup = lookupWith(activeAccount("ACME-001"));
    final var service = new UserEntitlementService(lookup::get);

    final var codes = new ArrayList<String>();
    codes.add(null);
    codes.add("ACME-001");
    codes.add("");

    final Set<String> result = service.validateAccounts(codes);

    assertEquals(Set.of("ACME-001"), result);
  }

  @Test
  void validateAccounts_allInvalid_returnsEmptySet() {
    final var service = new UserEntitlementService(code -> null);

    final Set<String> result = service.validateAccounts(List.of("UNKNOWN-1", "UNKNOWN-2"));

    assertTrue(result.isEmpty());
  }

  // --- Result immutability ---

  @Test
  void validateAccounts_returnedSet_isUnmodifiable() {
    final var lookup = lookupWith(activeAccount("ACME-001"));
    final var service = new UserEntitlementService(lookup::get);

    final Set<String> result = service.validateAccounts(List.of("ACME-001"));

    try {
      result.add("HACKED");
      throw new AssertionError("Expected UnsupportedOperationException");
    } catch (final UnsupportedOperationException expected) {
      // Set.copyOf returns unmodifiable set
    }
  }

  // --- Helpers ---

  private static Map<String, AccountReadModel> lookupWith(final AccountReadModel... accounts) {
    final var map = new HashMap<String, AccountReadModel>();
    for (final AccountReadModel account : accounts) {
      map.put(account.accountCode(), account);
    }
    return map;
  }

  private static AccountReadModel activeAccount(final String code) {
    return accountWithStatus(code, AccountStatusEnum.Active);
  }

  private static AccountReadModel accountWithStatus(
      final String code, final AccountStatusEnum status) {
    return new AccountReadModel(
        1L, // accountId
        0L, // parentAccountId
        code, // accountCode
        AcctIDSourceEnum.Internal, // acctIdSource
        "Test Account", // accountName
        AccountTypeEnum.Client, // accountType
        "USD", // baseCurrency
        status, // status
        ComplianceStatusEnum.OK, // complianceStatus
        3L, // capabilities
        true, // canTrade
        true, // canRequestQuotes
        0L, // transactTime
        0L, // sequenceNumber
        0L,
        List.of(),
        List.of()); // lastUpdatedAt
  }
}
